/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/compaction/ob_medium_compaction_info_mds_helper.h"
#include "lib/utility/ob_unify_serialize.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_inner_sql_connection.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace compaction;
using namespace transaction;
using oceanbase::observer::ObInnerSQLConnection;

namespace storage
{

// Unlike an independent Medium clog, transactional MDS must leave row
// commit/abort to the transaction context supplied by the replay framework.
class ObTabletMediumInfoMdsReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletMediumInfoMdsReplayExecutor();
  int init(mds::BufferCtx &user_ctx,
           const share::SCN &scn,
           const compaction::ObMediumCompactionInfo &medium_info);
protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }
  int do_replay_(ObTabletHandle &tablet_handle) override;
  bool is_replay_update_mds_table_() const override
  {
    return true;
  }
private:
  mds::BufferCtx *user_ctx_;
  const compaction::ObMediumCompactionInfo *medium_info_;
  share::SCN scn_;
};

ObTabletMediumInfoMdsReplayExecutor::ObTabletMediumInfoMdsReplayExecutor()
  : user_ctx_(nullptr), medium_info_(nullptr), scn_()
{
}

int ObTabletMediumInfoMdsReplayExecutor::init(
    mds::BufferCtx &user_ctx,
    const share::SCN &scn,
    const compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("medium info mds replay executor init twice", KR(ret));
  } else if (OB_UNLIKELY(!scn.is_valid() || !medium_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid medium info mds replay argument", KR(ret), K(scn), K(medium_info));
  } else {
    user_ctx_ = &user_ctx;
    medium_info_ = &medium_info;
    scn_ = scn;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletMediumInfoMdsReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_handle.is_valid()) || OB_ISNULL(user_ctx_) || OB_ISNULL(medium_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid medium info mds replay state", KR(ret), K(tablet_handle), KP_(user_ctx), KP_(medium_info));
  } else {
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(*user_ctx_);
    if (OB_FAIL(replay_to_mds_table_(tablet_handle,
        compaction::ObMediumCompactionInfoKey(medium_info_->medium_snapshot_), *medium_info_, user_ctx, scn_))) {
      LOG_WARN("failed to replay transactional medium info", KR(ret), K_(scn));
    }
  }
  return ret;
}

ObTabletMediumCompactionInfoMdsItem::ObTabletMediumCompactionInfoMdsItem(ObIAllocator &allocator)
  : allocator_(&allocator),
    tablet_id_(),
    medium_info_()
{
}

ObTabletMediumCompactionInfoMdsItem::~ObTabletMediumCompactionInfoMdsItem()
{
  reset();
}

void ObTabletMediumCompactionInfoMdsItem::reset()
{
  tablet_id_.reset();
  medium_info_.reset();
}

bool ObTabletMediumCompactionInfoMdsItem::is_valid() const
{
  return tablet_id_.is_valid() && medium_info_.is_valid();
}

OB_DEF_SERIALIZE(ObTabletMediumCompactionInfoMdsItem)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, tablet_id_, medium_info_);
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletMediumCompactionInfoMdsItem)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, tablet_id_);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", KR(ret));
  } else if (OB_FAIL(medium_info_.deserialize(*allocator_, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize medium info", KR(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTabletMediumCompactionInfoMdsItem)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, tablet_id_, medium_info_);
  return len;
}

ObTabletMediumCompactionInfoMdsArg::ObTabletMediumCompactionInfoMdsArg()
  : unused_(0),
    ls_id_(),
    items_(),
    allocator_(ObMemAttr(MTL_ID(), "MediumInfoMds"))
{
}

ObTabletMediumCompactionInfoMdsArg::~ObTabletMediumCompactionInfoMdsArg()
{
  reset();
}

void ObTabletMediumCompactionInfoMdsArg::reset()
{
  for (int64_t i = 0; i < items_.count(); ++i) {
    if (OB_NOT_NULL(items_.at(i))) {
      items_.at(i)->~ObTabletMediumCompactionInfoMdsItem();
    }
  }
  unused_ = 0;
  ls_id_.reset();
  items_.reset();
  allocator_.reset();
}

bool ObTabletMediumCompactionInfoMdsArg::is_valid() const
{
  bool valid = ls_id_.is_valid() && items_.count() > 0;
  for (int64_t i = 0; valid && i < items_.count(); ++i) {
    valid = OB_NOT_NULL(items_.at(i)) && items_.at(i)->is_valid();
  }
  return valid;
}

int ObTabletMediumCompactionInfoMdsArg::add_item(
    const ObTabletID &tablet_id,
    const ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  ObTabletMediumCompactionInfoMdsItem *item = nullptr;
  if (OB_UNLIKELY(!tablet_id.is_valid() || !medium_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), K(medium_info));
  } else if (OB_ISNULL(item = OB_NEWx(ObTabletMediumCompactionInfoMdsItem, &allocator_, allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate mds item", KR(ret));
  } else if (OB_FAIL(item->medium_info_.init(allocator_, medium_info))) {
    LOG_WARN("failed to init medium info", KR(ret), K(tablet_id));
  } else {
    item->tablet_id_ = tablet_id;
    if (OB_FAIL(items_.push_back(item))) {
      LOG_WARN("failed to push back mds item", KR(ret), K(tablet_id));
    } else {
      item = nullptr;
    }
  }
  if (OB_NOT_NULL(item)) {
    OB_DELETEx(ObTabletMediumCompactionInfoMdsItem, &allocator_, item);
  }
  return ret;
}

OB_DEF_SERIALIZE(ObTabletMediumCompactionInfoMdsArg)
{
  int ret = OB_SUCCESS;
  const int64_t item_cnt = items_.count();
  LST_DO_CODE(OB_UNIS_ENCODE, unused_, ls_id_);
  OB_UNIS_ENCODE(item_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < item_cnt; ++i) {
    const ObTabletMediumCompactionInfoMdsItem *item = items_.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mds item is null", KR(ret), K(i));
    } else {
      OB_UNIS_ENCODE(*item);
    }
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletMediumCompactionInfoMdsArg)
{
  int ret = OB_SUCCESS;
  int64_t item_cnt = 0;
  LST_DO_CODE(OB_UNIS_DECODE, unused_, ls_id_);
  OB_UNIS_DECODE(item_cnt);
  for (int64_t i = 0; OB_SUCC(ret) && i < item_cnt; ++i) {
    ObTabletMediumCompactionInfoMdsItem *item = nullptr;
    if (OB_ISNULL(item = OB_NEWx(ObTabletMediumCompactionInfoMdsItem, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate mds item", KR(ret), K(i));
    } else if (OB_FAIL(item->deserialize(buf, data_len, pos))) {
      LOG_WARN("failed to deserialize item", KR(ret), K(i));
    } else if (OB_FAIL(items_.push_back(item))) {
      LOG_WARN("failed to push back mds item", KR(ret), K(i), KPC(item));
    } else {
      item = nullptr;
    }
    if (OB_NOT_NULL(item)) {
      OB_DELETEx(ObTabletMediumCompactionInfoMdsItem, &allocator_, item);
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTabletMediumCompactionInfoMdsArg)
{
  int64_t len = 0;
  const int64_t item_cnt = items_.count();
  LST_DO_CODE(OB_UNIS_ADD_LEN, unused_, ls_id_);
  OB_UNIS_ADD_LEN(item_cnt);
  for (int64_t i = 0; i < item_cnt; ++i) {
    const ObTabletMediumCompactionInfoMdsItem *item = items_.at(i);
    if (OB_NOT_NULL(item)) {
      OB_UNIS_ADD_LEN(*item);
    }
  }
  return len;
}

int ObTabletMediumCompactionInfoMdsHelper::process_one(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObMediumCompactionInfo &medium_info,
    const SCN &scn,
    mds::BufferCtx &ctx,
    const bool for_replay)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle is invalid", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K(ls_id), K(tablet_id));
  } else if (!for_replay) {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);
    if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
      LOG_WARN("failed to get tablet", KR(ret), K(ls_id), K(tablet_id), K(medium_info));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet->ObITabletMdsInterface::set(
        ObMediumCompactionInfoKey(medium_info.medium_snapshot_),
        medium_info,
        user_ctx,
        0/*lock_timeout_us*/))) {
      LOG_WARN("failed to set medium compaction info", KR(ret), K(ls_id), K(tablet_id), K(medium_info));
    } else {
      FLOG_INFO("success to register medium compaction info", KR(ret), K(ls_id), K(tablet_id), K(medium_info));
    }
  } else {
    ObTabletMediumInfoMdsReplayExecutor replay_executor;
    if (OB_FAIL(replay_executor.init(ctx, scn, medium_info))) {
      LOG_WARN("failed to init replay executor", KR(ret), K(ls_id), K(tablet_id), K(medium_info), K(scn));
    } else if (OB_FAIL(replay_executor.execute(scn, ls_id, tablet_id))) {
      if (OB_TABLET_NOT_EXIST == ret || OB_NO_NEED_UPDATE == ret) {
        LOG_INFO("skip medium compaction info replay", KR(ret), K(ls_id), K(tablet_id));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to replay medium compaction info", KR(ret), K(ls_id), K(tablet_id), K(scn));
      }
    } else {
      FLOG_INFO("success to replay medium compaction info", KR(ret), K(ls_id), K(tablet_id), K(scn));
    }
  }
  return ret;
}

int ObTabletMediumCompactionInfoMdsHelper::process_batch(
    const ObTabletMediumCompactionInfoMdsArg &arg,
    const SCN &scn,
    mds::BufferCtx &ctx,
    const bool for_replay)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.items_.count(); ++i) {
    const ObTabletMediumCompactionInfoMdsItem *item = arg.items_.at(i);
    if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mds item is null", KR(ret), K(i), K(arg));
    } else if (OB_FAIL(process_one(
        arg.ls_id_,
        item->tablet_id_,
        item->medium_info_,
        scn,
        ctx,
        for_replay))) {
      LOG_WARN("failed to process medium compaction info", KR(ret), K(i), K(for_replay), K(arg), K(scn));
    }
  }
  return ret;
}

int ObTabletMediumCompactionInfoMdsHelper::on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTabletMediumCompactionInfoMdsArg arg;
  int64_t pos = 0;
  if (OB_UNLIKELY(nullptr == buf || len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(process_batch(arg, SCN(), ctx, false/*for_replay*/))) {
    LOG_WARN("failed to process medium compaction infos", KR(ret), K(arg));
  }
  return ret;
}

int ObTabletMediumCompactionInfoMdsHelper::on_replay(const char *buf, const int64_t len, const SCN &scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTabletMediumCompactionInfoMdsArg arg;
  int64_t pos = 0;
  if (OB_UNLIKELY(nullptr == buf || len <= 0 || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len), K(scn));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize arg", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_FAIL(process_batch(arg, scn, ctx, true/*for_replay*/))) {
    LOG_WARN("failed to process medium compaction infos", KR(ret), K(arg), K(scn));
  }
  return ret;
}

int ObTabletMediumCompactionInfoMdsHelper::register_mds(
    const ObTabletMediumCompactionInfoMdsArg &arg,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObInnerSQLConnection *conn = nullptr;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(arg));
  } else if (OB_ISNULL(conn = static_cast<ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid connection", KR(ret));
  } else {
    const int64_t size = arg.get_serialize_size();
    ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "MediumInfoMds"));
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate", KR(ret));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      LOG_WARN("failed to serialize arg", KR(ret), K(arg));
    } else if (OB_FAIL(conn->register_multi_data_source(
        MTL_ID(), arg.ls_id_, ObTxDataSourceType::MEDIUM_COMPACTION_INFO, buf, pos))) {
      LOG_WARN("failed to register medium compaction mds", KR(ret), K(arg));
    }
  }
  return ret;
}

int ObTabletMediumCompactionInfoMdsHelper::submit_medium_compaction_infos(
    const ObTabletMediumCompactionInfoMdsArg &arg)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (!arg.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid submit arg", KR(ret), K(arg));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, MTL_ID()))) {
    LOG_WARN("failed to start transaction", KR(ret));
  } else if (OB_FAIL(register_mds(arg, trans))) {
    LOG_WARN("failed to register medium compaction mds", KR(ret), K(arg));
  }
  if (trans.is_started()) {
    int trans_ret = trans.end(OB_SUCCESS == ret);
    if (OB_SUCCESS != trans_ret) {
      LOG_WARN("end transaction failed", KR(trans_ret), K(arg));
      ret = OB_SUCCESS == ret ? trans_ret : ret;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
