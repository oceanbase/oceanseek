/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_fork_mds_helper.h"

#include "common/ob_timeout_ctx.h"
#include "common/ob_version_def.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "observer/ob_inner_sql_connection.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "share/location_cache/ob_location_struct.h"
#include "share/ob_debug_sync.h"
#include "share/ob_share_util.h"
#include "storage/high_availability/ob_fork_table_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/ob_tablet_autoinc_seq_rpc_handler.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_mds_helper.h"
#include "storage/tablet/ob_tablet_fork_src_mds_data.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
namespace storage
{

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_FORK_TABLE_BLOCK_FINISH_DST_REGISTER, "Block FINISH_FORK_DST registration");
ERRSIM_POINT_DEF(EN_FORK_TABLE_TRACE_FINISH_DST_REGISTER);
ERRSIM_POINT_DEF(EN_FORK_TABLE_TRACE_FINISH_DST_WAIT);
#endif

OB_SERIALIZE_MEMBER(ObTabletForkStatusMdsArg,
                    tenant_id_,
                    ls_id_,
                    tablet_ids_,
                    fork_dst_tablet_ids_,
                    tablet_status_,
                    data_type_,
                    fork_snapshot_version_);

bool ObTabletForkStatusMdsArg::is_valid() const
{
  bool valid = OB_INVALID_TENANT_ID != tenant_id_
      && ls_id_.is_valid()
      && !tablet_ids_.empty()
      && tablet_status_ != ObTabletStatus::NONE
      && tablet_status_.is_valid()
      && data_type_ != ObTabletMdsUserDataType::NONE;
  if (valid && need_write_src_fork_mds()) {
    valid = tablet_ids_.count() == fork_dst_tablet_ids_.count();
    for (int64_t i = 0; valid && i < fork_dst_tablet_ids_.count(); ++i) {
      valid = fork_dst_tablet_ids_.at(i).is_valid();
    }
    if (valid && ObTabletMdsUserDataType::START_FORK_SRC == data_type_) {
      valid = fork_snapshot_version_ > 0;
    }
  } else if (valid) {
    valid = fork_dst_tablet_ids_.empty();
  }
  return valid;
}

void ObTabletForkStatusMdsArg::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  tablet_ids_.reset();
  fork_dst_tablet_ids_.reset();
  tablet_status_ = ObTabletStatus::NONE;
  data_type_ = ObTabletMdsUserDataType::NONE;
  fork_snapshot_version_ = 0;
}

int ObTabletForkStatusMdsArg::set_fork_src_mds_data(const int64_t fork_snapshot_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fork_snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid src fork mds data", K(ret), K(fork_snapshot_version));
  } else {
    fork_snapshot_version_ = fork_snapshot_version;
  }
  return ret;
}

int ObTabletForkStatusMdsArg::init(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    const common::ObIArray<common::ObTabletID> &fork_dst_tablet_ids,
    const ObTabletStatus tablet_status,
    const ObTabletMdsUserDataType data_type)
{
  int ret = OB_SUCCESS;
  const bool need_fork_dst_tablet_ids = ObTabletMdsUserDataType::START_FORK_SRC == data_type
      || ObTabletMdsUserDataType::FINISH_FORK_SRC == data_type
      || ObTabletMdsUserDataType::ABORT_FORK_SRC == data_type;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !ls_id.is_valid()
                  || tablet_ids.empty()
                  || (need_fork_dst_tablet_ids
                      && tablet_ids.count() != fork_dst_tablet_ids.count())
                  || (!need_fork_dst_tablet_ids && !fork_dst_tablet_ids.empty())
                  || tablet_status == ObTabletStatus::NONE
                  || !tablet_status.is_valid()
                  || data_type == ObTabletMdsUserDataType::NONE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid arg", K(ret), K(tenant_id), K(ls_id), K(tablet_ids),
        K(fork_dst_tablet_ids), K(tablet_status), K(data_type));
  } else if (OB_FAIL(tablet_ids_.assign(tablet_ids))) {
    LOG_WARN("[FORKTABLE] failed to assign tablet ids", K(ret), K(tablet_ids));
  } else if (OB_FAIL(fork_dst_tablet_ids_.assign(fork_dst_tablet_ids))) {
    tablet_ids_.reset();
    LOG_WARN("[FORKTABLE] failed to assign fork dst tablet ids", K(ret), K(fork_dst_tablet_ids));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    tablet_status_ = tablet_status;
    data_type_ = data_type;
  }
  return ret;
}

int ObTabletForkStatusMdsHelper::register_mds(
    const ObTabletForkStatusMdsArg &arg,
    ObMySQLTransaction &trans,
    const ObTxDataSourceType type)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *isql_conn = nullptr;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork status mds arg", K(ret), K(arg));
  } else if (OB_UNLIKELY(ObTxDataSourceType::FORK_TABLE_SRC_MDS != type
                         && ObTxDataSourceType::FORK_TABLE_FINISH_MDS != type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork status mds type", K(ret), K(type), K(arg));
  } else if (OB_ISNULL(isql_conn = trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] invalid connection", K(ret), K(arg));
  } else {
    const int64_t size = arg.get_serialize_size();
    // RS 可能不承载目标用户租户，临时序列化内存必须从 server tenant 申请。
    ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "ForkStatMdsBuf"), OB_MALLOC_NORMAL_BLOCK_SIZE);
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[FORKTABLE] failed to allocate", K(ret), K(size));
    } else if (OB_FAIL(arg.serialize(buf, size, pos))) {
      LOG_WARN("[FORKTABLE] failed to serialize arg", K(ret), K(arg));
    } else if (OB_FAIL(static_cast<observer::ObInnerSQLConnection *>(isql_conn)->register_multi_data_source(
        arg.tenant_id_, arg.ls_id_, type, buf, pos))) {
      LOG_WARN("[FORKTABLE] failed to register fork status mds", K(ret), K(type), K(arg));
    }
  }
  return ret;
}

int ObTabletForkStatusMdsHelper::on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletForkStatusMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("[FORKTABLE] failed to deserialize arg", K(ret));
#ifdef ERRSIM
  } else if (ObTabletMdsUserDataType::FINISH_FORK_DST == arg.data_type_
             && OB_UNLIKELY(EN_FORK_TABLE_BLOCK_FINISH_DST_REGISTER)) {
    ret = OB_EAGAIN;
    LOG_INFO("[FORKTABLE] EN_FORK_TABLE_BLOCK_FINISH_DST_REGISTER: block finish dst registration", K(ret), K(arg));
    for (int64_t i = 0; i < arg.tablet_ids_.count(); ++i) {
      if (OB_SUCCESS != EVENT_CALL(EN_FORK_TABLE_TRACE_FINISH_DST_REGISTER, arg.tablet_ids_.at(i).id())) {
        SERVER_EVENT_ADD("FORK_TABLE_TEST", "FINISH_DST_REGISTER_BLOCKED",
            "tenant_id", arg.tenant_id_, "ls_id", arg.ls_id_.id(),
            "tablet_id", arg.tablet_ids_.at(i).id());
      }
    }
#endif
  } else if (OB_FAIL(modify(arg, SCN::invalid_scn(), ctx))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("[FORKTABLE] failed to register process", K(ret), K(arg));
    }
  }
  return ret;
}

int ObTabletForkStatusMdsHelper::on_replay(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletForkStatusMdsArg arg;
  if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("[FORKTABLE] failed to deserialize arg", K(ret));
  } else if (OB_FAIL(modify(arg, scn, ctx))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("[FORKTABLE] failed to replay process", K(ret), K(arg), K(scn));
    }
  }
  return ret;
}

int ObTabletForkStatusMdsHelper::modify(
    const ObTabletForkStatusMdsArg &arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const share::ObLSID &ls_id = arg.ls_id_;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTabletID fork_dst_tablet_id;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid arg", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("[FORKTABLE] fail to get ls", K(ret), K(arg));
    }
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] ls should not be NULL", K(ret), K(arg), KP(ls));
  }
  if (OB_SUCC(ret) && ObTabletMdsUserDataType::FINISH_FORK_DST == arg.data_type_) {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    bool need_retry = false;
    // FINISH_FORK_DST 整批预检通过后才写入 MDS；replay 遇到 OB_EAGAIN 时继续投递后续 tablet 的 refresh。
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
      int check_ret = OB_SUCCESS;
      const ObTabletID &tablet_id = arg.tablet_ids_.at(i);
      tablet_handle.reset();
      tablet = nullptr;
      if (OB_SUCCESS != (check_ret = ObTabletBindingHelper::get_tablet_for_new_mds(*ls, tablet_id, scn,
          tablet_handle))) {
        if (OB_NO_NEED_UPDATE == check_ret) {
          check_ret = OB_SUCCESS;
        } else if (OB_EAGAIN != check_ret) {
          LOG_WARN("[FORKTABLE] failed to get tablet before finish fork dst", KR(check_ret), K(ls_id), K(tablet_id),
              K(scn));
        }
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        check_ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[FORKTABLE] tablet should not be NULL", KR(check_ret), K(ls_id), K(tablet_id), K(scn));
      } else if (OB_SUCCESS != (check_ret = check_fork_dst_finish_ready_(*tablet))) {
        const share::ObForkTabletInfo &fork_info = tablet->get_tablet_meta().fork_info_;
        if (OB_EAGAIN == check_ret && scn.is_valid() && fork_info.is_valid() && fork_info.is_data_complete()
            && !fork_info.is_all_replicas_land_complete()) {
          ObForkTableService *fork_table_service = MTL(ObForkTableService *);
          if (OB_ISNULL(fork_table_service)) {
            LOG_WARN_RET(OB_ERR_UNEXPECTED, "[FORKTABLE] fork table service is null for finish meta refresh",
                K(ls_id), K(tablet_id), K(scn), K(fork_info));
          } else if (OB_SUCCESS != (tmp_ret = fork_table_service->submit_finish_meta_refresh(ls_id, tablet_id,
                         tablet->get_reorganization_scn(), fork_info))) {
            LOG_WARN("[FORKTABLE] failed to submit finish meta refresh", KR(tmp_ret), K(ls_id), K(tablet_id),
                K(scn), K(fork_info));
          }
        }
        if (OB_EAGAIN != check_ret) {
          LOG_WARN("[FORKTABLE] failed to precheck fork dst finish readiness", KR(check_ret), K(ls_id), K(tablet_id),
              K(scn));
        }
      }
      if (OB_EAGAIN == check_ret && scn.is_valid()) {
        need_retry = true;
      } else if (OB_SUCCESS != check_ret) {
        ret = check_ret;
      }
    }
    if (OB_SUCC(ret) && need_retry) {
      ret = OB_EAGAIN;
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = arg.tablet_ids_[i];
    if (arg.need_write_src_fork_mds()) {
      fork_dst_tablet_id = arg.fork_dst_tablet_ids_.at(i);
    }
    if (OB_FAIL(set_tablet_status(*ls, tablet_id, arg.tablet_status_, arg.data_type_, scn, ctx))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("[FORKTABLE] failed to set tablet status", K(ret), K(ls_id), K(tablet_id), K(scn), K(arg));
      }
    } else if (arg.need_write_src_fork_mds()
               && OB_FAIL(write_src_fork_mds(ls_id, tablet_id, fork_dst_tablet_id, arg.data_type_,
                                             arg.fork_snapshot_version_, scn, ctx))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("[FORKTABLE] failed to write src fork mds", K(ret), K(ls_id), K(tablet_id), K(scn), K(arg));
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[FORKTABLE] modify fork tablet status", K(ret), K(scn), K(ctx.get_writer()), K(arg));
  } else if (OB_EAGAIN != ret && REACH_TIME_INTERVAL(1 * 1000 * 1000L)) {
    LOG_INFO("[FORKTABLE] modify fork tablet status", K(ret), K(scn), K(ctx.get_writer()), K(arg));
  }
  return ret;
}

class ObTabletForkSrcMdsReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletForkSrcMdsReplayExecutor()
    : logservice::ObTabletReplayExecutor(),
      user_ctx_(nullptr),
      scn_(),
      key_(nullptr),
      data_(nullptr)
  {}
  int init(mds::BufferCtx &user_ctx, const share::SCN &scn,
           const ObTabletForkSrcMdsKey &key, const ObTabletForkSrcMdsData &data)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("[FORKTABLE] fork src mds replay executor init twice", K(ret), K_(is_inited));
    } else if (OB_UNLIKELY(!scn.is_valid() || !key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FORKTABLE] invalid argument", K(ret), K(scn), K(key));
    } else {
      user_ctx_ = &user_ctx;
      scn_ = scn;
      key_ = &key;
      data_ = &data;
      is_inited_ = true;
    }
    return ret;
  }
protected:
  bool is_replay_update_tablet_status_() const override { return true; }
  bool is_replay_update_mds_table_() const override { return true; }
  int do_replay_(ObTabletHandle &tablet_handle) override
  {
    int ret = OB_SUCCESS;
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(*user_ctx_);
    if (OB_FAIL(replay_to_mds_table_(tablet_handle, *key_, *data_, user_ctx, scn_))) {
      LOG_WARN("[FORKTABLE] failed to replay src fork mds", K(ret), K(scn_));
    }
    return ret;
  }
private:
  mds::BufferCtx *user_ctx_;
  share::SCN scn_;
  const ObTabletForkSrcMdsKey *key_;
  const ObTabletForkSrcMdsData *data_;
};

int ObTabletForkStatusMdsHelper::write_src_fork_mds(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &fork_dst_tablet_id,
    const ObTabletMdsUserDataType data_type,
    const int64_t fork_snapshot_version,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObTabletForkSrcMdsKey fork_key(fork_dst_tablet_id);
  ObTabletForkSrcMdsData data;
  if (OB_UNLIKELY(!fork_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork dst tablet id", K(ret), K(tablet_id), K(fork_dst_tablet_id));
  } else if (ObTabletMdsUserDataType::START_FORK_SRC == data_type) {
    if (OB_UNLIKELY(fork_snapshot_version <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid src fork mds data for start", K(ret), K(tablet_id), K(fork_snapshot_version));
    } else {
      data.fork_snapshot_version_ = fork_snapshot_version;
    }
  } else if (ObTabletMdsUserDataType::FINISH_FORK_SRC == data_type
             || ObTabletMdsUserDataType::ABORT_FORK_SRC == data_type) {
    data.set_invalid();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] unexpected data type for src fork mds", K(ret), K(data_type), K(tablet_id));
  }
  if (OB_FAIL(ret)) {
  } else if (!replay_scn.is_valid()) {
    ObTabletMapKey key(ls_id, tablet_id);
    ObTabletHandle tablet_handle;
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(ctx);
    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
      LOG_WARN("[FORKTABLE] failed to get src tablet for fork mds", K(ret), K(key));
    } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::set(
                   fork_key, data, user_ctx, 0/*lock_timeout*/, false/*check_dup*/))) {
      LOG_WARN("[FORKTABLE] failed to set src fork mds", K(ret), K(key), K(data));
    }
  } else {
    ObTabletForkSrcMdsReplayExecutor executor;
    if (OB_FAIL(executor.init(ctx, replay_scn, fork_key, data))) {
      LOG_WARN("[FORKTABLE] failed to init src fork mds replay executor", K(ret), K(replay_scn));
    } else if (OB_FAIL(executor.execute(replay_scn, ls_id, tablet_id))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("[FORKTABLE] failed to replay src fork mds", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObTabletForkStatusMdsHelper::set_tablet_status(
    ObLS &ls,
    const ObTabletID &tablet_id,
    const ObTabletStatus tablet_status,
    const ObTabletMdsUserDataType data_type,
    const share::SCN &replay_scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_stat;
  share::SCN trans_version;
  // on_register() 传入无效的 replay_scn，on_replay() 传入有效的日志 SCN， 因此这里通过 replay_scn 是否有效区分注册和回放。
  // 注册阶段读取最新状态以检查写冲突；回放阶段读取最新已提交状态，避免已刷入 LS_MDS_TABLET 的更新未提交记录阻塞较早日志的回放。
  if (OB_FAIL(ObTabletBindingHelper::get_tablet_for_new_mds(ls, tablet_id, replay_scn, tablet_handle))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN != ret) {
      LOG_WARN("[FORKTABLE] failed to get tablet", K(ret), K(ls_id), K(tablet_id), K(replay_scn));
    }
  } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_FAIL(replay_scn.is_valid()
                     ? tablet->get_latest_committed_tablet_status(user_data)
                     : tablet->ObITabletMdsInterface::get_latest_tablet_status(
                         user_data, writer, trans_stat, trans_version))) {
    if (OB_EMPTY_RESULT == ret
        && (!tablet->get_tablet_meta().ha_status_.check_allow_read()
            || ObTabletMdsUserDataType::FINISH_FORK_DST == data_type)) {
      // FINISH_FORK_DST 由 replay barrier 保序；暂时读不到前序 START 状态时等待重试。
      ret = OB_EAGAIN;
    } else if (OB_EAGAIN != ret) {
      // 其他错误码原样交由上层处理；回放场景由 replay service 统一分类。
      LOG_WARN("[FORKTABLE] failed to get tablet status",
               K(ret), K(ls_id), K(tablet_id), K(data_type), K(replay_scn), KPC(tablet));
    }
  } else if (OB_UNLIKELY(!replay_scn.is_valid()
                         && trans_stat != mds::TwoPhaseCommitState::ON_COMMIT)) {
    ret = OB_EAGAIN;
    DEBUG_SYNC(FORK_TABLET_STATUS_MDS_WRITER_CONFLICT);
  } else if (ObTabletMdsUserDataType::START_FORK_SRC == data_type
             && !replay_scn.is_valid()
             && OB_UNLIKELY(user_data.tablet_status_ != ObTabletStatus::NORMAL)) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("[FORKTABLE] fork src tablet is not NORMAL, cannot start fork", K(ret), K(user_data));
  } else if (ObTabletMdsUserDataType::FINISH_FORK_DST == data_type
             && OB_FAIL(check_fork_dst_finish_ready_(*tablet))) {
    // 每个副本的 local data 和 shared 完成位均已落入本地 meta 后，才能把 FORK_DST 切回 NORMAL。
    if (OB_EAGAIN != ret) {
      LOG_WARN("[FORKTABLE] fork dst is not ready to finish", K(ret), K(ls_id), K(tablet_id));
    }
  } else {
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx&>(ctx);
    user_data.data_type_ = data_type;
    user_data.tablet_status_ = tablet_status;
    if (!replay_scn.is_valid()) {
      if (OB_FAIL(ls.get_tablet_svr()->set_tablet_status(tablet_id, user_data, user_ctx))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("[FORKTABLE] failed to set user data", K(ret), K(user_data), KPC(tablet));
        }
      }
    } else {
      if (OB_FAIL(ls.get_tablet_svr()->replay_set_tablet_status(tablet_id, replay_scn, user_data, user_ctx))) {
        if (OB_NO_NEED_UPDATE == ret) {
          ret = OB_SUCCESS;
        } else if (OB_EAGAIN != ret) {
          LOG_WARN("[FORKTABLE] failed to set user data", K(ret), K(user_data), KPC(tablet));
        }
      }
    }
  }
  return ret;
}

int ObTabletForkStatusMdsHelper::check_fork_dst_finish_ready_(ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  share::ObForkTabletInfo fork_info;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  if (OB_FAIL(tablet.get_fork_info(fork_info))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_EMPTY_RESULT == ret) {
      ret = OB_EAGAIN;
      LOG_WARN("[FORKTABLE] fork info missing on dst finish, retry", K(ret), K(tablet_id));
    } else {
      LOG_WARN("[FORKTABLE] failed to get fork info for dst finish guard", K(ret), K(tablet_id));
    }
  } else if (!fork_info.is_data_complete() || !fork_info.is_all_replicas_land_complete()) {
    ret = OB_EAGAIN;
#ifdef ERRSIM
    if (OB_SUCCESS != EVENT_CALL(EN_FORK_TABLE_TRACE_FINISH_DST_WAIT, tablet_id.id())) {
      SERVER_EVENT_ADD("FORK_TABLE_TEST", "FINISH_DST_WAIT_LOCAL",
          "tenant_id", MTL_ID(), "ls_id", tablet.get_ls_id().id(),
          "tablet_id", tablet_id.id(), "fork_snapshot", fork_info.get_fork_snapshot_version(),
          "src_tablet_id", fork_info.get_src_tablet_id().id());
    }
#endif
    if (REACH_TIME_INTERVAL(60 * 1000 * 1000L)) {
      LOG_INFO("[FORKTABLE] wait fork dst local finish state", K(ret), K(tablet_id), K(fork_info));
    }
  }
  return ret;
}

int ObTabletForkSrcMdsHelper::on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx)
{
  return ObTabletForkStatusMdsHelper::on_register(buf, len, ctx);
}

int ObTabletForkSrcMdsHelper::on_replay(
    const char *buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  return ObTabletForkStatusMdsHelper::on_replay(buf, len, scn, ctx);
}

ObTabletForkDestMdsArg::ObTabletForkDestMdsArg()
  : tenant_id_(OB_INVALID_TENANT_ID),
    ls_id_(),
    autoinc_seq_arg_(),
    data_version_(DATA_VERSION_4_6_2_0)
{
}

int ObTabletForkDestMdsArg::set_autoinc_seq_arg(const obrpc::ObBatchSetTabletAutoincSeqArg &arg)
{
  return autoinc_seq_arg_.assign(arg);
}

int ObTabletForkDestMdsHelper::register_autoinc_mds(
    const obrpc::ObBatchSetTabletAutoincSeqArg &autoinc_arg,
    const uint64_t data_version,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTabletForkDestMdsArg arg;
  if (OB_UNLIKELY(!autoinc_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid autoinc seq arg for fork dest mds", K(ret), K(autoinc_arg));
  } else if (OB_FAIL(arg.set_autoinc_seq_arg(autoinc_arg))) {
    LOG_WARN("failed to set autoinc seq arg", K(ret), K(autoinc_arg));
  } else {
    arg.tenant_id_ = autoinc_arg.tenant_id_;
    arg.ls_id_ = autoinc_arg.ls_id_;
    arg.data_version_ = data_version;
    if (OB_FAIL(register_fork_dest_mds_(arg, trans))) {
      LOG_WARN("failed to register fork dest mds", K(ret), K(autoinc_arg));
    }
  }
  return ret;
}

int ObTabletForkDestMdsHelper::register_fork_dest_mds_(
    const ObTabletForkDestMdsArg &arg,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObISQLConnection *isql_conn = nullptr;
  const obrpc::ObBatchSetTabletAutoincSeqArg &autoinc_arg = arg.autoinc_seq_arg_;
  if (OB_UNLIKELY(!autoinc_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid autoinc seq arg for fork dest mds", K(ret), K(arg));
  } else if (OB_ISNULL(isql_conn = trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] invalid connection", K(ret), K(arg));
  } else {
    const int64_t size = serialization::encoded_length(arg.data_version_)
      + autoinc_arg.get_serialize_size();
    ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "ForkDestMdsBuf"), OB_MALLOC_NORMAL_BLOCK_SIZE);
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("[FORKTABLE] failed to allocate", K(ret), K(size));
    } else if (OB_FAIL(serialization::encode(buf, size, pos, arg.data_version_))) {
      LOG_WARN("[FORKTABLE] failed to serialize data version", K(ret), K(arg));
    } else if (OB_FAIL(autoinc_arg.serialize(buf, size, pos))) {
      LOG_WARN("[FORKTABLE] failed to serialize autoinc seq arg", K(ret), K(autoinc_arg));
    } else if (OB_FAIL(static_cast<observer::ObInnerSQLConnection *>(isql_conn)->register_multi_data_source(
        arg.tenant_id_, arg.ls_id_, ObTxDataSourceType::FORK_TABLE_DEST_MDS, buf, pos))) {
      LOG_WARN("[FORKTABLE] failed to register fork dest mds", K(ret), K(arg));
    }
  }
  return ret;
}

class ObTabletForkDestAutoincReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletForkDestAutoincReplayExecutor()
    : logservice::ObTabletReplayExecutor(), user_ctx_(nullptr), scn_(), autoinc_seq_(nullptr)
  {}

  int init(
      mds::BufferCtx &user_ctx,
      const share::SCN &scn,
      const ObTabletAutoincSeq &autoinc_seq)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("[FORKTABLE] fork dest autoinc replay executor init twice", K(ret), K_(is_inited));
    } else if (OB_UNLIKELY(!scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("[FORKTABLE] invalid fork dest autoinc replay scn", K(ret), K(scn));
    } else {
      user_ctx_ = &user_ctx;
      scn_ = scn;
      autoinc_seq_ = &autoinc_seq;
      is_inited_ = true;
    }
    return ret;
  }

protected:
  bool is_replay_update_tablet_status_() const override { return true; }
  bool is_replay_update_mds_table_() const override { return true; }
  bool replay_allow_tablet_not_exist_() override { return false; }

  int do_replay_(ObTabletHandle &tablet_handle) override
  {
    int ret = OB_SUCCESS;
    mds::MdsCtx &user_ctx = static_cast<mds::MdsCtx &>(*user_ctx_);
    if (OB_FAIL(replay_to_mds_table_(tablet_handle, *autoinc_seq_, user_ctx, scn_))) {
      LOG_WARN("[FORKTABLE] failed to replay fork dst autoinc seq", K(ret), K(scn_));
    }
    return ret;
  }

private:
  mds::BufferCtx *user_ctx_;
  share::SCN scn_;
  const ObTabletAutoincSeq *autoinc_seq_;
};

int ObTabletForkDestMdsHelper::on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx)
{
  return process_fork_dest_mds_(buf, len, share::SCN()/*invalid*/, ctx);
}

int ObTabletForkDestMdsHelper::on_replay(
    const char *buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  return process_fork_dest_mds_(buf, len, scn, ctx);
}

int ObTabletForkDestMdsHelper::process_fork_dest_mds_(
    const char *buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  uint64_t data_version = DATA_VERSION_4_6_2_0;
  obrpc::ObBatchSetTabletAutoincSeqArg autoinc_arg;
  if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid args", K(ret), KP(buf), K(len));
  } else if (OB_FAIL(serialization::decode(buf, len, pos, data_version))) {
    LOG_WARN("[FORKTABLE] failed to deserialize fork dest data version", K(ret), K(len), K(pos), K(scn));
  } else if (OB_UNLIKELY(data_version < DATA_VERSION_4_6_2_0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork dest data version", K(ret), K(data_version), K(scn));
  } else if (OB_FAIL(autoinc_arg.deserialize(buf, len, pos))) {
    LOG_WARN("[FORKTABLE] failed to deserialize fork dest autoinc arg",
             K(ret), K(len), K(pos), K(data_version), K(scn));
  } else if (OB_FAIL(write_dst_autoinc_seq_(autoinc_arg, scn, ctx))) {
    LOG_WARN("[FORKTABLE] failed to write fork dest autoinc seq", K(ret), K(data_version), K(scn));
  }
  return ret;
}

int ObTabletForkDestMdsHelper::write_dst_autoinc_seq_(
    const obrpc::ObBatchSetTabletAutoincSeqArg &autoinc_arg,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_UNLIKELY(!autoinc_arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[FORKTABLE] invalid fork dest autoinc arg", K(ret), K(autoinc_arg), K(scn));
  } else if (OB_FAIL(MTL(ObLSService*)->get_ls(autoinc_arg.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("[FORKTABLE] failed to get ls for fork dst autoinc", K(ret), K(autoinc_arg));
  } else if (OB_UNLIKELY(nullptr == (ls = ls_handle.get_ls()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[FORKTABLE] ls should not be NULL", K(ret), K(autoinc_arg));
  } else if (!scn.is_valid()) {
    if (OB_FAIL(ObTabletAutoincSeqRpcHandler::get_instance().batch_set_tablet_autoinc_seq_in_trans(
            *ls, autoinc_arg, scn, ctx))) {
      LOG_WARN("[FORKTABLE] failed to set fork dst autoinc seq in trans", K(ret), K(autoinc_arg));
    }
  } else {
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "ForkDstMds"));
    const share::ObLSID &ls_id = autoinc_arg.ls_id_;
    for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_arg.autoinc_params_.count(); ++i) {
      allocator.reuse();
      const ObTabletID &tablet_id = autoinc_arg.autoinc_params_.at(i).dest_tablet_id_;
      const uint64_t autoinc_seq = autoinc_arg.autoinc_params_.at(i).autoinc_seq_;
      ObTabletAutoincSeq data;
      ObTabletForkDestAutoincReplayExecutor replay_executor;
      if (OB_FAIL(data.set_autoinc_seq_value(allocator, autoinc_seq))) {
        LOG_WARN("[FORKTABLE] failed to set fork dst autoinc seq value",
            K(ret), K(tablet_id), K(autoinc_seq));
      } else if (OB_FAIL(replay_executor.init(ctx, scn, data))) {
        LOG_WARN("[FORKTABLE] failed to init fork dst autoinc replay executor", K(ret), K(tablet_id));
      } else if (OB_FAIL(replay_executor.execute(scn, ls_id, tablet_id))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("[FORKTABLE] failed to replay fork dst autoinc seq", K(ret), K(tablet_id), K(scn));
        }
      }
    }
  }
  return ret;
}

int ObTabletForkFinishMdsHelper::on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx)
{
  return ObTabletForkStatusMdsHelper::on_register(buf, len, ctx);
}

int ObTabletForkFinishMdsHelper::on_replay(
    const char *buf,
    const int64_t len,
    const share::SCN &scn,
    mds::BufferCtx &ctx)
{
  return ObTabletForkStatusMdsHelper::on_replay(buf, len, scn, ctx);
}

} // namespace storage
} // namespace oceanbase
