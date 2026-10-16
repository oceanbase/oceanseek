/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX MDS

#include "storage/mds_table/ob_test_mds_types.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "logservice/replayservice/ob_tablet_replay_executor.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

OB_SERIALIZE_MEMBER(ObTestMdsSingleRowValue, value_, commit_version_, commit_scn_, redo_scn_);
OB_SERIALIZE_MEMBER(ObTestMdsMultiRowKey, key_);
OB_SERIALIZE_MEMBER(ObTestMdsMultiRowValue, value_, commit_version_, commit_scn_, redo_scn_);
OB_SERIALIZE_MEMBER(ObTestMdsRegisterArg, ls_id_, tablet_id_, key_, value_, check_dup_);

namespace
{

int get_test_mds_tablet_(const ObTestMdsRegisterArg &arg, ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService *);
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid test mds register arg", KR(ret), K(arg));
  } else if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret));
  } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("failed to get ls", KR(ret), K(arg.ls_id_));
  } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls handle is invalid", KR(ret), K(arg.ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K(arg.ls_id_));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(arg.tablet_id_, tablet_handle))) {
    LOG_WARN("failed to get tablet", KR(ret), K(arg));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", KR(ret), K(arg));
  }
  return ret;
}

ObTestMdsSingleRowValue build_single_row_value_(const ObTestMdsRegisterArg &arg)
{
  return ObTestMdsSingleRowValue(arg.value_);
}

ObTestMdsMultiRowValue build_multi_row_value_(const ObTestMdsRegisterArg &arg)
{
  return ObTestMdsMultiRowValue(arg.value_);
}

class ObTestMdsSingleRowReplayExecutor : public logservice::ObTabletReplayExecutor
{
public:
  ObTestMdsSingleRowReplayExecutor(const ObTestMdsRegisterArg &arg,
                                   const ObTestMdsSingleRowValue &value)
    : arg_(arg),
      value_(value),
      user_ctx_(nullptr),
      scn_()
  {}

  int init(MdsCtx &ctx, const share::SCN &scn)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("init twice", KR(ret));
    } else if (OB_UNLIKELY(!arg_.is_valid() || !scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(arg_), K(scn));
    } else {
      user_ctx_ = &ctx;
      scn_ = scn;
      is_inited_ = true;
    }
    return ret;
  }

protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }

  bool is_replay_update_mds_table_() const override
  {
    return true;
  }

  int do_replay_(ObTabletHandle &tablet_handle) override
  {
    return replay_to_mds_table_(tablet_handle, value_, *user_ctx_, scn_);
  }

private:
  const ObTestMdsRegisterArg &arg_;
  const ObTestMdsSingleRowValue &value_;
  MdsCtx *user_ctx_;
  share::SCN scn_;
};

class ObTestMdsMultiRowReplayExecutor : public logservice::ObTabletReplayExecutor
{
public:
  ObTestMdsMultiRowReplayExecutor(const ObTestMdsRegisterArg &arg,
                                  const ObTestMdsMultiRowKey &key,
                                  const ObTestMdsMultiRowValue &value)
    : arg_(arg),
      key_(key),
      value_(value),
      user_ctx_(nullptr),
      scn_()
  {}

  int init(MdsCtx &ctx, const share::SCN &scn)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("init twice", KR(ret));
    } else if (OB_UNLIKELY(!arg_.is_valid() || !scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(arg_), K(scn));
    } else {
      user_ctx_ = &ctx;
      scn_ = scn;
      is_inited_ = true;
    }
    return ret;
  }

protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }

  bool is_replay_update_mds_table_() const override
  {
    return true;
  }

  int do_replay_(ObTabletHandle &tablet_handle) override
  {
    return replay_to_mds_table_(tablet_handle, key_, value_, *user_ctx_, scn_);
  }

private:
  const ObTestMdsRegisterArg &arg_;
  const ObTestMdsMultiRowKey &key_;
  const ObTestMdsMultiRowValue &value_;
  MdsCtx *user_ctx_;
  share::SCN scn_;
};

} // namespace

int ObTestMdsSingleRowHelper::on_register(const char *buf, const int64_t len, BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTestMdsRegisterArg arg;
  int64_t pos = 0;
  ObTabletHandle tablet_handle;
  MdsCtx &mds_ctx = static_cast<MdsCtx &>(ctx);
  if (OB_UNLIKELY(nullptr == buf || len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize test mds register arg", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid test mds register arg", KR(ret), K(arg));
  } else if (OB_FAIL(get_test_mds_tablet_(arg, tablet_handle))) {
    LOG_WARN("failed to get tablet for test mds register", KR(ret), K(arg));
  } else if (OB_FAIL(tablet_handle.get_obj()->set(build_single_row_value_(arg), mds_ctx))) {
    LOG_WARN("failed to set single row test mds value", KR(ret), K(arg));
  }
  return ret;
}

int ObTestMdsSingleRowHelper::on_replay(const char *buf,
                                        const int64_t len,
                                        const share::SCN &scn,
                                        BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTestMdsRegisterArg arg;
  int64_t pos = 0;
  MdsCtx &mds_ctx = static_cast<MdsCtx &>(ctx);
  ObTestMdsSingleRowValue value;
  if (OB_UNLIKELY(nullptr == buf || len <= 0 || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len), K(scn));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize test mds register arg", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid test mds register arg", KR(ret), K(arg));
  } else if (FALSE_IT(value = build_single_row_value_(arg))) {
  } else {
    ObTestMdsSingleRowReplayExecutor executor(arg, value);
    if (OB_FAIL(executor.init(mds_ctx, scn))) {
      LOG_WARN("failed to init single row test mds replay executor", KR(ret), K(arg), K(scn));
    } else if (OB_FAIL(executor.execute(scn, arg.ls_id_, arg.tablet_id_))) {
      if (OB_NO_NEED_UPDATE != ret) {
        LOG_WARN("failed to replay single row test mds value", KR(ret), K(arg), K(scn));
      }
    }
  }
  return ret;
}

int ObTestMdsMultiRowHelper::on_register(const char *buf, const int64_t len, BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTestMdsRegisterArg arg;
  int64_t pos = 0;
  ObTabletHandle tablet_handle;
  MdsCtx &mds_ctx = static_cast<MdsCtx &>(ctx);
  if (OB_UNLIKELY(nullptr == buf || len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize test mds register arg", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid test mds register arg", KR(ret), K(arg));
  } else if (OB_FAIL(get_test_mds_tablet_(arg, tablet_handle))) {
    LOG_WARN("failed to get tablet for test mds register", KR(ret), K(arg));
  } else if (OB_FAIL(tablet_handle.get_obj()->set(ObTestMdsMultiRowKey(arg.key_),
                                                  build_multi_row_value_(arg),
                                                  mds_ctx,
                                                  0 /*lock_timeout_us*/,
                                                  arg.check_dup_))) {
    LOG_WARN("failed to set multi row test mds value", KR(ret), K(arg));
  }
  return ret;
}

int ObTestMdsMultiRowHelper::on_replay(const char *buf,
                                       const int64_t len,
                                       const share::SCN &scn,
                                       BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTestMdsRegisterArg arg;
  int64_t pos = 0;
  MdsCtx &mds_ctx = static_cast<MdsCtx &>(ctx);
  if (OB_UNLIKELY(nullptr == buf || len <= 0 || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len), K(scn));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("failed to deserialize test mds register arg", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid test mds register arg", KR(ret), K(arg));
  } else {
    const ObTestMdsMultiRowKey key(arg.key_);
    const ObTestMdsMultiRowValue value = build_multi_row_value_(arg);
    ObTestMdsMultiRowReplayExecutor executor(arg, key, value);
    if (OB_FAIL(executor.init(mds_ctx, scn))) {
      LOG_WARN("failed to init multi row test mds replay executor", KR(ret), K(arg), K(scn));
    } else if (OB_FAIL(executor.execute(scn, arg.ls_id_, arg.tablet_id_))) {
      if (OB_NO_NEED_UPDATE != ret) {
        LOG_WARN("failed to replay multi row test mds value", KR(ret), K(arg), K(scn));
      }
    }
  }
  return ret;
}

} // end of namespace mds
} // end of namespace storage
} // end of namespace oceanbase
