/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_dag.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashutils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "observer/ob_server_struct.h"
#include "share/ob_ls_id.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ob_sswriter_service.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/atomic_protocol/ob_checkpoint_atomic_protocol.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_writer.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/incremental/sslog/ob_sslog_kv_palf_adapter.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace blocksstable;

namespace storage
{

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_SS_LS_CKPT_PALF_PUBLISH);
#endif

static const int64_t LS_CKPT_PALF_KV_VALUE_BUF_LEN = 64;
static const int64_t LS_CKPT_PALF_KV_MAX_RETRY = 8;
static const int64_t LS_CKPT_RESERVED_CREATE_MAX_RETRY = 4;
static const int64_t LS_CKPT_RESERVED_CREATE_RETRY_INTERVAL_US = 100 * 1000;
static const int64_t LS_CKPT_RESERVED_SQL_TIMEOUT_US = 10 * 1000 * 1000;
typedef ObDefaultSSMetaValue<ObSSMetaLSCkptVal> ObSSLSCkptValue;

constexpr int64_t ObSSLsCkptModePolicy::DEFAULT_BOOTSTRAP_UPLOAD_PARALLELISM;
constexpr int64_t ObSSLsCkptModePolicy::NORMAL_UPLOAD_PARALLELISM;

bool ObSSLsCkptModePolicy::is_valid(const ObSSLsCkptMode mode)
{
  return ObSSLsCkptMode::BOOTSTRAP == mode || ObSSLsCkptMode::NORMAL == mode;
}

bool ObSSLsCkptModePolicy::need_check_sswriter(const ObSSLsCkptMode mode)
{
  return ObSSLsCkptMode::NORMAL == mode;
}

bool ObSSLsCkptModePolicy::need_check_existing_object(const ObSSLsCkptMode mode)
{
  return ObSSLsCkptMode::NORMAL == mode;
}

int64_t ObSSLsCkptModePolicy::get_upload_parallelism(const ObSSLsCkptMode mode)
{
  int64_t parallelism = 0;
  if (ObSSLsCkptMode::BOOTSTRAP == mode) {
    parallelism = 0 == GCONF.sys_ckpt_thread_score
        ? DEFAULT_BOOTSTRAP_UPLOAD_PARALLELISM
        : GCONF.sys_ckpt_thread_score;
  } else if (ObSSLsCkptMode::NORMAL == mode) {
    parallelism = NORMAL_UPLOAD_PARALLELISM;
  }
  return parallelism;
}

void ObSSLsCkptPrepareTask::build_ckpt_op_init_param_(
    const int64_t op_id,
    ObSSMetaAtomicOpInitParam &init_param) const
{
  init_param.op_id_ = op_id;
  init_param.ls_id_ = ctx_->ls_id_;
  init_param.protocol_type_.type_ = ObAtomicProtocolTypeEnum::CHECKPOINT;
}

int ObSSLsCkptPrepareTask::create_reserved_meta_scn_(
    SCN &reserved_meta_scn) const
{
  int ret = OB_SUCCESS;
  SCN gts;
  const ObLSID &ls_id = ctx_->ls_id_;
  ObSharedMetaReservedMgr reserved_mgr;
  const ObSharedMetaReservedType reserved_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  reserved_meta_scn.reset();
  if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_sys_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id for creating reserved meta scn", KR(ret), K(ls_id));
  } else if (OB_FAIL(reserved_mgr.init(OB_SYS_TENANT_ID))) {
    LOG_WARN("init reserved mgr failed", KR(ret));
  }
  if (OB_SUCC(ret)) {
    int64_t retry_cnt = 0;
    do {
      if (retry_cnt > 0) {
        ob_usleep(LS_CKPT_RESERVED_CREATE_RETRY_INTERVAL_US);
      }
      gts.reset();
      if (OB_FAIL(ObAtomicProtocolUtil::get_gts_sync(OB_SYS_TENANT_ID, gts))) {
        LOG_WARN("get gts for ls ckpt reserved failed", KR(ret), K(ls_id));
      } else if (OB_FAIL(reserved_mgr.create_reserved_scn(reserved_type, gts))) {
        if (OB_OP_NOT_ALLOW == ret) {
          LOG_INFO("create ls ckpt reserved scn got OP_NOT_ALLOW, retry",
                   KR(ret), K(ls_id), K(gts), K(retry_cnt));
          ret = OB_SUCCESS;
        } else if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          ret = OB_SUCCESS;
          FLOG_INFO("same ls checkpoint reservation key already exists, retry with new gts",
                    K(ls_id), K(gts), K(retry_cnt));
        } else {
          LOG_WARN("create ls ckpt reserved scn failed",
                   KR(ret), K(ls_id), K(gts), K(retry_cnt));
        }
      } else {
        reserved_meta_scn = gts;
      }
      ++retry_cnt;
    } while (OB_SUCC(ret)
             && !reserved_meta_scn.is_valid()
             && retry_cnt < LS_CKPT_RESERVED_CREATE_MAX_RETRY);
    if (OB_SUCC(ret) && !reserved_meta_scn.is_valid()) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("create ls ckpt reserved scn exceeds max retry",
               KR(ret), K(ls_id), K(gts), K(retry_cnt));
    }
  }
  return ret;
}

int ObSSLsCkptFinishTask::remove_reserved_meta_scn_(
    const ObLSID &ls_id,
    const SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  ObSharedMetaReservedMgr reserved_mgr;
  const ObSharedMetaReservedType reserved_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_sys_ls() || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for removing ls ckpt reservation", KR(ret), K(ls_id), K(meta_scn));
  } else if (OB_FAIL(reserved_mgr.init(OB_SYS_TENANT_ID))) {
    LOG_WARN("init reserved mgr failed", KR(ret));
  } else if (OB_FAIL(reserved_mgr.remove_reserved_scn(reserved_type, meta_scn))) {
    LOG_WARN("remove ls ckpt reserved scn failed", KR(ret), K(ls_id), K(meta_scn));
  }
  return ret;
}

int ObSSLsCkptFinishTask::remove_reserved_meta_scns_at_or_before_(
    const ObLSID &ls_id,
    const SCN &meta_scn_upper_bound)
{
  int ret = OB_SUCCESS;
  const int64_t scan_limit_count = 1024;
  int64_t removed_count = 0;
  ObSharedMetaReservedOperator reserved_operator;
  ObSharedMetaReservedMgr reserved_mgr;
  ObSEArray<ObSharedMetaReservedInfo, 16> reserved_infos;
  const ObSharedMetaReservedType reserved_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || !ls_id.is_sys_ls()
                  || !meta_scn_upper_bound.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid upper bound for removing ls ckpt reservations",
             KR(ret), K(ls_id), K(meta_scn_upper_bound));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(reserved_operator.init(
                 *GCTX.sql_proxy_, LS_CKPT_RESERVED_SQL_TIMEOUT_US))) {
    LOG_WARN("init reserved operator failed", KR(ret));
  } else if (OB_FAIL(reserved_operator.scan_rows(
                 OB_SYS_TENANT_ID, reserved_type,
                 scan_limit_count, reserved_infos))) {
    LOG_WARN("scan ls ckpt reserved meta scns failed", KR(ret), K(ls_id));
  } else if (OB_FAIL(reserved_mgr.init(OB_SYS_TENANT_ID))) {
    LOG_WARN("init reserved mgr failed", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < reserved_infos.count(); ++i) {
      const ObSharedMetaReservedInfo &reserved_info = reserved_infos.at(i);
      if (reserved_info.meta_scn_ > meta_scn_upper_bound) {
        break;
      } else if (OB_FAIL(reserved_mgr.remove_reserved_scn(
                     reserved_type, reserved_info.meta_scn_))) {
        LOG_WARN("remove ls ckpt reserved meta scn failed",
                 KR(ret), K(ls_id), K(meta_scn_upper_bound), K(reserved_info));
      } else {
        ++removed_count;
      }
    }
  }
  LOG_INFO("range remove ls ckpt reserved meta scns finish",
           KR(ret), K(ls_id), K(meta_scn_upper_bound), K(removed_count));
  return ret;
}

int ObSSLsCkptFinishTask::parse_palf_kv_meta_scn_(
    const ObLSID &ls_id,
    const ObString &value,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  MacroBlockId ckpt_macro_id;
  meta_scn.reset();
  if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_sys_ls())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id for palf kv value", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(value.empty())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("empty palf kv value", KR(ret), K(ls_id));
  } else if (OB_FAIL(ckpt_macro_id.deserialize(value.ptr(), value.length(), pos))) {
    ret = OB_INVALID_DATA;
    LOG_WARN("deserialize ls ckpt macro id failed", KR(ret), K(ls_id), K(value));
  } else if (OB_UNLIKELY(pos != value.length())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("ls ckpt macro id value has trailing data",
             KR(ret), K(ls_id), K(pos), "value_len", value.length(), K(ckpt_macro_id));
  } else if (OB_FAIL(validate_ls_ckpt_object_id(ls_id, ckpt_macro_id))) {
    LOG_WARN("invalid ls ckpt macro id value", KR(ret), K(ls_id), K(ckpt_macro_id));
  } else if (OB_FAIL(meta_scn.convert_for_inner_table_field(
      static_cast<uint64_t>(ckpt_macro_id.meta_scn())))) {
    LOG_WARN("convert ls ckpt meta scn failed", KR(ret), K(ls_id), K(ckpt_macro_id));
  }
  return ret;
}

int ObSSLsCkptFinishTask::write_to_palf_kv_(
    const ObLSID &ls_id,
    const SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  const SCN &cur_scn = meta_scn;
  char key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  int64_t key_len = 0;
  char val_buf[LS_CKPT_PALF_KV_VALUE_BUF_LEN] = {0};
  int64_t val_len = 0;
  ObSSMetaLSCkptKey ckpt_key;
  ckpt_key.ls_id_ = ls_id;
  ckpt_key.meta_scn_ = cur_scn;
  const MacroBlockId ckpt_macro_id = ckpt_key.convert_to_macro_id(false /*is_in_table*/);
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;

  if (OB_UNLIKELY(!ls_id.is_valid() || !ls_id.is_sys_ls() || !cur_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid palf kv write argument", KR(ret), K(ls_id), K(cur_scn));
  } else if (OB_FAIL(validate_ls_ckpt_object_id(ls_id, ckpt_macro_id))) {
    LOG_WARN("invalid ls ckpt macro id", KR(ret), K(ckpt_key), K(ckpt_macro_id));
  } else if (OB_FAIL(ObSSMetaInfoUtil::build_ls_ckpt_key(
                 ls_id, key_buf, sizeof(key_buf), key_len))) {
    LOG_WARN("build ls ckpt palf kv key failed", KR(ret), K(ls_id), K(key_len));
  } else if (OB_FAIL(ckpt_macro_id.serialize(val_buf, sizeof(val_buf), val_len))) {
    LOG_WARN("serialize ls ckpt macro id failed", KR(ret), K(ckpt_macro_id), K(val_len));
  } else if (OB_FAIL(palf_kv_adapter.init(GCONF.cluster_id, OB_SYS_TENANT_ID))) {
    LOG_WARN("init palf kv failed", KR(ret));
  } else {
    const ObString kv_key(key_len, key_buf);
    const ObString kv_value(val_len, val_buf);
    bool done = false;
    for (int64_t i = 0; OB_SUCC(ret) && !done && i < LS_CKPT_PALF_KV_MAX_RETRY; ++i) {
      ObArenaAllocator tmp_alloc("LsCkptKvGet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      ObStringBuffer prev_value(&tmp_alloc);
      ret = palf_kv_adapter.get(kv_key, prev_value);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(palf_kv_adapter.put(kv_key, kv_value))) {
          LOG_WARN("put ls ckpt palf kv failed", KR(ret), K(kv_key), K(kv_value));
        } else {
          done = true;
        }
      } else if (OB_FAIL(ret)) {
        LOG_WARN("get ls ckpt palf kv failed", KR(ret), K(kv_key), K(prev_value));
      } else {
        SCN prev_scn;
        if (OB_FAIL(parse_palf_kv_meta_scn_(ls_id, prev_value.string(), prev_scn))) {
          LOG_WARN("parse meta scn from palf kv value failed", KR(ret), K(kv_key), K(prev_value));
          break;
        } else if (cur_scn <= prev_scn) {
          done = true;
          LOG_INFO("skip ls ckpt palf kv publish, current is not newer",
                   K(ls_id), K(cur_scn), K(prev_scn));
        } else {
          bool expected = false;
          if (OB_FAIL(palf_kv_adapter.cas(kv_key, prev_value.string(), kv_value, expected))) {
            LOG_WARN("cas ls ckpt palf kv failed", KR(ret), K(kv_key), K(kv_value), K(expected));
          } else if (expected) {
            done = true;
          }
        }
      }
      // Retry PALF get/put/cas failures within the bounded loop; exhaustion returns OB_EAGAIN.
      // Invalid stored values break above and preserve the parsing error.
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && !done) {
      ret = OB_EAGAIN;
      LOG_WARN("write ls ckpt palf kv exceeds max retry", KR(ret), K(ls_id), K(cur_scn));
    }
  }
  return ret;
}

int ObSSLsCkptPrepareTask::prepare_checkpoint_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_enabled = false;
  ObCheckpointAtomicProtocol ckpt_protocol;
  SCN ckpt_meta_scn;
  int64_t op_id = -1;
  ObSSLsCkptWriteCtx &ctx = *ctx_;
  const bool need_check_sswriter =
      ObSSLsCkptModePolicy::need_check_sswriter(ctx.get_mode());

  if (OB_FAIL(ctx.check_status())) {
    LOG_WARN("ls checkpoint cannot start", KR(ret));
  } else if (OB_FAIL(check_ss_ls_ckpt_enabled(OB_SYS_TENANT_ID, is_enabled))) {
    LOG_WARN("check ls checkpoint enabled failed", KR(ret));
  } else if (!is_enabled) {
    ret = OB_CANCELED;
    LOG_INFO("ls checkpoint is disabled before prepare", KR(ret));
  } else if (OB_FAIL(ctx.check_lease_valid())) {
    LOG_WARN("ls checkpoint lease is invalid before prepare", KR(ret));
  } else if (OB_FAIL(create_reserved_meta_scn_(ctx.ckpt_data_.reserved_meta_scn_))) {
    LOG_WARN("create reserved meta scn failed", KR(ret), K(ctx.ls_id_));
  } else if (OB_FALSE_IT(ctx.is_scn_reserved_ = true)) {
  } else if (OB_FAIL(ckpt_protocol.init(OB_SYS_TENANT_ID, ctx.ls_id_))) {
    LOG_WARN("init checkpoint protocol failed", KR(ret), K(ctx.ls_id_));
  } else if (OB_FAIL(ckpt_protocol.create(need_check_sswriter))) {
    LOG_WARN("create checkpoint protocol failed", KR(ret));
  } else if (OB_FAIL(ckpt_protocol.get_op_id(op_id))) {
    LOG_WARN("get checkpoint protocol op id failed", KR(ret));
  } else {
    ObSSMetaAtomicLSCkptOp ckpt_op;
    ObSSMetaAtomicOpInitParam ckpt_op_param;
    ObSSLSCkptValue ckpt_val;
    ObSSMetaHeadValueInfo head_info;
    build_ckpt_op_init_param_(op_id, ckpt_op_param);
    if (OB_FAIL(head_info.set(ObMetaUpdateReason::CREATE_LS_CKPT))) {
      LOG_WARN("set checkpoint update reason failed", KR(ret));
    } else if (OB_FAIL(ckpt_val.set_meta_head_info(head_info))) {
      LOG_WARN("set checkpoint head info failed", KR(ret), K(head_info));
    } else if (OB_FALSE_IT(ckpt_val.meta_value_.op_id_ = op_id)) {
    } else if (OB_FAIL(ckpt_op.init(ckpt_op_param))) {
      LOG_WARN("init ls checkpoint op failed", KR(ret), K(ckpt_op_param));
    } else if (OB_FAIL(ckpt_op.write_meta_info(ckpt_val))) {
      LOG_WARN("write ls checkpoint value failed", KR(ret), K(ckpt_val));
    } else if (OB_FAIL(ckpt_protocol.finish(ckpt_op))) {
      LOG_WARN("finish checkpoint protocol failed", KR(ret));
    }
  }

  if (OB_FAIL(ret) && OB_TMP_FAIL(ckpt_protocol.abort())) {
    LOG_WARN("abort checkpoint protocol failed", KR(tmp_ret), KR(ret));
  }
  if (FAILEDx(ckpt_protocol.get_meta_scn(ckpt_meta_scn))) {
    LOG_WARN("get checkpoint meta scn failed", KR(ret));
  } else if (OB_UNLIKELY(ckpt_meta_scn < ctx.ckpt_data_.reserved_meta_scn_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("checkpoint meta scn is smaller than reserved meta scn",
              KR(ret), K(ckpt_meta_scn), K(ctx.ckpt_data_.reserved_meta_scn_));
  } else {
    ctx.ckpt_data_.ckpt_scn_ = ckpt_meta_scn;
    ctx.ckpt_data_.ckpt_ts_ = ObTimeUtility::current_time();
  }
  return ret;
}

ObSSLsCkptWriteCtx::ObSSLsCkptWriteCtx()
  : is_inited_(false),
    mode_(ObSSLsCkptMode::MAX_MODE),
    sswriter_epoch_(-1),
    ls_id_(),
    is_scn_reserved_(false),
    ckpt_data_(),
    tablet_keys_(),
    upload_parallelism_(0),
    next_upload_idx_(0),
    dag_ret_(OB_SUCCESS)
{
}

int ObSSLsCkptWriteCtx::init(
    const ObSSLsCkptMode mode,
    const int64_t sswriter_epoch,
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls ckpt write dag ctx init twice", KR(ret));
  } else if (OB_UNLIKELY(!ObSSLsCkptModePolicy::is_valid(mode)
                         || !ls_id.is_valid()
                         || !ls_id.is_sys_ls()
                         || (ObSSLsCkptModePolicy::need_check_sswriter(mode)
                             && sswriter_epoch < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls ckpt write dag ctx argument",
             KR(ret), K(mode), K(sswriter_epoch), K(ls_id));
  } else {
    mode_ = mode;
    sswriter_epoch_ = sswriter_epoch;
    ls_id_ = ls_id;
    upload_parallelism_ = ObSSLsCkptModePolicy::get_upload_parallelism(mode);
    is_scn_reserved_ = false;
    ckpt_data_.reset();
    tablet_keys_.reset();
    reset_next_upload_idx();
    ATOMIC_STORE(&dag_ret_, OB_SUCCESS);
    is_inited_ = true;
  }
  return ret;
}

bool ObSSLsCkptWriteCtx::is_valid() const
{
  return is_inited_
      && ObSSLsCkptModePolicy::is_valid(mode_)
      && upload_parallelism_ > 0
      && ls_id_.is_valid()
      && ls_id_.is_sys_ls()
      && (!ObSSLsCkptModePolicy::need_check_sswriter(mode_)
          || sswriter_epoch_ >= 0);
}

int64_t ObSSLsCkptWriteCtx::get_upload_parallelism() const
{
  return upload_parallelism_;
}

int ObSSLsCkptWriteCtx::check_lease_valid() const
{
  int ret = OB_SUCCESS;
  if (ObSSLsCkptModePolicy::need_check_sswriter(mode_)) {
#ifdef ERRSIM
    (void)(OB_I(check_lease, this) OB_SUCCESS);
#endif
    bool is_sswriter = false;
    int64_t cur_epoch = -1;
    ObSSWriterService *service = MTL(ObSSWriterService *);
    const ObSSWriterKey sswriter_key(ObSSWriterType::META, ls_id_);
    if (OB_ISNULL(service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sswriter service is null when checking ls checkpoint lease", KR(ret));
    } else if (OB_FAIL(service->check_lease(sswriter_key, is_sswriter, cur_epoch))) {
      LOG_WARN("failed to check ls checkpoint lease", KR(ret), K_(sswriter_epoch));
    } else if (!is_sswriter || cur_epoch != sswriter_epoch_) {
      ret = OB_NOT_MASTER;
      LOG_WARN("ls checkpoint sswriter lease changed",
               KR(ret), K(is_sswriter), K(cur_epoch), K_(sswriter_epoch));
    }
  }
  return ret;
}

void ObSSLsCkptWriteCtx::record_error(const int error_code)
{
  if (OB_SUCCESS != error_code) {
    (void)ATOMIC_BCAS(&dag_ret_, OB_SUCCESS, error_code);
  }
}

int ObSSLsCkptWriteCtx::check_status()
{
  int ret = ATOMIC_LOAD(&dag_ret_);
  share::ObTenantDagWorker *dag_worker = share::ObTenantDagWorker::self();
  share::ObITask *task = OB_ISNULL(dag_worker) ? nullptr : dag_worker->get_task();
  share::ObIDag *dag = OB_ISNULL(task) ? nullptr : task->get_dag();
  if (OB_SUCC(ret) && OB_NOT_NULL(dag) && dag->has_set_stop()) {
    record_error(OB_CANCELED);
    ret = ATOMIC_LOAD(&dag_ret_);
  }
  return ret;
}

ObSSLsCkptDag::ObSSLsCkptDag()
  : share::ObIDag(share::ObDagType::DAG_TYPE_SS_LS_CKPT),
    is_inited_(false),
    ctx_()
{
}

int ObSSLsCkptDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls ckpt write dag init twice", KR(ret));
  } else if (OB_ISNULL(param) || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls ckpt write dag param", KR(ret), KP(param));
  } else {
    const ObSSLsCkptDagParam *dag_param =
        static_cast<const ObSSLsCkptDagParam *>(param);
    if (OB_FAIL(ctx_.init(
            dag_param->mode_, dag_param->sswriter_epoch_, dag_param->ls_id_))) {
      LOG_WARN("init ls ckpt write dag owned ctx failed", KR(ret), KPC(dag_param));
    } else {
      set_max_concurrent_task_cnt(ctx_.get_upload_parallelism());
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSLsCkptDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSLsCkptPrepareTask *prepare_task = nullptr;
  ObSSLsCkptUploadTask *upload_task = nullptr;
  ObSSLsCkptFinishTask *finish_task = nullptr;
  ObSEArray<ObITask *, 3> task_array;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt write dag not inited", KR(ret));
  } else if (OB_FAIL(alloc_task(prepare_task))) {
    LOG_WARN("alloc ls ckpt write prepare task failed", KR(ret));
  } else if (OB_FAIL(prepare_task->init(ctx_))) {
    LOG_WARN("init ls ckpt write prepare task failed", KR(ret));
  // Seed only worker 0; the scheduler lazily creates subsequent workers before dispatch
  // and copies Finish dependencies.
  } else if (OB_FAIL(alloc_task(upload_task))) {
    LOG_WARN("alloc first ls ckpt write upload task failed", KR(ret));
  } else if (OB_FAIL(upload_task->init(ctx_, 0 /*worker_idx*/))) {
    LOG_WARN("init first ls ckpt write upload task failed", KR(ret));
  } else if (OB_FAIL(alloc_task(finish_task))) {
    LOG_WARN("alloc ls ckpt write finish task failed", KR(ret));
  } else if (OB_FAIL(finish_task->init(ctx_))) {
    LOG_WARN("init ls ckpt write finish task failed", KR(ret));
  } else if (OB_FAIL(prepare_task->add_child(*upload_task))) {
    LOG_WARN("link ls ckpt write prepare and first upload task failed", KR(ret));
  } else if (OB_FAIL(upload_task->add_child(*finish_task))) {
    LOG_WARN("link first ls ckpt write upload and finish task failed", KR(ret));
  } else if (OB_FAIL(task_array.push_back(prepare_task))) {
    LOG_WARN("add ls ckpt write prepare task to array failed", KR(ret));
  } else if (OB_FAIL(task_array.push_back(upload_task))) {
    LOG_WARN("add first ls ckpt write upload task to array failed", KR(ret));
  } else if (OB_FAIL(task_array.push_back(finish_task))) {
    LOG_WARN("add ls ckpt write finish task to array failed", KR(ret));
  } else if (OB_FAIL(batch_add_task(task_array))) {
    LOG_WARN("add ls ckpt write tasks failed", KR(ret), K(task_array.count()));
  }
  return ret;
}

bool ObSSLsCkptDag::operator==(const share::ObIDag &other) const
{
  return this == &other || get_type() == other.get_type();
}

uint64_t ObSSLsCkptDag::hash() const
{
  const share::ObDagType::ObDagTypeEnum dag_type = get_type();
  return common::murmurhash(&dag_type, sizeof(dag_type), 0);
}

int ObSSLsCkptDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt write dag not inited", KR(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(
                 out_param, allocator, get_type(), static_cast<int64_t>(ctx_.mode_)))) {
    LOG_WARN("fill ls ckpt write dag info failed", KR(ret), K_(ctx));
  }
  return ret;
}

int ObSSLsCkptDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt write dag not inited", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "mode=%ld",
                                    static_cast<int64_t>(ctx_.mode_)))) {
    LOG_WARN("fill ls ckpt write dag key failed", KR(ret), K(buf_len), K_(ctx));
  }
  return ret;
}

int ObSSLsCkptDag::report_result()
{
  int ret = get_dag_ret();
  const ObDagStatus dag_status = get_dag_status();
  if (OB_SUCC(ret) && DAG_STATUS_FINISH != dag_status) {
    ret = OB_CANCELED;
  }
  LOG_INFO("report ls ckpt write dag result", KR(ret), K(dag_status), K_(ctx));
  // Return success for the reporting callback, not for the checkpoint execution.
  // The execution result remains in the DAG status and dag_ret.
  return OB_SUCCESS;
}

ObSSLsCkptPrepareTask::ObSSLsCkptPrepareTask()
  : share::ObITask(share::ObITask::TASK_TYPE_SS_LS_CKPT),
    is_inited_(false),
    ctx_(nullptr)
{
}

int ObSSLsCkptPrepareTask::init(ObSSLsCkptWriteCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls ckpt write prepare task init twice", KR(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls ckpt write prepare ctx", KR(ret), K(ctx));
  } else {
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLsCkptPrepareTask::collect_tablet_keys_()
{
  int ret = OB_SUCCESS;
  ObSSMetaTabletKeyIterator iter;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null for ls ckpt write", KR(ret));
  } else if (OB_FAIL(iter.init(
                 *GCTX.sql_proxy_, OB_SYS_TENANT_ID, ctx_->ls_id_,
                 ctx_->ckpt_data_.reserved_meta_scn_, true /*skip_mark_delete*/))) {
    LOG_WARN("init ls ckpt write tablet key iterator failed", KR(ret),
             K(ctx_->ckpt_data_.reserved_meta_scn_));
  } else {
    ObSSMetaTabletKey tablet_key;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_->check_status())) {
        LOG_WARN("ls ckpt write prepare task is stopped", KR(ret));
        break;
      }
      tablet_key.reset();
      if (OB_FAIL(iter.get_next(tablet_key))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get ls ckpt write tablet key failed", KR(ret));
        }
        break;
      } else if (OB_FAIL(ctx_->tablet_keys_.push_back(tablet_key))) {
        LOG_WARN("save ls ckpt write tablet key failed", KR(ret), K(tablet_key));
      }
    }
  }
  return ret;
}

int ObSSLsCkptPrepareTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt write prepare task not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls ckpt write prepare runs in unexpected tenant", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(prepare_checkpoint_())) {
    LOG_WARN("prepare ls checkpoint state failed", KR(ret));
  } else {
    ObSSLsCkptWriter writer(ObSSLsCkptModePolicy::need_check_existing_object(ctx_->get_mode()));
    ctx_->tablet_keys_.reset();
    ctx_->ckpt_data_.ls_meta_.reset();
    ctx_->ckpt_data_.tablet_meta_keys_.reset();
    ctx_->reset_next_upload_idx();
    if (OB_FAIL(writer.collect_ls_meta(
            ctx_->ls_id_, ctx_->ckpt_data_.reserved_meta_scn_, ctx_->ckpt_data_.ls_meta_))) {
      LOG_WARN("collect ls ckpt write ls meta failed",
               KR(ret), K(ctx_->ckpt_data_.reserved_meta_scn_));
    } else if (OB_FAIL(collect_tablet_keys_())) {
      LOG_WARN("collect ls ckpt write tablet keys failed", KR(ret));
    } else {
      LOG_INFO("prepare ls ckpt upload tasks", "mode", ctx_->get_mode(),
               "tablet_count", ctx_->tablet_keys_.count(),
               "parallelism", ctx_->get_upload_parallelism());
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->record_error(ret);
    // A failed task prevents successors from running, so report success to the scheduler here.
    // Upload exits on the context error, and Finish cleans up the reservation
    // and returns the original error.
    ret = OB_SUCCESS;
  }
  return ret;
}

ObSSLsCkptUploadTask::ObSSLsCkptUploadTask()
  : share::ObITask(share::ObITask::TASK_TYPE_SS_LS_CKPT),
    is_inited_(false),
    next_task_generated_(false),
    worker_idx_(-1),
    ctx_(nullptr)
{
}

int ObSSLsCkptUploadTask::init(
    ObSSLsCkptWriteCtx &ctx,
    const int64_t worker_idx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls ckpt write upload task init twice", KR(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid()
                         || worker_idx < 0
                         || worker_idx >= ctx.get_upload_parallelism())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls ckpt write upload task argument", KR(ret), K(worker_idx), K(ctx));
  } else {
    ctx_ = &ctx;
    worker_idx_ = worker_idx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLsCkptUploadTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt write upload task not inited", KR(ret));
  } else if (next_task_generated_) {
    ret = OB_ITER_END;
  } else if (OB_SUCCESS != ctx_->get_first_error()
             || worker_idx_ + 1 >= ctx_->get_upload_parallelism()) {
    next_task_generated_ = true;
    ret = OB_ITER_END;
  } else if (OB_ISNULL(dag_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls ckpt write upload task dag is null", KR(ret), K_(worker_idx));
  } else {
    ObSSLsCkptUploadTask *upload_task = nullptr;
    if (OB_FAIL(dag_->alloc_task(upload_task))) {
      LOG_WARN("alloc next ls ckpt write upload task failed", KR(ret), K_(worker_idx));
    } else if (OB_FAIL(upload_task->init(*ctx_, worker_idx_ + 1))) {
      LOG_WARN("init next ls ckpt write upload task failed", KR(ret), K_(worker_idx));
    }
    if (OB_FAIL(ret)) {
      ctx_->record_error(ret);
      next_task_generated_ = true;
      // The DAG owns allocated tasks and reclaims them when it is destroyed.
      // Keep Finish schedulable to clean up the reservation and report the original error.
      ret = OB_ITER_END;
    } else {
      next_task_generated_ = true;
      next_task = upload_task;
    }
  }
  return ret;
}

int ObSSLsCkptUploadTask::process()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt write upload task not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls ckpt write upload runs in unexpected tenant", KR(ret), K(MTL_ID()), K_(worker_idx));
  } else {
    ObSSLsCkptWriter writer(ObSSLsCkptModePolicy::need_check_existing_object(ctx_->get_mode()));
    while (OB_SUCC(ret)) {
      if (OB_FAIL(ctx_->check_status())) {
        LOG_WARN("ls ckpt write upload task is stopped", KR(ret), K_(worker_idx));
        break;
      }
      const int64_t tablet_idx = ctx_->fetch_next_tablet_idx();
      if (tablet_idx >= ctx_->tablet_keys_.count()) {
        break;
      }

      if (OB_FAIL(ctx_->check_lease_valid())) {
        LOG_WARN("ls ckpt lease became invalid before uploading tablet meta",
                 KR(ret), K(tablet_idx), K_(worker_idx));
#ifdef ERRSIM
      } else if (OB_FAIL(OB_I(before_upload, ctx_, tablet_idx) OB_SUCCESS)) {
        LOG_WARN("ERRSIM ls ckpt tablet upload failed", KR(ret), K(tablet_idx), K_(worker_idx));
#endif
      } else if (OB_FAIL(writer.upload_tablet_meta(ctx_->tablet_keys_.at(tablet_idx)))) {
        LOG_WARN("ls ckpt tablet upload failed", KR(ret), K(tablet_idx), K_(worker_idx));
#ifdef ERRSIM
      } else {
        (void)(OB_I(after_upload, ctx_, tablet_idx) OB_SUCCESS);
#endif
      }
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->record_error(ret);
    // Let Finish return the operation error so reservation cleanup can still be scheduled.
    ret = OB_SUCCESS;
  }
  return ret;
}

ObSSLsCkptFinishTask::ObSSLsCkptFinishTask()
  : share::ObITask(share::ObITask::TASK_TYPE_SS_LS_CKPT),
    is_inited_(false),
    ctx_(nullptr)
{
}

int ObSSLsCkptFinishTask::init(ObSSLsCkptWriteCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls ckpt write finish task init twice", KR(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls ckpt write finish ctx", KR(ret), K(ctx));
  } else if (OB_FAIL(parent_.reserve(ctx.get_upload_parallelism()))) {
    LOG_WARN("reserve ls ckpt write finish parents failed",
             KR(ret), "parallelism", ctx.get_upload_parallelism());
  } else {
    ctx_ = &ctx;
    is_inited_ = true;
  }
  return ret;
}

int ObSSLsCkptFinishTask::collect_tablet_meta_object_ids_()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt write finish task not inited", KR(ret));
  } else {
    ctx_->ckpt_data_.tablet_meta_keys_.reset();
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->tablet_keys_.count(); ++i) {
    ObSSMetaTabletKey tablet_key(ctx_->tablet_keys_.at(i));
    const blocksstable::MacroBlockId tablet_meta_object_id =
        tablet_key.convert_to_macro_id(false /*is_in_table*/);
    if (OB_UNLIKELY(tablet_key.ls_id_ != ctx_->ls_id_)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("tablet key ls id does not match checkpoint",
               KR(ret), K(i), K(tablet_key), K(ctx_->ls_id_));
    } else if (OB_FAIL(ctx_->ckpt_data_.add_tablet_meta_key(tablet_meta_object_id))) {
      LOG_WARN("generate ls ckpt tablet object id failed", KR(ret), K(i), K(tablet_key));
    }
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(ctx_)) {
      ctx_->ckpt_data_.tablet_meta_keys_.reset();
    }
  }
  return ret;
}

int ObSSLsCkptFinishTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls ckpt write finish task not inited", KR(ret));
  } else if (OB_UNLIKELY(OB_SYS_TENANT_ID != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls ckpt write finish runs in unexpected tenant", KR(ret), K(MTL_ID()));
  } else if (OB_FAIL(collect_tablet_meta_object_ids_())) {
    LOG_WARN("collect ls ckpt tablet object ids failed", KR(ret));
  } else {
    ObSSLsCkptWriter writer(ObSSLsCkptModePolicy::need_check_existing_object(ctx_->get_mode()));
    if (OB_FAIL(writer.write_checkpoint_object(ctx_->ls_id_, ctx_->ckpt_data_))) {
      LOG_WARN("write ls checkpoint object failed", KR(ret), K(ctx_->ckpt_data_.ckpt_scn_));
    } else if (OB_FAIL(ctx_->check_status())) {
      LOG_WARN("ls ckpt write finish task is stopped before publishing", KR(ret));
    } else if (OB_FAIL(ctx_->check_lease_valid())) {
      LOG_WARN("ls ckpt lease became invalid before publishing", KR(ret));
    } else if (OB_ISNULL(dag_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls ckpt write finish task dag is null", KR(ret));
    } else if (OB_FAIL(dag_->get_dag_ret())) {
      LOG_WARN("ls ckpt dag failed before publishing", KR(ret));
#ifdef ERRSIM
    } else if (OB_FAIL(OB_E(EN_SS_LS_CKPT_PALF_PUBLISH,
                           static_cast<int64_t>(ctx_->get_mode())) OB_SUCCESS)) {
      LOG_WARN("ERRSIM ls checkpoint palf publish failed", KR(ret));
#endif
    } else if (GCONF.enable_logservice
               && OB_FAIL(write_to_palf_kv_(ctx_->ls_id_, ctx_->ckpt_data_.ckpt_scn_))) {
      LOG_WARN("write ls checkpoint to palf kv failed", KR(ret), K(ctx_->ckpt_data_.ckpt_scn_));
    }
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(ctx_)) {
    ctx_->record_error(ret);
  }
  if (OB_NOT_NULL(ctx_) && ctx_->is_scn_reserved_) {
    ret = ctx_->get_first_error();
    if (OB_SUCC(ret)) {
      if (OB_TMP_FAIL(remove_reserved_meta_scns_at_or_before_(
              ctx_->ls_id_, ctx_->ckpt_data_.reserved_meta_scn_))) {
        FLOG_WARN("remove reserved meta scn on success path failed",
                  KR(tmp_ret), K(ctx_->ckpt_data_.reserved_meta_scn_));
      }
    } else if (OB_TMP_FAIL(remove_reserved_meta_scn_(
                   ctx_->ls_id_, ctx_->ckpt_data_.reserved_meta_scn_))) {
      FLOG_WARN("remove reserved meta scn on failure path failed",
                KR(tmp_ret), KR(ret), K(ctx_->ckpt_data_.reserved_meta_scn_));
    }
  }
  LOG_INFO("finish ls checkpoint write dag", KR(ret), KPC(ctx_));
  return ret;
}

} // namespace storage
} // namespace oceanbase
