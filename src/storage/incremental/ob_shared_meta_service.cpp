/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/incremental/share/ob_ss_diagnose_mgr.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/atomic_protocol/ob_ls_atomic_protocol.h"
#include "storage/incremental/atomic_protocol/ob_tablet_atomic_protocol.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/ob_storage_struct.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_sstablet_persister.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/string/ob_string_holder.h"
#include "lib/string/ob_sql_string.h"
#include "storage/ls/ob_ls.h"
#include "observer/ob_srv_network_frame.h"
#include "ob_ss_tablet_merge_helper.h"
#include "storage/high_availability/ob_ss_ha_macro_copy_task_generator.h"
#include "lib/utility/serialization.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_helper.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_data.h"
#include "storage/incremental/sslog/ob_sslog_kv_palf_adapter.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_perf_stat.h"
#ifdef ERRSIM
#include "share/location_cache/ob_location_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#endif

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::logservice;
using namespace oceanbase::transaction;
using namespace oceanbase::blocksstable;

typedef ObDefaultSSMetaValue<ObSSLSMeta> ObSSLSMetaValue;
ERRSIM_POINT_DEF(EN_SS_LS_CKPT_REMOTE_DELETE_ERROR);
ERRSIM_POINT_DEF(EN_SS_LS_CKPT_TABLE_DELETE_ERROR);
ERRSIM_POINT_DEF(EN_SS_TABLET_META_REMOTE_DELETE_ERROR);
ERRSIM_POINT_DEF(EN_SS_TABLET_META_TABLE_DELETE_ERROR);
#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_CHECK_SHARED_META_LOCATION_READY);
#endif
#define META_SRV_PERF_TIMEGUARD_INIT() PERF_TIMEGUARD_WITH_MOD_INIT(MetaService)
// MetaService perf stat items (only multi-step ops, aligned with timeguard usage)
PERF_STAT_ITEM(perf_create_ls);
PERF_STAT_ITEM(perf_update_ls_id_meta);
PERF_STAT_ITEM(perf_update_ls_gc_state);
PERF_STAT_ITEM(perf_update_ls_meta);
PERF_STAT_ITEM(perf_create_tablet);
PERF_STAT_ITEM(perf_update_tablet_table_store);
PERF_STAT_ITEM(perf_replace_tablet_minor_sstable);
PERF_STAT_ITEM(perf_build_tablet_with_batch_tables);
PERF_STAT_ITEM(perf_update_tablet_meta);
PERF_STAT_ITEM(perf_mark_tablet_deleted);
PERF_STAT_ITEM(perf_mark_inner_tablet_delete);

class ObSSMetaDiagnoseGuardV2
{
public:
  ObSSMetaDiagnoseGuardV2(const int64_t threshold,
                          const bool is_read,
                          const char* func_name,
                          int &ret)
    : threshold_(threshold),
      abs_start_time_ts_(ObTimeUtility::current_time()),
      is_read_(is_read),
      func_name_(func_name),
      ret_(ret) {}
  ~ObSSMetaDiagnoseGuardV2()
  {
    int64_t curr_time_ts = ObTimeUtility::current_time();
    int64_t delay_us = curr_time_ts - abs_start_time_ts_;
    // add diagnose info
    if (OB_SUCCESS != ret_) {
      // add diagnose info because of failure
      const char* reason = "failed because of error code";
      MTL(ObSSMetaService*)->add_diagnose_info(func_name_,
                                               reason,
                                               ret_);
    } else if (delay_us >= threshold_) {
      // add diagnose info only if the threshold is exceeded
      const char* reason = "cost too much time";
      MTL(ObSSMetaService*)->add_diagnose_info(func_name_,
                                               reason,
                                               delay_us);
    } else {
      // do nothing
    }

    // add sysstat event info
    if (is_read_) {
      EVENT_INC(ObStatEventIds::SS_META_READ_COUNT);
      EVENT_ADD(ObStatEventIds::SS_META_READ_DELAY, delay_us);
    } else {
      EVENT_INC(ObStatEventIds::SS_META_WRITE_COUNT);
      EVENT_ADD(ObStatEventIds::SS_META_WRITE_DELAY, delay_us);
    }
  }
public:
  const int64_t threshold_;
  int64_t abs_start_time_ts_;
  bool is_read_;
  const char *func_name_;
  int &ret_;
};

#define SS_META_READ_GUARD ObSSMetaDiagnoseGuardV2 _ss_meta_read_stat_guard(100_ms, true, __PRETTY_FUNCTION__, ret)
#define SS_META_WRITE_GUARD ObSSMetaDiagnoseGuardV2 _ss_meta_write_stat_guard(500_ms, false, __PRETTY_FUNCTION__, ret)
#define READ_GUARD_COST_US (ObTimeUtility::current_time() - _ss_meta_read_stat_guard.abs_start_time_ts_)
#define WRITE_GUARD_COST_US (ObTimeUtility::current_time() - _ss_meta_write_stat_guard.abs_start_time_ts_)

class CreateLsRetryOp : public ObSSMetaRetryOp
{
public:
  CreateLsRetryOp(int &ret, ObSSMetaService &meta_svr,
                  ObLS *ls, const ObSSLSCreateMode create_mode)
    : ObSSMetaRetryOp(ret), meta_svr_(meta_svr), ls_(ls), create_mode_(create_mode)
  {}
  void operator()() override { ret_ = meta_svr_.create_ls_(ls_, create_mode_); }
private:
  ObSSMetaService &meta_svr_;
  ObLS *ls_;
  const ObSSLSCreateMode create_mode_;
};

class CreateLsInnerTabletRetryOp : public ObSSMetaRetryOp
{
public:
  CreateLsInnerTabletRetryOp(int &ret, ObSSMetaService &meta_svr, ObLS *ls)
    : ObSSMetaRetryOp(ret), meta_svr_(meta_svr), ls_(ls) {}
  void operator()() override { ret_ = meta_svr_.create_ls_inner_tablet_(ls_); }
private:
  ObSSMetaService &meta_svr_;
  ObLS *ls_;
};

class CreateTabletRetryOp : public ObSSMetaRetryOp
{
public:
  CreateTabletRetryOp(int &ret, ObSSMetaService &meta_svr,
                      const ObLSID &ls_id,
                      const ObTabletID &tablet_id,
                      const SCN &reorgan_scn,
                      const ObTabletHandle &local_tablet_hdl,
                      const bool check_lease)
    : ObSSMetaRetryOp(ret), meta_svr_(meta_svr),
      ls_id_(ls_id), tablet_id_(tablet_id), reorgan_scn_(reorgan_scn),
      local_tablet_hdl_(local_tablet_hdl), check_lease_(check_lease) {}
  void operator()() override
  {
    ret_ = meta_svr_.create_tablet_(ls_id_, tablet_id_, reorgan_scn_,
                                    local_tablet_hdl_, check_lease_);
  }
private:
  ObSSMetaService &meta_svr_;
  const ObLSID &ls_id_;
  const ObTabletID &tablet_id_;
  const SCN &reorgan_scn_;
  const ObTabletHandle &local_tablet_hdl_;
  const bool check_lease_;
};

class UpdateTabletTableStoreRetryOp : public ObSSMetaRetryOp
{
public:
  UpdateTabletTableStoreRetryOp(int &ret, ObSSMetaService &meta_svr,
                                const ObLSID &ls_id,
                                const ObTabletID &tablet_id,
                                const SCN &reorgan_scn,
                                const ObMetaUpdateReason update_reason,
                                ObUpdateTableStoreParam &param,
                                const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
                                const bool check_lease)
    : ObSSMetaRetryOp(ret), meta_svr_(meta_svr),
      ls_id_(ls_id), tablet_id_(tablet_id), reorgan_scn_(reorgan_scn),
      update_reason_(update_reason), param_(param),
      meta_ctrl_keys_(meta_ctrl_keys), check_lease_(check_lease) {}
  void operator()() override
  {
    ret_ = meta_svr_.do_update_tablet_table_store_(ls_id_, tablet_id_, reorgan_scn_,
               update_reason_, param_, meta_ctrl_keys_, check_lease_);
  }
private:
  ObSSMetaService &meta_svr_;
  const ObLSID &ls_id_;
  const ObTabletID &tablet_id_;
  const SCN &reorgan_scn_;
  const ObMetaUpdateReason update_reason_;
  ObUpdateTableStoreParam &param_;
  const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys_;
  const bool check_lease_;
};

class ReplaceTabletMinorSstableRetryOp : public ObSSMetaRetryOp
{
public:
  ReplaceTabletMinorSstableRetryOp(int &ret, ObSSMetaService &meta_svr,
                                   const ObLSID &ls_id,
                                   const ObTabletID &tablet_id,
                                   const SCN &reorgan_scn,
                                   const ObMetaUpdateReason update_reason,
                                   ObUpdateTableStoreParam &param,
                                   const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
                                   const bool check_lease)
    : ObSSMetaRetryOp(ret), meta_svr_(meta_svr),
      ls_id_(ls_id), tablet_id_(tablet_id), reorgan_scn_(reorgan_scn),
      update_reason_(update_reason), param_(param),
      meta_ctrl_keys_(meta_ctrl_keys), check_lease_(check_lease) {}
  void operator()() override
  {
    ret_ = meta_svr_.do_replace_tablet_minor_sstable_(ls_id_, tablet_id_, reorgan_scn_,
               update_reason_, param_, meta_ctrl_keys_, check_lease_);
  }
private:
  ObSSMetaService &meta_svr_;
  const ObLSID &ls_id_;
  const ObTabletID &tablet_id_;
  const SCN &reorgan_scn_;
  const ObMetaUpdateReason update_reason_;
  ObUpdateTableStoreParam &param_;
  const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys_;
  const bool check_lease_;
};

int ObSSMetaService::mtl_init(ObSSMetaService *&meta_service)
{
  int ret = OB_SUCCESS;
  LOG_INFO("ObSSMetaService mtl init");
  return meta_service->init();
}

int ObSSMetaService::init()
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "MetaService");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(bucket_lock_.init(BUCKET_CNT, ObLatchIds::OB_SS_META_SERVICE_BUCKET_LOCK, "SSMetaBucket", MTL_ID()))) {
    LOG_WARN("fail to init bucket lock", KR(ret));
  } else if (OB_FAIL(GCTX.net_frame_->get_proxy(meta_rpc_proxy_))) {
    LOG_WARN("fail to get meta rpc proxy", KR(ret));
  } else if (OB_FAIL(ss_usage_report_task_.init(*GCTX.sql_proxy_))) {
    LOG_WARN("fail to init ss_usage_report_task_", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSSMetaService::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_SCHEDULE(MTL(omt::ObSharedTimer*)->get_tg_id(),
          ss_usage_report_task_, ObSSUsageReportTask::SS_USAGE_REPORT_INTERVAL, true))) {
    LOG_WARN("fail to schedule ss_usage_report_task", KR(ret));
  } else if (OB_FAIL(ss_sstable_gc_processor_.start())) {
    LOG_WARN("fail to start ss sstable gc processor", KR(ret));
  } else {
    FLOG_INFO("success to start ss meta service");
  }
  return ret;
}

int ObSSMetaService::stop()
{
  int ret = OB_SUCCESS;
  TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), ss_usage_report_task_);
  FLOG_INFO("success to stop ss meta service");
  if (OB_FAIL(ss_sstable_gc_processor_.stop())) {
    LOG_WARN("fail to stop ss sstable gc processor", KR(ret));
  }
  return ret;
}

int ObSSMetaService::wait()
{
  int ret = OB_SUCCESS;
  ss_sstable_gc_processor_.wait();
  return ret;
}

void ObSSMetaService::destroy()
{
  is_inited_ = false;
  TG_CANCEL_TASK(MTL(omt::ObSharedTimer *)->get_tg_id(), ss_usage_report_task_);
  bucket_lock_.destroy();
  ss_sstable_gc_processor_.destroy();
  FLOG_INFO("success to destroy ss meta service");
}

int ObSSMetaService::resolve_create_ret_(
    const int create_ret,
    const ObSSMetaTypeEnum meta_type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn)
{
  int ret = create_ret;
  int tmp_ret = OB_SUCCESS;
  if (OB_ENTRY_EXIST == ret) {
    // if ls/tablet meta already exist, treat it as succ.
    ret = OB_SUCCESS;
  } else if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
    if (ObSSMetaTypeEnum::LS_META == meta_type) {
      ObSSLSMeta ls_meta;
      tmp_ret = get_ls_from_ss_(ls_id, SCN::max_scn(), ls_meta, true/*by_prefix*/);
      if (OB_SUCC(tmp_ret)) {
        if (ls_meta.is_valid()) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls meta is invalid after duplicate", KR(ret), K(ls_id), K(ls_meta));
        }
      } else if (OB_LS_NOT_EXIST == tmp_ret) {
        ret = OB_NEED_RETRY;
        LOG_INFO("ls is not visible after duplicate, need retry", KR(ret), K(ls_id));
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to verify ls after duplicate", KR(ret), K(ls_id));
      }
    } else if (ObSSMetaTypeEnum::TABLET_META == meta_type) {
      ObArenaAllocator allocator("CrtRetCheck", OB_MALLOC_NORMAL_BLOCK_SIZE,
                                 MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
      ObTabletHandle tablet_handle;
      SCN meta_scn;
      tmp_ret = get_tablet_from_ss_(ls_id, tablet_id, reorgan_scn,
                                    allocator, tablet_handle, meta_scn);
      if (OB_SUCC(tmp_ret)) {
        if (tablet_handle.is_valid()) {
          ret = OB_SUCCESS;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet handle is invalid after duplicate", KR(ret), K(ls_id),
                 K(tablet_id), K(reorgan_scn));
        }
      } else if (OB_OBJECT_NOT_EXIST == tmp_ret) {
        ret = OB_NEED_RETRY;
        LOG_INFO("tablet is not visible after duplicate, need retry", KR(ret), K(ls_id),
                 K(tablet_id), K(reorgan_scn));
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to verify tablet after duplicate", KR(ret), K(ls_id),
                 K(tablet_id), K(reorgan_scn));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid meta type for create result", KR(ret),
               "meta_type", ss_meta_to_string(meta_type));
    }
  }
  return ret;
}

bool ObSSMetaService::need_retry_(const int ret)
{
  return (OB_EAGAIN == ret || OB_NEED_RETRY == ret);
}

// 所以目前类似于mark_tablet_deleted等接口，都没有使用do_with_retry_进行封装
void ObSSMetaService::do_with_retry_(const char *op_name, ObSSMetaRetryOp &op)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t retry_timeout_us = SSMETA_TABLE_SQL_TIMEOUT_US;
  const uint64_t tenant_id = MTL_ID();
  if (OB_TMP_FAIL(ObSSMetaInfoUtil::get_ssmeta_trans_timeout_us(tenant_id, retry_timeout_us))) {
    LOG_WARN("fail to get ssmeta trans timeout", KR(tmp_ret), K(tenant_id));
    retry_timeout_us = SSMETA_TABLE_SQL_TIMEOUT_US; // set it as default value
  }

  int retry_cnt = 0;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t sleep_us = 1_ms;
  do {
    op();
    ret = op.get_ret();
    if (OB_SUCC(ret)) {
      break;
    } else if (need_retry_(ret)) {
      if (ObTimeUtility::current_time() - start_ts > retry_timeout_us) {
        LOG_WARN("retry timeout", KR(ret), "op", op_name, K(start_ts), K(retry_timeout_us), K(retry_cnt));
        break;
      }
      ++retry_cnt;
      sleep_us = MIN(200_ms, sleep_us * 2);
      ob_usleep(sleep_us);
      LOG_INFO("sleep done, retry", KR(ret), "op", op_name, K(sleep_us), K(retry_cnt));
    } else {
      LOG_WARN("fail to execute op in retry_process", KR(ret), "op", op_name, K(sleep_us), K(retry_cnt));
    }
  } while (need_retry_(ret));
}

void ObSSMetaService::add_diagnose_info(const char* func_name, const char* reason, const int64_t extra_info)
{
  const ObLSID ls_id(0);
  const ObTabletID tablet_id(0);
  ADD_SS_DIAGNOSE_INFO(ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE,
                       ls_id,
                       tablet_id,
                       ObTimeUtility::current_time(),
                       "func_name", func_name,
                       "reason", reason,
                       K(extra_info));
}

int ObSSMetaService::build_tablet_protocol_init_param_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObArray<ObSSMetaCtrlKeyInfo> &key_infos,
    ObTabletAtomicProtocolParam &init_param)
{
  return build_tablet_protocol_init_param_(ls_id, tablet_id, reorgan_scn, key_infos, false, SCN(), init_param);
}

int ObSSMetaService::build_tablet_protocol_init_param_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObArray<ObSSMetaCtrlKeyInfo> &key_infos,
    const bool is_deleted,
    const SCN &deleted_scn,
    ObTabletAtomicProtocolParam &init_param)
{
  int ret = OB_SUCCESS;
  init_param.reset();
  if (key_infos.count() > 0 && OB_FAIL(init_param.key_infos_.assign(key_infos))) {
    LOG_WARN("fail to assign", KR(ret), K(key_infos));
  }

  if (OB_SUCC(ret)) {
    init_param.tenant_id_ = MTL_ID();
    init_param.ls_id_ = ls_id;
    init_param.tablet_id_ = tablet_id;
    init_param.reorg_scn_ = reorgan_scn;
    init_param.is_deleted_ = is_deleted;
    init_param.deleted_scn_ = deleted_scn;
  }
  return ret;
}

void ObSSMetaService::build_tablet_op_init_param_(
    const int64_t op_id,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    ObSSMetaAtomicOpInitParam &init_param)
{
  init_param.op_id_ = op_id;
  init_param.ls_id_ = ls_id;
  init_param.tablet_id_ = tablet_id;
  init_param.reorgan_scn_ = reorgan_scn;
  init_param.protocol_type_.type_ = ObAtomicProtocolTypeEnum::TABLET_META;
}

void ObSSMetaService::build_ls_op_init_param_(
    const int64_t op_id,
    const ObLSID &ls_id,
    ObSSMetaAtomicOpInitParam &init_param)
{
  init_param.op_id_ = op_id;
  init_param.ls_id_ = ls_id;
  init_param.protocol_type_.type_ = ObAtomicProtocolTypeEnum::LS_META;
}

int ObSSMetaService::get_max_committed_meta_scn(SCN &scn)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (OB_FAIL(ObAtomicProtocolUtil::get_gts(MTL_ID(), scn))) {
    LOG_WARN("fail to get gts", KR(ret));
  }
  return ret;
}

int ObSSMetaService::get_raw_meta_iter(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObSSMetaIterGuard<ObSSMetaRawRowIter> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaRawRowIter *iter = nullptr;
  ObSSMetaType meta_type;
  ObSSMetaVersionFilter version_filter;
  version_filter.set_upper_bound(read_snapshot);
  ObSqlString prefix;
  const bool is_ls_type = (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META
                           || read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT);
  const bool skip_mark_delete = !read_param.read_mark_delete_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else {
    if (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    } else if (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
    } else {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    }
    if (OB_FAIL(ObSSMetaInfoUtil::gen_meta_key_prefix(read_param, prefix))) {
      LOG_WARN("fail to gen meta key prefix", KR(ret), K(read_param));
    } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaRawRowIter), "MetaService"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc raw row iter failed", KR(ret));
    } else if (FALSE_IT(iter = new (ptr) ObSSMetaRawRowIter())) {
    } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                  is_ls_type ? read_param.ls_key_.ls_id_ : read_param.tablet_key_.ls_id_,
                                  meta_type, prefix.string(), version_filter,
                                  skip_mark_delete))) {
      LOG_WARN("fail to init raw row iter", KR(ret), K(read_param), K(read_snapshot));
    } else {
      iter_guard.set_iter(iter);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
      free_iterator(iter);
      iter = nullptr;
    }
  }
  LOG_INFO("[SS_META_SVR]: get raw_meta iter finish", KR(ret), K(read_param), K(read_snapshot), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_latest_raw_meta_iter(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObSSMetaIterGuard<ObSSMetaRawRowIter> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaRawRowIter *iter = nullptr;
  ObSSMetaType meta_type;
  ObSSMetaVersionFilter version_filter;
  version_filter.set_upper_bound(read_snapshot);
  version_filter.set_desc_order();
  version_filter.set_dedup_by_prefix();
  ObSqlString prefix;
  const bool is_ls_type = (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META
                           || read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT);
  const bool skip_mark_delete = !read_param.read_mark_delete_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else {
    if (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    } else if (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
    } else {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    }
    if (OB_FAIL(ObSSMetaInfoUtil::gen_meta_key_prefix(read_param, prefix))) {
      LOG_WARN("fail to gen meta key prefix", KR(ret), K(read_param));
    } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaRawRowIter), "MetaService"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc raw row iter failed", KR(ret));
    } else if (FALSE_IT(iter = new (ptr) ObSSMetaRawRowIter())) {
    } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                  is_ls_type ? read_param.ls_key_.ls_id_ : read_param.tablet_key_.ls_id_,
                                  meta_type, prefix.string(), version_filter,
                                  skip_mark_delete))) {
      LOG_WARN("fail to init latest raw row iter", KR(ret), K(read_param), K(read_snapshot));
    } else {
      iter_guard.set_iter(iter);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
      free_iterator(iter);
      iter = nullptr;
    }
  }
  LOG_INFO("[SS_META_SVR]: get latest raw_meta iter finish", KR(ret), K(read_param),
           K(read_snapshot), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_raw_meta_iter(
    const ObSSMetaReadParamV2 &read_param,
    const ObScnRange &range,
    ObSSMetaIterGuard<ObSSMetaRawRowIter> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaRawRowIter *iter = nullptr;
  ObSSMetaType meta_type;
  ObSSMetaVersionFilter version_filter;
  ObSqlString prefix;
  const bool is_ls_type = (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META
                           || read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT);
  const bool skip_mark_delete = !read_param.read_mark_delete_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(range));
  } else if (OB_FAIL(version_filter.set_version_range(range.start_scn_, range.end_scn_))) {
    LOG_WARN("fail to set version range", KR(ret), K(range));
  } else {
    if (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::LS_META) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    } else if (read_param.protocol_type_ == ObAtomicProtocolTypeEnum::CHECKPOINT) {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
    } else {
      meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    }
    if (OB_FAIL(ObSSMetaInfoUtil::gen_meta_key_prefix(read_param, prefix))) {
      LOG_WARN("fail to gen meta key prefix", KR(ret), K(read_param));
    } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaRawRowIter), "MetaService"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc raw row iter failed", KR(ret));
    } else if (FALSE_IT(iter = new (ptr) ObSSMetaRawRowIter())) {
    } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                  is_ls_type ? read_param.ls_key_.ls_id_ : read_param.tablet_key_.ls_id_,
                                  meta_type, prefix.string(), version_filter,
                                  skip_mark_delete))) {
      LOG_WARN("fail to init raw row iter", KR(ret), K(read_param), K(range));
    } else {
      iter_guard.set_iter(iter);
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
      free_iterator(iter);
      iter = nullptr;
    }
  }
  LOG_INFO("[SS_META_SVR]: get raw_meta iter finish", KR(ret), K(read_param), K(range), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_raw_meta(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObIAllocator &allocator,
    ObSSRawMetaRow &row)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  ObArenaAllocator tmp_alloc("GetRawMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObSSRawMetaRow tmp_row;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else if (OB_FAIL(ObSSMetaTableHelper::get_meta_info(read_param, read_snapshot, tmp_alloc, tmp_row))) {
    LOG_WARN("fail to get meta info", KR(ret), K(read_param), K(read_snapshot));
  } else if (OB_FAIL(row.deep_copy(allocator, tmp_row))) {
    LOG_WARN("fail to deep copy row", KR(ret));
  }
  LOG_INFO("[SS_META_SVR]: get raw_meta finish", KR(ret), K(read_param), K(read_snapshot), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::insert_raw_meta(const ObSSRawMetaRow &row)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  ObSSMetaTableOperator op;
  ObSSMetaBaseType meta_type;
  meta_type.type_val_ = row.meta_type_;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
    LOG_WARN("fail to init ss meta operator", KR(ret));
  } else if (OB_FAIL(op.insert_row(row.tenant_id_, row.ls_id_, meta_type,
                                    row.meta_key_, row.meta_value_, affected_rows))) {
    LOG_WARN("fail to insert raw meta row", KR(ret), K(row));
  }
  LOG_INFO("[SS_META_SVR]: insert raw_meta finish", KR(ret), K(row), K(affected_rows), "cost_us",
    WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::create_ls(const obrpc::ObCreateLSArg &arg,
                               const ObSSLSCreateMode create_mode)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  META_SRV_PERF_TIMEGUARD_INIT();
  PERF_GUARD_INIT(perf_create_ls);
  const ObLSID &ls_id = arg.get_ls_id();
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObLSService *ls_service = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!arg.is_valid()
             || (ObSSLSCreateMode::NORMAL_CREATE != create_mode
                 && ObSSLSCreateMode::BOOTSTRAP_CREATE != create_mode))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg), K(create_mode));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_service is nullptr", KR(ret), K(arg));
  } else if (CLICK_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::SHARED_META_SERVICE))) {
    LOG_WARN("fail to get ls", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is nullptr", KR(ret), K(ls_id), KP(ls));
  } else {
    // 2. create shared ls
    CLICK();
    CreateLsRetryOp create_ls_op(ret, *this, ls, create_mode);
    do_with_retry_("create_ls", create_ls_op);
    CLICK();
  }

  if (OB_SUCC(ret) && arg.need_create_inner_tablets()) {
    // 3. create shared ls inner tablet
    // TODO: zeyong RESTORE TENANT SHOULD MAKE SURE HAS ALL THE INNER TABLET
    CLICK();
    CreateLsInnerTabletRetryOp create_ls_inner_tablet_op(ret, *this, ls);
    do_with_retry_("create_ls_inner_tablet", create_ls_inner_tablet_op);
    CLICK();
  }
  LOG_INFO("[SS_META_SVR]: create ls finish", KR(ret), K(arg), K(create_mode),
           "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::update_ls_id_meta(
    const ObLSID &ls_id,
    const int64_t service_type,
    const int64_t limited_id,
    const SCN &latest_scn,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  PERF_GUARD_INIT(perf_update_ls_id_meta);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !latest_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(latest_scn));
  } else {
    const int64_t curr_time = ObTimeUtility::current_time();
    ObUpdateSharedLSMetaArg arg(ls_id, service_type, limited_id,
                                latest_scn, curr_time + timeout_us);
    if (OB_FAIL(update_for_ss_id_meta_(arg, ls_id, service_type, limited_id, latest_scn))) {
      LOG_WARN("fail to update ls id meta", KR(ret), K(ls_id), K(service_type), K(limited_id), K(latest_scn));
    }
  }
  LOG_INFO("[SS_META_SVR]: update ls_id_meta finish", KR(ret), K(ls_id), K(service_type), K(limited_id),
    K(latest_scn), K(timeout_us), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::update_ls_gc_state(
    const ObLSID &ls_id,
    const LSGCState &gc_state,
    const SCN &offline_scn,
    const int64_t timeout_us,
    const bool is_replay)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  PERF_GUARD_INIT(perf_update_ls_gc_state);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid()
                         || (ObGCHandler::is_ls_offline_gc_state(gc_state) && !offline_scn.is_valid())
                         || (!ObGCHandler::is_ls_offline_gc_state(gc_state) && offline_scn.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(gc_state), K(offline_scn), K(is_replay));
  } else {
    const int64_t curr_time = ObTimeUtility::current_time();
    ObUpdateSharedLSMetaArg arg(ls_id, gc_state, offline_scn, curr_time + timeout_us);
    if (OB_FAIL(update_for_ss_gc_state_(arg, ls_id, gc_state, offline_scn, is_replay))) {
      LOG_WARN("fail to update ls meta", KR(ret), K(ls_id), K(gc_state), K(offline_scn));
    }
  }
  LOG_INFO("[SS_META_SVR]: update ls gc_state finish", KR(ret), K(ls_id), K(gc_state), K(offline_scn),
    K(timeout_us), K(is_replay), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::update_ls_meta(
    const ObLSID &ls_id,
    const ObMetaUpdateReason update_reason,
    ObIMetaFunction &fn,
    const bool check_lease,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  PERF_GUARD_INIT(perf_update_ls_meta);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else {
    const int64_t curr_time = ObTimeUtility::current_time();
    const int64_t func_code = fn.get_func_code();
    ObArenaAllocator allocator("UptLsMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
    ObString functor_buf;
    if (ObIMetaFunction::need_transmit_functor_buf(func_code)) {
      const int64_t serialize_size = fn.get_serialize_size();
      char *buf = nullptr;
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(serialize_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate memory for functor serialization", KR(ret), K(serialize_size));
      } else if (OB_FAIL(fn.serialize(buf, serialize_size, pos))) {
        LOG_WARN("fail to serialize functor", KR(ret), K(serialize_size));
      } else {
        functor_buf.assign_ptr(buf, static_cast<int32_t>(pos));
      }
    }

    if (OB_SUCC(ret)) {
      ObUpdateSharedLSMetaArg arg(ls_id, update_reason, func_code, functor_buf,
                                  curr_time + timeout_us);
      arg.check_lease_ = check_lease;
      if (OB_FAIL(update_for_ss_ls_meta_(arg, ls_id, fn))) {
        LOG_WARN("fail to update ls meta", KR(ret), K(ls_id), K(update_reason));
      }
    }
  }
  LOG_INFO("[SS_META_SVR]: update ls_meta finish", KR(ret), K(ls_id), K(update_reason), K(fn), K(check_lease),
    "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_ls_meta(
    const ObLSID &ls_id,
    ObSSLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_from_ss_(ls_id, SCN::max_scn(), ls_meta, true/*by_prefix*/))) {
    LOG_WARN("get ls meta from ss failed", KR(ret), K(ls_id));
  }
  LOG_TRACE("[SS_META_SVR]: get ls_meta finish", KR(ret), K(ls_id), K(ls_meta), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_ls_meta(
    const ObLSID &ls_id,
    const SCN &meta_scn,
    ObSSLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_from_ss_(ls_id, meta_scn, ls_meta, false/*by_prefix*/))) {
    LOG_WARN("get ls meta from ss failed", KR(ret), K(ls_id), K(meta_scn));
  }
  LOG_TRACE("[SS_META_SVR]: get ls_meta finish", KR(ret), K(ls_id), K(ls_meta), K(meta_scn), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_ls_meta(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObSSLSMeta &ls_meta)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else {
    SMART_VAR(ObDefaultSSMetaValue<ObSSLSMeta>, ls_value) {
      SCN meta_scn;
      if (OB_FAIL(ObSSMetaTableHelper::get_ls(read_param, read_snapshot, ls_value, meta_scn))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_LS_NOT_EXIST;
        }
        LOG_WARN("fail to get ls meta", KR(ret), K(read_param), K(read_snapshot));
      } else {
        ls_meta = ls_value.meta_value_;
      }
    }
  }
  LOG_TRACE("[SS_META_SVR]: get ls_meta finish", KR(ret), K(read_param), K(read_snapshot), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_ls_id_iter(
    const SCN &read_snapshot,
    ObSSMetaIterGuard<ObSSMetaLSIDIter> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaLSIDIter *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_snapshot));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaLSIDIter), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ls id iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaLSIDIter())) {
  } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(), read_snapshot))) {
    LOG_WARN("fail to init ls id iter", KR(ret), K(read_snapshot));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_INFO("[SS_META_SVR]: get ls_id iter finish", KR(ret), K(read_snapshot), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::remove_ls(const ObLSID &ls_id, const SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  int64_t affected_rows = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(meta_scn));
  } else {
    ObSSMetaLSKey ls_key;
    ls_key.ls_id_ = ls_id;
    ls_key.meta_scn_ = meta_scn;
    MacroBlockId macro_id = ls_key.convert_to_macro_id(true/*is_in_table*/);
    char path_buf[SSMETA_PATH_MAX_LEN] = {0};
    int64_t pos = 0;
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
    ObSSMetaTableOperator op;
    if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(macro_id, path_buf, sizeof(path_buf), pos))) {
      LOG_WARN("fail to format meta_key path", KR(ret), K(macro_id));
    } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
      LOG_WARN("fail to init ss meta operator", KR(ret));
    } else {
      ObString meta_key(pos, path_buf);
      if (OB_FAIL(op.delete_row(MTL_ID(), ls_id, meta_type, meta_key,
                                false/*prefix_delete*/, affected_rows))) {
        LOG_WARN("fail to delete ls meta row", KR(ret), K(ls_id), K(meta_scn), K(meta_key));
      }
    }
  }
  LOG_INFO("[SS_META_SVR]: remove ls finish", KR(ret), K(ls_id), K(meta_scn), K(affected_rows), "cost_us",
    WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_ls_ckpt_meta_scn(
    const ObLSID &ls_id,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  meta_scn.reset();
  char key_buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  int64_t key_len = 0;
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter;
  ObArenaAllocator allocator("LsCkptKvGet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObStringBuffer value_buf(&allocator);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(MTL_ID()) || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "tenant_id", MTL_ID(), K(ls_id));
  } else if (OB_UNLIKELY(!GCONF.enable_logservice)) {
    // ls_ckpt is published to palf_kv only when logservice is enabled
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ls ckpt palf kv is not available when logservice is disabled", KR(ret), K(ls_id));
  } else if (OB_FAIL(ObSSMetaInfoUtil::build_ls_ckpt_key(
      ls_id, key_buf, sizeof(key_buf), key_len))) {
    LOG_WARN("fail to build ls ckpt palf kv key", KR(ret), K(ls_id));
  } else if (OB_FAIL(palf_kv_adapter.init(GCONF.cluster_id, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to init palf kv adapter", KR(ret));
  } else if (OB_FAIL(palf_kv_adapter.get(ObString(static_cast<int32_t>(key_len), key_buf), value_buf))) {
    LOG_WARN("fail to get ls ckpt palf kv", KR(ret), K(ls_id));
  } else {
    int64_t pos = 0;
    MacroBlockId macro_id;
    const ObString value = value_buf.string();
    if (OB_FAIL(macro_id.deserialize(value.ptr(), value.length(), pos))) {
      LOG_WARN("fail to deserialize ls ckpt macro id", KR(ret), K(value));
    } else if (OB_UNLIKELY(pos != value.length()
                           || !macro_id.is_valid()
                           || macro_id.storage_object_type() != ObStorageObjectType::SHARED_LS_CKPT
                           || macro_id.second_id() != ls_id.id()
                           || macro_id.meta_scn() < 0)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid ls ckpt macro id", KR(ret), K(pos), K(value), K(macro_id), K(ls_id));
    } else if (OB_FAIL(meta_scn.convert_for_logservice(static_cast<uint64_t>(macro_id.meta_scn())))) {
      LOG_WARN("fail to convert ls ckpt meta_scn", KR(ret), K(macro_id));
    }
  }
  LOG_INFO("[SS_META_SVR]: get ls_ckpt meta_scn finish", KR(ret), K(ls_id), K(meta_scn),
    "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::read_ls_ckpt_data(
    const ObLSID &ls_id,
    const SCN &ckpt_meta_scn,
    ObSSLSMeta &ls_meta,
    ObIArray<MacroBlockId> &tablet_meta_keys)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  ls_meta.reset();
  tablet_meta_keys.reset();

  ObArenaAllocator allocator("ReadLsCkpt", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObSSMetaLSCkptKey ckpt_key;
  ObSSLsCkptData ckpt_data;
  MacroBlockId ckpt_macro_id;
  ObStorageObjectHandle read_handle;
  ObStorageObjectReadInfo read_info;
  int64_t object_size = 0;
  char *read_buf = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(MTL_ID()) || !ls_id.is_valid() || !ckpt_meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "tenant_id", MTL_ID(), K(ls_id), K(ckpt_meta_scn));
  } else {
    ckpt_key.ls_id_ = ls_id;
    ckpt_key.meta_scn_ = ckpt_meta_scn;
    ckpt_macro_id = ckpt_key.convert_to_macro_id(false/*is_in_table*/);
    if (OB_UNLIKELY(!ckpt_macro_id.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ls ckpt macro id", KR(ret), K(ls_id), K(ckpt_meta_scn), K(ckpt_macro_id));
    } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.get_object_size(ckpt_macro_id, 0/*ls_epoch_id*/, object_size))) {
      LOG_WARN("fail to get ls ckpt object size", KR(ret), K(ckpt_macro_id));
    } else if (OB_UNLIKELY(object_size <= 0)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid ls ckpt object size", KR(ret), K(object_size), K(ckpt_macro_id));
    } else if (OB_UNLIKELY(object_size > INT32_MAX)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("ls ckpt object is too large", KR(ret), K(object_size), K(ckpt_macro_id));
    } else if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(object_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ls ckpt read buffer failed", KR(ret), K(object_size), K(ckpt_macro_id));
    } else {
      read_info.macro_block_id_ = ckpt_macro_id;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
      read_info.buf_ = read_buf;
      read_info.offset_ = 0;
      read_info.size_ = object_size;
      read_info.mtl_tenant_id_ = MTL_ID();
      read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(read_info.mtl_tenant_id_);
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.read_object(read_info, read_handle))) {
        LOG_WARN("fail to read ls ckpt object", KR(ret), K(ckpt_macro_id), K(read_info));
      } else {
        const char *data_buf = read_handle.get_buffer();
        const int64_t data_size = read_handle.get_data_size();
        int64_t pos = 0;
        if (OB_ISNULL(data_buf)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls ckpt object buffer is null", KR(ret), K(ckpt_macro_id));
        } else if (OB_UNLIKELY(data_size <= 0 || data_size > object_size)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid ls ckpt object data size", KR(ret), K(data_size), K(object_size),
                   K(ckpt_macro_id));
        } else if (OB_FAIL(ckpt_data.deserialize(data_buf, data_size, pos))) {
          LOG_WARN("fail to deserialize ls ckpt data", KR(ret), K(data_size), K(ckpt_macro_id));
        } else if (OB_UNLIKELY(pos != data_size
                               || !ckpt_data.is_valid()
                               || ckpt_data.ckpt_scn_ != ckpt_meta_scn
                               || ckpt_data.ls_meta_.ls_id_ != ls_id)) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid ls ckpt data", KR(ret), K(pos), K(data_size), K(ls_id),
                   K(ckpt_meta_scn), K(ckpt_macro_id), K(ckpt_data));
        } else {
          const uint64_t reserved_meta_scn_val = ckpt_data.get_reserved_meta_scn_val();
          for (int64_t i = 0; OB_SUCC(ret) && i < ckpt_data.tablet_meta_keys_.count(); ++i) {
            const MacroBlockId &tablet_meta_key = ckpt_data.tablet_meta_keys_.at(i);
            if (OB_UNLIKELY(!tablet_meta_key.is_valid()
                            || tablet_meta_key.storage_object_type()
                               != ObStorageObjectType::SHARED_TABLET_META
                            || !tablet_meta_key.is_in_object_storage())) {
              ret = OB_INVALID_DATA;
              LOG_WARN("invalid tablet meta key in ls ckpt data", KR(ret), K(i), K(tablet_meta_key),
                       K(ckpt_macro_id));
            } else if (OB_UNLIKELY(tablet_meta_key.meta_scn() < 0
                                   || tablet_meta_key.meta_scn() > reserved_meta_scn_val)) {
              ret = OB_INVALID_DATA;
              LOG_WARN("tablet meta scn exceeds ls ckpt snapshot", KR(ret), K(i), K(tablet_meta_key),
                K(ckpt_data.reserved_meta_scn_), K(ckpt_macro_id));
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_FAIL(tablet_meta_keys.assign(ckpt_data.tablet_meta_keys_))) {
              LOG_WARN("fail to assign tablet meta keys", KR(ret), K(ls_id), K(ckpt_meta_scn),
                       "tablet_meta_key_count", ckpt_data.tablet_meta_keys_.count());
            } else {
              ls_meta = ckpt_data.ls_meta_;
            }
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    ls_meta.reset();
    tablet_meta_keys.reset();
  }
  LOG_INFO("[SS_META_SVR]: read ls_ckpt data finish", KR(ret), K(ls_id), K(ckpt_meta_scn),
    "tablet_meta_key_count", tablet_meta_keys.count(), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::batch_del_ls_ckpt(
    const ObLSID &ls_id,
    const ObIArray<SCN> &meta_scns)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  int64_t affected_rows = 0;
  bool need_double_delete = false;
  SCN max_del_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!is_sys_tenant(MTL_ID()) || !ls_id.is_valid() || meta_scns.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "tenant_id", MTL_ID(), K(ls_id), K(meta_scns));
  } else {
    // Validate the whole batch before any deletion, so a bad trailing item
    // cannot leave an already-deleted prefix.
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_scns.count(); ++i) {
      const SCN &meta_scn = meta_scns.at(i);
      if (OB_UNLIKELY(!meta_scn.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ls ckpt meta_scn", KR(ret), K(ls_id), K(meta_scn), K(i));
      } else if (!max_del_scn.is_valid() || max_del_scn < meta_scn) {
        max_del_scn = meta_scn;
      }
    }
  }

  if (FAILEDx(check_double_route_ss_meta(MTL_ID(), need_double_delete))) {
    LOG_WARN("fail to check ls checkpoint needs double route", KR(ret), "tenant_id", MTL_ID(), K(ls_id));
  }

  if (OB_SUCC(ret) && GCONF.enable_logservice) {
    SCN published_scn;
    // PALF_KV publication only moves forward. Requiring every candidate to be
    // strictly older also prevents an in-flight newer version from becoming
    // the published version after this check.
    if (OB_FAIL(get_ls_ckpt_meta_scn(ls_id, published_scn))) {
      LOG_WARN("fail to get published ls ckpt meta_scn", KR(ret), K(ls_id));
    } else if (max_del_scn >= published_scn) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls ckpt to delete is not older than published version", KR(ret),
               K(ls_id), K(max_del_scn), K(published_scn));
    }
  }

  if (OB_SUCC(ret)) {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
    ObSSMetaTableOperator op;
    if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
      LOG_WARN("fail to init ss meta operator", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_scns.count(); ++i) {
      const SCN &meta_scn = meta_scns.at(i);
      ObSSMetaLSCkptKey ckpt_key;
      ckpt_key.ls_id_ = ls_id;
      ckpt_key.meta_scn_ = meta_scn;
      if (OB_UNLIKELY(!meta_scn.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ls ckpt meta_scn", KR(ret), K(ls_id), K(meta_scn), K(i));
      } else {
        char path_buf[SSMETA_PATH_MAX_LEN] = {0};
        int64_t pos = 0;
        MacroBlockId in_table_macro_id = ckpt_key.convert_to_macro_id(true/*is_in_table*/);
        int64_t cur_affected_rows = 0;
        // 双写模式先删object_storage再删内表行; 如果删obj成功但删表失败，GC会重试清理。
        if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(in_table_macro_id, path_buf, sizeof(path_buf), pos))) {
          LOG_WARN("fail to format ls ckpt meta key", KR(ret), K(in_table_macro_id), K(ckpt_key));
        } else if (need_double_delete) {
          MacroBlockId obj_macro_id = ckpt_key.convert_to_macro_id(false/*is_in_table*/);
          if (OB_FAIL(OB_E(EN_SS_LS_CKPT_REMOTE_DELETE_ERROR, GETTID())
                          OB_STORAGE_OBJECT_MGR.delete_object(obj_macro_id, 0/*ls_epoch_id*/))) {
            if (OB_OBJECT_NOT_EXIST == ret) {
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("fail to delete ls ckpt object", KR(ret), K(ls_id), K(meta_scn), K(obj_macro_id));
            }
          } else {
            FLOG_INFO("succ to delete ls ckpt object", K(i), K(obj_macro_id));
          }
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(OB_E(EN_SS_LS_CKPT_TABLE_DELETE_ERROR, GETTID())
                            op.delete_row(MTL_ID(), ls_id, meta_type,
                                ObString(static_cast<int32_t>(pos), path_buf),
                                false/*prefix_delete*/, cur_affected_rows))) {
          LOG_WARN("fail to delete ls ckpt meta row", KR(ret), K(ls_id), K(meta_scn), K(in_table_macro_id));
        } else {
          affected_rows += cur_affected_rows;
          FLOG_INFO("succ to delete ls ckpt meta row", K(i), K(in_table_macro_id));
        }
      }
    }
  }
  LOG_INFO("[SS_META_SVR]: batch delete ls_ckpt finish", KR(ret), K(ls_id), K(meta_scns.count()),
    K(affected_rows), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_ls_ckpt_iter(
    const ObLSID &ls_id,
    const SCN &read_snapshot,
    const bool read_mark_delete,
    ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaLSCkptKeyIter *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(read_snapshot));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaLSCkptKeyIter), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ls ckpt iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaLSCkptKeyIter())) {
  } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(), ls_id, read_snapshot,
                                !read_mark_delete/*skip_mark_delete*/))) {
    LOG_WARN("fail to init ls ckpt iter", KR(ret), K(ls_id), K(read_snapshot),
             K(read_mark_delete));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_INFO("[SS_META_SVR]: get ls_ckpt iter finish", KR(ret), K(ls_id), K(read_snapshot),
    K(read_mark_delete), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_ls_from_ss_(
    const ObLSID &ls_id,
    const SCN &scn,
    ObSSLSMeta &ls_meta,
    const bool by_prefix)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  read_param.set_read_mark_delete(true);
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!by_prefix && !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scn should be valid if by key", KR(ret), K(by_prefix), K(scn));
  } else if (by_prefix) {
    read_param.set_ls_level_param(
        ObSSMetaReadParamType::LS_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::LS_META,
        ls_id,
        SCN::max_scn() /*meta_scn*/,
        true /*is_in_table*/);
    SCN meta_scn;
    SMART_VAR(ObDefaultSSMetaValue<ObSSLSMeta>, ls_value) {
      if (OB_FAIL(ObSSMetaTableHelper::get_ls(read_param, scn/*read_snapshot*/, ls_value, meta_scn))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_LS_NOT_EXIST;
        }
        LOG_WARN("fail to get ls meta", KR(ret), K(ls_id), K(scn), K(read_param));
      } else {
        ls_meta = ls_value.meta_value_;
      }
    }
  } else {
    read_param.set_ls_level_param(
        ObSSMetaReadParamType::LS_KEY,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::LS_META,
        ls_id,
        scn /*meta_scn*/,
        true /*is_in_table*/);
    SMART_VAR(ObDefaultSSMetaValue<ObSSLSMeta>, ls_value) {
      if (OB_FAIL(ObSSMetaTableHelper::get_ls(read_param, ls_value))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_LS_NOT_EXIST;
        }
        LOG_WARN("fail to get ls meta", KR(ret), K(ls_id), K(scn), K(read_param));
      } else {
        ls_meta = ls_value.meta_value_;
      }
    }
  }
  return ret;
}

int ObSSMetaService::create_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const bool check_lease)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  PERF_GUARD_INIT(perf_create_tablet);
  ObLSService* ls_svr = NULL;
  ObLSTabletService *tablet_svr = NULL;
  ObLSHandle ls_handle;
  ObTabletHandle local_tablet_hdl;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (check_lease && GCTX.in_bootstrap_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("create tablet is not supported in bootstrap", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(ls_svr = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_service is nullptr", KR(ret));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::SHARED_META_SERVICE))) {
    LOG_WARN("fail to get ls", KR(ret), K(ls_id), KP(ls_svr), K(ls_handle));
  } else if (OB_ISNULL(tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_tablet service is nullptr", KR(ret));
  // READ_WITHOUT_CHECK: 无论tablet是否处于mds事务，都将其返回，不保证其对业务'可见'
  } else if (OB_FAIL(tablet_svr->get_tablet(tablet_id, local_tablet_hdl, 0,
             ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("fail to get_tablet", KR(ret), K(ls_id), K(tablet_id));
  } else {
    CreateTabletRetryOp create_tablet_op(ret, *this, ls_id, tablet_id, reorgan_scn, local_tablet_hdl, check_lease);
    do_with_retry_("create_tablet", create_tablet_op);
  }

  LOG_INFO("[SS_META_SVR]: create tablet finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(check_lease),
    "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::create_tablet_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObTabletHandle &local_tablet_hdl,
    const bool check_lease)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  META_SRV_PERF_TIMEGUARD_INIT();
  const uint64_t tenant_id = MTL_ID();
  ObArenaAllocator allocator("CrtTblMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id, ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle old_tablet_hdl;
  ObTablet *old_tablet = nullptr;

  ObSSMetaAtomicTabletOp tablet_meta_op;
  int64_t op_id = -1;

  ObTabletAtomicProtocol tablet_protocol;
  ObTabletAtomicProtocolParam tablet_protocol_param;
  ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
  // acquire the bucket lock here (instead of in the caller) so it is released between
  // retries, consistent with do_update_tablet_table_store_/do_replace_tablet_minor_sstable_.
  ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());
  if (CLICK_FAIL(build_tablet_protocol_init_param_(ls_id, tablet_id, reorgan_scn, meta_ctrl_keys, tablet_protocol_param))) {
    LOG_WARN("fail to build tablet protocol param", KR(ret));
  } else if (OB_FAIL(tablet_protocol.init(tablet_protocol_param))) {
    LOG_WARN("fail to init tablet protocol", KR(ret), K(tablet_protocol_param));
  } else if (CLICK_FAIL(tablet_protocol.create(check_lease))) {
    LOG_WARN("fail to create tablet protocol", KR(ret), K(check_lease));
  } else if (CLICK_FAIL(tablet_protocol.get_op_id(op_id))) {
    LOG_WARN("fail to get op_id", KR(ret));
  } else if (OB_UNLIKELY(op_id == -1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected op_id", KR(ret), K(op_id));
  } else {
    ObSSMetaAtomicOpInitParam tablet_op_param;
    build_tablet_op_init_param_(op_id, ls_id, tablet_id, reorgan_scn, tablet_op_param);
    if (CLICK_FAIL(tablet_meta_op.init(tablet_op_param))) {
      LOG_WARN("fail to init tablet meta_op", KR(ret), K(tablet_op_param));
    } else if (OB_FALSE_IT(CLICK())) {
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    // check exist, do nothing if the ss tablet exist.
    SCN tmp_meta_scn;
    if (CLICK_FAIL(get_tablet_from_ss_(ls_id, tablet_id, reorgan_scn, allocator, old_tablet_hdl, tmp_meta_scn))) {
      if (OB_OBJECT_NOT_EXIST != ret) {
        LOG_WARN("fail to get tablet from ss", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
      }
    } else if (OB_FALSE_IT(CLICK())) {
    } else if (!old_tablet_hdl.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet_handle is invalid", KR(ret), K(old_tablet_hdl));
    } else {
      old_tablet = old_tablet_hdl.get_obj();
      ret = OB_ENTRY_EXIST; // if already exists, should execute abort_op
    }

    if (ret == OB_OBJECT_NOT_EXIST) {
      ret = OB_SUCCESS;
      uint64_t data_version = 0;
      // create ss tablet based on local tablet hdl if there is no ss tablet
      if (CLICK_FAIL(acquire_first_created_ss_tablet_(allocator, local_tablet_hdl, old_tablet_hdl))) {
        LOG_WARN("fail to create a first_created_ss_tablet", KR(ret), K(tablet_id));
      } else {
        CLICK();
        LOG_TRACE("get tablet by first created", K(ls_id), K(tablet_id), K(reorgan_scn));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(old_tablet = old_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old_tablet is nullptr", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
      } else if (old_tablet_hdl.get_obj()->get_reorganization_scn() != reorgan_scn) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("local tablet not with expected reorganization scn", K(reorgan_scn),
          K(old_tablet_hdl.get_obj()->get_reorganization_scn()));
      // persist new tablet
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
        LOG_WARN("fail to get min data version", KR(ret));
      } else if (CLICK_FAIL(persist_tablet_(data_version,
                                            old_tablet,
                                            ObMetaUpdateReason::CREATE_TABLET,
                                            &tablet_meta_op))) {
        LOG_WARN("fail to persist tablet", KR(ret), KPC(old_tablet));
      } else if (CLICK_FAIL(tablet_protocol.finish(tablet_meta_op))) {
        LOG_WARN("fail to finish tablet_protocol", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }
  }

  if (OB_FAIL(ret) && OB_TMP_FAIL(tablet_protocol.abort())) {
    LOG_WARN("fail to abort tablet protocol", KR(ret), KR(tmp_ret));
  }

  // only handle entry_exist and key_dup errno, otherwise return original ret
  ret = resolve_create_ret_(ret, ObSSMetaTypeEnum::TABLET_META,
                            ls_id, tablet_id, reorgan_scn);
  return ret;
}

static int build_upper_trans_version_update_info_v2_(
  const ObLSID &ls_id,
  const ObTablet &tablet,
  ObIArray<UpdateUpperTransParam::SCNAndVersion> &upper_trans_list);

int ObSSMetaService::update_tablet_table_store(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObMetaUpdateReason update_reason,
    ObUpdateTableStoreParam &param,
    const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
    const bool check_lease)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  PERF_GUARD_INIT(perf_update_tablet_table_store);
  UpdateTabletTableStoreRetryOp update_op(ret, *this, ls_id, tablet_id, reorgan_scn, update_reason, param,
      meta_ctrl_keys, check_lease);
  do_with_retry_("update_tablet_table_store", update_op);
  LOG_INFO("[SS_META_SVR]: update tablet table store finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn),
    K(update_reason), K(check_lease), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::do_update_tablet_table_store_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObMetaUpdateReason update_reason,
    ObUpdateTableStoreParam &param,
    const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
    const bool check_lease)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  META_SRV_PERF_TIMEGUARD_INIT();
  uint64_t data_version = 0;
  ObArenaAllocator allocator("UpldTblMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTablet *old_ss_tablet = nullptr;
  ObTabletHandle old_ss_tablet_hdl;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(param));
  } else {
    // 1. lock cache
    ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());

    // 2. start-op
    ObSSMetaAtomicTabletOp tablet_meta_op;
    int64_t op_id = -1;

    ObTabletAtomicProtocol tablet_protocol;
    ObTabletAtomicProtocolParam tablet_protocol_param;
    if (OB_FAIL(build_tablet_protocol_init_param_(ls_id, tablet_id, reorgan_scn, meta_ctrl_keys, tablet_protocol_param))) {
      LOG_WARN("fail to build tablet protocol param", KR(ret));
    } else if (OB_FAIL(tablet_protocol.init(tablet_protocol_param))) {
      LOG_WARN("fail to init tablet protocol", KR(ret), K(tablet_protocol_param));
    } else if (CLICK_FAIL(tablet_protocol.create(check_lease /*need_check_sswriter*/))) {
      LOG_WARN("fail to create tablet protocol", KR(ret), K(check_lease));
    } else if (CLICK_FAIL(tablet_protocol.get_op_id(op_id))) {
      LOG_WARN("fail to get op_id", KR(ret));
    } else {
      ObSSMetaAtomicOpInitParam tablet_op_param;
      build_tablet_op_init_param_(op_id, ls_id, tablet_id, reorgan_scn, tablet_op_param);
      if (CLICK_FAIL(tablet_meta_op.init(tablet_op_param))) {
        LOG_WARN("fail to init tablet meta_op", KR(ret), K(tablet_op_param));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      // must have ss tablet first and then update.
      bool can_generate_new_tablet = true;
      bool can_gc_sstable = true;
      // Whether to persist a new Shared tablet version.
      bool need_update = true;
      ObSSTabletHdlDesFunc deserialize_func(ls_id, tablet_id, reorgan_scn, op_id, &allocator);
      if (CLICK_FAIL(tablet_meta_op.read_meta_info<ObTabletHandle>(old_ss_tablet_hdl, deserialize_func))) {
        if (OB_OBJECT_NOT_EXIST == ret) {
          ret = OB_TABLET_NOT_EXIST;
        } else {
          LOG_WARN("fail to get shared tablet", KR(ret), K(ls_id), K(tablet_id), K(op_id));
        }
      } else if (OB_ISNULL(old_ss_tablet = old_ss_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old_ss_tablet is nullptr", KR(ret), K(ls_id), K(tablet_id));
      } else if (OB_NOT_NULL(param.sstable_)
          && param.sstable_->is_inc_major_ddl_dump_sstable()
          && param.sstable_->get_end_scn() <= old_ss_tablet->get_ddl_checkpoint_scn()) {
        // Recheck the uploader's coverage condition against the latest Shared metadata.
        need_update = false;
        LOG_INFO("skip inc major dump covered by shared DDL checkpoint", K(ls_id), K(tablet_id),
                 "dump_end_scn", param.sstable_->get_end_scn(),
                 "ddl_checkpoint_scn", old_ss_tablet->get_ddl_checkpoint_scn());
      } else if (OB_NOT_NULL(param.sstable_)
          && param.sstable_->is_inc_major_type_sstable()
          && ObIncMajorSSTableAction::ADD == param.inc_major_sstable_action_
          && param.ddl_info_.need_check_ddl_checkpoint_
          && old_ss_tablet->get_ddl_checkpoint_scn() >= param.ddl_info_.ddl_checkpoint_scn_) {
        // END replay supplies the actual log SCN, so exact coverage is sufficient.
        need_update = false;
        LOG_INFO("skip completed dli end replay in atomic update", K(ls_id), K(tablet_id),
            "end_scn", param.ddl_info_.ddl_checkpoint_scn_,
            "shared_ddl_checkpoint_scn", old_ss_tablet->get_ddl_checkpoint_scn());
      } else if (need_update) {
        // Split and transfer constrain new versions, not an already completed END.
        if (OB_FAIL(compaction::ObSSTabletMergeHelper::check_can_generate_new_tablet(
            ls_id, old_ss_tablet_hdl/*shared_tablet_hdl*/, update_reason, true/*need_check_split*/,
            can_generate_new_tablet, can_gc_sstable))) {
          LOG_WARN("fail to check can_generate_new_tablet", KR(ret), K(old_ss_tablet_hdl));
        } else if (!can_generate_new_tablet) {
          ret = OB_OP_NOT_ALLOW;
          LOG_WARN("disallowed to generate a new version", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
        } else if (OB_FAIL(old_ss_tablet->fetch_table_store_with_prewarm(table_store_wrapper))) {
          LOG_WARN("fail to fetch table store with sstable meta prewarm", KR(ret), K(ls_id), K(tablet_id));
        }
      }

      // for minor merge: verify all input sstables still exist in table-store, abort if any was GC'd
      // minor merge是'read-modify-write'流程，避免从调度到执行的时间内，有其他并发操作将输入sstable GC掉了;
      // 同时，也会检查sstable的filled_tx_scn是否一致，避免sstable元数据被修改
      if (OB_SUCC(ret) && compaction::ObMergeType::MINOR_MERGE == param.compaction_info_.merge_type_) {
        if (ObMetaUpdateReason::TABLET_UPLOAD_COMPACT_ADD_DATA_MINOR_SSTABLE == update_reason) {
        // this type of minor is compaction of local sstables
        } else if (OB_ISNULL(param.input_tables_handle_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("input tables handle is nullptr", KR(ret), K(ls_id), K(tablet_id));
        } else if (CLICK_FAIL(compaction::check_minor_merge_input_sstables_exist(*old_ss_tablet, *param.input_tables_handle_))) {
          LOG_WARN("fail to check minor merge input sstables exist", KR(ret), K(ls_id), K(tablet_id));
        } else if (OB_FALSE_IT(CLICK())) {
        }
      }

      // 对尚未确定upper_trans_version的mini/minor sstables，查询其最大事务版本，收集到upper_trans_list中
      ObSEArray<UpdateUpperTransParam::SCNAndVersion, 4> upper_trans_list;
      upper_trans_list.set_attr(ObMemAttr(MTL_ID(), "SSCaclUppVer"));
      // 如果sstable不允许被gc，回填upper_trans_version就没有收益；且查询事务模块是有实际开销的
      if (OB_SUCC(ret) && need_update && can_gc_sstable) {
        if (CLICK_TMP_FAIL(build_upper_trans_version_update_info_v2_(ls_id, *old_ss_tablet, upper_trans_list))) {
          LOG_WARN("build upper trans version param fail", K(tmp_ret), K(ls_id), K(tablet_id));
        } else if (OB_FALSE_IT(CLICK())) {
        } else if (!upper_trans_list.empty()) {
          param.upper_trans_param_.ss_new_upper_trans_ = &upper_trans_list;
        }
      }
      DEFER(param.upper_trans_param_.ss_new_upper_trans_ = NULL;);

      // 4. create tmp new tablet
      SCN flush_scn = param.compaction_info_.clog_checkpoint_scn_;
      ObTablet *tmp_tablet = nullptr;
      ObTabletHandle tmp_tablet_hdl;
      if (OB_FAIL(ret)) {
      } else if (!need_update) {
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(ObTabletMapKey(ls_id, tablet_id), allocator, tmp_tablet_hdl))) { // todo feidu inc_shared_storage : tablet maybe not exist in this server
        LOG_WARN("fail to acquire temporary tablet", KR(ret), K(ls_id), K(tablet_id));
      } else if (OB_ISNULL(tmp_tablet = tmp_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet_hdl", KR(ret), K(ls_id), K(tablet_id), K(tmp_tablet_hdl), KPC(tmp_tablet));
      } else if (CLICK_FAIL(tmp_tablet->init_for_merge(allocator, param, *old_ss_tablet))) {
        LOG_WARN("fail to init tablet", KR(ret), K(param), KPC(old_ss_tablet), K(flush_scn));
      }

      // 5. persist new tablet
      if (OB_FAIL(ret)) {
      } else if (!need_update) {
        // Release the empty atomic operation without creating a tablet version.
        if (OB_FAIL(tablet_protocol.abort_self())) {
          LOG_WARN("failed to finish covered inc major update", KR(ret), K(ls_id), K(tablet_id));
        }
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("fail to get min data version", KR(ret));
      } else if (CLICK_FAIL(persist_tablet_(data_version, tmp_tablet, update_reason, &tablet_meta_op,
                param.ddl_info_.ddl_redo_callback_, param.ddl_info_.ddl_finish_callback_))) {
        LOG_WARN("fail to persist tablet", KR(ret), KPC(tmp_tablet));
      } else if (CLICK_FAIL(tablet_protocol.finish(tablet_meta_op))) {
        LOG_WARN("fail to finish tablet_protocol", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }

    if (OB_FAIL(ret)) {
      if (need_retry_(ret) && OB_TMP_FAIL(tablet_protocol.abort_self())) {
        LOG_WARN("fail to abort tablet protocol itself for retry", KR(ret), KR(tmp_ret));
      } else if (!need_retry_(ret) && OB_TMP_FAIL(tablet_protocol.abort())) {
        LOG_WARN("fail to abort tablet protocol", KR(ret), KR(tmp_ret));
      }
    }
  }
  return ret;
}

int ObSSMetaService::replace_tablet_minor_sstable(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObMetaUpdateReason update_reason,
    ObUpdateTableStoreParam &param,
    const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
    const bool check_lease)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  PERF_GUARD_INIT(perf_replace_tablet_minor_sstable);
  ReplaceTabletMinorSstableRetryOp replace_op(ret, *this, ls_id, tablet_id, reorgan_scn, update_reason, param,
      meta_ctrl_keys, check_lease);
  do_with_retry_("replace_tablet_minor_sstable", replace_op);
  LOG_INFO("[SS_META_SVR]: replace tablet minor sstable finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn),
    K(update_reason), K(check_lease), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::do_replace_tablet_minor_sstable_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObMetaUpdateReason update_reason,
    ObUpdateTableStoreParam &param,
    const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
    const bool check_lease)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  META_SRV_PERF_TIMEGUARD_INIT();
  uint64_t data_version = 0;
  ObArenaAllocator allocator("RepTblMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTablet *old_ss_tablet = nullptr;
  ObTabletHandle old_ss_tablet_hdl;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(param));
  } else if (OB_UNLIKELY(!param.is_ss_replace_minor_sstable_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("minor sstable replace param flag is not set", KR(ret), K(ls_id), K(tablet_id), K(param));
  } else if (OB_UNLIKELY(ObMetaUpdateReason::TABLET_UPLOAD_REPLACE_MINOR_SSTABLE != update_reason
                      && ObMetaUpdateReason::TABLET_TRANSFER_REPLACE_MINOR_SSTABLE != update_reason)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected update reason for minor sstable replace", KR(ret), K(ls_id), K(tablet_id), K(update_reason));
  } else if (OB_UNLIKELY(nullptr == param.sstable_
                      || ObITable::TableType::MINOR_SSTABLE != param.sstable_->get_key().table_type_
                      || !param.sstable_->get_key().scn_range_.is_valid()
                      || param.sstable_->get_key().scn_range_.is_empty())) {
    // only accept one data minor sstable, nothing else
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("minor sstable replace expects one data minor sstable", KR(ret), K(ls_id), K(tablet_id), KPC(param.sstable_));
  } else if (OB_UNLIKELY((!compaction::is_mini_merge(param.compaction_info_.merge_type_)
                       && !compaction::is_minor_merge(param.compaction_info_.merge_type_))
                       || param.ddl_info_.update_with_major_flag_
                       || !param.ddl_info_.slice_sstables_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected param payload for minor sstable replace", KR(ret), K(ls_id), K(tablet_id), K(param));
  } else {
    // 1. lock cache
    ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());

    // 2. start-op
    ObSSMetaAtomicTabletOp tablet_meta_op;
    int64_t op_id = -1;

    ObTabletAtomicProtocol tablet_protocol;
    ObTabletAtomicProtocolParam tablet_protocol_param;
    if (OB_FAIL(build_tablet_protocol_init_param_(ls_id, tablet_id, reorgan_scn, meta_ctrl_keys, tablet_protocol_param))) {
      LOG_WARN("fail to build tablet protocol param", KR(ret));
    } else if (OB_FAIL(tablet_protocol.init(tablet_protocol_param))) {
      LOG_WARN("fail to init tablet protocol", KR(ret), K(tablet_protocol_param));
    } else if (CLICK_FAIL(tablet_protocol.create(check_lease /*need_check_sswriter*/))) {
      LOG_WARN("fail to create tablet protocol", KR(ret), K(check_lease));
    } else if (CLICK_FAIL(tablet_protocol.get_op_id(op_id))) {
      LOG_WARN("fail to get op_id", KR(ret));
    } else {
      ObSSMetaAtomicOpInitParam tablet_op_param;
      build_tablet_op_init_param_(op_id, ls_id, tablet_id, reorgan_scn, tablet_op_param);
      if (CLICK_FAIL(tablet_meta_op.init(tablet_op_param))) {
        LOG_WARN("fail to init tablet meta_op", KR(ret), K(tablet_op_param));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      // 3. get the old_ss_tablet
      // must have ss tablet first and then update.
      bool can_generate_new_tablet = true;
      bool unused_can_gc_sstable = true;
      ObSSTabletHdlDesFunc deserialize_func(ls_id, tablet_id, reorgan_scn, op_id, &allocator);
      if (CLICK_FAIL(tablet_meta_op.read_meta_info<ObTabletHandle>(old_ss_tablet_hdl, deserialize_func))) {
        if (OB_OBJECT_NOT_EXIST == ret) {
          ret = OB_TABLET_NOT_EXIST;
        } else {
          LOG_WARN("fail to get shared tablet", KR(ret), K(ls_id), K(tablet_id), K(op_id));
        }
      } else if (OB_ISNULL(old_ss_tablet = old_ss_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old_ss_tablet is nullptr", KR(ret), K(ls_id), K(tablet_id));
        // split和transfer会限制生成新版本与回收sstable
      } else if (OB_FAIL(compaction::ObSSTabletMergeHelper::check_can_generate_new_tablet(
                ls_id, old_ss_tablet_hdl/*shared_tablet_hdl*/, update_reason, true/*need_check_split*/,
                can_generate_new_tablet, unused_can_gc_sstable))) {
        LOG_WARN("fail to check can_generate_new_tablet", KR(ret), K(old_ss_tablet_hdl));
      } else if (!can_generate_new_tablet) {
        // no need to check can_gc_sstable: it only gates the upper_trans backfill which drives
        // recycling old minors in place, while the replace discards all of them anyway.
        // transfer replace may be scheduled on the transfer src tablet during upload.
        ret = OB_OP_NOT_ALLOW;
        LOG_WARN("disallowed to replace minor sstable", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn),
          K(can_generate_new_tablet));
      } else if (OB_FAIL(check_can_replace_minor_sstable_(*old_ss_tablet, param))) {
        LOG_WARN("fail to check can replace minor sstable", KR(ret), K(ls_id), K(tablet_id), K(param));
      }

      // 4. create tmp new tablet
      ObTablet *tmp_tablet = nullptr;
      ObTabletHandle tmp_tablet_hdl;
      if (OB_FAIL(ret)) {
      } else if (CLICK_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(ObTabletMapKey(ls_id, tablet_id), allocator, tmp_tablet_hdl))) {
        LOG_WARN("fail to acquire temporary tablet", KR(ret), K(ls_id), K(tablet_id));
      } else if (OB_ISNULL(tmp_tablet = tmp_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet_hdl", KR(ret), K(ls_id), K(tablet_id), K(tmp_tablet_hdl), KPC(tmp_tablet));
      } else if (CLICK_FAIL(tmp_tablet->init_for_merge(allocator, param, *old_ss_tablet))) {
        LOG_WARN("fail to init tablet", KR(ret), K(param), KPC(old_ss_tablet));
      }

      // 5. persist new tablet
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("fail to get min data version", KR(ret));
      } else if (CLICK_FAIL(persist_tablet_(data_version, tmp_tablet, update_reason, &tablet_meta_op,
                nullptr/*ddl_redo_callback*/, nullptr/*ddl_finish_callback*/))) {
        LOG_WARN("fail to persist tablet", KR(ret), KPC(tmp_tablet));
      } else if (CLICK_FAIL(tablet_protocol.finish(tablet_meta_op))) {
        LOG_WARN("fail to finish tablet_protocol", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }

    if (OB_FAIL(ret)) {
      if (need_retry_(ret) && OB_TMP_FAIL(tablet_protocol.abort_self())) {
        LOG_WARN("fail to abort tablet protocol itself for retry", KR(ret), KR(tmp_ret));
      } else if (!need_retry_(ret) && OB_TMP_FAIL(tablet_protocol.abort())) {
        LOG_WARN("fail to abort tablet protocol", KR(ret), KR(tmp_ret));
      }
    }
  }
  return ret;
}

// the new minor sstable must cover all the existing data mini/minor sstables:
// 1. its end_scn must be bigger than all the existing ones (otherwise the data of
//    (new_end, old_end] would be lost after the replace);
// 2. its start_scn is free to be bigger or smaller than the existing ones, but must
//    not leave a scn gap above the existing range;
// 3. its recycle_version must be covered by the multi_version_start of the new tablet
//    (in shared storage mode, recycle_version is always 0, so this check is only defensive)
int ObSSMetaService::check_can_replace_minor_sstable_(
    const ObTablet &old_tablet,
    const ObUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  const ObTabletID &tablet_id = old_tablet.get_tablet_id();
  const ObSSTable *new_minor = param.sstable_;
  ObSSTableMetaHandle sst_meta_hdl;
  if (OB_FAIL(old_tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store for minor replace check", KR(ret), K(tablet_id));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("fail to get table store", KR(ret), K(tablet_id));
  } else if (OB_ISNULL(table_store)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table store is nullptr", KR(ret), K(tablet_id));
  } else {
    const ObSSTableArray &old_minors = table_store->get_minor_sstables();
    SCN right_boundary;
    if (old_minors.empty()) { // minor sstables are recycled by major compaction 
      right_boundary = old_tablet.get_clog_checkpoint_scn();
    } else {
      const ObITable *last_minor = old_minors.get_boundary_table(true/*last*/);
      right_boundary = last_minor->get_end_scn();
    }
    if (new_minor->get_end_scn() <= right_boundary) {
      ret = OB_NO_NEED_MERGE;
      LOG_WARN("new minor's end_scn is not bigger than the right boundary",
          KR(ret), K(tablet_id), KPC(new_minor), K(right_boundary));
    } else if (new_minor->get_start_scn() > right_boundary) {
      ret = OB_MINOR_SSTABLE_RANGE_CROSS;
      LOG_WARN("scn gap between the right boundary and the new minor",
          KR(ret), K(tablet_id), KPC(new_minor), K(right_boundary));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(new_minor->get_meta(sst_meta_hdl))) {
    LOG_WARN("fail to get new minor sstable meta", KR(ret), K(tablet_id), KPC(new_minor));
  } else {
    // same with ObTabletMeta::init for merge
    const int64_t new_snapshot_version = MAX(param.snapshot_version_, old_tablet.get_snapshot_version());
    const int64_t new_multi_version_start = MIN(MAX(param.multi_version_start_, old_tablet.get_multi_version_start()),
                                                new_snapshot_version);
    // shared storage mode recycle scn is always 0, so this check is only defensive
    const int64_t recycle_version = sst_meta_hdl.get_sstable_meta().get_recycle_version();
    if (recycle_version > new_multi_version_start) {
      ret = OB_OP_NOT_ALLOW;
      LOG_WARN("new minor has recycled multi version data above the multi_version_start of the new tablet",
          KR(ret), K(tablet_id), K(recycle_version), K(new_multi_version_start), KPC(new_minor));
    }
  }
  return ret;
}

int ObSSMetaService::build_tablet_with_batch_tables(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObMetaUpdateReason update_reason,
    const ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
    const ObBatchUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  META_SRV_PERF_TIMEGUARD_INIT();
  PERF_GUARD_INIT(perf_build_tablet_with_batch_tables);
  ObArenaAllocator allocator("UpldTblMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTablet *old_ss_tablet = nullptr;
  ObTabletHandle old_ss_tablet_hdl;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  // FIXME, transfer/migration need to use the correct merge type.
  const compaction::ObMergeType &merge_type = param.tablet_split_param_.merge_type_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(param));
  } else {
    // 1. lock cache
    ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());

    // 2. start-op
    ObSSMetaAtomicTabletOp tablet_meta_op;
    int64_t op_id = -1;

    ObTabletAtomicProtocol tablet_protocol;
    ObTabletAtomicProtocolParam tablet_protocol_param;
    if (OB_FAIL(build_tablet_protocol_init_param_(ls_id, tablet_id, reorgan_scn, meta_ctrl_keys, tablet_protocol_param))) {
      LOG_WARN("fail to build tablet protocol param", KR(ret));
    } else if (OB_FAIL(tablet_protocol.init(tablet_protocol_param))) {
      LOG_WARN("fail to init tablet protocol", KR(ret), K(tablet_protocol_param));
    } else if (CLICK_FAIL(tablet_protocol.create(true /*need_check_sswriter*/))) {
      LOG_WARN("fail to create tablet protocol", KR(ret));
    } else if (CLICK_FAIL(tablet_protocol.get_op_id(op_id))) {
      LOG_WARN("fail to get op_id", KR(ret));
    } else {
      ObSSMetaAtomicOpInitParam tablet_op_param;
      build_tablet_op_init_param_(op_id, ls_id, tablet_id, reorgan_scn, tablet_op_param);
      if (CLICK_FAIL(tablet_meta_op.init(tablet_op_param))) {
        LOG_WARN("fail to init tablet meta_op", KR(ret), K(tablet_op_param));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      // 3. get the old_ss_tablet
      // must have ss tablet first and then update.
      ObSSTabletHdlDesFunc deserialize_func(ls_id, tablet_id, reorgan_scn, op_id, &allocator);
      if (CLICK_FAIL(tablet_meta_op.read_meta_info<ObTabletHandle>(old_ss_tablet_hdl, deserialize_func))) {
        if (OB_OBJECT_NOT_EXIST == ret) {
          ret = OB_TABLET_NOT_EXIST;
        } else {
          LOG_WARN("fail to get shared tablet", KR(ret), K(ls_id), K(tablet_id), K(op_id));
        }
      } else if (OB_ISNULL(old_ss_tablet = old_ss_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old_ss_tablet is nullptr", KR(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(old_ss_tablet->fetch_table_store_with_prewarm(table_store_wrapper))) {
        LOG_WARN("fail to fetch table store with sstable meta prewarm", KR(ret), K(ls_id), K(tablet_id));
      }

      // 4. create tmp new tablet
      ObTablet *tmp_tablet = nullptr;
      ObTabletHandle tmp_tablet_hdl;
      uint64_t data_version = 0;
      if (CLICK_FAIL(ret)) {
      } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(ObTabletMapKey(ls_id, tablet_id), allocator, tmp_tablet_hdl))) { // todo feidu inc_shared_storage : tablet maybe not exist in this server
        LOG_WARN("fail to acquire temporary tablet", KR(ret), K(ls_id), K(tablet_id));
      } else if (OB_ISNULL(tmp_tablet = tmp_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet_hdl", KR(ret), K(ls_id), K(tablet_id), K(tmp_tablet_hdl), KPC(tmp_tablet));
      } else if (OB_FAIL(tmp_tablet->init_for_sstable_replace(allocator, param, *old_ss_tablet))) {
        LOG_WARN("fail to init tablet", KR(ret), K(param), KPC(old_ss_tablet));
      }

      // 5. persist new tablet
      if (CLICK_FAIL(ret)) {
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("fail to get min data version", KR(ret));
      } else if (CLICK_FAIL(persist_tablet_(data_version,
                                            tmp_tablet,
                                            update_reason,
                                            &tablet_meta_op,
                                            nullptr/*ddl_redo_callback*/,
                                            nullptr/*ddl_finish_callback*/,
                                            param.src_tablet_block_info_))) {
        LOG_WARN("fail to persist tablet", KR(ret), KPC(tmp_tablet));
      } else if (CLICK_FAIL(tablet_protocol.finish(tablet_meta_op))) {
        LOG_WARN("fail to finish tablet_protocol", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      } 
      
    }
    if (OB_FAIL(ret) && OB_TMP_FAIL(tablet_protocol.abort())) {
      LOG_WARN("fail to abort tablet protocol", KR(ret), KR(tmp_ret));
    }
  }
  LOG_INFO("[SS_META_SVR]: build tablet with batch tables finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn),
    K(update_reason), K(param), K(meta_ctrl_keys), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::gen_initial_ss_checkpoint_scn_(
    const ObSSLSCreateMode create_mode,
    const SCN &local_checkpoint_scn,
    const SCN &ss_rec_scn,
    SCN &initial_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  initial_checkpoint_scn.set_invalid();
  if (OB_UNLIKELY(!local_checkpoint_scn.is_valid()
      || (ObSSLSCreateMode::NORMAL_CREATE != create_mode
          && ObSSLSCreateMode::BOOTSTRAP_CREATE != create_mode)
      || (ObSSLSCreateMode::BOOTSTRAP_CREATE == create_mode
          && !ss_rec_scn.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(create_mode),
             K(local_checkpoint_scn), K(ss_rec_scn));
  } else if (ObSSLSCreateMode::NORMAL_CREATE == create_mode) {
    initial_checkpoint_scn = local_checkpoint_scn;
  } else {
    initial_checkpoint_scn = SCN::max(ss_rec_scn, SCN::base_scn());
    if (OB_UNLIKELY(initial_checkpoint_scn > local_checkpoint_scn)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bootstrap shared checkpoint is ahead of local checkpoint",
               KR(ret), K(initial_checkpoint_scn), K(local_checkpoint_scn),
               K(ss_rec_scn));
      initial_checkpoint_scn.set_invalid();
    }
  }
  return ret;
}

int ObSSMetaService::create_ls_(ObLS *ls, const ObSSLSCreateMode create_mode)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  META_SRV_PERF_TIMEGUARD_INIT();
  const ObLSID ls_id = ls->get_ls_id();
  ObLSMeta local_ls_meta;
  SCN local_checkpoint_scn;
  SCN ss_rec_scn;
  SCN init_ckpt_scn;
  palf::LSN init_ckpt_lsn;
  palf::PalfBaseInfo palf_meta;
  ObSSMetaHeadValueInfo meta_head_info;
  SMART_VAR(ObSSLSMetaValue, ls_meta_v) {
    // During bootstrap, local inner tablets and the local LS checkpoint may
    // have advanced before shared metadata is first created. The first-created
    // shared tablets deliberately omit local mini/minor SSTables, so their
    // effects must remain replayable from the aggregated shared recovery point.
    if (ObSSLSCreateMode::BOOTSTRAP_CREATE == create_mode
        && OB_FAIL(ls->get_ss_checkpoint_executor().get_ss_rec_scn(ss_rec_scn))) {
      LOG_WARN("fail to get bootstrap shared recovery scn", KR(ret), K(ls_id));
    } else if (FALSE_IT(local_ls_meta = ls->get_ls_meta())) {
    } else if (FALSE_IT(local_checkpoint_scn = local_ls_meta.get_clog_checkpoint_scn())) {
    } else if (ObSSLSCreateMode::NORMAL_CREATE == create_mode
               && FALSE_IT(ss_rec_scn = local_checkpoint_scn)) {
    } else if (OB_FAIL(gen_initial_ss_checkpoint_scn_(create_mode, local_checkpoint_scn,
               ss_rec_scn, init_ckpt_scn))) {
      LOG_WARN("fail to gen initial shared checkpoint scn", KR(ret), K(ls_id), K(create_mode),
        K(local_checkpoint_scn), K(ss_rec_scn));
    } else if (init_ckpt_scn == local_checkpoint_scn) {
      init_ckpt_lsn = local_ls_meta.get_clog_base_lsn();
    } else if (ObSSLSCreateMode::BOOTSTRAP_CREATE == create_mode
               && init_ckpt_scn.is_base_scn()) {
      const palf::LSN palf_initial_lsn(palf::PALF_INITIAL_LSN_VAL);
      palf::LSN begin_lsn;
      if (OB_FAIL(ls->get_log_handler()->get_begin_lsn(begin_lsn))) {
        LOG_WARN("fail to get palf begin lsn for initial shared checkpoint",
          KR(ret), K(ls_id), K(init_ckpt_scn));
      } else if (OB_UNLIKELY(begin_lsn != palf_initial_lsn)) {
        ret = OB_ERR_OUT_OF_LOWER_BOUND;
        LOG_WARN("palf initial log prefix is no longer available", KR(ret), K(ls_id), K(init_ckpt_scn),
          K(begin_lsn), K(palf_initial_lsn));
      } else {
        // base_scn is the logical boundary before the first physical PALF log.
        init_ckpt_lsn = palf_initial_lsn;
      }
    } else if (OB_FAIL(ls->get_log_handler()->locate_by_scn_coarsely(init_ckpt_scn, init_ckpt_lsn))) {
      LOG_WARN("fail to locate initial shared checkpoint lsn", KR(ret), K(ls_id), K(init_ckpt_scn),
        K(local_checkpoint_scn));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ls->get_palf_base_info(init_ckpt_lsn, palf_meta))) {
      LOG_WARN("fail to get initial shared palf base info", KR(ret), K(ls_id), K(init_ckpt_scn), K(init_ckpt_lsn));
    } else if (OB_UNLIKELY(ObSSLSCreateMode::BOOTSTRAP_CREATE == create_mode
               && init_ckpt_scn.is_base_scn()
               && (!palf_meta.prev_log_info_.scn_.is_valid()
                   || palf_meta.curr_lsn_ != init_ckpt_lsn
                   || palf_meta.prev_log_info_.scn_ > init_ckpt_scn))) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
      LOG_WARN("initial shared checkpoint does not match palf base info",
        KR(ret), K(ls_id), K(init_ckpt_scn), K(init_ckpt_lsn), K(palf_meta));
    } else if (OB_FAIL(ls_meta_v.meta_value_.init_for_ss(local_ls_meta))) {
      LOG_WARN("fail to init for ss ls", KR(ret), KPC(ls));
    } else if (OB_FAIL(ls_meta_v.meta_value_.set_ss_checkpoint(init_ckpt_scn, init_ckpt_lsn))) {
      LOG_WARN("fail to set initial shared checkpoint", KR(ret), K(ls_id), K(init_ckpt_scn), K(init_ckpt_lsn));
    } else if (OB_FAIL(meta_head_info.set(ObMetaUpdateReason::CREATE_LS))) {
    } else if (OB_FAIL(ls_meta_v.set_meta_head_info(meta_head_info))) {
      LOG_WARN("fail to set meta info", KR(ret), K(meta_head_info));
    } else if (OB_FAIL(ls_meta_v.meta_value_.set_palf_meta(palf_meta))) {
      LOG_WARN("fail to set palf meta", KR(ret), K(ls_meta_v));
    } else {
      FLOG_INFO("build initial shared ls recovery point", K(ls_id), K(create_mode), K(local_checkpoint_scn),
        K(ss_rec_scn), K(init_ckpt_scn), K(init_ckpt_lsn), K(palf_meta));
      ObLSAtomicProtocol ls_protocol;
      ObSSMetaAtomicLSOp ls_meta_op;
      int64_t op_id = -1;
      // 1. lock cache
      ObBucketHashWLockGuard guard(bucket_lock_, ls_id.hash());
      // 2. start-op
      if (OB_FAIL(ls_protocol.init(MTL_ID(), ls_id, false/*is_deleted*/, SCN()))) {
        LOG_WARN("fail to init ls_protocol", KR(ret), K(ls_id));
      } else if (CLICK_FAIL(ls_protocol.create(false/*check_lease*/))) {
        LOG_WARN("fail to create ls_protocol", KR(ret), K(ls_id));
      } else if (OB_FAIL(ls_protocol.get_op_id(op_id))) {
        LOG_WARN("fail to get op_id", KR(ret));
      } else {
        ObSSMetaAtomicOpInitParam ls_op_param;
        build_ls_op_init_param_(op_id, ls_id, ls_op_param);
        if (OB_FAIL(ls_meta_op.init(ls_op_param))) {
          LOG_WARN("fail to init ls meta_op", KR(ret), K(ls_op_param));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        SMART_VAR(ObSSLSMetaValue, tmp_meta_value) {
          if (CLICK_FAIL(ls_meta_op.read_meta_info(tmp_meta_value))) {
            if (OB_OBJECT_NOT_EXIST != ret) {
              LOG_WARN("fail to get ls from ss", KR(ret), K(ls_id));
            }
          } else {
            CLICK();
            ret = OB_ENTRY_EXIST; // thus to execute abort()
          }
        }
        // only create while there is no other has created it.
        if (ret == OB_OBJECT_NOT_EXIST) {
          ret = OB_SUCCESS;
          // 3. persist ls
          if (CLICK_FAIL(ls_meta_op.write_meta_info(ls_meta_v))) {
            LOG_WARN("fail to write ss ls_meta", KR(ret), K(ls_meta_v));
          } else if (CLICK_FAIL(ls_protocol.finish(ls_meta_op))) {
            LOG_WARN("fail to finish ls_protocol", KR(ret));
          } else if (OB_FALSE_IT(CLICK())) {
          }
        }
      }
      if (OB_FAIL(ret) && OB_TMP_FAIL(ls_protocol.abort())) {
        LOG_WARN("fail to abort ls_protocol", KR(ret), KR(tmp_ret));
      }
    }
  }

  // only handle entry_exist and key_dup errno, otherwise return original ret
  ret = resolve_create_ret_(ret, ObSSMetaTypeEnum::LS_META,
                            ls_id, ObTabletID(), SCN::invalid_scn());
  return ret;
}

#ifdef ERRSIM
static int check_shared_meta_location_ready_before_create_inner_tablet(const ObLSID &target_ls_id)
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = MTL_ID();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  static const char *SHARED_META_TABLE_NAMES[] = {
      OB_ALL_SHARED_META_CONTROL_TNAME,
      OB_ALL_SHARED_TIERED_META_TNAME,
      OB_ALL_SHARED_META_TNAME};
  schema::ObSchemaGetterGuard schema_guard;
  int64_t checked_tablet_count = 0;

  if (OB_UNLIKELY(!target_ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target ls id", KR(ret), K(target_ls_id));
  } else if (OB_ISNULL(GCTX.schema_service_) || OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema service or location service is null", KR(ret),
        KP(GCTX.schema_service_), KP(GCTX.location_service_));
  } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                 meta_tenant_id, schema_guard))) {
    LOG_WARN("failed to get meta tenant schema guard", KR(ret),
        K(user_tenant_id), K(meta_tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(SHARED_META_TABLE_NAMES); ++i) {
      const char *table_name = SHARED_META_TABLE_NAMES[i];
      const schema::ObTableSchema *table_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(
              meta_tenant_id,
              ObString::make_string("oceanbase"),
              ObString::make_string(table_name),
              false /*is_index*/,
              table_schema))) {
        LOG_WARN("failed to get shared meta table schema", KR(ret),
            K(meta_tenant_id), K(table_name));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("shared meta table schema does not exist", KR(ret),
            K(meta_tenant_id), K(table_name));
      } else if (OB_UNLIKELY(PARTITION_LEVEL_ONE != table_schema->get_part_level()
                 || !table_schema->is_hash_part())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared meta table has unexpected partition schema", KR(ret),
            K(meta_tenant_id), K(table_name), KPC(table_schema));
      } else {
        ObObj ls_id_obj;
        ObObjectID part_id = OB_INVALID_ID;
        ObTabletID tablet_id;
        ObLSID tablet_host_ls_id;
        ObAddr leader;
        ls_id_obj.set_int(target_ls_id.id());
        ObNewRow partition_row(&ls_id_obj, 1);
        if (OB_FAIL(schema::ObPartitionUtils::get_tablet_and_part_id(
                *table_schema, partition_row, tablet_id, part_id))) {
          LOG_WARN("failed to get shared meta tablet id by ls id", KR(ret),
              K(meta_tenant_id), K(table_name), K(target_ls_id));
        } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid shared meta tablet id calculated by ls id", KR(ret),
              K(meta_tenant_id), K(table_name), K(target_ls_id), K(part_id));
        } else if (OB_FAIL(GCTX.location_service_->nonblock_get(
                       meta_tenant_id, tablet_id, tablet_host_ls_id))) {
          LOG_WARN("shared meta tablet to ls cache is not ready", KR(ret),
              K(user_tenant_id), K(meta_tenant_id), K(table_name), K(target_ls_id), K(tablet_id));
        } else if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(
                       GCONF.cluster_id, meta_tenant_id, tablet_host_ls_id, leader))) {
          LOG_WARN("shared meta ls leader cache is not ready", KR(ret),
              K(user_tenant_id), K(meta_tenant_id), K(table_name), K(target_ls_id),
              K(tablet_id), K(tablet_host_ls_id));
        } else if (OB_UNLIKELY(!leader.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("shared meta ls leader is invalid", KR(ret),
              K(user_tenant_id), K(meta_tenant_id), K(table_name), K(target_ls_id),
              K(tablet_id), K(tablet_host_ls_id), K(leader));
        } else {
          ++checked_tablet_count;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("[ERRSIM] shared meta location cache ready before create ls inner tablet",
        K(user_tenant_id), K(meta_tenant_id), K(GCTX.self_addr()),
        K(target_ls_id), K(checked_tablet_count));
  }
  return ret;
}
#endif

int ObSSMetaService::create_ls_inner_tablet_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  static constexpr int64_t MAX_TABLET_CNT = 5;
  ObTabletID inner_tablet_ids[MAX_TABLET_CNT] = {
                                   LS_TX_CTX_TABLET,
                                   LS_TX_DATA_TABLET,
                                   LS_LOCK_TABLET,
                                   LS_REORG_INFO_TABLET,
                                   LS_MDS_TABLET};
#ifdef ERRSIM
  if (is_user_tenant(MTL_ID())
      && OB_UNLIKELY(OB_SUCCESS != EN_CHECK_SHARED_META_LOCATION_READY)
      && OB_FAIL(check_shared_meta_location_ready_before_create_inner_tablet(ls->get_ls_id()))) {
    LOG_WARN("shared meta location cache is not ready before create ls inner tablet", KR(ret),
        "user_tenant_id", MTL_ID(), "meta_tenant_id", gen_meta_tenant_id(MTL_ID()));
  }
#endif
  for (int64_t i = 0; OB_SUCC(ret) && i < MAX_TABLET_CNT; i++) {
    const ObTabletID tablet_id = inner_tablet_ids[i];
    ObTablet *tablet = nullptr;
    tablet_handle.reset();
    if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle))) {
      LOG_WARN("fail to get tablet", KR(ret), K(tablet_id));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inner tablet is null", KR(ret), K(tablet_id));
    } else if (OB_FAIL(create_tablet_(tablet->get_ls_id(), tablet_id,
               tablet->get_reorganization_scn(), tablet_handle, false /*check_lease*/))) {
      LOG_WARN("fail to create shared inner tablet", KR(ret), K(tablet_id),
        "reorganization_scn", tablet->get_reorganization_scn());
    }
  }

  return ret;
}

int ObSSMetaService::persist_tablet_(
    const uint64_t data_version,
    const ObTablet *tablet,
    const ObMetaUpdateReason update_reason,
    ObSSMetaAtomicTabletOp *tablet_op,
    ObIMacroBlockFlushCallback *ddl_redo_callback,
    ObIMacroBlockFlushCallback *ddl_finish_callback,
    const ObSSTransferSrcTabletBlockInfo *src_tablet_block_info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet) || OB_ISNULL(tablet_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(tablet), KP(tablet_op));
  } else if (ObMetaUpdateReason::TABLET_BACKFILL_REPLACE_ADD_SSTABLES == update_reason
             && (OB_ISNULL(src_tablet_block_info) || !src_tablet_block_info->is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("src_tablet_block_info must been provided if update reason is certain one",
      KR(ret), K(update_reason), KP(src_tablet_block_info));
  } else {
    const ObTabletID tablet_id = tablet->get_tablet_id();
    const ObLSID ls_id = tablet->get_ls_id();
    const ObTabletPersisterParam persist_param(data_version, ls_id, tablet_id, update_reason,
                                               0/*curr_macro_seq*/, tablet_op,
                                               ddl_redo_callback, ddl_finish_callback,
                                               tablet->get_reorganization_scn(),
                                               src_tablet_block_info);
    if (OB_FAIL(ObSSTabletPersister::persist_tablet(persist_param, *tablet))) {
      LOG_WARN("fail to persist new_ss_tablet", KR(ret), K(persist_param), KPC(tablet));
    }
  }
  return ret;
}

int ObSSMetaService::update_tablet_meta(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObTabletMeta &tablet_meta)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  META_SRV_PERF_TIMEGUARD_INIT();
  PERF_GUARD_INIT(perf_update_tablet_meta);
  ObArenaAllocator allocator("UptTblMeta", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTablet *old_ss_tablet = nullptr;
  ObTabletHandle old_ss_tablet_hdl;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid()
                         || !reorgan_scn.is_valid() || !tablet_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ls_id), K(tablet_id), K(reorgan_scn), K(tablet_meta));
  } else {
    // 1. lock for cache
    ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());
    // 2. start-op
    ObSSMetaAtomicTabletOp tablet_meta_op;
    int64_t op_id = -1;

    ObTabletAtomicProtocol tablet_protocol;
    ObTabletAtomicProtocolParam tablet_protocol_param;
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    if (OB_FAIL(build_tablet_protocol_init_param_(ls_id, tablet_id, reorgan_scn, meta_ctrl_keys, tablet_protocol_param))) {
      LOG_WARN("fail to build tablet protocol param", KR(ret));
    } else if (OB_FAIL(tablet_protocol.init(tablet_protocol_param))) {
      LOG_WARN("fail to init tablet protocol", KR(ret), K(tablet_protocol_param));
    } else if (CLICK_FAIL(tablet_protocol.create(true /*need_check_sswriter*/))) {
      LOG_WARN("fail to create tablet protocol", KR(ret));
    } else if (CLICK_FAIL(tablet_protocol.get_op_id(op_id))) {
      LOG_WARN("fail to get op_id", KR(ret));
    } else {
      ObSSMetaAtomicOpInitParam tablet_op_param;
      build_tablet_op_init_param_(op_id, ls_id, tablet_id, reorgan_scn, tablet_op_param);
      if (CLICK_FAIL(tablet_meta_op.init(tablet_op_param))) {
        LOG_WARN("fail to init tablet meta_op", KR(ret), K(tablet_op_param));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      uint64_t data_version = 0;
      // 3. get the old_ss_tablet
      // must have ss tablet first and then update.
      if (CLICK_FAIL(get_tablet(ls_id, tablet_id, reorgan_scn, allocator, old_ss_tablet_hdl))) {
        LOG_WARN("fail to get shared tablet", KR(ret), K(ls_id), K(tablet_id));
      } else if (OB_ISNULL(old_ss_tablet = old_ss_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old_ss_tablet should not be nullptr", KR(ret), K(ls_id), K(tablet_id));
      // 4. update tablet meta and persist new tablet meta
      } else if (CLICK_FAIL(old_ss_tablet->update_ss_tablet_meta(tablet_meta))) {
        LOG_WARN("fail to update ss tablet meta", KR(ret), KPC(old_ss_tablet), K(tablet_meta));
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("fail to get min data version", KR(ret));
      } else if (CLICK_FAIL(persist_tablet_(data_version,
                                            old_ss_tablet,
                                            ObMetaUpdateReason::TABLET_META_UPDATE,
                                            &tablet_meta_op))) {
        LOG_WARN("fail to persist tablet", KR(ret), KPC(old_ss_tablet));
      } else if (CLICK_FAIL(tablet_protocol.finish(tablet_meta_op))) {
        LOG_WARN("fail to finish tablet_protocol", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }
    if (OB_FAIL(ret) && OB_TMP_FAIL(tablet_protocol.abort())) {
      LOG_WARN("fail to abort tablet protocol", KR(ret), KR(tmp_ret));
    }
  }
  LOG_INFO("[SS_META_SVR]: update tablet meta finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn),
    "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::mark_tablet_deleted(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const SCN &deleted_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  META_SRV_PERF_TIMEGUARD_INIT();
  PERF_GUARD_INIT(perf_mark_tablet_deleted);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
  } else if (OB_UNLIKELY(!tablet_id.is_ls_inner_tablet() && !reorgan_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
  } else if (OB_FALSE_IT(CLICK())) {
  } else {
    ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());

    ObSSMetaAtomicTabletOp tablet_meta_op;
    int64_t op_id = -1;

    ObTabletAtomicProtocol tablet_protocol;
    ObTabletAtomicProtocolParam tablet_protocol_param;
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    ObSSMetaReadParamV2 read_param;
    ObSSMetaOpBufferGuard meta_value_guard;
    ObSSMetaOpBuffer &meta_value = meta_value_guard.get_buffer();
    int64_t last_major_snapshot_version = -1;
    bool is_mark_delete = false;
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id, tablet_id, SCN::min_scn(), reorgan_scn, true/*is_in_table*/);
    read_param.set_read_mark_delete(true);
    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(ObSSMetaTableHelper::get_tablet_last_major_snapshot_version(
        read_param, last_major_snapshot_version, is_mark_delete, &meta_value))) {
      LOG_WARN("fail to get tablet last major snapshot version", KR(ret), K(read_param));
    } else if (is_mark_delete) {
      //do nothing
    } else if (OB_UNLIKELY(!deleted_scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid deleted_scn", KR(ret), K(deleted_scn));
    } else if (CLICK_FAIL(build_tablet_protocol_init_param_(ls_id, tablet_id, reorgan_scn, meta_ctrl_keys,
                                                            true/*is_deleted*/, deleted_scn, tablet_protocol_param))) {
      LOG_WARN("fail to build tablet protocol param", KR(ret));
    } else if (FALSE_IT(tablet_protocol_param.last_major_snapshot_ = last_major_snapshot_version)) {
    } else if (OB_FAIL(tablet_protocol.init(tablet_protocol_param))) {
      LOG_WARN("fail to init tablet protocol", KR(ret), K(tablet_protocol_param));
    } else if (CLICK_FAIL(tablet_protocol.create(false /*need_check_sswriter*/))) {
      LOG_WARN("fail to create tablet protocol", KR(ret));
    } else if (CLICK_FAIL(tablet_protocol.get_op_id(op_id))) {
      LOG_WARN("fail to get op_id", KR(ret));
    } else {
      ObSSMetaAtomicOpInitParam tablet_op_param;
      build_tablet_op_init_param_(op_id, ls_id, tablet_id, reorgan_scn, tablet_op_param);
      tablet_op_param.meta_value_ = &meta_value;
      if (OB_FAIL(tablet_meta_op.init(tablet_op_param))) {
        LOG_WARN("fail to init tablet meta_op", KR(ret), K(tablet_op_param));
      } else if (CLICK_FAIL(tablet_meta_op.handle_mark_delete(deleted_scn))) {
        LOG_WARN("fail to mark delete shared tablet", KR(ret), K(ls_id), K(tablet_id), K(deleted_scn));
      } else if (CLICK_FAIL(tablet_protocol.finish(tablet_meta_op))) {
        LOG_WARN("fail to finish tablet_protocol", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }

    if (OB_FAIL(ret) && OB_TMP_FAIL(tablet_protocol.abort())) {
      LOG_WARN("fail to abort tablet protocol", KR(ret), KR(tmp_ret));
    }
  }
  LOG_INFO("[SS_META_SVR]: mark tablet deleted finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(deleted_scn),
    "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::remove_remote_tablet_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObScnRange &range)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObArray<MacroBlockId> obj_macro_ids;
  obj_macro_ids.set_attr(ObMemAttr(tenant_id, "RmTbltObj"));

  // 先枚举删除范围内的全部 tablet_meta 版本，以便先删除对应的对象存储对象。
  ObSSMetaTabletKeyIterator key_iter;
  if (OB_FAIL(key_iter.init(*GCTX.sql_proxy_, tenant_id, ls_id, tablet_id,
                            reorgan_scn, range))) {
    LOG_WARN("fail to init tablet key iterator", KR(ret), K(ls_id), K(tablet_id),
      K(reorgan_scn), K(range));
  } else {
    ObSSMetaTabletKey tablet_key;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(key_iter.get_next(tablet_key))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next tablet key", KR(ret), K(ls_id), K(tablet_id));
        }
      } else {
        MacroBlockId obj_macro_id = tablet_key.convert_to_macro_id(false/*is_in_table*/);
        if (OB_FAIL(obj_macro_ids.push_back(obj_macro_id))) {
          LOG_WARN("fail to push object storage macro id", KR(ret), K(obj_macro_id));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!obj_macro_ids.empty()) {
    ObTenantFileManager *file_mgr = MTL(ObTenantFileManager *);
    if (OB_ISNULL(file_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant file manager is null", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(OB_E(EN_SS_TABLET_META_REMOTE_DELETE_ERROR, GETTID())
                       file_mgr->delete_remote_files(obj_macro_ids))) {
      if (OB_OBJECT_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("some tablet meta objects do not exist on object storage, skip",
          K(ls_id), K(tablet_id), "count", obj_macro_ids.count());
      } else {
        LOG_WARN("fail to delete tablet meta on object storage", KR(ret),
          K(ls_id), K(tablet_id), "count", obj_macro_ids.count());
      }
    }
  }
  return ret;
}

int ObSSMetaService::remove_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const ObScnRange &range)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  META_SRV_PERF_TIMEGUARD_INIT();
  int64_t total_affected_rows = 0;
  const uint64_t tenant_id = MTL_ID();
  bool need_double_delete = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid()
      || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(range));
  } else if (OB_UNLIKELY(!tablet_id.is_ls_inner_tablet() && !reorgan_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    ObSSMetaTableOperator op;
    ObSqlString prefix;
    ObSqlString lower_buf;
    ObSqlString upper_buf;
    // meta_key format: {prefix}/scn{20-digit-zero-padded-scn}.T{type}
    // SCN is zero-padded so lexicographic ordering = numeric ordering.
    // upper bound appends (0xFF) which sorts after all normal suffix chars.
    // prefix 兼容 inner_tablet(ls/...) 与普通 tablet(tablet/...)
    if (CLICK_FAIL(ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(ls_id, tablet_id, reorgan_scn, prefix))) {
      LOG_WARN("fail to build prefix", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
    } else if (OB_FAIL(lower_buf.assign_fmt("%s/%s%020lu",
                                             prefix.ptr(),
                                             SCN_KEY_STR,
                                             range.start_scn_.get_val_for_inner_table_field()))) {
      LOG_WARN("fail to build lower bound", KR(ret));
    } else if (OB_FAIL(upper_buf.assign_fmt("%s/%s%020lu\xff",
                                             prefix.ptr(),
                                             SCN_KEY_STR,
                                             range.end_scn_.get_val_for_inner_table_field()))) {
      LOG_WARN("fail to build upper bound", KR(ret));
    } else if (OB_FAIL(check_double_route_ss_meta(tenant_id, need_double_delete))) {
      LOG_WARN("fail to check tablet metadata needs double delete", KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
      LOG_WARN("fail to init ss meta operator", KR(ret));
    } else if (need_double_delete
               && OB_FAIL(remove_remote_tablet_(ls_id, tablet_id, reorgan_scn, range))) {
      LOG_WARN("fail to remove remote tablet", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(range));
    } else if (CLICK_FAIL(OB_E(EN_SS_TABLET_META_TABLE_DELETE_ERROR, GETTID())
                          op.delete_range_row(tenant_id, ls_id, meta_type, lower_buf.string(),
                                              upper_buf.string(), total_affected_rows))) {
      LOG_WARN("fail to delete tablet meta rows by range", KR(ret), K(ls_id), K(tablet_id),
        K(lower_buf), K(upper_buf));
    } else {
      if (OB_SUCC(ret) && OB_FALSE_IT(CLICK())) {
      }
    }
  }
  LOG_INFO("[SS_META_SVR]: remove tablet finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(range),
    K(total_affected_rows), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::check_tablet_mark_deleted(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN reorgan_scn,
    bool &is_mark_deleted)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  is_mark_deleted = false;
  ObArenaAllocator tmp_alloc("ChkMarkDel", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObSSRawMetaRow row;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else {
    ObSSMetaReadParamV2 read_param;
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id, tablet_id, SCN::min_scn()/*meta_scn*/, reorgan_scn, true);
    read_param.set_read_mark_delete(true);
    if (OB_FAIL(ObSSMetaTableHelper::get_meta_info(read_param, SCN::max_scn(), tmp_alloc, row))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      }
      LOG_WARN("fail to get tablet meta info", KR(ret), K(ls_id), K(tablet_id));
    } else {
      int64_t pos = 0;
      ObSSMetaHeadValue head_value;
      if (OB_FAIL(head_value.deserialize_value_info(row.meta_value_.ptr(), row.meta_value_.length(), pos))) {
        LOG_WARN("fail to deserialize head value", KR(ret), K(row.meta_value_.length()));
      } else {
        is_mark_deleted = head_value.get_meta_head_info().is_mark_deleted();
      }
    }
  }
  LOG_TRACE("[SS_META_SVR]: check tablet mark_deleted finish", KR(ret), K(ls_id), K(tablet_id),
      K(reorgan_scn), K(is_mark_deleted), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::check_tablet_exist(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    bool &exist)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  exist = false;
  ObArenaAllocator allocator("ChkSSTablet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle tablet_handle;
  SCN meta_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !reorgan_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
  } else if (OB_FAIL(get_tablet_from_ss_(ls_id, tablet_id, reorgan_scn, allocator, tablet_handle, meta_scn))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet from ss", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
    }
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet handle is invalid", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(tablet_handle));
  } else {
    exist = true;
  }
  LOG_TRACE("[SS_META_SVR]: check tablet exist finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(exist),
    K(meta_scn), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN reorgan_scn,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_hdl)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  SCN meta_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else {
    SMART_VAR(ObSSMetaUpdateMetaInfo, update_meta_info) {
      const bool read_mark_delete = true;
      if (OB_FAIL(get_tablet_from_ss_(ls_id, tablet_id, reorgan_scn,
          allocator, tablet_hdl, update_meta_info, meta_scn, read_mark_delete))) {
        if (OB_OBJECT_NOT_EXIST == ret) {
          ret = OB_TABLET_NOT_EXIST;
        }
        LOG_WARN("fail to get tablet from ss", KR(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  LOG_TRACE("[SS_META_SVR]: get tablet finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN reorgan_scn,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_hdl,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else {
    SMART_VAR(ObSSMetaUpdateMetaInfo, update_meta_info) {
      const bool read_mark_delete = true;
      if (OB_FAIL(get_tablet_from_ss_(ls_id, tablet_id, reorgan_scn,
          allocator, tablet_hdl, update_meta_info, meta_scn, read_mark_delete))) {
        if (OB_OBJECT_NOT_EXIST == ret) {
          ret = OB_TABLET_NOT_EXIST;
        }
        LOG_WARN("fail to get tablet from ss", KR(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  LOG_TRACE("[SS_META_SVR]: get tablet finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(meta_scn),
    "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN reorgan_scn,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_hdl,
    ObSSMetaUpdateMetaInfo &update_meta_info,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else {
    const bool read_mark_delete = false;
    if (OB_FAIL(get_tablet_from_ss_(ls_id, tablet_id, reorgan_scn,
        allocator, tablet_hdl, update_meta_info, meta_scn, read_mark_delete))) {
      if (OB_OBJECT_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      }
      LOG_WARN("fail to get tablet from ss", KR(ret), K(ls_id), K(tablet_id));
    }
  }
  LOG_TRACE("[SS_META_SVR]: get tablet finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(meta_scn),
    K(update_meta_info), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else {
    SCN meta_scn;
    ObSSMetaUpdateMetaInfo update_meta_info;
    if (OB_FAIL(ObSSMetaTableHelper::get_tablet(read_param, read_snapshot,
        allocator, tablet_handle, update_meta_info, meta_scn))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_OBJECT_NOT_EXIST;
      }
      LOG_WARN("fail to get tablet", KR(ret), K(read_param), K(read_snapshot));
    }
  }
  LOG_TRACE("[SS_META_SVR]: get tablet finish", KR(ret), K(read_param), K(read_snapshot), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const SCN &meta_scn,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_hdl)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !meta_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(meta_scn));
  } else {
    ObSSMetaReadParamV2 read_param;
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_KEY,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id,
        tablet_id,
        meta_scn,
        reorgan_scn,
        true /*is_in_table*/);
    read_param.set_read_mark_delete(true);
    if (OB_FAIL(ObSSMetaTableHelper::get_tablet(read_param, allocator, tablet_hdl))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      }
      LOG_WARN("fail to get tablet", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(meta_scn));
    }
  }
  LOG_TRACE("[SS_META_SVR]: get tablet finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(meta_scn), 
    "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_raw_buffer(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObArenaAllocator &allocator,
    SCN &meta_scn,
    char *&dest_buf,
    int64_t &dest_buf_len)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  ObArenaAllocator tmp_alloc("GetTblRaw", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObSSRawMetaRow row;
  dest_buf = nullptr;
  dest_buf_len = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else if (OB_FAIL(ObSSMetaTableHelper::get_meta_info(read_param, read_snapshot, tmp_alloc, row))) {
    LOG_WARN("fail to get meta info", KR(ret), K(read_param), K(read_snapshot));
  } else {
    uint64_t scn_val = 0;
    if (OB_FAIL(ObSSMetaInfoUtil::extract_scn_value(row.meta_key_, scn_val))) {
      LOG_WARN("fail to extract scn from meta_key", KR(ret), K(row.meta_key_));
    } else if (OB_FAIL(ObSSTabletMetaValue::get_tablet_raw_buffer(
                   row.meta_value_.ptr(), row.meta_value_.length(),
                   allocator, dest_buf, dest_buf_len))) {
      LOG_WARN("fail to get tablet raw buffer", KR(ret));
    } else {
      meta_scn.convert_for_logservice(scn_val);
    }
  }
  LOG_INFO("[SS_META_SVR]: get tablet raw_buffer finish", KR(ret), K(read_param), K(read_snapshot), K(meta_scn),
    K(dest_buf_len), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_iter(
    const ObSSMetaReadParamV2 &read_param,
    const ObScnRange &range,
    ObSSMetaIterGuard<ObSSMetaTabletIter> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaTabletIter *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(range));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaTabletIter), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc tablet iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaTabletIter())) {
  } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.tablet_key_.ls_id_,
                                read_param.tablet_key_.tablet_id_,
                                read_param.tablet_key_.reorganization_scn_,
                                range.start_scn_,
                                range.end_scn_,
                                !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet iter", KR(ret), K(read_param), K(range));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_INFO("[SS_META_SVR]: get tablet iter finish", KR(ret), K(read_param), K(range), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_iter(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObSSMetaIterGuard<ObSSMetaTabletIter> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaTabletIter *iter = nullptr;
  const bool is_ls_prefix = (read_param.read_param_type_ == ObSSMetaReadParamType::LS_PREFIX);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaTabletIter), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc tablet iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaTabletIter())) {
  } else if (is_ls_prefix && OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                     read_param.tablet_key_.ls_id_, read_snapshot,
                                     !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet iter", KR(ret), K(read_param), K(read_snapshot));
  } else if (!is_ls_prefix && OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                      read_param.tablet_key_.ls_id_, read_param.tablet_key_.tablet_id_,
                                      read_param.tablet_key_.reorganization_scn_, read_snapshot,
                                      !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet iter", KR(ret), K(read_param), K(read_snapshot));
  } else {
    iter_guard.set_iter(iter);
  }
  
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_INFO("[SS_META_SVR]: get tablet iter finish", KR(ret), K(read_param), K(read_snapshot), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_id_iter(
    const ObSSMetaReadParamV2 &read_param,
    const SCN snapshot,
    ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaTabletKeyIterator *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(snapshot));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaTabletKeyIterator), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc tablet key iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaTabletKeyIterator())) {
  } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.tablet_key_.ls_id_, snapshot,
                                !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet id iter", KR(ret), K(read_param), K(snapshot));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_INFO("[SS_META_SVR]: get tablet_id iter finish", KR(ret), K(read_param), K(snapshot), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_ls_scn_iter(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObSSMetaIterGuard<ObSSMetaLSSCNIterator> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaLSSCNIterator *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaLSSCNIterator), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc ls scn iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaLSSCNIterator())) {
  } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.ls_key_.ls_id_, read_snapshot,
                                !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init ls scn iter", KR(ret), K(read_param), K(read_snapshot));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_INFO("[SS_META_SVR]: get ls_scn iter finish", KR(ret), K(read_param), K(read_snapshot), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_key_iter(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaTabletKeyIterator *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaTabletKeyIterator), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc tablet key iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaTabletKeyIterator())) {
  } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.tablet_key_.ls_id_, read_snapshot,
                                !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet key iter", KR(ret), K(read_param), K(read_snapshot));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_INFO("[SS_META_SVR]: get tablet_key iter finish", KR(ret), K(read_param), K(read_snapshot), "cost_us",
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_tiny_info_iter(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObSSMetaIterGuard<ObSSMetaTabletTinyInfoIterator> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaTabletTinyInfoIterator *iter = nullptr;
  const bool is_ls_prefix = (ObSSMetaReadParamType::LS_PREFIX == read_param.read_param_type_);
  const bool is_tablet_prefix = (ObSSMetaReadParamType::TABLET_PREFIX == read_param.read_param_type_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid()
                         || !read_snapshot.is_valid()
                         || ObAtomicProtocolTypeEnum::TABLET_META != read_param.protocol_type_
                         || (!is_ls_prefix && !is_tablet_prefix))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaTabletTinyInfoIterator), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc tablet tiny info iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaTabletTinyInfoIterator())) {
  } else if (is_ls_prefix && OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                               read_param.tablet_key_.ls_id_, read_snapshot,
                                               !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet tiny info iter", KR(ret), K(read_param), K(read_snapshot));
  } else if (is_tablet_prefix && OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                                   read_param.tablet_key_.ls_id_,
                                                   read_param.tablet_key_.tablet_id_,
                                                   read_param.tablet_key_.reorganization_scn_,
                                                   read_snapshot,
                                                   !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet tiny info iter", KR(ret), K(read_param), K(read_snapshot));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_TRACE("[SS_META_SVR]: get tablet_tiny_info iter finish", KR(ret), K(read_param),
      K(read_snapshot), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_scn_iter(
    const ObSSMetaReadParamV2 &read_param,
    const SCN &read_snapshot,
    ObSSMetaIterGuard<ObSSMetaTabletSCNIterator> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaTabletSCNIterator *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || !read_snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(read_snapshot));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaTabletSCNIterator), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc tablet scn iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaTabletSCNIterator())) {
  } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.tablet_key_.ls_id_,
                                read_param.tablet_key_.tablet_id_,
                                read_param.tablet_key_.reorganization_scn_,
                                read_snapshot, false/*only_latest_row*/,
                                !read_param.read_mark_delete_))) {
    LOG_WARN("fail to init tablet scn iter", KR(ret), K(read_param), K(read_snapshot));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_TRACE("[SS_META_SVR]: get tablet_scn iter finish", KR(ret), K(read_param), K(read_snapshot),
      "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_macro_ids_iter(
    const ObSSMetaReadParamV2 &read_param,
    const ObIArray<SCN> &meta_scns,
    ObSSMetaIterGuard<ObSSMetaTabletMacroIDIterator> &iter_guard)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  void *ptr = nullptr;
  ObSSMetaTabletMacroIDIterator *iter = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!read_param.is_valid() || meta_scns.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(read_param), K(meta_scns));
  } else if (OB_ISNULL(ptr = mtl_malloc(sizeof(ObSSMetaTabletMacroIDIterator), "MetaService"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc tablet macro id iter failed", KR(ret));
  } else if (FALSE_IT(iter = new (ptr) ObSSMetaTabletMacroIDIterator())) {
  } else if (OB_FAIL(iter->init(*GCTX.sql_proxy_, MTL_ID(),
                                read_param.tablet_key_.ls_id_,
                                read_param.tablet_key_.tablet_id_,
                                read_param.tablet_key_.reorganization_scn_,
                                meta_scns))) {
    LOG_WARN("fail to init tablet macro id iter", KR(ret), K(read_param), K(meta_scns));
  } else {
    iter_guard.set_iter(iter);
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(iter)) {
    free_iterator(iter);
    iter = nullptr;
  }
  LOG_INFO("[SS_META_SVR]: get tablet_macro_ids iter finish", KR(ret), K(read_param), K(meta_scns), "cost_us", 
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::free_iterator(ObSSMetaIterator *iter)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(iter)) {
    iter->~ObSSMetaIterator();
    mtl_free(iter);
  }
  return ret;
}

int ObSSMetaService::get_tablet_latest_version(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN reorgan_scn,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(get_tablet_latest_version_from_ss_(ls_id, tablet_id,
                                                         reorgan_scn, meta_scn))) {
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
    }
    LOG_WARN("fail to get tablet latest version", KR(ret), K(ls_id), K(tablet_id));
  }
  LOG_INFO("[SS_META_SVR]: get tablet latest_version finish", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), 
    K(meta_scn), "cost_us", READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::insert_tablet_meta(const ObSSMetaTabletRow &row)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> rows;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_FAIL(rows.push_back(row)  )) {
    LOG_WARN("fail to push back meta row", KR(ret));
  } else if (OB_FAIL(batch_insert_tablet_metas(rows))) {
    LOG_WARN("fail to insert meta row", KR(ret));
  }
  LOG_INFO("[SS_META_SVR]: insert tablet_meta finish", KR(ret), K(row), K(rows.count()));
  return ret;
}

int ObSSMetaService::read_tablet_meta(
    const ObSSMetaTabletKey &key,
    ObArenaAllocator &allocator,
    ObSSMetaTabletRow &row)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObSSMetaTabletKey, SSMETA_ROW_BATCH_CNT> keys;
  ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> rows;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_FAIL(keys.push_back(key))) {
    LOG_WARN("fail to push back meta key", KR(ret));
  } else if (OB_FAIL(batch_read_tablet_metas(keys, allocator, rows))) {
    LOG_WARN("fail to read meta row", KR(ret), K(key));
  } else if (OB_UNLIKELY(rows.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected rows", KR(ret), K(rows.count()));
  } else {
    row = rows[0];
  }
  LOG_INFO("[SS_META_SVR]: read tablet_meta finish", KR(ret), K(key), K(row));
  return ret;
}

int ObSSMetaService::batch_insert_tablet_metas(const ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> &rows)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  ObSSMetaTableOperator op;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(rows.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch insert empty rows", K(rows));
  } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
    LOG_WARN("fail to init ss meta operator", KR(ret));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const ObSSMetaTabletRow &row = rows.at(i);
      ObSSMetaTabletKey key = row.key_;
      MacroBlockId macro_id = key.convert_to_macro_id(true/*is_in_table*/);
      char path_buf[SSMETA_PATH_MAX_LEN] = {0};
      int64_t pos = 0;
      int64_t affected_rows = 0;
      if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(macro_id, path_buf, sizeof(path_buf), pos))) {
        LOG_WARN("fail to format meta_key path", KR(ret), K(macro_id), K(key));
      } else {
        ObString meta_key(pos, path_buf);
        if (OB_FAIL(op.insert_row(MTL_ID(), key.ls_id_, meta_type, meta_key, row.value_, affected_rows))) {
          LOG_WARN("fail to insert tablet meta row", KR(ret), K(key), K(meta_key));
        }
      }
    }
  }
  LOG_INFO("[SS_META_SVR]: batch insert tablet_metas finish", KR(ret), K(rows.count()), "cost_us",
    WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::batch_read_tablet_metas(
    const ObSEArray<ObSSMetaTabletKey, SSMETA_ROW_BATCH_CNT> &keys,
    ObArenaAllocator &allocator,
    ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> &rows)
{
  int ret = OB_SUCCESS;
  SS_META_READ_GUARD;
  ObSSMetaTableOperator op;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(keys.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch read empty rows", KR(ret), K(keys));
  } else if (OB_FAIL(op.init(*GCTX.sql_proxy_, OB_ALL_SHARED_META_TNAME))) {
    LOG_WARN("fail to init ss meta operator", KR(ret));
  } else {
    ObSSMetaType meta_type;
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
    for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
      ObSSMetaTabletKey key = keys.at(i);
      MacroBlockId macro_id = key.convert_to_macro_id(true/*is_in_table*/);
      char path_buf[SSMETA_PATH_MAX_LEN] = {0};
      int64_t pos = 0;
      ObString meta_value;
      if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(macro_id, path_buf, sizeof(path_buf), pos))) {
        LOG_WARN("fail to format meta_key path", KR(ret), K(macro_id), K(key));
      } else {
        ObString meta_key(pos, path_buf);
        if (OB_FAIL(op.read_row(MTL_ID(), key.ls_id_, meta_type, meta_key,
                                allocator, meta_value))) {
          LOG_WARN("fail to read tablet meta row", KR(ret), K(key), K(meta_key));
        } else {
          ObSSMetaTabletRow row(static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META),
                                key, meta_value);
          if (OB_FAIL(rows.push_back(row))) {
            LOG_WARN("fail to push back tablet meta row", KR(ret), K(key));
          }
        }
      }
    }
  }
  LOG_INFO("[SS_META_SVR]: batch read tablet_metas finish", KR(ret), K(keys.count()), K(rows.count()), "cost_us", 
    READ_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::delete_meta_row(ObSSMetaDeleteParamV2 &delete_param)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_FAIL(ObSSMetaTableHelper::delete_meta_info(delete_param))) {
    LOG_WARN("fail to delete meta info", KR(ret), K(delete_param));
  }
  LOG_INFO("[SS_META_SVR]: delete meta_row finish", KR(ret), K(delete_param), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::get_tablet_from_ss_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN reorgan_scn,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    SCN &meta_scn,
    const bool read_mark_delete)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObSSMetaUpdateMetaInfo, update_meta_info) {
    ret = get_tablet_from_ss_(ls_id, tablet_id, reorgan_scn, allocator, tablet_handle,
                              update_meta_info, meta_scn, read_mark_delete);
  }
  return ret;
}

int ObSSMetaService::get_tablet_from_ss_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN reorgan_scn,
    ObArenaAllocator &allocator,
    ObTabletHandle &tablet_handle,
    ObSSMetaUpdateMetaInfo &update_meta_info,
    SCN &meta_scn,
    const bool read_mark_delete)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
  } else {
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id,
        tablet_id,
        SCN::max_scn(),
        reorgan_scn,
        true /*is_in_table*/);
    read_param.set_read_mark_delete(read_mark_delete);
    if (OB_FAIL(ObSSMetaTableHelper::get_tablet(read_param, SCN::max_scn(),
        allocator, tablet_handle, update_meta_info, meta_scn))) {
      LOG_WARN("fail to get tablet", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(read_param));
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_OBJECT_NOT_EXIST;
      }
    }
  }
  return ret;
}

int ObSSMetaService::get_tablet_latest_version_from_ss_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN reorgan_scn,
    SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
  } else {
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_ONLY_KEY,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id,
        tablet_id,
        SCN::max_scn(),
        reorgan_scn,
        true /*is_in_table*/);
    read_param.set_read_mark_delete(true);
    if (OB_FAIL(ObSSMetaTableHelper::get_tablet_scn(read_param, SCN::max_scn(), meta_scn))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_OBJECT_NOT_EXIST;
      }
      LOG_WARN("fail to get tablet latest version", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
    }
  }
  return ret;
}

// TODO: yanyuan.cxf how to deal with this if there is no local tablet.
int ObSSMetaService::acquire_first_created_ss_tablet_(
    ObArenaAllocator &allocator,
    const ObTabletHandle &local_tablet_hdl,
    ObTabletHandle &tablet_hdl) const
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  ObTablet *old_tablet = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  const ObTabletMeta &cur_task_tablet_meta = local_tablet_hdl.get_obj()->get_tablet_meta();
  ObTableStoreIterator iter;
  ObTableHandleV2 local_empty_major;
  // CAUTION: this tablet should be a tmp_tablet only exist in this stack;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_FAIL(local_tablet_hdl.get_obj()->load_storage_schema(allocator,
                                                                     storage_schema))) {
    LOG_WARN("fail to get storage schema", KR(ret));
  } else if (local_tablet_hdl.get_obj()->get_major_table_count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("local tablet has more than one major sstable, unexpected", KR(ret), K(local_tablet_hdl));
  } else if (OB_FAIL(local_tablet_hdl.get_obj()->get_all_sstables(iter, false /*need_unpack*/))) {
    LOG_WARN("fail to get all sstable iter", KR(ret), K(local_tablet_hdl));
  } else if (OB_FAIL(get_local_empty_major_sstable_(iter, local_empty_major))) {
    LOG_WARN("fail to get local empty major sstable", KR(ret), K(local_tablet_hdl));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObTablet)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), KP(buf), "size of", sizeof(ObTablet));
  } else if (FALSE_IT(old_tablet = new (buf) ObTablet())) {
  } else if (OB_FAIL(old_tablet->init_for_shared_storage_first_creation(allocator,
                                                                       cur_task_tablet_meta,
                                                                       *storage_schema,
                                                                       local_empty_major))) {
    LOG_WARN("fail to first init for ss_tablet", KR(ret), K(cur_task_tablet_meta), KPC(storage_schema), KPC(old_tablet));
  } else if (OB_FAIL(old_tablet->assign_pointer_handle(local_tablet_hdl.get_obj()->get_pointer_handle()))) {
    LOG_WARN("fail to assign tablet_pointer_hdl", KR(ret), KPC(local_tablet_hdl.get_obj()), KPC(old_tablet));
  } else {
    tablet_hdl.set_obj(ObTabletHandle::ObTabletHdlType::STANDALONE, old_tablet, &allocator, nullptr);
    tablet_hdl.set_wash_priority(WashTabletPriority::WTP_HIGH);
    tablet_hdl.get_obj()->set_allocator(&allocator);
  }
  ObTabletObjLoadHelper::free(allocator, storage_schema);
  return ret;
}

int ObSSMetaService::check_lease_(
    const ObLSID &ls_id,
    bool &is_sswriter)
{
  int ret = OB_SUCCESS;
  int64_t epoch = 0;
  ObSSWriterKey key(ObSSWriterType::META,
                    ls_id);
  if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key,
                                                  is_sswriter,
                                                  epoch))) {
    LOG_WARN("check lease fail", KR(ret), K(key));
  }
  return ret;
}

int ObSSMetaService::get_leader_(
    const ObLSID &ls_id,
    ObAddr &addr,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  ObSSWriterService *sswriter_svr = MTL(ObSSWriterService *);
  ObSSWriterKey key(ObSSWriterType::META,
                    ls_id);
  ObSSWriterAddr sswriter_addr;
  if (OB_ISNULL(sswriter_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sswriter service should not be null", KR(ret));
  } else if (OB_FAIL(sswriter_svr->get_sswriter_addr(key, sswriter_addr, timeout_us))) {
    LOG_WARN("fail to get sswriter addr", KR(ret), K(key));
  } else {
    addr = sswriter_addr.addr_;
  }
  return ret;
}

int ObSSMetaService::forward_request_(
    const ObAddr &addr,
    const int64_t rpc_type,
    const ObSharedMetaUpdateArg &req,
    ObSharedRpcTransmitResult &res)
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = MTL_ID();
  char *tmp_str = nullptr;
  int64_t pos = 0;
  ObString rpc_content;
  if (OB_ISNULL(tmp_str = static_cast<char *>(mtl_malloc(req.get_serialize_size(), "MetaRpc")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(req.get_serialize_size()));
  } else if (OB_FAIL(req.serialize(tmp_str, req.get_serialize_size(), pos))) {
    LOG_WARN("fail to serialize meta rpc arg", KR(ret), K(req));
  } else {
    rpc_content.assign_ptr(tmp_str, pos);
    ObSharedRpcTransmitArg arg((ObSharedRpcTransmitArg::RPCType)rpc_type,
                               tenant_id,
                               rpc_content);
    if (OB_FAIL(meta_rpc_proxy_.to(addr)
                .by(tenant_id)
                .timeout(req.get_timeout_remain())
                .meta_rpc_sync_transmit(arg, res))) {
      LOG_WARN("fail to meta_rpc_sync_transmit", KR(ret), K(addr), K(tenant_id), K(req), K(arg));
    }
  }

  if (OB_NOT_NULL(tmp_str)) {
    mtl_free(tmp_str);
  }
  return ret;
}

static int build_upper_trans_version_update_info_v2_(
  const ObLSID &ls_id,
  const ObTablet &tablet,
  ObIArray<UpdateUpperTransParam::SCNAndVersion> &upper_trans_list)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTableStoreIterator minor_iter(false/*is reverse*/, false/*need load sstable*/);
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id,
                                         ls_handle,
                                         ObLSGetMod::SHARED_META_SERVICE))) {
    LOG_WARN("fail to get ls", KR(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is nullptr", KR(ret), K(ls_id), KP(ls));
  } else if (OB_FAIL(tablet.get_all_minor_sstables(minor_iter))) {
    LOG_WARN("fail to get minor sstables", KR(ret), K(tablet));
  } else {
    ObITable *table = NULL;
    int tmp_ret = OB_SUCCESS;
    while(OB_SUCC(minor_iter.get_next(table))) {
      SCN upper_trans_version = SCN::max_scn();
      if (table->get_upper_trans_version() == INT64_MAX) {
        if (OB_TMP_FAIL(ls->get_upper_trans_version_before_given_scn(table->get_end_scn(), upper_trans_version))) {
          LOG_WARN("get upper trans fail", K(tmp_ret), K(table->get_end_scn()));
        } else if (upper_trans_version.is_max()) {
          LOG_TRACE("upper trans version not decided", K(table->get_end_scn()));
        } else if (OB_TMP_FAIL(upper_trans_list.push_back(UpdateUpperTransParam::SCNAndVersion(table->get_end_scn(), upper_trans_version.get_val_for_tx())))) {
          LOG_WARN("add upper trans param fail", K(tmp_ret), K(table->get_end_scn()), K(upper_trans_version));
        }
      }
    }
    if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
  }
  return ret;
}
int ObSSMetaService::process_meta_rpc(
    const ObSharedRpcTransmitArg &arg,
    ObSharedRpcTransmitResult &res)
{
  int ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  ObSharedMetaUpdateArg tmp_arg;
  const char *buf = arg.get_rpc_content().ptr();
  const int64_t data_len = arg.get_rpc_content().length();
  int64_t pos = 0;
  int64_t tmp_pos = 0;
  if (OB_FAIL(tmp_arg.deserialize(buf, data_len, tmp_pos))) {
    LOG_WARN("fail to deserialize meta arg", KR(ret), K(arg), K(tmp_pos));
  } else {
    switch (tmp_arg.type_) {
    case ObSharedMetaUpdateArg::UPDATE_LS_ID_META:
    case ObSharedMetaUpdateArg::UPDATE_LS_GC_STATE:
    case ObSharedMetaUpdateArg::UPDATE_LS_WITH_FUNC: {
      ObUpdateSharedLSMetaArg real_arg;
      if (OB_FAIL(real_arg.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to get real arg", KR(ret), K(pos), K(data_len));
      } else if (OB_FAIL(process_ls_meta_update_(real_arg))) {
        LOG_WARN("fail to process meta rpc local", KR(ret), K(real_arg));
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("meet not supported request", K(tmp_arg), K(arg));
    } // default
    }
  }
  LOG_INFO("[SS_META_SVR]: process meta_rpc finish", KR(ret), K(arg), K(res), "cost_us", WRITE_GUARD_COST_US);
  return ret;
}

int ObSSMetaService::process_ls_meta_update_(const ObUpdateSharedLSMetaArg &arg)
{
  int ret = OB_SUCCESS;
  switch (arg.type_) {
  case ObSharedMetaUpdateArg::UPDATE_LS_ID_META: {
    if (OB_FAIL(update_for_ss_id_meta_local_(arg.id_meta_arg_.ls_id_,
                                             arg.update_reason_,
                                             arg.check_lease_,
                                             arg.id_meta_arg_.service_type_,
                                             arg.id_meta_arg_.limited_id_,
                                             arg.id_meta_arg_.latest_scn_))) {
      LOG_WARN("fail to update ls id meta", KR(ret), K(arg));
    }
    break;
  }
  case ObSharedMetaUpdateArg::UPDATE_LS_GC_STATE: {
    if (OB_FAIL(update_for_ss_gc_state_local_(arg.gc_arg_.ls_id_,
                                              arg.update_reason_,
                                              arg.check_lease_,
                                              arg.gc_arg_.gc_state_,
                                              arg.gc_arg_.offline_scn_))) {
      LOG_WARN("fail to update ls gc state", KR(ret), K(arg));
    }
    break;
  }
  case ObSharedMetaUpdateArg::UPDATE_LS_WITH_FUNC: {
    switch (arg.update_fn_arg_.func_code_) {
    case ObIMetaFunction::MetaFuncType::FUNC_TYPE_TEST: {
      ObTestMetaFunction fn;
      if (OB_FAIL(update_for_ss_ls_meta_local_(arg.update_fn_arg_.ls_id_,
                                               arg.update_reason_,
                                               arg.check_lease_,
                                               fn))) {
        LOG_WARN("fail to update ls meta local", KR(ret), K(arg));
      }
      break;
    }
    case ObIMetaFunction::MetaFuncType::FUNC_TYPE_RESTORE: {
      ObArenaAllocator allocator("RestoreLs", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
      ObRestoreLSMetaFunctor functor(ObIMetaFunction::MetaFuncType::FUNC_TYPE_RESTORE, arg.update_fn_arg_.ls_id_);
      int64_t pos = 0;
      if (OB_FAIL(functor.deserialize(arg.functor_buf_.ptr(),
                                      arg.functor_buf_.length(),
                                      pos))) {
        LOG_WARN("fail to deserialize restore functor", KR(ret), K(arg));
      } else if (OB_FAIL(update_for_ss_ls_meta_local_(arg.update_fn_arg_.ls_id_,
                                                      arg.update_reason_,
                                                      arg.check_lease_,
                                                      functor))) {
        LOG_WARN("fail to update ls meta local", KR(ret), K(arg));
      }
      break;
    }
    default:{
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("meet not supported function code", KR(ret), K(arg));
    } // default
    } // switch
    break;
  }
  default: {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meet not supported request", K(arg));
  } // default
  } // switch
  return ret;
}

// ======================== update_for_ss_id_meta ========================
int ObSSMetaService::update_for_ss_id_meta_(
    const ObSharedMetaUpdateArg &rpc_req,
    const ObLSID &ls_id,
    const int64_t service_type,
    const int64_t limited_id,
    const SCN &latest_scn)
{
  int ret = OB_SUCCESS;
  META_SRV_PERF_TIMEGUARD_INIT();
  ObAddr sswriter_addr;
  bool is_sswriter = false;
  ObSharedRpcTransmitResult res;
  do {
    if (rpc_req.get_timeout_remain() <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("rpc timeout", KR(ret), K(rpc_req));
    } else if (!rpc_req.check_lease_) {
      if (CLICK_FAIL(update_for_ss_id_meta_local_(ls_id, rpc_req.update_reason_, rpc_req.check_lease_,
                                                  service_type, limited_id, latest_scn))) {
        LOG_WARN("fail to update ls meta local", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    } else if (OB_FAIL(check_lease_(ls_id, is_sswriter))) {
      LOG_WARN("fail to check lease", KR(ret), K(ls_id));
    } else if (is_sswriter) {
      if (CLICK_FAIL(update_for_ss_id_meta_local_(ls_id, rpc_req.update_reason_, rpc_req.check_lease_,
                                                  service_type, limited_id, latest_scn))) {
        LOG_WARN("fail to update ls meta local", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    } else if (OB_FAIL(get_leader_(ls_id, sswriter_addr, rpc_req.get_timeout_remain()))) {
      LOG_WARN("fail to get meta sswriter", KR(ret));
    } else if (sswriter_addr == GCTX.self_addr()) {
      // 如果发现sswriter是当前observer节点，则重试
      ret = OB_EAGAIN;
    } else if (CLICK_FAIL(forward_request_(sswriter_addr,
                                           ObSharedRpcTransmitArg::RPCType::RPC_TYPE_SHARED_META,
                                           rpc_req, res))) {
      LOG_WARN("fail to forward request", KR(ret));
    } else if (OB_FALSE_IT(CLICK())) {
    }
  } while (need_retry_(ret));
  return ret;
}

int ObSSMetaService::update_for_ss_id_meta_local_(
    const ObLSID &ls_id,
    const ObMetaUpdateReason reason,
    const bool check_lease,
    const int64_t service_type,
    const int64_t limited_id,
    const SCN &latest_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSAtomicProtocol ls_protocol;
  ObSSMetaAtomicLSOp ls_meta_op;
  int64_t op_id = -1;
  bool need_update = true;
  ObBucketHashWLockGuard guard(bucket_lock_, ls_id.hash());
  if (OB_FAIL(ls_protocol.init(MTL_ID(), ls_id, false/*is_deleted*/, SCN()))) {
    LOG_WARN("fail to init ls_protocol", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_protocol.create(true))) {
    LOG_WARN("fail to create ls_protocol", KR(ret));
  } else if (OB_FAIL(ls_protocol.get_op_id(op_id))) {
    LOG_WARN("fail to get op_id", KR(ret));
  } else {
    ObSSMetaAtomicOpInitParam ls_op_param;
    build_ls_op_init_param_(op_id, ls_id, ls_op_param);
    if (OB_FAIL(ls_meta_op.init(ls_op_param))) {
      LOG_WARN("fail to init ls meta_op", KR(ret), K(ls_op_param));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    SMART_VAR(ObSSLSMetaValue, tmp_v) {
      ObSSLSMeta &tmp = tmp_v.meta_value_;
      ObSSMetaHeadValueInfo meta_head_info;
      if (OB_FAIL(ls_meta_op.read_meta_info(tmp_v))) {
        LOG_WARN("fail to get ls meta", KR(ret), K(ls_id));
      } else if (OB_FAIL(tmp.update_for_ss_id_meta(service_type, limited_id, latest_scn, need_update))) {
        LOG_WARN("fail to update ss ls meta", KR(ret), K(ls_id));
      } else if (need_update) {
        if (OB_FAIL(meta_head_info.set(reason))) {
          LOG_WARN("fail to set meta info", KR(ret), K(reason));
        } else if (OB_FAIL(tmp_v.set_meta_head_info(meta_head_info))) {
          LOG_WARN("fail to set meta info", KR(ret), K(meta_head_info));
        } else if (OB_FAIL(ls_meta_op.write_meta_info(tmp_v))) {
          LOG_WARN("fail to persist shared ls", KR(ret), K(tmp_v));
        } else if (OB_FAIL(ls_protocol.finish(ls_meta_op))) {
          LOG_WARN("fail to finish ls_protocol", KR(ret));
        }
      }
    }
  }
  if ((OB_FAIL(ret) || (!need_update)) && OB_TMP_FAIL(ls_protocol.abort())) {
    LOG_WARN("fail to abort ls protocol", KR(ret), KR(tmp_ret), K(need_update));
  }
  return ret;
}

// ======================== update_for_ss_gc_state ========================
int ObSSMetaService::get_ls_gc_state_from_ss_(
    const ObLSID &ls_id,
    LSGCState &gc_state,
    SCN &offline_scn)
{
  int ret = OB_SUCCESS;
  ObSSLSMeta ls_meta;
  gc_state = LSGCState::INVALID_LS_GC_STATE;
  offline_scn = SCN::invalid_scn();
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(get_ls_from_ss_(ls_id, SCN::max_scn(), ls_meta, true /*by_prefix*/))) {
    LOG_WARN("fail to get ls meta from ss", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_meta.get_gc_state(gc_state))) {
    LOG_WARN("fail to get gc state", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_meta.get_offline_scn(offline_scn))) {
    LOG_WARN("fail to get offline scn", KR(ret), K(ls_id));
  }
  return ret;
}

int ObSSMetaService::check_ls_gc_state_covered_(
    const ObLSID &ls_id,
    const LSGCState &gc_state,
    const SCN &offline_scn,
    bool &is_covered)
{
  int ret = OB_SUCCESS;
  LSGCState cur_gc_state = LSGCState::INVALID_LS_GC_STATE;
  SCN cur_offline_scn;
  is_covered = false;
  if (OB_UNLIKELY(!ls_id.is_valid()
                  || (LSGCState::LS_OFFLINE != gc_state && LSGCState::WAIT_GC != gc_state)
                  || (LSGCState::LS_OFFLINE == gc_state && !offline_scn.is_valid())
                  || (LSGCState::WAIT_GC == gc_state && offline_scn.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(gc_state), K(offline_scn));
  } else if (OB_FAIL(get_ls_gc_state_from_ss_(ls_id, cur_gc_state, cur_offline_scn))) {
    LOG_WARN("fail to get persisted ls gc state", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!ObGCHandler::is_valid_ls_gc_state(cur_gc_state))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid persisted ls gc state", KR(ret), K(ls_id), K(cur_gc_state));
  } else if (LSGCState::LS_OFFLINE == gc_state) {
    // 共享gc_state达到/推过ls_offline且offline_scn也达到/推过，都算ls_offline的状态已设置过
    is_covered = cur_gc_state >= LSGCState::LS_OFFLINE
                 && cur_offline_scn.is_valid()
                 && cur_offline_scn >= offline_scn;
  } else {
    // WAIT_GC请求不携带offline_scn，沿用LS_OFFLINE阶段持久化的offline_scn。
    is_covered = LSGCState::WAIT_GC == cur_gc_state && cur_offline_scn.is_valid();
  }
  return ret;
}

int ObSSMetaService::update_for_ss_gc_state_(
    const ObSharedMetaUpdateArg &rpc_req,
    const ObLSID &ls_id,
    const logservice::LSGCState &gc_state,
    const SCN &offline_scn,
    const bool is_replay)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  META_SRV_PERF_TIMEGUARD_INIT();
  ObAddr sswriter_addr;
  bool is_sswriter = false;
  bool is_sswriter_addr_invalid = false;
  ObSharedRpcTransmitResult res;
  do {
    if (rpc_req.get_timeout_remain() <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("rpc timeout", KR(ret), K(rpc_req));
    } else if (!rpc_req.check_lease_) {
      if (CLICK_FAIL(update_for_ss_gc_state_local_(ls_id, rpc_req.update_reason_, rpc_req.check_lease_,
                                                   gc_state, offline_scn))) {
        LOG_WARN("fail to update ls meta local", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    } else if (OB_FAIL(check_lease_(ls_id, is_sswriter))) {
      LOG_WARN("fail to check lease", KR(ret), K(ls_id));
    } else if (is_sswriter) {
      if (CLICK_FAIL(update_for_ss_gc_state_local_(ls_id, rpc_req.update_reason_, rpc_req.check_lease_,
                                                   gc_state, offline_scn))) {
        LOG_WARN("fail to update ls meta local", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    } else if (OB_FAIL(get_leader_(ls_id, sswriter_addr, rpc_req.get_timeout_remain()))) {
      LOG_WARN("fail to get meta sswriter", KR(ret));
    } else if (OB_UNLIKELY(!sswriter_addr.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      is_sswriter_addr_invalid = true;
      LOG_WARN("meta sswriter addr is invalid", KR(ret), K(ls_id), K(sswriter_addr), K(is_replay));
    } else if (sswriter_addr == GCTX.self_addr()) {
      // 如果发现sswriter是当前observer节点，则重试
      ret = OB_EAGAIN;
    } else if (CLICK_FAIL(forward_request_(sswriter_addr,
                                           ObSharedRpcTransmitArg::RPCType::RPC_TYPE_SHARED_META,
                                           rpc_req, res))) {
      LOG_WARN("fail to forward request", KR(ret), K(ls_id), K(sswriter_addr));
    } else if (OB_FALSE_IT(CLICK())) {
    }
  } while (need_retry_(ret) && !is_sswriter_addr_invalid);

  if (OB_FAIL(ret) && is_replay && LSGCState::LS_OFFLINE == gc_state && is_sswriter_addr_invalid) {
    bool is_gc_state_covered = false;
    if (OB_TMP_FAIL(check_ls_gc_state_covered_(ls_id, gc_state, offline_scn, is_gc_state_covered))) {
      LOG_WARN("fail to check persisted ls offline state before lease bypass", KR(tmp_ret),
        K(ls_id), K(offline_scn));
    }
    if (is_gc_state_covered) {
      ret = OB_SUCCESS;
      FLOG_INFO("persisted ls meta covers replayed offline update", K(ls_id), K(offline_scn));
    } else {
      // The coverage read is best-effort. The LS atomic protocol reads the
      // latest durable state again and applies the monotonic update.
      FLOG_INFO("ready to update ss gc_state without checking lease", K(ret), K(ls_id), K(gc_state), K(offline_scn));
      if (CLICK_FAIL(update_for_ss_gc_state_local_(ls_id, rpc_req.update_reason_, false/*check_lease*/,
          gc_state, offline_scn))) {
        LOG_WARN("fail to update replayed offline state without lease", KR(ret), K(ls_id), K(offline_scn));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    }
  }
  return ret;
}

int ObSSMetaService::update_for_ss_gc_state_local_(
    const ObLSID &ls_id,
    const ObMetaUpdateReason reason,
    const bool check_lease,
    const logservice::LSGCState &gc_state,
    const SCN &offline_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSAtomicProtocol ls_protocol;
  ObSSMetaAtomicLSOp ls_meta_op;
  int64_t op_id = -1;
  bool need_update = true;
  bool need_check_deleted_ls = false;
  // Mark the ls meta deleted only when entering the terminal WAIT_GC state, not at
  // LS_OFFLINE. Marking the lock row deleted freezes it (any later update returns
  // OB_STATE_NOT_MATCH), so it must happen on the last state transition. The offline_scn
  // was already persisted at the LS_OFFLINE stage, so read it back here as the deleted_scn.
  const bool mark_delete = (logservice::LSGCState::WAIT_GC == gc_state);
  SCN deleted_scn;
  if (OB_UNLIKELY(!check_lease && LSGCState::LS_OFFLINE != gc_state)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lease bypass is only allowed for ls offline", KR(ret), K(ls_id), K(gc_state));
  } else if (mark_delete) {
    LSGCState cur_gc_state = LSGCState::INVALID_LS_GC_STATE;
    if (OB_FAIL(get_ls_gc_state_from_ss_(ls_id, cur_gc_state, deleted_scn))) {
      LOG_WARN("fail to get persisted ls gc state", KR(ret), K(ls_id));
    } else if (OB_UNLIKELY(!deleted_scn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("offline_scn is invalid when mark delete", KR(ret), K(ls_id), K(gc_state));
    } else if (LSGCState::WAIT_GC != cur_gc_state
               && OB_FAIL(mark_ls_inner_tablets_deleted_(ls_id, deleted_scn))) {
      // 必须在获取下面的 ls hash bucket 锁之前执行：mark_tablet_deleted 会获取 tablet hash bucket 锁，
      // 两个 hash 可能落到同一个 latch 上，嵌套加锁会导致自死锁。
      // 该步骤是幂等的，失败后会重试（ls gc 状态机会重新进入 WAIT_GC，直到本函数成功）.
      LOG_WARN("fail to mark ls inner tablets deleted", KR(ret), K(ls_id), K(deleted_scn));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObBucketHashWLockGuard guard(bucket_lock_, ls_id.hash());
    if (OB_FAIL(ls_protocol.init(MTL_ID(), ls_id, mark_delete/*is_deleted*/, deleted_scn))) {
      LOG_WARN("fail to init ls_protocol", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_protocol.create(check_lease))) {
      // 并发请求场景下，后续请求可以直接验证当前持久化的gc_state状态
      need_check_deleted_ls = (OB_STATE_NOT_MATCH == ret);
      LOG_WARN("fail to create ls_protocol", KR(ret));
    } else if (OB_FAIL(ls_protocol.get_op_id(op_id))) {
      LOG_WARN("fail to get op_id", KR(ret));
    } else {
      ObSSMetaAtomicOpInitParam ls_op_param;
      build_ls_op_init_param_(op_id, ls_id, ls_op_param);
      if (OB_FAIL(ls_meta_op.init(ls_op_param))) {
        LOG_WARN("fail to init ls meta_op", KR(ret), K(ls_op_param));
      }
    }
    if (OB_FAIL(ret)) {
    } else {
      SMART_VAR(ObSSLSMetaValue, tmp_v) {
        ObSSLSMeta &tmp = tmp_v.meta_value_;
        ObSSMetaHeadValueInfo meta_head_info;
        if (OB_FAIL(ls_meta_op.read_meta_info(tmp_v))) {
          LOG_WARN("fail to get ls meta", KR(ret), K(ls_id));
        } else if (OB_FAIL(tmp.update_for_ss_gc_state(gc_state, offline_scn, need_update))) {
          LOG_WARN("fail to update ss ls meta", KR(ret), K(ls_id));
        } else if (!need_update && mark_delete) {
          ret = OB_STATE_NOT_MATCH;
          LOG_WARN("ls meta is already in wait gc but delete mark is incomplete", KR(ret), K(ls_id));
        } else if (need_update) {
          if (OB_FAIL(meta_head_info.set(reason, mark_delete))) {
            LOG_WARN("fail to set meta info", KR(ret), K(reason), K(mark_delete));
          } else if (OB_FAIL(tmp_v.set_meta_head_info(meta_head_info))) {
            LOG_WARN("fail to set meta info", KR(ret), K(meta_head_info));
          } else if (OB_FAIL(ls_meta_op.write_meta_info(tmp_v))) {
            LOG_WARN("fail to persist shared ls", KR(ret), K(tmp_v));
          } else if (OB_FAIL(ls_protocol.finish(ls_meta_op))) {
            need_check_deleted_ls = (OB_STATE_NOT_MATCH == ret);
            LOG_WARN("fail to finish ls_protocol", KR(ret));
          }
        }
      }
    }
    if ((OB_FAIL(ret) || (!need_update)) && OB_TMP_FAIL(ls_protocol.abort())) {
      LOG_WARN("fail to abort ls protocol", KR(ret), KR(tmp_ret), K(need_update));
    }
  }

  // A deleted lock row may be produced by a concurrent WAIT_GC update. Verify
  // whether that terminal state already covers the requested GC transition.
  if (need_check_deleted_ls
      && (LSGCState::LS_OFFLINE == gc_state || LSGCState::WAIT_GC == gc_state)) {
    bool is_covered = false;
    if (OB_TMP_FAIL(check_ls_gc_state_covered_(ls_id, gc_state, offline_scn, is_covered))) {
      LOG_WARN("fail to check persisted ls gc state after conflict",
          KR(tmp_ret), KR(ret), K(ls_id), K(gc_state), K(offline_scn));
    } else if (is_covered) {
      LOG_TRACE("persisted ls meta covers repeated gc update after conflict",
          KR(ret), K(ls_id), K(gc_state), K(offline_scn));
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

// 使用 LS 的 offline_scn 作为 deleted_scn，对所有 LS inner tablet 进行 mark delete，
// 以便 SS GC 可以从每个 inner tablet meta head 中读取 deleted_scn
// （inner tablet 没有 tablet 维度的删除事务，因此也没有自己的 delete_commit_scn）。
// mark_inner_tablet_delete_ 是幂等的（已标记的 tablet 会被跳过），因此部分失败后重试是安全的。
int ObSSMetaService::mark_ls_inner_tablets_deleted_(
    const ObLSID &ls_id,
    const SCN &deleted_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || !deleted_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(deleted_scn));
  } else {
    ObLS::ObLSInnerTabletIDIter inner_tablet_id_iter;
    ObTabletID tablet_id;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(inner_tablet_id_iter.get_next(tablet_id))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get next ls inner tablet id", KR(ret), K(ls_id));
        }
      } else if (OB_FAIL(mark_inner_tablet_delete_(ls_id, tablet_id, SCN::min_scn()/*reorgan_scn*/,
                                                  deleted_scn))) {
        LOG_WARN("fail to mark inner tablet deleted", KR(ret), K(ls_id), K(tablet_id),
            K(deleted_scn));
      }
    }
  }
  return ret;
}

// ======================== update_for_ss_ls_meta ========================
int ObSSMetaService::update_for_ss_ls_meta_(
    const ObSharedMetaUpdateArg &rpc_req,
    const ObLSID &ls_id,
    ObIMetaFunction &fn)
{
  int ret = OB_SUCCESS;
  META_SRV_PERF_TIMEGUARD_INIT();
  ObAddr sswriter_addr;
  bool is_sswriter = false;
  ObSharedRpcTransmitResult res;
  do {
    if (rpc_req.get_timeout_remain() <= 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("rpc timeout", KR(ret), K(rpc_req));
    } else if (!rpc_req.check_lease_) {
      if (CLICK_FAIL(update_for_ss_ls_meta_local_(ls_id, rpc_req.update_reason_, rpc_req.check_lease_, fn))) {
        LOG_WARN("fail to update ls meta local", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    } else if (OB_FAIL(check_lease_(ls_id, is_sswriter))) {
      LOG_WARN("fail to check lease", KR(ret), K(ls_id));
    } else if (is_sswriter) {
      if (CLICK_FAIL(update_for_ss_ls_meta_local_(ls_id, rpc_req.update_reason_, rpc_req.check_lease_, fn))) {
        LOG_WARN("fail to update ls meta local", KR(ret));
      } else if (OB_FALSE_IT(CLICK())) {
      }
    } else if (OB_FAIL(get_leader_(ls_id, sswriter_addr, rpc_req.get_timeout_remain()))) {
      LOG_WARN("fail to get meta sswriter", KR(ret));
    } else if (sswriter_addr == GCTX.self_addr()) {
      // 如果发现sswriter是当前observer节点，则重试
      ret = OB_EAGAIN;
    } else if (CLICK_FAIL(forward_request_(sswriter_addr,
                                           ObSharedRpcTransmitArg::RPCType::RPC_TYPE_SHARED_META,
                                           rpc_req, res))) {
      LOG_WARN("fail to forward request", KR(ret));
    } else if (OB_FALSE_IT(CLICK())) {
    }
  } while (need_retry_(ret));
  return ret;
}

int ObSSMetaService::update_for_ss_ls_meta_local_(
    const ObLSID &ls_id,
    const ObMetaUpdateReason reason,
    const bool check_lease,
    ObIMetaFunction &fn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObLSAtomicProtocol ls_protocol;
  ObSSMetaAtomicLSOp ls_meta_op;
  int64_t op_id = -1;
  bool need_update = true;
  ObBucketHashWLockGuard guard(bucket_lock_, ls_id.hash());
  if (OB_FAIL(ls_protocol.init(MTL_ID(), ls_id, false/*is_deleted*/, SCN()))) {
    LOG_WARN("fail to init ls_protocol", KR(ret), K(ls_id));
  } else if (OB_FAIL(ls_protocol.create(check_lease/*need_check_sswriter*/))) {
    LOG_WARN("fail to create ls_protocol", KR(ret));
  } else if (OB_FAIL(ls_protocol.get_op_id(op_id))) {
    LOG_WARN("fail to get op_id", KR(ret));
  } else {
    ObSSMetaAtomicOpInitParam ls_op_param;
    build_ls_op_init_param_(op_id, ls_id, ls_op_param);
    if (OB_FAIL(ls_meta_op.init(ls_op_param))) {
      LOG_WARN("fail to init ls meta_op", KR(ret), K(ls_op_param));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    SMART_VAR(ObSSLSMetaValue, tmp_v) {
      ObSSLSMeta &tmp = tmp_v.meta_value_;
      ObSSMetaHeadValueInfo meta_head_info;
      if (OB_FAIL(ls_meta_op.read_meta_info(tmp_v))) {
        LOG_WARN("fail to get ls meta", KR(ret), K(ls_id));
      } else if (OB_FAIL(tmp.update_for_ss_ls_meta(fn, need_update))) {
        LOG_WARN("fail to update ss ls meta", KR(ret), K(ls_id));
      } else if (need_update) {
        if (OB_FAIL(meta_head_info.set(reason))) {
          LOG_WARN("fail to set meta info", KR(ret), K(reason));
        } else if (OB_FAIL(tmp_v.set_meta_head_info(meta_head_info))) {
          LOG_WARN("fail to set meta info", KR(ret), K(meta_head_info));
        } else if (OB_FAIL(ls_meta_op.write_meta_info(tmp_v))) {
          LOG_WARN("fail to persist shared ls", KR(ret), K(tmp_v));
        } else if (OB_FAIL(ls_protocol.finish(ls_meta_op))) {
          LOG_WARN("fail to finish ls_protocol", KR(ret));
        }
      }
    }
  }
  if ((OB_FAIL(ret) || (!need_update)) && OB_TMP_FAIL(ls_protocol.abort())) {
    LOG_WARN("fail to abort ls protocol", KR(ret), KR(tmp_ret), K(need_update));
  }
  return ret;
}

int ObSSMetaService::get_local_empty_major_sstable_(
    ObTableStoreIterator &iter,
    ObTableHandleV2 &local_empty_major) const
{
  int ret = OB_SUCCESS;
  local_empty_major.reset();

  ObTableHandleV2 tmp_handle;
  while (OB_SUCC(ret)) {
    tmp_handle.reset();
    const ObSSTable *sstable = nullptr;
    if (OB_FAIL(iter.get_next(tmp_handle))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next sstable", KR(ret));
      }
    } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
      LOG_WARN("fail to get sstable", KR(ret), K(tmp_handle));
    } else if (!sstable->is_major_sstable() || sstable->is_meta_major_sstable()) {
      //do nothing
    } else if (!sstable->is_empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sstable is not empty, unexpected", KR(ret), KPC(sstable));
    } else {
      local_empty_major = tmp_handle;
    }
  }
  return ret;
}

// Mark an LS inner tablet deleted by rewriting only MetaValue head
// (mark_delete_ + deleted_scn_). No need to rewrite tablet body / empty_shell
// because shared tablet deleted_scn is carried by the head after this change.
// Differs from mark_tablet_deleted() mainly by need_check_sswriter=true.
int ObSSMetaService::mark_inner_tablet_delete_(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const SCN &reorgan_scn,
    const SCN &delete_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  SS_META_WRITE_GUARD;
  META_SRV_PERF_TIMEGUARD_INIT();
  PERF_GUARD_INIT(perf_mark_inner_tablet_delete);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObSSMetaService not inited.", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid()
                         || !tablet_id.is_valid()
                         || !tablet_id.is_ls_inner_tablet()
                         || !reorgan_scn.is_valid()
                         || !delete_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(delete_scn));
  } else {
    ObBucketHashWLockGuard guard(bucket_lock_, tablet_id.hash());
    ObSSMetaAtomicTabletOp tablet_meta_op;
    int64_t op_id = -1;
    ObTabletAtomicProtocol tablet_protocol;
    ObTabletAtomicProtocolParam tablet_protocol_param;
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    ObSSMetaReadParamV2 read_param;
    ObSSMetaOpBufferGuard meta_value_guard;
    ObSSMetaOpBuffer &meta_value = meta_value_guard.get_buffer();
    int64_t last_major_snapshot_version = -1;
    bool is_mark_delete = false;

    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id, tablet_id, SCN::min_scn(), reorgan_scn, true/*is_in_table*/);
    read_param.set_read_mark_delete(true);
    if (OB_FAIL(ObSSMetaTableHelper::get_tablet_last_major_snapshot_version(
        read_param, last_major_snapshot_version, is_mark_delete, &meta_value))) {
      LOG_WARN("fail to get tablet last major snapshot version", KR(ret), K(read_param));
    } else if (is_mark_delete) {
      // already mark-deleted, keep idempotent behavior and skip unnecessary rewrite.
    } else if (OB_FAIL(build_tablet_protocol_init_param_(ls_id, tablet_id, reorgan_scn, meta_ctrl_keys,
        true/*is_deleted*/, delete_scn, tablet_protocol_param))) {
      LOG_WARN("fail to build tablet protocol param", KR(ret), K(ls_id), K(tablet_id),
          K(reorgan_scn), K(delete_scn), K(is_mark_delete));
    } else if (FALSE_IT(tablet_protocol_param.last_major_snapshot_ = last_major_snapshot_version)) {
    } else if (OB_FAIL(tablet_protocol.init(tablet_protocol_param))) {
      LOG_WARN("fail to init tablet protocol", KR(ret), K(tablet_protocol_param));
    } else if (OB_FAIL(tablet_protocol.create(true/*need_check_sswriter*/))) {
      LOG_WARN("fail to create tablet protocol", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
    } else if (OB_FAIL(tablet_protocol.get_op_id(op_id))) {
      LOG_WARN("fail to get op_id", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
    } else {
      ObSSMetaAtomicOpInitParam tablet_op_param;
      build_tablet_op_init_param_(op_id, ls_id, tablet_id, reorgan_scn, tablet_op_param);
      tablet_op_param.meta_value_ = &meta_value;
      if (OB_FAIL(tablet_meta_op.init(tablet_op_param))) {
        LOG_WARN("fail to init tablet meta_op", KR(ret), K(tablet_op_param));
      } else if (OB_FAIL(tablet_meta_op.handle_mark_delete(delete_scn))) {
        LOG_WARN("fail to mark delete shared tablet", KR(ret), K(ls_id), K(tablet_id),
            K(reorgan_scn), K(delete_scn));
      } else if (OB_FAIL(tablet_protocol.finish(tablet_meta_op))) {
        LOG_WARN("fail to finish tablet_protocol", KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
      }
    }

    if (OB_FAIL(ret) && OB_TMP_FAIL(tablet_protocol.abort())) {
      LOG_WARN("fail to abort tablet protocol", KR(ret), KR(tmp_ret), K(ls_id), K(tablet_id), K(reorgan_scn));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
