// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE
#include "ob_ss_sstable_gc.h"
#include "share/ob_thread_define.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "share/ob_shared_meta_common.h"
#include "ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/worker.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/ob_ss_diagnose_mgr.h"

namespace oceanbase {
namespace storage {

ERRSIM_POINT_DEF(EN_SS_SSTABLE_GC_SKIP);

ObSSSSTableGCProcessor::ObSSSSTableGCProcessor()
: ss_sstable_gc_tg_id_(-1),
  stopped_(false),
  processing_(false)
{
}

ObSSSSTableGCProcessor::~ObSSSSTableGCProcessor()
{
  destroy();
}

int ObSSSSTableGCProcessor::start()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SS_SSTableGCTimer, ss_sstable_gc_tg_id_))) {
    LOG_WARN("failed to create ss sstable gc thread", K(ret));
  } else if (OB_FAIL(TG_START(ss_sstable_gc_tg_id_))) {
    LOG_WARN("failed to start ss sstable gc thread", K(ret));
  } else if (OB_FAIL(TG_SCHEDULE(ss_sstable_gc_tg_id_, *this, SS_SSTABLE_GC_INTERVAL, true))) {
    LOG_WARN("failed to schedule ss sstable gc task", K(ret));
  } else {
    stopped_ = false;
  }
  return ret;
}

int ObSSSSTableGCProcessor::stop()
{
  int ret = OB_SUCCESS;
  stopped_ = true;
  if (ss_sstable_gc_tg_id_ > 0) {
    TG_STOP(ss_sstable_gc_tg_id_);
  }
  return ret;
}

void ObSSSSTableGCProcessor::wait()
{
  if (ss_sstable_gc_tg_id_ > 0) {
    TG_WAIT(ss_sstable_gc_tg_id_);
  }
}

void ObSSSSTableGCProcessor::destroy()
{
  if (ss_sstable_gc_tg_id_ > 0) {
    TG_DESTROY(ss_sstable_gc_tg_id_);
    ss_sstable_gc_tg_id_ = -1;
  }
}

void ObSSSSTableGCProcessor::runTimerTask()
{
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::fast_current_time();
  ObCurTraceId::init(GCONF.self_addr_);
  ObSEArray<ObLSID, 16> ls_ids;
  if (EN_SS_SSTABLE_GC_SKIP.test(static_cast<int64_t>(MTL_ID()))) {
    // 按租户暂停后台扫描，保留定时任务以便解除注入后恢复。
  } else if (ATOMIC_CAS(&processing_, false, true)) {
    LOG_INFO("ss sstable gc is already running, skip it");
  } else {
    // the maximum allowed time to get all tablets from iter
    const int64_t SCAN_ALL_LS_TABLET_TIMEOUT = 102L * 365L * 24L * 60L * 60L * 1000L * 1000L; // 102 years
    const int64_t prev_timeout_ts = THIS_WORKER.get_timeout_ts();
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + SCAN_ALL_LS_TABLET_TIMEOUT);
    if (OB_FAIL(get_all_ls_ids_(ls_ids))) {
      LOG_WARN("get all ls ids failed", K(ret));
    } else {
      for (int64_t i = 0; !stopped_ && i < ls_ids.count(); i++) {
        ObLSHandle ls_handle;
        LOG_INFO("process for ls start", "ls_id", ls_ids.at(i));
        if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_ids.at(i), ls_handle, ObLSGetMod::STORAGE_MOD))) {
          if (OB_LS_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("ls not exist, skip it", K(ls_ids.at(i)));
          } else {
            LOG_WARN("failed to get ls", K(ret), K(ls_ids.at(i)));
          }
        } else if (OB_FAIL(loop_all_tablets_(*ls_handle.get_ls()))) {
          LOG_WARN("failed to process ss sstable gc", K(ret));
        }
        LOG_INFO("process for ls done", "ls_id", ls_ids.at(i));
      }
    }
    THIS_WORKER.set_timeout_ts(prev_timeout_ts);
  }
  if (processing_) {
    int64_t cost_ts = ObTimeUtility::fast_current_time() - start_ts;
    LOG_INFO("finish do ss-sstable-gc", K(cost_ts), K(start_ts));
    ATOMIC_STORE(&processing_, false);
  }
}

int ObSSSSTableGCProcessor::get_all_ls_ids_(ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  ObSSMetaIterGuard<ObSSMetaLSIDIter> iter_guard;
  if (OB_FAIL(MTL(ObSSMetaService *)->get_ls_id_iter(SCN::max_scn(), iter_guard))) {
    LOG_WARN("failed to get ls ids", K(ret));
  } else {
    ObSSMetaLSIDIter *iter = NULL;
    if (OB_FAIL(iter_guard.get_iter(iter))) {
      LOG_WARN("failed to get ls id iter", K(ret));
    } else {
      ObLSID ls_id;
      while (OB_SUCC(ret) && OB_SUCC(iter->get_next(ls_id))) {
        if (OB_FAIL(ls_ids.push_back(ls_id))) {
          LOG_WARN("failed to push back ls id", K(ret), K(ls_id));
        }
      }
      if (OB_ITER_END == ret || OB_INVALID_QUERY_TIMESTAMP == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  LOG_INFO("get all ls ids", K(ret),K(ls_ids));
  return ret;
}

int ObSSSSTableGCProcessor::loop_all_tablets_(ObLS &ls)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id = ls.get_ls_id();
  ObSSMetaReadParamV2 read_param;
  read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                ObSSMetaReadResultType::READ_WHOLE_ROW,
                                ObAtomicProtocolTypeEnum::TABLET_META,
                                ls_id,
                                share::SCN::min_scn(),
                                true);
  read_param.set_read_mark_delete(true);
  ObSSMetaIterGuard<ObSSMetaTabletIter> iter_guard;
  ObSSMetaTabletIter *iter = NULL;
  if (OB_FAIL(MTL(ObSSMetaService *)->get_tablet_iter(read_param, SCN::max_scn(), iter_guard))) {
    LOG_WARN("failed to get tablet ids", K(ret));
  } else if (OB_FAIL(iter_guard.get_iter(iter))) {
    LOG_WARN("failed to get tablet iter", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    while (OB_SUCC(ret) && !stopped_ && ls.is_running()) {
      common::ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "SS_TableGC"));
      ObTabletHandle tablet_hdl;
      share::SCN meta_scn;
      share::SCN reorgan_scn;
      ObSSMetaUpdateMetaInfo update_meta_info;
      bool is_sswriter = false;
      int64_t sswriter_epoch = 0;
      ObTabletID tablet_id;
      bool is_mark_deleted = false;
      if (OB_SUCC(iter->get_next(allocator, tablet_hdl, update_meta_info, reorgan_scn, meta_scn))) {
        tablet_id = tablet_hdl.get_obj()->get_tablet_id();
        if (OB_TMP_FAIL(MTL(ObSSMetaService*)->check_tablet_mark_deleted(
                ls_id, tablet_id, tablet_hdl.get_obj()->get_reorganization_scn(), is_mark_deleted))) {
          LOG_WARN("check_tablet_mark_deleted failed", K(tmp_ret), K(ls_id), K(tablet_id));
        } else if (is_mark_deleted) {
          LOG_INFO("skip gc sstables for deleted tablet",
                   K(ls_id),
                   K(tablet_id),
                   "transfer_scn", tablet_hdl.get_obj()->get_reorganization_scn(),
                   "update_meta_info", update_meta_info);
        } else if (OB_TMP_FAIL(MTL(ObSSWriterService*)->check_lease(ObSSWriterKey(ObSSWriterType::COMPACTION, ls_id, tablet_id),
                                                                is_sswriter,
                                                                sswriter_epoch))) {
          LOG_WARN("failed to check is compaction sswriter", K(tmp_ret), K(ls_id), K(tablet_id));
        } else if (!is_sswriter) {
          LOG_TRACE("skip tablet because is not sswriter", K(ls_id), K(tablet_id));
        } else if (OB_TMP_FAIL(process_one_tablet_(ls, *tablet_hdl.get_obj(), update_meta_info.table_store_meta_info_))) {
          LOG_WARN("failed to process one tablet", K(tmp_ret), K(tablet_hdl), K(update_meta_info));
        } else {
          LOG_TRACE("process one tablet done", K(ls_id), K(tablet_id));
        }
      } else if (OB_ITER_END == ret || OB_INVALID_QUERY_TIMESTAMP == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next tablet", K(ret));
      }
    }
    if (!ls.is_running()) {
      LOG_INFO("ls is not running, stop ss sstable gc", K(ls_id), K(ls));
    }
  }
  return ret;
}

int ObSSSSTableGCProcessor::process_one_tablet_(ObLS &ls,
                                                const ObTablet &tablet,
                                                const ObSSTabletTableStoreMetaInfo &table_store_meta_info)
{
  int ret = OB_SUCCESS;
  const ObTabletID tablet_id = tablet.get_tablet_id();
  const ObLSID ls_id = ls.get_ls_id();
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  const ObTabletTableStore *table_store = nullptr;
  if (tablet.get_clog_checkpoint_scn().is_base_scn()) {
  } else if (table_store_meta_info.minor_trans_state_determined_ &&
             table_store_meta_info.major_tables_cnt_ <= 1) {
    LOG_TRACE("skip gc sstables", K(table_store_meta_info), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member(table_store))) {
    LOG_WARN("fail to get table store", K(ret), K(table_store_wrapper));
  } else if (table_store->get_table_count() > 0) {
    bool need_gc = false;
    bool need_decide_upper_trans_version = false;
    const ObSSTableArray &minor_tables = table_store->get_minor_sstables();
    for (int i = 0; i < minor_tables.count(); i++) {
      ObITable *table = minor_tables.at(i);
      if (!table->is_trans_state_deterministic()) {
        need_decide_upper_trans_version = true;
        share::SCN utv;
        if (OB_FAIL(ls.get_upper_trans_version_before_given_scn(table->get_end_scn(), utv))) {
          LOG_WARN("get upper trans version fail", K(table->get_key()));
        } else if (!utv.is_max()) {
          need_gc = true;
          LOG_INFO("SS GC SSTABLE: there are minor upper trans version can be decided", K(tablet_id), K(ls_id));
          break;
        }
      }
    }
    int64_t multi_version_start = tablet.get_multi_version_start();
    if (OB_SUCC(ret) && !need_decide_upper_trans_version) {
      // look at major and decide need gc major sstables
      const ObSSTableArray &major_tables = table_store->get_major_sstables();
      if (major_tables.count() > 1) {
        ObTabletHandle local_tablet;
        if (OB_FAIL(ls.get_tablet_svr()->get_tablet(tablet.get_tablet_id(), local_tablet))) {
          LOG_WARN("get local tablet fail", K(ret), K(tablet_id), K(ls_id));
        } else if (tablet.get_reorganization_scn() != local_tablet.get_obj()->get_reorganization_scn()) {
          ret = OB_EAGAIN;
          LOG_WARN("shared tablet reorg_scn not match with local, skip", K(ret),
                   K(ls_id), K(tablet_id),
                   K(tablet.get_reorganization_scn()),
                   K(local_tablet.get_obj()->get_reorganization_scn()));
        } else if (local_tablet.get_obj()->get_multi_version_start() >= major_tables.at(1)->get_snapshot_version()) {
          multi_version_start = local_tablet.get_obj()->get_multi_version_start();
          LOG_INFO("SS GC SSTABLE: can recycle shared major because multi version start >= their snapshot version",
                   K(tablet_id), K(ls_id), K(multi_version_start));
          need_gc = true;
        }
      }
    }
    if (OB_SUCC(ret) && need_gc) {
      ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "SSGCSSTable"));
      ObStorageSchema *schema = NULL;
      if (OB_FAIL(tablet.load_storage_schema(allocator, schema))) {
        LOG_WARN("failed to load storage schema", K(ret), K(tablet));
      } else {
        ObUpdateTableStoreParam param(tablet.get_snapshot_version(),
                                      multi_version_start,
                                      schema,
                                      ls.get_rebuild_seq(),
                                      NULL);
        const ObArray<ObSSMetaCtrlKeyInfo> empty_ctrl_keys;
        if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(ls_id,
                                                                       tablet_id,
                                                                       tablet.get_reorganization_scn(),
                                                                       ObMetaUpdateReason::TABLET_GC_SSTABLE,
                                                                       param,
                                                                       empty_ctrl_keys,
                                                                       false))) {
          LOG_WARN("update table store fail v2", K(ret));
        } else {
          LOG_INFO("SS GC SSTABLE: succ update table store", K(ls_id), K(tablet_id));
	  EVENT_INC(ObStatEventIds::SS_GC_TABLET_SSTABLE_COUNT);
        }
      }
      ObTabletObjLoadHelper::free(allocator, schema);
    }
  }
  if (OB_FAIL(ret) && OB_EAGAIN != ret && OB_NO_NEED_MERGE != ret) {
    ADD_SS_DIAGNOSE_INFO(storage::ObSSDiagnoseInfo::DIA_TYPE_SSTABLE_GC,
                         ls_id,
                         tablet_id,
                         ObTimeUtility::current_time_us(),
                         "ret", ret,
                         "ls_id", ls_id,
                         "tablet_id", tablet_id,
                         "transfer_scn", tablet.get_reorganization_scn(),
                         "trace_id", *common::ObCurTraceId::get_trace_id());
  }
  return ret;
}
}
}
