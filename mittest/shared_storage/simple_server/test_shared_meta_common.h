/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_MITTEST_SHARED_STORAGE_TEST_SHARED_META_COMMON_H_
#define OCEANBASE_MITTEST_SHARED_STORAGE_TEST_SHARED_META_COMMON_H_

#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>
#include <map>

#define protected public
#define private public
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_reader.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
#include "src/storage/incremental/ssmeta/ob_ss_ls_ckpt_task.h"
#include "src/storage/incremental/ssmeta/ob_ss_ls_ckpt_dag.h"
#include "lib/guard/ob_scope_guard.h"
#include "lib/string/ob_string_buffer.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/utility/ob_test_util.h"
#include "share/ob_cluster_version.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#define protected public
#define private public
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ob_ss_tablet_merge_helper.h"
#include "src/storage/incremental/ob_sswriter_service.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#undef private
#define private public
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_writer.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#undef private
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_data.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_helper.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_info_iterator.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_util.h"
#include "storage/incremental/sslog/ob_sslog_kv_palf_adapter.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blockstore/ob_shared_object_reader_writer.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "share/io/ob_io_define.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "src/storage/tiered_metadata_store/ob_tiered_metadata_store.h"
#include "logservice/ob_garbage_collector.h"
#undef protected

using namespace oceanbase::transaction;
using namespace oceanbase::storage;
using oceanbase::common::serialization::encode_vi64;
using oceanbase::common::serialization::encode_fixed_bytes_i64;
using oceanbase::common::serialization::encoded_length_vi64;

// Shared by both test translation units; definitions live in test_shared_meta_service.cpp.
extern int64_t lease_epoch;
extern bool mock_sswriter_enabled;
extern thread_local int64_t check_lease_call_count;
extern thread_local bool mock_is_sswriter;
extern thread_local bool mock_sswriter_addr_valid;

namespace oceanbase
{
extern char *shared_storage_info;
extern bool new_tenant_created;
namespace unittest
{

static constexpr int64_t SYS_CKPT_STARTUP_THREAD_SCORE = 32;
static constexpr char SYS_CKPT_STARTUP_OPTION[] =
    "sys_ckpt_thread_score=32";

struct TestRunCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};

class NamedEventGuard
{
public:
  NamedEventGuard() : name_(nullptr), event_item_(nullptr), initial_occur_(0), is_set_(false) {}
  ~NamedEventGuard() { reset(); }

  int init(const char *name, const int64_t error_code, const int64_t occur)
  {
    return init_(name, error_code, occur, 0 /*trigger_freq*/, GETTID());
  }

  int init_always_for_all_threads(const char *name, const int64_t error_code)
  {
    return init_(name, error_code, 0 /*occur*/, 1 /*trigger_freq*/, 0 /*cond*/);
  }

  int init_for_all_threads(const char *name, const int64_t error_code, const int64_t occur)
  {
    return init_(name, error_code, occur, 0 /*trigger_freq*/, 0 /*cond*/);
  }

  int init_for_condition(const char *name, const int64_t error_code, const int64_t occur, const int64_t cond)
  {
    return init_(name, error_code, occur, 0 /*trigger_freq*/, cond);
  }

  // Only verify bounded error injections; do not use events as general counters
  // or synchronization switches.
  int64_t get_trigger_count() const
  {
    return nullptr == event_item_ ? 0 : initial_occur_ - ATOMIC_LOAD(&event_item_->occur_);
  }

  void reset()
  {
    if (is_set_) {
      common::EventItem item;
      (void)common::EventTable::set_event(name_, item);
      name_ = nullptr;
      event_item_ = nullptr;
      initial_occur_ = 0;
      is_set_ = false;
    }
  }

private:
  int init_(const char *name,
            const int64_t error_code,
            const int64_t occur,
            const int64_t trigger_freq,
            const int64_t cond)
  {
    int ret = OB_SUCCESS;
    common::EventItem item;
    item.error_code_ = error_code;
    item.occur_ = occur;
    item.trigger_freq_ = trigger_freq;
    item.cond_ = cond;
    common::EventItem *event_item = nullptr;
    if (nullptr != name) {
      DLIST_FOREACH_NORET(i, common::EventTable::global_item_list()) {
        if (nullptr != i->item_.name_ && 0 == strcasecmp(i->item_.name_, name)) {
          event_item = &i->item_;
          break;
        }
      }
    }
    if (OB_FAIL(common::EventTable::set_event(name, item))) {
      LOG_WARN("fail to set named event", KR(ret), KCSTRING(name), K(error_code), K(occur));
    } else if (nullptr == event_item) {
      ret = OB_ERR_UNEXPECTED;
      common::EventItem empty_item;
      (void)common::EventTable::set_event(name, empty_item);
    } else {
      name_ = name;
      event_item_ = event_item;
      initial_occur_ = occur;
      is_set_ = true;
    }
    return ret;
  }
  const char *name_;
  common::EventItem *event_item_;
  int64_t initial_occur_;
  bool is_set_;
};

#if defined(ERRSIM) && __ENABLE_TRACEPOINT__
// Save and restore the previous callback.
// Async tests must drain the DAG before destroying this guard.
class ScopedTracepoint
{
public:
  ScopedTracepoint() : slot_(nullptr), saved_callback_(nullptr) {}
  ~ScopedTracepoint() { reset(); }
  ScopedTracepoint(const ScopedTracepoint &) = delete;
  ScopedTracepoint &operator=(const ScopedTracepoint &) = delete;

  int init(const char *file, const char *func, const char *key, void *callback)
  {
    int ret = OB_SUCCESS;
    if (nullptr != slot_) {
      ret = OB_INIT_TWICE;
    } else if (nullptr == file || nullptr == func || nullptr == key || nullptr == callback) {
      ret = OB_INVALID_ARGUMENT;
    } else if (nullptr == (slot_ = common::tracepoint_get(common::refine_tp_key(file, func, key)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      saved_callback_ = TP_AL(slot_);
      TP_AS(slot_, callback);
    }
    return ret;
  }

  void reset()
  {
    if (nullptr != slot_) {
      TP_AS(slot_, saved_callback_);
      slot_ = nullptr;
      saved_callback_ = nullptr;
    }
  }

private:
  void **slot_;
  void *saved_callback_;
};
#endif

/**
 * Isolate SYS LS checkpoints in mittest.
 *
 * simple_server runs automatic background checkpoints, while some tests using this fixture
 * directly manipulate LS_CKPT reservations, checkpoint objects, or PALF_KV. To prevent
 * background tasks from concurrently modifying this shared state, pause() performs these steps:
 * 1. Disable the mock SSWriter so new tasks fail the lease check.
 * 2. Advance the lease epoch to fence in-flight tasks that captured the old epoch.
 * 3. Cancel pending DAGs and wait for in-flight DAGs to exit cooperatively,
 *    establishing a stable test window.
 *
 * This guard only isolates tests. It neither pauses the entire DAG scheduler nor prevents
 * tests from explicitly running BOOTSTRAP checkpoints. Its destructor restores only the previous
 * mock SSWriter state, without rolling back the lease epoch,
 * so old tasks cannot become valid again.
 */
class SysLsCkptPauseGuard
{
public:
  SysLsCkptPauseGuard()
    : saved_sswriter_enabled_(false),
      is_paused_(false)
  {}

  ~SysLsCkptPauseGuard()
  {
    if (is_paused_) {
      ATOMIC_STORE(&mock_sswriter_enabled, saved_sswriter_enabled_);
    }
  }

  int pause()
  {
    int ret = OB_SUCCESS;
    share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler *);
    ObSSLsCkptDag dag;
    ObSSLsCkptDagParam dag_param(ObSSLsCkptMode::BOOTSTRAP, -1, SYS_LS);
    if (OB_UNLIKELY(is_paused_)) {
      ret = OB_INIT_TWICE;
      LOG_WARN("sys ls checkpoint scheduling has already been paused", KR(ret));
    } else if (OB_ISNULL(scheduler)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant dag scheduler is null", KR(ret));
    } else if (OB_FAIL(dag.init_by_param(&dag_param))) {
      LOG_WARN("init ls checkpoint dag failed", KR(ret));
    } else {
      saved_sswriter_enabled_ = ATOMIC_LOAD(&mock_sswriter_enabled);
      // New tasks fail the lease check; running tasks fail fast after the epoch changes.
      ATOMIC_STORE(&mock_sswriter_enabled, false);
      ATOMIC_INC(&lease_epoch);
      is_paused_ = true;

      static const int64_t WAIT_TIMEOUT_US = 30L * 1000L * 1000L;
      static const int64_t WAIT_INTERVAL_US = 10L * 1000L;
      const int64_t deadline_us = ObTimeUtility::current_time() + WAIT_TIMEOUT_US;
      bool dag_exist = true;
      bool is_emergency = false;
      while (OB_SUCC(ret) && dag_exist) {
        if (OB_FAIL(scheduler->cancel_dag(&dag, false /*force_cancel*/))) {
          LOG_WARN("cancel ls checkpoint dag failed", KR(ret));
        } else if (OB_FAIL(scheduler->check_dag_exist(
                       &dag, dag_exist, is_emergency))) {
          LOG_WARN("check ls checkpoint dag failed", KR(ret));
        } else if (dag_exist && ObTimeUtility::current_time() >= deadline_us) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait ls checkpoint dag exit timeout", KR(ret));
        } else if (dag_exist) {
          ob_usleep(WAIT_INTERVAL_US);
        }
      }
    }
    return ret;
  }

private:
  bool saved_sswriter_enabled_;
  bool is_paused_;
};

class TestSharedMetaService : public ObSimpleClusterTestBase
{
public:
  explicit TestSharedMetaService(const char *env_prefix = "test_shared_meta_service")
    : ObSimpleClusterTestBase(env_prefix, "50G", "50G", "50G")
  {}

  int exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  int sys_exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  // Change only the tiered meta row's type, leaving its value unchanged. This simulates a mismatch
  // between meta_type and the actual payload after backup restoration, verifying that checkpoints
  // do not depend on this field.
  int update_sys_tiered_meta_type(const blocksstable::MacroBlockId &macro_id,
                                  const int64_t meta_type)
  {
    int ret = OB_SUCCESS;
    char key_buf[common::MAX_PATH_SIZE] = {0};
    int64_t key_len = 0;
    ObSqlString sql;
    if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
                    macro_id, key_buf, sizeof(key_buf), key_len))) {
      LOG_WARN("failed to convert tiered meta macro id to path", KR(ret), K(macro_id));
    } else if (OB_FAIL(sql.assign_fmt(
                   "update oceanbase.__all_shared_tiered_meta set meta_type=%ld "
                   "where tenant_id=%lu and meta_key='%.*s'",
                   meta_type, OB_SYS_TENANT_ID, static_cast<int>(key_len), key_buf))) {
      LOG_WARN("failed to build tiered meta type update sql", KR(ret), K(macro_id), K(meta_type));
    } else if (OB_FAIL(sys_exe_sql(sql.ptr()))) {
      LOG_WARN("failed to update tiered meta type", KR(ret), K(macro_id), K(meta_type));
    }
    return ret;
  }

  int get_complete_ls_meta(const ObLSID &ls_id,
                           ObSSLSMeta &ls_meta,
                           share::SCN &meta_scn,
                           bool &is_mark_deleted)
  {
    int ret = OB_SUCCESS;
    ObSSMetaReadParamV2 read_param;
    read_param.set_read_mark_delete(true);
    read_param.set_ls_level_param(
        ObSSMetaReadParamType::LS_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::LS_META,
        ls_id,
        share::SCN::max_scn() /*meta_scn*/,
        true /*is_in_table*/);
    SMART_VAR(ObDefaultSSMetaValue<ObSSLSMeta>, ls_value) {
      if (OB_FAIL(ObSSMetaTableHelper::get_ls(
              read_param, share::SCN::max_scn() /*read_snapshot*/, ls_value, meta_scn))) {
        LOG_WARN("fail to get complete ls meta", KR(ret), K(ls_id));
      } else {
        ls_meta = ls_value.meta_value_;
        is_mark_deleted = ls_value.get_meta_head_info().is_mark_deleted();
      }
    }
    return ret;
  }

  void wait_ls_safe_destroy(ObLSService *ls_svr, const ObLSID &ls_id)
  {
    ASSERT_NE(nullptr, ls_svr);
    logservice::ObGarbageCollector *gc_service =
        MTL(logservice::ObGarbageCollector *);
    ASSERT_NE(nullptr, gc_service);

    // The GC thread normally processes safe-destroy tasks every 10 seconds.
    // Drive the queue explicitly so fast consecutive cases do not exhaust the
    // tenant LS limit while removed LS objects are waiting for reclamation.
    bool waiting = true;
    for (int64_t i = 0; i < 100 && waiting; ++i) {
      gc_service->safe_destroy_handler_.queue_.loop();
      ASSERT_EQ(OB_SUCCESS,
                ls_svr->check_ls_waiting_safe_destroy(ls_id, waiting));
      if (waiting) {
        ob_usleep(100 * 1000);
      }
    }
    ASSERT_FALSE(waiting);
  }

  int replay_offline_log(logservice::ObGCHandler *gc_handler,
                         const share::SCN &offline_scn)
  {
    int ret = OB_SUCCESS;
    char log_buf[1024];
    int64_t pos = 0;
    logservice::ObGCLSLog gc_log(logservice::ObGCLSLOGType::OFFLINE_LS);
    const palf::LSN replay_lsn(0);
    if (OB_ISNULL(gc_handler) || OB_UNLIKELY(!offline_scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(gc_log.serialize(log_buf, sizeof(log_buf), pos))) {
      LOG_WARN("fail to serialize offline ls log", KR(ret), K(offline_scn));
    } else if (OB_FAIL(gc_handler->replay(log_buf, pos, replay_lsn, offline_scn))) {
      LOG_WARN("fail to replay offline ls log", KR(ret), K(offline_scn));
    }
    return ret;
  }

  virtual void SetUp() override
  {
    int ret = OB_SUCCESS;
    check_lease_call_count = 0;
    mock_is_sswriter = true;
    mock_sswriter_addr_valid = true;
    ObSimpleClusterTestBase::SetUp();
    if (!new_tenant_created) {
      LOG_INFO("start to create tt1 tenant");
      OK(create_tenant_with_retry("tt1", "5G", "10G", false/*oracle_mode*/, 8));
      OK(get_curr_simple_server().init_sql_proxy2());
      new_tenant_created = true;
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
      {
        share::ObTenantSwitchGuard tguard;
        OK(tguard.switch_to(run_ctx_.tenant_id_));
        OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
        OK(sys_exe_sql("alter system set _shared_advance_checkpoint_interval = '1m' tenant tt1;"));
        OK(sys_exe_sql("alter system set_tp tp_name = EN_COMPACTION_SS_MINOR_MERGE_FAST_SKIP,error_code = 4016,frequency = 1;"));
        // clean up stale test data from previous runs
        exe_sql("delete from oceanbase.__all_shared_meta where meta_type = 2");
        // create a table via SQL to get a local tablet
        OK(exe_sql("create table if not exists test_svc2_table (a int)"));
        LOG_INFO("create table test_svc2_table done");
      }
      LOG_INFO("finish to create tt1 tenant");
    } else {
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
    }
    // Stop the LS garbage collector. Test cases create LS directly via
    // ObLSService::create_ls without registering them in __all_ls_status, so the
    // GC thread would treat them as orphaned (LS_STATUS_ENTRY_NOT_EXIST) and
    // remove the LS together with its inner tablets, racing with the subsequent
    // ObSSMetaService::create_ls and causing OB_TABLET_NOT_EXIST.
    {
      share::ObTenantSwitchGuard tguard;
      OK(tguard.switch_to(run_ctx_.tenant_id_));
      logservice::ObGarbageCollector *gc_service = MTL(logservice::ObGarbageCollector *);
      ASSERT_NE(nullptr, gc_service);
      gc_service->stop_create_new_gc_task_ = true;
    }
  }

  static void TearDownTestCase();

  // Build a buffer whose layout matches what the production write path
  // (ObSSTabletMetaValue::serialize) produces:
  //   [outer_version (vi64)] [outer_len (5-byte fixed)] [HeadValue::serialize output]
  // so that ObSSMetaHeadValue::deserialize_value_info / check_mark_deleted
  // can decode the head section.  We isolate these rows via synthetic ls_ids.
  int build_head_value_str(ObIAllocator &alloc,
                           const ObMetaUpdateReason reason,
                           ObString &out_str,
                           const bool mark_delete = false)
  {
    int ret = OB_SUCCESS;
    ObSSMetaHeadValueInfo head_info;
    ObSSMetaHeadValue head_value;
    char *buf = nullptr;
    int64_t pos = 0;
    int64_t inner_ser_size = 0;
    int64_t total_ser_size = 0;
    const int64_t OUTER_VERSION = 1;
    if (OB_FAIL(head_info.set(reason, mark_delete))) {
      LOG_WARN("fail to set head info", KR(ret), K(reason));
    } else if (OB_FAIL(head_value.set_meta_head_info(head_info))) {
      LOG_WARN("fail to set meta head info", KR(ret));
    } else if (FALSE_IT(inner_ser_size = head_value.get_serialize_size())) {
    } else if (OB_UNLIKELY(inner_ser_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid ser size", KR(ret), K(inner_ser_size));
    } else {
      total_ser_size = inner_ser_size
                       + encoded_length_vi64(OUTER_VERSION)
                       + oceanbase::common::serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(total_ser_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc head value buf failed", KR(ret), K(total_ser_size));
    } else if (OB_FAIL(encode_vi64(buf, total_ser_size, pos, OUTER_VERSION))) {
      LOG_WARN("fail to encode outer version", KR(ret));
    } else {
      int64_t size_nbytes = oceanbase::common::serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
      int64_t pos_bak = (pos += size_nbytes);
      if (OB_FAIL(head_value.serialize(buf, total_ser_size, pos))) {
        LOG_WARN("fail to serialize head value", KR(ret));
      } else {
        int64_t serial_size = pos - pos_bak;
        int64_t tmp_pos = 0;
        if (OB_FAIL(encode_fixed_bytes_i64(
                buf + pos_bak - size_nbytes, size_nbytes, tmp_pos, serial_size))) {
          LOG_WARN("fail to encode outer length", KR(ret));
        } else {
          out_str.assign_ptr(buf, static_cast<int32_t>(pos));
        }
      }
    }
    return ret;
  }

  // Delete a single tablet_meta row (by full primary key) so a test case can
  // clean up its synthetic rows from __all_shared_meta on success path.
  int delete_tablet_meta_row(ObSSMetaService &meta_svr,
                             const ObSSMetaTabletKey &key)
  {
    ObSSMetaDeleteParamV2 delete_param;
    delete_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_KEY,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        key.ls_id_, key.tablet_id_, key.meta_scn_, key.reorganization_scn_,
        true /*is_in_table*/);
    return meta_svr.delete_meta_row(delete_param);
  }

  int make_scn(const uint64_t scn_val, share::SCN &scn)
  {
    scn.reset();
    return scn.convert_for_inner_table_field(scn_val);
  }

  int insert_ls_ckpt_row(ObSSMetaService &meta_svr,
                         const ObLSID &ls_id,
                         const share::SCN &meta_scn,
                         const bool mark_delete)
  {
    int ret = OB_SUCCESS;
    ObSSMetaLSCkptKey key;
    ObArenaAllocator allocator("InsLsCkpt");
    ObString value;
    char path_buf[SSMETA_PATH_MAX_LEN] = {0};
    int64_t path_pos = 0;
    key.ls_id_ = ls_id;
    key.meta_scn_ = meta_scn;
    const blocksstable::MacroBlockId macro_id = key.convert_to_macro_id(true/*is_in_table*/);
    if (OB_UNLIKELY(!key.is_valid() || !macro_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ls ckpt key", KR(ret), K(key), K(macro_id));
    } else if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
                   macro_id, path_buf, sizeof(path_buf), path_pos))) {
      LOG_WARN("fail to format ls ckpt key", KR(ret), K(key), K(macro_id));
    } else if (OB_FAIL(build_head_value_str(
                   allocator, ObMetaUpdateReason::CREATE_LS_CKPT, value, mark_delete))) {
      LOG_WARN("fail to build ls ckpt value", KR(ret), K(key));
    } else {
      const ObString meta_key(static_cast<int32_t>(path_pos), path_buf);
      const ObSSRawMetaRow row(MTL_ID(), ls_id,
          static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT), meta_key, value);
      if (OB_FAIL(meta_svr.insert_raw_meta(row))) {
        LOG_WARN("fail to insert ls ckpt row", KR(ret), K(key));
      }
    }
    return ret;
  }

  int delete_ls_ckpt_row(ObSSMetaService &meta_svr,
                         const ObLSID &ls_id,
                         const share::SCN &meta_scn)
  {
    ObSSMetaDeleteParamV2 delete_param;
    delete_param.set_ls_level_param(
        ObSSMetaReadParamType::LS_KEY,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::CHECKPOINT,
        ls_id, meta_scn, true/*is_in_table*/);
    return meta_svr.delete_meta_row(delete_param);
  }

  int check_ls_ckpt_row_exist(ObSSMetaService &meta_svr,
                              const ObLSID &ls_id,
                              const share::SCN &meta_scn,
                              bool &exist)
  {
    int ret = OB_SUCCESS;
    ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> iter_guard;
    ObSSMetaLSCkptKeyIter *iter = nullptr;
    ObSSMetaLSCkptKey key;
    exist = false;
    if (OB_FAIL(meta_svr.get_ls_ckpt_iter(
            ls_id, meta_scn, true/*read_mark_delete*/, iter_guard))) {
      LOG_WARN("fail to get ls ckpt iter", KR(ret), K(ls_id), K(meta_scn));
    } else if (OB_FAIL(iter_guard.get_iter(iter))) {
      LOG_WARN("fail to get ls ckpt iterator", KR(ret), K(ls_id), K(meta_scn));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls ckpt iterator is null", KR(ret), K(ls_id), K(meta_scn));
    } else if (OB_FAIL(iter->get_next(key))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next ls ckpt key", KR(ret), K(ls_id), K(meta_scn));
      }
    } else {
      // The iterator is ordered by meta_scn descending and bounded by
      // meta_scn, so an exact match exists iff the first key equals it.
      exist = (key.ls_id_ == ls_id && key.meta_scn_ == meta_scn);
    }
    return ret;
  }

  int get_latest_ls_ckpt_scn(ObSSMetaService &meta_svr,
                             const ObLSID &ls_id,
                             share::SCN &meta_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> iter_guard;
    ObSSMetaLSCkptKeyIter *iter = nullptr;
    ObSSMetaLSCkptKey key;
    meta_scn.reset();
    if (OB_FAIL(meta_svr.get_ls_ckpt_iter(
            ls_id, share::SCN::max_scn(), false /*read_mark_delete*/, iter_guard))) {
      LOG_WARN("fail to get latest ls ckpt iter", KR(ret), K(ls_id));
    } else if (OB_FAIL(iter_guard.get_iter(iter))) {
      LOG_WARN("fail to get latest ls ckpt iterator", KR(ret), K(ls_id));
    } else if (OB_ISNULL(iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("latest ls ckpt iterator is null", KR(ret), K(ls_id));
    } else if (OB_FAIL(iter->get_next(key))) {
      LOG_WARN("fail to get latest ls ckpt key", KR(ret), K(ls_id));
    } else if (OB_UNLIKELY(key.ls_id_ != ls_id || !key.meta_scn_.is_valid())) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid latest ls ckpt key", KR(ret), K(ls_id), K(key));
    } else {
      meta_scn = key.meta_scn_;
    }
    return ret;
  }

  int wait_ls_ckpt_write_dag_exit(ObSSLsCkptExecutor &executor)
  {
    int ret = OB_SUCCESS;
    // NORMAL uses one upload task for about 1,700 tablets against local MinIO in simple_server.
    // Repeated runs can exceed 180 seconds; extend only the test wait, not the DAG timeout.
    static const int64_t WAIT_TIMEOUT_US = 300L * 1000L * 1000L;
    static const int64_t WAIT_INTERVAL_US = 10L * 1000L;
    const int64_t deadline_us = ObTimeUtility::current_time() + WAIT_TIMEOUT_US;
    bool dag_exist = true;
    while (OB_SUCC(ret) && dag_exist) {
      if (OB_FAIL(executor.check_ckpt_dag_exist_(dag_exist))) {
        LOG_WARN("check ls checkpoint write dag failed", KR(ret));
      } else if (dag_exist && ObTimeUtility::current_time() >= deadline_us) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait ls checkpoint write dag exit timeout", KR(ret));
      } else if (dag_exist) {
        ob_usleep(WAIT_INTERVAL_US);
      }
    }
    return ret;
  }

  int check_tablet_meta_row_exist(ObSSMetaService &meta_svr,
                                  const ObSSMetaTabletKey &key,
                                  bool &exist)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator("ChkTbltMeta");
    ObSSMetaTabletRow row;
    exist = false;
    if (OB_FAIL(meta_svr.read_tablet_meta(key, allocator, row))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to read tablet meta row", KR(ret), K(key));
      }
    } else {
      exist = true;
    }
    return ret;
  }

  int insert_test_tablet_meta_row(ObSSMetaService &meta_svr,
                                  const ObSSMetaTabletKey &key)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator("InsTbltMeta");
    ObString value;
    if (OB_FAIL(build_head_value_str(
            allocator, ObMetaUpdateReason::CREATE_TABLET, value))) {
      LOG_WARN("fail to build tablet meta value", KR(ret), K(key));
    } else {
      const ObSSMetaTabletRow row(
          static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key, value);
      if (OB_FAIL(meta_svr.insert_tablet_meta(row))) {
        LOG_WARN("fail to insert tablet meta row", KR(ret), K(key));
      }
    }
    return ret;
  }

  int write_test_object(const blocksstable::MacroBlockId &macro_id,
                        const char *buf,
                        const int64_t size,
                        const bool need_align)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator("WriteTestObj");
    const char *write_buf = buf;
    int64_t write_size = size;
    if (OB_UNLIKELY(!macro_id.is_valid() || OB_ISNULL(buf) || size <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid test object", KR(ret), K(macro_id), KP(buf), K(size));
    } else if (need_align) {
      write_size = upper_align(size, DIO_READ_ALIGN_SIZE);
      if (write_size != size) {
        char *align_buf = static_cast<char *>(allocator.alloc(write_size));
        if (OB_ISNULL(align_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc aligned object buffer", KR(ret), K(write_size));
        } else {
          MEMSET(align_buf, 0, write_size);
          MEMCPY(align_buf, buf, size);
          write_buf = align_buf;
        }
      }
    }
    if (OB_SUCC(ret)) {
      blocksstable::ObStorageObjectWriteInfo write_info;
      blocksstable::ObStorageObjectHandle handle;
      write_info.buffer_ = write_buf;
      write_info.offset_ = 0;
      write_info.size_ = write_size;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      write_info.io_desc_.set_unsealed();
      write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
      write_info.mtl_tenant_id_ = MTL_ID();
      write_info.set_ls_epoch_id(0);
      if (OB_FAIL(OB_STORAGE_OBJECT_MGR.async_write_object(macro_id, write_info, handle))) {
        LOG_WARN("fail to async write test object", KR(ret), K(macro_id));
      } else if (OB_FAIL(handle.wait())) {
        LOG_WARN("fail to wait test object write", KR(ret), K(macro_id));
      }
    }
    return ret;
  }

  int delete_test_object(const blocksstable::MacroBlockId &macro_id)
  {
    int ret = OB_STORAGE_OBJECT_MGR.delete_object(macro_id, 0/*ls_epoch_id*/);
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    return ret;
  }

  int check_test_object_exist(const blocksstable::MacroBlockId &macro_id,
                              bool &exist)
  {
    return OB_STORAGE_OBJECT_MGR.ss_is_exist_object(macro_id, 0/*ls_epoch*/, exist);
  }

  int get_tablet_and_ls_id(const char *table_name,
                           ObTabletID &tablet_id,
                           ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    uint64_t uid = 0;
    int64_t id = 0;
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    if (OB_FAIL(sql.assign_fmt(
            "select tablet_id from oceanbase.__all_virtual_table where table_name='%s';",
            table_name))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
        if (OB_FAIL(sql_proxy.read(res1, sql.ptr()))) {
          LOG_WARN("fail to read tablet_id", KR(ret));
        } else {
          sqlclient::ObMySQLResult *result = res1.get_result();
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(result->next())) {
            LOG_WARN("fail to get next row", KR(ret));
          } else if (OB_FAIL(result->get_uint("tablet_id", uid))) {
            LOG_WARN("fail to get tablet_id", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      tablet_id = ObTabletID(uid);
      sql.reset();
      if (OB_FAIL(sql.assign_fmt(
              "select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;",
              uid))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
          if (OB_FAIL(sql_proxy.read(res2, sql.ptr()))) {
            LOG_WARN("fail to read ls_id", KR(ret));
          } else {
            sqlclient::ObMySQLResult *result = res2.get_result();
            if (OB_ISNULL(result)) {
              ret = OB_ERR_UNEXPECTED;
            } else if (OB_FAIL(result->next())) {
              LOG_WARN("fail to get next row", KR(ret));
            } else if (OB_FAIL(result->get_int("ls_id", id))) {
              LOG_WARN("fail to get ls_id", KR(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ls_id = ObLSID(id);
      }
    }
    return ret;
  }

  // Same as get_tablet_and_ls_id() but resolves a table created in the *sys*
  // tenant via the sys sql proxy (get_sql_proxy()).  Used by the sys-tenant
  // sys-tenant tiered-meta cases below.
  int get_sys_tablet_and_ls_id(const char *table_name,
                               ObTabletID &tablet_id,
                               ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    uint64_t uid = 0;
    int64_t id = 0;
    common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    if (OB_FAIL(sql.assign_fmt(
            "select tablet_id from oceanbase.__all_virtual_table where table_name='%s';",
            table_name))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
        if (OB_FAIL(sql_proxy.read(res1, sql.ptr()))) {
          LOG_WARN("fail to read tablet_id", KR(ret));
        } else {
          sqlclient::ObMySQLResult *result = res1.get_result();
          if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(result->next())) {
            LOG_WARN("fail to get next row", KR(ret));
          } else if (OB_FAIL(result->get_uint("tablet_id", uid))) {
            LOG_WARN("fail to get tablet_id", KR(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      tablet_id = ObTabletID(uid);
      sql.reset();
      if (OB_FAIL(sql.assign_fmt(
              "select ls_id from oceanbase.__all_tablet_to_ls where tablet_id=%ld;",
              uid))) {
        LOG_WARN("fail to assign sql", KR(ret));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, res2) {
          if (OB_FAIL(sql_proxy.read(res2, sql.ptr()))) {
            LOG_WARN("fail to read ls_id", KR(ret));
          } else {
            sqlclient::ObMySQLResult *result = res2.get_result();
            if (OB_ISNULL(result)) {
              ret = OB_ERR_UNEXPECTED;
            } else if (OB_FAIL(result->next())) {
              LOG_WARN("fail to get next row", KR(ret));
            } else if (OB_FAIL(result->get_int("ls_id", id))) {
              LOG_WARN("fail to get ls_id", KR(ret));
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ls_id = ObLSID(id);
      }
    }
    return ret;
  }

  int cleanup_sys_tablet_meta(ObSSMetaService &meta_svr,
                              ObTieredMetadataStore &tiered_store,
                              const ObLSID &ls_id,
                              const ObTabletID &tablet_id,
                              const share::SCN &reorgan_scn)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid()
                    || !reorgan_scn.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid sys tablet meta cleanup argument",
               KR(ret), K(ls_id), K(tablet_id), K(reorgan_scn));
    } else {
      ObSSMetaDeleteParamV2 delete_param;
      delete_param.set_tablet_level_param(
          ObSSMetaReadParamType::TABLET_PREFIX,
          ObSSMetaReadResultType::READ_WHOLE_ROW,
          ObAtomicProtocolTypeEnum::TABLET_META,
          ls_id, tablet_id, share::SCN::min_scn(), reorgan_scn,
          true /*is_in_table*/);
      int tmp_ret = meta_svr.delete_meta_row(delete_param);
      if (OB_SUCCESS != tmp_ret && OB_ENTRY_NOT_EXIST != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("cleanup sys tablet meta row failed",
                 KR(tmp_ret), K(ls_id), K(tablet_id), K(reorgan_scn));
      }

      ObArray<blocksstable::MacroBlockId> stale_ids;
      tmp_ret = tiered_store.list_meta_macro_files(
          tablet_id.is_ls_inner_tablet(), ls_id.id(), tablet_id.id(),
          reorgan_scn.get_val_for_tx(), stale_ids);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("list stale sys tablet sub-meta failed",
                 KR(tmp_ret), K(ls_id), K(tablet_id), K(reorgan_scn));
      } else if (!stale_ids.empty()
                 && OB_SUCCESS != (tmp_ret = tiered_store.batch_del_meta(stale_ids))) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("cleanup stale sys tablet sub-meta failed",
                 KR(tmp_ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(stale_ids));
      }

      ObArray<blocksstable::MacroBlockId> remaining_ids;
      tmp_ret = tiered_store.list_meta_macro_files(
          tablet_id.is_ls_inner_tablet(), ls_id.id(), tablet_id.id(),
          reorgan_scn.get_val_for_tx(), remaining_ids);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("verify sys tablet sub-meta cleanup failed",
                 KR(tmp_ret), K(ls_id), K(tablet_id), K(reorgan_scn));
      } else if (!remaining_ids.empty()) {
        tmp_ret = OB_ERR_UNEXPECTED;
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
        LOG_WARN("sys tablet sub-meta remains after cleanup",
                 KR(tmp_ret), K(ls_id), K(tablet_id), K(reorgan_scn), K(remaining_ids));
      }
    }
    return ret;
  }

protected:
  static TestRunCtx run_ctx_;
};

} // namespace unittest
} // namespace oceanbase

#endif // OCEANBASE_MITTEST_SHARED_STORAGE_TEST_SHARED_META_COMMON_H_
