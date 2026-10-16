/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <unistd.h>

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/time/ob_time_utility.h"

#define private public
#define protected public
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ss_garbage_collector/ob_ss_gc_task_runner.h"
#undef protected
#undef private
#include "share/backup/ob_backup_io_adapter.h"

namespace oceanbase
{
namespace storage
{
using share::OB_INVALID_DEST_ID;
}
}

#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/shared_storage/ob_remote_io_adapter.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/ss_data_check/ob_ss_data_check_dag.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_producer.h"
#include "storage/ss_data_check/ob_ss_data_check_meta_ref_checker.h"
#include "storage/tablet/ob_sstablet_persister.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{

char *shared_storage_info = nullptr;
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT

namespace storage
{

using namespace blocksstable;
using namespace common;
using namespace share;

class DataCheckNeverStopChecker final
    : public ObCheckStopProvider
{
public:
  virtual int check_stop() const override { return OB_SUCCESS; }
};

bool ObSSTabletPersister::write_to_meta_table_()
{
  // 测试进程全程固定写入对象存储，避免一次持久化过程切换写入策略。
  return false;
}

class ObSSDataCheckExpectedSnapshotStat final
{
public:
  ObSSDataCheckExpectedSnapshotStat()
    : target_data_item_(),
      expected_count_(0),
      target_expected_count_(0),
      target_version_count_(0),
      missing_count_(0)
  {}

  void reset()
  {
    target_data_item_.reset();
    expected_count_ = 0;
    target_expected_count_ = 0;
    target_version_count_ = 0;
    missing_count_ = 0;
  }

  bool is_ready() const
  {
    return expected_count_ > 0
        && target_expected_count_ > 0
        && target_data_item_.is_valid()
        && 0 == missing_count_;
  }

  TO_STRING_KV(K_(target_data_item), K_(expected_count),
      K_(target_expected_count), K_(target_version_count), K_(missing_count));

  ObSSDataCheckExpectedItem target_data_item_;
  int64_t expected_count_;
  int64_t target_expected_count_;
  int64_t target_version_count_;
  int64_t missing_count_;
};

class ScopedDataCheckListDebugSync final
{
public:
  explicit ScopedDataCheckListDebugSync(
      unittest::ObSimpleClusterTestBase &test)
    : test_(test), is_armed_(false)
  {}

  ~ScopedDataCheckListDebugSync()
  {
    const int ret = release();
    if (OB_SUCCESS != ret) {
      LOG_WARN("failed to release data check LIST debug sync", KR(ret));
    }
  }

  int arm();
  int wait_until_reached();
  int release();

private:
  int execute_sys_sql_(const char *sql);

private:
  unittest::ObSimpleClusterTestBase &test_;
  bool is_armed_;
  DISALLOW_COPY_AND_ASSIGN(ScopedDataCheckListDebugSync);
};

class ScopedDataCheckRunnerCleanup final
{
public:
  explicit ScopedDataCheckRunnerCleanup(ObSSGCTaskRunner &runner)
    : runner_(runner), is_started_(false)
  {}

  ~ScopedDataCheckRunnerCleanup()
  {
    cleanup();
  }

  void mark_started() { is_started_ = true; }

  void cleanup()
  {
    if (is_started_) {
      runner_.stop();
      is_started_ = false;
    }
    runner_.destroy();
  }

private:
  ObSSGCTaskRunner &runner_;
  bool is_started_;
  DISALLOW_COPY_AND_ASSIGN(ScopedDataCheckRunnerCleanup);
};

class TestSSDataCheckE2E : public unittest::ObSimpleClusterTestBase
{
public:
  static constexpr int64_t MEMORY_LIMIT = 8 * 1024 * 1024;
  static constexpr int64_t FILE_BUF_SIZE = 32 * 1024;
  static constexpr int64_t EXPECTED_READY_TIMEOUT_US = 300L * 1000 * 1000;
  static constexpr int64_t DAG_TIMEOUT_US = 300L * 1000 * 1000;
  static constexpr int64_t EVENT_TIMEOUT_US = 30L * 1000 * 1000;
  static constexpr int64_t OBJECT_DELETE_TIMEOUT_US = 30L * 1000 * 1000;

  TestSSDataCheckE2E()
    : unittest::ObSimpleClusterTestBase(
        "test_ss_data_check_e2e", "50G", "50G", "50G")
  {}
  virtual ~TestSSDataCheckE2E() = default;

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    unittest::ObSimpleClusterTestBase::TearDownTestCase();
  }

  int wait_sys_ls_leader_();
  int execute_tenant_sql_(const char *sql_str);
  int get_tablet_id_(const char *table_name, ObTabletID &tablet_id);
  int inspect_expected_snapshot_(
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const ObTabletID &target_tablet_id,
      const ObSSDataCheckMode check_mode,
      ObSSDataCheckExpectedSnapshotStat &stat);
  int get_target_tablet_row_(
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const ObTabletID &target_tablet_id,
      ObIAllocator &allocator,
      ObSSRawMetaRow &target_row);
  int get_table_store_expected_item_(
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const ObTabletID &target_tablet_id,
      ObSSDataCheckExpectedItem &item,
      ObMetaDiskAddr &table_store_addr);
  int get_tablet_expand_ret_(
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const ObTabletID &target_tablet_id,
      int &expand_ret);
  int evict_shared_object_caches_(
      const uint64_t tenant_id,
      const MacroBlockId &macro_id,
      const ObMetaDiskAddr &meta_addr);
  int wait_ready_snapshot_(
      const uint64_t tenant_id,
      const ObTabletID &target_tablet_id,
      SCN &snapshot_meta_scn,
      ObSSDataCheckExpectedSnapshotStat &stat);
  int wait_full_snapshot_ready_(
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const ObTabletID &target_tablet_id,
      const int64_t single_target_version_count,
      ObSSDataCheckExpectedSnapshotStat &stat);
  int wait_object_deleted_(
      const uint64_t tenant_id,
      const MacroBlockId &macro_id);
  int wait_data_check_dag_(
      const uint64_t tenant_id,
      const bool expected_exist,
      const int64_t timeout_us);
  int read_running_expected_item_(
      const uint64_t tenant_id,
      const MacroBlockId &macro_id,
      SCN &snapshot_meta_scn,
      ObSSDataCheckMode &check_mode,
      ObSSDataCheckExpectedItem &item,
      int64_t &expected_file_fd);
  int check_tmp_file_exist_(
      const uint64_t tenant_id,
      const int64_t fd,
      bool &is_exist);
  int wait_tmp_file_removed_(
      const uint64_t tenant_id,
      const int64_t fd,
      const int64_t timeout_us);
  int delete_exact_shared_meta_row_(
      const ObSSDataCheckExpectedItem &item,
      int64_t &affected_rows);
  int delete_tablet_shared_meta_rows_(
      const ObSSDataCheckExpectedItem &item,
      int64_t &affected_rows);
  int submit_and_wait_dag_(
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const ObSSDataCheckMode check_mode);
  int get_data_check_event_count_(
      const int64_t begin_ts,
      const uint64_t tenant_id,
      const char *event,
      int64_t &event_count);
  int wait_result_event_(
      const int64_t begin_ts,
      const uint64_t tenant_id,
      const SCN &snapshot_meta_scn,
      const ObSSDataCheckMode check_mode,
      const char *status,
      const ObSSDataCheckExpectedItem *missing_item);
};

int ScopedDataCheckListDebugSync::execute_sys_sql_(const char *sql)
{
  int64_t affected_rows = 0;
  return test_.exec_write_sql_sys(sql, affected_rows);
}

int ScopedDataCheckListDebugSync::arm()
{
  int ret = OB_SUCCESS;
  const int64_t expected_timeout_us = 300L * 1000L * 1000L;
  if (OB_UNLIKELY(is_armed_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check LIST debug sync is already armed", KR(ret));
  } else if (OB_FAIL(execute_sys_sql_(
      "alter system set debug_sync_timeout = '300s'"))) {
    LOG_WARN("failed to configure data check debug sync timeout", KR(ret));
  } else {
    for (int64_t i = 0;
         i < 100 && expected_timeout_us
             != static_cast<int64_t>(GCONF.debug_sync_timeout);
         ++i) {
      ob_usleep(100L * 1000L);
    }
    if (OB_UNLIKELY(expected_timeout_us
        != static_cast<int64_t>(GCONF.debug_sync_timeout))) {
      ret = OB_TIMEOUT;
      LOG_WARN("data check debug sync timeout config did not take effect",
          KR(ret), "actual_timeout_us",
          static_cast<int64_t>(GCONF.debug_sync_timeout));
    } else if (OB_FAIL(execute_sys_sql_(
        "set ob_global_debug_sync = 'reset'"))) {
      LOG_WARN("failed to reset data check debug sync", KR(ret));
    } else if (OB_FAIL(execute_sys_sql_(
        "set ob_global_debug_sync = "
        "'BEFORE_SS_DATA_CHECK_LIST_COMPARE signal dc_list_reached "
        "wait_for dc_list_continue execute 1'"))) {
      LOG_WARN("failed to arm data check LIST debug sync", KR(ret));
    } else {
      is_armed_ = true;
    }
  }
  return ret;
}

int ScopedDataCheckListDebugSync::wait_until_reached()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_monotonic_time();
  if (OB_UNLIKELY(!is_armed_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check LIST debug sync is not armed", KR(ret));
  } else if (OB_FAIL(execute_sys_sql_(
      "set ob_global_debug_sync = "
      "'now wait_for dc_list_reached execute 1'"))) {
    LOG_WARN("failed to wait data check LIST debug sync", KR(ret));
  } else if (OB_UNLIKELY(ObTimeUtility::current_monotonic_time() - start_ts
      >= 290L * 1000L * 1000L)) {
    ret = OB_TIMEOUT;
    LOG_WARN("data check LIST debug sync wait timed out", KR(ret));
  }
  return ret;
}

int ScopedDataCheckListDebugSync::release()
{
  int ret = OB_SUCCESS;
  if (is_armed_) {
    int tmp_ret = execute_sys_sql_(
        "set ob_global_debug_sync = 'now signal dc_list_continue'");
    if (OB_SUCCESS != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("failed to continue data check LIST debug sync", KR(tmp_ret));
    }
    tmp_ret = execute_sys_sql_("set ob_global_debug_sync = 'reset'");
    if (OB_SUCCESS != tmp_ret) {
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
      LOG_WARN("failed to reset data check LIST debug sync", KR(tmp_ret));
    }
    is_armed_ = false;
  }
  return ret;
}

int TestSSDataCheckE2E::wait_sys_ls_leader_()
{
  int ret = OB_SUCCESS;
  bool is_leader = false;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret));
  } else if (OB_FAIL(ls_service->get_ls(
      ObLSID(ObLSID::SYS_LS_ID), ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get SYS_LS", KR(ret));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("SYS_LS is null", KR(ret));
  } else {
    const int64_t deadline = ObTimeUtility::current_time() + 10L * 1000 * 1000;
    while (OB_SUCC(ret) && !is_leader
        && ObTimeUtility::current_time() < deadline) {
      ObRole role;
      int64_t proposal_id = 0;
      if (OB_FAIL(ls_handle.get_ls()->get_log_handler()->get_role(
          role, proposal_id))) {
        LOG_WARN("failed to get SYS_LS role", KR(ret));
      } else if (ObRole::LEADER == role) {
        is_leader = true;
      } else {
        ob_usleep(10 * 1000);
      }
    }
    if (OB_SUCC(ret) && !is_leader) {
      ret = OB_TIMEOUT;
      LOG_WARN("SYS_LS leader election timed out", KR(ret));
    }
  }
  return ret;
}

int TestSSDataCheckE2E::execute_tenant_sql_(const char *sql_str)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObSqlString sql;
  if (OB_ISNULL(sql_str)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant sql is null", KR(ret), KP(sql_str));
  } else if (OB_FAIL(sql.assign(sql_str))) {
    LOG_WARN("failed to assign tenant sql", KR(ret), KCSTRING(sql_str));
  } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().write(
      sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute tenant sql", KR(ret), K(sql));
  }
  return ret;
}

int TestSSDataCheckE2E::get_tablet_id_(
    const char *table_name,
    ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  uint64_t tablet_id_value = OB_INVALID_ID;
  ObSqlString sql;
  tablet_id.reset();
  if (OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table name is null", KR(ret), KP(table_name));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT tablet_id FROM oceanbase.__all_virtual_table "
      "WHERE table_name = '%s'",
      table_name))) {
    LOG_WARN("failed to build tablet id query", KR(ret), KCSTRING(table_name));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().read(
          result_holder, sql.ptr()))) {
        LOG_WARN("failed to query tablet id", KR(ret), K(sql));
      } else if (OB_ISNULL(result = result_holder.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet id query result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to read tablet id row", KR(ret), K(sql));
      } else if (OB_FAIL(result->get_uint("tablet_id", tablet_id_value))) {
        LOG_WARN("failed to extract tablet id", KR(ret), K(sql));
      } else {
        tablet_id = ObTabletID(tablet_id_value);
        if (OB_UNLIKELY(!tablet_id.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("queried tablet id is invalid", KR(ret), K(tablet_id));
        }
      }
    }
  }
  return ret;
}

int TestSSDataCheckE2E::inspect_expected_snapshot_(
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObTabletID &target_tablet_id,
    const ObSSDataCheckMode check_mode,
    ObSSDataCheckExpectedSnapshotStat &stat)
{
  int ret = OB_SUCCESS;
  bool source_end = false;
  ObSSDataCheckSharedMetaExpectedSource source;
  ObArenaAllocator batch_allocator(
      "DCE2EBatch", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObArenaAllocator tablet_allocator(
      "DCE2ETablet", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSEArray<ObSSRawMetaRow, 16> rows;
  ObSEArray<ObTabletBlockInfo, 64> block_infos;
  DataCheckNeverStopChecker stop_checker;
  rows.set_attr(ObMemAttr(tenant_id, "DCE2ERows"));
  block_infos.set_attr(ObMemAttr(tenant_id, "DCE2EBlocks"));
  stat.reset();

  if (OB_FAIL(source.open(
      tenant_id, snapshot_meta_scn, check_mode))) {
    LOG_WARN("failed to open shared meta expected source",
        KR(ret), K(tenant_id), K(snapshot_meta_scn), K(check_mode));
  }
  while (OB_SUCC(ret) && !source_end) {
    rows.reuse();
    batch_allocator.reuse();
    if (OB_FAIL(source.get_next_tablet_batch(
        batch_allocator, 64, 8 * 1024 * 1024, stop_checker, rows))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        source_end = true;
      } else {
        LOG_WARN("failed to get shared meta tablet batch", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      ObLSID parsed_ls_id;
      ObTabletID tablet_id;
      SCN reorganize_scn;
      SCN meta_scn;
      const ObSSRawMetaRow &row = rows.at(i);
      tablet_allocator.reuse();
      block_infos.reuse();
      if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(
          row.meta_key_, parsed_ls_id, tablet_id,
          reorganize_scn, meta_scn))) {
        LOG_WARN("failed to parse shared meta tablet key", KR(ret), K(row));
      } else if (OB_FAIL(
          ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_meta_value(
              tablet_allocator, row.ls_id_, tablet_id, reorganize_scn,
              row.meta_value_, false /*fill_occupy_size*/,
              false /*include_prewarm*/, block_infos))) {
        LOG_WARN("failed to expand shared meta tablet value", KR(ret), K(row));
      } else if (tablet_id == target_tablet_id) {
        ++stat.target_version_count_;
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < block_infos.count(); ++j) {
        const MacroBlockId &candidate = block_infos.at(j).macro_id();
        if (candidate.is_in_object_storage()
            && SSObjUtil::is_shared(candidate)
            && (SSObjUtil::is_macro(candidate)
                || SSObjUtil::is_shared_tablet_sub_meta(candidate))) {
          ++stat.expected_count_;
          if (tablet_id == target_tablet_id) {
            ++stat.target_expected_count_;
            if (!stat.target_data_item_.is_valid()
                && SSObjUtil::is_macro_data(candidate)) {
              if (OB_FAIL(stat.target_data_item_.init(
                  candidate, row.ls_id_, tablet_id,
                  reorganize_scn, meta_scn))) {
                LOG_WARN("failed to save target data macro reference",
                    KR(ret), K(row), K(candidate));
              }
            }
          }
          bool is_exist = false;
          if (OB_SUCC(ret) && OB_FAIL(ObRemoteIoAdapter::is_exist(
              tenant_id, candidate, GCONF.observer_id, is_exist))) {
            LOG_WARN("failed to check expected object existence",
                KR(ret), K(tenant_id), K(tablet_id), K(candidate));
          } else if (!is_exist) {
            ++stat.missing_count_;
          }
        }
      }
    }
  }
  source.reset();
  return ret;
}

int TestSSDataCheckE2E::get_target_tablet_row_(
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObTabletID &target_tablet_id,
    ObIAllocator &allocator,
    ObSSRawMetaRow &target_row)
{
  int ret = OB_SUCCESS;
  bool is_end = false;
  bool is_found = false;
  ObSSDataCheckSharedMetaExpectedSource source;
  ObArenaAllocator batch_allocator(
      "DCE2ETarget", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSEArray<ObSSRawMetaRow, 16> rows;
  DataCheckNeverStopChecker stop_checker;
  target_row.reset();
  rows.set_attr(ObMemAttr(tenant_id, "DCE2ETarget"));
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !snapshot_meta_scn.is_valid()
      || !target_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid target tablet row argument", KR(ret), K(tenant_id),
        K(snapshot_meta_scn), K(target_tablet_id));
  } else if (OB_FAIL(source.open(
      tenant_id, snapshot_meta_scn, ObSSDataCheckMode::SINGLE))) {
    LOG_WARN("failed to open target tablet source", KR(ret),
        K(tenant_id), K(snapshot_meta_scn), K(target_tablet_id));
  }

  while (OB_SUCC(ret) && !is_end && !is_found) {
    rows.reuse();
    batch_allocator.reuse();
    if (OB_FAIL(source.get_next_tablet_batch(
        batch_allocator, 64, 8 * 1024 * 1024, stop_checker, rows))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_end = true;
      } else {
        LOG_WARN("failed to read target tablet batch", KR(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && !is_found && i < rows.count(); ++i) {
      ObLSID parsed_ls_id;
      ObTabletID tablet_id;
      SCN reorganize_scn;
      SCN meta_scn;
      if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(
          rows.at(i).meta_key_, parsed_ls_id, tablet_id,
          reorganize_scn, meta_scn))) {
        LOG_WARN("failed to parse target tablet key", KR(ret), K(rows.at(i)));
      } else if (tablet_id == target_tablet_id) {
        if (OB_FAIL(target_row.deep_copy(allocator, rows.at(i)))) {
          LOG_WARN("failed to copy target tablet row", KR(ret), K(rows.at(i)));
        } else {
          is_found = true;
        }
      }
    }
  }
  source.reset();
  if (OB_SUCC(ret) && !is_found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("target tablet row does not exist", KR(ret),
        K(tenant_id), K(snapshot_meta_scn), K(target_tablet_id));
  }
  return ret;
}

int TestSSDataCheckE2E::get_table_store_expected_item_(
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObTabletID &target_tablet_id,
    ObSSDataCheckExpectedItem &item,
    ObMetaDiskAddr &table_store_addr)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(
      "DCE2ETblStore", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSSRawMetaRow row;
  ObLSID parsed_ls_id;
  ObTabletID tablet_id;
  SCN reorganize_scn;
  SCN meta_scn;
  ObTabletHandle tablet_handle;
  MacroBlockId macro_id;
  item.reset();
  table_store_addr.reset();
  if (OB_FAIL(get_target_tablet_row_(
      tenant_id, snapshot_meta_scn, target_tablet_id, allocator, row))) {
    LOG_WARN("failed to get table-store source row", KR(ret),
        K(target_tablet_id));
  } else if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(
      row.meta_key_, parsed_ls_id, tablet_id, reorganize_scn, meta_scn))) {
    LOG_WARN("failed to parse table-store source row", KR(ret), K(row));
  } else if (OB_FAIL(ObSSMetaInfoUtil::build_tablet_handle(
      row.ls_id_, tablet_id, reorganize_scn,
      row.meta_value_, allocator, tablet_handle))) {
    LOG_WARN("failed to build table-store source tablet", KR(ret), K(row));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table-store source tablet is null", KR(ret), K(row));
  } else if (FALSE_IT(
      table_store_addr = tablet_handle.get_obj()->get_table_store_addr())) {
  } else if (OB_FAIL(table_store_addr.get_macro_block_id(macro_id))) {
    LOG_WARN("failed to get table-store macro id", KR(ret),
        K(table_store_addr), K(row));
  } else if (OB_UNLIKELY(!macro_id.is_in_object_storage()
      || !SSObjUtil::is_shared_tablet_sub_meta(macro_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table-store is not an OSS shared tablet sub-meta",
        KR(ret), K(macro_id), K(table_store_addr), K(row));
  } else if (OB_FAIL(item.init(
      macro_id, row.ls_id_, tablet_id, reorganize_scn, meta_scn))) {
    LOG_WARN("failed to initialize table-store Expected item",
        KR(ret), K(macro_id), K(row));
  }
  return ret;
}

int TestSSDataCheckE2E::get_tablet_expand_ret_(
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObTabletID &target_tablet_id,
    int &expand_ret)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(
      "DCE2EExpand", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSSRawMetaRow row;
  ObLSID parsed_ls_id;
  ObTabletID tablet_id;
  SCN reorganize_scn;
  SCN meta_scn;
  ObSEArray<ObTabletBlockInfo, 64> block_infos;
  expand_ret = OB_SUCCESS;
  block_infos.set_attr(ObMemAttr(tenant_id, "DCE2EExpand"));
  if (OB_FAIL(get_target_tablet_row_(
      tenant_id, snapshot_meta_scn, target_tablet_id, allocator, row))) {
    LOG_WARN("failed to get expansion source row", KR(ret),
        K(target_tablet_id));
  } else if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(
      row.meta_key_, parsed_ls_id, tablet_id, reorganize_scn, meta_scn))) {
    LOG_WARN("failed to parse expansion source row", KR(ret), K(row));
  } else {
    expand_ret =
        ObSSMetaTabletMacroIDIterator::collect_blocks_from_tablet_meta_value(
            allocator, row.ls_id_, tablet_id, reorganize_scn,
            row.meta_value_, false /*fill_occupy_size*/,
            false /*include_prewarm*/, block_infos);
  }
  return ret;
}

int TestSSDataCheckE2E::evict_shared_object_caches_(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    const ObMetaDiskAddr &meta_addr)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  const ObStorageMetaKey meta_key(tenant_id, meta_addr);
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !macro_id.is_valid()
      || !meta_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared object cache eviction argument", KR(ret),
        K(tenant_id), K(macro_id), K(meta_addr));
  } else if (OB_ISNULL(macro_cache_mgr) || OB_ISNULL(tenant_kvcache_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared object cache manager is null", KR(ret),
        KP(macro_cache_mgr), KP(tenant_kvcache_mgr));
  } else if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().erase(meta_key))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to erase storage meta cache", KR(ret), K(meta_key));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(
      macro_cache_mgr->force_evict_by_macro_id(macro_id))) {
    LOG_WARN("failed to evict shared macro cache", KR(ret), K(macro_id));
  } else if (OB_SUCC(ret) && OB_FAIL(
      tenant_kvcache_mgr->erase_macro_block(macro_id))) {
    LOG_WARN("failed to erase shared macro KVCACHE", KR(ret), K(macro_id));
  }
  return ret;
}

int TestSSDataCheckE2E::wait_ready_snapshot_(
    const uint64_t tenant_id,
    const ObTabletID &target_tablet_id,
    SCN &snapshot_meta_scn,
    ObSSDataCheckExpectedSnapshotStat &stat)
{
  int ret = OB_SUCCESS;
  bool is_ready = false;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  const int64_t deadline =
      ObTimeUtility::current_time() + EXPECTED_READY_TIMEOUT_US;
  snapshot_meta_scn.reset();
  stat.reset();
  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret), K(tenant_id));
  }
  while (OB_SUCC(ret) && !is_ready
      && ObTimeUtility::current_time() < deadline) {
    if (OB_FAIL(meta_service->get_max_committed_meta_scn(snapshot_meta_scn))) {
      LOG_WARN("failed to get candidate data check snapshot", KR(ret), K(tenant_id));
    } else if (OB_UNLIKELY(!snapshot_meta_scn.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("candidate data check snapshot is invalid",
          KR(ret), K(tenant_id), K(snapshot_meta_scn));
    } else if (OB_FAIL(inspect_expected_snapshot_(
        tenant_id, snapshot_meta_scn, target_tablet_id,
        ObSSDataCheckMode::SINGLE, stat))) {
      LOG_WARN("failed to inspect candidate data check snapshot",
          KR(ret), K(tenant_id), K(snapshot_meta_scn));
    } else {
      is_ready = stat.is_ready();
      if (!is_ready) {
        ob_usleep(500 * 1000);
      }
    }
  }
  if (OB_SUCC(ret) && !is_ready) {
    ret = OB_TIMEOUT;
    LOG_WARN("uploaded expected objects did not become ready",
        KR(ret), K(tenant_id), K(target_tablet_id), K(snapshot_meta_scn),
        K(stat));
  } else if (OB_SUCC(ret)) {
    LOG_INFO("data check expected snapshot is ready",
        K(tenant_id), K(target_tablet_id), K(snapshot_meta_scn),
        K(stat));
  }
  return ret;
}

int TestSSDataCheckE2E::wait_full_snapshot_ready_(
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObTabletID &target_tablet_id,
    const int64_t single_target_version_count,
    ObSSDataCheckExpectedSnapshotStat &stat)
{
  int ret = OB_SUCCESS;
  bool is_ready = false;
  const int64_t deadline =
      ObTimeUtility::current_time() + EXPECTED_READY_TIMEOUT_US;
  stat.reset();
  if (OB_UNLIKELY(!snapshot_meta_scn.is_valid()
      || single_target_version_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid FULL expected wait argument", KR(ret),
        K(tenant_id), K(snapshot_meta_scn), K(target_tablet_id),
        K(single_target_version_count));
  }
  while (OB_SUCC(ret) && !is_ready
      && ObTimeUtility::current_time() < deadline) {
    if (OB_FAIL(inspect_expected_snapshot_(
        tenant_id, snapshot_meta_scn, target_tablet_id,
        ObSSDataCheckMode::FULL, stat))) {
      LOG_WARN("failed to inspect FULL data check snapshot",
          KR(ret), K(tenant_id), K(snapshot_meta_scn));
    } else if (OB_UNLIKELY(
        stat.target_version_count_ <= single_target_version_count)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("FULL mode did not enumerate historical tablet versions",
          KR(ret), K(snapshot_meta_scn), K(single_target_version_count), K(stat));
    } else {
      is_ready = stat.is_ready();
      if (!is_ready) {
        ob_usleep(500 * 1000);
      }
    }
  }
  if (OB_SUCC(ret) && !is_ready) {
    ret = OB_TIMEOUT;
    LOG_WARN("FULL expected objects did not become ready",
        KR(ret), K(tenant_id), K(snapshot_meta_scn), K(target_tablet_id), K(stat));
  } else if (OB_SUCC(ret)) {
    LOG_INFO("FULL data check expected snapshot is ready",
        K(tenant_id), K(snapshot_meta_scn), K(target_tablet_id), K(stat));
  }
  return ret;
}

int TestSSDataCheckE2E::wait_object_deleted_(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  const int64_t deadline =
      ObTimeUtility::current_time() + OBJECT_DELETE_TIMEOUT_US;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid deleted object wait argument",
        KR(ret), K(tenant_id), K(macro_id));
  }
  while (OB_SUCC(ret) && is_exist
      && ObTimeUtility::current_time() < deadline) {
    if (OB_FAIL(ObRemoteIoAdapter::is_exist(
        tenant_id, macro_id, GCONF.observer_id, is_exist))) {
      LOG_WARN("failed to check deleted object existence",
          KR(ret), K(tenant_id), K(macro_id));
    } else if (is_exist) {
      ob_usleep(100 * 1000);
    }
  }
  if (OB_SUCC(ret) && is_exist) {
    ret = OB_TIMEOUT;
    LOG_WARN("deleted object remained visible",
        KR(ret), K(tenant_id), K(macro_id));
  }
  return ret;
}

int TestSSDataCheckE2E::wait_data_check_dag_(
    const uint64_t tenant_id,
    const bool expected_exist,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool is_exist = !expected_exist;
  bool is_emergency = false;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObSSDataCheckDag probe;
  const int64_t deadline = ObTimeUtility::current_time() + timeout_us;
  probe.set_tenant_id(tenant_id);
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant DAG scheduler is null", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check DAG wait argument",
        KR(ret), K(tenant_id), K(timeout_us));
  }
  while (OB_SUCC(ret) && is_exist != expected_exist
      && ObTimeUtility::current_time() < deadline) {
    if (OB_FAIL(scheduler->check_dag_exist(
        &probe, is_exist, is_emergency))) {
      LOG_WARN("failed to check data check DAG existence",
          KR(ret), K(tenant_id));
    } else if (is_exist != expected_exist) {
      ob_usleep(100L * 1000L);
    }
  }
  if (OB_SUCC(ret) && is_exist != expected_exist) {
    ret = OB_TIMEOUT;
    LOG_WARN("data check DAG state wait timed out",
        KR(ret), K(tenant_id), K(expected_exist), K(is_exist));
  }
  return ret;
}

int TestSSDataCheckE2E::read_running_expected_item_(
    const uint64_t tenant_id,
    const MacroBlockId &macro_id,
    SCN &snapshot_meta_scn,
    ObSSDataCheckMode &check_mode,
    ObSSDataCheckExpectedItem &item,
    int64_t &expected_file_fd)
{
  int ret = OB_SUCCESS;
  bool is_found = false;
  int64_t expected_item_count = 0;
  ObSSDataCheckSortedFileInfo expected_file;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  snapshot_meta_scn.reset();
  check_mode = ObSSDataCheckMode::MAX;
  item.reset();
  expected_file_fd = -1;
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant DAG scheduler is null", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid running Expected lookup argument",
        KR(ret), K(tenant_id), K(macro_id));
  } else {
    ObSSDataCheckDag probe;
    probe.set_tenant_id(tenant_id);
    ObDagPrioScheduler &prio_scheduler =
        scheduler->prio_sche_[ObDagPrio::DAG_PRIO_SS_DATA_CHECK];
    SpinRLockGuard guard(prio_scheduler.prio_rwlock_);
    ObIDag *stored_dag = nullptr;
    if (OB_FAIL(prio_scheduler.get_stored_dag_(probe, stored_dag))) {
      LOG_WARN("failed to get running data check DAG", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(stored_dag)
        || OB_UNLIKELY(ObDagType::DAG_TYPE_SS_DATA_CHECK
            != stored_dag->get_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid running data check DAG",
          KR(ret), KP(stored_dag), K(tenant_id));
    } else {
      ObSSDataCheckDag *data_check_dag =
          static_cast<ObSSDataCheckDag *>(stored_dag);
      ObSSDataCheckCtx *ctx = data_check_dag->get_ctx();
      if (OB_ISNULL(ctx) || OB_UNLIKELY(!ctx->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("running data check context is invalid",
            KR(ret), KP(ctx), K(tenant_id));
      } else {
        snapshot_meta_scn = ctx->get_snapshot_meta_scn();
        check_mode = ctx->get_check_mode();
        if (OB_FAIL(ctx->get_expected_sort_pipeline().get_sorted_item_count(
            expected_item_count))) {
          LOG_WARN("failed to get running Expected item count", KR(ret));
        } else if (OB_FAIL(ctx->get_expected_sort_pipeline().get_sorted_file(
            expected_file))) {
          LOG_WARN("failed to retain running Expected file", KR(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(expected_item_count <= 0
      || !expected_file.is_valid()
      || expected_file.get_item_count() != expected_item_count)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid running Expected file",
        KR(ret), K(expected_item_count), K(expected_file));
  } else if (OB_SUCC(ret)) {
    ObSSDataCheckExpectedPathIterator iterator(FILE_BUF_SIZE);
    expected_file_fd = expected_file.get_fd();
    if (OB_FAIL(iterator.init_for_pread(
        expected_file, 0 /*io_expire_timestamp*/, tenant_id,
        FILE_BUF_SIZE))) {
      LOG_WARN("failed to initialize running Expected iterator", KR(ret));
    } else if (OB_FAIL(iterator.prefetch())) {
      LOG_WARN("failed to prefetch running Expected iterator", KR(ret));
    } else {
      while (OB_SUCC(ret) && !is_found) {
        const ObSSDataCheckExpectedSortItem *sort_item = nullptr;
        if (OB_FAIL(iterator.get_next_item(sort_item))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to read running Expected item", KR(ret));
          }
          break;
        } else if (OB_ISNULL(sort_item)
            || OB_UNLIKELY(!sort_item->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid running Expected item", KR(ret), KP(sort_item));
        } else if (sort_item->get_item().get_macro_id() == macro_id) {
          if (OB_FAIL(item.assign(sort_item->get_item()))) {
            LOG_WARN("failed to copy running Expected item", KR(ret));
          } else {
            is_found = true;
          }
        }
      }
    }
    iterator.reset();
  }
  expected_file.reset();
  if (OB_SUCC(ret) && !is_found) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("target macro is absent from running Expected file",
        KR(ret), K(tenant_id), K(macro_id), K(snapshot_meta_scn), K(check_mode));
  }
  if (OB_SUCCESS != ret) {
    expected_file_fd = -1;
  }
  return ret;
}

int TestSSDataCheckE2E::check_tmp_file_exist_(
    const uint64_t tenant_id,
    const int64_t fd,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 64> fds;
  is_exist = false;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || fd < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmp file lookup argument", KR(ret), K(tenant_id), K(fd));
  } else if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(
      tenant_id, fds))) {
    LOG_WARN("failed to get tenant tmp file fds", KR(ret), K(tenant_id), K(fd));
  } else {
    for (int64_t i = 0; !is_exist && i < fds.count(); ++i) {
      is_exist = fd == fds.at(i);
    }
  }
  return ret;
}

int TestSSDataCheckE2E::wait_tmp_file_removed_(
    const uint64_t tenant_id,
    const int64_t fd,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  bool is_exist = true;
  const int64_t deadline = ObTimeUtility::current_time() + timeout_us;
  if (OB_UNLIKELY(timeout_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmp file removal timeout", KR(ret), K(timeout_us));
  }
  while (OB_SUCC(ret) && is_exist
      && ObTimeUtility::current_time() < deadline) {
    if (OB_FAIL(check_tmp_file_exist_(tenant_id, fd, is_exist))) {
      LOG_WARN("failed to check canceled data check tmp file",
          KR(ret), K(tenant_id), K(fd));
    } else if (is_exist) {
      ob_usleep(10L * 1000L);
    }
  }
  if (OB_SUCC(ret) && is_exist) {
    ret = OB_TIMEOUT;
    LOG_WARN("canceled data check tmp file was not removed",
        KR(ret), K(tenant_id), K(fd));
  }
  return ret;
}

int TestSSDataCheckE2E::delete_exact_shared_meta_row_(
    const ObSSDataCheckExpectedItem &item,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ObSSMetaDeleteParamV2 delete_param;
  affected_rows = 0;
  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret));
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid exact shared meta item", KR(ret), K(item));
  } else {
    delete_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_KEY,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        item.get_ls_id(), item.get_tablet_id(), item.get_meta_scn(),
        item.get_reorganize_scn(), true /*is_in_table*/);
    if (OB_FAIL(meta_service->delete_meta_row(delete_param))) {
      LOG_WARN("failed to delete exact shared meta row", KR(ret), K(item));
    } else {
      affected_rows = delete_param.affected_rows_;
    }
  }
  return ret;
}

int TestSSDataCheckE2E::delete_tablet_shared_meta_rows_(
    const ObSSDataCheckExpectedItem &item,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ObSSMetaDeleteParamV2 delete_param;
  affected_rows = 0;
  if (OB_ISNULL(meta_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta service is null", KR(ret));
  } else if (OB_UNLIKELY(!item.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid shared meta item", KR(ret), K(item));
  } else {
    delete_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        item.get_ls_id(), item.get_tablet_id(), SCN::min_scn(),
        item.get_reorganize_scn(), true /*is_in_table*/);
    if (OB_FAIL(meta_service->delete_meta_row(delete_param))) {
      LOG_WARN("failed to delete shared meta tablet rows", KR(ret), K(item));
    } else {
      affected_rows = delete_param.affected_rows_;
    }
  }
  return ret;
}

int TestSSDataCheckE2E::submit_and_wait_dag_(
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObSSDataCheckMode check_mode)
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *scheduler = MTL(ObTenantDagScheduler *);
  ObSSDataCheckDagParam param;
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant DAG scheduler is null", KR(ret));
  } else if (OB_FAIL(param.init(
      tenant_id, snapshot_meta_scn, check_mode,
      MEMORY_LIMIT, FILE_BUF_SIZE, 0 /*io_expire_timestamp*/))) {
    LOG_WARN("failed to initialize data check DAG param",
        KR(ret), K(tenant_id), K(snapshot_meta_scn), K(check_mode));
  } else {
    for (int64_t i = 0; i < 120; ++i) {
      ret = scheduler->create_and_add_dag<ObSSDataCheckDag>(&param);
      if (OB_SUCCESS == ret) {
        break;
      } else if (OB_EAGAIN == ret || OB_TASK_EXIST == ret) {
        ob_usleep(100 * 1000);
      } else {
        LOG_WARN("failed to submit data check DAG", KR(ret));
        break;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSSDataCheckDag probe;
    probe.set_tenant_id(tenant_id);
    bool dag_exist = true;
    bool is_emergency = false;
    const int64_t deadline = ObTimeUtility::current_time() + DAG_TIMEOUT_US;
    while (OB_SUCC(ret) && dag_exist
        && ObTimeUtility::current_time() < deadline) {
      if (OB_FAIL(scheduler->check_dag_exist(
          &probe, dag_exist, is_emergency))) {
        LOG_WARN("failed to check data check DAG existence", KR(ret));
      } else if (dag_exist) {
        ob_usleep(100 * 1000);
      }
    }
    if (OB_SUCC(ret) && dag_exist) {
      ret = OB_TIMEOUT;
      LOG_WARN("data check DAG timed out", KR(ret), K(snapshot_meta_scn));
    }
  }
  return ret;
}

int TestSSDataCheckE2E::get_data_check_event_count_(
    const int64_t begin_ts,
    const uint64_t tenant_id,
    const char *event,
    int64_t &event_count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  event_count = 0;
  if (OB_UNLIKELY(begin_ts < 0
      || !is_valid_tenant_id(tenant_id)
      || OB_ISNULL(event)
      || '\0' == event[0])) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check event count argument",
        KR(ret), K(begin_ts), K(tenant_id), KP(event));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT COUNT(*) AS cnt FROM oceanbase.%s "
      "WHERE time_to_usec(gmt_create) >= %ld "
      "AND module = 'SHARED_GC' AND event = '%s' "
      "AND name1 = 'tenant_id' AND value1 = '%lu'",
      OB_ALL_SERVER_EVENT_HISTORY_TNAME,
      begin_ts, event, tenant_id))) {
    LOG_WARN("failed to build data check event count query", KR(ret));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(
          result_holder, sql.ptr()))) {
        LOG_WARN("failed to query data check event count", KR(ret), K(sql));
      } else if (OB_ISNULL(result = result_holder.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data check event count result is null", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("failed to read data check event count", KR(ret), K(sql));
      } else if (OB_FAIL(result->get_int("cnt", event_count))) {
        LOG_WARN("failed to extract data check event count", KR(ret), K(sql));
      }
    }
  }
  return ret;
}

int TestSSDataCheckE2E::wait_result_event_(
    const int64_t begin_ts,
    const uint64_t tenant_id,
    const SCN &snapshot_meta_scn,
    const ObSSDataCheckMode check_mode,
    const char *status,
    const ObSSDataCheckExpectedItem *missing_item)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObSqlString sql;
  const char *check_mode_str = ObSSDataCheckModeUtil::to_string(check_mode);
  const int64_t deadline = ObTimeUtility::current_time() + EVENT_TIMEOUT_US;
  if (OB_UNLIKELY(!ObSSDataCheckModeUtil::is_enabled(check_mode)
      || OB_ISNULL(status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check event wait argument",
        KR(ret), K(check_mode), KP(status));
  }
  while (OB_SUCC(ret) && !found
      && ObTimeUtility::current_time() < deadline) {
    int64_t event_count = 0;
    if (OB_ISNULL(missing_item)) {
      if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM oceanbase.%s "
        "WHERE time_to_usec(gmt_create) >= %ld "
        "AND module = 'SHARED_GC' AND event = 'DATA_CHECK_RESULT' "
        "AND name1 = 'tenant_id' AND value1 = '%lu' "
        "AND name2 = 'snapshot_meta_scn' AND value2 = '%lu' "
        "AND name3 = 'check_mode' AND value3 = '%s' "
        "AND name4 = 'status' AND value4 = '%s'",
        OB_ALL_SERVER_EVENT_HISTORY_TNAME,
        begin_ts,
        tenant_id,
        snapshot_meta_scn.get_val_for_inner_table_field(),
        check_mode_str,
        status))) {
        LOG_WARN("failed to build data check event query", KR(ret));
      }
    } else if (OB_UNLIKELY(!missing_item->is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid missing item for data check event",
          KR(ret), KPC(missing_item));
    } else if (OB_FAIL(sql.assign_fmt(
        "SELECT COUNT(*) AS cnt FROM oceanbase.%s "
        "WHERE time_to_usec(gmt_create) >= %ld "
        "AND module = 'SHARED_GC' AND event = 'DATA_CHECK_RESULT' "
        "AND name1 = 'tenant_id' AND value1 = '%lu' "
        "AND name2 = 'snapshot_meta_scn' AND value2 = '%lu' "
        "AND name3 = 'check_mode' AND value3 = '%s' "
        "AND name4 = 'status' AND value4 = '%s' "
        "AND name5 = 'ls_id' AND value5 = '%ld' "
        "AND name6 = 'tablet_id' AND value6 = '%lu' "
        "AND extra_info LIKE '%%macro_id%%' "
        "AND extra_info LIKE '%%reorganize_scn%%' "
        "AND extra_info LIKE '%%meta_scn%%'",
        OB_ALL_SERVER_EVENT_HISTORY_TNAME,
        begin_ts,
        tenant_id,
        snapshot_meta_scn.get_val_for_inner_table_field(),
        check_mode_str,
        status,
        missing_item->get_ls_id().id(),
        missing_item->get_tablet_id().id()))) {
      LOG_WARN("failed to build unhealthy data check event query", KR(ret));
    }
    if (OB_SUCC(ret)) {
      SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
        ObMySQLResult *result = nullptr;
        if (OB_FAIL(get_curr_simple_server().get_sql_proxy().read(
            result_holder, sql.ptr()))) {
          LOG_WARN("failed to query data check event", KR(ret), K(sql));
        } else if (OB_ISNULL(result = result_holder.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("data check event query result is null", KR(ret), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to read data check event count", KR(ret), K(sql));
        } else if (OB_FAIL(result->get_int("cnt", event_count))) {
          LOG_WARN("failed to extract data check event count", KR(ret), K(sql));
        } else {
          found = event_count > 0;
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ob_usleep(100 * 1000);
    }
  }
  if (OB_SUCC(ret) && !found) {
    ret = OB_TIMEOUT;
    LOG_WARN("data check result event timed out",
        KR(ret), K(snapshot_meta_scn), K(check_mode), KCSTRING(status), K(sql));
  }
  return ret;
}

TEST_F(TestSSDataCheckE2E,
    single_full_missing_and_production_entry_recheck)
{
  {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(sys_tenant_guard);
    ASSERT_EQ(OB_SUCCESS, sys_tenant_guard.switch_to(OB_SYS_TENANT_ID));
    ASSERT_EQ(OB_SUCCESS, wait_sys_ls_leader_());
  }

  ASSERT_EQ(OB_SUCCESS, create_tenant_with_retry("tt1"));
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id, "tt1"));
  ASSERT_TRUE(is_user_tenant(tenant_id));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());

  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  ASSERT_EQ(OB_SUCCESS, wait_sys_ls_leader_());
  ASSERT_EQ(OB_SUCCESS, execute_tenant_sql_(
      "ALTER SYSTEM SET _shared_inc_sstable_upload_interval = '1s'"));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_sql_(
      "ALTER SYSTEM SET inc_sstable_upload_thread_score = 20"));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_sql_(
      "CREATE TABLE data_check_e2e (c1 BIGINT, c2 VARCHAR(1024))"));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_sql_(
      "CREATE TABLE data_check_inline_e2e (c1 BIGINT, c2 VARCHAR(1024))"));

  ObTabletID target_tablet_id;
  ObTabletID inline_target_tablet_id;
  ASSERT_EQ(OB_SUCCESS,
      get_tablet_id_("data_check_e2e", target_tablet_id));
  ASSERT_EQ(OB_SUCCESS,
      get_tablet_id_("data_check_inline_e2e", inline_target_tablet_id));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_sql_(
      "INSERT INTO data_check_e2e "
      "SELECT 1, randstr(1024, random()) "
      "FROM TABLE(generator(10000))"));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_sql_(
      "INSERT INTO data_check_inline_e2e "
      "SELECT 1, randstr(1024, random()) "
      "FROM TABLE(generator(2000))"));
  ASSERT_EQ(OB_SUCCESS, execute_tenant_sql_("ALTER SYSTEM MINOR FREEZE"));

  // 固定快照前确认目标 tablet 已上传，且快照中的全部 Expected 对象都存在。
  SCN snapshot_meta_scn;
  ObSSDataCheckExpectedSnapshotStat single_stat;
  ASSERT_EQ(OB_SUCCESS,
      wait_ready_snapshot_(
          tenant_id, target_tablet_id,
          snapshot_meta_scn, single_stat));
  ObSSDataCheckExpectedItem inline_table_store_item;
  ObMetaDiskAddr inline_table_store_addr;
  ASSERT_EQ(OB_SUCCESS, get_table_store_expected_item_(
      tenant_id, snapshot_meta_scn, inline_target_tablet_id,
      inline_table_store_item, inline_table_store_addr));
  bool inline_table_store_exist = false;
  ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::is_exist(
      tenant_id, inline_table_store_item.get_macro_id(),
      GCONF.observer_id, inline_table_store_exist));
  ASSERT_TRUE(inline_table_store_exist);
  const int64_t single_health_begin_ts = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, submit_and_wait_dag_(
      tenant_id, snapshot_meta_scn, ObSSDataCheckMode::SINGLE));
  ASSERT_EQ(OB_SUCCESS, wait_result_event_(
      single_health_begin_ts, tenant_id, snapshot_meta_scn,
      ObSSDataCheckMode::SINGLE,
      "HEALTH", nullptr));

  // 同一快照下 FULL 必须枚举到历史版本，且全部 Expected 对象均已存在。
  ObSSDataCheckExpectedSnapshotStat full_stat;
  ASSERT_EQ(OB_SUCCESS, wait_full_snapshot_ready_(
      tenant_id, snapshot_meta_scn, target_tablet_id,
      single_stat.target_version_count_, full_stat));
  ASSERT_GT(full_stat.target_version_count_, single_stat.target_version_count_);

  const int64_t full_health_begin_ts = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, submit_and_wait_dag_(
      tenant_id, snapshot_meta_scn, ObSSDataCheckMode::FULL));
  ASSERT_EQ(OB_SUCCESS, wait_result_event_(
      full_health_begin_ts, tenant_id, snapshot_meta_scn,
      ObSSDataCheckMode::FULL,
      "HEALTH", nullptr));

  // 在最终 Expected 文件已经被 LIST Task 持有时取消真实 DAG，验证 Context
  // 析构会释放该文件，并且取消轮次不发布业务结果。
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      tenant_id, false, 10L * 1000L * 1000L));
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  ASSERT_NE(nullptr, dag_scheduler);
  ScopedDataCheckListDebugSync cancel_debug_sync(*this);
  ASSERT_EQ(OB_SUCCESS, cancel_debug_sync.arm());
  ObSSDataCheckDagParam cancel_param;
  ASSERT_EQ(OB_SUCCESS, cancel_param.init(
      tenant_id, snapshot_meta_scn, ObSSDataCheckMode::SINGLE,
      MEMORY_LIMIT, FILE_BUF_SIZE, 0 /*io_expire_timestamp*/));
  ob_usleep(10L * 1000L);
  const int64_t cancel_begin_ts = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS,
      dag_scheduler->create_and_add_dag<ObSSDataCheckDag>(&cancel_param));
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      tenant_id, true, 10L * 1000L * 1000L));
  ASSERT_EQ(OB_SUCCESS, cancel_debug_sync.wait_until_reached());

  SCN cancel_snapshot_meta_scn;
  ObSSDataCheckMode cancel_check_mode = ObSSDataCheckMode::MAX;
  ObSSDataCheckExpectedItem cancel_expected_item;
  int64_t canceled_expected_fd = -1;
  ASSERT_EQ(OB_SUCCESS, read_running_expected_item_(
      tenant_id, single_stat.target_data_item_.get_macro_id(),
      cancel_snapshot_meta_scn, cancel_check_mode,
      cancel_expected_item, canceled_expected_fd));
  ASSERT_EQ(snapshot_meta_scn, cancel_snapshot_meta_scn);
  ASSERT_EQ(ObSSDataCheckMode::SINGLE, cancel_check_mode);
  ASSERT_GE(canceled_expected_fd, 0);
  bool canceled_expected_file_exist = false;
  ASSERT_EQ(OB_SUCCESS, check_tmp_file_exist_(
      tenant_id, canceled_expected_fd, canceled_expected_file_exist));
  ASSERT_TRUE(canceled_expected_file_exist);

  ObSSDataCheckDag cancel_probe;
  cancel_probe.set_tenant_id(tenant_id);
  ASSERT_EQ(OB_SUCCESS,
      dag_scheduler->cancel_dag(&cancel_probe, true /*force_cancel*/));
  dag_scheduler->notify();
  ASSERT_EQ(OB_SUCCESS, cancel_debug_sync.release());
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      tenant_id, false, DAG_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, wait_tmp_file_removed_(
      tenant_id, canceled_expected_fd, 10L * 1000L * 1000L));
  ob_usleep(1L * 1000L * 1000L);
  int64_t canceled_result_event_count = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_check_event_count_(
      cancel_begin_ts, tenant_id,
      "DATA_CHECK_RESULT", canceled_result_event_count));
  ASSERT_EQ(0, canceled_result_event_count);

  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_service);
  ObSSDataCheckMetaRefChecker meta_ref_checker;
  ASSERT_EQ(OB_SUCCESS, meta_ref_checker.init(*meta_service));
  bool reference_exist = false;

  // 删除冷缓存中的 table-store 管理对象，验证已知 inline 引用不会因 deep
  // 展开失败被丢弃，并最终形成 NOT_HEALTH 证据。
  ASSERT_EQ(OB_SUCCESS, evict_shared_object_caches_(
      tenant_id, inline_table_store_item.get_macro_id(),
      inline_table_store_addr));
  ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::del_file(
      tenant_id, inline_table_store_item.get_macro_id(),
      0 /*ls_epoch_id*/, GCONF.observer_id));
  ASSERT_EQ(OB_SUCCESS, wait_object_deleted_(
      tenant_id, inline_table_store_item.get_macro_id()));
  ASSERT_EQ(OB_SUCCESS, evict_shared_object_caches_(
      tenant_id, inline_table_store_item.get_macro_id(),
      inline_table_store_addr));
  int inline_expand_ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, get_tablet_expand_ret_(
      tenant_id, snapshot_meta_scn,
      inline_target_tablet_id, inline_expand_ret));
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, inline_expand_ret);
  ASSERT_EQ(OB_SUCCESS, meta_ref_checker.check_reference_exist(
      inline_table_store_item, reference_exist));
  ASSERT_TRUE(reference_exist);

  ScopedDataCheckListDebugSync inline_debug_sync(*this);
  ASSERT_EQ(OB_SUCCESS, inline_debug_sync.arm());
  ObSSDataCheckDagParam inline_param;
  ASSERT_EQ(OB_SUCCESS, inline_param.init(
      tenant_id, snapshot_meta_scn, ObSSDataCheckMode::SINGLE,
      MEMORY_LIMIT, FILE_BUF_SIZE, 0 /*io_expire_timestamp*/));
  const int64_t inline_missing_begin_ts = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS,
      dag_scheduler->create_and_add_dag<ObSSDataCheckDag>(&inline_param));
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      tenant_id, true, 10L * 1000L * 1000L));
  ASSERT_EQ(OB_SUCCESS, inline_debug_sync.wait_until_reached());

  SCN inline_snapshot_meta_scn;
  ObSSDataCheckMode inline_check_mode = ObSSDataCheckMode::MAX;
  ObSSDataCheckExpectedItem retained_inline_item;
  int64_t inline_expected_fd = -1;
  ASSERT_EQ(OB_SUCCESS, read_running_expected_item_(
      tenant_id, inline_table_store_item.get_macro_id(),
      inline_snapshot_meta_scn, inline_check_mode,
      retained_inline_item, inline_expected_fd));
  ASSERT_EQ(snapshot_meta_scn, inline_snapshot_meta_scn);
  ASSERT_EQ(ObSSDataCheckMode::SINGLE, inline_check_mode);
  ASSERT_GE(inline_expected_fd, 0);
  ASSERT_EQ(inline_table_store_item.get_macro_id(),
      retained_inline_item.get_macro_id());
  ASSERT_EQ(inline_table_store_item.get_ls_id(),
      retained_inline_item.get_ls_id());
  ASSERT_EQ(inline_table_store_item.get_tablet_id(),
      retained_inline_item.get_tablet_id());
  ASSERT_EQ(inline_table_store_item.get_reorganize_scn(),
      retained_inline_item.get_reorganize_scn());
  ASSERT_EQ(inline_table_store_item.get_meta_scn(),
      retained_inline_item.get_meta_scn());

  ASSERT_EQ(OB_SUCCESS, inline_debug_sync.release());
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      tenant_id, false, DAG_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, wait_result_event_(
      inline_missing_begin_ts, tenant_id, snapshot_meta_scn,
      ObSSDataCheckMode::SINGLE,
      "NOT_HEALTH", &inline_table_store_item));

  // 清除专用 tablet 的全部版本，避免旧版本复用缺失对象并影响后续用例。
  int64_t inline_affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, delete_tablet_shared_meta_rows_(
      inline_table_store_item, inline_affected_rows));
  ASSERT_GE(inline_affected_rows, 1);
  ASSERT_EQ(OB_SUCCESS, meta_ref_checker.check_reference_exist(
      inline_table_store_item, reference_exist));
  ASSERT_FALSE(reference_exist);

  // 删除被精确 shared meta 行引用的数据宏块，验证完整负向链路。
  ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::del_file(
      tenant_id, single_stat.target_data_item_.get_macro_id(),
      0 /*ls_epoch_id*/, GCONF.observer_id));
  ASSERT_EQ(OB_SUCCESS, wait_object_deleted_(
      tenant_id, single_stat.target_data_item_.get_macro_id()));
  ASSERT_EQ(OB_SUCCESS, meta_ref_checker.check_reference_exist(
      single_stat.target_data_item_, reference_exist));
  ASSERT_TRUE(reference_exist);

  const int64_t missing_begin_ts = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, submit_and_wait_dag_(
      tenant_id, snapshot_meta_scn, ObSSDataCheckMode::SINGLE));
  ASSERT_EQ(OB_SUCCESS, wait_result_event_(
      missing_begin_ts, tenant_id, snapshot_meta_scn,
      ObSSDataCheckMode::SINGLE,
      "NOT_HEALTH", &single_stat.target_data_item_));

  // 下一轮从生产 Runner 的 GenTask 发起，并在最终 Expected 已固化后暂停。
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      tenant_id, false, 10L * 1000L * 1000L));
  ObSSGCTaskRunner runner;
  ScopedDataCheckRunnerCleanup runner_cleanup(runner);
  ASSERT_EQ(OB_SUCCESS, runner.init(tenant_id));
  ASSERT_EQ(OB_SUCCESS, runner.start());
  runner_cleanup.mark_started();
  runner.stop_timer_groups_();
  runner.wait_timer_groups_();
  ASSERT_FALSE(runner.is_stopped_);

  ScopedDataCheckListDebugSync list_debug_sync(*this);
  ASSERT_EQ(OB_SUCCESS, list_debug_sync.arm());
  runner.data_check_gen_task_.reset_dag_schedule_controller_();
  runner.data_check_gen_task_.commit_gc_dag_schedule_time_(0);
  const uint64_t run_count_before = runner.data_check_gen_task_.run_cnt_;
  const int64_t unreferenced_begin_ts = ObTimeUtility::current_time();
  runner.data_check_gen_task_.runTimerTask();
  ASSERT_EQ(run_count_before + 1, runner.data_check_gen_task_.run_cnt_);
  ASSERT_EQ(ObSSGCDagScheduleController::State::WAIT_FOR_DAG_FINISH,
      runner.data_check_gen_task_.dag_schedule_controller_.state());
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      tenant_id, true, 10L * 1000L * 1000L));
  ASSERT_EQ(OB_SUCCESS, list_debug_sync.wait_until_reached());

  // 必须从本轮最终文件读取实际保留的去重来源，避免诊断来源不稳定造成误测。
  SCN production_snapshot_meta_scn;
  ObSSDataCheckMode production_check_mode = ObSSDataCheckMode::MAX;
  ObSSDataCheckExpectedItem production_expected_item;
  int64_t production_expected_fd = -1;
  ASSERT_EQ(OB_SUCCESS, read_running_expected_item_(
      tenant_id, single_stat.target_data_item_.get_macro_id(),
      production_snapshot_meta_scn, production_check_mode,
      production_expected_item, production_expected_fd));
  ASSERT_TRUE(production_snapshot_meta_scn.is_valid());
  ASSERT_EQ(ObSSDataCheckMode::SINGLE, production_check_mode);
  ASSERT_GE(production_expected_fd, 0);
  ASSERT_LE(production_expected_item.get_meta_scn(),
      production_snapshot_meta_scn);

  bool object_exist = true;
  ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::is_exist(
      tenant_id, production_expected_item.get_macro_id(),
      GCONF.observer_id, object_exist));
  ASSERT_FALSE(object_exist);
  ASSERT_EQ(OB_SUCCESS, meta_ref_checker.check_reference_exist(
      production_expected_item, reference_exist));
  ASSERT_TRUE(reference_exist);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, delete_exact_shared_meta_row_(
      production_expected_item, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, meta_ref_checker.check_reference_exist(
      production_expected_item, reference_exist));
  ASSERT_FALSE(reference_exist);

  // 测试侧已释放最终文件 Handle，此后才能放行 LIST 和 Context 清理。
  ASSERT_EQ(OB_SUCCESS, list_debug_sync.release());
  ASSERT_EQ(OB_SUCCESS, wait_data_check_dag_(
      tenant_id, false, DAG_TIMEOUT_US));
  ASSERT_EQ(OB_SUCCESS, wait_result_event_(
      unreferenced_begin_ts, tenant_id, production_snapshot_meta_scn,
      production_check_mode, "HEALTH", nullptr));

  int64_t submit_event_count = 0;
  int64_t result_event_count = 0;
  ASSERT_EQ(OB_SUCCESS, get_data_check_event_count_(
      unreferenced_begin_ts, tenant_id,
      "SUBMIT_DATA_CHECK_DAG", submit_event_count));
  ASSERT_EQ(1, submit_event_count);
  ASSERT_EQ(OB_SUCCESS, get_data_check_event_count_(
      unreferenced_begin_ts, tenant_id,
      "DATA_CHECK_RESULT", result_event_count));
  ASSERT_EQ(1, result_event_count);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  char storage_info[1000] = {};
  const int64_t current_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&"
      "max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET,
      current_time_ns,
      oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK,
      oceanbase::unittest::S3_SK,
      oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = storage_info;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
