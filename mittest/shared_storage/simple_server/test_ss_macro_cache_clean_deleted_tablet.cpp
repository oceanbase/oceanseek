/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif

#include <gtest/gtest.h>
#define protected public
#define private public
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/macro_cache/macro_block_kvcache/ob_ss_tenant_macro_block_kvcache_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{

} // namespace sslog
} // namespace oceanbase

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::storage;

const char *get_per_file_test_name()
{
  return "test_ss_macro_cache_clean_deleted_tablet_";
}

namespace oceanbase
{
namespace unittest
{

class ForkServiceDebugSyncGuard
{
public:
  ForkServiceDebugSyncGuard() : enabled_(false) {}
  ~ForkServiceDebugSyncGuard() { reset(); }

  int enable()
  {
    int ret = OB_SUCCESS;
    const int64_t debug_sync_timeout_us = 1000000LL * 1000 * 1000;
    const int64_t config_wait_timeout_us = 10_s;
    if (enabled_) {
      ret = OB_INIT_TWICE;
      LOG_WARN("fork service debug sync is already enabled", KR(ret));
    } else if (OB_FAIL(sys_exe_sql("alter system set debug_sync_timeout = '1000000s'"))) {
      LOG_WARN("fail to set debug sync timeout", KR(ret));
    } else {
      const int64_t start_time_us = ObTimeUtility::current_time_us();
      while (debug_sync_timeout_us != static_cast<int64_t>(GCONF.debug_sync_timeout)
          && ObTimeUtility::current_time_us() - start_time_us < config_wait_timeout_us) {
        ob_usleep(100 * 1000);
      }
      if (debug_sync_timeout_us != static_cast<int64_t>(GCONF.debug_sync_timeout)) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait debug sync timeout config timeout", KR(ret), K(debug_sync_timeout_us),
            "actual_timeout_us", static_cast<int64_t>(GCONF.debug_sync_timeout));
      } else if (OB_FAIL(sys_exe_sql(
          "set ob_global_debug_sync = 'BEFORE_FORK_TABLE_SERVICE_RUNNING "
          "wait_for macro_cleanup_continue execute 100000'"))) {
        LOG_WARN("fail to block fork table service", KR(ret));
      } else {
        enabled_ = true;
      }
    }
    return ret;
  }

  int release()
  {
    int ret = OB_SUCCESS;
    if (enabled_) {
      int tmp_ret = sys_exe_sql(
          "set ob_global_debug_sync = 'BEFORE_FORK_TABLE_SERVICE_RUNNING clear'");
      if (OB_SUCCESS != tmp_ret) {
        ret = tmp_ret;
        LOG_WARN("fail to clear fork service debug sync", KR(tmp_ret));
      }
      tmp_ret = sys_exe_sql(
          "set ob_global_debug_sync = 'now broadcast macro_cleanup_continue'");
      if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
        ret = tmp_ret;
        LOG_WARN("fail to broadcast fork service debug sync", KR(tmp_ret));
      }
      if (OB_SUCCESS == ret) {
        enabled_ = false;
        tmp_ret = sys_exe_sql("alter system set debug_sync_timeout = '0'");
        if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to reset debug sync timeout", KR(tmp_ret));
        }
      }
    }
    return ret;
  }

  void reset()
  {
    if (enabled_) {
      const int tmp_ret = release();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN_RET(tmp_ret, "fail to reset fork service debug sync", K(tmp_ret));
      }
    }
  }

private:
  bool enabled_;
};

class IncSSTableUploadPauseGuard final
{
public:
  IncSSTableUploadPauseGuard()
      : ls_handle_(), uploader_(nullptr), old_stop_add_dag_(false),
        old_full_upload_finish_ts_(0), need_restore_(false)
  {}
  ~IncSSTableUploadPauseGuard() { reset(); }

  int pause(const ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    ObLSService *ls_service = MTL(ObLSService *);
    if (need_restore_) {
      ret = OB_INIT_TWICE;
      LOG_WARN("inc sstable upload scheduling is already paused", KR(ret), K(ls_id));
    } else if (OB_UNLIKELY(!ls_id.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid ls id", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret), K(ls_id));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle_, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls_id));
    } else if (OB_ISNULL(ls_handle_.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", KR(ret), K(ls_id));
    } else {
      uploader_ = &ls_handle_.get_ls()->get_inc_sstable_uploader();
      old_stop_add_dag_ = uploader_->stop_add_dag_;
      old_full_upload_finish_ts_ = uploader_->upload_record_.full_upload_finish_ts_;
      need_restore_ = true;
      // 把 full upload 完成时间设到未来，压住周期 full/emergency upload；Tablet-create
      // entry 仍会被处理，因此 Fork destination 仍能在 shared storage 上就绪。
      uploader_->upload_record_.record_full_upload(INT64_MAX);
      const int64_t timeout_us = 60_s;
      const int64_t stable_interval_us = 1_s;
      const int64_t start_time_us = ObTimeUtility::current_time_us();
      bool is_paused = false;
      while (OB_SUCC(ret) && !is_paused
          && ObTimeUtility::current_time_us() - start_time_us < timeout_us) {
        if (uploader_->is_processing()) {
          ob_usleep(100 * 1000);
        } else {
          // 已在运行的 full upload 可能重置 stop_add_dag_ 和完成时间；等该轮退出后
          // 重新设置两者，并要求状态稳定保持一个时间窗口。
          uploader_->stop_add_dag_ = true;
          uploader_->upload_record_.record_full_upload(INT64_MAX);
          ob_usleep(stable_interval_us);
          is_paused = !uploader_->is_processing()
              && uploader_->stop_add_dag_
              && INT64_MAX == uploader_->upload_record_.full_upload_finish_ts_;
        }
      }
      if (OB_SUCC(ret) && !is_paused) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait inc sstable upload scheduling paused timeout", KR(ret),
            K(ls_id), K(timeout_us));
      } else if (OB_SUCC(ret)) {
        LOG_INFO("paused ordinary inc sstable upload scheduling", K(ls_id),
            K(old_stop_add_dag_), K(old_full_upload_finish_ts_));
      }
    }
    return ret;
  }

  int restore()
  {
    int ret = OB_SUCCESS;
    if (need_restore_) {
      if (OB_ISNULL(uploader_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("inc sstable uploader is null", KR(ret));
      } else {
        uploader_->stop_add_dag_ = old_stop_add_dag_;
        uploader_->upload_record_.record_full_upload(old_full_upload_finish_ts_);
        need_restore_ = false;
        LOG_INFO("restored ordinary inc sstable upload scheduling",
            K(old_stop_add_dag_), K(old_full_upload_finish_ts_));
      }
    }
    return ret;
  }

  void reset()
  {
    if (need_restore_) {
      const int tmp_ret = restore();
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN_RET(tmp_ret, "fail to restore inc sstable upload scheduling", K(tmp_ret));
      }
    }
  }

private:
  ObLSHandle ls_handle_;
  storage::ObLSIncSSTableUploader *uploader_;
  bool old_stop_add_dag_;
  int64_t old_full_upload_finish_ts_;
  bool need_restore_;
  DISALLOW_COPY_AND_ASSIGN(IncSSTableUploadPauseGuard);
};

class TestSSMacroCacheCleanDeletedTablet : public ObStorageCachePolicyPrewarmerTest
{
public:
  TestSSMacroCacheCleanDeletedTablet() : ObStorageCachePolicyPrewarmerTest(), read_buf_() {}

  int get_tablet_and_ls_id_(
      const char *table_name,
      ObTabletID &tablet_id,
      ObLSID &ls_id)
  {
    int ret = OB_SUCCESS;
    uint64_t tablet_id_value = ObTabletID::INVALID_TABLET_ID;
    int64_t ls_id_value = ObLSID::INVALID_LS_ID;
    ObSqlString sql;
    ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    if (OB_ISNULL(table_name)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("table name is null", KR(ret), KP(table_name));
    } else if (OB_FAIL(sql.assign_fmt(
        "select tablet_id from oceanbase.__all_virtual_table "
        "where table_name='%s' limit 1",
        table_name))) {
      LOG_WARN("fail to build tablet query", KR(ret), K(table_name));
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
        sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(sql_proxy.read(result_holder, sql.ptr()))) {
          LOG_WARN("fail to query tablet id", KR(ret), K(sql));
        } else if (OB_ISNULL(result = result_holder.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet query result is null", KR(ret), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get tablet query row", KR(ret), K(sql));
        } else if (OB_FAIL(result->get_uint("tablet_id", tablet_id_value))) {
          LOG_WARN("fail to get tablet id", KR(ret), K(sql));
        }
      }
    }
    if (OB_SUCC(ret)) {
      sql.reset();
      if (OB_FAIL(sql.assign_fmt(
          "select ls_id from oceanbase.__all_tablet_to_ls "
          "where tablet_id=%lu limit 1",
          tablet_id_value))) {
        LOG_WARN("fail to build ls query", KR(ret), K(tablet_id_value));
      } else {
        SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
          sqlclient::ObMySQLResult *result = nullptr;
          if (OB_FAIL(sql_proxy.read(result_holder, sql.ptr()))) {
            LOG_WARN("fail to query ls id", KR(ret), K(sql));
          } else if (OB_ISNULL(result = result_holder.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ls query result is null", KR(ret), K(sql));
          } else if (OB_FAIL(result->next())) {
            LOG_WARN("fail to get ls query row", KR(ret), K(sql));
          } else if (OB_FAIL(result->get_int("ls_id", ls_id_value))) {
            LOG_WARN("fail to get ls id", KR(ret), K(sql));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      tablet_id = ObTabletID(tablet_id_value);
      ls_id = ObLSID(ls_id_value);
      if (OB_UNLIKELY(!tablet_id.is_valid() || !ls_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet or ls id", KR(ret), K(table_name), K(tablet_id), K(ls_id));
      }
    }
    return ret;
  }

  int wait_ddl_task_count_(
      const int64_t expected_count,
      const int64_t timeout_us)
  {
    int ret = OB_SUCCESS;
    int64_t task_count = -1;
    const int64_t start_time_us = ObTimeUtility::current_time_us();
    ObSqlString sql;
    ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
    if (OB_FAIL(sql.assign_fmt(
        "select count(*) as cnt from oceanbase.__all_virtual_ddl_task_status "
        "where tenant_id=%lu and ddl_type=1019",
        run_ctx_.tenant_id_))) {
      LOG_WARN("fail to build ddl task query", KR(ret), K(run_ctx_));
    }
    while (OB_SUCC(ret) && expected_count != task_count
        && ObTimeUtility::current_time_us() - start_time_us < timeout_us) {
      SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
        sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(sql_proxy.read(result_holder, sql.ptr()))) {
          LOG_WARN("fail to query ddl task", KR(ret), K(sql));
        } else if (OB_ISNULL(result = result_holder.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ddl task query result is null", KR(ret), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get ddl task row", KR(ret), K(sql));
        } else if (OB_FAIL(result->get_int("cnt", task_count))) {
          LOG_WARN("fail to get ddl task count", KR(ret), K(sql));
        }
      }
      if (OB_SUCC(ret) && expected_count != task_count) {
        ob_usleep(100 * 1000);
      }
    }
    if (OB_SUCC(ret) && expected_count != task_count) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait ddl task timeout", KR(ret), K(expected_count), K(task_count),
          K(timeout_us), K(run_ctx_));
    }
    return ret;
  }

  int wait_memtable_flushed_(
      const ObTabletID &tablet_id,
      const int64_t timeout_us)
  {
    int ret = OB_SUCCESS;
    int64_t memtable_count = -1;
    const int64_t start_time_us = ObTimeUtility::current_time_us();
    ObSqlString sql;
    ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
    if (OB_FAIL(sql.assign_fmt(
        "select count(*) as cnt from oceanbase.__all_virtual_table_mgr "
        "where tenant_id=%lu and tablet_id=%lu and table_type=0",
        run_ctx_.tenant_id_, tablet_id.id()))) {
      LOG_WARN("fail to build memtable query", KR(ret), K(tablet_id), K(run_ctx_));
    }
    while (OB_SUCC(ret) && 0 != memtable_count
        && ObTimeUtility::current_time_us() - start_time_us < timeout_us) {
      SMART_VAR(ObMySQLProxy::MySQLResult, result_holder) {
        sqlclient::ObMySQLResult *result = nullptr;
        if (OB_FAIL(sql_proxy.read(result_holder, sql.ptr()))) {
          LOG_WARN("fail to query memtable count", KR(ret), K(sql));
        } else if (OB_ISNULL(result = result_holder.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("memtable query result is null", KR(ret), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("fail to get memtable query row", KR(ret), K(sql));
        } else if (OB_FAIL(result->get_int("cnt", memtable_count))) {
          LOG_WARN("fail to get memtable count", KR(ret), K(sql));
        }
      }
      if (OB_SUCC(ret) && 0 != memtable_count) {
        ob_usleep(100 * 1000);
      }
    }
    if (OB_SUCC(ret) && 0 != memtable_count) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait memtable flush timeout", KR(ret), K(tablet_id),
          K(memtable_count), K(timeout_us));
    }
    return ret;
  }

  int wait_private_macro_blocks_(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObIArray<MacroBlockId> &private_macro_ids)
  {
    int ret = OB_SUCCESS;
    const int64_t timeout_us = 60_s;
    const int64_t start_time_us = ObTimeUtility::current_time_us();
    ObSEArray<MacroBlockId, 8> data_macro_ids;
    ObSEArray<MacroBlockId, 8> meta_macro_ids;
    private_macro_ids.reset();
    while (OB_SUCC(ret) && private_macro_ids.empty()
        && ObTimeUtility::current_time_us() - start_time_us < timeout_us) {
      data_macro_ids.reuse();
      meta_macro_ids.reuse();
      if (OB_FAIL(get_macro_blocks(ls_id, tablet_id, data_macro_ids, meta_macro_ids))) {
        LOG_WARN("fail to get source macro blocks", KR(ret), K(ls_id), K(tablet_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_macro_ids.count(); ++i) {
          if (data_macro_ids.at(i).is_private_macro()
              && OB_FAIL(private_macro_ids.push_back(data_macro_ids.at(i)))) {
            LOG_WARN("fail to add private data macro", KR(ret), K(i), K(tablet_id));
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < meta_macro_ids.count(); ++i) {
          if (meta_macro_ids.at(i).is_private_macro()
              && OB_FAIL(private_macro_ids.push_back(meta_macro_ids.at(i)))) {
            LOG_WARN("fail to add private meta macro", KR(ret), K(i), K(tablet_id));
          }
        }
      }
      if (OB_SUCC(ret) && private_macro_ids.empty()) {
        ob_usleep(100 * 1000);
      }
    }
    if (OB_SUCC(ret) && private_macro_ids.empty()) {
      ret = OB_TIMEOUT;
      LOG_WARN("wait private macro blocks timeout", KR(ret), K(ls_id), K(tablet_id));
    }
    return ret;
  }

  int inject_stale_effective_owner_(
      ObSSMacroCacheMgr &macro_cache_mgr,
      const ObIArray<MacroBlockId> &private_macro_ids,
      const ObTabletID &destination_tablet_id,
      MacroBlockId &polluted_macro_id)
  {
    int ret = OB_SUCCESS;
    bool found = false;
    polluted_macro_id.reset();
    for (int64_t i = 0; OB_SUCC(ret) && !found && i < private_macro_ids.count(); ++i) {
      const MacroBlockId &macro_id = private_macro_ids.at(i);
      if (macro_id.is_data()) {
        ObSSMacroCacheMetaHandle meta_handle;
        const int tmp_ret = macro_cache_mgr.meta_map_.get_refactored(
            macro_id, meta_handle);
        if (OB_HASH_NOT_EXIST == tmp_ret) {
          // Try the next private data macro candidate.
        } else if (OB_SUCCESS != tmp_ret) {
          ret = tmp_ret;
          LOG_WARN("fail to get private macro cache meta", KR(ret), K(i),
              K(macro_id), K(destination_tablet_id));
        } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("private macro cache meta is invalid", KR(ret), K(i),
              K(macro_id), K(destination_tablet_id));
        } else {
          // Fork source reads now keep the source as the effective owner. Inject the
          // legacy/stale owner state directly so this case remains focused on the
          // cleanup fallback rather than relying on the old owner-drift behavior.
          meta_handle()->set_effective_tablet_id(destination_tablet_id.id());
          polluted_macro_id = macro_id;
          found = true;
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_HASH_NOT_EXIST;
      LOG_WARN("no private data macro cache meta can be injected", KR(ret),
          K(destination_tablet_id), K(private_macro_ids.count()));
    }
    return ret;
  }

  int remove_tablet_(const ObLSID &ls_id, const ObTabletID &tablet_id)
  {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObArray<ObTabletID> tablet_ids;
    ObLSService *ls_service = MTL(ObLSService *);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("fail to get ls", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_ISNULL(ls_handle.get_ls())
        || OB_ISNULL(ls_handle.get_ls()->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls or tablet service is null", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
      LOG_WARN("fail to add tablet id", KR(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->remove_tablets(tablet_ids))) {
      LOG_WARN("fail to remove tablet", KR(ret), K(ls_id), K(tablet_id));
    }
    return ret;
  }

  int backdate_macro_(
      ObSSMacroCacheMgr &macro_cache_mgr,
      const MacroBlockId &macro_id)
  {
    int ret = OB_SUCCESS;
    ObSSMacroCacheMetaHandle meta_handle;
    if (OB_FAIL(macro_cache_mgr.meta_map_.get_refactored(macro_id, meta_handle))) {
      LOG_WARN("fail to get polluted macro cache meta", KR(ret), K(macro_id));
    } else if (OB_UNLIKELY(!meta_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("polluted macro cache meta is invalid", KR(ret), K(macro_id));
    } else {
      meta_handle()->set_last_access_time_us(1);
    }
    return ret;
  }

  int check_macro_owner_(
      ObSSMacroCacheMgr &macro_cache_mgr,
      const MacroBlockId &macro_id,
      const ObTabletID &born_tablet_id,
      const ObTabletID &effective_tablet_id)
  {
    int ret = OB_SUCCESS;
    ObSSMacroCacheMetaHandle meta_handle;
    if (OB_FAIL(macro_cache_mgr.meta_map_.get_refactored(macro_id, meta_handle))) {
      LOG_WARN("fail to get private macro cache meta", KR(ret), K(macro_id),
          K(born_tablet_id), K(effective_tablet_id));
    } else if (OB_UNLIKELY(!meta_handle.is_valid()
        || macro_id.second_id() != born_tablet_id.id()
        || meta_handle()->get_effective_tablet_id() != effective_tablet_id.id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected private macro owner", KR(ret), K(macro_id),
          K(born_tablet_id), K(effective_tablet_id), KPC(meta_handle()));
    }
    return ret;
  }

  int read_private_macro_(
      const MacroBlockId &macro_id,
      const ObTabletID &effective_tablet_id)
  {
    int ret = OB_SUCCESS;
    ObStorageObjectReadInfo read_info;
    ObStorageObjectHandle object_handle;
    ObSSPrivateMacroReader reader;
    memset(read_buf_, 0, sizeof(read_buf_));
    read_info.macro_block_id_ = macro_id;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
    read_info.buf_ = read_buf_;
    read_info.offset_ = 0;
    read_info.size_ = READ_IO_SIZE;
    read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.set_effective_tablet_id(effective_tablet_id);
    if (OB_FAIL(reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to submit private macro read", KR(ret), K(macro_id),
          K(effective_tablet_id));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("fail to wait private macro read", KR(ret), K(macro_id),
          K(effective_tablet_id));
    } else if (OB_UNLIKELY(object_handle.get_data_size() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("private macro read returned empty data", KR(ret), K(macro_id),
          K(effective_tablet_id), "data_size", object_handle.get_data_size());
    }
    return ret;
  }

protected:
  static constexpr int64_t READ_IO_SIZE = 2 * 1024L * 1024L;
  char read_buf_[READ_IO_SIZE];
};

// ----------------------------------------------------------------------------
// Private macro GC 只依据 born Tablet，忽略 effective owner 是否存活
//
// 场景：simple_server 通过 SQL 创建 source private MINI，只抑制 source 的普通
//       SSTable upload 以保持 private 形态；卡住真实 Fork service 并创建 destination。
//       当前 Fork source read 会保持 source owner，因此测试直接注入旧版本或异常状态下
//       effective owner=destination 的 stale meta。
// 操作：先删除 effective destination 并保留 born source，验证缓存保留；再创建一个 active
//       effective owner 并删除 born source，验证缓存清理。两次均回拨访问时间后直接触发
//       clean_deleted_tablet_，从而避免等待 30 分钟后台周期。
// 校验：(a) born 活跃、effective 删除时保留 private macro 且仍可读；
//       (b) born 删除、effective 活跃时删除 private macro 的 meta 和本地文件。
// ----------------------------------------------------------------------------
TEST_F(TestSSMacroCacheCleanDeletedTablet, private_macro_gc_uses_born_tablet_only)
{
  LOG_INFO("========== private_macro_gc_uses_born_tablet_only begin ==========");
  share::ObTenantSwitchGuard tenant_guard;
  OK(tenant_guard.switch_to(run_ctx_.tenant_id_));
  OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ObTenantFileManager *file_manager = MTL(ObTenantFileManager *);
  ObSSTenantMacroBlockKVCacheMgr *tenant_kvcache_mgr =
      MTL(ObSSTenantMacroBlockKVCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ASSERT_NE(nullptr, file_manager);
  ASSERT_NE(nullptr, tenant_kvcache_mgr);
  IncSSTableUploadPauseGuard inc_upload_guard;

  // step 1：构造 source private MINI，并保存其中全部 private macro 候选。
  OK(exe_sql("drop table if exists mc_cleanup_dst"));
  OK(exe_sql("drop table if exists mc_cleanup_src"));
  OK(exe_sql(
      "create table mc_cleanup_src(id bigint primary key, payload varchar(2048)) "
      "tablet_size=2097152 compression='none'"));
  ObTabletID source_tablet_id;
  ObLSID source_ls_id;
  OK(get_tablet_and_ls_id_("mc_cleanup_src", source_tablet_id, source_ls_id));
  run_ctx_.tablet_id_ = source_tablet_id;
  run_ctx_.ls_id_ = source_ls_id;
  // MINI merge 完成时会立即尝试 inc upload。在写数据前只抑制该 LS 的
  // 普通 SSTable upload；Tablet-create upload 仍然执行，避免阻塞 Fork destination 就绪。
  OK(inc_upload_guard.pause(source_ls_id));
  OK(exe_sql(
      "insert into mc_cleanup_src select row_number() over(), repeat('p', 512) "
      "from table(generator(1024))"));
  OK(exe_sql("alter system minor freeze"));
  OK(wait_memtable_flushed_(source_tablet_id, 120_s));
  ObSEArray<MacroBlockId, 16> private_macro_ids;
  OK(wait_private_macro_blocks_(source_ls_id, source_tablet_id, private_macro_ids));
  for (int64_t i = 0; i < private_macro_ids.count(); ++i) {
    OK(tenant_kvcache_mgr->erase_macro_block(private_macro_ids.at(i)));
    bool candidate_kvcache_exist = true;
    OK(tenant_kvcache_mgr->check_exist(
        private_macro_ids.at(i), candidate_kvcache_exist));
    ASSERT_FALSE(candidate_kvcache_exist);
  }
  LOG_INFO("step 1: source private macro candidates are ready",
      K(source_tablet_id), K(source_ls_id), K(private_macro_ids));

  // step 2：卡住真实 Fork service，创建 INCOMPLETE destination，并直接注入历史/异常的
  // effective owner 漂移状态。生产读路径现在会保持 source owner，不能再依赖 DML 制造该状态。
  ForkServiceDebugSyncGuard debug_sync_guard;
  OK(debug_sync_guard.enable());
  OK(exe_sql("fork table mc_cleanup_src to mc_cleanup_dst"));
  OK(wait_ddl_task_count_(1, 60_s));
  ObTabletID destination_tablet_id;
  ObLSID destination_ls_id;
  OK(get_tablet_and_ls_id_(
      "mc_cleanup_dst", destination_tablet_id, destination_ls_id));
  MacroBlockId polluted_macro_id;
  OK(inject_stale_effective_owner_(
      *macro_cache_mgr, private_macro_ids, destination_tablet_id, polluted_macro_id));
  ASSERT_EQ(source_tablet_id.id(), polluted_macro_id.second_id());
  LOG_INFO("step 2: injected stale effective owner",
      K(polluted_macro_id), K(source_tablet_id), K(destination_tablet_id));

  // step 3：完成 Fork 并删除 destination，确保 cleanup 的真实 active set 只保留 source。
  OK(debug_sync_guard.release());
  OK(wait_ddl_task_count_(0, 180_s));
  OK(exe_sql("drop table mc_cleanup_dst"));
  OK(remove_tablet_(destination_ls_id, destination_tablet_id));
  hash::ObHashSet<uint64_t> active_tablet_ids;
  ObMemAttr active_set_attr(MTL_ID(), "MCCleanTest");
  OK(active_tablet_ids.create(1024, active_set_attr));
  OK(get_active_tablet_ids(active_tablet_ids, active_set_attr));
  ASSERT_EQ(OB_HASH_EXIST, active_tablet_ids.exist_refactored(source_tablet_id.id()));
  ASSERT_EQ(OB_HASH_NOT_EXIST,
      active_tablet_ids.exist_refactored(destination_tablet_id.id()));

  // step 4：不等待 30 分钟，回拨目标时间并执行完整 cleanup；active born 必须保留。
  OK(check_macro_owner_(
      *macro_cache_mgr,
      polluted_macro_id,
      source_tablet_id,
      destination_tablet_id));
  OK(backdate_macro_(*macro_cache_mgr, polluted_macro_id));
  bool remote_exist = true;
  OK(file_manager->is_exist_remote_file(
      polluted_macro_id, 0 /*ls_epoch_id*/, remote_exist));
  ASSERT_FALSE(remote_exist);
  OK(macro_cache_mgr->clean_deleted_tablet_());
  bool cache_exist = false;
  OK(macro_cache_mgr->exist(polluted_macro_id, cache_exist));
  ASSERT_TRUE(cache_exist);
  bool local_exist = false;
  OK(file_manager->is_exist_local_file(
      polluted_macro_id, 0 /*ls_epoch_id*/, local_exist));
  ASSERT_TRUE(local_exist);
  OK(tenant_kvcache_mgr->erase_macro_block(polluted_macro_id));
  bool kvcache_exist = true;
  OK(tenant_kvcache_mgr->check_exist(polluted_macro_id, kvcache_exist));
  ASSERT_FALSE(kvcache_exist);
  // 清空上层 KVCACHE 后以 source Tablet 身份读取，确保命中被保留的本地 private macro。
  OK(read_private_macro_(polluted_macro_id, source_tablet_id));
  LOG_INFO("step 4: active born Tablet retained readable private macro",
      K(polluted_macro_id), K(source_tablet_id), K(destination_tablet_id));

  // step 5：重建一个 active effective owner，删除 born source 后 cleanup 必须清理 private macro。
  OK(exe_sql(
      "create table mc_cleanup_dst(id bigint primary key, payload varchar(2048)) "
      "tablet_size=2097152 compression='none'"));
  ObTabletID active_effective_tablet_id;
  ObLSID active_effective_ls_id;
  OK(get_tablet_and_ls_id_(
      "mc_cleanup_dst", active_effective_tablet_id, active_effective_ls_id));
  {
    ObSSMacroCacheMetaHandle meta_handle;
    OK(macro_cache_mgr->meta_map_.get_refactored(polluted_macro_id, meta_handle));
    ASSERT_TRUE(meta_handle.is_valid());
    meta_handle()->set_effective_tablet_id(active_effective_tablet_id.id());
  }
  OK(check_macro_owner_(
      *macro_cache_mgr,
      polluted_macro_id,
      source_tablet_id,
      active_effective_tablet_id));
  OK(tenant_kvcache_mgr->erase_macro_block(polluted_macro_id));
  OK(exe_sql("drop table mc_cleanup_src"));
  OK(remove_tablet_(source_ls_id, source_tablet_id));

  hash::ObHashSet<uint64_t> active_tablet_ids_after_source_drop;
  ObMemAttr second_active_set_attr(MTL_ID(), "MCCleanTest2");
  OK(active_tablet_ids_after_source_drop.create(
      1024 /*bucket_num*/, second_active_set_attr));
  OK(get_active_tablet_ids(active_tablet_ids_after_source_drop, second_active_set_attr));
  ASSERT_EQ(OB_HASH_NOT_EXIST,
      active_tablet_ids_after_source_drop.exist_refactored(source_tablet_id.id()));
  ASSERT_EQ(OB_HASH_EXIST,
      active_tablet_ids_after_source_drop.exist_refactored(active_effective_tablet_id.id()));

  bool cache_exist_before_cleanup = false;
  OK(macro_cache_mgr->exist(polluted_macro_id, cache_exist_before_cleanup));
  ASSERT_TRUE(cache_exist_before_cleanup);
  bool local_exist_before_cleanup = false;
  OK(file_manager->is_exist_local_file(
      polluted_macro_id, 0 /*ls_epoch_id*/, local_exist_before_cleanup));
  ASSERT_TRUE(local_exist_before_cleanup);
  OK(backdate_macro_(*macro_cache_mgr, polluted_macro_id));
  OK(macro_cache_mgr->clean_deleted_tablet_());

  bool cache_exist_after_cleanup = true;
  OK(macro_cache_mgr->exist(polluted_macro_id, cache_exist_after_cleanup));
  ASSERT_FALSE(cache_exist_after_cleanup);
  bool local_exist_after_cleanup = true;
  OK(file_manager->is_exist_local_file(
      polluted_macro_id, 0 /*ls_epoch_id*/, local_exist_after_cleanup));
  ASSERT_FALSE(local_exist_after_cleanup);
  LOG_INFO("step 5: deleted born Tablet removed private macro despite active effective owner",
      K(polluted_macro_id), K(source_tablet_id), K(active_effective_tablet_id),
      K(active_effective_ls_id));

  OK(inc_upload_guard.restore());
  OK(exe_sql("drop table mc_cleanup_dst"));
  LOG_INFO("========== private_macro_gc_uses_born_tablet_only end ==========",
      K(polluted_macro_id), K(source_tablet_id), K(active_effective_tablet_id));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t current_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, current_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("20G");
  GCONF.memory_limit.set_value("10G");
  GCONF.system_memory.set_value("3G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
