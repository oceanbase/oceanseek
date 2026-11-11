/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
// Run explicitly with --gtest_also_run_disabled_tests. PAIMON_PLUGIN_TEST_BUNDLE
// and PAIMON_PLUGIN_TEST_TABLE select the paired plugin and a native-eligible
// local PK fixture with an int column f2 (field id 2).
#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>
#include <cstdlib>
#include <memory>
#include <string>
#include <vector>

#define private          public
#define protected        public
#define USING_LOG_PREFIX SQL_ENG
#include "common/ob_tenant_data_version_mgr.h"
#include "lib/utility/ob_defer.h"
#include "plugin/v2/external_table/ob_ext_format_registry.h"
#include "plugin/v2/external_table/ob_ext_json_protocol.h"
#include "plugin/v2/host/ob_ext_host_provider.h"
#include "share/config/ob_server_config.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/external_table/ob_external_table_part_info.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/ob_get_compat_mode.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/basic/utils/expr_maker.h"
#include "sql/engine/expr/ob_expr_get_path.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_external_data_access_mgr.h"
#include "sql/engine/table/ob_external_data_page_cache.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/engine/table/ob_ext_table_plugin_row_iter.h"
#include "sql/engine/table/ob_parquet_min_max_iter.h"
#include "sql/engine/table/ob_orc_min_max_iter.h"
#include "sql/engine/table/ob_orc_table_row_iter.h"
#undef private
#undef protected
#include "sql/engine/table/ob_pcached_external_file_service.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/blocksstable/ob_data_file_prepare.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::storage;

TEST(RealPluginInterface, DISABLED_NativeAndSdkList)
{
  ObTenantDataVersionMgr::get_instance().set_mock_data_version(DATA_VERSION_4_6_1_0);
  ObSimpleMemLimitGetter mem_limit;
  ASSERT_EQ(
      OB_SUCCESS,
      mem_limit.add_tenant(OB_SERVER_TENANT_ID, 256 * 1024 * 1024, 1024L * 1024 * 1024));
  blocksstable::TestDataFilePrepareUtil io_fixture;
  ASSERT_EQ(
      OB_SUCCESS,
      io_fixture.init(&mem_limit, "paimon07_interface", OB_DEFAULT_MACRO_BLOCK_SIZE, 128));
  ASSERT_EQ(OB_SUCCESS, io_fixture.open());
  ObClusterVersion::get_instance().destroy();
  ASSERT_EQ(OB_SUCCESS, ObClusterVersion::get_instance().init(CLUSTER_CURRENT_VERSION));
  ObTenantBase *tenant = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, tenant);
  ObRefHolder<ObTenantIOManager> io_holder;
  ASSERT_EQ(OB_SUCCESS, OB_IO_MANAGER.get_tenant_io_manager(OB_SERVER_TENANT_ID, io_holder));
  ObTenantIOManager *tenant_io = io_holder.get_ptr();
  ASSERT_NE(nullptr, tenant_io);
  tenant->set(tenant_io);
  ObTenantEnv::set_tenant(tenant);
  ObExternalDataAccessMgr data_access;
  ASSERT_EQ(OB_SUCCESS, data_access.init());
  tenant->set(&data_access);
  ObTenantEnv::set_tenant(tenant);
  ObPCachedExternalFileService cached_files;
  ASSERT_EQ(OB_SUCCESS, cached_files.init(OB_SERVER_TENANT_ID));
  tenant->set(&cached_files);
  ObTenantEnv::set_tenant(tenant);
  ASSERT_EQ(OB_SUCCESS, ObExternalDataPageCache::get_instance().init("Paimon07Page", 1));
  DEFER(cached_files.destroy(); data_access.destroy();
        ObExternalDataPageCache::get_instance().destroy();
        tenant->set(static_cast<ObTenantIOManager *>(nullptr)));

  const char *plugin = getenv("PAIMON_PLUGIN_TEST_BUNDLE");
  const char *table = getenv("PAIMON_PLUGIN_TEST_TABLE");
  ASSERT_NE(nullptr, plugin);
  ASSERT_NE(nullptr, table);
  std::string cfg = std::string("[{\"name\":\"paimon\",\"path\":\"") + plugin + "\"}]";
  ASSERT_TRUE(GCONF.ext_plugin_config.set_value(cfg.c_str()));
  const ObExtTablePluginApi *api = ObExtFormatRegistry::get_instance().get_plugin_by_slot(0);
  ASSERT_NE(nullptr, api);
  ext_plugin::ObExtHostCtx host_ctx;
  host_ctx.default_fs.set_tenant_id(OB_SERVER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, host_ctx.default_fs.set_path(ObString::make_string(table)));
  host_ctx.default_fs.set_cache_options(ObExternalFileCacheOptions(false, false));
  ObExtTableHostApi host = {};
  ext_plugin::build_ext_host_api(host, &host_ctx);
  char *json = nullptr;
  int32_t len = 0;
  ASSERT_EQ(OB_SUCCESS, api->plan_create(table, "{}", nullptr, nullptr, -1, 1, &host, &json, &len));
  ObArenaAllocator arena;
  ObExtScanTaskArray planned;
  ASSERT_EQ(OB_SUCCESS, parse_scan_tasks_json(arena, json, len, planned));
  api->tasks_destroy(json, len, &host);
  ObExtFileScanDescriptor *desc = nullptr;
  int chosen = -1;
  for (int i = 0; i < planned.count && chosen < 0; ++i) {
    ASSERT_EQ(
        OB_SUCCESS,
        ObExtFileScanDescriptor::parse(
            arena,
            planned.tasks[i].task_json,
            planned.tasks[i].task_json_len,
            desc));
    if (desc != nullptr) {
      chosen = i;
    }
  }
  ASSERT_GE(chosen, 0) << "fixture needs a native-eligible PK split";
  ASSERT_EQ(ObExtFileScanFormat::PARQUET, desc->file_format_);
  ASSERT_GT(desc->file_count_, 0);
  std::vector<std::unique_ptr<ObPluginScanTask>> native_tasks;
  std::vector<std::string> native_urls;
  native_urls.reserve(desc->file_count_);
  for (int64_t child = 0; child < desc->file_count_; ++child) {
    native_urls.emplace_back(
        desc->files_[child].file_path_.ptr(),
        desc->files_[child].file_path_.length());
    std::string &url = native_urls.back();
    // The SDK's local file:/ paths need file:/// for the native URI parser.
    if (url.compare(0, 6, "file:/") == 0 && url.compare(0, 8, "file:///") != 0) {
      url.insert(5, "//");
    }
    auto task = std::make_unique<ObPluginScanTask>();
    task->reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
    task->file_url_ = ObString::make_string(url.c_str());
    task->file_size_ = desc->files_[child].byte_size_;
    task->assigned_file_bytes_ = task->file_size_;
    task->part_id_ = 0;
    native_tasks.push_back(std::move(task));
  }
  ObPluginScanTask sdk_task;
  sdk_task.reader_type_ = ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT;
  sdk_task.assigned_file_bytes_ = planned.tasks[chosen].byte_size;
  sdk_task.part_id_ = 0;
  sdk_task.plugin_task_json_.assign_ptr(
      planned.tasks[chosen].task_json,
      planned.tasks[chosen].task_json_len);

  ObExecContext ctx(arena);
  ObSQLSessionInfo session;
  ASSERT_EQ(
      OB_SUCCESS,
      session.set_tenant(ObString::make_string("interface_test"), OB_SERVER_TENANT_ID));
  session.set_stmt_type(stmt::T_INSERT);  // Exercise file IO without cache initialization.
  ctx.set_my_session(&session);
  ctx.set_mem_attr(ObMemAttr(OB_SERVER_TENANT_ID, "PaimonPageTest", ObCtxIds::EXECUTE_CTX_ID));
  ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().init_for_obcdc());
  lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
  ASSERT_EQ(
      OB_SUCCESS,
      ObCompatModeGetter::instance().set_tenant_compat_mode(OB_SERVER_TENANT_ID, mode));
  ObEvalCtx eval(ctx);
  static constexpr int64_t BATCH = 8;
  static constexpr uint64_t COLUMN_ID = OB_APP_MIN_COLUMN_ID + 2;
  SeqIntGenExprMaker maker(BATCH, BATCH, VEC_FIXED);
  static_cast<ExprMaker &>(maker).set_frame_idx(0);
  eval.frames_ = static_cast<char **>(arena.alloc(sizeof(char *)));
  ASSERT_NE(nullptr, eval.frames_);
  eval.frames_[0] = static_cast<char *>(arena.alloc(maker.frame_mem_size()));
  ASSERT_NE(nullptr, eval.frames_[0]);
  MEMSET(eval.frames_[0], 0, maker.frame_mem_size());
  ctx.set_frames(eval.frames_);
  ctx.set_frame_cnt(1);
  eval.max_batch_size_ = BATCH;
  ObExpr expr;
  maker.make(expr, eval);
  expr.datum_meta_.type_ = ObIntType;
  expr.obj_meta_.set_int();
  expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
      ObIntType,
      expr.datum_meta_.cs_type_,
      expr.datum_meta_.scale_,
      false,
      false,
      expr.datum_meta_.precision_);
  expr.type_ = T_PSEUDO_EXTERNAL_FILE_COL;
  ObDataAccessPathExtraInfo extra(arena, T_PSEUDO_EXTERNAL_FILE_COL);
  extra.data_access_path_ = ObString::make_string("f2");
  extra.mapped_column_id_ = COLUMN_ID;
  expr.extra_info_ = &extra;
  ObPushdownExprSpec spec(arena);
  spec.max_batch_size_ = BATCH;
  ASSERT_EQ(OB_SUCCESS, spec.access_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.ext_file_column_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.ext_column_convert_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.ext_mapping_column_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.ext_mapping_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.access_exprs_.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, spec.ext_file_column_exprs_.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, spec.ext_column_convert_exprs_.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, spec.ext_mapping_column_exprs_.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, spec.ext_mapping_column_ids_.push_back(COLUMN_ID));
  ObPushdownOperator op(eval, spec, false);
  ASSERT_EQ(OB_SUCCESS, op.init_pushdown_storage_filter());
  ObTableSchema schema;
  schema.set_tenant_id(OB_SERVER_TENANT_ID);
  schema.set_table_id(202302);
  schema.set_database_id(100);
  schema.set_schema_version(1);
  ASSERT_EQ(OB_SUCCESS, schema.set_table_name("mixed_interface"));
  schema.set_table_type(EXTERNAL_TABLE);
  schema.set_rowkey_column_num(1);
  schema.set_max_used_column_id(COLUMN_ID);
  ObColumnSchemaV2 col;
  col.set_tenant_id(OB_SERVER_TENANT_ID);
  col.set_table_id(202302);
  col.set_column_id(COLUMN_ID);
  col.set_data_type(ObIntType);
  col.set_rowkey_position(1);
  ASSERT_EQ(OB_SUCCESS, col.set_column_name("f2"));
  ASSERT_EQ(OB_SUCCESS, schema.add_column(col));
  ObExternalTablePartInfoArray partition_infos(arena);
  ObTableScanParam param;
  param.allocator_ = &arena;
  param.scan_allocator_ = &arena;
  param.op_ = &op;
  param.pd_storage_filters_ = op.pd_storage_filters_;
  param.partition_infos_ = &partition_infos;
  param.tenant_id_ = OB_SERVER_TENANT_ID;
  param.index_id_ = 202302;
  param.timeout_ = ObTimeUtility::current_time() + 60000000;
  ASSERT_EQ(OB_SUCCESS, param.column_ids_.push_back(COLUMN_ID));
  ObTableParam table_param(arena);
  ASSERT_EQ(OB_SUCCESS, table_param.convert(schema, param.column_ids_, spec.pd_storage_flag_));
  param.table_param_ = &table_param;
  param.output_exprs_ = &spec.access_exprs_;
  param.ext_file_column_exprs_ = &spec.ext_file_column_exprs_;
  param.ext_column_dependent_exprs_ = &spec.ext_column_convert_exprs_;
  param.ext_mapping_column_exprs_ = &spec.ext_mapping_column_exprs_;
  param.ext_mapping_column_ids_ = &spec.ext_mapping_column_ids_;
  param.lake_table_format_ = lake_plugin_format_of(0);
  param.external_file_location_ = ObString::make_string(table);
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  param.external_file_format_.cpp_plugin_format_.reader_options_json_ = ObString::make_string("{}");
  // Process tasks in input order and reuse one reader for each type.
  for (int repeat = 0; repeat < 2; ++repeat) {
    for (const auto &task : native_tasks) {
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(task.get()));
    }
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&sdk_task));
  }
  ObExternalTableAccessService service;
  ObNewRowIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
  DEFER(if (nullptr != iter) { service.revert_scan_iter(iter); });
  auto drain = [&](std::vector<std::string> &values) {
    int ret = OB_SUCCESS;
    int64_t count = 0;
    while (OB_SUCCESS == (ret = iter->get_next_rows(count, BATCH))) {
      EXPECT_GT(count, 0);
      ObIVector *vec = expr.get_vector(eval);
      for (int64_t i = 0; i < count; ++i) {
        values.push_back(vec->is_null(i) ? "NULL" : std::to_string(vec->get_int(i)));
      }
    }
    return ret;
  };
  std::vector<std::string> mixed;
  ASSERT_EQ(OB_ITER_END, drain(mixed));
  ASSERT_FALSE(mixed.empty());
  auto *dispatcher = static_cast<ObLakeTableRowIterDispatcher *>(iter);
  auto *sdk_reader = dispatcher->readers_[0].iter_;
  auto *native_reader = dispatcher->readers_[1].iter_;
  ASSERT_NE(nullptr, sdk_reader);
  ASSERT_NE(nullptr, native_reader);
  auto *sdk_iter = static_cast<ext_plugin::ObExtTablePluginRowIterator *>(sdk_reader);
  auto sdk_worker = sdk_iter->reader_worker_state_;
  ASSERT_NE(nullptr, sdk_worker);
  EXPECT_TRUE(sdk_iter->scan_active_);
  EXPECT_FALSE(sdk_iter->has_open_task_);
  EXPECT_EQ(nullptr, sdk_iter->cur_record_batch_);
  ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, iter));
  EXPECT_FALSE(sdk_iter->scan_active_);
  param.scan_tasks_.reuse();
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&sdk_task));
  ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
  std::vector<std::string> sdk;
  ASSERT_EQ(OB_ITER_END, drain(sdk));
  ASSERT_FALSE(sdk.empty());
  EXPECT_EQ(sdk_reader, dispatcher->readers_[0].iter_);
  EXPECT_EQ(sdk_worker, sdk_iter->reader_worker_state_);
  EXPECT_TRUE(sdk_iter->scan_active_);
  ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, iter));
  EXPECT_FALSE(sdk_iter->scan_active_);
  param.scan_tasks_.reuse();
  for (const auto &task : native_tasks) {
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(task.get()));
  }
  ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
  std::vector<std::string> native;
  ASSERT_EQ(OB_ITER_END, drain(native));
  EXPECT_EQ(native_reader, dispatcher->readers_[1].iter_);
  EXPECT_EQ(sdk_reader, dispatcher->readers_[0].iter_);
  // Independent native and SDK reads of the same split must agree exactly.
  ASSERT_EQ(sdk, native);
  std::vector<std::string> expected;
  for (int repeat = 0; repeat < 2; ++repeat) {
    expected.insert(expected.end(), native.begin(), native.end());
    expected.insert(expected.end(), sdk.begin(), sdk.end());
  }
  EXPECT_EQ(expected, mixed);  // Full order, NULLs and duplicate tasks are preserved.
}

int main(int argc, char **argv)
{
  ObClockGenerator::init();
  ObTimerService::get_instance().start();
  ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  ObTenantEnv::set_tenant(nullptr);
  ObTimerService::get_instance().stop();
  ObTimerService::get_instance().wait();
  ObTimerService::get_instance().destroy();
  ObClockGenerator::destroy();
  return ret;
}
