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

// Test-only access to the private state of the selected native reader.
static const ObExternalIteratorState &native_reader_state(
    const ObLakeTableRowIterDispatcher &dispatcher, const bool aggregate)
{
  const auto *iter = dispatcher.current_reader_->iter_;
  if (dispatcher.current_reader_ == &dispatcher.readers_[1]) {
    return aggregate ? static_cast<const ObParquetMinMaxIter *>(iter)->state_
                     : static_cast<const ObParquetTableRowIterator *>(iter)->state_;
  }
  return aggregate ? static_cast<const ObOrcMinMaxIter *>(iter)->state_
                   : static_cast<const ObOrcTableRowIterator *>(iter)->state_;
}

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

TEST(RealPluginInterface, NativeParquetOrcHandoffsAndMinMax)
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

  ObArenaAllocator arena;
  char cwd[4096];
  ASSERT_NE(nullptr, getcwd(cwd, sizeof(cwd)));
  const std::string location = std::string("file://") + cwd;
  const char *table = location.c_str();
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
  static constexpr uint64_t COLUMN_ID = OB_APP_MIN_COLUMN_ID;
  SeqIntGenExprMaker maker(BATCH, BATCH, VEC_FIXED);
  static_cast<ExprMaker &>(maker).set_frame_idx(0);
  eval.frames_ = static_cast<char **>(arena.alloc(4 * sizeof(char *)));
  ASSERT_NE(nullptr, eval.frames_);
  eval.frames_[0] = static_cast<char *>(arena.alloc(maker.frame_mem_size()));
  ASSERT_NE(nullptr, eval.frames_[0]);
  MEMSET(eval.frames_[0], 0, maker.frame_mem_size());
  eval.frames_[1] = static_cast<char *>(arena.alloc(maker.frame_mem_size()));
  ASSERT_NE(nullptr, eval.frames_[1]);
  MEMSET(eval.frames_[1], 0, maker.frame_mem_size());
  eval.frames_[2] = static_cast<char *>(arena.alloc(maker.frame_mem_size()));
  ASSERT_NE(nullptr, eval.frames_[2]);
  MEMSET(eval.frames_[2], 0, maker.frame_mem_size());
  eval.frames_[3] = static_cast<char *>(arena.alloc(maker.frame_mem_size()));
  ASSERT_NE(nullptr, eval.frames_[3]);
  MEMSET(eval.frames_[3], 0, maker.frame_mem_size());
  ctx.set_frames(eval.frames_);
  ctx.set_frame_cnt(4);
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
  extra.data_access_path_ = ObString::make_string("i_no_bf");
  extra.mapped_column_id_ = COLUMN_ID;
  expr.extra_info_ = &extra;
  // Match codegen: materialized T_REF_COLUMN outputs are distinct from file
  // expressions and are excluded from calc_exprs_ (whose flags are cleared).
  ObExpr output_expr;
  static_cast<ExprMaker &>(maker).set_frame_idx(2);
  maker.make(output_expr, eval);
  output_expr.type_ = T_REF_COLUMN;
  output_expr.datum_meta_ = expr.datum_meta_;
  output_expr.obj_meta_ = expr.obj_meta_;
  output_expr.basic_funcs_ = expr.basic_funcs_;
  ObExpr result_expr;
  static_cast<ExprMaker &>(maker).set_frame_idx(1);
  maker.make(result_expr, eval);
  result_expr.datum_meta_ = expr.datum_meta_;
  result_expr.obj_meta_ = expr.obj_meta_;
  result_expr.basic_funcs_ = expr.basic_funcs_;
  ObExpr *args[] = {&output_expr};
  result_expr.args_ = args;
  result_expr.arg_cnt_ = 1;
  result_expr.eval_vector_func_ = expr_default_eval_vector_func;
  result_expr.eval_batch_func_ = expr_default_eval_batch_func;
  result_expr.eval_func_ = [](const ObExpr &self, ObEvalCtx &ctx, ObDatum &datum) {
    const auto *vec = self.args_[0]->get_vector(ctx);
    datum.set_int(!vec->is_null(ctx.get_batch_idx())
        && vec->get_int(ctx.get_batch_idx()) > self.extra_);
    return OB_SUCCESS;
  };
  ObPushdownExprSpec spec(arena);
  spec.max_batch_size_ = BATCH;
  ASSERT_EQ(OB_SUCCESS, spec.access_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.ext_file_column_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.ext_column_convert_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.ext_mapping_column_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.ext_mapping_column_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, spec.access_exprs_.push_back(&output_expr));
  ASSERT_EQ(OB_SUCCESS, spec.ext_file_column_exprs_.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, spec.ext_column_convert_exprs_.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, spec.ext_mapping_column_exprs_.push_back(&output_expr));
  ASSERT_EQ(OB_SUCCESS, spec.ext_mapping_column_ids_.push_back(COLUMN_ID));
  ASSERT_EQ(OB_SUCCESS, spec.calc_exprs_.init(2));
  ASSERT_EQ(OB_SUCCESS, spec.calc_exprs_.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, spec.calc_exprs_.push_back(&result_expr));
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
  ASSERT_EQ(OB_SUCCESS, col.set_column_name("i_no_bf"));
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
  ObPluginScanTask tasks[5];
  const ObExternalFileFormat::FormatType types[] = {ObExternalFileFormat::PARQUET_FORMAT,
      ObExternalFileFormat::PARQUET_FORMAT, ObExternalFileFormat::ORC_FORMAT,
      ObExternalFileFormat::PARQUET_FORMAT, ObExternalFileFormat::ORC_FORMAT};
  for (int i = 0; i < 5; ++i) {
    tasks[i].reader_type_ = types[i];
    tasks[i].file_url_ = ObString::make_string(types[i] == ObExternalFileFormat::PARQUET_FORMAT
        ? "bloom_filter_smoking_test.parquet" : "bloom_filter_smoking_test.orc");
    tasks[i].part_id_ = 0;
    tasks[i].file_id_ = i + 1;
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
  }
  ExprFixedArray predicates(arena);
  ASSERT_EQ(OB_SUCCESS, predicates.init(1));
  ASSERT_EQ(OB_SUCCESS, predicates.push_back(&result_expr));
  ExprFixedArray aggregates(arena);
  ASSERT_EQ(OB_SUCCESS, aggregates.init(1));
  ASSERT_EQ(OB_SUCCESS, aggregates.push_back(&result_expr));
  ObPushdownBlackFilterNode filter_node(arena);
  ASSERT_EQ(OB_SUCCESS, filter_node.col_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, filter_node.get_col_ids().push_back(COLUMN_ID));
  ASSERT_EQ(OB_SUCCESS, filter_node.column_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, filter_node.column_exprs_.push_back(&output_expr));
  ASSERT_EQ(OB_SUCCESS, filter_node.filter_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, filter_node.filter_exprs_.push_back(&result_expr));
  ObBlackFilterExecutor storage_filter(arena, filter_node, op);
  // One-row batches force empty filtered batches before surviving row 4.
  // Modes: plain, residual prefix/all filtered, min/max, storage prefix/all filtered.
  for (int mode = 0; mode < 7; ++mode) {
    const bool aggregate = mode == 3 || mode == 4;
    SCOPED_TRACE(mode);
    param.ext_enable_late_materialization_ = true;
    param.op_filters_ = (mode == 1 || mode == 2) ? &predicates : nullptr;
    param.aggregate_exprs_ = aggregate ? &aggregates : nullptr;
    param.pd_storage_filters_ = mode >= 5 ? &storage_filter : nullptr;
    ObStoragePushdownFlag flags;
    flags.set_aggregate_pushdown(aggregate);
    param.pd_storage_flag_ = flags.pd_flag_;
    result_expr.type_ = mode == 3 ? T_FUN_SYS_EXT_MIN : mode == 4 ? T_FUN_SYS_EXT_MAX : T_OP_GT;
    result_expr.extra_ = (mode == 2 || mode == 6) ? 100 : 2;
    ObExternalTableAccessService service;
    ObNewRowIterator *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
    DEFER(if (nullptr != iter) { service.revert_scan_iter(iter); });
    for (int scan = 0; scan < 2; ++scan) {
      const int64_t capacity = scan == 0 ? 1 : BATCH;
      std::vector<int64_t> actual;
      int ret = OB_SUCCESS;
      int64_t count = 0;
      while (OB_SUCCESS == (ret = iter->get_next_rows(count, capacity))) {
        ASSERT_GT(count, 0);
        ASSERT_LE(count, capacity);
        auto *dispatcher = static_cast<ObLakeTableRowIterDispatcher *>(iter);
        const auto &file_state = native_reader_state(*dispatcher, aggregate);
        EXPECT_EQ(file_state.task_idx_, file_state.cur_file_id_);
        if (mode == 0) {
          EXPECT_EQ(actual.size() / 3 + 1, file_state.cur_file_id_);
          EXPECT_EQ((actual.size() + count - 1) % 3 + 1, file_state.cur_line_number_);
        }
        if (!aggregate) {
          EXPECT_TRUE(output_expr.get_eval_info(eval).is_evaluated(eval));
        }
        for (int64_t row = 0; row < count; ++row) {
          actual.push_back(aggregate ? result_expr.locate_batch_datums(eval)[row].get_int()
                                    : output_expr.get_vector(eval)->get_int(row));
        }
        ASSERT_LE(actual.size(), 15U);
      }
      ASSERT_EQ(OB_ITER_END, ret);
      EXPECT_EQ(0, count);
      EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, 1));
      std::vector<int64_t> expected;
      if (aggregate) {
        expected.assign(4, mode == 3 ? 1 : 4); // P/P, O, P, O: four partial aggregates.
      } else if (mode != 2 && mode != 6) {
        for (int i = 0; i < 5; ++i) {
          if (mode == 0) { expected.push_back(1); expected.push_back(2); }
          expected.push_back(4);
        }
      }
      EXPECT_EQ(expected, actual);
      ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
    }
  }
  // Test Iceberg's direct reader selection and the dispatcher separately.
  {
    param.lake_table_format_ = ObLakeTableFormat::ICEBERG;
    param.external_file_format_.format_type_ = ObExternalFileFormat::PARQUET_FORMAT;
    param.op_filters_ = nullptr;
    param.pd_storage_filters_ = nullptr;
    param.scan_tasks_.reuse();
    ObIcebergScanTask parquet_task(arena);
    ObIcebergScanTask orc_task(arena);
    parquet_task.file_format_ = iceberg::DataFileFormat::PARQUET;
    parquet_task.file_url_ = tasks[0].file_url_;
    parquet_task.file_size_ = -1;
    parquet_task.file_id_ = 1;
    orc_task.file_format_ = iceberg::DataFileFormat::ORC;
    orc_task.file_url_ = tasks[2].file_url_;
    orc_task.file_size_ = -1;
    orc_task.file_id_ = 2;
    // The service chooses the reader from the first task and resets it on rescan.
    param.aggregate_exprs_ = nullptr;
    param.pd_storage_flag_ = ObStoragePushdownFlag().pd_flag_;
    for (ObIcebergScanTask *task : {&parquet_task, &orc_task}) {
      param.scan_tasks_.reuse();
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(task));
      ObExternalTableAccessService service;
      ObNewRowIterator *iter = nullptr;
      ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
      DEFER(if (nullptr != iter) { service.revert_scan_iter(iter); });
      if (task == &parquet_task) {
        ASSERT_NE(nullptr, dynamic_cast<ObParquetTableRowIterator *>(iter));
      } else {
        ASSERT_NE(nullptr, dynamic_cast<ObOrcTableRowIterator *>(iter));
      }
      for (int pass = 0; pass < 2; ++pass) {
        std::vector<int64_t> actual;
        int64_t count = 0;
        int ret = OB_SUCCESS;
        while (OB_SUCCESS == (ret = iter->get_next_rows(count, BATCH))) {
          ASSERT_GT(count, 0);
          for (int64_t row = 0; row < count; ++row) {
            actual.push_back(output_expr.get_vector(eval)->get_int(row));
          }
          ASSERT_LE(actual.size(), 3U);
        }
        ASSERT_EQ(OB_ITER_END, ret);
        EXPECT_EQ((std::vector<int64_t>{1, 2, 4}), actual);
        if (pass == 0) {
          ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
        }
      }
    }
    param.scan_tasks_.reuse();
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&parquet_task));
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&orc_task));
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&parquet_task));
    for (bool aggregate : {false, true}) {
      param.aggregate_exprs_ = aggregate ? &aggregates : nullptr;
      ObStoragePushdownFlag flags;
      flags.set_aggregate_pushdown(aggregate);
      param.pd_storage_flag_ = flags.pd_flag_;
      result_expr.type_ = T_FUN_SYS_EXT_MIN;
      ObLakeTableRowIterDispatcher dispatcher;
      ASSERT_EQ(OB_SUCCESS, dispatcher.init(&param));
      ObNewRowIterator *iter = &dispatcher;
      for (int pass = 0; pass < 2; ++pass) {
        std::vector<int64_t> actual;
        int64_t count = 0;
        int ret = OB_SUCCESS;
        while (OB_SUCCESS == (ret = iter->get_next_rows(count, BATCH))) {
          ASSERT_GT(count, 0);
          for (int64_t row = 0; row < count; ++row) {
            actual.push_back(aggregate ? result_expr.locate_batch_datums(eval)[row].get_int()
                                       : output_expr.get_vector(eval)->get_int(row));
          }
          ASSERT_LE(actual.size(), 9U);
        }
        ASSERT_EQ(OB_ITER_END, ret);
        const std::vector<int64_t> expected = aggregate ? std::vector<int64_t>{1, 1, 1}
            : std::vector<int64_t>{1, 2, 4, 1, 2, 4, 1, 2, 4};
        EXPECT_EQ(expected, actual);
        if (pass == 0) {
          ASSERT_EQ(OB_SUCCESS, dispatcher.rescan());
        }
      }
    }
    param.scan_tasks_.reuse();
    for (auto &task : tasks) {
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
    }
    param.lake_table_format_ = lake_plugin_format_of(0);
    param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  }
  // Rescan can introduce ORC after a partially consumed Parquet task. The
  // new reader must already exist when table_rescan returns, before fetching.
  {
    param.op_filters_ = nullptr;
    param.pd_storage_filters_ = nullptr;
    param.aggregate_exprs_ = nullptr;
    param.pd_storage_flag_ = ObStoragePushdownFlag().pd_flag_;
    param.scan_tasks_.reuse();
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
    ObExternalTableAccessService service;
    ObNewRowIterator *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
    DEFER(if (nullptr != iter) { service.revert_scan_iter(iter); });
    auto *dispatcher = static_cast<ObLakeTableRowIterDispatcher *>(iter);
    auto *parquet_reader = dispatcher->readers_[1].iter_;
    EXPECT_EQ(nullptr, dispatcher->readers_[2].iter_);
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, iter->get_next_rows(count, 1));
    ASSERT_EQ(1, count);
    EXPECT_EQ(1, output_expr.get_vector(eval)->get_int(0));
    ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, iter));
    param.scan_tasks_.reuse();
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[2])); // ORC first.
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
    ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
    ASSERT_NE(nullptr, dispatcher->readers_[2].iter_);
    EXPECT_EQ(parquet_reader, dispatcher->readers_[1].iter_);
    std::vector<int64_t> actual;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter->get_next_rows(count, 1))) {
      ASSERT_EQ(1, count);
      actual.push_back(output_expr.get_vector(eval)->get_int(0));
      ASSERT_LE(actual.size(), 6U);
    }
    EXPECT_EQ(OB_ITER_END, ret);
    const std::vector<int64_t> expected = {1, 2, 4, 1, 2, 4};
    EXPECT_EQ(expected, actual);
  }
  // A partial ORC sector belongs to the old scan, even though the reader survives.
  {
    param.op_filters_ = nullptr;
    param.pd_storage_filters_ = &storage_filter;
    param.aggregate_exprs_ = nullptr;
    param.pd_storage_flag_ = ObStoragePushdownFlag().pd_flag_;
    result_expr.type_ = T_OP_GT;
    result_expr.extra_ = 0; // Keep all three rows so capacity 1 leaves a pending sector.
    param.scan_tasks_.reuse();
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[2]));
    ObExternalTableAccessService service;
    ObNewRowIterator *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
    DEFER(if (nullptr != iter) { service.revert_scan_iter(iter); });
    auto *dispatcher = static_cast<ObLakeTableRowIterDispatcher *>(iter);
    auto *orc_reader = static_cast<ObOrcTableRowIterator *>(dispatcher->readers_[2].iter_);
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, iter->get_next_rows(count, 1));
    ASSERT_EQ(1, count);
    EXPECT_EQ(1, output_expr.get_vector(eval)->get_int(0));
    ASSERT_NE(nullptr, orc_reader->sector_reader_);
    EXPECT_FALSE(orc_reader->sector_reader_->is_finished());
    ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, iter));
    param.scan_tasks_.reuse();
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[4]));
    ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
    std::vector<int64_t> actual;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = iter->get_next_rows(count, 1))) {
      ASSERT_EQ(1, count);
      EXPECT_EQ(tasks[4].file_id_, native_reader_state(*dispatcher, false).cur_file_id_);
      actual.push_back(output_expr.get_vector(eval)->get_int(0));
      ASSERT_LE(actual.size(), 3U);
    }
    EXPECT_EQ(OB_ITER_END, ret);
    const std::vector<int64_t> expected = {1, 2, 4};
    EXPECT_EQ(expected, actual);
  }
  param.pd_storage_filters_ = nullptr;
  // Use native task objects in standalone scans to catch
  // accidental access to the plugin-only reader_type_ payload.
  for (auto format : {ObExternalFileFormat::PARQUET_FORMAT, ObExternalFileFormat::ORC_FORMAT}) {
    param.external_file_format_.format_type_ = format;
    param.scan_tasks_.reuse();
    ObExtTableScanTask task;
    task.file_url_ = format == ObExternalFileFormat::PARQUET_FORMAT
        ? tasks[0].file_url_ : tasks[2].file_url_;
    task.file_id_ = 1;
    ObExtTableScanTask dummy;
    dummy.file_url_ = ObExternalTableUtils::dummy_file_name();
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&dummy));
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&dummy));
    for (int mode = 0; mode < 7; ++mode) {
      SCOPED_TRACE(static_cast<int>(format));
      SCOPED_TRACE(mode);
      const bool aggregate = mode == 3 || mode == 4;
      param.op_filters_ = (mode == 1 || mode == 2) ? &predicates : nullptr;
      param.aggregate_exprs_ = aggregate ? &aggregates : nullptr;
      param.pd_storage_filters_ = mode >= 5 ? &storage_filter : nullptr;
      ObStoragePushdownFlag flags;
      flags.set_aggregate_pushdown(aggregate);
      param.pd_storage_flag_ = flags.pd_flag_;
      result_expr.type_ = mode == 3 ? T_FUN_SYS_EXT_MIN : mode == 4 ? T_FUN_SYS_EXT_MAX : T_OP_GT;
      result_expr.extra_ = (mode == 2 || mode == 6) ? 100 : 2;
      ObExternalTableAccessService service;
      ObNewRowIterator *iter = nullptr;
      ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
      DEFER(if (nullptr != iter) { service.revert_scan_iter(iter); });
      for (int scan = 0; scan < 2; ++scan) {
        std::vector<int64_t> actual;
        int ret = OB_SUCCESS;
        int64_t count = 0;
        while (OB_SUCCESS == (ret = iter->get_next_rows(count, scan == 0 ? 1 : BATCH))) {
          ASSERT_GT(count, 0);
          for (int64_t row = 0; row < count; ++row) {
            actual.push_back(aggregate ? result_expr.locate_batch_datums(eval)[row].get_int()
                                      : output_expr.get_vector(eval)->get_int(row));
          }
          ASSERT_LE(actual.size(), 6U);
        }
        EXPECT_EQ(OB_ITER_END, ret);
        EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, BATCH));
        EXPECT_EQ(0, count);
        std::vector<int64_t> expected;
        if (aggregate) {
          expected.push_back(mode == 3 ? 1 : 4);
        } else if (mode != 2 && mode != 6) {
          for (int i = 0; i < 2; ++i) {
            if (mode == 0) { expected.push_back(1); expected.push_back(2); }
            expected.push_back(4);
          }
        }
        EXPECT_EQ(expected, actual);
        ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
      }
    }
  }
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  // Exercise the partition collector itself: a returned aggregate postpones
  // the boundary notification, but must not consume the other reader's task.
  param.op_filters_ = nullptr;
  param.pd_storage_filters_ = nullptr;
  param.aggregate_exprs_ = &aggregates;
  ObStoragePushdownFlag aggregate_flags;
  aggregate_flags.set_aggregate_pushdown(true);
  param.pd_storage_flag_ = aggregate_flags.pd_flag_;
  result_expr.type_ = T_FUN_SYS_EXT_MIN;
  for (ObExternalFileFormat::FormatType type :
       {ObExternalFileFormat::PARQUET_FORMAT, ObExternalFileFormat::ORC_FORMAT}) {
    SCOPED_TRACE(static_cast<int>(type));
    const ObExternalFileFormat::FormatType other = type == ObExternalFileFormat::PARQUET_FORMAT
        ? ObExternalFileFormat::ORC_FORMAT : ObExternalFileFormat::PARQUET_FORMAT;
    param.scan_tasks_.reuse();
    for (int i = 0; i < 3; ++i) {
      tasks[i].reader_type_ = i == 1 ? other : type;
      tasks[i].file_url_ = ObString::make_string(tasks[i].reader_type_ == ObExternalFileFormat::PARQUET_FORMAT
          ? "bloom_filter_smoking_test.parquet" : "bloom_filter_smoking_test.orc");
      tasks[i].file_size_ = -1;
      tasks[i].modification_time_ = 0;
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
    }
    ObExternalTableAccessService service;
    ObNewRowIterator *iter = nullptr;
    ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
    DEFER(if (nullptr != iter) { service.revert_scan_iter(iter); });
    auto *dispatcher = static_cast<ObLakeTableRowIterDispatcher *>(iter);
    auto verify = [&](auto &reader) {
      int64_t count = 0;
      int64_t next_idx = -1;
      // Task0 belongs to the verified reader. Pull its aggregate through the
      // dispatcher: it is the only production entry that drives readers.
      const auto *mine = dispatcher->current_reader_;
      ASSERT_NE(nullptr, mine);
      EXPECT_EQ(&reader, mine->iter_);
      ASSERT_EQ(OB_SUCCESS, dispatcher->get_next_rows(count, 1));
      EXPECT_EQ(1, count);
      EXPECT_EQ(1, result_expr.locate_batch_datums(eval)[0].get_int());
      // The returned aggregate postpones the boundary: the reader parks its
      // cursor on the other reader's unconsumed task and truthfully reports a
      // handoff when driven directly. Routing is the dispatcher's job.
      ASSERT_EQ(OB_SUCCESS, dispatcher->peek_next_task_idx(reader.get_next_task_idx(), next_idx));
      EXPECT_EQ(1, next_idx);
      EXPECT_EQ(OB_NOT_THE_OBJECT, reader.get_next_rows(count, 1));
      EXPECT_EQ(0, count);
      // The dispatcher routes task1 to the other reader and returns its result.
      ASSERT_EQ(OB_SUCCESS, dispatcher->get_next_rows(count, 1));
      EXPECT_EQ(1, count);
      EXPECT_EQ(1, result_expr.locate_batch_datums(eval)[0].get_int());
      EXPECT_NE(mine, dispatcher->current_reader_);
      // Task2 routes back to the verified reader, then the scan ends.
      ASSERT_EQ(OB_SUCCESS, dispatcher->get_next_rows(count, 1));
      EXPECT_EQ(1, count);
      EXPECT_EQ(1, result_expr.locate_batch_datums(eval)[0].get_int());
      EXPECT_EQ(mine, dispatcher->current_reader_);
      EXPECT_EQ(OB_ITER_END, reader.get_next_rows(count, 1));
      EXPECT_EQ(0, count);
      count = 99;
      EXPECT_EQ(OB_ITER_END, dispatcher->get_next_rows(count, 1));
      EXPECT_EQ(0, count);

      // All tasks skipped before any file was collected: preserve the boundary,
      // rather than manufacturing a NULL partition result.
      ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, iter));
      param.scan_tasks_.reuse();
      tasks[0].file_size_ = 0;
      tasks[0].modification_time_ = 1; // Known zero-byte task; next_file skips IO.
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
      ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
      EXPECT_EQ(OB_ITER_END, reader.collect_partition_aggregate());
      ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, iter));
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[1]));
      ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
      EXPECT_EQ(OB_NOT_THE_OBJECT, reader.collect_partition_aggregate());
      ASSERT_EQ(OB_SUCCESS, dispatcher->peek_next_task_idx(reader.get_next_task_idx(), next_idx));
      EXPECT_EQ(1, next_idx);
    };
    if (type == ObExternalFileFormat::PARQUET_FORMAT) {
      verify(*static_cast<ObParquetMinMaxIter *>(dispatcher->readers_[1].iter_));
    } else {
      verify(*static_cast<ObOrcMinMaxIter *>(dispatcher->readers_[2].iter_));
    }
  }

  // A valid file -> skipped files in A -> a valid file in B, all assigned to
  // the same reader. Check the partition expression as well as MIN/MAX, and
  // ensure B is still pending when A is returned.
  ObExpr partition_expr;
  static_cast<ExprMaker &>(maker).set_frame_idx(3);
  maker.make(partition_expr, eval);
  partition_expr.type_ = T_PSEUDO_PARTITION_LIST_COL;
  partition_expr.extra_ = 1;
  partition_expr.datum_meta_ = expr.datum_meta_;
  partition_expr.obj_meta_ = expr.obj_meta_;
  partition_expr.basic_funcs_ = expr.basic_funcs_;
  ExprFixedArray partition_columns(arena);
  ASSERT_EQ(OB_SUCCESS, partition_columns.init(2));
  ASSERT_EQ(OB_SUCCESS, partition_columns.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, partition_columns.push_back(&partition_expr));
  param.ext_file_column_exprs_ = &partition_columns;
  ObObj partition_values[2];
  ASSERT_EQ(OB_SUCCESS, partition_infos.reserve(2));
  for (int i = 0; i < 2; ++i) {
    partition_values[i].set_int(100 + i);
    ObExternalTablePartInfo part;
    part.part_id_ = i + 1;
    part.list_row_value_.assign(&partition_values[i], 1);
    ASSERT_EQ(OB_SUCCESS, partition_infos.set_part_pair_by_idx(i, part));
  }
  char temp_dir[] = "/tmp/ob_min_max_partition_XXXXXX";
  ASSERT_NE(nullptr, mkdtemp(temp_dir));
  DEFER(rmdir(temp_dir));
  const std::string empty_path = std::string(temp_dir) + "/empty";
  auto empty_sink = arrow::io::FileOutputStream::Open(empty_path);
  ASSERT_TRUE(empty_sink.ok());
  DEFER(unlink(empty_path.c_str()));
  ASSERT_TRUE(empty_sink.ValueOrDie()->Close().ok());
  const std::string partition_location = std::string("file://") + temp_dir;
  param.external_file_location_ = ObString::make_string(partition_location.c_str());
  for (ObExternalFileFormat::FormatType type :
       {ObExternalFileFormat::PARQUET_FORMAT, ObExternalFileFormat::ORC_FORMAT}) {
    SCOPED_TRACE(static_cast<int>(type));
    const char *file_names[] = {
        type == ObExternalFileFormat::PARQUET_FORMAT ? "a.parquet" : "a.orc",
        type == ObExternalFileFormat::PARQUET_FORMAT ? "b.parquet" : "b.orc"};
    const std::string paths[] = {std::string(temp_dir) + "/" + file_names[0],
                                 std::string(temp_dir) + "/" + file_names[1]};
    DEFER(unlink(paths[0].c_str()); unlink(paths[1].c_str()));
    for (int i = 0; i < 2; ++i) {
      const int64_t values[] = {10 + i * 10, 30 + i * 10};
      if (type == ObExternalFileFormat::PARQUET_FORMAT) {
        arrow::Int64Builder builder;
        ASSERT_TRUE(builder.AppendValues(values, 2).ok());
        std::shared_ptr<arrow::Array> array;
        ASSERT_TRUE(builder.Finish(&array).ok());
        const auto table = arrow::Table::Make(
            arrow::schema({arrow::field("i_no_bf", arrow::int64())}), {array});
        auto sink_result = arrow::io::FileOutputStream::Open(paths[i]);
        ASSERT_TRUE(sink_result.ok());
        auto sink = sink_result.ValueOrDie();
        ASSERT_TRUE(parquet::arrow::WriteTable(*table, arrow::default_memory_pool(), sink, 2).ok());
        ASSERT_TRUE(sink->Close().ok());
      } else {
        auto sink = orc::writeLocalFile(paths[i]);
        auto schema = orc::Type::buildTypeFromString("struct<i_no_bf:bigint>");
        auto writer = orc::createWriter(*schema, sink.get(), orc::WriterOptions());
        auto batch = writer->createRowBatch(2);
        auto *root = static_cast<orc::StructVectorBatch *>(batch.get());
        auto *column = static_cast<orc::LongVectorBatch *>(root->fields[0]);
        root->numElements = column->numElements = 2;
        column->data[0] = values[0];
        column->data[1] = values[1];
        writer->add(*batch);
        writer->close();
      }
    }
    for (auto aggregate : {T_FUN_SYS_EXT_MIN, T_FUN_SYS_EXT_MAX}) {
      SCOPED_TRACE(static_cast<int>(aggregate));
      result_expr.type_ = aggregate;
      // One zero-byte file, one missing file, both consecutively, or no skip.
      for (int skip_mode = 0; skip_mode < 4; ++skip_mode) {
        SCOPED_TRACE(skip_mode);
        const int skipped_count = skip_mode == 3 ? 0 : (skip_mode == 2 ? 2 : 1);
        param.scan_tasks_.reuse();
        for (int i = 0; i < skipped_count + 2; ++i) {
          auto &task = tasks[i];
          const bool skipped = i > 0 && i <= skipped_count;
          const bool missing = skipped && (skip_mode == 1 || i == 2);
          task.reader_type_ = type;
          task.part_id_ = i <= skipped_count ? 1 : 2;
          task.file_url_ = ObString::make_string(skipped ? (missing ? "missing" : "empty")
                                                      : file_names[i == 0 ? 0 : 1]);
          task.file_size_ = skipped ? (missing ? 1 : 0) : -1;
          task.modification_time_ = skipped ? 1 : 0;
          ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
        }
        ObExternalTableAccessService service;
        ObNewRowIterator *iter = nullptr;
        ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
        DEFER(if (nullptr != iter) { service.revert_scan_iter(iter); });
        auto *dispatcher = static_cast<ObLakeTableRowIterDispatcher *>(iter);
        for (int pass = 0; pass < 2; ++pass) {
          SCOPED_TRACE(pass);
          int64_t count = 0;
          const int64_t expected_a = aggregate == T_FUN_SYS_EXT_MIN ? 10 : 30;
          for (int i = 0; i < 2; ++i) {
            ASSERT_EQ(OB_SUCCESS, iter->get_next_rows(count, 1));
            ASSERT_EQ(1, count);
            EXPECT_EQ(100 + i, partition_expr.locate_batch_datums(eval)[0].get_int());
            EXPECT_EQ(expected_a + i * 10, result_expr.locate_batch_datums(eval)[0].get_int());
            EXPECT_EQ(i + 1, native_reader_state(*dispatcher, true).part_id_);
            EXPECT_EQ(skipped_count + 1 + i, dispatcher->current_reader_->iter_->get_next_task_idx());
          }
          EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, 1));
          EXPECT_EQ(0, count);
          if (pass == 0) {
            ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
          }
        }
        if (skip_mode == 2) {
          // No aggregate has started when all of A is skipped: continue to B
          // without manufacturing an empty result for A.
          tasks[0].file_size_ = 0;
          tasks[0].modification_time_ = 1;
          ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
          int64_t count = 0;
          ASSERT_EQ(OB_SUCCESS, iter->get_next_rows(count, 1));
          ASSERT_EQ(1, count);
          EXPECT_EQ(101, partition_expr.locate_batch_datums(eval)[0].get_int());
          EXPECT_EQ(aggregate == T_FUN_SYS_EXT_MIN ? 20 : 40,
                    result_expr.locate_batch_datums(eval)[0].get_int());
          EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, 1));
          EXPECT_EQ(0, count);
        }
      }
    }
  }
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
