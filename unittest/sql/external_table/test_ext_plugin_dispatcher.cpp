/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "share/diagnosis/ob_runtime_profile.h"
#include "share/ob_cluster_version.h"
#include <gtest/gtest.h>
#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"
#include "share/external_table/ob_external_table_utils.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase {
namespace sql {
namespace unittest {
using namespace common;
using namespace storage;

// This base outlives the dispatcher's cached readers.
struct DispatcherReaderState {
  int constructed_ = 0;
  int initialized_ = 0;
  int calls_ = 0;
  int batches_per_task_ = 1;
  int empty_batches_per_task_ = 0;
  int fail_call_ = -1;
  int fail_init_ = -1;
  bool eof_with_batch_ = true;
  int reset_ = 0;
  int destroyed_ = 0;
  const int64_t *output_ = nullptr;
  ObSEArray<ObIExtTblScanTask *, 4> seen_;
  ObSEArray<const ObExternalTableRowIterator *, 4> task_readers_;
  ObSEArray<const ObTableScanParam *, 2> params_;
};

class DispatcherReader : public ObExternalTableRowIterator {
public:
  explicit DispatcherReader(DispatcherReaderState &state,
                            ObExternalFileFormat::FormatType type = ObExternalFileFormat::PARQUET_FORMAT)
      : state_(state), format_(type), value_(0) {}
  ~DispatcherReader() { ++state_.destroyed_; }
  bool can_handle_task(const ObIExtTblScanTask *task) const override
  { return can_handle_file_task(task, format_); }
  ObExternalTableFilterEvalInitializer *get_filter_eval_initializer() override {
    return &filter_eval_initializer_;
  }
  int64_t get_next_task_idx() const override { return iter_state_.task_idx_; }
  int set_next_task_idx(int64_t idx) override { iter_state_.task_idx_ = idx; return OB_SUCCESS; }
  int go_next_task_idx(int64_t &idx) {
    return ObExternalTableRowIterator::go_next_task_idx(iter_state_, idx);
  }
  int peek_reader_task(int64_t &idx) const {
    return ObExternalTableRowIterator::peek_next_task_idx_for_reader(iter_state_.task_idx_, idx);
  }
  int peek_next_task(int64_t &idx) const {
    return ObExternalTableRowIterator::peek_next_task_idx(iter_state_.task_idx_, idx);
  }
  ObExternalIteratorState &iterator_state() { return iter_state_; }
  int init(const ObTableScanParam *param) override {
    int ret = OB_SUCCESS;
    ++state_.initialized_;
    scan_param_ = param;
    EXPECT_EQ(OB_SUCCESS, state_.params_.push_back(param));
    if (state_.initialized_ == state_.fail_init_) {
      ret = OB_IO_ERROR;
    }
    return ret;
  }
  int get_next_row(ObNewRow *&row) override { return OB_NOT_SUPPORTED; }
  void reset() override {
    ++state_.reset_;
    iter_state_.reuse();
    filter_eval_initializer_.reset();
    remaining_ = empty_remaining_ = 0;
    state_.output_ = nullptr;
  }
  int get_next_rows(int64_t &count, int64_t capacity) override {
    int ret = OB_SUCCESS;
    count = 0;
    ++state_.calls_;
    if (state_.calls_ == state_.fail_call_) {
      ret = OB_IO_ERROR;
    } else if (OB_FAIL(filter_eval_initializer_.ensure_filter_eval_inited_once(
                   scan_param_->pd_storage_filters_, scan_param_))) {
    } else {
      if (remaining_ == 0 && empty_remaining_ == 0) {
        int64_t task_idx = -1;
        if (OB_SUCC(ret = go_next_task_idx(task_idx))) {
          EXPECT_EQ(task_idx + 1, iter_state_.task_idx_);
          auto *task = scan_param_->scan_tasks_.at(task_idx);
          EXPECT_EQ(OB_SUCCESS, state_.seen_.push_back(task));
          EXPECT_EQ(OB_SUCCESS, state_.task_readers_.push_back(this));
          value_ = static_cast<ObFileScanTask *>(task)->file_id_;
          remaining_ = state_.batches_per_task_;
          empty_remaining_ = state_.empty_batches_per_task_;
        }
      }
      if (OB_SUCC(ret)) {
        if (empty_remaining_ > 0) {
          --empty_remaining_; // Progress inside this task, with no output yet.
        } else if (remaining_ > 0) {
          --remaining_;
          state_.output_ = &value_;
          count = 1;
          int64_t next_task_idx = -1;
          if (remaining_ == 0 && state_.eof_with_batch_
              && OB_ITER_END == peek_reader_task(next_task_idx)) {
            ret = OB_ITER_END;
          }
        }
      }
    }
    return ret;
  }

private:
  DispatcherReaderState &state_;
  const ObExternalFileFormat::FormatType format_;
  ObExternalIteratorState iter_state_;
  ObExternalTableFilterEvalInitializer filter_eval_initializer_;
  int64_t value_;
  int remaining_ = 0;
  int empty_remaining_ = 0;
};

class TestDispatcher : public DispatcherReaderState,
                       public ObLakeTableRowIterDispatcher {
public:
  explicit TestDispatcher(const ObTableScanParam &shared_scan_param)
      : shared_scan_param_(shared_scan_param) {}

protected:
  int create_task_reader(const ObExternalFileFormat::FormatType reader_type,
                         ObIAllocator &allocator,
                         ObExternalTableRowIterator *&reader) override {
    EXPECT_NE(shared_scan_param_.allocator_, &allocator);
    ++constructed_;
    reader = OB_NEWx(DispatcherReader, &allocator, *this, reader_type);
    return reader == nullptr ? OB_ALLOCATE_MEMORY_FAILED : OB_SUCCESS;
  }

private:
  const ObTableScanParam &shared_scan_param_;
};

TEST(TestExtPluginDispatcher, RejectsOlderClusterVersion) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.external_file_format_.format_type_ =
      ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask task;
  task.reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  task.assigned_file_bytes_ = 10;
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));

  auto &version = ObClusterVersion::get_instance();
  version.update_cluster_version(CLUSTER_VERSION_4_6_3_0);
  TestDispatcher iter(param);
  EXPECT_EQ(OB_OP_NOT_ALLOW, iter.init(&param));
  EXPECT_EQ(0, iter.constructed_);
  version.update_cluster_version(CLUSTER_VERSION_4_6_3_1);
}

TEST(TestExtPluginDispatcher, OrderedTasksAndFinalBatchLifetime) {
  // P/S/P/S exercises reactivation; P/P/S/S/P exercises adjacent tasks as well.
  for (int scenario = 0; scenario < 2; ++scenario) {
    ObArenaAllocator arena;
    ObOpProfile<ObMetric> profile(ObProfileId::PHY_TABLE_SCAN, &arena);
    ObProfileSwitcher scope(&profile);
    ObTableScanParam param;
    param.tenant_id_ = OB_SYS_TENANT_ID;
    param.allocator_ = &arena;
    param.ext_tbl_filter_pd_level_ = 3;
    param.external_file_format_.format_type_ =
        ObExternalFileFormat::CPP_PLUGIN_FORMAT;
    ObPluginScanTask tasks[5];
    const int task_count = scenario == 0 ? 4 : 5;
    for (int i = 0; i < task_count; ++i) {
      const bool parquet = scenario == 0 ? i % 2 == 0 : i < 2 || i == 4;
      tasks[i].reader_type_ =
          parquet ? ObExternalFileFormat::PARQUET_FORMAT : ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT;
      tasks[i].file_id_ = i + 1;
      tasks[i].assigned_file_bytes_ = 10;
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
    }
    TestDispatcher iter(param);
    ASSERT_EQ(OB_SUCCESS, iter.init(&param));
    EXPECT_EQ(2, iter.constructed_);
    const ObExternalTableRowIterator *instances[2] = {nullptr, nullptr};
    int64_t count = 0;
    for (int i = 0; i < task_count; ++i) {
      if (i > 0) {
        // The prior final batch remains valid until the next fetch.
        ASSERT_NE(nullptr, iter.output_);
        EXPECT_EQ(tasks[i - 1].file_id_, *iter.output_);
        EXPECT_EQ(i, iter.seen_.count());
      }
      ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
      ASSERT_EQ(1, count);
      ASSERT_NE(nullptr, iter.output_);
      EXPECT_EQ(tasks[i].file_id_, *iter.output_);
      EXPECT_EQ(&tasks[i], iter.seen_.at(i));
      const int readers = 2;
      EXPECT_EQ(readers, iter.constructed_);
      EXPECT_EQ(readers, iter.initialized_);
      EXPECT_EQ(0, iter.destroyed_);
      const int type =
          tasks[i].reader_type_ == ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT ? 1 : 0;
      if (instances[type] == nullptr) {
        instances[type] = iter.task_readers_.at(i);
      }
      EXPECT_EQ(instances[type], iter.task_readers_.at(i));
      EXPECT_EQ(&param, iter.params_.at(readers - 1));
    }
    for (int repeat = 0; repeat < 2; ++repeat) {
      count = 99;
      EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
      EXPECT_EQ(0, count);
      EXPECT_EQ(0, iter.destroyed_);
    }
    ASSERT_EQ(task_count, param.scan_tasks_.count());
    for (int i = 0; i < task_count; ++i) {
      EXPECT_EQ(&tasks[i], param.scan_tasks_.at(i));
    }
    EXPECT_EQ(3, param.ext_tbl_filter_pd_level_);
    for (auto route : {ObProfileId::EXT_PLUGIN_OB_READER,
                       ObProfileId::EXT_PLUGIN_SDK_READER}) {
      ObOpProfile<ObMetric> *reader = nullptr;
      ASSERT_EQ(OB_SUCCESS, profile.get_or_register_child(route, reader));
      const int assigned =
          route == ObProfileId::EXT_PLUGIN_SDK_READER ? 2 : task_count - 2;
      for (auto metric : {ObMetricId::EXT_PLUGIN_ASSIGNED_TASKS,
                          ObMetricId::EXT_PLUGIN_OUTPUT_ROWS}) {
        ASSERT_NE(nullptr, reader->get_metric(metric));
        EXPECT_EQ(assigned, reader->get_metric(metric)->value());
      }
      ASSERT_NE(nullptr,
                reader->get_metric(ObMetricId::EXT_PLUGIN_ASSIGNED_FILE_BYTES));
      EXPECT_EQ(assigned * 10,
                reader->get_metric(ObMetricId::EXT_PLUGIN_ASSIGNED_FILE_BYTES)
                    ->value());
    }
  }
}

TEST(TestExtPluginDispatcher, RescanReusesReaderWithNewAssignment) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.timeout_ = 100;
  param.external_file_format_.format_type_ =
      ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[2];
  for (int i = 0; i < 2; ++i) {
    tasks[i].reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
    tasks[i].file_id_ = i + 7;
    tasks[i].assigned_file_bytes_ = 10;
  }
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
  TestDispatcher iter(param);
  ASSERT_EQ(OB_SUCCESS, iter.init(&param));
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
  ASSERT_EQ(1, count);
  EXPECT_EQ(7, *iter.output_);
  const auto *reader = iter.task_readers_.at(0);
  const auto *bound_param = iter.params_.at(0);
  ObExternalTableAccessService service;
  ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, &iter));
  // Repeated reuse is harmless; table_rescan sees the replacement assignment.
  ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, &iter));
  EXPECT_EQ(1, iter.reset_);
  param.scan_tasks_.reuse();
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[1]));
  param.timeout_ = 200;
  ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, &iter));
  EXPECT_EQ(1, iter.reset_);
  EXPECT_EQ(1, iter.initialized_);
  ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
  ASSERT_EQ(1, count);
  EXPECT_EQ(8, *iter.output_);
  EXPECT_EQ(&tasks[1], iter.seen_.at(1));
  EXPECT_EQ(reader, iter.task_readers_.at(1));
  EXPECT_EQ(&param, bound_param);
  EXPECT_EQ(&tasks[1], bound_param->scan_tasks_.at(0));
  EXPECT_EQ(200, bound_param->timeout_);
  EXPECT_EQ(1, iter.constructed_);
  EXPECT_EQ(1, iter.initialized_);

  EXPECT_EQ(0, iter.destroyed_);
  EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
}

TEST(TestExtPluginDispatcher, RescanPreparesNewReaderBeforeFetch) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[3];
  for (int i = 0; i < 3; ++i) {
    tasks[i].reader_type_ = i == 1 ? ObExternalFileFormat::ORC_FORMAT : ObExternalFileFormat::PARQUET_FORMAT;
    tasks[i].file_id_ = i + 1;
  }
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
  TestDispatcher iter(param);
  ASSERT_EQ(OB_SUCCESS, iter.init(&param));
  EXPECT_EQ(1, iter.initialized_);
  EXPECT_EQ(0, iter.calls_);
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
  const auto *parquet_reader = iter.task_readers_.at(0);
  const int calls_before_rescan = iter.calls_;
  ObExternalTableAccessService service;
  ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, &iter));
  param.scan_tasks_.reuse();
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[1]));
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[2]));
  // Fetch must not initialize a newly introduced type behind the caller's back.
  EXPECT_EQ(OB_NOT_INIT, iter.get_next_rows(count, 8));
  EXPECT_EQ(0, count);
  EXPECT_EQ(1, iter.constructed_);
  EXPECT_EQ(1, iter.initialized_);
  EXPECT_EQ(calls_before_rescan, iter.calls_);
  ASSERT_EQ(OB_SUCCESS, service.table_rescan(param, &iter));
  EXPECT_EQ(2, iter.constructed_);
  EXPECT_EQ(2, iter.initialized_);
  EXPECT_EQ(1, iter.reset_);
  EXPECT_EQ(calls_before_rescan, iter.calls_);
  EXPECT_EQ(1, iter.seen_.count()); // Preparation does not claim or read a task.
  for (int i = 1; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
    ASSERT_EQ(1, count);
    EXPECT_EQ(tasks[i].file_id_, *iter.output_);
    EXPECT_EQ(&tasks[i], iter.seen_.at(i));
    EXPECT_EQ(2, iter.initialized_);
  }
  EXPECT_EQ(parquet_reader, iter.task_readers_.at(2));
  EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
}

TEST(TestExtPluginDispatcher, ReaderInitFailureIsReportedByLifecycleEntry) {
  for (bool fail_initial_scan : {true, false}) {
    ObArenaAllocator arena;
    ObTableScanParam param;
    param.tenant_id_ = OB_SYS_TENANT_ID;
    param.allocator_ = &arena;
    param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
    ObPluginScanTask tasks[2];
    tasks[0].reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
    tasks[1].reader_type_ = ObExternalFileFormat::ORC_FORMAT;
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
    TestDispatcher iter(param);
    if (fail_initial_scan) {
      iter.fail_init_ = 1;
      EXPECT_EQ(OB_IO_ERROR, iter.init(&param));
    } else {
      ASSERT_EQ(OB_SUCCESS, iter.init(&param));
      ObExternalTableAccessService service;
      ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, &iter));
      param.scan_tasks_.reuse();
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[1]));
      iter.fail_init_ = 2;
      EXPECT_EQ(OB_IO_ERROR, service.table_rescan(param, &iter));
    }
    const int init_attempts = iter.initialized_;
    int64_t count = 99;
    EXPECT_EQ(OB_NOT_INIT, iter.get_next_rows(count, 8));
    EXPECT_EQ(0, count);
    EXPECT_EQ(init_attempts, iter.initialized_);
    EXPECT_EQ(0, iter.calls_);
    EXPECT_EQ(1, iter.destroyed_);
  }
}

TEST(TestExtPluginDispatcher, CursorProbeDoesNotConsumeAnotherReadersTask) {
  ObTableScanParam param;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[4];
  tasks[0].file_url_ = share::ObExternalTableUtils::dummy_file_name();
  tasks[1].reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  tasks[2].reader_type_ = ObExternalFileFormat::ORC_FORMAT;
  tasks[3].reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  for (auto &task : tasks) {
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
  }
  DispatcherReaderState reader_state;
  DispatcherReader plugin(reader_state, ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT);
  DispatcherReader parquet(reader_state, ObExternalFileFormat::PARQUET_FORMAT);
  DispatcherReader orc(reader_state, ObExternalFileFormat::ORC_FORMAT);
  ASSERT_EQ(OB_SUCCESS, plugin.init(&param));
  ASSERT_EQ(OB_SUCCESS, parquet.init(&param));
  ASSERT_EQ(OB_SUCCESS, orc.init(&param));
  EXPECT_FALSE(parquet.can_handle_task(nullptr));
  EXPECT_TRUE(parquet.can_handle_task(&tasks[1]));
  EXPECT_FALSE(orc.can_handle_task(&tasks[1]));
  EXPECT_TRUE(orc.can_handle_task(&tasks[2]));
  ObDirectLoadStreamScanTask direct_load_task;
  ObIExtTblScanTask base_task;
  ObArenaAllocator arena;
  ObIcebergScanTask iceberg_task(arena);
  EXPECT_FALSE(parquet.can_handle_task(&direct_load_task));
  EXPECT_FALSE(parquet.can_handle_task(&base_task));
  EXPECT_FALSE(parquet.can_handle_task(&iceberg_task));
  int64_t idx = -1;
  plugin.iterator_state().cur_file_id_ = 17;
  for (int repeat = 0; repeat < 2; ++repeat) {
    EXPECT_EQ(OB_NOT_THE_OBJECT, plugin.peek_reader_task(idx));
    EXPECT_EQ(-1, idx);
    EXPECT_EQ(0, plugin.get_next_task_idx());
    EXPECT_EQ(17, plugin.iterator_state().cur_file_id_);
  }
  EXPECT_EQ(OB_NOT_THE_OBJECT, plugin.go_next_task_idx(idx));
  EXPECT_EQ(0, plugin.get_next_task_idx());
  EXPECT_EQ(OB_SUCCESS, plugin.peek_next_task(idx));
  EXPECT_EQ(1, idx);
  ASSERT_EQ(OB_SUCCESS, parquet.go_next_task_idx(idx));
  EXPECT_EQ(1, idx);
  EXPECT_EQ(2, parquet.get_next_task_idx());
  for (int i = 0; i < 2; ++i) {
    EXPECT_EQ(OB_NOT_THE_OBJECT, parquet.go_next_task_idx(idx));
    EXPECT_EQ(2, parquet.get_next_task_idx());
    EXPECT_EQ(OB_SUCCESS, parquet.peek_next_task(idx));
    EXPECT_EQ(2, idx);
  }
  ASSERT_EQ(OB_SUCCESS, orc.set_next_task_idx(parquet.get_next_task_idx()));
  ASSERT_EQ(OB_SUCCESS, orc.go_next_task_idx(idx));
  EXPECT_EQ(2, idx);
  ASSERT_EQ(OB_SUCCESS, parquet.set_next_task_idx(orc.get_next_task_idx()));
  ASSERT_EQ(OB_SUCCESS, parquet.go_next_task_idx(idx));
  EXPECT_EQ(3, idx);
  EXPECT_EQ(OB_ITER_END, parquet.peek_next_task(idx));
  EXPECT_EQ(4, parquet.get_next_task_idx());
}

TEST(TestExtPluginDispatcher, HiveTaskUsesScanFormat) {
  ObTableScanParam param;
  param.external_file_format_.format_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  ObHiveScanTask task;
  DispatcherReaderState reader_state;
  DispatcherReader parquet(reader_state, ObExternalFileFormat::PARQUET_FORMAT);
  DispatcherReader orc(reader_state, ObExternalFileFormat::ORC_FORMAT);
  ASSERT_EQ(OB_SUCCESS, parquet.init(&param));
  ASSERT_EQ(OB_SUCCESS, orc.init(&param));
  EXPECT_TRUE(parquet.can_handle_task(&task));
  EXPECT_FALSE(orc.can_handle_task(&task));
  param.external_file_format_.format_type_ = ObExternalFileFormat::ORC_FORMAT;
  EXPECT_FALSE(parquet.can_handle_task(&task));
  EXPECT_TRUE(orc.can_handle_task(&task));
}

TEST(TestExtPluginDispatcher, NativeDummyTasksDoNotChangeTheCursorDuringProbe) {
  for (auto format : {ObExternalFileFormat::PARQUET_FORMAT, ObExternalFileFormat::ORC_FORMAT}) {
    SCOPED_TRACE(static_cast<int>(format));
    ObTableScanParam param;
    param.external_file_format_.format_type_ = format;
    ObExtTableScanTask tasks[5];
    for (int i = 0; i < 5; ++i) {
      tasks[i].file_url_ = i % 2 == 0 ? share::ObExternalTableUtils::dummy_file_name()
                                    : ObString::make_string("native-file");
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
    }
    DispatcherReaderState reader_state;
    const auto type = format == ObExternalFileFormat::PARQUET_FORMAT
        ? ObExternalFileFormat::PARQUET_FORMAT : ObExternalFileFormat::ORC_FORMAT;
    DispatcherReader reader(reader_state, type);
    ASSERT_EQ(OB_SUCCESS, reader.init(&param));
    EXPECT_TRUE(reader.can_handle_task(&tasks[1]));
    auto &state = reader.iterator_state();
    int64_t idx = -1;
    for (int64_t next_idx : {1, 3}) {
      const int64_t old_cursor = state.task_idx_;
      state.cur_file_id_ = 17;
      EXPECT_EQ(OB_SUCCESS, reader.peek_reader_task(idx));
      EXPECT_EQ(next_idx, idx);
      EXPECT_EQ(old_cursor, state.task_idx_);
      EXPECT_EQ(17, state.cur_file_id_);
      ASSERT_EQ(OB_SUCCESS, reader.go_next_task_idx(idx));
      EXPECT_EQ(next_idx, idx);
      EXPECT_EQ(next_idx + 1, state.task_idx_);
    }
    state.cur_file_id_ = 19;
    for (int repeat = 0; repeat < 2; ++repeat) {
      EXPECT_EQ(OB_ITER_END, reader.peek_reader_task(idx));
      EXPECT_EQ(OB_ITER_END, reader.go_next_task_idx(idx));
      EXPECT_EQ(-1, idx);
      EXPECT_EQ(4, state.task_idx_); // A trailing dummy is only inspected.
      EXPECT_EQ(19, state.cur_file_id_);
    }
    reader.reset();
    param.scan_tasks_.reuse();
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
    for (int repeat = 0; repeat < 2; ++repeat) {
      EXPECT_EQ(OB_ITER_END, reader.go_next_task_idx(idx));
      EXPECT_EQ(-1, idx);
      EXPECT_EQ(0, state.task_idx_);
    }
  }
}

TEST(TestExtPluginDispatcher, IcebergReaderChecksTaskFileFormat) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.external_file_format_.format_type_ = ObExternalFileFormat::INVALID_FORMAT;
  ObIcebergScanTask task(arena);
  task.file_url_ = ObString::make_string("iceberg-file");
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
  DispatcherReaderState reader_state;
  DispatcherReader parquet(reader_state, ObExternalFileFormat::PARQUET_FORMAT);
  DispatcherReader orc(reader_state, ObExternalFileFormat::ORC_FORMAT);
  ASSERT_EQ(OB_SUCCESS, parquet.init(&param));
  ASSERT_EQ(OB_SUCCESS, orc.init(&param));

  int64_t idx = -1;
  task.file_format_ = iceberg::DataFileFormat::PARQUET;
  EXPECT_EQ(OB_SUCCESS, parquet.peek_reader_task(idx));
  EXPECT_EQ(0, idx);
  EXPECT_EQ(OB_NOT_THE_OBJECT, orc.peek_reader_task(idx));
  EXPECT_EQ(-1, idx);
  task.file_format_ = iceberg::DataFileFormat::ORC;
  EXPECT_EQ(OB_NOT_THE_OBJECT, parquet.peek_reader_task(idx));
  EXPECT_EQ(-1, idx);
  EXPECT_EQ(OB_SUCCESS, orc.peek_reader_task(idx));
  EXPECT_EQ(0, idx);
}

TEST(TestExtPluginDispatcher, IcebergTasksSwitchReadersAndRescan) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.lake_table_format_ = share::ObLakeTableFormat::ICEBERG;
  param.external_file_format_.format_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  ObIcebergScanTask parquet(arena);
  ObIcebergScanTask orc(arena);
  ObIcebergScanTask dummy(arena);
  parquet.file_format_ = iceberg::DataFileFormat::PARQUET;
  parquet.file_url_ = ObString::make_string("data.parquet");
  parquet.file_id_ = 1;
  orc.file_format_ = iceberg::DataFileFormat::ORC;
  orc.file_url_ = ObString::make_string("data.orc");
  orc.file_id_ = 2;
  dummy.file_url_ = share::ObExternalTableUtils::dummy_file_name();
  for (ObIExtTblScanTask *task : {static_cast<ObIExtTblScanTask *>(&dummy),
                                  static_cast<ObIExtTblScanTask *>(&parquet),
                                  static_cast<ObIExtTblScanTask *>(&orc),
                                  static_cast<ObIExtTblScanTask *>(&parquet)}) {
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(task));
  }
  TestDispatcher iter(param);
  auto &version = ObClusterVersion::get_instance();
  version.update_cluster_version(CLUSTER_VERSION_4_6_3_0);
  const int init_ret = iter.init(&param);
  version.update_cluster_version(CLUSTER_VERSION_4_6_3_1);
  ASSERT_EQ(OB_SUCCESS, init_ret); // The plugin rolling-upgrade gate does not apply.
  ASSERT_EQ(2, iter.constructed_);
  int64_t count = 0;
  for (int64_t id : {1, 2, 1}) {
    ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
    ASSERT_EQ(1, count);
    ASSERT_NE(nullptr, iter.output_);
    EXPECT_EQ(id, *iter.output_);
  }
  EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
  ObExternalTableAccessService service;
  ASSERT_EQ(OB_SUCCESS, service.reuse_scan_iter(false, &iter));
  param.scan_tasks_.reuse();
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&orc));
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&parquet));
  ASSERT_EQ(OB_SUCCESS, iter.rescan());
  for (int64_t id : {2, 1}) {
    ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
    ASSERT_EQ(1, count);
    EXPECT_EQ(id, *iter.output_);
  }
  EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
  EXPECT_EQ(2, iter.constructed_);

  orc.file_format_ = iceberg::DataFileFormat::INVALID;
  EXPECT_EQ(OB_NOT_SUPPORTED, iter.rescan());
  EXPECT_EQ(OB_NOT_INIT, iter.get_next_rows(count, 8));
  orc.file_format_ = iceberg::DataFileFormat::ORC;
  ASSERT_EQ(OB_SUCCESS, iter.rescan());
  ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
  EXPECT_EQ(2, *iter.output_);
}

TEST(TestExtPluginDispatcher, EmptyIcebergAssignmentUsesDummyReader) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.lake_table_format_ = share::ObLakeTableFormat::ICEBERG;
  param.external_file_format_.format_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  ObIcebergScanTask dummy(arena);
  dummy.file_url_ = share::ObExternalTableUtils::dummy_file_name();
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&dummy));
  ObExternalTableAccessService service;
  ObNewRowIterator *iter = nullptr;
  ASSERT_EQ(OB_SUCCESS, service.table_scan(param, iter));
  ASSERT_NE(nullptr, iter);
  EXPECT_EQ(nullptr, dynamic_cast<ObLakeTableRowIterDispatcher *>(iter));
  int64_t count = -1;
  EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, 8));
  EXPECT_EQ(OB_SUCCESS, service.table_rescan(param, iter));
  EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, 8));
  EXPECT_EQ(OB_SUCCESS, service.revert_scan_iter(iter));
}

TEST(TestExtPluginDispatcher, ReaderFileStateIsPrivateAndOnlyCursorIsTransferred) {
  ObTableScanParam param;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[3];
  tasks[0].reader_type_ = tasks[2].reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  tasks[1].reader_type_ = ObExternalFileFormat::ORC_FORMAT;
  for (auto &task : tasks) {
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
  }
  DispatcherReaderState reader_state;
  DispatcherReader parquet(reader_state, ObExternalFileFormat::PARQUET_FORMAT);
  DispatcherReader orc(reader_state, ObExternalFileFormat::ORC_FORMAT);
  ASSERT_EQ(OB_SUCCESS, parquet.init(&param));
  ASSERT_EQ(OB_SUCCESS, orc.init(&param));
  auto &pq_state = parquet.iterator_state();
  auto &orc_state = orc.iterator_state();
  EXPECT_NE(&pq_state, &orc_state);
  int64_t idx = -1;
  ASSERT_EQ(OB_SUCCESS, parquet.go_next_task_idx(idx));
  EXPECT_EQ(0, idx);
  pq_state.cur_file_id_ = 17;
  pq_state.cur_file_url_ = ObString::make_string("p0.parquet");
  pq_state.cur_line_number_ = 10;
  pq_state.batch_first_row_line_num_ = 8;
  pq_state.is_delete_file_loaded_ = true;
  pq_state.part_id_ = 7;
  ObObj partition_cell;
  partition_cell.set_int(42);
  pq_state.part_list_val_.cells_ = &partition_cell;
  pq_state.part_list_val_.count_ = 1;
  orc_state.cur_file_id_ = 18;
  orc_state.cur_line_number_ = 9;
  orc_state.part_id_ = 8;
  for (int repeat = 0; repeat < 2; ++repeat) {
    EXPECT_EQ(OB_NOT_THE_OBJECT, parquet.go_next_task_idx(idx));
    EXPECT_EQ(-1, idx);
    EXPECT_EQ(1, pq_state.task_idx_);
    EXPECT_EQ(17, pq_state.cur_file_id_);
    EXPECT_EQ(10, pq_state.cur_line_number_);
    EXPECT_TRUE(pq_state.is_delete_file_loaded_);
  }
  ASSERT_EQ(OB_SUCCESS, orc.set_next_task_idx(parquet.get_next_task_idx()));
  EXPECT_EQ(18, orc_state.cur_file_id_);
  EXPECT_EQ(9, orc_state.cur_line_number_);
  EXPECT_EQ(8, orc_state.part_id_);
  ASSERT_EQ(OB_SUCCESS, orc.peek_reader_task(idx));
  EXPECT_EQ(1, idx);
  EXPECT_EQ(18, orc_state.cur_file_id_);
  ASSERT_EQ(OB_SUCCESS, orc.go_next_task_idx(idx));
  EXPECT_EQ(2, orc_state.task_idx_);
  EXPECT_EQ(0, orc_state.cur_file_id_);
  EXPECT_EQ(0, orc_state.cur_line_number_);
  EXPECT_EQ(8, orc_state.part_id_);
  EXPECT_EQ(17, pq_state.cur_file_id_);
  EXPECT_EQ(10, pq_state.cur_line_number_);
  ASSERT_EQ(OB_SUCCESS, parquet.set_next_task_idx(orc.get_next_task_idx()));
  ASSERT_EQ(OB_SUCCESS, parquet.go_next_task_idx(idx));
  EXPECT_EQ(2, idx);
  EXPECT_EQ(3, pq_state.task_idx_);
  EXPECT_EQ(0, pq_state.cur_file_id_);
  EXPECT_EQ(0, pq_state.cur_line_number_);
  EXPECT_EQ(0, pq_state.batch_first_row_line_num_);
  EXPECT_TRUE(pq_state.cur_file_url_.empty());
  EXPECT_FALSE(pq_state.is_delete_file_loaded_);
  EXPECT_EQ(7, pq_state.part_id_);
  EXPECT_EQ(&partition_cell, pq_state.part_list_val_.cells_);
  pq_state.cur_file_id_ = 19;
  EXPECT_EQ(OB_ITER_END, parquet.go_next_task_idx(idx));
  EXPECT_EQ(3, pq_state.task_idx_);
  EXPECT_EQ(19, pq_state.cur_file_id_);
  parquet.reset();
  EXPECT_EQ(0, pq_state.task_idx_);
  EXPECT_EQ(0, pq_state.part_id_);
  EXPECT_EQ(nullptr, pq_state.part_list_val_.cells_);
  EXPECT_EQ(0, pq_state.cur_file_id_);
  EXPECT_EQ(2, orc_state.task_idx_);
  orc.reset();
  EXPECT_EQ(0, orc_state.task_idx_);
}

TEST(TestExtPluginDispatcher, HandoffAcrossDummyTasksPreservesFileIds) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[5];
  const int64_t file_ids[] = {101, 202, 303, 404, 505};
  for (int i = 0; i < 5; ++i) {
    tasks[i].file_id_ = file_ids[i];
    if (i % 2 == 1) {
      tasks[i].file_url_ = share::ObExternalTableUtils::dummy_file_name();
    } else {
      tasks[i].reader_type_ = i == 2 ? ObExternalFileFormat::ORC_FORMAT
                                     : ObExternalFileFormat::PARQUET_FORMAT;
    }
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
  }
  TestDispatcher iter(param);
  ASSERT_EQ(OB_SUCCESS, iter.init(&param));
  for (int i = 0; i < 3; ++i) {
    const int task_idx = i * 2;
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 1));
    ASSERT_EQ(1, count);
    ASSERT_NE(nullptr, iter.output_);
    EXPECT_EQ(file_ids[task_idx], *iter.output_);
    ASSERT_EQ(i + 1, iter.seen_.count());
    EXPECT_EQ(&tasks[task_idx], iter.seen_.at(i));
    EXPECT_EQ(task_idx + 1, iter.task_readers_.at(i)->get_next_task_idx());
    for (int j = 0; j < 5; ++j) {
      EXPECT_EQ(file_ids[j], tasks[j].file_id_);
    }
  }
  EXPECT_EQ(iter.task_readers_.at(0), iter.task_readers_.at(2));
  EXPECT_NE(iter.task_readers_.at(0), iter.task_readers_.at(1));
  int64_t count = 0;
  EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 1));
  EXPECT_EQ(0, count);
}

TEST(TestExtPluginDispatcher, EmptyBatchesKeepCurrentReaderUntilTaskIsFinished) {
  for (bool eof_with_batch : {false, true}) {
    ObArenaAllocator arena;
    ObTableScanParam param;
    param.tenant_id_ = OB_SYS_TENANT_ID;
    param.allocator_ = &arena;
    param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
    ObPluginScanTask tasks[7];
    const ObExternalFileFormat::FormatType types[] = {ObExternalFileFormat::PARQUET_FORMAT,
        ObExternalFileFormat::ORC_FORMAT, ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT,
        ObExternalFileFormat::PARQUET_FORMAT, ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT};
    tasks[0].file_url_ = tasks[3].file_url_ = share::ObExternalTableUtils::dummy_file_name();
    int real_idx = 0;
    for (int i = 0; i < 7; ++i) {
      if (i != 0 && i != 3) {
        tasks[i].reader_type_ = types[real_idx++];
        tasks[i].file_id_ = real_idx;
      }
      ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
    }
    TestDispatcher iter(param);
    iter.batches_per_task_ = 3;
    iter.empty_batches_per_task_ = 2;
    iter.eof_with_batch_ = eof_with_batch;
    ASSERT_EQ(OB_SUCCESS, iter.init(&param));
    for (int task = 1; task <= 5; ++task) {
      for (int batch = 0; batch < 3; ++batch) {
        int64_t count = 99;
        ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
        EXPECT_EQ(1, count);
        ASSERT_NE(nullptr, iter.output_);
        EXPECT_EQ(task, *iter.output_);
        EXPECT_EQ(task, iter.seen_.count()); // No premature claim of the next type.
      }
    }
    int64_t count = 99;
    EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
    EXPECT_EQ(0, count);
    const int calls_at_end = iter.calls_;
    EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
    EXPECT_EQ(calls_at_end, iter.calls_);
    EXPECT_EQ(3, iter.constructed_);
    EXPECT_EQ(0, iter.reset_);
  }
}

TEST(TestExtPluginDispatcher, EmptyTasksReachGlobalEndAndErrorsDoNotSwitchReaders) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[3];
  const ObExternalFileFormat::FormatType reader_types[] = {
      ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT, ObExternalFileFormat::PARQUET_FORMAT,
      ObExternalFileFormat::ORC_FORMAT};
  for (int i = 0; i < 3; ++i) {
    tasks[i].reader_type_ = reader_types[i];
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
  }
  TestDispatcher iter(param);
  iter.batches_per_task_ = 0;
  iter.empty_batches_per_task_ = 2;
  ASSERT_EQ(OB_SUCCESS, iter.init(&param));
  int64_t count = 99;
  EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
  EXPECT_EQ(0, count);
  EXPECT_EQ(3, iter.seen_.count());
  ASSERT_EQ(OB_SUCCESS, iter.rescan());
  iter.batches_per_task_ = 2;
  iter.empty_batches_per_task_ = 0;
  iter.fail_call_ = iter.calls_ + 2;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
  EXPECT_EQ(OB_IO_ERROR, iter.get_next_rows(count, 8));
  EXPECT_EQ(0, count);
  EXPECT_EQ(4, iter.seen_.count());
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.get_next_rows(count, 0));
}

TEST(TestExtPluginDispatcher, DummyOnlyAndEmptyRescanHaveStableEnd) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask dummy;
  dummy.file_url_ = share::ObExternalTableUtils::dummy_file_name();
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&dummy));
  TestDispatcher iter(param);
  ASSERT_EQ(OB_SUCCESS, iter.init(&param));
  for (int scan = 0; scan < 2; ++scan) {
    int64_t count = 99;
    EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
    EXPECT_EQ(0, count);
    EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
    EXPECT_EQ(0, iter.constructed_);
    EXPECT_EQ(0, iter.calls_);
    iter.reset();
    param.scan_tasks_.reuse();
    ASSERT_EQ(OB_SUCCESS, iter.rescan());
  }
}

TEST(TestExtPluginDispatcher, InvalidReaderTypesAreRejectedBeforeInit) {
  for (const int type : {-128, -1, 0, static_cast<int>(ObExternalFileFormat::ODPS_FORMAT),
                         static_cast<int>(ObExternalFileFormat::JAVA_PLUGIN_FORMAT),
                         static_cast<int>(ObExternalFileFormat::KAFKA_FORMAT),
                         static_cast<int>(ObExternalFileFormat::CPP_PLUGIN_FORMAT),
                         static_cast<int>(ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT),
                         static_cast<int>(ObExternalFileFormat::MAX_FORMAT), 127}) {
    SCOPED_TRACE(type);
    ObArenaAllocator arena;
    ObTableScanParam param;
    param.tenant_id_ = OB_SYS_TENANT_ID;
    param.allocator_ = &arena;
    param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
    ObPluginScanTask task;
    task.reader_type_ = static_cast<ObExternalFileFormat::FormatType>(type);
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
    TestDispatcher iter(param);
    EXPECT_EQ(OB_ERR_UNEXPECTED, iter.init(&param));
    EXPECT_EQ(0, iter.constructed_);
    EXPECT_EQ(0, iter.initialized_);
    int64_t count = 99;
    EXPECT_EQ(OB_NOT_INIT, iter.get_next_rows(count, 8));
    EXPECT_EQ(0, count);
    EXPECT_EQ(0, iter.calls_);
  }
}

TEST(TestExtPluginDispatcher, InvalidReaderTypesFailRescanAndAllowRecovery) {
  ObArenaAllocator arena;
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask task;
  task.reader_type_ = ObExternalFileFormat::ORC_FORMAT;
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
  TestDispatcher iter(param);
  ASSERT_EQ(OB_SUCCESS, iter.init(&param));
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
  ASSERT_EQ(1, count);
  const int calls = iter.calls_;
  for (const int type : {-128, -1, 0, static_cast<int>(ObExternalFileFormat::ODPS_FORMAT),
                         static_cast<int>(ObExternalFileFormat::JAVA_PLUGIN_FORMAT),
                         static_cast<int>(ObExternalFileFormat::KAFKA_FORMAT),
                         static_cast<int>(ObExternalFileFormat::CPP_PLUGIN_FORMAT),
                         static_cast<int>(ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT),
                         static_cast<int>(ObExternalFileFormat::MAX_FORMAT), 127}) {
    SCOPED_TRACE(type);
    iter.reset();
    task.reader_type_ = static_cast<ObExternalFileFormat::FormatType>(type);
    EXPECT_EQ(OB_ERR_UNEXPECTED, iter.rescan());
    count = 99;
    EXPECT_EQ(OB_NOT_INIT, iter.get_next_rows(count, 8));
    EXPECT_EQ(0, count);
    EXPECT_EQ(calls, iter.calls_);
    EXPECT_EQ(1, iter.constructed_);
  }
  task.reader_type_ = ObExternalFileFormat::ORC_FORMAT;
  ASSERT_EQ(OB_SUCCESS, iter.rescan());
  ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
  EXPECT_EQ(1, count);
  EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
  EXPECT_EQ(1, iter.constructed_);
  EXPECT_EQ(1, iter.initialized_);
}

TEST(TestExtPluginDispatcher, HandoffChecksReaderTypeAtArrayAccess) {
  for (const int type : {-128, -1, 0, static_cast<int>(ObExternalFileFormat::ODPS_FORMAT),
                         static_cast<int>(ObExternalFileFormat::JAVA_PLUGIN_FORMAT),
                         static_cast<int>(ObExternalFileFormat::KAFKA_FORMAT),
                         static_cast<int>(ObExternalFileFormat::CPP_PLUGIN_FORMAT),
                         static_cast<int>(ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT),
                         static_cast<int>(ObExternalFileFormat::MAX_FORMAT), 127}) {
    SCOPED_TRACE(type);
    ObArenaAllocator arena;
    ObTableScanParam param;
    param.tenant_id_ = OB_SYS_TENANT_ID;
    param.allocator_ = &arena;
    param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
    ObPluginScanTask tasks[2];
    tasks[0].reader_type_ = ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT;
    tasks[1].reader_type_ = ObExternalFileFormat::ORC_FORMAT;
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[1]));
    TestDispatcher iter(param);
    ASSERT_EQ(OB_SUCCESS, iter.init(&param));
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
    ASSERT_EQ(1, count);
    // Fault injection after validation: routing must check before indexing even
    // if the caller violates the immutable-assignment contract.
    tasks[1].reader_type_ = static_cast<ObExternalFileFormat::FormatType>(type);
    for (int retry = 0; retry < 2; ++retry) {
      count = 99;
      EXPECT_EQ(OB_ERR_UNEXPECTED, iter.get_next_rows(count, 8));
      EXPECT_EQ(0, count);
      EXPECT_EQ(1, iter.seen_.count());
      EXPECT_EQ(2, iter.constructed_);
    }
  }
}

// This reader models a format that does not implement late materialization.
class DispatcherReaderWithoutFilter : public ObExternalTableRowIterator {
public:
  DispatcherReaderWithoutFilter(DispatcherReaderState &state, ObExternalFileFormat::FormatType type)
      : state_(state), format_(type), value_(0) {}
  ~DispatcherReaderWithoutFilter() { ++state_.destroyed_; }
  bool can_handle_task(const ObIExtTblScanTask *task) const override
  { return can_handle_file_task(task, format_); }
  int64_t get_next_task_idx() const override { return iter_state_.task_idx_; }
  int set_next_task_idx(int64_t idx) override { iter_state_.task_idx_ = idx; return OB_SUCCESS; }
  int init(const ObTableScanParam *param) override {
    scan_param_ = param;
    ++state_.initialized_;
    return state_.params_.push_back(param);
  }
  int get_next_row(ObNewRow *&row) override { UNUSED(row); return OB_NOT_SUPPORTED; }
  void reset() override {
    ++state_.reset_;
    iter_state_.reuse();
    state_.output_ = nullptr;
  }
  int get_next_rows(int64_t &count, int64_t capacity) override {
    UNUSED(capacity);
    int ret = OB_SUCCESS;
    count = 0;
    ++state_.calls_;
    int64_t task_idx = -1;
    if (OB_FAIL(go_next_task_idx(iter_state_, task_idx))) {
    } else {
      ObIExtTblScanTask *task = scan_param_->scan_tasks_.at(task_idx);
      if (OB_FAIL(state_.seen_.push_back(task))) {
      } else if (OB_FAIL(state_.task_readers_.push_back(this))) {
      } else {
        value_ = static_cast<ObPluginScanTask *>(task)->file_id_;
        state_.output_ = &value_;
        count = 1;
      }
    }
    return ret;
  }
private:
  DispatcherReaderState &state_;
  const ObExternalFileFormat::FormatType format_;
  ObExternalIteratorState iter_state_;
  int64_t value_;
};

class MixedCapabilityDispatcher : public TestDispatcher {
public:
  explicit MixedCapabilityDispatcher(const ObTableScanParam &param) : TestDispatcher(param) {}
protected:
  int create_task_reader(const ObExternalFileFormat::FormatType type,
                         ObIAllocator &allocator,
                         ObExternalTableRowIterator *&reader) override {
    int ret = OB_SUCCESS;
    if (ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT == type) {
      ++constructed_;
      reader = OB_NEWx(DispatcherReaderWithoutFilter, &allocator, *this, type);
      if (OB_ISNULL(reader)) { ret = OB_ALLOCATE_MEMORY_FAILED; }
    } else {
      ret = TestDispatcher::create_task_reader(type, allocator, reader);
    }
    return ret;
  }
};

class CountingFilter : public ObPushdownFilterExecutor {
public:
  CountingFilter(ObIAllocator &alloc, ObPushdownOperator &op) : ObPushdownFilterExecutor(alloc, op) {
    set_type(PushdownExecutorType::BLACK_FILTER_EXECUTOR);
  }
  ObIArray<uint64_t> &get_col_ids() override { return cols_; }
  int init_evaluated_datums(bool &valid) override { ++inits_; valid = true; return OB_SUCCESS; }
  void clear() override { ++clears_; }
  int inits_ = 0;
  int clears_ = 0;
private:
  ObSEArray<uint64_t, 1> cols_;
};

TEST(TestExtPluginDispatcher, FailedReaderInitClearsFilterAndRescanReinitializesIt) {
  ObArenaAllocator arena;
  ObExecContext exec(arena);
  ObEvalCtx eval(exec);
  ObPushdownExprSpec spec(arena);
  ObPushdownOperator op(eval, spec);
  CountingFilter filter(arena, op);
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.pd_storage_filters_ = &filter;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[2];
  tasks[0].reader_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  tasks[1].reader_type_ = ObExternalFileFormat::ORC_FORMAT;
  ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[0]));
  {
    TestDispatcher iter(param);
    ASSERT_EQ(OB_SUCCESS, iter.init(&param));
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
    ASSERT_EQ(1, filter.inits_);
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[1]));
    iter.fail_init_ = 2;
    EXPECT_EQ(OB_IO_ERROR, iter.rescan());
    EXPECT_EQ(1, filter.clears_);
    EXPECT_EQ(OB_NOT_INIT, iter.get_next_rows(count, 8));
    iter.fail_init_ = -1;
    ASSERT_EQ(OB_SUCCESS, iter.rescan());
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
      EXPECT_EQ(1, count);
      EXPECT_EQ(2, filter.inits_);
      EXPECT_EQ(1, filter.clears_);
    }
  }
  EXPECT_EQ(4, filter.clears_);
}

TEST(TestExtPluginDispatcher, FilterInitializationSurvivesReaderWithoutCapability) {
  ObArenaAllocator arena;
  ObExecContext exec(arena);
  ObEvalCtx eval(exec);
  ObPushdownExprSpec spec(arena);
  ObPushdownOperator op(eval, spec);
  CountingFilter filter(arena, op);
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.pd_storage_filters_ = &filter;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[5];
  const ObExternalFileFormat::FormatType types[] = {
      ObExternalFileFormat::PARQUET_FORMAT, ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT,
      ObExternalFileFormat::ORC_FORMAT, ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT,
      ObExternalFileFormat::PARQUET_FORMAT};
  for (int i = 0; i < 5; ++i) {
    tasks[i].reader_type_ = types[i];
    tasks[i].file_id_ = 100 + i;
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
  }
  {
    MixedCapabilityDispatcher iter(param);
    ASSERT_EQ(OB_SUCCESS, iter.init(&param));
    for (int scan = 0; scan < 2; ++scan) {
      if (scan > 0) {
        ASSERT_EQ(OB_SUCCESS, iter.rescan());
      }
      for (int i = 0; i < 5; ++i) {
        int64_t count = 0;
        ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
        ASSERT_EQ(1, count);
        ASSERT_NE(nullptr, iter.output_);
        EXPECT_EQ(tasks[i].file_id_, *iter.output_);
        ASSERT_EQ(scan * 5 + i + 1, iter.task_readers_.count());
        ObExternalTableRowIterator *reader =
            const_cast<ObExternalTableRowIterator *>(iter.task_readers_.at(scan * 5 + i));
        const ObExternalTableFilterEvalInitializer *capability = reader->get_filter_eval_initializer();
        if (ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT == types[i]) {
          EXPECT_EQ(nullptr, capability);
        } else {
          ASSERT_NE(nullptr, capability);
          EXPECT_TRUE(capability->is_filter_eval_inited());
        }
        EXPECT_EQ(scan + 1, filter.inits_);
        EXPECT_EQ(0, filter.clears_);
      }
      int64_t count = 0;
      EXPECT_EQ(OB_ITER_END, iter.get_next_rows(count, 8));
      EXPECT_EQ(0, count);
      iter.reset();
    }
  }
  EXPECT_EQ(4, filter.clears_); // Three cached readers and the dispatcher.
}

TEST(TestExtPluginDispatcher, FilterIsSharedAcrossHandoffsAndClearedAtTeardown) {
  ObArenaAllocator arena;
  ObExecContext exec(arena);
  ObEvalCtx eval(exec);
  ObPushdownExprSpec spec(arena);
  ObPushdownOperator op(eval, spec);
  CountingFilter filter(arena, op);
  ObTableScanParam param;
  param.tenant_id_ = OB_SYS_TENANT_ID;
  param.allocator_ = &arena;
  param.pd_storage_filters_ = &filter;
  param.external_file_format_.format_type_ = ObExternalFileFormat::CPP_PLUGIN_FORMAT;
  ObPluginScanTask tasks[4];
  for (int i = 0; i < 4; ++i) {
    tasks[i].reader_type_ = i % 2 ? ObExternalFileFormat::CPP_PLUGIN_SDK_FORMAT : ObExternalFileFormat::PARQUET_FORMAT;
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&tasks[i]));
  }
  {
    TestDispatcher iter(param);
    ASSERT_EQ(OB_SUCCESS, iter.init(&param));
    for (int scan = 0; scan < 2; ++scan) {
      if (scan > 0) {
        ASSERT_EQ(OB_SUCCESS, iter.rescan());
      }
      for (int i = 0; i < 4; ++i) {
        int64_t count = 0;
        ASSERT_EQ(OB_SUCCESS, iter.get_next_rows(count, 8));
        EXPECT_EQ(scan + 1, filter.inits_);
        EXPECT_EQ(0, filter.clears_);
      }
      iter.reset();
    }
  }
  EXPECT_EQ(3, filter.clears_); // Two cached readers and the dispatcher.
  // A standalone reader also clears the filter at teardown.
  param.external_file_format_.format_type_ = ObExternalFileFormat::PARQUET_FORMAT;
  param.scan_tasks_.reuse();
  ObExtTableScanTask native_tasks[4];
  for (auto &task : native_tasks) {
    ASSERT_EQ(OB_SUCCESS, param.scan_tasks_.push_back(&task));
  }
  DispatcherReaderState native_state;
  native_state.eof_with_batch_ = false;
  {
    DispatcherReader native(native_state);
    ASSERT_EQ(OB_SUCCESS, native.init(&param));
    for (int scan = 0; scan < 2; ++scan) {
      for (int i = 0; i < 4; ++i) {
        int64_t count = 0;
        ASSERT_EQ(OB_SUCCESS, native.get_next_rows(count, 8));
        EXPECT_EQ(scan + 3, filter.inits_);
        EXPECT_EQ(3, filter.clears_);
      }
      native.reset();
    }
  }
  EXPECT_EQ(4, filter.clears_);
}

} // namespace unittest
} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv) {
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  oceanbase::common::ObClusterVersion::get_instance().update_cluster_version(
      CLUSTER_VERSION_4_6_3_1); // Minimum version required for this test.
  testing::InitGoogleTest(&argc, argv);
  int ret = oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_CURRENT_VERSION);
  if (oceanbase::common::OB_SUCCESS == ret) {
    ret = RUN_ALL_TESTS();
  }
  oceanbase::common::ObClusterVersion::get_instance().destroy();
  return ret;
}
