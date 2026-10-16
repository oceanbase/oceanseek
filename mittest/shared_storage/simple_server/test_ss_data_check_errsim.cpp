/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef ERRSIM
#error "test_ss_data_check_errsim must be built with OB_ERRSIM=ON"
#endif

#define private public
#define protected public
#include "deps/oblib/src/common/storage/ob_device_common.h"
#undef protected
#undef private

#include "mtlenv/mock_tenant_module_env.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_sorter.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"

#include <algorithm>
#include <vector>

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;

namespace unittest
{

static ObSimpleMemLimitGetter DATA_CHECK_ERRSIM_MEM_LIMIT_GETTER;

class TestSSDataCheckErrsim : public blocksstable::TestDataFilePrepare
{
public:
  static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
  static const int64_t MACRO_BLOCK_COUNT = 15 * 1024;
  static const int64_t BUF_SIZE = 32 * 1024;
  static const int64_t MEM_LIMIT = 8 * 1024 * 1024;

  TestSSDataCheckErrsim()
    : TestDataFilePrepare(
          &DATA_CHECK_ERRSIM_MEM_LIMIT_GETTER,
          "TestSSDataCheckErrsim",
          MACRO_BLOCK_SIZE,
          MACRO_BLOCK_COUNT),
      dag_scheduler_(nullptr)
  {}

  virtual void SetUp() override;
  virtual void TearDown() override;

  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, ObTimerService::get_instance().start());
  }

  static void TearDownTestCase()
  {
    ObTimerService::get_instance().stop();
    ObTimerService::get_instance().wait();
    ObTimerService::get_instance().destroy();
  }

private:
  int init_tenant_mgr_();

private:
  share::ObTenantDagScheduler *dag_scheduler_;
};

int TestSSDataCheckErrsim::init_tenant_mgr_()
{
  int ret = OB_SUCCESS;
  const int64_t ulmt = 128LL << 30;
  if (OB_FAIL(DATA_CHECK_ERRSIM_MEM_LIMIT_GETTER.add_tenant(
      OB_SYS_TENANT_ID, ulmt, ulmt))) {
    STORAGE_LOG(WARN, "fail to add sys tenant memory limit", K(ret));
  } else if (OB_FAIL(DATA_CHECK_ERRSIM_MEM_LIMIT_GETTER.add_tenant(
      OB_SERVER_TENANT_ID, ulmt, ulmt))) {
    STORAGE_LOG(WARN, "fail to add server tenant memory limit", K(ret));
  } else {
    lib::set_memory_limit(128LL << 32);
  }
  return ret;
}

void TestSSDataCheckErrsim::SetUp()
{
  TestDataFilePrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, init_tenant_mgr_());
  ASSERT_EQ(OB_SUCCESS, common::ObClockGenerator::init());
  ASSERT_EQ(OB_SUCCESS,
      tmp_file::ObTmpPageCache::get_instance().init("dc_errsim_page_cache", 1));

  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ASSERT_EQ(OB_SUCCESS, mtl_new_default(dag_scheduler_));
  tenant_ctx.set(dag_scheduler_);
  ObTenantIOManager *io_service = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  ASSERT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  ASSERT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);

  ObTimerService *timer_service = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObTimerService::mtl_new(timer_service));
  tenant_ctx.set(timer_service);

  tmp_file::ObTenantTmpFileManager *tmp_file_mgr = nullptr;
  ASSERT_EQ(OB_SUCCESS, mtl_new_default(tmp_file_mgr));
  tenant_ctx.set(tmp_file_mgr);
  ASSERT_EQ(OB_SUCCESS,
      tmp_file::ObTenantTmpFileManager::mtl_init(tmp_file_mgr));
  tmp_file_mgr->get_sn_file_manager().write_cache_.default_memory_limit_ =
      40 * 1024 * 1024;
  ASSERT_EQ(OB_SUCCESS, ObTimerService::mtl_start(timer_service));
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr->start());
  ASSERT_EQ(OB_SUCCESS,
      share::ObTenantDagScheduler::mtl_init(dag_scheduler_));
  ObTenantEnv::set_tenant(&tenant_ctx);
}

void TestSSDataCheckErrsim::TearDown()
{
  ASSERT_NE(nullptr, dag_scheduler_);
  dag_scheduler_->stop();
  dag_scheduler_->wait();
  dag_scheduler_->destroy();
  ObTenantEnv::get_tenant()->set(
      static_cast<share::ObTenantDagScheduler *>(nullptr));
  mtl_destroy_default(dag_scheduler_);

  tmp_file::ObTenantTmpFileManager *tmp_file_mgr =
      MTL(tmp_file::ObTenantTmpFileManager *);
  if (OB_NOT_NULL(tmp_file_mgr)) {
    tmp_file_mgr->stop();
    tmp_file_mgr->wait();
    tmp_file_mgr->destroy();
  }
  tmp_file::ObTmpPageCache::get_instance().destroy();
  TestDataFilePrepare::TearDown();
  common::ObClockGenerator::destroy();

  ObTimerService *timer_service = MTL(ObTimerService *);
  ASSERT_NE(nullptr, timer_service);
  timer_service->stop();
  timer_service->wait();
  timer_service->destroy();
}

static MacroBlockId make_data_check_errsim_id(const int64_t seq)
{
  MacroBlockId macro_id(
      MacroBlockId::DEFAULT_FIRST_ID, 200001, 100, 200, seq);
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  return macro_id;
}

static int init_data_check_errsim_item(
    const int64_t macro_seq,
    ObSSDataCheckExpectedItem &item)
{
  int ret = OB_SUCCESS;
  share::SCN reorganize_scn;
  share::SCN meta_scn;
  if (OB_FAIL(reorganize_scn.convert_for_logservice(100))) {
    STORAGE_LOG(WARN, "fail to build errsim reorganize scn", K(ret));
  } else if (OB_FAIL(meta_scn.convert_for_logservice(200))) {
    STORAGE_LOG(WARN, "fail to build errsim meta scn", K(ret));
  } else if (OB_FAIL(item.init(
      make_data_check_errsim_id(macro_seq), share::ObLSID(1),
      ObTabletID(200001), reorganize_scn, meta_scn))) {
    STORAGE_LOG(WARN, "fail to initialize errsim expected item",
        K(ret), K(macro_seq));
  }
  return ret;
}

static int get_data_check_tmp_file_fds(
    const uint64_t tenant_id,
    std::vector<int64_t> &fds)
{
  int ret = OB_SUCCESS;
  ObSEArray<int64_t, 64> fd_array;
  fds.clear();
  if (OB_FAIL(FILE_MANAGER_INSTANCE_WITH_MTL_SWITCH.get_tmp_file_fds(
      tenant_id, fd_array))) {
    STORAGE_LOG(WARN, "fail to get data check tmp file fds",
        K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; i < fd_array.count(); ++i) {
      fds.push_back(fd_array.at(i));
    }
    std::sort(fds.begin(), fds.end());
  }
  return ret;
}

static int wait_data_check_tmp_file_fds(
    const uint64_t tenant_id,
    const std::vector<int64_t> &expected_fds)
{
  int ret = OB_SUCCESS;
  bool is_equal = false;
  std::vector<int64_t> actual_fds;
  const int64_t deadline = ObTimeUtility::current_time() + 10L * 1000L * 1000L;
  while (OB_SUCC(ret) && !is_equal
      && ObTimeUtility::current_time() < deadline) {
    if (OB_FAIL(get_data_check_tmp_file_fds(tenant_id, actual_fds))) {
      STORAGE_LOG(WARN, "fail to read data check tmp file fds",
          K(ret), K(tenant_id));
    } else {
      is_equal = actual_fds == expected_fds;
      if (!is_equal) {
        ob_usleep(10L * 1000L);
      }
    }
  }
  if (OB_SUCC(ret) && !is_equal) {
    ret = OB_TIMEOUT;
    STORAGE_LOG(WARN, "data check tmp file fds did not return to expected set",
        K(ret), K(tenant_id),
        "expected_fd_count", expected_fds.size(),
        "actual_fd_count", actual_fds.size());
  }
  return ret;
}

static int build_one_data_check_run(
    ObMarkSweepTmpFileMgr &tmp_file_mgr,
    const int64_t macro_seq,
    ObSSDataCheckSortedFileInfo &run)
{
  int ret = OB_SUCCESS;
  ObSSDataCheckExpectedRunWriter writer;
  ObSSDataCheckExpectedItem item;
  ObSEArray<ObSSDataCheckSortedFileInfo, 1> runs;
  run.reset();
  if (OB_FAIL(writer.init(
      OB_SYS_TENANT_ID, TestSSDataCheckErrsim::MEM_LIMIT,
      TestSSDataCheckErrsim::BUF_SIZE, 0, &tmp_file_mgr))) {
    STORAGE_LOG(WARN, "fail to initialize data check run writer", K(ret));
  } else if (OB_FAIL(init_data_check_errsim_item(macro_seq, item))) {
    STORAGE_LOG(WARN, "fail to initialize data check run item",
        K(ret), K(macro_seq));
  } else if (OB_FAIL(writer.add_item(item))) {
    STORAGE_LOG(WARN, "fail to add data check run item",
        K(ret), K(macro_seq));
  } else if (OB_FAIL(writer.finish(runs))) {
    STORAGE_LOG(WARN, "fail to finish data check run",
        K(ret), K(macro_seq));
  } else if (OB_UNLIKELY(1 != runs.count())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected data check run count",
        K(ret), K(macro_seq), "run_count", runs.count());
  } else if (OB_FAIL(run.assign(runs.at(0)))) {
    STORAGE_LOG(WARN, "fail to retain data check run",
        K(ret), K(macro_seq));
  }
  runs.reset();
  writer.reset();
  return ret;
}

class ScopedDataCheckErrsimEvent final
{
public:
  ScopedDataCheckErrsimEvent() : event_name_(nullptr), is_armed_(false) {}
  ~ScopedDataCheckErrsimEvent()
  {
    const int ret = reset();
    if (OB_SUCCESS != ret) {
      STORAGE_LOG(WARN, "fail to reset data check errsim event", K(ret));
    }
  }

  int arm(const char *event_name)
  {
    int ret = OB_SUCCESS;
    EventItem event_item;
    if (OB_UNLIKELY(is_armed_ || OB_ISNULL(event_name))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      event_item.error_code_ = OB_ERR_UNEXPECTED;
      event_item.occur_ = 1;
      if (OB_FAIL(EventTable::set_event(event_name, event_item))) {
        STORAGE_LOG(WARN, "fail to arm data check errsim event",
            K(ret), KCSTRING(event_name));
      } else {
        event_name_ = event_name;
        is_armed_ = true;
      }
    }
    return ret;
  }

  int reset()
  {
    int ret = OB_SUCCESS;
    if (is_armed_) {
      EventItem event_item;
      if (OB_FAIL(EventTable::set_event(event_name_, event_item))) {
        STORAGE_LOG(WARN, "fail to clear data check errsim event",
            K(ret), KCSTRING(event_name_));
      } else {
        event_name_ = nullptr;
        is_armed_ = false;
      }
    }
    return ret;
  }

private:
  const char *event_name_;
  bool is_armed_;
  DISALLOW_COPY_AND_ASSIGN(ScopedDataCheckErrsimEvent);
};

TEST_F(TestSSDataCheckErrsim, expected_run_acquire_failure_removes_tmp_file)
{
  std::vector<int64_t> baseline_fds;
  ASSERT_EQ(OB_SUCCESS,
      get_data_check_tmp_file_fds(OB_SYS_TENANT_ID, baseline_fds));

  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SERVER_TENANT_ID));
  ObSSDataCheckExpectedRunWriter writer;
  ObSSDataCheckExpectedItem item;
  ObSEArray<ObSSDataCheckSortedFileInfo, 1> runs;
  ASSERT_EQ(OB_SUCCESS, writer.init(
      OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &tmp_file_mgr));
  ASSERT_EQ(OB_SUCCESS, init_data_check_errsim_item(1001, item));
  ASSERT_EQ(OB_SUCCESS, writer.add_item(item));
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.finish(runs));
  EXPECT_EQ(0, runs.count());

  runs.reset();
  writer.reset();
  ASSERT_EQ(OB_SUCCESS,
      wait_data_check_tmp_file_fds(OB_SYS_TENANT_ID, baseline_fds));
  tmp_file_mgr.reset();
}

TEST_F(TestSSDataCheckErrsim, expected_run_failures_release_tmp_file)
{
  static const char *ERRSIM_EVENTS[] = {
      "EN_SS_DATA_CHECK_EXPECTED_RUN_WRITE_ERROR",
      "EN_SS_DATA_CHECK_EXPECTED_RUN_SYNC_ERROR",
      "EN_SS_DATA_CHECK_EXPECTED_RUN_PUBLISH_ERROR"};
  std::vector<int64_t> baseline_fds;
  ASSERT_EQ(OB_SUCCESS,
      get_data_check_tmp_file_fds(OB_SYS_TENANT_ID, baseline_fds));

  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  for (int64_t i = 0; i < ARRAYSIZEOF(ERRSIM_EVENTS); ++i) {
    ObSSDataCheckExpectedRunWriter writer;
    ObSSDataCheckExpectedItem item;
    ObSEArray<ObSSDataCheckSortedFileInfo, 1> runs;
    ScopedDataCheckErrsimEvent errsim_event;
    ASSERT_EQ(OB_SUCCESS, writer.init(
        OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &tmp_file_mgr));
    ASSERT_EQ(OB_SUCCESS, init_data_check_errsim_item(1100 + i, item));
    ASSERT_EQ(OB_SUCCESS, writer.add_item(item));
    ASSERT_EQ(OB_SUCCESS, errsim_event.arm(ERRSIM_EVENTS[i]));
    EXPECT_EQ(OB_ERR_UNEXPECTED, writer.finish(runs));
    EXPECT_EQ(0, runs.count());
    ASSERT_EQ(OB_SUCCESS, errsim_event.reset());

    runs.reset();
    writer.reset();
    ASSERT_EQ(OB_SUCCESS,
        wait_data_check_tmp_file_fds(OB_SYS_TENANT_ID, baseline_fds));
  }
  tmp_file_mgr.reset();
}

TEST_F(TestSSDataCheckErrsim, expected_merge_failures_release_output_file)
{
  static const char *ERRSIM_EVENTS[] = {
      "EN_SS_DATA_CHECK_EXPECTED_MERGE_WRITE_ERROR",
      "EN_SS_DATA_CHECK_EXPECTED_MERGE_SYNC_ERROR",
      "EN_SS_DATA_CHECK_EXPECTED_MERGE_PUBLISH_ERROR"};
  std::vector<int64_t> baseline_fds;
  ASSERT_EQ(OB_SUCCESS,
      get_data_check_tmp_file_fds(OB_SYS_TENANT_ID, baseline_fds));

  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ObSSDataCheckExpectedMerger merger;
  ObSEArray<ObSSDataCheckSortedFileInfo, 2> inputs;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  for (int64_t i = 0; i < 2; ++i) {
    ObSSDataCheckSortedFileInfo run;
    ASSERT_EQ(OB_SUCCESS,
        build_one_data_check_run(tmp_file_mgr, 1200 + i, run));
    ASSERT_EQ(OB_SUCCESS, inputs.push_back(run));
    run.reset();
  }

  ASSERT_EQ(OB_SUCCESS, merger.init(
      OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &tmp_file_mgr));
  ASSERT_EQ(OB_SUCCESS, merger.add_inputs(inputs));
  inputs.reset();
  ASSERT_EQ(OB_SUCCESS, merger.start_merge());
  int64_t pass_idx = -1;
  int64_t batch_count = 0;
  ASSERT_EQ(OB_SUCCESS, merger.get_current_pass_idx(pass_idx));
  ASSERT_EQ(0, pass_idx);
  ASSERT_EQ(OB_SUCCESS, merger.get_batch_count(pass_idx, batch_count));
  ASSERT_EQ(1, batch_count);

  std::vector<int64_t> input_fds;
  ASSERT_EQ(OB_SUCCESS,
      get_data_check_tmp_file_fds(OB_SYS_TENANT_ID, input_fds));
  ASSERT_EQ(baseline_fds.size() + 2, input_fds.size());
  for (int64_t i = 0; i < ARRAYSIZEOF(ERRSIM_EVENTS); ++i) {
    ScopedDataCheckErrsimEvent errsim_event;
    ASSERT_EQ(OB_SUCCESS, errsim_event.arm(ERRSIM_EVENTS[i]));
    EXPECT_EQ(OB_ERR_UNEXPECTED, merger.batch_merge(pass_idx, 0));
    ASSERT_EQ(OB_SUCCESS, errsim_event.reset());
    ASSERT_EQ(OB_SUCCESS,
        wait_data_check_tmp_file_fds(OB_SYS_TENANT_ID, input_fds));
  }

  ASSERT_EQ(OB_SUCCESS, merger.cleanup());
  ASSERT_EQ(OB_SUCCESS,
      wait_data_check_tmp_file_fds(OB_SYS_TENANT_ID, baseline_fds));
  tmp_file_mgr.reset();
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_ss_data_check_errsim.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
