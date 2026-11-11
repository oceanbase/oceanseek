/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define private public
#define protected public
#include "deps/oblib/src/common/storage/ob_device_common.h"
#undef protected
#undef private

#include "lib/utility/ob_defer.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_cluster_version.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/ss_data_check/ob_ss_data_check_ctx.h"
#include "storage/ss_data_check/ob_ss_data_check_dag.h"
#include "storage/ss_data_check/ob_ss_data_check_define.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_producer.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_sort_task.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_sorter.h"
#define private public
#include "storage/ss_data_check/ob_ss_data_check_list_sorter.h"
#include "storage/ss_data_check/ob_ss_data_check_list_checker.h"
#undef private
#include "storage/ss_data_check/ob_ss_data_check_gen_task.h"
#include "storage/ss_data_check/ob_ss_data_check_list_task.h"
#include "storage/ss_data_check/ob_ss_data_check_meta_ref_checker.h"

#include <algorithm>
#include <atomic>
#include <functional>
#include <string>
#include <thread>
#include <vector>

namespace oceanbase
{
using namespace blocksstable;
using namespace common;
using namespace storage;

namespace unittest
{

static ObSimpleMemLimitGetter DATA_CHECK_MEM_LIMIT_GETTER;

class TestSSDataCheck : public blocksstable::TestDataFilePrepare
{
public:
  static const int64_t MACRO_BLOCK_SIZE = 2 * 1024 * 1024;
  static const int64_t MACRO_BLOCK_COUNT = 15 * 1024;
  static const int64_t BUF_SIZE = 32 * 1024;
  static const int64_t MEM_LIMIT = 8 * 1024 * 1024;

  TestSSDataCheck()
    : TestDataFilePrepare(
          &DATA_CHECK_MEM_LIMIT_GETTER,
          "TestSSDataCheck",
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

int TestSSDataCheck::init_tenant_mgr_()
{
  int ret = OB_SUCCESS;
  const int64_t ulmt = 128LL << 30;
  if (OB_FAIL(DATA_CHECK_MEM_LIMIT_GETTER.add_tenant(
      OB_SYS_TENANT_ID, ulmt, ulmt))) {
    STORAGE_LOG(WARN, "fail to add sys tenant memory limit", K(ret));
  } else if (OB_FAIL(DATA_CHECK_MEM_LIMIT_GETTER.add_tenant(
      OB_SERVER_TENANT_ID, ulmt, ulmt))) {
    STORAGE_LOG(WARN, "fail to add server tenant memory limit", K(ret));
  } else {
    lib::set_memory_limit(128LL << 32);
  }
  return ret;
}

void TestSSDataCheck::SetUp()
{
  TestDataFilePrepare::SetUp();
  ASSERT_EQ(OB_SUCCESS, init_tenant_mgr_());
  ASSERT_EQ(OB_SUCCESS,
      omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, common::ObClockGenerator::init());
  ASSERT_EQ(OB_SUCCESS,
      tmp_file::ObTmpPageCache::get_instance().init("dc_tmp_page_cache", 1));

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
  SERVER_STORAGE_META_SERVICE.is_started_ = true;
}

void TestSSDataCheck::TearDown()
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

static MacroBlockId make_data_check_id(const int64_t seq)
{
  MacroBlockId macro_id(
      MacroBlockId::DEFAULT_FIRST_ID, 200001, 100, 200, seq);
  macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
  return macro_id;
}

static MacroBlockId make_data_check_object_id(
    const ObStorageObjectType object_type,
    const int64_t seq)
{
  MacroBlockId macro_id = make_data_check_id(seq);
  macro_id.set_storage_object_type(static_cast<uint64_t>(object_type));
  macro_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
  return macro_id;
}

static share::SCN data_check_snapshot_scn()
{
  return share::SCN::max_scn();
}

static int build_data_check_relative_path(
    const MacroBlockId &macro_id,
    std::string &relative_path)
{
  int ret = OB_SUCCESS;
  char path_buf[OB_MAX_URI_LENGTH] = {};
  int64_t pos = 0;
  relative_path.clear();
  if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(
      macro_id, path_buf, sizeof(path_buf), pos))) {
    STORAGE_LOG(WARN, "fail to build data check relative path", K(ret), K(macro_id));
  } else if (OB_UNLIKELY(pos <= 0 || pos > sizeof(path_buf))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid data check relative path length", K(ret), K(pos));
  } else {
    relative_path.assign(path_buf, static_cast<size_t>(pos));
  }
  return ret;
}

static int init_expected_item_with_macro_id(
    const MacroBlockId &macro_id,
    ObSSDataCheckExpectedItem &item,
    const uint64_t ls_id,
    const uint64_t tablet_id,
    const int64_t reorganize_scn_value,
    const int64_t meta_scn_value)
{
  int ret = OB_SUCCESS;
  share::SCN reorganize_scn;
  share::SCN meta_scn;
  if (OB_FAIL(reorganize_scn.convert_for_logservice(
      reorganize_scn_value))) {
    STORAGE_LOG(WARN, "fail to build reorganize scn", K(ret));
  } else if (OB_FAIL(meta_scn.convert_for_logservice(meta_scn_value))) {
    STORAGE_LOG(WARN, "fail to build meta scn", K(ret));
  } else if (OB_FAIL(item.init(
      macro_id, share::ObLSID(static_cast<int64_t>(ls_id)),
      ObTabletID(tablet_id),
      reorganize_scn, meta_scn))) {
    STORAGE_LOG(WARN, "fail to initialize expected item", K(ret), K(macro_id));
  }
  return ret;
}

static int init_expected_item(
    const int64_t macro_seq,
    ObSSDataCheckExpectedItem &item,
    const uint64_t ls_id = 1,
    const uint64_t tablet_id = 200001,
    const int64_t reorganize_scn_value = 100,
    const int64_t meta_scn_value = 200)
{
  return init_expected_item_with_macro_id(
      make_data_check_id(macro_seq), item, ls_id, tablet_id,
      reorganize_scn_value, meta_scn_value);
}

static int read_expected_file(
    const ObSSDataCheckSortedFileInfo &file_info,
    std::vector<std::string> &relative_paths)
{
  int ret = OB_SUCCESS;
  relative_paths.clear();
  if (file_info.is_valid()) {
    ObSSDataCheckExpectedItem empty_sample;
    ObFragmentReaderV2<ObSSDataCheckExpectedItem> reader(
        OB_MALLOC_NORMAL_BLOCK_SIZE);
    if (OB_FAIL(reader.init_for_pread(
        file_info.get_fd(), file_info.get_dir_id(), 0,
        file_info.get_tenant_id(), empty_sample, TestSSDataCheck::BUF_SIZE))) {
      STORAGE_LOG(WARN, "fail to initialize expected reader", K(ret));
    } else if (OB_FAIL(reader.open())) {
      STORAGE_LOG(WARN, "fail to open expected reader", K(ret));
    } else {
      bool is_complete = false;
      const ObSSDataCheckExpectedItem *item = nullptr;
      while (OB_SUCC(ret) && !is_complete) {
        if (OB_FAIL(reader.get_next_item(item))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            is_complete = true;
          }
        } else if (OB_ISNULL(item) || OB_UNLIKELY(!item->is_valid())) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          std::string relative_path;
          if (OB_FAIL(build_data_check_relative_path(
              item->get_macro_id(), relative_path))) {
            STORAGE_LOG(WARN, "fail to rebuild expected relative path", K(ret), KPC(item));
          } else {
            relative_paths.push_back(relative_path);
          }
        }
      }
    }
  }
  return ret;
}

static int merge_batches(
    ObSSDataCheckExpectedSortPipeline &pipeline,
    const int64_t pass_idx,
    const int64_t batch_count,
    const bool run_in_parallel)
{
  int ret = OB_SUCCESS;
  if (!run_in_parallel) {
    for (int64_t batch_idx = 0;
         OB_SUCC(ret) && batch_idx < batch_count;
         ++batch_idx) {
      if (OB_FAIL(pipeline.batch_merge(pass_idx, batch_idx))) {
        STORAGE_LOG(WARN, "fail to merge expected batch", K(ret),
            K(pass_idx), K(batch_idx));
      }
    }
  } else {
    ObTenantBase *tenant_ctx = ObTenantEnv::get_tenant();
    if (OB_ISNULL(tenant_ctx)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      std::vector<int> merge_rets(batch_count, OB_SUCCESS);
      std::vector<std::thread> workers;
      std::atomic<int64_t> ready_count(0);
      std::atomic<bool> start_workers(false);
      workers.reserve(batch_count);
      for (int64_t batch_idx = 0; batch_idx < batch_count; ++batch_idx) {
        workers.emplace_back([
            &pipeline, pass_idx, batch_idx, tenant_ctx,
            &merge_rets, &ready_count, &start_workers]() {
          ObTenantEnv::set_tenant(tenant_ctx);
          ready_count.fetch_add(1, std::memory_order_release);
          while (!start_workers.load(std::memory_order_acquire)) {
            std::this_thread::yield();
          }
          merge_rets.at(batch_idx) =
              pipeline.batch_merge(pass_idx, batch_idx);
        });
      }
      while (ready_count.load(std::memory_order_acquire) < batch_count) {
        std::this_thread::yield();
      }
      start_workers.store(true, std::memory_order_release);
      for (auto &worker : workers) {
        worker.join();
      }
      for (int64_t batch_idx = 0;
           OB_SUCC(ret) && batch_idx < batch_count;
           ++batch_idx) {
        if (OB_SUCCESS != merge_rets.at(batch_idx)) {
          ret = merge_rets.at(batch_idx);
        }
      }
    }
  }
  return ret;
}

static int drive_expected_merge(
    ObSSDataCheckExpectedSortPipeline &pipeline,
    const bool run_batches_in_parallel,
    int64_t &pass_count)
{
  int ret = OB_SUCCESS;
  int64_t pass_idx = -1;
  int64_t input_count = 0;
  pass_count = 0;
  if (OB_FAIL(pipeline.prepare_merge())) {
    STORAGE_LOG(WARN, "fail to prepare expected merge", K(ret));
  } else if (OB_FAIL(pipeline.get_current_pass_idx(pass_idx))) {
    STORAGE_LOG(WARN, "fail to get current expected pass", K(ret));
  } else if (OB_FAIL(pipeline.get_input_count(pass_idx, input_count))) {
    STORAGE_LOG(WARN, "fail to get expected input count", K(ret));
  }

  while (OB_SUCC(ret) && input_count > 1) {
    int64_t batch_count = 0;
    if (OB_FAIL(pipeline.get_batch_count(pass_idx, batch_count))) {
      STORAGE_LOG(WARN, "fail to get expected batch count", K(ret));
    } else if (OB_FAIL(merge_batches(
        pipeline, pass_idx, batch_count, run_batches_in_parallel))) {
      STORAGE_LOG(WARN, "fail to execute expected merge batches", K(ret));
    } else if (OB_FAIL(pipeline.prepare_next_pass(pass_idx))) {
      STORAGE_LOG(WARN, "fail to prepare next expected pass", K(ret));
    } else {
      ++pass_count;
      if (OB_FAIL(pipeline.get_current_pass_idx(pass_idx))) {
        STORAGE_LOG(WARN, "fail to get next expected pass", K(ret));
      } else if (OB_FAIL(pipeline.get_input_count(pass_idx, input_count))) {
        STORAGE_LOG(WARN, "fail to get next expected input count", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(pipeline.complete_merge(pass_idx))) {
    STORAGE_LOG(WARN, "fail to complete expected merge", K(ret));
  }
  return ret;
}

class FakeDataCheckObjectLister final : public ObISSDataCheckObjectLister
{
public:
  FakeDataCheckObjectLister()
    : expected_prefixes_(),
      object_name_sets_(),
      results_(),
      emit_counts_before_error_(),
      called_prefixes_(),
      call_count_(0),
      emitted_object_count_(0),
      accepted_object_count_(0),
      accepted_item_count_(0),
      after_emit_hook_()
  {}

  void set_listing(
      const std::string &expected_prefix,
      const std::vector<std::string> &object_names,
      const int result = OB_SUCCESS,
      const int64_t emit_count_before_error = 0)
  {
    expected_prefixes_.clear();
    object_name_sets_.clear();
    results_.clear();
    emit_counts_before_error_.clear();
    called_prefixes_.clear();
    call_count_ = 0;
    emitted_object_count_ = 0;
    accepted_object_count_ = 0;
    accepted_item_count_ = 0;
    after_emit_hook_ = nullptr;
    add_listing(
        expected_prefix, object_names, result, emit_count_before_error);
  }

  void set_after_emit_hook(
      const std::function<void(int64_t)> &after_emit_hook)
  {
    after_emit_hook_ = after_emit_hook;
  }

  void add_listing(
      const std::string &expected_prefix,
      const std::vector<std::string> &object_names,
      const int result = OB_SUCCESS,
      const int64_t emit_count_before_error = 0)
  {
    expected_prefixes_.push_back(expected_prefix);
    object_name_sets_.push_back(object_names);
    results_.push_back(result);
    emit_counts_before_error_.push_back(emit_count_before_error);
  }

  virtual int list_files(
      const ObString &relative_prefix,
      ObBaseDirEntryOperator &op) override
  {
    int ret = OB_SUCCESS;
    const int64_t listing_idx = call_count_;
    ++call_count_;
    const std::string actual_prefix(
        relative_prefix.ptr(), relative_prefix.length());
    called_prefixes_.push_back(actual_prefix);
    if (listing_idx >= expected_prefixes_.size()
        || listing_idx >= object_name_sets_.size()
        || listing_idx >= results_.size()
        || listing_idx >= emit_counts_before_error_.size()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (actual_prefix != expected_prefixes_.at(listing_idx)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const std::vector<std::string> &object_names =
          object_name_sets_.at(listing_idx);
      const int result = results_.at(listing_idx);
      const int64_t emit_count = OB_SUCCESS == result
          ? object_names.size()
          : MIN(static_cast<int64_t>(object_names.size()),
                emit_counts_before_error_.at(listing_idx));
      for (int64_t i = 0;
           OB_SUCC(ret) && i < emit_count;
           ++i) {
        struct dirent entry;
        MEMSET(&entry, 0, sizeof(entry));
        const std::string &name = object_names.at(i);
        if (name.empty() || name.length() >= sizeof(entry.d_name)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          MEMCPY(entry.d_name, name.data(), name.length());
          entry.d_name[name.length()] = '\0';
          ret = op.func(&entry);
          // 该 Fake 只由 ListChecker 使用，传入的 callback 固定为 actual sorter。
          ObSSDataCheckListedObjectSorter &sorter =
              static_cast<ObSSDataCheckListedObjectSorter &>(op);
          accepted_object_count_ = sorter.collected_count_;
          accepted_item_count_ = sorter.item_list_.size();
          if (OB_SUCCESS == ret) {
            ++emitted_object_count_;
            if (after_emit_hook_) {
              after_emit_hook_(emitted_object_count_);
            }
          }
        }
      }
      if (OB_SUCC(ret) && OB_SUCCESS != result) {
        ret = result;
      }
    }
    return ret;
  }

  int64_t get_call_count() const { return call_count_; }
  int64_t get_emitted_object_count() const
  { return emitted_object_count_; }
  int64_t get_accepted_object_count() const
  { return accepted_object_count_; }
  int64_t get_accepted_item_count() const
  { return accepted_item_count_; }
  const std::vector<std::string> &get_called_prefixes() const
  { return called_prefixes_; }

private:
  std::vector<std::string> expected_prefixes_;
  std::vector<std::vector<std::string>> object_name_sets_;
  std::vector<int> results_;
  std::vector<int64_t> emit_counts_before_error_;
  std::vector<std::string> called_prefixes_;
  int64_t call_count_;
  int64_t emitted_object_count_;
  int64_t accepted_object_count_;
  int64_t accepted_item_count_;
  std::function<void(int64_t)> after_emit_hook_;
};

class FakeDataCheckListStopChecker final
    : public share::ObCheckStopProvider
{
public:
  FakeDataCheckListStopChecker()
    : result_(OB_SUCCESS), result_check_idx_(INT64_MAX), call_count_(0)
  {}

  void set_result(
      const int64_t result_check_idx,
      const int result)
  {
    result_ = result;
    result_check_idx_ = result_check_idx;
    call_count_ = 0;
  }

  void arm_result(const int result)
  {
    result_ = result;
    result_check_idx_ = call_count_ + 1;
  }

  virtual int check_stop() const override
  {
    ++call_count_;
    return call_count_ >= result_check_idx_ ? result_ : OB_SUCCESS;
  }

  int64_t get_call_count() const { return call_count_; }

private:
  int result_;
  int64_t result_check_idx_;
  mutable int64_t call_count_;
};

class CollectDataCheckListedObjectOp final
    : public ObBaseDirEntryOperator
{
public:
  CollectDataCheckListedObjectOp() : names_() {}

  virtual int func(const dirent *entry) override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(entry) || '\0' == entry->d_name[0]) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      names_.push_back(entry->d_name);
    }
    return ret;
  }

  const std::vector<std::string> &get_names() const { return names_; }

private:
  std::vector<std::string> names_;
};

static int append_one_data_check_list_run(
    ObSSDataCheckListedObjectSorter &sorter,
    const int64_t sequence,
    std::string &name)
{
  int ret = OB_SUCCESS;
  const std::string sequence_text = std::to_string(sequence);
  name.assign("listed_");
  name.append(20 - sequence_text.length(), '0');
  name.append(sequence_text);
  struct dirent entry;
  MEMSET(&entry, 0, sizeof(entry));
  if (OB_UNLIKELY(sequence < 0
      || name.length() >= sizeof(entry.d_name))) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(entry.d_name, name.data(), name.length());
    entry.d_name[name.length()] = '\0';
    if (OB_FAIL(sorter.func(&entry))) {
      STORAGE_LOG(WARN, "fail to append one listed object",
          K(ret), "name", name.c_str());
    } else if (OB_FAIL(sorter.flush_run_())) {
      STORAGE_LOG(WARN, "fail to flush one listed object run",
          K(ret), "name", name.c_str());
    }
  }
  return ret;
}

class FakeDataCheckMetaRefChecker final
    : public ObISSDataCheckMetaRefChecker
{
public:
  FakeDataCheckMetaRefChecker()
    : results_(),
      result_(OB_SUCCESS),
      call_count_(0),
      called_source_keys_()
  {}

  void set_results(
      const std::vector<bool> &results,
      const int result = OB_SUCCESS)
  {
    results_ = results;
    result_ = result;
    call_count_ = 0;
    called_source_keys_.clear();
  }

  void set_result(const int result) { result_ = result; }

  virtual int check_reference_exist(
      const ObSSDataCheckExpectedItem &item,
      bool &is_exist) const override
  {
    int ret = result_;
    called_source_keys_.push_back(build_source_key_(item));
    if (OB_SUCCESS == ret) {
      is_exist = call_count_ < results_.size()
          ? results_.at(call_count_)
          : false;
    }
    ++call_count_;
    return ret;
  }

  int64_t get_call_count() const { return call_count_; }
  int64_t get_unique_source_key_count() const
  {
    std::vector<std::string> keys = called_source_keys_;
    std::sort(keys.begin(), keys.end());
    return std::unique(keys.begin(), keys.end()) - keys.begin();
  }
  bool is_each_source_key_called_once() const
  {
    std::vector<std::string> keys = called_source_keys_;
    std::sort(keys.begin(), keys.end());
    return std::adjacent_find(keys.begin(), keys.end()) == keys.end();
  }

private:
  static std::string build_source_key_(
      const ObSSDataCheckExpectedItem &item)
  {
    return std::to_string(item.get_ls_id().id()) + "/"
        + std::to_string(item.get_tablet_id().id()) + "/"
        + std::to_string(
            item.get_reorganize_scn().get_val_for_inner_table_field()) + "/"
        + std::to_string(
            item.get_meta_scn().get_val_for_inner_table_field());
  }

private:
  std::vector<bool> results_;
  int result_;
  mutable int64_t call_count_;
  mutable std::vector<std::string> called_source_keys_;
};

class ScriptedSSDataCheckGenTask : public ObSSDataCheckGenTask
{
public:
  explicit ScriptedSSDataCheckGenTask(volatile bool &is_stopped)
    : ObSSDataCheckGenTask(is_stopped),
      is_stopped_(is_stopped),
      check_time_us_(0),
      exec_interval_us_(100),
      leader_check_ret_(OB_SUCCESS),
      is_leader_(true),
      stop_after_mode_(false),
      snapshot_ret_(OB_SUCCESS),
      snapshot_meta_scn_(share::SCN::min_scn()),
      mode_ret_(OB_SUCCESS),
      check_mode_(ObSSDataCheckMode::SINGLE),
      submit_ret_(OB_SUCCESS),
      probe_ret_(OB_SUCCESS),
      probe_exist_(true),
      leader_check_count_(0),
      snapshot_count_(0),
      mode_count_(0),
      submit_count_(0),
      probe_count_(0),
      submitted_tenant_id_(OB_INVALID_TENANT_ID),
      submitted_memory_limit_(0),
      submitted_file_buf_size_(0),
      submitted_io_expire_timestamp_(-1),
      submitted_snapshot_meta_scn_(),
      submitted_check_mode_(ObSSDataCheckMode::MAX)
  {}

  int64_t get_schedule_anchor() const
  { return last_period_commit_time_us_; }
  uint64_t get_run_count() const { return run_cnt_; }
  int64_t get_config_interval_us() const
  {
    int64_t interval_us = 0;
    ObSSDataCheckGenTask::get_gc_exec_interval_us_(interval_us);
    return interval_us;
  }

protected:
  virtual int64_t get_gc_dag_schedule_check_time_us_() const override
  { return check_time_us_; }

  virtual void get_gc_exec_interval_us_(
      int64_t &exec_interval_us) const override
  { exec_interval_us = exec_interval_us_; }

  virtual int check_sys_ls_strong_leader_(bool &is_leader) const override
  {
    int ret = leader_check_ret_;
    if (OB_SUCCESS == ret) {
      ++leader_check_count_;
      is_leader = is_leader_;
    }
    return ret;
  }

  virtual int get_shared_meta_snapshot_(
      share::SCN &snapshot_meta_scn) const override
  {
    ++snapshot_count_;
    snapshot_meta_scn = snapshot_meta_scn_;
    return snapshot_ret_;
  }

  virtual int get_data_check_mode_(
      ObSSDataCheckMode &check_mode) const override
  {
    ++mode_count_;
    check_mode = check_mode_;
    if (stop_after_mode_) {
      is_stopped_ = true;
    }
    return mode_ret_;
  }

  virtual int submit_data_check_dag_(
      const ObSSDataCheckDagParam &param) override
  {
    ++submit_count_;
    submitted_tenant_id_ = param.tenant_id_;
    submitted_memory_limit_ = param.memory_limit_;
    submitted_file_buf_size_ = param.file_buf_size_;
    submitted_io_expire_timestamp_ = param.io_expire_timestamp_;
    submitted_snapshot_meta_scn_ = param.snapshot_meta_scn_;
    submitted_check_mode_ = param.check_mode_;
    return submit_ret_;
  }

  virtual int check_data_check_dag_exist_(bool &exist) override
  {
    ++probe_count_;
    exist = probe_exist_;
    return probe_ret_;
  }

public:
  volatile bool &is_stopped_;
  int64_t check_time_us_;
  int64_t exec_interval_us_;
  int leader_check_ret_;
  bool is_leader_;
  bool stop_after_mode_;
  int snapshot_ret_;
  share::SCN snapshot_meta_scn_;
  int mode_ret_;
  ObSSDataCheckMode check_mode_;
  int submit_ret_;
  int probe_ret_;
  bool probe_exist_;
  mutable int64_t leader_check_count_;
  mutable int64_t snapshot_count_;
  mutable int64_t mode_count_;
  int64_t submit_count_;
  int64_t probe_count_;
  uint64_t submitted_tenant_id_;
  int64_t submitted_memory_limit_;
  int64_t submitted_file_buf_size_;
  int64_t submitted_io_expire_timestamp_;
  share::SCN submitted_snapshot_meta_scn_;
  ObSSDataCheckMode submitted_check_mode_;
};

static int init_data_check_gen_task_options(
    ObSSDataCheckGenTaskOptions &options,
    const int64_t io_timeout_us = 50)
{
  return options.init(
      TestSSDataCheck::MEM_LIMIT, TestSSDataCheck::BUF_SIZE, io_timeout_us);
}

static int split_data_check_path(
    const std::string &relative_path,
    std::string &prefix,
    std::string &name)
{
  int ret = OB_SUCCESS;
  const std::string::size_type slash_pos = relative_path.rfind('/');
  prefix.clear();
  name.clear();
  if (std::string::npos == slash_pos
      || slash_pos + 1 >= relative_path.length()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    prefix.assign(relative_path, 0, slash_pos + 1);
    name.assign(relative_path, slash_pos + 1, std::string::npos);
  }
  return ret;
}

static int build_list_check_expected_with_source_keys(
    ObSSDataCheckCtx &ctx,
    const std::vector<MacroBlockId> &macro_ids,
    const int64_t source_key_count,
    ObSSDataCheckSortedFileInfo &expected_file,
    int64_t &expected_count,
    std::vector<std::string> &sorted_paths)
{
  int ret = OB_SUCCESS;
  expected_file.reset();
  expected_count = 0;
  sorted_paths.clear();
  ObSSDataCheckExpectedSorter sorter;
  if (macro_ids.empty()
      || source_key_count <= 0
      || source_key_count > macro_ids.size()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(sorter.init(&ctx.get_expected_sort_pipeline()))) {
    STORAGE_LOG(WARN, "fail to initialize list check sorter", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < macro_ids.size(); ++i) {
    ObSSDataCheckExpectedItem item;
    std::string relative_path;
    const int64_t source_key_idx = i % source_key_count;
    if (OB_FAIL(init_expected_item_with_macro_id(
        macro_ids.at(i), item, 1,
        200001 + source_key_idx,
        100 + source_key_idx,
        200 + source_key_idx))) {
      STORAGE_LOG(WARN, "fail to initialize list check expected item", K(ret));
    } else if (OB_FAIL(build_data_check_relative_path(
        item.get_macro_id(), relative_path))) {
      STORAGE_LOG(WARN, "fail to build list check expected path", K(ret));
    } else if (OB_FAIL(sorter.add_item(item))) {
      STORAGE_LOG(WARN, "fail to add list check expected item", K(ret));
    } else {
      sorted_paths.push_back(relative_path);
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sorter.finish())) {
    STORAGE_LOG(WARN, "fail to finish list check sorter", K(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(ctx.finish_expected_collect())) {
    STORAGE_LOG(WARN, "fail to finish list check expected collect", K(ret));
  }
  if (OB_SUCC(ret)) {
    int64_t pass_count = 0;
    if (OB_FAIL(drive_expected_merge(
        ctx.get_expected_sort_pipeline(), false, pass_count))) {
      STORAGE_LOG(WARN, "fail to merge list check expected file", K(ret));
    } else if (OB_FAIL(ctx.get_expected_sort_pipeline().get_sorted_file(
        expected_file))) {
      STORAGE_LOG(WARN, "fail to get list check expected file", K(ret));
    } else if (OB_FAIL(ctx.get_expected_sort_pipeline().get_sorted_item_count(
        expected_count))) {
      STORAGE_LOG(WARN, "fail to get list check expected count", K(ret));
    } else {
      std::sort(sorted_paths.begin(), sorted_paths.end());
    }
  }
  return ret;
}

static int build_list_check_expected_by_macro_ids(
    ObSSDataCheckCtx &ctx,
    const std::vector<MacroBlockId> &macro_ids,
    ObSSDataCheckSortedFileInfo &expected_file,
    int64_t &expected_count,
    std::vector<std::string> &sorted_paths)
{
  return build_list_check_expected_with_source_keys(
      ctx, macro_ids, 1, expected_file, expected_count, sorted_paths);
}

static int build_list_check_expected(
    ObSSDataCheckCtx &ctx,
    const std::vector<int64_t> &macro_seqs,
    ObSSDataCheckSortedFileInfo &expected_file,
    int64_t &expected_count,
    std::vector<std::string> &sorted_paths)
{
  std::vector<MacroBlockId> macro_ids;
  for (int64_t i = 0; i < macro_seqs.size(); ++i) {
    macro_ids.push_back(make_data_check_id(macro_seqs.at(i)));
  }
  return build_list_check_expected_by_macro_ids(
      ctx, macro_ids, expected_file, expected_count, sorted_paths);
}

static int add_single_item_run(
    ObSSDataCheckCtx &ctx,
    const int64_t macro_seq)
{
  int ret = OB_SUCCESS;
  bool task_entered = false;
  ObSSDataCheckExpectedSorter sorter;
  ObSSDataCheckExpectedItem item;
  if (OB_FAIL(ctx.inc_active_task_count())) {
    STORAGE_LOG(WARN, "fail to enter expected producer task", K(ret));
  } else {
    task_entered = true;
    if (OB_FAIL(sorter.init(&ctx.get_expected_sort_pipeline()))) {
      STORAGE_LOG(WARN, "fail to initialize expected sorter", K(ret));
    } else if (OB_FAIL(init_expected_item(macro_seq, item))) {
      STORAGE_LOG(WARN, "fail to initialize expected item", K(ret));
    } else if (OB_FAIL(sorter.add_item(item))) {
      STORAGE_LOG(WARN, "fail to add expected item", K(ret));
    } else if (OB_FAIL(sorter.finish())) {
      STORAGE_LOG(WARN, "fail to finish expected sorter", K(ret));
    }
  }
  sorter.reset();
  if (task_entered) {
    ctx.dec_active_task_count(ret);
  }
  return ret;
}

// 用于执行生产动态任务图的最小调度宿主。
class TestDataCheckDag final : public ObSSGCDagBase
{
public:
  TestDataCheckDag()
    : ObSSGCDagBase(share::ObDagType::DAG_TYPE_MARK_SWEEP)
  {}
  virtual ~TestDataCheckDag() = default;

  virtual bool operator==(const share::ObIDag &other) const override
  { return this == &other; }
  virtual uint64_t hash() const override
  { return reinterpret_cast<uint64_t>(this); }
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override
  {
    UNUSED(allocator);
    out_param = nullptr;
    return OB_SUCCESS;
  }
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      buf[0] = '\0';
    }
    return ret;
  }
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
};

class TestThrottledSharedGCStopCheckTask final
    : public ObSharedGCBaseTask
{
public:
  TestThrottledSharedGCStopCheckTask()
    : ObSharedGCBaseTask(TASK_TYPE_SS_DATA_CHECK_FINALIZE)
  {}
  virtual ~TestThrottledSharedGCStopCheckTask() = default;

  int prime_stop_check() const
  { return check_if_stopped_or_failed(); }
  virtual int process() override
  { return OB_SUCCESS; }
};

static int execute_data_check_task_graph(
    share::ObIDag &dag,
    ObIArray<share::ObITask::ObITaskType> &execution_order)
{
  int ret = OB_SUCCESS;
  static const int64_t MAX_EXECUTED_TASK_COUNT = 1024;
  execution_order.reset();
  while (OB_SUCC(ret) && dag.get_task_list_count() > 0
      && execution_order.count() < MAX_EXECUTED_TASK_COUNT) {
    share::ObITask *task = nullptr;
    if (OB_FAIL(dag.get_next_ready_task(task))) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      task->set_status(share::ObITask::TASK_STATUS_RUNNING);
      if (OB_FAIL(execution_order.push_back(task->get_type()))) {
        STORAGE_LOG(WARN, "fail to record expected sort task", K(ret));
      } else if (OB_FAIL(task->generate_next_task())) {
        STORAGE_LOG(WARN, "fail to generate expected sort task", K(ret));
      } else if (OB_FAIL(task->do_work(false))) {
        STORAGE_LOG(WARN, "fail to execute expected sort task", K(ret));
      } else {
        task->set_status(share::ObITask::TASK_STATUS_FINISHED);
        if (OB_FAIL(dag.finish_task(task))) {
          STORAGE_LOG(WARN, "fail to finish expected sort task", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(dag.get_task_list_count() > 0)) {
    ret = OB_SIZE_OVERFLOW;
  }
  return ret;
}

static int run_finalize_task_graph(
    ObSSDataCheckCtx &ctx,
    ObIArray<share::ObITask::ObITaskType> &execution_order)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator dag_allocator("DCFinalizeDag");
  TestDataCheckDag dag;
  ObSSDataCheckFinalizeTask *finalize_task = nullptr;
  if (OB_FAIL(dag.basic_init(dag_allocator))) {
    STORAGE_LOG(WARN, "fail to initialize finalize test dag", K(ret));
  } else if (FALSE_IT(
      dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING))) {
  } else if (OB_FAIL(dag.alloc_task(finalize_task))) {
    STORAGE_LOG(WARN, "fail to allocate data check finalize task", K(ret));
  } else if (OB_FAIL(finalize_task->init(&ctx))) {
    STORAGE_LOG(WARN, "fail to initialize data check finalize task", K(ret));
  } else if (OB_FAIL(dag.add_task(*finalize_task))) {
    STORAGE_LOG(WARN, "fail to add data check finalize task", K(ret));
  } else if (OB_FAIL(execute_data_check_task_graph(dag, execution_order))) {
    STORAGE_LOG(WARN, "fail to execute finalize task graph", K(ret));
  }
  return ret;
}

static int run_expected_sort_task_graph(
    ObSSDataCheckCtx &ctx,
    ObIArray<share::ObITask::ObITaskType> &execution_order)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator dag_allocator("DCExpTaskDag");
  TestDataCheckDag dag;
  ObSSDataCheckExpectedMergeStartTask *merge_start_task = nullptr;
  ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task = nullptr;
  if (OB_FAIL(ctx.finish_expected_collect())) {
    STORAGE_LOG(WARN, "fail to finish expected collect before merge graph", K(ret));
  } else if (OB_FAIL(dag.basic_init(dag_allocator))) {
    STORAGE_LOG(WARN, "fail to initialize expected sort test dag", K(ret));
  } else if (FALSE_IT(
      dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING))) {
  } else if (OB_FAIL(dag.alloc_task(merge_start_task))) {
    STORAGE_LOG(WARN, "fail to allocate expected merge start task", K(ret));
  } else if (OB_FAIL(dag.alloc_task(merge_complete_task))) {
    STORAGE_LOG(WARN, "fail to allocate expected merge complete task", K(ret));
  } else if (OB_FAIL(merge_complete_task->init(&ctx))) {
    STORAGE_LOG(WARN, "fail to initialize expected merge complete task", K(ret));
  } else if (OB_FAIL(merge_start_task->init(&ctx, merge_complete_task))) {
    STORAGE_LOG(WARN, "fail to initialize expected merge start task", K(ret));
  } else if (OB_FAIL(merge_start_task->add_child(*merge_complete_task))) {
    STORAGE_LOG(WARN, "fail to wire expected merge static anchor", K(ret));
  } else if (OB_FAIL(dag.add_task(*merge_start_task))) {
    STORAGE_LOG(WARN, "fail to add expected merge start task", K(ret));
  } else if (OB_FAIL(dag.add_task(*merge_complete_task))) {
    STORAGE_LOG(WARN, "fail to add expected merge complete task", K(ret));
  } else if (OB_FAIL(execute_data_check_task_graph(dag, execution_order))) {
    STORAGE_LOG(WARN, "fail to execute expected sort task graph", K(ret));
  }
  return ret;
}

TEST(TestSSDataCheckMode, parse_and_format)
{
  ObSSDataCheckMode mode = ObSSDataCheckMode::MAX;
  EXPECT_TRUE(ObSSDataCheckModeUtil::is_valid(ObSSDataCheckMode::OFF));
  EXPECT_TRUE(ObSSDataCheckModeUtil::is_valid(ObSSDataCheckMode::SINGLE));
  EXPECT_TRUE(ObSSDataCheckModeUtil::is_valid(ObSSDataCheckMode::FULL));
  EXPECT_FALSE(ObSSDataCheckModeUtil::is_valid(ObSSDataCheckMode::MAX));
  EXPECT_FALSE(ObSSDataCheckModeUtil::is_enabled(ObSSDataCheckMode::OFF));
  EXPECT_TRUE(ObSSDataCheckModeUtil::is_enabled(ObSSDataCheckMode::SINGLE));
  EXPECT_TRUE(ObSSDataCheckModeUtil::is_enabled(ObSSDataCheckMode::FULL));

  ASSERT_EQ(OB_SUCCESS, ObSSDataCheckModeUtil::parse(
      ObString::make_string("off"), mode));
  EXPECT_EQ(ObSSDataCheckMode::OFF, mode);
  ASSERT_STREQ("OFF", ObSSDataCheckModeUtil::to_string(mode));

  ASSERT_EQ(OB_SUCCESS, ObSSDataCheckModeUtil::parse(
      ObString::make_string("SINGLE"), mode));
  EXPECT_EQ(ObSSDataCheckMode::SINGLE, mode);
  ASSERT_STREQ("SINGLE", ObSSDataCheckModeUtil::to_string(mode));

  ASSERT_EQ(OB_SUCCESS, ObSSDataCheckModeUtil::parse(
      ObString::make_string("full"), mode));
  EXPECT_EQ(ObSSDataCheckMode::FULL, mode);
  ASSERT_STREQ("FULL", ObSSDataCheckModeUtil::to_string(mode));

  EXPECT_EQ(OB_INVALID_CONFIG, ObSSDataCheckModeUtil::parse(
      ObString::make_string("INVALID"), mode));
  EXPECT_EQ(ObSSDataCheckMode::MAX, mode);
  ASSERT_STREQ("INVALID", ObSSDataCheckModeUtil::to_string(mode));
}

TEST_F(TestSSDataCheck, data_check_gen_task_init_and_destroy)
{
  ObSSDataCheckGenTaskOptions options;
  EXPECT_FALSE(options.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, options.init(
      0, BUF_SIZE, 50));
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(options, 50));
  EXPECT_TRUE(options.is_valid());
  EXPECT_EQ(OB_INIT_TWICE,
      init_data_check_gen_task_options(options, 50));

  volatile bool is_stopped = false;
  ScriptedSSDataCheckGenTask task(is_stopped);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      task.init(OB_INVALID_TENANT_ID, options));
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID, options));
  EXPECT_EQ(OB_INIT_TWICE, task.init(OB_SYS_TENANT_ID, options));
  task.destroy();
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID, options));

  volatile bool default_task_stopped = false;
  ScriptedSSDataCheckGenTask default_task(default_task_stopped);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      default_task.init(OB_INVALID_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, default_task.init(OB_SYS_TENANT_ID));
  default_task.check_time_us_ = 1LL * 60LL * 60LL * 1000LL * 1000LL;
  default_task.runTimerTask();
  EXPECT_EQ(1, default_task.submit_count_);
  EXPECT_EQ(16L << 20, default_task.submitted_memory_limit_);
  EXPECT_EQ(32L << 10, default_task.submitted_file_buf_size_);
  EXPECT_EQ(0, default_task.submitted_io_expire_timestamp_);
  EXPECT_EQ(OB_INIT_TWICE, default_task.init(OB_SYS_TENANT_ID));
  default_task.destroy();
  ASSERT_EQ(OB_SUCCESS, default_task.init(OB_SYS_TENANT_ID));

  ObSSDataCheckMode default_check_mode = ObSSDataCheckMode::MAX;
  ASSERT_EQ(OB_SUCCESS, default_task.ObSSDataCheckGenTask::get_data_check_mode_(
      default_check_mode));
  EXPECT_EQ(ObSSDataCheckMode::SINGLE, default_check_mode);
  EXPECT_EQ(7LL * 24LL * 60LL * 60LL * 1000LL * 1000LL,
      default_task.get_config_interval_us());
}

TEST_F(TestSSDataCheck, data_check_gen_task_requires_sys_ls_leader)
{
  ObSSDataCheckGenTaskOptions options;
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(options, 50));

  volatile bool follower_stopped = false;
  ScriptedSSDataCheckGenTask follower_task(follower_stopped);
  follower_task.is_leader_ = false;
  follower_task.check_time_us_ = 200;
  ASSERT_EQ(OB_SUCCESS,
      follower_task.init(OB_SYS_TENANT_ID, options));
  follower_task.runTimerTask();
  EXPECT_EQ(1, follower_task.leader_check_count_);
  EXPECT_EQ(0, follower_task.snapshot_count_);
  EXPECT_EQ(0, follower_task.mode_count_);
  EXPECT_EQ(0, follower_task.submit_count_);
  EXPECT_EQ(0, follower_task.get_run_count());
  EXPECT_EQ(0, follower_task.get_schedule_anchor());

  volatile bool leader_stopped = false;
  ScriptedSSDataCheckGenTask leader_task(leader_stopped);
  leader_task.is_leader_ = true;
  leader_task.check_time_us_ = 200;
  ASSERT_EQ(OB_SUCCESS,
      leader_task.init(OB_SYS_TENANT_ID, options));
  leader_task.runTimerTask();
  EXPECT_EQ(1, leader_task.leader_check_count_);
  EXPECT_EQ(1, leader_task.snapshot_count_);
  EXPECT_EQ(1, leader_task.mode_count_);
  EXPECT_EQ(1, leader_task.submit_count_);
  EXPECT_EQ(1, leader_task.get_run_count());
  EXPECT_EQ(200, leader_task.get_schedule_anchor());
}

TEST_F(TestSSDataCheck, data_check_gen_task_submits_and_waits_for_dag)
{
  ObSSDataCheckGenTaskOptions options;
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(options, 50));
  volatile bool is_stopped = false;
  ScriptedSSDataCheckGenTask task(is_stopped);
  task.check_time_us_ = 200;
  task.check_mode_ = ObSSDataCheckMode::FULL;
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID, options));

  task.runTimerTask();
  EXPECT_EQ(1, task.leader_check_count_);
  EXPECT_EQ(1, task.snapshot_count_);
  EXPECT_EQ(1, task.mode_count_);
  EXPECT_EQ(1, task.submit_count_);
  EXPECT_EQ(1, task.get_run_count());
  EXPECT_EQ(200, task.get_schedule_anchor());
  EXPECT_EQ(OB_SYS_TENANT_ID, task.submitted_tenant_id_);
  EXPECT_TRUE(MEM_LIMIT == task.submitted_memory_limit_);
  EXPECT_TRUE(BUF_SIZE == task.submitted_file_buf_size_);
  EXPECT_EQ(250, task.submitted_io_expire_timestamp_);
  EXPECT_EQ(task.snapshot_meta_scn_, task.submitted_snapshot_meta_scn_);
  EXPECT_EQ(ObSSDataCheckMode::FULL, task.submitted_check_mode_);

  task.check_mode_ = ObSSDataCheckMode::SINGLE;
  task.runTimerTask();
  EXPECT_EQ(1, task.probe_count_);
  EXPECT_EQ(1, task.leader_check_count_);
  EXPECT_EQ(1, task.submit_count_);
  EXPECT_EQ(1, task.mode_count_);
  EXPECT_EQ(ObSSDataCheckMode::FULL, task.submitted_check_mode_);

  task.probe_exist_ = false;
  task.runTimerTask();
  EXPECT_EQ(2, task.probe_count_);
  EXPECT_EQ(1, task.leader_check_count_);
  EXPECT_EQ(1, task.submit_count_);
  EXPECT_EQ(1, task.mode_count_);
  EXPECT_EQ(ObSSDataCheckMode::FULL, task.submitted_check_mode_);

  task.check_time_us_ = 250;
  task.runTimerTask();
  EXPECT_EQ(2, task.leader_check_count_);
  EXPECT_EQ(1, task.submit_count_);
  EXPECT_EQ(200, task.get_schedule_anchor());
  EXPECT_EQ(1, task.mode_count_);

  task.check_time_us_ = 300;
  task.runTimerTask();
  EXPECT_EQ(3, task.leader_check_count_);
  EXPECT_EQ(2, task.submit_count_);
  EXPECT_EQ(2, task.get_run_count());
  EXPECT_EQ(2, task.snapshot_count_);
  EXPECT_EQ(2, task.mode_count_);
  EXPECT_EQ(300, task.get_schedule_anchor());
  EXPECT_EQ(350, task.submitted_io_expire_timestamp_);
  EXPECT_EQ(ObSSDataCheckMode::SINGLE, task.submitted_check_mode_);
}

TEST_F(TestSSDataCheck,
    data_check_gen_task_off_skips_snapshot_and_submits_after_enable)
{
  ObSSDataCheckGenTaskOptions options;
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(options, 50));
  volatile bool is_stopped = false;
  ScriptedSSDataCheckGenTask task(is_stopped);
  task.check_time_us_ = 200;
  task.check_mode_ = ObSSDataCheckMode::OFF;
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID, options));

  task.runTimerTask();
  EXPECT_EQ(1, task.leader_check_count_);
  EXPECT_EQ(1, task.mode_count_);
  EXPECT_EQ(0, task.snapshot_count_);
  EXPECT_EQ(0, task.submit_count_);
  EXPECT_EQ(0, task.probe_count_);
  EXPECT_EQ(0, task.get_run_count());
  EXPECT_EQ(0, task.get_schedule_anchor());

  task.check_mode_ = ObSSDataCheckMode::SINGLE;
  task.runTimerTask();
  EXPECT_EQ(2, task.leader_check_count_);
  EXPECT_EQ(2, task.mode_count_);
  EXPECT_EQ(1, task.snapshot_count_);
  EXPECT_EQ(1, task.submit_count_);
  EXPECT_EQ(1, task.get_run_count());
  EXPECT_EQ(200, task.get_schedule_anchor());
  EXPECT_EQ(ObSSDataCheckMode::SINGLE, task.submitted_check_mode_);
}

TEST_F(TestSSDataCheck, data_check_gen_task_off_preserves_due_retry)
{
  const int64_t hour_us = 60LL * 60LL * 1000LL * 1000LL;
  const int64_t normal_interval_us = 10 * hour_us;
  ObSSDataCheckGenTaskOptions options;
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(options, 50));
  volatile bool is_stopped = false;
  ScriptedSSDataCheckGenTask task(is_stopped);
  task.exec_interval_us_ = normal_interval_us;
  task.check_time_us_ = normal_interval_us;
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID, options));
  task.runTimerTask();
  ASSERT_EQ(1, task.submit_count_);

  const int64_t finish_ts = normal_interval_us + 5;
  const int64_t retry_ts = finish_ts + hour_us;
  task.report_data_check_dag_result(true, finish_ts);
  task.probe_exist_ = false;
  task.check_time_us_ = finish_ts;
  task.runTimerTask();
  ASSERT_EQ(1, task.submit_count_);

  task.check_mode_ = ObSSDataCheckMode::OFF;
  task.check_time_us_ = retry_ts;
  task.runTimerTask();
  EXPECT_EQ(1, task.submit_count_);
  EXPECT_EQ(1, task.snapshot_count_);
  EXPECT_EQ(normal_interval_us, task.get_schedule_anchor());

  task.check_mode_ = ObSSDataCheckMode::SINGLE;
  task.runTimerTask();
  EXPECT_EQ(2, task.submit_count_);
  EXPECT_EQ(2, task.snapshot_count_);
  EXPECT_EQ(retry_ts, task.get_schedule_anchor());
}

TEST_F(TestSSDataCheck, data_check_gen_task_handles_eagain_and_stop_race)
{
  ObSSDataCheckGenTaskOptions options;
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(options, 50));

  volatile bool eagain_stopped = false;
  ScriptedSSDataCheckGenTask eagain_task(eagain_stopped);
  eagain_task.check_time_us_ = 200;
  eagain_task.submit_ret_ = OB_EAGAIN;
  eagain_task.probe_exist_ = true;
  ASSERT_EQ(OB_SUCCESS,
      eagain_task.init(OB_SYS_TENANT_ID, options));
  eagain_task.runTimerTask();
  EXPECT_EQ(1, eagain_task.submit_count_);
  EXPECT_EQ(1, eagain_task.probe_count_);
  EXPECT_EQ(200, eagain_task.get_schedule_anchor());
  eagain_task.runTimerTask();
  EXPECT_EQ(1, eagain_task.submit_count_);
  EXPECT_EQ(2, eagain_task.probe_count_);

  volatile bool race_stopped = false;
  ScriptedSSDataCheckGenTask race_task(race_stopped);
  race_task.check_time_us_ = 200;
  race_task.stop_after_mode_ = true;
  ASSERT_EQ(OB_SUCCESS, race_task.init(OB_SYS_TENANT_ID, options));
  race_task.runTimerTask();
  EXPECT_TRUE(race_stopped);
  EXPECT_EQ(1, race_task.leader_check_count_);
  EXPECT_EQ(0, race_task.snapshot_count_);
  EXPECT_EQ(1, race_task.mode_count_);
  EXPECT_EQ(0, race_task.submit_count_);
  EXPECT_EQ(0, race_task.get_run_count());
  EXPECT_EQ(0, race_task.get_schedule_anchor());
}

TEST_F(TestSSDataCheck, data_check_gen_task_runtime_failure_bounded_backoff)
{
  const int64_t hour_us = 60LL * 60LL * 1000LL * 1000LL;
  const int64_t normal_interval_us = 10 * hour_us;
  ObSSDataCheckGenTaskOptions options;
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(options, 50));

  volatile bool is_stopped = false;
  ScriptedSSDataCheckGenTask task(is_stopped);
  task.exec_interval_us_ = normal_interval_us;
  task.check_time_us_ = normal_interval_us;
  ASSERT_EQ(OB_SUCCESS, task.init(OB_SYS_TENANT_ID, options));
  task.runTimerTask();
  ASSERT_EQ(1, task.submit_count_);

  int64_t finish_ts = normal_interval_us + 5;
  task.report_data_check_dag_result(true, finish_ts);
  task.probe_exist_ = false;
  task.check_time_us_ = finish_ts;
  task.runTimerTask();
  ASSERT_EQ(1, task.submit_count_);

  int64_t retry_ts = finish_ts + hour_us;
  task.check_time_us_ = retry_ts - 1;
  task.runTimerTask();
  ASSERT_EQ(1, task.submit_count_);
  task.check_time_us_ = retry_ts;
  task.runTimerTask();
  ASSERT_EQ(2, task.submit_count_);

  finish_ts = retry_ts + 5;
  task.report_data_check_dag_result(true, finish_ts);
  task.check_time_us_ = finish_ts;
  task.runTimerTask();
  retry_ts = finish_ts + 2 * hour_us;
  task.check_time_us_ = retry_ts - 1;
  task.runTimerTask();
  ASSERT_EQ(2, task.submit_count_);
  task.check_time_us_ = retry_ts;
  task.runTimerTask();
  ASSERT_EQ(3, task.submit_count_);

  finish_ts = retry_ts + 5;
  task.report_data_check_dag_result(true, finish_ts);
  task.check_time_us_ = finish_ts;
  task.runTimerTask();
  retry_ts = finish_ts + 4 * hour_us;
  task.check_time_us_ = retry_ts - 1;
  task.runTimerTask();
  ASSERT_EQ(3, task.submit_count_);
  task.check_time_us_ = retry_ts;
  task.runTimerTask();
  ASSERT_EQ(4, task.submit_count_);

  // 三次加速重试后，连续失败回到正常租户周期。
  finish_ts = retry_ts + 5;
  task.report_data_check_dag_result(true, finish_ts);
  task.check_time_us_ = finish_ts;
  task.runTimerTask();
  retry_ts = finish_ts + normal_interval_us;
  task.check_time_us_ = retry_ts - 1;
  task.runTimerTask();
  ASSERT_EQ(4, task.submit_count_);
  task.check_time_us_ = retry_ts;
  task.runTimerTask();
  ASSERT_EQ(5, task.submit_count_);

  // 成功结果清零连续失败计数，后续失败重新从一小时开始退避。
  finish_ts = retry_ts + 5;
  task.report_data_check_dag_result(false, finish_ts);
  task.check_time_us_ = finish_ts;
  task.runTimerTask();
  const int64_t normal_schedule_ts = retry_ts + normal_interval_us;
  task.check_time_us_ = normal_schedule_ts - 1;
  task.runTimerTask();
  ASSERT_EQ(5, task.submit_count_);
  task.check_time_us_ = normal_schedule_ts;
  task.runTimerTask();
  ASSERT_EQ(6, task.submit_count_);

  finish_ts = normal_schedule_ts + 5;
  task.report_data_check_dag_result(true, finish_ts);
  task.check_time_us_ = finish_ts;
  task.runTimerTask();
  retry_ts = finish_ts + hour_us;
  task.check_time_us_ = retry_ts - 1;
  task.runTimerTask();
  ASSERT_EQ(6, task.submit_count_);
  task.check_time_us_ = retry_ts;
  task.runTimerTask();
  ASSERT_EQ(7, task.submit_count_);
}

TEST_F(TestSSDataCheck, data_check_gen_task_io_timeout_boundary)
{
  volatile bool is_stopped = false;
  ObSSDataCheckGenTaskOptions no_timeout_options;
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(no_timeout_options, 0));
  ScriptedSSDataCheckGenTask no_timeout_task(is_stopped);
  no_timeout_task.check_time_us_ = 200;
  ASSERT_EQ(OB_SUCCESS,
      no_timeout_task.init(OB_SYS_TENANT_ID, no_timeout_options));
  no_timeout_task.runTimerTask();
  EXPECT_EQ(0, no_timeout_task.submitted_io_expire_timestamp_);

  ObSSDataCheckGenTaskOptions overflow_options;
  ASSERT_EQ(OB_SUCCESS,
      init_data_check_gen_task_options(overflow_options, 50));
  ScriptedSSDataCheckGenTask overflow_task(is_stopped);
  overflow_task.check_time_us_ = INT64_MAX - 25;
  ASSERT_EQ(OB_SUCCESS,
      overflow_task.init(OB_SYS_TENANT_ID, overflow_options));
  overflow_task.runTimerTask();
  EXPECT_EQ(INT64_MAX, overflow_task.submitted_io_expire_timestamp_);
}

TEST(TestSSDataCheckDag, data_check_dag_registration_and_identity)
{
  ASSERT_STREQ("SS_DATA_CHECK",
      ObIDag::get_dag_type_str(ObDagType::DAG_TYPE_SS_DATA_CHECK));
  ASSERT_EQ(ObDagPrio::DAG_PRIO_SS_DATA_CHECK,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_DATA_CHECK].init_dag_prio_);
  ASSERT_EQ(3,
      OB_DAG_PRIOS[ObDagPrio::DAG_PRIO_SS_DATA_CHECK].score_);
  ASSERT_EQ(ObSysTaskType::SS_DATA_CHECK_TASK,
      OB_DAG_TYPES[ObDagType::DAG_TYPE_SS_DATA_CHECK].sys_task_type_);
  ASSERT_STREQ("SS_DATA_CHECK_TASK",
      sys_task_type_to_str(ObSysTaskType::SS_DATA_CHECK_TASK));

  ObSSDataCheckDagParam param;
  EXPECT_FALSE(param.is_valid());
  EXPECT_EQ(OB_INVALID_ARGUMENT, param.init(
      OB_INVALID_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::SINGLE,
      TestSSDataCheck::MEM_LIMIT, TestSSDataCheck::BUF_SIZE, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, param.init(
      OB_SYS_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::MAX,
      TestSSDataCheck::MEM_LIMIT, TestSSDataCheck::BUF_SIZE, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, param.init(
      OB_SYS_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::OFF,
      TestSSDataCheck::MEM_LIMIT, TestSSDataCheck::BUF_SIZE, 0));
  EXPECT_FALSE(param.is_valid());
  ASSERT_EQ(OB_SUCCESS, param.init(
      OB_SYS_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::SINGLE,
      TestSSDataCheck::MEM_LIMIT, TestSSDataCheck::BUF_SIZE, 0));
  EXPECT_TRUE(param.is_valid());
  EXPECT_EQ(ObSSDataCheckMode::SINGLE, param.check_mode_);
  EXPECT_EQ(OB_INIT_TWICE, param.init(
      OB_SYS_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::SINGLE,
      TestSSDataCheck::MEM_LIMIT, TestSSDataCheck::BUF_SIZE, 0));

  ObSSDataCheckDag first;
  ObSSDataCheckDag same_tenant;
  ObSSDataCheckDag other_tenant;
  first.set_tenant_id(OB_SYS_TENANT_ID);
  same_tenant.set_tenant_id(OB_SYS_TENANT_ID);
  other_tenant.set_tenant_id(1001);
  EXPECT_TRUE(first == same_tenant);
  EXPECT_EQ(first.hash(), same_tenant.hash());
  EXPECT_FALSE(first == other_tenant);
}

TEST_F(TestSSDataCheck, expected_collect_reentry_uses_context_state)
{
  {
    ObSSDataCheckSharedMetaExpectedSource expected_source;
    ObSSDataCheckCtx ctx;
    ASSERT_EQ(OB_SUCCESS,
        ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
            ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
    ASSERT_EQ(OB_SUCCESS, ctx.finish_expected_collect());
    {
      ObArenaAllocator dag_allocator("DCCollReentry");
      TestDataCheckDag dag;
      ObSSDataCheckExpectedCollectTask *collect_task = nullptr;
      ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
      dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING);
      ASSERT_EQ(OB_SUCCESS, dag.alloc_task(collect_task));
      ASSERT_EQ(OB_SUCCESS, collect_task->init(&ctx, &expected_source));
      ASSERT_EQ(OB_SUCCESS, dag.add_task(*collect_task));
      collect_task->set_status(share::ObITask::TASK_STATUS_RUNNING);
      ASSERT_EQ(OB_SUCCESS, collect_task->do_work(false));
      EXPECT_TRUE(ctx.is_expected_collect_complete());
      ASSERT_EQ(OB_SUCCESS, collect_task->do_work(false));
      EXPECT_EQ(OB_SUCCESS, ctx.get_first_error());
      EXPECT_EQ(0, ctx.get_active_task_count());
      collect_task->set_status(share::ObITask::TASK_STATUS_FINISHED);
      share::ObITask *task = collect_task;
      ASSERT_EQ(OB_SUCCESS, dag.finish_task(task));
    }
    ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  }

  {
    ObSSDataCheckSharedMetaExpectedSource expected_source;
    ObSSDataCheckCtx ctx;
    ASSERT_EQ(OB_SUCCESS,
        ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
            ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
    ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
    ctx.dec_active_task_count(OB_TIMEOUT);
    {
      ObArenaAllocator dag_allocator("DCCollFailRetry");
      TestDataCheckDag dag;
      ObSSDataCheckExpectedCollectTask *collect_task = nullptr;
      ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
      dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING);
      ASSERT_EQ(OB_SUCCESS, dag.alloc_task(collect_task));
      ASSERT_EQ(OB_SUCCESS, collect_task->init(&ctx, &expected_source));
      ASSERT_EQ(OB_SUCCESS, dag.add_task(*collect_task));
      collect_task->set_status(share::ObITask::TASK_STATUS_RUNNING);
      ASSERT_EQ(OB_TIMEOUT, collect_task->do_work(false));
      EXPECT_FALSE(ctx.is_expected_collect_complete());
      EXPECT_EQ(OB_TIMEOUT, ctx.get_first_error());
      ASSERT_EQ(OB_TIMEOUT, collect_task->do_work(false));
      EXPECT_EQ(OB_TIMEOUT, ctx.get_first_error());
      EXPECT_EQ(0, ctx.get_active_task_count());
      collect_task->set_status(share::ObITask::TASK_STATUS_FINISHED);
      share::ObITask *task = collect_task;
      ASSERT_EQ(OB_SUCCESS, dag.finish_task(task));
    }
    ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  }
}

TEST_F(TestSSDataCheck,
    expected_collect_check_stop_observes_context_and_deadline)
{
  {
    ObSSDataCheckSharedMetaExpectedSource expected_source;
    ObSSDataCheckCtx ctx;
    ObArenaAllocator dag_allocator("DCCollStop");
    TestDataCheckDag dag;
    ObSSDataCheckExpectedCollectTask collect_task;
    ASSERT_EQ(OB_SUCCESS,
        ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
            ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
    dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING);
    collect_task.set_dag(dag);
    ASSERT_EQ(OB_SUCCESS, collect_task.init(&ctx, &expected_source));
    EXPECT_EQ(OB_SUCCESS, collect_task.check_stop());

    ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
    ctx.dec_active_task_count(OB_TIMEOUT);
    EXPECT_EQ(OB_TIMEOUT, collect_task.check_stop());

    dag.simply_set_stop();
    EXPECT_EQ(OB_CANCELED, collect_task.check_stop());
    ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  }

  {
    ObSSDataCheckSharedMetaExpectedSource expected_source;
    ObSSDataCheckCtx ctx;
    ObArenaAllocator dag_allocator("DCCollExpire");
    TestDataCheckDag dag;
    ObSSDataCheckExpectedCollectTask collect_task;
    const int64_t expired_timestamp = ObTimeUtility::current_time_us() - 1;
    ASSERT_EQ(OB_SUCCESS,
        ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
            ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE,
            expired_timestamp));
    ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
    dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING);
    collect_task.set_dag(dag);
    ASSERT_EQ(OB_SUCCESS, collect_task.init(&ctx, &expected_source));
    EXPECT_EQ(OB_TIMEOUT, collect_task.check_stop());
    ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  }
}

TEST_F(TestSSDataCheck, expected_collect_generate_worker_pool_with_barrier)
{
  ObSSDataCheckCtx ctx;
  ObSSDataCheckSharedMetaExpectedSource expected_source;
  ObArenaAllocator dag_allocator("DCWorkerPool");
  TestDataCheckDag dag;
  ObSSDataCheckExpectedCollectTask *collect_task = nullptr;
  ObSSDataCheckExpectedMergeStartTask *merge_start_task = nullptr;
  ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING);
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(collect_task));
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(merge_start_task));
  ASSERT_EQ(OB_SUCCESS, dag.alloc_task(merge_complete_task));
  ASSERT_EQ(OB_SUCCESS, collect_task->init(&ctx, &expected_source));
  EXPECT_EQ(OB_INIT_TWICE, collect_task->init(&ctx, &expected_source));
  ASSERT_EQ(OB_SUCCESS, merge_complete_task->init(&ctx));
  ASSERT_EQ(OB_SUCCESS,
      merge_start_task->init(&ctx, merge_complete_task));
  ASSERT_EQ(OB_SUCCESS,
      merge_start_task->add_child(*merge_complete_task));
  ASSERT_EQ(OB_SUCCESS, collect_task->add_child(*merge_start_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*collect_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*merge_start_task));
  ASSERT_EQ(OB_SUCCESS, dag.add_task(*merge_complete_task));
  int64_t worker_limit = 0;
  ASSERT_NE(nullptr, MTL(share::ObTenantDagScheduler *));
  ASSERT_EQ(OB_SUCCESS, MTL(share::ObTenantDagScheduler *)->get_limit(
      share::ObDagPrio::DAG_PRIO_SS_DATA_CHECK, worker_limit));
  ASSERT_GT(worker_limit, 0);

  for (int64_t worker_idx = 0;
       worker_idx < worker_limit;
       ++worker_idx) {
    share::ObITask *task = nullptr;
    ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(task));
    ASSERT_NE(nullptr, task);
    ASSERT_EQ(share::ObITask::TASK_TYPE_SS_DATA_CHECK_EXPECTED_COLLECT,
        task->get_type());
    task->set_status(share::ObITask::TASK_STATUS_RUNNING);
    ASSERT_EQ(OB_SUCCESS, task->generate_next_task());
    const int64_t task_count_after_generate = dag.get_task_list_count();
    ASSERT_EQ(OB_SUCCESS, task->generate_next_task());
    EXPECT_EQ(task_count_after_generate, dag.get_task_list_count());
    task->set_status(share::ObITask::TASK_STATUS_FINISHED);
    ASSERT_EQ(OB_SUCCESS, dag.finish_task(task));
    EXPECT_EQ(share::ObITask::TASK_STATUS_WAITING,
        merge_start_task->get_status());
  }

  share::ObITask *ready_task = nullptr;
  ASSERT_EQ(OB_SUCCESS, dag.get_next_ready_task(ready_task));
  ASSERT_EQ(merge_start_task, ready_task);
}

TEST_F(TestSSDataCheck, finalize_publishes_not_health_for_referenced_missing)
{
  ObSSDataCheckCtx ctx;
  ObSSDataCheckExpectedItem missing_item;
  ASSERT_EQ(OB_SUCCESS, init_expected_item(1, missing_item));
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, ctx.record_incomplete_expected());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_expected_collect());
  ASSERT_EQ(OB_SUCCESS, ctx.record_referenced_missing(missing_item));
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());

  ObSEArray<share::ObITask::ObITaskType, 8> execution_order;
  ASSERT_EQ(OB_SUCCESS, run_finalize_task_graph(ctx, execution_order));
  ASSERT_EQ(1, execution_order.count());
  EXPECT_EQ(share::ObITask::TASK_TYPE_SS_DATA_CHECK_FINALIZE,
      execution_order.at(0));
  EXPECT_EQ(OB_SUCCESS, ctx.get_first_error());
  EXPECT_EQ(0, ctx.get_active_task_count());
  EXPECT_TRUE(ctx.is_list_check_complete());
  EXPECT_TRUE(ctx.has_published_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
  ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::NOT_HEALTH, status);
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck,
    incomplete_expected_without_negative_witness_does_not_publish_health)
{
  ObSSDataCheckCtx ctx;
  ObSSDataCheckStatus status = ObSSDataCheckStatus::NOT_HEALTH;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, ctx.record_incomplete_expected());
  ASSERT_EQ(OB_SUCCESS, ctx.record_incomplete_expected());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_expected_collect());
  EXPECT_EQ(OB_STATE_NOT_MATCH, ctx.record_incomplete_expected());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, ctx.publish_status());
  EXPECT_EQ(OB_OBJECT_NOT_EXIST, ctx.get_first_error());
  EXPECT_FALSE(ctx.has_published_status());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::NOT_HEALTH, status);
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, finalize_does_not_publish_after_task_failure)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ctx.dec_active_task_count(OB_TIMEOUT);

  ObSEArray<share::ObITask::ObITaskType, 8> execution_order;
  EXPECT_EQ(OB_TIMEOUT, run_finalize_task_graph(ctx, execution_order));
  ASSERT_EQ(1, execution_order.count());
  EXPECT_EQ(share::ObITask::TASK_TYPE_SS_DATA_CHECK_FINALIZE,
      execution_order.at(0));
  EXPECT_EQ(OB_TIMEOUT, ctx.get_first_error());
  EXPECT_EQ(0, ctx.get_active_task_count());
  EXPECT_FALSE(ctx.is_list_check_complete());
  EXPECT_FALSE(ctx.has_published_status());
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, finalize_checks_canceled_dag_without_throttle)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, ctx.finish_expected_collect());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());

  ObTenantBase *tenant_ctx = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, tenant_ctx);
  int worker_ret = OB_SUCCESS;
  int probe_ret = OB_SUCCESS;
  int stop_observed_ret = OB_SUCCESS;
  int finalize_ret = OB_SUCCESS;
  std::thread worker([&]() {
    int ret = OB_SUCCESS;
    ObTenantEnv::set_tenant(tenant_ctx);
    ObArenaAllocator dag_allocator("DCCancelDag");
    TestDataCheckDag dag;
    TestThrottledSharedGCStopCheckTask probe_task;
    ObSSDataCheckFinalizeTask finalize_task;
    if (OB_FAIL(dag.basic_init(dag_allocator))) {
      STORAGE_LOG(WARN, "fail to initialize canceled finalize test dag",
          K(ret));
    } else {
      dag.set_dag_status(share::ObIDag::DAG_STATUS_NODE_RUNNING);
      probe_task.set_dag(dag);
      finalize_task.set_dag(dag);
      if (OB_FAIL(finalize_task.init(&ctx))) {
        STORAGE_LOG(WARN, "fail to initialize canceled finalize task",
            K(ret));
      } else if (OB_SUCCESS != (probe_ret = probe_task.prime_stop_check())) {
        ret = probe_ret;
        STORAGE_LOG(WARN, "fail to prime shared GC stop check throttle",
            K(ret));
      } else {
        dag.simply_set_stop();
        stop_observed_ret = ObSSGCDagBase::check_if_stopped_or_failed(dag);
        finalize_ret = finalize_task.process();
      }
    }
    worker_ret = ret;
  });
  worker.join();

  ASSERT_EQ(OB_SUCCESS, worker_ret);
  EXPECT_EQ(OB_SUCCESS, probe_ret);
  EXPECT_EQ(OB_CANCELED, stop_observed_ret);
  EXPECT_EQ(OB_CANCELED, finalize_ret);
  EXPECT_EQ(OB_SUCCESS, ctx.get_first_error());
  EXPECT_EQ(0, ctx.get_active_task_count());
  EXPECT_TRUE(ctx.is_expected_collect_complete());
  EXPECT_TRUE(ctx.is_list_check_complete());
  EXPECT_FALSE(ctx.has_published_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ctx.get_status(status));
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, expected_sorter_discards_unfinished_result_on_reset)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  ObSSDataCheckExpectedSorter sorter;
  ObSSDataCheckExpectedItem item;
  ASSERT_EQ(OB_SUCCESS, sorter.init(&ctx.get_expected_sort_pipeline()));
  ASSERT_EQ(OB_SUCCESS, init_expected_item(1, item));
  ASSERT_EQ(OB_SUCCESS, sorter.add_item(item));
  sorter.reset();

  ASSERT_EQ(OB_SUCCESS, ctx.finish_expected_collect());
  int64_t pass_count = 0;
  ASSERT_EQ(OB_SUCCESS,
      drive_expected_merge(ctx.get_expected_sort_pipeline(), false, pass_count));
  int64_t expected_count = -1;
  ASSERT_EQ(OB_SUCCESS,
      ctx.get_expected_sort_pipeline().get_sorted_item_count(expected_count));
  EXPECT_EQ(0, expected_count);
  EXPECT_EQ(OB_SUCCESS, ctx.get_first_error());
  EXPECT_EQ(0, ctx.get_active_task_count());
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST(TestSSDataCheckExpectedItem, relative_path_order_copy_and_compare_error)
{
  ObSSDataCheckExpectedItem item_10;
  ObSSDataCheckExpectedItem item_2;
  MacroBlockId in_table_macro_id;
  in_table_macro_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_META));
  in_table_macro_id.set_location(ObMacroBlockIdLocation::TABLE);
  in_table_macro_id.set_second_id(3);
  ASSERT_TRUE(in_table_macro_id.is_valid());
  ASSERT_TRUE(in_table_macro_id.is_in_table());
  ObSSDataCheckExpectedItem in_table_item;
  EXPECT_EQ(OB_INVALID_ARGUMENT, in_table_item.init(
      in_table_macro_id, share::ObLSID(1), ObTabletID(200001),
      share::SCN::min_scn(), share::SCN::min_scn()));
  EXPECT_FALSE(in_table_item.is_valid());

  const MacroBlockId valid_macro_id = make_data_check_id(1);
  const share::SCN valid_scn = share::SCN::min_scn();
  const share::SCN invalid_scn = share::SCN::invalid_scn();
  ObSSDataCheckExpectedItem invalid_source_item;
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_source_item.init(
      valid_macro_id, share::ObLSID(), ObTabletID(200001),
      valid_scn, valid_scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_source_item.init(
      valid_macro_id, share::ObLSID(1), ObTabletID(),
      valid_scn, valid_scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_source_item.init(
      valid_macro_id, share::ObLSID(1), ObTabletID(200001),
      invalid_scn, valid_scn));
  EXPECT_EQ(OB_INVALID_ARGUMENT, invalid_source_item.init(
      valid_macro_id, share::ObLSID(1), ObTabletID(200001),
      valid_scn, invalid_scn));

  ASSERT_EQ(OB_SUCCESS, init_expected_item(10, item_10));
  ASSERT_EQ(OB_SUCCESS, init_expected_item(2, item_2));
  std::string path_10;
  std::string path_2;
  ASSERT_EQ(OB_SUCCESS,
      build_data_check_relative_path(item_10.get_macro_id(), path_10));
  ASSERT_EQ(OB_SUCCESS,
      build_data_check_relative_path(item_2.get_macro_id(), path_2));
  EXPECT_LT(path_10, path_2);

  ObSSDataCheckExpectedSortItem sort_item_10;
  ObSSDataCheckExpectedSortItem sort_item_2;
  ASSERT_EQ(OB_SUCCESS, sort_item_10.init(
      item_10, ObString(static_cast<int32_t>(path_10.length()), path_10.data())));
  ASSERT_EQ(OB_SUCCESS, sort_item_2.init(
      item_2, ObString(static_cast<int32_t>(path_2.length()), path_2.data())));
  int compare_ret = OB_SUCCESS;
  ObSSDataCheckExpectedSortItemCompare compare(compare_ret);
  EXPECT_TRUE(compare(&sort_item_10, &sort_item_2));
  EXPECT_FALSE(compare(&sort_item_2, &sort_item_10));
  EXPECT_EQ(OB_SUCCESS, compare_ret);

  std::vector<ObSSDataCheckExpectedSortItem *> invalid_items = {
      nullptr, &sort_item_10};
  lib::ob_sort(invalid_items.begin(), invalid_items.end(), compare);
  EXPECT_EQ(OB_INVALID_ARGUMENT, compare_ret);

  ObArenaAllocator allocator("DataCheckItem");
  EXPECT_EQ(0, item_10.get_deep_copy_size());
  int64_t pos = 0;
  ObSSDataCheckExpectedItem copy;
  ASSERT_EQ(OB_SUCCESS, copy.deep_copy(
      item_10, nullptr, item_10.get_deep_copy_size(), pos));
  EXPECT_EQ(0, pos);
  EXPECT_TRUE(item_10 == copy);
  EXPECT_EQ(item_10.get_ls_id(), copy.get_ls_id());
  EXPECT_EQ(item_10.get_tablet_id(), copy.get_tablet_id());
  EXPECT_EQ(item_10.get_reorganize_scn(), copy.get_reorganize_scn());
  EXPECT_EQ(item_10.get_meta_scn(), copy.get_meta_scn());

  const int64_t serialize_size = item_10.get_serialize_size();
  char *serialize_buf = static_cast<char *>(allocator.alloc(serialize_size));
  ASSERT_NE(nullptr, serialize_buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      item_10.serialize(serialize_buf, serialize_size, pos));
  pos = 0;
  ObSSDataCheckExpectedItem deserialized;
  ASSERT_EQ(OB_SUCCESS,
      deserialized.deserialize(serialize_buf, serialize_size, pos));
  EXPECT_TRUE(item_10 == deserialized);
  EXPECT_EQ(item_10.get_ls_id(), deserialized.get_ls_id());
  EXPECT_EQ(item_10.get_tablet_id(), deserialized.get_tablet_id());
  EXPECT_EQ(item_10.get_reorganize_scn(), deserialized.get_reorganize_scn());
  EXPECT_EQ(item_10.get_meta_scn(), deserialized.get_meta_scn());
}

TEST_F(TestSSDataCheck, expected_sort_byte_order_and_dedup)
{
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  ObSSDataCheckExpectedSortPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(
      OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &tmp_file_mgr));

  ObSSDataCheckExpectedSorter sorter;
  ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
  const int64_t macro_seqs[] = {2, 10, 1};
  std::vector<std::string> expected_keys;
  for (int64_t i = 0; i < ARRAYSIZEOF(macro_seqs); ++i) {
    ObSSDataCheckExpectedItem item;
    std::string relative_path;
    ASSERT_EQ(OB_SUCCESS, init_expected_item(macro_seqs[i], item));
    ASSERT_EQ(OB_SUCCESS,
        build_data_check_relative_path(item.get_macro_id(), relative_path));
    expected_keys.push_back(relative_path);
    ASSERT_EQ(OB_SUCCESS, sorter.add_item(item));
  }
  ObSSDataCheckExpectedItem duplicate;
  ASSERT_EQ(OB_SUCCESS,
      init_expected_item(1, duplicate, 2, 200002, 101, 201));
  ASSERT_EQ(OB_SUCCESS, sorter.add_item(duplicate));
  ASSERT_EQ(OB_SUCCESS, sorter.finish());

  int64_t pass_count = -1;
  ASSERT_EQ(OB_SUCCESS, drive_expected_merge(pipeline, false, pass_count));
  EXPECT_EQ(0, pass_count);
  ObSSDataCheckSortedFileInfo result;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_sorted_file(result));
  int64_t item_count = 0;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_sorted_item_count(item_count));
  EXPECT_EQ(3, item_count);

  std::vector<std::string> actual_keys;
  ASSERT_EQ(OB_SUCCESS, read_expected_file(result, actual_keys));
  std::sort(expected_keys.begin(), expected_keys.end());
  EXPECT_EQ(expected_keys, actual_keys);
  ASSERT_EQ(OB_SUCCESS, pipeline.cleanup());
  result.reset();
  tmp_file_mgr.reset();
}

TEST_F(TestSSDataCheck, expected_sort_concurrent_producers_no_loss)
{
  static const int64_t UNIQUE_ITEM_COUNT = 96;
  static const int64_t DUPLICATE_ITEM_COUNT = 12;
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  ObSSDataCheckExpectedSortPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(
      OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &tmp_file_mgr));

  ObTenantBase *tenant_ctx = ObTenantEnv::get_tenant();
  ASSERT_NE(nullptr, tenant_ctx);
  int64_t producer_count = 0;
  ASSERT_NE(nullptr, MTL(share::ObTenantDagScheduler *));
  ASSERT_EQ(OB_SUCCESS, MTL(share::ObTenantDagScheduler *)->get_limit(
      share::ObDagPrio::DAG_PRIO_SS_DATA_CHECK, producer_count));
  ASSERT_GT(producer_count, 0);
  std::vector<int> worker_rets(producer_count, OB_SUCCESS);
  std::vector<std::thread> workers;
  std::atomic<int64_t> ready_count(0);
  std::atomic<bool> start_workers(false);
  workers.reserve(producer_count);
  for (int64_t worker_idx = 0;
       worker_idx < producer_count;
       ++worker_idx) {
    workers.emplace_back([
        worker_idx, producer_count, tenant_ctx, &pipeline, &worker_rets,
        &ready_count, &start_workers]() {
      ObTenantEnv::set_tenant(tenant_ctx);
      int ret = OB_SUCCESS;
      ObSSDataCheckExpectedSorter sorter;
      ready_count.fetch_add(1, std::memory_order_release);
      while (!start_workers.load(std::memory_order_acquire)) {
        std::this_thread::yield();
      }
      if (OB_FAIL(sorter.init(&pipeline))) {
        STORAGE_LOG(WARN, "fail to initialize concurrent expected sorter",
            K(ret), K(worker_idx));
      }
      for (int64_t macro_seq = worker_idx;
           OB_SUCC(ret) && macro_seq < UNIQUE_ITEM_COUNT;
           macro_seq += producer_count) {
        ObSSDataCheckExpectedItem item;
        if (OB_FAIL(init_expected_item(macro_seq, item))) {
          STORAGE_LOG(WARN, "fail to initialize concurrent expected item",
              K(ret), K(worker_idx), K(macro_seq));
        } else if (OB_FAIL(sorter.add_item(item))) {
          STORAGE_LOG(WARN, "fail to add concurrent expected item",
              K(ret), K(worker_idx), K(macro_seq));
        }
      }
      for (int64_t macro_seq = 0;
           OB_SUCC(ret) && macro_seq < DUPLICATE_ITEM_COUNT;
           ++macro_seq) {
        ObSSDataCheckExpectedItem item;
        if (OB_FAIL(init_expected_item(macro_seq, item))) {
          STORAGE_LOG(WARN, "fail to initialize duplicate expected item",
              K(ret), K(worker_idx), K(macro_seq));
        } else if (OB_FAIL(sorter.add_item(item))) {
          STORAGE_LOG(WARN, "fail to add duplicate expected item",
              K(ret), K(worker_idx), K(macro_seq));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(sorter.finish())) {
        STORAGE_LOG(WARN, "fail to finish concurrent expected sorter",
            K(ret), K(worker_idx));
      }
      sorter.reset();
      worker_rets.at(worker_idx) = ret;
    });
  }
  while (ready_count.load(std::memory_order_acquire) < producer_count) {
    std::this_thread::yield();
  }
  start_workers.store(true, std::memory_order_release);
  for (auto &worker : workers) {
    worker.join();
  }
  for (int64_t worker_idx = 0;
       worker_idx < producer_count;
       ++worker_idx) {
    ASSERT_EQ(OB_SUCCESS, worker_rets.at(worker_idx));
  }

  int64_t pass_count = 0;
  ASSERT_EQ(OB_SUCCESS, drive_expected_merge(pipeline, true, pass_count));
  ObSSDataCheckSortedFileInfo result;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_sorted_file(result));
  int64_t actual_item_count = 0;
  ASSERT_EQ(OB_SUCCESS,
      pipeline.get_sorted_item_count(actual_item_count));
  EXPECT_EQ(UNIQUE_ITEM_COUNT, actual_item_count);

  std::vector<std::string> expected_paths;
  for (int64_t macro_seq = 0;
       macro_seq < UNIQUE_ITEM_COUNT;
       ++macro_seq) {
    std::string relative_path;
    ASSERT_EQ(OB_SUCCESS, build_data_check_relative_path(
        make_data_check_id(macro_seq), relative_path));
    expected_paths.push_back(relative_path);
  }
  std::sort(expected_paths.begin(), expected_paths.end());
  std::vector<std::string> actual_paths;
  ASSERT_EQ(OB_SUCCESS, read_expected_file(result, actual_paths));
  EXPECT_EQ(expected_paths, actual_paths);
  ASSERT_EQ(OB_SUCCESS, pipeline.cleanup());
  result.reset();
  tmp_file_mgr.reset();
}

TEST_F(TestSSDataCheck, expected_sort_producer_barrier)
{
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  ObSSDataCheckExpectedSortPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(
      OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &tmp_file_mgr));
  ObSSDataCheckExpectedSorter sorter;
  ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
  EXPECT_EQ(OB_STATE_NOT_MATCH, pipeline.prepare_merge());
  EXPECT_EQ(OB_STATE_NOT_MATCH, pipeline.cleanup());
  ASSERT_EQ(OB_SUCCESS, sorter.finish());
  int64_t pass_count = -1;
  ASSERT_EQ(OB_SUCCESS, drive_expected_merge(pipeline, false, pass_count));
  EXPECT_EQ(0, pass_count);
  ASSERT_EQ(OB_SUCCESS, pipeline.cleanup());
  tmp_file_mgr.reset();
}

TEST_F(TestSSDataCheck, expected_merge_multi_pass)
{
  ObMarkSweepTmpFileMgr tmp_file_mgr;
  ASSERT_EQ(OB_SUCCESS, tmp_file_mgr.init(OB_SYS_TENANT_ID));
  ObSSDataCheckExpectedSortPipeline pipeline;
  ASSERT_EQ(OB_SUCCESS, pipeline.init(
      OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0, &tmp_file_mgr));
  const int64_t item_count = ObSSDataCheckExpectedMerger::MAX_FAN_IN + 1;
  std::vector<std::string> expected_keys;
  for (int64_t i = 0; i < item_count; ++i) {
    std::string relative_path;
    ASSERT_EQ(OB_SUCCESS,
        build_data_check_relative_path(make_data_check_id(i), relative_path));
    expected_keys.push_back(relative_path);
    ObSSDataCheckExpectedSorter sorter;
    ObSSDataCheckExpectedItem item;
    ASSERT_EQ(OB_SUCCESS, sorter.init(&pipeline));
    ASSERT_EQ(OB_SUCCESS, init_expected_item(i, item));
    ASSERT_EQ(OB_SUCCESS, sorter.add_item(item));
    ASSERT_EQ(OB_SUCCESS, sorter.finish());
  }
  int64_t pass_count = 0;
  ASSERT_EQ(OB_SUCCESS, drive_expected_merge(pipeline, true, pass_count));
  EXPECT_EQ(2, pass_count);
  ObSSDataCheckSortedFileInfo result;
  ASSERT_EQ(OB_SUCCESS, pipeline.get_sorted_file(result));
  std::vector<std::string> actual_keys;
  ASSERT_EQ(OB_SUCCESS, read_expected_file(result, actual_keys));
  std::sort(expected_keys.begin(), expected_keys.end());
  EXPECT_EQ(expected_keys, actual_keys);
  ASSERT_EQ(OB_SUCCESS, pipeline.cleanup());
  result.reset();
  tmp_file_mgr.reset();
}

TEST_F(TestSSDataCheck, context_lob_meta_policy_is_frozen_for_round)
{
  const uint64_t previous_cluster_version = GET_MIN_CLUSTER_VERSION();
  DEFER(ObClusterVersion::get_instance().update_cluster_version(previous_cluster_version));
  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_3_0);
  ObSSDataCheckCtx ctx;
  EXPECT_FALSE(ctx.is_lob_meta_scan_enabled());
  ASSERT_EQ(OB_SUCCESS, ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  EXPECT_FALSE(ctx.is_lob_meta_scan_enabled());

  ObClusterVersion::get_instance().update_cluster_version(CLUSTER_VERSION_4_6_3_1);
  EXPECT_FALSE(ctx.is_lob_meta_scan_enabled());
  EXPECT_EQ(OB_INIT_TWICE, ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  EXPECT_FALSE(ctx.is_lob_meta_scan_enabled());
  ObSSDataCheckCtx next_ctx;
  ASSERT_EQ(OB_SUCCESS, next_ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  EXPECT_TRUE(next_ctx.is_lob_meta_scan_enabled());
  ASSERT_EQ(OB_SUCCESS, next_ctx.cleanup());
  EXPECT_FALSE(next_ctx.is_lob_meta_scan_enabled());

  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  EXPECT_FALSE(ctx.is_lob_meta_scan_enabled());
  ASSERT_EQ(OB_SUCCESS, ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
      ObSSDataCheckMode::SINGLE, MEM_LIMIT, BUF_SIZE, 0));
  EXPECT_TRUE(ctx.is_lob_meta_scan_enabled());
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  EXPECT_FALSE(ctx.is_lob_meta_scan_enabled());
}

TEST_F(TestSSDataCheck, context_rejects_cleanup_with_active_task)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  EXPECT_EQ(OB_INIT_TWICE,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  EXPECT_TRUE(ctx.is_valid());
  EXPECT_EQ(ObSSDataCheckMode::SINGLE, ctx.get_check_mode());
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  EXPECT_EQ(1, ctx.get_active_task_count());
  EXPECT_EQ(OB_STATE_NOT_MATCH, ctx.cleanup());
  ctx.dec_active_task_count(OB_TIMEOUT);
  EXPECT_EQ(0, ctx.get_active_task_count());
  EXPECT_EQ(OB_TIMEOUT, ctx.get_first_error());
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  EXPECT_FALSE(ctx.is_valid());
}

TEST_F(TestSSDataCheck, context_publishes_two_state_result)
{
  ObSSDataCheckStatus status = ObSSDataCheckStatus::NOT_HEALTH;
  ObSSDataCheckExpectedItem missing_item;
  ASSERT_EQ(OB_SUCCESS, init_expected_item(1, missing_item));

  ObSSDataCheckCtx health_ctx;
  ASSERT_EQ(OB_SUCCESS,
      health_ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  EXPECT_FALSE(health_ctx.has_published_status());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, health_ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::NOT_HEALTH, status);
  EXPECT_EQ(OB_STATE_NOT_MATCH, health_ctx.publish_status());
  ASSERT_EQ(OB_SUCCESS, health_ctx.finish_expected_collect());
  EXPECT_EQ(OB_STATE_NOT_MATCH, health_ctx.publish_status());
  ASSERT_EQ(OB_SUCCESS, health_ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, health_ctx.publish_status());
  EXPECT_TRUE(health_ctx.has_published_status());
  ASSERT_EQ(OB_SUCCESS, health_ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::HEALTH, status);
  EXPECT_EQ(OB_SUCCESS, health_ctx.publish_status());
  EXPECT_EQ(OB_STATE_NOT_MATCH, health_ctx.inc_active_task_count());
  EXPECT_EQ(OB_STATE_NOT_MATCH,
      health_ctx.record_referenced_missing(missing_item));
  ASSERT_EQ(OB_SUCCESS, health_ctx.cleanup());

  ObSSDataCheckCtx not_health_ctx;
  ASSERT_EQ(OB_SUCCESS,
      not_health_ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, not_health_ctx.finish_expected_collect());
  ASSERT_EQ(OB_SUCCESS, not_health_ctx.inc_active_task_count());
  EXPECT_EQ(OB_STATE_NOT_MATCH, not_health_ctx.publish_status());
  ASSERT_EQ(OB_SUCCESS,
      not_health_ctx.record_referenced_missing(missing_item));
  ObSSDataCheckExpectedItem retained_missing_item;
  ASSERT_EQ(OB_SUCCESS, not_health_ctx.get_first_referenced_missing_item(
      retained_missing_item));
  EXPECT_EQ(missing_item.get_macro_id(), retained_missing_item.get_macro_id());
  EXPECT_EQ(missing_item.get_ls_id(), retained_missing_item.get_ls_id());
  EXPECT_EQ(missing_item.get_tablet_id(), retained_missing_item.get_tablet_id());
  EXPECT_EQ(missing_item.get_reorganize_scn(),
      retained_missing_item.get_reorganize_scn());
  EXPECT_EQ(missing_item.get_meta_scn(), retained_missing_item.get_meta_scn());
  ObSSDataCheckExpectedItem later_missing_item;
  ASSERT_EQ(OB_SUCCESS, init_expected_item(
      2, later_missing_item, 2, 200002, 300, 400));
  ASSERT_EQ(OB_SUCCESS,
      not_health_ctx.record_referenced_missing(later_missing_item));
  ASSERT_EQ(OB_SUCCESS, not_health_ctx.get_first_referenced_missing_item(
      retained_missing_item));
  EXPECT_EQ(missing_item.get_macro_id(), retained_missing_item.get_macro_id());
  not_health_ctx.dec_active_task_count(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, not_health_ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, not_health_ctx.publish_status());
  ASSERT_EQ(OB_SUCCESS, not_health_ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::NOT_HEALTH, status);
  ASSERT_EQ(OB_SUCCESS, not_health_ctx.cleanup());

  ObSSDataCheckCtx failed_ctx;
  ASSERT_EQ(OB_SUCCESS,
      failed_ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, failed_ctx.inc_active_task_count());
  failed_ctx.dec_active_task_count(OB_TIMEOUT);
  EXPECT_EQ(OB_TIMEOUT, failed_ctx.publish_status());
  EXPECT_FALSE(failed_ctx.has_published_status());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, failed_ctx.get_status(status));
  ASSERT_EQ(OB_SUCCESS, failed_ctx.cleanup());
}

TEST_F(TestSSDataCheck, meta_ref_error_does_not_publish_status)
{
  ObSSDataCheckExpectedItem item;
  ASSERT_EQ(OB_SUCCESS, init_expected_item(1, item));
  ObSSDataCheckExpectedItem invalid_item;
  ObSSMetaService meta_service;
  ObSSDataCheckMetaRefChecker checker;
  bool is_exist = true;

  EXPECT_EQ(OB_NOT_INIT, checker.check_reference_exist(item, is_exist));
  EXPECT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, checker.init(meta_service));
  EXPECT_EQ(OB_INIT_TWICE, checker.init(meta_service));
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      checker.check_reference_exist(invalid_item, is_exist));
  EXPECT_TRUE(is_exist);

  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  const int check_ret = checker.check_reference_exist(invalid_item, is_exist);
  EXPECT_EQ(OB_INVALID_ARGUMENT, check_ret);
  ctx.dec_active_task_count(check_ret);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ctx.publish_status());
  EXPECT_FALSE(ctx.has_published_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::HEALTH, status);
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, meta_ref_cache_uses_exact_key_and_skips_errors)
{
  ObSSDataCheckExpectedItem base_item;
  ObSSDataCheckExpectedItem duplicate_item;
  ObSSDataCheckExpectedItem different_ls_item;
  ObSSDataCheckExpectedItem different_tablet_item;
  ObSSDataCheckExpectedItem different_reorganize_item;
  ObSSDataCheckExpectedItem different_meta_item;
  ASSERT_EQ(OB_SUCCESS, init_expected_item(
      1, base_item, 1, 200001, 100, 200));
  ASSERT_EQ(OB_SUCCESS, init_expected_item(
      2, duplicate_item, 1, 200001, 100, 200));
  ASSERT_EQ(OB_SUCCESS, init_expected_item(
      3, different_ls_item, 2, 200001, 100, 200));
  ASSERT_EQ(OB_SUCCESS, init_expected_item(
      4, different_tablet_item, 1, 200002, 100, 200));
  ASSERT_EQ(OB_SUCCESS, init_expected_item(
      5, different_reorganize_item, 1, 200001, 101, 200));
  ASSERT_EQ(OB_SUCCESS, init_expected_item(
      6, different_meta_item, 1, 200001, 100, 201));

  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({true, false, true, false, true});
  ObSSDataCheckListChecker::MetaRefCache cache;
  ASSERT_EQ(OB_SUCCESS,
      cache.init(OB_SYS_TENANT_ID, 6, meta_ref_checker));
  EXPECT_FALSE(cache.result_map_.created());
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS,
      cache.check_reference_exist(base_item, is_exist));
  EXPECT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS,
      cache.check_reference_exist(duplicate_item, is_exist));
  EXPECT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS,
      cache.check_reference_exist(different_ls_item, is_exist));
  EXPECT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS,
      cache.check_reference_exist(different_tablet_item, is_exist));
  EXPECT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS,
      cache.check_reference_exist(different_reorganize_item, is_exist));
  EXPECT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS,
      cache.check_reference_exist(different_meta_item, is_exist));
  EXPECT_TRUE(is_exist);
  EXPECT_EQ(5, meta_ref_checker.get_call_count());
  EXPECT_EQ(5, meta_ref_checker.get_unique_source_key_count());
  EXPECT_TRUE(meta_ref_checker.is_each_source_key_called_once());

  FakeDataCheckMetaRefChecker error_checker;
  error_checker.set_results({}, OB_IO_ERROR);
  ObSSDataCheckListChecker::MetaRefCache error_cache;
  ASSERT_EQ(OB_SUCCESS,
      error_cache.init(OB_SYS_TENANT_ID, 1, error_checker));
  is_exist = true;
  EXPECT_EQ(OB_IO_ERROR,
      error_cache.check_reference_exist(base_item, is_exist));
  EXPECT_TRUE(is_exist);
  EXPECT_EQ(1, error_checker.get_call_count());
  error_checker.set_result(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS,
      error_cache.check_reference_exist(base_item, is_exist));
  EXPECT_FALSE(is_exist);
  EXPECT_EQ(2, error_checker.get_call_count());
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS,
      error_cache.check_reference_exist(duplicate_item, is_exist));
  EXPECT_FALSE(is_exist);
  EXPECT_EQ(2, error_checker.get_call_count());
}

TEST_F(TestSSDataCheck, expected_sort_task_graph_empty_input)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ObArray<share::ObITask::ObITaskType> execution_order;
  ASSERT_EQ(OB_SUCCESS, run_expected_sort_task_graph(ctx, execution_order));
  ASSERT_EQ(3, execution_order.count());
  EXPECT_EQ(share::ObITask::TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_START,
      execution_order.at(0));
  EXPECT_EQ(share::ObITask::TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_PASS_CTRL,
      execution_order.at(1));
  EXPECT_EQ(share::ObITask::TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_COMPLETE,
      execution_order.at(2));
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, expected_sort_task_graph_max_fan_in_plus_one)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  const int64_t item_count = ObSSDataCheckExpectedMerger::MAX_FAN_IN + 1;
  std::vector<std::string> expected_keys;
  for (int64_t i = 0; i < item_count; ++i) {
    std::string relative_path;
    ASSERT_EQ(OB_SUCCESS,
        build_data_check_relative_path(make_data_check_id(i), relative_path));
    expected_keys.push_back(relative_path);
    ASSERT_EQ(OB_SUCCESS, add_single_item_run(ctx, i));
  }
  ObArray<share::ObITask::ObITaskType> execution_order;
  ASSERT_EQ(OB_SUCCESS, run_expected_sort_task_graph(ctx, execution_order));

  int64_t pass_ctrl_count = 0;
  int64_t batch_count = 0;
  for (int64_t i = 0; i < execution_order.count(); ++i) {
    if (share::ObITask::TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_PASS_CTRL
        == execution_order.at(i)) {
      ++pass_ctrl_count;
    } else if (share::ObITask::TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_BATCH
        == execution_order.at(i)) {
      ++batch_count;
    }
  }
  EXPECT_EQ(3, pass_ctrl_count);
  EXPECT_EQ(3, batch_count);
  EXPECT_EQ(share::ObITask::TASK_TYPE_SS_DATA_CHECK_EXPECTED_MERGE_COMPLETE,
      execution_order.at(execution_order.count() - 1));

  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckSortedFileInfo result;
  int64_t sorted_item_count = 0;
  ASSERT_EQ(OB_SUCCESS,
      ctx.get_expected_sort_pipeline().get_sorted_item_count(sorted_item_count));
  ASSERT_EQ(OB_SUCCESS,
      ctx.get_expected_sort_pipeline().get_sorted_file(result));
  std::vector<std::string> actual_keys;
  ASSERT_EQ(OB_SUCCESS, read_expected_file(result, actual_keys));
  result.reset();
  ctx.dec_active_task_count(OB_SUCCESS);
  EXPECT_EQ(item_count, sorted_item_count);
  std::sort(expected_keys.begin(), expected_keys.end());
  EXPECT_EQ(expected_keys, actual_keys);
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_matches_byte_order_and_ignores_extra)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected(
      ctx, {2, 10, 1}, expected_file, expected_count, sorted_paths));

  std::string prefix;
  std::vector<std::string> object_names;
  for (int64_t i = 0; i < sorted_paths.size(); ++i) {
    std::string current_prefix;
    std::string name;
    ASSERT_EQ(OB_SUCCESS, split_data_check_path(
        sorted_paths.at(i), current_prefix, name));
    if (prefix.empty()) {
      prefix = current_prefix;
    }
    ASSERT_EQ(prefix, current_prefix);
    object_names.push_back(name);
  }
  object_names.push_back("000-extra-object");
  object_names.push_back(object_names.at(1));
  std::sort(object_names.begin(), object_names.end());

  FakeDataCheckObjectLister object_lister;
  object_lister.set_listing(prefix, object_names);
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({});
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  ASSERT_EQ(OB_SUCCESS, check_ret);
  EXPECT_EQ(expected_count, stat.expected_count_);
  EXPECT_EQ(expected_count, stat.found_count_);
  EXPECT_EQ(0, stat.missing_count_);
  EXPECT_EQ(0, meta_ref_checker.get_call_count());
  EXPECT_EQ(1, object_lister.get_call_count());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::NOT_HEALTH;
  ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::HEALTH, status);
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_handles_mixed_object_types_and_multiple_prefixes)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  const std::vector<MacroBlockId> macro_ids = {
      make_data_check_object_id(
          ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 1),
      make_data_check_object_id(
          ObStorageObjectType::SHARED_MAJOR_DATA_MACRO, 2),
      make_data_check_object_id(
          ObStorageObjectType::SHARED_MINOR_DATA_MACRO, 3),
      make_data_check_object_id(
          ObStorageObjectType::SHARED_MINOR_DATA_MACRO, 4),
      make_data_check_object_id(
          ObStorageObjectType::SHARED_TABLET_SUB_META, 5),
      make_data_check_object_id(
          ObStorageObjectType::SHARED_TABLET_SUB_META, 6)};
  for (int64_t i = 0; i < macro_ids.size(); ++i) {
    ASSERT_TRUE(macro_ids.at(i).is_valid());
    ASSERT_TRUE(macro_ids.at(i).is_in_object_storage());
  }
  ASSERT_TRUE(macro_ids.at(4).is_shared_tablet_sub_meta());
  ASSERT_TRUE(macro_ids.at(5).is_shared_tablet_sub_meta());

  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected_by_macro_ids(
      ctx, macro_ids, expected_file, expected_count, sorted_paths));
  ASSERT_EQ(6, expected_count);
  ASSERT_EQ(6, sorted_paths.size());

  std::vector<std::string> prefixes;
  std::vector<std::vector<std::string>> expected_name_sets;
  for (int64_t i = 0; i < sorted_paths.size(); ++i) {
    std::string prefix;
    std::string name;
    ASSERT_EQ(OB_SUCCESS,
        split_data_check_path(sorted_paths.at(i), prefix, name));
    if (prefixes.empty() || prefixes.back() != prefix) {
      prefixes.push_back(prefix);
      expected_name_sets.push_back(std::vector<std::string>());
    }
    expected_name_sets.back().push_back(name);
  }
  ASSERT_EQ(3, prefixes.size());
  ASSERT_EQ(prefixes.size(), expected_name_sets.size());
  for (int64_t i = 0; i < expected_name_sets.size(); ++i) {
    ASSERT_EQ(2, expected_name_sets.at(i).size());
  }

  FakeDataCheckObjectLister object_lister;
  for (int64_t i = 0; i < prefixes.size(); ++i) {
    std::vector<std::string> actual_names;
    if (0 == i) {
      actual_names.push_back(expected_name_sets.at(i).at(0));
      actual_names.push_back("000-extra-object");
    } else if (1 == i) {
      actual_names.push_back(expected_name_sets.at(i).at(0));
    } else {
      actual_names = expected_name_sets.at(i);
    }
    std::sort(actual_names.begin(), actual_names.end());
    object_lister.add_listing(prefixes.at(i), actual_names);
  }
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({true});
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  ASSERT_EQ(OB_SUCCESS, check_ret);
  EXPECT_EQ(6, stat.expected_count_);
  EXPECT_EQ(4, stat.found_count_);
  EXPECT_EQ(2, stat.missing_count_);
  EXPECT_EQ(2, stat.referenced_missing_count_);
  EXPECT_EQ(0, stat.unreferenced_missing_count_);
  EXPECT_EQ(5, stat.listed_object_count_);
  EXPECT_EQ(3, stat.listed_prefix_count_);
  EXPECT_EQ(1, meta_ref_checker.get_call_count());
  EXPECT_EQ(3, object_lister.get_call_count());
  EXPECT_EQ(prefixes, object_lister.get_called_prefixes());
  EXPECT_EQ(16, stop_checker.get_call_count());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
  ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::NOT_HEALTH, status);
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_caches_meta_reference_by_source_key)
{
  const int64_t missing_object_count = 10 * 1000;
  const int64_t source_key_count = 100;
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));

  std::vector<MacroBlockId> macro_ids;
  macro_ids.reserve(missing_object_count);
  for (int64_t i = 0; i < missing_object_count; ++i) {
    ObStorageObjectType object_type = ObStorageObjectType::SHARED_MAJOR_DATA_MACRO;
    if (1 == i % 3) {
      object_type = ObStorageObjectType::SHARED_MINOR_DATA_MACRO;
    } else if (2 == i % 3) {
      object_type = ObStorageObjectType::SHARED_TABLET_SUB_META;
    }
    macro_ids.push_back(make_data_check_object_id(object_type, i + 1));
  }

  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected_with_source_keys(
      ctx, macro_ids, source_key_count,
      expected_file, expected_count, sorted_paths));
  ASSERT_EQ(missing_object_count, expected_count);

  FakeDataCheckObjectLister object_lister;
  std::vector<std::string> prefixes;
  for (int64_t i = 0; i < sorted_paths.size(); ++i) {
    std::string prefix;
    std::string name;
    ASSERT_EQ(OB_SUCCESS,
        split_data_check_path(sorted_paths.at(i), prefix, name));
    if (prefixes.empty() || prefixes.back() != prefix) {
      prefixes.push_back(prefix);
      object_lister.add_listing(prefix, {});
    }
  }
  ASSERT_GT(prefixes.size(), 1);

  FakeDataCheckMetaRefChecker meta_ref_checker;
  std::vector<bool> reference_results(source_key_count, false);
  for (int64_t i = 0; i < source_key_count / 2; ++i) {
    reference_results.at(i) = true;
  }
  meta_ref_checker.set_results(reference_results);
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  ASSERT_EQ(OB_SUCCESS, check_ret);
  EXPECT_EQ(missing_object_count, stat.expected_count_);
  EXPECT_EQ(0, stat.found_count_);
  EXPECT_EQ(missing_object_count, stat.missing_count_);
  EXPECT_EQ(missing_object_count / 2, stat.referenced_missing_count_);
  EXPECT_EQ(missing_object_count / 2, stat.unreferenced_missing_count_);
  EXPECT_EQ(0, stat.listed_object_count_);
  EXPECT_EQ(prefixes.size(), stat.listed_prefix_count_);
  EXPECT_EQ(source_key_count, meta_ref_checker.get_call_count());
  EXPECT_EQ(source_key_count,
      meta_ref_checker.get_unique_source_key_count());
  EXPECT_TRUE(meta_ref_checker.is_each_source_key_called_once());
  EXPECT_EQ(prefixes.size(), object_lister.get_call_count());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
  ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::NOT_HEALTH, status);
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_stops_after_first_prefix_before_drain)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  MacroBlockId major_id = make_data_check_id(1);
  MacroBlockId minor_id = make_data_check_id(2);
  minor_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MINOR_DATA_MACRO));
  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected_by_macro_ids(
      ctx, {major_id, minor_id}, expected_file, expected_count, sorted_paths));
  ASSERT_EQ(2, expected_count);
  ASSERT_EQ(2, sorted_paths.size());

  std::string first_prefix;
  std::string first_name;
  std::string second_prefix;
  std::string second_name;
  ASSERT_EQ(OB_SUCCESS, split_data_check_path(
      sorted_paths.at(0), first_prefix, first_name));
  ASSERT_EQ(OB_SUCCESS, split_data_check_path(
      sorted_paths.at(1), second_prefix, second_name));
  ASSERT_NE(first_prefix, second_prefix);

  FakeDataCheckObjectLister object_lister;
  object_lister.set_listing(first_prefix, {});
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({true});
  FakeDataCheckListStopChecker stop_checker;
  stop_checker.set_result(3, OB_CANCELED);
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  EXPECT_EQ(OB_CANCELED, check_ret);
  EXPECT_EQ(3, stop_checker.get_call_count());
  EXPECT_EQ(1, object_lister.get_call_count());
  EXPECT_EQ(0, meta_ref_checker.get_call_count());
  EXPECT_EQ(expected_count, stat.expected_count_);
  EXPECT_EQ(0, stat.found_count_);
  EXPECT_EQ(0, stat.missing_count_);
  EXPECT_EQ(0, stat.listed_object_count_);
  EXPECT_EQ(1, stat.listed_prefix_count_);
  EXPECT_EQ(OB_CANCELED, ctx.get_first_error());
  EXPECT_FALSE(ctx.is_list_check_complete());
  EXPECT_EQ(OB_CANCELED, ctx.publish_status());
  EXPECT_FALSE(ctx.has_published_status());
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_stops_on_next_list_callback)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected(
      ctx, {1}, expected_file, expected_count, sorted_paths));
  std::string prefix;
  std::string unused_name;
  ASSERT_EQ(OB_SUCCESS,
      split_data_check_path(sorted_paths.at(0), prefix, unused_name));

  FakeDataCheckObjectLister object_lister;
  object_lister.set_listing(
      prefix, {"first-extra", "second-extra", "third-extra"});
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({});
  FakeDataCheckListStopChecker stop_checker;
  object_lister.set_after_emit_hook(
      [&stop_checker](const int64_t emitted_object_count) {
        if (1 == emitted_object_count) {
          stop_checker.arm_result(OB_CANCELED);
        }
      });
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  EXPECT_EQ(OB_CANCELED, check_ret);
  EXPECT_EQ(3, stop_checker.get_call_count());
  EXPECT_EQ(1, object_lister.get_call_count());
  EXPECT_EQ(1, object_lister.get_emitted_object_count());
  EXPECT_EQ(1, object_lister.get_accepted_object_count());
  EXPECT_EQ(1, object_lister.get_accepted_item_count());
  EXPECT_EQ(0, stat.listed_object_count_);
  EXPECT_EQ(0, stat.listed_prefix_count_);
  EXPECT_EQ(0, meta_ref_checker.get_call_count());
  EXPECT_EQ(OB_CANCELED, ctx.get_first_error());
  EXPECT_EQ(OB_CANCELED, ctx.publish_status());
  EXPECT_FALSE(ctx.has_published_status());
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_stop_checker_reports_expired_deadline)
{
  FakeDataCheckListStopChecker stop_checker;
  const int64_t expired_timestamp = ObTimeUtility::current_time_us() - 1;
  ObSSDataCheckListChecker::ListStopChecker list_stop_checker(
      stop_checker, expired_timestamp);
  EXPECT_EQ(OB_TIMEOUT, list_stop_checker.check_stop());
  EXPECT_EQ(1, stop_checker.get_call_count());

  // 停止状态优先于同时发生的 deadline，保留原始停止返回码。
  stop_checker.arm_result(OB_CANCELED);
  EXPECT_EQ(OB_CANCELED, list_stop_checker.check_stop());
  EXPECT_EQ(2, stop_checker.get_call_count());
}

TEST_F(TestSSDataCheck, list_checker_stops_before_missing_reference_recheck)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected(
      ctx, {1, 2}, expected_file, expected_count, sorted_paths));
  std::string prefix;
  std::string unused_name;
  ASSERT_EQ(OB_SUCCESS,
      split_data_check_path(sorted_paths.at(0), prefix, unused_name));

  FakeDataCheckObjectLister object_lister;
  object_lister.set_listing(prefix, {});
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({true});
  FakeDataCheckListStopChecker stop_checker;
  stop_checker.set_result(4, OB_CANCELED);
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  EXPECT_EQ(OB_CANCELED, check_ret);
  EXPECT_EQ(4, stop_checker.get_call_count());
  EXPECT_EQ(0, meta_ref_checker.get_call_count());
  EXPECT_EQ(1, object_lister.get_call_count());
  EXPECT_EQ(expected_count, stat.expected_count_);
  EXPECT_EQ(0, stat.found_count_);
  EXPECT_EQ(0, stat.missing_count_);
  EXPECT_EQ(0, stat.listed_object_count_);
  EXPECT_EQ(1, stat.listed_prefix_count_);
  EXPECT_EQ(OB_CANCELED, ctx.get_first_error());
  EXPECT_EQ(OB_CANCELED, ctx.publish_status());
  EXPECT_FALSE(ctx.has_published_status());
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_rechecks_missing_reference)
{
  for (int64_t reference_case = 0; reference_case < 2; ++reference_case) {
    const bool reference_exist = 0 == reference_case;
    ObSSDataCheckCtx ctx;
    ASSERT_EQ(OB_SUCCESS,
        ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
            ObSSDataCheckMode::SINGLE,
            MEM_LIMIT, BUF_SIZE, 0));
    ObSSDataCheckSortedFileInfo expected_file;
    int64_t expected_count = 0;
    std::vector<std::string> sorted_paths;
    ASSERT_EQ(OB_SUCCESS, build_list_check_expected(
        ctx, {1, 2}, expected_file, expected_count, sorted_paths));

    std::string prefix;
    std::string found_name;
    ASSERT_EQ(OB_SUCCESS,
        split_data_check_path(sorted_paths.at(0), prefix, found_name));
    FakeDataCheckObjectLister object_lister;
    object_lister.set_listing(prefix, {found_name});
    FakeDataCheckMetaRefChecker meta_ref_checker;
    meta_ref_checker.set_results({reference_exist});
    FakeDataCheckListStopChecker stop_checker;
    ObSSDataCheckListChecker checker;
    ASSERT_EQ(OB_SUCCESS, checker.init(
        OB_SYS_TENANT_ID, BUF_SIZE, 0,
        object_lister, meta_ref_checker, stop_checker, ctx));
    ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
    ObSSDataCheckListCompareStat stat;
    const int check_ret = checker.check(expected_file, expected_count, stat);
    ctx.dec_active_task_count(check_ret);
    ASSERT_EQ(OB_SUCCESS, check_ret);
    EXPECT_EQ(1, stat.found_count_);
    EXPECT_EQ(1, stat.missing_count_);
    EXPECT_EQ(reference_exist ? 1 : 0, stat.referenced_missing_count_);
    EXPECT_EQ(reference_exist ? 0 : 1, stat.unreferenced_missing_count_);
    EXPECT_EQ(1, meta_ref_checker.get_call_count());
    ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
    ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
    ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
    ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
    EXPECT_EQ(reference_exist
        ? ObSSDataCheckStatus::NOT_HEALTH
        : ObSSDataCheckStatus::HEALTH,
        status);
    expected_file.reset();
    ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  }
}

TEST_F(TestSSDataCheck, missing_outtable_object_requires_live_source_reference)
{
  const MacroBlockId macro_id = make_data_check_object_id(
      ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO, 1);
  for (const bool reference_exist : {false, true}) {
    ObSSDataCheckCtx ctx;
    ASSERT_EQ(OB_SUCCESS, ctx.init(
        OB_SYS_TENANT_ID, data_check_snapshot_scn(), ObSSDataCheckMode::SINGLE,
        MEM_LIMIT, BUF_SIZE, 0));
    ObSSDataCheckSortedFileInfo expected_file;
    int64_t expected_count = 0;
    std::vector<std::string> sorted_paths;
    ASSERT_EQ(OB_SUCCESS, build_list_check_expected_by_macro_ids(
        ctx, {macro_id}, expected_file, expected_count, sorted_paths));
    ASSERT_EQ(1, expected_count);
    ASSERT_EQ(1, sorted_paths.size());
    std::string prefix;
    std::string name;
    ASSERT_EQ(OB_SUCCESS, split_data_check_path(sorted_paths.at(0), prefix, name));
    FakeDataCheckObjectLister object_lister;
    object_lister.set_listing(prefix, {});
    FakeDataCheckMetaRefChecker meta_ref_checker;
    meta_ref_checker.set_results({reference_exist});
    FakeDataCheckListStopChecker stop_checker;
    ObSSDataCheckListChecker checker;
    ASSERT_EQ(OB_SUCCESS, checker.init(
        OB_SYS_TENANT_ID, BUF_SIZE, 0,
        object_lister, meta_ref_checker, stop_checker, ctx));
    ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
    ObSSDataCheckListCompareStat stat;
    const int check_ret = checker.check(expected_file, expected_count, stat);
    ctx.dec_active_task_count(check_ret);
    ASSERT_EQ(OB_SUCCESS, check_ret);
    EXPECT_EQ(1, stat.missing_count_);
    EXPECT_EQ(0, stat.found_count_);
    EXPECT_EQ(reference_exist ? 1 : 0, stat.referenced_missing_count_);
    EXPECT_EQ(reference_exist ? 0 : 1, stat.unreferenced_missing_count_);
    EXPECT_EQ(1, meta_ref_checker.get_call_count());
    ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
    ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
    ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
    ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
    EXPECT_EQ(reference_exist ? ObSSDataCheckStatus::NOT_HEALTH : ObSSDataCheckStatus::HEALTH,
        status);
    expected_file.reset();
    ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  }
}

TEST_F(TestSSDataCheck, list_checker_sorts_unordered_results_and_publishes_health)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected(
      ctx, {9, 10, 2}, expected_file, expected_count, sorted_paths));

  std::string prefix;
  std::vector<std::string> object_names;
  const std::vector<int64_t> unordered_sequences = {9, 10, 2};
  for (int64_t i = 0; i < unordered_sequences.size(); ++i) {
    std::string relative_path;
    ASSERT_EQ(OB_SUCCESS, build_data_check_relative_path(
        make_data_check_id(unordered_sequences.at(i)), relative_path));
    std::string current_prefix;
    std::string name;
    ASSERT_EQ(OB_SUCCESS,
        split_data_check_path(relative_path, current_prefix, name));
    if (prefix.empty()) {
      prefix = current_prefix;
    }
    ASSERT_EQ(prefix, current_prefix);
    object_names.push_back(name);
  }
  ASSERT_GT(object_names.at(0), object_names.at(1));

  FakeDataCheckObjectLister object_lister;
  object_lister.set_listing(prefix, object_names);
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({});
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  ASSERT_EQ(OB_SUCCESS, check_ret);
  EXPECT_EQ(3, stat.expected_count_);
  EXPECT_EQ(3, stat.found_count_);
  EXPECT_EQ(0, stat.missing_count_);
  EXPECT_EQ(0, stat.referenced_missing_count_);
  EXPECT_EQ(0, stat.unreferenced_missing_count_);
  EXPECT_EQ(3, stat.listed_object_count_);
  EXPECT_EQ(1, stat.listed_prefix_count_);
  EXPECT_EQ(0, meta_ref_checker.get_call_count());
  EXPECT_EQ(1, object_lister.get_call_count());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::NOT_HEALTH;
  ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::HEALTH, status);
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_supports_real_file_backend)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected(
      ctx, {9, 10, 2}, expected_file, expected_count, sorted_paths));

  ObObjectStorageInfo storage_info;
  ASSERT_EQ(OB_SUCCESS, storage_info.set(OB_STORAGE_FILE, ""));
  char tenant_root_uri[OB_MAX_URI_LENGTH] = {};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(
      tenant_root_uri, sizeof(tenant_root_uri),
      "file://%s/ss_data_check_file_backend",
      get_storage_env().data_dir_));

  std::string prefix;
  std::vector<std::string> relative_paths;
  const std::vector<int64_t> creation_order = {9, 10, 2};
  for (int64_t i = 0; i < creation_order.size(); ++i) {
    std::string relative_path;
    ASSERT_EQ(OB_SUCCESS, build_data_check_relative_path(
        make_data_check_id(creation_order.at(i)), relative_path));
    std::string current_prefix;
    std::string name;
    ASSERT_EQ(OB_SUCCESS,
        split_data_check_path(relative_path, current_prefix, name));
    if (prefix.empty()) {
      prefix = current_prefix;
    }
    ASSERT_EQ(prefix, current_prefix);
    relative_paths.push_back(relative_path);
  }

  char parent_uri[OB_MAX_URI_LENGTH] = {};
  ASSERT_EQ(OB_SUCCESS, databuff_printf(
      parent_uri, sizeof(parent_uri), "%s/%s",
      tenant_root_uri, prefix.c_str()));
  ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::mkdir(
      ObString::make_string(parent_uri), &storage_info));
  for (int64_t i = 0; i < relative_paths.size(); ++i) {
    char object_uri[OB_MAX_URI_LENGTH] = {};
    ASSERT_EQ(OB_SUCCESS, databuff_printf(
        object_uri, sizeof(object_uri), "%s/%s",
        tenant_root_uri, relative_paths.at(i).c_str()));
    ASSERT_EQ(OB_SUCCESS, ObBackupIoAdapter::write_single_file(
        ObString::make_string(object_uri), &storage_info,
        "x", 1, ObStorageIdMod::get_default_id_mod()));
  }

  ObSSDataCheckObjectLister object_lister;
  ASSERT_EQ(OB_SUCCESS, object_lister.init(
      ObString::make_string(tenant_root_uri), &storage_info));
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({});
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  ASSERT_EQ(OB_SUCCESS, check_ret);
  EXPECT_EQ(3, stat.expected_count_);
  EXPECT_EQ(3, stat.found_count_);
  EXPECT_EQ(0, stat.missing_count_);
  EXPECT_EQ(3, stat.listed_object_count_);
  EXPECT_EQ(1, stat.listed_prefix_count_);
  EXPECT_EQ(0, meta_ref_checker.get_call_count());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::NOT_HEALTH;
  ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::HEALTH, status);
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
  EXPECT_EQ(OB_SUCCESS, ObBackupIoAdapter::del_dir(
      ObString::make_string(tenant_root_uri), &storage_info, true));
}

TEST_F(TestSSDataCheck, list_sorter_compacts_runs_by_level)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListedObjectSorter sorter;
  ASSERT_EQ(OB_SUCCESS, sorter.init(
      OB_SYS_TENANT_ID, MEM_LIMIT, BUF_SIZE, 0,
      stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());

  int run_ret = OB_SUCCESS;
  std::vector<std::string> expected_names;
  for (int64_t sequence = 127;
       OB_SUCCESS == run_ret && sequence > 0;
       --sequence) {
    std::string name;
    run_ret = append_one_data_check_list_run(sorter, sequence, name);
    if (OB_SUCCESS == run_ret) {
      expected_names.push_back(name);
    }
  }

  EXPECT_EQ(OB_SUCCESS, run_ret);
  EXPECT_EQ(127, sorter.collected_count_);
  EXPECT_EQ(63, sorter.run_levels_[0].count());
  EXPECT_EQ(1, sorter.run_levels_[1].count());
  EXPECT_EQ(64, sorter.run_levels_[1].at(0).get_item_count());
  int64_t pending_item_count = 0;
  for (int64_t level_idx = 0;
       level_idx < ObSSDataCheckListedObjectSorter::MAX_COMPACT_LEVEL_COUNT;
       ++level_idx) {
    const auto &level = sorter.run_levels_[level_idx];
    for (int64_t run_idx = 0; run_idx < level.count(); ++run_idx) {
      pending_item_count += level.at(run_idx).get_item_count();
    }
  }
  EXPECT_EQ(127, pending_item_count);

  CollectDataCheckListedObjectOp collector;
  if (OB_SUCCESS == run_ret) {
    run_ret = sorter.sort_and_replay(collector);
  }
  sorter.reset();
  ctx.dec_active_task_count(run_ret);
  EXPECT_EQ(OB_SUCCESS, run_ret);
  std::sort(expected_names.begin(), expected_names.end());
  EXPECT_EQ(expected_names, collector.get_names());
  EXPECT_TRUE(std::is_sorted(
      collector.get_names().begin(), collector.get_names().end()));
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_partial_list_error_blocks_status_publication)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected(
      ctx, {1, 2}, expected_file, expected_count, sorted_paths));
  std::string prefix;
  std::string unused_name;
  ASSERT_EQ(OB_SUCCESS,
      split_data_check_path(sorted_paths.at(0), prefix, unused_name));

  FakeDataCheckObjectLister object_lister;
  object_lister.set_listing(
      prefix, {unused_name, "partial_extra"}, OB_IO_ERROR, 2);
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({});
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  EXPECT_EQ(OB_IO_ERROR, check_ret);
  EXPECT_EQ(OB_IO_ERROR, ctx.get_first_error());
  EXPECT_EQ(expected_count, stat.expected_count_);
  EXPECT_EQ(0, stat.found_count_);
  EXPECT_EQ(0, stat.missing_count_);
  EXPECT_EQ(0, stat.referenced_missing_count_);
  EXPECT_EQ(0, stat.unreferenced_missing_count_);
  EXPECT_EQ(0, stat.listed_object_count_);
  EXPECT_EQ(0, stat.listed_prefix_count_);
  EXPECT_EQ(0, meta_ref_checker.get_call_count());
  EXPECT_EQ(1, object_lister.get_call_count());
  EXPECT_EQ(OB_IO_ERROR, ctx.publish_status());
  EXPECT_FALSE(ctx.has_published_status());
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_spills_and_merges_unordered_results)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ObSSDataCheckSortedFileInfo expected_file;
  int64_t expected_count = 0;
  std::vector<std::string> sorted_paths;
  ASSERT_EQ(OB_SUCCESS, build_list_check_expected(
      ctx, {1, 2, 10}, expected_file, expected_count, sorted_paths));
  ASSERT_EQ(3, expected_count);

  std::string prefix;
  std::vector<std::string> expected_names;
  for (int64_t i = 0; i < sorted_paths.size(); ++i) {
    std::string current_prefix;
    std::string name;
    ASSERT_EQ(OB_SUCCESS,
        split_data_check_path(sorted_paths.at(i), current_prefix, name));
    if (prefix.empty()) {
      prefix = current_prefix;
    }
    ASSERT_EQ(prefix, current_prefix);
    expected_names.push_back(name);
  }

  const int64_t extra_name_length = 240;
  const int64_t sequence_width = 8;
  struct dirent sample_entry;
  ASSERT_LT(extra_name_length,
      static_cast<int64_t>(sizeof(sample_entry.d_name)));
  const int64_t charged_size = sizeof(ObSSDataCheckListedObjectItem)
      + extra_name_length
      + 2 * sizeof(ObSSDataCheckListedObjectItem *);
  const int64_t sorter_memory_limit = 8 * 1024 * 1024;
  const int64_t extra_object_count = sorter_memory_limit / charged_size + 1024;
  ASSERT_GT(extra_object_count * charged_size, sorter_memory_limit);
  std::vector<std::string> object_names;
  object_names.reserve(extra_object_count + 2);
  for (int64_t i = extra_object_count; i > 0; --i) {
    const std::string sequence = std::to_string(i);
    ASSERT_LE(static_cast<int64_t>(sequence.length()), sequence_width);
    std::string name("spill_extra_");
    name.append(sequence_width - sequence.length(), '0');
    name.append(sequence);
    name.push_back('_');
    ASSERT_LT(static_cast<int64_t>(name.length()), extra_name_length);
    name.append(extra_name_length - name.length(), 'x');
    ASSERT_EQ(extra_name_length, static_cast<int64_t>(name.length()));
    object_names.push_back(name);
  }
  object_names.push_back(expected_names.at(2));
  object_names.push_back(expected_names.at(0));
  ASSERT_GT(object_names.at(0), object_names.at(1));

  FakeDataCheckObjectLister object_lister;
  object_lister.set_listing(prefix, object_names);
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({false});
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, expected_count, stat);
  ctx.dec_active_task_count(check_ret);
  ASSERT_EQ(OB_SUCCESS, check_ret);
  EXPECT_EQ(3, stat.expected_count_);
  EXPECT_EQ(2, stat.found_count_);
  EXPECT_EQ(1, stat.missing_count_);
  EXPECT_EQ(0, stat.referenced_missing_count_);
  EXPECT_EQ(1, stat.unreferenced_missing_count_);
  EXPECT_EQ(extra_object_count + 2, stat.listed_object_count_);
  EXPECT_EQ(1, stat.listed_prefix_count_);
  EXPECT_EQ(1, meta_ref_checker.get_call_count());
  EXPECT_EQ(1, object_lister.get_call_count());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::NOT_HEALTH;
  ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::HEALTH, status);
  expected_file.reset();
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

TEST_F(TestSSDataCheck, list_checker_empty_expected_skips_list)
{
  ObSSDataCheckCtx ctx;
  ASSERT_EQ(OB_SUCCESS,
      ctx.init(OB_SYS_TENANT_ID, data_check_snapshot_scn(),
          ObSSDataCheckMode::SINGLE,
          MEM_LIMIT, BUF_SIZE, 0));
  ASSERT_EQ(OB_SUCCESS, ctx.finish_expected_collect());
  FakeDataCheckObjectLister object_lister;
  object_lister.set_listing("unused/", {});
  FakeDataCheckMetaRefChecker meta_ref_checker;
  meta_ref_checker.set_results({});
  FakeDataCheckListStopChecker stop_checker;
  ObSSDataCheckListChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(
      OB_SYS_TENANT_ID, BUF_SIZE, 0,
      object_lister, meta_ref_checker, stop_checker, ctx));
  ASSERT_EQ(OB_SUCCESS, ctx.inc_active_task_count());
  ObSSDataCheckSortedFileInfo expected_file;
  ObSSDataCheckListCompareStat stat;
  const int check_ret = checker.check(expected_file, 0, stat);
  ctx.dec_active_task_count(check_ret);
  ASSERT_EQ(OB_SUCCESS, check_ret);
  EXPECT_TRUE(stat.is_valid());
  EXPECT_EQ(0, object_lister.get_call_count());
  EXPECT_EQ(0, meta_ref_checker.get_call_count());
  ASSERT_EQ(OB_SUCCESS, ctx.finish_list_check());
  ASSERT_EQ(OB_SUCCESS, ctx.publish_status());
  ObSSDataCheckStatus status = ObSSDataCheckStatus::NOT_HEALTH;
  ASSERT_EQ(OB_SUCCESS, ctx.get_status(status));
  EXPECT_EQ(ObSSDataCheckStatus::HEALTH, status);
  ASSERT_EQ(OB_SUCCESS, ctx.cleanup());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_data_check.log*");
  OB_LOGGER.set_file_name("test_ss_data_check.log", true, true);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
