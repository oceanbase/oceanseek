/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

// 场景：验证共享存储块回收处理与私有宏块检查。

#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#define USING_LOG_PREFIX SERVER
#define protected public
#define private public
#include "src/storage/shared_storage/ob_private_block_gc_task.h"
#include "storage/ls/ob_ls.h"
namespace oceanbase
{
using namespace common;

namespace unittest
{

class TestBlockGCHandler : public ::testing::Test,
                           public storage::ObBlockGCHandler
{
public:
  TestBlockGCHandler() 
  : storage::ObBlockGCHandler(
      ObTabletID(1),
      is_set_stop_),
      is_set_stop_(false)
  {}
  virtual ~TestBlockGCHandler() {}
  
  virtual int list_tablet_meta_version(
    ObIArray<int64_t> &tablet_versions)
  {
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < 3; i++) {
      ret = tablet_versions.push_back(i);
    }
    return ret;
  }
  virtual int get_blocks_for_tablet(
    int64_t tablet_meta_version,
    const bool is_shared,
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    UNUSED(tablet_meta_version);
    UNUSED(is_shared);
    int ret = OB_SUCCESS;
    for (int i = 0; OB_SUCC(ret) && i < 3; i++) {
      ret = block_ids.push_back(macro_block_ids_[tablet_meta_version][i]);
    }
    return ret;
  }
  virtual int delete_tablet_meta_version(
    int64_t tablet_meta_version)
  {
    return OB_SUCCESS;
  }
  virtual int try_delete_tablet_meta_dir()
  {
    return OB_SUCCESS;
  }

  virtual int delete_macro_blocks(
      ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    gc_blocks_.push_back(block_ids);
    return OB_SUCCESS;
  }

  virtual int delete_macro_cache_(const ObIArray<blocksstable::MacroBlockId> &block_ids) override
  {
    return OB_SUCCESS;
  }
  void reset()
  {
    macro_block_ids_ = NULL;
    gc_blocks_.reset();
  }

  int64_t to_string(
      char* buf, 
      const int64_t buf_len) const
  { return OB_SUCCESS; }

  int get_block_ids_from_dir(
      ObIArray<blocksstable::MacroBlockId> &block_ids)
  { return OB_SUCCESS; }

  int try_delete_tablet_data_dir()
  { return OB_SUCCESS; }

  int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &collect_check_block_op)
  { return OB_SUCCESS; }

  blocksstable::MacroBlockId (*macro_block_ids_)[3];
  ObArray<blocksstable::MacroBlockId> gc_blocks_;
  bool is_set_stop_;
};

class TestPrivateBlockGCHandler : public ::testing::Test,
                                  public storage::ObPrivateBlockGCHandler
{
public:
  TestPrivateBlockGCHandler() 
  : storage::ObPrivateBlockGCHandler(share::ObLSID(1), 1, ObTabletID(1), 1, -1, 1, 10, 20, 0, is_set_stop_),
    is_set_stop_(false)
  {
    for (int i = 0; i < 30; i++) {
      macro_block_ids_[i].set_id_mode(
          static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
      macro_block_ids_[i].set_storage_object_type(
          static_cast<uint64_t>(blocksstable::ObStorageObjectType::PRIVATE_DATA_MACRO));
      macro_block_ids_[i].set_second_id(1);
      macro_block_ids_[i].set_reorganization_scn(1);
      macro_block_ids_[i].set_op_id(0);
      macro_block_ids_[i].set_tenant_seq(i);
    }
  }
  virtual ~TestPrivateBlockGCHandler() {}

  TO_STRING_KV(K(macro_block_ids_[0].tenant_seq_), K(macro_block_ids_[11].tenant_seq_), K(macro_block_ids_[12].tenant_seq_), K(macro_block_ids_[21].tenant_seq_));

  int get_blocks_for_tablet(
      int64_t tablet_meta_version,
      const bool is_shared,
      ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    UNUSED(tablet_meta_version);
    UNUSED(is_shared);
    for (int i = 11; i < 20; i++) {
      if (0 == i % 2) {
        block_ids.push_back(macro_block_ids_[i]);
      }
    }
    return OB_SUCCESS;
  }
  int list_tablet_meta_version(
      ObIArray<int64_t> &tablet_versions,
      const bool,
      int64_t &)
  {
    tablet_versions.push_back(1);
    return OB_SUCCESS;
  }

  int list_tablet_meta_version(
      ObIArray<int64_t> &tablet_versions)
  {
    tablet_versions.push_back(1);
    return OB_SUCCESS;
  }

  int get_block_ids_from_dir(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    for (int i = 0; i < 30; i++) {
      if (10 != i && 20 != i) {
        block_ids.push_back(macro_block_ids_[i]);
      }
    }
    return OB_SUCCESS;
  }
  int delete_macro_blocks(
        ObIArray<blocksstable::MacroBlockId> &block_ids) 
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; i < block_ids.count(); i++) {
      macro_block_ids_[block_ids.at(i).tenant_seq_].third_id_ = 2;
    }
    return ret;
  }
  virtual int delete_macro_cache_(const ObIArray<blocksstable::MacroBlockId> &block_ids) override
  {
    return OB_SUCCESS;
  }

  int update_tablet_last_gc_version_(const int64_t) {
    return OB_SUCCESS;
  }

  int safe_delete_blocks_for_macro_check_(
        ObIArray<blocksstable::MacroBlockId> &block_ids) override
  {
    return delete_macro_blocks(block_ids);
  }
  
  blocksstable::MacroBlockId macro_block_ids_[30];
  bool is_set_stop_;
};

TEST_F(TestPrivateBlockGCHandler, test_macro_check)
{
  LOG_INFO("test detect_and_gc start", KPC(this));
  ASSERT_EQ(OB_SUCCESS, macro_block_check());
  for (int i = 11; i < 20; i++) {
    if (0 == i % 2) {
      ASSERT_EQ(1, macro_block_ids_[i].third_id_);
    } else {
      ASSERT_EQ(2, macro_block_ids_[i].third_id_);
    }
  }
  for (int i = 0; i < 30; i++) {
    if (i < 10 || i >= 20) {
      ASSERT_EQ(1, macro_block_ids_[i].third_id_);
    }
  }
  LOG_INFO("test test_macro_check finish", KPC(this));
}


static blocksstable::MacroBlockId make_private_gc_test_id(
    const uint64_t tablet_id, const uint64_t sequence)
{
  blocksstable::MacroBlockId id;
  id.set_id_mode(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
  id.set_storage_object_type(
      static_cast<uint64_t>(blocksstable::ObStorageObjectType::PRIVATE_DATA_MACRO));
  id.set_second_id(tablet_id);
  id.set_reorganization_scn(1);
  id.set_op_id(0);
  id.set_tenant_seq(sequence);
  return id;
}

class RecordingPrivateGCBatcher : public storage::ObPrivateGCBatcher
{
public:
  explicit RecordingPrivateGCBatcher(const bool &stop)
    : ObPrivateGCBatcher(stop),
      fail_on_batch_(0),
      read_in_progress_(nullptr),
      stop_after_delete_(nullptr)
  {}

  int64_t fail_on_batch_;
  const bool *read_in_progress_;
  bool *stop_after_delete_;
  ObArray<int64_t> batch_sizes_;
  ObArray<blocksstable::MacroBlockId> deleted_ids_;

protected:
  int delete_macro_blocks_(ObIArray<blocksstable::MacroBlockId> &block_ids) override
  {
    std::lock_guard<std::mutex> guard(records_lock_);
    int ret = OB_SUCCESS;
    if (nullptr != read_in_progress_ && *read_in_progress_) {
      ADD_FAILURE() << "a version must finish reading before its IDs can be submitted";
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(batch_sizes_.push_back(block_ids.count()))) {
    } else if (fail_on_batch_ == batch_sizes_.count()) {
      ret = OB_IO_ERROR;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < block_ids.count(); ++i) {
        ret = deleted_ids_.push_back(block_ids.at(i));
      }
      if (OB_SUCC(ret) && nullptr != stop_after_delete_) {
        *stop_after_delete_ = true;
      }
    }
    return ret;
  }
private:
  std::mutex records_lock_;
};

class BlockingPrivateGCBatcher : public RecordingPrivateGCBatcher
{
public:
  explicit BlockingPrivateGCBatcher(const bool &stop)
    : RecordingPrivateGCBatcher(stop), first_delete_started_(false), release_first_delete_(false)
  {}

  bool wait_for_first_delete()
  {
    std::unique_lock<std::mutex> guard(gate_lock_);
    return gate_.wait_for(guard, std::chrono::seconds(5),
        [this]() { return first_delete_started_; });
  }

  void release_first_delete()
  {
    std::lock_guard<std::mutex> guard(gate_lock_);
    release_first_delete_ = true;
    gate_.notify_all();
  }

protected:
  int delete_macro_blocks_(ObIArray<blocksstable::MacroBlockId> &block_ids) override
  {
    {
      std::unique_lock<std::mutex> guard(gate_lock_);
      if (!first_delete_started_) {
        first_delete_started_ = true;
        gate_.notify_all();
        gate_.wait(guard, [this]() { return release_first_delete_; });
      }
    }
    return RecordingPrivateGCBatcher::delete_macro_blocks_(block_ids);
  }

private:
  std::mutex gate_lock_;
  std::condition_variable gate_;
  bool first_delete_started_;
  bool release_first_delete_;
};

TEST_F(TestBlockGCHandler, test_block_gc)
{
  ObArray<int64_t> tablet_versions;
  blocksstable::MacroBlockId test_array[3][3] =
  {
    { make_private_gc_test_id(1, 1),
      make_private_gc_test_id(1, 2),
      make_private_gc_test_id(1, 3) },
    { make_private_gc_test_id(1, 4),
      make_private_gc_test_id(1, 5),
      make_private_gc_test_id(1, 3) },
    { make_private_gc_test_id(1, 6),
      make_private_gc_test_id(1, 4),
      make_private_gc_test_id(1, 3) },
  };

  macro_block_ids_ = test_array;
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(OB_SUCCESS, tablet_versions.push_back(i));
  }
  RecordingPrivateGCBatcher batcher(is_set_stop_);
  const int64_t min_retain_tablet_meta_version = 2;
  ASSERT_EQ(OB_SUCCESS, gc_blocks_for_tablet_meta_versions(
      tablet_versions, min_retain_tablet_meta_version, false /* is_transfer_out_deleted */, batcher));
  EXPECT_EQ(0, batcher.deleted_ids_.count());
  ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
  ASSERT_EQ(3, batcher.deleted_ids_.count());
  for (int i = 0; i < batcher.deleted_ids_.count(); i++) {
    const uint64_t sequence = batcher.deleted_ids_.at(i).tenant_seq();
    EXPECT_TRUE(1 == sequence || 2 == sequence || 5 == sequence);
  }
  ASSERT_EQ(OB_SUCCESS, delete_tablet_meta_versions_(tablet_versions, min_retain_tablet_meta_version - 1));

  reset();
  macro_block_ids_ = test_array;
  ASSERT_EQ(OB_SUCCESS, gc_tablet(tablet_versions, GCTabletType::DropTablet));
  ASSERT_EQ(6, gc_blocks_.count());
  for (int i = 0; i < gc_blocks_.count(); i++) {
    const uint64_t sequence = gc_blocks_.at(i).tenant_seq();
    EXPECT_TRUE(1 <= sequence && sequence <= 6);
  }
}

class MockPrivateVersionGCHandler : public storage::ObPrivateBlockGCHandler
{
public:
  MockPrivateVersionGCHandler(
      const uint64_t tablet_id, const bool &stop, const int64_t retain_version = 2)
    : ObPrivateBlockGCHandler(share::ObLSID(1), 1, ObTabletID(tablet_id),
          retain_version, -1, 1, 0, stop, false),
      version_count_(retain_version + 1),
      read_in_progress_(false),
      fail_read_version_(-1),
      fail_delete_version_(-1),
      stop_after_last_meta_delete_(nullptr),
      waterline_updates_(0),
      last_updated_version_(-1)
  {}

  int add_blocks(const int64_t version, const uint64_t first, const int64_t count)
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      ret = version_blocks_[version].push_back(
          make_private_gc_test_id(tablet_id_.id(), first + i));
    }
    return ret;
  }

  int list_tablet_meta_version(
      ObIArray<int64_t> &versions, const bool, int64_t &gc_end_version) override
  {
    int ret = OB_SUCCESS;
    versions.reset();
    gc_end_version = max_tablet_meta_version_ - 1;
    for (int64_t i = 0; OB_SUCC(ret) && i < version_count_
        && i <= max_tablet_meta_version_; ++i) {
      ret = versions.push_back(i);
    }
    return ret;
  }

  int get_blocks_for_tablet(
      const int64_t version, const bool is_shared,
      ObIArray<blocksstable::MacroBlockId> &ids) override
  {
    int ret = OB_SUCCESS;
    ids.reset();
    if (is_shared) {
    } else if (version < 0 || version > max_tablet_meta_version_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      read_in_progress_ = true;
      for (int64_t i = 0; OB_SUCC(ret) && i < version_blocks_[version].count(); ++i) {
        ret = ids.push_back(version_blocks_[version].at(i));
      }
      read_in_progress_ = false;
      if (OB_SUCC(ret) && fail_read_version_ == version) {
        ret = OB_IO_ERROR; // preserve partial output from a failed version read
      }
    }
    return ret;
  }

  int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &, const int64_t, storage::BlockCollectOP &) override
  {
    return OB_SUCCESS;
  }

  int delete_tablet_meta_version(const int64_t version) override
  {
    int ret = deleted_versions_.push_back(version);
    if (OB_SUCC(ret)) {
      if (fail_delete_version_ == version) {
        ret = OB_IO_ERROR;
      } else if (0 == version && nullptr != stop_after_last_meta_delete_) {
        *stop_after_last_meta_delete_ = true;
      }
    }
    return ret;
  }

  int delete_macro_cache_(const ObIArray<blocksstable::MacroBlockId> &) override
  {
    return OB_SUCCESS;
  }

  int update_tablet_last_gc_version_(const int64_t version) override
  {
    ++waterline_updates_;
    last_updated_version_ = version;
    return OB_SUCCESS;
  }

  int64_t version_count_;
  bool read_in_progress_;
  int64_t fail_read_version_;
  int64_t fail_delete_version_;
  bool *stop_after_last_meta_delete_;
  int64_t waterline_updates_;
  int64_t last_updated_version_;
  ObArray<blocksstable::MacroBlockId> version_blocks_[11];
  ObArray<int64_t> deleted_versions_;
};

struct PrivateGCHandlerResult
{
  int64_t metadata_delete_count_ = 0;
  int64_t waterline_updates_ = 0;
  bool destroyed_ = false;
};

class ObservedPrivateVersionGCHandler : public MockPrivateVersionGCHandler
{
public:
  ObservedPrivateVersionGCHandler(
      const uint64_t tablet_id, const bool &stop, PrivateGCHandlerResult &result)
    : MockPrivateVersionGCHandler(tablet_id, stop), result_(result)
  {}
  ~ObservedPrivateVersionGCHandler() override
  {
    result_.metadata_delete_count_ = deleted_versions_.count();
    result_.waterline_updates_ = waterline_updates_;
    result_.destroyed_ = true;
  }

private:
  PrivateGCHandlerResult &result_;
};

class TestPrivateGCCoordinator : public ::testing::Test
{
public:
  TestPrivateGCCoordinator()
    : ls_(new storage::ObLS()), coordinator_(*ls_)
  {}

  void TearDown() override
  {
    using storage::ObPrivateBlockGCHandler;
    for (int64_t i = 0; i < pending_handlers_.count(); ++i) {
      ObPrivateBlockGCHandler *handler = pending_handlers_.at(i);
      OB_DELETEx(ObPrivateBlockGCHandler, &allocator_, handler);
    }
    pending_handlers_.reuse();
  }

  ObservedPrivateVersionGCHandler *add_handler(const int64_t result_idx)
  {
    void *buf = allocator_.alloc(sizeof(ObservedPrivateVersionGCHandler));
    ObservedPrivateVersionGCHandler *handler = nullptr;
    if (nullptr != buf) {
      handler = new (buf) ObservedPrivateVersionGCHandler(
          1001 + result_idx, coordinator_.is_set_stop_, results_[result_idx]);
      if (OB_SUCCESS != pending_handlers_.push_back(handler)) {
        OB_DELETEx(ObservedPrivateVersionGCHandler, &allocator_, handler);
      }
    }
    return handler;
  }

  void run_worker(storage::ObPrivateBlockGCHandler *handler)
  {
    // Run the real worker body synchronously without starting a tenant thread pool.
    ATOMIC_INC(&gc_thread_.unfinished_task_cnt_);
    gc_thread_.handle(handler);
    EXPECT_EQ(0, gc_thread_.unfinished_task_cnt_);
  }

  PrivateGCHandlerResult results_[3];
  std::unique_ptr<storage::ObLS> ls_;
  storage::ObLSPrivateBlockGCHandler coordinator_;
  storage::ObPrivateBlockGCThread gc_thread_;
  ObArenaAllocator allocator_;
  ObArray<storage::ObPrivateBlockGCHandler *> pending_handlers_;
};

// A worker read error or a tail-delete error retries the failed group without deleting
// its metadata. A fresh group can complete, and the original error is not lost on requeue.
TEST_F(TestPrivateGCCoordinator, failed_group_requeues_all_and_fresh_group_can_finish)
{
  for (const bool fail_during_read : {true, false}) {
    SCOPED_TRACE(fail_during_read);
    for (auto &result : results_) {
      result = PrivateGCHandlerResult();
    }
    {
      RecordingPrivateGCBatcher batcher(coordinator_.is_set_stop_);
      storage::ObPrivateBlockGCThreadGuard guard(
          gc_thread_, storage::ProcessType::GCTabletMetaVersion, allocator_, &batcher);
      auto *first = add_handler(0);
      auto *second = add_handler(1);
      ASSERT_NE(nullptr, first);
      ASSERT_NE(nullptr, second);
      ASSERT_EQ(OB_SUCCESS, first->add_blocks(0, 1, 1));
      ASSERT_EQ(OB_SUCCESS, second->add_blocks(0, 1, 1));
      if (fail_during_read) {
        second->fail_read_version_ = 0;
      } else {
        batcher.fail_on_batch_ = 1;
      }
      run_worker(first);
      run_worker(second);
      EXPECT_EQ(fail_during_read ? OB_IO_ERROR : OB_SUCCESS, batcher.get_error());
      EXPECT_EQ(OB_IO_ERROR, coordinator_.process_pending_handlers_(
          guard, batcher, pending_handlers_, allocator_, true));
      EXPECT_EQ(0, pending_handlers_.count());
      EXPECT_EQ(0, batcher.get_gc_macro_block_cnt());
      EXPECT_EQ(OB_IO_ERROR, batcher.get_error());
      for (int64_t i = 0; i < 2; ++i) {
        EXPECT_TRUE(results_[i].destroyed_);
        EXPECT_EQ(0, results_[i].metadata_delete_count_);
        EXPECT_EQ(0, results_[i].waterline_updates_);
      }
    }
    EXPECT_EQ(nullptr, gc_thread_.get_gc_task_ctx());

    {
      RecordingPrivateGCBatcher batcher(coordinator_.is_set_stop_);
      storage::ObPrivateBlockGCThreadGuard guard(
          gc_thread_, storage::ProcessType::GCTabletMetaVersion, allocator_, &batcher);
      auto *next = add_handler(2);
      ASSERT_NE(nullptr, next);
      ASSERT_EQ(OB_SUCCESS, next->add_blocks(0, 1, 1));
      run_worker(next);
      EXPECT_EQ(OB_SUCCESS, coordinator_.process_pending_handlers_(
          guard, batcher, pending_handlers_, allocator_, true));
      EXPECT_EQ(0, pending_handlers_.count());
      EXPECT_EQ(1, batcher.get_gc_macro_block_cnt());
      EXPECT_EQ(1, batcher.get_batch_count());
      EXPECT_TRUE(results_[2].destroyed_);
      EXPECT_EQ(2, results_[2].metadata_delete_count_);
      EXPECT_EQ(1, results_[2].waterline_updates_);
    }
    EXPECT_EQ(nullptr, gc_thread_.get_gc_task_ctx());
    const ObIArray<storage::ObTabletGCInfo> &retry = coordinator_.drain_tablet_infos_for_gc_();
    ASSERT_EQ(2, retry.count());
    EXPECT_EQ(ObTabletID(1001), retry.at(0).tablet_id_);
    EXPECT_EQ(ObTabletID(1002), retry.at(1).tablet_id_);
  }
}

// Finalization errors are tablet-local after all shared blocks have been deleted.
TEST_F(TestPrivateGCCoordinator, metadata_failure_does_not_block_other_tablets)
{
  RecordingPrivateGCBatcher batcher(coordinator_.is_set_stop_);
  storage::ObPrivateBlockGCThreadGuard guard(
      gc_thread_, storage::ProcessType::GCTabletMetaVersion, allocator_, &batcher);
  for (int64_t i = 0; i < 3; ++i) {
    auto *handler = add_handler(i);
    ASSERT_NE(nullptr, handler);
    ASSERT_EQ(OB_SUCCESS, handler->add_blocks(0, 1, 1));
    if (0 == i || 2 == i) {
      handler->fail_delete_version_ = 1;
    }
    run_worker(handler);
  }
  EXPECT_EQ(OB_IO_ERROR, coordinator_.process_pending_handlers_(
      guard, batcher, pending_handlers_, allocator_, true));
  EXPECT_EQ(0, pending_handlers_.count());
  EXPECT_EQ(OB_SUCCESS, batcher.get_error());
  EXPECT_EQ(3, batcher.get_gc_macro_block_cnt());
  EXPECT_EQ(1, batcher.get_batch_count());
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_TRUE(results_[i].destroyed_);
    EXPECT_EQ(1 == i ? 2 : 1, results_[i].metadata_delete_count_);
    EXPECT_EQ(1 == i ? 1 : 0, results_[i].waterline_updates_);
  }
  const ObIArray<storage::ObTabletGCInfo> &retry = coordinator_.drain_tablet_infos_for_gc_();
  ASSERT_EQ(2, retry.count());
  EXPECT_EQ(ObTabletID(1001), retry.at(0).tablet_id_);
  EXPECT_EQ(ObTabletID(1003), retry.at(1).tablet_id_);
}

TEST(PrivateGCBatcher, batch_boundaries_and_no_empty_requests)
{
  const int64_t counts[] = {0, 1, 999, 1000, 1001, 1500, 2001};
  for (const int64_t count : counts) {
    SCOPED_TRACE(count);
    bool stop = false;
    RecordingPrivateGCBatcher batcher(stop);
    ObArray<blocksstable::MacroBlockId> block_ids;
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, block_ids.push_back(make_private_gc_test_id(1001, i + 1)));
    }
    ASSERT_EQ(OB_SUCCESS, batcher.append(block_ids));
    ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
    const int64_t expected_batches = (count + 999) / 1000;
    EXPECT_EQ(expected_batches, batcher.get_batch_count());
    EXPECT_EQ(expected_batches, batcher.batch_sizes_.count());
    EXPECT_EQ(count, batcher.get_gc_macro_block_cnt());
    ASSERT_EQ(count, batcher.deleted_ids_.count());
    for (int64_t i = 0; i < expected_batches; ++i) {
      EXPECT_EQ(MIN(1000L, count - i * 1000), batcher.batch_sizes_.at(i));
    }
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(static_cast<uint64_t>(i + 1), batcher.deleted_ids_.at(i).tenant_seq());
    }
  }
}

TEST(PrivateGCBatcher, concurrent_append_shares_full_batches)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  const int64_t thread_count = 4;
  const int64_t blocks_per_thread = 1500;
  std::atomic<int> first_error(OB_SUCCESS);
  std::vector<std::thread> workers;
  for (int64_t i = 0; i < thread_count; ++i) {
    workers.emplace_back([&, i]() {
      int ret = OB_SUCCESS;
      ObArray<blocksstable::MacroBlockId> block_ids;
      for (int64_t j = 0; OB_SUCC(ret) && j < blocks_per_thread; ++j) {
        ret = block_ids.push_back(make_private_gc_test_id(1001, i * blocks_per_thread + j + 1));
      }
      if (OB_SUCC(ret)) {
        ret = batcher.append(block_ids);
      }
      if (OB_SUCCESS != ret) {
        int expected = OB_SUCCESS;
        first_error.compare_exchange_strong(expected, ret);
      }
    });
  }
  for (auto &worker : workers) {
    worker.join();
  }
  ASSERT_EQ(OB_SUCCESS, first_error.load());
  ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
  EXPECT_EQ(6000, batcher.get_gc_macro_block_cnt());
  EXPECT_EQ(6, batcher.get_batch_count());
  ASSERT_EQ(6, batcher.batch_sizes_.count());
  for (int64_t i = 0; i < batcher.batch_sizes_.count(); ++i) {
    EXPECT_EQ(1000, batcher.batch_sizes_.at(i));
  }
  std::vector<uint64_t> deleted_sequences;
  for (int64_t i = 0; i < batcher.deleted_ids_.count(); ++i) {
    deleted_sequences.push_back(batcher.deleted_ids_.at(i).tenant_seq());
  }
  std::sort(deleted_sequences.begin(), deleted_sequences.end());
  ASSERT_EQ(6000, deleted_sequences.size());
  for (int64_t i = 0; i < 6000; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 1), deleted_sequences[i]);
  }
}

// Pause A's full-batch delete; B must finish get_error() and append() before A resumes.
// Cover both successful deletion (1000 + 1000 + 1 IDs) and B failing before A succeeds.
// On B's failure, A returns success without clearing the shared error; the caller skips the tail.
// Verify successful batch and block counts, and IDs to catch lost or duplicate deletes.
TEST(PrivateGCBatcher, full_batch_io_allows_other_workers_and_preserves_first_error)
{
  for (const bool fail_second_delete : {false, true}) {
    SCOPED_TRACE(fail_second_delete);
    bool stop = false;
    BlockingPrivateGCBatcher batcher(stop);
    // The first delete is paused, so the second delete reaches the recorder first.
    batcher.fail_on_batch_ = fail_second_delete ? 1 : 0;
    ObArray<blocksstable::MacroBlockId> first_ids;
    ObArray<blocksstable::MacroBlockId> second_ids;
    for (int64_t i = 1; i <= 1000; ++i) {
      ASSERT_EQ(OB_SUCCESS, first_ids.push_back(make_private_gc_test_id(1001, i)));
    }
    for (int64_t i = 1001; i <= 2001; ++i) {
      ASSERT_EQ(OB_SUCCESS, second_ids.push_back(make_private_gc_test_id(1001, i)));
    }

    int first_ret = OB_SUCCESS;
    std::thread first_worker([&]() { first_ret = batcher.append(first_ids); });
    EXPECT_TRUE(batcher.wait_for_first_delete());
    std::promise<int> second_result;
    auto second_future = second_result.get_future();
    std::thread second_worker([&]() {
      int ret = batcher.get_error();
      if (OB_SUCC(ret)) {
        ret = batcher.append(second_ids);
      }
      second_result.set_value(ret);
    });
    const bool second_finished = (std::future_status::ready
        == second_future.wait_for(std::chrono::seconds(5)));
    // Always unblock and join both threads, including when the assertion will fail.
    batcher.release_first_delete();
    first_worker.join();
    second_worker.join();
    EXPECT_TRUE(second_finished);

    const int expected_ret = fail_second_delete ? OB_IO_ERROR : OB_SUCCESS;
    // This worker returns its own success; the shared first error remains recorded.
    EXPECT_EQ(OB_SUCCESS, first_ret);
    EXPECT_EQ(expected_ret, second_future.get());
    EXPECT_EQ(expected_ret, batcher.get_error());
    EXPECT_EQ(fail_second_delete ? 1 : 2, batcher.get_batch_count());
    // The coordinator checks the shared error after all workers have finished.
    int tail_ret = batcher.get_error();
    if (OB_SUCCESS == tail_ret) {
      tail_ret = batcher.batch_delete_remaining_macro_blocks();
    }
    EXPECT_EQ(expected_ret, tail_ret);
    EXPECT_EQ(fail_second_delete ? 1 : 3, batcher.get_batch_count());
    const int64_t expected_deleted = fail_second_delete ? 1000 : 2001;
    EXPECT_EQ(expected_deleted, batcher.get_gc_macro_block_cnt());
    std::vector<uint64_t> deleted_sequences;
    for (int64_t i = 0; i < batcher.deleted_ids_.count(); ++i) {
      deleted_sequences.push_back(batcher.deleted_ids_.at(i).tenant_seq());
    }
    std::sort(deleted_sequences.begin(), deleted_sequences.end());
    ASSERT_EQ(expected_deleted, deleted_sequences.size());
    for (int64_t i = 0; i < expected_deleted; ++i) {
      EXPECT_EQ(static_cast<uint64_t>(i + 1), deleted_sequences[i]);
    }
  }
}

// Pause deletion of a one-ID tail; another thread must finish get_error() before it resumes.
// This verifies that tail I/O does not hold the batcher state lock.
// Only the state query runs concurrently; all appends finish before tail deletion starts.
// After resuming, verify one deleted ID and one batch.
TEST(PrivateGCBatcher, tail_batch_io_does_not_hold_state_lock)
{
  bool stop = false;
  BlockingPrivateGCBatcher batcher(stop);
  ObArray<blocksstable::MacroBlockId> block_ids;
  ASSERT_EQ(OB_SUCCESS, block_ids.push_back(make_private_gc_test_id(1001, 1)));
  ASSERT_EQ(OB_SUCCESS, batcher.append(block_ids));
  int delete_ret = OB_SUCCESS;
  std::thread delete_worker([&]() { delete_ret = batcher.batch_delete_remaining_macro_blocks(); });
  EXPECT_TRUE(batcher.wait_for_first_delete());
  std::promise<int> state_result;
  auto state_future = state_result.get_future();
  std::thread state_reader([&]() { state_result.set_value(batcher.get_error()); });
  const bool state_read_finished = (std::future_status::ready
      == state_future.wait_for(std::chrono::seconds(5)));
  batcher.release_first_delete();
  delete_worker.join();
  state_reader.join();
  EXPECT_TRUE(state_read_finished);
  EXPECT_EQ(OB_SUCCESS, state_future.get());
  EXPECT_EQ(OB_SUCCESS, delete_ret);
  EXPECT_EQ(1, batcher.get_gc_macro_block_cnt());
  EXPECT_EQ(1, batcher.get_batch_count());
}

TEST(PrivateGCBatcher, delete_failure_blocks_later_append_and_preserves_first_error)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  batcher.fail_on_batch_ = 1;
  ObArray<blocksstable::MacroBlockId> block_ids;
  for (int64_t i = 0; i < 1500; ++i) {
    ASSERT_EQ(OB_SUCCESS, block_ids.push_back(make_private_gc_test_id(1001, i + 1)));
  }
  // A failed full batch stops before appending the remaining 500 IDs.
  ASSERT_EQ(OB_IO_ERROR, batcher.append(block_ids));
  block_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, block_ids.push_back(make_private_gc_test_id(1002, 1)));
  EXPECT_EQ(OB_IO_ERROR, batcher.append(block_ids));
  EXPECT_EQ(OB_IO_ERROR, batcher.get_error());
  batcher.set_error(OB_SERVICE_STOPPED);
  batcher.reuse_block_ids();
  EXPECT_EQ(OB_IO_ERROR, batcher.get_error());
  EXPECT_EQ(0, batcher.get_batch_count());
  EXPECT_EQ(0, batcher.get_gc_macro_block_cnt());
}

TEST(PrivateGCBatcher, worker_failure_prevents_tail_submission)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  ObArray<blocksstable::MacroBlockId> block_ids;
  ASSERT_EQ(OB_SUCCESS, block_ids.push_back(make_private_gc_test_id(1001, 1)));
  ASSERT_EQ(OB_SUCCESS, batcher.append(block_ids));
  batcher.set_error(OB_IO_ERROR);
  batcher.set_error(OB_SUCCESS);
  block_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, block_ids.push_back(make_private_gc_test_id(1002, 1)));
  EXPECT_EQ(OB_IO_ERROR, batcher.append(block_ids));
  // Match the coordinator: an existing worker error prevents tail submission.
  int tail_ret = batcher.get_error();
  if (OB_SUCCESS == tail_ret) {
    tail_ret = batcher.batch_delete_remaining_macro_blocks();
  }
  EXPECT_EQ(OB_IO_ERROR, tail_ret);
  batcher.reuse_block_ids();
  EXPECT_EQ(OB_IO_ERROR, batcher.get_error());
  EXPECT_EQ(0, batcher.get_batch_count());
  EXPECT_EQ(0, batcher.get_gc_macro_block_cnt());
}

TEST(PrivateVersionGCBatch, shares_tail_across_versions_and_tablets)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  MockPrivateVersionGCHandler first(1001, stop);
  MockPrivateVersionGCHandler second(1002, stop);
  ASSERT_EQ(OB_SUCCESS, first.add_blocks(0, 1, 600));
  ASSERT_EQ(OB_SUCCESS, first.add_blocks(0, 1, 1)); // duplicate in the same version
  ASSERT_EQ(OB_SUCCESS, first.add_blocks(1, 301, 600)); // reused across versions
  ASSERT_EQ(OB_SUCCESS, first.add_blocks(2, 100, 1)); // retained reference
  ASSERT_EQ(OB_SUCCESS, second.add_blocks(0, 1, 600));
  batcher.read_in_progress_ = &first.read_in_progress_;
  ASSERT_EQ(OB_SUCCESS, first.gc_blocks_for_tablet_meta_versions(batcher));
  EXPECT_EQ(0, batcher.get_batch_count());
  EXPECT_EQ(0, first.deleted_versions_.count());
  batcher.read_in_progress_ = &second.read_in_progress_;
  ASSERT_EQ(OB_SUCCESS, second.gc_blocks_for_tablet_meta_versions(batcher));
  ASSERT_EQ(1, batcher.batch_sizes_.count());
  EXPECT_EQ(1000, batcher.batch_sizes_.at(0));
  EXPECT_EQ(0, second.deleted_versions_.count());
  ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
  ASSERT_EQ(2, batcher.batch_sizes_.count());
  EXPECT_EQ(499, batcher.batch_sizes_.at(1));
  EXPECT_EQ(1499, batcher.get_gc_macro_block_cnt());
  int64_t first_count = 0;
  int64_t second_count = 0;
  for (int64_t i = 0; i < batcher.deleted_ids_.count(); ++i) {
    const blocksstable::MacroBlockId &id = batcher.deleted_ids_.at(i);
    if (1001 == id.second_id()) {
      ++first_count;
      EXPECT_NE(100, id.tenant_seq());
    } else if (1002 == id.second_id()) {
      ++second_count;
    } else {
      ADD_FAILURE() << "unexpected tablet";
    }
  }
  EXPECT_EQ(899, first_count);
  EXPECT_EQ(600, second_count);
  ASSERT_EQ(OB_SUCCESS, first.delete_tablet_meta_versions());
  ASSERT_EQ(OB_SUCCESS, second.delete_tablet_meta_versions());
  ASSERT_EQ(2, first.deleted_versions_.count());
  EXPECT_EQ(1, first.deleted_versions_.at(0));
  EXPECT_EQ(0, first.deleted_versions_.at(1));
  EXPECT_EQ(1, first.waterline_updates_);
  EXPECT_EQ(1, first.last_updated_version_);
}

TEST(PrivateVersionGCBatch, ten_old_versions_use_six_full_batches)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  MockPrivateVersionGCHandler handler(1001, stop, 10);
  for (int64_t version = 0; version < 10; ++version) {
    ASSERT_EQ(OB_SUCCESS, handler.add_blocks(version, version * 600 + 1, 600));
  }
  batcher.read_in_progress_ = &handler.read_in_progress_;
  ASSERT_EQ(OB_SUCCESS, handler.gc_blocks_for_tablet_meta_versions(batcher));
  EXPECT_TRUE(handler.need_gc_tablet_meta_versions());
  EXPECT_EQ(6, batcher.get_batch_count());
  EXPECT_EQ(6000, batcher.get_gc_macro_block_cnt());
  EXPECT_EQ(0, handler.deleted_versions_.count());
  ASSERT_EQ(6, batcher.batch_sizes_.count());
  for (int64_t i = 0; i < batcher.batch_sizes_.count(); ++i) {
    EXPECT_EQ(1000, batcher.batch_sizes_.at(i));
  }
  ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
  EXPECT_EQ(6, batcher.get_batch_count()); // no additional tail request
  ASSERT_EQ(OB_SUCCESS, handler.delete_tablet_meta_versions());
  ASSERT_EQ(10, handler.deleted_versions_.count());
  for (int64_t i = 0; i < handler.deleted_versions_.count(); ++i) {
    EXPECT_EQ(9 - i, handler.deleted_versions_.at(i));
  }
  EXPECT_EQ(1, handler.waterline_updates_);
  EXPECT_EQ(9, handler.last_updated_version_);
}

TEST(PrivateVersionGCBatch, metadata_failure_stops_reverse_finalize_and_waterline)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  MockPrivateVersionGCHandler handler(1001, stop, 10);
  ASSERT_EQ(OB_SUCCESS, handler.add_blocks(0, 1, 1));
  ASSERT_EQ(OB_SUCCESS, handler.gc_blocks_for_tablet_meta_versions(batcher));
  ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
  ASSERT_EQ(1, batcher.get_gc_macro_block_cnt());
  handler.fail_delete_version_ = 8;
  EXPECT_EQ(OB_IO_ERROR, handler.delete_tablet_meta_versions());
  ASSERT_EQ(2, handler.deleted_versions_.count());
  EXPECT_EQ(9, handler.deleted_versions_.at(0));
  EXPECT_EQ(8, handler.deleted_versions_.at(1));
  EXPECT_EQ(0, handler.waterline_updates_);
}

TEST(PrivateVersionGCBatch, stop_after_last_metadata_delete_updates_waterline)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  MockPrivateVersionGCHandler handler(1001, stop, 10);
  ASSERT_EQ(OB_SUCCESS, handler.add_blocks(0, 1, 1));
  ASSERT_EQ(OB_SUCCESS, handler.gc_blocks_for_tablet_meta_versions(batcher));
  ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
  ASSERT_EQ(1, batcher.get_gc_macro_block_cnt());
  handler.stop_after_last_meta_delete_ = &stop;
  // All metadata deletions succeeded, so record the completed GC progress.
  EXPECT_EQ(OB_SUCCESS, handler.delete_tablet_meta_versions());
  EXPECT_TRUE(stop);
  ASSERT_EQ(10, handler.deleted_versions_.count());
  EXPECT_EQ(0, handler.deleted_versions_.at(9));
  EXPECT_EQ(1, handler.waterline_updates_);
  EXPECT_EQ(9, handler.last_updated_version_);
}

TEST(PrivateVersionGCBatch, ten_small_tablets_use_six_full_batches)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  for (uint64_t tablet_id = 1001; tablet_id <= 1010; ++tablet_id) {
    MockPrivateVersionGCHandler handler(tablet_id, stop);
    ASSERT_EQ(OB_SUCCESS, handler.add_blocks(0, 1, 600));
    ASSERT_EQ(OB_SUCCESS, handler.gc_blocks_for_tablet_meta_versions(batcher));
    EXPECT_EQ(0, handler.deleted_versions_.count());
  }
  ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
  EXPECT_EQ(6, batcher.get_batch_count());
  EXPECT_EQ(6000, batcher.get_gc_macro_block_cnt());
  ASSERT_EQ(6, batcher.batch_sizes_.count());
  for (int64_t i = 0; i < batcher.batch_sizes_.count(); ++i) {
    EXPECT_EQ(1000, batcher.batch_sizes_.at(i));
  }
}

TEST(PrivateVersionGCBatch, full_and_tail_failure_leave_metadata_untouched)
{
  for (const int64_t failed_batch : {1L, 2L}) {
    SCOPED_TRACE(failed_batch);
    bool stop = false;
    RecordingPrivateGCBatcher batcher(stop);
    MockPrivateVersionGCHandler handler(1001, stop);
    ASSERT_EQ(OB_SUCCESS, handler.add_blocks(0, 1, 1500));
    batcher.fail_on_batch_ = failed_batch;
    const int process_ret = handler.gc_blocks_for_tablet_meta_versions(batcher);
    if (1 == failed_batch) {
      EXPECT_EQ(OB_IO_ERROR, process_ret);
      EXPECT_EQ(0, batcher.get_gc_macro_block_cnt());
      EXPECT_EQ(0, batcher.get_batch_count());
    } else {
      ASSERT_EQ(OB_SUCCESS, process_ret);
      EXPECT_EQ(OB_IO_ERROR, batcher.batch_delete_remaining_macro_blocks());
      EXPECT_EQ(1000, batcher.get_gc_macro_block_cnt());
      EXPECT_EQ(1, batcher.get_batch_count());
    }
    batcher.reuse_block_ids();
    EXPECT_EQ(0, handler.deleted_versions_.count());
    EXPECT_EQ(0, handler.waterline_updates_);
  }
}

TEST(PrivateVersionGCBatch, later_read_failure_does_not_submit_its_partial_output)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  MockPrivateVersionGCHandler handler(1001, stop);
  ASSERT_EQ(OB_SUCCESS, handler.add_blocks(0, 1, 1000));
  ASSERT_EQ(OB_SUCCESS, handler.add_blocks(1, 2001, 1500));
  handler.fail_read_version_ = 1;
  EXPECT_EQ(OB_IO_ERROR, handler.gc_blocks_for_tablet_meta_versions(batcher));
  EXPECT_EQ(1, batcher.get_batch_count());
  EXPECT_EQ(1000, batcher.get_gc_macro_block_cnt());
  EXPECT_EQ(0, handler.deleted_versions_.count());
  EXPECT_EQ(0, handler.waterline_updates_);
}

TEST(PrivateVersionGCBatch, stop_blocks_metadata_even_when_tail_completes)
{
  for (const bool stop_during_io : {false, true}) {
    SCOPED_TRACE(stop_during_io);
    bool stop = false;
    RecordingPrivateGCBatcher batcher(stop);
    MockPrivateVersionGCHandler handler(1001, stop);
    ASSERT_EQ(OB_SUCCESS, handler.add_blocks(0, 1, 1500));
    if (stop_during_io) {
      batcher.stop_after_delete_ = &stop;
      EXPECT_EQ(OB_SERVICE_STOPPED, handler.gc_blocks_for_tablet_meta_versions(batcher));
    } else {
      ASSERT_EQ(OB_SUCCESS, handler.gc_blocks_for_tablet_meta_versions(batcher));
      stop = true;
    }
    // An append interrupted by stop reports an error, so the caller skips the tail.
    // If all appends completed successfully, the tail may finish after stop is set.
    int tail_ret = batcher.get_error();
    if (OB_SUCCESS == tail_ret) {
      tail_ret = batcher.batch_delete_remaining_macro_blocks();
    }
    EXPECT_EQ(stop_during_io ? OB_SERVICE_STOPPED : OB_SUCCESS, tail_ret);
    EXPECT_EQ(stop_during_io ? 1 : 2, batcher.get_batch_count());
    EXPECT_EQ(stop_during_io ? 1000 : 1500, batcher.get_gc_macro_block_cnt());
    EXPECT_EQ(OB_SERVICE_STOPPED, handler.delete_tablet_meta_versions());
    EXPECT_EQ(0, handler.deleted_versions_.count());
    EXPECT_EQ(0, handler.waterline_updates_);
    batcher.reuse_block_ids();
  }
}

TEST(PrivateVersionGCBatch, zero_or_one_version_is_distinct_from_zero_candidates)
{
  for (const int64_t count : {0L, 1L, 3L}) {
    SCOPED_TRACE(count);
    bool stop = false;
    RecordingPrivateGCBatcher batcher(stop);
    MockPrivateVersionGCHandler handler(1001, stop);
    handler.version_count_ = count;
    ASSERT_EQ(OB_SUCCESS, handler.gc_blocks_for_tablet_meta_versions(batcher));
    EXPECT_EQ(count > 1, handler.need_gc_tablet_meta_versions());
    ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
    EXPECT_EQ(0, batcher.get_batch_count());
    if (handler.need_gc_tablet_meta_versions()) {
      ASSERT_EQ(OB_SUCCESS, handler.delete_tablet_meta_versions());
    }
    EXPECT_EQ(count > 1 ? count - 1 : 0, handler.deleted_versions_.count());
    EXPECT_EQ(count > 1 ? 1 : 0, handler.waterline_updates_);
  }
}

class PostFilterGCOP : public storage::GCTabletOP
{
public:
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &ids,
      ObIArray<blocksstable::MacroBlockId> &result) override
  {
    int ret = GCTabletOP::operator()(ids, result);
    if (OB_SUCC(ret) && result.count() > 0) {
      ret = result.remove(result.count() - 1);
    }
    return ret;
  }
};

TEST(PrivateVersionGCBatch, submits_final_filtered_ids_instead_of_dedup_set)
{
  bool stop = false;
  RecordingPrivateGCBatcher batcher(stop);
  MockPrivateVersionGCHandler handler(1001, stop);
  ObArray<int64_t> versions;
  PostFilterGCOP op;
  ASSERT_EQ(OB_SUCCESS, versions.push_back(0));
  ASSERT_EQ(OB_SUCCESS, handler.add_blocks(0, 1, 3));
  ASSERT_EQ(OB_SUCCESS, handler.gc_blocks_(versions, 0, false, op, &batcher));
  EXPECT_EQ(3, op.get_result_block_id_set().size());
  ASSERT_EQ(OB_SUCCESS, batcher.batch_delete_remaining_macro_blocks());
  ASSERT_EQ(2, batcher.deleted_ids_.count());
  EXPECT_EQ(1, batcher.deleted_ids_.at(0).tenant_seq());
  EXPECT_EQ(2, batcher.deleted_ids_.at(1).tenant_seq());
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_block_gc_handler.log*");
  OB_LOGGER.set_file_name("test_block_gc_handler.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
