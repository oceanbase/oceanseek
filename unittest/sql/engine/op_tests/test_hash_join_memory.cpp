/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include "lib/container/ob_se_array.h"
#include "lib/ob_errno.h"
#define private public
#define protected public
#include "unittest/sql/engine/op_tests/ob_op_test_engine.h"
#include "sql/engine/join/hash_join/ob_hash_join_vec_op.h"
#include "sql/engine/join/ob_join_filter_op.h"
#include "sql/engine/px/ob_sqc_ctx.h"
#undef protected
#undef private

namespace oceanbase
{
namespace sql
{

class PartitionFailAllocator : public common::ObWrapperAllocator
{
public:
  explicit PartitionFailAllocator(common::ObIAllocator &allocator)
      : common::ObWrapperAllocator(allocator)
  {}
  void *alloc(const int64_t size) override
  {
    return should_fail() ? nullptr : common::ObWrapperAllocator::alloc(size);
  }
  void *alloc(const int64_t size, const common::ObMemAttr &attr) override
  {
    return should_fail() ? nullptr : common::ObWrapperAllocator::alloc(size, attr);
  }
  int64_t allocations_before_failure_{-1};
private:
  bool should_fail()
  {
    const bool fail = 0 == allocations_before_failure_;
    if (allocations_before_failure_ > 0) {
      --allocations_before_failure_;
    }
    return fail;
  }
};

class HashJoinMemoryTest : public ::testing::Test
{
public:
  void SetUp() override
  {
    engine_.init();
    spec_.reset(new ObHashJoinVecSpec(engine_.get_allocator(), PHY_VEC_HASH_JOIN));
    // Match codegen: rescan expects each spec to belong to a physical plan.
    spec_->plan_ = &engine_.get_phy_plan();
    spec_->max_batch_size_ = 64;
    spec_->compress_type_ = common::NONE_COMPRESSOR;
    spec_->jf_material_control_info_.enable_material_ = true;
    hj_input_.reset(new ObHashJoinVecInput(engine_.get_exec_ctx(), *spec_));
    op_.reset(new ObHashJoinVecOp(engine_.get_exec_ctx(), *spec_, hj_input_.get()));
    ASSERT_EQ(OB_SUCCESS, op_->init_mem_context(OB_SYS_TENANT_ID));
    op_->jt_ctx_.max_batch_size_ = spec_->max_batch_size_;
    ASSERT_EQ(OB_SUCCESS, op_->join_table_.init_generic_ht(op_->jt_ctx_, *op_->alloc_));
    op_->cur_join_table_ = &op_->join_table_;
    void *mgr_buf = op_->alloc_->alloc(sizeof(ObHJPartitionMgr));
    ASSERT_NE(nullptr, mgr_buf);
    op_->part_mgr_ = new (mgr_buf) ObHJPartitionMgr(*op_->alloc_, OB_SYS_TENANT_ID);

    jf_spec_.reset(new ObJoinFilterSpec(engine_.get_allocator(), PHY_JOIN_FILTER));
    jf_spec_->plan_ = &engine_.get_phy_plan();
    jf_spec_->max_batch_size_ = 64;
    jf_spec_->rows_ = 10000;
    jf_spec_->width_ = 128;
    jf_spec_->mode_ = JoinFilterMode::CREATE;
    jf_spec_->filter_shared_type_ = JoinFilterSharedType::NONSHARED_JOIN_FILTER;
    jf_spec_->jf_material_control_info_.enable_material_ = true;
    jf_spec_->jf_material_control_info_.is_controller_ = true;
    jf_spec_->jf_material_control_info_.join_filter_count_ = 1;
    jf_input_.reset(new ObJoinFilterOpInput(engine_.get_exec_ctx(), *jf_spec_));
    jf_.reset(new ObJoinFilterOp(engine_.get_exec_ctx(), *jf_spec_, jf_input_.get()));
    jf_->parent_ = op_.get();
    op_->left_ = jf_.get();
    probe_spec_.reset(new MockDataSourceSpec(engine_.get_allocator(), PHY_TABLE_SCAN));
    probe_spec_->plan_ = &engine_.get_phy_plan();
    probe_.reset(new MockDataSourceOp(engine_.get_exec_ctx(), *probe_spec_, nullptr));
    op_->right_ = probe_.get();
    children_[0] = jf_.get();
    children_[1] = probe_.get();
    ASSERT_EQ(OB_SUCCESS, op_->set_children_pointer(children_, 2));
    jf_->build_rows_output_ = &jf_spec_->output_;
    op_->jt_ctx_.build_output_ = &jf_spec_->output_;
  }

  void TearDown() override
  {
    if (nullptr != op_) {
      EXPECT_EQ(OB_SUCCESS, close_join());
      op_->destroy();
      op_.reset();
    }
    partition_allocator_.reset();
    hj_input_.reset();
    jf_.reset();
    probe_.reset();
    probe_spec_.reset();
    jf_input_.reset();
    jf_spec_.reset();
    spec_.reset();
    engine_.destroy();
  }

  void materialize_rows(int64_t count, int64_t part_idx = 0)
  {
    if (nullptr == jf_->sql_mem_processor_) {
      ASSERT_EQ(OB_SUCCESS, jf_->init_material_partitions());
    }
    ObPartitionStore *store = jf_->partition_splitter_->get_partition(part_idx);
    alignas(ObCompactRow) char row_buf[128] = {};
    ObCompactRow *row = new (row_buf) ObCompactRow();
    row->init(store->get_row_store().row_meta_);
    row->set_row_size(sizeof(row_buf));
    for (int64_t i = 0; i < count; ++i) {
      ObCompactRow *stored_row = nullptr;
      ASSERT_EQ(OB_SUCCESS, store->add_row(row, stored_row));
      ++jf_->partition_splitter_->total_row_count_;
    }
  }

  bool needs_dump(int64_t rows, int64_t bound)
  {
    op_->sql_mem_processor_.set_default_usable_mem_size(bound);
    bool need_dump = false;
    op_->update_remain_data_memory_size(rows, bound, need_dump);
    return need_dump;
  }

  void bind_materialized_rows()
  {
    auto *splitter = jf_->partition_splitter_;
    op_->join_filter_partition_splitter_ = splitter;
    op_->part_count_ = splitter->get_part_count();
    const int64_t count = op_->part_count_;
    ASSERT_EQ(count, op_->part_mgr_->get_part_list_size());
    ObHJPartition *first_part = nullptr;
    ASSERT_EQ(OB_SUCCESS, op_->part_mgr_->get_or_create_part(
        0, 0, static_cast<int64_t>(op_->part_round_) << 32, true, first_part, true));
    ASSERT_NE(nullptr, first_part);
    ASSERT_EQ(OB_SUCCESS, op_->init_join_partition());
    EXPECT_EQ(first_part, op_->left_part_array_[0]);
    EXPECT_EQ(count, op_->part_mgr_->get_part_list_size());
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(splitter->get_partition(i), op_->left_part_array_[i]->get_partition_store());
      EXPECT_TRUE(op_->right_part_array_[i]->get_row_store().is_inited());
    }
    if (nullptr == op_->jt_ctx_.build_cols_have_null_) {
      void *bitmap_buf = op_->mem_context_->get_arena_allocator().alloc(ObBitVector::memory_size(1));
      ASSERT_NE(nullptr, bitmap_buf);
      op_->jt_ctx_.build_cols_have_null_ = static_cast<ObBitVector *>(bitmap_buf);
      op_->jt_ctx_.build_cols_have_null_->reset(1);
    }
    int64_t rows = 0;
    ASSERT_EQ(OB_SUCCESS, op_->fill_partition_from_join_filter(rows));
    EXPECT_EQ(splitter->get_total_row_count(), rows);
  }

  int close_join()
  {
    int ret = OB_SUCCESS;
    if (!op_closed_) {
      ret = op_->close();
      op_closed_ = true;
    }
    return ret;
  }

  int64_t non_arena_malloc_used() const
  {
    // malloc_used() includes ObAllocator's list node for each retained arena page.
    const int64_t pages = op_->mem_context_->get_arena_allocator().get_arena().pages();
    return op_->mem_context_->malloc_used() - pages * sizeof(common::ObAllocator::AllocNode);
  }

protected:
  OpTestEngine engine_;
  std::unique_ptr<ObHashJoinVecSpec> spec_;
  std::unique_ptr<ObHashJoinVecInput> hj_input_;
  std::unique_ptr<ObHashJoinVecOp> op_;
  std::unique_ptr<PartitionFailAllocator> partition_allocator_;
  std::unique_ptr<ObJoinFilterSpec> jf_spec_;
  std::unique_ptr<ObJoinFilterOpInput> jf_input_;
  std::unique_ptr<ObJoinFilterOp> jf_;
  std::unique_ptr<MockDataSourceSpec> probe_spec_;
  std::unique_ptr<MockDataSourceOp> probe_;
  ObOperator *children_[2]{nullptr, nullptr};
  bool op_closed_{false};
};

TEST_F(HashJoinMemoryTest, MaterialRowsUseOneContextProcessorAndProfile)
{
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  EXPECT_EQ(nullptr, jf_->sql_mem_processor_);
  const int64_t arena_used_before = op_->mem_context_->arena_used();
  materialize_rows(4096);
  ASSERT_EQ(op_->mem_context_, jf_->mem_context_);
  ASSERT_EQ(&op_->sql_mem_processor_, jf_->sql_mem_processor_);
  EXPECT_EQ(&op_->profile_, &jf_->sql_mem_processor_->profile_);
  auto *splitter = jf_->partition_splitter_;
  EXPECT_EQ(&op_->mem_context_->get_arena_allocator(), splitter->arena_alloc_);
  EXPECT_EQ(op_->alloc_, splitter->malloc_alloc_);
  EXPECT_EQ(op_->alloc_, splitter->get_partition(0)->get_row_store().allocator_);
  const int64_t structure_size = sizeof(ObJoinFilterPartitionSplitter)
      + sizeof(ObPartitionStore) * splitter->get_part_count();
  EXPECT_GE(op_->mem_context_->arena_used() - arena_used_before, structure_size);
  EXPECT_EQ(op_->mem_context_->malloc_used() + op_->mem_context_->arena_used(),
            op_->get_mem_used());
  ASSERT_EQ(splitter->get_part_count(), op_->part_mgr_->get_part_list_size());
  for (int64_t i = 0; i < splitter->get_part_count(); ++i) {
    ObHJPartition *left = nullptr;
    ObHJPartition *right = nullptr;
    const int64_t partno = (static_cast<int64_t>(op_->part_round_) << 32) + i;
    ASSERT_EQ(OB_SUCCESS, op_->part_mgr_->get_or_create_part(0, 0, partno, true, left, true));
    ASSERT_EQ(OB_SUCCESS, op_->part_mgr_->get_or_create_part(0, 0, partno, false, right, true));
    ASSERT_NE(nullptr, left);
    ASSERT_NE(nullptr, right);
    EXPECT_EQ(splitter->get_partition(i), left->get_partition_store());
    EXPECT_FALSE(left->is_store_created_by_hj());
    EXPECT_TRUE(right->is_store_created_by_hj());
    EXPECT_FALSE(right->get_row_store().is_inited());
    EXPECT_EQ(op_->part_shift_ + min(__builtin_ctz(splitter->get_part_count()), 8),
              left->get_part_shift());
  }
  EXPECT_EQ(nullptr, op_->left_part_array_);
  EXPECT_EQ(nullptr, op_->join_filter_partition_splitter_);
  EXPECT_EQ(PHY_VEC_HASH_JOIN, op_->profile_.get_operator_type());
  EXPECT_GT(op_->sql_mem_processor_.get_data_size(), 0);
  const int64_t used = op_->get_cur_mem_used();
  const int64_t data = op_->get_data_mem_used();
  op_->join_filter_partition_splitter_ = jf_->partition_splitter_;
  EXPECT_EQ(used, op_->get_cur_mem_used());
  EXPECT_EQ(data, op_->get_data_mem_used());
  EXPECT_EQ(op_->get_mem_used() - op_->join_table_.get_mem_used(), used);
  EXPECT_TRUE(needs_dump(100000, 128 * 1024));
  EXPECT_FALSE(needs_dump(100000, 64L * 1024 * 1024));
}

TEST_F(HashJoinMemoryTest, LaterProcessorInitPreservesLiveRowsAndDirectory)
{
  materialize_rows(4096);
  const int64_t data = op_->sql_mem_processor_.get_data_size();
  const int64_t dir_id = op_->sql_mem_processor_.get_dir_id();
  op_->sql_mem_processor_.set_number_pass(1);
  op_->join_filter_partition_splitter_ = jf_->partition_splitter_;
  ASSERT_EQ(OB_SUCCESS, op_->calc_basic_info());
  EXPECT_EQ(4096, op_->profile_.get_row_count());
  ASSERT_EQ(OB_SUCCESS, op_->get_max_memory_size(op_->profile_.get_input_size()));
  EXPECT_EQ(data, op_->sql_mem_processor_.get_data_size());
  EXPECT_EQ(dir_id, op_->sql_mem_processor_.get_dir_id());
  EXPECT_EQ(jf_->partition_splitter_->get_part_count(), op_->part_count_);
  EXPECT_EQ(1, op_->profile_.number_pass_);
  jf_->partition_splitter_->reuse_for_rescan();
  EXPECT_EQ(0, op_->sql_mem_processor_.get_data_size());
}

TEST_F(HashJoinMemoryTest, TopLevelMaterialStatsUseResidentRowsAndBytes)
{
  materialize_rows(4096);
  materialize_rows(257, 1);
  auto *splitter = jf_->partition_splitter_;
  op_->join_filter_partition_splitter_ = splitter;
  const int64_t bytes = splitter->get_partition(0)->get_size_in_memory()
                        + splitter->get_partition(1)->get_size_in_memory();
  ASSERT_GT(bytes, 0);
  jf_spec_->width_ = 1024;
  for (int64_t estimated_rows : {1L, 100000000L}) {
    jf_spec_->rows_ = estimated_rows;
    ASSERT_EQ(OB_SUCCESS, op_->calc_basic_info());
    EXPECT_EQ(4353, op_->profile_.get_row_count());
    EXPECT_EQ(bytes, op_->profile_.get_input_size());
    EXPECT_EQ(op_->calc_bucket_number(4353), op_->profile_.get_bucket_size());
  }
}

TEST_F(HashJoinMemoryTest, EmptyMaterialStatsKeepZeroRowsAndBytes)
{
  materialize_rows(0);
  op_->join_filter_partition_splitter_ = jf_->partition_splitter_;
  ASSERT_EQ(OB_SUCCESS, op_->calc_basic_info());
  EXPECT_EQ(0, op_->profile_.get_row_count());
  EXPECT_EQ(0, op_->profile_.get_input_size());
  ASSERT_EQ(OB_SUCCESS, op_->get_max_memory_size(op_->profile_.get_input_size()));
  EXPECT_EQ(jf_->partition_splitter_->get_part_count(), op_->part_count_);
}

TEST_F(HashJoinMemoryTest, RescanBeforeAndAfterMaterialization)
{
  ASSERT_EQ(OB_SUCCESS, jf_->do_create_filter_rescan());
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  materialize_rows(4096);
  auto *splitter = jf_->partition_splitter_;
  ObPartitionStore *store = splitter->get_partition(0);
  bind_materialized_rows();
  const int64_t arena_used = op_->mem_context_->arena_used();
  EXPECT_EQ(store, op_->left_part_array_[0]->get_partition_store());
  EXPECT_FALSE(op_->left_part_array_[0]->is_store_created_by_hj());
  EXPECT_EQ(4096, op_->left_part_array_[0]->get_row_count_in_memory());
  splitter->first_dumped_part_idx_ = 0;
  splitter->get_partition(0)->set_memory_limit(1);
  ASSERT_EQ(OB_SUCCESS, jf_->do_create_filter_rescan());
  EXPECT_EQ(nullptr, jf_->sql_mem_processor_);
  EXPECT_EQ(0, op_->get_data_mem_used());
  EXPECT_EQ(splitter->get_part_count(), splitter->get_first_dumped_part_idx());
  EXPECT_EQ(0, splitter->get_partition(0)->get_row_store().get_mem_limit());
  ASSERT_EQ(OB_SUCCESS, op_->inner_rescan());
  EXPECT_EQ(nullptr, op_->join_filter_partition_splitter_);
  EXPECT_EQ(nullptr, op_->left_part_array_);
  EXPECT_EQ(0, op_->part_mgr_->get_part_list_size());
  EXPECT_EQ(store, splitter->get_partition(0));
  materialize_rows(4096);
  EXPECT_EQ(splitter, jf_->partition_splitter_);
  EXPECT_GT(op_->get_data_mem_used(), 0);
  bind_materialized_rows();
  EXPECT_EQ(store, op_->left_part_array_[0]->get_partition_store());
  EXPECT_EQ(4096, op_->left_part_array_[0]->get_row_count_in_memory());
  ASSERT_EQ(OB_SUCCESS, op_->calc_basic_info());
  EXPECT_EQ(4096, op_->profile_.get_row_count());
  EXPECT_EQ(arena_used, op_->mem_context_->arena_used());
}

TEST_F(HashJoinMemoryTest, PartialPartitionPreparationRollsBackAndCanRetry)
{
  partition_allocator_.reset(new PartitionFailAllocator(*op_->alloc_));
  op_->part_mgr_->~ObHJPartitionMgr();
  op_->part_mgr_ = new (op_->part_mgr_)
      ObHJPartitionMgr(*partition_allocator_, OB_SYS_TENANT_ID);
  lib::MemoryContext context = nullptr;
  ObSqlMemMgrProcessor *processor = nullptr;
  ASSERT_EQ(OB_SUCCESS, op_->acquire_workarea(1024 * 1024, context, processor));
  const int64_t malloc_used_before = non_arena_malloc_used();
  // Fail each allocation in the first two pairs, including after one pair was attached.
  for (int64_t fail_after = 0; fail_after < 8; ++fail_after) {
    SCOPED_TRACE(fail_after);
    partition_allocator_->allocations_before_failure_ = fail_after;
    ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, jf_->init_material_partitions());
    EXPECT_EQ(nullptr, jf_->partition_splitter_);
    EXPECT_EQ(nullptr, jf_->sql_mem_processor_);
    EXPECT_EQ(nullptr, op_->join_filter_partition_splitter_);
    EXPECT_EQ(0, op_->part_mgr_->get_part_list_size());
    EXPECT_EQ(0, op_->part_mgr_->part_count_);
    EXPECT_EQ(0, op_->get_data_mem_used());
    // Failed attempts leave only arena structures until HJ closes.
    EXPECT_EQ(malloc_used_before, non_arena_malloc_used());
  }
  partition_allocator_->allocations_before_failure_ = -1;
  materialize_rows(4096);
  ASSERT_EQ(jf_->partition_splitter_->get_part_count(), op_->part_mgr_->get_part_list_size());
  EXPECT_EQ(4096, jf_->partition_splitter_->get_partition(0)->get_row_count_in_memory());
}

TEST_F(HashJoinMemoryTest, CreatedStoreResetReclaimsRowsWithoutClose)
{
  materialize_rows(0);
  const int64_t data_before = op_->get_data_mem_used();
  ObHJPartitionMgr mgr(*op_->alloc_, OB_SYS_TENANT_ID);
  ObHJPartition *part = nullptr;
  ASSERT_EQ(OB_SUCCESS, mgr.get_or_create_part(0, 32, 1, true, part));
  ASSERT_TRUE(part->is_store_created_by_hj());
  ASSERT_EQ(OB_SUCCESS,
      part->init(jf_spec_->output_, spec_->max_batch_size_, common::NONE_COMPRESSOR, 0));
  part->set_callback(&op_->sql_mem_processor_);
  ObPartitionStore *store = part->get_partition_store();
  alignas(ObCompactRow) char row_buf[128] = {};
  ObCompactRow *row = new (row_buf) ObCompactRow();
  row->init(store->get_row_store().row_meta_);
  row->set_row_size(sizeof(row_buf));
  for (int64_t i = 0; i < 4096; ++i) {
    ObCompactRow *stored_row = nullptr;
    ASSERT_EQ(OB_SUCCESS, store->add_row(row, stored_row));
  }
  ASSERT_GT(op_->get_data_mem_used(), data_before);
  mgr.reset();
  EXPECT_EQ(0, mgr.get_part_list_size());
  EXPECT_EQ(data_before, op_->get_data_mem_used());
}

TEST_F(HashJoinMemoryTest, BorrowedStoreSurvivesWrapperReset)
{
  materialize_rows(4096);
  ObPartitionStore *store = jf_->partition_splitter_->get_partition(0);
  op_->part_mgr_->reset();
  EXPECT_EQ(0, store->get_row_count_in_memory());
  EXPECT_EQ(nullptr, store->get_row_store().batch_ctx_);
  EXPECT_EQ(store, jf_->partition_splitter_->get_partition(0));
  ASSERT_EQ(OB_SUCCESS, jf_->do_create_filter_rescan());
  ASSERT_EQ(OB_SUCCESS, op_->inner_rescan());
  materialize_rows(4096);
  EXPECT_EQ(4096, store->get_row_count_in_memory());
}

TEST_F(HashJoinMemoryTest, CompletedPartitionReleasesRowsBeforeStoreObject)
{
  materialize_rows(4096);
  ObPartitionStore *store = jf_->partition_splitter_->get_partition(0);
  ASSERT_GT(store->get_size_in_memory(), 0);
  ASSERT_EQ(OB_SUCCESS, op_->part_mgr_->remove_undumped_part(INT64_MAX, op_->part_round_));
  EXPECT_EQ(0, op_->part_mgr_->get_part_list_size());
  EXPECT_EQ(0, store->get_row_count_in_memory());
  EXPECT_EQ(0, store->get_size_in_memory());
  EXPECT_EQ(nullptr, store->get_row_store().batch_ctx_);
  EXPECT_EQ(store, jf_->partition_splitter_->get_partition(0));
}

TEST_F(HashJoinMemoryTest, ChildCloseRetainsStoresUntilParentCleanup)
{
  materialize_rows(4096);
  auto *splitter = jf_->partition_splitter_;
  ObPartitionStore *store = splitter->get_partition(0);
  op_->join_filter_partition_splitter_ = splitter;
  ASSERT_EQ(OB_SUCCESS, jf_->close_join_filter_create());
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  EXPECT_EQ(nullptr, jf_->mem_context_);
  EXPECT_EQ(nullptr, jf_->sql_mem_processor_);
  EXPECT_EQ(4096, store->get_row_count_in_memory());
  EXPECT_GT(op_->get_data_mem_used(), 0);
  EXPECT_NE(nullptr, op_->part_mgr_);
  ASSERT_EQ(OB_SUCCESS, op_->inner_close());
  op_closed_ = true;
  EXPECT_EQ(nullptr, op_->part_mgr_);
  EXPECT_EQ(nullptr, op_->join_filter_partition_splitter_);
  EXPECT_EQ(0, op_->mem_context_->arena_used());
  EXPECT_EQ(0, non_arena_malloc_used());
}

TEST_F(HashJoinMemoryTest, ChildDestroyAfterCloseKeepsParentWorkareaValid)
{
  materialize_rows(4096);
  ASSERT_EQ(OB_SUCCESS, close_join());
  jf_->destroy();
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  EXPECT_NE(nullptr, op_->mem_context_);
  op_->destroy();
  EXPECT_EQ(nullptr, op_->mem_context_);
}

TEST_F(HashJoinMemoryTest, ParentDestroyAfterCloseLeavesNoChildArenaReferences)
{
  materialize_rows(4096);
  ASSERT_EQ(OB_SUCCESS, close_join());
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  EXPECT_EQ(0, op_->mem_context_->arena_used());
  EXPECT_EQ(0, non_arena_malloc_used());
  op_->destroy();
  EXPECT_EQ(nullptr, op_->mem_context_);
  jf_->destroy();
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  EXPECT_EQ(nullptr, jf_->mem_context_);
  EXPECT_EQ(nullptr, jf_->sql_mem_processor_);
}

TEST_F(HashJoinMemoryTest, ParentCloseCleansStoresBeforeMaterializationCompletes)
{
  materialize_rows(4096);
  auto *splitter = jf_->partition_splitter_;
  ASSERT_EQ(nullptr, op_->join_filter_partition_splitter_);
  ASSERT_EQ(splitter->get_part_count(), op_->part_mgr_->get_part_list_size());
  ASSERT_EQ(nullptr, op_->left_part_array_);
  ASSERT_EQ(OB_SUCCESS, close_join());
  EXPECT_NE(nullptr, op_->mem_context_);
  EXPECT_EQ(nullptr, op_->part_mgr_);
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  EXPECT_EQ(0, op_->mem_context_->arena_used());
  EXPECT_EQ(0, non_arena_malloc_used());
  op_->destroy();
  EXPECT_EQ(nullptr, op_->mem_context_);
  jf_->destroy();
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
}

TEST_F(HashJoinMemoryTest, ParentCloseCleansDequeuedPartition)
{
  materialize_rows(0);
  const int64_t last_part = jf_->partition_splitter_->get_part_count() - 1;
  materialize_rows(4096, last_part);
  ObPartitionStore *store = jf_->partition_splitter_->get_partition(last_part);
  ObHJPartitionPair pair;
  ASSERT_EQ(OB_SUCCESS, op_->part_mgr_->next_part_pair(pair));
  op_->left_part_ = pair.left_;
  op_->right_part_ = pair.right_;
  ASSERT_EQ(store, pair.left_->get_partition_store());
  ASSERT_EQ(OB_SUCCESS, close_join());
  EXPECT_NE(nullptr, op_->mem_context_);
  EXPECT_EQ(nullptr, op_->left_part_);
  EXPECT_EQ(nullptr, op_->right_part_);
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  EXPECT_EQ(0, op_->mem_context_->arena_used());
  EXPECT_EQ(0, non_arena_malloc_used());
  op_->destroy();
  EXPECT_EQ(nullptr, op_->mem_context_);
  jf_->destroy();
}

TEST_F(HashJoinMemoryTest, RescanWithoutRematerializationReleasesStoreData)
{
  materialize_rows(4096);
  ObPartitionStore *store = jf_->partition_splitter_->get_partition(0);
  ASSERT_EQ(OB_SUCCESS, jf_->do_create_filter_rescan());
  ASSERT_EQ(OB_SUCCESS, op_->inner_rescan());
  EXPECT_EQ(nullptr, jf_->sql_mem_processor_);
  EXPECT_EQ(0, op_->part_mgr_->get_part_list_size());
  EXPECT_EQ(0, store->get_row_count_in_memory());
  EXPECT_EQ(nullptr, store->get_row_store().batch_ctx_);
  ASSERT_EQ(OB_SUCCESS, close_join());
  op_->destroy();
  EXPECT_EQ(nullptr, op_->mem_context_);
  jf_->destroy();
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
}

TEST_F(HashJoinMemoryTest, StoreRowMetadataSurvivesCloseWithoutWorkareaAllocations)
{
  ObExpr column;
  column.is_fixed_length_data_ = true;
  column.len_ = sizeof(int64_t);
  common::ObSEArray<ObExpr *, 32> exprs;
  for (int64_t i = 0; i < 32; ++i) {
    ASSERT_EQ(OB_SUCCESS, exprs.push_back(&column));
  }
  ObPartitionStore store(OB_SYS_TENANT_ID, *op_->alloc_);
  // Use the production expression init path, including its wide-schema fallback.
  ASSERT_EQ(OB_SUCCESS, store.init(exprs, spec_->max_batch_size_, common::NONE_COMPRESSOR, 0));
  RowMeta &meta = store.get_row_store().row_meta_;
  EXPECT_EQ(nullptr, meta.allocator_);
  EXPECT_FALSE(meta.fixed_expr_reordered());
  ASSERT_EQ(OB_SUCCESS, store.get_row_store().init_batch_ctx());
  store.close();
  EXPECT_EQ(nullptr, store.get_row_store().batch_ctx_);
  EXPECT_EQ(32, meta.col_cnt_);
  op_->destroy();
  op_closed_ = true;
  EXPECT_EQ(nullptr, op_->mem_context_);
  EXPECT_EQ(32, meta.col_cnt_);
  // The store object is destroyed after the HJ allocator, with no live internal allocations.
}

TEST_F(HashJoinMemoryTest, WorkareaMetadataParticipatesInDumpDecisions)
{
  materialize_rows(0);
  const int64_t used = op_->get_mem_used();
  const int64_t data = op_->get_data_mem_used();
  ASSERT_GT(used, data);
  const int64_t bound = data + (used - data) / 2;
  op_->sql_mem_processor_.is_auto_mgr_ = false;
  op_->sql_mem_processor_.set_default_usable_mem_size(bound);
  EXPECT_GT(bound, data);
  EXPECT_TRUE(jf_->need_dump());
  EXPECT_TRUE(jf_->partition_splitter_->need_dump());
  ObJoinFilterExtendMaxMemChecker checker(used);
  EXPECT_TRUE(checker(bound));
  EXPECT_FALSE(checker(used));
}

TEST_F(HashJoinMemoryTest, EmptyMaterialFinalizationDoesNotInitializeWorkarea)
{
  // Exercise real local Bloom initialization and group finalization without PX
  // transport. The distributed piece/whole-message protocol needs integration coverage.
  ObPxRpcInitSqcArgs sqc_args;
  ObSqcCtx sqc_ctx(sqc_args);
  jf_input_->set_sqc_proxy(sqc_ctx.sqc_proxy_);
  jf_input_->config_.bloom_filter_ratio_ = 0.01;
  jf_input_->config_.runtime_bloom_filter_max_size_ = 1024 * 1024;
  ObRFBloomFilterMsg msg;
  ASSERT_EQ(OB_SUCCESS, msg.init(1, 1, 0, OB_SYS_TENANT_ID,
      INT64_MAX, common::ObRegisterDmInfo()));
  msg.msg_type_ = ObP2PDatahubMsgBase::BLOOM_FILTER_VEC_MSG;
  jf_->bf_vec_msg_ = &msg;
  ASSERT_EQ(OB_SUCCESS, jf_->init_material_group_exec_info());
  jf_->force_dump_ = true;
  const int64_t used = op_->get_mem_used();
  ASSERT_EQ(OB_SUCCESS, jf_->build_and_broadcast_runtime_filter());
  EXPECT_TRUE(jf_->has_sent_runtime_filter_);
  EXPECT_EQ(0, jf_->op_monitor_info_.output_row_count_);
  EXPECT_EQ(nullptr, jf_->partition_splitter_);
  EXPECT_EQ(nullptr, jf_->sql_mem_processor_);
  EXPECT_EQ(used, op_->get_mem_used());
  jf_->bf_vec_msg_ = nullptr;
}

} // namespace sql
} // namespace oceanbase
