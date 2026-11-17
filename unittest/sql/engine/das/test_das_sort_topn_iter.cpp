/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <cstdio>
#include <cstdlib>
#include <set>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "sql/das/iter/ob_das_sort_topn_iter.h"
#include "sql/das/iter/ob_das_search_driver_iter.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/engine/ob_exec_context.h"
#undef protected
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

namespace
{

// Track individually reclaimable allocations, including partial entry copies on error.
// Hash-map storage uses its own allocator and is released by ObDASSortTopNHeap::reset().
class EntryAllocator : public ObIAllocator
{
public:
  EntryAllocator() : remaining_(-1) {}
  ~EntryAllocator() override
  {
    for (void *ptr : live_) {
      std::free(ptr);
    }
  }
  void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    if (remaining_ != 0) {
      if (remaining_ > 0) {
        --remaining_;
      }
      ptr = std::malloc(size);
      if (ptr != nullptr) {
        live_.insert(ptr);
      }
    }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (ptr != nullptr) {
      const size_t erased = live_.erase(ptr);
      EXPECT_EQ(1U, erased);
      if (erased == 1) {
        std::free(ptr);
      }
    }
  }
  void fail_after(const int64_t successful_allocations) { remaining_ = successful_allocations; }
  size_t outstanding() const { return live_.size(); }

private:
  int64_t remaining_;
  std::set<void *> live_;
};

int comparisons_before_failure = -1;

int compare_int_with_failure(const ObDatum &left, const ObDatum &right, int &cmp)
{
  int ret = OB_SUCCESS;
  if (comparisons_before_failure == 0) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    if (comparisons_before_failure > 0) {
      --comparisons_before_failure;
    }
    cmp = left.get_int() < right.get_int() ? -1 : left.get_int() > right.get_int() ? 1 : 0;
  }
  return ret;
}

int create_sort_context(ObDASHybridTopNImpl &sorter)
{
  lib::ContextParam param;
  param.set_mem_attr(MTL_ID(), ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA);
  return ROOT_CONTEXT->CREATE_CONTEXT(sorter.mem_context_, param);
}

class TestDASSortTopN : public ::testing::Test
{
public:
  TestDASSortTopN()
    : allocator_("SortTopNTest"), source_allocator_("SortTopNSrc"),
      output_allocator_("SortTopNOut"), query_(allocator_, DAS_OP_SCALAR_SCAN_QUERY),
      sort_(allocator_)
  {}

  void SetUp() override
  {
    share::ObTenantEnv::get_tenant_local()->id_ = 500;
    comparisons_before_failure = -1;
    children_[0] = &query_;
    sort_.children_ = children_;
    sort_.children_cnt_ = 1;
    sort_.das_sort_topn_enable_ = true;
    sort_.has_sort_value_lookup_ = true;
    shard_args_[0] = &tablet_expr_;
    shard_args_[1] = &increment_expr_;
    shard_expr_.type_ = T_FUN_SYS_HYBRID_UNIQUE_SHARD_DOC;
    shard_expr_.arg_cnt_ = 2;
    shard_expr_.args_ = shard_args_;
    shard_expr_.datum_meta_.type_ = ObVarcharType;
    shard_expr_.datum_meta_.cs_type_ = CS_TYPE_BINARY;
    ASSERT_EQ(OB_SUCCESS, sort_.sort_exprs_.init(3));
    ASSERT_EQ(OB_SUCCESS, sort_.sort_collations_.init(3));
    ASSERT_EQ(OB_SUCCESS, sort_.sort_cmp_funcs_.init(3));
    ASSERT_EQ(OB_SUCCESS, sort_.score_sort_flags_.init(3));
    ASSERT_EQ(OB_SUCCESS, sort_.unique_shard_doc_flags_.init(3));
    ASSERT_EQ(OB_SUCCESS, append_key(primary_expr_, ObIntType, true, false));
    ASSERT_EQ(OB_SUCCESS, append_key(secondary_expr_, ObIntType, false, false));
    ASSERT_EQ(OB_SUCCESS, append_key(shard_expr_, ObVarcharType, true, true));
    ASSERT_EQ(OB_SUCCESS, sort_.validate());
  }

  void TearDown() override { EXPECT_EQ(0U, entries_.outstanding()); }

  int append_key(ObExpr &expr, ObObjType type, bool ascending, bool shard_doc)
  {
    int ret = OB_SUCCESS;
    ObSortCmpFunc cmp;
    cmp.cmp_func_ = ObDatumFuncs::get_nullsafe_cmp_func(
        type, type, NULL_LAST, CS_TYPE_BINARY, 0, false);
    if (cmp.cmp_func_ == nullptr) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(sort_.sort_exprs_.push_back(&expr))) {
    } else if (OB_FAIL(sort_.sort_collations_.push_back(
                   ObSortFieldCollation(sort_.sort_collations_.count(), CS_TYPE_BINARY,
                                        ascending, NULL_LAST)))) {
    } else if (OB_FAIL(sort_.sort_cmp_funcs_.push_back(cmp))) {
    } else if (OB_FAIL(sort_.score_sort_flags_.push_back(false))) {
    } else if (OB_FAIL(sort_.unique_shard_doc_flags_.push_back(shard_doc))) {
    }
    return ret;
  }

  // Every borrowed datum points into source_allocator_, never into a helper's stack.
  int make_entry(int64_t primary, int64_t secondary, uint64_t id, ObDASSortTopNEntry &entry,
                 bool null_primary = false)
  {
    int ret = OB_SUCCESS;
    int64_t *numbers = static_cast<int64_t *>(source_allocator_.alloc(sizeof(int64_t) * 2));
    char *shard = static_cast<char *>(source_allocator_.alloc(33));
    if (numbers == nullptr || shard == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      numbers[0] = primary;
      numbers[1] = secondary;
      std::snprintf(shard, 33, "%016llx%016llx", 1ULL, static_cast<unsigned long long>(id));
      ObDatum values[3];
      values[0].ptr_ = reinterpret_cast<const char *>(&numbers[0]);
      values[0].set_int(primary);
      if (null_primary) {
        values[0].set_null();
      }
      values[1].ptr_ = reinterpret_cast<const char *>(&numbers[1]);
      values[1].set_int(secondary);
      values[2].set_string(shard, 32);
      entry.rowid_.set_uint64(id);
      entry.raw_score_ = 2.5;
      entry.final_score_ = 5.0;
      for (int64_t i = 0; OB_SUCC(ret) && i < 3; ++i) {
        ret = entry.sort_values_.push_back(values[i]);
      }
    }
    return ret;
  }

  int offer(ObDASSortTopNHeap &heap, int64_t primary, int64_t secondary, uint64_t id,
            bool null_primary = false)
  {
    ObDASSortTopNEntry entry;
    int ret = make_entry(primary, secondary, id, entry, null_primary);
    if (OB_SUCC(ret)) {
      ret = heap.offer(entry);
    }
    return ret;
  }

protected:
  ObArenaAllocator allocator_;
  ObArenaAllocator source_allocator_;
  ObArenaAllocator output_allocator_;
  EntryAllocator entries_;
  ObIDASSearchCtDef query_;
  ObDASHybridSearchSortCtDef sort_;
  ObDASBaseCtDef *children_[1];
  ObExpr primary_expr_;
  ObExpr secondary_expr_;
  ObExpr shard_expr_;
  ObExpr tablet_expr_;
  ObExpr increment_expr_;
  ObExpr *shard_args_[2];
};

TEST_F(TestDASSortTopN, definition_rejects_malformed_metadata)
{
  EXPECT_EQ(OB_SUCCESS, sort_.validate());
  shard_expr_.args_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, sort_.validate());
  shard_expr_.args_ = shard_args_;
  shard_args_[1] = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, sort_.validate());
  shard_args_[1] = &increment_expr_;
  sort_.unique_shard_doc_flags_.at(2) = false;
  EXPECT_EQ(OB_ERR_UNEXPECTED, sort_.validate());
  sort_.unique_shard_doc_flags_.at(2) = true;
  sort_.sort_cmp_funcs_.at(0).cmp_func_ = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, sort_.validate());
  sort_.sort_cmp_funcs_.at(0).cmp_func_ = compare_int_with_failure;
  EXPECT_EQ(OB_SUCCESS, sort_.validate());
  sort_.score_sort_flags_.reset();
  EXPECT_EQ(OB_ERR_UNEXPECTED, sort_.validate());
}

TEST_F(TestDASSortTopN, iterator_lookup_and_cursor_release_and_destruction)
{
  for (bool release_first : {false, true}) {
    SCOPED_TRACE(release_first);
    ObDASSortTopNIter iter;
    ASSERT_EQ(OB_SUCCESS, iter.lookup_.candidate_indices_.create(16, ObMemAttr(MTL_ID(), "SortMapTest")));
    ASSERT_EQ(OB_SUCCESS, iter.lookup_.candidate_indices_.set_refactored(42, 0));
    ObDASSortTopNEntry entry;
    ASSERT_EQ(OB_SUCCESS, make_entry(1, 0, 42, entry));
    ObDASSortCandidate candidate;
    candidate.rowid_ = 42;
    ASSERT_EQ(OB_SUCCESS, candidate.shard_doc_value_.deep_copy(
        entry.sort_values_.at(2), iter.lookup_.candidate_allocator_));
    ASSERT_EQ(OB_SUCCESS, iter.lookup_.candidates_.push_back(candidate));
    if (release_first) {
      ASSERT_EQ(OB_SUCCESS, iter.release());
      EXPECT_FALSE(iter.lookup_.candidate_indices_.created());
      EXPECT_EQ(0, iter.lookup_.candidates_.count());
      EXPECT_EQ(nullptr, iter.topn_impl_.after_row_);
      ASSERT_EQ(OB_SUCCESS, iter.release());
      EXPECT_FALSE(iter.lookup_.candidate_indices_.created());
    }
    // Exercise both direct destruction and destruction after repeated release.
  }
}

TEST_F(TestDASSortTopN, cursor_uses_sorter_memory_context)
{
  ObDASHybridTopNImpl sorter;
  ASSERT_EQ(OB_SUCCESS, create_sort_context(sorter));
  EXPECT_EQ(MTL_ID(), sorter.mem_context_->attr_.tenant_id_);
}

TEST_F(TestDASSortTopN, das_release_frees_cursor_context_before_iterator_destruction)
{
  ObArenaAllocator iter_storage(ObMemAttr(MTL_ID(), "SortIterTest"));
  for (int64_t execution = 0; execution < 32; ++execution) {
    SCOPED_TRACE(execution);
    void *body = iter_storage.alloc(sizeof(ObDASSortTopNIter));
    ASSERT_NE(nullptr, body);
    ObDASSortTopNIter *iter = new (body) ObDASSortTopNIter();
    ASSERT_EQ(OB_SUCCESS, create_sort_context(iter->topn_impl_));
    ObDASSortTopNEntry cursor;
    ASSERT_EQ(OB_SUCCESS, make_entry(10, 50, 100, cursor));
    const int64_t cells = cursor.sort_values_.count();
    const int64_t bytes = ObChunkDatumStore::Block::row_store_size(
        cursor.sort_values_.get_data(), cells);
    void *buf = iter->topn_impl_.mem_context_->get_malloc_allocator().alloc(bytes);
    ASSERT_NE(nullptr, buf);
    iter->topn_impl_.after_row_ = new (buf) ObChunkDatumStore::StoredRow();
    ASSERT_EQ(OB_SUCCESS, iter->topn_impl_.after_row_->copy_shadow_datums(
        cursor.sort_values_.get_data(), cells,
        reinterpret_cast<char *>(iter->topn_impl_.after_row_->cells()),
        bytes - sizeof(ObChunkDatumStore::StoredRow), bytes, 0));
    ASSERT_GT(iter->topn_impl_.mem_context_->used(), 0);

    // Ordinary reuse retains the fixed cursor until the iterator is released.
    const auto *after_row = iter->topn_impl_.after_row_;
    const auto *sort_context = iter->topn_impl_.mem_context_.ref_context();
    const int64_t retained = sort_context->used();
    iter->topn_impl_.reuse();
    EXPECT_EQ(after_row, iter->topn_impl_.after_row_);
    EXPECT_EQ(sort_context, iter->topn_impl_.mem_context_.ref_context());
    EXPECT_EQ(retained, sort_context->used());
    EXPECT_EQ(10, after_row->cells()[0].get_int());
    ObDASSortTopNEntry *output = iter->output_rows_.alloc_place_holder();
    ASSERT_NE(nullptr, output);
    ObDatum null_value;
    null_value.set_null();
    for (int64_t i = 0; i < 6; ++i) {
      ASSERT_EQ(OB_SUCCESS, output->sort_values_.push_back(null_value));
    }
    iter->saved_error_ = execution % 2 == 0 ? OB_SUCCESS : OB_ERR_UNEXPECTED;
    ASSERT_EQ(OB_SUCCESS, iter->release());
    EXPECT_EQ(nullptr, iter->topn_impl_.mem_context_);
    EXPECT_EQ(nullptr, iter->topn_impl_.after_row_);
    ASSERT_EQ(OB_SUCCESS, iter->release());
    EXPECT_EQ(nullptr, iter->topn_impl_.mem_context_);

    // Match DAS teardown: discard the body without invoking its C++ destructor.
    iter_storage.reuse();
  }
}

TEST_F(TestDASSortTopN, compare_uses_every_key_and_direction)
{
  ObDASSortTopNEntry a;
  ObDASSortTopNEntry b;
  ObDASSortTopNEntry c;
  ObDASSortTopNEntry d;
  ASSERT_EQ(OB_SUCCESS, make_entry(1, 5, 4, a));
  ASSERT_EQ(OB_SUCCESS, make_entry(2, 9, 1, b));
  ASSERT_EQ(OB_SUCCESS, make_entry(1, 6, 3, c));
  ASSERT_EQ(OB_SUCCESS, make_entry(1, 5, 5, d));
  int cmp = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, a.sort_values_, b.sort_values_, cmp));
  EXPECT_LT(cmp, 0);
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, a.sort_values_, c.sort_values_, cmp));
  EXPECT_GT(cmp, 0); // The secondary field is DESC.
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, a.sort_values_, d.sort_values_, cmp));
  EXPECT_LT(cmp, 0); // Equal user fields are ordered by the complete shard identifier.
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, a.sort_values_, a.sort_values_, cmp));
  EXPECT_EQ(0, cmp);
  sort_.sort_collations_.at(2).is_ascending_ = false;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, a.sort_values_, d.sort_values_, cmp));
  EXPECT_GT(cmp, 0);
}

TEST_F(TestDASSortTopN, compare_nulls_width_and_comparator_errors)
{
  ObDASSortTopNEntry null_entry;
  ObDASSortTopNEntry value;
  ObSEArray<ObDatum, 4> short_tuple;
  ASSERT_EQ(OB_SUCCESS, make_entry(0, 1, 1, null_entry, true));
  ASSERT_EQ(OB_SUCCESS, make_entry(10, 1, 2, value));
  int cmp = 0;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, null_entry.sort_values_, value.sort_values_, cmp));
  EXPECT_GT(cmp, 0);
  sort_.sort_collations_.at(0).is_ascending_ = false;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, null_entry.sort_values_, value.sort_values_, cmp));
  EXPECT_LT(cmp, 0);
  ASSERT_EQ(OB_SUCCESS, short_tuple.push_back(value.sort_values_.at(0)));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, short_tuple, value.sort_values_, cmp));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, value.sort_values_, short_tuple, cmp));
  sort_.sort_cmp_funcs_.at(0).cmp_func_ = compare_int_with_failure;
  comparisons_before_failure = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObDASSearchUtils::compare_hybrid_sort_values(
      sort_, value.sort_values_, value.sort_values_, cmp));
}

TEST_F(TestDASSortTopN, bounded_heap_keeps_best_rows_and_ties)
{
  ObDASSortTopNHeap heap(allocator_);
  ObSEArray<ObDASSortTopNEntry, 16> rows;
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 3, entries_));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 2, 0, 1));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 3, 2));
  EXPECT_FALSE(heap.full());
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 2, 3));
  EXPECT_TRUE(heap.full());
  EXPECT_EQ(1U, heap.top().rowid_.get_uint64());
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 3, 4));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 3, 5));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 0, 9, 6, true)); // ASC NULL LAST is noncompetitive.
  ASSERT_EQ(OB_SUCCESS, heap.drain_sorted(rows, output_allocator_));
  ASSERT_EQ(3, rows.count());
  EXPECT_EQ(2U, rows.at(0).rowid_.get_uint64());
  EXPECT_EQ(4U, rows.at(1).rowid_.get_uint64());
  EXPECT_EQ(5U, rows.at(2).rowid_.get_uint64());
  EXPECT_FALSE(heap.full());
  EXPECT_EQ(0U, entries_.outstanding());
}

TEST_F(TestDASSortTopN, descending_primary_capacity_one_and_empty_drain)
{
  sort_.sort_collations_.at(0).is_ascending_ = false;
  ObDASSortTopNHeap heap(allocator_);
  ObSEArray<ObDASSortTopNEntry, 16> rows;
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 1, entries_));
  ASSERT_EQ(OB_SUCCESS, heap.drain_sorted(rows, output_allocator_));
  EXPECT_EQ(0, rows.count());
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 0, 1));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 2, 0, 2));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 0, 3));
  ASSERT_EQ(OB_SUCCESS, heap.drain_sorted(rows, output_allocator_));
  ASSERT_EQ(1, rows.count());
  EXPECT_EQ(2U, rows.at(0).rowid_.get_uint64());
}

TEST_F(TestDASSortTopN, heap_remains_bounded_across_repeated_replacement)
{
  // Cross the heap/output inline capacities while offering substantially more candidates than K.
  const int64_t capacity = 17;
  EntryAllocator heap_allocator;
  ObDASSortTopNHeap heap(heap_allocator);
  ObSEArray<ObDASSortTopNEntry, 16> rows;
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, capacity, entries_));
  for (int64_t id = 128; id > 0; --id) {
    ASSERT_EQ(OB_SUCCESS, offer(heap, id, 0, id));
    EXPECT_LE(entries_.outstanding(), static_cast<size_t>(capacity * 4));
  }
  EXPECT_GT(heap_allocator.outstanding(), 0U);
  ASSERT_EQ(OB_SUCCESS, heap.drain_sorted(rows, output_allocator_));
  ASSERT_EQ(capacity, rows.count());
  for (int64_t i = 0; i < capacity; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 1), rows.at(i).rowid_.get_uint64());
  }
  EXPECT_EQ(0U, entries_.outstanding());
  heap.reset();
  EXPECT_EQ(0U, heap_allocator.outstanding());
}

TEST_F(TestDASSortTopN, duplicate_documents_do_not_consume_capacity)
{
  ObDASSortTopNHeap heap(allocator_);
  ObDASSortTopNEntry duplicate;
  ObSEArray<ObDASSortTopNEntry, 16> rows;
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 2, entries_));
  ASSERT_EQ(OB_SUCCESS, make_entry(1, 0, 1, duplicate));
  ASSERT_EQ(OB_SUCCESS, heap.offer(duplicate));
  ASSERT_EQ(OB_SUCCESS, heap.offer(duplicate));
  EXPECT_FALSE(heap.full());
  duplicate.raw_score_ += 1.0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, heap.offer(duplicate));
  duplicate.raw_score_ -= 1.0;
  duplicate.sort_values_.at(0).set_int(2);
  EXPECT_EQ(OB_ERR_UNEXPECTED, heap.offer(duplicate));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 2, 0, 2));
  ASSERT_EQ(OB_SUCCESS, heap.drain_sorted(rows, output_allocator_));
  ASSERT_EQ(2, rows.count());
  EXPECT_EQ(1, rows.at(0).sort_values_.at(0).get_int());
  EXPECT_DOUBLE_EQ(2.5, rows.at(0).raw_score_);
}

TEST_F(TestDASSortTopN, reject_invalid_capacity_state_and_shard_identifier)
{
  ObDASSortTopNHeap heap(allocator_);
  ObDASSortTopNEntry entry;
  ASSERT_EQ(OB_SUCCESS, make_entry(1, 0, 1, entry));
  EXPECT_EQ(OB_NOT_INIT, heap.offer(entry));
  EXPECT_EQ(OB_INVALID_ARGUMENT, heap.init(sort_, 0, entries_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, heap.init(sort_, -1, entries_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, heap.init(sort_, INT64_MAX, entries_));
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 1, entries_));
  EXPECT_EQ(OB_INVALID_ARGUMENT, heap.init(sort_, 1, entries_));
  entry.sort_values_.at(2).set_null();
  EXPECT_EQ(OB_ERR_UNEXPECTED, heap.offer(entry));
  entry.sort_values_.at(2).set_string("bad", 3);
  EXPECT_EQ(OB_ERR_UNEXPECTED, heap.offer(entry));
  entry.sort_values_.reset();
  EXPECT_EQ(OB_NOT_INIT, heap.offer(entry));
  heap.reset();
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 1, entries_));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 0, 1));
}

TEST_F(TestDASSortTopN, copied_entries_and_drained_output_survive_source_reset)
{
  ObDASSortTopNHeap heap(allocator_);
  ObDASSortTopNEntry source;
  ObDASSortTopNEntry copied;
  ObSEArray<ObDASSortTopNEntry, 16> rows;
  ASSERT_EQ(OB_SUCCESS, make_entry(7, 8, 9, source));
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::deep_copy_hybrid_sort_entry(source, copied, output_allocator_));
  for (int64_t i = 0; i < source.sort_values_.count(); ++i) {
    EXPECT_NE(source.sort_values_.at(i).ptr_, copied.sort_values_.at(i).ptr_);
  }
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 1, entries_));
  ASSERT_EQ(OB_SUCCESS, heap.offer(source));
  source.sort_values_.at(0).set_int(100);
  const_cast<char *>(source.sort_values_.at(2).ptr_)[0] = 'f';
  source_allocator_.reset();
  ASSERT_EQ(OB_SUCCESS, heap.drain_sorted(rows, output_allocator_));
  heap.reset();
  ASSERT_EQ(1, rows.count());
  EXPECT_EQ(7, rows.at(0).sort_values_.at(0).get_int());
  EXPECT_EQ('0', rows.at(0).sort_values_.at(2).ptr_[0]);
  EXPECT_EQ(7, copied.sort_values_.at(0).get_int());
  EXPECT_EQ(9U, copied.rowid_.get_uint64());
  EXPECT_DOUBLE_EQ(2.5, copied.raw_score_);
  EXPECT_DOUBLE_EQ(5.0, copied.final_score_);
}

TEST_F(TestDASSortTopN, deep_copy_preserves_null_payload)
{
  ObDASSortTopNEntry source;
  ObDASSortTopNEntry copied;
  ASSERT_EQ(OB_SUCCESS, make_entry(0, 8, 9, source, true));
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::deep_copy_hybrid_sort_entry(source, copied, output_allocator_));
  source_allocator_.reset();
  ASSERT_EQ(3, copied.sort_values_.count());
  EXPECT_TRUE(copied.sort_values_.at(0).is_null());
  EXPECT_EQ(8, copied.sort_values_.at(1).get_int());
}

TEST_F(TestDASSortTopN, failed_entry_allocations_release_partial_payloads)
{
  ObDASSortTopNHeap heap(allocator_);
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 2, entries_));
  for (int64_t successful = 0; successful < 4; ++successful) {
    entries_.fail_after(successful);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, offer(heap, 1, 0, 1));
    EXPECT_EQ(0U, entries_.outstanding());
  }
  entries_.fail_after(-1);
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 0, 1));
  heap.reset();
  EXPECT_EQ(0U, entries_.outstanding());
}

TEST_F(TestDASSortTopN, comparator_failure_is_sticky_until_reset)
{
  sort_.sort_cmp_funcs_.at(0).cmp_func_ = compare_int_with_failure;
  ObDASSortTopNHeap heap(allocator_);
  ObSEArray<ObDASSortTopNEntry, 16> rows;
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 3, entries_));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 4, 0, 1));
  comparisons_before_failure = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, offer(heap, 3, 0, 2));
  comparisons_before_failure = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, offer(heap, 2, 0, 3));
  EXPECT_EQ(OB_ERR_UNEXPECTED, heap.drain_sorted(rows, output_allocator_));
  heap.reset();
  EXPECT_EQ(0U, entries_.outstanding());
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 1, entries_));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 0, 4));
  ASSERT_EQ(OB_SUCCESS, heap.drain_sorted(rows, output_allocator_));
  ASSERT_EQ(1, rows.count());
  EXPECT_EQ(4U, rows.at(0).rowid_.get_uint64());
}

TEST_F(TestDASSortTopN, failed_replace_keeps_single_ownership_of_entries)
{
  sort_.sort_cmp_funcs_.at(0).cmp_func_ = compare_int_with_failure;
  ObDASSortTopNHeap heap(allocator_);
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 3, entries_));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 5, 0, 1));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 4, 0, 2));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 3, 0, 3));
  // The competitiveness check succeeds; comparison fails after replace_top takes ownership.
  comparisons_before_failure = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, offer(heap, 2, 0, 4));
  EXPECT_EQ(12U, entries_.outstanding()); // Three entries, each owning three non-NULL payloads.
  comparisons_before_failure = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, offer(heap, 1, 0, 5));
  heap.reset();
  EXPECT_EQ(0U, entries_.outstanding());
}

TEST_F(TestDASSortTopN, failed_pop_releases_removed_entry_and_preserves_survivors)
{
  sort_.sort_cmp_funcs_.at(0).cmp_func_ = compare_int_with_failure;
  ObDASSortTopNHeap heap(allocator_);
  ObSEArray<ObDASSortTopNEntry, 16> rows;
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 3, entries_));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 5, 0, 1));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 4, 0, 2));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 3, 0, 3));
  // drain_sorted copies the worst entry, then pop removes it before comparing the survivors.
  comparisons_before_failure = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, heap.drain_sorted(rows, output_allocator_));
  EXPECT_EQ(8U, entries_.outstanding()); // The two surviving entries remain owned by the heap.
  ASSERT_EQ(3, rows.count());
  EXPECT_EQ(1U, rows.at(2).rowid_.get_uint64()); // The partial copied output has independent storage.
  comparisons_before_failure = -1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, heap.drain_sorted(rows, output_allocator_));
  heap.reset();
  EXPECT_EQ(0U, entries_.outstanding());
  ASSERT_EQ(OB_SUCCESS, heap.init(sort_, 1, entries_));
  ASSERT_EQ(OB_SUCCESS, offer(heap, 1, 0, 4));
  ASSERT_EQ(OB_SUCCESS, heap.drain_sorted(rows, output_allocator_));
  ASSERT_EQ(1, rows.count());
  EXPECT_EQ(4U, rows.at(0).rowid_.get_uint64());
}

TEST_F(TestDASSortTopN, find_driver_accepts_only_consistent_wrapper)
{
  ObDASSearchDriverIter driver;
  ObDASSearchDriverIter other;
  ObDASSortTopNIter wrapper;
  ObDASIter *children[] = {&driver};
  EXPECT_EQ(nullptr, ObDASSearchUtils::find_search_driver_iter(nullptr));
  EXPECT_EQ(&driver, ObDASSearchUtils::find_search_driver_iter(&driver));
  EXPECT_EQ(nullptr, ObDASSearchUtils::find_search_driver_iter(&wrapper));
  wrapper.driver_ = &driver;
  wrapper.get_children() = children;
  wrapper.set_children_cnt(1);
  EXPECT_EQ(&driver, ObDASSearchUtils::find_search_driver_iter(&wrapper));
  children[0] = &other;
  EXPECT_EQ(nullptr, ObDASSearchUtils::find_search_driver_iter(&wrapper));
  wrapper.set_children_cnt(0);
  EXPECT_EQ(nullptr, ObDASSearchUtils::find_search_driver_iter(&wrapper));
}

TEST_F(TestDASSortTopN, iterator_error_and_reuse_state_without_storage)
{
  ObDASSortTopNIter iter;
  int64_t count = 99;
  EXPECT_EQ(OB_NOT_IMPLEMENT, iter.inner_get_next_row());
  EXPECT_EQ(OB_STATE_NOT_MATCH, iter.inner_get_next_rows(count, 8));
  EXPECT_EQ(0, count);
  iter.consumed_ = true;
  EXPECT_EQ(OB_STATE_NOT_MATCH, iter.inner_get_next_rows(count, 8));
  ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
  iter.consumed_ = true; // Even an empty page requires a valid execution context.
  EXPECT_EQ(OB_STATE_NOT_MATCH, iter.inner_get_next_rows(count, 8));
  EXPECT_EQ(0, count);
  EXPECT_EQ(OB_STATE_NOT_MATCH, iter.inner_get_next_rows(count, 0));
  EXPECT_EQ(OB_STATE_NOT_MATCH, iter.inner_get_next_rows(count, 8));
  ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
  EXPECT_EQ(OB_SUCCESS, iter.saved_error_);
  EXPECT_FALSE(iter.consumed_);
  EXPECT_EQ(0, iter.emitted_rows_);
}

TEST_F(TestDASSortTopN, driver_rejects_mixed_read_modes_and_invalid_score_positions)
{
  ObDASSearchDriverIter driver;
  ObDASGroupBatchInfo info;
  int64_t count = 99;
  double score = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, driver.get_current_candidate_raw_score(0, score));
  ASSERT_EQ(OB_SUCCESS, driver.scores_.init(1));
  ASSERT_EQ(OB_SUCCESS, driver.scores_.push_back(3.5));
  EXPECT_EQ(OB_ERR_UNEXPECTED, driver.get_current_candidate_raw_score(-1, score));
  EXPECT_EQ(OB_ERR_UNEXPECTED, driver.get_current_candidate_raw_score(1, score));
  ASSERT_EQ(OB_SUCCESS, driver.get_current_candidate_raw_score(0, score));
  EXPECT_DOUBLE_EQ(3.5, score);
  driver.cursor_read_started_ = true;
  EXPECT_EQ(OB_STATE_NOT_MATCH, driver.inner_get_next_rows(count, 8));
  EXPECT_EQ(0, count);
  ASSERT_EQ(OB_SUCCESS, driver.inner_reuse());
  EXPECT_FALSE(driver.cursor_read_started_);
  EXPECT_FALSE(driver.ordinary_read_started_);
  driver.ordinary_read_started_ = true;
  info.group_end_ = true;
  info.all_end_ = true;
  EXPECT_EQ(OB_STATE_NOT_MATCH, driver.read_candidate_batch(8, nullptr, count, info));
  EXPECT_EQ(0, count);
  EXPECT_FALSE(info.group_end_);
  EXPECT_FALSE(info.all_end_);
}

TEST_F(TestDASSortTopN, sync_lookup_binding_uses_root_tablet_without_async_partition)
{
  ObDASScanOp scan(allocator_);
  scan.get_scan_param().tablet_id_ = ObTabletID(1002);
  ObDASSearchCtx ctx(allocator_, scan);
  ctx.set_ls_id(share::ObLSID(1001));
  EXPECT_FALSE(ctx.is_async_index_query());
  EXPECT_EQ(nullptr, ctx.get_current_async_partition());
  EXPECT_EQ(scan.get_scan_param().tablet_id_, ctx.get_main_tablet_id());

  ObDASSortValueLookupIter lookup;
  lookup.search_ctx_ = &ctx;
  ASSERT_EQ(OB_SUCCESS, lookup.prepare_batch_binding());
  EXPECT_FALSE(lookup.lookup_param_.need_switch_param_);
  EXPECT_EQ(scan.get_scan_param().tablet_id_, lookup.lookup_tablet_id_);
  EXPECT_EQ(ctx.get_ls_id(), lookup.lookup_ls_id_);

  lookup.is_first_lookup_ = false;
  lookup.lookup_param_.ls_id_ = ctx.get_ls_id();
  lookup.lookup_param_.tablet_id_ = scan.get_scan_param().tablet_id_;
  ASSERT_EQ(OB_SUCCESS, lookup.prepare_batch_binding());
  EXPECT_FALSE(lookup.lookup_param_.need_switch_param_);
  ctx.set_ls_id(share::ObLSID(2001));
  ASSERT_EQ(OB_SUCCESS, lookup.prepare_batch_binding());
  EXPECT_TRUE(lookup.lookup_param_.need_switch_param_);
  EXPECT_EQ(ctx.get_ls_id(), lookup.lookup_ls_id_);
}

TEST_F(TestDASSortTopN, lookup_binding_distinguishes_first_scan_and_snapshot_switch)
{
  ObDASScanOp scan(allocator_);
  scan.get_scan_param().tablet_id_ = ObTabletID(1002);
  ObDASSearchCtx ctx(allocator_, scan);
  ObDASAsyncIndexQueryCtx async;
  ObAsyncIndexPartitionSegCtx partition;
  partition.main_tablet_id_ = ObTabletID(2002);
  ASSERT_EQ(OB_SUCCESS, partition.storage_group_ctx_.async_index_read_snapshot_.convert_for_tx(100));
  ctx.set_async_query_ctx(&async);
  ctx.set_current_async_partition(&partition);
  ctx.set_ls_id(share::ObLSID(2001));
  EXPECT_EQ(partition.main_tablet_id_, ctx.get_main_tablet_id());
  ObDASSortValueLookupIter lookup;
  lookup.search_ctx_ = &ctx;
  lookup.lookup_param_.ls_id_ = share::ObLSID(1001);
  lookup.lookup_param_.tablet_id_ = ObTabletID(1002);
  lookup.lookup_param_.snapshot_.specify_snapshot_scn(partition.get_read_snapshot());

  ASSERT_TRUE(lookup.is_first_lookup_);
  ASSERT_EQ(OB_SUCCESS, lookup.prepare_batch_binding());
  EXPECT_FALSE(lookup.lookup_param_.need_switch_param_);
  EXPECT_EQ(partition.main_tablet_id_, lookup.lookup_tablet_id_);
  EXPECT_EQ(ctx.get_ls_id(), lookup.lookup_ls_id_);

  lookup.is_first_lookup_ = false;
  ASSERT_EQ(OB_SUCCESS, lookup.prepare_batch_binding());
  EXPECT_TRUE(lookup.lookup_param_.need_switch_param_); // Existing scanner changes LS.
  lookup.lookup_param_.ls_id_ = ctx.get_ls_id();
  lookup.lookup_param_.tablet_id_ = partition.main_tablet_id_;
  lookup.lookup_param_.need_switch_param_ = false;
  ASSERT_EQ(OB_SUCCESS, lookup.prepare_batch_binding());
  EXPECT_FALSE(lookup.lookup_param_.need_switch_param_); // Same binding reuses ranges.

  ASSERT_EQ(OB_SUCCESS, partition.storage_group_ctx_.async_index_read_snapshot_.convert_for_tx(101));
  ASSERT_EQ(OB_SUCCESS, lookup.prepare_batch_binding());
  EXPECT_TRUE(lookup.lookup_param_.need_switch_param_); // Same tablet, different MVCC snapshot.
  ctx.set_current_async_partition(nullptr);
  EXPECT_FALSE(ctx.get_main_tablet_id().is_valid());
  EXPECT_EQ(OB_ERR_UNEXPECTED, lookup.prepare_batch_binding());
}

TEST_F(TestDASSortTopN, reuse_after_failure_preserves_lookup_and_fixed_expression_layout)
{
  ObExecContext exec(allocator_);
  ObDASSortTopNIter iter;
  ASSERT_EQ(OB_SUCCESS, iter.lookup_.candidate_indices_.create(16, ObMemAttr(MTL_ID(), "SortMapTest")));
  void *buf = iter.lookup_.allocator_.alloc(sizeof(ObEvalCtx));
  ASSERT_NE(nullptr, buf);
  iter.lookup_.private_eval_ctx_ = new (buf) ObEvalCtx(exec, &iter.lookup_.allocator_);
  iter.sort_eval_ctx_ = iter.lookup_.private_eval_ctx_;
  ObEvalCtx *private_eval_ctx = iter.sort_eval_ctx_;
  ASSERT_EQ(OB_SUCCESS, iter.sort_exprs_.push_back(&primary_expr_));
  iter.lookup_.is_first_lookup_ = false; // Parent consumes this before opening storage.
  iter.saved_error_ = OB_ERR_UNEXPECTED;
  ASSERT_EQ(OB_SUCCESS, iter.inner_reuse());
  EXPECT_EQ(OB_SUCCESS, iter.saved_error_);
  EXPECT_FALSE(iter.lookup_.is_first_lookup_);
  EXPECT_TRUE(iter.lookup_.candidate_indices_.created());
  EXPECT_EQ(private_eval_ctx, iter.lookup_.private_eval_ctx_);
  EXPECT_EQ(private_eval_ctx, iter.sort_eval_ctx_);
  ASSERT_EQ(1, iter.sort_exprs_.count());
  EXPECT_EQ(&primary_expr_, iter.sort_exprs_.at(0));
  EXPECT_TRUE(iter.reset_exprs_.empty());
  EXPECT_TRUE(iter.visited_exprs_.empty());
}

struct CountedSortKey
{
  ObDatum value_;
  int64_t calls_ = 0;
  int error_ = OB_SUCCESS;
};

int eval_counted_sort_key(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &result)
{
  CountedSortKey &key = *reinterpret_cast<CountedSortKey *>(expr.extra_);
  ++key.calls_;
  int ret = key.error_;
  if (OB_SUCC(ret)) {
    result = key.value_;
  }
  return ret;
}

class TestDASHybridLazySort : public TestDASSortTopN
{
public:
  TestDASHybridLazySort() : exec_(allocator_), eval_(exec_) {}

  void SetUp() override
  {
    TestDASSortTopN::SetUp();
    eval_.max_batch_size_ = 256;
    char **frames = static_cast<char **>(allocator_.alloc(3 * sizeof(char *)));
    ASSERT_NE(nullptr, frames);
    for (int64_t i = 0; i < 3; ++i) {
      ObExpr &expr = *sort_.sort_exprs_.at(i);
      expr.frame_idx_ = i;
      expr.batch_result_ = true;
      expr.batch_idx_mask_ = UINT64_MAX;
      expr.datum_off_ = 0;
      expr.eval_info_off_ = 256 * sizeof(ObDatum);
      expr.eval_flags_off_ = expr.eval_info_off_ + sizeof(ObEvalInfo);
      expr.res_buf_off_ = expr.eval_flags_off_ + ObBitVector::memory_size(256);
      expr.res_buf_len_ = 32;
      expr.vector_header_off_ = UINT32_MAX; // Exercise the ordinary datum evaluator.
      const int64_t bytes = expr.res_buf_off_ + 256 * expr.res_buf_len_;
      frames[i] = static_cast<char *>(allocator_.alloc(bytes));
      ASSERT_NE(nullptr, frames[i]);
      MEMSET(frames[i], 0, bytes);
      expr.eval_func_ = eval_counted_sort_key;
      expr.extra_ = reinterpret_cast<uint64_t>(&keys_[i]);
      ASSERT_EQ(OB_SUCCESS, iter_.reset_exprs_.push_back(&expr));
    }
    exec_.frames_ = frames;
    exec_.frame_cnt_ = 3;
    eval_.frames_ = frames;
    iter_.sort_eval_ctx_ = &eval_;
    ASSERT_EQ(OB_SUCCESS, topn_.after_cmp_.init(
        &sort_.sort_collations_, &sort_.sort_cmp_funcs_, &exec_, false));
    ObDASSortTopNEntry cursor;
    ASSERT_EQ(OB_SUCCESS, make_entry(10, 50, 100, cursor));
    const int64_t bytes = ObChunkDatumStore::Block::row_store_size(cursor.sort_values_.get_data(), 3);
    ASSERT_EQ(OB_SUCCESS, create_sort_context(topn_));
    void *buf = topn_.mem_context_->get_malloc_allocator().alloc(bytes);
    ASSERT_NE(nullptr, buf);
    topn_.after_row_ = new (buf) ObChunkDatumStore::StoredRow();
    ASSERT_EQ(OB_SUCCESS, topn_.after_row_->copy_shadow_datums(
        cursor.sort_values_.get_data(), 3, reinterpret_cast<char *>(topn_.after_row_->cells()),
        bytes - sizeof(*topn_.after_row_), bytes, 0));
  }

  void candidate(int64_t first, int64_t second, uint64_t id, bool null_first = false)
  {
    ObDASSortTopNEntry row;
    ASSERT_EQ(OB_SUCCESS, make_entry(first, second, id, row, null_first));
    for (int64_t i = 0; i < 3; ++i) {
      keys_[i].value_ = row.sort_values_.at(i);
      keys_[i].calls_ = 0;
    }
    iter_.invalidate_sort_exprs();
  }

  ObExecContext exec_;
  ObEvalCtx eval_;
  CountedSortKey keys_[3];
  ObDASSortTopNIter iter_;
  ObDASHybridTopNImpl topn_;
};

TEST_F(TestDASHybridLazySort, reuse_preserves_cursor_and_comparisons_and_clears_execution_state)
{
  ASSERT_EQ(OB_SUCCESS, topn_.comp_.init(
      &sort_.sort_collations_, &sort_.sort_cmp_funcs_, &exec_, false));
  const auto *after_row = topn_.after_row_;
  const auto *sort_context = topn_.mem_context_.ref_context();
  topn_.topn_cnt_ = 3;
  topn_.outputted_rows_cnt_ = 3;
  topn_.got_first_row_ = true;
  topn_.sorted_ = true;
  topn_.comp_.ret_ = OB_ERR_UNEXPECTED;
  topn_.comp_.cmp_count_ = 17;
  topn_.after_cmp_.ret_ = OB_ERR_UNEXPECTED;
  topn_.after_cmp_.cmp_count_ = 23;

  topn_.reuse();
  EXPECT_EQ(after_row, topn_.after_row_);
  EXPECT_EQ(sort_context, topn_.mem_context_.ref_context());
  EXPECT_EQ(3, topn_.topn_cnt_);
  EXPECT_EQ(0, topn_.outputted_rows_cnt_);
  EXPECT_FALSE(topn_.got_first_row_);
  EXPECT_FALSE(topn_.sorted_);
  EXPECT_EQ(OB_SUCCESS, topn_.comp_.get_error_code());
  EXPECT_EQ(OB_SUCCESS, topn_.after_cmp_.get_error_code());
  EXPECT_EQ(0, topn_.comp_.cmp_count_);
  EXPECT_EQ(0, topn_.after_cmp_.cmp_count_);
  EXPECT_EQ(&sort_.sort_collations_, topn_.comp_.sort_collations_);
  EXPECT_EQ(&sort_.sort_cmp_funcs_, topn_.comp_.sort_cmp_funs_);
  EXPECT_EQ(&sort_.sort_collations_, topn_.after_cmp_.sort_collations_);
  EXPECT_EQ(&sort_.sort_cmp_funcs_, topn_.after_cmp_.sort_cmp_funs_);

  ObEvalCtx::BatchInfoScopeGuard guard(eval_);
  guard.set_batch_size(1);
  guard.set_batch_idx(0);
  bool passes = true;
  candidate(10, 50, 100);
  ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_FALSE(passes);
  candidate(10, 49, 100); // The retained second comparison remains DESC.
  ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_TRUE(passes);
}

TEST_F(TestDASHybridLazySort, strict_after_short_circuits_and_reuses_prefix)
{
  ObEvalCtx::BatchInfoScopeGuard guard(eval_);
  guard.set_batch_size(1);
  guard.set_batch_idx(0);
  bool passes = false;
  candidate(11, 0, 1);
  ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_TRUE(passes);
  EXPECT_EQ(1, keys_[0].calls_);
  EXPECT_EQ(0, keys_[1].calls_);
  EXPECT_EQ(0, keys_[2].calls_);
  // The parent TopN comparator sees the same frame and reuses the after prefix.
  ObSortOpImpl::Compare compare;
  ASSERT_EQ(OB_SUCCESS, compare.init(&sort_.sort_collations_, &sort_.sort_cmp_funcs_, &exec_, false));
  EXPECT_FALSE(compare(&sort_.sort_exprs_, topn_.after_row_, eval_));
  EXPECT_EQ(OB_SUCCESS, compare.get_error_code());
  EXPECT_EQ(1, keys_[0].calls_);

  candidate(9, 0, 1);
  ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_FALSE(passes);
  EXPECT_EQ(0, keys_[1].calls_);
  candidate(10, 49, 1); // Second key is DESC.
  ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_TRUE(passes);
  EXPECT_EQ(1, keys_[1].calls_);
  EXPECT_EQ(0, keys_[2].calls_);
  candidate(10, 50, 100); // Complete tuple equal: strict after excludes it.
  ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_FALSE(passes);
  EXPECT_EQ(1, keys_[2].calls_);
  candidate(10, 50, 101);
  ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_TRUE(passes);
  candidate(0, 0, 1, true); // NULLS LAST on the ASC first key.
  ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_TRUE(passes);
  EXPECT_EQ(0, keys_[1].calls_);
}

TEST_F(TestDASHybridLazySort, invalidate_whole_batch_after_partial_evaluation)
{
  ObEvalCtx::BatchInfoScopeGuard guard(eval_);
  bool passes = false;
  const int64_t sizes[] = {256, 17, 256};
  const int64_t values[] = {11, 9, 12};
  for (int64_t b = 0; b < 3; ++b) {
    candidate(values[b], 0, 1);
    guard.set_batch_size(sizes[b]);
    for (int64_t index : {int64_t(0), sizes[b] - 1}) {
      guard.set_batch_idx(index);
      ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
      EXPECT_EQ(values[b] > 10, passes);
      ASSERT_EQ(OB_SUCCESS, topn_.is_after(sort_.sort_exprs_, eval_, passes));
    }
    EXPECT_EQ(2, keys_[0].calls_); // Each visited position once, including reused pos 255.
    EXPECT_EQ(0, keys_[1].calls_);
    EXPECT_EQ(0, keys_[2].calls_);
  }
}

TEST_F(TestDASHybridLazySort, evaluation_error_is_not_treated_as_equal_or_after)
{
  ObEvalCtx::BatchInfoScopeGuard guard(eval_);
  guard.set_batch_size(1);
  guard.set_batch_idx(0);
  candidate(11, 0, 1);
  keys_[0].error_ = OB_ERR_UNEXPECTED;
  bool passes = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_FALSE(passes);
  keys_[0].error_ = OB_SUCCESS;
  EXPECT_EQ(OB_ERR_UNEXPECTED, topn_.is_after(sort_.sort_exprs_, eval_, passes));
  EXPECT_EQ(1, keys_[0].calls_); // Comparator preserves the first failure.
}

} // namespace

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
