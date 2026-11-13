/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_DAS

#include <gtest/gtest.h>
#include <cstdlib>
#include <algorithm>
#include <memory>
#include <time.h>
#include "lib/utility/ob_sort.h"
#include "lib/utility/utility.h"
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#ifdef CONST
#undef CONST
#endif
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_vec_define.h"
#include "sql/das/iter/ob_das_vec_index_scan_iter.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_das_async_segment_merge_op.h"
#include "sql/das/search/ob_das_async_segment_group_merge_op.h"
#include "sql/das/search/ob_das_async_token_op.h"
#include "sql/das/search/ob_das_async_token_helper.h"
#include "sql/das/search/ob_das_match_phrase_op.h"
#include "sql/das/search/ob_das_match_phrase_query.h"
#include "sql/das/search/ob_das_fts_cost_utils.h"
#include "sql/hybrid_search/ob_fulltext_search_node.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_writer.h"
#include "storage/retrieval/ob_phrase_match_counter.h"
#include "share/ob_fts_pos_list_codec.h"
#include "plugin/legacy/sys/ob_plugin_mgr.h"
#include "share/ob_server_struct.h"
#include "sql/das/search/ob_das_async_scalar_range_op.h"
#include "sql/das/search/ob_das_bitmap_op.h"
#include "sql/das/search/ob_das_sort_op.h"
#include "sql/das/search/ob_das_async_scalar_ror_op.h"
#include "sql/das/search/ob_das_boolean_query.h"
#include "sql/das/search/ob_das_bmm_op.h"
#include "sql/das/search/ob_das_topk_collect_op.h"
#include "sql/das/search/ob_das_match_query.h"
#include "sql/das/search/ob_das_multi_match_query.h"
#include "sql/das/search/ob_das_nested_query_op.h"
#include "sql/das/search/ob_das_query_string_query.h"
#include "sql/das/search/ob_das_search_utils.h"
#include "sql/das/search/ob_das_conjunction_op.h"
#include "sql/das/search/ob_das_disjunction_op.h"
#include "sql/das/search/ob_das_disjunction_filter_op.h"
#include "sql/das/search/ob_das_dummy_op.h"
#include "sql/das/search/ob_das_req_excl_op.h"
#include "sql/das/search/ob_das_req_opt_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "storage/blocksstable/async_index/ob_posting_value_block_writer.h"
#include "storage/tablet/ob_tablet.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/px/ob_sqc_ctx.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/session/ob_sql_session_info.h"
#include "storage/tx_storage/ob_access_service.h"
#undef protected
#undef private
#include "test_das_bmm_common.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::sql::test;

namespace oceanbase
{
namespace unittest
{

static const uint64_t TEST_INDEX_TABLE_ID = 9001;
static const uint64_t TEST_INDEX_TABLE_ID_2 = 9002;

// Borrow storage objects only for the fixture lifetime; production lookup still
// resolves the logical version range through the pinned tablet's table store.
class PinnedSegmentTables
{
public:
  TO_STRING_KV(K_(table_count));
  explicit PinnedSegmentTables(ObArenaAllocator &allocator)
    : allocator_(allocator), handle_(nullptr), tablet_(), store_(), meta_(), tables_(nullptr),
      major_tables_(nullptr), inc_tables_(nullptr), table_count_(0)
  {}
  ~PinnedSegmentTables()
  {
    if (nullptr != handle_) {
      handle_->obj_ = nullptr;
      handle_->allocator_ = nullptr;
    }
    tablet_.table_store_addr_.ptr_ = nullptr;
    store_.major_tables_.sstable_array_ = nullptr;
    store_.major_tables_.cnt_ = 0;
    store_.inc_major_tables_.sstable_array_ = nullptr;
    store_.inc_major_tables_.cnt_ = 0;
    for (int64_t i = 0; i < table_count_; ++i) {
      tables_[i].meta_ = nullptr;
      tables_[i].~ObInvSSTable();
    }
    if (nullptr != tables_) {
      allocator_.free(tables_);
    }
    if (nullptr != major_tables_) {
      allocator_.free(major_tables_);
    }
    if (nullptr != inc_tables_) {
      allocator_.free(inc_tables_);
    }
  }

  int init(ObAsyncIndexPartitionSegCtx &partition,
           const uint64_t *min_ids, const uint64_t *max_ids, const int64_t count,
           const int64_t tablet_idx = 0)
  {
    int ret = OB_SUCCESS;
    int64_t table_idx = 0;
    if (nullptr != tables_) {
      ret = OB_INIT_TWICE;
    } else if (nullptr == min_ids || nullptr == max_ids || count <= 0
        || count > INT64_MAX / sizeof(blocksstable::ObInvSSTable)
        || tablet_idx < 0 || tablet_idx >= partition.index_tablet_ids_.count()
        || tablet_idx >= partition.storage_group_ctx_.index_tablet_handles_.count()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (nullptr == (tables_ = static_cast<blocksstable::ObInvSSTable *>(
                   allocator_.alloc(count * sizeof(*tables_))))
        || nullptr == (major_tables_ = static_cast<blocksstable::ObSSTable **>(
                   allocator_.alloc(count * sizeof(*major_tables_))))
        || nullptr == (inc_tables_ = static_cast<blocksstable::ObSSTable **>(
                   allocator_.alloc(count * sizeof(*inc_tables_))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      for (int64_t i = 0; i < count; ++i) {
        new (&tables_[i]) blocksstable::ObInvSSTable();
        ++table_count_;
      }
      tablet_.is_inited_ = true;
      tablet_.tablet_meta_.tablet_id_ = partition.index_tablet_ids_.at(tablet_idx);
      tablet_.tablet_meta_.ls_id_ = partition.ls_id_;
      tablet_.table_store_addr_.ptr_ = &store_;
      store_.is_inited_ = true;
      store_.major_tables_.is_inited_ = true;
      store_.major_tables_.sstable_array_ = major_tables_;
      store_.inc_major_tables_.is_inited_ = true;
      store_.inc_major_tables_.sstable_array_ = inc_tables_;
    }
    for (int64_t group_idx = 0; OB_SUCC(ret)
         && group_idx < partition.storage_group_ctx_.get_group_count(); ++group_idx) {
      const ObIArray<ObNewVersionRange> &ranges = partition.storage_group_ctx_
          .get_group(group_idx)->get_tablet_version_ranges(tablet_idx);
      for (int64_t range_idx = 0; OB_SUCC(ret) && range_idx < ranges.count(); ++range_idx) {
        if (table_idx >= count) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          blocksstable::ObInvSSTable &table = tables_[table_idx];
          table.key_.tablet_id_ = partition.index_tablet_ids_.at(tablet_idx);
          table.key_.table_type_ = 0 == ranges.at(range_idx).base_version_
              ? storage::ObITable::INV_INDEX_SSTABLE : storage::ObITable::INC_INV_INDEX_SSTABLE;
          if (0 == ranges.at(range_idx).base_version_) {
            table.key_.version_range_ = ranges.at(range_idx);
          } else {
            // IncMajor TableKey stores physical CLOG SCNs; the Segment catalog
            // and query path identify it by the logical merged-version range.
            table.key_.version_range_.base_version_ = 1000000 + table_idx * 100;
            table.key_.version_range_.snapshot_version_ =
                table.key_.version_range_.base_version_ + 50;
            table.meta_cache_.min_merged_trans_version_ = ranges.at(range_idx).base_version_;
            table.meta_cache_.max_merged_trans_version_ = ranges.at(range_idx).snapshot_version_;
            table.meta_cache_.upper_trans_version_ = ranges.at(range_idx).snapshot_version_;
          }
          table.valid_for_reading_ = true;
          table.meta_ = &meta_;
          table.meta_cache_.data_macro_block_count_ = min_ids[table_idx] <= max_ids[table_idx]
              ? 1 : 0;
          blocksstable::ObInvIdxStatsMeta inv_stats;
          inv_stats.index_type_ = blocksstable::INV_IDX_TYPE_SEARCH;
          inv_stats.doc_id_obj_meta_.set_uint64();
          inv_stats.min_doc_id_ = min_ids[table_idx];
          inv_stats.max_doc_id_ = max_ids[table_idx];
          if (OB_FAIL(table.inv_meta_.init(inv_stats))) {
            LOG_WARN("failed to initialize inverted sstable metadata", K(ret));
          } else if (0 == ranges.at(range_idx).base_version_) {
            major_tables_[store_.major_tables_.cnt_++] = &table;
          } else {
            inc_tables_[store_.inc_major_tables_.cnt_++] = &table;
          }
          ++table_idx;
        }
      }
    }
    if (OB_SUCC(ret) && table_idx != count) {
      ret = OB_INVALID_ARGUMENT;
    }
    if (OB_SUCC(ret)) {
      handle_ = &partition.storage_group_ctx_.index_tablet_handles_.at(tablet_idx);
      handle_->type_ = storage::ObTabletHandle::ObTabletHdlType::STANDALONE;
      handle_->obj_ = &tablet_;
      handle_->allocator_ = &allocator_;
    }
    return ret;
  }

  ObArenaAllocator &allocator_;
  storage::ObTabletHandle *handle_;
  storage::ObTablet tablet_;
  storage::ObTabletTableStore store_;
  blocksstable::ObSSTableMeta meta_;
  blocksstable::ObInvSSTable *tables_;
  blocksstable::ObSSTable **major_tables_;
  blocksstable::ObSSTable **inc_tables_;
  int64_t table_count_;
};

// Keep legacy heap tests on overlapping, valid physical envelopes in every partition/index.
// Declare after the catalog so borrowed handles are detached before the catalog is destroyed.
class PinnedOverlappingCatalog
{
public:
  explicit PinnedOverlappingCatalog(ObArenaAllocator &allocator) : allocator_(allocator) {}
  ~PinnedOverlappingCatalog()
  {
    for (int64_t i = 0; i < storage_.count(); ++i) {
      storage_.at(i)->~PinnedSegmentTables();
      allocator_.free(storage_.at(i));
    }
  }

  int init(ObDASAsyncIndexQueryCtx &catalog)
  {
    int ret = OB_SUCCESS;
    ObArray<uint64_t> min_ids;
    ObArray<uint64_t> max_ids;
    for (int64_t p = 0; OB_SUCC(ret) && p < catalog.partitions_.count(); ++p) {
      ObAsyncIndexPartitionSegCtx &partition = *catalog.partitions_.at(p);
      for (int64_t t = 0; OB_SUCC(ret) && t < partition.index_table_ids_.count(); ++t) {
        int64_t count = 0;
        PinnedSegmentTables *storage = nullptr;
        void *buf = nullptr;
        min_ids.reuse();
        max_ids.reuse();
        if (OB_FAIL(partition.get_index_segment_cnt(partition.index_table_ids_.at(t), count))) {
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          if (OB_FAIL(min_ids.push_back(0))) {
          } else if (OB_FAIL(max_ids.push_back(UINT64_MAX))) {
          }
        }
        if (OB_FAIL(ret)) {
        } else if (0 == count) {
          // Empty domains have no physical tables to pin.
        } else if (nullptr == (buf = allocator_.alloc(sizeof(PinnedSegmentTables)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          storage = new (buf) PinnedSegmentTables(allocator_);
          if (OB_FAIL(storage_.push_back(storage))) {
            storage->~PinnedSegmentTables();
            allocator_.free(storage);
          } else if (OB_FAIL(storage->init(
                         partition, min_ids.get_data(), max_ids.get_data(), count, t))) {
          } else if (!storage->handle_->is_valid()) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
        // Exercise the production lookup, not just the handle's superficial validity.
        for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          bool available = false;
          uint64_t min_id = UINT64_MAX;
          uint64_t max_id = 0;
          ObNewVersionRange logical_range;
          if (OB_FAIL(storage->tables_[i].get_async_index_version_range(logical_range))) {
          } else if (OB_FAIL(partition.get_segment_rowid_range(
                         partition.index_table_ids_.at(t), logical_range,
                         available, min_id, max_id))) {
          } else if (!available || 0 != min_id || UINT64_MAX != max_id) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
      }
    }
    return ret;
  }

private:
  ObArenaAllocator &allocator_;
  ObSEArray<PinnedSegmentTables *, 4> storage_;
};

struct MockSegmentOpParam : public ObIDASSearchOpParam
{
  MockSegmentOpParam() : ObIDASSearchOpParam(DAS_SEARCH_OP_DUMMY) {}
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    UNUSED(children);
    return OB_SUCCESS;
  }
};

class AsyncTermsDirectDrainConfigGuard
{
public:
  explicit AsyncTermsDirectDrainConfigGuard(const bool enabled = true)
    : tenant_config_(TENANT_CONF(MTL_ID())), old_value_(false), is_valid_(tenant_config_.is_valid())
  {
    if (is_valid_) {
      old_value_ = tenant_config_->_enable_bmm_vectorized_merge;
      tenant_config_->_enable_bmm_vectorized_merge = enabled;
    }
  }

  ~AsyncTermsDirectDrainConfigGuard()
  {
    if (is_valid_) {
      tenant_config_->_enable_bmm_vectorized_merge = old_value_;
    }
  }

  bool is_valid() const { return is_valid_; }

private:
  omt::ObTenantConfigGuard tenant_config_;
  bool old_value_;
  bool is_valid_;
};

class CountingFailAllocator : public ObIAllocator
{
public:
  CountingFailAllocator()
    : fail_at_(-1), alloc_attempts_(0), outstanding_allocations_(0)
  {}
  ~CountingFailAllocator() override {}

  void set_fail_at(const int64_t fail_at) { fail_at_ = fail_at; }
  int64_t alloc_attempts() const { return alloc_attempts_; }
  int64_t outstanding_allocations() const { return outstanding_allocations_; }

  void *alloc(const int64_t size) override
  {
    return alloc_impl(size);
  }

  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc_impl(size);
  }

  void free(void *ptr) override
  {
    if (nullptr != ptr) {
      std::free(ptr);
      --outstanding_allocations_;
    }
  }

private:
  void *alloc_impl(const int64_t size)
  {
    void *ptr = nullptr;
    ++alloc_attempts_;
    if (alloc_attempts_ != fail_at_) {
      ptr = std::malloc(size);
      if (nullptr != ptr) {
        ++outstanding_allocations_;
      }
    }
    return ptr;
  }

private:
  int64_t fail_at_;
  int64_t alloc_attempts_;
  int64_t outstanding_allocations_;
};

class MockFailPostingAccessService : public storage::ObAccessService
{
public:
  explicit MockFailPostingAccessService(const int open_ret)
    : open_ret_(open_ret), open_count_(0)
  {}

  int async_index_posting_scan(
      const share::ObLSID &ls_id,
      const storage::ObTabletHandle *pinned_tablet_handle,
      const ObNewVersionRange &version_range,
      ObTableScanParam &scan_param,
      blocksstable::ObInvPostingScanParam &posting_param,
      blocksstable::ObInvSSTablePostingIter *&iter) override
  {
    UNUSEDx(ls_id, pinned_tablet_handle, version_range, scan_param, posting_param);
    ++open_count_;
    iter = nullptr;
    return open_ret_;
  }

  int open_ret_;
  int64_t open_count_;
};

class MockRangeAccessService : public storage::ObAccessService
{
public:
  MockRangeAccessService()
    : open_count_(0), doc_ids_{101, 102}, sstable_()
  {
    sstable_.inv_meta_.stats_.index_type_ = blocksstable::INV_IDX_TYPE_SEARCH;
  }

  int async_index_range_scan(
      const share::ObLSID &ls_id,
      const storage::ObTabletHandle *pinned_tablet_handle,
      const ObNewVersionRange &version_range,
      ObTableScanParam &scan_param,
      blocksstable::ObInvRangeScanParam &range_param,
      blocksstable::ObInvSSTableRangeIter *&iter) override
  {
    int ret = OB_SUCCESS;
    UNUSEDx(ls_id, pinned_tablet_handle, version_range, scan_param);
    iter = OB_NEWx(blocksstable::ObInvSSTableRangeIter, range_param.allocator_);
    if (OB_ISNULL(iter)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      iter->is_opened_ = true;
      iter->is_end_ = 0 == open_count_;
      iter->enable_doc_id_drain_ = true;
      iter->sstable_ = &sstable_;
      if (!iter->is_end_) {
        iter->prepared_doc_ids_ = doc_ids_;
        iter->prepared_doc_count_ = ARRAYSIZEOF(doc_ids_);
      }
      ++open_count_;
    }
    return ret;
  }

  int64_t open_count_;
  uint64_t doc_ids_[2];
  blocksstable::ObInvSSTable sstable_;
};

class TrackedAsyncSegmentMergeOp : public ObDASAsyncSegmentMergeOp
{
public:
  explicit TrackedAsyncSegmentMergeOp(ObDASSearchCtx &search_ctx)
    : ObDASAsyncSegmentMergeOp(search_ctx),
      allocator_(search_ctx.get_allocator()),
      tracked_buf_(allocator_.alloc(8))
  {}
  ~TrackedAsyncSegmentMergeOp() override
  {
    if (nullptr != tracked_buf_) {
      allocator_.free(tracked_buf_);
      tracked_buf_ = nullptr;
    }
  }

private:
  ObIAllocator &allocator_;
  void *tracked_buf_;
};

struct FailingGetChildrenSegmentParam : public ObDASAsyncSegmentMergeOpParam
{
  explicit FailingGetChildrenSegmentParam(const ObIArray<ObIDASSearchOp *> &segment_ops)
    : ObDASAsyncSegmentMergeOpParam(
          segment_ops, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID))
  {}
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    UNUSED(children);
    return OB_ERR_UNEXPECTED;
  }
};

class MockSegmentOp : public ObIDASSearchOp
{
public:
  MockSegmentOp(ObDASSearchCtx &search_ctx,
                const std::vector<uint64_t> &rowids,
                const std::vector<double> &scores)
    : ObIDASSearchOp(search_ctx), rowids_(rowids), scores_(scores), pos_(-1),
      bind_count_(0), last_range_base_(-1), last_threshold_(-1.0),
      shallow_count_(0), open_count_(0), rescan_count_(0), release_count_(0), advance_count_(0),
      drain_count_(0), max_drain_capacity_(0), advance_ret_(OB_SUCCESS),
      next_ret_(OB_SUCCESS), drain_ret_(OB_SUCCESS), shallow_ret_(OB_SUCCESS),
      last_advance_target_(),
      in_shallow_status_(false)
  {
    op_type_ = DAS_SEARCH_OP_DUMMY;
  }
  int64_t bind_count() const { return bind_count_; }
  int64_t last_range_base() const { return last_range_base_; }
  double last_threshold() const { return last_threshold_; }
  int64_t shallow_count() const { return shallow_count_; }
  int64_t open_count() const { return open_count_; }
  int64_t rescan_count() const { return rescan_count_; }
  int64_t release_count() const { return release_count_; }
  void release_scan_resources() override
  {
    ++release_count_;
    ObIDASSearchOp::release_scan_resources();
  }
  int64_t advance_count() const { return advance_count_; }
  int64_t drain_count() const { return drain_count_; }
  int64_t max_drain_capacity() const { return max_drain_capacity_; }
  void set_advance_ret(const int advance_ret) { advance_ret_ = advance_ret; }
  void set_next_ret(const int ret) { next_ret_ = ret; }
  void set_drain_ret(const int ret) { drain_ret_ = ret; }
  void set_shallow_ret(const int ret) { shallow_ret_ = ret; }
  const ObDASRowID &last_advance_target() const { return last_advance_target_; }

private:
  int do_open() override
  {
    ++open_count_;
    pos_ = -1;
    advance_count_ = 0;
    drain_count_ = 0;
    max_drain_capacity_ = 0;
    in_shallow_status_ = false;
    return OB_SUCCESS;
  }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override
  {
    ++rescan_count_;
    pos_ = -1;
    advance_count_ = 0;
    drain_count_ = 0;
    max_drain_capacity_ = 0;
    in_shallow_status_ = false;
    return OB_SUCCESS;
  }
  int do_rescan_segment(const ObNewVersionRange &range) override
  {
    pos_ = -1;
    advance_count_ = 0;
    drain_count_ = 0;
    max_drain_capacity_ = 0;
    in_shallow_status_ = false;
    ++bind_count_;
    last_range_base_ = range.base_version_;
    return OB_SUCCESS;
  }
  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    int ret = OB_SUCCESS;
    if (OB_SUCCESS != next_ret_) {
      ret = next_ret_;
    } else if (in_shallow_status_) {
      ret = OB_NOT_SUPPORTED;
    } else if (++pos_ >= static_cast<int64_t>(rowids_.size())) {
      rowid.set_max();
      ret = OB_ITER_END;
    } else {
      rowid.set_uint64(rowids_.at(pos_));
      score = scores_.at(pos_);
    }
    return ret;
  }
  int do_advance_to(const ObDASRowID &target, ObDASRowID &rowid, double &score) override
  {
    int ret = OB_SUCCESS;
    const uint64_t target_value = target.is_min() ? 0 : target.get_uint64();
    ++advance_count_;
    last_advance_target_ = target;
    in_shallow_status_ = false;
    if (OB_SUCCESS != advance_ret_) {
      ret = advance_ret_;
    } else {
      if (pos_ < 0) {
        pos_ = 0;
      }
      while (pos_ < static_cast<int64_t>(rowids_.size()) && rowids_.at(pos_) < target_value) {
        ++pos_;
      }
      if (pos_ >= static_cast<int64_t>(rowids_.size())) {
        rowid.set_max();
        ret = OB_ITER_END;
      } else {
        rowid.set_uint64(rowids_.at(pos_));
        score = scores_.at(pos_);
      }
    }
    return ret;
  }
  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  {
    int ret = OB_SUCCESS;
    int64_t shallow_pos = 0;
    double max_score = 0.0;
    const uint64_t target_value = target.get_uint64();
    max_score_tuple = nullptr;
    ret = shallow_ret_;
    while (OB_SUCC(ret) && shallow_pos < static_cast<int64_t>(rowids_.size())
           && (rowids_.at(shallow_pos) < target_value
               || (!inclusive && rowids_.at(shallow_pos) == target_value))) {
      ++shallow_pos;
    }
    ++shallow_count_;
    if (OB_FAIL(ret)) {
    } else if (shallow_pos >= static_cast<int64_t>(rowids_.size())) {
      ret = OB_ITER_END;
    } else {
      for (int64_t i = shallow_pos; i < static_cast<int64_t>(scores_.size()); ++i) {
        max_score = MAX(max_score, scores_.at(i));
      }
      ObDASRowID min_id;
      ObDASRowID max_id;
      min_id.set_uint64(rowids_.at(shallow_pos));
      max_id.set_uint64(rowids_.at(rowids_.size() - 1));
      max_score_tuple_.set(min_id, max_id, max_score);
      max_score_tuple = &max_score_tuple_;
      in_shallow_status_ = true;
    }
    return ret;
  }
  int do_set_min_competitive_score(const double &threshold) override
  {
    last_threshold_ = threshold;
    return ObIDASSearchOp::do_set_min_competitive_score(threshold);
  }
  int do_calc_max_score(double &max_score) override
  {
    max_score = 0.0;
    for (int64_t i = 0; i < static_cast<int64_t>(scores_.size()); ++i) {
      max_score = MAX(max_score, scores_.at(i));
    }
    return OB_SUCCESS;
  }
  int do_drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after) override
  {
    ++drain_count_;
    max_drain_capacity_ = MAX(max_drain_capacity_, capacity);
    return OB_SUCCESS != drain_ret_ ? drain_ret_ : ObIDASSearchOp::do_drain_until(
        boundary, capacity, out_ids, out_scores, count,
        next_id_after, next_score_after);
  }

private:
  std::vector<uint64_t> rowids_;
  std::vector<double> scores_;
  int64_t pos_;
  int64_t bind_count_;
  int64_t last_range_base_;
  double last_threshold_;
  int64_t shallow_count_;
  int64_t open_count_;
  int64_t rescan_count_;
  int64_t release_count_;
  int64_t advance_count_;
  int64_t drain_count_;
  int64_t max_drain_capacity_;
  int advance_ret_;
  int next_ret_;
  int drain_ret_;
  int shallow_ret_;
  ObDASRowID last_advance_target_;
  bool in_shallow_status_;
};

class MockProbeSegmentOp : public MockSegmentOp
{
public:
  MockProbeSegmentOp(ObDASSearchCtx &search_ctx,
                     const std::vector<uint64_t> &rowids,
                     const std::vector<double> &scores)
    : MockSegmentOp(search_ctx, rowids, scores)
  {}

  bool is_probe_mode() const override { return true; }
};

// Keep the production range-to-term/empty-slot contract, replacing only storage
// access with postings derived from the current term. Term 0 has no postings.
class MockRuntimeRangeRorOp : public ObDASAsyncScalarRorOp
{
public:
  explicit MockRuntimeRangeRorOp(ObDASSearchCtx &search_ctx)
    : ObDASAsyncScalarRorOp(search_ctx), rowids_(), pos_(-1)
  {}

  // This fixture replaces the storage posting iterator with a vector-backed
  // ordered implementation, so exercise the union compatibility fallback.
  bool supports_direct_rowid_drain() const override { return false; }

private:
  int do_open() override
  {
    int ret = OB_SUCCESS;
    pos_ = -1;
    rowids_.clear();
    if (OB_FAIL(init_term_key())) {
    } else if (!is_empty_range_) {
      const uint64_t term = term_key_.get_datum(SEARCH_INDEX_VALUE).get_int();
      if (term != 0) {
        rowids_.push_back(term);
        rowids_.push_back(1000);
        rowids_.push_back(2000 + term);
      }
    }
    return ret;
  }
  int do_rescan() override { return do_open(); }
  int do_rescan_segment(const ObNewVersionRange &range) override
  {
    UNUSED(range);
    return do_open();
  }
  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    int ret = OB_SUCCESS;
    score = 0.0;
    if (++pos_ >= static_cast<int64_t>(rowids_.size())) {
      ret = OB_ITER_END;
    } else {
      rowid.set_uint64(rowids_[pos_]);
    }
    return ret;
  }
  int do_advance_to(const ObDASRowID &target, ObDASRowID &rowid, double &score) override
  {
    int ret = OB_SUCCESS;
    score = 0.0;
    pos_ = MAX(0L, pos_);
    while (pos_ < static_cast<int64_t>(rowids_.size())
        && rowids_[pos_] < target.get_uint64()) {
      ++pos_;
    }
    if (pos_ >= static_cast<int64_t>(rowids_.size())) {
      ret = OB_ITER_END;
    } else {
      rowid.set_uint64(rowids_[pos_]);
    }
    return ret;
  }
  int do_drain_until(const ObDASRowID &boundary, int64_t capacity,
      uint64_t *out_ids, double *out_scores, int64_t &count,
      ObDASRowID &next_id_after, double &next_score_after) override
  {
    return ObIDASSearchOp::do_drain_until(boundary, capacity, out_ids,
        out_scores, count, next_id_after, next_score_after);
  }

  std::vector<uint64_t> rowids_;
  int64_t pos_;
};

class FixedEnumerableFilterRtDef : public ObDASScalarScanRtDef
{
public:
  explicit FixedEnumerableFilterRtDef(MockSegmentOp *filter_op)
    : ObDASScalarScanRtDef(), filter_op_(filter_op), generate_count_(0)
  {}

  int compute_cost(ObDASSearchCtx &search_ctx, ObDASSearchCost &cost) override
  {
    UNUSED(search_ctx);
    cost = ObDASSearchCost(1);
    return OB_SUCCESS;
  }

  int generate_op(
      ObDASSearchCost lead_cost,
      ObDASSearchCtx &search_ctx,
      ObIDASSearchOp *&op) override
  {
    int ret = OB_SUCCESS;
    UNUSED(lead_cost);
    UNUSED(search_ctx);
    op = nullptr;
    ++generate_count_;
    if (OB_ISNULL(filter_op_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      op = filter_op_;
    }
    return ret;
  }

  MockSegmentOp *filter_op_;
  int64_t generate_count_;
};

struct ScriptedBatchStep
{
  ScriptedBatchStep(const int ret,
                    const int64_t count,
                    const std::initializer_list<uint64_t> values)
    : ret_(ret), count_(count), values_(values)
  {}

  int ret_;
  int64_t count_;
  std::vector<uint64_t> values_;
};

class ScriptedBatchSegmentOp : public ObIDASSearchOp
{
public:
  ScriptedBatchSegmentOp(ObDASSearchCtx &search_ctx,
                         uint64_t *batch_frame,
                         const int64_t frame_capacity)
    : ObIDASSearchOp(search_ctx), batch_frame_(batch_frame),
      frame_capacity_(frame_capacity),
      rowid_output_exprs_(search_ctx.get_scope(
          search_ctx.get_op_creating_scope()).rowid_exprs_),
      steps_(), step_idx_(0),
      open_count_(0), rescan_count_(0), bind_count_(0), batch_call_count_(0)
  {
    op_type_ = DAS_SEARCH_OP_DUMMY;
  }

  const ObIArray<ObExpr *> *get_rowid_output_exprs() const override
  { return rowid_output_exprs_; }
  void set_rowid_output_exprs(const ObIArray<ObExpr *> *rowid_output_exprs)
  { rowid_output_exprs_ = rowid_output_exprs; }
  void append_step(const ScriptedBatchStep &step) { steps_.push_back(step); }
  void replace_current_step(const ScriptedBatchStep &step)
  {
    if (step_idx_ < static_cast<int64_t>(steps_.size())) {
      steps_.at(step_idx_) = step;
    } else {
      steps_.push_back(step);
    }
  }
  int64_t open_count() const { return open_count_; }
  int64_t rescan_count() const { return rescan_count_; }
  int64_t bind_count() const { return bind_count_; }
  int64_t step_idx() const { return step_idx_; }
  int64_t batch_call_count() const { return batch_call_count_; }
private:
  int do_open() override { ++open_count_; step_idx_ = 0; return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { ++rescan_count_; step_idx_ = 0; return OB_SUCCESS; }
  int do_rescan_segment(const ObNewVersionRange &range) override
  { UNUSED(range); ++bind_count_; return OB_SUCCESS; }
  int do_advance_to(const ObDASRowID &target, ObDASRowID &rowid, double &score) override
  { UNUSED(target); UNUSED(rowid); UNUSED(score); return OB_NOT_SUPPORTED; }
  int do_next_rowid(ObDASRowID &rowid, double &score) override
  { UNUSED(rowid); UNUSED(score); return OB_NOT_SUPPORTED; }
  int do_next_rowids(const int64_t capacity, int64_t &count) override
  {
    int ret = OB_ITER_END;
    ++batch_call_count_;
    count = 0;
    if (step_idx_ < static_cast<int64_t>(steps_.size())) {
      const ScriptedBatchStep &step = steps_.at(step_idx_);
      ret = step.ret_;
      count = step.count_;
      const int64_t copy_count = MIN(
          MIN(static_cast<int64_t>(step.values_.size()), frame_capacity_),
          MAX(0L, MIN(count, capacity)));
      for (int64_t i = 0; i < copy_count; ++i) {
        batch_frame_[i] = step.values_.at(i);
      }
      if ((OB_SUCCESS == ret || OB_ITER_END == ret)
          && count > 0 && count <= capacity) {
        ++step_idx_;
      }
    }
    return ret;
  }
private:
  uint64_t *batch_frame_;
  int64_t frame_capacity_;
  const ObIArray<ObExpr *> *rowid_output_exprs_;
  std::vector<ScriptedBatchStep> steps_;
  int64_t step_idx_;
  int64_t open_count_;
  int64_t rescan_count_;
  int64_t bind_count_;
  int64_t batch_call_count_;
};

class ScopedSortTmpFileManager
{
public:
  ScopedSortTmpFileManager()
    : tenant_(ObTenantEnv::get_tenant_local()),
      old_tenant_id_(OB_NOT_NULL(tenant_) ? tenant_->id_ : OB_INVALID_TENANT_ID),
      old_tmp_file_mgr_(OB_NOT_NULL(tenant_)
          ? tenant_->get<tmp_file::ObTenantTmpFileManager *>() : nullptr),
      tmp_file_mgr_(), inited_(false)
  {}

  ~ScopedSortTmpFileManager()
  {
    if (inited_) {
      tmp_file_mgr_.get_sn_file_manager().is_running_ = false;
      tmp_file_mgr_.destroy();
    }
    if (OB_NOT_NULL(tenant_)) {
      tenant_->set(old_tmp_file_mgr_);
      tenant_->id_ = old_tenant_id_;
    }
  }

  int init()
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(tenant_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      tenant_->id_ = OB_SYS_TENANT_ID;
      if (OB_FAIL(tmp_file_mgr_.init())) {
      } else {
        inited_ = true;
        // Sort allocates a tmp-file directory even when this tiny batch never spills.
        tmp_file_mgr_.get_sn_file_manager().is_running_ = true;
        tenant_->set(&tmp_file_mgr_);
      }
    }
    return ret;
  }

private:
  ObTenantBase *tenant_;
  uint64_t old_tenant_id_;
  tmp_file::ObTenantTmpFileManager *old_tmp_file_mgr_;
  tmp_file::ObTenantTmpFileManager tmp_file_mgr_;
  bool inited_;
};

class CollectingRowIDBatchConsumer : public ObDASRowIDBatchConsumer
{
public:
  explicit CollectingRowIDBatchConsumer(const bool deduplicate = false)
    : deduplicate_(deduplicate), rowids_()
  {}

  int consume(const uint64_t *rowids, const int64_t count) override
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(count < 0 || (count > 0 && OB_ISNULL(rowids)))) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        if (!deduplicate_
            || !has_exist_in_array(rowids_, rowids[i])) {
          if (OB_FAIL(rowids_.push_back(rowids[i]))) {
            LOG_WARN("failed to collect RowID", K(ret), K(i), K(rowids[i]));
          }
        }
      }
    }
    return ret;
  }

  void reset() { rowids_.reset(); }
  const ObIArray<uint64_t> &rowids() const { return rowids_; }

private:
  bool deduplicate_;
  ObSEArray<uint64_t, 8> rowids_;
};

static void expect_rowids_equal(
    const ObIArray<uint64_t> &actual,
    const uint64_t *expected,
    const int64_t expected_count)
{
  ASSERT_EQ(expected_count, actual.count());
  for (int64_t i = 0; i < expected_count; ++i) {
    EXPECT_EQ(expected[i], actual.at(i));
  }
}

class MockReusingCompactRowOp : public ObIDASSearchOp
{
public:
  MockReusingCompactRowOp(
      ObDASSearchCtx &search_ctx,
      const std::vector<ObDASRowID> &rowids)
    : ObIDASSearchOp(search_ctx), rows_(), output_buf_(), pos_(-1)
  {
    scope_ = DAS_SCOPE_NESTED_SYNC;
    op_type_ = DAS_SEARCH_OP_DUMMY;
    int64_t max_row_size = 0;
    for (int64_t i = 0; i < static_cast<int64_t>(rowids.size()); ++i) {
      const ObCompactRow *row = rowids.at(i).get_compact_row();
      if (nullptr != row) {
        const int64_t row_size = row->get_row_size();
        rows_.push_back(std::vector<char>(row_size));
        MEMCPY(rows_.back().data(), row, row_size);
        max_row_size = MAX(max_row_size, row_size);
      }
    }
    output_buf_.resize(max_row_size);
  }

private:
  int do_open() override
  {
    pos_ = -1;
    return OB_SUCCESS;
  }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override
  {
    pos_ = -1;
    return OB_SUCCESS;
  }
  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    ++pos_;
    return materialize(rowid, score);
  }
  int do_advance_to(
      const ObDASRowID &target,
      ObDASRowID &rowid,
      double &score) override
  {
    int ret = OB_SUCCESS;
    if (target.is_max()) {
      pos_ = static_cast<int64_t>(rows_.size());
    } else {
      if (pos_ < 0) {
        pos_ = 0;
      }
      while (OB_SUCC(ret) && pos_ < static_cast<int64_t>(rows_.size())) {
        ObDASRowID source_rowid;
        int cmp = 0;
        source_rowid.set_compact_row(
            reinterpret_cast<const ObCompactRow *>(rows_.at(pos_).data()));
        if (OB_FAIL(compare_rowid(source_rowid, target, cmp))) {
        } else if (cmp < 0) {
          ++pos_;
        } else {
          break;
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = materialize(rowid, score);
    }
    return ret;
  }

  int materialize(ObDASRowID &rowid, double &score)
  {
    int ret = OB_SUCCESS;
    if (pos_ < 0 || pos_ >= static_cast<int64_t>(rows_.size())) {
      rowid.set_max();
      ret = OB_ITER_END;
    } else {
      const std::vector<char> &source = rows_.at(pos_);
      MEMCPY(output_buf_.data(), source.data(), source.size());
      rowid.set_compact_row(
          reinterpret_cast<const ObCompactRow *>(output_buf_.data()));
      score = 0.0;
    }
    return ret;
  }

private:
  std::vector<std::vector<char> > rows_;
  std::vector<char> output_buf_;
  int64_t pos_;
};

class TestDASAsyncSegmentMerge : public ::testing::Test
{
public:
  TestDASAsyncSegmentMerge()
    : allocator_("AsyncSegTest"), scan_allocator_("AsyncSegScan"),
      scan_op_(scan_allocator_), exec_ctx_(allocator_), eval_ctx_(exec_ctx_),
      rowid_expr_(), rowid_exprs_(allocator_), output_(allocator_),
      search_ctx_(allocator_, scan_op_), fetch_cursor_(0)
  {}

  static void SetUpTestCase()
  {
    ObTenantBase *tenant = ObTenantEnv::get_tenant_local();
    tenant->id_ = 500;
    ASSERT_EQ(OB_SUCCESS, fake_ls_service_.ls_map_.init(tenant->id_, &fake_ls_allocator_));
    void *ls_buf = fake_ls_allocator_.alloc(sizeof(storage::ObLS));
    ASSERT_NE(nullptr, ls_buf);
    fake_ls_ = new (ls_buf) storage::ObLS();
    fake_ls_->ls_meta_.ls_id_ = ObLSID(1001);
    fake_ls_->ls_meta_.replica_type_ = REPLICA_TYPE_LOGONLY;
    ASSERT_EQ(OB_SUCCESS, fake_ls_service_.ls_map_.add_ls(*fake_ls_));
    fake_ls_service_.tenant_id_ = tenant->id_;
    fake_ls_service_.is_inited_ = true;
    fake_ls_service_.is_stopped_ = true;
    tenant->set<storage::ObLSService *>(&fake_ls_service_);
  }

  static void TearDownTestCase()
  {
    ObTenantEnv::get_tenant_local()->set<storage::ObLSService *>(nullptr);
    fake_ls_service_.is_inited_ = false;
    fake_ls_service_.is_stopped_ = true;
    fake_ls_service_.ls_map_.destroy();
    fake_ls_ = nullptr;
  }

  void SetUp() override
  {
    ObTenantBase *tenant = ObTenantEnv::get_tenant_local();
    tenant->id_ = 500;
    fake_ls_service_.tenant_id_ = tenant->id_;
    fake_ls_service_.is_inited_ = true;
    fake_ls_service_.is_stopped_ = true;
    tenant->set<storage::ObLSService *>(&fake_ls_service_);
    eval_ctx_.max_batch_size_ = 256;
    scan_rtdef_.eval_ctx_ = &eval_ctx_;
    scan_op_.set_scan_rtdef(&scan_rtdef_);
    search_ctx_.eval_ctx_ = &eval_ctx_;
    ASSERT_EQ(OB_SUCCESS, rowid_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, rowid_exprs_.push_back(&rowid_expr_));
    search_ctx_.rowid_exprs_ = &rowid_exprs_;
    search_ctx_.output_ = &output_;
    search_ctx_.rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_BASE].rowid_exprs_ = &rowid_exprs_;
    search_ctx_.scopes_[DAS_SCOPE_NESTED_ASYNC].rowid_type_ = DAS_ROWID_TYPE_UINT64;
    search_ctx_.scopes_[DAS_SCOPE_NESTED_ASYNC].rowid_exprs_ = &rowid_exprs_;
  }

  void check_async_terms_bmm_direct_selection(const bool new_false_range);
  void check_async_empty_filter_bmm();
  void check_bmm_input_after_config_change(
      ObIDASSearchOp *filter_op, const ObDASBMMBuildOptions &options);
  void check_async_terms_ordered_direct(
      const int64_t shape, const int64_t domain_size, const uint64_t base_id);

  static int init_async_scalar_scan(ObDASScalarScanCtDef &ctdef,
                                    ObDASScalarScanRtDef &rtdef,
                                    ObExpr &rowid_expr)
  {
    ctdef.ref_table_id_ = TEST_INDEX_TABLE_ID;
    ctdef.is_async_index_ = true;
    int ret = ctdef.rowkey_exprs_.init(1);
    if (OB_SUCC(ret)) {
      ret = ctdef.rowkey_exprs_.push_back(&rowid_expr);
    }
    if (OB_SUCC(ret)) {
      rtdef.ctdef_ = &ctdef;
    }
    return ret;
  }

  static void prepare_async_scalar_range(ObNewRange &range,
                                         ObObj *start_objs,
                                         ObObj *end_objs,
                                         const int64_t start_value,
                                         const int64_t end_value)
  {
    for (int64_t i = 0; i < SEARCH_INDEX_DOC_ID; ++i) {
      start_objs[i].set_int(i + 1);
      end_objs[i].set_int(i + 1);
    }
    start_objs[SEARCH_INDEX_VALUE].set_int(start_value);
    end_objs[SEARCH_INDEX_VALUE].set_int(end_value);
    start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
    end_objs[SEARCH_INDEX_DOC_ID].set_max_value();
    range.start_key_.assign(start_objs, SEARCH_INDEX_KEY_CNT);
    range.end_key_.assign(end_objs, SEARCH_INDEX_KEY_CNT);
    range.border_flag_.set_inclusive_start();
    range.border_flag_.set_inclusive_end();
  }

  static int append_async_scalar_range(ObDASScalarScanRtDef &rtdef,
                                       ObNewRange &range,
                                       ObObj *start_objs,
                                       ObObj *end_objs,
                                       const int64_t start_value,
                                       const int64_t end_value)
  {
    prepare_async_scalar_range(
        range, start_objs, end_objs, start_value, end_value);
    return rtdef.key_ranges_.push_back(range);
  }

  static int add_mock_storage_group(
      ObAsyncIndexPartitionSegCtx &partition,
      const std::vector<std::vector<ObNewVersionRange> > &tablet_ranges)
  {
    int ret = OB_SUCCESS;
    typedef storage::ObAsyncIndexSegmentGroup StorageSegmentGroup;
    StorageSegmentGroup *group = nullptr;
    if (tablet_ranges.empty()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_ISNULL(group = OB_NEWx(
                   storage::ObAsyncIndexSegmentGroup,
                   &partition.storage_group_ctx_.allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(group->init(tablet_ranges.size()))) {
    }
    for (int64_t t = 0; OB_SUCC(ret)
         && t < static_cast<int64_t>(tablet_ranges.size()); ++t) {
      for (int64_t r = 0; OB_SUCC(ret)
           && r < static_cast<int64_t>(tablet_ranges.at(t).size()); ++r) {
        if (OB_FAIL(group->add_version_range(t, tablet_ranges.at(t).at(r)))) {
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(partition.storage_group_ctx_.groups_.push_back(group))) {
    }
    if (OB_FAIL(ret) && OB_NOT_NULL(group)) {
      group->reset();
      OB_DELETEx(StorageSegmentGroup, &partition.storage_group_ctx_.allocator_, group);
    }
    return ret;
  }

  static int add_mock_storage_tablets(
      ObAsyncIndexPartitionSegCtx &partition,
      const int64_t tablet_cnt)
  {
    int ret = OB_SUCCESS;
    if (tablet_cnt <= 0) {
      ret = OB_INVALID_ARGUMENT;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
      if (OB_ISNULL(partition.storage_group_ctx_.index_tablet_handles_
                        .alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      }
    }
    return ret;
  }

  static int fetch_partition(void *arg,
                             const ObLSID &ls_id,
                             const ObIArray<uint64_t> &index_table_ids,
                             const ObIArray<ObTabletID> &index_tablet_ids,
                             const ObTabletID &delete_tablet_id,
                             const SCN &sql_read_snapshot,
                             ObAsyncIndexPartitionSegCtx &partition)
  {
    int ret = OB_SUCCESS;
    TestDASAsyncSegmentMerge *self = static_cast<TestDASAsyncSegmentMerge *>(arg);
    UNUSED(ls_id);
    UNUSED(delete_tablet_id);
    UNUSED(sql_read_snapshot);
    if (self->fetch_cursor_ >= static_cast<int64_t>(self->segment_counts_.size())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (index_table_ids.count() != 1 || index_tablet_ids.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (OB_FAIL(partition.storage_group_ctx_.async_index_read_snapshot_.convert_for_tx(
              10000 + self->fetch_cursor_))) {
      } else if (OB_FAIL(add_mock_storage_tablets(partition, index_tablet_ids.count()))) {
      }
      const int64_t segment_cnt = self->segment_counts_.at(self->fetch_cursor_++);
      if (OB_SUCC(ret) && 0 == segment_cnt) {
        std::vector<std::vector<ObNewVersionRange> > tablet_ranges(1);
        ret = add_mock_storage_group(partition, tablet_ranges);
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < segment_cnt; ++i) {
        ObNewVersionRange range;
        std::vector<std::vector<ObNewVersionRange> > tablet_ranges(1);
        range.base_version_ = i * 100;
        range.snapshot_version_ = (i + 1) * 100;
        tablet_ranges.at(0).push_back(range);
        if (OB_FAIL(add_mock_storage_group(partition, tablet_ranges))) {
        }
      }
    }
    return ret;
  }

  static int fetch_multi_index_partition(
      void *arg,
      const ObLSID &ls_id,
      const ObIArray<uint64_t> &index_table_ids,
      const ObIArray<ObTabletID> &index_tablet_ids,
      const ObTabletID &delete_tablet_id,
      const SCN &sql_read_snapshot,
      ObAsyncIndexPartitionSegCtx &partition)
  {
    int ret = OB_SUCCESS;
    TestDASAsyncSegmentMerge *self = static_cast<TestDASAsyncSegmentMerge *>(arg);
    UNUSED(ls_id);
    UNUSED(delete_tablet_id);
    UNUSED(sql_read_snapshot);
    const int64_t partition_idx = self->fetch_cursor_;
    if (partition_idx >= static_cast<int64_t>(self->multi_segment_counts_.size())
        || partition_idx >= static_cast<int64_t>(self->partition_aligned_.size())
        || index_table_ids.count() != index_tablet_ids.count()
        || index_table_ids.count()
           != static_cast<int64_t>(self->multi_segment_counts_.at(partition_idx).size())) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ++self->fetch_cursor_;
      if (OB_FAIL(partition.storage_group_ctx_.async_index_read_snapshot_.convert_for_tx(
              10000 + partition_idx))) {
      } else if (OB_FAIL(add_mock_storage_tablets(partition, index_tablet_ids.count()))) {
      }
      if (OB_SUCC(ret) && self->partition_aligned_.at(partition_idx)) {
        const int64_t aligned_segment_cnt = self->multi_segment_counts_.at(partition_idx).at(0);
        for (int64_t t = 1; OB_SUCC(ret) && t < index_table_ids.count(); ++t) {
          if (aligned_segment_cnt != self->multi_segment_counts_.at(partition_idx).at(t)) {
            ret = OB_ERR_UNEXPECTED;
          }
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < aligned_segment_cnt; ++i) {
          std::vector<std::vector<ObNewVersionRange> > tablet_ranges(
              index_table_ids.count());
          for (int64_t t = 0; OB_SUCC(ret) && t < index_table_ids.count(); ++t) {
            ObNewVersionRange range;
            range.base_version_ = i * 100;
            range.snapshot_version_ = (i + 1) * 100;
            tablet_ranges.at(t).push_back(range);
          }
          if (OB_SUCC(ret) && OB_FAIL(add_mock_storage_group(partition, tablet_ranges))) {
          }
        }
      } else if (OB_SUCC(ret)) {
        std::vector<std::vector<ObNewVersionRange> > tablet_ranges(
            index_table_ids.count());
        for (int64_t t = 0; OB_SUCC(ret) && t < index_table_ids.count(); ++t) {
          const int64_t segment_cnt = self->multi_segment_counts_.at(partition_idx).at(t);
          for (int64_t i = 0; i < segment_cnt; ++i) {
            ObNewVersionRange range;
            range.base_version_ = i * 1000 / segment_cnt;
            range.snapshot_version_ = (i + 1) * 1000 / segment_cnt;
            tablet_ranges.at(t).push_back(range);
          }
        }
        if (OB_SUCC(ret) && OB_FAIL(add_mock_storage_group(partition, tablet_ranges))) {
        }
      }
    }
    return ret;
  }

  void build_catalog(ObDASAsyncIndexQueryCtx &catalog,
                     const std::vector<int64_t> &segment_counts)
  {
    segment_counts_ = segment_counts;
    fetch_cursor_ = 0;
    catalog.set_mock_fetcher(fetch_partition, this);
    ObSEArray<uint64_t, 1> index_tables;
    ObSEArray<ObTabletID, 1> index_tablets;
    SCN sql_snapshot;
    sql_snapshot.convert_for_tx(2000);
    ASSERT_EQ(OB_SUCCESS, index_tables.push_back(TEST_INDEX_TABLE_ID));
    ASSERT_EQ(OB_SUCCESS, index_tablets.push_back(ObTabletID(10001)));
    for (int64_t i = 0; i < static_cast<int64_t>(segment_counts.size()); ++i) {
      ASSERT_EQ(OB_SUCCESS, catalog.add_partition(
          ObLSID(1001), ObTabletID(i + 1), index_tables, index_tablets,
          ObTabletID(20001), sql_snapshot));
    }
    ASSERT_EQ(OB_SUCCESS, catalog.finalize());
    search_ctx_.set_async_query_ctx(&catalog);
    select_partition(catalog, 1);
  }

  void build_multi_index_catalog(
      ObDASAsyncIndexQueryCtx &catalog,
      const std::vector<std::vector<int64_t> > &multi_segment_counts,
      const std::vector<bool> &partition_aligned)
  {
    multi_segment_counts_ = multi_segment_counts;
    partition_aligned_ = partition_aligned;
    fetch_cursor_ = 0;
    catalog.set_mock_fetcher(fetch_multi_index_partition, this);
    ObSEArray<uint64_t, 2> index_tables;
    ObSEArray<ObTabletID, 2> index_tablets;
    SCN sql_snapshot;
    sql_snapshot.convert_for_tx(2000);
    ASSERT_EQ(OB_SUCCESS, index_tables.push_back(TEST_INDEX_TABLE_ID));
    ASSERT_EQ(OB_SUCCESS, index_tables.push_back(TEST_INDEX_TABLE_ID_2));
    ASSERT_EQ(OB_SUCCESS, index_tablets.push_back(ObTabletID(10001)));
    ASSERT_EQ(OB_SUCCESS, index_tablets.push_back(ObTabletID(10002)));
    ASSERT_EQ(multi_segment_counts.size(), partition_aligned.size());
    for (int64_t i = 0; i < static_cast<int64_t>(multi_segment_counts.size()); ++i) {
      ASSERT_EQ(OB_SUCCESS, catalog.add_partition(
          ObLSID(1001), ObTabletID(i + 1), index_tables, index_tablets,
          ObTabletID(20001), sql_snapshot));
    }
    ASSERT_EQ(OB_SUCCESS, catalog.finalize());
    search_ctx_.set_async_query_ctx(&catalog);
    select_partition(catalog, 1);
  }

  void select_partition(const ObDASAsyncIndexQueryCtx &catalog, const uint64_t tablet_id)
  {
    const ObAsyncIndexPartitionSegCtx *partition = nullptr;
    ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(tablet_id), partition));
    ASSERT_NE(nullptr, partition);
    search_ctx_.set_current_async_partition(partition);
  }

  MockSegmentOp *create_child(const std::vector<uint64_t> &rowids,
                              const std::vector<double> &scores)
  {
    MockSegmentOp *child = OB_NEWx(MockSegmentOp, &allocator_, search_ctx_, rowids, scores);
    MockSegmentOpParam param;
    EXPECT_NE(nullptr, child);
    if (nullptr != child) {
      EXPECT_EQ(OB_SUCCESS, child->init(param));
    }
    return child;
  }

  OB_NOINLINE int run_compact_disjunction_alias_case()
  {
    int ret = OB_SUCCESS;
    ObExpr doc_id_expr;
    ObExpr nested_key_expr;
    ExprFixedArray nested_rowid_exprs(allocator_);
    doc_id_expr.type_ = T_REF_COLUMN;
    doc_id_expr.max_length_ = 20;
    doc_id_expr.is_fixed_length_data_ = true;
    doc_id_expr.len_ = sizeof(uint64_t);
    nested_key_expr.type_ = T_REF_COLUMN;
    nested_key_expr.max_length_ = SEARCH_INDEX_NESTED_KEY_LENGTH;
    nested_key_expr.is_fixed_length_data_ = false;
    if (OB_FAIL(nested_rowid_exprs.init(2))) {
    } else if (OB_FAIL(nested_rowid_exprs.push_back(&doc_id_expr))) {
    } else if (OB_FAIL(nested_rowid_exprs.push_back(&nested_key_expr))) {
    } else if (OB_FAIL(search_ctx_.register_nested_scope(&nested_rowid_exprs, DAS_SCOPE_NESTED_SYNC))) {
    } else {
      doc_id_expr.datum_meta_.type_ = ObUInt64Type;
      doc_id_expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
      doc_id_expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
          doc_id_expr.datum_meta_.type_, doc_id_expr.datum_meta_.cs_type_);
      nested_key_expr.datum_meta_.type_ = ObVarcharType;
      nested_key_expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
      nested_key_expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
          nested_key_expr.datum_meta_.type_, nested_key_expr.datum_meta_.cs_type_);
      if (OB_ISNULL(doc_id_expr.basic_funcs_)
          || OB_ISNULL(nested_key_expr.basic_funcs_)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }

    ObArenaAllocator source_allocator("CompactRows");
    std::vector<ObDASRowID> first_rowids;
    std::vector<ObDASRowID> second_rowids;
    auto append_rowid = [&](const uint64_t value,
                            std::vector<ObDASRowID> &rowids) -> int {
      int ret = OB_SUCCESS;
      static const char NESTED_KEY[] = {0, 0, 0, 1};
      ObDatum doc_id_datum(
          reinterpret_cast<const char *>(&value), sizeof(value), false);
      ObDatum nested_key_datum(NESTED_KEY, sizeof(NESTED_KEY), false);
      ObSEArray<ObDatum, 2> datums;
      ObDASRowID rowid;
      if (OB_FAIL(datums.push_back(doc_id_datum))) {
      } else if (OB_FAIL(datums.push_back(nested_key_datum))) {
      } else if (OB_FAIL(search_ctx_.write_datums_to_rowid(
          DAS_SCOPE_NESTED_SYNC, datums, rowid, source_allocator))) {
      } else {
        rowids.push_back(rowid);
      }
      return ret;
    };
    if (OB_SUCC(ret) && OB_FAIL(append_rowid(1, first_rowids))) {
    } else if (OB_SUCC(ret) && OB_FAIL(append_rowid(2, first_rowids))) {
    } else if (OB_SUCC(ret) && OB_FAIL(append_rowid(1, second_rowids))) {
    } else if (OB_SUCC(ret) && OB_FAIL(append_rowid(3, second_rowids))) {
    }

    if (OB_SUCC(ret)) {
      MockReusingCompactRowOp first_child(search_ctx_, first_rowids);
      MockReusingCompactRowOp second_child(search_ctx_, second_rowids);
      MockSegmentOpParam child_param;
      ObArray<ObIDASSearchOp *> children;
      ObDASDisjunctionFilterOp *disjunction = nullptr;
      void *disjunction_buf = nullptr;
      bool disjunction_opened = false;
      if (OB_FAIL(first_child.init(child_param))) {
      } else if (OB_FAIL(second_child.init(child_param))) {
      } else if (OB_FAIL(children.push_back(&first_child))) {
      } else if (OB_FAIL(children.push_back(&second_child))) {
      } else if (OB_ISNULL(disjunction_buf = allocator_.alloc(
          sizeof(ObDASDisjunctionFilterOp)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        disjunction = new (disjunction_buf) ObDASDisjunctionFilterOp(search_ctx_);
        disjunction->scope_ = DAS_SCOPE_NESTED_SYNC;
        ObDASDisjunctionFilterOpParam disjunction_param(
            children, 1, ObDASSearchCost(1));
        if (OB_FAIL(disjunction->init(disjunction_param))) {
        } else if (OB_FAIL(disjunction->open())) {
        } else {
          disjunction_opened = true;
          const uint64_t expected_rowids[] = {1, 2, 3};
          ObDASRowID rowid;
          double score = 0.0;
          for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(expected_rowids); ++i) {
            ObDatum datum;
            if (OB_FAIL(disjunction->next_rowid(rowid, score))) {
            } else if (OB_FAIL(search_ctx_.get_datum_from_rowid(
                DAS_SCOPE_NESTED_SYNC, rowid, datum, 0))) {
            } else if (datum.get_uint() != expected_rowids[i]) {
              ret = OB_ERR_UNEXPECTED;
            }
          }
          if (OB_SUCC(ret)) {
            const int end_ret = disjunction->next_rowid(rowid, score);
            if (OB_ITER_END != end_ret) {
              ret = OB_SUCCESS == end_ret ? OB_ERR_UNEXPECTED : end_ret;
            }
          }
        }
      }
      if (disjunction_opened) {
        const int close_ret = disjunction->close();
        if (OB_SUCCESS == ret && OB_SUCCESS != close_ret) {
          ret = close_ret;
        }
        first_child.close();
        second_child.close();
      }
      if (OB_NOT_NULL(disjunction)) {
        disjunction->~ObDASDisjunctionFilterOp();
      }
    }
    return ret;
  }

  void prepare_const_expr_frame(const int64_t frame_size)
  {
    char **frames = static_cast<char **>(allocator_.alloc(sizeof(char *)));
    ASSERT_NE(nullptr, frames);
    frames[0] = static_cast<char *>(allocator_.alloc(frame_size));
    ASSERT_NE(nullptr, frames[0]);
    MEMSET(frames[0], 0, frame_size);
    exec_ctx_.frames_ = frames;
    exec_ctx_.frame_cnt_ = 1;
    eval_ctx_.frames_ = frames;
  }

  void init_int_const_expr(
      ObExpr &expr,
      int64_t &frame_pos,
      const int64_t value)
  {
    expr.eval_func_ = nullptr;
    expr.obj_meta_.set_type(ObIntType);
    expr.datum_meta_.type_ = ObIntType;
    expr.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    expr.vec_value_tc_ = get_vec_value_tc(
        ObIntType, expr.datum_meta_.scale_, expr.datum_meta_.precision_);
    expr.frame_idx_ = 0;
    expr.datum_off_ = frame_pos;
    frame_pos += sizeof(ObDatum);
    expr.eval_info_off_ = frame_pos;
    frame_pos += sizeof(ObEvalInfo);
    expr.res_buf_off_ = frame_pos;
    expr.res_buf_len_ = sizeof(int64_t);
    frame_pos += expr.res_buf_len_;
    expr.vector_header_off_ = frame_pos;
    frame_pos += sizeof(VectorHeader);
    expr.locate_datum_for_write(eval_ctx_).set_int(value);
    expr.set_evaluated_projected(eval_ctx_);
  }

  void init_string_const_expr(
      ObExpr &expr,
      int64_t &frame_pos,
      const ObString &value)
  {
    expr.eval_func_ = nullptr;
    expr.obj_meta_.set_varchar();
    expr.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    expr.datum_meta_.type_ = ObVarcharType;
    expr.datum_meta_.cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    expr.obj_datum_map_ = OBJ_DATUM_STRING;
    expr.frame_idx_ = 0;
    expr.datum_off_ = frame_pos;
    frame_pos += sizeof(ObDatum);
    expr.eval_info_off_ = frame_pos;
    frame_pos += sizeof(ObEvalInfo);
    expr.res_buf_off_ = frame_pos;
    expr.res_buf_len_ = value.length();
    frame_pos += expr.res_buf_len_;
    expr.vector_header_off_ = frame_pos;
    frame_pos += sizeof(VectorHeader);
    expr.locate_datum_for_write(eval_ctx_).set_string(value);
    expr.set_evaluated_projected(eval_ctx_);
  }

  void init_double_const_expr(
      ObExpr &expr,
      int64_t &frame_pos,
      const double value)
  {
    expr.eval_func_ = nullptr;
    expr.obj_meta_.set_type(ObDoubleType);
    expr.datum_meta_.type_ = ObDoubleType;
    expr.obj_datum_map_ = OBJ_DATUM_8BYTE_DATA;
    expr.vec_value_tc_ = get_vec_value_tc(
        ObDoubleType, expr.datum_meta_.scale_, expr.datum_meta_.precision_);
    expr.frame_idx_ = 0;
    expr.datum_off_ = frame_pos;
    frame_pos += sizeof(ObDatum);
    expr.eval_info_off_ = frame_pos;
    frame_pos += sizeof(ObEvalInfo);
    expr.res_buf_off_ = frame_pos;
    expr.res_buf_len_ = sizeof(double);
    frame_pos += expr.res_buf_len_;
    expr.vector_header_off_ = frame_pos;
    frame_pos += sizeof(VectorHeader);
    expr.locate_datum_for_write(eval_ctx_).set_double(value);
    expr.set_evaluated_projected(eval_ctx_);
  }

  void init_varchar_const_expr(
      ObExpr &expr,
      int64_t &frame_pos,
      const ObString &value)
  {
    expr.eval_func_ = nullptr;
    expr.obj_meta_.set_varchar();
    expr.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    expr.datum_meta_.type_ = ObVarcharType;
    expr.datum_meta_.cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
    expr.obj_datum_map_ = OBJ_DATUM_STRING;
    expr.vec_value_tc_ = get_vec_value_tc(
        ObVarcharType, expr.datum_meta_.scale_, expr.datum_meta_.precision_);
    expr.frame_idx_ = 0;
    expr.datum_off_ = frame_pos;
    frame_pos += sizeof(ObDatum);
    expr.eval_info_off_ = frame_pos;
    frame_pos += sizeof(ObEvalInfo);
    expr.res_buf_off_ = frame_pos;
    expr.res_buf_len_ = 128;
    frame_pos += expr.res_buf_len_;
    expr.vector_header_off_ = frame_pos;
    frame_pos += sizeof(VectorHeader);
    expr.locate_datum_for_write(eval_ctx_).set_string(value);
    expr.set_evaluated_projected(eval_ctx_);
  }

  static int collect_async_scalar_rors(
      ObIDASSearchOp *root,
      ObIArray<ObDASAsyncScalarRorOp *> &rors)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(root)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (DAS_SEARCH_OP_ASYNC_SCALAR_ROR == root->get_op_type()) {
      ret = rors.push_back(static_cast<ObDASAsyncScalarRorOp *>(root));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < root->get_children_cnt(); ++i) {
        ret = collect_async_scalar_rors(root->get_children()[i], rors);
      }
    }
    return ret;
  }

  ObIDASSearchOp *find_search_op(
      ObIDASSearchOp *root,
      const ObDASSearchOpType target_type)
  {
    ObIDASSearchOp *found = nullptr;
    if (OB_ISNULL(root)) {
      // no subtree
    } else if (target_type == root->get_op_type()) {
      found = root;
    } else {
      for (int64_t i = 0; OB_ISNULL(found) && i < root->get_children_cnt(); ++i) {
        found = find_search_op(root->get_children()[i], target_type);
      }
    }
    return found;
  }

  void verify_zero_score_filter_domain(ObIDASSearchOp *root)
  {
    const uint64_t expected_ids[] = {3, 7};
    ObDASRowID rowid;
    double score = -1.0;
    ASSERT_NE(nullptr, root);
    ASSERT_EQ(OB_SUCCESS, root->open());
    for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
      ASSERT_EQ(OB_SUCCESS, root->next_rowid(rowid, score));
      EXPECT_EQ(expected_ids[i], rowid.get_uint64());
      EXPECT_DOUBLE_EQ(0.0, score);
    }
    EXPECT_EQ(OB_ITER_END, root->next_rowid(rowid, score));
    EXPECT_EQ(OB_SUCCESS, root->close());
  }

protected:
  ObArenaAllocator allocator_;
  ObArenaAllocator scan_allocator_;
  ObDASScanOp scan_op_;
  ObDASScanRtDef scan_rtdef_;
  ObExecContext exec_ctx_;
  ObEvalCtx eval_ctx_;
  ObExpr rowid_expr_;
  ExprFixedArray rowid_exprs_;
  ExprFixedArray output_;
  ObDASSearchCtx search_ctx_;
  std::vector<int64_t> segment_counts_;
  std::vector<std::vector<int64_t> > multi_segment_counts_;
  std::vector<bool> partition_aligned_;
  int64_t fetch_cursor_;
  static ObArenaAllocator fake_ls_allocator_;
  static storage::ObLSService fake_ls_service_;
  static storage::ObLS *fake_ls_;
};

ObArenaAllocator TestDASAsyncSegmentMerge::fake_ls_allocator_("AsyncSegLS");
storage::ObLSService TestDASAsyncSegmentMerge::fake_ls_service_;
storage::ObLS *TestDASAsyncSegmentMerge::fake_ls_ = nullptr;
TEST_F(TestDASAsyncSegmentMerge, rowid_only_operator_reports_unimplemented_nk_interfaces)
{
  MockSegmentOp leaf(search_ctx_, {1}, {1.0});
  ObDASNestedKey key;
  ObDASNestedKey target;
  double score = 0.0;
  EXPECT_EQ(OB_NOT_IMPLEMENT, leaf.next_rowid(key, score));
  EXPECT_EQ(OB_NOT_IMPLEMENT, leaf.advance_to(target, key, score));
}

// NK-only leaf for exercising the real composite operators' prefix seek paths.
class PrefixSeekLeaf : public ObIDASSearchOp
{
public:
  PrefixSeekLeaf(ObDASSearchCtx &ctx, const uint32_t *components,
                 const int32_t depth, const int64_t count)
    : ObIDASSearchOp(ctx), last_target_length_(0), bytes_(depth * 4 * count, '\0')
  {
    scope_ = DAS_SCOPE_NESTED_ASYNC;
    nesting_depth_ = depth;
    is_scoring_ = false;
    for (int64_t i = 0; i < depth * count; ++i) {
      const uint32_t encoded = htobe32(components[i]);
      MEMCPY(&bytes_[i * 4], &encoded, 4);
    }
  }

  int do_open() override { return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { return OB_NOT_SUPPORTED; }
  int do_next_rowid(ObDASRowID &, double &) override { return OB_NOT_SUPPORTED; }
  int do_advance_to(const ObDASRowID &, ObDASRowID &, double &) override
  { return OB_NOT_SUPPORTED; }
  int do_next_rowid(ObDASNestedKey &key, double &score) override
  {
    return read_next_nk(key, score);
  }
  int do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &key, double &score) override
  {
    return read_advance_to_nk(target, key, score);
  }
  int read_next_nk(ObDASNestedKey &rowid, double &score)
  {
    ++nk_pos_;
    score = 0.0;
    return current_nk(rowid);
  }
  int read_advance_to_nk(const ObDASNestedKey &target, ObDASNestedKey &rowid, double &score)
  {
    last_target_length_ = target.length();
    score = 0.0;
    nk_pos_ = target.is_max() ? bytes_.size() / (nesting_depth_ * 4) : MAX(0L, nk_pos_);
    ObDASNestedKey candidate;
    int ret = current_nk(candidate);
    while (OB_SUCCESS == ret && !target.is_min()
           && candidate.compare(target, target.length()) < 0) {
      ++nk_pos_;
      ret = current_nk(candidate);
    }
    if (OB_SUCCESS == ret) { rowid = candidate; }
    return ret;
  }
  int current_nk(ObDASNestedKey &key) const
  {
    const int32_t key_bytes = nesting_depth_ * 4;
    const int64_t count = bytes_.size() / key_bytes;
    int ret = OB_ITER_END;
    if (nk_pos_ >= 0 && nk_pos_ < count) {
      key.assign(bytes_.data() + nk_pos_ * key_bytes, key_bytes);
      ret = OB_SUCCESS;
    }
    return ret;
  }
  int32_t last_target_length_;
private:
  std::string bytes_;
  int64_t nk_pos_ = -1;
};

TEST_F(TestDASAsyncSegmentMerge, ror_inherits_consumer_depth_and_returns_complete_keys)
{
  char bytes[32];
  const uint32_t components[] = {1, 2, 3, 4, 5, 6, 7, 8};
  for (int i = 0; i < 8; ++i) {
    const uint32_t value = htobe32(components[i]);
    MEMCPY(bytes + i * 4, &value, 4);
  }
  for (int physical_depth : {1, 2, 3, 4}) {
    ObStringBuffer list_buf(&allocator_);
    ObSearchIndexNestedKeyListWriter writer;
    ASSERT_EQ(OB_SUCCESS, writer.init(list_buf));
    for (int i = 0; i < 2; ++i) {
      ObDatum key(bytes + i * physical_depth * 4, physical_depth * 4, false);
      ASSERT_EQ(OB_SUCCESS, writer.append(key));
    }
    ObString list;
    ASSERT_EQ(OB_SUCCESS, writer.build(list));
    const ObDatum list_datum(list.ptr(), list.length(), false);
    for (int depth = 1; depth <= physical_depth; ++depth) {
      ObDASScopeId old_scope;
      const int32_t old_depth = search_ctx_.get_op_creating_depth();
      search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_ASYNC, depth, old_scope);
      ObDASAsyncScalarRorOp ror(search_ctx_);
      search_ctx_.exit_scope_after_op_created(old_scope, old_depth);
      EXPECT_EQ(depth * 4, ror.nested_compare_bytes());
      ASSERT_EQ(OB_SUCCESS, ror.nested_key_list_reader_.init(list_datum));
      ror.is_first_nk_ = false;
      ObDASNestedKey key;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, ror.next_rowid(key, score));
      EXPECT_EQ(physical_depth * 4, key.length());
      if (physical_depth <= 2) {
        EXPECT_EQ(physical_depth == 1 ? 1ULL << 32 : (1ULL << 32) | 2, key.value());
      } else {
        EXPECT_EQ(list.ptr() + ObSearchIndexNestedKeyListFormat::HEADER_SIZE, key.ptr());
        EXPECT_EQ(0, MEMCMP(bytes, key.ptr(), physical_depth * 4));
      }
      ObDASNestedKey target;
      target.assign(bytes + physical_depth * 4, depth * 4);
      ASSERT_EQ(OB_SUCCESS, ror.advance_to(target, key, score));
      EXPECT_EQ(physical_depth * 4, key.length());
      EXPECT_EQ(0, target.compare(key, target.length()));
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, ror_reader_lazy_prefix_seeks_and_document_reinitialization)
{
  char keys[4][12];
  const uint32_t components[4][3] = {{1, 2, 3}, {1, 2, 7}, {1, 5, 1}, {2, 1, 1}};
  ObStringBuffer list_buf(&allocator_);
  ObSearchIndexNestedKeyListWriter list_writer;
  ASSERT_EQ(OB_SUCCESS, list_writer.init(list_buf));
  for (int i = 0; i < 4; ++i) {
    for (int j = 0; j < 3; ++j) {
      const uint32_t encoded = htobe32(components[i][j]);
      MEMCPY(keys[i] + j * 4, &encoded, 4);
    }
    const ObDatum key(keys[i], sizeof(keys[i]), false);
    ASSERT_EQ(OB_SUCCESS, list_writer.append(key));
  }
  ObString list;
  ASSERT_EQ(OB_SUCCESS, list_writer.build(list));

  // A cached posting value block exercises the real lazy list getter without IO.
  ObSEArray<ObColDesc, 1> columns;
  ObColDesc column;
  column.col_type_.set_varchar();
  ASSERT_EQ(OB_SUCCESS, columns.push_back(column));
  blocksstable::ObPostingValueBlockWriteParam param;
  param.col_descs_ = &columns;
  param.block_capacity_ = 2;
  param.position_list_col_idx_ = -1;
  param.nested_key_col_idx_ = 0;
  blocksstable::ObPostingValueBlockWriter value_writer;
  ASSERT_EQ(OB_SUCCESS, value_writer.init(param, allocator_));
  blocksstable::ObDatumRow value_row;
  ASSERT_EQ(OB_SUCCESS, value_row.init(1));
  value_row.storage_datums_[0].set_string(list);
  ASSERT_EQ(OB_SUCCESS, value_writer.append_row(value_row));
  value_row.storage_datums_[0].set_null();
  ASSERT_EQ(OB_SUCCESS, value_writer.append_row(value_row));
  blocksstable::ObPostingValueBlockDesc value_block;
  ASSERT_EQ(OB_SUCCESS, value_writer.build_block(value_block));
  blocksstable::ObInvSSTablePostingIter posting;
  ASSERT_EQ(OB_SUCCESS, posting.posting_value_block_reader_.init(value_block.buf_, value_block.size_));
  posting.is_opened_ = true;
  posting.is_end_ = false;
  posting.curr_block_loaded_ = true;
  posting.curr_value_block_loaded_ = true;
  posting.need_posting_value_ = true;
  posting.curr_row_idx_ = 0;
  posting.curr_id_storage_ = 10;
  posting.curr_id_datum_.ptr_ = reinterpret_cast<const char *>(&posting.curr_id_storage_);
  posting.curr_id_datum_.set_uint(10);
  posting.curr_id_ = &posting.curr_id_datum_;
  posting.curr_relevance_materialized_ = true;
  uint64_t doc_ids[] = {10, 20};
  posting.curr_meta_.idx_ = 0;
  posting.curr_meta_.locator_.block_idx_ = 0;
  posting.curr_meta_.min_id_ = 10;
  posting.curr_meta_.max_id_ = 20;
  posting.curr_meta_.doc_count_ = 2;
  blocksstable::ObPostingDocIdView doc_id_view;
  doc_id_view.values_ = doc_ids;
  doc_id_view.width_ = sizeof(uint64_t);
  blocksstable::ObPostingRelevanceView relevance_view;
  ASSERT_EQ(OB_SUCCESS, posting.posting_block_data_.assign(2, doc_id_view, relevance_view));

  ObDASAsyncScalarRorOp ror(search_ctx_);
  ror.scope_ = DAS_SCOPE_NESTED_ASYNC;
  ror.nesting_depth_ = 3;
  ror.is_scoring_ = false;
  ror.posting_iter_ = &posting;
  ObDASRowID doc_target;
  ObDASRowID doc;
  doc_target.set_uint64(10);
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, ror.advance_to(doc_target, doc, score));
  EXPECT_EQ(10, doc.get_uint64());
  EXPECT_TRUE(ror.is_first_nk_);
  EXPECT_FALSE(ror.nested_key_list_reader_.is_inited());

  ObDASNestedKey target;
  ObDASNestedKey key;
  target.assign(keys[1], 12);
  ASSERT_EQ(OB_SUCCESS, ror.advance_to(target, key, score));
  ASSERT_EQ(12, key.length());
  EXPECT_EQ(0, MEMCMP(keys[1], key.ptr(), 12));
  EXPECT_FALSE(ror.is_first_nk_);
  posting.need_posting_value_ = false; // Later NK calls must reuse the loaded reader.
  ASSERT_EQ(OB_SUCCESS, ror.advance_to(doc_target, doc, score));
  EXPECT_FALSE(ror.is_first_nk_);
  EXPECT_EQ(0, MEMCMP(keys[1], ror.curr_nested_key_.ptr(), 12));
  for (int prefix_bytes : {4, 8}) {
    target.assign(keys[0], prefix_bytes);
    ASSERT_EQ(OB_SUCCESS, ror.advance_to(target, key, score));
    EXPECT_EQ(0, MEMCMP(keys[1], key.ptr(), 12)); // Do not rewind inside the prefix.
  }
  target.set_min();
  ASSERT_EQ(OB_SUCCESS, ror.advance_to(target, key, score));
  EXPECT_EQ(0, MEMCMP(keys[1], key.ptr(), 12));
  ASSERT_EQ(OB_SUCCESS, ror.next_rowid(key, score));
  EXPECT_EQ(0, MEMCMP(keys[2], key.ptr(), 12));
  target.assign(keys[3], 4);
  ASSERT_EQ(OB_SUCCESS, ror.advance_to(target, key, score));
  EXPECT_EQ(0, MEMCMP(keys[3], key.ptr(), 12));
  EXPECT_EQ(OB_ITER_END, ror.next_rowid(key, score));
  target.set_min();
  EXPECT_EQ(OB_ITER_END, ror.advance_to(target, key, score));
  EXPECT_EQ(OB_ITER_END, ror.next_rowid(key, score));

  // Same-document seeks preserve EOF for both NK entry points.
  for (bool use_seek : {true, false}) {
    ASSERT_EQ(OB_SUCCESS, ror.advance_to(doc_target, doc, score));
    EXPECT_FALSE(ror.is_first_nk_);
    target.set_min();
    EXPECT_EQ(OB_ITER_END, use_seek ? ror.advance_to(target, key, score) : ror.next_rowid(key, score));
  }

  // A key shorter than the required depth is empty for that scope.
  posting.need_posting_value_ = true;
  ObDASAsyncScalarRorOp deeper(search_ctx_);
  deeper.scope_ = DAS_SCOPE_NESTED_ASYNC;
  deeper.nesting_depth_ = 4;
  deeper.is_scoring_ = false;
  deeper.posting_iter_ = &posting;
  EXPECT_EQ(OB_ITER_END, deeper.next_rowid(key, score));
  EXPECT_FALSE(deeper.is_first_nk_);
  deeper.posting_iter_ = nullptr;

  // Forward DocID seek resets NK lazily. The next cached posting has a NULL list.
  doc_target.set_uint64(20);
  ASSERT_EQ(OB_SUCCESS, ror.advance_to(doc_target, doc, score));
  EXPECT_EQ(20, doc.get_uint64());
  EXPECT_TRUE(ror.is_first_nk_);
  EXPECT_EQ(1, posting.curr_row_idx_);
  EXPECT_EQ(OB_ITER_END, ror.next_rowid(key, score));
  EXPECT_FALSE(ror.is_first_nk_);
  posting.need_posting_value_ = false;
  target.set_min();
  EXPECT_EQ(OB_ITER_END, ror.advance_to(target, key, score));
  EXPECT_EQ(OB_ITER_END, ror.next_rowid(key, score));
  ror.posting_iter_ = nullptr;
}

TEST_F(TestDASAsyncSegmentMerge, nested_precise_target_keeps_its_length_through_composites)
{
  ObDASRowID document;
  document.set_uint64(1);
  for (int32_t depth : {2, 4}) {
    const std::vector<uint32_t> keys = 2 == depth
        ? std::vector<uint32_t>{2, 1, 2, 5, 3, 1}
        : std::vector<uint32_t>{2, 3, 4, 1, 2, 3, 4, 5, 3, 1, 1, 1};
    for (int kind = 0; kind < 6; ++kind) {
      PrefixSeekLeaf leaf(search_ctx_, keys.data(), depth, 3);
      ObIDASSearchOp *children[] = {&leaf};
      ObDASConjunctionOp conjunction(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&leaf));
      ObDASDisjunctionFilterOp disjunction(search_ctx_);
      disjunction.children_ = children;
      disjunction.children_cnt_ = 1;
      disjunction.min_should_match_ = 1;
      disjunction.cur_row_id_ = document;
      ASSERT_EQ(OB_SUCCESS, disjunction.child_row_ids_.push_back(document));
      ObDASAsyncSegmentMergeOp merge(search_ctx_);
      merge.children_ = children;
      merge.children_cnt_ = 1;
      ASSERT_EQ(OB_SUCCESS, merge.pending_child_indices_.push_back(0));
      ObDASReqOptOp optional(search_ctx_);
      optional.required_ = &leaf;
      ObDASReqExclOp exclusion(search_ctx_);
      exclusion.required_ = &leaf;
      // No excluded posting for the current document.
      exclusion.is_first_nk_ = false;
      exclusion.nested_excluded_matches_doc_ = false;
      ObDASAsyncScalarRorUnionOp ror_union(search_ctx_);
      ror_union.children_ = children;
      ror_union.children_cnt_ = 1;
      ror_union.min_should_match_ = 1;
      ror_union.cur_row_id_ = document;
      ASSERT_EQ(OB_SUCCESS, ror_union.child_row_ids_.push_back(document));
      char disjunction_key_buf[16];
      char union_key_buf[16];
      ASSERT_EQ(OB_SUCCESS, merge.nested_key_heads_.prepare_allocate(1));
      disjunction.current_nk_buf_ = disjunction_key_buf;
      ror_union.current_nk_buf_ = union_key_buf;
      ASSERT_EQ(OB_SUCCESS, disjunction.child_nested_keys_.prepare_allocate(1));
      ASSERT_EQ(OB_SUCCESS, ror_union.child_nested_keys_.prepare_allocate(1));
      ObIDASSearchOp *ops[] = {&conjunction, &disjunction, &merge,
                              &optional, &exclusion, &ror_union};
      ObIDASSearchOp &op = *ops[kind];
      op.scope_ = DAS_SCOPE_NESTED_ASYNC;
      op.nesting_depth_ = depth;
      ObDASNestedKey row;
      double score = 0;
      ASSERT_EQ(OB_SUCCESS, op.next_rowid(row, score));
      const ObDASNestedKey first = row;
      // A precise target within the same parent prefix must advance the
      // full-length composite head instead of comparing only that prefix.
      char encoded[16];
      for (int i = 0; i < depth; ++i) {
        const uint32_t component = htobe32(keys[depth + i]);
        MEMCPY(encoded + i * 4, &component, 4);
      }
      ObDASNestedKey target;
      target.assign(encoded, depth * 4);
      ASSERT_EQ(OB_SUCCESS, op.advance_to(target, row, score));
      EXPECT_EQ(depth * 4, leaf.last_target_length_);
      EXPECT_EQ(0, target.compare(row, target.length()));
      int cmp = first.compare(row, op.nested_compare_bytes());
      EXPECT_LT(cmp, 0);
      ASSERT_EQ(OB_SUCCESS, op.next_rowid(row, score));
      ObDASNestedKey parent;
      parent.set_value(3ULL << 32, 4);
      EXPECT_EQ(0, row.compare(parent, parent.length()));
      EXPECT_EQ(OB_ITER_END, op.next_rowid(row, score));
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_short_target_conjunction_matches_full_keys)
{
  ObDASRowID document;
  document.set_uint64(1);
  const uint32_t a[] = {2, 1, 2, 5};
  const uint32_t b[] = {2, 3, 2, 5};
  PrefixSeekLeaf left(search_ctx_, a, 2, 2);
  PrefixSeekLeaf right(search_ctx_, b, 2, 2);
  ObDASConjunctionOp conjunction(search_ctx_);
  conjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  conjunction.nesting_depth_ = 2;
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&left));
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&right));
  ObDASNestedKey target;
  target.set_value(2ULL << 32, 4);
  ObDASNestedKey result;
  double score = 0;
  ASSERT_EQ(OB_SUCCESS, conjunction.advance_to(target, result, score));
  EXPECT_EQ((2ULL << 32) | 5, result.value());
  EXPECT_EQ(8, result.length());
  EXPECT_EQ(8, right.last_target_length_);
  EXPECT_EQ(OB_ITER_END, conjunction.next_rowid(result, score));
}

TEST_F(TestDASAsyncSegmentMerge, nested_short_target_disjunction_counts_full_keys)
{
  ObDASRowID document;
  document.set_uint64(1);
  // Child order deliberately differs from NK order. MSM=1 must return 2.1;
  // MSM=2 must skip the distinct 2.1/2.3 objects and return their common 2.5.
  const uint32_t a[] = {2, 3, 2, 5};
  const uint32_t b[] = {2, 1, 2, 5};
  for (int64_t msm : {1, 2}) {
    PrefixSeekLeaf left(search_ctx_, a, 2, 2);
    PrefixSeekLeaf right(search_ctx_, b, 2, 2);
    ObIDASSearchOp *children[] = {&left, &right};
    ObDASDisjunctionFilterOp disjunction(search_ctx_);
    disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
    disjunction.nesting_depth_ = 2;
    disjunction.children_ = children;
    disjunction.children_cnt_ = 2;
    disjunction.min_should_match_ = msm;
    // Both children have already been selected for the same document.
    disjunction.is_first_nk_ = false;
    ObDASNestedKey head;
    head.set_min();
    ASSERT_EQ(OB_SUCCESS, disjunction.child_nested_keys_.push_back(head));
    ASSERT_EQ(OB_SUCCESS, disjunction.child_nested_keys_.push_back(head));
    ObDASNestedKey target;
    target.set_value(2ULL << 32, 4);
    ObDASNestedKey result;
    double score = 0;
    ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, result, score));
    EXPECT_EQ((2ULL << 32) | (1 == msm ? 1 : 5), result.value());
    EXPECT_EQ(8, result.length());
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_short_target_disjunction_requires_same_full_object)
{
  const uint32_t a[] = {1, 2};
  const uint32_t b[] = {1, 3};
  PrefixSeekLeaf left(search_ctx_, a, 2, 1);
  PrefixSeekLeaf right(search_ctx_, b, 2, 1);
  ObIDASSearchOp *children[] = {&left, &right};
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  disjunction.nesting_depth_ = 2;
  disjunction.children_ = children;
  disjunction.children_cnt_ = 2;
  disjunction.min_should_match_ = 2;
  disjunction.is_first_nk_ = false;
  ASSERT_EQ(OB_SUCCESS, disjunction.child_nested_keys_.prepare_allocate(2));
  ObDASNestedKey target;
  target.set_value(1ULL << 32, 4);
  ObDASNestedKey result;
  double score = 0.0;
  EXPECT_EQ(OB_ITER_END, disjunction.advance_to(target, result, score));
}

TEST_F(TestDASAsyncSegmentMerge, seek_preserves_pending_heap_error)
{
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  merge.force_merge_ = true;
  merge.children_cnt_ = 2;
  merge.active_segment_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, merge.head_rowids_.prepare_allocate(2));
  merge.head_rowids_.at(1).set_uint64(20);
  // An allocated but unbound child must fail when its pending head is restored.
  ASSERT_EQ(OB_SUCCESS, merge.pending_child_indices_.push_back(1));
  ObDASRowID target;
  ObDASRowID result;
  target.set_uint64(10);
  result.set_uint64(99);
  double score = 0.0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, merge.do_advance_to(target, result, score));
  EXPECT_EQ(99, result.get_uint64());
  EXPECT_FALSE(merge.iter_end_);
}

TEST_F(TestDASAsyncSegmentMerge, nested_short_target_segment_merge_orders_full_keys)
{
  ObDASRowID document;
  document.set_uint64(1);
  const uint32_t a[] = {2, 3, 5};
  const uint32_t b[] = {2, 1, 9};
  PrefixSeekLeaf left(search_ctx_, a, 3, 1);
  PrefixSeekLeaf right(search_ctx_, b, 3, 1);
  ObIDASSearchOp *children[] = {&left, &right};
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  merge.scope_ = DAS_SCOPE_NESTED_ASYNC;
  merge.nesting_depth_ = 3;
  merge.children_ = children;
  merge.children_cnt_ = 2;
  merge.active_segment_cnt_ = 2;
  ASSERT_EQ(OB_SUCCESS, merge.pending_child_indices_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, merge.pending_child_indices_.push_back(1));
  ASSERT_EQ(OB_SUCCESS, merge.nested_key_heads_.prepare_allocate(2));
  ObDASNestedKey target;
  target.set_value(2ULL << 32, 4);
  ObDASNestedKey result;
  double score = 0;
  ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, result, score));
  EXPECT_EQ(12, result.length());
  EXPECT_EQ((2ULL << 32) | 1, ObDASNestedKey::load(result.ptr(), 8));
  EXPECT_EQ(4, left.last_target_length_);
  EXPECT_EQ(4, right.last_target_length_);
  ASSERT_EQ(OB_SUCCESS, merge.next_rowid(result, score));
  EXPECT_EQ((2ULL << 32) | 3, ObDASNestedKey::load(result.ptr(), 8));
}

TEST_F(TestDASAsyncSegmentMerge, nested_short_target_crosses_boundaries_without_padding)
{
  ObDASRowID document;
  document.set_uint64(1);
  const uint32_t keys[] = {2, 3, 5, 2, 4, 1, 3, 1, 2};
  PrefixSeekLeaf leaf(search_ctx_, keys, 3, 3);
  ObDASNestedQueryOp replies(search_ctx_);
  replies.scope_ = DAS_SCOPE_NESTED_ASYNC;
  replies.op_type_ = DAS_SEARCH_OP_NESTED;
  replies.nesting_depth_ = 3;
  replies.inner_op_ = &leaf;
  ObDASNestedQueryOp comments(search_ctx_);
  comments.scope_ = DAS_SCOPE_NESTED_ASYNC;
  comments.op_type_ = DAS_SEARCH_OP_NESTED;
  comments.nesting_depth_ = 2;
  comments.inner_op_ = &replies;
  ObDASNestedKey target;
  target.set_value(2ULL << 32, 4);
  ObDASNestedKey result;
  double score = 0;
  ASSERT_EQ(OB_SUCCESS, comments.advance_to(target, result, score));
  EXPECT_EQ(4, leaf.last_target_length_);
  EXPECT_EQ(4, result.length());
  EXPECT_EQ(2ULL << 32, result.value());
  ASSERT_EQ(OB_SUCCESS, comments.next_rowid(result, score));
  EXPECT_EQ(4, result.length());
  EXPECT_EQ(2ULL << 32, result.value()); // next does not skip the parent's prefix.
  // A precise target passes through both boundaries despite their shallower depths.
  const uint32_t wanted[] = {3, 1, 2};
  char encoded[12];
  for (int i = 0; i < 3; ++i) { const uint32_t v = htobe32(wanted[i]); MEMCPY(encoded + i * 4, &v, 4); }
  target.assign(encoded, sizeof(encoded));
  ASSERT_EQ(OB_SUCCESS, comments.advance_to(target, result, score));
  EXPECT_EQ(12, leaf.last_target_length_);
  EXPECT_EQ(4, result.length());
  EXPECT_EQ(3ULL << 32, result.value());
  EXPECT_EQ(OB_ITER_END, comments.next_rowid(result, score));
}

TEST_F(TestDASAsyncSegmentMerge, nested_short_target_preserves_long_keys)
{
  ObDASRowID document;
  document.set_uint64(1);
  const uint32_t keys[] = {1, 3, 255, 7, 1, 3, 255, 8, 1, 3, 256, 1};
  PrefixSeekLeaf leaf(search_ctx_, keys, 4, 3);
  ObDASNestedQueryOp nested(search_ctx_);
  nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
  nested.op_type_ = DAS_SEARCH_OP_NESTED;
  nested.nesting_depth_ = 4;
  nested.inner_op_ = &leaf;
  ObDASNestedKey target;
  target.set_value(1ULL << 32, 4);
  ObDASNestedKey result;
  double score = 0;
  ASSERT_EQ(OB_SUCCESS, nested.advance_to(target, result, score));
  EXPECT_EQ(4, leaf.last_target_length_);
  EXPECT_EQ(12, result.length());
  ASSERT_EQ(OB_SUCCESS, nested.next_rowid(result, score));
  EXPECT_EQ(255ULL << 32, ObDASNestedKey::load(result.ptr() + 8, 4));
  ASSERT_EQ(OB_SUCCESS, nested.next_rowid(result, score));
  EXPECT_EQ(256ULL << 32, ObDASNestedKey::load(result.ptr() + 8, 4));
  EXPECT_EQ(OB_ITER_END, nested.next_rowid(result, score));

  const uint32_t maximum[] = {UINT32_MAX, UINT32_MAX, UINT32_MAX, 0};
  for (int32_t depth : {2, 3, 4}) {
    PrefixSeekLeaf end_leaf(search_ctx_, maximum, depth, 1);
    ObDASNestedQueryOp end_nested(search_ctx_);
    end_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
    end_nested.op_type_ = DAS_SEARCH_OP_NESTED;
    end_nested.nesting_depth_ = depth;
    end_nested.inner_op_ = &end_leaf;
    result.set_min();
    ASSERT_EQ(OB_SUCCESS, end_nested.next_rowid(result, score));
    EXPECT_EQ(OB_ITER_END, end_nested.next_rowid(result, score));
  }
}

class MultiDocNKLeaf : public ObIDASSearchOp
{
public:
  MultiDocNKLeaf(ObDASSearchCtx &ctx, int depth, std::vector<uint64_t> docs,
                std::vector<std::vector<uint32_t>> keys)
      : ObIDASSearchOp(ctx), docs_(docs), keys_(keys)
  {
    scope_ = DAS_SCOPE_NESTED_ASYNC;
    nesting_depth_ = depth;
    is_scoring_ = false;
  }
  int do_open() override { return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { is_first_nk_ = true; doc_idx_ = -1; return OB_SUCCESS; }
  int do_rescan_segment(const ObNewVersionRange &) override { return do_rescan(); }
  int do_next_rowid(ObDASRowID &row, double &score) override
  {
    is_first_nk_ = true;
    ++doc_idx_;
    return current_doc(row, score);
  }
  int do_next_rowid(ObDASNestedKey &key, double &score) override
  {
    int ret = OB_SUCCESS;
    score = 0.0;
    if (is_first_nk_ && OB_FAIL(init_nested_key_cursor())) {
    } else {
      ret = read_next_nk(key, score);
    }
    return ret;
  }
  int do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &key, double &score) override
  {
    int ret = OB_SUCCESS;
    score = 0.0;
    if (is_first_nk_ && OB_FAIL(init_nested_key_cursor())) {
    } else {
      ret = read_advance_to_nk(target, key, score);
    }
    return ret;
  }
  bool is_first_nk_ = true;
  int read_next_nk(ObDASNestedKey &row, double &score)
  {
    ++nk_pos_;
    score = 0.0;
    return current_nk(row);
  }
  int read_advance_to_nk(const ObDASNestedKey &target, ObDASNestedKey &row, double &score)
  {
    ++nk_seeks_;
    score = 0.0;
    nk_pos_ = target.is_max() ? bytes_.size() / (nesting_depth_ * 4) : MAX(0L, nk_pos_);
    ObDASNestedKey candidate;
    int ret = current_nk(candidate);
    while (OB_SUCCESS == ret && !target.is_min()
           && candidate.compare(target, target.length()) < 0) {
      ++nk_pos_;
      ret = current_nk(candidate);
    }
    if (OB_SUCCESS == ret) { row = candidate; }
    return ret;
  }
  int current_nk(ObDASNestedKey &key) const
  {
    const int32_t key_bytes = nesting_depth_ * 4;
    const int64_t count = bytes_.size() / key_bytes;
    int ret = OB_ITER_END;
    if (nk_pos_ >= 0 && nk_pos_ < count) {
      key.assign(bytes_.data() + nk_pos_ * key_bytes, key_bytes);
      ret = OB_SUCCESS;
    }
    return ret;
  }
  int do_advance_to(const ObDASRowID &target, ObDASRowID &row, double &score) override
  {
    int ret = OB_SUCCESS;
    const int64_t previous_doc_idx = doc_idx_;
    ++doc_seeks_;
    if (target.is_max()) {
      doc_idx_ = docs_.size();
      ret = OB_ITER_END;
    } else {
      doc_idx_ = MAX(0L, doc_idx_);
      while (doc_idx_ < docs_.size() && !target.is_min() && docs_[doc_idx_] < target.get_uint64()) {
        ++doc_idx_;
      }
      ret = current_doc(row, score);
    }
    if (doc_idx_ != previous_doc_idx) {
      is_first_nk_ = true;
    }
    return ret;
  }
  int init_nested_key_cursor()
  {
    ++list_loads_;
    bytes_.clear();
    for (uint32_t value : keys_[doc_idx_]) {
      const uint32_t encoded = htobe32(value);
      bytes_.append(reinterpret_cast<const char *>(&encoded), sizeof(encoded));
    }
    nk_pos_ = -1;
    is_first_nk_ = false;
    return OB_SUCCESS;
  }


  int current_doc(ObDASRowID &row, double &score)
  {
    int ret = OB_SUCCESS;
    if (doc_idx_ < 0 || doc_idx_ >= docs_.size()) {
      ret = OB_ITER_END;
    } else {
      row.set_uint64(docs_[doc_idx_]);
      score = doc_score_;
    }
    return ret;
  }
  double doc_score_ = 0.0;
  int64_t doc_seeks_ = 0;
  int64_t nk_seeks_ = 0;
  int64_t list_loads_ = 0;
private:
  std::vector<uint64_t> docs_;
  std::vector<std::vector<uint32_t>> keys_;
  int64_t doc_idx_ = -1;
  std::string bytes_;
  int64_t nk_pos_ = -1;
};

TEST_F(TestDASAsyncSegmentMerge, nested_interfaces_preserve_document_and_object_cursors)
{
  MultiDocNKLeaf leaf(search_ctx_, 1, {1, 2, 3}, {{1, 3}, {}, {8}});
  ObDASRowID doc;
  ObDASRowID target;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, leaf.next_rowid(doc, score));
  EXPECT_EQ(0, leaf.list_loads_);
  ASSERT_EQ(OB_SUCCESS, leaf.next_rowid(key, score));
  EXPECT_EQ(1ULL << 32, key.value());
  target.set_uint64(1);
  ASSERT_EQ(OB_SUCCESS, leaf.advance_to(target, doc, score));
  ASSERT_EQ(OB_SUCCESS, leaf.advance_to(key, key, score));
  EXPECT_EQ(1, leaf.list_loads_);
  ASSERT_EQ(OB_SUCCESS, leaf.next_rowid(key, score));
  EXPECT_EQ(3ULL << 32, key.value());
  EXPECT_EQ(OB_ITER_END, leaf.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, leaf.next_rowid(doc, score));
  EXPECT_EQ(2, doc.get_uint64());
  EXPECT_EQ(OB_ITER_END, leaf.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, leaf.next_rowid(doc, score));
  ASSERT_EQ(OB_SUCCESS, leaf.next_rowid(key, score));
  EXPECT_EQ(8ULL << 32, key.value());
  ASSERT_EQ(OB_SUCCESS, leaf.rescan());
  target.set_uint64(3);
  const int64_t loaded = leaf.list_loads_;
  ASSERT_EQ(OB_SUCCESS, leaf.advance_to(target, doc, score));
  EXPECT_EQ(loaded, leaf.list_loads_);
  ASSERT_EQ(OB_SUCCESS, leaf.next_rowid(key, score));
  EXPECT_EQ(8ULL << 32, key.value());
}

TEST_F(TestDASAsyncSegmentMerge, nested_disjunction_same_document_seek_preserves_child_keys)
{
  MultiDocNKLeaf left(search_ctx_, 1, {1, 2}, {{1, 3}, {7}});
  MultiDocNKLeaf right(search_ctx_, 1, {1, 2}, {{1, 5}, {7}});
  ObIDASSearchOp *children[] = {&left, &right};
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  disjunction.nesting_depth_ = 1;
  disjunction.children_ = children;
  disjunction.children_cnt_ = 2;
  disjunction.min_should_match_ = 1;
  ASSERT_EQ(OB_SUCCESS, disjunction.reset_merge_cursor());
  ObDASRowID document;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(document, score));
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
  EXPECT_EQ(1ULL << 32, key.value());
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
  EXPECT_EQ(3ULL << 32, key.value());
  const int64_t left_seeks = left.doc_seeks_;
  const int64_t right_seeks = right.doc_seeks_;
  for (int round = 0; round < 2; ++round) {
    ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document, document, score));
    EXPECT_EQ(1, document.get_uint64());
    ObDASNestedKey target;
    ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, key, score));
    EXPECT_EQ(3ULL << 32, key.value());
  }
  EXPECT_EQ(left_seeks, left.doc_seeks_);
  EXPECT_EQ(right_seeks, right.doc_seeks_);
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
  EXPECT_EQ(5ULL << 32, key.value()); // Preserve the other child's prefetched head.
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document, document, score));
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(key, score));
  ObDASRowID earlier;
  earlier.set_min();
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(earlier, document, score));
  EXPECT_EQ(1, document.get_uint64());
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(key, score));
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(1, right.list_loads_);

  document.set_uint64(2);
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document, document, score));
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
  EXPECT_EQ(7ULL << 32, key.value());
  EXPECT_EQ(2, left.list_loads_);
  EXPECT_EQ(2, right.list_loads_);
}

TEST_F(TestDASAsyncSegmentMerge, nested_disjunction_same_document_seek_preserves_msm_and_long_keys)
{
  for (int depth : {1, 2, 3}) {
    auto keys = [depth](std::initializer_list<uint32_t> first_components) {
      std::vector<uint32_t> result;
      for (uint32_t first : first_components) {
        result.push_back(first);
        for (int i = 1; i < depth; ++i) {
          result.push_back(1);
        }
      }
      return result;
    };
    const auto first_component = [](const ObDASNestedKey &key) {
      return (key.length() <= sizeof(uint64_t)
          ? key.value() : ObDASNestedKey::load(key.ptr(), 8)) >> 32;
    };
    for (int64_t msm : {0, 1, 2}) {
      MultiDocNKLeaf left(search_ctx_, depth, {1, 2}, {keys({1, 3}), keys({7})});
      MultiDocNKLeaf right(search_ctx_, depth, {1, 2}, {keys({1, 5}), keys({7})});
      ObIDASSearchOp *children[] = {&left, &right};
      ObDASDisjunctionFilterOp disjunction(search_ctx_);
      disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
      disjunction.nesting_depth_ = depth;
      disjunction.children_ = children;
      disjunction.children_cnt_ = 2;
      disjunction.min_should_match_ = msm;
      ASSERT_EQ(OB_SUCCESS, disjunction.open());
      ObDASRowID document;
      ObDASNestedKey key;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(document, score));
      ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
      EXPECT_EQ(1, first_component(key));
      ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document, document, score));
      ObDASNestedKey target;
      ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, key, score));
      EXPECT_EQ(1, first_component(key));
      if (msm <= 1) {
        for (uint32_t first : {3, 5}) {
          ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
          EXPECT_EQ(first, first_component(key));
        }
      }
      EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(key, score));
      ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document, document, score));
      EXPECT_EQ(OB_ITER_END, disjunction.advance_to(target, key, score));
      EXPECT_EQ(0, left.doc_seeks_);
      EXPECT_EQ(0, right.doc_seeks_);
      EXPECT_EQ(1, left.list_loads_);
      EXPECT_EQ(1, right.list_loads_);

      document.set_uint64(2);
      ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document, document, score));
      ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
      EXPECT_EQ(7, first_component(key));
      EXPECT_EQ(2, left.list_loads_);
      EXPECT_EQ(2, right.list_loads_);
      if (0 == msm) {
        // A different optional candidate is still accepted, even below the cached DocID.
        document.set_uint64(1);
        ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document, document, score));
        EXPECT_EQ(1, document.get_uint64());
        EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(key, score));
        target.set_value(9ULL << 32, 4);
        ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, key, score));
        EXPECT_EQ(9, first_component(key));
        ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document, document, score));
        target.set_min();
        ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, key, score));
        EXPECT_EQ(9, first_component(key));
      }
      ASSERT_EQ(OB_SUCCESS, disjunction.close());
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_ror_union_single_child_forwards_document_and_key_cursors)
{
  for (int32_t depth : {1, 2, 3}) {
    auto make_key = [depth](uint32_t first) {
      std::vector<uint32_t> components(depth, 1);
      components[0] = first;
      return components;
    };
    std::vector<uint32_t> first_keys = make_key(1);
    const std::vector<uint32_t> second_key = make_key(5);
    first_keys.insert(first_keys.end(), second_key.begin(), second_key.end());
    MultiDocNKLeaf leaf(search_ctx_, depth, {1, 2, 3}, {first_keys, {}, make_key(9)});
    MultiDocNKLeaf other(search_ctx_, depth, {1}, {make_key(2)});
    ObIDASSearchOp *children[] = {&leaf, &other};
    ObDASScalarScanRtDef rtdef;
    ObNewRange range;
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(range));
    ObDASAsyncScalarRorUnionOp ror_union(search_ctx_);
    ror_union.scope_ = DAS_SCOPE_NESTED_ASYNC;
    ror_union.nesting_depth_ = depth;
    ror_union.scalar_rtdef_ = &rtdef;
    ror_union.children_ = children;
    ror_union.children_cnt_ = ARRAYSIZEOF(children);
    ror_union.min_should_match_ = 1;
    ASSERT_EQ(OB_SUCCESS, ror_union.open());
    ASSERT_TRUE(ror_union.use_single_child_fast_path());

    auto check_key = [depth](const ObDASNestedKey &key, uint32_t first) {
      char bytes[12];
      for (int32_t i = 0; i < depth; ++i) {
        const uint32_t component = htobe32(0 == i ? first : 1);
        MEMCPY(bytes + i * sizeof(component), &component, sizeof(component));
      }
      ObDASNestedKey expected;
      expected.assign(bytes, depth * 4);
      EXPECT_EQ(depth * 4, key.length());
      EXPECT_EQ(0, key.compare(expected, depth * 4));
    };
    ObDASRowID document;
    ObDASNestedKey key;
    double score = 0.0;
    ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(document, score));
    EXPECT_EQ(1, document.get_uint64());
    EXPECT_EQ(0, leaf.list_loads_);
    ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
    check_key(key, 1);

    // Repositioning the same document preserves NK, including after EOF.
    ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(document, document, score));
    ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(document, document, score));
    EXPECT_EQ(1, leaf.list_loads_);
    ObDASNestedKey prefix;
    prefix.set_value(5ULL << 32, 4);
    ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(prefix, key, score));
    check_key(key, 5);
    EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));
    ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(document, document, score));
    EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));
    EXPECT_EQ(1, leaf.list_loads_);

    // An empty NK list ends only the NK stage; the next document remains readable.
    ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(document, score));
    EXPECT_EQ(2, document.get_uint64());
    EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));
    ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(document, score));
    EXPECT_EQ(3, document.get_uint64());
    ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
    check_key(key, 9);
    EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));
    EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(document, score));
    EXPECT_EQ(0, other.doc_seeks_);
    EXPECT_EQ(0, other.list_loads_);

    // Activating another range restores DISJ's DocID and NK merge paths.
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(range));
    ASSERT_EQ(OB_SUCCESS, ror_union.rescan());
    ASSERT_FALSE(ror_union.use_single_child_fast_path());
    ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(document, score));
    EXPECT_EQ(1, document.get_uint64());
    for (uint32_t first : {1, 2, 5}) {
      ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
      check_key(key, first);
    }
    EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));

    rtdef.key_ranges_.reuse();
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(range));
    ASSERT_EQ(OB_SUCCESS, ror_union.rescan());
    ASSERT_TRUE(ror_union.use_single_child_fast_path());
    document.set_uint64(3);
    ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(document, document, score));
    ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
    check_key(key, 9);
    ASSERT_EQ(OB_SUCCESS, ror_union.close());
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_ror_union_same_document_seek_preserves_child_keys)
{
  MultiDocNKLeaf left(search_ctx_, 1, {1}, {{1, 3}});
  MultiDocNKLeaf right(search_ctx_, 1, {1}, {{1, 5}});
  ObIDASSearchOp *children[] = {&left, &right};
  ObDASAsyncScalarRorUnionOp ror_union(search_ctx_);
  ror_union.scope_ = DAS_SCOPE_NESTED_ASYNC;
  ror_union.nesting_depth_ = 1;
  ror_union.children_ = children;
  ror_union.children_cnt_ = 2;
  ror_union.min_should_match_ = 1;
  ASSERT_EQ(OB_SUCCESS, ror_union.reset_merge_cursor());
  ObDASRowID document;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(document, score));
  EXPECT_EQ(1, document.get_uint64());
  EXPECT_EQ(0, left.list_loads_);
  EXPECT_EQ(0, right.list_loads_);
  ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
  EXPECT_EQ(1ULL << 32, key.value());
  ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
  EXPECT_EQ(3ULL << 32, key.value());

  const int64_t left_seeks = left.doc_seeks_;
  const int64_t right_seeks = right.doc_seeks_;
  for (int64_t round = 0; round < 2; ++round) {
    ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(document, document, score));
    ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(document, document, score));
    EXPECT_EQ(1, document.get_uint64());
    EXPECT_EQ(1, left.list_loads_);
    EXPECT_EQ(1, right.list_loads_);
    ObDASNestedKey target;
    if (0 == round) {
      ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(target, key, score));
      EXPECT_EQ(3ULL << 32, key.value());
      ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
      EXPECT_EQ(5ULL << 32, key.value());
      EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));
    } else {
      EXPECT_EQ(OB_ITER_END, ror_union.advance_to(target, key, score));
      EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));
    }
  }
  EXPECT_EQ(left_seeks, left.doc_seeks_);
  EXPECT_EQ(right_seeks, right.doc_seeks_);
  EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(document, score));
}

TEST_F(TestDASAsyncSegmentMerge, nested_ror_union_forward_document_seek_changes_contributors)
{
  MultiDocNKLeaf left(search_ctx_, 1, {1, 3}, {{1, 3}, {7}});
  MultiDocNKLeaf right(search_ctx_, 1, {1, 2, 3}, {{1, 5}, {6}, {8}});
  ObIDASSearchOp *children[] = {&left, &right};
  ObDASAsyncScalarRorUnionOp ror_union(search_ctx_);
  ror_union.scope_ = DAS_SCOPE_NESTED_ASYNC;
  ror_union.nesting_depth_ = 1;
  ror_union.children_ = children;
  ror_union.children_cnt_ = 2;
  ror_union.min_should_match_ = 1;
  ASSERT_EQ(OB_SUCCESS, ror_union.reset_merge_cursor());
  ObDASRowID document;
  ObDASRowID target;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(document, score));
  EXPECT_EQ(1, document.get_uint64());
  ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
  EXPECT_EQ(1ULL << 32, key.value());

  target.set_uint64(2);
  ASSERT_EQ(OB_SUCCESS, ror_union.advance_to(target, document, score));
  EXPECT_EQ(2, document.get_uint64());
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(1, right.list_loads_);
  ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
  EXPECT_EQ(6ULL << 32, key.value());
  EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(2, right.list_loads_);

  ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(document, score));
  EXPECT_EQ(3, document.get_uint64());
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(2, right.list_loads_);
  for (uint64_t value : {7, 8}) {
    ASSERT_EQ(OB_SUCCESS, ror_union.next_rowid(key, score));
    EXPECT_EQ(value << 32, key.value());
  }
  EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(key, score));
  EXPECT_EQ(2, left.list_loads_);
  EXPECT_EQ(3, right.list_loads_);
  target.set_uint64(4);
  EXPECT_EQ(OB_ITER_END, ror_union.advance_to(target, document, score));
  EXPECT_EQ(OB_ITER_END, ror_union.next_rowid(document, score));
}


// Each NK read overwrites the child's previous output, as composite children can do.
class ReusingOutputNKLeaf : public MultiDocNKLeaf
{
public:
  using MultiDocNKLeaf::MultiDocNKLeaf;
  int do_next_rowid(ObDASNestedKey &key, double &score) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(MultiDocNKLeaf::do_next_rowid(key, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to fetch next nested key", K(ret));
    } else {
      copy_output(key);
    }
    return ret;
  }
  int do_advance_to(const ObDASNestedKey &target, ObDASNestedKey &key, double &score) override
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(MultiDocNKLeaf::do_advance_to(target, key, score))) {
      LOG_WARN_IGNORE_ITER_END(ret, "failed to advance nested key", K(ret));
    } else {
      copy_output(key);
    }
    return ret;
  }
private:
  void copy_output(ObDASNestedKey &key)
  {
    if (key.length() > sizeof(uint64_t)) {
      MEMCPY(output_, key.ptr(), key.length());
      key.assign(output_, key.length());
    }
  }
  char output_[12];
};

TEST_F(TestDASAsyncSegmentMerge, nested_single_segment_skips_both_heaps)
{
  for (bool force_merge : {false, true}) {
    for (int depth : {1, 3}) {
      ObDASAsyncIndexQueryCtx catalog;
      build_catalog(catalog, {1});
      const auto keys = [depth](std::initializer_list<uint32_t> values) {
        std::vector<uint32_t> result;
        for (uint32_t value : values) {
          result.push_back(value);
          for (int i = 1; i < depth; ++i) {
            result.push_back(1);
          }
        }
        return result;
      };
      ReusingOutputNKLeaf leaf(search_ctx_, depth, {1, 5}, {keys({1, 3}), keys({7})});
      ObArray<ObIDASSearchOp *> children;
      ASSERT_EQ(OB_SUCCESS, children.push_back(&leaf));
      ObDASAsyncSegmentMergeOpParam param(
          children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), force_merge);
      param.set_is_scoring(false);
      ObDASAsyncSegmentMergeOp merge(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, merge.init(param));
      merge.scope_ = DAS_SCOPE_NESTED_ASYNC;
      merge.nesting_depth_ = depth;
      ASSERT_EQ(OB_SUCCESS, merge.open());
      EXPECT_EQ(!force_merge, merge.use_single_child_fast_path());
      EXPECT_EQ(!force_merge, merge.heap_child_indices_.empty());
      EXPECT_FALSE(merge.use_batch_merge_);
      const auto first_component = [](const ObDASNestedKey &key) {
        return (key.length() <= sizeof(uint64_t)
            ? key.value() : ObDASNestedKey::load(key.ptr(), 8)) >> 32;
      };
      ObDASRowID doc;
      ObDASNestedKey key;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
      EXPECT_EQ(1, doc.get_uint64());
      EXPECT_EQ(!force_merge, merge.current_doc_has_single_nk_source());
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
      EXPECT_EQ(1, first_component(key));
      ASSERT_EQ(OB_SUCCESS, merge.advance_to(doc, doc, score));
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
      EXPECT_EQ(3, first_component(key));
      EXPECT_EQ(1, leaf.list_loads_);
      EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
      EXPECT_EQ(5, doc.get_uint64());
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
      EXPECT_EQ(7, first_component(key));
      EXPECT_EQ(depth * 4, key.length());
      EXPECT_TRUE(merge.nested_heap_child_indices_.empty());
      EXPECT_EQ(OB_ITER_END, merge.next_rowid(doc, score));
      ASSERT_EQ(OB_SUCCESS, merge.rescan());
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
      EXPECT_EQ(1, doc.get_uint64());
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
      EXPECT_EQ(1, first_component(key));
      ASSERT_EQ(OB_SUCCESS, merge.close());
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_sole_live_seek_preserves_and_resets_key_cursor)
{
  for (bool force_merge : {false, true}) {
    ObDASAsyncIndexQueryCtx catalog;
    build_catalog(catalog, {3});
    PinnedOverlappingCatalog pinned_storage(allocator_);
    ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
    MultiDocNKLeaf early(search_ctx_, 1, {1}, {{1, 3}});
    MultiDocNKLeaf middle(search_ctx_, 1, {1, 2}, {{2, 4}, {8}});
    MultiDocNKLeaf live(search_ctx_, 1, {3, 5, 8}, {{7, 9}, {}, {14}});
    ObArray<ObIDASSearchOp *> children;
    ASSERT_EQ(OB_SUCCESS, children.push_back(&early));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&middle));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&live));
    ObDASAsyncSegmentMergeOpParam param(
        children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), force_merge);
    param.set_is_scoring(false);
    ObDASAsyncSegmentMergeOp merge(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, merge.init(param));
    merge.scope_ = DAS_SCOPE_NESTED_ASYNC;
    merge.nesting_depth_ = 1;
    ASSERT_EQ(OB_SUCCESS, merge.open());
    EXPECT_FALSE(merge.use_ordered_concat_);
    EXPECT_FALSE(merge.use_batch_merge_);
    ObDASRowID doc;
    ObDASNestedKey key;
    double score = 0.0;
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
    EXPECT_EQ(1, doc.get_uint64());
    EXPECT_FALSE(merge.current_doc_has_single_nk_source());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
    EXPECT_EQ(2, doc.get_uint64());
    EXPECT_EQ(!force_merge, merge.current_doc_has_single_nk_source());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(8ULL << 32, key.value());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
    EXPECT_EQ(3, doc.get_uint64());
    EXPECT_TRUE(merge.heap_child_indices_.empty());
    ASSERT_EQ(1, merge.pending_child_indices_.count());
    EXPECT_EQ(2, merge.pending_child_indices_.at(0));
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(7ULL << 32, key.value());
    bool handled = false;
    ASSERT_EQ(OB_SUCCESS, merge.try_advance_single_live(doc, doc, score, handled));
    EXPECT_EQ(!force_merge, handled);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(doc, doc, score));
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(9ULL << 32, key.value());
    EXPECT_EQ(1, live.list_loads_);
    EXPECT_EQ(0, live.doc_seeks_);
    ObDASRowID target;
    target.set_uint64(4);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, doc, score));
    EXPECT_EQ(5, doc.get_uint64());
    EXPECT_EQ(force_merge ? 0 : 1, live.doc_seeks_);
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
    target.set_uint64(8);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, doc, score));
    EXPECT_EQ(8, doc.get_uint64());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(14ULL << 32, key.value());
    const int64_t seeks = live.doc_seeks_;
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(doc, doc, score));
    EXPECT_EQ(seeks, live.doc_seeks_);
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));

    ASSERT_EQ(OB_SUCCESS, merge.rescan());
    target.set_uint64(3);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, doc, score));
    EXPECT_EQ(3, doc.get_uint64());
    ObObj lo;
    ObObj hi;
    lo.set_min_value();
    hi.set_uint64(7);
    search_ctx_.set_docid_range(lo, hi);
    target.set_uint64(6);
    EXPECT_EQ(OB_ITER_END, merge.advance_to(target, doc, score));
    EXPECT_TRUE(merge.iter_end_);
    search_ctx_.has_docid_range_ = false;
    ASSERT_EQ(OB_SUCCESS, merge.close());
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_sole_live_key_seek_after_other_contributors_exhaust)
{
  for (bool force_merge : {false, true}) {
    for (int depth : {1, 3}) {
      ObDASAsyncIndexQueryCtx catalog;
      build_catalog(catalog, {2});
      PinnedOverlappingCatalog pinned_storage(allocator_);
      ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
      const auto keys = [depth](std::initializer_list<uint32_t> values) {
        std::vector<uint32_t> result;
        for (uint32_t value : values) {
          result.push_back(value);
          for (int i = 1; i < depth; ++i) {
            result.push_back(1);
          }
        }
        return result;
      };
      ReusingOutputNKLeaf left(search_ctx_, depth, {1, 2}, {keys({1, 2}), keys({1, 3})});
      ReusingOutputNKLeaf right(search_ctx_, depth, {1, 2},
                               {keys({1, 3, 7, 9}), keys({1, 4})});
      ObArray<ObIDASSearchOp *> children;
      ASSERT_EQ(OB_SUCCESS, children.push_back(&left));
      ASSERT_EQ(OB_SUCCESS, children.push_back(&right));
      ObDASAsyncSegmentMergeOpParam param(
          children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), force_merge);
      param.set_is_scoring(false);
      ObDASAsyncSegmentMergeOp merge(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, merge.init(param));
      merge.scope_ = DAS_SCOPE_NESTED_ASYNC;
      merge.nesting_depth_ = depth;
      ASSERT_EQ(OB_SUCCESS, merge.open());
      const auto first_component = [](const ObDASNestedKey &key) {
        return (key.length() <= sizeof(uint64_t)
            ? key.value() : ObDASNestedKey::load(key.ptr(), 8)) >> 32;
      };
      ObDASRowID doc;
      ObDASNestedKey key;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
      ASSERT_EQ(2, merge.pending_child_indices_.count());
      EXPECT_FALSE(merge.current_doc_has_single_nk_source());
      for (uint64_t value : {1, 2, 3}) {
        ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
        EXPECT_EQ(value, first_component(key));
      }
      EXPECT_TRUE(merge.nested_heap_child_indices_.empty());
      ASSERT_EQ(1, merge.nested_pending_child_indices_.count());
      EXPECT_EQ(1, merge.nested_pending_child_indices_.at(0));
      ObDASNestedKey target;
      target.set_value(5ULL << 32, 4);
      ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, key, score));
      EXPECT_EQ(7, first_component(key));
      EXPECT_EQ(depth * 4, key.length());
      ASSERT_EQ(OB_SUCCESS, merge.advance_to(doc, doc, score));
      ASSERT_EQ(OB_SUCCESS, merge.advance_to(key, key, score));
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
      EXPECT_EQ(9, first_component(key));
      target.set_value(10ULL << 32, 4);
      EXPECT_EQ(OB_ITER_END, merge.advance_to(target, key, score));
      EXPECT_TRUE(merge.curr_nk_.is_max());
      EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
      EXPECT_EQ(2, doc.get_uint64());
      ASSERT_EQ(2, merge.pending_child_indices_.count());
      for (uint64_t value : {1, 3, 4}) {
        ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
        EXPECT_EQ(value, first_component(key));
      }
      EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
      ASSERT_EQ(OB_SUCCESS, merge.close());
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_disjoint_segments_concat_preserves_document_and_key_cursors)
{
  for (int depth : {1, 3}) {
    ObDASAsyncIndexQueryCtx catalog;
    build_catalog(catalog, {3});
    PinnedSegmentTables storage(allocator_);
    const uint64_t min_ids[] = {20, 1, 10};
    const uint64_t max_ids[] = {24, 5, 14};
    ASSERT_EQ(OB_SUCCESS, storage.init(
        *catalog.partitions_.at(0), min_ids, max_ids, ARRAYSIZEOF(min_ids)));
    for (int i = 0; i < ARRAYSIZEOF(min_ids); ++i) {
      storage.tables_[i].inv_meta_.stats_.has_nested_key_ = true;
    }
    const auto keys = [depth](std::initializer_list<uint32_t> values) {
      std::vector<uint32_t> result;
      for (uint32_t value : values) {
        result.push_back(value);
        for (int i = 1; i < depth; ++i) {
          result.push_back(1);
        }
      }
      return result;
    };
    ReusingOutputNKLeaf late(search_ctx_, depth, {20, 24}, {keys({20}), keys({24})});
    ReusingOutputNKLeaf early(search_ctx_, depth, {1, 5}, {keys({1, 3, 5}), {}});
    ReusingOutputNKLeaf middle(search_ctx_, depth, {10, 14}, {keys({7, 9}), keys({14})});
    ObArray<ObIDASSearchOp *> children;
    ASSERT_EQ(OB_SUCCESS, children.push_back(&late));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&early));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&middle));
    ObDASAsyncSegmentMergeOpParam param(
        children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
    param.set_is_scoring(false);
    ObDASAsyncSegmentMergeOp merge(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, merge.init(param));
    merge.scope_ = DAS_SCOPE_NESTED_ASYNC;
    merge.nesting_depth_ = depth;
    ASSERT_EQ(OB_SUCCESS, merge.open());
    ASSERT_TRUE(merge.use_ordered_concat_);
    EXPECT_FALSE(merge.use_batch_merge_);
    EXPECT_TRUE(merge.heap_child_indices_.empty());
    ObDASRowID doc;
    ObDASNestedKey key;
    double score = 0.0;
    const auto first_component = [](const ObDASNestedKey &key) {
      return (key.length() <= sizeof(uint64_t)
          ? key.value() : ObDASNestedKey::load(key.ptr(), 8)) >> 32;
    };
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
    EXPECT_EQ(1, doc.get_uint64());
    EXPECT_TRUE(merge.pending_child_indices_.empty());
    EXPECT_EQ(1, merge.current_ordered_child_idx());
    EXPECT_EQ(0, early.list_loads_);
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(1, first_component(key));
    const int64_t seeks = early.doc_seeks_;
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(doc, doc, score));
    EXPECT_EQ(seeks, early.doc_seeks_);
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(3, first_component(key));
    // Long targets can alias the child's reusable output buffer.
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(key, key, score));
    EXPECT_EQ(3, first_component(key));
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(5, first_component(key));
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(doc, doc, score));
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
    EXPECT_EQ(1, early.list_loads_);
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
    EXPECT_EQ(5, doc.get_uint64());
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));

    doc.set_uint64(10);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(doc, doc, score));
    EXPECT_EQ(10, doc.get_uint64());
    EXPECT_TRUE(merge.pending_child_indices_.empty());
    EXPECT_EQ(2, merge.current_ordered_child_idx());
    ObDASNestedKey target;
    target.set_value(8ULL << 32, 4);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, key, score));
    EXPECT_EQ(9, first_component(key));
    EXPECT_EQ(depth * 4, key.length());
    doc.set_uint64(12);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(doc, doc, score));
    EXPECT_EQ(14, doc.get_uint64());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(14, first_component(key));
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(doc, score));
    EXPECT_EQ(20, doc.get_uint64());
    EXPECT_TRUE(merge.pending_child_indices_.empty());
    EXPECT_EQ(0, merge.current_ordered_child_idx());
    const MaxScoreTuple *tuple = nullptr;
    ASSERT_EQ(OB_SUCCESS, merge.advance_shallow(doc, false, tuple));
    ASSERT_NE(nullptr, tuple);
    EXPECT_EQ(24, tuple->get_min_id().get_uint64());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(24, first_component(key));
    EXPECT_TRUE(merge.nested_heap_child_indices_.empty());
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(doc, score));

    ASSERT_EQ(OB_SUCCESS, merge.rescan());
    ASSERT_TRUE(merge.use_ordered_concat_);
    uint64_t ids[4];
    int64_t count = 0;
    ObDASRowID boundary;
    boundary.set_uint64(5);
    ObDASRowID after;
    double after_score = 0.0;
    ASSERT_EQ(OB_SUCCESS, merge.drain_until(
        boundary, ARRAYSIZEOF(ids), ids, nullptr, count, after, after_score));
    ASSERT_EQ(2, count);
    EXPECT_EQ(1, ids[0]);
    EXPECT_EQ(5, ids[1]);
    EXPECT_EQ(10, after.get_uint64());
    EXPECT_TRUE(merge.pending_child_indices_.empty());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(7, first_component(key));
    ASSERT_EQ(OB_SUCCESS, merge.close());
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_heap_merge_preserves_both_contributor_sets)
{
  for (int depth : {1, 3}) {
    ObDASAsyncIndexQueryCtx catalog;
    build_catalog(catalog, {3});
    PinnedOverlappingCatalog pinned_storage(allocator_);
    ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
    const auto keys = [depth](std::initializer_list<uint32_t> values) {
      std::vector<uint32_t> result;
      for (uint32_t value : values) {
        result.push_back(value);
        for (int i = 1; i < depth; ++i) {
          result.push_back(1);
        }
      }
      return result;
    };
    ReusingOutputNKLeaf left(search_ctx_, depth, {1, 2},
                            {keys({1, 4, 7, 9}), keys({2, 8})});
    ReusingOutputNKLeaf right(search_ctx_, depth, {1, 2, 3},
                             {keys({1, 5, 7, 10}), {}, keys({6, 12})});
    ReusingOutputNKLeaf third(search_ctx_, depth, {1, 3},
                             {keys({3, 4, 7, 11}), keys({6})});
    ObArray<ObIDASSearchOp *> children;
    ASSERT_EQ(OB_SUCCESS, children.push_back(&left));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&right));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&third));
    ObDASAsyncSegmentMergeOpParam param(
        children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
    param.set_is_scoring(false);
    ObDASAsyncSegmentMergeOp merge(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, merge.init(param));
    merge.scope_ = DAS_SCOPE_NESTED_ASYNC;
    merge.nesting_depth_ = depth;
    ASSERT_EQ(OB_SUCCESS, merge.open());
    EXPECT_FALSE(merge.use_ordered_concat_);
    ObDASRowID document;
    ObDASNestedKey key;
    double score = 0.0;
    const auto first_component = [](const ObDASNestedKey &key) {
      return (key.length() <= sizeof(uint64_t)
          ? key.value() : ObDASNestedKey::load(key.ptr(), 8)) >> 32;
    };
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(document, score));
    EXPECT_EQ(1, document.get_uint64());
    EXPECT_EQ(0, left.list_loads_ + right.list_loads_ + third.list_loads_);
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(1, first_component(key));
    EXPECT_EQ(2, merge.nested_pending_child_indices_.count());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(3, first_component(key));
    EXPECT_EQ(1, merge.nested_pending_child_indices_.count());
    EXPECT_EQ(3, merge.pending_child_indices_.count());

    ObDASNestedKey target;
    target.set_value(4ULL << 32, 4); // A prefix seek still outputs a complete key.
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, key, score));
    EXPECT_EQ(4, first_component(key));
    EXPECT_EQ(depth * 4, key.length());
    EXPECT_EQ(2, merge.nested_pending_child_indices_.count());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(5, first_component(key));
    char target_bytes[12];
    const uint32_t wanted[] = {6, 1, 1};
    for (int i = 0; i < depth; ++i) {
      const uint32_t value = htobe32(wanted[i]);
      MEMCPY(target_bytes + i * 4, &value, 4);
    }
    target.assign(target_bytes, depth * 4);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, key, score));
    EXPECT_EQ(7, first_component(key));
    EXPECT_EQ(3, merge.nested_pending_child_indices_.count());
    const int64_t seeks = left.nk_seeks_ + right.nk_seeks_ + third.nk_seeks_;
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(key, key, score));
    EXPECT_EQ(7, first_component(key));
    EXPECT_EQ(seeks, left.nk_seeks_ + right.nk_seeks_ + third.nk_seeks_);
    for (uint64_t value : {9, 10, 11}) {
      ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
      EXPECT_EQ(value, first_component(key));
    }
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
    EXPECT_EQ(3, merge.pending_child_indices_.count());
    EXPECT_FALSE(merge.iter_end_);
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(document, score));
    EXPECT_EQ(2, document.get_uint64());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(2, first_component(key));
    EXPECT_FALSE(merge.child_ended_.at(1)); // Its empty NK list only ends this document.

    // Jump documents while one contributor still has unread NKs.
    document.set_uint64(3);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(document, document, score));
    EXPECT_EQ(3, document.get_uint64());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(6, first_component(key));
    EXPECT_EQ(2, merge.nested_pending_child_indices_.count());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(12, first_component(key));
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
    EXPECT_EQ(OB_ITER_END, merge.next_rowid(document, score));

    ASSERT_EQ(OB_SUCCESS, merge.rescan());
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(document, score));
    EXPECT_EQ(1, document.get_uint64());
    target.set_value(4ULL << 32, 4);
    ASSERT_EQ(OB_SUCCESS, merge.advance_to(target, key, score));
    EXPECT_EQ(4, first_component(key));
    ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
    EXPECT_EQ(5, first_component(key));
    ASSERT_EQ(OB_SUCCESS, merge.close());
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_segment_merge_same_document_seek_preserves_nested_cursor)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  MultiDocNKLeaf left(search_ctx_, 1, {1, 2}, {{1, 3}, {7}});
  MultiDocNKLeaf right(search_ctx_, 1, {1, 3}, {{1, 5}, {9}});
  MultiDocNKLeaf later(search_ctx_, 1, {2}, {{8}});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&left));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&right));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&later));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(false);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(param));
  merge.scope_ = DAS_SCOPE_NESTED_ASYNC;
  merge.nesting_depth_ = 1;
  ASSERT_EQ(OB_SUCCESS, merge.open());
  ObDASRowID document;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge.next_rowid(document, score));
  ASSERT_EQ(1, document.get_uint64());
  EXPECT_EQ(0, left.list_loads_);
  EXPECT_EQ(0, right.list_loads_);
  EXPECT_EQ(0, later.list_loads_);
  ASSERT_EQ(2, merge.pending_child_indices_.count());
  ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
  EXPECT_EQ(1ULL << 32, key.value());
  ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
  EXPECT_EQ(3ULL << 32, key.value());
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(1, right.list_loads_);

  // A target aliasing an internal head must keep both the current NK cursor
  // and the other contributor's prefetched NK, without repositioning children.
  const int64_t left_seeks = left.doc_seeks_;
  const int64_t right_seeks = right.doc_seeks_;
  const ObDASRowID &same_document =
      merge.head_rowids_.at(merge.pending_child_indices_.at(0));
  ASSERT_EQ(OB_SUCCESS, merge.advance_to(same_document, document, score));
  EXPECT_EQ(1, document.get_uint64());
  EXPECT_EQ(left_seeks, left.doc_seeks_);
  EXPECT_EQ(right_seeks, right.doc_seeks_);
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(1, right.list_loads_);
  EXPECT_EQ(0, later.list_loads_);
  ObDASRowID earlier_document;
  earlier_document.set_min();
  ASSERT_EQ(OB_SUCCESS, merge.advance_to(earlier_document, document, score));
  EXPECT_EQ(1, document.get_uint64());
  ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
  EXPECT_EQ(5ULL << 32, key.value());
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(1, right.list_loads_);
  EXPECT_EQ(0, later.list_loads_);
  EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, merge.advance_to(document, document, score));
  EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
  EXPECT_EQ(left_seeks, left.doc_seeks_);
  EXPECT_EQ(right_seeks, right.doc_seeks_);

  // Consumed contributors may reach the target through next_rowid before
  // advance_heads_to runs. The already aligned third child must stay untouched.
  const int64_t later_seeks = later.doc_seeks_;
  ObDASRowID next_document;
  next_document.set_uint64(2);
  ASSERT_EQ(OB_SUCCESS, merge.advance_to(next_document, document, score));
  EXPECT_EQ(2, document.get_uint64());
  EXPECT_EQ(later_seeks, later.doc_seeks_);
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(1, right.list_loads_);
  EXPECT_EQ(0, later.list_loads_);
  ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
  EXPECT_EQ(7ULL << 32, key.value());
  ASSERT_EQ(OB_SUCCESS, merge.next_rowid(key, score));
  EXPECT_EQ(8ULL << 32, key.value());
  EXPECT_EQ(OB_ITER_END, merge.next_rowid(key, score));
  EXPECT_EQ(OB_SUCCESS, merge.close());
}

TEST_F(TestDASAsyncSegmentMerge, nested_disjunction_zero_msm_accepts_candidate_without_child_match)
{
  MultiDocNKLeaf left(search_ctx_, 2, {1}, {{2, 3}});
  MultiDocNKLeaf right(search_ctx_, 2, {1}, {{4, 5}});
  ObIDASSearchOp *children[] = {&left, &right};
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  disjunction.nesting_depth_ = 2;
  disjunction.children_ = children;
  disjunction.children_cnt_ = 2;
  disjunction.min_should_match_ = 0;
  ASSERT_EQ(OB_SUCCESS, disjunction.reset_merge_cursor());
  ObDASRowID document;
  ObDASRowID document_target;
  document_target.set_uint64(1);
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document_target, document, score));
  ObDASNestedKey target;
  target.set_value((1ULL << 32) | 9, 8);
  ObDASNestedKey result;
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, result, score));
  EXPECT_EQ(target.length(), result.length());
  EXPECT_EQ(target.value(), result.value());
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(result, score));
  EXPECT_EQ((2ULL << 32) | 3, result.value());
  document_target.set_uint64(2);
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(document_target, document, score));
  EXPECT_EQ(2, document.get_uint64()); // The required side supplies this candidate.
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, result, score));
  EXPECT_EQ(target.value(), result.value());
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(result, score));
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, result, score));
  EXPECT_EQ(target.value(), result.value());
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(result, score));
  ObDASNestedKey minimum;
  EXPECT_EQ(OB_ITER_END, disjunction.advance_to(minimum, result, score));
}


TEST_F(TestDASAsyncSegmentMerge, nested_disjunction_zero_msm_owns_long_candidate)
{
  const uint32_t components[] = {2, 1, 1};
  PrefixSeekLeaf leaf(search_ctx_, components, 3, 1);
  ObIDASSearchOp *children[] = {&leaf};
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  disjunction.nesting_depth_ = 3;
  disjunction.children_ = children;
  disjunction.children_cnt_ = 1;
  disjunction.min_should_match_ = 0;
  disjunction.is_first_nk_ = false;
  ASSERT_EQ(OB_SUCCESS, disjunction.child_nested_keys_.prepare_allocate(1));
  char saved_key[12];
  disjunction.current_nk_buf_ = saved_key;
  char target_bytes[12];
  for (int i = 0; i < 3; ++i) {
    const uint32_t component = htobe32(1);
    MEMCPY(target_bytes + i * 4, &component, sizeof(component));
  }
  ObDASNestedKey target;
  target.assign(target_bytes, sizeof(target_bytes));
  ObDASNestedKey result;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, result, score));
  ASSERT_EQ(12, result.length());
  EXPECT_NE(target_bytes, disjunction.curr_nk_.ptr());
  EXPECT_EQ(0, MEMCMP(target_bytes, result.ptr(), sizeof(target_bytes)));

  // Reuse the caller's seek buffer for the real first child key. The saved
  // candidate must stay at (1, 1, 1), or next would wrongly consume (2, 1, 1).
  const uint32_t next_first = htobe32(2);
  MEMCPY(target_bytes, &next_first, sizeof(next_first));
  EXPECT_EQ(1ULL << 32, ObDASNestedKey::load(disjunction.curr_nk_.ptr(), 4));
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(result, score));
  ASSERT_EQ(12, result.length());
  EXPECT_EQ(0, MEMCMP(target_bytes, result.ptr(), sizeof(target_bytes)));
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(result, score));
}

TEST_F(TestDASAsyncSegmentMerge, document_iteration_invalidates_nested_cursor_without_loading_keys)
{
  MultiDocNKLeaf left(search_ctx_, 1, {1, 2, 3}, {{2}, {7}, {9}});
  MultiDocNKLeaf right(search_ctx_, 1, {1, 2, 3}, {{2}, {7}, {9}});
  ObDASConjunctionOp conjunction(search_ctx_);
  conjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  conjunction.nesting_depth_ = 1;
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&left));
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&right));
  ObDASRowID document;
  ObDASNestedKey key;
  double score = 0;
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(document, score));
  EXPECT_EQ(1, document.get_uint64());
  EXPECT_TRUE(conjunction.is_first_nk_);
  EXPECT_EQ(0, left.list_loads_);
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(key, score));
  EXPECT_EQ(2ULL << 32, key.value());
  EXPECT_EQ(1, document.get_uint64());
  EXPECT_EQ(OB_ITER_END, conjunction.next_rowid(key, score));

  // DocID calls invalidate NK lazily; skipped documents never load their lists.
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(document, score));
  EXPECT_EQ(2, document.get_uint64());
  EXPECT_TRUE(conjunction.is_first_nk_);
  EXPECT_EQ(1, left.list_loads_);
  ObDASRowID target;
  target.set_uint64(3);
  ASSERT_EQ(OB_SUCCESS, conjunction.advance_to(target, document, score));
  EXPECT_EQ(3, document.get_uint64());
  EXPECT_EQ(1, left.list_loads_);
  EXPECT_EQ(1, right.list_loads_);

  // The first NK request initializes the new document and clears the old EOF.
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(key, score));
  EXPECT_EQ(9ULL << 32, key.value());
  EXPECT_EQ(2, left.list_loads_); // The skipped document's list was never read.
  EXPECT_EQ(2, right.list_loads_);
}


TEST_F(TestDASAsyncSegmentMerge, outer_nested_selects_documents_and_rebinds_independent_subtrees)
{
  MultiDocNKLeaf left(search_ctx_, 1, {1, 2, 3, 4}, {{1}, {2}, {3}, {8}});
  MultiDocNKLeaf right(search_ctx_, 1, {1, 2, 3, 4}, {{9}, {2}, {4}, {8}});
  ObIDASSearchOp *children[] = {&left, &right};
  ObDASConjunctionOp conjunction(search_ctx_);
  conjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  conjunction.nesting_depth_ = 1;
  conjunction.children_ = children;
  conjunction.children_cnt_ = ARRAYSIZEOF(children);
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&left));
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&right));
  ObDASNestedQueryOp outer(search_ctx_);
  outer.scope_ = DAS_SCOPE_BASE;
  outer.inner_op_ = &conjunction;
  MultiDocNKLeaf other_leaf(search_ctx_, 1, {100}, {{7}});
  ObDASNestedQueryOp other_outer(search_ctx_);
  other_outer.scope_ = DAS_SCOPE_BASE;
  other_outer.inner_op_ = &other_leaf;
  ObDASRowID document;
  ObDASRowID target;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, outer.next_rowid(document, score));
  EXPECT_EQ(2, document.get_uint64()); // Document 1 has no matching object.
  const int64_t loaded = left.list_loads_;
  ASSERT_EQ(OB_SUCCESS, other_outer.next_rowid(document, score));
  EXPECT_EQ(100, document.get_uint64());
  target.set_uint64(2);
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
  EXPECT_EQ(2, document.get_uint64());
  EXPECT_EQ(loaded, left.list_loads_); // The boundary reuses its verified document.
  ASSERT_EQ(OB_SUCCESS, outer.next_rowid(document, score));
  EXPECT_EQ(4, document.get_uint64()); // Document 3 also has no intersection.
  EXPECT_EQ(OB_ITER_END, outer.next_rowid(document, score));

  ASSERT_EQ(OB_SUCCESS, outer.rescan());
  target.set_uint64(4);
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
  EXPECT_EQ(4, document.get_uint64()); // Equal DocID still reinitializes after rescan.
  EXPECT_EQ(5, left.list_loads_);
}

TEST_F(TestDASAsyncSegmentMerge, outer_nested_caches_verified_document_and_score)
{
  MultiDocNKLeaf leaf(search_ctx_, 1, {10, 20, 30, 40}, {{1}, {}, {3}, {}});
  leaf.doc_score_ = 2.5;
  leaf.is_scoring_ = true;
  ObDASNestedQueryOp outer(search_ctx_);
  outer.scope_ = DAS_SCOPE_BASE;
  outer.is_scoring_ = true;
  outer.inner_op_ = &leaf;
  ObDASRowID target;
  ObDASRowID document;
  double score = 0.0;
  target.set_uint64(5);
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
  EXPECT_EQ(10, document.get_uint64());
  EXPECT_DOUBLE_EQ(2.5, score);
  EXPECT_EQ(1, leaf.list_loads_);
  const int64_t doc_seeks = leaf.doc_seeks_;
  const int64_t nk_seeks = leaf.nk_seeks_;
  for (uint64_t id : {9, 10}) {
    target.set_uint64(id);
    score = -1.0;
    ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
    EXPECT_EQ(10, document.get_uint64());
    EXPECT_DOUBLE_EQ(2.5, score);
  }
  target.set_min();
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
  EXPECT_EQ(10, document.get_uint64());
  EXPECT_EQ(doc_seeks, leaf.doc_seeks_);
  EXPECT_EQ(nk_seeks, leaf.nk_seeks_);
  EXPECT_EQ(1, leaf.list_loads_);

  // A larger target skips the document with an empty NK list and caches the match.
  target.set_uint64(20);
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
  EXPECT_EQ(30, document.get_uint64());
  EXPECT_EQ(3, leaf.list_loads_);
  target.set_uint64(30);
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
  EXPECT_EQ(3, leaf.list_loads_);
  EXPECT_EQ(OB_ITER_END, outer.next_rowid(document, score));
  EXPECT_FALSE(outer.has_emitted_);
  EXPECT_EQ(OB_ITER_END, outer.advance_to(target, document, score));

  ASSERT_EQ(OB_SUCCESS, outer.rescan());
  leaf.doc_score_ = 4.5;
  ASSERT_EQ(OB_SUCCESS, outer.next_rowid(document, score));
  EXPECT_EQ(10, document.get_uint64());
  target.set_uint64(10);
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
  EXPECT_DOUBLE_EQ(4.5, score);
  ASSERT_EQ(OB_SUCCESS, outer.next_rowid(document, score));
  EXPECT_EQ(30, document.get_uint64());
  target.set_uint64(35);
  EXPECT_EQ(OB_ITER_END, outer.advance_to(target, document, score));
  EXPECT_FALSE(outer.has_emitted_);
}

TEST_F(TestDASAsyncSegmentMerge, outer_nested_max_targets_and_reset_clear_cached_document)
{
  MultiDocNKLeaf leaf(search_ctx_, 1, {UINT64_MAX}, {{1}});
  ObDASNestedQueryOp outer(search_ctx_);
  outer.scope_ = DAS_SCOPE_BASE;
  outer.inner_op_ = &leaf;
  ObDASRowID target;
  ObDASRowID document;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, outer.next_rowid(document, score));
  target.set_uint64(UINT64_MAX);
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, document, score));
  EXPECT_EQ(UINT64_MAX, document.get_uint64());
  EXPECT_EQ(1, leaf.list_loads_);

  const MaxScoreTuple *tuple = nullptr;
  EXPECT_EQ(OB_ITER_END, outer.advance_shallow(target, false, tuple));
  EXPECT_FALSE(outer.has_emitted_);
  ASSERT_EQ(OB_SUCCESS, outer.rescan());
  ASSERT_EQ(OB_SUCCESS, outer.next_rowid(document, score));
  target.set_max();
  EXPECT_EQ(OB_ITER_END, outer.advance_to(target, document, score));
  EXPECT_FALSE(outer.has_emitted_);
  target.set_min();
  EXPECT_EQ(OB_ITER_END, outer.advance_to(target, document, score));

  ASSERT_EQ(OB_SUCCESS, outer.rescan());
  ASSERT_EQ(OB_SUCCESS, outer.next_rowid(document, score));
  ASSERT_EQ(OB_SUCCESS, outer.close());
  EXPECT_FALSE(outer.has_emitted_);
  EXPECT_DOUBLE_EQ(0.0, outer.last_emitted_score_);
}

TEST_F(TestDASAsyncSegmentMerge, nested_disjunction_uses_document_heads_to_select_children)
{
  MultiDocNKLeaf left(search_ctx_, 1, {1, 3}, {{1}, {3}});
  MultiDocNKLeaf right(search_ctx_, 1, {2, 3, 4}, {{2}, {8}, {4}});
  MultiDocNKLeaf empty(search_ctx_, 1, {}, {});
  ObIDASSearchOp *children[] = {&left, &right, &empty};
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  disjunction.nesting_depth_ = 1;
  disjunction.children_ = children;
  disjunction.children_cnt_ = ARRAYSIZEOF(children);
  disjunction.min_should_match_ = 1;
  ASSERT_EQ(OB_SUCCESS, disjunction.reset_merge_cursor());
  const std::vector<std::vector<uint64_t>> expected = {{1}, {2}, {3, 8}, {4}};
  ObDASRowID document;
  ObDASNestedKey key;
  double score = 0.0;
  for (uint64_t doc = 1; doc <= 4; ++doc) {
    ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(document, score));
    ASSERT_EQ(doc, document.get_uint64());
    for (uint64_t value : expected[doc - 1]) {
      ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
      EXPECT_EQ(value << 32, key.value());
    }
    EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(key, score));
    EXPECT_EQ(doc < 3 ? 1 : 2, left.list_loads_);
    EXPECT_EQ(doc == 1 ? 0 : doc - 1, right.list_loads_);
    EXPECT_EQ(0, empty.list_loads_);
  }
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(document, score));
}

TEST_F(TestDASAsyncSegmentMerge, dual_interfaces_keep_nested_eof_separate_from_document_iteration)
{
  MultiDocNKLeaf left(search_ctx_, 1, {1, 2}, {{2}, {7}});
  MultiDocNKLeaf right(search_ctx_, 1, {1, 2}, {{2}, {7}});
  ObDASConjunctionOp conjunction(search_ctx_);
  conjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  conjunction.nesting_depth_ = 1;
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&left));
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&right));
  ObDASRowID document;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(document, score));
  ASSERT_EQ(1, document.get_uint64());
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(key, score));
  ASSERT_EQ(OB_ITER_END, conjunction.next_rowid(key, score));
  EXPECT_EQ(OB_ITER_END, conjunction.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(document, score));
  EXPECT_EQ(2, document.get_uint64());
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(key, score));
  EXPECT_EQ(7ULL << 32, key.value());
}

TEST_F(TestDASAsyncSegmentMerge, nested_conjunction_aligns_projected_parent_keys)
{
  ObDASRowID document;
  document.set_uint64(1);
  const uint32_t a[] = {1, 2, 9, 1, 3, 8};
  const uint32_t b[] = {1, 2, 1, 1, 2, 2, 1, 3, 2};
  PrefixSeekLeaf left(search_ctx_, a, 3, 2);
  PrefixSeekLeaf right(search_ctx_, b, 3, 3);
  ObDASNestedQueryOp left_nested(search_ctx_);
  left_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
  left_nested.nesting_depth_ = 3;
  left_nested.inner_op_ = &left;
  ObDASNestedQueryOp right_nested(search_ctx_);
  right_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
  right_nested.nesting_depth_ = 3;
  right_nested.inner_op_ = &right;
  ObDASConjunctionOp conjunction(search_ctx_);
  conjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  conjunction.nesting_depth_ = 2;
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&left_nested));
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&right_nested));
  ObDASNestedKey result;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(result, score));
  EXPECT_EQ(8, result.length());
  EXPECT_EQ((1ULL << 32) | 2, result.value());
  EXPECT_EQ(8, right.last_target_length_);
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(result, score));
  EXPECT_EQ((1ULL << 32) | 3, result.value());
  EXPECT_EQ(OB_ITER_END, conjunction.next_rowid(result, score));
}

TEST_F(TestDASAsyncSegmentMerge, nested_optional_filter_only_reads_required)
{
  MultiDocNKLeaf required(search_ctx_, 1, {1, 3, 4, 5, 6}, {{0, 5, 9}, {5}, {1}, {}, {2}});
  MultiDocNKLeaf optional(search_ctx_, 1, {1, 2, 4, 7}, {{5, 8}, {9}, {2}, {2}});
  ObIDASSearchOp *children[] = {&required, &optional};
  ObDASReqOptOp op(search_ctx_);
  op.scope_ = DAS_SCOPE_NESTED_ASYNC;
  op.nesting_depth_ = 1;
  op.is_scoring_ = false;
  op.required_ = &required;
  op.optional_ = &optional;
  op.children_ = children;
  op.children_cnt_ = 2;
  op.is_inited_ = true;
  const uint64_t docs[] = {1, 3, 4, 5, 6};
  const std::vector<std::vector<uint32_t>> keys = {{0, 5, 9}, {5}, {1}, {}, {2}};
  ObDASRowID doc;
  ObDASNestedKey key;
  double score = 0.0;
  for (int pass = 0; pass < 3; ++pass) {
    if (1 == pass) {
      ASSERT_EQ(OB_SUCCESS, op.rescan());
    } else if (2 == pass) {
      ObNewVersionRange range;
      ASSERT_EQ(OB_SUCCESS, op.rescan_segment(range));
    }
    for (int i = 0; i < ARRAYSIZEOF(docs); ++i) {
      ASSERT_EQ(OB_SUCCESS, op.next_rowid(doc, score));
      EXPECT_EQ(docs[i], doc.get_uint64());
      for (uint32_t value : keys[i]) {
        ASSERT_EQ(OB_SUCCESS, op.next_rowid(key, score));
        EXPECT_EQ(static_cast<uint64_t>(value) << 32, key.value());
        ASSERT_EQ(OB_SUCCESS, op.advance_to(key, key, score));
        EXPECT_EQ(static_cast<uint64_t>(value) << 32, key.value());
      }
      EXPECT_EQ(OB_ITER_END, op.next_rowid(key, score));
    }
    EXPECT_EQ(OB_ITER_END, op.next_rowid(doc, score));
    EXPECT_EQ(0, optional.doc_seeks_);
    EXPECT_EQ(0, optional.nk_seeks_);
    EXPECT_EQ(0, optional.list_loads_);
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_exclusion_only_removes_same_object)
{
  MultiDocNKLeaf required(search_ctx_, 1, {1, 2}, {{1, 2, 3}, {2}});
  MultiDocNKLeaf excluded(search_ctx_, 1, {1}, {{2}});
  ObDASReqExclOp op(search_ctx_);
  op.scope_ = DAS_SCOPE_NESTED_ASYNC;
  op.nesting_depth_ = 1;
  op.required_ = &required;
  op.excluded_ = &excluded;
  ObDASRowID doc;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(doc, score));
  EXPECT_EQ(0, excluded.doc_seeks_);
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(key, score));
  EXPECT_EQ(1ULL << 32, key.value());
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(key, score));
  EXPECT_EQ(3ULL << 32, key.value());
  EXPECT_EQ(OB_ITER_END, op.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(doc, score));
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(key, score));
  EXPECT_EQ(2ULL << 32, key.value());
}

TEST_F(TestDASAsyncSegmentMerge, nested_parent_prefix_carry_and_overflow_keep_projected_results)
{
  ObDASRowID document;
  document.set_uint64(1);
  const uint32_t a[] = {1, 2, 255, 9, 1, 2, 256, 9,
      UINT32_MAX, UINT32_MAX, UINT32_MAX, 1};
  const uint32_t b[] = {1, 2, 255, 1, 1, 2, 255, 2, 1, 2, 256, 2,
      UINT32_MAX, UINT32_MAX, UINT32_MAX, 2};
  PrefixSeekLeaf left(search_ctx_, a, 4, 3);
  PrefixSeekLeaf right(search_ctx_, b, 4, 4);
  ObDASNestedQueryOp left_nested(search_ctx_);
  left_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
  left_nested.nesting_depth_ = 4;
  left_nested.inner_op_ = &left;
  ObDASNestedQueryOp right_nested(search_ctx_);
  right_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
  right_nested.nesting_depth_ = 4;
  right_nested.inner_op_ = &right;
  ObDASConjunctionOp op(search_ctx_);
  op.scope_ = DAS_SCOPE_NESTED_ASYNC;
  op.nesting_depth_ = 3;
  ASSERT_EQ(OB_SUCCESS, op.driver_ops_.push_back(&left_nested));
  ASSERT_EQ(OB_SUCCESS, op.driver_ops_.push_back(&right_nested));
  ObDASNestedKey key;
  double score = 0.0;
  for (uint32_t last : {255U, 256U, UINT32_MAX}) {
    ASSERT_EQ(OB_SUCCESS, op.next_rowid(key, score));
    EXPECT_EQ(12, key.length());
    uint32_t component = 0;
    MEMCPY(&component, key.ptr() + 8, 4);
    EXPECT_EQ(last, be32toh(component));
  }
  EXPECT_EQ(OB_ITER_END, op.next_rowid(key, score));
}

TEST_F(TestDASAsyncSegmentMerge, nested_msm_counts_children_once_per_parent_object)
{
  ObDASRowID document;
  document.set_uint64(1);
  for (bool second_matches : {false, true}) {
    const uint32_t a[] = {1, 2, 1, 1, 2, 2, 1, 2, 3};
    const uint32_t b[] = {1, second_matches ? 2U : 3U, 9};
    PrefixSeekLeaf left(search_ctx_, a, 3, 3);
    PrefixSeekLeaf right(search_ctx_, b, 3, 1);
    ObDASNestedQueryOp left_nested(search_ctx_);
    left_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
    left_nested.nesting_depth_ = 3;
    left_nested.inner_op_ = &left;
    ObDASNestedQueryOp right_nested(search_ctx_);
    right_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
    right_nested.nesting_depth_ = 3;
    right_nested.inner_op_ = &right;
    ObIDASSearchOp *children[] = {&left_nested, &right_nested};
    ObDASDisjunctionFilterOp op(search_ctx_);
    op.scope_ = DAS_SCOPE_NESTED_ASYNC;
    op.nesting_depth_ = 2;
    op.children_ = children;
    op.children_cnt_ = 2;
    op.min_should_match_ = 2;
    op.is_first_nk_ = false;
    for (int i = 0; i < 2; ++i) {
      ASSERT_EQ(OB_SUCCESS, op.child_nested_keys_.push_back(ObDASNestedKey()));
    }
    ObDASNestedKey key;
    double score = 0.0;
    if (second_matches) {
      ASSERT_EQ(OB_SUCCESS, op.next_rowid(key, score));
      EXPECT_EQ((1ULL << 32) | 2, key.value());
      EXPECT_EQ(8, key.length());
    }
    EXPECT_EQ(OB_ITER_END, op.next_rowid(key, score));
  }
}

TEST_F(TestDASAsyncSegmentMerge, nested_segments_union_all_contributors_and_deduplicate_objects)
{
  ObDASRowID document;
  document.set_uint64(1);
  const uint32_t a[] = {0, 2, 5};
  const uint32_t b[] = {1, 2, 6};
  PrefixSeekLeaf left(search_ctx_, a, 1, 3);
  PrefixSeekLeaf right(search_ctx_, b, 1, 3);
  ObIDASSearchOp *children[] = {&left, &right};
  ObDASAsyncSegmentMergeOp op(search_ctx_);
  op.scope_ = DAS_SCOPE_NESTED_ASYNC;
  op.nesting_depth_ = 1;
  op.children_ = children;
  op.children_cnt_ = 2;
  op.active_segment_cnt_ = 2;
  ASSERT_EQ(OB_SUCCESS, op.pending_child_indices_.push_back(1));
  ASSERT_EQ(OB_SUCCESS, op.pending_child_indices_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, op.nested_key_heads_.prepare_allocate(2));
  ObDASNestedKey key;
  double score = 0.0;
  for (uint64_t value : {0, 1, 2, 5, 6}) {
    ASSERT_EQ(OB_SUCCESS, op.next_rowid(key, score));
    EXPECT_EQ(value << 32, key.value());
    ASSERT_EQ(OB_SUCCESS, op.advance_to(key, key, score));
    EXPECT_EQ(value << 32, key.value());
  }
  EXPECT_EQ(OB_ITER_END, op.next_rowid(key, score));
}

TEST_F(TestDASAsyncSegmentMerge, nested_conjunction_preserves_document_and_object_cursors)
{
  MultiDocNKLeaf left(search_ctx_, 1, {1}, {{1, 3, 9}});
  MultiDocNKLeaf right(search_ctx_, 1, {1}, {{2, 3, 9}});
  ObDASConjunctionOp op(search_ctx_);
  op.scope_ = DAS_SCOPE_NESTED_ASYNC;
  op.nesting_depth_ = 1;
  ASSERT_EQ(OB_SUCCESS, op.driver_ops_.push_back(&left));
  ASSERT_EQ(OB_SUCCESS, op.driver_ops_.push_back(&right));
  ObDASRowID doc;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(doc, score));
  EXPECT_EQ(0, left.list_loads_);
  EXPECT_EQ(0, right.list_loads_);
  ASSERT_EQ(OB_SUCCESS, op.next_rowid(key, score));
  EXPECT_EQ(3ULL << 32, key.value());
  ASSERT_EQ(OB_SUCCESS, op.advance_to(doc, doc, score));
  ASSERT_EQ(OB_SUCCESS, op.advance_to(key, key, score));
  key.set_value(9ULL << 32, 4);
  ASSERT_EQ(OB_SUCCESS, op.advance_to(key, key, score));
  EXPECT_EQ(9ULL << 32, key.value());
}

struct NestedPairInput
{
  uint64_t doc_id_;
  std::vector<uint32_t> keys_;
  uint32_t key_depth_ = 1;
};

// RANGE-compatible pair batches invalidate borrowed key bytes on every call.
class ScriptedNestedPairOp : public ObIDASSearchOp
{
public:
  ScriptedNestedPairOp(ObDASSearchCtx &ctx, std::vector<NestedPairInput> rows)
      : ObIDASSearchOp(ctx), rows_(rows)
  { scope_ = DAS_SCOPE_NESTED_ASYNC; nesting_depth_ = 1; }
  int do_open() override { cursor_ = key_idx_ = 0; return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { cursor_ = key_idx_ = 0; return OB_SUCCESS; }
  int do_rescan_segment(const ObNewVersionRange &) override { return do_rescan(); }
  int do_next_rowid(ObDASRowID &, double &) override { return OB_NOT_SUPPORTED; }
  int do_advance_to(const ObDASRowID &, ObDASRowID &, double &) override { return OB_NOT_SUPPORTED; }
  const ObIArray<ObExpr *> *get_rowid_output_exprs() const override
  { return search_ctx_.get_scope(scope_).rowid_exprs_; }
  int do_next_rowids(int64_t capacity, int64_t &count) override
  {
    ++calls_;
    MEMSET(bytes_, 0xdd, sizeof(bytes_));
    count = 0;
    int64_t offset = 0;
    ObDatum *docs = get_rowid_output_exprs()->at(0)->locate_batch_datums(eval_ctx());
    ObDatum *keys = get_rowid_output_exprs()->at(1)->locate_batch_datums(eval_ctx());
    while (cursor_ < rows_.size() && count < capacity) {
      const NestedPairInput &row = rows_[cursor_];
      if (key_idx_ == row.keys_.size()) { ++cursor_; key_idx_ = 0; }
      else {
        const int64_t len = row.key_depth_ * sizeof(uint32_t);
        OB_ASSERT(offset + len <= sizeof(bytes_));
        for (int64_t i = 0; i < row.key_depth_; ++i) {
          const uint32_t key = htobe32(row.keys_[key_idx_++]);
          MEMCPY(bytes_ + offset + i * 4, &key, 4);
        }
        docs[count] = ObDatum(reinterpret_cast<const char *>(&row.doc_id_),
                              sizeof(row.doc_id_), false);
        keys[count].set_string(bytes_ + offset, len);
        offset += len;
        ++count;
      }
    }
    return count > 0 ? OB_SUCCESS : OB_ITER_END;
  }
  int64_t calls_ = 0;
private:
  std::vector<NestedPairInput> rows_;
  size_t cursor_ = 0;
  size_t key_idx_ = 0;
  char bytes_[2048];
};

class TestDASNestedSort : public TestDASAsyncSegmentMerge
{
public:
  TestDASNestedSort() : exprs_(allocator_), ctdef_(allocator_) {}
  void SetUp() override
  {
    TestDASAsyncSegmentMerge::SetUp();
    tmp_file_manager_.reset(new ScopedSortTmpFileManager());
    ASSERT_EQ(OB_SUCCESS, tmp_file_manager_->init());
    ASSERT_EQ(OB_SUCCESS, session_.test_init(0, 0, 0, nullptr));
    exec_ctx_.set_my_session(&session_);
    ASSERT_EQ(OB_SUCCESS, exec_ctx_.create_physical_plan_ctx());
    exec_ctx_.get_physical_plan_ctx()->set_timeout_timestamp(INT64_MAX);
    eval_ctx_.max_batch_size_ = 16;
    void *skip_buf = allocator_.alloc(ObBitVector::memory_size(eval_ctx_.max_batch_size_));
    ASSERT_NE(nullptr, skip_buf);
    search_ctx_.mock_skip_ = to_bit_vector(skip_buf);
    search_ctx_.mock_skip_->init(eval_ctx_.max_batch_size_);
    prepare_const_expr_frame(16384);
    int64_t pos = 0;
    init_output_expr(doc_expr_, pos, ObUInt64Type);
    init_output_expr(list_expr_, pos, ObVarcharType);
    ASSERT_EQ(OB_SUCCESS, exprs_.init(2));
    ASSERT_EQ(OB_SUCCESS, exprs_.push_back(&doc_expr_));
    ASSERT_EQ(OB_SUCCESS, exprs_.push_back(&list_expr_));
    ASSERT_EQ(OB_SUCCESS, search_ctx_.register_nested_scope(&exprs_, DAS_SCOPE_NESTED_ASYNC));
    ASSERT_EQ(OB_SUCCESS, ctdef_.rowkey_exprs_.init(2));
    ASSERT_EQ(OB_SUCCESS, ctdef_.rowkey_exprs_.push_back(&doc_expr_));
    ASSERT_EQ(OB_SUCCESS, ctdef_.rowkey_exprs_.push_back(&list_expr_));
    rtdef_.ctdef_ = &ctdef_;
    search_ctx_.table_row_count_ = ObDASSearchCost(64);
  }
  void TearDown() override
  {
    exec_ctx_.set_my_session(nullptr);
    tmp_file_manager_.reset();
  }
  void init_output_expr(ObExpr &expr, int64_t &pos, ObObjType type)
  {
    expr.type_ = T_REF_COLUMN;
    expr.obj_meta_.set_type(type);
    expr.obj_meta_.set_collation_type(CS_TYPE_BINARY);
    expr.datum_meta_.type_ = type;
    expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
    expr.basic_funcs_ = ObDatumFuncs::get_basic_func(type, CS_TYPE_BINARY);
    ASSERT_NE(nullptr, expr.basic_funcs_);
    expr.obj_datum_map_ = type == ObUInt64Type ? OBJ_DATUM_8BYTE_DATA : OBJ_DATUM_STRING;
    expr.vec_value_tc_ = get_vec_value_tc(type, expr.datum_meta_.scale_, expr.datum_meta_.precision_);
    expr.batch_result_ = true;
    expr.batch_idx_mask_ = UINT64_MAX;
    expr.max_length_ = type == ObUInt64Type ? 8 : 2048;
    expr.is_fixed_length_data_ = type == ObUInt64Type;
    expr.len_ = type == ObUInt64Type ? 8 : 0;
    expr.frame_idx_ = 0;
    expr.datum_off_ = pos;
    pos += sizeof(ObDatum) * eval_ctx_.max_batch_size_;
    expr.eval_info_off_ = pos;
    pos += sizeof(ObEvalInfo);
    expr.res_buf_off_ = pos;
    expr.res_buf_len_ = 8;
    pos += 8 * eval_ctx_.max_batch_size_;
    expr.vector_header_off_ = pos;
    pos += sizeof(VectorHeader);
    ASSERT_EQ(OB_SUCCESS, expr.init_vector(eval_ctx_, VEC_UNIFORM, eval_ctx_.max_batch_size_, false));
  }
  void open_sort(ObDASSortOp &sort, ObIDASSearchOp &input, int32_t depth = 1)
  {
    sort.scope_ = DAS_SCOPE_NESTED_ASYNC;
    sort.nesting_depth_ = depth;
    ObDASSortOpParam param(&input, &ctdef_, &rtdef_, true /* unique_sort */);
    param.set_is_scoring(false);
    ASSERT_EQ(OB_SUCCESS, sort.init(param));
    ASSERT_EQ(OB_SUCCESS, sort.open());
  }
  void expect_keys(ObDASSortOp &sort, std::vector<uint32_t> expected)
  {
    ObDASNestedKey key;
    double score = 0.0;
    for (uint32_t value : expected) {
      ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
      ObDASNestedKey expected_key;
      expected_key.set_value(static_cast<uint64_t>(value) << 32, 4);
      EXPECT_EQ(0, key.compare(expected_key, 4));
    }
    EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  }
protected:
  std::unique_ptr<ScopedSortTmpFileManager> tmp_file_manager_;
  ObSQLSessionInfo session_;
  ObExpr doc_expr_;
  ObExpr list_expr_;
  ExprFixedArray exprs_;
  ObDASScalarScanCtDef ctdef_;
  ObDASScalarScanRtDef rtdef_;
};

TEST_F(TestDASNestedSort, same_doc_seek_preserves_long_key_cursor)
{
  ScriptedNestedPairOp source(search_ctx_, {
      {1, {1, 2, 3}, 3}, {1, {2, 3, 4}, 3}, {2, {6, 7, 8}, 3}});
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source, 3);
  ObDASRowID doc;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
  EXPECT_EQ(12, key.length());
  EXPECT_EQ((1ULL << 32) | 2, ObDASNestedKey::load(key.ptr(), 8));
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
  EXPECT_EQ(12, key.length());
  EXPECT_EQ((2ULL << 32) | 3, ObDASNestedKey::load(key.ptr(), 8));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  EXPECT_EQ(1, doc.get_uint64());
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(2, doc.get_uint64());
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
  EXPECT_EQ(12, key.length());
  EXPECT_EQ((6ULL << 32) | 7, ObDASNestedKey::load(key.ptr(), 8));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  EXPECT_EQ(OB_SUCCESS, sort.close());
  EXPECT_EQ(nullptr, sort.curr_nk_buf_);
}
TEST_F(TestDASNestedSort, fixed_sixteen_byte_keys_cross_batch_boundary)
{
  std::vector<uint32_t> values;
  for (uint32_t i = 20; i > 0; --i) {
    values.insert(values.end(), {i, 2, 3, 4});
  }
  ScriptedNestedPairOp source(search_ctx_, {{1, values, 4}, {2, {21, 2, 3, 4}, 4}});
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source, 4);
  ObDASRowID doc;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  for (uint32_t i = 1; i <= 20; ++i) {
    ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
    EXPECT_EQ(16, key.length());
    EXPECT_EQ((static_cast<uint64_t>(i) << 32) | 2,
        ObDASNestedKey::load(key.ptr(), 8));
    EXPECT_EQ((3ULL << 32) | 4, ObDASNestedKey::load(key.ptr() + 8, 8));
  }
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(2, doc.get_uint64());
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
  EXPECT_EQ((21ULL << 32) | 2, ObDASNestedKey::load(key.ptr(), 8));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

TEST_F(TestDASNestedSort, docid_seeks_keep_remaining_keys_and_document_boundary)
{
  ScriptedNestedPairOp source(search_ctx_, {{10, {1, 2, 3}}, {20, {4, 5}}, {30, {6}}});
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source);
  ObDASRowID doc, target;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(10, doc.get_uint64());
  for (uint32_t value : {1, 2}) {
    ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
    EXPECT_EQ(static_cast<uint64_t>(value) << 32, key.value());
  }
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  target.set_min();
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(target, doc, score));
  target.set_uint64(5);
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(target, doc, score));
  EXPECT_EQ(10, doc.get_uint64());
  expect_keys(sort, {3});
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  target.set_uint64(15);
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(target, doc, score));
  EXPECT_EQ(20, doc.get_uint64());
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
  EXPECT_EQ(4ULL << 32, key.value());
  // Moving to another document also skips any unrequested keys.
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(30, doc.get_uint64());
  expect_keys(sort, {6});
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  EXPECT_EQ(30, doc.get_uint64());
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

TEST_F(TestDASNestedSort, first_seek_filters_docid_range_and_rescan_rebuilds_input)
{
  ScriptedNestedPairOp source(search_ctx_, {
      {1, {1}}, {8, {8}}, {10, {10}},
      {2, {2}}, {8, {9}}, {12, {12}}});
  ObObj lo;
  ObObj hi;
  lo.set_uint64(5);
  hi.set_uint64(10);
  search_ctx_.set_docid_range(lo, hi);
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source);
  ObDASRowID target;
  ObDASRowID doc;
  target.set_uint64(8);
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(target, doc, score));
  EXPECT_EQ(8, doc.get_uint64());
  EXPECT_EQ(3, sort.sort_impl_->datum_store_.get_row_cnt());
  expect_keys(sort, {8, 9});
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(10, doc.get_uint64());
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  ASSERT_EQ(OB_SUCCESS, sort.rescan());
  EXPECT_FALSE(sort.sort_finished_);
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(8, doc.get_uint64());
  expect_keys(sort, {8, 9});
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = 100;
  ASSERT_EQ(OB_SUCCESS, sort.rescan_segment(range));
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(target, doc, score));
  expect_keys(sort, {8, 9});
  EXPECT_EQ(OB_SUCCESS, sort.close());
}
TEST_F(TestDASNestedSort, empty_input_and_seek_past_end_keep_document_eof)
{
  for (bool empty : {true, false}) {
    ScriptedNestedPairOp source(search_ctx_, empty ? std::vector<NestedPairInput>{}
        : std::vector<NestedPairInput>{{1, {1}}});
    ObDASSortOp sort(search_ctx_);
    open_sort(sort, source);
    ObDASRowID target;
    ObDASRowID doc;
    target.set_uint64(2);
    double score = 0.0;
    EXPECT_EQ(OB_ITER_END, sort.advance_to(target, doc, score));
    EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
    target.set_min();
    EXPECT_EQ(OB_ITER_END, sort.advance_to(target, doc, score));
    EXPECT_EQ(OB_SUCCESS, sort.close());
  }
}
TEST_F(TestDASNestedSort, max_seek_does_not_materialize_input)
{
  ScriptedNestedPairOp source(search_ctx_, {{1, {1}}});
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source);
  ObDASRowID target;
  ObDASRowID doc;
  target.set_max();
  double score = 0.0;
  EXPECT_EQ(OB_ITER_END, sort.advance_to(target, doc, score));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  target.set_min();
  EXPECT_EQ(OB_ITER_END, sort.advance_to(target, doc, score));
  EXPECT_EQ(0, source.calls_);
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

TEST_F(TestDASNestedSort, disjunction_preserves_long_key_before_sort_reuses_output_buffer)
{
  ScriptedNestedPairOp source(search_ctx_, {
      {1, {1, 1, 1, 3, 1, 1}, 3}, {1, {2, 1, 1}, 3}});
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source, 3);
  MultiDocNKLeaf other(search_ctx_, 3, {1}, {{1, 1, 1, 3, 1, 1}});
  ObIDASSearchOp *children[] = {&sort, &other};
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  disjunction.nesting_depth_ = 3;
  disjunction.children_ = children;
  disjunction.children_cnt_ = 2;
  disjunction.min_should_match_ = 1;
  disjunction.current_nk_buf_ = static_cast<char *>(disjunction.ctx_allocator().alloc(12));
  ASSERT_NE(nullptr, disjunction.current_nk_buf_);
  ASSERT_EQ(OB_SUCCESS, disjunction.reset_merge_cursor());
  ObDASRowID document;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(document, score));
  EXPECT_EQ(1, document.get_uint64());
  for (uint32_t first : {1, 2, 3}) {
    ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(key, score));
    EXPECT_EQ(12, key.length());
    EXPECT_EQ(static_cast<uint64_t>(first) << 32, ObDASNestedKey::load(key.ptr(), 4));
  }
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(key, score));
  EXPECT_EQ(OB_SUCCESS, disjunction.do_close());
}

TEST_F(TestDASNestedSort, outer_nested_reuses_disjunction_sort_match_for_exclusion)
{
  ScriptedNestedPairOp left_source(search_ctx_, {{10, {1}, 1}, {30, {3}, 1}});
  ScriptedNestedPairOp right_source(search_ctx_, {{10, {1}, 1}, {30, {3}, 1}});
  ObDASSortOp left(search_ctx_);
  ObDASSortOp right(search_ctx_);
  open_sort(left, left_source);
  open_sort(right, right_source);
  MultiDocNKLeaf empty(search_ctx_, 1, {}, {});
  ObIDASSearchOp *children[] = {&left, &right, &empty};
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  disjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  disjunction.nesting_depth_ = 1;
  disjunction.children_ = children;
  disjunction.children_cnt_ = ARRAYSIZEOF(children);
  disjunction.min_should_match_ = 2;
  disjunction.is_scoring_ = false;
  ASSERT_EQ(OB_SUCCESS, disjunction.reset_merge_cursor());
  ObDASNestedQueryOp outer(search_ctx_);
  outer.scope_ = DAS_SCOPE_BASE;
  outer.inner_op_ = &disjunction;
  MockSegmentOp required(search_ctx_, {5, 9, 10, 30, 40}, {0, 0, 0, 0, 0});
  ObDASReqExclOp exclusion(search_ctx_);
  exclusion.scope_ = DAS_SCOPE_BASE;
  exclusion.required_ = &required;
  exclusion.excluded_ = &outer;
  ObDASRowID document;
  double score = 0.0;
  // Probing 5 and then 9 must leave the previously verified exclusion at 10.
  // Each SORT has only one matching NK per document: consuming it again loses the match.
  for (uint64_t expected : {5, 9, 40}) {
    ASSERT_EQ(OB_SUCCESS, exclusion.next_rowid(document, score));
    EXPECT_EQ(expected, document.get_uint64());
  }
  EXPECT_EQ(OB_ITER_END, exclusion.next_rowid(document, score));
  EXPECT_EQ(OB_SUCCESS, disjunction.do_close());
  EXPECT_EQ(OB_SUCCESS, left.close());
  EXPECT_EQ(OB_SUCCESS, right.close());
}

TEST_F(TestDASNestedSort, pair_input_is_sorted_once_and_docids_can_skip_unrequested_keys)
{
  ScriptedNestedPairOp source(search_ctx_, {{3, {7}}, {1, {3, 1}}, {1, {1, 2}}, {2, {4}}});
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source);
  EXPECT_TRUE(sort.unique_sort_);
  EXPECT_EQ(2, sort.sort_collations_.count());
  ObDASRowID doc;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(1, doc.get_uint64());
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
  EXPECT_EQ(1ULL << 32, key.value());
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  expect_keys(sort, {2, 3});
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(2, doc.get_uint64());
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(3, doc.get_uint64());
  expect_keys(sort, {7});
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  EXPECT_EQ(nullptr, sort.curr_nk_buf_);
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

TEST_F(TestDASNestedSort, pairs_cross_batches_and_short_targets_keep_full_keys)
{
  std::vector<NestedPairInput> rows;
  for (uint32_t i = 128; i > 0; --i) { rows.push_back({i, {1, 2, 1, 4, 3, 5}, 2}); }
  ScriptedNestedPairOp source(search_ctx_, rows);
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source, 2);
  ObDASRowID doc;
  ObDASNestedKey key, target;
  double score = 0.0;
  for (uint32_t i = 1; i <= 128; ++i) {
    ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
    EXPECT_EQ(i, doc.get_uint64());
    target.set_value(2ULL << 32, 4);
    ASSERT_EQ(OB_SUCCESS, sort.advance_to(target, key, score));
    EXPECT_EQ((3ULL << 32) | 5, key.value());
    EXPECT_EQ(8, key.length());
    ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
    ASSERT_EQ(OB_SUCCESS, sort.advance_to(key, key, score));
    EXPECT_EQ((3ULL << 32) | 5, key.value());
    EXPECT_EQ(8, key.length());
    EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  }
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

TEST_F(TestDASNestedSort, docid_seek_ignores_nested_keys_within_and_across_batches)
{
  std::vector<NestedPairInput> rows;
  for (uint32_t i = 40; i > 0; --i) { rows.push_back({i, {9, 1}}); }
  ScriptedNestedPairOp source(search_ctx_, rows);
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source);
  ObDASRowID doc;
  double score = 0.0;
  doc.set_uint64(2);
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  EXPECT_EQ(2, doc.get_uint64());
  // Seek within the retained batch, then skip several complete batches.
  for (uint64_t value : {3, 25, 26, 40}) {
    doc.set_uint64(value);
    ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
    EXPECT_EQ(value, doc.get_uint64());
    expect_keys(sort, {1, 9});
  }
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

TEST_F(TestDASNestedSort, synchronous_pair_store_seeks_by_both_columns)
{
  ObDASRowIDScope &scope = search_ctx_.scopes_[DAS_SCOPE_NESTED_SYNC];
  scope.rowid_type_ = DAS_ROWID_TYPE_COMPACT;
  scope.rowid_exprs_ = &exprs_;
  ASSERT_EQ(OB_SUCCESS, scope.rowid_meta_.init(exprs_, 0));
  ScriptedNestedPairOp source(search_ctx_, {{1, {1, 4}}, {2, {2}}});
  ASSERT_EQ(OB_SUCCESS, source.open());
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, source.next_rowids(16, count));
  CompactRowIDStore store(search_ctx_, DAS_SCOPE_NESTED_SYNC);
  ASSERT_EQ(OB_SUCCESS, store.init(16));
  ASSERT_EQ(OB_SUCCESS, store.fill(0, count, exprs_));
  RowIDStore::Iterator iter(&store);
  iter.reuse();

  const uint64_t target_doc = 1;
  const uint32_t target_key = htobe32(3);
  ObSEArray<ObDatum, 2> datums;
  ASSERT_EQ(OB_SUCCESS, datums.push_back(
      ObDatum(reinterpret_cast<const char *>(&target_doc), sizeof(target_doc), false)));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(
      ObDatum(reinterpret_cast<const char *>(&target_key), sizeof(target_key), false)));
  ObDASRowID target;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.write_datums_to_rowid(
      DAS_SCOPE_NESTED_SYNC, datums, target, allocator_));
  ASSERT_EQ(OB_SUCCESS, iter.lower_bound(target));
  ObDASRowID row;
  ObDatum doc;
  ObDatum key;
  ASSERT_EQ(OB_SUCCESS, iter.get_cur_rowid(row));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.get_datum_from_rowid(DAS_SCOPE_NESTED_SYNC, row, doc, 0));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.get_datum_from_rowid(DAS_SCOPE_NESTED_SYNC, row, key, 1));
  EXPECT_EQ(1, doc.get_uint64());
  EXPECT_EQ(4ULL << 32, ObDASNestedKey::load(key.ptr_, key.len_));
  iter.next_idx();
  ASSERT_EQ(OB_SUCCESS, iter.get_cur_rowid(row));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.get_datum_from_rowid(DAS_SCOPE_NESTED_SYNC, row, doc, 0));
  EXPECT_EQ(2, doc.get_uint64());
  EXPECT_EQ(OB_SUCCESS, source.close());
}

TEST_F(TestDASNestedSort, original_unique_sort_truncates_pairs_before_deduplication)
{
  ScriptedNestedPairOp source(search_ctx_, {{1, {1, 2, 1, 4, 3, 5}, 2}});
  ctdef_.nesting_depth_for_full_subtree_ = 1;
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source);
  ObDASRowID doc;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  expect_keys(sort, {1, 3});
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

TEST_F(TestDASNestedSort, large_document_streams_keys_across_batches_without_restarting)
{
  std::vector<uint32_t> values;
  for (uint32_t i = 0; i < 4096; ++i) { values.push_back(i); }
  ScriptedNestedPairOp source(search_ctx_, {{1, values}, {2, {8}}});
  ObDASSortOp sort(search_ctx_);
  open_sort(sort, source);
  ObDASRowID doc;
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  for (uint32_t i = 0; i < 4096; ++i) {
    if (i == 2 || i == 16 || i == 4095) {
      ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
    }
    ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
    EXPECT_EQ(static_cast<uint64_t>(i) << 32, key.value());
  }
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, sort.advance_to(doc, doc, score));
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
  ASSERT_EQ(OB_SUCCESS, sort.next_rowid(doc, score));
  EXPECT_EQ(2, doc.get_uint64());
  expect_keys(sort, {8});
  EXPECT_EQ(OB_ITER_END, sort.next_rowid(doc, score));
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

TEST_F(TestDASNestedSort, direct_merge_forwards_pair_batches_across_empty_segments)
{
  ScriptedNestedPairOp left(search_ctx_, {{8, {1}}, {9, {2}}});
  ScriptedNestedPairOp empty(search_ctx_, {});
  ScriptedNestedPairOp right(search_ctx_, {{1, {3}}, {9, {4}}});
  ObIDASSearchOp *children[] = {&left, &empty, &right};
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  merge.scope_ = DAS_SCOPE_NESTED_ASYNC;
  merge.mode_ = ObDASAsyncSegmentMergeMode::DIRECT_MERGE;
  merge.children_ = children;
  merge.children_cnt_ = merge.active_segment_cnt_ = 3;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, merge.next_rowids(16, count));
  ASSERT_EQ(2, count);
  EXPECT_EQ(8, doc_expr_.locate_batch_datums(eval_ctx_)[0].get_uint64());
  EXPECT_EQ(4, list_expr_.locate_batch_datums(eval_ctx_)[0].len_);
  ASSERT_EQ(OB_SUCCESS, merge.next_rowids(16, count));
  ASSERT_EQ(2, count);
  EXPECT_EQ(1, doc_expr_.locate_batch_datums(eval_ctx_)[0].get_uint64());
  EXPECT_EQ(OB_ITER_END, merge.next_rowids(16, count));
}

TEST_F(TestDASAsyncSegmentMerge, scalar_release_scan_resources_without_pool)
{
  ObDASAsyncScalarRangeOp range_op(search_ctx_);
  ObDASAsyncScalarRorOp ror_op(search_ctx_);
  void *range_buf = range_op.range_iter_allocator_.alloc(
      sizeof(blocksstable::ObInvSSTableRangeIter));
  ASSERT_NE(nullptr, range_buf);
  range_op.range_iter_ = new (range_buf) blocksstable::ObInvSSTableRangeIter();
  void *posting_buf = ror_op.posting_iter_allocator_.alloc(
      sizeof(blocksstable::ObInvSSTablePostingIter));
  ASSERT_NE(nullptr, posting_buf);
  ror_op.posting_iter_ = new (posting_buf) blocksstable::ObInvSSTablePostingIter();
  ASSERT_EQ(nullptr, ror_op.posting_entry_);
  // Dispatch through the same interface as GroupMerge; repeated release is safe.
  ObIDASSearchOp *ops[] = { &range_op, &ror_op };
  for (int64_t round = 0; round < 2; ++round) {
    for (int64_t i = 0; i < 2; ++i) {
      ops[i]->release_scan_resources();
    }
    EXPECT_EQ(nullptr, range_op.range_iter_);
    EXPECT_EQ(nullptr, ror_op.posting_iter_);
    EXPECT_EQ(0, range_op.range_iter_allocator_.total());
    EXPECT_EQ(0, ror_op.posting_iter_allocator_.total());
    EXPECT_EQ(nullptr, ror_op.posting_scan_param_.reusable_iter_);
  }
}

TEST_F(TestDASAsyncSegmentMerge, posting_scan_helpers_preserve_fallback_lifecycle)
{
  ObArenaAllocator allocator;
  blocksstable::ObInvPostingScanParam param;
  blocksstable::ObInvPostingIterEntry *entry = nullptr;
  const bool use_pool = false;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.prepare_posting_scan(
      use_pool, allocator, param, entry));
  EXPECT_EQ(nullptr, entry);
  EXPECT_EQ(&allocator, param.allocator_);
  EXPECT_EQ(nullptr, param.reusable_iter_);
  EXPECT_EQ(&search_ctx_.get_posting_table_cache(), param.table_cache_);
  void *buf = allocator.alloc(sizeof(blocksstable::ObInvSSTablePostingIter));
  ASSERT_NE(nullptr, buf);
  blocksstable::ObInvSSTablePostingIter *iter =
      new (buf) blocksstable::ObInvSSTablePostingIter();
  search_ctx_.close_posting_scan(param, allocator, entry, iter);
  EXPECT_EQ(nullptr, iter);
  EXPECT_EQ(nullptr, entry);
  EXPECT_EQ(&allocator, param.allocator_);
  EXPECT_EQ(nullptr, param.reusable_iter_);
  // A live lease cannot be silently overwritten by another preparation.
  blocksstable::ObInvPostingIterEntry live_entry;
  entry = &live_entry;
  EXPECT_EQ(OB_INVALID_ARGUMENT, search_ctx_.prepare_posting_scan(
      use_pool, allocator, param, entry));
  EXPECT_EQ(&live_entry, entry);
}

TEST_F(TestDASAsyncSegmentMerge, async_binding_guard_restores_complete_binding)
{
  search_ctx_.set_async_binding(TEST_INDEX_TABLE_ID, 2, true);
  {
    ObDASAsyncBindingGuard guard(search_ctx_);
    guard.set(TEST_INDEX_TABLE_ID_2, 5, false);
    EXPECT_EQ(TEST_INDEX_TABLE_ID_2, search_ctx_.get_async_index_table_id());
    EXPECT_EQ(5, search_ctx_.get_async_group_slot());
    EXPECT_FALSE(search_ctx_.get_async_direct_bind());
  }
  EXPECT_EQ(TEST_INDEX_TABLE_ID, search_ctx_.get_async_index_table_id());
  EXPECT_EQ(2, search_ctx_.get_async_group_slot());
  EXPECT_TRUE(search_ctx_.get_async_direct_bind());
}

TEST_F(TestDASAsyncSegmentMerge, segment_domain_loads_only_selected_group)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  ASSERT_EQ(3, catalog.get_max_group_cnt());
  ASSERT_EQ(1, catalog.get_max_segment_cnt(1, TEST_INDEX_TABLE_ID));

  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *child = create_child({11}, {1.0});
  ASSERT_EQ(OB_SUCCESS, children.push_back(child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 1));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_EQ(1, child->bind_count());
  EXPECT_EQ(100, child->last_range_base());
}

TEST_F(TestDASAsyncSegmentMerge, empty_segment_domain_keeps_children_unopened)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{0, 1}}, {false});
  EXPECT_EQ(0, catalog.get_max_segment_cnt(0, TEST_INDEX_TABLE_ID));
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  MockSegmentOp *child = create_child({99}, {1.0});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(child));
  ObIDASSearchOp *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::create_async_segment_merge_op(
      children, TEST_INDEX_TABLE_ID, true, search_ctx_, root, true));
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, root->get_op_type());
  ASSERT_EQ(OB_SUCCESS, root->open());
  EXPECT_EQ(0, child->open_count());
  EXPECT_EQ(0, child->bind_count());
  ObDASRowID rowid;
  ObDASRowID target;
  double score = 0.0;
  target.set_uint64(1);
  const MaxScoreTuple *tuple = nullptr;
  EXPECT_EQ(OB_ITER_END, root->next_rowid(rowid, score));
  EXPECT_EQ(OB_ITER_END, root->advance_to(target, rowid, score));
  EXPECT_EQ(OB_ITER_END, root->advance_shallow(target, true, tuple));
  EXPECT_EQ(nullptr, tuple);
  EXPECT_EQ(OB_SUCCESS, root->rescan());
  EXPECT_EQ(OB_ITER_END, root->next_rowid(rowid, score));
  EXPECT_EQ(0, child->open_count());
  EXPECT_EQ(0, child->rescan_count());
}

TEST_F(TestDASAsyncSegmentMerge, sparse_single_segment_uses_direct_binding)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1, 0, 1});
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *child = create_child({11}, {1.0});
  ASSERT_EQ(OB_SUCCESS, children.push_back(child));
  ObIDASSearchOp *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::create_async_segment_merge_op(
      children, TEST_INDEX_TABLE_ID, true, search_ctx_, root));
  EXPECT_EQ(child, root);
  for (int64_t partition = 1; partition <= 3; ++partition) {
    select_partition(catalog, partition);
    const auto *part = search_ctx_.get_current_async_partition();
    search_ctx_.set_ls_id(part->ls_id_);
    ObNewVersionRange range;
    bool is_empty = false;
    ASSERT_EQ(OB_SUCCESS, search_ctx_.resolve_direct_async_index_segment(
        TEST_INDEX_TABLE_ID, part->index_tablet_ids_.at(0), 0, range, is_empty));
    EXPECT_EQ(2 == partition, is_empty);
    if (!is_empty) {
      EXPECT_EQ(0, range.base_version_);
      EXPECT_EQ(100, range.snapshot_version_);
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, sparse_multiple_segments_keep_merge)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{2, 1}, {0, 1}}, {false, false});
  PinnedOverlappingCatalog pinned(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned.init(catalog));
  search_ctx_.set_ls_id(ObLSID(1001));
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  EXPECT_EQ(2, catalog.get_max_segment_cnt(0, TEST_INDEX_TABLE_ID));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({11}, {1.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({23}, {2.0})));
  ObIDASSearchOp *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::create_async_segment_merge_op(
      children, TEST_INDEX_TABLE_ID, true, search_ctx_, root));
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, root->get_op_type());
  ASSERT_EQ(OB_SUCCESS, root->open());
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, root->next_rowid(rowid, score));
  EXPECT_EQ(11, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, root->next_rowid(rowid, score));
  EXPECT_EQ(23, rowid.get_uint64());
  EXPECT_EQ(OB_ITER_END, root->next_rowid(rowid, score));
  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, root->rescan());
  EXPECT_EQ(OB_ITER_END, root->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, direct_range_rescans_nonempty_empty_nonempty)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1, 0, 1});
  search_ctx_.set_ls_id(ObLSID(1001));
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_ctdefs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_tablet_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_ctdefs_.push_back(&ctdef));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_tablet_ids_.push_back(ObTabletID(10001)));
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 4));
  ObDASAsyncScalarRangeOpParam param(&ctdef, &rtdef, TEST_INDEX_TABLE_ID, 0, true);
  ObDASAsyncScalarRangeOp leaf(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, leaf.init(param));
  MockRangeAccessService access_service;
  access_service.open_count_ = 1; // Return a prepared batch on the first physical scan.
  leaf.tsc_service_ = &access_service;
  leaf.inv_idx_scan_param_inited_ = true;
  for (int64_t partition = 1; partition <= 3; ++partition) {
    select_partition(catalog, partition);
    ASSERT_EQ(OB_SUCCESS, 1 == partition ? leaf.open() : leaf.rescan());
    const uint64_t *ids = nullptr;
    int64_t count = -1;
    if (2 == partition) {
      EXPECT_EQ(nullptr, leaf.range_iter_);
      EXPECT_EQ(OB_ITER_END, leaf.get_next_doc_ids(8, ids, count));
      EXPECT_EQ(0, count);
      EXPECT_EQ(2, access_service.open_count_);
    } else {
      ASSERT_EQ(OB_SUCCESS, leaf.get_next_doc_ids(8, ids, count));
      ASSERT_EQ(2, count);
      EXPECT_EQ(101, ids[0]);
      EXPECT_EQ(102, ids[1]);
    }
  }
  EXPECT_EQ(3, access_service.open_count_);
  EXPECT_EQ(OB_SUCCESS, leaf.close());
}

TEST_F(TestDASAsyncSegmentMerge, empty_direct_token_interfaces_end_without_storage)
{
  ObDASAsyncTokenOp token(search_ctx_);
  token.is_empty_segment_ = true;
  ObDASRowID rowid;
  ObDASRowID target;
  target.set_uint64(1);
  double score = 0.0;
  const MaxScoreTuple *tuple = nullptr;
  EXPECT_EQ(OB_ITER_END, token.next_rowid(rowid, score));
  EXPECT_EQ(OB_ITER_END, token.advance_to(target, rowid, score));
  EXPECT_EQ(OB_ITER_END, token.advance_to_id(target, rowid));
  EXPECT_EQ(OB_ITER_END, token.advance_shallow(target, true, tuple));
  EXPECT_EQ(OB_SUCCESS, token.calc_max_score(score));
  EXPECT_DOUBLE_EQ(0.0, score);
  bool can_calc = false;
  EXPECT_EQ(OB_SUCCESS, token.calc_range_max_score(target, true, target, score, can_calc));
  EXPECT_TRUE(can_calc);
  EXPECT_DOUBLE_EQ(0.0, score);
  uint64_t ids[2];
  int64_t count = -1;
  EXPECT_EQ(OB_SUCCESS, token.drain_until(target, 2, ids, nullptr, count, rowid, score));
  EXPECT_EQ(0, count);
  EXPECT_TRUE(rowid.is_max());
  uint64_t typed_id = 0;
  EXPECT_EQ(OB_ITER_END, token.advance_to_id_uint64(1, typed_id));
  SparseCandidateBuffer candidates;
  ASSERT_EQ(OB_SUCCESS, candidates.init(2, allocator_));
  ASSERT_EQ(OB_SUCCESS, candidates.append(1, 2.0));
  EXPECT_EQ(OB_SUCCESS, token.score_candidates(candidates, rowid, score));
  EXPECT_TRUE(rowid.is_max());
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, candidates.next(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, typed_id, score));
  EXPECT_EQ(1, typed_id);
  EXPECT_DOUBLE_EQ(2.0, score);
  ObDASAsyncTokenHelper helper(search_ctx_);
  helper.is_empty_segment_ = true;
  uint64_t id_value = 1;
  ObDatum id(reinterpret_cast<const char *>(&id_value), sizeof(id_value), false);
  EXPECT_EQ(OB_ITER_END, helper.get_next_row());
  EXPECT_EQ(OB_ITER_END, helper.advance_to(id));
  EXPECT_EQ(OB_ITER_END, helper.advance_shallow(id, true));
  score = 1.0;
  EXPECT_EQ(OB_SUCCESS, helper.get_max_token_relevance(score));
  EXPECT_DOUBLE_EQ(0.0, score);
}

TEST_F(TestDASAsyncSegmentMerge, direct_merge_rescans_nonempty_empty_nonempty)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1, 0, 1});
  uint64_t frame[2] = {0, 0};
  ScriptedBatchSegmentOp child(search_ctx_, frame, ARRAYSIZEOF(frame));
  child.append_step(ScriptedBatchStep(OB_SUCCESS, 1, {101}));
  child.append_step(ScriptedBatchStep(OB_ITER_END, 0, {}));
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, child.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child));
  ObDASAsyncSegmentMergeOpParam param(children,
      ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(param));
  ASSERT_EQ(OB_SUCCESS, merge.open());
  for (int64_t partition = 1; partition <= 3; ++partition) {
    if (partition > 1) {
      select_partition(catalog, partition);
      ASSERT_EQ(OB_SUCCESS, merge.rescan());
    }
    int64_t count = -1;
    if (2 != partition) {
      ASSERT_EQ(OB_SUCCESS, merge.next_rowids(2, count));
      EXPECT_EQ(1, count);
      EXPECT_EQ(101, frame[0]);
    }
    EXPECT_EQ(OB_ITER_END, merge.next_rowids(2, count));
    EXPECT_EQ(0, count);
  }
  EXPECT_EQ(1, child.open_count());
  EXPECT_EQ(1, child.rescan_count());
  EXPECT_EQ(2, child.bind_count());
  EXPECT_EQ(4, child.batch_call_count());
  EXPECT_EQ(OB_SUCCESS, merge.close());
}

TEST_F(TestDASAsyncSegmentMerge, all_empty_scalar_domain_uses_regular_leaf)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {0, 0, 0});
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  search_ctx_.set_ls_id(ObLSID(1001));
  search_ctx_.table_row_count_ = ObDASSearchCost(100);
  EXPECT_EQ(0, catalog.get_max_segment_cnt(0, TEST_INDEX_TABLE_ID));
  share::SCN snapshot_scn;
  transaction::ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(100));
  snapshot.init_special_read(snapshot_scn);
  search_ctx_.snapshot_ = &snapshot;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObPushdownOperator pd_op(eval_ctx_, ctdef.pd_expr_spec_);
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  rtdef.p_pd_expr_op_ = &pd_op;
  ASSERT_EQ(OB_SUCCESS, scan_op_.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.set_related_task_info(
      &ctdef, &rtdef, ObTabletID(10001)));
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 3));
  for (int64_t use_ror = 0; use_ror <= 1; ++use_ror) {
    ctdef.is_index_term_point_scan_ = use_ror;
    ObIDASSearchOp *root = nullptr;
    ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
    ASSERT_NE(nullptr, root);
    EXPECT_EQ(use_ror ? DAS_SEARCH_OP_ASYNC_SCALAR_ROR : DAS_SEARCH_OP_BITMAP,
        root->get_op_type());
    EXPECT_EQ(nullptr, find_search_op(root, DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE));
    EXPECT_EQ(nullptr, find_search_op(root, DAS_SEARCH_OP_DUMMY));
    // No SSTable handles are populated: physical scans would fail.
    for (int64_t partition = 1; partition <= 3; ++partition) {
      select_partition(catalog, partition);
      ASSERT_EQ(OB_SUCCESS, 1 == partition ? root->open() : root->rescan());
      ObDASRowID rowid;
      double score = 0.0;
      EXPECT_EQ(OB_ITER_END, root->next_rowid(rowid, score));
    }
    ASSERT_EQ(OB_SUCCESS, root->close());
  }
  search_ctx_.snapshot_ = nullptr;
}

TEST_F(TestDASAsyncSegmentMerge, group_merge_activates_partition_specific_children)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1, 3});
  MockSegmentOp *group0 = create_child({1}, {1.0});
  MockSegmentOp *group1 = create_child({2}, {1.0});
  MockSegmentOp *group2 = create_child({3}, {1.0});
  ObArray<ObIDASSearchOp *> group_ops;
  ASSERT_EQ(OB_SUCCESS, group_ops.push_back(group0));
  ASSERT_EQ(OB_SUCCESS, group_ops.push_back(group1));
  ASSERT_EQ(OB_SUCCESS, group_ops.push_back(group2));
  ObDASAsyncSegmentGroupMergeOpParam param(group_ops);
  ObDASAsyncSegmentGroupMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_EQ(0, group0->open_count());
  EXPECT_EQ(0, group1->open_count());
  EXPECT_EQ(0, group2->open_count());
  EXPECT_EQ(0, group0->release_count());

  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(1);
  EXPECT_EQ(OB_NOT_SUPPORTED, merge->advance_to(target, rowid, score));
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(1, group0->open_count());
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
  EXPECT_EQ(1, group0->release_count());
  EXPECT_EQ(0, group1->release_count());
  EXPECT_EQ(0, group2->release_count());

  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_EQ(2, group0->release_count());
  EXPECT_EQ(0, group0->rescan_count());
  EXPECT_EQ(0, group1->open_count());
  EXPECT_EQ(0, group2->open_count());
  for (uint64_t expected_id = 1; expected_id <= 3; ++expected_id) {
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(expected_id, rowid.get_uint64());
    EXPECT_EQ(1, group0->rescan_count());
    EXPECT_EQ(expected_id >= 2 ? 1 : 0, group1->open_count());
    EXPECT_EQ(expected_id >= 3 ? 1 : 0, group2->open_count());
  }
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));

  select_partition(catalog, 1);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_EQ(1, group0->rescan_count());
  EXPECT_EQ(0, group1->rescan_count());
  EXPECT_EQ(0, group2->rescan_count());
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(2, group0->rescan_count());
  EXPECT_EQ(1, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, merge->set_min_competitive_score(0.5));
  EXPECT_DOUBLE_EQ(0.5, group0->last_threshold());
  EXPECT_DOUBLE_EQ(-1.0, group1->last_threshold());
  EXPECT_DOUBLE_EQ(-1.0, group2->last_threshold());
}

TEST_F(TestDASAsyncSegmentMerge, group_merge_propagates_batch_capacity)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  std::vector<uint64_t> group0_ids;
  std::vector<double> group0_scores;
  std::vector<uint64_t> group1_ids;
  std::vector<double> group1_scores;
  for (uint64_t id = 1; id <= 10; ++id) {
    group0_ids.push_back(id);
    group0_scores.push_back(static_cast<double>(id));
  }
  for (uint64_t id = 11; id <= 40; ++id) {
    group1_ids.push_back(id);
    group1_scores.push_back(static_cast<double>(id));
  }
  MockSegmentOp *group0 = create_child(group0_ids, group0_scores);
  MockSegmentOp *group1 = create_child(group1_ids, group1_scores);
  ObArray<ObIDASSearchOp *> group_ops;
  ASSERT_NE(nullptr, group0);
  ASSERT_NE(nullptr, group1);
  ASSERT_EQ(OB_SUCCESS, group_ops.push_back(group0));
  ASSERT_EQ(OB_SUCCESS, group_ops.push_back(group1));
  ObDASAsyncSegmentGroupMergeOpParam param(group_ops);
  ObDASAsyncSegmentGroupMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());

  static constexpr int64_t BATCH_SIZE = 31;
  uint64_t batch_ids[BATCH_SIZE];
  double batch_scores[BATCH_SIZE];
  int64_t count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_max();
  ASSERT_EQ(OB_SUCCESS, merge->drain_until(
      boundary, BATCH_SIZE, batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(BATCH_SIZE, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 1), batch_ids[i]);
    EXPECT_DOUBLE_EQ(static_cast<double>(i + 1), batch_scores[i]);
  }
  EXPECT_EQ(1, group0->drain_count());
  EXPECT_EQ(BATCH_SIZE, group0->max_drain_capacity());
  EXPECT_EQ(1, group1->drain_count());
  EXPECT_EQ(BATCH_SIZE - 10, group1->max_drain_capacity());

  ASSERT_EQ(OB_SUCCESS, merge->drain_until(
      boundary, BATCH_SIZE, batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(9, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 32), batch_ids[i]);
    EXPECT_DOUBLE_EQ(static_cast<double>(i + 32), batch_scores[i]);
  }
  EXPECT_TRUE(next_id_after.is_max());
}

TEST_F(TestDASAsyncSegmentMerge, catalog_is_immutable_and_worker_selects_partition)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1, 3});
  EXPECT_EQ(2, catalog.get_partition_cnt());
  EXPECT_EQ(3, catalog.get_max_segment_cnt(TEST_INDEX_TABLE_ID));
  const ObAsyncIndexPartitionSegCtx *partition = nullptr;
  EXPECT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(2), partition));
  ASSERT_NE(nullptr, partition);
  int64_t tablet_idx = -1;
  int64_t segment_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
            partition->find_index_tablet_idx(TEST_INDEX_TABLE_ID, tablet_idx));
  EXPECT_EQ(0, tablet_idx);
  ASSERT_EQ(OB_SUCCESS,
            partition->get_index_segment_cnt(TEST_INDEX_TABLE_ID, segment_cnt));
  EXPECT_EQ(3, segment_cnt);
  EXPECT_EQ(ObTabletID(10001), partition->index_tablet_ids_.at(tablet_idx));
  const storage::ObAsyncIndexSegmentGroup *first_group =
      partition->get_storage_group_ctx().get_group(0);
  ASSERT_NE(nullptr, first_group);
  EXPECT_EQ(1, first_group->get_tablet_cnt());
  EXPECT_EQ(1, first_group->get_tablet_version_ranges(tablet_idx).count());
  ObSEArray<uint64_t, 1> table_ids;
  ObSEArray<ObTabletID, 1> tablets;
  SCN snapshot;
  snapshot.convert_for_tx(3000);
  ASSERT_EQ(OB_SUCCESS, table_ids.push_back(TEST_INDEX_TABLE_ID));
  ASSERT_EQ(OB_SUCCESS, tablets.push_back(ObTabletID(10002)));
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            catalog.add_partition(
                ObLSID(1001), ObTabletID(3), table_ids, tablets,
                ObTabletID(20001), snapshot));
}

TEST_F(TestDASAsyncSegmentMerge, nested_async_scalar_enables_posting_value_read)
{
  ObObj lo;
  ObObj hi;
  lo.set_uint64(10);
  hi.set_uint64(20);
  search_ctx_.set_docid_range(lo, hi);
  ObDASAsyncScalarRorOp base_op(search_ctx_);
  base_op.inv_idx_tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS, base_op.init_posting_scan_param());
  EXPECT_FALSE(base_op.posting_scan_param_.need_posting_value_);
  EXPECT_EQ(10, base_op.posting_scan_param_.min_doc_id_);
  EXPECT_EQ(20, base_op.posting_scan_param_.max_doc_id_);

  ObDASAsyncScalarRorOp nested_op(search_ctx_);
  nested_op.scope_ = DAS_SCOPE_NESTED_ASYNC;
  nested_op.inv_idx_tablet_id_ = ObTabletID(10001);
  ASSERT_EQ(OB_SUCCESS, nested_op.init_posting_scan_param());
  EXPECT_TRUE(nested_op.posting_scan_param_.need_posting_value_);
  EXPECT_EQ(0, nested_op.posting_scan_param_.min_doc_id_);
  EXPECT_EQ(UINT64_MAX, nested_op.posting_scan_param_.max_doc_id_);
}

TEST_F(TestDASAsyncSegmentMerge, nested_async_scalar_uses_four_column_range)
{
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  for (int64_t i = 0; i < SEARCH_INDEX_DOC_ID; ++i) {
    start_objs[i].set_int(i + 1);
    end_objs[i].set_int(i + 1);
  }
  start_objs[SEARCH_INDEX_DOC_ID].set_min_value();
  end_objs[SEARCH_INDEX_DOC_ID].set_max_value();

  ObNewRange logical_range;
  logical_range.start_key_.assign(start_objs, SEARCH_INDEX_KEY_CNT);
  logical_range.end_key_.assign(end_objs, SEARCH_INDEX_KEY_CNT);
  ObDASScalarScanCtDef scalar_ctdef(allocator_);
  ObDASScalarScanRtDef scalar_rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(
      scalar_ctdef, scalar_rtdef, rowid_expr));
  ASSERT_EQ(OB_SUCCESS, scalar_rtdef.key_ranges_.push_back(logical_range));

  ObDASAsyncScalarRorOp nested_op(search_ctx_);
  nested_op.scope_ = DAS_SCOPE_NESTED_SYNC;
  nested_op.scalar_ctdef_ = &scalar_ctdef;
  nested_op.scalar_rtdef_ = &scalar_rtdef;
  nested_op.range_idx_ = 0;
  ASSERT_EQ(OB_SUCCESS, nested_op.init_term_key());
  EXPECT_EQ(SEARCH_INDEX_DOC_ID, nested_op.term_key_.get_datum_cnt());
}

TEST_F(TestDASAsyncSegmentMerge, compact_rowid_records_size_and_rejects_null)
{
  ObExpr doc_id_expr;
  doc_id_expr.type_ = T_REF_COLUMN;
  doc_id_expr.max_length_ = 20;
  doc_id_expr.is_fixed_length_data_ = true;
  doc_id_expr.len_ = sizeof(uint64_t);
  ObExpr nested_key_expr;
  nested_key_expr.type_ = T_REF_COLUMN;
  nested_key_expr.max_length_ = SEARCH_INDEX_NESTED_KEY_LENGTH;
  nested_key_expr.is_fixed_length_data_ = false;
  ExprFixedArray nested_rowid_exprs(allocator_);
  ASSERT_EQ(OB_SUCCESS, nested_rowid_exprs.init(2));
  ASSERT_EQ(OB_SUCCESS, nested_rowid_exprs.push_back(&doc_id_expr));
  ASSERT_EQ(OB_SUCCESS, nested_rowid_exprs.push_back(&nested_key_expr));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.register_nested_scope(&nested_rowid_exprs, DAS_SCOPE_NESTED_SYNC));

  const uint64_t doc_id = 42;
  const char nested_key[] = {0, 0, 0, 1};
  ObDatum doc_id_datum(reinterpret_cast<const char *>(&doc_id), sizeof(doc_id), false);
  ObDatum nested_key_datum(nested_key, sizeof(nested_key), false);
  ObSEArray<ObDatum, 2> datums;
  ASSERT_EQ(OB_SUCCESS, datums.push_back(doc_id_datum));
  ASSERT_EQ(OB_SUCCESS, datums.push_back(nested_key_datum));

  ObArenaAllocator rowid_allocator("CompactRowID");
  ObDASRowID rowid;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.write_datums_to_rowid(
      DAS_SCOPE_NESTED_SYNC, datums, rowid, rowid_allocator));
  const ObCompactRow *row = rowid.get_compact_row();
  ASSERT_NE(nullptr, row);
  const RowMeta &row_meta = search_ctx_.get_scope(DAS_SCOPE_NESTED_SYNC).rowid_meta_;
  EXPECT_EQ(row_meta.get_row_fixed_size() + sizeof(nested_key), row->get_row_size());

  ObArenaAllocator copy_allocator("RowIDCopy");
  ObDASRowID copied_rowid;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.deep_copy_rowid(
      DAS_SCOPE_NESTED_SYNC, rowid, copied_rowid, copy_allocator));
  ASSERT_NE(nullptr, copied_rowid.get_compact_row());
  EXPECT_NE(row, copied_rowid.get_compact_row());
  EXPECT_EQ(row->get_row_size(), copied_rowid.get_compact_row()->get_row_size());
  ObDatum copied_doc_id;
  ObDatum copied_nested_key;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.get_datum_from_rowid(
      DAS_SCOPE_NESTED_SYNC, copied_rowid, copied_doc_id, 0));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.get_datum_from_rowid(
      DAS_SCOPE_NESTED_SYNC, copied_rowid, copied_nested_key, 1));
  EXPECT_EQ(doc_id, copied_doc_id.get_uint());
  ASSERT_EQ(sizeof(nested_key), copied_nested_key.len_);
  EXPECT_EQ(0, MEMCMP(nested_key, copied_nested_key.ptr_, sizeof(nested_key)));

  ObDatum null_datum;
  null_datum.set_null();
  ObSEArray<ObDatum, 2> null_nested_key_datums;
  ASSERT_EQ(OB_SUCCESS, null_nested_key_datums.push_back(doc_id_datum));
  ASSERT_EQ(OB_SUCCESS, null_nested_key_datums.push_back(null_datum));
  ObDASRowID invalid_rowid;
  EXPECT_EQ(OB_ERR_UNEXPECTED, search_ctx_.write_datums_to_rowid(
      DAS_SCOPE_NESTED_SYNC, null_nested_key_datums, invalid_rowid, rowid_allocator));

  ObSEArray<ObDatum, 2> null_doc_id_datums;
  ASSERT_EQ(OB_SUCCESS, null_doc_id_datums.push_back(null_datum));
  ASSERT_EQ(OB_SUCCESS, null_doc_id_datums.push_back(nested_key_datum));
  EXPECT_EQ(OB_ERR_UNEXPECTED, search_ctx_.write_datums_to_rowid(
      DAS_SCOPE_NESTED_SYNC, null_doc_id_datums, invalid_rowid, rowid_allocator));
}

TEST_F(TestDASAsyncSegmentMerge, disjunction_owns_emitted_compact_rowid)
{
  const int ret = run_compact_disjunction_alias_case();
  EXPECT_TRUE(OB_SUCCESS == ret);
}

TEST_F(TestDASAsyncSegmentMerge, match_filter_pushdown_tracks_catalog_and_query_type)
{
  const int64_t frame_size = 512;
  char **frames = static_cast<char **>(allocator_.alloc(sizeof(char *)));
  ASSERT_NE(nullptr, frames);
  frames[0] = static_cast<char *>(allocator_.alloc(frame_size));
  ASSERT_NE(nullptr, frames[0]);
  MEMSET(frames[0], 0, frame_size);
  exec_ctx_.frames_ = frames;
  exec_ctx_.frame_cnt_ = 1;
  eval_ctx_.frames_ = frames;
  int64_t frame_pos = 0;
  ObExpr minimum_should_match_expr;
  ObExpr match_operator_expr;
  ObExpr match_type_expr;
  ObExpr query_text_expr;
  init_int_const_expr(minimum_should_match_expr, frame_pos, 1);
  init_int_const_expr(
      match_operator_expr, frame_pos, ObMatchOperator::MATCH_OPERATOR_OR);
  init_int_const_expr(
      match_type_expr, frame_pos, ObMatchFieldsType::MATCH_BEST_FIELDS);
  init_string_const_expr(
      query_text_expr, frame_pos, ObString::make_string("ocean"));
  ASSERT_LT(frame_pos, frame_size);

  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASScalarScanRtDef inv_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASIRScanRtDef ir_rtdef;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ObDASBaseRtDef *ir_rtdef_children[] = {&inv_rtdef};
  inv_ctdef.ref_table_id_ = TEST_INDEX_TABLE_ID;
  inv_ctdef.table_param_.parser_name_ = ObString::make_string("analyzer.1");
  inv_ctdef.table_param_.parser_properties_ =
      ObString::make_string("{\"analyzer\":\"standard\"}");
  inv_ctdef.table_param_.fts_index_type_ =
      share::schema::OB_FTS_INDEX_TYPE_MATCH;
  inv_rtdef.ctdef_ = &inv_ctdef;
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = 1;
  ir_ctdef.use_scalar_scan_def_ = 1;
  ir_ctdef.is_async_index_ = 1;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ir_rtdef.children_ = ir_rtdef_children;
  ir_rtdef.children_cnt_ = 1;

  ObDASMatchCtDef match_ctdef(allocator_);
  ObDASMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *match_rtdef_children[] = {&ir_rtdef};
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = 1;
  match_ctdef.ir_ctdef_idx_ = 0;
  match_ctdef.minimum_should_match_ = &minimum_should_match_expr;
  match_ctdef.match_operator_ = &match_operator_expr;
  match_ctdef.set_is_top_level_scoring(true);
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.eval_ctx_ = &eval_ctx_;
  match_rtdef.children_ = match_rtdef_children;
  match_rtdef.children_cnt_ = 1;

  ObDASAsyncIndexQueryCtx single_segment_catalog;
  build_catalog(single_segment_catalog, {1, 1});
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  bool can_pushdown = false;
  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  search_ctx_.set_async_binding(common::OB_INVALID_ID, -1, false);
  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);

  search_ctx_.set_async_query_ctx(nullptr);
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);

  ObDASAsyncIndexQueryCtx uninitialized_catalog;
  search_ctx_.set_async_query_ctx(&uninitialized_catalog);
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);

  search_ctx_.set_async_query_ctx(&single_segment_catalog);
  inv_ctdef.ref_table_id_ = common::OB_INVALID_ID;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  inv_ctdef.ref_table_id_ = TEST_INDEX_TABLE_ID;

  ir_ctdef.children_cnt_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_ctdef_children);

  ObDASAsyncIndexQueryCtx multi_segment_catalog;
  build_multi_index_catalog(
      multi_segment_catalog, {{2, 2}, {2, 1}}, {true, false});
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  search_ctx_.set_async_binding(common::OB_INVALID_ID, 1, false);
  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  search_ctx_.set_async_binding(common::OB_INVALID_ID, -1, false);
  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);

  ObIDASSearchOp *filter_op = reinterpret_cast<ObIDASSearchOp *>(1);
  ObIDASSearchOp *match_op = nullptr;
  match_rtdef.set_pushdown_filter(false, filter_op);
  EXPECT_EQ(OB_STATE_NOT_MATCH,
            match_rtdef.generate_op(
                ObDASSearchCost(1), search_ctx_, match_op));
  EXPECT_EQ(nullptr, match_rtdef.pushdown_filter_op_);

  ir_ctdef.is_async_index_ = 0;
  ASSERT_EQ(OB_SUCCESS,
            match_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  ObDASMultiMatchCtDef multi_ctdef(allocator_);
  ObDASMultiMatchRtDef multi_rtdef;
  ObDASBaseCtDef *multi_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *multi_rtdef_children[] = {&ir_rtdef};
  multi_ctdef.children_ = multi_ctdef_children;
  multi_ctdef.children_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, multi_ctdef.ir_ctdef_indices_.init(1));
  ASSERT_EQ(OB_SUCCESS, multi_ctdef.ir_ctdef_indices_.prepare_allocate(1));
  multi_ctdef.ir_ctdef_indices_.at(0) = 0;
  multi_ctdef.match_operator_ = &match_operator_expr;
  multi_ctdef.minimum_should_match_ = &minimum_should_match_expr;
  multi_ctdef.type_ = &match_type_expr;
  multi_ctdef.query_text_ = &query_text_expr;
  multi_ctdef.set_is_top_level_scoring(true);
  multi_rtdef.ctdef_ = &multi_ctdef;
  multi_rtdef.eval_ctx_ = &eval_ctx_;
  multi_rtdef.children_ = multi_rtdef_children;
  multi_rtdef.children_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS,
            multi_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  ir_ctdef.is_async_index_ = 1;
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS,
            multi_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);

  search_ctx_.set_async_binding(common::OB_INVALID_ID, 1, false);
  ASSERT_EQ(OB_SUCCESS,
            multi_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  search_ctx_.set_async_binding(common::OB_INVALID_ID, -1, false);
  ASSERT_EQ(OB_SUCCESS,
            multi_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);

  search_ctx_.set_async_query_ctx(&single_segment_catalog);
  search_ctx_.set_async_binding(common::OB_INVALID_ID, -1, false);
  ASSERT_EQ(OB_SUCCESS,
            multi_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  minimum_should_match_expr.locate_datum_for_write(eval_ctx_).set_int(-1);
  minimum_should_match_expr.set_evaluated_projected(eval_ctx_);
  ASSERT_EQ(OB_SUCCESS,
            multi_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  minimum_should_match_expr.locate_datum_for_write(eval_ctx_).set_int(0);
  minimum_should_match_expr.set_evaluated_projected(eval_ctx_);
  multi_ctdef.is_msm_unresolved_expr_ = true;
  ASSERT_EQ(OB_SUCCESS,
            multi_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  multi_ctdef.is_msm_unresolved_expr_ = false;
  ASSERT_EQ(OB_SUCCESS,
            multi_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  minimum_should_match_expr.locate_datum_for_write(eval_ctx_).set_int(1);
  minimum_should_match_expr.set_evaluated_projected(eval_ctx_);

  ObDASQueryStringCtDef query_string_ctdef(allocator_);
  ObDASQueryStringRtDef query_string_rtdef;
  ObDASBaseCtDef *query_string_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *query_string_rtdef_children[] = {&ir_rtdef};
  query_string_ctdef.children_ = query_string_ctdef_children;
  query_string_ctdef.children_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, query_string_ctdef.ir_ctdef_indices_.init(1));
  ASSERT_EQ(OB_SUCCESS, query_string_ctdef.ir_ctdef_indices_.prepare_allocate(1));
  query_string_ctdef.ir_ctdef_indices_.at(0) = 0;
  query_string_ctdef.default_operator_ = &match_operator_expr;
  query_string_ctdef.minimum_should_match_ = &minimum_should_match_expr;
  query_string_ctdef.type_ = &match_type_expr;
  query_string_ctdef.query_text_ = &query_text_expr;
  query_string_ctdef.set_is_top_level_scoring(true);
  query_string_rtdef.ctdef_ = &query_string_ctdef;
  query_string_rtdef.eval_ctx_ = &eval_ctx_;
  query_string_rtdef.children_ = query_string_rtdef_children;
  query_string_rtdef.children_cnt_ = 1;
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS,
            query_string_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  search_ctx_.set_async_query_ctx(&multi_segment_catalog);
  ASSERT_EQ(OB_SUCCESS,
            query_string_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  search_ctx_.set_async_query_ctx(&single_segment_catalog);
  search_ctx_.set_async_binding(common::OB_INVALID_ID, -1, false);
  ASSERT_EQ(OB_SUCCESS,
            query_string_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);

  minimum_should_match_expr.locate_datum_for_write(eval_ctx_).set_int(-1);
  minimum_should_match_expr.set_evaluated_projected(eval_ctx_);
  ASSERT_EQ(OB_SUCCESS,
            query_string_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  minimum_should_match_expr.locate_datum_for_write(eval_ctx_).set_int(0);
  minimum_should_match_expr.set_evaluated_projected(eval_ctx_);
  query_string_ctdef.is_msm_unresolved_expr_ = true;
  ASSERT_EQ(OB_SUCCESS,
            query_string_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_FALSE(can_pushdown);
  query_string_ctdef.is_msm_unresolved_expr_ = false;
  ASSERT_EQ(OB_SUCCESS,
            query_string_rtdef.can_pushdown_filter_to_bmm(
                search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  minimum_should_match_expr.locate_datum_for_write(eval_ctx_).set_int(1);
  minimum_should_match_expr.set_evaluated_projected(eval_ctx_);

  uint64_t index_table_id = OB_INVALID_ID;
  int64_t segment_op_cnt = 0;
  search_ctx_.set_async_binding(common::OB_INVALID_ID, -1, false);
  {
    ObDASAsyncBindingGuard binding_guard(search_ctx_);
    ASSERT_EQ(OB_SUCCESS,
              ObDASSearchUtils::resolve_async_field_segment_domain(
                  ir_ctdef, search_ctx_, binding_guard,
                  index_table_id, segment_op_cnt));
    EXPECT_EQ(TEST_INDEX_TABLE_ID, index_table_id);
    EXPECT_EQ(1, segment_op_cnt);
    EXPECT_EQ(-1, search_ctx_.get_async_group_slot());
    EXPECT_FALSE(search_ctx_.get_async_direct_bind());
  }

  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  {
    ObDASAsyncBindingGuard binding_guard(search_ctx_);
    ASSERT_EQ(OB_SUCCESS,
              ObDASSearchUtils::resolve_async_field_segment_domain(
                  ir_ctdef, search_ctx_, binding_guard,
                  index_table_id, segment_op_cnt));
    EXPECT_EQ(TEST_INDEX_TABLE_ID, index_table_id);
    EXPECT_EQ(1, segment_op_cnt);
    EXPECT_EQ(0, search_ctx_.get_async_group_slot());
    EXPECT_TRUE(search_ctx_.get_async_direct_bind());
  }

  ObDASAsyncIndexQueryCtx empty_partition_catalog;
  build_catalog(empty_partition_catalog, {1, 0});
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS,
      match_rtdef.can_pushdown_filter_to_bmm(search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  ASSERT_EQ(OB_SUCCESS,
      multi_rtdef.can_pushdown_filter_to_bmm(search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  ASSERT_EQ(OB_SUCCESS,
      query_string_rtdef.can_pushdown_filter_to_bmm(search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  {
    ObDASAsyncBindingGuard binding_guard(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::resolve_async_field_segment_domain(
        ir_ctdef, search_ctx_, binding_guard, index_table_id, segment_op_cnt));
    EXPECT_EQ(1, segment_op_cnt);
    EXPECT_TRUE(search_ctx_.get_async_direct_bind());
  }
  ObDASAsyncIndexQueryCtx empty_catalog;
  build_catalog(empty_catalog, {0});
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::resolve_async_segment_domain(
      ir_ctdef, search_ctx_, index_table_id, segment_op_cnt));
  EXPECT_EQ(0, segment_op_cnt);
  ASSERT_EQ(OB_SUCCESS,
      match_rtdef.can_pushdown_filter_to_bmm(search_ctx_, can_pushdown));
  EXPECT_TRUE(can_pushdown);
  {
    ObDASAsyncBindingGuard binding_guard(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::resolve_async_field_segment_domain(
        ir_ctdef, search_ctx_, binding_guard, index_table_id, segment_op_cnt));
    EXPECT_EQ(0, segment_op_cnt);
    EXPECT_TRUE(search_ctx_.get_async_direct_bind());
  }
}

TEST_F(TestDASAsyncSegmentMerge, optional_filter_reaches_native_single_segment_async_bmm)
{
  prepare_const_expr_frame(512);
  int64_t frame_pos = 0;
  ObExpr minimum_should_match_expr;
  ObExpr match_operator_expr;
  ObExpr boost_expr;
  init_int_const_expr(minimum_should_match_expr, frame_pos, 0);
  init_int_const_expr(
      match_operator_expr, frame_pos, ObMatchOperator::MATCH_OPERATOR_OR);
  init_double_const_expr(boost_expr, frame_pos, 1.0);

  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASScalarScanRtDef inv_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASIRScanRtDef ir_rtdef;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ObDASBaseRtDef *ir_rtdef_children[] = {&inv_rtdef};
  inv_ctdef.ref_table_id_ = TEST_INDEX_TABLE_ID;
  inv_rtdef.ctdef_ = &inv_ctdef;
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_ctdef_children);
  ir_ctdef.use_scalar_scan_def_ = 1;
  ir_ctdef.is_async_index_ = 1;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ir_rtdef.children_ = ir_rtdef_children;
  ir_rtdef.children_cnt_ = ARRAYSIZEOF(ir_rtdef_children);
  ObDASMatchCtDef match_ctdef(allocator_);
  ObDASMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *match_rtdef_children[] = {&ir_rtdef};
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(match_ctdef_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  match_ctdef.boost_ = &boost_expr;
  match_ctdef.match_operator_ = &match_operator_expr;
  match_ctdef.minimum_should_match_ = &minimum_should_match_expr;
  match_ctdef.set_is_top_level_scoring(true);
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.eval_ctx_ = &eval_ctx_;
  match_rtdef.children_ = match_rtdef_children;
  match_rtdef.children_cnt_ = ARRAYSIZEOF(match_rtdef_children);
  match_rtdef.query_tokens_prepared_ = true;
  ASSERT_EQ(OB_SUCCESS,
      match_rtdef.query_tokens_.push_back(ObString::make_string("alpha")));
  ASSERT_EQ(OB_SUCCESS,
      match_rtdef.query_tokens_.push_back(ObString::make_string("beta")));
  // prepare_query_tokens always fills token_counts_ alongside query_tokens_;
  // keep the manual injection consistent with that invariant.
  ASSERT_EQ(OB_SUCCESS, match_rtdef.token_counts_.push_back(1));
  ASSERT_EQ(OB_SUCCESS, match_rtdef.token_counts_.push_back(1));
  match_rtdef.total_token_cnt_ = 2;
  ASSERT_EQ(OB_SUCCESS,
      match_rtdef.token_costs_.push_back(ObDASSearchCost(2)));
  ASSERT_EQ(OB_SUCCESS,
      match_rtdef.token_costs_.push_back(ObDASSearchCost(3)));
  match_rtdef.cost_ = ObDASSearchCost(5);
  ObDASScalarScanCtDef delete_ctdef(allocator_);
  ObDASScalarScanRtDef delete_rtdef;
  delete_rtdef.ctdef_ = &delete_ctdef;
  ObDASBooleanQueryCtDef async_ctdef(allocator_);
  ObDASBooleanQueryRtDef async_rtdef;
  ObDASBaseCtDef *async_ctdef_children[] = {&match_ctdef, &delete_ctdef};
  ObDASBaseRtDef *async_rtdef_children[] = {&match_rtdef, &delete_rtdef};
  async_ctdef.children_ = async_ctdef_children;
  async_ctdef.children_cnt_ = ARRAYSIZEOF(async_ctdef_children);
  async_ctdef.set_must(0, 1);
  async_ctdef.set_must_not(1, 1);
  async_ctdef.set_is_async(true);
  async_ctdef.set_is_scoring(true);
  async_ctdef.set_is_top_level_scoring(true);
  async_rtdef.ctdef_ = &async_ctdef;
  async_rtdef.children_ = async_rtdef_children;
  async_rtdef.children_cnt_ = ARRAYSIZEOF(async_rtdef_children);
  async_rtdef.cost_ = ObDASSearchCost(5);
  MockSegmentOp *filter_op = create_child({1, 4}, {0.0, 0.0});
  ASSERT_NE(nullptr, filter_op);
  ObDASScalarScanCtDef filter_ctdef(allocator_);
  FixedEnumerableFilterRtDef filter_rtdef(filter_op);
  filter_rtdef.ctdef_ = &filter_ctdef;
  ObDASBooleanQueryCtDef outer_ctdef(allocator_);
  ObDASBooleanQueryRtDef outer_rtdef;
  ObDASBaseCtDef *outer_ctdef_children[] = {&filter_ctdef, &async_ctdef};
  ObDASBaseRtDef *outer_rtdef_children[] = {&filter_rtdef, &async_rtdef};
  outer_ctdef.children_ = outer_ctdef_children;
  outer_ctdef.children_cnt_ = ARRAYSIZEOF(outer_ctdef_children);
  outer_ctdef.set_filter(0, 1);
  outer_ctdef.set_should(1, 1);
  outer_ctdef.set_min_should_match(0);
  outer_ctdef.set_is_scoring(true);
  outer_ctdef.set_is_top_level_scoring(true);
  outer_rtdef.ctdef_ = &outer_ctdef;
  outer_rtdef.children_ = outer_rtdef_children;
  outer_rtdef.children_cnt_ = ARRAYSIZEOF(outer_rtdef_children);
  outer_rtdef.cost_ = ObDASSearchCost(6);
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1});
  catalog.group_alignment_enabled_ = true;
  search_ctx_.set_async_binding(OB_INVALID_ID, -1, false);
  ObIDASSearchOp *root = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      outer_rtdef.generate_op(ObDASSearchCost(10), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  EXPECT_EQ(1, filter_rtdef.generate_count_);

  ObIDASSearchOp *bmm_base = find_search_op(root, DAS_SEARCH_OP_BMM);
  ASSERT_NE(nullptr, bmm_base);
  ObDASBMMOp *bmm = static_cast<ObDASBMMOp *>(bmm_base);
  EXPECT_TRUE(bmm->query_optional_);
  EXPECT_EQ(filter_op, bmm->get_filter_op());
  ASSERT_EQ(3, bmm->get_children_cnt());
  for (int64_t i = 0; i < 2; ++i) {
    ASSERT_EQ(DAS_SEARCH_OP_ASYNC_TOKEN, bmm->get_children()[i]->get_op_type());
    ObDASAsyncTokenOp *token =
        static_cast<ObDASAsyncTokenOp *>(bmm->get_children()[i]);
    EXPECT_TRUE(token->direct_bind_);
    EXPECT_EQ(TEST_INDEX_TABLE_ID, token->index_table_id_);
    EXPECT_EQ(0, token->group_slot_);
  }
  EXPECT_EQ(filter_op, bmm->get_children()[2]);
}

TEST_F(TestDASAsyncSegmentMerge, explicit_options_reach_native_async_bmm_consumers)
{
  const int add_ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
  ASSERT_TRUE(OB_SUCCESS == add_ret || OB_ENTRY_EXIST == add_ret);
  prepare_const_expr_frame(512);
  int64_t frame_pos = 0;
  ObExpr msm_expr;
  ObExpr operator_expr;
  ObExpr boost_expr;
  init_int_const_expr(msm_expr, frame_pos, 1);
  init_int_const_expr(operator_expr, frame_pos, ObMatchOperator::MATCH_OPERATOR_OR);
  init_double_const_expr(boost_expr, frame_pos, 1.0);

  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1});
  search_ctx_.set_async_binding(TEST_INDEX_TABLE_ID, 0, true);
  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASScalarScanRtDef inv_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASIRScanRtDef ir_rtdef;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ObDASBaseRtDef *ir_rtdef_children[] = {&inv_rtdef};
  inv_ctdef.ref_table_id_ = TEST_INDEX_TABLE_ID;
  inv_rtdef.ctdef_ = &inv_ctdef;
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_ctdef_children);
  ir_ctdef.use_scalar_scan_def_ = 1;
  ir_ctdef.is_async_index_ = 1;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ir_rtdef.children_ = ir_rtdef_children;
  ir_rtdef.children_cnt_ = ARRAYSIZEOF(ir_rtdef_children);
  ObDASBaseCtDef *ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *rtdef_children[] = {&ir_rtdef};

  ObDASMatchCtDef match_ctdef(allocator_);
  ObDASMatchRtDef match_rtdef;
  match_ctdef.children_ = ctdef_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  match_ctdef.boost_ = &boost_expr;
  match_ctdef.match_operator_ = &operator_expr;
  match_ctdef.minimum_should_match_ = &msm_expr;
  match_ctdef.set_is_top_level_scoring(true);
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.eval_ctx_ = &eval_ctx_;
  match_rtdef.children_ = rtdef_children;
  match_rtdef.children_cnt_ = ARRAYSIZEOF(rtdef_children);
  match_rtdef.query_tokens_prepared_ = true;
  ASSERT_EQ(OB_SUCCESS, match_rtdef.query_tokens_.push_back(ObString::make_string("alpha")));
  ASSERT_EQ(OB_SUCCESS, match_rtdef.query_tokens_.push_back(ObString::make_string("beta")));
  // Model "alpha alpha beta" so option propagation also covers token folding.
  ASSERT_EQ(OB_SUCCESS, match_rtdef.token_counts_.push_back(2));
  ASSERT_EQ(OB_SUCCESS, match_rtdef.token_counts_.push_back(1));
  match_rtdef.total_token_cnt_ = 3;
  ASSERT_EQ(OB_SUCCESS, match_rtdef.token_costs_.push_back(ObDASSearchCost(2)));
  ASSERT_EQ(OB_SUCCESS, match_rtdef.token_costs_.push_back(ObDASSearchCost(3)));
  match_rtdef.cost_ = ObDASSearchCost(5);

  ObDASMultiMatchCtDef multi_ctdef(allocator_);
  ObDASMultiMatchRtDef multi_rtdef;
  multi_ctdef.children_ = ctdef_children;
  multi_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  ASSERT_EQ(OB_SUCCESS, multi_ctdef.ir_ctdef_indices_.init(1));
  ASSERT_EQ(OB_SUCCESS, multi_ctdef.ir_ctdef_indices_.push_back(0));
  multi_ctdef.set_is_top_level_scoring(true);
  multi_rtdef.ctdef_ = &multi_ctdef;
  multi_rtdef.children_ = rtdef_children;
  multi_rtdef.children_cnt_ = ARRAYSIZEOF(rtdef_children);
  // The mock bypasses cost estimation (no eval_ctx), so pre-populate the flat
  // BMM cost cache directly: 1 field x 2 tokens.
  ASSERT_EQ(OB_SUCCESS, multi_rtdef.field_token_costs_.push_back(ObDASSearchCost(2)));
  ASSERT_EQ(OB_SUCCESS, multi_rtdef.field_token_costs_.push_back(ObDASSearchCost(3)));

  ObDASQueryStringCtDef query_ctdef(allocator_);
  ObDASQueryStringRtDef query_rtdef;
  query_ctdef.children_ = ctdef_children;
  query_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  ASSERT_EQ(OB_SUCCESS, query_ctdef.ir_ctdef_indices_.init(2));
  ASSERT_EQ(OB_SUCCESS, query_ctdef.ir_ctdef_indices_.push_back(0));
  query_ctdef.set_is_top_level_scoring(true);
  query_rtdef.ctdef_ = &query_ctdef;
  query_rtdef.children_ = rtdef_children;
  query_rtdef.children_cnt_ = ARRAYSIZEOF(rtdef_children);
  // Same as above: pre-populate the flat BMM token cost cache for 1 field x 2
  // tokens; a second field is appended below and needs two more entries.
  ASSERT_EQ(OB_SUCCESS, query_rtdef.field_token_costs_.push_back(ObDASSearchCost(2)));
  ASSERT_EQ(OB_SUCCESS, query_rtdef.field_token_costs_.push_back(ObDASSearchCost(3)));

  for (int64_t mode = 0; mode < 2; ++mode) {
    const ObDASBMMBuildOptions options(0 != mode);
    AsyncTermsDirectDrainConfigGuard changed_config(0 == mode);
    ASSERT_TRUE(changed_config.is_valid());
    ObIDASSearchRtDef *consumers[] = {&match_rtdef, &multi_rtdef, &query_rtdef};
    for (int64_t i = 0; i < ARRAYSIZEOF(consumers); ++i) {
      MockSegmentOp *filter = create_child({1, 4}, {0.0, 0.0});
      ASSERT_NE(nullptr, filter);
      ASSERT_EQ(OB_SUCCESS, consumers[i]->set_bmm_input(false, filter, ObDASSearchCost(2), options));
      ObIDASSearchOp *root = nullptr;
      if (0 == i) {
        ASSERT_EQ(OB_SUCCESS, match_rtdef.generate_op(ObDASSearchCost(11), search_ctx_, root));
        EXPECT_FALSE(match_rtdef.has_bmm_build_options_);
      } else if (1 == i) {
        ASSERT_EQ(OB_SUCCESS, multi_rtdef.generate_field_op(
            0, match_rtdef.query_tokens_, 1.0, ObMatchOperator::MATCH_OPERATOR_OR,
            1, ObMatchFieldsType::MATCH_BEST_FIELDS, ObDASSearchCost(11), search_ctx_, root));
      } else {
        ASSERT_EQ(OB_SUCCESS, query_rtdef.generate_group_field_op(
            0, 0, match_rtdef.query_tokens_, 1.0, ObMatchOperator::MATCH_OPERATOR_OR,
            1, ObMatchFieldsType::MATCH_BEST_FIELDS, ObDASSearchCost(11), search_ctx_, root));
      }
      ASSERT_NE(nullptr, root);
      ASSERT_EQ(DAS_SEARCH_OP_BMM, root->get_op_type());
      ObDASBMMOp *bmm = static_cast<ObDASBMMOp *>(root);
      EXPECT_EQ(0 != mode, bmm->enable_vec_merge_);
      EXPECT_EQ(filter, bmm->get_filter_op());
      ASSERT_EQ(3, bmm->get_children_cnt());
      if (0 == i) {
        ASSERT_EQ(DAS_SEARCH_OP_ASYNC_TOKEN, bmm->get_children()[0]->get_op_type());
        ASSERT_EQ(DAS_SEARCH_OP_ASYNC_TOKEN, bmm->get_children()[1]->get_op_type());
        EXPECT_DOUBLE_EQ(2.0,
            static_cast<ObDASAsyncTokenOp *>(bmm->get_children()[0])->token_boost_);
        EXPECT_DOUBLE_EQ(1.0,
            static_cast<ObDASAsyncTokenOp *>(bmm->get_children()[1])->token_boost_);
      }
    }
  }

  // A multi-field query can contain a BMM that does not consume this filter.
  // It must retain its own initialization-time configuration read.
  ASSERT_EQ(OB_SUCCESS, query_ctdef.ir_ctdef_indices_.push_back(0));
  // The second field extends the flat token cost cache to 2 fields x 2 tokens.
  ASSERT_EQ(OB_SUCCESS, query_rtdef.field_token_costs_.push_back(ObDASSearchCost(2)));
  ASSERT_EQ(OB_SUCCESS, query_rtdef.field_token_costs_.push_back(ObDASSearchCost(3)));
  MockSegmentOp *filter = create_child({1, 4}, {0.0, 0.0});
  ASSERT_NE(nullptr, filter);
  ASSERT_EQ(OB_SUCCESS, query_rtdef.set_bmm_input(
      false, filter, ObDASSearchCost(2), ObDASBMMBuildOptions(false)));
  AsyncTermsDirectDrainConfigGuard enabled_config;
  ASSERT_TRUE(enabled_config.is_valid());
  ObIDASSearchOp *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, query_rtdef.generate_group_field_op(
      0, 0, match_rtdef.query_tokens_, 1.0, ObMatchOperator::MATCH_OPERATOR_OR,
      1, ObMatchFieldsType::MATCH_BEST_FIELDS, ObDASSearchCost(11), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_BMM, root->get_op_type());
  ObDASBMMOp *bmm = static_cast<ObDASBMMOp *>(root);
  EXPECT_TRUE(bmm->enable_vec_merge_);
  EXPECT_EQ(nullptr, bmm->get_filter_op());
}

TEST_F(TestDASAsyncSegmentMerge, empty_optional_scoring_preserves_filter_domain)
{
  prepare_const_expr_frame(512);
  int64_t frame_pos = 0;
  ObExpr minimum_should_match_expr;
  ObExpr empty_query_expr;
  init_int_const_expr(minimum_should_match_expr, frame_pos, 0);
  init_varchar_const_expr(empty_query_expr, frame_pos, ObString());
  ASSERT_LT(frame_pos, 512);

  ObDASScalarScanCtDef inv_ctdef(allocator_);
  ObDASScalarScanRtDef inv_rtdef;
  ObDASIRScanCtDef ir_ctdef(allocator_);
  ObDASIRScanRtDef ir_rtdef;
  ObDASBaseCtDef *ir_ctdef_children[] = {&inv_ctdef};
  ObDASBaseRtDef *ir_rtdef_children[] = {&inv_rtdef};
  inv_rtdef.ctdef_ = &inv_ctdef;
  inv_ctdef.table_param_.parser_name_ = ObString::make_string("space.1");
  ir_ctdef.children_ = ir_ctdef_children;
  ir_ctdef.children_cnt_ = ARRAYSIZEOF(ir_ctdef_children);
  ir_ctdef.use_scalar_scan_def_ = 1;
  ir_rtdef.ctdef_ = &ir_ctdef;
  ir_rtdef.children_ = ir_rtdef_children;
  ir_rtdef.children_cnt_ = ARRAYSIZEOF(ir_rtdef_children);

  ObDASMatchCtDef match_ctdef(allocator_);
  ObDASMatchRtDef match_rtdef;
  ObDASBaseCtDef *match_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *match_rtdef_children[] = {&ir_rtdef};
  match_ctdef.children_ = match_ctdef_children;
  match_ctdef.children_cnt_ = ARRAYSIZEOF(match_ctdef_children);
  match_ctdef.ir_ctdef_idx_ = 0;
  match_ctdef.minimum_should_match_ = &minimum_should_match_expr;
  match_rtdef.ctdef_ = &match_ctdef;
  match_rtdef.eval_ctx_ = &eval_ctx_;
  match_rtdef.children_ = match_rtdef_children;
  match_rtdef.children_cnt_ = ARRAYSIZEOF(match_rtdef_children);
  match_rtdef.query_tokens_prepared_ = true;

  MockSegmentOp *match_filter_op = create_child({3, 7}, {0.0, 0.0});
  ASSERT_NE(nullptr, match_filter_op);
  match_rtdef.set_pushdown_filter(true, match_filter_op);
  ObIDASSearchOp *root = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      match_rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_EQ(match_filter_op, root);
  verify_zero_score_filter_domain(root);

  ObDASMultiMatchCtDef multi_ctdef(allocator_);
  ObDASMultiMatchRtDef multi_rtdef;
  ObDASBaseCtDef *multi_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *multi_rtdef_children[] = {&ir_rtdef};
  multi_ctdef.children_ = multi_ctdef_children;
  multi_ctdef.children_cnt_ = ARRAYSIZEOF(multi_ctdef_children);
  ASSERT_EQ(OB_SUCCESS, multi_ctdef.ir_ctdef_indices_.init(1));
  ASSERT_EQ(OB_SUCCESS, multi_ctdef.ir_ctdef_indices_.prepare_allocate(1));
  multi_ctdef.ir_ctdef_indices_.at(0) = 0;
  multi_ctdef.query_text_ = &empty_query_expr;
  multi_rtdef.ctdef_ = &multi_ctdef;
  multi_rtdef.eval_ctx_ = &eval_ctx_;
  multi_rtdef.children_ = multi_rtdef_children;
  multi_rtdef.children_cnt_ = ARRAYSIZEOF(multi_rtdef_children);
  MockSegmentOp *multi_filter_op = create_child({3, 7}, {0.0, 0.0});
  ASSERT_NE(nullptr, multi_filter_op);
  multi_rtdef.set_pushdown_filter(true, multi_filter_op);
  root = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      multi_rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_EQ(multi_filter_op, root);
  verify_zero_score_filter_domain(root);

  ObDASQueryStringCtDef query_string_ctdef(allocator_);
  ObDASQueryStringRtDef query_string_rtdef;
  ObDASBaseCtDef *query_string_ctdef_children[] = {&ir_ctdef};
  ObDASBaseRtDef *query_string_rtdef_children[] = {&ir_rtdef};
  query_string_ctdef.children_ = query_string_ctdef_children;
  query_string_ctdef.children_cnt_ = ARRAYSIZEOF(query_string_ctdef_children);
  ASSERT_EQ(OB_SUCCESS, query_string_ctdef.ir_ctdef_indices_.init(1));
  ASSERT_EQ(OB_SUCCESS,
      query_string_ctdef.ir_ctdef_indices_.prepare_allocate(1));
  query_string_ctdef.ir_ctdef_indices_.at(0) = 0;
  query_string_ctdef.query_text_ = &empty_query_expr;
  query_string_rtdef.ctdef_ = &query_string_ctdef;
  query_string_rtdef.eval_ctx_ = &eval_ctx_;
  query_string_rtdef.children_ = query_string_rtdef_children;
  query_string_rtdef.children_cnt_ = ARRAYSIZEOF(query_string_rtdef_children);
  MockSegmentOp *query_string_filter_op = create_child({3, 7}, {0.0, 0.0});
  ASSERT_NE(nullptr, query_string_filter_op);
  query_string_rtdef.set_pushdown_filter(true, query_string_filter_op);
  root = nullptr;
  ASSERT_EQ(OB_SUCCESS,
      query_string_rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_EQ(query_string_filter_op, root);
  verify_zero_score_filter_domain(root);
}

TEST_F(TestDASAsyncSegmentMerge, independent_index_domains_use_their_own_segment_counts)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{2, 3}, {4, 1}}, {false, false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  EXPECT_EQ(4, catalog.get_max_segment_cnt(TEST_INDEX_TABLE_ID));
  EXPECT_EQ(3, catalog.get_max_segment_cnt(TEST_INDEX_TABLE_ID_2));

  const ObAsyncIndexPartitionSegCtx *partition = nullptr;
  ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(1), partition));
  ASSERT_NE(nullptr, partition);
  int64_t first_segment_cnt = 0;
  int64_t second_segment_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
            partition->get_index_segment_cnt(TEST_INDEX_TABLE_ID, first_segment_cnt));
  ASSERT_EQ(OB_SUCCESS,
            partition->get_index_segment_cnt(TEST_INDEX_TABLE_ID_2, second_segment_cnt));
  EXPECT_EQ(2, first_segment_cnt);
  EXPECT_EQ(3, second_segment_cnt);

  ObArray<ObIDASSearchOp *> first_children;
  MockSegmentOp *first_child_ops[4] = {nullptr, nullptr, nullptr, nullptr};
  for (int64_t i = 0; i < ARRAYSIZEOF(first_child_ops); ++i) {
    first_child_ops[i] = create_child({static_cast<uint64_t>(i + 1)}, {1.0});
    ASSERT_EQ(OB_SUCCESS, first_children.push_back(first_child_ops[i]));
  }
  ObDASAsyncSegmentMergeOpParam first_param(
      first_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *first_merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(first_param, first_merge));
  ASSERT_EQ(OB_SUCCESS, first_merge->open());
  EXPECT_FALSE(first_merge->use_ordered_concat_);
  EXPECT_EQ(1, first_child_ops[0]->bind_count());
  EXPECT_EQ(1, first_child_ops[1]->bind_count());
  EXPECT_EQ(0, first_child_ops[2]->bind_count());
  EXPECT_EQ(0, first_child_ops[3]->bind_count());
  EXPECT_EQ(0, first_child_ops[0]->last_range_base());
  EXPECT_EQ(500, first_child_ops[1]->last_range_base());

  ObArray<ObIDASSearchOp *> second_children;
  MockSegmentOp *second_child_ops[3] = {nullptr, nullptr, nullptr};
  for (int64_t i = 0; i < ARRAYSIZEOF(second_child_ops); ++i) {
    second_child_ops[i] = create_child({static_cast<uint64_t>(10 + i)}, {1.0});
    ASSERT_EQ(OB_SUCCESS, second_children.push_back(second_child_ops[i]));
  }
  ObDASAsyncSegmentMergeOpParam second_param(
      second_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID_2));
  ObDASAsyncSegmentMergeOp *second_merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(second_param, second_merge));
  ASSERT_EQ(OB_SUCCESS, second_merge->open());
  EXPECT_FALSE(second_merge->use_ordered_concat_);
  EXPECT_EQ(1, second_child_ops[0]->bind_count());
  EXPECT_EQ(1, second_child_ops[1]->bind_count());
  EXPECT_EQ(1, second_child_ops[2]->bind_count());
  EXPECT_EQ(0, second_child_ops[0]->last_range_base());
  EXPECT_EQ(333, second_child_ops[1]->last_range_base());
  EXPECT_EQ(666, second_child_ops[2]->last_range_base());
}

TEST_F(TestDASAsyncSegmentMerge, async_tree_detection_and_sqc_registry)
{
  ObDASScalarScanCtDef ordinary_scan(allocator_);
  ObDASScalarScanCtDef async_scalar_scan(allocator_);
  ObDASScalarScanCtDef async_inv_scan(allocator_);
  ObDASScalarScanCtDef sync_inv_scan(allocator_);
  ObDASIRScanCtDef async_ir_scan(allocator_);
  ObDASIRScanCtDef sync_ir_scan(allocator_);
  ObDASBaseCtDef *async_ir_children[] = {&async_inv_scan};
  ObDASBaseCtDef *sync_ir_children[] = {&sync_inv_scan};
  ObDASBooleanQueryCtDef root(allocator_);
  ObDASBaseCtDef *root_children[] = {&ordinary_scan, &async_ir_scan};
  async_ir_scan.children_ = async_ir_children;
  async_ir_scan.children_cnt_ = ARRAYSIZEOF(async_ir_children);
  async_ir_scan.is_async_index_ = true;
  sync_ir_scan.children_ = sync_ir_children;
  sync_ir_scan.children_cnt_ = ARRAYSIZEOF(sync_ir_children);
  sync_ir_scan.is_async_index_ = false;
  async_scalar_scan.is_async_index_ = true;
  root.children_ = root_children;
  root.children_cnt_ = ARRAYSIZEOF(root_children);

  EXPECT_TRUE(ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(&root));
  EXPECT_FALSE(ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(nullptr));
  EXPECT_FALSE(ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(&ordinary_scan));
  EXPECT_FALSE(ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(&sync_ir_scan));
  EXPECT_TRUE(ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(&async_scalar_scan));

  ObPxRpcInitSqcArgs sqc_args;
  ObSqcCtx sqc_ctx(sqc_args);
  ObDASAsyncIndexQueryCtx *query_ctx = nullptr;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, sqc_ctx.get_async_index_query_ctx(101, query_ctx));
  EXPECT_EQ(nullptr, query_ctx);
  ASSERT_EQ(OB_SUCCESS, sqc_ctx.create_async_index_query_ctx(101, query_ctx));
  ASSERT_NE(nullptr, query_ctx);
  ObDASAsyncIndexQueryCtx *found_ctx = nullptr;
  EXPECT_EQ(OB_SUCCESS, sqc_ctx.get_async_index_query_ctx(101, found_ctx));
  EXPECT_EQ(query_ctx, found_ctx);
}

TEST_F(TestDASAsyncSegmentMerge, async_index_access_and_shared_context_detection)
{
  ObDASVecAuxScanCtDef sync_vector(allocator_);
  ObDASVecAuxScanCtDef async_vector(allocator_);
  ObDASVecIndexAsyncIVFScanCtDef shared_vector(allocator_);
  ObDASIRScanCtDef sync_ir(allocator_);
  ObDASIRScanCtDef async_ir(allocator_);
  ObDASScalarScanCtDef sync_scalar(allocator_);
  ObDASScalarScanCtDef async_scalar(allocator_);
  ObDASBooleanQueryCtDef async_boolean(allocator_);
  async_vector.is_async_index_ = true;
  async_ir.is_async_index_ = true;
  async_scalar.is_async_index_ = true;
  async_boolean.set_is_async(true);
  const struct {
    const ObDASBaseCtDef *ctdef_;
    bool has_access_;
    bool uses_shared_context_;
  } cases[] = {
    {nullptr, false, false},
    {&sync_vector, false, false},
    {&async_vector, true, false},
    {&shared_vector, true, true},
    {&sync_ir, false, false},
    {&async_ir, true, true},
    {&sync_scalar, false, false},
    {&async_scalar, true, true},
    {&async_boolean, true, true}
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    SCOPED_TRACE(i);
    EXPECT_EQ(cases[i].has_access_,
              ObDASAsyncIndexQueryCtx::contains_async_index_access(cases[i].ctdef_));
    EXPECT_EQ(cases[i].uses_shared_context_,
              ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(cases[i].ctdef_));
  }

  ObDASTableLookupCtDef lookup(allocator_);
  ObDASSortCtDef sort(allocator_);
  ObDASBaseCtDef *lookup_children[] = {&sync_scalar, &async_vector};
  ObDASBaseCtDef *sort_children[] = {&lookup};
  lookup.children_ = lookup_children;
  lookup.children_cnt_ = ARRAYSIZEOF(lookup_children);
  sort.children_ = sort_children;
  sort.children_cnt_ = ARRAYSIZEOF(sort_children);
  EXPECT_TRUE(ObDASAsyncIndexQueryCtx::contains_async_index_access(&sort));
  EXPECT_FALSE(ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(&sort));
  async_vector.is_async_index_ = false;
  EXPECT_FALSE(ObDASAsyncIndexQueryCtx::contains_async_index_access(&sort));
  EXPECT_FALSE(ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(&sort));

  ObDASBooleanQueryCtDef mixed(allocator_);
  ObDASBaseCtDef *mixed_children[] = {nullptr, &sort, &async_scalar};
  mixed.children_ = mixed_children;
  mixed.children_cnt_ = ARRAYSIZEOF(mixed_children);
  EXPECT_TRUE(ObDASAsyncIndexQueryCtx::contains_async_index_access(&mixed));
  EXPECT_TRUE(ObDASAsyncIndexQueryCtx::contains_shared_async_index_query(&mixed));
}

TEST_F(TestDASAsyncSegmentMerge, collect_async_scalar_index_table_ids)
{
  ObDASScalarScanCtDef async_scalar_scan1(allocator_);
  ObDASScalarScanCtDef async_scalar_scan2(allocator_);
  ObDASScalarScanCtDef inv_scan(allocator_);
  ObDASIRScanCtDef ir_scan(allocator_);
  ObDASBaseCtDef *ir_children[] = {&inv_scan};
  ObDASBooleanQueryCtDef root(allocator_);
  ObDASBaseCtDef *root_children[] = {&async_scalar_scan1, &ir_scan, &async_scalar_scan2};
  async_scalar_scan1.ref_table_id_ = 9001;
  async_scalar_scan1.is_async_index_ = true;
  async_scalar_scan2.ref_table_id_ = 9002;
  async_scalar_scan2.is_async_index_ = true;
  inv_scan.ref_table_id_ = 9001;
  ir_scan.children_ = ir_children;
  ir_scan.children_cnt_ = ARRAYSIZEOF(ir_children);
  ir_scan.is_async_index_ = true;
  ir_scan.use_scalar_scan_def_ = true;
  root.children_ = root_children;
  root.children_cnt_ = ARRAYSIZEOF(root_children);

  ObSEArray<uint64_t, 2> logical_table_ids;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::collect_async_index_table_ids(
                &async_scalar_scan1, logical_table_ids));
  ASSERT_EQ(1, logical_table_ids.count());
  EXPECT_EQ(9001, logical_table_ids.at(0));

  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::collect_async_index_table_ids(
                &root, logical_table_ids));
  ASSERT_EQ(2, logical_table_ids.count());
  EXPECT_EQ(9001, logical_table_ids.at(0));
  EXPECT_EQ(9002, logical_table_ids.at(1));
}

TEST_F(TestDASAsyncSegmentMerge, domain_analysis_rejects_sync_scalar_and_ignores_vector)
{
  ObDASScalarScanCtDef async_scalar(allocator_);
  ObDASScalarScanCtDef other_scalar(allocator_);
  ObDASScalarScanCtDef primary_scan(allocator_);
  ObDASScalarCtDef primary_probe(allocator_);
  ObDASVecAuxScanCtDef vector_scan(allocator_);
  ObDASBooleanQueryCtDef root(allocator_);
  ObDASAsyncIndexDomainAnalysis analysis;
  ObDASBaseCtDef *children[] = {&async_scalar, &other_scalar};
  ObDASBaseCtDef *probe_children[] = {&primary_scan};
  async_scalar.ref_table_id_ = TEST_INDEX_TABLE_ID;
  async_scalar.is_async_index_ = true;
  other_scalar.ref_table_id_ = TEST_INDEX_TABLE_ID_2;
  root.children_ = children;
  root.children_cnt_ = ARRAYSIZEOF(children);

  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &root, analysis));
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_MIXED_SYNC, analysis.type_);
  EXPECT_FALSE(analysis.has_async_fts_wrapper_);

  other_scalar.is_async_index_ = true;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &root, analysis));
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_MULTI, analysis.type_);

  children[1] = &vector_scan;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &root, analysis));
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_SINGLE, analysis.type_);
  EXPECT_EQ(TEST_INDEX_TABLE_ID, analysis.index_table_id_);

  primary_scan.ref_table_id_ = 8001;
  primary_scan.is_primary_table_scan_ = true;
  primary_probe.children_ = probe_children;
  primary_probe.children_cnt_ = ARRAYSIZEOF(probe_children);
  primary_probe.set_has_main_scan(true);
  children[1] = &primary_probe;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &root, analysis));
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_MIXED_SYNC, analysis.type_);
  EXPECT_EQ(ASYNC_INDEX_EXEC_LEGACY, analysis.get_exec_mode());
}

TEST_F(TestDASAsyncSegmentMerge,
       domain_analysis_same_fts_multi_match_selects_group_and_nested_is_non_alignable)
{
  ObDASScalarScanCtDef inv_scan1(allocator_);
  ObDASScalarScanCtDef inv_scan2(allocator_);
  ObDASIRScanCtDef ir_scan1(allocator_);
  ObDASIRScanCtDef ir_scan2(allocator_);
  ObDASMatchCtDef match1(allocator_);
  ObDASMatchCtDef match2(allocator_);
  ObDASBooleanQueryCtDef root(allocator_);
  ObDASNestedQueryCtDef nested(allocator_);
  ObDASScalarScanCtDef delete_scan(allocator_);
  ObDASBooleanQueryCtDef nested_wrapper(allocator_);
  ObDASBaseCtDef *ir_children1[] = {&inv_scan1};
  ObDASBaseCtDef *ir_children2[] = {&inv_scan2};
  ObDASBaseCtDef *match_children1[] = {&ir_scan1};
  ObDASBaseCtDef *match_children2[] = {&ir_scan2};
  ObDASBaseCtDef *root_children[] = {&match1, &match2};
  ObDASBaseCtDef *nested_children[] = {&ir_scan1};
  ObDASBaseCtDef *nested_wrapper_children[] = {&nested, &delete_scan};

  inv_scan1.ref_table_id_ = TEST_INDEX_TABLE_ID;
  inv_scan2.ref_table_id_ = TEST_INDEX_TABLE_ID;
  ir_scan1.children_ = ir_children1;
  ir_scan1.children_cnt_ = ARRAYSIZEOF(ir_children1);
  ir_scan1.is_async_index_ = true;
  ir_scan1.use_scalar_scan_def_ = true;
  ir_scan2.children_ = ir_children2;
  ir_scan2.children_cnt_ = ARRAYSIZEOF(ir_children2);
  ir_scan2.is_async_index_ = true;
  ir_scan2.use_scalar_scan_def_ = true;
  match1.children_ = match_children1;
  match1.children_cnt_ = ARRAYSIZEOF(match_children1);
  match1.ir_ctdef_idx_ = 0;
  match2.children_ = match_children2;
  match2.children_cnt_ = ARRAYSIZEOF(match_children2);
  match2.ir_ctdef_idx_ = 0;
  root.children_ = root_children;
  root.children_cnt_ = ARRAYSIZEOF(root_children);

  ObDASAsyncIndexDomainAnalysis analysis;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(&root, analysis));
  ASSERT_EQ(ASYNC_INDEX_DOMAIN_SINGLE, analysis.type_);
  ASSERT_EQ(1, analysis.semantic_branch_counts_.count());
  EXPECT_EQ(TEST_INDEX_TABLE_ID,
            analysis.semantic_branch_counts_.at(0).index_table_id_);
  EXPECT_EQ(2, analysis.semantic_branch_counts_.at(0).branch_count_);
  EXPECT_EQ(ASYNC_INDEX_EXEC_GROUP, analysis.get_exec_mode());

  root.children_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(&root, analysis));
  ASSERT_EQ(ASYNC_INDEX_DOMAIN_SINGLE, analysis.type_);
  ASSERT_EQ(1, analysis.semantic_branch_counts_.count());
  EXPECT_EQ(1, analysis.semantic_branch_counts_.at(0).branch_count_);
  EXPECT_EQ(ASYNC_INDEX_EXEC_LEGACY, analysis.get_exec_mode());

  nested.children_ = nested_children;
  nested.children_cnt_ = ARRAYSIZEOF(nested_children);
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(&nested, analysis));
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_NON_ALIGNABLE, analysis.type_);
  EXPECT_EQ(ASYNC_INDEX_EXEC_LEGACY, analysis.get_exec_mode());

  delete_scan.ref_table_id_ = 7001;
  nested_wrapper.children_ = nested_wrapper_children;
  nested_wrapper.children_cnt_ = ARRAYSIZEOF(nested_wrapper_children);
  nested_wrapper.set_must(0, 1);
  nested_wrapper.set_must_not(1, 1);
  nested_wrapper.set_is_async(true);
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::analyze_async_index_domain(
                &nested_wrapper, analysis));
  EXPECT_EQ(ASYNC_INDEX_DOMAIN_NON_ALIGNABLE, analysis.type_);
  EXPECT_TRUE(analysis.has_async_fts_wrapper_);
  EXPECT_EQ(ASYNC_INDEX_EXEC_LEGACY, analysis.get_exec_mode());
}

TEST_F(TestDASAsyncSegmentMerge, collect_async_fts_wrapper_accepts_match_semantic_root)
{
  ObDASScalarScanCtDef inv_scan(allocator_);
  ObDASIRScanCtDef ir_scan(allocator_);
  ObDASMatchCtDef match(allocator_);
  ObDASScalarScanCtDef delete_scan(allocator_);
  ObDASBooleanQueryCtDef async_wrapper(allocator_);
  ObDASBaseCtDef *ir_children[] = {&inv_scan};
  ObDASBaseCtDef *match_children[] = {&ir_scan};
  ObDASBaseCtDef *wrapper_children[] = {&match, &delete_scan};

  inv_scan.ref_table_id_ = TEST_INDEX_TABLE_ID;
  ir_scan.children_ = ir_children;
  ir_scan.children_cnt_ = ARRAYSIZEOF(ir_children);
  ir_scan.is_async_index_ = true;
  ir_scan.use_scalar_scan_def_ = true;
  match.children_ = match_children;
  match.children_cnt_ = ARRAYSIZEOF(match_children);
  match.ir_ctdef_idx_ = 0;
  delete_scan.ref_table_id_ = 7001;
  async_wrapper.children_ = wrapper_children;
  async_wrapper.children_cnt_ = ARRAYSIZEOF(wrapper_children);
  async_wrapper.set_must(0, 1);
  async_wrapper.set_must_not(1, 1);
  async_wrapper.set_is_async(true);

  ObSEArray<uint64_t, 1> table_ids;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::collect_async_index_table_ids(
                &async_wrapper, table_ids));
  ASSERT_EQ(1, table_ids.count());
  EXPECT_EQ(TEST_INDEX_TABLE_ID, table_ids.at(0));
}

TEST_F(TestDASAsyncSegmentMerge, sync_scalar_fallback_still_collects_wrapped_async_catalog)
{
  ObDASScalarScanCtDef async_scalar(allocator_);
  ObDASScalarScanCtDef delete_scan(allocator_);
  ObDASScalarScanCtDef sync_scalar(allocator_);
  ObDASBooleanQueryCtDef async_wrapper(allocator_);
  ObDASBooleanQueryCtDef root(allocator_);
  ObDASBaseCtDef *wrapper_children[] = {&async_scalar, &delete_scan};
  ObDASBaseCtDef *root_children[] = {&async_wrapper, &sync_scalar};
  async_scalar.ref_table_id_ = TEST_INDEX_TABLE_ID;
  async_scalar.is_async_index_ = true;
  delete_scan.ref_table_id_ = 7001;
  sync_scalar.ref_table_id_ = TEST_INDEX_TABLE_ID_2;
  async_wrapper.children_ = wrapper_children;
  async_wrapper.children_cnt_ = ARRAYSIZEOF(wrapper_children);
  async_wrapper.set_must(0, 1);
  async_wrapper.set_must_not(1, 1);
  async_wrapper.set_is_async(true);
  root.children_ = root_children;
  root.children_cnt_ = ARRAYSIZEOF(root_children);

  ObSEArray<uint64_t, 1> table_ids;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::collect_async_index_table_ids(
                &root, table_ids));
  ASSERT_EQ(1, table_ids.count());
  EXPECT_EQ(TEST_INDEX_TABLE_ID, table_ids.at(0));
  uint64_t delete_table_id = OB_INVALID_ID;
  ASSERT_EQ(OB_SUCCESS,
            ObDASAsyncIndexQueryCtx::collect_async_delete_table_id(
                &root, delete_table_id));
  EXPECT_EQ(7001, delete_table_id);
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_equality_predicate_keeps_ror)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObIDASSearchOp *root = nullptr;
  build_catalog(catalog, {1});
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_rowkey_order_scan_ = true;
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 3));

  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, root->get_op_type());
  EXPECT_EQ(ObDASAsyncSegmentMergeMode::SORT_MERGE,
            static_cast<ObDASAsyncSegmentMergeOp *>(root)->mode_);
  ASSERT_EQ(1, root->get_children_cnt());
  ASSERT_NE(nullptr, root->get_children()[0]);
  EXPECT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR,
            root->get_children()[0]->get_op_type());
}

TEST_F(TestDASAsyncSegmentMerge, group_scoped_single_segment_scalar_is_direct)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObIDASSearchOp *root = nullptr;
  build_catalog(catalog, {1});
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_rowkey_order_scan_ = true;
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 3));

  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(
      ObDASSearchCost(1), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR, root->get_op_type());
  ObDASAsyncScalarRorOp *ror = static_cast<ObDASAsyncScalarRorOp *>(root);
  EXPECT_EQ(TEST_INDEX_TABLE_ID, ror->index_table_id_);
  EXPECT_EQ(0, ror->group_slot_);
  EXPECT_TRUE(ror->direct_bind_);
}

TEST_F(TestDASAsyncSegmentMerge, group_scoped_single_segment_range_direct_binds_leaf)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObIDASSearchOp *root = nullptr;
  build_catalog(catalog, {1});
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 4));

  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(
      ObDASSearchCost(1), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_BITMAP, root->get_op_type());
  ASSERT_EQ(1, root->get_children_cnt());
  ASSERT_NE(nullptr, root->get_children()[0]);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_RANGE,
            root->get_children()[0]->get_op_type());
  ObDASAsyncScalarRangeOp *range_op =
      static_cast<ObDASAsyncScalarRangeOp *>(root->get_children()[0]);
  EXPECT_TRUE(range_op->direct_bind_);
  EXPECT_EQ(0, range_op->group_slot_);
  EXPECT_EQ(TEST_INDEX_TABLE_ID, range_op->index_table_id_);
}

TEST_F(TestDASAsyncSegmentMerge, claimed_multi_segment_range_uses_only_outer_sort_merge)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObSEArray<ObIDASSearchOp *, 2> segment_ops;
  build_multi_index_catalog(catalog, {{2, 1}}, {false});
  ASSERT_EQ(2, catalog.get_max_segment_cnt(0, TEST_INDEX_TABLE_ID));
  search_ctx_.set_async_binding(TEST_INDEX_TABLE_ID, 0, false);
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 4));

  for (int64_t i = 0; i < 2; ++i) {
    ObIDASSearchOp *segment_op = nullptr;
    ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(
        ObDASSearchCost(1), search_ctx_, segment_op));
    ASSERT_NE(nullptr, segment_op);
    ASSERT_EQ(DAS_SEARCH_OP_BITMAP, segment_op->get_op_type());
    ASSERT_EQ(1, segment_op->get_children_cnt());
    ASSERT_NE(nullptr, segment_op->get_children()[0]);
    EXPECT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_RANGE,
              segment_op->get_children()[0]->get_op_type());
    ObDASAsyncScalarRangeOp *range_op =
        static_cast<ObDASAsyncScalarRangeOp *>(segment_op->get_children()[0]);
    EXPECT_FALSE(range_op->direct_bind_);
    ASSERT_EQ(OB_SUCCESS, segment_ops.push_back(segment_op));
  }

  ObDASAsyncSegmentMergeOpParam merge_param(
      segment_ops, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  ObDASAsyncSegmentMergeOp *outer_merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(merge_param, outer_merge));
  ASSERT_NE(nullptr, outer_merge);
  EXPECT_EQ(ObDASAsyncSegmentMergeMode::SORT_MERGE, outer_merge->mode_);
  ASSERT_EQ(2, outer_merge->get_children_cnt());
  ASSERT_EQ(OB_SUCCESS, outer_merge->load_current_partition_segments());
  EXPECT_EQ(2, outer_merge->segment_ranges_.count());
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_multi_point_predicate_keeps_ror)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[2][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[2][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[2];
  ObIDASSearchOp *root = nullptr;
  build_catalog(catalog, {1});
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  // The union is not globally ordered, but every range is still a term point lookup.
  ctdef.is_rowkey_order_scan_ = false;
  ctdef.is_index_term_point_scan_ = true;
  for (int64_t range_idx = 0; range_idx < ARRAYSIZEOF(ranges); ++range_idx) {
    ASSERT_EQ(OB_SUCCESS, append_async_scalar_range(rtdef, ranges[range_idx],
        start_objs[range_idx], end_objs[range_idx], range_idx + 3, range_idx + 3));
  }

  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, root->get_op_type());
  EXPECT_EQ(ObDASAsyncSegmentMergeMode::SORT_MERGE,
            static_cast<ObDASAsyncSegmentMergeOp *>(root)->mode_);
  ASSERT_EQ(1, root->get_children_cnt());
  ObIDASSearchOp *range_union = root->get_children()[0];
  ASSERT_NE(nullptr, range_union);
  ASSERT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, range_union->get_op_type());
  ASSERT_EQ(ARRAYSIZEOF(ranges), range_union->get_children_cnt());
  for (int64_t i = 0; i < range_union->get_children_cnt(); ++i) {
    ASSERT_NE(nullptr, range_union->get_children()[i]);
    EXPECT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR,
              range_union->get_children()[i]->get_op_type());
  }
}

void TestDASAsyncSegmentMerge::check_bmm_input_after_config_change(
    ObIDASSearchOp *filter_op, const ObDASBMMBuildOptions &options)
{
  AsyncTermsDirectDrainConfigGuard changed_config(!options.enable_vectorized_merge_);
  ASSERT_TRUE(changed_config.is_valid());
  MockSegmentOp token(search_ctx_, {10}, {2.0});
  MockSegmentOpParam token_param;
  ASSERT_EQ(OB_SUCCESS, token.init(token_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
  ObDASBMMOpParam bmm_param(score_ops, filter_op, allocator_, options);
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
  EXPECT_EQ(options.enable_vectorized_merge_, bmm.enable_vec_merge_);
  EXPECT_EQ(options.filter_mode_, bmm.filter_mode_);
  EXPECT_EQ(filter_op, bmm.get_filter_op());
  ASSERT_EQ(OB_SUCCESS, bmm.close());
}

void TestDASAsyncSegmentMerge::check_async_terms_bmm_direct_selection(
    const bool new_false_range)
{
  ObDASBMMBuildOptions bmm_options(false);
  bool has_bmm_options = false;
  const int add_ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(
      OB_SYS_TENANT_ID);
  ASSERT_TRUE(OB_SUCCESS == add_ret || OB_ENTRY_EXIST == add_ret);
  AsyncTermsDirectDrainConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());

  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarCtDef scalar_ctdef(allocator_);
  ObDASScalarRtDef scalar_rtdef;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[2][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[2][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[2];
  ObDASBaseCtDef *ctdef_children[] = {&ctdef};
  ObDASBaseRtDef *rtdef_children[] = {&rtdef};
  build_catalog(catalog, {2});
  ASSERT_EQ(2, catalog.get_max_group_cnt());
  ASSERT_EQ(1, catalog.get_max_segment_cnt(0, TEST_INDEX_TABLE_ID));
  search_ctx_.set_async_group_slot(0);
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_rowkey_order_scan_ = false;
  ctdef.is_index_term_point_scan_ = true;
  ctdef.enable_new_false_range_ = new_false_range;
  for (int64_t range_idx = 0; range_idx < ARRAYSIZEOF(ranges); ++range_idx) {
    ASSERT_EQ(OB_SUCCESS, append_async_scalar_range(
        rtdef, ranges[range_idx], start_objs[range_idx], end_objs[range_idx],
        range_idx + 3, range_idx + 3));
  }
  scalar_ctdef.children_ = ctdef_children;
  scalar_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  scalar_ctdef.set_has_index_scan(true);
  scalar_rtdef.ctdef_ = &scalar_ctdef;
  scalar_rtdef.children_ = rtdef_children;
  scalar_rtdef.children_cnt_ = ARRAYSIZEOF(rtdef_children);
  rtdef.cost_ = ObDASSearchCost(ARRAYSIZEOF(ranges));

  ObDASBooleanQueryRtDef filter_builder;
  ObIDASSearchRtDef *clauses[] = {&scalar_rtdef};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(
      clauses, ARRAYSIZEOF(clauses));
  ObDASSearchCost filter_cost;

  search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObIDASSearchOp *direct_root = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
      filter_clauses, ObDASSearchCost(11), search_ctx_, direct_root,
      filter_cost, false /* query_optional */, bmm_options, has_bmm_options));
  ASSERT_NE(nullptr, direct_root);
  EXPECT_TRUE(has_bmm_options);
  EXPECT_TRUE(bmm_options.enable_vectorized_merge_);
  EXPECT_EQ(ARRAYSIZEOF(ranges), filter_cost.cost());
  ASSERT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, direct_root->get_op_type());
  EXPECT_TRUE(direct_root->supports_direct_rowid_drain());
  EXPECT_FALSE(direct_root->can_enumerate_rowids());
  EXPECT_EQ(ARRAYSIZEOF(ranges), direct_root->get_children_cnt());
  check_bmm_input_after_config_change(direct_root, bmm_options);

  search_ctx_.set_async_group_slot(-1);
  filter_cost.reset();
  ObIDASSearchOp *multi_segment_root = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
      filter_clauses, ObDASSearchCost(11), search_ctx_, multi_segment_root,
      filter_cost, false /* query_optional */, bmm_options, has_bmm_options));
  ASSERT_NE(nullptr, multi_segment_root);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, multi_segment_root->get_op_type());
  ObDASAsyncSegmentMergeOp *direct_merge =
      static_cast<ObDASAsyncSegmentMergeOp *>(multi_segment_root);
  EXPECT_EQ(ObDASAsyncSegmentMergeMode::DIRECT_MERGE, direct_merge->mode_);
  EXPECT_TRUE(direct_merge->supports_direct_rowid_drain());
  ASSERT_EQ(2, direct_merge->get_children_cnt());
  for (int64_t i = 0; i < direct_merge->get_children_cnt(); ++i) {
    ASSERT_NE(nullptr, direct_merge->get_children()[i]);
    EXPECT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER,
              direct_merge->get_children()[i]->get_op_type());
    EXPECT_TRUE(direct_merge->get_children()[i]->supports_direct_rowid_drain());
  }
  ASSERT_TRUE(has_bmm_options);
  EXPECT_TRUE(bmm_options.enable_vectorized_merge_);
  check_bmm_input_after_config_change(multi_segment_root, bmm_options);

  search_ctx_.set_async_group_slot(0);
  search_ctx_.table_row_count_ = ObDASSearchCost(1000000);
  filter_cost.reset();
  ObIDASSearchOp *fallback_root = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
      filter_clauses, ObDASSearchCost(1), search_ctx_, fallback_root,
      filter_cost, false /* query_optional */, bmm_options, has_bmm_options));
  ASSERT_NE(nullptr, fallback_root);
  EXPECT_TRUE(has_bmm_options);
  EXPECT_EQ(ObDASBMMBuildOptions::FilterMode::ORDERED, bmm_options.filter_mode_);
  EXPECT_EQ(ARRAYSIZEOF(ranges), filter_cost.cost());
  ASSERT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, fallback_root->get_op_type());
  EXPECT_FALSE(fallback_root->supports_direct_rowid_drain());
  EXPECT_TRUE(fallback_root->can_enumerate_rowids());
  EXPECT_EQ(ARRAYSIZEOF(ranges), fallback_root->get_children_cnt());

  search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  ObIDASSearchOp *optional_root = nullptr;
  ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
      filter_clauses, ObDASSearchCost(11), search_ctx_, optional_root,
      filter_cost, true /* query_optional */, bmm_options, has_bmm_options));
  ASSERT_NE(nullptr, optional_root);
  EXPECT_FALSE(optional_root->supports_direct_rowid_drain());
  EXPECT_TRUE(optional_root->can_enumerate_rowids());
  EXPECT_TRUE(has_bmm_options);
  EXPECT_EQ(ObDASBMMBuildOptions::FilterMode::ORDERED, bmm_options.filter_mode_);

  {
    AsyncTermsDirectDrainConfigGuard disabled_guard(false);
    ASSERT_TRUE(disabled_guard.is_valid());
    ObIDASSearchOp *disabled_root = nullptr;
    ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
        filter_clauses, ObDASSearchCost(11), search_ctx_, disabled_root,
        filter_cost, false /* query_optional */, bmm_options, has_bmm_options));
    ASSERT_NE(nullptr, disabled_root);
    ASSERT_TRUE(has_bmm_options);
    EXPECT_FALSE(bmm_options.enable_vectorized_merge_);
    EXPECT_FALSE(disabled_root->supports_direct_rowid_drain());
    EXPECT_TRUE(disabled_root->can_enumerate_rowids());
    check_bmm_input_after_config_change(disabled_root, bmm_options);
  }

  if (!new_false_range) {
    // The leaf's capabilities are independent of the BMM consumer's mode.
    rtdef.key_ranges_.reuse();
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[0]));
    filter_cost.reset();
    ObIDASSearchOp *optional_leaf = nullptr;
    ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
        filter_clauses, ObDASSearchCost(11), search_ctx_, optional_leaf,
        filter_cost, true /* query_optional */, bmm_options, has_bmm_options));
    ASSERT_NE(nullptr, optional_leaf);
    EXPECT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR, optional_leaf->get_op_type());
    EXPECT_TRUE(optional_leaf->supports_direct_rowid_drain());
    EXPECT_TRUE(optional_leaf->can_enumerate_rowids());
    EXPECT_TRUE(has_bmm_options);
    MockSegmentOp optional_token(search_ctx_, {10}, {2.0});
    MockSegmentOpParam optional_token_param;
    ASSERT_EQ(OB_SUCCESS, optional_token.init(optional_token_param));
    ObSEArray<ObIDASSearchOp *, 1> optional_score_ops;
    ASSERT_EQ(OB_SUCCESS, optional_score_ops.push_back(&optional_token));
    ObDASBMMBuildOptions optional_bmm_param_options = bmm_options;
    optional_bmm_param_options.query_optional_ = true;
    ObDASBMMOpParam optional_bmm_param(
        optional_score_ops, optional_leaf, allocator_, optional_bmm_param_options);
    EXPECT_TRUE(optional_bmm_param.is_valid());
    ObDASBMMOp optional_bmm(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, optional_bmm.init(optional_bmm_param));
    EXPECT_EQ(ObDASBMMBuildOptions::FilterMode::ORDERED, optional_bmm.filter_mode_);
    ASSERT_EQ(OB_SUCCESS, optional_bmm.close());

    {
      AsyncTermsDirectDrainConfigGuard disabled_guard(false);
      ASSERT_TRUE(disabled_guard.is_valid());
      filter_cost.reset();
      ObIDASSearchOp *disabled_leaf = nullptr;
      ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
          filter_clauses, ObDASSearchCost(11), search_ctx_, disabled_leaf,
          filter_cost, false /* query_optional */, bmm_options, has_bmm_options));
      ASSERT_NE(nullptr, disabled_leaf);
      EXPECT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR, disabled_leaf->get_op_type());
      EXPECT_TRUE(disabled_leaf->supports_direct_rowid_drain());
      EXPECT_TRUE(disabled_leaf->can_enumerate_rowids());
      ASSERT_TRUE(has_bmm_options);
      EXPECT_FALSE(bmm_options.enable_vectorized_merge_);
      MockSegmentOp disabled_token(search_ctx_, {10}, {2.0});
      MockSegmentOpParam disabled_token_param;
      ASSERT_EQ(OB_SUCCESS, disabled_token.init(disabled_token_param));
      ObSEArray<ObIDASSearchOp *, 1> disabled_score_ops;
      ASSERT_EQ(OB_SUCCESS, disabled_score_ops.push_back(&disabled_token));
      ObDASBMMOpParam disabled_bmm_param(disabled_score_ops, disabled_leaf, allocator_, bmm_options);
      EXPECT_TRUE(disabled_bmm_param.is_valid());
      ObDASBMMOp disabled_bmm(search_ctx_);
      ASSERT_EQ(OB_SUCCESS, disabled_bmm.init(disabled_bmm_param));
      EXPECT_EQ(ObDASBMMBuildOptions::FilterMode::ORDERED, disabled_bmm.filter_mode_);
      ASSERT_EQ(OB_SUCCESS, disabled_bmm.close());
    }
  }

  // A single Term uses the same direct consumer path. An initially empty/new
  // domain retains the enumerable placeholder until a concrete Term appears.
  for (int64_t range_count = 1; range_count >= (new_false_range ? 0 : 1); --range_count) {
    rtdef.key_ranges_.reuse();
    if (range_count > 0) {
      ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[0]));
    }
    ObIDASSearchOp *small_root = nullptr;
    ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
        filter_clauses, ObDASSearchCost(11), search_ctx_, small_root,
        filter_cost, false /* query_optional */, bmm_options, has_bmm_options));
    ASSERT_NE(nullptr, small_root);
    EXPECT_TRUE(has_bmm_options);
    EXPECT_EQ(range_count > 0 ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN
        : ObDASBMMBuildOptions::FilterMode::ORDERED, bmm_options.filter_mode_);
    EXPECT_EQ(range_count > 0, small_root->supports_direct_rowid_drain());
    EXPECT_EQ(!new_false_range || 0 == range_count, small_root->can_enumerate_rowids());
    if (!new_false_range) {
      EXPECT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR, small_root->get_op_type());
      EXPECT_EQ(0, small_root->get_children_cnt());
      EXPECT_EQ(ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN, bmm_options.filter_mode_);
      check_bmm_input_after_config_change(small_root, bmm_options);
    } else if (range_count > 0) {
      EXPECT_EQ(1, small_root->get_children_cnt());
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, async_terms_bmm_cost_selects_direct_in_single_segment_group)
{
  check_async_terms_bmm_direct_selection(false);
}

TEST_F(TestDASAsyncSegmentMerge, async_terms_new_query_range_selects_direct)
{
  check_async_terms_bmm_direct_selection(true);
}

void TestDASAsyncSegmentMerge::check_async_empty_filter_bmm()
{
  const int add_ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(
      OB_SYS_TENANT_ID);
  ASSERT_TRUE(OB_SUCCESS == add_ret || OB_ENTRY_EXIST == add_ret);
  AsyncTermsDirectDrainConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());

  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{0, 1}, {0, 1}}, {false, false});
  ASSERT_EQ(1, catalog.get_max_group_cnt());
  ASSERT_EQ(0, catalog.get_max_segment_cnt(0, TEST_INDEX_TABLE_ID));
  ASSERT_EQ(1, catalog.get_max_segment_cnt(0, TEST_INDEX_TABLE_ID_2));
  search_ctx_.set_async_binding(common::OB_INVALID_ID, 0, false);
  share::SCN snapshot_scn;
  transaction::ObTxReadSnapshot snapshot;
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(100));
  snapshot.init_special_read(snapshot_scn);
  search_ctx_.snapshot_ = &snapshot;
  search_ctx_.set_ls_id(ObLSID(1001));
  ASSERT_EQ(DAS_SCOPE_BASE, search_ctx_.get_op_creating_scope());
  ASSERT_EQ(DAS_ROWID_TYPE_UINT64, search_ctx_.get_rowid_type());
  ASSERT_FALSE(search_ctx_.is_logical_table());

  ObDASScalarCtDef scalar_ctdef(allocator_);
  ObDASScalarRtDef scalar_rtdef;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObPushdownOperator pd_op(eval_ctx_, ctdef.pd_expr_spec_);
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObDASBaseCtDef *ctdef_children[] = {&ctdef};
  ObDASBaseRtDef *rtdef_children[] = {&rtdef};
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  rtdef.p_pd_expr_op_ = &pd_op;
  ASSERT_EQ(OB_SUCCESS, scan_op_.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.set_related_task_info(
      &ctdef, &rtdef, ObTabletID(10001)));
  ctdef.is_index_term_point_scan_ = true;
  ctdef.enable_new_false_range_ = false;
  ASSERT_FALSE(ctdef.is_scoring());
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 3));
  ASSERT_EQ(1, rtdef.key_ranges_.count());
  scalar_ctdef.children_ = ctdef_children;
  scalar_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  scalar_ctdef.set_has_index_scan(true);
  scalar_rtdef.ctdef_ = &scalar_ctdef;
  scalar_rtdef.children_ = rtdef_children;
  scalar_rtdef.children_cnt_ = ARRAYSIZEOF(rtdef_children);
  rtdef.cost_ = ObDASSearchCost(1);
  search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  const ObDASSearchCost lead_cost(11);
  ASSERT_TRUE(scalar_rtdef.can_build_direct_rowid_drain(search_ctx_));
  ObDASBMMBuildOptions options(false);
  ObDASBMMOp::select_filter_build_options(search_ctx_, lead_cost, rtdef.cost_,
      false /* query_optional */, true /* supports_direct_drain */, options);
  ASSERT_TRUE(options.enable_vectorized_merge_);
  ASSERT_EQ(ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN, options.filter_mode_);

  ObDASBooleanQueryRtDef filter_builder;
  ObIDASSearchRtDef *clauses[] = {&scalar_rtdef};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
  ObIDASSearchOp *filter_op = nullptr;
  ObDASSearchCost filter_cost;
  bool has_options = false;
  const int64_t op_count = search_ctx_.op_store_.count();
  ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(filter_clauses, lead_cost,
      search_ctx_, filter_op, filter_cost, false /* query_optional */, options, has_options));
  ASSERT_TRUE(has_options);
  ASSERT_TRUE(options.enable_vectorized_merge_);
  EXPECT_EQ(ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN, options.filter_mode_);
  ASSERT_EQ(1, filter_cost.cost());
  ASSERT_NE(nullptr, filter_op);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR, filter_op->get_op_type());
  ASSERT_EQ(0, filter_op->get_children_cnt());
  EXPECT_EQ(op_count + 1, search_ctx_.op_store_.count());
  EXPECT_TRUE(filter_op->can_enumerate_rowids());
  EXPECT_TRUE(filter_op->supports_direct_rowid_drain());

  MockSegmentOp scoring_op(search_ctx_, {10}, {2.0});
  MockSegmentOpParam scoring_param;
  scoring_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, scoring_op.init(scoring_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ObSEArray<ObDASSearchCost, 1> score_costs;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
  ASSERT_EQ(OB_SUCCESS, score_costs.push_back(lead_cost));
  ObDASBMMBuildOptions bmm_param_options = options;
  bmm_param_options.query_optional_ = false;
  bmm_param_options.lead_cost_ = lead_cost;
  bmm_param_options.filter_cost_ = filter_cost;
  ObDASBMMOpParam bmm_param(
      score_ops, filter_op, allocator_, bmm_param_options, &score_costs);
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
  EXPECT_TRUE(bmm.enable_vec_merge_);
  EXPECT_EQ(ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN, bmm.filter_mode_);
  EXPECT_EQ(filter_op, bmm.get_filter_op());
  // No SSTable handles are populated: physical scans would fail.
  ASSERT_EQ(OB_SUCCESS, bmm.open());
  EXPECT_EQ(1, scoring_op.open_count());
  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  EXPECT_EQ(1, scoring_op.rescan_count());
  EXPECT_EQ(OB_ITER_END, bmm.next_rowid(rowid, score));
  ASSERT_EQ(OB_SUCCESS, bmm.close());
  search_ctx_.snapshot_ = nullptr;
}

TEST_F(TestDASAsyncSegmentMerge, async_empty_scalar_filter_bmm_keeps_direct_drain)
{
  check_async_empty_filter_bmm();
}

TEST_F(TestDASAsyncSegmentMerge, async_bmm_direct_selection_preserves_candidate_filter_cost_strategy)
{
  const int add_ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(
      OB_SYS_TENANT_ID);
  ASSERT_TRUE(OB_SUCCESS == add_ret || OB_ENTRY_EXIST == add_ret);
  AsyncTermsDirectDrainConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());

  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  ObDASScalarCtDef scalar_ctdef(allocator_);
  ObDASScalarRtDef scalar_rtdef;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_index_term_point_scan_ = true;
  ObDASBaseCtDef *ctdef_children[] = {&ctdef};
  ObDASBaseRtDef *rtdef_children[] = {&rtdef};
  scalar_ctdef.children_ = ctdef_children;
  scalar_ctdef.children_cnt_ = ARRAYSIZEOF(ctdef_children);
  scalar_ctdef.set_has_index_scan(true);
  scalar_rtdef.ctdef_ = &scalar_ctdef;
  scalar_rtdef.children_ = rtdef_children;
  scalar_rtdef.children_cnt_ = ARRAYSIZEOF(rtdef_children);
  ObObj start_objs[2][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[2][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[2];
  for (int64_t i = 0; i < ARRAYSIZEOF(ranges); ++i) {
    prepare_async_scalar_range(ranges[i], start_objs[i], end_objs[i], i + 3, i + 3);
  }
  ObDASBooleanQueryRtDef filter_builder;
  ObIDASSearchRtDef *clauses[] = {&scalar_rtdef};
  ObBooleanSubClause<ObIDASSearchRtDef> filter_clauses(clauses, ARRAYSIZEOF(clauses));
  search_ctx_.table_row_count_ = ObDASSearchCost(1000);
  const ObDASSearchCost lead_cost(20);
  const int64_t filter_costs[] = {61, 62, 900};

  for (int64_t group_slot = -1; group_slot <= 0; ++group_slot) {
    search_ctx_.set_async_group_slot(group_slot);
    for (int64_t new_false_range = 0; new_false_range < 2; ++new_false_range) {
      ctdef.enable_new_false_range_ = new_false_range;
      rtdef.key_ranges_.reuse();
      for (int64_t range_idx = 0; range_idx < ARRAYSIZEOF(ranges); ++range_idx) {
        ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[range_idx]));
        for (int64_t cost : filter_costs) {
          SCOPED_TRACE(::testing::Message() << "group=" << group_slot
              << " new_false_range=" << new_false_range
              << " ranges=" << rtdef.key_ranges_.count() << " filter_cost=" << cost);
          scalar_rtdef.cost_.reset();
          rtdef.cost_ = ObDASSearchCost(cost);
          ASSERT_TRUE(scalar_rtdef.can_build_direct_rowid_drain(search_ctx_));
          ObIDASSearchOp *filter_op = nullptr;
          ObDASSearchCost filter_cost;
          ObDASBMMBuildOptions options(false);
          bool has_options = false;
          ASSERT_EQ(OB_SUCCESS, filter_builder.prepare_bmm_input(
              filter_clauses, lead_cost, search_ctx_, filter_op, filter_cost,
              false /* query_optional */, options, has_options));
          ASSERT_NE(nullptr, filter_op);
          ASSERT_TRUE(has_options);
          ASSERT_TRUE(options.enable_vectorized_merge_);
          ASSERT_EQ(cost, filter_cost.cost());
          const bool expect_filter_after_score = cost >= 62;
          EXPECT_EQ(expect_filter_after_score
              ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN_AFTER_ESSENTIAL
              : ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN, options.filter_mode_);
          EXPECT_TRUE(filter_op->supports_direct_rowid_drain());
          EXPECT_EQ(0 == group_slot && !new_false_range && 1 == rtdef.key_ranges_.count(),
              filter_op->can_enumerate_rowids());

          MockSegmentOp token(search_ctx_, {10}, {2.0});
          MockSegmentOpParam token_param;
          token_param.set_is_scoring(true);
          ASSERT_EQ(OB_SUCCESS, token.init(token_param));
          ObSEArray<ObIDASSearchOp *, 1> score_ops;
          ObSEArray<ObDASSearchCost, 1> score_costs;
          ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&token));
          ASSERT_EQ(OB_SUCCESS, score_costs.push_back(lead_cost));
          ObDASBMMBuildOptions bmm_param_options = options;
          bmm_param_options.query_optional_ = false;
          bmm_param_options.lead_cost_ = lead_cost;
          bmm_param_options.filter_cost_ = filter_cost;
          ObDASBMMOpParam bmm_param(
              score_ops, filter_op, allocator_, bmm_param_options, &score_costs);
          ObDASBMMOp bmm(search_ctx_);
          ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
          EXPECT_EQ(options.filter_mode_, bmm.filter_mode_);
          EXPECT_EQ(expect_filter_after_score, bmm.filter_after_score_);
          EXPECT_EQ(search_ctx_.max_batch_size() * (expect_filter_after_score ? 64 : 16),
              bmm.doc_score_accumulator_.get_capacity());
          ASSERT_EQ(OB_SUCCESS, bmm.close());
        }
      }
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, async_terms_new_query_range_direct_rescans)
{
  const int add_ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
  ASSERT_TRUE(OB_SUCCESS == add_ret || OB_ENTRY_EXIST == add_ret);
  AsyncTermsDirectDrainConfigGuard config_guard;
  ASSERT_TRUE(config_guard.is_valid());
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1});
  search_ctx_.set_async_group_slot(0);
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  ObObj start_objs[4][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[4][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[4];
  const int64_t terms[] = {1, 2, 0, 4};
  for (int64_t i = 0; i < ARRAYSIZEOF(ranges); ++i) {
    prepare_async_scalar_range(ranges[i], start_objs[i], end_objs[i], terms[i], terms[i]);
    if (i < 2) {
      ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[i]));
    }
  }
  ObIDASSearchOp *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(
      ObDASSearchCost(11), search_ctx_, true /* allow_direct_drain */, root));
  ASSERT_NE(nullptr, root);
  ASSERT_TRUE(root->supports_direct_rowid_drain());
  ASSERT_FALSE(root->can_enumerate_rowids());
  ObDASAsyncScalarRorUnionOp *range_union = static_cast<ObDASAsyncScalarRorUnionOp *>(root);
  ASSERT_EQ(2, root->get_children_cnt());

  // Exercise real child expansion and binding before substituting storage.
  for (int64_t i = 2; i < ARRAYSIZEOF(ranges); ++i) {
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[i]));
  }
  ASSERT_EQ(OB_SUCCESS, range_union->ensure_range_children());
  ASSERT_EQ(4, root->get_children_cnt());
  MockRuntimeRangeRorOp child_ops[] = {
      MockRuntimeRangeRorOp(search_ctx_), MockRuntimeRangeRorOp(search_ctx_),
      MockRuntimeRangeRorOp(search_ctx_), MockRuntimeRangeRorOp(search_ctx_)};
  for (int64_t i = 0; i < root->get_children_cnt(); ++i) {
    ObDASAsyncScalarRorOp *leaf = static_cast<ObDASAsyncScalarRorOp *>(root->get_children()[i]);
    EXPECT_EQ(i, leaf->range_idx_);
    EXPECT_EQ(0, leaf->group_slot_);
    EXPECT_TRUE(leaf->direct_bind_);
    EXPECT_TRUE(leaf->supports_direct_rowid_drain());
    ASSERT_EQ(OB_SUCCESS, leaf->init_term_key());
    EXPECT_FALSE(leaf->is_empty_range_);
    ObDASAsyncScalarRorOpParam param(
        &ctdef, &rtdef, i, TEST_INDEX_TABLE_ID, 0, true);
    param.set_is_scoring(false);
    ASSERT_EQ(OB_SUCCESS, child_ops[i].init(param));
    root->get_children()[i] = &child_ops[i];
  }

  const int64_t range_counts[] = {4, 1, 0, 3, 4};
  for (int64_t round = 0; round < ARRAYSIZEOF(range_counts); ++round) {
    SCOPED_TRACE(round);
    if (round == 4) {
      prepare_async_scalar_range(ranges[0], start_objs[0], end_objs[0], 5, 5);
    }
    rtdef.key_ranges_.reuse();
    for (int64_t i = 0; i < range_counts[round]; ++i) {
      ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[i]));
    }
    ASSERT_EQ(OB_SUCCESS, round == 0 ? root->open() : root->rescan());
    EXPECT_FALSE(range_union->heap_initialized_);
    EXPECT_TRUE(range_union->ready_child_heap_.empty());
    for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
      EXPECT_EQ(i >= range_counts[round], child_ops[i].is_empty_range_);
    }

    ObDASBMMOp bmm(search_ctx_);
    const int64_t capacity = 256;
    ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.init(capacity, allocator_));
    void *filter_buf = allocator_.alloc(ObBitVector::memory_size(capacity));
    ASSERT_NE(nullptr, filter_buf);
    bmm.filter_bitset_ = to_bit_vector(filter_buf);
    bmm.filter_bitset_->init(capacity);
    bmm.filter_op_ = root;
    bmm.filter_mode_ = ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN;
    ObDASRowID first;
    ObDASRowID last;
    first.set_uint64(0);
    last.set_uint64(100);
    bool need_evaluate = false;
    // Exercise BMM's selected consumption mode through its batch entry point.
    ASSERT_EQ(OB_SUCCESS, bmm.prepare_filter_for_vectorized_batch(first, last, need_evaluate));
    EXPECT_EQ(range_counts[round] > 0, need_evaluate);
    EXPECT_TRUE(range_union->heap_initialized_);
    EXPECT_FALSE(range_union->batch_cursor_primed_);
    for (int64_t id = 0; id <= 100; ++id) {
      const bool expected = (id == (round == 4 ? 5 : 1) && range_counts[round] > 0)
          || (id == 2 && range_counts[round] > 1)
          || (id == 4 && range_counts[round] > 3);
      EXPECT_EQ(expected, bmm.filter_bitset_->exist(id)) << id;
    }
    if (range_counts[round] == 0) {
      EXPECT_TRUE(bmm.filter_end_);
    } else {
      ASSERT_TRUE(bmm.curr_filter_id_.is_normal());
      EXPECT_EQ(1000, bmm.curr_filter_id_.get_uint64());
    }
    // Leave lookahead pending across rescan, including the transition to empty.
    allocator_.free(filter_buf);
  }
  CollectingRowIDBatchConsumer consumer(true /* deduplicate */);
  ObDASRowID next;
  ASSERT_EQ(OB_SUCCESS, root->direct_drain_rowids(101, UINT64_MAX, consumer, next));
  EXPECT_TRUE(next.is_max());
  const uint64_t expected[] = {1000, 2002, 2004, 2005};
  ObSEArray<uint64_t, 4> sorted_ids;
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  expect_rowids_equal(sorted_ids, expected, ARRAYSIZEOF(expected));
  EXPECT_EQ(OB_SUCCESS, root->close());
  for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
    EXPECT_EQ(OB_SUCCESS, child_ops[i].close());
  }
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_parameterized_range_does_not_freeze_ror)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObObj rescan_start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj rescan_end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObNewRange rescan_range;
  ObIDASSearchOp *root = nullptr;
  build_catalog(catalog, {1});
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  // v >= :lo AND v <= :hi is structurally a range even when lo == hi once.
  ctdef.is_rowkey_order_scan_ = false;
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 3));

  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_BITMAP, root->get_op_type());
  ASSERT_EQ(1, root->get_children_cnt());
  ObIDASSearchOp *direct = root->get_children()[0];
  ASSERT_NE(nullptr, direct);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, direct->get_op_type());
  EXPECT_EQ(ObDASAsyncSegmentMergeMode::DIRECT_MERGE,
            static_cast<ObDASAsyncSegmentMergeOp *>(direct)->mode_);
  ASSERT_EQ(1, direct->get_children_cnt());
  ASSERT_NE(nullptr, direct->get_children()[0]);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_RANGE,
            direct->get_children()[0]->get_op_type());

  // A rescan reuses the leaf while replacing its runtime boundaries.
  rtdef.key_ranges_.reuse();
  ASSERT_EQ(OB_SUCCESS, append_async_scalar_range(rtdef, rescan_range,
      rescan_start_objs, rescan_end_objs, 3, 4));

  ObDASAsyncScalarRangeOp *range_op =
      static_cast<ObDASAsyncScalarRangeOp *>(direct->get_children()[0]);
  EXPECT_FALSE(range_op->direct_bind_);
  EXPECT_EQ(DAS_SCOPE_BASE, range_op->get_scope());
  const uint64_t *invalid_doc_ids = nullptr;
  int64_t invalid_count = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      range_op->get_next_doc_ids(0, invalid_doc_ids, invalid_count));
  EXPECT_EQ(nullptr, invalid_doc_ids);
  EXPECT_EQ(0, invalid_count);
  ASSERT_EQ(OB_SUCCESS, range_op->init_range_scan_param());
  EXPECT_TRUE(range_op->range_scan_param_.enable_doc_id_drain_);
  ASSERT_EQ(SEARCH_INDEX_KEY_CNT, range_op->term_range_.get_end_key().get_datum_cnt());
  EXPECT_EQ(4, range_op->term_range_.get_end_key().get_datum(SEARCH_INDEX_VALUE).get_int());
  EXPECT_TRUE(range_op->term_range_.get_end_key().get_datum(SEARCH_INDEX_DOC_ID).is_max());

  // An empty runtime range set reuses the same leaf and produces EOF.
  rtdef.key_ranges_.reuse();
  ASSERT_EQ(OB_SUCCESS, range_op->init_range_scan_param());
  EXPECT_FALSE(range_op->has_current_range());
  const uint64_t *doc_ids = nullptr;
  int64_t count = -1;
  EXPECT_EQ(OB_ITER_END,
      range_op->get_next_doc_ids(1, doc_ids, count));
  EXPECT_EQ(nullptr, doc_ids);
  EXPECT_EQ(0, count);
  count = -1;
  EXPECT_EQ(OB_ITER_END, range_op->get_next_rows(count, 1));
  EXPECT_EQ(0, count);
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = 1;
  ASSERT_TRUE(version_range.is_valid());
  EXPECT_EQ(OB_SUCCESS, range_op->do_rescan_segment(version_range));
  EXPECT_EQ(nullptr, range_op->range_iter_);

  // A later nonempty rescan prepares the same leaf again.
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(rescan_range));
  ASSERT_EQ(OB_SUCCESS, range_op->init_range_scan_param());
  EXPECT_TRUE(range_op->has_current_range());
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_multi_range_uses_one_global_bitmap_over_direct_merge)
{
  static const int64_t RANGE_COUNT = 32;
  static const int64_t SEGMENT_COUNT = 8;
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[RANGE_COUNT][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[RANGE_COUNT][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[RANGE_COUNT];
  ObIDASSearchOp *root = nullptr;
  build_catalog(catalog, {SEGMENT_COUNT});
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  for (int64_t range_idx = 0; range_idx < RANGE_COUNT; ++range_idx) {
    prepare_async_scalar_range(ranges[range_idx],
        start_objs[range_idx], end_objs[range_idx], range_idx + 3, range_idx + 4);
  }

  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_BITMAP, root->get_op_type());
  ASSERT_EQ(1, root->get_children_cnt());
  ObIDASSearchOp *direct = root->get_children()[0];
  ASSERT_NE(nullptr, direct);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, direct->get_op_type());
  ObDASAsyncSegmentMergeOp *merge =
      static_cast<ObDASAsyncSegmentMergeOp *>(direct);
  EXPECT_EQ(ObDASAsyncSegmentMergeMode::DIRECT_MERGE, merge->mode_);
  EXPECT_FALSE(merge->supports_direct_rowid_drain());
  EXPECT_TRUE(merge->direct_child_next_rowids_.empty());
  EXPECT_TRUE(merge->direct_active_child_indices_.empty());
  EXPECT_TRUE(merge->can_enumerate_rowids());
  EXPECT_TRUE(root->can_enumerate_rowids());
  ASSERT_EQ(SEGMENT_COUNT, direct->get_children_cnt());
  for (int64_t segment_idx = 0; segment_idx < SEGMENT_COUNT; ++segment_idx) {
    ObIDASSearchOp *range_child = direct->get_children()[segment_idx];
    ASSERT_NE(nullptr, range_child);
    ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_RANGE, range_child->get_op_type());
    EXPECT_FALSE(static_cast<ObDASAsyncScalarRangeOp *>(range_child)->direct_bind_);
  }

  // The materialized Range can be shared by multiple consumers and remains
  // eligible for query-optional vectorized BMM.
  ObDASFilterBitmapProvider *provider = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_filter_bitmap_provider(
      reinterpret_cast<const ObIDASSearchRtDef *>(0x1234),
      DAS_FILTER_BITMAP_QUERY_SCOPE, DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT,
      root, provider));
  ASSERT_NE(nullptr, provider);
  ObDASBitmapOp *first_view = nullptr;
  ObDASBitmapOp *second_view = nullptr;
  ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx_, first_view));
  ASSERT_EQ(OB_SUCCESS, provider->create_view(search_ctx_, second_view));
  ASSERT_NE(nullptr, first_view);
  ASSERT_NE(nullptr, second_view);
  EXPECT_TRUE(first_view->can_enumerate_rowids());
  EXPECT_TRUE(second_view->can_enumerate_rowids());

  MockSegmentOp scoring_op(search_ctx_, {10}, {2.0});
  MockSegmentOpParam scoring_param;
  scoring_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, scoring_op.init(scoring_param));
  ObSEArray<ObIDASSearchOp *, 1> score_ops;
  ASSERT_EQ(OB_SUCCESS, score_ops.push_back(&scoring_op));
  ObDASBMMBuildOptions bmm_options(true /* enable_vectorized_merge */);
  ObDASBMMBuildOptions bmm_param_options = bmm_options;
  bmm_param_options.query_optional_ = true;
  ObDASBMMOpParam bmm_param(score_ops, first_view, allocator_, bmm_param_options);
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, bmm.init(bmm_param));
  EXPECT_TRUE(bmm.enable_vec_merge_);
  EXPECT_TRUE(bmm.can_seek_filter_for_vec_handoff());
  ASSERT_EQ(OB_SUCCESS, bmm.close());

  // One leaf covers any runtime range count; estimates never cap the tree.
  ObDASAsyncScalarRangeOp *range_op = static_cast<ObDASAsyncScalarRangeOp *>(
      direct->get_children()[0]);
  ASSERT_EQ(OB_SUCCESS, range_op->init_range_scan_param());
  EXPECT_FALSE(range_op->has_current_range());
  for (int64_t range_idx = 0; range_idx < RANGE_COUNT; ++range_idx) {
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[range_idx]));
  }
  range_op->current_range_idx_ = RANGE_COUNT - 1;
  ASSERT_EQ(OB_SUCCESS, range_op->init_range_scan_param());
  EXPECT_EQ(RANGE_COUNT + 3, range_op->term_range_.get_end_key()
      .get_datum(SEARCH_INDEX_VALUE).get_int());
}

TEST_F(TestDASAsyncSegmentMerge, nested_multi_segment_range_uses_one_global_lazy_sort)
{
  static const int64_t SEGMENT_COUNT = 3;
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr doc_id_expr;
  ObExpr nested_key_expr;
  ExprFixedArray nested_rowid_exprs(allocator_);
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ObIDASSearchOp *root = nullptr;

  build_catalog(catalog, {SEGMENT_COUNT});
  doc_id_expr.type_ = T_REF_COLUMN;
  doc_id_expr.max_length_ = sizeof(uint64_t);
  doc_id_expr.is_fixed_length_data_ = true;
  doc_id_expr.len_ = sizeof(uint64_t);
  doc_id_expr.datum_meta_.type_ = ObUInt64Type;
  doc_id_expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
  doc_id_expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
      ObUInt64Type, CS_TYPE_BINARY);
  nested_key_expr.type_ = T_REF_COLUMN;
  nested_key_expr.max_length_ = SEARCH_INDEX_NESTED_KEY_LENGTH;
  nested_key_expr.is_fixed_length_data_ = false;
  nested_key_expr.datum_meta_.type_ = ObVarcharType;
  nested_key_expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
  nested_key_expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
      ObVarcharType, CS_TYPE_BINARY);
  ASSERT_NE(nullptr, doc_id_expr.basic_funcs_);
  ASSERT_NE(nullptr, nested_key_expr.basic_funcs_);
  ASSERT_EQ(OB_SUCCESS, nested_rowid_exprs.init(2));
  ASSERT_EQ(OB_SUCCESS, nested_rowid_exprs.push_back(&doc_id_expr));
  ASSERT_EQ(OB_SUCCESS, nested_rowid_exprs.push_back(&nested_key_expr));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.register_nested_scope(&nested_rowid_exprs, DAS_SCOPE_NESTED_ASYNC));

  ctdef.ref_table_id_ = TEST_INDEX_TABLE_ID;
  ctdef.is_async_index_ = true;
  ctdef.enable_new_false_range_ = true;
  ctdef.is_nested_scope_ = true;
  ctdef.nesting_depth_for_full_subtree_ = 2;
  ASSERT_EQ(OB_SUCCESS, ctdef.rowkey_exprs_.init(2));
  ASSERT_EQ(OB_SUCCESS, ctdef.rowkey_exprs_.push_back(&doc_id_expr));
  ASSERT_EQ(OB_SUCCESS, ctdef.rowkey_exprs_.push_back(&nested_key_expr));
  rtdef.ctdef_ = &ctdef;
  ASSERT_EQ(OB_SUCCESS,
      append_async_scalar_range(rtdef, range, start_objs, end_objs, 3, 4));

  ObDASScopeId old_scope = DAS_SCOPE_BASE;
  const int32_t old_depth = search_ctx_.get_op_creating_depth();
  search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_ASYNC, 2, old_scope);
  const int generate_ret = rtdef.generate_op(
      ObDASSearchCost(1), search_ctx_, root);
  search_ctx_.exit_scope_after_op_created(old_scope, old_depth);
  ASSERT_EQ(OB_SUCCESS, generate_ret);
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_SORT, root->get_op_type());
  ObDASSortOp *sort = static_cast<ObDASSortOp *>(root);
  EXPECT_EQ(DAS_SCOPE_NESTED_ASYNC, sort->scope_);
  EXPECT_TRUE(sort->unique_sort_);
  EXPECT_EQ(2, sort->nk_truncate_depth_);
  EXPECT_EQ(2, sort->sort_collations_.count());
  EXPECT_EQ(DAS_ROWID_TYPE_UINT64, sort->get_rowid_type());
  ASSERT_EQ(1, sort->get_children_cnt());
  ObIDASSearchOp *direct = sort->get_children()[0];
  ASSERT_NE(nullptr, direct);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, direct->get_op_type());
  ObDASAsyncSegmentMergeOp *merge =
      static_cast<ObDASAsyncSegmentMergeOp *>(direct);
  EXPECT_EQ(ObDASAsyncSegmentMergeMode::DIRECT_MERGE, merge->mode_);
  const ObIArray<ObExpr *> *merge_output_exprs =
      merge->get_rowid_output_exprs();
  ASSERT_NE(nullptr, merge_output_exprs);
  ASSERT_EQ(2, merge_output_exprs->count());
  EXPECT_EQ(&doc_id_expr, merge_output_exprs->at(0));
  EXPECT_EQ(&nested_key_expr, merge_output_exprs->at(1));
  EXPECT_EQ(merge_output_exprs, sort->input_rowid_exprs_);
  ASSERT_EQ(SEGMENT_COUNT, merge->get_children_cnt());
  for (int64_t i = 0; i < SEGMENT_COUNT; ++i) {
    ASSERT_NE(nullptr, merge->get_children()[i]);
    ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_RANGE,
              merge->get_children()[i]->get_op_type());
    EXPECT_FALSE(static_cast<ObDASAsyncScalarRangeOp *>(
                     merge->get_children()[i])->direct_bind_);
  }
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_batch_continues_after_range_eof)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[2][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[2][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[2];
  ObIDASSearchOp *root = nullptr;
  build_catalog(catalog, {1});
  search_ctx_.set_ls_id(ObLSID(1001));
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  for (int64_t i = 0; i < ARRAYSIZEOF(ranges); ++i) {
    ASSERT_EQ(OB_SUCCESS, append_async_scalar_range(
        rtdef, ranges[i], start_objs[i], end_objs[i], i + 3, i + 4));
  }

  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ObDASAsyncSegmentMergeOp *merge = static_cast<ObDASAsyncSegmentMergeOp *>(
      root->get_children()[0]);
  ObDASAsyncScalarRangeOp *range_op = static_cast<ObDASAsyncScalarRangeOp *>(
      merge->get_children()[0]);
  MockRangeAccessService access_service;
  range_op->tsc_service_ = &access_service;
  range_op->inv_idx_tablet_id_ = ObTabletID(10001);
  range_op->inv_idx_scan_param_inited_ = true;
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = 1;
  ASSERT_EQ(OB_SUCCESS, range_op->do_rescan_segment(version_range));
  ASSERT_EQ(1, access_service.open_count_);
  merge->active_segment_cnt_ = 1;
  merge->current_batch_child_idx_ = 0;

  const uint64_t *doc_ids = nullptr;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, merge->get_next_doc_ids(2, doc_ids, count));
  ASSERT_NE(nullptr, doc_ids);
  ASSERT_EQ(2, count);
  EXPECT_EQ(101, doc_ids[0]);
  EXPECT_EQ(102, doc_ids[1]);
  EXPECT_EQ(1, range_op->current_range_idx_);
  EXPECT_EQ(2, access_service.open_count_);

  range_op->range_iter_->is_end_ = true;
  EXPECT_EQ(OB_ITER_END, merge->get_next_doc_ids(2, doc_ids, count));
  EXPECT_EQ(nullptr, doc_ids);
  EXPECT_EQ(0, count);
  EXPECT_EQ(nullptr, range_op->range_iter_);
  EXPECT_EQ(1, merge->current_batch_child_idx_);
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_dynamic_point_domain_expands_beyond_estimate)
{
  static const int64_t RANGE_COUNT = 32;
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[RANGE_COUNT][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[RANGE_COUNT][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[RANGE_COUNT];
  ObIDASSearchOp *root = nullptr;
  build_catalog(catalog, {1});
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;

  // An initially empty domain keeps one ROR placeholder instead of a Dummy.
  ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(ObDASSearchCost(1), search_ctx_, root));
  ASSERT_NE(nullptr, root);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, root->get_op_type());
  ASSERT_EQ(1, root->get_children_cnt());
  ObDASAsyncScalarRorUnionOp *range_union =
      static_cast<ObDASAsyncScalarRorUnionOp *>(root->get_children()[0]);
  ASSERT_NE(nullptr, range_union);
  ASSERT_EQ(DAS_SEARCH_OP_DISJUNCTION_FILTER, range_union->get_op_type());
  ASSERT_EQ(1, range_union->get_children_cnt());
  ObDASAsyncScalarRorOp *placeholder_ror =
      static_cast<ObDASAsyncScalarRorOp *>(range_union->get_children()[0]);
  ASSERT_NE(nullptr, placeholder_ror);
  ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR, placeholder_ror->get_op_type());
  ASSERT_EQ(OB_SUCCESS, placeholder_ror->init_term_key());
  EXPECT_TRUE(placeholder_ror->is_empty_range_);

  for (int64_t range_idx = 0; range_idx < RANGE_COUNT; ++range_idx) {
    prepare_async_scalar_range(ranges[range_idx],
        start_objs[range_idx], end_objs[range_idx], range_idx + 3, range_idx + 3);
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[range_idx]));
  }

  // Runtime domain cardinality can exceed the optimizer's estimate of 25.
  const int64_t old_children_cnt = range_union->get_children_cnt();
  ASSERT_EQ(OB_SUCCESS, range_union->ensure_range_children());
  EXPECT_EQ(1, old_children_cnt);
  ASSERT_EQ(RANGE_COUNT, range_union->get_children_cnt());
  for (int64_t range_idx = 0; range_idx < RANGE_COUNT; ++range_idx) {
    ObDASAsyncScalarRorOp *ror_op = static_cast<ObDASAsyncScalarRorOp *>(
        range_union->get_children()[range_idx]);
    ASSERT_NE(nullptr, ror_op);
    ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SCALAR_ROR, ror_op->get_op_type());
    ASSERT_EQ(range_idx, ror_op->range_idx_);
    ASSERT_EQ(OB_SUCCESS, ror_op->init_term_key());
    EXPECT_EQ(range_idx + 3, ror_op->term_key_.get_datum(SEARCH_INDEX_VALUE).get_int());
    EXPECT_FALSE(ror_op->is_empty_range_);
  }
  ObDASAsyncScalarRorOp *last_ror_op = static_cast<ObDASAsyncScalarRorOp *>(
      range_union->get_children()[RANGE_COUNT - 1]);
  EXPECT_EQ(RANGE_COUNT - 1, last_ror_op->range_idx_);
  EXPECT_EQ(SEARCH_INDEX_DOC_ID, last_ror_op->term_key_.get_datum_cnt());

  // Shrinking keeps historical leaves empty so a later expansion can reuse them.
  rtdef.key_ranges_.reuse();
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[0]));
  ASSERT_EQ(OB_SUCCESS, last_ror_op->init_term_key());
  EXPECT_TRUE(last_ror_op->is_empty_range_);
  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_ITER_END, last_ror_op->do_next_rowid(rowid, score));

  for (int64_t range_idx = 1; range_idx < RANGE_COUNT; ++range_idx) {
    ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[range_idx]));
  }
  ASSERT_EQ(OB_SUCCESS, last_ror_op->init_term_key());
  EXPECT_EQ(RANGE_COUNT + 2, last_ror_op->term_key_.get_datum(SEARCH_INDEX_VALUE).get_int());
  EXPECT_FALSE(last_ror_op->is_empty_range_);
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_leaf_drains_storage_views_without_copy)
{
  static const int64_t ROW_COUNT = blocksstable::DEFAULT_POSTING_BLOCK_CAPACITY;
  uint64_t ids[ROW_COUNT];
  for (int64_t i = 0; i < ROW_COUNT; ++i) {
    ids[i] = 2 * i;
  }
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ObDASAsyncScalarRorOpParam param(&ctdef, &rtdef, 0, TEST_INDEX_TABLE_ID, 0, true);
  param.set_is_scoring(false);
  ObDASAsyncScalarRorOp leaf(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, leaf.init(param));
  EXPECT_TRUE(leaf.supports_direct_rowid_drain());
  EXPECT_TRUE(leaf.can_enumerate_rowids());

  // Start at the decoded final posting block. Both the DAS leaf and the storage
  // iterator use their production drain implementations; SSTable IO is excluded.
  blocksstable::ObInvSSTablePostingIter posting_iter;
  posting_iter.is_opened_ = true;
  posting_iter.curr_block_loaded_ = true;
  posting_iter.curr_meta_.idx_ = 0;
  posting_iter.curr_meta_.locator_.block_idx_ = 0;
  posting_iter.curr_meta_.min_id_ = ids[0];
  posting_iter.curr_meta_.max_id_ = ids[ROW_COUNT - 1];
  posting_iter.curr_meta_.doc_count_ = ROW_COUNT;
  blocksstable::ObPostingDocIdView doc_id_view;
  doc_id_view.values_ = ids;
  doc_id_view.width_ = sizeof(uint64_t);
  blocksstable::ObPostingRelevanceView relevance_view;
  ASSERT_EQ(OB_SUCCESS, posting_iter.posting_block_data_.assign(
      ROW_COUNT, doc_id_view, relevance_view));
  posting_iter.scanner_.is_inited_ = true;
  posting_iter.scanner_.scan_iter_end_ = true;
  leaf.posting_iter_ = &posting_iter;

  class StorageViewConsumer : public ObDASRowIDBatchConsumer
  {
  public:
    explicit StorageViewConsumer(const uint64_t *expected) : expected_(expected), count_(0) {}
    int consume(const uint64_t *rowids, const int64_t count) override
    {
      EXPECT_EQ(expected_ + count_, rowids);
      count_ += count;
      return OB_SUCCESS;
    }
    const uint64_t *expected_;
    int64_t count_;
  } consumer(ids + 1);

  ObDASRowID next;
  const uint64_t first_end = ids[ROW_COUNT - 3];
  // The first non-min seek must include the row positioned by advance_to_id().
  ASSERT_EQ(OB_SUCCESS, leaf.direct_drain_rowids(1, first_end, consumer, next));
  EXPECT_EQ(ROW_COUNT - 3, consumer.count_);
  ASSERT_TRUE(next.is_normal());
  EXPECT_EQ(ids[ROW_COUNT - 2], next.get_uint64());
  ASSERT_EQ(OB_SUCCESS, leaf.direct_drain_rowids(first_end + 1, first_end + 1, consumer, next));
  EXPECT_EQ(ROW_COUNT - 3, consumer.count_);
  EXPECT_EQ(ids[ROW_COUNT - 2], next.get_uint64());
  ASSERT_EQ(OB_SUCCESS, leaf.direct_drain_rowids(ids[ROW_COUNT - 2], UINT64_MAX, consumer, next));
  EXPECT_EQ(ROW_COUNT - 1, consumer.count_);
  EXPECT_TRUE(next.is_max());
  ASSERT_EQ(OB_SUCCESS, leaf.direct_drain_rowids(UINT64_MAX, UINT64_MAX, consumer, next));
  EXPECT_EQ(ROW_COUNT - 1, consumer.count_);
  EXPECT_TRUE(next.is_max());
  leaf.posting_iter_ = nullptr;
  EXPECT_EQ(OB_SUCCESS, leaf.close());
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_direct_drain_reuses_lookahead_for_many_terms)
{
  static const int64_t TERM_COUNT = 100;
  static const uint64_t FIRST_POSTING_ID = 10000;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_index_term_point_scan_ = true;

  MockSegmentOpParam child_param;
  ObArray<ObIDASSearchOp *> children;
  ObSEArray<MockSegmentOp *, 128> child_ops;
  for (int64_t i = 0; i < TERM_COUNT; ++i) {
    MockSegmentOp *child = OB_NEWx(MockSegmentOp, &allocator_,
        search_ctx_, {FIRST_POSTING_ID + static_cast<uint64_t>(i) * 1000}, {1.0});
    ASSERT_NE(nullptr, child);
    ASSERT_EQ(OB_SUCCESS, child->init(child_param));
    ASSERT_EQ(OB_SUCCESS, children.push_back(child));
    ASSERT_EQ(OB_SUCCESS, child_ops.push_back(child));
  }

  ObDASAsyncScalarRorUnionOpParam direct_param(
      children, ObDASSearchCost(1), 0.0, &rtdef,
      TEST_INDEX_TABLE_ID, 0, true /* direct_bind */,
      true /* direct_drain_enabled */);
  direct_param.set_is_scoring(false);
  ObDASAsyncScalarRorUnionOp direct_op(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, direct_op.init(direct_param));
  ASSERT_EQ(OB_SUCCESS, direct_op.open());

  CollectingRowIDBatchConsumer consumer;
  ObDASRowID next_id_after;
  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      0, 99, consumer, next_id_after));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(FIRST_POSTING_ID, next_id_after.get_uint64());
  for (int64_t i = 0; i < TERM_COUNT; ++i) {
    EXPECT_EQ(1, child_ops.at(i)->advance_count());
    EXPECT_EQ(0, child_ops.at(i)->drain_count());
  }

  // Empty windows below the minimum lookahead do not revisit all Terms.
  for (uint64_t window_start = 100; window_start < 1000; window_start += 100) {
    ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
        window_start, window_start + 99, consumer, next_id_after));
    ASSERT_TRUE(next_id_after.is_normal());
    EXPECT_EQ(FIRST_POSTING_ID, next_id_after.get_uint64());
  }
  for (int64_t i = 0; i < TERM_COUNT; ++i) {
    EXPECT_EQ(1, child_ops.at(i)->advance_count());
    EXPECT_EQ(0, child_ops.at(i)->drain_count());
  }

  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      FIRST_POSTING_ID, FIRST_POSTING_ID, consumer, next_id_after));
  ASSERT_TRUE(next_id_after.is_normal());
  ASSERT_EQ(1, consumer.rowids().count());
  EXPECT_EQ(FIRST_POSTING_ID, consumer.rowids().at(0));
  EXPECT_EQ(1, child_ops.at(0)->drain_count());
  for (int64_t i = 1; i < TERM_COUNT; ++i) {
    EXPECT_EQ(0, child_ops.at(i)->drain_count());
  }

  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      FIRST_POSTING_ID + 1, UINT64_MAX, consumer, next_id_after));
  EXPECT_TRUE(next_id_after.is_max());
  ObSEArray<uint64_t, 128> sorted_ids;
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  ASSERT_EQ(TERM_COUNT, sorted_ids.count());
  for (int64_t i = 0; i < TERM_COUNT; ++i) {
    EXPECT_EQ(FIRST_POSTING_ID + static_cast<uint64_t>(i) * 1000,
              sorted_ids.at(i));
  }

  ASSERT_EQ(OB_SUCCESS, direct_op.rescan());
  consumer.reset();
  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      0, UINT64_MAX, consumer, next_id_after));
  EXPECT_TRUE(next_id_after.is_max());
  sorted_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  ASSERT_EQ(TERM_COUNT, sorted_ids.count());
  for (int64_t i = 0; i < TERM_COUNT; ++i) {
    EXPECT_EQ(FIRST_POSTING_ID + static_cast<uint64_t>(i) * 1000,
              sorted_ids.at(i));
  }

  EXPECT_EQ(OB_SUCCESS, direct_op.close());
  for (int64_t i = 0; i < TERM_COUNT; ++i) {
    OB_DELETEx(MockSegmentOp, &allocator_, child_ops.at(i));
  }
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_direct_drain_emits_complete_deduplicated_domain)
{
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_index_term_point_scan_ = true;

  MockSegmentOp child_ops[] = {
      MockSegmentOp(search_ctx_, {3, 7, 12}, {1.0, 1.0, 1.0}),
      MockSegmentOp(search_ctx_, {3, 8, 12}, {1.0, 1.0, 1.0}),
      MockSegmentOp(search_ctx_, {5, 12}, {1.0, 1.0})};
  MockSegmentOpParam child_param;
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
    ASSERT_EQ(OB_SUCCESS, child_ops[i].init(child_param));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&child_ops[i]));
  }

  ObDASAsyncScalarRorUnionOpParam direct_param(
      children, ObDASSearchCost(1), 0.0, &rtdef,
      TEST_INDEX_TABLE_ID, 0, true /* direct_bind */,
      true /* direct_drain_enabled */);
  direct_param.set_is_scoring(false);
  ObDASAsyncScalarRorUnionOp direct_op(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, direct_op.init(direct_param));
  ASSERT_EQ(OB_SUCCESS, direct_op.open());

  CollectingRowIDBatchConsumer consumer(true /* deduplicate */);
  ObDASRowID next_id_after;
  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      0, 7, consumer, next_id_after));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(8, next_id_after.get_uint64());
  ObSEArray<uint64_t, 8> first_window_ids;
  ASSERT_EQ(OB_SUCCESS, first_window_ids.assign(consumer.rowids()));
  lib::ob_sort(first_window_ids.begin(), first_window_ids.end());
  const uint64_t first_window_expected[] = {3, 5, 7};
  expect_rowids_equal(
      first_window_ids, first_window_expected, ARRAYSIZEOF(first_window_expected));

  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      8, 20, consumer, next_id_after));
  EXPECT_TRUE(next_id_after.is_max());
  ObSEArray<uint64_t, 8> all_ids;
  ASSERT_EQ(OB_SUCCESS, all_ids.assign(consumer.rowids()));
  lib::ob_sort(all_ids.begin(), all_ids.end());
  const uint64_t all_expected[] = {3, 5, 7, 8, 12};
  expect_rowids_equal(all_ids, all_expected, ARRAYSIZEOF(all_expected));

  ASSERT_EQ(OB_SUCCESS, direct_op.rescan());
  consumer.reset();
  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      0, UINT64_MAX, consumer, next_id_after));
  EXPECT_TRUE(next_id_after.is_max());
  ASSERT_EQ(OB_SUCCESS, all_ids.assign(consumer.rowids()));
  lib::ob_sort(all_ids.begin(), all_ids.end());
  expect_rowids_equal(all_ids, all_expected, ARRAYSIZEOF(all_expected));
  EXPECT_EQ(OB_SUCCESS, direct_op.close());
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_direct_drain_crosses_256_row_batch_boundary)
{
  static const int64_t ROW_COUNT = 257;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_index_term_point_scan_ = true;
  prepare_async_scalar_range(range, start_objs, end_objs, 1, 1);
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(range));

  std::vector<uint64_t> rowids;
  std::vector<double> scores;
  for (int64_t i = 1; i <= ROW_COUNT; ++i) {
    rowids.push_back(static_cast<uint64_t>(i));
    scores.push_back(1.0);
  }
  MockSegmentOp child(search_ctx_, rowids, scores);
  MockSegmentOp empty_child(search_ctx_, {}, {});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, child.init(child_param));
  ASSERT_EQ(OB_SUCCESS, empty_child.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&empty_child));

  ObDASAsyncScalarRorUnionOpParam direct_param(
      children, ObDASSearchCost(1), 0.0, &rtdef,
      TEST_INDEX_TABLE_ID, 0, true /* direct_bind */,
      true /* direct_drain_enabled */);
  direct_param.set_is_scoring(false);
  ObDASAsyncScalarRorUnionOp direct_op(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, direct_op.init(direct_param));
  ASSERT_EQ(OB_SUCCESS, direct_op.open());

  CollectingRowIDBatchConsumer consumer;
  ObDASRowID next_id_after;
  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      1, 255, consumer, next_id_after));
  ASSERT_EQ(255, consumer.rowids().count());
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(256, next_id_after.get_uint64());
  EXPECT_TRUE(direct_op.ready_child_heap_.empty());
  EXPECT_EQ(0, direct_op.direct_single_cursor_.child_idx_);

  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      256, 256, consumer, next_id_after));
  ASSERT_EQ(256, consumer.rowids().count());
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(257, next_id_after.get_uint64());

  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      257, 257, consumer, next_id_after));
  EXPECT_TRUE(next_id_after.is_max());
  ObSEArray<uint64_t, 300> sorted_ids;
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  ASSERT_EQ(ROW_COUNT, sorted_ids.count());
  for (int64_t i = 0; i < ROW_COUNT; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 1), sorted_ids.at(i));
  }

  ASSERT_EQ(OB_SUCCESS, direct_op.rescan());
  consumer.reset();
  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      1, UINT64_MAX, consumer, next_id_after));
  EXPECT_TRUE(next_id_after.is_max());
  sorted_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  ASSERT_EQ(ROW_COUNT, sorted_ids.count());
  for (int64_t i = 0; i < ROW_COUNT; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 1), sorted_ids.at(i));
  }
  EXPECT_EQ(OB_SUCCESS, direct_op.close());
}

void TestDASAsyncSegmentMerge::check_async_terms_ordered_direct(
    const int64_t shape, const int64_t domain_size, const uint64_t base_id)
{
  SCOPED_TRACE(::testing::Message() << "shape=" << shape << " base=" << base_id);
  const int64_t term_count = 2 == shape ? 33 : 4;
  const int64_t capacity = 4096;
  ASSERT_GT(domain_size, 0);
  ASSERT_LE(static_cast<uint64_t>(domain_size - 1), UINT64_MAX - base_id);
  std::vector<uint8_t> expected(domain_size, 0);
  std::vector<std::unique_ptr<MockSegmentOp> > child_owners;
  ObArray<ObIDASSearchOp *> children[2];
  MockSegmentOpParam child_param;
  int64_t posting_count = 0;
  for (int64_t term = 0; term < term_count; ++term) {
    std::vector<uint64_t> ids;
    std::vector<double> scores;
    for (int64_t offset = 0; offset < domain_size; ++offset) {
      const bool present = (0 == shape && offset % 5 == term)
          || (1 == shape && (offset * 1103515245ULL + term * 12345) % 17 < 12)
          || (2 == shape && 0 == term && 0 == offset % 4093)
          || (4 == shape && (offset / 4096) % 4 == term && offset % 4096 < 512);
      if (present) {
        ids.push_back(base_id + offset);
        scores.push_back(0.0);
        expected[offset] = 1;
        ++posting_count;
      }
    }
    for (int64_t mode = 0; mode < 2; ++mode) {
      child_owners.emplace_back(new MockSegmentOp(search_ctx_, ids, scores));
      ASSERT_EQ(OB_SUCCESS, child_owners.back()->init(child_param));
      ASSERT_EQ(OB_SUCCESS, children[mode].push_back(child_owners.back().get()));
    }
  }

  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_index_term_point_scan_ = true;
  ctdef.enable_new_false_range_ = true;
  // Mock children own postings; only the active range count is read by Union.
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.prepare_allocate(term_count));
  ObDASAsyncScalarRorUnionOp ordered(search_ctx_);
  ObDASAsyncScalarRorUnionOp direct(search_ctx_);
  ObDASAsyncScalarRorUnionOp *unions[] = {&ordered, &direct};
  ObDASBMMOp ordered_bmm(search_ctx_);
  ObDASBMMOp direct_bmm(search_ctx_);
  ObDASBMMOp *bmms[] = {&ordered_bmm, &direct_bmm};
  for (int64_t mode = 0; mode < 2; ++mode) {
    ObDASAsyncScalarRorUnionOpParam param(
        children[mode], ObDASSearchCost(domain_size), 0.0, &rtdef,
        TEST_INDEX_TABLE_ID, 0, true, 1 == mode);
    param.set_is_scoring(false);
    ASSERT_EQ(OB_SUCCESS, unions[mode]->init(param));
    ASSERT_EQ(1 == mode, unions[mode]->supports_direct_rowid_drain());
    ASSERT_EQ(0 == mode, unions[mode]->can_enumerate_rowids());
    ObDASBMMOp &bmm = *bmms[mode];
    ASSERT_EQ(OB_SUCCESS, bmm.doc_score_accumulator_.init(capacity, allocator_));
    void *buf = allocator_.alloc(ObBitVector::memory_size(capacity));
    ASSERT_NE(nullptr, buf);
    bmm.filter_bitset_ = to_bit_vector(buf);
    bmm.filter_bitset_->init(capacity);
    bmm.filter_op_ = unions[mode];
    bmm.filter_mode_ = 1 == mode ? ObDASBMMBuildOptions::FilterMode::DIRECT_DRAIN
        : ObDASBMMBuildOptions::FilterMode::ORDERED;
  }

  // Alternate order and rescan the same instances. Timing excludes the independent
  // per-bit oracle; there is deliberately no wall-time assertion in this test.
  const int64_t window_sizes[] = {4096, 4096, 257, 257};
  for (int64_t round = 0; round < ARRAYSIZEOF(window_sizes); ++round) {
    for (int64_t order = 0; order < 2; ++order) {
      const int64_t mode = (order + round) % 2;
      SCOPED_TRACE(::testing::Message() << "round=" << round << " direct=" << mode);
      ObDASBMMOp &bmm = *bmms[mode];
      ASSERT_EQ(OB_SUCCESS, 0 == round ? unions[mode]->open() : unions[mode]->rescan());
      EXPECT_FALSE(unions[mode]->use_single_child_fast_path());
      bmm.curr_filter_id_.set_min();
      bmm.filter_end_ = false;
      bmm.cap_next_batch_to_filter_id_ = false;
      int64_t cpu_ns = 0;
      int64_t windows = 0;
      for (int64_t offset = 0; offset < domain_size;) {
        ObDASRowID first;
        ObDASRowID last;
        first.set_uint64(base_id + offset);
        const int64_t requested_end = MIN(domain_size - 1, offset + window_sizes[round] - 1);
        last.set_uint64(base_id + requested_end);
        bool need_evaluate = false;
        struct timespec before;
        struct timespec after;
        ASSERT_EQ(0, clock_gettime(CLOCK_THREAD_CPUTIME_ID, &before));
        const int ret = bmm.prepare_filter_for_vectorized_batch(first, last, need_evaluate);
        ASSERT_EQ(0, clock_gettime(CLOCK_THREAD_CPUTIME_ID, &after));
        ASSERT_EQ(OB_SUCCESS, ret);
        cpu_ns += (after.tv_sec - before.tv_sec) * 1000000000LL + after.tv_nsec - before.tv_nsec;
        ++windows;
        ASSERT_TRUE(last.is_normal());
        ASSERT_GE(last.get_uint64(), first.get_uint64());
        ASSERT_LE(last.get_uint64(), base_id + requested_end);
        const int64_t end_offset = last.get_uint64() - base_id;
        bool has_hit = false;
        for (int64_t i = offset; i <= end_offset; ++i) {
          has_hit = has_hit || expected[i];
        }
        ASSERT_EQ(has_hit, need_evaluate);
        if (need_evaluate) {
          for (int64_t bit = 0; bit < capacity; ++bit) {
            const bool expected_bit = bit <= end_offset - offset && expected[offset + bit];
            ASSERT_EQ(expected_bit, bmm.filter_bitset_->exist(bit))
                << "offset=" << offset << " bit=" << bit;
          }
        }
        offset = end_offset + 1;
      }
      EXPECT_TRUE(bmm.filter_end_);
      if (0 == mode) {
        EXPECT_FALSE(unions[mode]->heap_initialized_);
      } else {
        EXPECT_TRUE(unions[mode]->heap_initialized_);
        EXPECT_FALSE(unions[mode]->batch_cursor_primed_);
        EXPECT_TRUE(unions[mode]->ready_child_heap_.empty());
      }
      if (nullptr != std::getenv("MR1884_REPORT_LOCAL_AB")) {
        fprintf(stdout, "TERMS_LOCAL_AB shape=%ld base=%lu domain=%ld postings=%ld terms=%ld "
            "round=%ld direct=%ld window=%ld windows=%ld cpu_ns=%ld\n",
            shape, base_id, domain_size, posting_count, term_count, round, mode,
            window_sizes[round], windows, cpu_ns);
      }
    }
  }
  for (int64_t mode = 0; mode < 2; ++mode) {
    EXPECT_EQ(OB_SUCCESS, unions[mode]->close());
    allocator_.free(bmms[mode]->filter_bitset_);
    bmms[mode]->filter_bitset_ = nullptr;
    bmms[mode]->doc_score_accumulator_.reset();
  }
}

TEST_F(TestDASAsyncSegmentMerge, async_terms_ordered_direct_dense_bitsets)
{
  check_async_terms_ordered_direct(0, 262144, 0);
  check_async_terms_ordered_direct(1, 262144, 0);
}

TEST_F(TestDASAsyncSegmentMerge, async_terms_ordered_direct_sparse_empty_and_uint64_end)
{
  for (int64_t shape : {2, 3, 4}) {
    check_async_terms_ordered_direct(shape, 32768, 0);
    check_async_terms_ordered_direct(shape, 32768, UINT64_MAX - 32767);
  }
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_ror_prepares_terms_without_copying_ranges)
{
  static const int64_t CARDINALITIES[] = {1, 2, 16, 32, 128};
  static const int64_t MAX_RANGE_COUNT = 128;
  static const int64_t SEGMENT_COUNT = 3;
  ObDASAsyncIndexQueryCtx catalog;
  share::SCN snapshot_scn;
  transaction::ObTxReadSnapshot snapshot;
  build_catalog(catalog, {SEGMENT_COUNT});
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(100));
  snapshot.init_special_read(snapshot_scn);
  search_ctx_.snapshot_ = &snapshot;
  search_ctx_.set_ls_id(ObLSID(1001));

  for (int64_t cardinality_idx = 0;
       cardinality_idx < ARRAYSIZEOF(CARDINALITIES);
       ++cardinality_idx) {
    const int64_t range_count = CARDINALITIES[cardinality_idx];
    ObDASScalarScanCtDef ctdef(allocator_);
    ObDASScalarScanRtDef rtdef;
    ObPushdownOperator pd_op(eval_ctx_, ctdef.pd_expr_spec_);
    ObExpr rowid_expr;
    ObObj start_objs[MAX_RANGE_COUNT][SEARCH_INDEX_KEY_CNT];
    ObObj end_objs[MAX_RANGE_COUNT][SEARCH_INDEX_KEY_CNT];
    ObNewRange ranges[MAX_RANGE_COUNT];
    ObIDASSearchOp *root = nullptr;
    ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
    rtdef.p_pd_expr_op_ = &pd_op;
    ASSERT_EQ(OB_SUCCESS, scan_op_.reserve_related_buffer(1));
    ASSERT_EQ(OB_SUCCESS, scan_op_.set_related_task_info(
        &ctdef, &rtdef, ObTabletID(10001)));
    ctdef.enable_new_false_range_ = true;
    ctdef.is_index_term_point_scan_ = true;
    for (int64_t range_idx = 0; range_idx < range_count; ++range_idx) {
      ASSERT_EQ(OB_SUCCESS, append_async_scalar_range(
          rtdef, ranges[range_idx], start_objs[range_idx], end_objs[range_idx],
          range_idx + 1, range_idx + 1));
    }

    ASSERT_EQ(OB_SUCCESS, rtdef.generate_op(
        ObDASSearchCost(1), search_ctx_, root));
    ASSERT_NE(nullptr, root);
    ASSERT_EQ(DAS_SEARCH_OP_ASYNC_SEGMENT_MERGE, root->get_op_type());
    ObArray<ObDASAsyncScalarRorOp *> rors;
    ASSERT_EQ(OB_SUCCESS, collect_async_scalar_rors(root, rors));
    ASSERT_EQ(SEGMENT_COUNT * range_count, rors.count());
    for (int64_t i = 0; i < rors.count(); ++i) {
      ObDASAsyncScalarRorOp *ror = rors.at(i);
      ASSERT_NE(nullptr, ror);
      const int64_t range_idx = ror->range_idx_;
      ASSERT_GE(range_idx, 0);
      ASSERT_LT(range_idx, range_count);
      ASSERT_EQ(OB_SUCCESS, ror->open());
      EXPECT_TRUE(ror->inv_idx_scan_param_.key_ranges_.empty());
      ASSERT_EQ(SEARCH_INDEX_DOC_ID, ror->term_key_.get_datum_cnt());
      EXPECT_EQ(range_idx + 1, ror->term_key_.get_datum(SEARCH_INDEX_VALUE).get_int());
      ASSERT_EQ(OB_SUCCESS, ror->close());
    }
    EXPECT_EQ(range_count, rtdef.key_ranges_.count());
  }
  search_ctx_.snapshot_ = nullptr;
}

TEST_F(TestDASAsyncSegmentMerge, init_scan_param_does_not_copy_key_ranges)
{
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[2][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[2][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[2];
  ObPushdownOperator pd_op(eval_ctx_, ctdef.pd_expr_spec_);
  share::SCN snapshot_scn;
  transaction::ObTxReadSnapshot snapshot;
  ObTableScanParam scan_param;
  ObTabletID tablet_id(10001);
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(100));
  snapshot.init_special_read(snapshot_scn);
  search_ctx_.snapshot_ = &snapshot;
  rtdef.p_pd_expr_op_ = &pd_op;
  for (int64_t i = 0; i < ARRAYSIZEOF(ranges); ++i) {
    ASSERT_EQ(OB_SUCCESS, append_async_scalar_range(
        rtdef, ranges[i], start_objs[i], end_objs[i], i + 1, i + 1));
  }

  ASSERT_EQ(OB_SUCCESS, search_ctx_.init_scan_param(
      tablet_id, &ctdef, &rtdef, scan_param,
      false /* use_async_read_snapshot */));
  EXPECT_TRUE(scan_param.key_ranges_.empty());
  EXPECT_EQ(rtdef.key_ranges_.count(), 2);
  search_ctx_.snapshot_ = nullptr;
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_ror_open_rescan_refreshes_runtime_range)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj first_start[SEARCH_INDEX_KEY_CNT];
  ObObj first_end[SEARCH_INDEX_KEY_CNT];
  ObObj replacement_start[SEARCH_INDEX_KEY_CNT];
  ObObj replacement_end[SEARCH_INDEX_KEY_CNT];
  ObNewRange first_range;
  ObNewRange replacement_range;
  char first_value[] = "first";
  char replacement_value[] = "other";
  ObPushdownOperator pd_op(eval_ctx_, ctdef.pd_expr_spec_);
  share::SCN snapshot_scn;
  transaction::ObTxReadSnapshot snapshot;
  build_catalog(catalog, {1});
  search_ctx_.set_ls_id(ObLSID(1001));
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ASSERT_EQ(OB_SUCCESS, snapshot_scn.convert_for_tx(100));
  snapshot.init_special_read(snapshot_scn);
  search_ctx_.snapshot_ = &snapshot;
  rtdef.p_pd_expr_op_ = &pd_op;
  ASSERT_EQ(OB_SUCCESS, scan_op_.reserve_related_buffer(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.set_related_task_info(
      &ctdef, &rtdef, ObTabletID(10001)));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  ASSERT_EQ(OB_SUCCESS, append_async_scalar_range(
      rtdef, first_range, first_start, first_end, 11, 11));
  first_start[SEARCH_INDEX_VALUE].set_varchar(ObString::make_string(first_value));
  first_end[SEARCH_INDEX_VALUE].set_varchar(ObString::make_string(first_value));

  ObDASAsyncScalarRorOp ror(search_ctx_);
  ObDASAsyncScalarRorOpParam ror_param(
      &ctdef, &rtdef, 0, TEST_INDEX_TABLE_ID, -1, false /* direct_bind */);
  ASSERT_EQ(OB_SUCCESS, ror.init(ror_param));
  ASSERT_EQ(OB_SUCCESS, ror.open());
  EXPECT_TRUE(ror.inv_idx_scan_param_.key_ranges_.empty());
  EXPECT_EQ(ObString::make_string("first"),
      ror.term_key_.get_datum(SEARCH_INDEX_VALUE).get_string());

  first_value[0] = 'F';
  EXPECT_EQ(ObString::make_string("first"),
      ror.term_key_.get_datum(SEARCH_INDEX_VALUE).get_string());
  ASSERT_EQ(OB_SUCCESS, ror.rescan());
  EXPECT_EQ(ObString::make_string("First"),
      ror.term_key_.get_datum(SEARCH_INDEX_VALUE).get_string());

  prepare_async_scalar_range(
      replacement_range, replacement_start, replacement_end, 29, 29);
  replacement_start[SEARCH_INDEX_VALUE].set_varchar(ObString::make_string(replacement_value));
  replacement_end[SEARCH_INDEX_VALUE].set_varchar(ObString::make_string(replacement_value));
  rtdef.key_ranges_.reuse();
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(replacement_range));
  ASSERT_EQ(OB_SUCCESS, ror.rescan());
  EXPECT_TRUE(ror.inv_idx_scan_param_.key_ranges_.empty());
  EXPECT_EQ(ObString::make_string("other"),
      ror.term_key_.get_datum(SEARCH_INDEX_VALUE).get_string());

  rtdef.key_ranges_.reuse();
  ASSERT_EQ(OB_SUCCESS, ror.rescan());
  EXPECT_TRUE(ror.inv_idx_scan_param_.key_ranges_.empty());
  EXPECT_TRUE(ror.is_empty_range_);
  EXPECT_EQ(0, ror.term_key_.get_datum_cnt());
  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_ITER_END, ror.next_rowid(rowid, score));

  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(replacement_range));
  ASSERT_EQ(OB_SUCCESS, ror.rescan());
  EXPECT_TRUE(ror.inv_idx_scan_param_.key_ranges_.empty());
  EXPECT_FALSE(ror.is_empty_range_);
  EXPECT_EQ(ObString::make_string("other"),
      ror.term_key_.get_datum(SEARCH_INDEX_VALUE).get_string());
  EXPECT_EQ(1, rtdef.key_ranges_.count());
  EXPECT_EQ(OB_SUCCESS, ror.close());
  EXPECT_EQ(OB_SUCCESS, ror.do_close());
  EXPECT_EQ(1, rtdef.key_ranges_.count());
  search_ctx_.snapshot_ = nullptr;
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_ror_posting_failure_cleans_private_state)
{
  ObDASAsyncIndexQueryCtx catalog;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange runtime_range;
  build_catalog(catalog, {1});
  search_ctx_.set_ls_id(ObLSID(1001));
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  ASSERT_EQ(OB_SUCCESS, append_async_scalar_range(
      rtdef, runtime_range, start_objs, end_objs, 7, 7));

  MockFailPostingAccessService access_service(OB_TIMEOUT);
  ObDASAsyncScalarRorOp ror(search_ctx_);
  ror.is_inited_ = true;
  ror.scalar_ctdef_ = &ctdef;
  ror.scalar_rtdef_ = &rtdef;
  ror.range_idx_ = 0;
  ror.inv_idx_tablet_id_ = ObTabletID(10001);
  ror.inv_idx_scan_param_inited_ = true;
  ror.tsc_service_ = &access_service;
  ASSERT_EQ(OB_SUCCESS, ror.init_term_key());
  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = 100;
  ASSERT_TRUE(version_range.is_valid());

  EXPECT_EQ(OB_TIMEOUT, ror.do_rescan_segment(version_range));
  EXPECT_EQ(1, access_service.open_count_);
  EXPECT_EQ(nullptr, ror.posting_iter_);
  EXPECT_TRUE(ror.inv_idx_scan_param_.key_ranges_.empty());
  EXPECT_EQ(0, ror.term_key_.get_datum_cnt());
  EXPECT_FALSE(ror.version_range_.is_valid());
  EXPECT_EQ(1, rtdef.key_ranges_.count());

  access_service.open_ret_ = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, ror.init_term_key());
  EXPECT_EQ(7, ror.term_key_.get_datum(SEARCH_INDEX_VALUE).get_int());
  EXPECT_EQ(1, rtdef.key_ranges_.count());
  EXPECT_EQ(OB_SUCCESS, ror.do_close());
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_dynamic_union_single_active_child_passthrough)
{
  static const int64_t RANGE_COUNT = 3;
  static const double MATCH_SCORE = 7.0;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[RANGE_COUNT][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[RANGE_COUNT][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[RANGE_COUNT];
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  for (int64_t range_idx = 0; range_idx < RANGE_COUNT; ++range_idx) {
    prepare_async_scalar_range(ranges[range_idx],
        start_objs[range_idx], end_objs[range_idx], range_idx + 1, range_idx + 1);
  }
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[0]));

  MockSegmentOp child_ops[] = {
      MockSegmentOp(search_ctx_, {1, 3, 5}, {1.0, 1.0, 1.0}),
      MockSegmentOp(search_ctx_, {2, 4, 6}, {2.0, 2.0, 2.0}),
      MockSegmentOp(search_ctx_, {7, 8}, {3.0, 3.0})};
  MockSegmentOpParam child_param;
  child_param.set_is_scoring(true);
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
    ASSERT_EQ(OB_SUCCESS, child_ops[i].init(child_param));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&child_ops[i]));
  }

  ObDASAsyncScalarRorUnionOpParam union_param(
      children, ObDASSearchCost(1), MATCH_SCORE, &rtdef,
      TEST_INDEX_TABLE_ID, -1, false /* direct_bind */,
      false /* direct_drain_enabled */);
  union_param.set_is_scoring(true);
  ObDASAsyncScalarRorUnionOp range_union(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, range_union.init(union_param));
  ASSERT_EQ(OB_SUCCESS, range_union.open());
  EXPECT_TRUE(range_union.use_single_child_fast_path());
  EXPECT_FALSE(range_union.batch_cursor_primed_);

  uint64_t batch_ids[8];
  double batch_scores[8];
  int64_t count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_uint64(3);
  ASSERT_EQ(OB_SUCCESS, range_union.drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(2, count);
  EXPECT_EQ(1, batch_ids[0]);
  EXPECT_EQ(3, batch_ids[1]);
  EXPECT_DOUBLE_EQ(MATCH_SCORE, batch_scores[0]);
  EXPECT_DOUBLE_EQ(MATCH_SCORE, batch_scores[1]);
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(5, next_id_after.get_uint64());
  EXPECT_DOUBLE_EQ(MATCH_SCORE, next_score_after);
  EXPECT_EQ(1, child_ops[0].drain_count());
  EXPECT_EQ(0, child_ops[1].drain_count());
  EXPECT_EQ(0, child_ops[2].drain_count());

  // Expanding the runtime range domain must restore the existing heap merge.
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[1]));
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[2]));
  ASSERT_EQ(OB_SUCCESS, range_union.rescan());
  EXPECT_FALSE(range_union.use_single_child_fast_path());
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  boundary.set_uint64(4);
  ASSERT_EQ(OB_SUCCESS, range_union.drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(4, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 1), batch_ids[i]);
    EXPECT_DOUBLE_EQ(MATCH_SCORE, batch_scores[i]);
  }
  EXPECT_GT(child_ops[1].drain_count(), 0);

  // Historical children remain allocated after shrink, but only range 0 is
  // active and must regain the passthrough fast path.
  rtdef.key_ranges_.reuse();
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[0]));
  ASSERT_EQ(OB_SUCCESS, range_union.rescan());
  EXPECT_TRUE(range_union.use_single_child_fast_path());
  count = 0;
  next_id_after.reset();
  next_score_after = 0.0;
  boundary.set_uint64(3);
  ASSERT_EQ(OB_SUCCESS, range_union.drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(2, count);
  EXPECT_EQ(1, child_ops[0].drain_count());
  EXPECT_EQ(0, child_ops[1].drain_count());
  EXPECT_EQ(0, child_ops[2].drain_count());
}

TEST_F(TestDASAsyncSegmentMerge, async_scalar_dynamic_union_single_child_advance_to_sentinels)
{
  static const double MATCH_SCORE = 7.0;
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange range;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  prepare_async_scalar_range(range, start_objs, end_objs, 1, 1);
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(range));

  MockSegmentOp child_op(search_ctx_, {1, 3, 5}, {1.0, 1.0, 1.0});
  MockSegmentOpParam child_param;
  child_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, child_op.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child_op));

  ObDASAsyncScalarRorUnionOpParam union_param(
      children, ObDASSearchCost(1), MATCH_SCORE, &rtdef,
      TEST_INDEX_TABLE_ID, -1, false /* direct_bind */,
      false /* direct_drain_enabled */);
  union_param.set_is_scoring(true);
  ObDASAsyncScalarRorUnionOp range_union(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, range_union.init(union_param));
  ASSERT_EQ(OB_SUCCESS, range_union.open());
  EXPECT_TRUE(range_union.use_single_child_fast_path());

  ObDASRowID rowid;
  double score = 0.0;
  ObDASRowID min_id;
  min_id.set_uint64(99);
  min_id.set_min();
  ASSERT_EQ(OB_SUCCESS, range_union.advance_to(min_id, rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(MATCH_SCORE, score);
  ASSERT_EQ(1, child_op.advance_count());
  EXPECT_FALSE(child_op.last_advance_target().is_min());
  EXPECT_FALSE(child_op.last_advance_target().is_max());
  EXPECT_EQ(0, child_op.last_advance_target().get_uint64());

  ASSERT_EQ(OB_SUCCESS, range_union.rescan());
  EXPECT_TRUE(range_union.use_single_child_fast_path());
  ObDASRowID max_id;
  max_id.set_uint64(0);
  max_id.set_max();
  EXPECT_EQ(OB_ITER_END, range_union.advance_to(max_id, rowid, score));
  EXPECT_EQ(0, child_op.advance_count());
  EXPECT_EQ(OB_ITER_END, range_union.next_rowid(rowid, score));

  uint64_t batch_ids[8];
  double batch_scores[8];
  int64_t count = -1;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_max();
  ASSERT_EQ(OB_SUCCESS, range_union.drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  EXPECT_EQ(0, count);
  EXPECT_TRUE(next_id_after.is_max());

  ASSERT_EQ(OB_SUCCESS, range_union.rescan());
  ASSERT_EQ(OB_SUCCESS, range_union.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
}

TEST_F(TestDASAsyncSegmentMerge, prepare_is_one_shot_and_normalizes_input)
{
  static const uint64_t TEST_DELETE_TABLE_ID = 9101;
  ObDASScalarScanCtDef inv_scan(allocator_);
  ObDASIRScanCtDef ir_scan(allocator_);
  ObDASMatchCtDef match(allocator_);
  ObDASBaseCtDef *ir_children[] = {&inv_scan};
  ObDASBaseCtDef *match_children[] = {&ir_scan};
  ObDASScalarScanCtDef delete_scan(allocator_);
  ObDASBooleanQueryCtDef async_boolean(allocator_);
  ObDASBaseCtDef *boolean_children[] = {&match, &delete_scan};
  ObDASTableLocMeta main_loc_meta(allocator_);
  ObDASTableLocMeta index_loc_meta(allocator_);
  ObDASTableLocMeta delete_loc_meta(allocator_);
  ObDASTabletLoc main_loc1;
  ObDASTabletLoc index_loc1;
  ObDASTabletLoc delete_loc1;
  ObDASTabletLoc main_loc2;
  ObDASTabletLoc index_loc2;
  ObDASTabletLoc delete_loc2;
  ObDASTabletLoc conflicting_main_loc;
  ObDASTabletLoc conflicting_index_loc;
  ObDASTabletLoc conflicting_delete_loc;
  ObSEArray<const ObDASTabletLoc *, 3> tablet_locs;
  ObDASAsyncIndexQueryCtx catalog;
  SCN sql_snapshot;

  inv_scan.ref_table_id_ = TEST_INDEX_TABLE_ID;
  ir_scan.children_ = ir_children;
  ir_scan.children_cnt_ = ARRAYSIZEOF(ir_children);
  ir_scan.is_async_index_ = true;
  ir_scan.use_scalar_scan_def_ = true;
  match.children_ = match_children;
  match.children_cnt_ = ARRAYSIZEOF(match_children);
  match.ir_ctdef_idx_ = 0;
  delete_scan.ref_table_id_ = TEST_DELETE_TABLE_ID;
  async_boolean.children_ = boolean_children;
  async_boolean.children_cnt_ = ARRAYSIZEOF(boolean_children);
  async_boolean.set_must(0, 1);
  async_boolean.set_must_not(1, 1);
  async_boolean.set_is_async(true);
  main_loc_meta.ref_table_id_ = 8001;
  index_loc_meta.ref_table_id_ = TEST_INDEX_TABLE_ID;
  delete_loc_meta.ref_table_id_ = TEST_DELETE_TABLE_ID;
  main_loc1.tablet_id_ = ObTabletID(1);
  main_loc1.ls_id_ = ObLSID(1001);
  main_loc1.loc_meta_ = &main_loc_meta;
  main_loc1.next_ = &index_loc1;
  index_loc1.tablet_id_ = ObTabletID(10001);
  index_loc1.ls_id_ = ObLSID(1001);
  index_loc1.loc_meta_ = &index_loc_meta;
  index_loc1.next_ = &delete_loc1;
  delete_loc1.tablet_id_ = ObTabletID(20001);
  delete_loc1.ls_id_ = ObLSID(1001);
  delete_loc1.loc_meta_ = &delete_loc_meta;
  delete_loc1.next_ = &main_loc1;
  main_loc2.tablet_id_ = ObTabletID(2);
  main_loc2.ls_id_ = ObLSID(1002);
  main_loc2.loc_meta_ = &main_loc_meta;
  main_loc2.next_ = &index_loc2;
  index_loc2.tablet_id_ = ObTabletID(10002);
  index_loc2.ls_id_ = ObLSID(1002);
  index_loc2.loc_meta_ = &index_loc_meta;
  index_loc2.next_ = &delete_loc2;
  delete_loc2.tablet_id_ = ObTabletID(20002);
  delete_loc2.ls_id_ = ObLSID(1002);
  delete_loc2.loc_meta_ = &delete_loc_meta;
  delete_loc2.next_ = &main_loc2;
  conflicting_main_loc.tablet_id_ = main_loc1.tablet_id_;
  conflicting_main_loc.ls_id_ = main_loc1.ls_id_;
  conflicting_main_loc.loc_meta_ = &main_loc_meta;
  conflicting_main_loc.next_ = &conflicting_index_loc;
  conflicting_index_loc.tablet_id_ = ObTabletID(11001);
  conflicting_index_loc.ls_id_ = main_loc1.ls_id_;
  conflicting_index_loc.loc_meta_ = &index_loc_meta;
  conflicting_index_loc.next_ = &conflicting_delete_loc;
  conflicting_delete_loc.tablet_id_ = delete_loc1.tablet_id_;
  conflicting_delete_loc.ls_id_ = main_loc1.ls_id_;
  conflicting_delete_loc.loc_meta_ = &delete_loc_meta;
  conflicting_delete_loc.next_ = &conflicting_main_loc;

  segment_counts_ = {1, 2};
  fetch_cursor_ = 0;
  catalog.set_mock_fetcher(fetch_partition, this);
  ASSERT_EQ(OB_SUCCESS, sql_snapshot.convert_for_tx(2000));
  ASSERT_EQ(OB_SUCCESS, tablet_locs.push_back(&main_loc2));
  ASSERT_EQ(OB_SUCCESS, tablet_locs.push_back(&main_loc1));
  ASSERT_EQ(OB_SUCCESS, tablet_locs.push_back(&main_loc2));
  ASSERT_EQ(OB_SUCCESS, catalog.prepare(
      &async_boolean, tablet_locs, sql_snapshot, ASYNC_INDEX_EXEC_LEGACY));
  EXPECT_EQ(2, fetch_cursor_);
  EXPECT_EQ(2, catalog.get_partition_cnt());

  EXPECT_EQ(OB_INIT_TWICE, catalog.prepare(
      &async_boolean, tablet_locs, sql_snapshot, ASYNC_INDEX_EXEC_LEGACY));
  EXPECT_EQ(2, fetch_cursor_);

  const ObAsyncIndexPartitionSegCtx *old_partition = nullptr;
  ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(1), old_partition));
  ASSERT_NE(nullptr, old_partition);
  ASSERT_EQ(1, old_partition->index_table_ids_.count());
  ASSERT_EQ(1, old_partition->index_tablet_ids_.count());
  EXPECT_EQ(TEST_INDEX_TABLE_ID, old_partition->index_table_ids_.at(0));
  EXPECT_EQ(ObTabletID(10001), old_partition->index_tablet_ids_.at(0));

  ObDASAsyncIndexQueryCtx conflicting_catalog;
  conflicting_catalog.set_mock_fetcher(fetch_partition, this);
  segment_counts_ = {1};
  fetch_cursor_ = 0;
  tablet_locs.reset();
  ASSERT_EQ(OB_SUCCESS, tablet_locs.push_back(&main_loc1));
  ASSERT_EQ(OB_SUCCESS, tablet_locs.push_back(&conflicting_main_loc));
  EXPECT_EQ(OB_STATE_NOT_MATCH, conflicting_catalog.prepare(
      &async_boolean, tablet_locs, sql_snapshot, ASYNC_INDEX_EXEC_LEGACY));
  EXPECT_EQ(0, fetch_cursor_);
  EXPECT_FALSE(conflicting_catalog.is_inited());
  EXPECT_EQ(0, conflicting_catalog.get_partition_cnt());
}

TEST_F(TestDASAsyncSegmentMerge, table_scan_rejects_existing_async_query_ctx)
{
  ObExecContext exec_ctx(allocator_);
  ObTableScanSpec spec(allocator_, PHY_TABLE_SCAN);
  ObTableScanOpInput input(exec_ctx, spec);
  oceanbase::sql::ObTableScanOp table_scan(exec_ctx, spec, &input);
  ObDASAsyncIndexQueryCtx query_ctx;
  spec.tsc_ctdef_.is_async_index_query_ = true;
  table_scan.async_query_ctx_ = &query_ctx;

  EXPECT_EQ(OB_INIT_TWICE, table_scan.prepare_async_index_query_ctx());

  table_scan.async_query_ctx_ = nullptr;
}

TEST_F(TestDASAsyncSegmentMerge, remote_prepare_is_one_shot_and_preserves_zipped_binding)
{
  static const uint64_t TEST_DELETE_TABLE_ID = 9101;
  ObDASScalarScanCtDef inv_scan1(allocator_);
  ObDASScalarScanCtDef inv_scan2(allocator_);
  ObDASScalarScanCtDef delete_scan(allocator_);
  ObDASIRScanCtDef ir_scan1(allocator_);
  ObDASIRScanCtDef ir_scan2(allocator_);
  ObDASBooleanQueryCtDef multi_query(allocator_);
  ObDASBooleanQueryCtDef async_wrapper(allocator_);
  ObDASBaseCtDef *ir_children1[] = {&inv_scan1};
  ObDASBaseCtDef *ir_children2[] = {&inv_scan2};
  ObDASBaseCtDef *multi_children[] = {&ir_scan1, &ir_scan2};
  ObDASBaseCtDef *wrapper_children[] = {&multi_query, &delete_scan};
  ObSEArray<uint64_t, 3> logical_ids;
  ObSEArray<ObTabletID, 3> physical_ids;
  ObDASAsyncIndexQueryCtx catalog;
  SCN sql_snapshot;

  inv_scan1.ref_table_id_ = TEST_INDEX_TABLE_ID;
  inv_scan2.ref_table_id_ = TEST_INDEX_TABLE_ID_2;
  ir_scan1.children_ = ir_children1;
  ir_scan1.children_cnt_ = ARRAYSIZEOF(ir_children1);
  ir_scan1.is_async_index_ = true;
  ir_scan1.use_scalar_scan_def_ = true;
  ir_scan2.children_ = ir_children2;
  ir_scan2.children_cnt_ = ARRAYSIZEOF(ir_children2);
  ir_scan2.is_async_index_ = true;
  ir_scan2.use_scalar_scan_def_ = true;
  multi_query.children_ = multi_children;
  multi_query.children_cnt_ = ARRAYSIZEOF(multi_children);
  delete_scan.ref_table_id_ = TEST_DELETE_TABLE_ID;
  async_wrapper.children_ = wrapper_children;
  async_wrapper.children_cnt_ = ARRAYSIZEOF(wrapper_children);
  async_wrapper.set_must(0, 1);
  async_wrapper.set_must_not(1, 1);
  async_wrapper.set_is_async(true);

  multi_segment_counts_ = {{1, 1}};
  partition_aligned_ = {true};
  fetch_cursor_ = 0;
  catalog.set_mock_fetcher(fetch_multi_index_partition, this);
  ASSERT_EQ(OB_SUCCESS, sql_snapshot.convert_for_tx(2000));
  ASSERT_EQ(OB_SUCCESS, logical_ids.push_back(TEST_INDEX_TABLE_ID_2));
  ASSERT_EQ(OB_SUCCESS, logical_ids.push_back(TEST_INDEX_TABLE_ID));
  ASSERT_EQ(OB_SUCCESS, logical_ids.push_back(TEST_INDEX_TABLE_ID_2));
  ASSERT_EQ(OB_SUCCESS, physical_ids.push_back(ObTabletID(10002)));
  ASSERT_EQ(OB_SUCCESS, physical_ids.push_back(ObTabletID(10001)));
  ASSERT_EQ(OB_SUCCESS, physical_ids.push_back(ObTabletID(10002)));
  ASSERT_EQ(OB_SUCCESS, catalog.prepare_resolved_partition(
      &async_wrapper, ObLSID(1001), ObTabletID(1), logical_ids, physical_ids,
      ObTabletID(20001), sql_snapshot, ASYNC_INDEX_EXEC_GROUP));
  EXPECT_EQ(1, fetch_cursor_);

  EXPECT_EQ(OB_INIT_TWICE, catalog.prepare_resolved_partition(
      &async_wrapper, ObLSID(1001), ObTabletID(1), logical_ids, physical_ids,
      ObTabletID(20001), sql_snapshot, ASYNC_INDEX_EXEC_GROUP));
  EXPECT_EQ(1, fetch_cursor_);

  const ObAsyncIndexPartitionSegCtx *partition = nullptr;
  ASSERT_EQ(OB_SUCCESS, catalog.find_partition(ObTabletID(1), partition));
  ASSERT_NE(nullptr, partition);
  ASSERT_EQ(2, partition->index_table_ids_.count());
  ASSERT_EQ(2, partition->index_tablet_ids_.count());
  EXPECT_EQ(TEST_INDEX_TABLE_ID, partition->index_table_ids_.at(0));
  EXPECT_EQ(ObTabletID(10001), partition->index_tablet_ids_.at(0));
  EXPECT_EQ(TEST_INDEX_TABLE_ID_2, partition->index_table_ids_.at(1));
  EXPECT_EQ(ObTabletID(10002), partition->index_tablet_ids_.at(1));

  ObDASAsyncIndexQueryCtx conflicting_catalog;
  logical_ids.reset();
  physical_ids.reset();
  ASSERT_EQ(OB_SUCCESS, logical_ids.push_back(TEST_INDEX_TABLE_ID));
  ASSERT_EQ(OB_SUCCESS, logical_ids.push_back(TEST_INDEX_TABLE_ID));
  ASSERT_EQ(OB_SUCCESS, physical_ids.push_back(ObTabletID(10001)));
  ASSERT_EQ(OB_SUCCESS, physical_ids.push_back(ObTabletID(11001)));
  fetch_cursor_ = 0;
  conflicting_catalog.set_mock_fetcher(fetch_multi_index_partition, this);
  EXPECT_EQ(OB_STATE_NOT_MATCH, conflicting_catalog.prepare_resolved_partition(
      &async_wrapper, ObLSID(1001), ObTabletID(1), logical_ids, physical_ids,
      ObTabletID(20001), sql_snapshot, ASYNC_INDEX_EXEC_GROUP));
  EXPECT_EQ(0, fetch_cursor_);
  EXPECT_FALSE(conflicting_catalog.is_inited());
}

TEST_F(TestDASAsyncSegmentMerge, create_op_cleans_dynamic_state_when_get_children_fails)
{
  CountingFailAllocator allocator;
  ObArenaAllocator scan_allocator("AsyncSegFail");
  ObDASScanOp scan_op(scan_allocator);
  ObDASSearchCtx search_ctx(allocator, scan_op);
  ObSEArray<ObIDASSearchOp *, 5> children;
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, children.push_back(nullptr));
  }
  FailingGetChildrenSegmentParam param(children);
  TrackedAsyncSegmentMergeOp *op = reinterpret_cast<TrackedAsyncSegmentMergeOp *>(1);
  EXPECT_EQ(OB_ERR_UNEXPECTED, search_ctx.create_op(param, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(0, search_ctx.op_store_.count());
  EXPECT_EQ(0, search_ctx.op_count_);
  EXPECT_EQ(0, allocator.outstanding_allocations());
}

TEST_F(TestDASAsyncSegmentMerge, create_op_cleans_dynamic_state_when_install_children_fails)
{
  CountingFailAllocator allocator;
  allocator.set_fail_at(3); // op object, tracked resource, then children pointer array
  ObArenaAllocator scan_allocator("AsyncSegFail");
  ObDASScanOp scan_op(scan_allocator);
  ObDASSearchCtx search_ctx(allocator, scan_op);
  ObSEArray<ObIDASSearchOp *, 5> children;
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, children.push_back(nullptr));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  TrackedAsyncSegmentMergeOp *op = reinterpret_cast<TrackedAsyncSegmentMergeOp *>(1);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, search_ctx.create_op(param, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(0, search_ctx.op_store_.count());
  EXPECT_EQ(0, search_ctx.op_count_);
  EXPECT_EQ(0, allocator.outstanding_allocations());
}

TEST_F(TestDASAsyncSegmentMerge, create_op_cleans_children_when_store_registration_fails)
{
  CountingFailAllocator allocator;
  ObArenaAllocator scan_allocator("AsyncSegFail");
  ObDASScanOp scan_op(scan_allocator);
  ObDASSearchCtx search_ctx(allocator, scan_op);
  search_ctx.op_store_.block_allocator_.set_allocator(&allocator);
  for (int64_t i = 0; i < 16; ++i) {
    ASSERT_EQ(OB_SUCCESS, search_ctx.op_store_.push_back(nullptr));
  }

  ObSEArray<ObIDASSearchOp *, 5> children;
  for (int64_t i = 0; i < 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, children.push_back(nullptr));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  allocator.set_fail_at(4); // op object, tracked resource, children array, then op_store growth
  TrackedAsyncSegmentMergeOp *op = reinterpret_cast<TrackedAsyncSegmentMergeOp *>(1);
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, search_ctx.create_op(param, op));
  EXPECT_EQ(nullptr, op);
  EXPECT_EQ(16, search_ctx.op_store_.count());
  EXPECT_EQ(0, search_ctx.op_count_);
  EXPECT_EQ(4, allocator.alloc_attempts());
  EXPECT_EQ(0, allocator.outstanding_allocations());
  search_ctx.op_store_.reset();
}

TEST_F(TestDASAsyncSegmentMerge, outer_segment_rescan_stops_at_logical_index_boundary)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1});
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *child = create_child({1}, {1.0});
  ASSERT_EQ(OB_SUCCESS, children.push_back(child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ObNewVersionRange unrelated_outer_range;
  unrelated_outer_range.base_version_ = 1000;
  unrelated_outer_range.snapshot_version_ = 2000;
  ASSERT_TRUE(unrelated_outer_range.is_valid());

  EXPECT_EQ(OB_SUCCESS, merge->rescan_segment(unrelated_outer_range));
  EXPECT_EQ(0, child->bind_count());
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_EQ(1, child->bind_count());
}

TEST_F(TestDASAsyncSegmentMerge, direct_leaf_rejects_segment_rescan)
{
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = 100;
  ASSERT_TRUE(range.is_valid());

  ObDASAsyncScalarRorOp scalar(search_ctx_);
  scalar.is_inited_ = true;
  scalar.direct_bind_ = true;
  EXPECT_EQ(OB_STATE_NOT_MATCH, scalar.do_rescan_segment(range));

  ObDASAsyncTokenHelper helper(search_ctx_);
  helper.is_inited_ = true;
  helper.direct_bind_ = true;
  EXPECT_EQ(OB_STATE_NOT_MATCH, helper.rescan_segment(range));

  ObDASAsyncTokenOp token(search_ctx_);
  token.is_inited_ = true;
  token.direct_bind_ = true;
  EXPECT_EQ(OB_STATE_NOT_MATCH, token.do_rescan_segment(range));
}

TEST_F(TestDASAsyncSegmentMerge, direct_drain_union_resets_lookahead_on_segment_rescan)
{
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_index_term_point_scan_ = true;

  MockSegmentOp child0(search_ctx_, {1, 5}, {1.0, 1.0});
  MockSegmentOp child1(search_ctx_, {2, 6}, {1.0, 1.0});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, child0.init(child_param));
  ASSERT_EQ(OB_SUCCESS, child1.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child1));

  ObDASAsyncScalarRorUnionOpParam direct_param(
      children, ObDASSearchCost(1), 0.0, &rtdef,
      TEST_INDEX_TABLE_ID, -1, false /* direct_bind */,
      true /* direct_drain_enabled */);
  direct_param.set_is_scoring(false);
  ObDASAsyncScalarRorUnionOp direct_op(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, direct_op.init(direct_param));
  ASSERT_EQ(OB_SUCCESS, direct_op.open());

  CollectingRowIDBatchConsumer consumer(true /* deduplicate */);
  ObDASRowID next_id_after;
  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      1, 2, consumer, next_id_after));
  const uint64_t expected[] = {1, 2};
  expect_rowids_equal(consumer.rowids(), expected, ARRAYSIZEOF(expected));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(5, next_id_after.get_uint64());

  ObNewVersionRange range;
  range.base_version_ = 100;
  range.snapshot_version_ = 200;
  ASSERT_TRUE(range.is_valid());
  ASSERT_EQ(OB_SUCCESS, direct_op.rescan_segment(range));
  EXPECT_EQ(1, child0.bind_count());
  EXPECT_EQ(1, child1.bind_count());

  consumer.reset();
  ASSERT_EQ(OB_SUCCESS, direct_op.direct_drain_rowids(
      1, 2, consumer, next_id_after));
  expect_rowids_equal(consumer.rowids(), expected, ARRAYSIZEOF(expected));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(5, next_id_after.get_uint64());
  EXPECT_EQ(OB_SUCCESS, direct_op.close());
}

TEST_F(TestDASAsyncSegmentMerge, independent_group_filter_rescans_without_forwarding_fts_version)
{
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[SEARCH_INDEX_KEY_CNT];
  ObNewRange key_range;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  prepare_async_scalar_range(key_range, start_objs, end_objs, 1, 1);
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(key_range));

  std::vector<uint64_t> ids;
  std::vector<double> scores;
  for (uint64_t i = 1; i <= 600; ++i) {
    ids.push_back(i * 3);
    scores.push_back(0.0);
  }
  MockSegmentOp leaf(search_ctx_, ids, scores);
  MockSegmentOpParam leaf_param;
  ASSERT_EQ(OB_SUCCESS, leaf.init(leaf_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&leaf));
  ObDASAsyncScalarRorUnionOpParam param(
      children, ObDASSearchCost(1), 0.0, &rtdef, TEST_INDEX_TABLE_ID, 0,
      true /* direct_bind */, false /* direct_drain_enabled */,
      true /* independent_group_filter */);
  param.set_is_scoring(false);
  ObDASAsyncScalarRorUnionOp filter(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, filter.init(param));
  ASSERT_EQ(OB_SUCCESS, filter.open());

  // Reuse the same object after partial consumption and after EOF. The foreign
  // FTS version must never reach the Search leaf's segment-binding interface.
  for (int64_t round = 0; round < 3; ++round) {
    if (round > 0) {
      ObNewVersionRange foreign_range;
      foreign_range.base_version_ = round * 100;
      foreign_range.snapshot_version_ = (round + 1) * 100;
      ASSERT_EQ(OB_SUCCESS, filter.rescan_segment(foreign_range));
      EXPECT_EQ(round, leaf.rescan_count());
      EXPECT_EQ(0, leaf.bind_count());
      EXPECT_EQ(-1, leaf.last_range_base());
    }
    const int64_t expected_count = 0 == round ? 300 : 600;
    ObDASRowID rowid;
    double score = 0.0;
    for (int64_t i = 0; i < expected_count; ++i) {
      ASSERT_EQ(OB_SUCCESS, filter.next_rowid(rowid, score));
      ASSERT_TRUE(rowid.is_normal());
      EXPECT_EQ(ids[i], rowid.get_uint64());
    }
    if (0 == round) {
      uint64_t batch_ids[256];
      int64_t count = 0;
      ObDASRowID boundary;
      ObDASRowID next_id_after;
      double next_score_after = 0.0;
      boundary.set_uint64(1200);
      ASSERT_EQ(OB_SUCCESS, filter.drain_until(
          boundary, ARRAYSIZEOF(batch_ids), batch_ids, nullptr,
          count, next_id_after, next_score_after));
      ASSERT_EQ(100, count);
      for (int64_t i = 0; i < count; ++i) {
        EXPECT_EQ(static_cast<uint64_t>(903 + 3 * i), batch_ids[i]);
      }
      ASSERT_TRUE(next_id_after.is_normal());
      EXPECT_EQ(1203, next_id_after.get_uint64());
    } else {
      EXPECT_EQ(OB_ITER_END, filter.next_rowid(rowid, score));
    }
  }
  // Ordinary rescan also resets EOF and advance state on this same instance.
  ASSERT_EQ(OB_SUCCESS, filter.rescan());
  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(899);
  ASSERT_EQ(OB_SUCCESS, filter.advance_to(target, rowid, score));
  EXPECT_EQ(900, rowid.get_uint64());
  EXPECT_EQ(3, leaf.rescan_count());
  EXPECT_EQ(0, leaf.bind_count());
  EXPECT_EQ(OB_SUCCESS, filter.close());
}

TEST_F(TestDASAsyncSegmentMerge, independent_group_filter_rescan_error_is_not_hidden)
{
  class FailingRescanLeaf : public MockSegmentOp
  {
  public:
    explicit FailingRescanLeaf(ObDASSearchCtx &ctx) : MockSegmentOp(ctx, {3}, {0.0}) {}
    int do_rescan() override { return OB_TIMEOUT; }
  };
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  FailingRescanLeaf leaf(search_ctx_);
  MockSegmentOpParam leaf_param;
  ASSERT_EQ(OB_SUCCESS, leaf.init(leaf_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&leaf));
  ObDASAsyncScalarRorUnionOpParam param(
      children, ObDASSearchCost(1), 0.0, &rtdef, TEST_INDEX_TABLE_ID, 0,
      true, false, true);
  param.set_is_scoring(false);
  ObDASAsyncScalarRorUnionOp filter(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, filter.init(param));
  ASSERT_EQ(OB_SUCCESS, filter.open());
  ObNewVersionRange foreign_range;
  foreign_range.base_version_ = 100;
  foreign_range.snapshot_version_ = 200;
  EXPECT_EQ(OB_TIMEOUT, filter.rescan_segment(foreign_range));
  EXPECT_EQ(0, leaf.bind_count());
  EXPECT_EQ(OB_SUCCESS, filter.close());
}

TEST_F(TestDASAsyncSegmentMerge, independent_group_filter_range_growth_restores_merge)
{
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ObObj start_objs[2][SEARCH_INDEX_KEY_CNT];
  ObObj end_objs[2][SEARCH_INDEX_KEY_CNT];
  ObNewRange ranges[2];
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  for (int64_t i = 0; i < 2; ++i) {
    prepare_async_scalar_range(ranges[i], start_objs[i], end_objs[i], i + 1, i + 1);
  }
  ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[0]));
  MockSegmentOp first(search_ctx_, {1, 3}, {0.0, 0.0});
  MockSegmentOp second(search_ctx_, {2, 4}, {0.0, 0.0});
  MockSegmentOpParam leaf_param;
  ASSERT_EQ(OB_SUCCESS, first.init(leaf_param));
  ASSERT_EQ(OB_SUCCESS, second.init(leaf_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&first));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&second));
  ObDASAsyncScalarRorUnionOpParam param(
      children, ObDASSearchCost(1), 0.0, &rtdef, TEST_INDEX_TABLE_ID, 0,
      true, false, true);
  param.set_is_scoring(false);
  ObDASAsyncScalarRorUnionOp filter(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, filter.init(param));
  ASSERT_EQ(OB_SUCCESS, filter.open());
  ObNewVersionRange foreign_range;
  foreign_range.base_version_ = 100;
  foreign_range.snapshot_version_ = 200;
  for (int64_t round = 0; round < 3; ++round) {
    if (1 == round) {
      ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[1]));
    } else if (2 == round) {
      rtdef.key_ranges_.reuse();
      ASSERT_EQ(OB_SUCCESS, rtdef.key_ranges_.push_back(ranges[0]));
    }
    if (round > 0) {
      ASSERT_EQ(OB_SUCCESS, filter.rescan_segment(foreign_range));
    }
    EXPECT_EQ(1 != round, filter.use_single_child_fast_path());
    ObDASRowID rowid;
    double score = 0.0;
    const int64_t count = 1 == round ? 4 : 2;
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_EQ(OB_SUCCESS, filter.next_rowid(rowid, score));
      EXPECT_EQ(static_cast<uint64_t>(1 == round ? i + 1 : i * 2 + 1), rowid.get_uint64());
    }
    EXPECT_EQ(OB_ITER_END, filter.next_rowid(rowid, score));
    EXPECT_EQ(0, first.bind_count());
    EXPECT_EQ(0, second.bind_count());
  }
  EXPECT_EQ(OB_SUCCESS, filter.close());
}

TEST_F(TestDASAsyncSegmentMerge, independent_group_filter_rejects_incompatible_binding)
{
  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.enable_new_false_range_ = true;
  ctdef.is_index_term_point_scan_ = true;
  MockSegmentOp leaf(search_ctx_, {3}, {0.0});
  MockSegmentOpParam leaf_param;
  ASSERT_EQ(OB_SUCCESS, leaf.init(leaf_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&leaf));
  for (int64_t mode = 0; mode < 4; ++mode) {
    ObDASAsyncScalarRorUnionOpParam param(
        children, ObDASSearchCost(1), 0.0, &rtdef, TEST_INDEX_TABLE_ID,
        1 == mode ? -1 : 0, 0 != mode /* direct_bind */,
        2 == mode /* direct_drain_enabled */, true /* independent_group_filter */);
    param.set_is_scoring(3 == mode);
    ObDASAsyncScalarRorUnionOp filter(search_ctx_);
    EXPECT_EQ(OB_INVALID_ARGUMENT, filter.init(param)) << "mode=" << mode;
  }
}

TEST_F(TestDASAsyncSegmentMerge, direct_merge_forwards_bounded_drain_to_all_segments)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});

  ObDASScalarScanCtDef ctdef(allocator_);
  ObDASScalarScanRtDef rtdef;
  ObExpr rowid_expr;
  ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr));
  ctdef.is_index_term_point_scan_ = true;

  MockSegmentOp child_ops[] = {
      MockSegmentOp(search_ctx_, {1, 5, 8}, {1.0, 1.0, 1.0}),
      MockSegmentOp(search_ctx_, {3, 6}, {1.0, 1.0}),
      MockSegmentOp(search_ctx_, {2, 4, 8}, {1.0, 1.0, 1.0}),
      MockSegmentOp(search_ctx_, {3, 7, 10, 12}, {1.0, 1.0, 1.0, 1.0})};
  MockSegmentOpParam child_param;
  for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
    ASSERT_EQ(OB_SUCCESS, child_ops[i].init(child_param));
  }

  ObArray<ObIDASSearchOp *> segment0_terms;
  ObArray<ObIDASSearchOp *> segment1_terms;
  ASSERT_EQ(OB_SUCCESS, segment0_terms.push_back(&child_ops[0]));
  ASSERT_EQ(OB_SUCCESS, segment0_terms.push_back(&child_ops[1]));
  ASSERT_EQ(OB_SUCCESS, segment1_terms.push_back(&child_ops[2]));
  ASSERT_EQ(OB_SUCCESS, segment1_terms.push_back(&child_ops[3]));
  ObDASAsyncScalarRorUnionOpParam segment0_param(
      segment0_terms, ObDASSearchCost(1), 0.0, &rtdef,
      TEST_INDEX_TABLE_ID, -1, false /* direct_bind */,
      true /* direct_drain_enabled */);
  ObDASAsyncScalarRorUnionOpParam segment1_param(
      segment1_terms, ObDASSearchCost(1), 0.0, &rtdef,
      TEST_INDEX_TABLE_ID, -1, false /* direct_bind */,
      true /* direct_drain_enabled */);
  segment0_param.set_is_scoring(false);
  segment1_param.set_is_scoring(false);
  ObDASAsyncScalarRorUnionOp segment0(search_ctx_);
  ObDASAsyncScalarRorUnionOp segment1(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, segment0.init(segment0_param));
  ASSERT_EQ(OB_SUCCESS, segment1.init(segment1_param));

  ObArray<ObIDASSearchOp *> segments;
  ASSERT_EQ(OB_SUCCESS, segments.push_back(&segment0));
  ASSERT_EQ(OB_SUCCESS, segments.push_back(&segment1));
  ObDASAsyncSegmentMergeOpParam merge_param(
      segments, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(merge_param));
  EXPECT_TRUE(merge.supports_direct_rowid_drain());
  EXPECT_EQ(nullptr, merge.get_rowid_output_exprs());
  ASSERT_EQ(OB_SUCCESS, merge.open());
  EXPECT_EQ(2, merge.direct_active_child_indices_.count());
  EXPECT_EQ(1, child_ops[0].bind_count());
  EXPECT_EQ(1, child_ops[1].bind_count());
  EXPECT_EQ(1, child_ops[2].bind_count());
  EXPECT_EQ(1, child_ops[3].bind_count());

  CollectingRowIDBatchConsumer consumer(true /* deduplicate */);
  ObDASRowID next_id_after;
  ObSEArray<uint64_t, 8> sorted_ids;
  ASSERT_EQ(OB_SUCCESS, merge.direct_drain_rowids(
      1, 3, consumer, next_id_after));
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  const uint64_t first_expected[] = {1, 2, 3};
  expect_rowids_equal(sorted_ids, first_expected, ARRAYSIZEOF(first_expected));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(4, next_id_after.get_uint64());

  consumer.reset();
  sorted_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, merge.direct_drain_rowids(
      4, 5, consumer, next_id_after));
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  const uint64_t second_expected[] = {4, 5};
  expect_rowids_equal(sorted_ids, second_expected, ARRAYSIZEOF(second_expected));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(6, next_id_after.get_uint64());

  consumer.reset();
  sorted_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, merge.direct_drain_rowids(
      6, 8, consumer, next_id_after));
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  const uint64_t third_expected[] = {6, 7, 8};
  expect_rowids_equal(sorted_ids, third_expected, ARRAYSIZEOF(third_expected));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(10, next_id_after.get_uint64());
  EXPECT_TRUE(merge.direct_child_next_rowids_.at(0).is_max());
  ASSERT_EQ(1, merge.direct_active_child_indices_.count());
  EXPECT_EQ(1, merge.direct_active_child_indices_.at(0));

  const int64_t child0_calls = child_ops[0].advance_count() + child_ops[0].drain_count()
      + child_ops[1].advance_count() + child_ops[1].drain_count();
  const int64_t child1_calls_before_gap = child_ops[2].advance_count() + child_ops[2].drain_count()
      + child_ops[3].advance_count() + child_ops[3].drain_count();
  consumer.reset();
  ASSERT_EQ(OB_SUCCESS, merge.direct_drain_rowids(
      9, 9, consumer, next_id_after));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(10, next_id_after.get_uint64());
  EXPECT_TRUE(consumer.rowids().empty());
  EXPECT_EQ(child1_calls_before_gap, child_ops[2].advance_count() + child_ops[2].drain_count()
      + child_ops[3].advance_count() + child_ops[3].drain_count());

  ASSERT_EQ(OB_SUCCESS, merge.direct_drain_rowids(
      10, 10, consumer, next_id_after));
  const uint64_t fourth_expected[] = {10};
  expect_rowids_equal(consumer.rowids(), fourth_expected, ARRAYSIZEOF(fourth_expected));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(12, next_id_after.get_uint64());
  EXPECT_EQ(1, merge.direct_active_child_indices_.count());

  consumer.reset();
  ASSERT_EQ(OB_SUCCESS, merge.direct_drain_rowids(
      11, 12, consumer, next_id_after));
  const uint64_t last_expected[] = {12};
  expect_rowids_equal(consumer.rowids(), last_expected, ARRAYSIZEOF(last_expected));
  EXPECT_TRUE(next_id_after.is_max());
  EXPECT_TRUE(merge.direct_child_next_rowids_.at(1).is_max());
  EXPECT_TRUE(merge.direct_active_child_indices_.empty());
  EXPECT_EQ(child0_calls, child_ops[0].advance_count() + child_ops[0].drain_count()
      + child_ops[1].advance_count() + child_ops[1].drain_count());

  const int64_t child1_calls = child_ops[2].advance_count() + child_ops[2].drain_count()
      + child_ops[3].advance_count() + child_ops[3].drain_count();
  consumer.reset();
  ASSERT_EQ(OB_SUCCESS, merge.direct_drain_rowids(
      13, 14, consumer, next_id_after));
  EXPECT_TRUE(next_id_after.is_max());
  EXPECT_TRUE(consumer.rowids().empty());
  EXPECT_EQ(child0_calls, child_ops[0].advance_count() + child_ops[0].drain_count()
      + child_ops[1].advance_count() + child_ops[1].drain_count());
  EXPECT_EQ(child1_calls, child_ops[2].advance_count() + child_ops[2].drain_count()
      + child_ops[3].advance_count() + child_ops[3].drain_count());

  ASSERT_EQ(OB_SUCCESS, merge.rescan());
  EXPECT_EQ(2, merge.direct_active_child_indices_.count());
  EXPECT_TRUE(merge.direct_child_next_rowids_.at(0).is_min());
  EXPECT_TRUE(merge.direct_child_next_rowids_.at(1).is_min());
  ASSERT_EQ(OB_SUCCESS, merge.direct_drain_rowids(
      1, 3, consumer, next_id_after));
  sorted_ids.reuse();
  ASSERT_EQ(OB_SUCCESS, sorted_ids.assign(consumer.rowids()));
  lib::ob_sort(sorted_ids.begin(), sorted_ids.end());
  expect_rowids_equal(sorted_ids, first_expected, ARRAYSIZEOF(first_expected));
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(4, next_id_after.get_uint64());
  EXPECT_EQ(OB_SUCCESS, merge.close());
}

TEST_F(TestDASAsyncSegmentMerge, direct_batch_concatenates_children_without_cross_child_fill)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {4});
  uint64_t frame[4] = {0, 0, 0, 0};
  ScriptedBatchSegmentOp child0(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child1(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child2(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child3(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp *children_storage[] = {&child0, &child1, &child2, &child3};
  child0.append_step(ScriptedBatchStep(OB_ITER_END, 0, {}));
  child1.append_step(ScriptedBatchStep(OB_SUCCESS, 3, {101, 102, 103}));
  child1.append_step(ScriptedBatchStep(OB_ITER_END, 2, {104, 105}));
  child2.append_step(ScriptedBatchStep(OB_ITER_END, 0, {}));
  child3.append_step(ScriptedBatchStep(OB_SUCCESS, 1, {301}));
  child3.append_step(ScriptedBatchStep(OB_ITER_END, 0, {}));

  MockSegmentOpParam child_param;
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < ARRAYSIZEOF(children_storage); ++i) {
    ASSERT_EQ(OB_SUCCESS, children_storage[i]->init(child_param));
    ASSERT_EQ(OB_SUCCESS, children.push_back(children_storage[i]));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(param));
  const ObIArray<ObExpr *> *merge_output_exprs =
      merge.get_rowid_output_exprs();
  ASSERT_NE(nullptr, merge_output_exprs);
  ASSERT_EQ(1, merge_output_exprs->count());
  EXPECT_EQ(&rowid_expr_, merge_output_exprs->at(0));
  ASSERT_EQ(OB_SUCCESS, merge.open());

  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, merge.next_rowids(4, count));
  ASSERT_EQ(3, count);
  EXPECT_EQ(std::vector<uint64_t>({101, 102, 103}),
            std::vector<uint64_t>(frame, frame + count));
  EXPECT_EQ(1, merge.current_batch_child_idx_);

  ASSERT_EQ(OB_SUCCESS, merge.next_rowids(4, count));
  ASSERT_EQ(2, count);
  EXPECT_EQ(std::vector<uint64_t>({104, 105}),
            std::vector<uint64_t>(frame, frame + count));
  EXPECT_EQ(2, merge.current_batch_child_idx_);

  ASSERT_EQ(OB_SUCCESS, merge.next_rowids(4, count));
  ASSERT_EQ(1, count);
  EXPECT_EQ(301, frame[0]);
  EXPECT_EQ(3, merge.current_batch_child_idx_);

  EXPECT_EQ(OB_ITER_END, merge.next_rowids(4, count));
  EXPECT_EQ(0, count);
  EXPECT_EQ(4, merge.current_batch_child_idx_);
  for (int64_t i = 0; i < ARRAYSIZEOF(children_storage); ++i) {
    EXPECT_EQ(1, children_storage[i]->open_count());
    EXPECT_EQ(1, children_storage[i]->bind_count());
  }
  EXPECT_EQ(OB_SUCCESS, merge.close());
}

TEST_F(TestDASAsyncSegmentMerge, direct_mode_rejects_mismatched_rowid_output_exprs)
{
  uint64_t frame[1] = {0};
  ObExpr other_rowid_expr;
  ExprFixedArray other_rowid_exprs(allocator_);
  ASSERT_EQ(OB_SUCCESS, other_rowid_exprs.init(1));
  ASSERT_EQ(OB_SUCCESS, other_rowid_exprs.push_back(&other_rowid_expr));

  ScriptedBatchSegmentOp child0(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child1(search_ctx_, frame, ARRAYSIZEOF(frame));
  child1.set_rowid_output_exprs(&other_rowid_exprs);
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, child0.init(child_param));
  ASSERT_EQ(OB_SUCCESS, child1.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child1));

  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  EXPECT_EQ(OB_STATE_NOT_MATCH, merge.init(param));
  EXPECT_EQ(nullptr, merge.get_rowid_output_exprs());
}

TEST_F(TestDASAsyncSegmentMerge, direct_batch_contract_errors_keep_child_retryable)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  uint64_t frame[4] = {0, 0, 0, 0};
  ScriptedBatchSegmentOp child0(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child1(search_ctx_, frame, ARRAYSIZEOF(frame));
  child0.append_step(ScriptedBatchStep(OB_SUCCESS, 0, {}));
  child1.append_step(ScriptedBatchStep(OB_ITER_END, 0, {}));
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, child0.init(child_param));
  ASSERT_EQ(OB_SUCCESS, child1.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child1));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(param));
  ASSERT_EQ(OB_SUCCESS, merge.open());

  int64_t count = 99;
  EXPECT_EQ(OB_ERR_UNEXPECTED, merge.next_rowids(4, count));
  EXPECT_EQ(0, count);
  EXPECT_EQ(0, merge.current_batch_child_idx_);

  child0.replace_current_step(ScriptedBatchStep(OB_TIMEOUT, 2, {11, 12}));
  count = 99;
  EXPECT_EQ(OB_TIMEOUT, merge.next_rowids(4, count));
  EXPECT_EQ(0, count);
  EXPECT_EQ(0, merge.current_batch_child_idx_);

  child0.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 2, {21, 22}));
  child0.append_step(ScriptedBatchStep(OB_ITER_END, 1, {23}));
  ASSERT_EQ(OB_SUCCESS, merge.next_rowids(4, count));
  ASSERT_EQ(2, count);
  EXPECT_EQ(21, frame[0]);
  EXPECT_EQ(22, frame[1]);
  EXPECT_EQ(0, merge.current_batch_child_idx_);

  ASSERT_EQ(OB_SUCCESS, merge.next_rowids(4, count));
  ASSERT_EQ(1, count);
  EXPECT_EQ(23, frame[0]);
  EXPECT_EQ(1, merge.current_batch_child_idx_);

  EXPECT_EQ(OB_ITER_END, merge.next_rowids(4, count));
  EXPECT_EQ(0, count);
  EXPECT_EQ(2, merge.current_batch_child_idx_);
  EXPECT_EQ(OB_SUCCESS, merge.close());
}

TEST_F(TestDASAsyncSegmentMerge, direct_mode_public_api_and_zero_sort_state)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2, 2});
  uint64_t frame[4] = {0, 0, 0, 0};
  ScriptedBatchSegmentOp child0(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child1(search_ctx_, frame, ARRAYSIZEOF(frame));
  child0.append_step(ScriptedBatchStep(OB_SUCCESS, 1, {7}));
  child0.append_step(ScriptedBatchStep(OB_ITER_END, 0, {}));
  child1.append_step(ScriptedBatchStep(OB_ITER_END, 0, {}));
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, child0.init(child_param));
  ASSERT_EQ(OB_SUCCESS, child1.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&child1));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(param));

  auto expect_zero_sort_state = [&merge]() {
    EXPECT_EQ(0, merge.head_rowids_.count());
    EXPECT_EQ(0, merge.head_scores_.count());
    EXPECT_EQ(0, merge.child_ended_.count());
    EXPECT_EQ(0, merge.heap_child_indices_.count());
    EXPECT_EQ(0, merge.pending_child_indices_.count());
    EXPECT_EQ(0, merge.batch_cursors_.count());
    EXPECT_EQ(nullptr, merge.batch_ids_);
    EXPECT_EQ(nullptr, merge.batch_scores_);
    EXPECT_EQ(nullptr, merge.batch_score_suffix_maxes_);
    EXPECT_EQ(0, merge.batch_child_capacity_);
    EXPECT_EQ(0, merge.batch_allocator_.used());
    EXPECT_EQ(0, merge.target_allocator_.used());
    EXPECT_FALSE(merge.threshold_cached_);
    EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, merge.cached_threshold_);
    EXPECT_EQ(0, merge.shallow_target_);
    EXPECT_FALSE(merge.use_batch_merge_);
    EXPECT_TRUE(merge.shallow_inclusive_);
    EXPECT_FALSE(merge.in_shallow_status_);
    EXPECT_FALSE(merge.iter_end_);
  };
  expect_zero_sort_state();
  EXPECT_FALSE(merge.is_probe_mode());
  ASSERT_EQ(OB_SUCCESS, merge.open());
  expect_zero_sort_state();

  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, merge.next_rowids(4, count));
  ASSERT_EQ(1, count);
  EXPECT_EQ(7, frame[0]);
  expect_zero_sort_state();

  ObNewVersionRange version_range;
  version_range.base_version_ = 0;
  version_range.snapshot_version_ = 1;
  ASSERT_EQ(OB_SUCCESS, merge.rescan_segment(version_range));
  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(1);
  EXPECT_EQ(OB_NOT_SUPPORTED, merge.next_rowid(rowid, score));
  EXPECT_EQ(OB_NOT_SUPPORTED, merge.advance_to(target, rowid, score));
  const MaxScoreTuple *max_score_tuple = nullptr;
  EXPECT_EQ(OB_NOT_SUPPORTED,
            merge.advance_shallow(target, true, max_score_tuple));
  EXPECT_EQ(nullptr, max_score_tuple);
  uint64_t out_ids[2] = {0, 0};
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  EXPECT_EQ(OB_NOT_SUPPORTED, merge.drain_until(
      target, ARRAYSIZEOF(out_ids), out_ids, nullptr, count,
      next_id_after, next_score_after));
  EXPECT_EQ(OB_NOT_SUPPORTED, merge.set_min_competitive_score(1.0));
  EXPECT_EQ(OB_NOT_SUPPORTED, merge.calc_max_score(score));
  expect_zero_sort_state();

  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, merge.rescan());
  expect_zero_sort_state();
  EXPECT_EQ(OB_SUCCESS, merge.close());
  expect_zero_sort_state();

  MockSegmentOp *sort_child0 = create_child({1}, {1.0});
  MockSegmentOp *sort_child1 = create_child({2}, {2.0});
  ObArray<ObIDASSearchOp *> sort_children;
  ASSERT_EQ(OB_SUCCESS, sort_children.push_back(sort_child0));
  ASSERT_EQ(OB_SUCCESS, sort_children.push_back(sort_child1));
  ObDASAsyncSegmentMergeOpParam sort_param(
      sort_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp sort_merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, sort_merge.init(sort_param));
  EXPECT_EQ(ObDASAsyncSegmentMergeMode::SORT_MERGE, sort_merge.mode_);
  EXPECT_EQ(2, sort_merge.head_rowids_.count());
  EXPECT_EQ(2, sort_merge.head_scores_.count());
  EXPECT_EQ(2, sort_merge.child_ended_.count());
  EXPECT_EQ(2, sort_merge.batch_cursors_.count());
  EXPECT_EQ(OB_SUCCESS, sort_merge.close());
}

TEST_F(TestDASAsyncSegmentMerge, direct_partition_rescan_three_one_three_has_no_stale_child)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3, 1, 3});
  uint64_t frame[2] = {0, 0};
  ScriptedBatchSegmentOp child0(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child1(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child2(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp *children_storage[] = {&child0, &child1, &child2};
  MockSegmentOpParam child_param;
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < ARRAYSIZEOF(children_storage); ++i) {
    children_storage[i]->append_step(ScriptedBatchStep(
        OB_SUCCESS, 1, {static_cast<uint64_t>(101 + i)}));
    children_storage[i]->append_step(ScriptedBatchStep(OB_ITER_END, 0, {}));
    ASSERT_EQ(OB_SUCCESS, children_storage[i]->init(child_param));
    ASSERT_EQ(OB_SUCCESS, children.push_back(children_storage[i]));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(param));
  ASSERT_EQ(OB_SUCCESS, merge.open());

  auto pull_phase = [&](const std::vector<uint64_t> &expected) {
    std::vector<uint64_t> actual;
    int64_t count = -1;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = merge.next_rowids(ARRAYSIZEOF(frame), count))) {
      ASSERT_EQ(1, count);
      actual.push_back(frame[0]);
    }
    EXPECT_EQ(OB_ITER_END, ret);
    EXPECT_EQ(expected, actual);
    EXPECT_EQ(0, count);
    EXPECT_EQ(merge.active_segment_cnt_, merge.current_batch_child_idx_);
  };

  EXPECT_EQ(3, merge.active_segment_cnt_);
  pull_phase({101, 102, 103});
  EXPECT_EQ(2, child1.batch_call_count());
  EXPECT_EQ(2, child2.batch_call_count());

  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, merge.rescan());
  EXPECT_EQ(1, merge.active_segment_cnt_);
  EXPECT_EQ(0, merge.current_batch_child_idx_);
  child0.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 1, {201}));
  pull_phase({201});
  EXPECT_EQ(2, child1.batch_call_count());
  EXPECT_EQ(2, child2.batch_call_count());
  EXPECT_EQ(1, child0.rescan_count());
  EXPECT_EQ(0, child1.rescan_count());
  EXPECT_EQ(0, child2.rescan_count());

  select_partition(catalog, 3);
  ASSERT_EQ(OB_SUCCESS, merge.rescan());
  EXPECT_EQ(3, merge.active_segment_cnt_);
  EXPECT_EQ(0, merge.current_batch_child_idx_);
  child0.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 1, {301}));
  child1.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 1, {302}));
  child2.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 1, {303}));
  pull_phase({301, 302, 303});
  EXPECT_EQ(2, child0.rescan_count());
  EXPECT_EQ(1, child1.rescan_count());
  EXPECT_EQ(1, child2.rescan_count());
  EXPECT_EQ(6, child0.batch_call_count());
  EXPECT_EQ(4, child1.batch_call_count());
  EXPECT_EQ(4, child2.batch_call_count());
  EXPECT_EQ(3, child0.bind_count());
  EXPECT_EQ(2, child1.bind_count());
  EXPECT_EQ(2, child2.bind_count());
  EXPECT_EQ(OB_SUCCESS, merge.close());
}

TEST_F(TestDASAsyncSegmentMerge, bitmap_over_direct_rescan_three_one_three_clears_old_rows)
{
  static const int64_t BATCH_SIZE = 4;
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3, 1, 3});

  uint64_t frame[BATCH_SIZE] = {};
  char **frames = static_cast<char **>(allocator_.alloc(sizeof(char *)));
  ASSERT_NE(nullptr, frames);
  frames[0] = static_cast<char *>(allocator_.alloc(sizeof(VectorHeader)));
  ASSERT_NE(nullptr, frames[0]);
  exec_ctx_.frames_ = frames;
  eval_ctx_.frames_ = frames;
  void *null_buf = allocator_.alloc(ObBitVector::memory_size(BATCH_SIZE));
  ASSERT_NE(nullptr, null_buf);
  ObBitVector *nulls = reinterpret_cast<ObBitVector *>(null_buf);
  nulls->init(BATCH_SIZE);
  VectorHeader *vector_header = new (frames[0]) VectorHeader();
  vector_header->set_format(VEC_FIXED);
  new (vector_header->vector_buf_)
      ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_UINTEGER>>(
          reinterpret_cast<char *>(frame), nulls);
  ObExpr rowid_expr;
  rowid_expr.frame_idx_ = 0;
  rowid_expr.vector_header_off_ = 0;
  ASSERT_EQ(1, rowid_exprs_.count());
  rowid_exprs_.at(0) = &rowid_expr;
  search_ctx_.table_row_count_ = ObDASSearchCost(16);

  ObDASScalarScanCtDef scalar_ctdef(allocator_);
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.push_back(&rowid_expr));
  ObDASScalarScanRtDef scalar_rtdef;
  scalar_rtdef.ctdef_ = &scalar_ctdef;

  ScriptedBatchSegmentOp child0(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child1(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp child2(search_ctx_, frame, ARRAYSIZEOF(frame));
  ScriptedBatchSegmentOp *children_storage[] = {&child0, &child1, &child2};
  const uint64_t initial_values[] = {13, 11, 12};
  MockSegmentOpParam child_param;
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < ARRAYSIZEOF(children_storage); ++i) {
    children_storage[i]->append_step(
        ScriptedBatchStep(OB_SUCCESS, 1, {initial_values[i]}));
    children_storage[i]->append_step(
        ScriptedBatchStep(OB_ITER_END, 0, {}));
    ASSERT_EQ(OB_SUCCESS, children_storage[i]->init(child_param));
    ASSERT_EQ(OB_SUCCESS, children.push_back(children_storage[i]));
  }

  ObDASAsyncSegmentMergeOpParam merge_param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(merge_param));
  ObDASBitmapOpParam bitmap_param(&merge, &scalar_ctdef, &scalar_rtdef);
  ObDASBitmapOp bitmap(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, bitmap.init(bitmap_param));
  EXPECT_EQ(&rowid_expr, bitmap.rowid_expr_);
  ASSERT_EQ(OB_SUCCESS, bitmap.open());

  auto pull_phase = [&](const std::vector<uint64_t> &expected) {
    std::vector<uint64_t> actual;
    ObDASRowID rowid;
    double score = -1.0;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = bitmap.next_rowid(rowid, score))) {
      actual.push_back(rowid.get_uint64());
      EXPECT_DOUBLE_EQ(0.0, score);
    }
    EXPECT_EQ(OB_ITER_END, ret);
    EXPECT_EQ(expected, actual);
  };

  pull_phase({11, 12, 13});

  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, bitmap.rescan());
  child0.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 1, {21}));
  pull_phase({21});
  EXPECT_EQ(0, child1.rescan_count());
  EXPECT_EQ(0, child2.rescan_count());

  select_partition(catalog, 3);
  ASSERT_EQ(OB_SUCCESS, bitmap.rescan());
  child0.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 1, {33}));
  child1.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 1, {31}));
  child2.replace_current_step(ScriptedBatchStep(OB_SUCCESS, 1, {32}));
  pull_phase({31, 32, 33});
  EXPECT_EQ(2, child0.rescan_count());
  EXPECT_EQ(1, child1.rescan_count());
  EXPECT_EQ(1, child2.rescan_count());
  EXPECT_EQ(OB_SUCCESS, bitmap.close());
}

TEST_F(TestDASAsyncSegmentMerge, sort_over_generic_child_preserves_eof_tail_batch)
{
  static const int64_t BATCH_SIZE = 4;
  ScopedSortTmpFileManager tmp_file_manager;
  ASSERT_EQ(OB_SUCCESS, tmp_file_manager.init());
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  exec_ctx_.set_my_session(&session);
  uint64_t frame[BATCH_SIZE] = {};
  char **frames = static_cast<char **>(allocator_.alloc(sizeof(char *)));
  ASSERT_NE(nullptr, frames);
  frames[0] = static_cast<char *>(allocator_.alloc(sizeof(VectorHeader)));
  ASSERT_NE(nullptr, frames[0]);
  exec_ctx_.frames_ = frames;
  eval_ctx_.frames_ = frames;
  void *null_buf = allocator_.alloc(ObBitVector::memory_size(BATCH_SIZE));
  ASSERT_NE(nullptr, null_buf);
  ObBitVector *nulls = reinterpret_cast<ObBitVector *>(null_buf);
  nulls->init(BATCH_SIZE);
  VectorHeader *vector_header = new (frames[0]) VectorHeader();
  vector_header->set_format(VEC_FIXED);
  new (vector_header->vector_buf_)
      ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_UINTEGER>>(
          reinterpret_cast<char *>(frame), nulls);

  ObExpr rowid_expr;
  rowid_expr.type_ = T_REF_COLUMN;
  rowid_expr.max_length_ = sizeof(uint64_t);
  rowid_expr.is_fixed_length_data_ = true;
  rowid_expr.len_ = sizeof(uint64_t);
  rowid_expr.datum_meta_.type_ = ObUInt64Type;
  rowid_expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
  rowid_expr.basic_funcs_ = ObDatumFuncs::get_basic_func(
      ObUInt64Type, CS_TYPE_BINARY);
  ASSERT_NE(nullptr, rowid_expr.basic_funcs_);
  rowid_expr.frame_idx_ = 0;
  rowid_expr.vector_header_off_ = 0;
  ASSERT_EQ(1, rowid_exprs_.count());
  rowid_exprs_.at(0) = &rowid_expr;
  search_ctx_.table_row_count_ = ObDASSearchCost(16);

  ObDASScalarScanCtDef scalar_ctdef(allocator_);
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scalar_ctdef.rowkey_exprs_.push_back(&rowid_expr));
  ObDASScalarScanRtDef scalar_rtdef;
  scalar_rtdef.ctdef_ = &scalar_ctdef;

  ScriptedBatchSegmentOp child(search_ctx_, frame, ARRAYSIZEOF(frame));
  child.append_step(ScriptedBatchStep(OB_ITER_END, 3, {13, 11, 12}));
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, child.init(child_param));

  ObDASSortOpParam sort_param(&child, &scalar_ctdef, &scalar_rtdef);
  ObDASSortOp sort(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, sort.init(sort_param));
  ASSERT_EQ(OB_SUCCESS, sort.open());
  ASSERT_EQ(OB_SUCCESS, sort.do_sort());
  EXPECT_TRUE(sort.sort_finished_);
  EXPECT_EQ(2, child.batch_call_count());
  ASSERT_NE(nullptr, sort.sort_impl_);
  EXPECT_EQ(3, sort.sort_impl_->datum_store_.get_row_cnt());
  EXPECT_EQ(OB_SUCCESS, sort.close());
}

// Keep storage batches scripted, but execute the materializers and BMM themselves.
class TestDASMaterializedRange : public TestDASAsyncSegmentMerge
{
public:
  static void SetUpTestCase()
  {
    const int ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_ENTRY_EXIST == ret);
  }

  void SetUp() override
  {
    TestDASAsyncSegmentMerge::SetUp();
    prepare_const_expr_frame(65536);
    int64_t pos = 0;
    init_batch_expr(rowid_expr_, pos, ObUInt64Type);
    ASSERT_NO_FATAL_FAILURE(init_fixed_frame());
    search_ctx_.table_row_count_ = ObDASSearchCost(100000);
  }

  void init_batch_expr(ObExpr &expr, int64_t &pos, const ObObjType type)
  {
    expr.type_ = T_REF_COLUMN;
    expr.obj_meta_.set_type(type);
    expr.obj_meta_.set_collation_type(CS_TYPE_BINARY);
    expr.datum_meta_.type_ = type;
    expr.datum_meta_.cs_type_ = CS_TYPE_BINARY;
    expr.obj_datum_map_ = ObUInt64Type == type ? OBJ_DATUM_8BYTE_DATA : OBJ_DATUM_STRING;
    expr.basic_funcs_ = ObDatumFuncs::get_basic_func(type, CS_TYPE_BINARY);
    ASSERT_NE(nullptr, expr.basic_funcs_);
    expr.is_fixed_length_data_ = ObUInt64Type == type;
    expr.len_ = sizeof(uint64_t);
    expr.max_length_ = ObUInt64Type == type ? sizeof(uint64_t) : SEARCH_INDEX_NESTED_KEY_LENGTH;
    expr.batch_result_ = true;
    expr.batch_idx_mask_ = UINT64_MAX;
    expr.vec_value_tc_ = get_vec_value_tc(type, expr.datum_meta_.scale_, expr.datum_meta_.precision_);
    expr.frame_idx_ = 0;
    expr.datum_off_ = pos;
    pos += sizeof(ObDatum) * eval_ctx_.max_batch_size_;
    expr.eval_info_off_ = pos;
    pos += sizeof(ObEvalInfo);
    expr.res_buf_off_ = pos;
    expr.res_buf_len_ = sizeof(uint64_t);
    pos += sizeof(uint64_t) * eval_ctx_.max_batch_size_;
    expr.vector_header_off_ = pos;
    pos += sizeof(VectorHeader);
    new (eval_ctx_.frames_[0] + expr.vector_header_off_) VectorHeader();
  }

  void init_fixed_frame()
  {
    ObBitVector *nulls = reinterpret_cast<ObBitVector *>(
        allocator_.alloc(ObBitVector::memory_size(eval_ctx_.max_batch_size_)));
    ASSERT_NE(nullptr, nulls);
    nulls->init(eval_ctx_.max_batch_size_);
    VectorHeader *header = reinterpret_cast<VectorHeader *>(
        eval_ctx_.frames_[0] + rowid_expr_.vector_header_off_);
    header->set_format(VEC_FIXED);
    new (header->vector_buf_) ObFixedLengthVector<uint64_t, VectorBasicOp<VEC_TC_UINTEGER>>(
        reinterpret_cast<char *>(batch_frame_), nulls);
  }

  void init_range(const std::vector<std::vector<ScriptedBatchStep>> &segments,
                  ObDASScalarScanCtDef &ctdef, ObDASScalarScanRtDef &rtdef,
                  const double boost,
                  std::vector<std::unique_ptr<ScriptedBatchSegmentOp>> &producers,
                  ObDASBitmapOp *&bitmap)
  {
    ASSERT_EQ(OB_SUCCESS, init_async_scalar_scan(ctdef, rtdef, rowid_expr_));
    rtdef.set_boost_weight(boost);
    ObArray<ObIDASSearchOp *> children;
    for (const auto &steps : segments) {
      std::unique_ptr<ScriptedBatchSegmentOp> child(
          new ScriptedBatchSegmentOp(search_ctx_, batch_frame_, ARRAYSIZEOF(batch_frame_)));
      for (const auto &step : steps) {
        child->append_step(step);
      }
      MockSegmentOpParam param;
      ASSERT_EQ(OB_SUCCESS, child->init(param));
      ASSERT_EQ(OB_SUCCESS, children.push_back(child.get()));
      producers.push_back(std::move(child));
    }
    ObDASAsyncSegmentMergeOpParam merge_param(
        children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
        ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
    ObDASAsyncSegmentMergeOp *merge = nullptr;
    ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(merge_param, merge));
    ObDASBitmapOpParam bitmap_param(merge, &ctdef, &rtdef);
    bitmap_param.set_is_scoring(boost > 0.0);
    ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(bitmap_param, bitmap));
  }

  void init_bmm(ObDASBMMOp &bmm, const ObIArray<ObIDASSearchOp *> &scores,
                ObIDASSearchOp *filter, const bool optional, const bool sparse)
  {
    ObSEArray<ObDASSearchCost, 3> costs;
    for (int64_t i = 0; i < scores.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, costs.push_back(ObDASSearchCost(100)));
    }
    ObDASBMMBuildOptions param_options;
    param_options.query_optional_ = optional;
    param_options.lead_cost_ = ObDASSearchCost(sparse ? 1 : 1001);
    param_options.filter_cost_ = ObDASSearchCost(10);
    ObDASBMMOpParam param(scores, filter, allocator_, param_options, &costs);
    param.set_is_scoring(true);
    ASSERT_EQ(OB_SUCCESS, bmm.init(param));
    ASSERT_TRUE(bmm.enable_vec_merge_);
    ASSERT_EQ(sparse ? ObDASBMMOp::FilterMergeMode::SPARSE_SEEK
                     : ObDASBMMOp::FilterMergeMode::DENSE_WINDOW,
        bmm.filter_merge_mode_);
  }

  void expect_rows(ObIDASSearchOp &op, const std::vector<BMMReferenceResult> &expected)
  {
    ObDASRowID rowid;
    double score = -1.0;
    for (const auto &row : expected) {
      ASSERT_EQ(OB_SUCCESS, op.next_rowid(rowid, score));
      EXPECT_EQ(row.doc_id_, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(row.score_, score);
    }
    EXPECT_EQ(OB_ITER_END, op.next_rowid(rowid, score));
  }

protected:
  uint64_t batch_frame_[256] = {};
};

// Feed a real Bitmap over DIRECT_MERGE into dense, sparse, and query-optional BMM, checking
// filtered scores and the initial-to-positive threshold transition against expected results.
// Verify partial-consumption rescans across 3/1/3 segments and recovery from a batch input error.
TEST_F(TestDASMaterializedRange, materialized_range_filter_drives_vectorized_bmm_lifecycle)
{
  BMMVectorizedMergeConfigGuard config;
  ASSERT_TRUE(config.is_valid());
  struct Mode { bool sparse_; bool optional_; };
  const Mode modes[] = {{false, false}, {true, false}, {true, true}};
  const std::vector<ScriptedBMMPostingList> postings = {
      {{3, 2, 32768, 2}, {5, 2, 32768, 2}, {7, 2, 32768, 2}, {17001, 2, 32768, 2}},
      {{1, 1, 32768, 3}, {3, 3, 32768, 3}, {17001, 3, 32768, 3}},
      {{3, 4, 32768, 4}, {5, 4, 32768, 4}, {17001, 4, 32768, 4}}};
  for (const Mode &mode : modes) {
    SCOPED_TRACE(testing::Message() << "sparse=" << mode.sparse_ << " optional=" << mode.optional_);
    ObDASAsyncIndexQueryCtx catalog;
    build_catalog(catalog, {3, 1, 3});
    std::vector<std::unique_ptr<ScriptedBatchSegmentOp>> producers;
    ObDASScalarScanCtDef ctdef(allocator_);
    ObDASScalarScanRtDef rtdef;
    ObDASBitmapOp *filter = nullptr;
    ASSERT_NO_FATAL_FAILURE(init_range({
        {{OB_SUCCESS, 3, {17001, 3, 0}}, {OB_ITER_END, 2, {5, 1}}},
        {{OB_ITER_END, 0, {}}},
        {{OB_ITER_END, 3, {9, 3, 17001}}}}, ctdef, rtdef, 0, producers, filter));
    ScriptedBMMSearchOp a(search_ctx_, postings[0], 2);
    ScriptedBMMSearchOp b(search_ctx_, postings[1], 3);
    ScriptedBMMSearchOp c(search_ctx_, postings[2], 4);
    ObSEArray<ObIDASSearchOp *, 3> scores;
    for (ScriptedBMMSearchOp *child : {&a, &b, &c}) {
      ScriptedBMMSearchOpParam param;
      param.set_is_scoring(true);
      child->use_batch_drain(true);
      ASSERT_EQ(OB_SUCCESS, child->init(param));
      ASSERT_EQ(OB_SUCCESS, scores.push_back(child));
    }
    ObDASBMMOp bmm(search_ctx_);
    ASSERT_NO_FATAL_FAILURE(init_bmm(bmm, scores, filter, mode.optional_, mode.sparse_));
    ASSERT_EQ(OB_SUCCESS, bmm.open());
    if (mode.optional_) {
      ObDASRowID rowid;
      double score = -1;
      ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
      EXPECT_EQ(0, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(0, score);
    }
    ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5));
    ScriptedBMMPostingList filter_rows = {{0, 0, 0, 0}, {1, 0, 0, 0}, {3, 0, 0, 0},
        {5, 0, 0, 0}, {9, 0, 0, 0}, {17001, 0, 0, 0}};
    ASSERT_NO_FATAL_FAILURE(expect_rows(bmm, BMMReferenceOracle::evaluate(postings, &filter_rows, 5)));

    // Rescan a partially consumed BMM into a smaller partition, then expand again.
    ASSERT_EQ(OB_SUCCESS, bmm.rescan());
    ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5));
    ObDASRowID rowid;
    double score = 0;
    ASSERT_EQ(OB_SUCCESS, bmm.next_rowid(rowid, score));
    ASSERT_EQ(3, rowid.get_uint64());
    const ObDASRowID target = rowid;
    ASSERT_EQ(OB_SUCCESS, bmm.advance_to(target, rowid, score));
    EXPECT_EQ(3, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(9, score);
    select_partition(catalog, 2);
    ASSERT_EQ(OB_SUCCESS, bmm.rescan());
    producers[0]->replace_current_step({OB_ITER_END, 1, {5}});
    ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5));
    ASSERT_NO_FATAL_FAILURE(expect_rows(bmm, {{5, 6}}));
    EXPECT_EQ(1, producers[1]->rescan_count()); // Only the earlier same-partition rescan.
    select_partition(catalog, 3);
    ASSERT_EQ(OB_SUCCESS, bmm.rescan());
    producers[0]->replace_current_step({OB_ITER_END, 1, {17001}});
    producers[2]->replace_current_step({OB_ITER_END, 1, {3}});
    ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5));
    ASSERT_NO_FATAL_FAILURE(expect_rows(bmm, {{3, 9}, {17001, 9}}));

    // Fail after one child has populated the bitmap; rescan must rebuild all input.
    ASSERT_EQ(OB_SUCCESS, bmm.rescan());
    producers[2]->replace_current_step({OB_TIMEOUT, 0, {}});
    EXPECT_EQ(OB_TIMEOUT, bmm.next_rowid(rowid, score));
    ASSERT_EQ(OB_SUCCESS, bmm.rescan());
    producers[2]->replace_current_step({OB_ITER_END, 1, {3}});
    ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(5));
    ASSERT_NO_FATAL_FAILURE(expect_rows(bmm, {{3, 9}, {17001, 9}}));
    EXPECT_EQ(OB_SUCCESS, bmm.close());
    EXPECT_EQ(OB_SUCCESS, filter->close());
  }
}

// Materialize three scoring range clauses and verify duplicate segment hits count once per
// clause while boosts accumulate across clauses. Check thresholded output and real TopK
// feedback against an independent oracle built from the raw document sets and clause boosts.
TEST_F(TestDASMaterializedRange, materialized_range_disjunction_matches_score_and_topk_oracle)
{
  BMMVectorizedMergeConfigGuard config;
  ASSERT_TRUE(config.is_valid());
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  std::vector<std::unique_ptr<ScriptedBatchSegmentOp>> producers;
  ObDASScalarScanCtDef ctdef_a(allocator_), ctdef_b(allocator_), ctdef_c(allocator_);
  ObDASScalarScanRtDef rtdef_a, rtdef_b, rtdef_c;
  ObDASBitmapOp *a = nullptr, *b = nullptr, *c = nullptr;
  ASSERT_NO_FATAL_FAILURE(init_range({{{OB_ITER_END, 3, {17001, 1, 3}}},
      {{OB_ITER_END, 2, {3, 17002}}}, {{OB_ITER_END, 1, {17001}}}},
      ctdef_a, rtdef_a, 2, producers, a));
  ASSERT_NO_FATAL_FAILURE(init_range({{{OB_ITER_END, 2, {17002, 2}}},
      {{OB_ITER_END, 2, {3, 17001}}}, {{OB_ITER_END, 1, {3}}}},
      ctdef_b, rtdef_b, 3, producers, b));
  ASSERT_NO_FATAL_FAILURE(init_range({{{OB_ITER_END, 2, {17001, 4}}},
      {{OB_ITER_END, 0, {}}}, {{OB_ITER_END, 1, {17001}}}},
      ctdef_c, rtdef_c, 4, producers, c));
  ObSEArray<ObIDASSearchOp *, 3> scores;
  ASSERT_EQ(OB_SUCCESS, scores.push_back(a));
  ASSERT_EQ(OB_SUCCESS, scores.push_back(b));
  ASSERT_EQ(OB_SUCCESS, scores.push_back(c));
  ObDASBMMOp bmm(search_ctx_);
  ASSERT_NO_FATAL_FAILURE(init_bmm(bmm, scores, nullptr, false, false));
  ObDASTopKCollectOpParam param;
  param.limit_ = 2;
  param.child_ = &bmm;
  param.set_is_scoring(true);
  param.dynamic_pruning_enabled_ = true;
  ObDASTopKCollectOp topk(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, topk.init(param));
  ASSERT_TRUE(topk.dynamic_pruning_enabled_);
  ASSERT_EQ(OB_SUCCESS, topk.open());
  // Deduplicate within a clause only. Different clauses contribute independently.
  const std::vector<std::vector<uint64_t>> raw_ids = {
      {17001, 1, 3, 3, 17002, 17001}, {17002, 2, 3, 17001, 3}, {17001, 4, 17001}};
  const double boosts[] = {2, 3, 4};
  std::map<uint64_t, double> score_by_id;
  for (int64_t i = 0; i < 3; ++i) {
    for (uint64_t id : std::set<uint64_t>(raw_ids[i].begin(), raw_ids[i].end())) {
      score_by_id[id] += boosts[i];
    }
  }
  std::vector<BMMReferenceResult> exhaustive;
  for (const auto &entry : score_by_id) {
    exhaustive.emplace_back(entry.first, entry.second);
  }
  ASSERT_NO_FATAL_FAILURE(expect_rows(bmm, exhaustive));
  ASSERT_EQ(OB_SUCCESS, bmm.rescan());
  ASSERT_EQ(OB_SUCCESS, bmm.set_min_competitive_score(4));
  ASSERT_NO_FATAL_FAILURE(expect_rows(bmm, {{3, 5}, {17001, 9}, {17002, 5}}));
  // Reset both collector and child before exercising actual threshold feedback.
  ASSERT_EQ(OB_SUCCESS, topk.rescan());
  std::vector<BMMReferenceResult> actual;
  ObDASRowID rowid;
  double score = 0;
  int ret = OB_SUCCESS;
  while (OB_SUCCESS == (ret = topk.next_rowid(rowid, score))) {
    actual.emplace_back(rowid.get_uint64(), score);
  }
  ASSERT_EQ(OB_ITER_END, ret);
  std::stable_sort(exhaustive.begin(), exhaustive.end(),
      [](const BMMReferenceResult &l, const BMMReferenceResult &r) { return l.score_ > r.score_; });
  exhaustive.erase(exhaustive.begin() + 2, exhaustive.end());
  auto by_id = [](const BMMReferenceResult &l, const BMMReferenceResult &r) {
    return l.doc_id_ < r.doc_id_;
  };
  std::sort(exhaustive.begin(), exhaustive.end(), by_id);
  std::sort(actual.begin(), actual.end(), by_id);
  ASSERT_EQ(exhaustive.size(), actual.size());
  for (size_t i = 0; i < actual.size(); ++i) {
    EXPECT_EQ(exhaustive[i].doc_id_, actual[i].doc_id_);
    EXPECT_DOUBLE_EQ(exhaustive[i].score_, actual[i].score_);
  }
  EXPECT_DOUBLE_EQ(5, bmm.min_competitive_score_);
  EXPECT_EQ(OB_SUCCESS, topk.close());
  EXPECT_EQ(OB_SUCCESS, bmm.close());
  EXPECT_EQ(OB_SUCCESS, a->close());
  EXPECT_EQ(OB_SUCCESS, b->close());
  EXPECT_EQ(OB_SUCCESS, c->close());
}

// Real group merge and BMM operators must forward Top-K thresholds to the next
// group, then reset them when partition rescans activate fewer/more groups.
TEST_F(TestDASMaterializedRange, group_bmm_topk_feedback_survives_partition_rescan)
{
  BMMVectorizedMergeConfigGuard config;
  ASSERT_TRUE(config.is_valid());
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1, 2, 1});
  const ScriptedBMMPostingList first = {{1, 1, 32768, 5}, {2, 2, 32768, 5}, {3, 5, 32768, 5}};
  const ScriptedBMMPostingList second = {{10, 1, 32768, 9}, {11, 6, 32768, 9}, {20000, 9, 32768, 9}};
  ScriptedBMMSearchOp a(search_ctx_, first, 5), b(search_ctx_, second, 9);
  ScriptedBMMSearchOpParam leaf_param;
  leaf_param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, a.init(leaf_param));
  ASSERT_EQ(OB_SUCCESS, b.init(leaf_param));
  a.use_batch_drain(true);
  b.use_batch_drain(true);
  ObSEArray<ObIDASSearchOp *, 1> scores_a, scores_b;
  ASSERT_EQ(OB_SUCCESS, scores_a.push_back(&a));
  ASSERT_EQ(OB_SUCCESS, scores_b.push_back(&b));
  ObDASBMMOp bmm_a(search_ctx_), bmm_b(search_ctx_);
  ASSERT_NO_FATAL_FAILURE(init_bmm(bmm_a, scores_a, nullptr, false, false));
  ASSERT_NO_FATAL_FAILURE(init_bmm(bmm_b, scores_b, nullptr, false, false));
  ObSEArray<ObIDASSearchOp *, 2> groups;
  ASSERT_EQ(OB_SUCCESS, groups.push_back(&bmm_a));
  ASSERT_EQ(OB_SUCCESS, groups.push_back(&bmm_b));
  ObDASAsyncSegmentGroupMergeOpParam group_param(groups);
  group_param.set_is_scoring(true);
  ObDASAsyncSegmentGroupMergeOp merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, merge.init(group_param));
  ObDASTopKCollectOpParam topk_param;
  topk_param.child_ = &merge;
  topk_param.limit_ = 2;
  topk_param.set_is_scoring(true);
  topk_param.dynamic_pruning_enabled_ = true;
  ObDASTopKCollectOp topk(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, topk.init(topk_param));
  ASSERT_TRUE(topk.dynamic_pruning_enabled_);
  ASSERT_EQ(OB_SUCCESS, topk.open());
  for (int64_t phase = 0; phase < 3; ++phase) {
    SCOPED_TRACE(phase);
    if (phase > 0) {
      select_partition(catalog, phase + 1);
      ASSERT_EQ(OB_SUCCESS, topk.rescan());
      // Rescan is deferred until the group is prepared for its first read.
      EXPECT_FALSE(merge.child_prepared_.at(0));
      EXPECT_DOUBLE_EQ(2, bmm_a.min_competitive_score_);
      // Leave the second rescan unprepared to exercise the normal next_rowid path.
      if (1 == phase) {
        ASSERT_EQ(OB_SUCCESS, merge.ensure_child_ready(0));
        EXPECT_DOUBLE_EQ(DAS_SEARCH_INITIAL_COMPETITIVE_SCORE, bmm_a.min_competitive_score_);
      }
    }
    EXPECT_EQ(phase == 1 ? 2 : 1, merge.active_group_cnt_);
    std::vector<std::pair<uint64_t, double>> actual;
    ObDASRowID rowid;
    double score = 0;
    int ret = OB_SUCCESS;
    while (OB_SUCCESS == (ret = topk.next_rowid(rowid, score))) {
      actual.emplace_back(rowid.get_uint64(), score);
    }
    ASSERT_EQ(OB_ITER_END, ret);
    std::sort(actual.begin(), actual.end());
    const std::vector<std::pair<uint64_t, double>> expected = phase == 1
        ? std::vector<std::pair<uint64_t, double>>{{11, 6}, {20000, 9}}
        : std::vector<std::pair<uint64_t, double>>{{2, 2}, {3, 5}};
    EXPECT_EQ(expected, actual);
    // The exhausted first group no longer receives the next group's threshold.
    EXPECT_DOUBLE_EQ(2, bmm_a.min_competitive_score_);
    if (phase == 1) {
      EXPECT_DOUBLE_EQ(6, bmm_b.min_competitive_score_);
    }
  }
  EXPECT_EQ(OB_SUCCESS, topk.close());
}

// Each call reuses the producer frame, including after Sort switches it to uniform output.
class NestedRangeBatchOp : public ObIDASSearchOp
{
public:
  using Row = std::pair<uint64_t, std::string>;
  NestedRangeBatchOp(ObDASSearchCtx &ctx, const std::vector<Row> &rows)
    : ObIDASSearchOp(ctx), rows_(rows), pos_(0),
      exprs_(ctx.get_scope(DAS_SCOPE_NESTED_ASYNC).rowid_exprs_)
  {}
  const ObIArray<ObExpr *> *get_rowid_output_exprs() const override { return exprs_; }
  void set_rows(const std::vector<Row> &rows) { rows_ = rows; }
private:
  int do_open() override { pos_ = 0; return OB_SUCCESS; }
  int do_close() override { return OB_SUCCESS; }
  int do_rescan() override { pos_ = 0; return OB_SUCCESS; }
  int do_rescan_segment(const ObNewVersionRange &range) override
  { UNUSED(range); return OB_SUCCESS; }
  int do_next_rowid(ObDASRowID &id, double &score) override
  { UNUSED(id); UNUSED(score); return OB_NOT_SUPPORTED; }
  int do_advance_to(const ObDASRowID &target, ObDASRowID &id, double &score) override
  { UNUSED(target); UNUSED(id); UNUSED(score); return OB_NOT_SUPPORTED; }
  int do_next_rowids(const int64_t capacity, int64_t &count) override
  {
    int ret = OB_SUCCESS;
    count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs_->count(); ++i) {
      if (OB_FAIL(exprs_->at(i)->init_vector(eval_ctx(), VEC_UNIFORM, capacity))) {
        LOG_WARN("failed to initialize nested test batch", K(ret), K(i));
      }
    }
    while (OB_SUCC(ret) && count < capacity && pos_ < rows_.size()) {
      const Row &row = rows_[pos_++];
      exprs_->at(0)->locate_batch_datums(eval_ctx())[count] = ObDatum(
          reinterpret_cast<const char *>(&row.first), sizeof(row.first), false);
      exprs_->at(1)->locate_batch_datums(eval_ctx())[count] = ObDatum(
          row.second.data(), row.second.size(), false);
      ++count;
    }
    if (OB_SUCC(ret) && pos_ == rows_.size()) {
      ret = OB_ITER_END; // Intentionally preserve a nonempty terminal batch.
    }
    return ret;
  }
  std::vector<Row> rows_;
  size_t pos_;
  const ObIArray<ObExpr *> *exprs_;
};

// Consume nested range batches through DIRECT_MERGE and a global UniqueSort, comparing ordered
// document/key pairs with an oracle that filters DOCID_RANGE, truncates keys, and removes duplicates.
// Verify EOF tail batches and 3/1/3-segment rescans preserve distinct keys without leaking old rows.
TEST_F(TestDASMaterializedRange, nested_global_unique_sort_preserves_identity_and_rescan)
{
  ScopedSortTmpFileManager tmp_file_manager;
  ASSERT_EQ(OB_SUCCESS, tmp_file_manager.init());
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, session.test_init(0, 0, 0, nullptr));
  exec_ctx_.set_my_session(&session);
  // A small batch also places duplicate identities on different Sort output batches.
  eval_ctx_.max_batch_size_ = 2;
  search_ctx_.mock_skip_ = reinterpret_cast<ObBitVector *>(
      allocator_.alloc(ObBitVector::memory_size(eval_ctx_.max_batch_size_)));
  ASSERT_NE(nullptr, search_ctx_.mock_skip_);
  search_ctx_.mock_skip_->init(eval_ctx_.max_batch_size_);
  ObExpr doc_expr, key_expr;
  int64_t pos = 0;
  ASSERT_NO_FATAL_FAILURE(init_batch_expr(doc_expr, pos, ObUInt64Type));
  ASSERT_NO_FATAL_FAILURE(init_batch_expr(key_expr, pos, ObVarcharType));
  ExprFixedArray exprs(allocator_);
  ASSERT_EQ(OB_SUCCESS, exprs.init(2));
  ASSERT_EQ(OB_SUCCESS, exprs.push_back(&doc_expr));
  ASSERT_EQ(OB_SUCCESS, exprs.push_back(&key_expr));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.register_nested_scope(&exprs, DAS_SCOPE_NESTED_ASYNC));
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3, 1, 3});
  ObDASScalarScanCtDef ctdef(allocator_);
  ctdef.nesting_depth_for_full_subtree_ = 1;
  ObDASScalarScanRtDef rtdef;
  rtdef.ctdef_ = &ctdef;
  const std::string key_a("\0\0\0\1", 4);
  const std::string key_b("\0\0\0\2", 4);
  using Row = NestedRangeBatchOp::Row;
  std::vector<std::vector<Row>> input = {
      {{8, key_a}, {3, key_a + key_a}, {2, key_a}, {3, key_b}},
      {}, {{3, key_a + key_b}, {5, key_a}, {3, key_b}, {6, key_b}}};
  ObDASScopeId old_scope;
  const int32_t old_depth = search_ctx_.get_op_creating_depth();
  search_ctx_.enter_scope_for_op_creating(DAS_SCOPE_NESTED_ASYNC, 1, old_scope);
  NestedRangeBatchOp child0(search_ctx_, input[0]);
  NestedRangeBatchOp child1(search_ctx_, input[1]);
  NestedRangeBatchOp child2(search_ctx_, input[2]);
  ObDASAsyncSegmentMergeOp merge(search_ctx_);
  ObDASSortOp sort(search_ctx_);
  search_ctx_.exit_scope_after_op_created(old_scope, old_depth);
  MockSegmentOpParam child_param;
  ObArray<ObIDASSearchOp *> children;
  for (NestedRangeBatchOp *child : {&child0, &child1, &child2}) {
    ASSERT_EQ(OB_SUCCESS, child->init(child_param));
    ASSERT_EQ(OB_SUCCESS, children.push_back(child));
  }
  ObDASAsyncSegmentMergeOpParam merge_param(children,
      ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID), true,
      ObDASAsyncSegmentMergeMode::DIRECT_MERGE);
  ASSERT_EQ(OB_SUCCESS, merge.init(merge_param));
  ObDASSortOpParam sort_param(&merge, &ctdef, &rtdef, true);
  ASSERT_EQ(OB_SUCCESS, sort.init(sort_param));
  ASSERT_EQ(DAS_ROWID_TYPE_UINT64, sort.get_rowid_type());
  ASSERT_TRUE(sort.unique_sort_);
  ASSERT_EQ(1, sort.nk_truncate_depth_);
  auto set_range = [&](uint64_t lo, uint64_t hi) {
    ObObj lower, upper;
    lower.set_uint64(lo);
    upper.set_uint64(hi);
    search_ctx_.set_docid_range(lower, upper);
  };
  auto check_rows = [&](uint64_t lo, uint64_t hi, size_t active) {
    std::set<Row> expected;
    for (size_t i = 0; i < active; ++i) {
      for (const Row &row : input[i]) {
        if (row.first >= lo && row.first <= hi) {
          expected.emplace(row.first, row.second.substr(0, 4));
        }
      }
    }
    ObDASRowID rowid;
    double score = -1;
    ObDASNestedKey key;
    for (auto row = expected.begin(); row != expected.end();) {
      const uint64_t doc_id = row->first;
      ASSERT_EQ(OB_SUCCESS, sort.next_rowid(rowid, score));
      EXPECT_EQ(doc_id, rowid.get_uint64());
      EXPECT_DOUBLE_EQ(0, score);
      do {
        ASSERT_EQ(OB_SUCCESS, sort.next_rowid(key, score));
        char key_bytes[sizeof(uint64_t)];
        ASSERT_EQ(4, key.length());
        ObDASNestedKey::store(key_bytes, key.value(), key.length());
        EXPECT_EQ(row->second, std::string(key_bytes, key.length()));
        EXPECT_DOUBLE_EQ(0, score);
        ++row;
      } while (row != expected.end() && row->first == doc_id);
      EXPECT_EQ(OB_ITER_END, sort.next_rowid(key, score));
    }
    EXPECT_EQ(OB_ITER_END, sort.next_rowid(rowid, score));
  };
  set_range(3, 6);
  ASSERT_EQ(OB_SUCCESS, sort.open());
  ASSERT_NO_FATAL_FAILURE(check_rows(3, 6, 3));
  select_partition(catalog, 2);
  input[0] = {{9, key_b}, {7, key_b + key_a}, {7, key_b + key_b}};
  child0.set_rows(input[0]);
  set_range(7, 8);
  ASSERT_EQ(OB_SUCCESS, sort.rescan());
  ASSERT_NO_FATAL_FAILURE(check_rows(7, 8, 1));
  select_partition(catalog, 3);
  input = {{{12, key_b}}, {{10, key_a}}, {{10, key_b}, {10, key_a + key_a}}};
  child0.set_rows(input[0]);
  child1.set_rows(input[1]);
  child2.set_rows(input[2]);
  set_range(10, 12);
  ASSERT_EQ(OB_SUCCESS, sort.rescan());
  ASSERT_NO_FATAL_FAILURE(check_rows(10, 12, 3));
  EXPECT_EQ(OB_SUCCESS, sort.close());
  EXPECT_EQ(OB_SUCCESS, merge.close());
}

TEST_F(TestDASAsyncSegmentMerge, k_way_merge_preserves_order_and_original_score)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({1, 4, 7}, {1.0, 1.0, 1.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({2, 5, 8}, {2.0, 2.0, 2.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({3, 6, 9}, {3.0, 3.0, 3.0})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  const uint64_t expected_ids[] = {1, 2, 3, 4, 5, 6, 7, 8, 9};
  const double expected_scores[] = {1, 2, 3, 1, 2, 3, 1, 2, 3};
  ObDASRowID rowid;
  double score = 0.0;
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(expected_ids[i], rowid.get_uint64());
    EXPECT_DOUBLE_EQ(expected_scores[i], score);
  }
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, group_local_scalar_fetches_batch_and_preserves_resume_state)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{3, 1}}, {false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *first_child = create_child({1, 4, 7}, {1.0, 1.0, 7.0});
  MockSegmentOp *second_child = create_child({2, 4, 8}, {2.0, 5.0, 8.0});
  MockSegmentOp *third_child = create_child({3, 4, 9}, {3.0, 4.0, 9.0});
  ASSERT_NE(nullptr, first_child);
  ASSERT_NE(nullptr, second_child);
  ASSERT_NE(nullptr, third_child);
  ASSERT_EQ(OB_SUCCESS, children.push_back(first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(second_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(third_child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  ASSERT_TRUE(merge->use_batch_merge_);

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(1.0, score);
  ASSERT_EQ(OB_SUCCESS, merge->set_min_competitive_score(1.5));
  EXPECT_DOUBLE_EQ(1.5, first_child->last_threshold());
  EXPECT_DOUBLE_EQ(1.5, second_child->last_threshold());
  EXPECT_DOUBLE_EQ(1.5, third_child->last_threshold());

  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(2.0, score);
  EXPECT_EQ(256, first_child->max_drain_capacity());
  ASSERT_EQ(OB_SUCCESS, merge->set_min_competitive_score(2.5));
  EXPECT_DOUBLE_EQ(2.5, first_child->last_threshold());
  EXPECT_DOUBLE_EQ(2.5, second_child->last_threshold());
  EXPECT_DOUBLE_EQ(2.5, third_child->last_threshold());

  ObDASRowID target;
  target.set_uint64(4);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(5.0, score);
  ASSERT_EQ(OB_SUCCESS, merge->set_min_competitive_score(3.5));
  EXPECT_DOUBLE_EQ(3.5, first_child->last_threshold());
  EXPECT_DOUBLE_EQ(3.5, second_child->last_threshold());
  EXPECT_DOUBLE_EQ(3.5, third_child->last_threshold());

  const uint64_t expected_ids[] = {7, 8, 9};
  const double expected_scores[] = {7.0, 8.0, 9.0};
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(expected_ids[i], rowid.get_uint64());
    EXPECT_DOUBLE_EQ(expected_scores[i], score);
  }
  EXPECT_EQ(256, first_child->max_drain_capacity());
  EXPECT_EQ(256, second_child->max_drain_capacity());
  EXPECT_EQ(256, third_child->max_drain_capacity());
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, generic_sync_reachable_scalar_fetch_capacity_stays_one)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *first_child = create_child({1, 3}, {1.0, 3.0});
  MockSegmentOp *second_child = create_child({2, 4}, {2.0, 4.0});
  ASSERT_NE(nullptr, first_child);
  ASSERT_NE(nullptr, second_child);
  ASSERT_EQ(OB_SUCCESS, children.push_back(first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(second_child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, -1));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  ASSERT_TRUE(merge->use_batch_merge_);

  ObDASRowID rowid;
  double score = 0.0;
  for (uint64_t expected_id = 1; expected_id <= 4; ++expected_id) {
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(expected_id, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(static_cast<double>(expected_id), score);
  }
  EXPECT_EQ(1, first_child->max_drain_capacity());
  EXPECT_EQ(1, second_child->max_drain_capacity());
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, group_local_incremental_advance_preserves_heap_state)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{3, 1}}, {false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *first_child = create_child({1, 4, 7, 10}, {1.0, 4.0, 7.0, 10.0});
  MockSegmentOp *second_child = create_child({2, 4, 8, 11}, {2.0, 40.0, 8.0, 11.0});
  MockSegmentOp *third_child = create_child({3, 4, 9, 12}, {3.0, 30.0, 9.0, 12.0});
  ASSERT_NE(nullptr, first_child);
  ASSERT_NE(nullptr, second_child);
  ASSERT_NE(nullptr, third_child);
  ASSERT_EQ(OB_SUCCESS, children.push_back(first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(second_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(third_child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  ASSERT_TRUE(merge->use_batch_merge_);
  ASSERT_FALSE(merge->use_single_child_fast_path());

  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(4);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(40.0, score);

  ASSERT_EQ(OB_SUCCESS, merge->advance_to(rowid, rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(40.0, score);
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(7, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(7.0, score);

  target.set_uint64(9);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(9, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(9.0, score);
  target.set_uint64(12);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(12, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(12.0, score);
  EXPECT_TRUE(merge->child_ended_.at(0));
  EXPECT_TRUE(merge->child_ended_.at(1));
  EXPECT_FALSE(merge->child_ended_.at(2));

  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, generic_sync_reachable_advance_keeps_protected_path)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(
      create_child({1, 4, 7}, {1.0, 4.0, 7.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(
      create_child({2, 4, 8}, {2.0, 5.0, 8.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(
      create_child({3, 4, 9}, {3.0, 6.0, 9.0})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, -1));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  ASSERT_TRUE(merge->use_batch_merge_);

  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(4);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(6.0, score);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(rowid, rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(6.0, score);
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(7, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(7.0, score);

  target.set_uint64(9);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(9, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(9.0, score);
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, single_live_direct_seek_matches_generic_path)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{3, 1}}, {false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  const std::vector<uint64_t> live_ids = {90, 92, 110, 120};
  const std::vector<double> live_scores = {9.0, 9.2, 11.0, 12.0};
  ObArray<ObIDASSearchOp *> optimized_children;
  ObArray<ObIDASSearchOp *> reference_children;
  MockSegmentOp *optimized_live = create_child(live_ids, live_scores);
  MockSegmentOp *reference_live = create_child(live_ids, live_scores);
  ASSERT_NE(nullptr, optimized_live);
  ASSERT_NE(nullptr, reference_live);
  ASSERT_EQ(OB_SUCCESS, optimized_children.push_back(optimized_live));
  ASSERT_EQ(OB_SUCCESS, reference_children.push_back(reference_live));
  for (int64_t i = 1; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, optimized_children.push_back(create_child({}, {})));
    ASSERT_EQ(OB_SUCCESS, reference_children.push_back(create_child({}, {})));
  }
  ObDASAsyncSegmentMergeOpParam optimized_param(
      optimized_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  ObDASAsyncSegmentMergeOpParam reference_param(
      reference_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, -1));
  optimized_param.set_is_scoring(true);
  reference_param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *optimized = nullptr;
  ObDASAsyncSegmentMergeOp *reference = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(optimized_param, optimized));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(reference_param, reference));
  ASSERT_EQ(OB_SUCCESS, optimized->open());
  EXPECT_FALSE(optimized->use_ordered_concat_);
  ASSERT_EQ(OB_SUCCESS, reference->open());
  EXPECT_FALSE(reference->use_ordered_concat_);

  ObDASRowID optimized_rowid;
  ObDASRowID reference_rowid;
  double optimized_score = 0.0;
  double reference_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, optimized->next_rowid(optimized_rowid, optimized_score));
  ASSERT_EQ(OB_SUCCESS, reference->next_rowid(reference_rowid, reference_score));
  ASSERT_EQ(90, optimized_rowid.get_uint64());
  ASSERT_EQ(optimized_rowid.get_uint64(), reference_rowid.get_uint64());
  const int64_t optimized_drain_before = optimized_live->drain_count();
  const int64_t reference_drain_before = reference_live->drain_count();
  ObDASRowID target;
  target.set_uint64(95);
  ASSERT_EQ(OB_SUCCESS, optimized->advance_to(target, optimized_rowid, optimized_score));
  ASSERT_EQ(OB_SUCCESS, reference->advance_to(target, reference_rowid, reference_score));
  EXPECT_EQ(110, optimized_rowid.get_uint64());
  EXPECT_EQ(optimized_rowid.get_uint64(), reference_rowid.get_uint64());
  EXPECT_DOUBLE_EQ(11.0, optimized_score);
  EXPECT_DOUBLE_EQ(optimized_score, reference_score);
  EXPECT_EQ(0, optimized_live->drain_count() - optimized_drain_before);
  EXPECT_EQ(1, reference_live->drain_count() - reference_drain_before);
  EXPECT_EQ(1, optimized_live->advance_count());
  EXPECT_EQ(1, reference_live->advance_count());

  const int64_t optimized_advance_after_seek = optimized_live->advance_count();
  ASSERT_EQ(OB_SUCCESS, optimized->advance_to(target, optimized_rowid, optimized_score));
  EXPECT_EQ(110, optimized_rowid.get_uint64());
  EXPECT_EQ(optimized_advance_after_seek, optimized_live->advance_count());
  ASSERT_EQ(OB_SUCCESS, optimized->next_rowid(optimized_rowid, optimized_score));
  EXPECT_EQ(120, optimized_rowid.get_uint64());
  EXPECT_DOUBLE_EQ(12.0, optimized_score);
  EXPECT_EQ(OB_ITER_END, optimized->next_rowid(optimized_rowid, optimized_score));
  EXPECT_EQ(OB_ITER_END, optimized->next_rowid(optimized_rowid, optimized_score));
}

TEST_F(TestDASAsyncSegmentMerge, single_live_direct_seek_respects_range_hi)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{3, 1}}, {false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_min_value();
  range_hi.set_uint64(100);
  search_ctx_.set_docid_range(range_lo, range_hi);
  ObArray<ObIDASSearchOp *> crossing_children;
  ObArray<ObIDASSearchOp *> equal_children;
  ASSERT_EQ(OB_SUCCESS, crossing_children.push_back(
      create_child({90, 110}, {9.0, 11.0})));
  ASSERT_EQ(OB_SUCCESS, equal_children.push_back(
      create_child({90, 100, 110}, {9.0, 10.0, 11.0})));
  for (int64_t i = 1; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, crossing_children.push_back(create_child({}, {})));
    ASSERT_EQ(OB_SUCCESS, equal_children.push_back(create_child({}, {})));
  }
  ObDASAsyncSegmentMergeOpParam crossing_param(
      crossing_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  ObDASAsyncSegmentMergeOpParam equal_param(
      equal_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  ObDASAsyncSegmentMergeOp *crossing = nullptr;
  ObDASAsyncSegmentMergeOp *equal = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(crossing_param, crossing));
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(equal_param, equal));
  ASSERT_EQ(OB_SUCCESS, crossing->open());
  EXPECT_FALSE(crossing->use_ordered_concat_);
  ASSERT_EQ(OB_SUCCESS, equal->open());
  EXPECT_FALSE(equal->use_ordered_concat_);

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, crossing->next_rowid(rowid, score));
  ASSERT_EQ(OB_SUCCESS, equal->next_rowid(rowid, score));
  ObDASRowID target;
  target.set_uint64(95);
  EXPECT_EQ(OB_ITER_END, crossing->advance_to(target, rowid, score));
  EXPECT_TRUE(crossing->iter_end_);
  ASSERT_EQ(OB_SUCCESS, equal->advance_to(target, rowid, score));
  EXPECT_EQ(100, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(10.0, score);
  EXPECT_EQ(OB_ITER_END, equal->next_rowid(rowid, score));
  EXPECT_EQ(OB_ITER_END, equal->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, single_live_direct_seek_preserves_owner_on_failure)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{3, 1}}, {false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *live_child = create_child({90, 110}, {9.0, 11.0});
  ASSERT_NE(nullptr, live_child);
  ASSERT_EQ(OB_SUCCESS, children.push_back(live_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({}, {})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({}, {})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  live_child->set_advance_ret(OB_ERR_UNEXPECTED);
  ObDASRowID target;
  target.set_uint64(95);
  EXPECT_EQ(OB_ERR_UNEXPECTED, merge->advance_to(target, rowid, score));
  EXPECT_EQ(90, merge->head_rowids_.at(0).get_uint64());
  EXPECT_FALSE(merge->child_ended_.at(0));
  EXPECT_FALSE(merge->iter_end_);
  ASSERT_EQ(1, merge->pending_child_indices_.count());
  EXPECT_EQ(0, merge->pending_child_indices_.at(0));
  live_child->set_advance_ret(OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(110, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(11.0, score);
}

TEST_F(TestDASAsyncSegmentMerge, duplicate_pending_keeps_old_max_score_path)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{3, 1}}, {false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *first_child = create_child({90, 110}, {9.0, 11.0});
  MockSegmentOp *second_child = create_child({90, 115}, {19.0, 15.0});
  ASSERT_EQ(OB_SUCCESS, children.push_back(first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(second_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({}, {})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(90, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(19.0, score);
  ASSERT_EQ(2, merge->pending_child_indices_.count());
  ObDASRowID target;
  target.set_uint64(95);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(110, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(11.0, score);
}

TEST_F(TestDASAsyncSegmentMerge, forced_merge_rejects_single_live_direct_seek)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{3, 1}}, {false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *live_child = create_child({90, 92, 110}, {9.0, 9.2, 11.0});
  ASSERT_EQ(OB_SUCCESS, children.push_back(live_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({}, {})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({}, {})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0), true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  const int64_t drain_before = live_child->drain_count();
  ObDASRowID target;
  target.set_uint64(95);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(110, rowid.get_uint64());
  EXPECT_EQ(1, live_child->drain_count() - drain_before);
  EXPECT_EQ(1, live_child->advance_count());
}

TEST_F(TestDASAsyncSegmentMerge, segment_rowid_metadata_uses_logical_version_range)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  ObAsyncIndexPartitionSegCtx &partition = *catalog.partitions_.at(0);
  PinnedSegmentTables storage(allocator_);
  const uint64_t min_ids[] = {20, 1, 10};
  const uint64_t max_ids[] = {24, 5, 14};
  ASSERT_EQ(OB_SUCCESS, storage.init(partition, min_ids, max_ids, ARRAYSIZEOF(min_ids)));
  bool available = false;
  uint64_t min_id = 0;
  uint64_t max_id = 0;
  for (int64_t i = 0; i < ARRAYSIZEOF(min_ids); ++i) {
    ObNewVersionRange logical_range;
    ASSERT_EQ(OB_SUCCESS, storage.tables_[i].get_async_index_version_range(logical_range));
    if (storage.tables_[i].is_inc_major_type_sstable()) {
      EXPECT_FALSE(storage.tables_[i].key_.version_range_ == logical_range);
    }
    ASSERT_EQ(OB_SUCCESS, partition.get_segment_rowid_range(
        TEST_INDEX_TABLE_ID, logical_range, available, min_id, max_id));
    EXPECT_TRUE(available);
    EXPECT_EQ(min_ids[i], min_id);
    EXPECT_EQ(max_ids[i], max_id);
  }
  ObNewVersionRange range;
  ASSERT_EQ(OB_SUCCESS, storage.tables_[1].get_async_index_version_range(range));
  ObNewVersionRange wrong_range = range;
  ++wrong_range.base_version_;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, wrong_range, available, min_id, max_id));
  EXPECT_FALSE(available);

  blocksstable::ObInvIdxStatsMeta &meta = storage.tables_[1].inv_meta_.stats_;
  meta.doc_id_obj_meta_.set_int();
  ASSERT_EQ(OB_SUCCESS, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_FALSE(available);
  meta.doc_id_obj_meta_.set_uint64();
  meta.has_nested_key_ = true;
  ASSERT_EQ(OB_SUCCESS, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_TRUE(available);
  EXPECT_EQ(min_ids[1], min_id);
  EXPECT_EQ(max_ids[1], max_id);
  meta.has_nested_key_ = false;
  meta.min_doc_id_ = 9;
  meta.max_doc_id_ = 8;
  EXPECT_EQ(OB_ERR_UNEXPECTED, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_FALSE(available);
  meta.min_doc_id_ = UINT64_MAX;
  meta.max_doc_id_ = 0;
  ASSERT_EQ(OB_SUCCESS, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_FALSE(available);
  storage.tables_[1].meta_cache_.data_macro_block_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_TRUE(available);
  EXPECT_EQ(UINT64_MAX, min_id);
  EXPECT_EQ(0, max_id);
  storage.tables_[1].meta_cache_.data_macro_block_count_ = 1;
  meta.index_type_ = blocksstable::INV_IDX_TYPE_INVALID;
  EXPECT_EQ(OB_ERR_UNEXPECTED, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  storage.tablet_.tablet_meta_.tablet_id_ = ObTabletID(99999);
  EXPECT_EQ(OB_ERR_UNEXPECTED, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  storage.tablet_.tablet_meta_.tablet_id_ = partition.index_tablet_ids_.at(0);
  meta.reset();
  storage.tables_[1].meta_cache_.data_macro_block_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_TRUE(available);
  EXPECT_EQ(UINT64_MAX, min_id);
  EXPECT_EQ(0, max_id);
  storage.tables_[1].meta_cache_.data_macro_block_count_ = 1;
  EXPECT_EQ(OB_ERR_UNEXPECTED, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_FALSE(available);

  // The handle borrows the fixture's tablet; detach before destroying the handle array.
  storage.handle_->obj_ = nullptr;
  storage.handle_->allocator_ = nullptr;
  storage.handle_ = nullptr;
  partition.storage_group_ctx_.index_tablet_handles_.reset();
  available = true;
  min_id = 1;
  max_id = 2;
  EXPECT_EQ(OB_ERR_UNEXPECTED, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_FALSE(available);
  EXPECT_EQ(UINT64_MAX, min_id);
  EXPECT_EQ(0, max_id);
  ASSERT_EQ(OB_SUCCESS, add_mock_storage_tablets(partition, 1));
  EXPECT_EQ(OB_ERR_UNEXPECTED, partition.get_segment_rowid_range(
      TEST_INDEX_TABLE_ID, range, available, min_id, max_id));
  EXPECT_FALSE(available);
}

TEST_F(TestDASAsyncSegmentMerge, segment_rowid_ranges_detect_disjoint_and_overlap)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedSegmentTables storage(allocator_);
  const uint64_t min_ids[] = {20, 1, 10};
  const uint64_t max_ids[] = {24, 5, 14};
  ASSERT_EQ(OB_SUCCESS, storage.init(
      *catalog.partitions_.at(0), min_ids, max_ids, ARRAYSIZEOF(min_ids)));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({20, 24}, {20.0, 24.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({1, 5}, {1.0, 5.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({10, 14}, {10.0, 14.0})));
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  bool disjoint = false;
  ObSEArray<int64_t, 4> ordered_indices;
  ASSERT_EQ(OB_SUCCESS, merge->check_segment_rowid_ranges(disjoint, ordered_indices));
  ASSERT_TRUE(disjoint);
  ASSERT_EQ(3, ordered_indices.count());
  EXPECT_EQ(1, ordered_indices.at(0));
  EXPECT_EQ(2, ordered_indices.at(1));
  EXPECT_EQ(0, ordered_indices.at(2));
  ObSEArray<int64_t, 1, ObNullAllocator> limited_indices;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
      merge->check_segment_rowid_ranges(disjoint, limited_indices));
  EXPECT_FALSE(disjoint);
  EXPECT_TRUE(limited_indices.empty());
  const uint64_t expected_ids[] = {1, 5, 10, 14, 20, 24};
  const uint64_t fallback_min_ids[] = {14, 12, 1};
  for (int64_t phase = 0; phase < 10; ++phase) {
    if (phase > 0) {
      // Touching, overlapping and containing envelopes must all keep the heap.
      storage.tables_[0].inv_meta_.stats_.min_doc_id_ = phase <= 3
          ? fallback_min_ids[phase - 1] : min_ids[0];
      if (5 == phase) {
        merge->use_batch_merge_ = false;
      }
      if (6 == phase) {
        // A posting-value NK list does not change the UINT64 DocID envelope.
        storage.tables_[0].inv_meta_.stats_.has_nested_key_ = true;
      } else {
        storage.tables_[0].inv_meta_.stats_.has_nested_key_ = false;
      }
      if (8 == phase) {
        // Nonempty physical storage with an empty statistics sentinel must not lose rows.
        storage.tables_[0].inv_meta_.stats_.min_doc_id_ = UINT64_MAX;
        storage.tables_[0].inv_meta_.stats_.max_doc_id_ = 0;
        ASSERT_FALSE(storage.tables_[0].is_empty());
      }
      if (9 == phase) {
        storage.tables_[0].inv_meta_.stats_.max_doc_id_ = max_ids[0];
        storage.tables_[0].inv_meta_.stats_.doc_id_obj_meta_.set_int();
      }
      ASSERT_EQ(OB_SUCCESS, merge->rescan());
    }
    ASSERT_EQ(OB_SUCCESS, merge->check_segment_rowid_ranges(disjoint, ordered_indices));
    EXPECT_EQ(0 == phase || 4 == phase || 5 == phase || 6 == phase || 7 == phase,
              disjoint);
    EXPECT_EQ(disjoint, merge->use_ordered_concat_);
    for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
      ObDASRowID rowid;
      double score = 0.0;
      ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
      EXPECT_EQ(expected_ids[i], rowid.get_uint64());
      EXPECT_DOUBLE_EQ(static_cast<double>(expected_ids[i]), score);
    }
    ObDASRowID rowid;
    double score = 0.0;
    EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
  }
  storage.tables_[0].inv_meta_.reset();
  EXPECT_EQ(OB_ERR_UNEXPECTED, merge->check_segment_rowid_ranges(disjoint, ordered_indices));
  EXPECT_FALSE(disjoint);
  EXPECT_TRUE(ordered_indices.empty());
}

TEST_F(TestDASAsyncSegmentMerge, segment_rowid_ranges_handle_empty_and_extreme_bounds)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedSegmentTables storage(allocator_);
  const uint64_t min_ids[] = {UINT64_MAX, UINT64_MAX - 1, 0};
  const uint64_t max_ids[] = {0, UINT64_MAX, 1};
  ASSERT_EQ(OB_SUCCESS, storage.init(
      *catalog.partitions_.at(0), min_ids, max_ids, ARRAYSIZEOF(min_ids)));
  // Storage creates an untouched empty baseline before the first INC flush.
  storage.tables_[0].inv_meta_.reset();
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({}, {})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({UINT64_MAX - 1, UINT64_MAX}, {3.0, 4.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({0, 1}, {1.0, 2.0})));
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  bool disjoint = false;
  ObSEArray<int64_t, 4> ordered_indices;
  ASSERT_EQ(OB_SUCCESS, merge->check_segment_rowid_ranges(disjoint, ordered_indices));
  ASSERT_TRUE(disjoint);
  ASSERT_EQ(2, ordered_indices.count());
  EXPECT_EQ(2, ordered_indices.at(0));
  EXPECT_EQ(1, ordered_indices.at(1));
  const uint64_t expected_ids[] = {0, 1, UINT64_MAX - 1, UINT64_MAX};
  ObDASRowID rowid;
  double score = 0.0;
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(expected_ids[i], rowid.get_uint64());
  }
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, ordered_concat_seek_is_independent_and_idempotent)
{
  ObDASRowID target95;
  ObDASRowID target150;
  target95.set_uint64(95);
  target150.set_uint64(150);
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{2, 1}}, {false});
  PinnedSegmentTables storage(allocator_);
  const uint64_t min_ids[] = {90, 200};
  const uint64_t max_ids[] = {120, 220};
  ASSERT_EQ(OB_SUCCESS, storage.init(*catalog.partitions_.at(0), min_ids, max_ids, 2));
  MockSegmentOp *early = create_child({90, 92, 110, 120}, {9, 9.2, 11, 12});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(early));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({200, 220}, {20, 22})));
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  for (int64_t phase = 0; phase < 2; ++phase) {
    merge->use_batch_merge_ = 0 == phase;
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    ASSERT_TRUE(merge->use_ordered_concat_);
    ObDASRowID rowid;
    double score = 0.0;
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(90, rowid.get_uint64());
    const int64_t drain_before = early->drain_count();
    early->set_advance_ret(OB_ERR_UNEXPECTED);
    EXPECT_EQ(OB_ERR_UNEXPECTED, merge->advance_to(target95, rowid, score));
    EXPECT_EQ(drain_before, early->drain_count());
    early->set_advance_ret(OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, merge->advance_to(target95, rowid, score));
    EXPECT_EQ(110, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(11, score);
    EXPECT_EQ(drain_before, early->drain_count());
    const int64_t seek_count = early->advance_count();
    ASSERT_EQ(OB_SUCCESS, merge->advance_to(target95, rowid, score));
    EXPECT_EQ(110, rowid.get_uint64());
    EXPECT_EQ(seek_count, early->advance_count());
    ASSERT_EQ(OB_SUCCESS, merge->advance_to(target150, rowid, score));
    EXPECT_EQ(200, rowid.get_uint64());
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(220, rowid.get_uint64());
    EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
    EXPECT_TRUE(merge->heap_child_indices_.empty());
    EXPECT_TRUE(merge->pending_child_indices_.empty());
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    ASSERT_EQ(OB_SUCCESS, merge->advance_to(target150, rowid, score));
    EXPECT_EQ(200, rowid.get_uint64());
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    ObDASRowID end_target;
    end_target.set_max();
    EXPECT_EQ(OB_ITER_END, merge->advance_to(end_target, rowid, score));
    EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    end_target.set_uint64(999);
    EXPECT_EQ(OB_ITER_END, merge->advance_to(end_target, rowid, score));
  }
}

TEST_F(TestDASAsyncSegmentMerge, ordered_concat_shallow_respects_query_bounds)
{
  ObDASRowID target2;
  ObDASRowID target20;
  target2.set_uint64(2);
  target20.set_uint64(20);
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{2, 1}}, {false});
  PinnedSegmentTables storage(allocator_);
  const uint64_t min_ids[] = {1, 20};
  const uint64_t max_ids[] = {3, 24};
  ASSERT_EQ(OB_SUCCESS, storage.init(*catalog.partitions_.at(0), min_ids, max_ids, 2));
  ObObj lo;
  ObObj hi;
  lo.set_uint64(2);
  hi.set_uint64(23);
  search_ctx_.set_docid_range(lo, hi);
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *early = create_child({1, 2, 3}, {1, 100, 3});
  ASSERT_EQ(OB_SUCCESS, children.push_back(early));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({20, 21, 23, 24}, {20, 21, 23, 24})));
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  for (int64_t phase = 0; phase < 2; ++phase) {
    merge->use_batch_merge_ = 0 == phase;
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    const MaxScoreTuple *tuple = nullptr;
    ASSERT_TRUE(merge->use_ordered_concat_);
    ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target2, true, tuple));
    ASSERT_NE(nullptr, tuple);
    EXPECT_GE(tuple->get_max_score(), 100);
    ObDASRowID rowid;
    double score = 0.0;
    ASSERT_EQ(OB_SUCCESS, merge->advance_to(target2, rowid, score));
    EXPECT_EQ(2, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(100, score);
    ASSERT_EQ(OB_SUCCESS, merge->set_min_competitive_score(2.5));
    EXPECT_DOUBLE_EQ(2.5, early->last_threshold());
    ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target2, false, tuple));
    EXPECT_EQ(OB_NOT_SUPPORTED, merge->next_rowid(rowid, score));
    ASSERT_EQ(OB_SUCCESS, merge->advance_to(target2, rowid, score));
    EXPECT_EQ(3, rowid.get_uint64());
    uint64_t ids[8];
    double scores[8];
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, merge->drain_until(target20, 8, ids, scores, count, rowid, score));
    ASSERT_EQ(1, count);
    EXPECT_EQ(20, ids[0]);
    EXPECT_EQ(21, rowid.get_uint64());
    EXPECT_DOUBLE_EQ(21, score);
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(23, rowid.get_uint64());
    EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
    EXPECT_TRUE(merge->heap_child_indices_.empty());
    EXPECT_TRUE(merge->pending_child_indices_.empty());
  }
}

TEST_F(TestDASAsyncSegmentMerge, ordered_concat_bounded_copy_preserves_lookahead)
{
  ObDASRowID boundary;
  boundary.set_uint64(3);
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  PinnedSegmentTables storage(allocator_);
  const uint64_t min_ids[] = {1, 10};
  const uint64_t max_ids[] = {4, 10};
  ASSERT_EQ(OB_SUCCESS, storage.init(*catalog.partitions_.at(0), min_ids, max_ids, 2));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({1, 2, 3, 4}, {1.1, 2.2, 3.3, 4.4})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({10}, {10.1})));
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  for (int64_t phase = 0; phase < 2; ++phase) {
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    ASSERT_TRUE(merge->use_ordered_concat_);
    uint64_t ids[8];
    double scores[8];
    int64_t count = 0;
    ObDASRowID after;
    double score = 0.0;
    ASSERT_EQ(OB_SUCCESS, merge->drain_until(boundary, 0 == phase ? 8 : 3,
        ids, scores, count, after, score));
    ASSERT_EQ(3, count);
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(i + 1, ids[i]);
      EXPECT_NEAR((i + 1) * 1.1, scores[i], 1e-10);
    }
    if (0 == phase) {
      EXPECT_EQ(4, after.get_uint64());
      EXPECT_DOUBLE_EQ(4.4, score);
    } else {
      ASSERT_EQ(OB_SUCCESS, merge->next_rowid(after, score));
      EXPECT_EQ(4, after.get_uint64());
    }
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(after, score));
    EXPECT_EQ(10, after.get_uint64());
    EXPECT_DOUBLE_EQ(10.1, score);
    EXPECT_EQ(OB_ITER_END, merge->next_rowid(after, score));
    EXPECT_TRUE(merge->heap_child_indices_.empty());
    EXPECT_TRUE(merge->pending_child_indices_.empty());
  }
}

TEST_F(TestDASAsyncSegmentMerge, ordered_concat_skips_empty_segments_and_empty_results)
{
  for (int64_t phase = 0; phase < 3; ++phase) {
    ObDASAsyncIndexQueryCtx catalog;
    build_catalog(catalog, {4});
    PinnedSegmentTables storage(allocator_);
    uint64_t mins[] = {1, 10, 20, 30};
    uint64_t maxs[] = {2, 11, 21, 31};
    if (0 == phase) {
      for (int64_t i = 0; i < 4; ++i) {
        mins[i] = UINT64_MAX;
        maxs[i] = 0;
      }
    }
    ASSERT_EQ(OB_SUCCESS, storage.init(*catalog.partitions_.at(0), mins, maxs, 4));
    ObArray<ObIDASSearchOp *> children;
    for (int64_t i = 0; i < 4; ++i) {
      const bool has_rows = 2 == phase && 1 == i % 2;
      ASSERT_EQ(OB_SUCCESS, children.push_back(create_child(
          has_rows ? std::vector<uint64_t>{mins[i], maxs[i]} : std::vector<uint64_t>{},
          has_rows ? std::vector<double>{1, 1} : std::vector<double>{})));
    }
    ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
    ObDASAsyncSegmentMergeOp *merge = nullptr;
    ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
    ASSERT_EQ(OB_SUCCESS, merge->open());
    for (int64_t batch = 0; batch < 2; ++batch) {
      merge->use_batch_merge_ = 0 == batch;
      ASSERT_EQ(OB_SUCCESS, merge->rescan());
      ASSERT_TRUE(merge->use_ordered_concat_);
      ObDASRowID rowid;
      double score = 0;
      const uint64_t expected[] = {10, 11, 30, 31};
      for (int64_t i = 0; 2 == phase && i < 4; ++i) {
        ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
        EXPECT_EQ(expected[i], rowid.get_uint64());
      }
      EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
      EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
      ASSERT_EQ(OB_SUCCESS, merge->rescan());
      ObDASRowID boundary;
      boundary.set_max();
      uint64_t ids[8];
      int64_t count = 0;
      ASSERT_EQ(OB_SUCCESS, merge->drain_until(boundary, 8, ids, nullptr, count, rowid, score));
      ASSERT_EQ(2 == phase ? 4 : 0, count);
      for (int64_t i = 0; i < count; ++i) {
        EXPECT_EQ(expected[i], ids[i]);
      }
      EXPECT_TRUE(rowid.is_max());
      EXPECT_TRUE(merge->heap_child_indices_.empty());
    }
    ASSERT_EQ(OB_SUCCESS, merge->close());
  }
}

TEST_F(TestDASAsyncSegmentMerge, ordered_concat_partition_rescan_rebuilds_order)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{3, 1}, {1, 1}, {3, 1}}, {false, false, false});
  PinnedSegmentTables first_storage(allocator_);
  PinnedSegmentTables single_storage(allocator_);
  PinnedSegmentTables last_storage(allocator_);
  const uint64_t mins[] = {30, 1, 20};
  const uint64_t maxs[] = {31, 2, 21};
  ASSERT_EQ(OB_SUCCESS, first_storage.init(*catalog.partitions_.at(0), mins, maxs, 3));
  ASSERT_EQ(OB_SUCCESS, single_storage.init(*catalog.partitions_.at(1), mins, maxs, 1));
  ASSERT_EQ(OB_SUCCESS, last_storage.init(*catalog.partitions_.at(2), mins, maxs, 3));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({30, 31}, {30, 31})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({1, 2}, {1, 2})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({20, 21}, {20, 21})));
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  ASSERT_TRUE(merge->use_ordered_concat_);
  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0;
  target.set_uint64(20);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(20, rowid.get_uint64());
  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_TRUE(merge->ordered_child_indices_.empty());
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(30, rowid.get_uint64());
  select_partition(catalog, 3);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  ASSERT_TRUE(merge->use_ordered_concat_);
  ASSERT_EQ(3, merge->ordered_child_indices_.count());
  EXPECT_EQ(1, merge->ordered_child_indices_.at(0));
  EXPECT_FALSE(merge->concat_head_consumed_);
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  // An overlapping partition must discard the previous concat cursor.
  for (int64_t i = 0; i < 3; ++i) {
    last_storage.tables_[i].inv_meta_.stats_.min_doc_id_ = 0;
    last_storage.tables_[i].inv_meta_.stats_.max_doc_id_ = UINT64_MAX;
  }
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_TRUE(merge->ordered_child_indices_.empty());
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  select_partition(catalog, 1);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  ASSERT_TRUE(merge->use_ordered_concat_);
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
}

TEST_F(TestDASAsyncSegmentMerge, ordered_concat_cross_segment_drain_without_scores)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedSegmentTables storage(allocator_);
  const uint64_t mins[] = {0, 300, 600};
  const uint64_t maxs[] = {299, 599, 899};
  ASSERT_EQ(OB_SUCCESS, storage.init(*catalog.partitions_.at(0), mins, maxs, 3));
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < 3; ++i) {
    std::vector<uint64_t> ids;
    std::vector<double> scores;
    for (uint64_t id = mins[i]; id <= maxs[i]; ++id) {
      ids.push_back(id);
      scores.push_back(99);
    }
    ASSERT_EQ(OB_SUCCESS, children.push_back(create_child(ids, scores)));
  }
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(false);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  ASSERT_TRUE(merge->use_ordered_concat_);
  uint64_t ids[650];
  double scores[650];
  ObDASRowID boundary;
  ObDASRowID after;
  boundary.set_max();
  double score = 0;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, merge->drain_until(boundary, 650, ids, scores, count, after, score));
  ASSERT_EQ(650, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(i, ids[i]);
    EXPECT_DOUBLE_EQ(0, scores[i]);
  }
  ASSERT_EQ(OB_SUCCESS, merge->drain_until(boundary, 650, ids, nullptr, count, after, score));
  ASSERT_EQ(250, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(i + 650, ids[i]);
  }
  EXPECT_TRUE(after.is_max());
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(after, score));
  EXPECT_TRUE(merge->heap_child_indices_.empty());
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  ObDASRowID target;
  target.set_uint64(10);
  const MaxScoreTuple *tuple = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target, true, tuple));
  ASSERT_NE(nullptr, tuple);
  EXPECT_DOUBLE_EQ(0, tuple->get_max_score());
  ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target, false, tuple));
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, after, score));
  EXPECT_EQ(11, after.get_uint64());
  EXPECT_DOUBLE_EQ(0, score);
}

TEST_F(TestDASAsyncSegmentMerge, ordered_concat_propagates_child_read_errors)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  PinnedSegmentTables storage(allocator_);
  const uint64_t mins[] = {1, 10};
  const uint64_t maxs[] = {3, 11};
  ASSERT_EQ(OB_SUCCESS, storage.init(*catalog.partitions_.at(0), mins, maxs, 2));
  MockSegmentOp *first = create_child({1, 2, 3}, {1, 2, 3});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(first));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({10, 11}, {10, 11})));
  ObDASAsyncSegmentMergeOpParam param(children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  for (int64_t phase = 0; phase < 2; ++phase) {
    merge->use_batch_merge_ = 0 == phase;
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    ASSERT_TRUE(merge->use_ordered_concat_);
    ObDASRowID rowid;
    double score = 0;
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(1, rowid.get_uint64());
    first->set_next_ret(OB_ERR_UNEXPECTED);
    first->set_drain_ret(OB_ERR_UNEXPECTED);
    EXPECT_EQ(OB_ERR_UNEXPECTED, merge->next_rowid(rowid, score));
    ObDASRowID boundary;
    boundary.set_max();
    uint64_t ids[8];
    int64_t count = 0;
    EXPECT_EQ(OB_ERR_UNEXPECTED,
        merge->drain_until(boundary, 8, ids, nullptr, count, rowid, score));
    EXPECT_EQ(0, count);
    first->set_next_ret(OB_SUCCESS);
    first->set_drain_ret(OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    ObDASRowID target;
    target.set_uint64(2);
    const MaxScoreTuple *tuple = nullptr;
    first->set_shallow_ret(OB_ERR_UNEXPECTED);
    EXPECT_EQ(OB_ERR_UNEXPECTED, merge->advance_shallow(target, true, tuple));
    first->set_shallow_ret(OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target, true, tuple));
    first->set_advance_ret(OB_ERR_UNEXPECTED);
    EXPECT_EQ(OB_ERR_UNEXPECTED, merge->advance_to(target, rowid, score));
    first->set_advance_ret(OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, merge->rescan());
    ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
    EXPECT_EQ(2, rowid.get_uint64());
  }
}

TEST_F(TestDASAsyncSegmentMerge, uint64_disjunction_defers_child_reads_until_iteration)
{
  MockSegmentOp first_child(search_ctx_, {1, 3}, {1.0, 1.0});
  MockSegmentOp second_child(search_ctx_, {2, 4}, {2.0, 2.0});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, first_child.init(child_param));
  ASSERT_EQ(OB_SUCCESS, second_child.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&second_child));
  ObDASDisjunctionFilterOpParam param(
      children, 1, ObDASSearchCost(1), 4.0);
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, disjunction.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction.open());

  // An enclosing async segment merge binds its concrete version range after
  // opening the query tree, so opening this operator must not read children.
  EXPECT_TRUE(disjunction.use_batch_merge_);
  EXPECT_FALSE(disjunction.batch_cursor_primed_);
  EXPECT_EQ(0, first_child.drain_count());
  EXPECT_EQ(0, second_child.drain_count());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_TRUE(disjunction.batch_cursor_primed_);
  EXPECT_EQ(2, first_child.drain_count());
  EXPECT_EQ(1, second_child.drain_count());
  EXPECT_EQ(1, first_child.max_drain_capacity());
  EXPECT_EQ(1, second_child.max_drain_capacity());
}

TEST_F(TestDASAsyncSegmentMerge, uint64_disjunction_batch_merges_refills_and_copies_runs)
{
  std::vector<uint64_t> rowids[4];
  std::vector<double> scores[4];
  for (uint64_t id = 0; id < 900; ++id) {
    const int64_t child_idx = (id / 100) % 3;
    rowids[child_idx].push_back(id);
    scores[child_idx].push_back(static_cast<double>(child_idx));
    if (0 == id % 300) {
      rowids[3].push_back(id);
      scores[3].push_back(3.0);
    }
  }

  MockSegmentOp child_ops[] = {
      MockSegmentOp(search_ctx_, rowids[0], scores[0]),
      MockSegmentOp(search_ctx_, rowids[1], scores[1]),
      MockSegmentOp(search_ctx_, rowids[2], scores[2]),
      MockSegmentOp(search_ctx_, rowids[3], scores[3])};
  MockSegmentOpParam child_param;
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
    ASSERT_EQ(OB_SUCCESS, child_ops[i].init(child_param));
    ASSERT_EQ(OB_SUCCESS, children.push_back(&child_ops[i]));
  }
  ObDASDisjunctionFilterOpParam param(
      children, 1, ObDASSearchCost(1), 7.0);
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, disjunction.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction.open());

  static constexpr int64_t BATCH_SIZE = 173;
  uint64_t batch_ids[BATCH_SIZE];
  double batch_scores[BATCH_SIZE];
  ObDASRowID boundary;
  boundary.set_max();
  int64_t output_count = 0;
  bool end = false;
  while (!end) {
    ObDASRowID next_id_after;
    double next_score_after = 0.0;
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, disjunction.drain_until(
        boundary, BATCH_SIZE, batch_ids, batch_scores,
        count, next_id_after, next_score_after));
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_LT(output_count, 900);
      EXPECT_EQ(static_cast<uint64_t>(output_count), batch_ids[i]);
      EXPECT_DOUBLE_EQ(7.0, batch_scores[i]);
      ++output_count;
    }
    if (count < BATCH_SIZE) {
      EXPECT_TRUE(next_id_after.is_max());
      end = true;
    }
  }
  EXPECT_EQ(900, output_count);
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_EQ(BATCH_SIZE, child_ops[i].max_drain_capacity());
    EXPECT_EQ(3, child_ops[i].drain_count());
  }
  EXPECT_EQ(BATCH_SIZE, child_ops[3].max_drain_capacity());
  EXPECT_EQ(2, child_ops[3].drain_count());
  ASSERT_EQ(OB_SUCCESS, disjunction.close());
  for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
    ASSERT_EQ(OB_SUCCESS, child_ops[i].close());
  }
}

TEST_F(TestDASAsyncSegmentMerge, uint64_disjunction_bounds_high_fanout_prefetch)
{
  static constexpr int64_t CHILD_COUNT = 1024;
  static constexpr int64_t ROWS_PER_CHILD = 2;
  static constexpr int64_t MAX_RANGE_COUNT = 100000;
  static constexpr int64_t EXPECTED_CHILD_BATCH_SIZE = 128;
  EXPECT_EQ(256,
      ObDASDisjunctionFilterOp::calc_child_batch_size(1));
  EXPECT_EQ(EXPECTED_CHILD_BATCH_SIZE,
      ObDASDisjunctionFilterOp::calc_child_batch_size(CHILD_COUNT));
  EXPECT_EQ(1,
      ObDASDisjunctionFilterOp::calc_child_batch_size(MAX_RANGE_COUNT));
  EXPECT_LE(
      MAX_RANGE_COUNT
          * ObDASDisjunctionFilterOp::calc_child_batch_size(MAX_RANGE_COUNT)
          * static_cast<int64_t>(sizeof(uint64_t)),
      ObDASDisjunctionFilterOp::BATCH_BUFFER_BYTE_BUDGET);

  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *child_ops[CHILD_COUNT];
  for (int64_t i = 0; i < CHILD_COUNT; ++i) {
    std::vector<uint64_t> child_ids;
    std::vector<double> child_scores;
    for (int64_t j = 0; j < ROWS_PER_CHILD; ++j) {
      child_ids.push_back(static_cast<uint64_t>(i + 1 + j * CHILD_COUNT));
      child_scores.push_back(1.0);
    }
    child_ops[i] = create_child(child_ids, child_scores);
    ASSERT_NE(nullptr, child_ops[i]);
    ASSERT_EQ(OB_SUCCESS, children.push_back(child_ops[i]));
  }
  ObDASDisjunctionFilterOpParam param(
      children, 1, ObDASSearchCost(1), 5.0);
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, disjunction.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction.open());

  static constexpr int64_t BATCH_SIZE = 173;
  static constexpr int64_t EXPECTED_ROW_COUNT = CHILD_COUNT * ROWS_PER_CHILD;
  uint64_t batch_ids[BATCH_SIZE];
  double batch_scores[BATCH_SIZE];
  ObDASRowID boundary;
  boundary.set_max();
  int64_t output_count = 0;
  while (output_count < EXPECTED_ROW_COUNT) {
    ObDASRowID next_id_after;
    double next_score_after = 0.0;
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, disjunction.drain_until(
        boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
        count, next_id_after, next_score_after));
    ASSERT_GT(count, 0);
    for (int64_t i = 0; i < count; ++i) {
      EXPECT_EQ(static_cast<uint64_t>(output_count + 1), batch_ids[i]);
      EXPECT_DOUBLE_EQ(5.0, batch_scores[i]);
      ++output_count;
    }
  }
  EXPECT_EQ(EXPECTED_ROW_COUNT, output_count);
  EXPECT_EQ(EXPECTED_CHILD_BATCH_SIZE, disjunction.child_batch_size_);
  EXPECT_EQ(CHILD_COUNT * EXPECTED_CHILD_BATCH_SIZE,
      disjunction.batch_row_capacity_);
  for (int64_t i = 0; i < CHILD_COUNT; ++i) {
    EXPECT_EQ(EXPECTED_CHILD_BATCH_SIZE, child_ops[i]->max_drain_capacity());
    EXPECT_EQ(2, child_ops[i]->drain_count());
  }

  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, disjunction.drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  EXPECT_EQ(0, count);
  EXPECT_TRUE(next_id_after.is_max());
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(boundary, next_score_after));
}

TEST_F(TestDASAsyncSegmentMerge, uint64_disjunction_boundary_lookahead_is_not_repeated)
{
  MockSegmentOp first_child(search_ctx_, {1, 3, 5}, {1.0, 1.0, 1.0});
  MockSegmentOp second_child(search_ctx_, {2, 4, 6}, {2.0, 2.0, 2.0});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, first_child.init(child_param));
  ASSERT_EQ(OB_SUCCESS, second_child.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&second_child));
  ObDASDisjunctionFilterOpParam param(
      children, 1, ObDASSearchCost(1), 4.0);
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, disjunction.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction.open());

  uint64_t batch_ids[8];
  double batch_scores[8];
  int64_t count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_uint64(3);
  ASSERT_EQ(OB_SUCCESS, disjunction.drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(3, count);
  EXPECT_EQ(1, batch_ids[0]);
  EXPECT_EQ(2, batch_ids[1]);
  EXPECT_EQ(3, batch_ids[2]);
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(4, next_id_after.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, next_score_after);

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(rowid, score));
  EXPECT_EQ(5, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(4.0, score);
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(rowid, rowid, score));
  EXPECT_EQ(5, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(rowid, score));
  EXPECT_EQ(6, rowid.get_uint64());
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, uint64_disjunction_seek_only_advances_lagging_children)
{
  std::vector<uint64_t> lagging_ids;
  std::vector<double> lagging_scores;
  for (uint64_t id = 0; id <= 300; ++id) {
    lagging_ids.push_back(id);
    lagging_scores.push_back(1.0);
  }
  MockSegmentOp lagging_child(search_ctx_, lagging_ids, lagging_scores);
  MockSegmentOp middle_child(search_ctx_, {500}, {2.0});
  MockSegmentOp leading_child(search_ctx_, {1000}, {3.0});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, lagging_child.init(child_param));
  ASSERT_EQ(OB_SUCCESS, middle_child.init(child_param));
  ASSERT_EQ(OB_SUCCESS, leading_child.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&lagging_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&middle_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&leading_child));
  ObDASDisjunctionFilterOpParam param(
      children, 1, ObDASSearchCost(1), 6.0);
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, disjunction.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction.open());

  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(300);
  ASSERT_EQ(OB_SUCCESS, disjunction.advance_to(target, rowid, score));
  EXPECT_EQ(300, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(6.0, score);
  EXPECT_EQ(1, lagging_child.advance_count());
  EXPECT_EQ(0, middle_child.advance_count());
  EXPECT_EQ(0, leading_child.advance_count());

  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(rowid, score));
  EXPECT_EQ(500, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(rowid, score));
  EXPECT_EQ(1000, rowid.get_uint64());
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, uint64_disjunction_eof_is_persistent_until_rescan)
{
  MockSegmentOp first_child(search_ctx_, {1, 3}, {1.0, 1.0});
  MockSegmentOp second_child(search_ctx_, {2, 4}, {2.0, 2.0});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, first_child.init(child_param));
  ASSERT_EQ(OB_SUCCESS, second_child.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&second_child));
  ObDASDisjunctionFilterOpParam param(
      children, 1, ObDASSearchCost(1), 4.0);
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, disjunction.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction.open());

  ObDASRowID rowid;
  double score = 0.0;
  for (uint64_t expected_id = 1; expected_id <= 4; ++expected_id) {
    ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(rowid, score));
    EXPECT_EQ(expected_id, rowid.get_uint64());
  }
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(rowid, score));
  ObDASRowID last_id;
  last_id.set_uint64(4);
  EXPECT_EQ(OB_ITER_END, disjunction.advance_to(last_id, rowid, score));
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(rowid, score));

  ASSERT_EQ(OB_SUCCESS, disjunction.rescan());
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, disjunction.rescan());
  ObDASRowID max_id;
  max_id.set_max();
  EXPECT_EQ(OB_ITER_END, disjunction.advance_to(max_id, rowid, score));
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(rowid, score));
  EXPECT_EQ(0, first_child.drain_count());
  EXPECT_EQ(0, second_child.drain_count());

  ASSERT_EQ(OB_SUCCESS, disjunction.rescan());
  ASSERT_EQ(OB_SUCCESS, disjunction.next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  ASSERT_EQ(OB_SUCCESS, disjunction.rescan());

  uint64_t batch_ids[8];
  double batch_scores[8];
  int64_t count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_max();
  ASSERT_EQ(OB_SUCCESS, disjunction.drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(4, count);
  EXPECT_TRUE(next_id_after.is_max());
  EXPECT_EQ(OB_ITER_END, disjunction.advance_to(last_id, rowid, score));
  EXPECT_EQ(OB_ITER_END, disjunction.next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, uint64_disjunction_drain_falls_back_for_min_should_match)
{
  MockSegmentOp first_child(search_ctx_, {1, 3, 5}, {1.0, 1.0, 1.0});
  MockSegmentOp second_child(search_ctx_, {2, 3, 5}, {2.0, 2.0, 2.0});
  MockSegmentOp third_child(search_ctx_, {3, 4, 5}, {3.0, 3.0, 3.0});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, first_child.init(child_param));
  ASSERT_EQ(OB_SUCCESS, second_child.init(child_param));
  ASSERT_EQ(OB_SUCCESS, third_child.init(child_param));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(&first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&second_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(&third_child));
  ObDASDisjunctionFilterOpParam param(
      children, 2, ObDASSearchCost(1), 6.0);
  ObDASDisjunctionFilterOp disjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, disjunction.init(param));
  ASSERT_EQ(OB_SUCCESS, disjunction.open());
  EXPECT_FALSE(disjunction.use_batch_merge_);

  uint64_t batch_ids[4];
  double batch_scores[4];
  int64_t count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_max();
  ASSERT_EQ(OB_SUCCESS, disjunction.drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(2, count);
  EXPECT_EQ(3, batch_ids[0]);
  EXPECT_EQ(5, batch_ids[1]);
  EXPECT_DOUBLE_EQ(6.0, batch_scores[0]);
  EXPECT_DOUBLE_EQ(6.0, batch_scores[1]);
  EXPECT_TRUE(next_id_after.is_max());
}

TEST_F(TestDASAsyncSegmentMerge, uint64_disjunction_falls_back_for_candidate_relative_child)
{
  MockSegmentOp leaf(search_ctx_, {100}, {1.0});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, leaf.init(child_param));

  ObArray<ObIDASSearchOp *> candidate_relative_children;
  ASSERT_EQ(OB_SUCCESS, candidate_relative_children.push_back(&leaf));
  ObDASDisjunctionFilterOpParam candidate_relative_param(
      candidate_relative_children, 0, ObDASSearchCost(1));
  ObDASDisjunctionFilterOp candidate_relative(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, candidate_relative.init(candidate_relative_param));

  ObArray<ObIDASSearchOp *> outer_children;
  ASSERT_EQ(OB_SUCCESS, outer_children.push_back(&candidate_relative));
  ObDASDisjunctionFilterOpParam outer_param(
      outer_children, 1, ObDASSearchCost(1));
  ObDASDisjunctionFilterOp outer(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, outer.init(outer_param));
  ASSERT_EQ(OB_SUCCESS, outer.open());

  EXPECT_FALSE(candidate_relative.can_enumerate_rowids());
  EXPECT_FALSE(outer.use_batch_merge_);
  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(42);
  ASSERT_EQ(OB_SUCCESS, outer.advance_to(target, rowid, score));
  EXPECT_EQ(42, rowid.get_uint64());
}

TEST_F(TestDASAsyncSegmentMerge, composite_enumerability_follows_rowid_drivers)
{
  MockSegmentOp enumerable_leaf(search_ctx_, {2, 4}, {1.0, 1.0});
  MockSegmentOp candidate_leaf(search_ctx_, {100}, {1.0});
  MockProbeSegmentOp probe_leaf(search_ctx_, {}, {});
  MockSegmentOpParam child_param;
  ASSERT_EQ(OB_SUCCESS, enumerable_leaf.init(child_param));
  ASSERT_EQ(OB_SUCCESS, candidate_leaf.init(child_param));
  ASSERT_EQ(OB_SUCCESS, probe_leaf.init(child_param));

  ObArray<ObIDASSearchOp *> candidate_children;
  ASSERT_EQ(OB_SUCCESS, candidate_children.push_back(&candidate_leaf));
  ObDASDisjunctionFilterOpParam candidate_param(
      candidate_children, 0, ObDASSearchCost(1));
  ObDASDisjunctionFilterOp candidate_relative(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, candidate_relative.init(candidate_param));
  EXPECT_FALSE(candidate_relative.can_enumerate_rowids());

  ObArray<ObIDASSearchOp *> enumerable_segment_children;
  ASSERT_EQ(OB_SUCCESS, enumerable_segment_children.push_back(&enumerable_leaf));
  ObDASAsyncSegmentMergeOpParam enumerable_merge_param(
      enumerable_segment_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp enumerable_merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, enumerable_merge.init(enumerable_merge_param));
  EXPECT_TRUE(enumerable_merge.can_enumerate_rowids());

  ObArray<ObIDASSearchOp *> mixed_segment_children;
  ASSERT_EQ(OB_SUCCESS, mixed_segment_children.push_back(&enumerable_leaf));
  ASSERT_EQ(OB_SUCCESS, mixed_segment_children.push_back(&candidate_relative));
  ObDASAsyncSegmentMergeOpParam mixed_merge_param(
      mixed_segment_children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp mixed_merge(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, mixed_merge.init(mixed_merge_param));
  EXPECT_FALSE(mixed_merge.can_enumerate_rowids());

  ObArray<ObIDASSearchOp *> conjunction_children;
  ASSERT_EQ(OB_SUCCESS, conjunction_children.push_back(&enumerable_leaf));
  ASSERT_EQ(OB_SUCCESS, conjunction_children.push_back(&probe_leaf));
  ObDASConjunctionOpParam conjunction_param(conjunction_children);
  ObDASConjunctionOp conjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, conjunction.init(conjunction_param));
  EXPECT_TRUE(conjunction.can_enumerate_rowids());

  ObArray<ObIDASSearchOp *> non_enumerable_conjunction_children;
  ASSERT_EQ(OB_SUCCESS, non_enumerable_conjunction_children.push_back(&enumerable_leaf));
  ASSERT_EQ(OB_SUCCESS, non_enumerable_conjunction_children.push_back(&candidate_relative));
  ObDASConjunctionOpParam non_enumerable_conjunction_param(
      non_enumerable_conjunction_children);
  ObDASConjunctionOp non_enumerable_conjunction(search_ctx_);
  ASSERT_EQ(OB_SUCCESS,
      non_enumerable_conjunction.init(non_enumerable_conjunction_param));
  EXPECT_FALSE(non_enumerable_conjunction.can_enumerate_rowids());

  ObDASReqOptOpParam req_opt_param(&enumerable_leaf, &candidate_relative, true);
  ObDASReqOptOp req_opt(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, req_opt.init(req_opt_param));
  EXPECT_TRUE(req_opt.can_enumerate_rowids());

  ObDASReqExclOpParam req_excl_param(&enumerable_leaf, &candidate_relative);
  ObDASReqExclOp req_excl(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, req_excl.init(req_excl_param));
  EXPECT_TRUE(req_excl.can_enumerate_rowids());

  ObDASReqOptOpParam non_enumerable_req_opt_param(
      &candidate_relative, &enumerable_leaf, true);
  ObDASReqOptOp non_enumerable_req_opt(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, non_enumerable_req_opt.init(non_enumerable_req_opt_param));
  EXPECT_FALSE(non_enumerable_req_opt.can_enumerate_rowids());

  ObDASReqExclOpParam non_enumerable_req_excl_param(
      &candidate_relative, &enumerable_leaf);
  ObDASReqExclOp non_enumerable_req_excl(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, non_enumerable_req_excl.init(non_enumerable_req_excl_param));
  EXPECT_FALSE(non_enumerable_req_excl.can_enumerate_rowids());
}

TEST_F(TestDASAsyncSegmentMerge, multi_segment_batch_merges_all_children)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  std::vector<uint64_t> rowids[3];
  std::vector<double> scores[3];
  for (uint64_t id = 0; id < 900; ++id) {
    const int64_t child_idx = id % 3;
    rowids[child_idx].push_back(id);
    scores[child_idx].push_back(static_cast<double>(id) + 0.5);
  }
  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *child_ops[3] = {nullptr, nullptr, nullptr};
  for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
    child_ops[i] = create_child(rowids[i], scores[i]);
    ASSERT_NE(nullptr, child_ops[i]);
    ASSERT_EQ(OB_SUCCESS, children.push_back(child_ops[i]));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(false);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);

  static constexpr int64_t BATCH_SIZE = 173;
  uint64_t batch_ids[BATCH_SIZE];
  double batch_scores[BATCH_SIZE];
  ObDASRowID boundary;
  boundary.set_max();
  int64_t output_count = 0;
  bool end = false;
  while (!end) {
    ObDASRowID next_id_after;
    double next_score_after = 0.0;
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, merge->drain_until(
        boundary, BATCH_SIZE, batch_ids, batch_scores,
        count, next_id_after, next_score_after));
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_LT(output_count, 900);
      EXPECT_EQ(static_cast<uint64_t>(output_count), batch_ids[i]);
      EXPECT_DOUBLE_EQ(0.0, batch_scores[i]);
      ++output_count;
    }
    if (count < BATCH_SIZE) {
      EXPECT_TRUE(next_id_after.is_max());
      end = true;
    }
  }
  EXPECT_EQ(900, output_count);
  for (int64_t i = 0; i < ARRAYSIZEOF(child_ops); ++i) {
    EXPECT_EQ(3, child_ops[i]->drain_count());
    EXPECT_EQ(BATCH_SIZE, child_ops[i]->max_drain_capacity());
  }
  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, segment_merge_finite_boundary_preserves_batch_capacity)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1});
  std::vector<uint64_t> rowids;
  std::vector<double> scores;
  for (uint64_t id = 0; id < 300; ++id) {
    rowids.push_back(id);
    scores.push_back(static_cast<double>(id) + 0.5);
  }
  MockSegmentOp *child = create_child(rowids, scores);
  ObArray<ObIDASSearchOp *> children;
  ASSERT_NE(nullptr, child);
  ASSERT_EQ(OB_SUCCESS, children.push_back(child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());

  static constexpr int64_t BATCH_SIZE = 173;
  uint64_t batch_ids[BATCH_SIZE];
  double batch_scores[BATCH_SIZE];
  int64_t count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_uint64(100);
  ASSERT_EQ(OB_SUCCESS, merge->drain_until(
      boundary, BATCH_SIZE, batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(101, count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i), batch_ids[i]);
    EXPECT_DOUBLE_EQ(static_cast<double>(i) + 0.5, batch_scores[i]);
  }
  ASSERT_TRUE(next_id_after.is_normal());
  EXPECT_EQ(101, next_id_after.get_uint64());
  EXPECT_DOUBLE_EQ(101.5, next_score_after);
  EXPECT_EQ(2, child->drain_count());
  EXPECT_EQ(BATCH_SIZE, child->max_drain_capacity());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(102, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(102.5, score);
}

TEST_F(TestDASAsyncSegmentMerge, multi_segment_scoring_batch_preserves_scores)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  std::vector<uint64_t> first_ids;
  std::vector<uint64_t> second_ids;
  std::vector<double> first_scores;
  std::vector<double> second_scores;
  for (uint64_t id = 0; id < 300; ++id) {
    first_ids.push_back(id);
    first_scores.push_back(static_cast<double>(id) + 0.25);
  }
  for (uint64_t id = 300; id < 600; ++id) {
    second_ids.push_back(id);
    second_scores.push_back(static_cast<double>(id) + 0.5);
  }

  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *first_child = create_child(first_ids, first_scores);
  MockSegmentOp *second_child = create_child(second_ids, second_scores);
  MockSegmentOp *duplicate_child = create_child({100, 350}, {1100.0, 1350.0});
  ASSERT_NE(nullptr, first_child);
  ASSERT_NE(nullptr, second_child);
  ASSERT_NE(nullptr, duplicate_child);
  ASSERT_EQ(OB_SUCCESS, children.push_back(first_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(second_child));
  ASSERT_EQ(OB_SUCCESS, children.push_back(duplicate_child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);

  static constexpr int64_t BATCH_SIZE = 73;
  uint64_t batch_ids[BATCH_SIZE];
  double batch_scores[BATCH_SIZE];
  ObDASRowID boundary;
  boundary.set_max();
  int64_t output_count = 0;
  bool end = false;
  while (!end) {
    ObDASRowID next_id_after;
    double next_score_after = 0.0;
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, merge->drain_until(
        boundary, BATCH_SIZE, batch_ids, batch_scores,
        count, next_id_after, next_score_after));
    for (int64_t i = 0; i < count; ++i) {
      ASSERT_LT(output_count, 600);
      EXPECT_EQ(static_cast<uint64_t>(output_count), batch_ids[i]);
      const double expected_score = 100 == output_count
          ? 1100.0
          : (350 == output_count
              ? 1350.0
              : (output_count < 300
                  ? static_cast<double>(output_count) + 0.25
                  : static_cast<double>(output_count) + 0.5));
      EXPECT_DOUBLE_EQ(expected_score, batch_scores[i]);
      ++output_count;
    }
    if (count < BATCH_SIZE) {
      EXPECT_TRUE(next_id_after.is_max());
      end = true;
    }
  }
  EXPECT_EQ(600, output_count);
  EXPECT_EQ(6, first_child->drain_count());
  EXPECT_EQ(6, second_child->drain_count());
  EXPECT_EQ(2, duplicate_child->drain_count());
  EXPECT_EQ(BATCH_SIZE, first_child->max_drain_capacity());
  EXPECT_EQ(BATCH_SIZE, second_child->max_drain_capacity());
  EXPECT_EQ(BATCH_SIZE, duplicate_child->max_drain_capacity());
}

TEST_F(TestDASAsyncSegmentMerge, docid_range_limits_batch_k_way_merge)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_uint64(4);
  range_hi.set_uint64(7);
  search_ctx_.set_docid_range(range_lo, range_hi);
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({1, 4, 7}, {1.0, 1.0, 1.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({2, 5, 8}, {2.0, 2.0, 2.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({3, 6, 9}, {3.0, 3.0, 3.0})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_TRUE(merge->use_batch_merge_);
  const uint64_t expected_ids[] = {4, 5, 6, 7};
  const double expected_scores[] = {1.0, 2.0, 3.0, 1.0};
  uint64_t batch_ids[8];
  double batch_scores[8];
  int64_t count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_max();
  ASSERT_EQ(OB_SUCCESS, merge->drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(ARRAYSIZEOF(expected_ids), count);
  EXPECT_TRUE(next_id_after.is_max());
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(expected_ids[i], batch_ids[i]);
    EXPECT_DOUBLE_EQ(expected_scores[i], batch_scores[i]);
  }
  ObDASRowID rowid;
  double score = 0.0;
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, docid_range_clamps_advance_target)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_uint64(4);
  range_hi.set_uint64(7);
  search_ctx_.set_docid_range(range_lo, range_hi);
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({1, 4, 7}, {1.0, 1.0, 1.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({2, 5, 8}, {2.0, 2.0, 2.0})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);

  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_min();
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  target.set_uint64(8);
  EXPECT_EQ(OB_ITER_END, merge->advance_to(target, rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, duplicate_rowid_across_segments_is_emitted_once)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({1, 4, 7}, {1.0, 1.0, 1.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({2, 4, 8}, {2.0, 2.0, 2.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({3, 4, 9}, {3.0, 3.0, 3.0})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  const uint64_t expected_ids[] = {1, 2, 3, 4, 7, 8, 9};
  const double expected_scores[] = {1.0, 2.0, 3.0, 3.0, 1.0, 2.0, 3.0};
  ObDASRowID rowid;
  double score = 0.0;
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
    ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
    EXPECT_EQ(expected_ids[i], rowid.get_uint64());
    EXPECT_DOUBLE_EQ(expected_scores[i], score);
  }
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, duplicate_rowid_preserves_advance_idempotence)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({1, 4, 7}, {1.0, 1.0, 1.0})));
  ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({4, 8}, {2.0, 2.0})));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(rowid, rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(7, rowid.get_uint64());
}

TEST_F(TestDASAsyncSegmentMerge, rescan_uses_current_partition_real_segment_count)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {3, 1});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  MockSegmentOp *child0 = create_child({1}, {1.0});
  MockSegmentOp *child1 = create_child({2}, {1.0});
  MockSegmentOp *child2 = create_child({3}, {1.0});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(child1));
  ASSERT_EQ(OB_SUCCESS, children.push_back(child2));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_FALSE(merge->use_ordered_concat_);
  ASSERT_EQ(OB_SUCCESS, merge->set_min_competitive_score(4.0));
  EXPECT_DOUBLE_EQ(4.0, child0->last_threshold());
  EXPECT_DOUBLE_EQ(-1.0, child1->last_threshold());
  EXPECT_DOUBLE_EQ(-1.0, child2->last_threshold());
  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
  EXPECT_EQ(2, child0->bind_count());
  EXPECT_EQ(1, child1->bind_count());
  EXPECT_EQ(1, child2->bind_count());
}

TEST_F(TestDASAsyncSegmentMerge, batch_buffer_grows_with_active_segment_count)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1, 3});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  for (uint64_t i = 0; i < 3; ++i) {
    ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({i + 1}, {1.0})));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_TRUE(merge->use_batch_merge_);
  EXPECT_EQ(1, merge->active_segment_cnt_);
  EXPECT_EQ(1, merge->batch_child_capacity_);

  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_TRUE(merge->use_batch_merge_);
  EXPECT_EQ(3, merge->active_segment_cnt_);
  EXPECT_EQ(3, merge->batch_child_capacity_);
}

TEST_F(TestDASAsyncSegmentMerge, batch_merge_falls_back_at_memory_budget)
{
  const int64_t child_batch_bytes = ObDASAsyncSegmentMergeOp::CHILD_BATCH_SIZE
      * (sizeof(uint64_t) + sizeof(double))
      + ObDASAsyncSegmentMergeOp::BATCH_SCORE_BLOCK_COUNT * sizeof(double);
  const int64_t child_count =
      ObDASAsyncSegmentMergeOp::BATCH_BUFFER_BYTE_BUDGET / child_batch_bytes + 1;
  // The budget uses prebuilt child count, not the current partition's Segment count.
  // Keep its table store valid while retaining enough rows to verify fallback reads.
  const int64_t active_segment_count = 5;
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {active_segment_count});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < child_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, children.push_back(
        create_child({static_cast<uint64_t>(i + 1)}, {static_cast<double>(i + 1)})));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_FALSE(merge->use_batch_merge_);
  EXPECT_EQ(0, merge->batch_child_capacity_);
  EXPECT_EQ(child_count, merge->children_cnt_);
  EXPECT_EQ(active_segment_count, merge->active_segment_cnt_);
  EXPECT_EQ(nullptr, merge->batch_ids_);
  EXPECT_EQ(nullptr, merge->batch_scores_);
  EXPECT_EQ(nullptr, merge->batch_score_suffix_maxes_);

  uint64_t batch_ids[4];
  double batch_scores[4];
  int64_t count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_max();
  ASSERT_EQ(OB_SUCCESS, merge->drain_until(
      boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
      count, next_id_after, next_score_after));
  ASSERT_EQ(ARRAYSIZEOF(batch_ids), count);
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(static_cast<uint64_t>(i + 1), batch_ids[i]);
    EXPECT_DOUBLE_EQ(static_cast<double>(i + 1), batch_scores[i]);
  }

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(5, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(5.0, score);
}

TEST_F(TestDASAsyncSegmentMerge, scoring_batch_uses_block_suffix_max)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1});
  std::vector<uint64_t> rowids;
  std::vector<double> scores;
  for (uint64_t id = 0; id < 256; ++id) {
    rowids.push_back(id);
    scores.push_back(static_cast<double>(256 - id));
  }
  MockSegmentOp *child = create_child(rowids, scores);
  ObArray<ObIDASSearchOp *> children;
  ASSERT_NE(nullptr, child);
  ASSERT_EQ(OB_SUCCESS, children.push_back(child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  ASSERT_EQ(OB_SUCCESS, merge->refill_batch_child(0, 256));

  ObDASRowID target;
  target.set_uint64(17);
  const MaxScoreTuple *max_score_tuple = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target, true, max_score_tuple));
  ASSERT_NE(nullptr, max_score_tuple);
  EXPECT_EQ(17, max_score_tuple->get_min_id().get_uint64());
  EXPECT_EQ(255, max_score_tuple->get_max_id().get_uint64());
  EXPECT_DOUBLE_EQ(239.0, max_score_tuple->get_max_score());
  EXPECT_EQ(0, child->shallow_count());
}

TEST_F(TestDASAsyncSegmentMerge, single_active_segment_uses_heapless_fast_path_across_rescan)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{1, 1}, {3, 1}, {1, 1}}, {false, false, false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  ASSERT_EQ(3, catalog.get_max_segment_cnt(0, TEST_INDEX_TABLE_ID));
  MockSegmentOp *child0 = create_child({1, 4, 7}, {1.0, 1.0, 1.0});
  MockSegmentOp *child1 = create_child({2, 5, 8}, {2.0, 2.0, 2.0});
  MockSegmentOp *child2 = create_child({3, 6, 9}, {3.0, 3.0, 3.0});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(child1));
  ASSERT_EQ(OB_SUCCESS, children.push_back(child2));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_uint64(2);
  range_hi.set_uint64(6);
  search_ctx_.set_docid_range(range_lo, range_hi);
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_EQ(1, merge->active_segment_cnt_);
  EXPECT_TRUE(merge->use_single_child_fast_path());
  EXPECT_TRUE(merge->heap_child_indices_.empty());
  EXPECT_EQ(1, child0->open_count());
  EXPECT_EQ(0, child1->open_count());
  EXPECT_EQ(0, child2->open_count());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_TRUE(merge->heap_child_indices_.empty());
  EXPECT_EQ(1, merge->pending_child_indices_.count());
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(rowid, rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
  EXPECT_TRUE(merge->heap_child_indices_.empty());
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));

  select_partition(catalog, 2);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_EQ(3, merge->active_segment_cnt_);
  EXPECT_FALSE(merge->use_single_child_fast_path());
  EXPECT_EQ(3, merge->heap_child_indices_.count());
  EXPECT_TRUE(merge->pending_child_indices_.empty());
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());

  select_partition(catalog, 3);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_EQ(1, merge->active_segment_cnt_);
  EXPECT_TRUE(merge->use_single_child_fast_path());
  EXPECT_TRUE(merge->heap_child_indices_.empty());
  EXPECT_EQ(2, child0->rescan_count());
  EXPECT_EQ(0, child1->rescan_count());
  EXPECT_EQ(0, child2->rescan_count());
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(4, rowid.get_uint64());
}

TEST_F(TestDASAsyncSegmentMerge, single_active_segment_batch_direct_copy_across_rescan)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{1, 1}, {3, 1}, {1, 1}}, {false, false, false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  std::vector<uint64_t> child_rowids[3];
  std::vector<double> child_scores[3];
  std::vector<uint64_t> single_expected_ids;
  std::vector<double> single_expected_scores;
  std::vector<uint64_t> merged_expected_ids;
  std::vector<double> merged_expected_scores;
  for (uint64_t id = 1; id <= 771; ++id) {
    const int64_t child_idx = static_cast<int64_t>((id - 1) % 3);
    child_rowids[child_idx].push_back(id);
    child_scores[child_idx].push_back(static_cast<double>(id));
    merged_expected_ids.push_back(id);
    merged_expected_scores.push_back(static_cast<double>(id));
    if (0 == child_idx) {
      single_expected_ids.push_back(id);
      single_expected_scores.push_back(static_cast<double>(id));
    }
  }

  ObArray<ObIDASSearchOp *> children;
  MockSegmentOp *child_ops[3] = {nullptr, nullptr, nullptr};
  for (int64_t i = 0; i < 3; ++i) {
    child_ops[i] = create_child(child_rowids[i], child_scores[i]);
    ASSERT_NE(nullptr, child_ops[i]);
    ASSERT_EQ(OB_SUCCESS, children.push_back(child_ops[i]));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);

  const std::vector<uint64_t> *expected_ids[] = {
      &single_expected_ids, &merged_expected_ids, &single_expected_ids};
  const std::vector<double> *expected_scores[] = {
      &single_expected_scores, &merged_expected_scores, &single_expected_scores};
  for (int64_t phase = 0; phase < 3; ++phase) {
    if (phase > 0) {
      select_partition(catalog, phase + 1);
      ASSERT_EQ(OB_SUCCESS, merge->rescan());
      EXPECT_FALSE(merge->use_ordered_concat_);
    }
    EXPECT_TRUE(merge->use_batch_merge_);
    if (1 == phase) {
      EXPECT_FALSE(merge->use_single_child_fast_path());
      EXPECT_EQ(3, merge->heap_child_indices_.count());
    } else {
      EXPECT_TRUE(merge->use_single_child_fast_path());
      EXPECT_TRUE(merge->heap_child_indices_.empty());
    }

    std::vector<uint64_t> actual_ids;
    std::vector<double> actual_scores;
    bool iter_end = false;
    for (int64_t drain_call = 0; !iter_end && drain_call < 32; ++drain_call) {
      uint64_t batch_ids[31];
      double batch_scores[31];
      int64_t count = 0;
      ObDASRowID boundary;
      ObDASRowID next_id_after;
      double next_score_after = 0.0;
      boundary.set_max();
      ASSERT_EQ(OB_SUCCESS, merge->drain_until(
          boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
          count, next_id_after, next_score_after));
      ASSERT_LE(count, ARRAYSIZEOF(batch_ids));
      for (int64_t i = 0; i < count; ++i) {
        actual_ids.push_back(batch_ids[i]);
        actual_scores.push_back(batch_scores[i]);
      }
      iter_end = next_id_after.is_max();
      EXPECT_TRUE(count > 0 || iter_end);
    }
    ASSERT_TRUE(iter_end);
    EXPECT_EQ(*expected_ids[phase], actual_ids);
    EXPECT_EQ(*expected_scores[phase], actual_scores);
    const int64_t active_child_count = 1 == phase ? 3 : 1;
    for (int64_t child_idx = 0; child_idx < active_child_count; ++child_idx) {
      EXPECT_EQ(10, child_ops[child_idx]->drain_count());
      EXPECT_EQ(31, child_ops[child_idx]->max_drain_capacity());
    }

    uint64_t batch_ids[1];
    double batch_scores[1];
    int64_t count = -1;
    ObDASRowID boundary;
    ObDASRowID next_id_after;
    double next_score_after = 0.0;
    boundary.set_max();
    ASSERT_EQ(OB_SUCCESS, merge->drain_until(
        boundary, ARRAYSIZEOF(batch_ids), batch_ids, batch_scores,
        count, next_id_after, next_score_after));
    EXPECT_EQ(0, count);
    EXPECT_TRUE(next_id_after.is_max());
  }

  ObObj range_lo;
  ObObj range_hi;
  range_lo.set_uint64(4);
  range_hi.set_uint64(94);
  search_ctx_.set_docid_range(range_lo, range_hi);
  select_partition(catalog, 3);
  ASSERT_EQ(OB_SUCCESS, merge->rescan());
  EXPECT_FALSE(merge->use_ordered_concat_);
  EXPECT_TRUE(merge->use_single_child_fast_path());
  EXPECT_TRUE(merge->heap_child_indices_.empty());
  uint64_t range_ids[31];
  double range_scores[31];
  int64_t range_count = 0;
  ObDASRowID boundary;
  ObDASRowID next_id_after;
  double next_score_after = 0.0;
  boundary.set_max();
  ASSERT_EQ(OB_SUCCESS, merge->drain_until(
      boundary, ARRAYSIZEOF(range_ids), range_ids, range_scores,
      range_count, next_id_after, next_score_after));
  ASSERT_EQ(ARRAYSIZEOF(range_ids), range_count);
  for (int64_t i = 0; i < range_count; ++i) {
    const uint64_t expected_id = static_cast<uint64_t>(4 + 3 * i);
    EXPECT_EQ(expected_id, range_ids[i]);
    EXPECT_DOUBLE_EQ(static_cast<double>(expected_id), range_scores[i]);
  }
  ASSERT_EQ(OB_SUCCESS, merge->drain_until(
      boundary, ARRAYSIZEOF(range_ids), range_ids, range_scores,
      range_count, next_id_after, next_score_after));
  EXPECT_EQ(0, range_count);
  EXPECT_TRUE(next_id_after.is_max());
}

TEST_F(TestDASAsyncSegmentMerge, forced_single_active_segment_keeps_heap_path)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{1, 1}, {2, 1}}, {false, false});
  MockSegmentOp *child0 = create_child({1, 3}, {1.0, 1.0});
  MockSegmentOp *child1 = create_child({2, 4}, {2.0, 2.0});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(child1));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID, 0), true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_single_child_fast_path());
  EXPECT_EQ(1, merge->heap_child_indices_.count());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
}

TEST_F(TestDASAsyncSegmentMerge, advance_and_threshold_cover_all_active_children)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  MockSegmentOp *child0 = create_child({1, 5, 9}, {1.0, 1.0, 1.0});
  MockSegmentOp *child1 = create_child({2, 6, 10}, {2.0, 2.0, 2.0});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(child1));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);
  ASSERT_EQ(OB_SUCCESS, merge->set_min_competitive_score(3.5));
  EXPECT_DOUBLE_EQ(3.5, child0->last_threshold());
  EXPECT_DOUBLE_EQ(3.5, child1->last_threshold());
  double max_score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->calc_max_score(max_score));
  EXPECT_DOUBLE_EQ(2.0, max_score);
  ObDASRowID target;
  ObDASRowID rowid;
  double score = 0.0;
  target.set_uint64(6);
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(6, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(2.0, score);
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(9, rowid.get_uint64());
}

TEST_F(TestDASAsyncSegmentMerge, shallow_uses_min_range_end_and_max_segment_score)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {2});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));
  MockSegmentOp *child0 = create_child({1, 5, 9}, {0.2, 0.4, 0.3});
  MockSegmentOp *child1 = create_child({2, 6, 10}, {0.7, 0.5, 0.6});
  ObArray<ObIDASSearchOp *> children;
  ASSERT_EQ(OB_SUCCESS, children.push_back(child0));
  ASSERT_EQ(OB_SUCCESS, children.push_back(child1));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_FALSE(merge->use_ordered_concat_);

  ObDASRowID target;
  const MaxScoreTuple *max_score_tuple = nullptr;
  target.set_uint64(1);
  ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target, true, max_score_tuple));
  ASSERT_NE(nullptr, max_score_tuple);
  EXPECT_EQ(1, max_score_tuple->get_min_id().get_uint64());
  EXPECT_EQ(1, max_score_tuple->get_max_id().get_uint64());
  EXPECT_DOUBLE_EQ(0.7, max_score_tuple->get_max_score());
  // Row-wise open only primes one head per child, so the first shallow range
  // is the cached point rather than the whole child stream.
  EXPECT_EQ(0, child0->shallow_count());
  EXPECT_EQ(0, child1->shallow_count());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(1, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.2, score);
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(2, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.7, score);

  target.set_uint64(9);
  ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target, false, max_score_tuple));
  ASSERT_NE(nullptr, max_score_tuple);
  EXPECT_EQ(10, max_score_tuple->get_min_id().get_uint64());
  EXPECT_EQ(10, max_score_tuple->get_max_id().get_uint64());
  EXPECT_DOUBLE_EQ(0.6, max_score_tuple->get_max_score());
  EXPECT_EQ(1, child0->shallow_count());
  EXPECT_EQ(1, child1->shallow_count());
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(10, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(0.6, score);
  EXPECT_EQ(OB_ITER_END, merge->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, scoring_shallow_materializes_beyond_cached_batch)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {1});
  std::vector<uint64_t> rowids;
  std::vector<double> scores;
  for (uint64_t id = 0; id < 300; ++id) {
    rowids.push_back(id);
    scores.push_back(static_cast<double>(id) + 0.5);
  }
  MockSegmentOp *child = create_child(rowids, scores);
  ObArray<ObIDASSearchOp *> children;
  ASSERT_NE(nullptr, child);
  ASSERT_EQ(OB_SUCCESS, children.push_back(child));
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  param.set_is_scoring(true);
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(OB_SUCCESS, merge->open());
  EXPECT_EQ(1, child->drain_count());

  ObDASRowID target;
  target.set_uint64(270);
  const MaxScoreTuple *max_score_tuple = nullptr;
  ASSERT_EQ(OB_SUCCESS, merge->advance_shallow(target, false, max_score_tuple));
  ASSERT_NE(nullptr, max_score_tuple);
  EXPECT_EQ(271, max_score_tuple->get_min_id().get_uint64());
  EXPECT_EQ(299, max_score_tuple->get_max_id().get_uint64());
  EXPECT_DOUBLE_EQ(299.5, max_score_tuple->get_max_score());
  EXPECT_EQ(1, child->shallow_count());

  ObDASRowID rowid;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, merge->advance_to(target, rowid, score));
  EXPECT_EQ(271, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(271.5, score);
  ASSERT_EQ(OB_SUCCESS, merge->next_rowid(rowid, score));
  EXPECT_EQ(272, rowid.get_uint64());
  EXPECT_DOUBLE_EQ(272.5, score);
}

TEST_F(TestDASAsyncSegmentMerge, query_tree_drains_per_index_segment_merge_rows)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_multi_index_catalog(catalog, {{2, 2}}, {false});
  PinnedOverlappingCatalog pinned_storage(allocator_);
  ASSERT_EQ(OB_SUCCESS, pinned_storage.init(catalog));

  ObArray<ObIDASSearchOp *> oceanbase_segment_ops;
  ASSERT_EQ(OB_SUCCESS, oceanbase_segment_ops.push_back(
      create_child({1, 3, 6}, {1.0, 1.0, 1.0})));
  ASSERT_EQ(OB_SUCCESS, oceanbase_segment_ops.push_back(
      create_child({}, {})));
  ObDASAsyncSegmentMergeOpParam oceanbase_merge_param(
      oceanbase_segment_ops, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *oceanbase_merge = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            search_ctx_.create_op(oceanbase_merge_param, oceanbase_merge));

  ObArray<ObIDASSearchOp *> mysql_segment_ops;
  ASSERT_EQ(OB_SUCCESS, mysql_segment_ops.push_back(
      create_child({2}, {1.0})));
  ASSERT_EQ(OB_SUCCESS, mysql_segment_ops.push_back(
      create_child({}, {})));
  ObDASAsyncSegmentMergeOpParam mysql_merge_param(
      mysql_segment_ops, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID_2));
  ObDASAsyncSegmentMergeOp *mysql_merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(mysql_merge_param, mysql_merge));

  ObArray<ObIDASSearchOp *> should_ops;
  ASSERT_EQ(OB_SUCCESS, should_ops.push_back(oceanbase_merge));
  ASSERT_EQ(OB_SUCCESS, should_ops.push_back(mysql_merge));
  ObDASSearchCost lead_cost;
  ObDASDisjunctionOpParam should_param(should_ops, 1, false, lead_cost);
  ObDASDisjunctionOp *should_disjunction = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            search_ctx_.create_op(should_param, should_disjunction));

  ObArray<ObIDASSearchOp *> database_segment_ops;
  ASSERT_EQ(OB_SUCCESS, database_segment_ops.push_back(
      create_child({1, 2, 6, 8}, {1.0, 1.0, 1.0, 1.0})));
  ASSERT_EQ(OB_SUCCESS, database_segment_ops.push_back(
      create_child({}, {})));
  ObDASAsyncSegmentMergeOpParam database_merge_param(
      database_segment_ops, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *database_merge = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            search_ctx_.create_op(database_merge_param, database_merge));

  ObArray<ObIDASSearchOp *> must_ops;
  ASSERT_EQ(OB_SUCCESS, must_ops.push_back(database_merge));
  ASSERT_EQ(OB_SUCCESS, must_ops.push_back(should_disjunction));
  ObDASConjunctionOpParam conjunction_param(must_ops);
  ObDASConjunctionOp *conjunction = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            search_ctx_.create_op(conjunction_param, conjunction));
  ObDASReqExclOpParam root_param(conjunction, create_child({}, {}));
  ObDASReqExclOp *root = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(root_param, root));
  ASSERT_EQ(OB_SUCCESS, root->open());
  EXPECT_FALSE(oceanbase_merge->use_ordered_concat_);
  EXPECT_FALSE(mysql_merge->use_ordered_concat_);
  EXPECT_FALSE(database_merge->use_ordered_concat_);

  const uint64_t expected_ids[] = {1, 2, 6};
  ObDASRowID rowid;
  double score = 0.0;
  for (int64_t i = 0; i < ARRAYSIZEOF(expected_ids); ++i) {
    ASSERT_EQ(OB_SUCCESS, root->next_rowid(rowid, score));
    EXPECT_EQ(expected_ids[i], rowid.get_uint64());
  }
  EXPECT_EQ(OB_ITER_END, root->next_rowid(rowid, score));
}

TEST_F(TestDASAsyncSegmentMerge, close_releases_dynamic_head_arrays)
{
  ObDASAsyncIndexQueryCtx catalog;
  build_catalog(catalog, {5});
  ObArray<ObIDASSearchOp *> children;
  for (uint64_t i = 1; i <= 5; ++i) {
    ASSERT_EQ(OB_SUCCESS, children.push_back(create_child({i}, {1.0})));
  }
  ObDASAsyncSegmentMergeOpParam param(
      children, ObDASAsyncSegmentDomain(TEST_INDEX_TABLE_ID));
  ObDASAsyncSegmentMergeOp *merge = nullptr;
  ASSERT_EQ(OB_SUCCESS, search_ctx_.create_op(param, merge));
  ASSERT_EQ(5, merge->head_rowids_.count());
  ASSERT_EQ(5, merge->head_scores_.count());
  ASSERT_EQ(5, merge->child_ended_.count());
  ASSERT_EQ(OB_SUCCESS, merge->close());
  EXPECT_EQ(0, merge->head_rowids_.count());
  EXPECT_EQ(0, merge->head_scores_.count());
  EXPECT_EQ(0, merge->child_ended_.count());
}

TEST_F(TestDASAsyncSegmentMerge, nested_projects_different_leaf_depths_before_conjunction)
{
  const char short_bytes[][8] = {{0, 0, 0, 0, 0, 0, 0, 0},
                                 {0, 0, 0, 0, 0, 0, 0, 1}};
  const char long_bytes[12] = {0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0};
  ObStringBuffer short_buf(&allocator_);
  ObStringBuffer long_buf(&allocator_);
  ObSearchIndexNestedKeyListWriter short_writer;
  ObSearchIndexNestedKeyListWriter long_writer;
  ASSERT_EQ(OB_SUCCESS, short_writer.init(short_buf));
  ASSERT_EQ(OB_SUCCESS, long_writer.init(long_buf));
  for (int i = 0; i < 2; ++i) {
    const ObDatum datum(short_bytes[i], 8, false);
    ASSERT_EQ(OB_SUCCESS, short_writer.append(datum));
  }
  const ObDatum long_datum(long_bytes, sizeof(long_bytes), false);
  ASSERT_EQ(OB_SUCCESS, long_writer.append(long_datum));
  ObString short_list;
  ObString long_list;
  ASSERT_EQ(OB_SUCCESS, short_writer.build(short_list));
  ASSERT_EQ(OB_SUCCESS, long_writer.build(long_list));
  ObDASAsyncScalarRorOp short_leaf(search_ctx_);
  ObDASAsyncScalarRorOp long_leaf(search_ctx_);
  short_leaf.scope_ = long_leaf.scope_ = DAS_SCOPE_NESTED_ASYNC;
  short_leaf.nesting_depth_ = 2;
  long_leaf.nesting_depth_ = 3;
  short_leaf.is_first_nk_ = long_leaf.is_first_nk_ = false;
  const ObDatum short_list_datum(short_list.ptr(), short_list.length(), false);
  const ObDatum long_list_datum(long_list.ptr(), long_list.length(), false);
  ASSERT_EQ(OB_SUCCESS, short_leaf.nested_key_list_reader_.init(short_list_datum));
  ASSERT_EQ(OB_SUCCESS, long_leaf.nested_key_list_reader_.init(long_list_datum));
  // Project the deeper leaf to the conjunction's depth before comparing keys.
  ObDASNestedQueryOp long_nested(search_ctx_);
  long_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
  long_nested.nesting_depth_ = 3;
  long_nested.inner_op_ = &long_leaf;
  ObDASConjunctionOp conjunction(search_ctx_);
  conjunction.scope_ = DAS_SCOPE_NESTED_ASYNC;
  conjunction.nesting_depth_ = 2;
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&short_leaf));
  ASSERT_EQ(OB_SUCCESS, conjunction.driver_ops_.push_back(&long_nested));
  ObDASNestedKey key;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(key, score));
  EXPECT_EQ(8, key.length());
  EXPECT_EQ(1, key.value());

  ObDASNestedQueryOp parent_nested(search_ctx_);
  parent_nested.scope_ = DAS_SCOPE_NESTED_ASYNC;
  parent_nested.nesting_depth_ = 2;
  parent_nested.inner_op_ = &conjunction;
  ObDASNestedKey target;
  ASSERT_EQ(OB_SUCCESS, parent_nested.advance_to(target, key, score));
  EXPECT_EQ(4, key.length());
  EXPECT_EQ(0, key.value());
  EXPECT_EQ(OB_ITER_END, parent_nested.next_rowid(key, score));
}

// Fixed posting seam: exercise the production DAS operators, decoded posting
// cursor, position-value codec and phrase counter. Scoring tests inject fixed
// corpus statistics; this fixture does not exercise SSTable publication.
class SearchTextPostingService : public storage::ObAccessService
{
public:
  SearchTextPostingService(ObIAllocator &allocator, ObDASSearchCtx &search_ctx)
    : allocator_(allocator), cache_(search_ctx.get_bm25_param_cache()), open_count_(0), expected_column_(2)
  {}

  int init(const ObDASSearchIndexTextInfo &info)
  {
    int ret = OB_SUCCESS;
    expected_column_ = info.column_idx_;
    if (OB_FAIL(ob_write_string(allocator_, info.encoded_path_, expected_path_))) {
      LOG_WARN("failed to copy posting fixture path", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, ObString(7, "alpha\0\0"), alpha_))) {
      LOG_WARN("failed to copy alpha posting fixture token", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, ObString(6, "beta\0\0"), beta_))) {
      LOG_WARN("failed to copy beta posting fixture token", K(ret));
    } else if (OB_FAIL(seed_field_statistics(info))) {
      LOG_WARN("failed to seed field statistics", K(ret));
    }
    return ret;
  }

  // Both scoring and filter scans use the same posting/statistics contract.
  int seed_field_statistics(const ObDASSearchIndexTextInfo &info)
  {
    int ret = OB_SUCCESS;
    const ObTabletID tablet_id(9001);
    OZ(cache_.set_total_doc_cnt(tablet_id, 5));
    OZ(cache_.set_avg_doc_token_cnt(tablet_id, info.column_idx_, info.encoded_path_, 2.0));
    OZ(cache_.set_token_doc_cnt(tablet_id, info.column_idx_, info.encoded_path_, alpha_, 4));
    OZ(cache_.set_token_doc_cnt(tablet_id, info.column_idx_, info.encoded_path_, beta_, 4));
    return ret;
  }

  int async_index_posting_scan(
      const share::ObLSID &ls_id,
      const storage::ObTabletHandle *pinned_tablet_handle,
      const ObNewVersionRange &version_range,
      ObTableScanParam &scan_param,
      blocksstable::ObInvPostingScanParam &param,
      blocksstable::ObInvSSTablePostingIter *&iter) override
  {
    UNUSEDx(ls_id, pinned_tablet_handle, version_range, scan_param);
    int ret = OB_SUCCESS;
    iter = nullptr;
    ++open_count_;
    EXPECT_TRUE(param.need_relevance_params_);
    EXPECT_EQ(5, param.total_doc_cnt_);
    EXPECT_EQ(4, param.doc_freq_);
    EXPECT_DOUBLE_EQ(2.0, param.avg_doc_length_);
    bool alpha = false;
    bool found = false;
    const uint64_t alpha_ids[] = {1, 2, 4, 5};
    const uint64_t beta_ids[] = {1, 3, 4, 5};
    uint64_t *copied_ids = nullptr;
    if (!param.is_valid() || param.search_key_->get_datum_cnt() != 3) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid Search Text posting fixture scan param", K(ret));
    } else {
      const blocksstable::ObDatumRowkey &key = *param.search_key_;
      const bool same_field = key.get_datum(0).get_uint64() == expected_column_
          && key.get_datum(1).get_string() == expected_path_;
      const ObString token = key.get_datum(2).get_string();
      alpha = token == alpha_;
      found = same_field && (alpha || token == beta_);
      void *buf = param.allocator_->alloc(sizeof(*iter));
      if (nullptr == buf) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate posting fixture iterator", K(ret));
      } else {
        iter = new (buf) blocksstable::ObInvSSTablePostingIter();
        iter->is_opened_ = true;
        iter->is_end_ = !found;
        iter->need_posting_value_ = param.need_posting_value_;
      }
    }
    if (OB_FAIL(ret) || !found) {
    } else if (OB_ISNULL(copied_ids = static_cast<uint64_t *>(param.allocator_->alloc(sizeof(alpha_ids))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate posting fixture document ids", K(ret));
    } else {
      const uint64_t *ids = alpha ? alpha_ids : beta_ids;
      MEMCPY(copied_ids, ids, sizeof(alpha_ids));
      iter->curr_block_loaded_ = true;
      iter->curr_meta_.idx_ = 0;
      iter->curr_meta_.locator_.block_idx_ = 0;
      iter->curr_meta_.min_id_ = ids[0];
      iter->curr_meta_.max_id_ = ids[3];
      iter->curr_meta_.doc_count_ = 4;
      blocksstable::ObPostingDocIdView doc_id_view;
      doc_id_view.values_ = copied_ids;
      doc_id_view.width_ = sizeof(uint64_t);
      {
        static const uint64_t alpha_tf[] = {2, 1, 1, 1};
        static const uint64_t beta_tf[] = {1, 1, 1, 1};
        static const uint64_t lengths[] = {3, 1, 2, 3};
        iter->need_relevance_params_ = true;
        blocksstable::ObPostingRelevanceView relevance_view;
        relevance_view.token_freqs_ = alpha ? alpha_tf : beta_tf;
        relevance_view.doc_lengths_ = lengths;
        relevance_view.width_ = sizeof(uint64_t);
        if (OB_FAIL(iter->posting_block_data_.assign(4, doc_id_view, relevance_view))) {
          LOG_WARN("failed to assign posting fixture data", K(ret));
        }
      }
      iter->scanner_.is_inited_ = true;
      iter->scanner_.scan_iter_end_ = true;
    }
    if (OB_SUCC(ret) && found && param.need_posting_value_) {
      ObArray<ObColDesc> cols;
      ObColDesc col;
      col.col_type_.set_varchar();
      blocksstable::ObPostingValueBlockWriteParam write_param;
      blocksstable::ObPostingValueBlockWriter writer;
      blocksstable::ObDatumRow row;
      blocksstable::ObPostingValueBlockDesc desc;
      if (OB_FAIL(cols.push_back(col))) {
        LOG_WARN("failed to append posting fixture column", K(ret));
      } else if (FALSE_IT(write_param.col_descs_ = &cols)) {
      } else if (FALSE_IT(write_param.position_list_col_idx_ = 0)) {
      } else if (OB_FAIL(writer.init(write_param, *param.allocator_))) {
        LOG_WARN("failed to initialize posting fixture writer", K(ret));
      } else if (OB_FAIL(row.init(1))) {
        LOG_WARN("failed to initialize posting fixture row", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < 4; ++i) {
        ObArray<int64_t> positions;
        ObFTSPositionListStore codec;
        char *encoded = nullptr;
        int64_t encoded_len = 0;
        // Row 1: "alpha alpha beta". Row 4 reverses the phrase. Row 5
        // is a multi-value field with alpha at 0 and beta at 101 (gap 100).
        const int64_t position = alpha ? (i == 2 ? 1 : 0)
            : (i == 0 ? 2 : i == 3 ? 101 : 0);
        if (OB_FAIL(positions.push_back(position))) {
          LOG_WARN("failed to append posting fixture position", K(ret), K(i));
        } else if (alpha && i == 0 && OB_FAIL(positions.push_back(1))) {
          LOG_WARN("failed to append repeated alpha position", K(ret));
        } else if (OB_FAIL(codec.encode_and_serialize(*param.allocator_, positions, encoded, encoded_len))) {
          LOG_WARN("failed to encode posting fixture positions", K(ret), K(i));
        } else if (FALSE_IT(row.storage_datums_[0].set_string(encoded, encoded_len))) {
        } else if (OB_FAIL(writer.append_row(row))) {
          LOG_WARN("failed to append posting fixture row", K(ret), K(i));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(writer.build_block(desc))) {
        LOG_WARN("failed to build posting fixture value block", K(ret));
      } else {
        char *value_buf = static_cast<char *>(param.allocator_->alloc(desc.size_));
        if (nullptr == value_buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate posting fixture value block", K(ret), K(desc.size_));
        } else {
          MEMCPY(value_buf, desc.buf_, desc.size_);
          if (OB_FAIL(iter->posting_value_block_reader_.init(value_buf, desc.size_))) {
            LOG_WARN("failed to initialize posting fixture value reader", K(ret));
          } else {
            iter->curr_value_block_loaded_ = true;
          }
        }
      }
    }
    return ret;
  }

  ObIAllocator &allocator_;
  storage::ObInvIdxParamCache &cache_;
  int64_t open_count_;
  uint64_t expected_column_;
  ObString expected_path_;
  ObString alpha_;
  ObString beta_;
};

static int init_search_text_fixture_info(ObDASIRScanCtDef &ir, ObIAllocator &allocator,
    const int64_t column_idx = 2, const ObString &path = ObString("$.title"))
{
  int ret = OB_SUCCESS;
  ir.is_async_index_ = true;
  ir.use_scalar_scan_def_ = true;
  ObColumnRefRawExpr field;
  field.set_ref_id(8000, 19);
  field.set_data_type(ObJsonType);
  ObMatchQueryNode node(allocator);
  node.field_ = &field;
  node.inv_idx_tid_ = TEST_INDEX_TABLE_ID;
  node.search_index_fulltext_info_.path_ = path;
  node.is_async_ = true;
  ObSearchIndexTextInfo &info = node.search_index_fulltext_info_;
  info.main_table_id_ = 8000;
  info.def_table_id_ = 9000;
  info.column_idx_ = column_idx;
  info.index_type_ = share::schema::OB_FTS_INDEX_TYPE_PHRASE_MATCH;
  info.parser_name_ = ObString("space.1");
  info.parser_properties_ = ObString("{\"min_token_size\":3,\"max_token_size\":5}");
  if (OB_FAIL(ir.search_index_fulltext_info_.assign(field, info, allocator))) {
    LOG_WARN("failed to initialize Search Text fixture info", K(ret));
  } else if (OB_ISNULL(ir.search_text_ = OB_NEWx(ObExpr, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate Search Text fixture expression", K(ret));
  } else {
    ir.search_text_->obj_meta_.set_varchar();
    ir.search_text_->obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  }
  return ret;
}

TEST_F(TestDASAsyncSegmentMerge, search_text_missing_expression_returns_error)
{
  ObDASIRScanCtDef ir(allocator_);
  ObDASIRScanRtDef rt;
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  ir.search_text_ = nullptr;

  ObDASAsyncTokenOpParam param;
  param.ir_ctdef_ = &ir;
  param.ir_rtdef_ = &rt;
  param.index_table_id_ = TEST_INDEX_TABLE_ID;
  param.token_boost_ = 1.0;
  param.query_token_ = ObString("alpha");
  ObDASAsyncTokenOp token(search_ctx_);
  EXPECT_EQ(OB_ERR_UNEXPECTED, token.init(param));
  EXPECT_FALSE(token.is_inited_);

  ObDASAsyncTokenHelperParam hp;
  hp.ir_ctdef_ = &ir;
  hp.ir_rtdef_ = &rt;
  hp.index_table_id_ = TEST_INDEX_TABLE_ID;
  hp.token_boost_ = 1.0;
  hp.query_token_ = ObString("alpha");
  ObDASAsyncTokenHelper helper(search_ctx_);
  EXPECT_EQ(OB_ERR_UNEXPECTED, helper.init(hp));
  EXPECT_FALSE(helper.is_inited_);

  ObDASScalarScanCtDef scalar(allocator_);
  ObDASBaseCtDef *children[] = {&scalar};
  ir.children_ = children;
  ir.children_cnt_ = 1;
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_ctdefs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_tablet_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_ctdefs_.push_back(&scalar));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_tablet_ids_.push_back(ObTabletID(9001)));
  ObArray<ObString> tokens;
  ObArray<int64_t> costs;
  EXPECT_EQ(OB_SUCCESS, ObDASFtsCostUtils::estimate_async_token_doc_cnts(
      search_ctx_, ir, rt, tokens, costs));
  ASSERT_EQ(OB_SUCCESS, tokens.push_back(ObString("alpha")));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObDASFtsCostUtils::estimate_async_token_doc_cnts(
      search_ctx_, ir, rt, tokens, costs));
  EXPECT_TRUE(costs.empty());
}

TEST_F(TestDASAsyncSegmentMerge, search_text_scoring_and_filtering_preserve_composite_key)
{
  ObDASIRScanCtDef ir(allocator_);
  ObDASIRScanRtDef rt;
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  ObDASAsyncTokenOpParam param;
  param.ir_ctdef_ = &ir;
  param.ir_rtdef_ = &rt;
  param.index_table_id_ = TEST_INDEX_TABLE_ID;
  param.token_boost_ = 1.0;
  param.query_token_ = ObString("alpha");
  ObDASAsyncTokenOp scoring(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, scoring.init(param));
  EXPECT_EQ(3, scoring.term_key_.get_datum_cnt());
  param.set_is_scoring(false);
  ObDASAsyncTokenOp token(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, token.init(param));
  EXPECT_EQ(3, token.term_key_.get_datum_cnt());
  EXPECT_EQ(2, token.term_key_.get_datum(0).get_uint64());
  EXPECT_EQ(ir.search_index_fulltext_info_.encoded_path_, token.term_key_.get_datum(1).get_string());
  EXPECT_EQ(ObString(7, "alpha\0\0"), token.query_token_);
  EXPECT_EQ(ObString(7, "alpha\0\0"), token.term_key_.get_datum(2).get_string());
  ObDASAsyncTokenHelperParam hp;
  hp.ir_ctdef_ = &ir;
  hp.ir_rtdef_ = &rt;
  hp.index_table_id_ = TEST_INDEX_TABLE_ID;
  hp.token_boost_ = 1.0;
  hp.query_token_ = ObString("alpha");
  ObDASAsyncTokenHelper helper(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, helper.init(hp));
  EXPECT_EQ(token.term_key_.get_datum(2).get_string(), helper.term_key_.get_datum(2).get_string());
  EXPECT_EQ(OB_SUCCESS, token.close());
}

// TODO: Re-enable after the repeated statistics-cache insertion failure is fixed.
TEST_F(TestDASAsyncSegmentMerge, DISABLED_search_text_match_and_rescan_isolate_field_and_path)
{
  ObDASIRScanCtDef ir(allocator_);
  ObDASIRScanRtDef rt;
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  SearchTextPostingService service(allocator_, search_ctx_);
  ASSERT_EQ(OB_SUCCESS, service.init(ir.search_index_fulltext_info_));
  ObDASAsyncTokenOp alpha(search_ctx_);
  ObDASAsyncTokenOp beta(search_ctx_);
  ObDASAsyncTokenOp *tokens[] = {&alpha, &beta};
  ObArray<ObIDASSearchOp *> children;
  for (int64_t i = 0; i < 2; ++i) {
    ObDASAsyncTokenOpParam param;
    param.ir_ctdef_ = &ir;
    param.ir_rtdef_ = &rt;
    param.index_table_id_ = TEST_INDEX_TABLE_ID;
    param.token_boost_ = 1.0;
    param.query_token_ = ObString(i == 0 ? "alpha" : "beta");
    param.set_is_scoring(false);
    ASSERT_EQ(OB_SUCCESS, tokens[i]->init(param));
    tokens[i]->inv_idx_tablet_id_ = ObTabletID(9001);
    tokens[i]->tsc_service_ = &service;
    ASSERT_EQ(OB_SUCCESS, children.push_back(tokens[i]));
  }
  ObDASConjunctionOp conjunction(search_ctx_);
  ObDASConjunctionOpParam cp(children);
  cp.set_is_scoring(false);
  ASSERT_EQ(OB_SUCCESS, conjunction.init(cp));
  ASSERT_EQ(OB_SUCCESS, conjunction.open());
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = 100;
  const uint64_t expected[] = {1, 4, 5};
  for (int round = 0; round < 2; ++round) {
    if (round > 0) { ASSERT_EQ(OB_SUCCESS, conjunction.rescan()); }
    ASSERT_EQ(OB_SUCCESS, conjunction.rescan_segment(range));
    ObDASRowID id;
    double score = 99.0;
    for (uint64_t expected_id : expected) {
      ASSERT_EQ(OB_SUCCESS, conjunction.next_rowid(id, score));
      EXPECT_EQ(expected_id, id.get_uint64());
      EXPECT_DOUBLE_EQ(0.0, score);
    }
    EXPECT_EQ(OB_ITER_END, conjunction.next_rowid(id, score));
  }
  EXPECT_EQ(4, service.open_count_);
  ASSERT_EQ(OB_SUCCESS, conjunction.close());
  // The same token on another source column must not hit this posting.
  for (int mismatch = 0; mismatch < 2; ++mismatch) {
    ObDASIRScanCtDef other(allocator_);
    ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(
        other, allocator_, mismatch == 0 ? 3 : 2,
        ObString(mismatch == 0 ? "$.title" : "$.body")));
    ASSERT_EQ(OB_SUCCESS, service.seed_field_statistics(other.search_index_fulltext_info_));
    ObDASAsyncTokenOpParam param;
    param.ir_ctdef_ = &other;
    param.ir_rtdef_ = &rt;
    param.index_table_id_ = TEST_INDEX_TABLE_ID;
    param.token_boost_ = 1.0;
    param.query_token_ = ObString("alpha");
    param.set_is_scoring(false);
    ObDASAsyncTokenOp token(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, token.init(param));
    token.inv_idx_tablet_id_ = ObTabletID(9001);
    token.tsc_service_ = &service;
    ASSERT_EQ(OB_SUCCESS, token.open());
    ASSERT_EQ(OB_SUCCESS, token.rescan_segment(range));
    ObDASRowID id;
    double score = 0.0;
    EXPECT_EQ(OB_ITER_END, token.next_rowid(id, score));
    ASSERT_EQ(OB_SUCCESS, token.close());
  }
}

// TODO: Re-enable after scoreless phrase initialization and pruning are fixed.
TEST_F(TestDASAsyncSegmentMerge, DISABLED_search_text_phrase_positions_slop_and_rescan)
{
  for (int64_t slop : {0, 2, 100}) {
    ObDASIRScanCtDef ir(allocator_);
    ObDASIRScanRtDef rt;
    ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
    SearchTextPostingService service(allocator_, search_ctx_);
    ASSERT_EQ(OB_SUCCESS, service.init(ir.search_index_fulltext_info_));
    ObDASMatchPhraseOpParam param;
    param.ir_ctdef_ = &ir;
    param.ir_rtdef_ = &rt;
    param.index_table_id_ = TEST_INDEX_TABLE_ID;
    param.boost_ = 1.0;
    param.slop_ = slop;
    param.set_is_scoring(false);
    ASSERT_EQ(OB_SUCCESS, param.query_tokens_.push_back(ObString("alpha")));
    ASSERT_EQ(OB_SUCCESS, param.query_tokens_.push_back(ObString("beta")));
    ASSERT_EQ(OB_SUCCESS, param.token_ids_.push_back(0));
    ASSERT_EQ(OB_SUCCESS, param.token_ids_.push_back(1));
    ObArray<int64_t> offsets;
    ASSERT_EQ(OB_SUCCESS, offsets.push_back(0));
    ASSERT_EQ(OB_SUCCESS, offsets.push_back(1));
    ASSERT_EQ(OB_SUCCESS, storage::ObPhraseMatchCounter::create(
        allocator_, param.token_ids_, offsets, slop, false, param.counter_));
    ObDASMatchPhraseOp phrase(search_ctx_);
    ASSERT_EQ(OB_SUCCESS, phrase.init(param));
    EXPECT_FALSE(phrase.dynamic_pruning_enabled_);
    for (int64_t i = 0; i < phrase.async_helpers_.count(); ++i) {
      phrase.async_helpers_[i]->set_tablet_id(ObTabletID(9001));
      phrase.async_helpers_[i]->set_tsc_service(&service);
    }
    ASSERT_EQ(OB_SUCCESS, phrase.open());
    ObNewVersionRange range;
    range.base_version_ = 0;
    range.snapshot_version_ = 100;
    for (int round = 0; round < 2; ++round) {
      if (round > 0) { ASSERT_EQ(OB_SUCCESS, phrase.rescan()); }
      ASSERT_EQ(OB_SUCCESS, phrase.rescan_segment(range));
      ObDASRowID id;
      double score = 99.0;
      ASSERT_EQ(OB_SUCCESS, phrase.next_rowid(id, score));
      EXPECT_EQ(1, id.get_uint64());
      EXPECT_DOUBLE_EQ(0.0, score);
      if (slop >= 2) {
        ASSERT_EQ(OB_SUCCESS, phrase.next_rowid(id, score));
        EXPECT_EQ(4, id.get_uint64());
      }
      if (slop >= 100) {
        ASSERT_EQ(OB_SUCCESS, phrase.next_rowid(id, score));
        EXPECT_EQ(5, id.get_uint64());
      }
      EXPECT_EQ(OB_ITER_END, phrase.next_rowid(id, score));
    }
    EXPECT_EQ(4, service.open_count_);
    ASSERT_EQ(OB_SUCCESS, phrase.close());
  }
}

// TODO: Re-enable after Search Text token equality is independent of query collation.
TEST_F(TestDASAsyncSegmentMerge, DISABLED_search_text_bound_parser_properties_match_and_phrase)
{
  plugin::ObPluginMgr plugins;
  ASSERT_EQ(OB_SUCCESS, plugins.init(ObString()));
  ASSERT_EQ(OB_SUCCESS, plugins.load_builtin_plugins());
  struct PluginRestore {
    plugin::ObPluginMgr *previous_;
    ~PluginRestore() { GCTX.plugin_mgr_ = previous_; }
  } restore{GCTX.plugin_mgr_};
  GCTX.plugin_mgr_ = &plugins;
  prepare_const_expr_frame(2048);
  int64_t frame_pos = 0;
  ObExpr query;
  init_string_const_expr(query, frame_pos, ObString("ALPHA alpha beta longtoken"));
  query.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  ObDASIRScanCtDef ir(allocator_);
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  ObDASScalarScanCtDef scalar(allocator_);
  // No parser is present on the shared physical table definition.
  EXPECT_TRUE(scalar.table_param_.get_parser_name().empty());
  ObDASBaseCtDef *ir_children[] = {&scalar};
  ir.children_ = ir_children;
  ir.children_cnt_ = 1;
  ir.use_scalar_scan_def_ = true;
  ObArray<ObString> tokens;
  ObArray<int64_t> counts;
  ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::eval_and_tokenize_query_text(
      allocator_, eval_ctx_, query, scalar, tokens, counts, &ir));
  ASSERT_EQ(2, tokens.count());
  for (int64_t i = 0; i < tokens.count(); ++i) {
    EXPECT_TRUE(tokens[i] == ObString("alpha") || tokens[i] == ObString("beta"));
    EXPECT_EQ(tokens[i] == ObString("alpha") ? 2 : 1, counts[i]);
  }
  ObDASMatchPhraseCtDef phrase(allocator_);
  ObDASBaseCtDef *phrase_children[] = {&ir};
  phrase.children_ = phrase_children;
  phrase.children_cnt_ = 1;
  phrase.ir_ctdef_idx_ = 0;
  phrase.query_text_ = &query;
  ObDASMatchPhraseRtDef runtime;
  runtime.ctdef_ = &phrase;
  runtime.eval_ctx_ = &eval_ctx_;
  ASSERT_EQ(OB_SUCCESS, runtime.prepare_query_tokens());
  ASSERT_EQ(2, runtime.query_tokens_.count());
  EXPECT_EQ(3, runtime.token_ids_.count());
  EXPECT_TRUE(runtime.has_duplicate_tokens_);
  runtime.reset_cached_runtime_state();
  ASSERT_EQ(OB_SUCCESS, runtime.prepare_query_tokens());
  EXPECT_EQ(3, runtime.token_ids_.count());
  // Reuse the process-wide parser plugin to check accent-sensitive token equality.
  {
    prepare_const_expr_frame(2048);
    int64_t frame_pos = 0;
    ObExpr query;
    init_string_const_expr(query, frame_pos, ObString("cafe café"));
    ObDASIRScanCtDef ir(allocator_);
    ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
    ObDASScalarScanCtDef scalar(allocator_);
    ObDASBaseCtDef *ir_children[] = {&scalar};
    ir.children_ = ir_children;
    ir.children_cnt_ = 1;
    ir.use_scalar_scan_def_ = true;
    ObDASMatchPhraseCtDef phrase(allocator_);
    ObDASBaseCtDef *phrase_children[] = {&ir};
    phrase.children_ = phrase_children;
    phrase.children_cnt_ = 1;
    phrase.ir_ctdef_idx_ = 0;
    phrase.query_text_ = &query;
    // Search Text token equality follows the index, regardless of input expression collation.
    for (ObCollationType coll : {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_GENERAL_CI}) {
      SCOPED_TRACE(coll);
      query.obj_meta_.set_collation_type(coll);
      ObArray<ObString> tokens;
      ObArray<int64_t> counts;
      ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::eval_and_tokenize_query_text(
          allocator_, eval_ctx_, query, scalar, tokens, counts, &ir));
      ASSERT_EQ(2, tokens.count());
      ASSERT_EQ(2, counts.count());
      EXPECT_NE(tokens[0], tokens[1]);
      for (int64_t i = 0; i < tokens.count(); ++i) {
        EXPECT_TRUE(tokens[i] == ObString("cafe") || tokens[i] == ObString("café"));
        EXPECT_EQ(1, counts[i]);
      }
      ASSERT_EQ(OB_SUCCESS, ObDASSearchUtils::eval_and_tokenize_query_text(
          allocator_, eval_ctx_, query, scalar, tokens, &ir));
      ASSERT_EQ(2, tokens.count());
      EXPECT_NE(tokens[0], tokens[1]);
      ObDASMatchPhraseRtDef runtime;
      runtime.ctdef_ = &phrase;
      runtime.eval_ctx_ = &eval_ctx_;
      ASSERT_EQ(OB_SUCCESS, runtime.prepare_query_tokens());
      ASSERT_EQ(2, runtime.query_tokens_.count());
      ASSERT_EQ(2, runtime.token_ids_.count());
      EXPECT_NE(runtime.token_ids_[0], runtime.token_ids_[1]);
      EXPECT_FALSE(runtime.has_duplicate_tokens_);
    }
  }
}

TEST_F(TestDASAsyncSegmentMerge, search_text_cost_uses_field_token_statistics)
{
  ObDASIRScanCtDef ir(allocator_);
  ObDASIRScanRtDef rt;
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  ObDASScalarScanCtDef scalar(allocator_);
  ObDASBaseCtDef *children[] = {&scalar};
  ir.children_ = children;
  ir.children_cnt_ = 1;
  const ObTabletID tablet_id(9001);
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_ctdefs_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_tablet_ids_.init(1));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_ctdefs_.push_back(&scalar));
  ASSERT_EQ(OB_SUCCESS, scan_op_.related_tablet_ids_.push_back(tablet_id));
  storage::ObInvIdxParamCache &cache = search_ctx_.get_bm25_param_cache();
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(tablet_id, ObString("alpha"), 99));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(tablet_id, 2,
      ir.search_index_fulltext_info_.encoded_path_, ObString(7, "alpha\0\0"), 4));
  ASSERT_EQ(OB_SUCCESS, cache.set_token_doc_cnt(tablet_id, 2,
      ir.search_index_fulltext_info_.encoded_path_, ObString(6, "beta\0\0"), 3));
  ObArray<ObString> tokens;
  ObArray<int64_t> costs;
  ASSERT_EQ(OB_SUCCESS, tokens.push_back(ObString("alpha")));
  ASSERT_EQ(OB_SUCCESS, tokens.push_back(ObString("beta")));
  ASSERT_EQ(OB_SUCCESS, ObDASFtsCostUtils::estimate_async_token_doc_cnts(
      search_ctx_, ir, rt, tokens, costs));
  ASSERT_EQ(2, costs.count());
  EXPECT_EQ(4, costs[0]);
  EXPECT_EQ(3, costs[1]);
}

// Inject fixed corpus statistics to test DAS score consumption separately from
// storage aggregation. Storage field-length/cache coverage lives in its own tests.
static void seed_search_text_statistics(storage::ObAsyncBM25ParamEstimator &estimator)
{
  estimator.total_doc_cnt_ = 5;
  estimator.avg_doc_token_cnt_ = 2.0;
  estimator.token_doc_cnt_ = 4;
  estimator.estimated_ = true;
}

TEST_F(TestDASAsyncSegmentMerge, search_text_token_scores_and_rescans_with_field_statistics)
{
  ObDASIRScanCtDef ir(allocator_);
  ObDASIRScanRtDef rt;
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  SearchTextPostingService service(allocator_, search_ctx_);
  ASSERT_EQ(OB_SUCCESS, service.init(ir.search_index_fulltext_info_));
  ObDASAsyncTokenOpParam param;
  param.ir_ctdef_ = &ir;
  param.ir_rtdef_ = &rt;
  param.index_table_id_ = TEST_INDEX_TABLE_ID;
  param.token_boost_ = 2.0;
  param.query_token_ = ObString("alpha");
  param.set_is_scoring(true);
  ObDASAsyncTokenOp token(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, token.init(param));
  token.inv_idx_tablet_id_ = ObTabletID(9001);
  token.tsc_service_ = &service;
  ASSERT_EQ(OB_SUCCESS, token.init_bm25_param_estimator());
  EXPECT_TRUE(token.param_estimator_.is_search_text_);
  EXPECT_EQ(2, token.param_estimator_.search_text_column_idx_);
  EXPECT_EQ(ir.search_index_fulltext_info_.encoded_path_, token.param_estimator_.search_text_path_);
  seed_search_text_statistics(token.param_estimator_);
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = 100;
  const uint64_t expected_ids[] = {1, 2, 4, 5};
  const uint64_t tf[] = {2, 1, 1, 1};
  const uint64_t dl[] = {3, 1, 2, 3};
  const double weight = ObExprBM25::query_token_weight(4, 5);
  for (int64_t round = 0; round < 2; ++round) {
    ASSERT_EQ(OB_SUCCESS, token.rescan_segment(range));
    EXPECT_TRUE(token.posting_scan_param_.need_relevance_params_);
    ObDASRowID id;
    double score = 0.0;
    for (int64_t i = 0; i < 4; ++i) {
      ASSERT_EQ(OB_SUCCESS, token.next_rowid(id, score));
      EXPECT_EQ(expected_ids[i], id.get_uint64());
      EXPECT_NEAR(2.0 * weight * ObExprBM25::doc_phrase_weight(tf[i], dl[i] / 2.0), score, 1e-12);
      EXPECT_GT(score, 0.0);
    }
    EXPECT_EQ(OB_ITER_END, token.next_rowid(id, score));
  }
  EXPECT_EQ(OB_SUCCESS, token.close());
}

TEST_F(TestDASAsyncSegmentMerge, search_text_phrase_scores_real_positions)
{
  ObDASIRScanCtDef ir(allocator_);
  ObDASIRScanRtDef rt;
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  SearchTextPostingService service(allocator_, search_ctx_);
  ASSERT_EQ(OB_SUCCESS, service.init(ir.search_index_fulltext_info_));
  ObDASMatchPhraseOpParam param;
  param.ir_ctdef_ = &ir;
  param.ir_rtdef_ = &rt;
  param.index_table_id_ = TEST_INDEX_TABLE_ID;
  param.boost_ = 2.0;
  param.set_is_scoring(true);
  ASSERT_EQ(OB_SUCCESS, param.query_tokens_.push_back(ObString("alpha")));
  ASSERT_EQ(OB_SUCCESS, param.query_tokens_.push_back(ObString("beta")));
  ASSERT_EQ(OB_SUCCESS, param.token_ids_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, param.token_ids_.push_back(1));
  ObArray<int64_t> offsets;
  ASSERT_EQ(OB_SUCCESS, offsets.push_back(0));
  ASSERT_EQ(OB_SUCCESS, offsets.push_back(1));
  ASSERT_EQ(OB_SUCCESS, storage::ObPhraseMatchCounter::create(
      allocator_, param.token_ids_, offsets, 0, false, param.counter_));
  ObDASMatchPhraseOp phrase(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, phrase.init(param));
  phrase.dynamic_pruning_enabled_ = false;
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = 100;
  for (int64_t i = 0; i < phrase.async_helpers_.count(); ++i) {
    ObDASAsyncTokenHelper *helper = phrase.async_helpers_[i];
    helper->set_tablet_id(ObTabletID(9001));
    helper->set_tsc_service(&service);
    ASSERT_EQ(OB_SUCCESS, helper->init_bm25_param_estimator());
    EXPECT_TRUE(helper->param_estimator_.is_search_text_);
    seed_search_text_statistics(helper->param_estimator_);
    ASSERT_EQ(OB_SUCCESS, helper->rescan_segment(range));
  }
  phrase.avg_doc_token_cnt_ = 2.0;
  phrase.total_token_weight_ = -1.0;
  ObDASRowID id;
  double score = 0.0;
  ASSERT_EQ(OB_SUCCESS, phrase.next_rowid(id, score));
  EXPECT_EQ(1, id.get_uint64());
  EXPECT_NEAR(4.0 * ObExprBM25::query_token_weight(4, 5)
      * ObExprBM25::doc_phrase_weight(1, 1.5), score, 1e-12);
  EXPECT_GT(score, 0.0);
  EXPECT_EQ(OB_ITER_END, phrase.next_rowid(id, score));
  EXPECT_EQ(OB_SUCCESS, phrase.close());
}

// TODO: Re-enable after scoreless phrase initialization is fixed.
TEST_F(TestDASAsyncSegmentMerge, DISABLED_search_text_repeated_phrase_token_uses_one_posting)
{
  ObDASIRScanCtDef ir(allocator_);
  ObDASIRScanRtDef rt;
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  SearchTextPostingService service(allocator_, search_ctx_);
  ASSERT_EQ(OB_SUCCESS, service.init(ir.search_index_fulltext_info_));
  ObDASMatchPhraseOpParam param;
  param.ir_ctdef_ = &ir;
  param.ir_rtdef_ = &rt;
  param.index_table_id_ = TEST_INDEX_TABLE_ID;
  param.boost_ = 1.0;
  param.set_is_scoring(false);
  ASSERT_EQ(OB_SUCCESS, param.query_tokens_.push_back(ObString("alpha")));
  ASSERT_EQ(OB_SUCCESS, param.token_ids_.push_back(0));
  ASSERT_EQ(OB_SUCCESS, param.token_ids_.push_back(0));
  ObArray<int64_t> offsets;
  ASSERT_EQ(OB_SUCCESS, offsets.push_back(0));
  ASSERT_EQ(OB_SUCCESS, offsets.push_back(1));
  ASSERT_EQ(OB_SUCCESS, storage::ObPhraseMatchCounter::create(
      allocator_, param.token_ids_, offsets, 0, true, param.counter_));
  ObDASMatchPhraseOp phrase(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, phrase.init(param));
  ASSERT_EQ(1, phrase.async_helpers_.count());
  phrase.async_helpers_[0]->set_tablet_id(ObTabletID(9001));
  phrase.async_helpers_[0]->set_tsc_service(&service);
  ASSERT_EQ(OB_SUCCESS, phrase.open());
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = 100;
  ASSERT_EQ(OB_SUCCESS, phrase.rescan_segment(range));
  ObDASRowID id;
  double score = 99.0;
  ASSERT_EQ(OB_SUCCESS, phrase.next_rowid(id, score));
  EXPECT_EQ(1, id.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  EXPECT_EQ(OB_ITER_END, phrase.next_rowid(id, score));
  EXPECT_EQ(1, service.open_count_);
  EXPECT_EQ(OB_SUCCESS, phrase.close());
}

TEST_F(TestDASAsyncSegmentMerge, search_text_filter_reuses_bm25_and_clears_scores)
{
  ObDASIRScanCtDef ir(allocator_);
  ObDASIRScanRtDef rt;
  ASSERT_EQ(OB_SUCCESS, init_search_text_fixture_info(ir, allocator_));
  SearchTextPostingService service(allocator_, search_ctx_);
  ASSERT_EQ(OB_SUCCESS, service.init(ir.search_index_fulltext_info_));
  ObDASAsyncTokenOpParam param;
  param.ir_ctdef_ = &ir;
  param.ir_rtdef_ = &rt;
  param.index_table_id_ = TEST_INDEX_TABLE_ID;
  param.token_boost_ = 2.0;
  param.query_token_ = ObString("alpha");
  param.set_is_scoring(false);
  ObDASAsyncTokenOp token(search_ctx_);
  ASSERT_EQ(OB_SUCCESS, token.init(param));
  token.inv_idx_tablet_id_ = ObTabletID(9001);
  token.tsc_service_ = &service;
  ASSERT_EQ(OB_SUCCESS, token.open());
  ObNewVersionRange range;
  range.base_version_ = 0;
  range.snapshot_version_ = 100;
  ASSERT_EQ(OB_SUCCESS, token.rescan_segment(range));
  EXPECT_TRUE(token.posting_scan_param_.need_relevance_params_);
  ObDASRowID target;
  target.set_uint64(1);
  ObDASRowID id;
  double score = 99.0;
  ASSERT_EQ(OB_SUCCESS, token.advance_to(target, id, score));
  EXPECT_EQ(1, id.get_uint64());
  EXPECT_DOUBLE_EQ(0.0, score);
  // The leaf computes BM25 as usual; the public filter interface clears its contribution.
  ASSERT_EQ(OB_SUCCESS, token.fetch_curr_on_demand(id, score));
  EXPECT_GT(score, 0.0);
  ASSERT_EQ(OB_SUCCESS, token.load_curr_score(score));
  EXPECT_DOUBLE_EQ(0.0, score);
  ASSERT_EQ(OB_SUCCESS, token.calc_max_score(score));
  EXPECT_DOUBLE_EQ(0.0, score);
  ObDASRowID boundary;
  boundary.set_max();
  uint64_t ids[8];
  double scores[8];
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, token.drain_until(boundary, 8, ids, scores, count, id, score));
  ASSERT_EQ(3, count);
  const uint64_t expected[] = {2, 4, 5};
  for (int64_t i = 0; i < count; ++i) {
    EXPECT_EQ(expected[i], ids[i]);
    EXPECT_DOUBLE_EQ(0.0, scores[i]);
  }
  EXPECT_TRUE(id.is_max());
  EXPECT_EQ(OB_SUCCESS, token.close());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
