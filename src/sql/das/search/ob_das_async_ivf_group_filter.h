/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_SQL_DAS_ASYNC_IVF_GROUP_FILTER_H_
#define OCEANBASE_SQL_DAS_ASYNC_IVF_GROUP_FILTER_H_
#include "lib/alloc/alloc_struct.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_das_async_index_query_ctx.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"

namespace oceanbase
{
namespace sql
{
// PRE-only execution delegate. POST continues to use the partition candidate
// validation path; synchronous/extracted filters are never duplicated into groups.
class ObDASAsyncIVFGroupFilter
{
public:
  explicit ObDASAsyncIVFGroupFilter(common::ObIAllocator &alloc) : alloc_(alloc),
      filters_(OB_MALLOC_NORMAL_BLOCK_SIZE, common::ModulePageAllocator(alloc)), count_(0) {}
  ~ObDASAsyncIVFGroupFilter() { reset(); }
  int prepare(ObIDASSearchRtDef &rtdef, ObDASSearchCtx &ctx)
  {
    int ret = OB_SUCCESS;
    const ObAsyncIndexPartitionSegCtx *partition = ctx.get_current_async_partition();
    if (!filters_.empty()) {
      ret = OB_INIT_TWICE;
      LOG_WARN("IVF group filters are already prepared", K(ret), K(filters_.count()));
    } else if (nullptr == partition || !ctx.is_uint64_rowid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid IVF group filter partition or RowID type", K(ret), KP(partition));
    }
    for (int64_t g = 0; OB_SUCC(ret) && g < partition->get_storage_group_ctx().get_group_count(); ++g) {
      ObDASAsyncBindingGuard guard(ctx);
      guard.set(OB_INVALID_ID, g, false);
      ObIDASSearchOp *op = nullptr;
      blocksstable::ObIvfPreFilterBitmap *bitmap = nullptr;
      blocksstable::ObIvfPreFilter *filter = nullptr;
      ObDASSearchCost cost;
      rtdef.reset_cost_recursive();
      if (OB_FAIL(rtdef.get_cost(ctx, cost))) {
        LOG_WARN("failed to estimate group PRE filter", K(ret), K(g));
      }
      void *buf = nullptr;
      if (OB_SUCC(ret)) {
        buf = alloc_.alloc(sizeof(blocksstable::ObIvfPreFilterBitmap));
        if (nullptr == buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate IVF group bitmap", K(ret), K(g));
        } else {
          bitmap = new (buf) blocksstable::ObIvfPreFilterBitmap(alloc_, cost.is_valid() ? cost.cost() : -1);
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(rtdef.generate_op(cost, ctx, op))) {
        LOG_WARN("failed to build group PRE filter", K(ret), K(g));
      } else if (OB_SUCC(ret) && nullptr == op) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("missing generated IVF group filter operator", K(ret), K(g));
      } else if (OB_SUCC(ret) && OB_FAIL(op->open())) {
        LOG_WARN("failed to open group PRE filter", K(ret), K(g));
      }
      uint64_t vids[256];
      int64_t vid_count = 0;
      while (OB_SUCC(ret)) {
        ObDASRowID rowid;
        ObDatum datum;
        double score = 0;
        if (OB_FAIL(op->next_rowid(rowid, score))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to read IVF group filter RowID", K(ret), K(g));
          }
        } else if (OB_FAIL(ctx.get_datum_from_rowid(rowid, datum))) {
          LOG_WARN("failed to read group filter VID", K(ret));
        } else if (datum.is_null() || datum.get_uint64() > static_cast<uint64_t>(INT64_MAX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid IVF group filter VID", K(ret), K(g), K(datum));
        } else {
          vids[vid_count++] = datum.get_uint64();
          if (256 == vid_count) {
            if (OB_FAIL(bitmap->value_add_many(vids, vid_count))) {
              LOG_WARN("failed to append group filter VIDs", K(ret));
            }
            vid_count = 0;
          }
        }
      }
      if (OB_ITER_END == ret) { ret = OB_SUCCESS; }
      if (OB_SUCC(ret) && vid_count > 0 && OB_FAIL(bitmap->value_add_many(vids, vid_count))) {
        LOG_WARN("failed to append final group filter VIDs", K(ret));
      }
      if (OB_SUCC(ret) && OB_FAIL(bitmap->finish())) {
        LOG_WARN("failed to finish IVF group bitmap", K(ret), K(g));
      }
      if (nullptr != op) {
        const int close_ret = op->close();
        if (OB_SUCCESS != close_ret) {
          LOG_WARN("failed to close IVF group filter", K(ret), K(close_ret), K(g));
        }
        if (OB_SUCC(ret)) { ret = close_ret; }
      }
      if (OB_SUCC(ret)) {
        buf = alloc_.alloc(sizeof(blocksstable::ObIvfPreFilter));
        if (nullptr == buf) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate IVF group VID filter", K(ret), K(g));
        }
        else {
          filter = new (buf) blocksstable::ObIvfPreFilter();
          if (OB_FAIL(filter->init(*bitmap))) {
            LOG_WARN("failed to bind IVF group VID filter", K(ret), K(g));
          } else if (OB_FAIL(filters_.push_back(filter))) {
            LOG_WARN("failed to retain IVF group VID filter", K(ret), K(g));
          } else {
            count_ += bitmap->get_cardinality();
            bitmap = nullptr;
            filter = nullptr;
          }
        }
      }
      // Do not carry this Group's cached cost into the next binding.
      rtdef.reset_cost_recursive();
      if (nullptr != filter) { filter->~ObIvfPreFilter(); alloc_.free(filter); }
      if (nullptr != bitmap) {
        bitmap->~ObIvfPreFilterBitmap(); alloc_.free(bitmap);
      }
    }
    if (OB_FAIL(ret)) { reset(); }
    return ret;
  }
  const common::ObIArray<const blocksstable::ObIvfPreFilter *> &filters() const { return filters_; }
  uint64_t count() const { return count_; }
  void reset()
  {
    for (int64_t i = 0; i < filters_.count(); ++i) {
      auto *filter = const_cast<blocksstable::ObIvfPreFilter *>(filters_.at(i));
      // This delegate owns the allocation; consumers only borrow its const view.
      auto *bitmap = const_cast<blocksstable::ObIvfPreFilterBitmap *>(filter->get_adaptive_vids());
      filter->reset();
      bitmap->~ObIvfPreFilterBitmap();
      alloc_.free(bitmap);
      filter->~ObIvfPreFilter();
      alloc_.free(filter);
    }
    filters_.reset();
    count_ = 0;
  }
private:
  common::ObIAllocator &alloc_;
  common::ObSEArray<const blocksstable::ObIvfPreFilter *, 4> filters_;
  uint64_t count_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncIVFGroupFilter);
};
} // namespace sql
} // namespace oceanbase
#endif
