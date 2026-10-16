/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_INDEX_DELETION_OP_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_INDEX_DELETION_OP_H_

#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/search/ob_das_scalar_define.h"
#include "storage/async_index/ob_deletion_bitmap_provider.h"

namespace oceanbase
{
namespace storage
{
class ObAccessService;
}
namespace sql
{

class ObDASAsyncIndexDeletionOpParam : public ObIDASSearchOpParam
{
public:
  ObDASAsyncIndexDeletionOpParam(
      const ObDASScalarScanCtDef *scan_ctdef,
      ObDASScalarScanRtDef *scan_rtdef)
      : ObIDASSearchOpParam(DAS_SEARCH_OP_DELETION),
        scan_ctdef_(scan_ctdef),
        scan_rtdef_(scan_rtdef) {}
  ~ObDASAsyncIndexDeletionOpParam() {}

  const ObDASScalarScanCtDef *get_scan_ctdef() const { return scan_ctdef_; }
  ObDASScalarScanRtDef *get_scan_rtdef() const { return scan_rtdef_; }
  int get_children_ops(ObIArray<ObIDASSearchOp *> &children) const override
  {
    return OB_SUCCESS;
  }

private:
  const ObDASScalarScanCtDef *scan_ctdef_;
  ObDASScalarScanRtDef *scan_rtdef_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncIndexDeletionOpParam);
};

class ObDASAsyncIndexDeletionOp : public ObIDASSearchOp
{
public:
  ObDASAsyncIndexDeletionOp(ObDASSearchCtx &search_ctx)
      : ObIDASSearchOp(search_ctx),
        scan_param_(),
        scalar_ctdef_(nullptr),
        scalar_rtdef_(nullptr),
        tablet_id_(),
        bitmap_handle_(),
        exhausted_(false),
        is_empty_(true)
  {
    op_type_ = DAS_SEARCH_OP_DELETION;
  }
  OB_INLINE bool contains_doc_id(const uint64_t doc_id) const
  {
    return bitmap_handle_.contains(static_cast<int64_t>(doc_id));
  }
  OB_INLINE bool contains_any_doc_id(
      const uint64_t min_doc_id,
      const uint64_t max_doc_id) const
  {
    return bitmap_handle_.contains_any_in_range(
        static_cast<int64_t>(min_doc_id), static_cast<int64_t>(max_doc_id));
  }

private:
  int do_init(const ObIDASSearchOpParam &op_param) override;
  int do_open() override;
  int do_close() override;
  int do_rescan() override;
  int do_advance_to(const ObDASRowID &target, ObDASRowID &curr_id, double &score) override;
  int do_next_rowid(ObDASRowID &next_id, double &score) override;

private:
  int load_bitmap();

private:
  storage::ObTableScanParam scan_param_;
  const ObDASScalarScanCtDef *scalar_ctdef_;
  ObDASScalarScanRtDef *scalar_rtdef_;
  common::ObTabletID tablet_id_;
  storage::ObDeletionBitmapHandle bitmap_handle_;
  bool exhausted_;
  bool is_empty_;
  DISALLOW_COPY_AND_ASSIGN(ObDASAsyncIndexDeletionOp);
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_ASYNC_INDEX_DELETION_OP_H_
