/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_STORAGE_FTS_OB_DELETION_ROW_ITER_H_
#define OB_STORAGE_FTS_OB_DELETION_ROW_ITER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/schema/ob_table_param.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
namespace common
{
class ObNewRowIterator;
}
namespace storage
{
class ObDeletionRowIter
{
public:
  ObDeletionRowIter();
  ~ObDeletionRowIter();

  int init(
      const share::ObLSID &ls_id,
      const common::ObTabletID &delete_log_tablet_id,
      const uint64_t table_id,
      const int64_t async_snapshot,
      common::ObIAllocator &allocator,
      const int64_t base_snapshot = 0);
  // The caller must first observe deletion publication covering its logical read version.
  // Acquire the current Tablet after that observation to derive physical MVCC visibility.
  static int prepare_read_snapshot(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const int64_t expire_ts,
      transaction::ObTxReadSnapshot &snapshot,
      const int64_t minimum_snapshot = 0);
  void reset();

  int64_t get_current_doc_id() const { return current_doc_id_; }
  int next();
  int advance_to(const int64_t target_doc_id);
  bool is_end() const { return is_end_; }
  bool is_empty() const { return is_empty_; }

private:
  int build_table_param(const uint64_t table_id);
  int build_scan_param(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const uint64_t table_id,
      common::ObIAllocator &allocator);
  int do_table_scan();
  int revert_scan_iter();

private:
  bool is_inited_;
  bool is_end_;
  bool is_empty_;
  int64_t current_doc_id_;
  int64_t async_snapshot_;
  int64_t base_snapshot_;
  int64_t trans_col_idx_;
  common::ObArenaAllocator inner_allocator_;
  share::schema::ObTableParam table_param_;
  ObTableScanParam scan_param_;
  common::ObNewRowIterator *row_iter_;

  DISALLOW_COPY_AND_ASSIGN(ObDeletionRowIter);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_FTS_OB_DELETION_ROW_ITER_H_
