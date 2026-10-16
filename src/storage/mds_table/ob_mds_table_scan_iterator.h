/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_SCAN_ITERATOR_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_SCAN_ITERATOR_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "common/row/ob_row_iterator.h"
#include "storage/access/ob_multiple_merge.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_scan_range.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/mds_table/ob_mds_table_datum_row_converter.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"

namespace oceanbase
{
namespace storage
{
class ObTableScanParam;
class ObTabletHandle;
class ObITabletMdsInterface;

class ObMdsTableScanIterator : public common::ObNewRowIterator
{
public:
  ObMdsTableScanIterator();
  virtual ~ObMdsTableScanIterator();
  ObMdsTableScanIterator(const ObMdsTableScanIterator &) = delete;
  ObMdsTableScanIterator &operator=(const ObMdsTableScanIterator &) = delete;
public:
  /// Rowkey projection from `ObMdsTabletSchemaHelper` (schema is fixed for LS MDS tablet).
  int init(
      ObTableScanParam &scan_param,
      const ObTabletHandle &tablet_handle,
      ObStoreCtx &store_ctx);
  virtual void reset() override;
public:
  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_row() override;
  virtual int get_next_rows(int64_t &count, int64_t capacity) override;
  int get_next_row(blocksstable::ObDatumRow *&row);

  /// Deserialize next LS aggregate MDS row into typed @p key / @p value.
  template <typename K, typename V>
  int get_next_mds_kv(common::ObIAllocator &user_allocator, K &key, V &value, ObMdsValueMeta &meta)
  {
    int ret = common::OB_SUCCESS;
    blocksstable::ObDatumRow *row = nullptr;
    meta.reset();
    if (OB_FAIL(get_next_row(row))) {
      if (common::OB_ITER_END != ret) {
        MDS_LOG(WARN, "fail to get next datum row", K(ret));
      }
    } else if (OB_ISNULL(row)) {
      ret = common::OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "row is null", K(ret), KP(row), KPC(this));
    } else if (OB_FAIL(ObMdsTableDatumRowConverter::convert_row_to_kv(&user_allocator, *row, key, value, meta))) {
      MDS_LOG(WARN, "fail to convert datum row to typed mds kv", K(ret));
    }
    return ret;
  }

private:
  int init_get_table_param(
      const ObTableScanParam &scan_param,
      const ObTabletHandle &tablet_handle);
  int init_and_open_iter(ObTableScanParam &scan_param);
  int init_and_open_single_get_merge(ObTableScanParam &scan_param);
  int init_and_open_multiple_get_merge(ObTableScanParam &scan_param);
  int init_and_open_multiple_scan_merge(ObTableScanParam &scan_param);
  template <typename MergeT, typename OpenArgT>
  int alloc_init_open_merge_(ObTableScanParam &scan_param, const OpenArgT &open_arg);
public:
  TO_STRING_KV(K_(is_inited),
               K_(access_param),
               K_(access_ctx),
               K_(get_table_param),
               K_(table_scan_range),
               K_(table_scan_param),
               KP_(multiple_merge));
private:
  bool is_inited_;
  ObTableAccessParam access_param_;
  ObTableAccessContext access_ctx_;
  ObGetTableParam get_table_param_;
  ObTableScanRange table_scan_range_;
  ObTableScanParam *table_scan_param_;
  ObMultipleMerge *multiple_merge_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_SCAN_ITERATOR_H_
