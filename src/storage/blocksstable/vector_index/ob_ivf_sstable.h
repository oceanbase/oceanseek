/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_H_

#include "lib/container/ob_iarray.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"

namespace oceanbase
{
namespace storage
{
class ObITableReadInfo;
class ObTableAccessContext;
class ObTableScanParam;
}
namespace blocksstable
{

class ObIvfSSTableSearcher;
class ObIvfCentroidIterator;
class ObIvfQueryVectorContext;
struct ObIvfKnnSearchParam;

// IVF vector index SSTable.
class ObIvfSSTable : public ObSSTable
{
public:
  ObIvfSSTable();
  virtual ~ObIvfSSTable();
  virtual int init(
      const ObTabletCreateSSTableParam &param,
      common::ObArenaAllocator *allocator) override;
  void reset();

  virtual int64_t get_snapshot_version() const override
  {
    return is_inc_ivf_sstable()
        ? ObSSTable::get_max_merged_trans_version()
        : get_key().get_snapshot_version();
  }
  virtual int64_t get_max_merged_trans_version() const override
  {
    return is_inc_ivf_sstable()
        ? ObSSTable::get_max_merged_trans_version()
        : get_key().get_snapshot_version();
  }
  virtual int64_t get_min_merged_trans_version() const override
  {
    return is_inc_ivf_sstable()
        ? ObSSTable::get_min_merged_trans_version()
        : get_key().version_range_.base_version_;
  }

  virtual int scan(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const ObDatumRange &key_range,
      ObStoreRowIterator *&row_iter) override;
  virtual int get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const ObDatumRowkey &rowkey,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_scan(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObIArray<ObDatumRange> &ranges,
      ObStoreRowIterator *&row_iter) override;
  virtual int multi_get(
      const storage::ObTableIterParam &param,
      storage::ObTableAccessContext &context,
      const common::ObIArray<ObDatumRowkey> &rowkeys,
      ObStoreRowIterator *&row_iter) override;

  int create_searcher(
      const storage::ObTableHandleV2 &table_handle,
      const storage::ObTableScanParam &scan_param,
      storage::ObTableAccessContext &access_ctx,
      ObIvfSSTableSearcher *&searcher);
  int create_centroid_iterator(
      const ObIvfKnnSearchParam &query_param,
      const float *rotated_query,
      common::ObIAllocator &allocator,
      common::ObIAllocator &query_allocator,
      const storage::ObTableIterParam &iter_param,
      const ObIvfSSTableRowDesc &row_desc,
      storage::ObTableAccessContext &access_ctx,
      ObIvfCentroidIterator *&iterator,
      const ObIvfQueryVectorContext *query_vector_ctx = nullptr);

  const ObIvfSSTableMeta &get_ivf_meta() const { return ivf_meta_; }
  int get_row_desc(ObIvfSSTableRowDesc &row_desc) const;
  const quantization::ObFhtKacRotator &get_fht_rotator() const { return fht_rotator_; }
  int set_ivf_meta(common::ObArenaAllocator &allocator, const ObIvfSSTableMeta &ivf_meta);

  virtual int64_t get_deep_copy_size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const override;
  virtual int64_t get_serialize_size(const uint64_t data_version) const override;
  virtual int serialize(
      const uint64_t data_version,
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;
  virtual int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos) override;
  virtual int serialize_full_table(
      const uint64_t data_version,
      char *buf,
      const int64_t buf_len,
      int64_t &pos) const override;
  virtual int64_t get_full_serialize_size(const uint64_t data_version) const override;

  INHERIT_TO_STRING_KV("ObSSTable", ObSSTable, "type", "ivf_sstable", K_(ivf_meta));
protected:
  ObIvfSSTableMeta ivf_meta_;
  quantization::ObFhtKacRotator fht_rotator_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_H_
