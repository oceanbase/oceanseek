/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_iterator.h"
#include "storage/blocksstable/vector_index/ob_disk_bbq_searcher.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_searcher.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

ObIvfSSTable::ObIvfSSTable()
  : ObSSTable(),
    ivf_meta_(),
    fht_rotator_()
{
}

ObIvfSSTable::~ObIvfSSTable()
{
  reset();
}

int ObIvfSSTable::init(
    const ObTabletCreateSSTableParam &param,
    ObArenaAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid()) || OB_ISNULL(allocator) ||
      !param.table_key().is_ivf_sstable() ||
      !param.ivf_meta_.is_valid()) {
    // An ivf sstable must carry a valid ivf_meta. Since a table_key is tagged
    // IVF_INDEX_SSTABLE only when its ivf_meta is valid, an ivf key paired with
    // an invalid meta is an upstream inconsistency, so fail fast.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for ivf sstable", K(ret), K(param), KP(allocator));
  } else if (OB_FAIL(ObSSTable::init(param, allocator))) {
    LOG_WARN("failed to init base sstable", K(ret), K(param));
  } else if (OB_FAIL(set_ivf_meta(*allocator, param.ivf_meta_))) {
    LOG_WARN("failed to set ivf meta from param", K(ret), K(param.ivf_meta_));
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObIvfSSTable::reset()
{
  fht_rotator_.reset();
  ObSSTable::reset();
  ivf_meta_.reset();
}

int ObIvfSSTable::get_row_desc(ObIvfSSTableRowDesc &row_desc) const
{
  int ret = OB_SUCCESS;
  row_desc.reset();
  if (OB_UNLIKELY(!is_valid() || ivf_meta_.is_empty())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable does not have a readable row format", K(ret), KPC(this));
  } else if (OB_FAIL(row_desc.init(ivf_meta_.format_version_))) {
    LOG_WARN("failed to get ivf sstable row descriptor", K(ret), K_(ivf_meta));
  }
  return ret;
}

// scan/get/multi_scan/multi_get 是 SQL/排查用的行访问接口，不是 KNN path。
// 禁止开启 data-block cache：低精度向量微块将以 FastScan (TODO(aozeliu.azl): support ratbitq fastscan block cache) 编码格式进入 cache，
// 不能和行扫描共用普通数据块 cache。
int ObIvfSSTable::scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObDatumRange &key_range,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  row_iter = nullptr;
  if (OB_UNLIKELY(context.query_flag_.is_use_block_cache())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ivf sstable scan interface should not use block cache", K(ret), K(param), K(context));
  } else if (OB_FAIL(ObSSTable::scan(param, context, key_range, row_iter))) {
    LOG_WARN("failed to scan ivf sstable", K(ret), K(param), K(context), K(key_range));
  }
  return ret;
}

int ObIvfSSTable::get(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObDatumRowkey &rowkey,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  row_iter = nullptr;
  if (OB_UNLIKELY(context.query_flag_.is_use_block_cache())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ivf sstable get interface should not use block cache", K(ret), K(param), K(context));
  } else if (OB_FAIL(ObSSTable::get(param, context, rowkey, row_iter))) {
    LOG_WARN("failed to get ivf sstable", K(ret), K(param), K(context), K(rowkey));
  }
  return ret;
}

int ObIvfSSTable::multi_scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObIArray<ObDatumRange> &ranges,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  row_iter = nullptr;
  if (OB_UNLIKELY(context.query_flag_.is_use_block_cache())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ivf sstable multi scan interface should not use block cache", K(ret), K(param), K(context));
  } else if (OB_FAIL(ObSSTable::multi_scan(param, context, ranges, row_iter))) {
    LOG_WARN("failed to multi scan ivf sstable", K(ret), K(param), K(context), K(ranges));
  }
  return ret;
}

int ObIvfSSTable::multi_get(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObIArray<ObDatumRowkey> &rowkeys,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_SUCCESS;
  row_iter = nullptr;
  if (OB_UNLIKELY(context.query_flag_.is_use_block_cache())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ivf sstable multi get interface should not use block cache", K(ret), K(param), K(context));
  } else if (OB_FAIL(ObSSTable::multi_get(param, context, rowkeys, row_iter))) {
    LOG_WARN("failed to multi get ivf sstable", K(ret), K(param), K(context), K(rowkeys));
  }
  return ret;
}

int ObIvfSSTable::create_searcher(
    const ObTableHandleV2 &table_handle,
    const ObTableScanParam &scan_param,
    ObTableAccessContext &access_ctx,
    ObIvfSSTableSearcher *&searcher)
{
  int ret = OB_SUCCESS;
  searcher = nullptr;
  if (OB_UNLIKELY(OB_ISNULL(scan_param.allocator_)
      || !access_ctx.is_valid()
      || scan_param.allocator_ != access_ctx.stmt_allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid knn table scan param", K(ret), K(scan_param));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("ivf sstable is not ready", K(ret), KPC(this));
  } else if (OB_UNLIKELY(access_ctx.tablet_id_ != get_key().get_tablet_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id for ivf sstable searcher", K(ret), K(access_ctx), K(get_key()));
  } else if (OB_UNLIKELY(!table_handle.is_valid() || table_handle.get_table() != this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf sstable handle", K(ret), K(table_handle), KPC(this));
  } else if (OB_UNLIKELY(!ivf_meta_.is_empty()
      && share::VIAT_DISK_BBQ != ivf_meta_.index_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported ivf sstable index type for searcher", K(ret), K_(ivf_meta));
  } else if (OB_ISNULL(searcher = OB_NEWx(
      ObDiskBBQSearcher, scan_param.allocator_, *scan_param.allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate disk bbq sstable searcher", K(ret));
  } else if (OB_FAIL(static_cast<ObDiskBBQSearcher *>(searcher)->open(
      table_handle, scan_param, access_ctx))) {
    LOG_WARN("failed to open disk bbq sstable searcher", K(ret), K(scan_param));
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(searcher)) {
      ObIvfSSTableSearcher::destroy(searcher);
    }
    searcher = nullptr;
  }
  return ret;
}

int ObIvfSSTable::create_centroid_iterator(
    const ObIvfKnnSearchParam &query_param,
    const float *rotated_query,
    ObIAllocator &allocator,
    ObIAllocator &query_allocator,
    const ObTableIterParam &iter_param,
    const ObIvfSSTableRowDesc &row_desc,
    ObTableAccessContext &access_ctx,
    ObIvfCentroidIterator *&iterator,
    const ObIvfQueryVectorContext *query_vector_ctx)
{
  int ret = OB_SUCCESS;
  ObIvfCentroidIterator *tmp_iterator = nullptr;
  iterator = nullptr;
  if (get_ivf_meta().is_hierarchical()) {
    tmp_iterator = OB_NEWx(ObIvfHierarchicalCentroidIterator, &allocator, query_allocator);
  } else {
    tmp_iterator = OB_NEWx(ObIvfFlatCentroidIterator, &allocator, query_allocator);
  }
  if (OB_ISNULL(tmp_iterator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for ivf centroid iterator", K(ret), K(query_param),
        "hierarchical", get_ivf_meta().is_hierarchical());
  } else if (OB_FAIL(tmp_iterator->open(query_param, rotated_query, *this, iter_param, row_desc,
      access_ctx, query_vector_ctx))) {
    LOG_WARN("failed to open ivf centroid iterator", K(ret), K(query_param));
  } else {
    iterator = tmp_iterator;
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(tmp_iterator)) {
      tmp_iterator->~ObIvfCentroidIterator();
      allocator.free(tmp_iterator);
      tmp_iterator = nullptr;
    }
    iterator = nullptr;
  }
  return ret;
}

int ObIvfSSTable::set_ivf_meta(ObArenaAllocator &allocator, const ObIvfSSTableMeta &ivf_meta)
{
  int ret = OB_SUCCESS;
  quantization::ObFhtKacRotator rotator;
  if (OB_UNLIKELY(!ivf_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf meta", K(ret), K(ivf_meta));
  } else if (!ivf_meta.is_empty()
      && OB_FAIL(quantization::ObFhtKacRotator::generate(
          allocator, ivf_meta.dim_, rotator,
          ivf_meta.fht_algo_version_))) {
    LOG_WARN("failed to generate ivf FHT rotator", K(ret), K(ivf_meta));
  } else if (OB_FAIL(ivf_meta_.assign(allocator, ivf_meta))) {
    LOG_WARN("failed to assign ivf meta", K(ret), K(ivf_meta));
  } else {
    fht_rotator_ = rotator;
  }
  return ret;
}

int64_t ObIvfSSTable::get_deep_copy_size() const
{
  int64_t size = sizeof(ObIvfSSTable) + fht_rotator_.signs_size();
  if (is_loaded()) {
    size += meta_->get_deep_copy_size();
  }
  return size;
}

int ObIvfSSTable::deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const
{
  int ret = OB_SUCCESS;
  value = nullptr;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_copy_size));
#if __aarch64__
  } else if (OB_UNLIKELY(0 != (reinterpret_cast<int64_t>(buf) % AARCH64_CP_BUF_ALIGN))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deep copy buffer on aarch64 platform not aligned", K(ret), KP(buf));
#endif
  } else {
    ObIvfSSTable *new_ivf_table = new (buf) ObIvfSSTable();
    int64_t pos = sizeof(ObIvfSSTable);
    new_ivf_table->key_ = key_;
    new_ivf_table->addr_ = addr_;
    new_ivf_table->meta_cache_ = meta_cache_;
    new_ivf_table->is_tmp_sstable_ = false;
    new_ivf_table->valid_for_reading_ = valid_for_reading_;
    if (is_loaded() && OB_FAIL(meta_->deep_copy(buf, buf_len, pos, new_ivf_table->meta_))) {
      LOG_WARN("failed to deep copy sstable meta", K(ret), KP(buf), K(buf_len), K(pos), KPC(meta_));
    } else if (OB_UNLIKELY(buf_len - pos < fht_rotator_.signs_size())) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("deep copy buffer is not enough for FHT flip state", K(ret), K(buf_len), K(pos), K_(ivf_meta));
    } else if (!fht_rotator_.is_inited()) {
      UNUSED(pos);
      new_ivf_table->ivf_meta_ = ivf_meta_;
      value = new_ivf_table;
    } else {
      new_ivf_table->ivf_meta_ = ivf_meta_;
      MEMCPY(buf + pos, fht_rotator_.signs(), fht_rotator_.signs_size());
      if (OB_FAIL(new_ivf_table->fht_rotator_.init(
          reinterpret_cast<const uint8_t *>(buf + pos),
          fht_rotator_.signs_size()))) {
        LOG_WARN("failed to initialize copied FHT rotator", K(ret), K_(ivf_meta));
      } else {
        pos += fht_rotator_.signs_size();
        value = new_ivf_table;
      }
    }
    if (OB_FAIL(ret)) {
      new_ivf_table->~ObIvfSSTable();
    }
  }
  return ret;
}

int64_t ObIvfSSTable::get_serialize_size(const uint64_t data_version) const
{
  int64_t len = ObSSTable::get_serialize_size(data_version);
  if (len > 0) {
    len += ivf_meta_.get_serialize_size();
  }
  return len;
}

int ObIvfSSTable::serialize(
    const uint64_t data_version,
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ivf_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ivf meta for ivf sstable", K(ret), K_(ivf_meta), KPC(this));
  } else if (OB_FAIL(ObSSTable::serialize(data_version, buf, buf_len, pos))) {
    LOG_WARN("failed to serialize base sstable", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(ivf_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize ivf meta", K(ret), KP(buf), K(buf_len), K(pos), K_(ivf_meta));
  }
  return ret;
}

int ObIvfSSTable::deserialize(
    ObArenaAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSTable::deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize base sstable", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(!is_ivf_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized non-ivf sstable", K(ret), KPC(this));
  } else if (OB_FAIL(ivf_meta_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize ivf meta", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(!ivf_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ivf meta after deserialize", K(ret), K_(ivf_meta), KPC(this));
  } else if (!ivf_meta_.is_empty()
      && OB_FAIL(quantization::ObFhtKacRotator::generate(
          allocator, ivf_meta_.dim_, fht_rotator_,
          ivf_meta_.fht_algo_version_))) {
    LOG_WARN("failed to initialize deserialized FHT rotator", K(ret), K_(ivf_meta));
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObIvfSSTable::serialize_full_table(
    const uint64_t data_version,
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ivf_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ivf meta for ivf sstable", K(ret), K_(ivf_meta), KPC(this));
  } else if (OB_FAIL(ObSSTable::serialize_full_table(data_version, buf, buf_len, pos))) {
    LOG_WARN("failed to serialize full base sstable", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(ivf_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize ivf meta", K(ret), KP(buf), K(buf_len), K(pos), K_(ivf_meta));
  }
  return ret;
}

int64_t ObIvfSSTable::get_full_serialize_size(const uint64_t data_version) const
{
  int64_t len = ObSSTable::get_full_serialize_size(data_version);
  if (len > 0) {
    len += ivf_meta_.get_serialize_size();
  }
  return len;
}

} // namespace blocksstable
} // namespace oceanbase
