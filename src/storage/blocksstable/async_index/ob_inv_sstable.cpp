/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_posting_iter.h"
#include "storage/blocksstable/async_index/ob_inv_sstable_range_iter.h"
#include "storage/blocksstable/async_index/ob_sstable_posting_scanner.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;
using namespace storage;

ObInvSSTable::ObInvSSTable()
  : ObSSTable(),
    inv_meta_()
{
}

ObInvSSTable::~ObInvSSTable()
{
  reset();
}

int ObInvSSTable::init(
    const ObTabletCreateSSTableParam &param,
    ObArenaAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid()) || OB_ISNULL(allocator) ||
      !param.table_key().is_inv_sstable() ||
      !param.inv_meta_.is_valid()) {
    // An inv sstable must carry a valid inv_meta. Since a table_key is tagged
    // INV_INDEX_SSTABLE only when its inv_meta is valid, an inv key paired with
    // an invalid meta is an upstream inconsistency, so fail fast. (An empty
    // inverted segment is still expected to produce a valid meta with
    // the empty doc-id range sentinel, not a missing one.)
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for inv sstable", K(ret), K(param), KP(allocator));
  } else if (OB_FAIL(ObSSTable::init(param, allocator))) {
    LOG_WARN("failed to init base sstable", K(ret), K(param));
  } else if (OB_FAIL(set_inv_meta(param.inv_meta_))) {
    LOG_WARN("failed to set inv meta from param", K(ret), K(param.inv_meta_));
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

void ObInvSSTable::reset()
{
  ObSSTable::reset();
  inv_meta_.reset();
}

int ObInvSSTable::get_frozen_schema_version(int64_t &schema_version) const
{
  return ObSSTable::get_frozen_schema_version(schema_version);
}

int ObInvSSTable::set_upper_trans_version(
    ObArenaAllocator &allocator,
    const int64_t upper_trans_version)
{
  int ret = OB_SUCCESS;
  if (is_inc_inv_sstable()) {
    if (OB_FAIL(ObSSTable::set_upper_trans_version(allocator, upper_trans_version))) {
      LOG_WARN("fail to set upper trans version for inc inv sstable", K(ret), KPC(this), K(upper_trans_version));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("set upper trans version is not supported for inv sstable", K(ret), KPC(this));
  }
  return ret;
}

int ObInvSSTable::scan_posting(
    const ObInvPostingScanParam &param,
    const ObITableReadInfo &rowkey_read_info,
    ObInvSSTablePostingIter *&iter)
{
  int ret = OB_SUCCESS;
  iter = nullptr;
  void *buf = nullptr;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid posting scan param", K(ret), K(param));
  } else if (OB_NOT_NULL(param.reusable_iter_)) {
    if (OB_FAIL(param.reusable_iter_->open(param, rowkey_read_info, *this))) {
      LOG_WARN("failed to open caller-owned posting iterator", K(ret));
    } else {
      iter = param.reusable_iter_;
    }
  } else if (OB_ISNULL(buf = param.allocator_->alloc(sizeof(ObInvSSTablePostingIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate posting iterator", K(ret));
  } else {
    ObInvSSTablePostingIter *tmp_iter = new (buf) ObInvSSTablePostingIter();
    if (OB_FAIL(tmp_iter->open(param, rowkey_read_info, *this))) {
      LOG_WARN("failed to open posting iterator", K(ret), K(param));
      tmp_iter->~ObInvSSTablePostingIter();
      param.allocator_->free(buf);
    } else {
      iter = tmp_iter;
    }
  }
  return ret;
}

int ObInvSSTable::scan_range(
    const ObInvRangeScanParam &param,
    const ObITableReadInfo &rowkey_read_info,
    ObInvSSTableRangeIter *&iter)
{
  int ret = OB_SUCCESS;
  iter = nullptr;
  void *buf = nullptr;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid range scan param", K(ret), K(param));
  } else if (OB_ISNULL(buf = param.allocator_->alloc(sizeof(ObInvSSTableRangeIter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate range iterator", K(ret));
  } else {
    ObInvSSTableRangeIter *tmp_iter = new (buf) ObInvSSTableRangeIter();
    if (OB_FAIL(tmp_iter->open(param, rowkey_read_info, *this))) {
      LOG_WARN("failed to open range iterator", K(ret), K(param));
      tmp_iter->~ObInvSSTableRangeIter();
      param.allocator_->free(buf);
    } else {
      iter = tmp_iter;
    }
  }
  return ret;
}

int ObInvSSTable::scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObDatumRange &key_range,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(param, context, key_range);
  row_iter = nullptr;
  LOG_WARN("scan is not supported yet for inv sstable", K(ret), KPC(this));
  return ret;
}

int ObInvSSTable::get(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObDatumRowkey &rowkey,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(param, context, rowkey);
  row_iter = nullptr;
  LOG_WARN("get is not supported for inv sstable", K(ret), KPC(this));
  return ret;
}

int ObInvSSTable::multi_scan(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObIArray<ObDatumRange> &ranges,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(param, context, ranges);
  row_iter = nullptr;
  LOG_WARN("multi_scan is not supported for inv sstable", K(ret), KPC(this));
  return ret;
}

int ObInvSSTable::multi_get(
    const ObTableIterParam &param,
    ObTableAccessContext &context,
    const ObIArray<ObDatumRowkey> &rowkeys,
    ObStoreRowIterator *&row_iter)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSEDx(param, context, rowkeys);
  row_iter = nullptr;
  LOG_WARN("multi_get is not supported for inv sstable", K(ret), KPC(this));
  return ret;
}

int ObInvSSTable::set_inv_meta(const ObInvSSTableMeta &inv_meta)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inv_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inv meta", K(ret), K(inv_meta));
  } else if (OB_FAIL(inv_meta_.assign(inv_meta))) {
    LOG_WARN("failed to assign inverted sstable meta", K(ret));
  }
  return ret;
}

int64_t ObInvSSTable::get_deep_copy_size() const
{
  int64_t size = sizeof(ObInvSSTable);
  if (is_loaded()) {
    size += meta_->get_deep_copy_size();
  }
  return size;
}

int ObInvSSTable::deep_copy(char *buf, const int64_t buf_len, ObIStorageMetaObj *&value) const
{
  int ret = OB_SUCCESS;
  value = nullptr;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < deep_copy_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len), K(deep_copy_size));
  } else {
    ObInvSSTable *new_inv_table = new (buf) ObInvSSTable();
    int64_t pos = sizeof(ObInvSSTable);
    new_inv_table->key_ = key_;
    new_inv_table->addr_ = addr_;
    new_inv_table->meta_cache_ = meta_cache_;
    new_inv_table->is_tmp_sstable_ = false;
    new_inv_table->valid_for_reading_ = valid_for_reading_;
    if (is_loaded() && OB_FAIL(meta_->deep_copy(buf, buf_len, pos, new_inv_table->meta_))) {
      LOG_WARN("failed to deep copy sstable meta", K(ret), KP(buf), K(buf_len), K(pos), KPC(meta_));
    } else if (OB_FAIL(new_inv_table->inv_meta_.assign(inv_meta_))) {
      LOG_WARN("failed to assign inverted meta", K(ret));
    } else {
      value = new_inv_table;
    }
  }
  return ret;
}

int64_t ObInvSSTable::get_serialize_size(const uint64_t data_version) const
{
  int64_t len = ObSSTable::get_serialize_size(data_version);
  if (len > 0) {
    len += inv_meta_.get_serialize_size();
  }
  return len;
}

int ObInvSSTable::serialize(
    const uint64_t data_version,
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inv_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inv meta for inv sstable", K(ret), K_(inv_meta), KPC(this));
  } else if (OB_FAIL(ObSSTable::serialize(data_version, buf, buf_len, pos))) {
    LOG_WARN("failed to serialize base sstable", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(inv_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize inv meta", K(ret), KP(buf), K(buf_len), K(pos), K_(inv_meta));
  }
  return ret;
}

int ObInvSSTable::deserialize(
    ObArenaAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObSSTable::deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize base sstable", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(!is_inv_sstable())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("deserialized non-inv sstable", K(ret), KPC(this));
  } else if (OB_FAIL(inv_meta_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize inv meta", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(!inv_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inv meta after deserialize", K(ret), K_(inv_meta), KPC(this));
  }
  return ret;
}

int ObInvSSTable::serialize_full_table(
    const uint64_t data_version,
    char *buf,
    const int64_t buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inv_meta_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid inv meta for inv sstable", K(ret), K_(inv_meta), KPC(this));
  } else if (OB_FAIL(ObSSTable::serialize_full_table(data_version, buf, buf_len, pos))) {
    LOG_WARN("failed to serialize full base sstable", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(inv_meta_.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize inv meta", K(ret), KP(buf), K(buf_len), K(pos), K_(inv_meta));
  }
  return ret;
}

int64_t ObInvSSTable::get_full_serialize_size(const uint64_t data_version) const
{
  int64_t len = ObSSTable::get_full_serialize_size(data_version);
  if (len > 0) {
    len += inv_meta_.get_serialize_size();
  }
  return len;
}

} // namespace blocksstable
} // namespace oceanbase
