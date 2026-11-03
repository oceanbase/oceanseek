/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/mds_table/ob_tablet_mds_reloc_batch_arg.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace storage
{

// ======================== ObTabletMdsRelocSkipBitmap ========================

void ObTabletMdsRelocSkipBitmap::reset()
{
  row_count_ = 0;
  target_count_ = 0;
  words_data_ptr_ = nullptr;
  words_.reset();
}

int ObTabletMdsRelocSkipBitmap::init(const int64_t row_count, const int64_t target_count)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(row_count <= 0 || target_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap dimensions", KR(ret), K(row_count), K(target_count));
  } else if (OB_UNLIKELY(row_count > (INT64_MAX - 63) / target_count)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("bitmap dimensions overflow", KR(ret), K(row_count), K(target_count));
  } else {
    const int64_t bit_count = row_count * target_count;
    const int64_t total = (bit_count + 63) / 64;
    if (OB_FAIL(words_.reserve(total))) {
      LOG_WARN("reserve bitmap words failed", KR(ret), K(total));
    } else {
      row_count_ = row_count;
      target_count_ = target_count;
      for (int64_t i = 0; i < total; ++i) {
        words_.push_back(0);
      }
    }
  }
  return ret;
}

void ObTabletMdsRelocSkipBitmap::set_skip(const int64_t row_idx, const int64_t tgt_idx)
{
  if (OB_LIKELY(row_idx >= 0 && row_idx < row_count_
                && tgt_idx >= 0 && tgt_idx < target_count_)) {
    const int64_t bit_idx = row_idx * target_count_ + tgt_idx;
    words_.at(bit_idx / 64) |= (1ULL << (bit_idx % 64));
  }
}

bool ObTabletMdsRelocSkipBitmap::is_skipped(const int64_t row_idx, const int64_t tgt_idx) const
{
  if (OB_UNLIKELY(!is_inited()
                  || row_idx < 0 || row_idx >= row_count_
                  || tgt_idx < 0 || tgt_idx >= target_count_)) {
    return false;
  }
  const int64_t bit_idx = row_idx * target_count_ + tgt_idx;
  return (words_.at(bit_idx / 64) & (1ULL << (bit_idx % 64))) != 0;
}

bool ObTabletMdsRelocSkipBitmap::has_any_skip() const
{
  for (int64_t i = 0; i < words_.count(); ++i) {
    if (words_.at(i) != 0) {
      return true;
    }
  }
  return false;
}

// Only words_ is serialized; row_count/target_count are inferred from
// BatchArg's rows_.count()/targets_.count() during deserialize.
// All elements use fixed-length encode_i64 (8 bytes) so that serialize,
// rewrite_inplace and get_serialize_size are always consistent.

int ObTabletMdsRelocSkipBitmap::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t cnt = words_.count();
  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, cnt))) {
    LOG_WARN("encode bitmap count failed", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, words_.at(i)))) {
      LOG_WARN("encode bitmap word failed", KR(ret), K(i));
    }
  }
  return ret;
}

int ObTabletMdsRelocSkipBitmap::deserialize(const char *buf, const int64_t data_len, int64_t &pos,
                                   const int64_t row_count, const int64_t target_count)
{
  int ret = OB_SUCCESS;
  reset();
  int64_t cnt = 0;
  if (OB_UNLIKELY(row_count < 0 || target_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bitmap dimensions", KR(ret), K(row_count), K(target_count));
  } else if (OB_UNLIKELY(row_count > (INT64_MAX - 63) / target_count)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("bitmap dimensions overflow", KR(ret), K(row_count), K(target_count));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &cnt))) {
    LOG_WARN("decode bitmap count failed", KR(ret));
  } else {
    const int64_t bit_count = row_count * target_count;
    const int64_t expected_cnt = (bit_count + 63) / 64;
    if (OB_UNLIKELY(cnt != expected_cnt)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("unexpected bitmap word count", KR(ret), K(cnt), K(expected_cnt), K(row_count), K(target_count));
    } else {
      words_data_ptr_ = cnt > 0 ? const_cast<char *>(buf) + pos : nullptr;
      if (OB_FAIL(words_.reserve(cnt))) {
        LOG_WARN("reserve bitmap words failed", KR(ret), K(cnt));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
        int64_t val = 0;
        if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &val))) {
          LOG_WARN("decode bitmap word failed", KR(ret), K(i));
        } else {
          words_.push_back(val);
        }
      }
      if (OB_SUCC(ret)) {
        row_count_ = row_count;
        target_count_ = target_count;
      }
    }
  }
  return ret;
}

int64_t ObTabletMdsRelocSkipBitmap::get_serialize_size() const
{
  // count (8 bytes) + each word (8 bytes)
  return static_cast<int64_t>(sizeof(int64_t)) * (1 + words_.count());
}

int ObTabletMdsRelocSkipBitmap::rewrite_inplace() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(words_data_ptr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("words_data_ptr not set, cannot rewrite bitmap inplace", KR(ret));
  } else {
    const int64_t buf_len = words_.count() * static_cast<int64_t>(sizeof(int64_t));
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < words_.count(); ++i) {
      if (OB_FAIL(serialization::encode_i64(words_data_ptr_, buf_len, pos, words_.at(i)))) {
        LOG_WARN("rewrite bitmap word failed", KR(ret), K(i));
      }
    }
  }
  return ret;
}

// ======================== ObTabletMdsRelocBatchArg ========================

ObTabletMdsRelocBatchArg::ObTabletMdsRelocBatchArg()
{
  reset();
}

void ObTabletMdsRelocBatchArg::reset()
{
  version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION;
  reloc_type_ = ObTabletMdsRelocType::INVALID;
  ls_id_.reset();
  src_ls_id_.reset();
  read_snapshot_.reset();
  targets_.reset();
  rows_.reset();
  skip_bitmap_.reset();
  is_final_ = false;
  fixed_ser_size_ = 0;
  rows_ser_size_ = 0;
}

void ObTabletMdsRelocBatchArg::reset_rows()
{
  rows_.reset();
  skip_bitmap_.reset();
  is_final_ = false;
  rows_ser_size_ = 0;
}

void ObTabletMdsRelocBatchArg::init_fixed_serialize_size()
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, version_, reloc_type_, ls_id_, targets_);
  if (is_v2()) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, src_ls_id_, read_snapshot_);
  }
  len += serialization::encoded_length_vi64(UNIS_VERSION)
      + serialization::OB_SERIALIZE_SIZE_NEED_BYTES
      + serialization::encoded_length_bool(is_final_);
  fixed_ser_size_ = len;
}

int ObTabletMdsRelocBatchArg::push_row(const ObTabletMdsRelocRowStorage &row)
{
  int ret = rows_.push_back(row);
  if (OB_SUCC(ret)) {
    rows_ser_size_ += row.get_serialize_size();
  }
  return ret;
}

void ObTabletMdsRelocBatchArg::pop_back_row()
{
  if (rows_.count() > 0) {
    rows_ser_size_ -= rows_.at(rows_.count() - 1).get_serialize_size();
    rows_.pop_back();
  }
}

int64_t ObTabletMdsRelocBatchArg::estimate_serialize_size_after_push_row(
    const int64_t incoming_row_serialize_size)
{
  // Always recompute: the fixed part depends on targets_, and a cached value
  // would silently go stale if targets_ were ever modified after the first
  // estimate.  The recomputation only walks the small target array.
  init_fixed_serialize_size();
  const int64_t row_count = rows_.count() + 1;
  const int64_t rows_serialize_size = rows_ser_size_ + incoming_row_serialize_size;
  const int64_t bit_count = row_count * targets_.count();
  const int64_t word_count = (bit_count + 63) / 64;
  return fixed_ser_size_
      + serialization::encoded_length_vi64(row_count)
      + rows_serialize_size
      + static_cast<int64_t>(sizeof(int64_t)) * (1 + word_count);
}

bool ObTabletMdsRelocBatchArg::is_valid() const
{
  bool is_valid = ls_id_.is_valid()
      && (LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION == version_
          || LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2 == version_)
      && is_valid_reloc_type(reloc_type_)
      && targets_.count() > 0
      && (rows_.count() > 0 || is_final_ || is_marker_lock_batch())
      && rows_.count() <= rows_.get_capacity();
  for (int64_t i = 0; is_valid && i < targets_.count(); ++i) {
    is_valid = targets_[i].tablet_id_.is_valid() && targets_[i].transfer_scn_.is_valid();
  }
  // V2 pulls op-type rows on replay from the source LS replica at read_snapshot_,
  // so both must be present.
  if (is_valid && is_v2()) {
    is_valid = src_ls_id_.is_valid() && read_snapshot_.is_valid();
    for (int64_t i = 0; is_valid && i < targets_.count(); ++i) {
      is_valid = targets_[i].src_tablet_id_.is_valid()
        && targets_[i].src_reorg_scn_.is_valid();
    }
  }
  // A V2 marker-lock batch is a protocol control payload: targets_ is already
  // required non-empty above, rows_ must be empty and no skip bit may be set.
  // A malformed or partially reused control payload is rejected here instead of
  // reaching the marker mutex path in apply_rows_.
  if (is_valid && is_marker_lock_batch()) {
    is_valid = !skip_bitmap_.has_any_skip();
  }
  return is_valid;
}

// src_tablet_id_ / src_reorg_scn_ are UNIS-appended tail fields kept at
// UNIS_VERSION(1): OB_UNIS_DECODE skips a field when pos == data_len (the
// object length header), so new readers of legacy V1 payload simply leave
// these fields at their default (invalid) value instead of failing, while old
// readers skip the trailing bytes via the same length header.  The legacy
// round-trip is pinned by unit tests in unittest/storage/mds_table/test_mds_table.cpp.
OB_SERIALIZE_MEMBER(ObTabletMdsRelocTarget, tablet_id_, transfer_scn_, src_tablet_id_, src_reorg_scn_);
OB_SERIALIZE_MEMBER(ObMdsValueMeta, writer_trans_id_, commit_version_, commit_scn_);
OB_SERIALIZE_MEMBER(ObTabletMdsRelocRowStorage, mds_unit_id_, meta_, ser_payload_);

// BatchArg serialization first encodes version_, reloc_type_, and ls_id_.  V2
// then conditionally appends src_ls_id_ and read_snapshot_, followed by
// targets_ and rows_.  skip_bitmap_ uses its own fixed-length encoding, with
// is_final_ at the tail.  Keeping is_final_ after the bitmap preserves the
// offsets used by ObTabletMdsRelocSkipBitmap::rewrite_inplace().

OB_DEF_SERIALIZE(ObTabletMdsRelocBatchArg)
{
  int ret = OB_SUCCESS;
  // version_ is encoded first, so the deserializer knows whether src_ls_id_
  // follows before it reaches it. V1 omits the batch source fields; target
  // tail fields remain backward compatible through the UNIS length header.
  LST_DO_CODE(OB_UNIS_ENCODE, version_, reloc_type_, ls_id_);
  if (OB_SUCC(ret) && is_v2()) {
    LST_DO_CODE(OB_UNIS_ENCODE, src_ls_id_, read_snapshot_);
  }
  LST_DO_CODE(OB_UNIS_ENCODE, targets_, rows_);
  if (OB_SUCC(ret) && OB_FAIL(skip_bitmap_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize skip bitmap failed", KR(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(serialization::encode_bool(buf, buf_len, pos, is_final_))) {
    LOG_WARN("serialize final flag failed", KR(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObTabletMdsRelocBatchArg)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(version_);
  if (OB_SUCC(ret)
      && LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION != version_
      && LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION_V2 != version_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported reloc batch version", KR(ret), K(version_));
  }
  LST_DO_CODE(OB_UNIS_DECODE, reloc_type_, ls_id_);
  if (OB_SUCC(ret) && is_v2()) {
    LST_DO_CODE(OB_UNIS_DECODE, src_ls_id_, read_snapshot_);
  }
  LST_DO_CODE(OB_UNIS_DECODE, targets_, rows_);
  skip_bitmap_.reset();
  if (OB_SUCC(ret)) {
    if (OB_FAIL(skip_bitmap_.deserialize(buf, data_len, pos, rows_.count(), targets_.count()))) {
      LOG_WARN("deserialize skip bitmap failed", KR(ret));
    } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &is_final_))) {
      LOG_WARN("deserialize final flag failed", KR(ret));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObTabletMdsRelocBatchArg)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, version_, reloc_type_, ls_id_);
  if (is_v2()) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, src_ls_id_, read_snapshot_);
  }
  LST_DO_CODE(OB_UNIS_ADD_LEN, targets_, rows_);
  len += skip_bitmap_.get_serialize_size();
  len += serialization::encoded_length_bool(is_final_);
  return len;
}

bool ObTabletMdsRelocBatchArg::is_marker_lock_batch() const
{
  return is_v2() && 0 == rows_.count() && !is_final_;
}

} // namespace storage
} // namespace oceanbase
