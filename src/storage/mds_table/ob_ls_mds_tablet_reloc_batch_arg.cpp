/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/mds_table/ob_ls_mds_tablet_reloc_batch_arg.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace storage
{

// ======================== ObRelocSkipBitmap ========================

void ObRelocSkipBitmap::reset()
{
  row_count_ = 0;
  target_count_ = 0;
  words_data_ptr_ = nullptr;
  words_.reset();
}

int ObRelocSkipBitmap::init(const int64_t row_count, const int64_t target_count)
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

void ObRelocSkipBitmap::set_skip(const int64_t row_idx, const int64_t tgt_idx)
{
  if (OB_LIKELY(row_idx >= 0 && row_idx < row_count_
                && tgt_idx >= 0 && tgt_idx < target_count_)) {
    const int64_t bit_idx = row_idx * target_count_ + tgt_idx;
    words_.at(bit_idx / 64) |= (1ULL << (bit_idx % 64));
  }
}

bool ObRelocSkipBitmap::is_skipped(const int64_t row_idx, const int64_t tgt_idx) const
{
  if (OB_UNLIKELY(!is_inited()
                  || row_idx < 0 || row_idx >= row_count_
                  || tgt_idx < 0 || tgt_idx >= target_count_)) {
    return false;
  }
  const int64_t bit_idx = row_idx * target_count_ + tgt_idx;
  return (words_.at(bit_idx / 64) & (1ULL << (bit_idx % 64))) != 0;
}

bool ObRelocSkipBitmap::has_any_skip() const
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

int ObRelocSkipBitmap::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
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

int ObRelocSkipBitmap::deserialize(const char *buf, const int64_t data_len, int64_t &pos,
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

int64_t ObRelocSkipBitmap::get_serialize_size() const
{
  // count (8 bytes) + each word (8 bytes)
  return static_cast<int64_t>(sizeof(int64_t)) * (1 + words_.count());
}

int ObRelocSkipBitmap::rewrite_inplace() const
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

// ======================== ObLsMdsTabletRelocBatchArg ========================

ObLsMdsTabletRelocBatchArg::ObLsMdsTabletRelocBatchArg()
{
  reset();
}

void ObLsMdsTabletRelocBatchArg::reset()
{
  version_ = LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION;
  reloc_type_ = ObLsMdsTabletRelocType::INVALID;
  ls_id_.reset();
  targets_.reset();
  rows_.reset();
  skip_bitmap_.reset();
  is_final_ = false;
  fixed_ser_size_ = 0;
  rows_ser_size_ = 0;
}

void ObLsMdsTabletRelocBatchArg::reset_rows()
{
  rows_.reset();
  skip_bitmap_.reset();
  is_final_ = false;
  rows_ser_size_ = 0;
}

void ObLsMdsTabletRelocBatchArg::init_fixed_serialize_size()
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, version_, reloc_type_, ls_id_, targets_);
  len += serialization::encoded_length_vi64(UNIS_VERSION)
      + serialization::OB_SERIALIZE_SIZE_NEED_BYTES
      + serialization::encoded_length_bool(is_final_);
  fixed_ser_size_ = len;
}

int ObLsMdsTabletRelocBatchArg::push_row(const ObLsMdsTabletRelocRowStorage &row)
{
  int ret = rows_.push_back(row);
  if (OB_SUCC(ret)) {
    rows_ser_size_ += row.get_serialize_size();
  }
  return ret;
}

void ObLsMdsTabletRelocBatchArg::pop_back_row()
{
  if (rows_.count() > 0) {
    rows_ser_size_ -= rows_.at(rows_.count() - 1).get_serialize_size();
    rows_.pop_back();
  }
}

int64_t ObLsMdsTabletRelocBatchArg::estimate_serialize_size_after_push_row(
    const int64_t incoming_row_serialize_size)
{
  if (0 == fixed_ser_size_) {
    init_fixed_serialize_size();
  }
  const int64_t row_count = rows_.count() + 1;
  const int64_t rows_serialize_size = rows_ser_size_ + incoming_row_serialize_size;
  const int64_t bit_count = row_count * targets_.count();
  const int64_t word_count = (bit_count + 63) / 64;
  return fixed_ser_size_
      + serialization::encoded_length_vi64(row_count)
      + rows_serialize_size
      + static_cast<int64_t>(sizeof(int64_t)) * (1 + word_count);
}

bool ObLsMdsTabletRelocBatchArg::is_valid() const
{
  return ls_id_.is_valid()
      && version_ == LS_MDS_TABLET_RELOC_BATCH_ARG_VERSION
      && is_valid_reloc_type(reloc_type_)
      && targets_.count() > 0
      && (rows_.count() > 0 || is_final_)
      && rows_.count() <= rows_.get_capacity();
}

OB_SERIALIZE_MEMBER(ObLsMdsTabletRelocTarget, tablet_id_, transfer_scn_);
OB_SERIALIZE_MEMBER(ObMdsValueMeta, writer_trans_id_, commit_version_, commit_scn_);
OB_SERIALIZE_MEMBER(ObLsMdsTabletRelocRowStorage, mds_unit_id_, meta_, ser_payload_);

// BatchArg serialization: the first five fields use standard UNIS variable-
// length encoding; skip_bitmap_ uses its own fixed-length encoding, followed
// by is_final_ at the tail.  Keeping is_final_ after the bitmap preserves the
// offsets used by ObRelocSkipBitmap::rewrite_inplace().

OB_DEF_SERIALIZE(ObLsMdsTabletRelocBatchArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, version_, reloc_type_, ls_id_, targets_, rows_);
  if (OB_SUCC(ret) && OB_FAIL(skip_bitmap_.serialize(buf, buf_len, pos))) {
    LOG_WARN("serialize skip bitmap failed", KR(ret));
  } else if (OB_SUCC(ret) && OB_FAIL(serialization::encode_bool(buf, buf_len, pos, is_final_))) {
    LOG_WARN("serialize final flag failed", KR(ret));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObLsMdsTabletRelocBatchArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, version_, reloc_type_, ls_id_, targets_, rows_);
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

OB_DEF_SERIALIZE_SIZE(ObLsMdsTabletRelocBatchArg)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, version_, reloc_type_, ls_id_, targets_, rows_);
  len += skip_bitmap_.get_serialize_size();
  len += serialization::encoded_length_bool(is_final_);
  return len;
}

} // namespace storage
} // namespace oceanbase
