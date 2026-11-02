/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_TX_SEQ_H
#define OCEANBASE_TRANSACTION_OB_TX_SEQ_H

#include "lib/ob_define.h"              // for int64_t
#include "lib/hash_func/murmur_hash.h"  // for murmurhash()
#include "lib/utility/ob_macro_utils.h" // for OB_ASSERT
#include "lib/utility/ob_print_utils.h" // for DECLARE_TO_STRING

namespace oceanbase
{
namespace transaction
{

// Transaction sequence number. Lakebase starts at 4.6.1 and only uses this layout:
//   part 1: sequence number offset against transaction start
//   part 2: id of parallel write branch
class ObTxSEQ
{
public:
  static constexpr int64_t MAX_SEQ = (1LL << 47) - 1;
  static bool is_valid_seq(int64_t seq) { return seq > 0 && seq <= MAX_SEQ; }
  ObTxSEQ() : raw_val_(0) {}
  explicit ObTxSEQ(int64_t seq, int16_t branch): raw_val_(0)
  {
    OB_ASSERT(is_valid_seq(seq));
    OB_ASSERT(branch >= 0);
    if (is_valid_seq(seq) && branch >= 0) {
      branch_ = branch;
      seq_ = seq;
      n_format_ = true;
    }
  }
private:
  explicit ObTxSEQ(int64_t raw_v): raw_val_(raw_v) {}
public:
  static const ObTxSEQ &INVL() { static ObTxSEQ v; return v; }
  static const ObTxSEQ &MIN_VAL() { static ObTxSEQ v(1, 0); return v; }
  static const ObTxSEQ &MAX_VAL() { static ObTxSEQ v(INT64_MAX); return v; }
  void reset() { raw_val_ = 0; }
  bool is_valid() const { return raw_val_ > 0 && n_format_ && seq_ > 0; }
  bool is_min() const { return *this == MIN_VAL(); }
  bool is_max() const { return *this == MAX_VAL(); }
  int clone_with_seq(int64_t seq_abs, int64_t seq_base, ObTxSEQ &result) const
  {
    int ret = OB_SUCCESS;
    if (!is_valid() || seq_base <= 0 || seq_abs <= seq_base
        || !is_valid_seq(seq_abs - seq_base)) {
      ret = OB_INVALID_ARGUMENT;
      _OB_LOG_RET(WARN, OB_INVALID_ARGUMENT,
                  "invalid transaction sequence clone: raw=%ld abs=%ld base=%ld",
                  raw_val_, seq_abs, seq_base);
    } else {
      result = ObTxSEQ(seq_abs - seq_base, branch_);
    }
    return ret;
  }
  bool operator>(const ObTxSEQ &b) const
  {
    return seq_ > b.seq_;
  }
  bool operator>=(const ObTxSEQ &b) const
  {
    return seq_ >= b.seq_;
  }
  bool operator<(const ObTxSEQ &b) const
  {
    return b > *this;
  }
  bool operator<=(const ObTxSEQ &b) const
  {
    return b >= *this;
  }
  bool operator==(const ObTxSEQ &b) const
  {
    return raw_val_ == b.raw_val_;
  }
  bool operator!=(const ObTxSEQ &b) const
  {
    return !(*this == b);
  }
  ObTxSEQ &operator++() {
    *this = *this + 1;
    return *this;
  }
  ObTxSEQ operator+(int n) const {
    return ObTxSEQ(static_cast<int64_t>(seq_) + n, branch_);
  }
  ObTxSEQ operator-(int n) const {
    return ObTxSEQ(static_cast<int64_t>(seq_) - n, branch_);
  }
  uint64_t hash() const { return murmurhash(&raw_val_, sizeof(raw_val_), 0); }
  // atomic incremental update
  int64_t inc_update(const ObTxSEQ &b) { return common::inc_update(&raw_val_, b.raw_val_); }
  uint64_t cast_to_int() const { return raw_val_; }
  // Only for cast_to_int() output (e.g. an SSTable SQL sequence), never a plain offset.
  // Malformed input logs and returns INVL; callers decide whether that is fatal.
  static ObTxSEQ cast_from_int(int64_t raw)
  {
    ObTxSEQ seq(raw);
    if (0 != raw && !seq.is_valid()) {
      _OB_LOG_RET(WARN, OB_INVALID_ARGUMENT,
                  "malformed encoded transaction sequence: raw=%ld", raw);
      return ObTxSEQ();
    }
    return seq;
  }
  // Wrap a plain sequence offset (e.g. lock-wait node's get_seq()), never a packed
  // cast_to_int() value. 0 and out-of-range offsets become INVL without asserting.
  static ObTxSEQ from_seq(int64_t seq)
  {
    return is_valid_seq(seq) ? ObTxSEQ(seq, 0) : INVL();
  }
  // return sequence number
  int64_t get_seq() const { return seq_; }
  int16_t get_branch() const { return branch_; }
  ObTxSEQ &set_branch(int16_t branch)
  {
    OB_ASSERT(is_valid() && branch >= 0);
    if (is_valid() && branch >= 0) { branch_ = branch; }
    return *this;
  }
  // atomic Load/Store
  void atomic_reset() { ATOMIC_SET(&raw_val_, 0); }
  ObTxSEQ atomic_load() const { int64_t v = ATOMIC_LOAD(&raw_val_); ObTxSEQ s; s.raw_val_ = v; return s; }
  void atomic_store(ObTxSEQ seq) { ATOMIC_STORE(&raw_val_, seq.raw_val_); }
  int64_t to_string(char* buf, const int64_t buf_len) const;
  NEED_SERIALIZE_AND_DESERIALIZE;
private:
  union {
    int64_t raw_val_;
    struct {
      uint16_t branch_     :15;
      uint64_t seq_        :47;
      bool     n_format_   :1; // Fixed wire-format marker; keep its position.
      int     _sign_       :1;
    };
  };
};
static_assert(sizeof(ObTxSEQ) == sizeof(int64_t), "ObTxSEQ should sizeof(int64_t)");

inline OB_DEF_SERIALIZE_SIMPLE(ObTxSEQ)
{
  return serialization::encode_vi64(buf, buf_len, pos, raw_val_);
}

inline OB_DEF_SERIALIZE_SIZE_SIMPLE(ObTxSEQ)
{
  return serialization::encoded_length_vi64(raw_val_);
}

inline OB_DEF_DESERIALIZE_SIMPLE(ObTxSEQ)
{
  int64_t raw = 0;
  int64_t new_pos = pos;
  int ret = serialization::decode_vi64(buf, data_len, new_pos, &raw);
  if (OB_SUCCESS == ret) {
    const ObTxSEQ seq(raw);
    if (raw != 0 && !seq.is_valid()) {
      ret = OB_VERSION_NOT_MATCH;
    } else {
      raw_val_ = raw;
      pos = new_pos;
    }
  }
  return ret;
}

}
}
#endif
