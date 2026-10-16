/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/logical_table/ob_logical_table_range_builder.h"

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/logical_table/ob_logical_table_encoder.h"
#include "share/logical_table/ob_logical_table_codec_utils.h"

namespace oceanbase
{
namespace share
{

using namespace common;

int ObLogicalTableRangeBuilder::next_prefix(
    const ObString &input,
    ObIAllocator &allocator,
    ObString &next)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(&input == &next)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input and next must not alias", K(ret));
  } else if (FALSE_IT(next.reset())) {
  } else if (OB_UNLIKELY(input.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid empty prefix", K(ret), K(input));
  } else {
    char *buf = static_cast<char *>(allocator.alloc(input.length()));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate next prefix buffer", K(ret), K(input.length()));
    } else {
      MEMCPY(buf, input.ptr(), input.length());
      int64_t pos = input.length() - 1;
      while (pos >= 0 && static_cast<unsigned char>(buf[pos]) == 0xFF) {
        --pos;
      }
      if (pos < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("prefix overflow when finding successor", K(ret), K(input));
      } else {
        buf[pos] = static_cast<char>(static_cast<unsigned char>(buf[pos]) + 1);
        next.assign_ptr(buf, static_cast<int32_t>(pos + 1));
      }
    }
  }
  return ret;
}

int ObLogicalTableRangeBuilder::convert_ranges(
    const uint64_t physical_ref_table_id,
    const ObLogicalTableKeyPrefix &prefix,
    const ObIArray<ObNewRange> &logical_ranges,
    ObIAllocator &allocator,
    ObIArray<ObNewRange> &physical_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!prefix.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical table key prefix", K(ret), K(prefix));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < logical_ranges.count(); ++i) {
      ObNewRange physical_range;
      if (OB_FAIL(convert_one_range_(physical_ref_table_id, prefix, logical_ranges.at(i), allocator, physical_range))) {
        LOG_WARN("failed to convert logical range", K(ret), K(i), K(logical_ranges.at(i)), K(prefix));
      } else if (OB_FAIL(physical_ranges.push_back(physical_range))) {
        LOG_WARN("failed to append physical range", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObLogicalTableRangeBuilder::build_lookup_ranges_from_rowids(
    const uint64_t physical_ref_table_id,
    const ObLogicalTableKeyPrefix &prefix,
    const ObIArray<uint64_t> &rowids,
    ObIAllocator &allocator,
    ObIArray<ObNewRange> &physical_ranges)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!prefix.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical table key prefix for lookup", K(ret), K(prefix));
  } else {
    ObSEArray<ObDatum, 1> datums;
    for (int64_t i = 0; OB_SUCC(ret) && i < rowids.count(); ++i) {
      datums.reuse();
      ObDatum datum;
      datum.ptr_ = reinterpret_cast<const char *>(&rowids.at(i));
      datum.pack_ = sizeof(uint64_t);
      ObString kv_key;
      ObNewRange range;
      if (OB_FAIL(datums.push_back(datum))) {
        LOG_WARN("failed to push rowid datum", K(ret), K(i));
      } else if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, datums, allocator, kv_key))) {
        LOG_WARN("failed to encode rowid as kv_key", K(ret), K(i), K(prefix));
      } else if (OB_FAIL(wrap_ns_kv_rowkey_(prefix.namespace_id_, kv_key, allocator, range.start_key_))) {
        LOG_WARN("failed to wrap start rowkey with ns", K(ret), K(i), K(prefix.namespace_id_));
      } else if (OB_FAIL(wrap_ns_kv_rowkey_(prefix.namespace_id_, kv_key, allocator, range.end_key_))) {
        LOG_WARN("failed to wrap end rowkey with ns", K(ret), K(i), K(prefix.namespace_id_));
      } else {
        range.table_id_ = physical_ref_table_id;
        range.border_flag_.set_inclusive_start();
        range.border_flag_.set_inclusive_end();
        range.border_flag_.unset_min_value();
        range.border_flag_.unset_max_value();
        if (OB_FAIL(physical_ranges.push_back(range))) {
          LOG_WARN("failed to append lookup range", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObLogicalTableRangeBuilder::build_namespace_range(
    const uint64_t physical_data_table_id,
    const uint64_t namespace_id,
    ObIAllocator &allocator,
    ObNewRange &physical_range)
{
  int ret = OB_SUCCESS;
  physical_range.reset();
  if (OB_UNLIKELY(OB_INVALID_ID == physical_data_table_id
                  || OB_INVALID_NAMESPACE_ID == namespace_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid namespace range arguments", K(ret),
             K(physical_data_table_id), K(namespace_id));
  } else {
    const ObString empty_kv_key;
    if (OB_FAIL(wrap_ns_kv_rowkey_(namespace_id, empty_kv_key, allocator,
                                   physical_range.start_key_))) {
      LOG_WARN("failed to wrap namespace start rowkey", K(ret), K(namespace_id));
    } else if (UINT64_MAX == namespace_id) {
      ObObj *objs = nullptr;
      if (OB_ISNULL(objs = static_cast<ObObj *>(
              allocator.alloc(sizeof(ObObj) * 2)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate end rowkey objs", K(ret));
      } else {
        new (&objs[0]) ObObj();
        new (&objs[1]) ObObj();
        objs[0].set_uint64(namespace_id);
        objs[1].set_max_value();
        physical_range.end_key_.assign(objs, 2);
        physical_range.table_id_ = physical_data_table_id;
        physical_range.border_flag_.set_inclusive_start();
        physical_range.border_flag_.set_inclusive_end();
        physical_range.border_flag_.set_max_value();
      }
    } else if (OB_FAIL(wrap_ns_kv_rowkey_(namespace_id + 1, empty_kv_key, allocator,
                                          physical_range.end_key_))) {
      LOG_WARN("failed to wrap namespace end rowkey", K(ret), K(namespace_id));
    } else {
      physical_range.table_id_ = physical_data_table_id;
      physical_range.border_flag_.set_inclusive_start();
      physical_range.border_flag_.unset_inclusive_end();
    }
  }
  return ret;
}

int ObLogicalTableRangeBuilder::build_primary_ranges(
    const uint64_t physical_ref_table_id,
    const ObLogicalTableKeyPrefix &prefix,
    const uint64_t lt_id_end,
    const bool lt_start_inclusive,
    const bool lt_end_inclusive,
    const bool has_rowid_lower_bound,
    const uint64_t rowid_start,
    const bool rowid_start_inclusive,
    const bool has_rowid_upper_bound,
    const uint64_t rowid_end,
    const bool rowid_end_inclusive,
    ObIAllocator &allocator,
    ObIArray<ObNewRange> &physical_ranges)
{
  int ret = OB_SUCCESS;
  const uint64_t ns_id = prefix.namespace_id_;
  const uint64_t lt_id_start = prefix.logical_table_id_;
  const uint64_t idx_seq = prefix.index_seq_;
  const uint8_t aux_byte = static_cast<uint8_t>(prefix.aux_type_);
  // lt_prefix layout (kv_key column content): [ns(8B)][lt(8B)][idx_seq(8B)][aux(1B)] = 25 bytes.
  // Must stay aligned with ObLogicalTableEncoder::encode_key / OB_LOGICAL_TABLE_KEY_PREFIX_LEN
  // so that scan ranges match the kv_key bytes written by DML.
  static const int64_t LT_PREFIX_LEN = OB_LOGICAL_TABLE_KEY_PREFIX_LEN;

  // Compute effective half-open lt range [lt_lo, lt_hi_excl):
  //   lt_lo:       smallest lt to include (UINT64_MAX+1 means empty range below)
  //   lt_hi_excl:  first lt to exclude   (saturated_hi means "infinity", no upper bound)
  bool empty_range = false;
  uint64_t lt_lo = 0;
  uint64_t lt_hi_excl = 0;
  bool saturated_hi = false;
  if (OB_UNLIKELY(!prefix.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical table key prefix", K(ret), K(prefix));
  } else if (OB_UNLIKELY(lt_id_start > lt_id_end)) {
    empty_range = true;
  } else {
    if (lt_start_inclusive) {
      lt_lo = lt_id_start;
    } else if (UINT64_MAX == lt_id_start) {
      empty_range = true;
    } else {
      lt_lo = lt_id_start + 1;
    }
    if (!empty_range) {
      if (lt_end_inclusive) {
        if (UINT64_MAX == lt_id_end) {
          saturated_hi = true;
        } else {
          lt_hi_excl = lt_id_end + 1;
        }
      } else {
        lt_hi_excl = lt_id_end;
      }
      if (!saturated_hi && lt_lo >= lt_hi_excl) {
        empty_range = true;
      }
    }
  }
  // is_single_lt = effective lt range covers exactly one ltable_id value
  // (then we honor rowid bounds; otherwise rowid bounds are ignored).
  const bool is_single_lt = !empty_range && !saturated_hi && (lt_hi_excl == lt_lo + 1);

  ObString start_kv_key;
  ObString end_kv_key;
  // These two flags are mutually exclusive by construction:
  //   - inclusive_end is only set in the single-lt branch (exact encoded rowid upper bound).
  //   - end_is_max_sentinel is only set in the multi-lt branch when lt range is unbounded above.
  // Both independently map the resulting storage range's end to inclusive; the assembly step
  // below asserts they are never set together so the two semantics stay decoupled.
  bool inclusive_end = false;        // border flag for assembly; default exclusive
  bool end_is_max_sentinel = false;  // when lt upper bound is +infinity (UINT64_MAX inclusive)

  if (OB_FAIL(ret)) {
  } else if (empty_range) {
    // empty range: produce no physical range so DAS scans nothing
  } else if (is_single_lt) {
    // single-lt prefix with rowid bounds (preserves prior equality behavior)
    char lt_buf[LT_PREFIX_LEN];
    int64_t tmp_pos = 0;
    unsigned char *dst = reinterpret_cast<unsigned char *>(lt_buf);
    if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
            ns_id, dst, LT_PREFIX_LEN, tmp_pos))) {
      LOG_WARN("failed to encode ns_id", K(ret), K(ns_id));
    } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
            lt_lo, dst, LT_PREFIX_LEN, tmp_pos))) {
      LOG_WARN("failed to encode lt_id", K(ret), K(lt_lo));
    } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
            idx_seq, dst, LT_PREFIX_LEN, tmp_pos))) {
      LOG_WARN("failed to encode idx_seq", K(ret), K(idx_seq));
    } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint8(
            aux_byte, dst, LT_PREFIX_LEN, tmp_pos))) {
      LOG_WARN("failed to encode aux_type", K(ret), K(aux_byte));
    }
    ObString lt_prefix;
    lt_prefix.assign_ptr(lt_buf, LT_PREFIX_LEN);
    // prefix-only kv_key (for open rowid bounds)
    ObString prefix_kv_key;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(append_key_bytes_(lt_prefix, ObString(), allocator, prefix_kv_key))) {
      LOG_WARN("failed to construct prefix kv_key", K(ret));
    }
    // build start kv_key: rowid bounds use fixed-width encode (+1 for exclusive),
    // not next_prefix on the composite key.
    if (OB_FAIL(ret)) {
    } else if (!has_rowid_lower_bound) {
      start_kv_key = prefix_kv_key;
    } else if (rowid_start_inclusive) {
      char rowid_buf[sizeof(uint64_t)];
      int64_t rpos = 0;
      unsigned char *rdst = reinterpret_cast<unsigned char *>(rowid_buf);
      if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
              rowid_start, rdst, sizeof(uint64_t), rpos))) {
        LOG_WARN("failed to encode rowid_start", K(ret), K(rowid_start));
      } else {
        ObString rowid_str;
        rowid_str.assign_ptr(rowid_buf, sizeof(uint64_t));
        if (OB_FAIL(append_key_bytes_(lt_prefix, rowid_str, allocator, start_kv_key))) {
          LOG_WARN("failed to construct start kv_key", K(ret), K(rowid_start));
        }
      }
    } else if (UINT64_MAX == rowid_start) {
      empty_range = true;
    } else {
      char rowid_buf[sizeof(uint64_t)];
      int64_t rpos = 0;
      unsigned char *rdst = reinterpret_cast<unsigned char *>(rowid_buf);
      if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
              rowid_start + 1, rdst, sizeof(uint64_t), rpos))) {
        LOG_WARN("failed to encode exclusive rowid_start", K(ret), K(rowid_start));
      } else {
        ObString rowid_str;
        rowid_str.assign_ptr(rowid_buf, sizeof(uint64_t));
        if (OB_FAIL(append_key_bytes_(lt_prefix, rowid_str, allocator, start_kv_key))) {
          LOG_WARN("failed to construct exclusive start kv_key", K(ret), K(rowid_start));
        }
      }
    }
    // build end kv_key
    ObString end_next;
    bool end_use_next = false;
    if (OB_FAIL(ret)) {
    } else if (empty_range) {
    } else if (!has_rowid_upper_bound) {
      end_kv_key = prefix_kv_key;
      end_use_next = true;  // open rowid upper bound -> next_prefix(prefix)
      inclusive_end = false;
    } else {
      char rowid_buf[sizeof(uint64_t)];
      int64_t rpos = 0;
      unsigned char *rdst = reinterpret_cast<unsigned char *>(rowid_buf);
      if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
              rowid_end, rdst, sizeof(uint64_t), rpos))) {
        LOG_WARN("failed to encode rowid_end", K(ret), K(rowid_end));
      } else {
        ObString rowid_str;
        rowid_str.assign_ptr(rowid_buf, sizeof(uint64_t));
        if (OB_FAIL(append_key_bytes_(lt_prefix, rowid_str, allocator, end_kv_key))) {
          LOG_WARN("failed to construct end kv_key", K(ret), K(rowid_end));
        }
      }
      if (OB_SUCC(ret)) {
        inclusive_end = rowid_end_inclusive;
      }
    }
    if (OB_SUCC(ret) && end_use_next) {
      if (OB_FAIL(next_prefix(end_kv_key, allocator, end_next))) {
        LOG_WARN("failed to advance end kv_key prefix", K(ret), K(end_kv_key));
      } else {
        end_kv_key = end_next;
      }
    }
  } else {
    // multi-lt range: rowid bounds ignored; scan [encode(ns|lt_lo|idx_seq|aux)|0 ..
    //   encode(ns|lt_hi_excl|0|0)|0). Upper side switches to ObObj max sentinel
    //   when lt range is unbounded above.
    char start_buf[LT_PREFIX_LEN];
    int64_t spos = 0;
    unsigned char *sdst = reinterpret_cast<unsigned char *>(start_buf);
    if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
            ns_id, sdst, LT_PREFIX_LEN, spos))) {
      LOG_WARN("failed to encode ns_id", K(ret), K(ns_id));
    } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
            lt_lo, sdst, LT_PREFIX_LEN, spos))) {
      LOG_WARN("failed to encode lt_lo", K(ret), K(lt_lo));
    } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
            idx_seq, sdst, LT_PREFIX_LEN, spos))) {
      LOG_WARN("failed to encode idx_seq", K(ret), K(idx_seq));
    } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint8(
            aux_byte, sdst, LT_PREFIX_LEN, spos))) {
      LOG_WARN("failed to encode aux_byte for start", K(ret), K(aux_byte));
    } else {
      ObString start_lt_prefix;
      start_lt_prefix.assign_ptr(start_buf, LT_PREFIX_LEN);
      // pad rowid=0 (8 zero bytes) so the binary length matches data keys
      char rowid_zero[sizeof(uint64_t)];
      MEMSET(rowid_zero, 0, sizeof(rowid_zero));
      ObString rowid_str;
      rowid_str.assign_ptr(rowid_zero, sizeof(rowid_zero));
      if (OB_FAIL(append_key_bytes_(start_lt_prefix, rowid_str, allocator, start_kv_key))) {
        LOG_WARN("failed to build multi-lt start kv_key", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (saturated_hi) {
        end_is_max_sentinel = true;
      } else {
        char end_buf[LT_PREFIX_LEN];
        int64_t epos = 0;
        unsigned char *edst = reinterpret_cast<unsigned char *>(end_buf);
        if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
                ns_id, edst, LT_PREFIX_LEN, epos))) {
          LOG_WARN("failed to encode ns_id for end", K(ret), K(ns_id));
        } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
                lt_hi_excl, edst, LT_PREFIX_LEN, epos))) {
          LOG_WARN("failed to encode lt_hi_excl", K(ret), K(lt_hi_excl));
        } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint64(
                static_cast<uint64_t>(0), edst, LT_PREFIX_LEN, epos))) {
          LOG_WARN("failed to encode idx_seq for end", K(ret));
        } else if (OB_FAIL(ObLogicalTableCodecUtils::encode_uint8(
                static_cast<uint8_t>(0), edst, LT_PREFIX_LEN, epos))) {
          LOG_WARN("failed to encode aux_byte for end", K(ret));
        } else {
          ObString end_lt_prefix;
          end_lt_prefix.assign_ptr(end_buf, LT_PREFIX_LEN);
          char rowid_zero[sizeof(uint64_t)];
          MEMSET(rowid_zero, 0, sizeof(rowid_zero));
          ObString rowid_str;
          rowid_str.assign_ptr(rowid_zero, sizeof(rowid_zero));
          if (OB_FAIL(append_key_bytes_(end_lt_prefix, rowid_str, allocator, end_kv_key))) {
            LOG_WARN("failed to build multi-lt end kv_key", K(ret));
          }
        }
      }
    }
  }

  // assemble 2-column range: (namespace_id uint64, kv_key varbinary)
  if (OB_SUCC(ret) && !empty_range) {
    ObNewRange range;
    range.reset();
    if (OB_FAIL(wrap_ns_kv_rowkey_(ns_id, start_kv_key, allocator, range.start_key_))) {
      LOG_WARN("failed to wrap start rowkey with ns", K(ret), K(ns_id));
    } else if (end_is_max_sentinel) {
      // build end rowkey as (ns_id, MAX_VALUE) so the scan reaches kv_key infinity within this ns
      ObObj *objs = nullptr;
      if (OB_ISNULL(objs = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * 2)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate end rowkey objs", K(ret));
      } else {
        new (&objs[0]) ObObj();
        new (&objs[1]) ObObj();
        objs[0].set_uint64(ns_id);
        objs[1].set_max_value();
        range.end_key_.assign(objs, 2);
      }
    } else if (OB_FAIL(wrap_ns_kv_rowkey_(ns_id, end_kv_key, allocator, range.end_key_))) {
      LOG_WARN("failed to wrap end rowkey with ns", K(ret), K(ns_id));
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(inclusive_end && end_is_max_sentinel)) {
      // single-lt (inclusive_end) and multi-lt (end_is_max_sentinel) paths are mutually
      // exclusive; both true means the branch logic above is broken.
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("inclusive_end and end_is_max_sentinel must not both be set",
               K(ret), K(is_single_lt), K(saturated_hi), K(prefix));
    }
    if (OB_SUCC(ret)) {
      range.table_id_ = physical_ref_table_id;
      range.border_flag_.set_inclusive_start();
      // end is inclusive when it is the exact encoded rowid (single-lt) OR the +infinity
      // MAX sentinel (multi-lt unbounded above); otherwise it stays exclusive (next_prefix).
      if (inclusive_end || end_is_max_sentinel) {
        range.border_flag_.set_inclusive_end();
      } else {
        range.border_flag_.unset_inclusive_end();
      }
      LOG_TRACE("[LT DEBUG][SCAN_PRIMARY_RANGE]",
                K(physical_ref_table_id), K(prefix), K(ns_id),
                K(lt_id_start), K(lt_id_end),
                K(lt_start_inclusive), K(lt_end_inclusive),
                K(has_rowid_lower_bound), K(has_rowid_upper_bound),
                K(rowid_start), K(rowid_end),
                K(rowid_start_inclusive), K(rowid_end_inclusive),
                K(is_single_lt), K(end_is_max_sentinel), K(inclusive_end),
                "start_kv_key_len", start_kv_key.length(),
                "start_kv_key_hex", common::ObHexStringWrap(start_kv_key),
                "end_kv_key_len", end_kv_key.length(),
                "end_kv_key_hex", common::ObHexStringWrap(end_kv_key),
                "border_inclusive_start", range.border_flag_.inclusive_start(),
                "border_inclusive_end", range.border_flag_.inclusive_end());
      if (OB_FAIL(physical_ranges.push_back(range))) {
        LOG_WARN("failed to append physical range", K(ret));
      }
    }
  }
  return ret;
}

int ObLogicalTableRangeBuilder::convert_one_range_(
    const uint64_t physical_ref_table_id,
    const ObLogicalTableKeyPrefix &prefix,
    const ObNewRange &logical_range,
    ObIAllocator &allocator,
    ObNewRange &physical_range)
{
  int ret = OB_SUCCESS;
  ObString start_key;
  ObString end_key;
  ObString start_key_next;
  ObString end_key_next;
  bool start_use_next_prefix = false;
  bool end_use_next_prefix = false;
  bool start_is_prefix_bound = false;
  bool end_is_prefix_bound = false;
  physical_range.reset();

  // The optimizer range uses MIN/MAX sentinels in rowkey tuples, but the binary key encoding
  // cannot reuse raw 0x00/0xFF to represent them because 0x00 is already reserved by the null
  // rank byte. We therefore convert each logical bound to either:
  //   1) [Prefix][encoded fixed columns]
  //   2) next_prefix([Prefix][encoded fixed columns])
  //
  // For prefix bounds, the physical scan range is always normalized to left-closed/right-open:
  //   start (..., MIN) -> [Prefix][encoded_value], closed
  //   start (..., MAX) -> next_prefix([Prefix][encoded_value]), closed
  //   end   (..., MIN) -> [Prefix][encoded_value], open
  //   end   (..., MAX) -> next_prefix([Prefix][encoded_value]), open
  //
  // Examples:
  //   c1 = 1         : (1, MIN) : (1, MAX)
  //                    -> [[Prefix][encode(1)], next_prefix([Prefix][encode(1)]))
  //   c1 > 1         : (1, MAX) : (MAX, MAX)
  //                    -> [next_prefix([Prefix][encode(1)]), next_prefix([Prefix]))
  //   c1 IS NULL     : (NULL, MIN) : (NULL, MAX)
  //                    -> [[Prefix][encode(NULL)], next_prefix([Prefix][encode(NULL)]))
  //   c1 IS NULL,c2>1: (NULL, 1, MAX) : (NULL, MAX, MAX)
  //                    -> [next_prefix([Prefix][encode(NULL)][encode(1)]),
  //                        next_prefix([Prefix][encode(NULL)]))
  if (OB_UNLIKELY(!logical_range.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical rowkey range", K(ret), K(logical_range));
  } else if (OB_FAIL(encode_rowkey_bound_(
      prefix,
      logical_range.get_start_key(),
      true,
      allocator,
      start_key,
      start_use_next_prefix,
      start_is_prefix_bound))) {
    LOG_WARN("failed to encode start key bound", K(ret), K(prefix), K(logical_range));
  } else if (OB_FAIL(encode_rowkey_bound_(
      prefix,
      logical_range.get_end_key(),
      false,
      allocator,
      end_key,
      end_use_next_prefix,
      end_is_prefix_bound))) {
    LOG_WARN("failed to encode end key bound", K(ret), K(prefix), K(logical_range));
  } else {
    LOG_TRACE("[LT DEBUG] convert_one_range encoded bounds",
             K(logical_range),
             K(start_key.length()),
             K(end_key.length()),
             K(start_use_next_prefix),
             K(end_use_next_prefix),
             K(start_is_prefix_bound),
             K(end_is_prefix_bound));
    if (start_use_next_prefix && OB_FAIL(next_prefix(start_key, allocator, start_key_next))) {
      LOG_WARN("failed to advance start prefix", K(ret), K(start_key));
    } else if (end_use_next_prefix && OB_FAIL(next_prefix(end_key, allocator, end_key_next))) {
      LOG_WARN("failed to advance end prefix", K(ret), K(end_key));
    } else {
      const ObString &start_bound_key = start_use_next_prefix ? start_key_next : start_key;
      const ObString &end_bound_key = end_use_next_prefix ? end_key_next : end_key;
      LOG_TRACE("[LT DEBUG] convert_one_range final kv bounds before rowkey wrap",
               K(physical_ref_table_id),
               K(prefix),
               "start_bound_key_len", start_bound_key.length(),
               "start_bound_key_hex", common::ObHexStringWrap(start_bound_key),
               "end_bound_key_len", end_bound_key.length(),
               "end_bound_key_hex", common::ObHexStringWrap(end_bound_key),
               K(start_use_next_prefix),
               K(end_use_next_prefix),
               "physical_kv_rowkey_shape", "ns_kv_rowkey");
      if (OB_FAIL(wrap_ns_kv_rowkey_(prefix.namespace_id_, start_bound_key, allocator, physical_range.start_key_))) {
        LOG_WARN("failed to wrap start rowkey with ns", K(ret), K(start_bound_key), K(prefix.namespace_id_));
      } else if (OB_FAIL(wrap_ns_kv_rowkey_(prefix.namespace_id_, end_bound_key, allocator, physical_range.end_key_))) {
        LOG_WARN("failed to wrap end rowkey with ns", K(ret), K(end_bound_key), K(prefix.namespace_id_));
      } else {
        physical_range.table_id_ = physical_ref_table_id;
        physical_range.border_flag_ = logical_range.border_flag_;
        // MIN/MAX sentinels are consumed during physical bound normalization, so the resulting
        // storage range only keeps open/close semantics on the rewritten binary boundaries.
        physical_range.border_flag_.unset_min_value();
        physical_range.border_flag_.unset_max_value();
        if (start_is_prefix_bound) {
          physical_range.border_flag_.set_inclusive_start();
        }
        if (end_is_prefix_bound) {
          physical_range.border_flag_.unset_inclusive_end();
        }
        LOG_TRACE("[LT DEBUG] convert_one_range final physical range",
                 K(physical_ref_table_id),
                 K(prefix),
                 "start_obj_cnt", physical_range.start_key_.get_obj_cnt(),
                 "end_obj_cnt", physical_range.end_key_.get_obj_cnt(),
                 K(physical_range));
      }
    }
  }

  return ret;
}

int ObLogicalTableRangeBuilder::encode_rowkey_bound_(
    const ObLogicalTableKeyPrefix &prefix,
    const ObRowkey &logical_rowkey,
    const bool is_start_bound,
    ObIAllocator &allocator,
    ObString &encoded_key,
    bool &use_next_prefix,
    bool &is_prefix_bound)
{
  int ret = OB_SUCCESS;
  use_next_prefix = false;
  is_prefix_bound = false;
  encoded_key.reset();

  // Encode the determined prefix before the first MIN/MAX sentinel. `use_next_prefix` answers
  // whether the encoded prefix still needs to be advanced to its lexicographic successor, while
  // `is_prefix_bound` tells the caller whether this bound came from MIN/MAX truncation and thus
  // should use normalized left-closed/right-open border semantics.
  //
  // Examples:
  //   start (1, MIN)      -> encoded_key = [Prefix][encode(1)],               use_next_prefix = false
  //   start (1, MAX)      -> encoded_key = [Prefix][encode(1)],               use_next_prefix = true
  //   end   (NULL, MAX)   -> encoded_key = [Prefix][encode(NULL)],            use_next_prefix = true
  //   end   (MAX, MAX)    -> encoded_key = [Prefix],                          use_next_prefix = true
  //
  // next_prefix returns the lexicographic successor prefix of the encoded binary key, e.g.
  //   {0x12, 0x34, 0xFF, 0xFF} -> {0x12, 0x35}
  // is_start_bound is only consumed in the NULL handling branch below (line 276):
  // for a non-nullable column, NULL in start bound → prefix bound (MIN sentinel),
  // NULL in end bound → next_prefix bound (MAX sentinel). MIN/MAX sentinels and
  // concrete values do not need is_start_bound for disambiguation.
  UNUSED(is_start_bound);
  if (OB_UNLIKELY(logical_rowkey.is_min_row() || logical_rowkey.is_max_row())) {
    ObSEArray<ObDatum, 1> empty_datums;
    is_prefix_bound = true;
    LOG_TRACE("[LT DEBUG] encode_rowkey_bound min/max row sentinel",
             K(is_start_bound),
             "is_min_row",
             logical_rowkey.is_min_row(),
             "is_max_row",
             logical_rowkey.is_max_row());
    if (OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, empty_datums, allocator, encoded_key))) {
      LOG_WARN("failed to encode min/max key", K(ret), K(prefix));
    } else if (logical_rowkey.is_max_row()) {
      use_next_prefix = true;
    }
  } else {
    const int64_t cnt = logical_rowkey.get_obj_cnt();
    const ObLogicalTableKeyColDesc *col_descs = nullptr;
    int64_t col_cnt = 0;
    ObSEArray<ObDatum, 4> datums;
    if (OB_FAIL(get_aux_type_col_descs(prefix.aux_type_, col_descs, col_cnt))) {
      LOG_WARN("failed to get aux type column descriptors", K(ret), K(prefix.aux_type_));
    } else if (OB_UNLIKELY(col_cnt != cnt)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("rowkey column count and obj count mismatched", K(ret), K(cnt), K(col_cnt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
        ObDatum datum;
        const ObObj &obj = logical_rowkey.get_obj_ptr()[i];
        LOG_TRACE("[LT DEBUG] encode_rowkey_bound obj",
                 K(is_start_bound),
                 K(i),
                 K(cnt),
                 "obj_type",
                 obj.get_type(),
                 K(obj));
        if (obj.is_min_value()) {
          is_prefix_bound = true;
          LOG_TRACE("[LT DEBUG] encode_rowkey_bound hit MIN sentinel at column, truncate prefix here",
                   K(is_start_bound),
                   K(i),
                   "datums_built",
                   datums.count());
          break;
        } else if (obj.is_max_value()) {
          is_prefix_bound = true;
          use_next_prefix = true;
          LOG_TRACE("[LT DEBUG] encode_rowkey_bound hit MAX sentinel at column, truncate prefix here",
                   K(is_start_bound),
                   K(i),
                   "datums_built",
                   datums.count());
          break;
        } else if (obj.is_null()) {
          if (col_descs[i].is_nullable_) {
            datum.set_null();
            if (OB_FAIL(datums.push_back(datum))) {
              LOG_WARN("failed to append null datum", K(ret), K(i), K(datum));
            }
          } else {
            // A non-nullable key column receiving a NULL ObObj usually signals an
            // illegal upstream range or a schema mismatch. We still degrade to a
            // prefix bound to stay robust, but log a warning so the anomaly is visible.
            LOG_WARN("non-nullable key column received NULL obj in logical range, treat as prefix bound",
                     K(i), K(is_start_bound), K(col_descs[i]), K(obj));
            is_prefix_bound = true;
            if (!is_start_bound) {
              use_next_prefix = true;
            }
          }
          break;
        } else {
          // ObDatum::set_int/set_uint require ptr_ to pre-point at writable storage (see
          // ob_datum.h). Stack ObDatum is uninitialized; borrow ObObj::v_ like the original code.
          switch (obj.get_type()) {
            case common::ObNullType:
              datum.set_null();
              break;
            case common::ObTinyIntType:
            case common::ObSmallIntType:
            case common::ObMediumIntType:
            case common::ObInt32Type:
            case common::ObIntType:
              datum.ptr_ = reinterpret_cast<const char *>(&obj.v_.int64_);
              datum.pack_ = sizeof(int64_t);
              break;
            case common::ObUTinyIntType:
            case common::ObUSmallIntType:
            case common::ObUMediumIntType:
            case common::ObUInt32Type:
            case common::ObUInt64Type:
              datum.ptr_ = reinterpret_cast<const char *>(&obj.v_.uint64_);
              datum.pack_ = sizeof(uint64_t);
              break;
            case common::ObVarcharType:
            case common::ObCharType:
              datum.set_string(obj.get_string_ptr(), obj.get_string_len());
              break;
            default:
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("unsupported logical range obj type", K(ret), K(obj), K(obj.get_type()));
              break;
          }
          if (OB_SUCC(ret) && OB_FAIL(datums.push_back(datum))) {
            LOG_WARN("failed to append datum", K(ret), K(i), K(datum));
          }
        }
      }
      if (OB_SUCC(ret)) {
        LOG_TRACE("[LT DEBUG] encode_rowkey_bound before encode_key",
                 K(is_start_bound),
                 K(cnt),
                 K(col_cnt),
                 "datum_cnt",
                 datums.count(),
                 K(prefix));
      }
      if (OB_SUCC(ret) && OB_FAIL(ObLogicalTableEncoder::encode_key(prefix, datums, allocator, encoded_key))) {
        LOG_WARN("failed to encode logical table key", K(ret), K(prefix), K(datums));
      } else if (OB_SUCC(ret)) {
        LOG_TRACE("[LT DEBUG] encode_rowkey_bound after encode_key",
                 K(is_start_bound),
                 K(encoded_key.length()),
                 KP(encoded_key.ptr()));
      }
    }
  }
  return ret;
}

int ObLogicalTableRangeBuilder::append_key_bytes_(
    const ObString &prefix,
    const ObString &suffix,
    ObIAllocator &allocator,
    ObString &key)
{
  int ret = OB_SUCCESS;
  const int64_t total_len = prefix.length() + suffix.length();
  char *buf = static_cast<char *>(allocator.alloc(total_len));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate key buffer", K(ret), K(total_len));
  } else {
    MEMCPY(buf, prefix.ptr(), prefix.length());
    if (suffix.length() > 0) {
      MEMCPY(buf + prefix.length(), suffix.ptr(), suffix.length());
    }
    key.assign_ptr(buf, static_cast<int32_t>(total_len));
  }
  return ret;
}

int ObLogicalTableRangeBuilder::wrap_ns_kv_rowkey_(
    const uint64_t ns_id,
    const ObString &binary_key,
    ObIAllocator &allocator,
    ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObObj *objs = nullptr;
  if (OB_ISNULL(objs = static_cast<ObObj *>(
      allocator.alloc(sizeof(ObObj) * 2)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate 2-col rowkey objs", K(ret));
  } else {
    new (&objs[0]) ObObj();
    new (&objs[1]) ObObj();
    objs[0].set_uint64(ns_id);
    objs[1].set_varbinary(binary_key);
    rowkey.assign(objs, 2);
    LOG_TRACE("[LT DEBUG] wrap_ns_kv_rowkey done",
             K(ns_id),
             "rowkey_obj_cnt", rowkey.get_obj_cnt(),
             "binary_key_len", binary_key.length(),
             "binary_key_hex", common::ObHexStringWrap(binary_key));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
