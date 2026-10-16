/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_RANGE_BUILDER_H_
#define OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_RANGE_BUILDER_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "common/rowkey/ob_rowkey.h"
#include "common/ob_range.h"
#include "share/logical_table/ob_logical_table_key_prefix.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObObj;
}

namespace share
{

// Static utility class: all methods are static; instantiation is disallowed via = delete.
class ObLogicalTableRangeBuilder
{
public:
  // Advance a binary key to its lexicographic successor prefix.
  static int next_prefix(
      const common::ObString &input,
      common::ObIAllocator &allocator,
      common::ObString &next);

  // Convert logical table rowkey ranges to physical KV table binary ranges.
  // @param physical_ref_table_id  table_id of the physical kv_data_table
  // @param prefix                 key prefix (ns_id / lt_id / index_seq / aux_type)
  // @param logical_ranges         optimizer-generated logical rowkey ranges (columns stripped)
  // @param allocator              arena allocator for output range memory
  // @param physical_ranges        output: converted single-column BINARY rowkey ranges
  static int convert_ranges(
      const uint64_t physical_ref_table_id,
      const ObLogicalTableKeyPrefix &prefix,
      const common::ObIArray<common::ObNewRange> &logical_ranges,
      common::ObIAllocator &allocator,
      common::ObIArray<common::ObNewRange> &physical_ranges);

  // Build single-point BINARY lookup ranges from a list of uint64 rowid values.
  // Used by the index lookup path to convert rowids back to KV primary table ranges.
  // @param physical_ref_table_id  table_id of the physical kv_data_table
  // @param prefix                 key prefix for the primary KV record
  // @param rowids                 uint64 row identifiers from the secondary index
  // @param allocator              arena allocator for range memory
  // @param physical_ranges        output: point ranges on the kv_key BINARY column
  static int build_lookup_ranges_from_rowids(
      const uint64_t physical_ref_table_id,
      const ObLogicalTableKeyPrefix &prefix,
      const common::ObIArray<uint64_t> &rowids,
      common::ObIAllocator &allocator,
      common::ObIArray<common::ObNewRange> &physical_ranges);

  // Build a namespace-level KV scan range covering all rows with PK namespace_id = namespace_id.
  // Output rowkey shape: (namespace_id uint64, kv_key varbinary).
  static int build_namespace_range(
      const uint64_t physical_ref_table_id,
      const uint64_t namespace_id,
      common::ObIAllocator &allocator,
      common::ObNewRange &physical_range);

  // Build binary KV scan ranges directly from ns/lt prefix and optional rowid bounds.
  // Used when ns/lt values come from filter expressions instead of multi-column PK ranges.
  //
  // Supports both single-lt (equality) and lt range scans:
  //   - When prefix.logical_table_id_ == lt_id_end and both lt bounds inclusive, the
  //     range covers a single lt prefix with optional rowid bounds.
  //   - When the lt range spans multiple lts, the resulting kv_key range covers
  //     [encode(lt_lo)|0|0|aux|0 .. encode(lt_hi_excl)|0|0|aux|0); rowid bounds
  //     are ignored. Note: this contiguous span also includes auxiliary records
  //     (idx_seq != 0 or aux != prefix.aux_type) of intermediate lts; the upper
  //     filter layer should drop those if any exist.
  //
  // @param physical_ref_table_id  table_id of the physical kv_data_table
  // @param prefix                 key prefix (ns / lt_id_start / idx_seq / aux_type)
  // @param lt_id_end              upper bound of ltable_id (UINT64_MAX for unbounded)
  // @param lt_start_inclusive     whether ltable_id lower bound is inclusive
  // @param lt_end_inclusive       whether ltable_id upper bound is inclusive
  // @param has_rowid_lower_bound  whether a rowid lower bound exists (false => unbounded below)
  // @param rowid_start            lower bound of rowid value (ignored when has_rowid_lower_bound is false;
  //                               only honored when lt is single value). 0 is a legal rowid value.
  // @param rowid_start_inclusive  whether rowid lower bound is inclusive
  // @param has_rowid_upper_bound  whether a rowid upper bound exists (false => unbounded above)
  // @param rowid_end              upper bound of rowid value (ignored when has_rowid_upper_bound is false;
  //                               only honored when lt is single value). UINT64_MAX is a legal rowid value.
  // @param rowid_end_inclusive    whether rowid upper bound is inclusive
  // @param allocator              arena allocator for output range memory
  // @param physical_ranges        output: 0 or 1 range covering the prefix scope (0 when empty)
  static int build_primary_ranges(
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
      common::ObIAllocator &allocator,
      common::ObIArray<common::ObNewRange> &physical_ranges);

private:
  ObLogicalTableRangeBuilder() = delete;
  ~ObLogicalTableRangeBuilder() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObLogicalTableRangeBuilder);

  static int convert_one_range_(
      const uint64_t physical_ref_table_id,
      const ObLogicalTableKeyPrefix &prefix,
      const common::ObNewRange &logical_range,
      common::ObIAllocator &allocator,
      common::ObNewRange &physical_range);

  static int encode_rowkey_bound_(
      const ObLogicalTableKeyPrefix &prefix,
      const common::ObRowkey &logical_rowkey,
      const bool is_start_bound,
      common::ObIAllocator &allocator,
      common::ObString &encoded_key,
      bool &use_next_prefix,
      bool &is_prefix_bound);

  // Append suffix bytes to a prefix to produce a composite binary key.
  static int append_key_bytes_(
      const common::ObString &prefix,
      const common::ObString &suffix,
      common::ObIAllocator &allocator,
      common::ObString &key);

  // Wrap (ns_id uint64, binary_key varbinary) into a 2-column rowkey.
  static int wrap_ns_kv_rowkey_(
      const uint64_t ns_id,
      const common::ObString &binary_key,
      common::ObIAllocator &allocator,
      common::ObRowkey &rowkey);
};

} // namespace share
} // namespace oceanbase

#endif /* OCEANBASE_SHARE_LOGICAL_TABLE_OB_LOGICAL_TABLE_RANGE_BUILDER_H_ */
