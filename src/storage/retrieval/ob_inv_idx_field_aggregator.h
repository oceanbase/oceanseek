/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_INV_IDX_FIELD_AGGREGATOR_H_
#define OCEANBASE_STORAGE_OB_INV_IDX_FIELD_AGGREGATOR_H_

#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
class ObDatumRowkey;
struct ObStorageDatum;
}
namespace storage
{
class ObITableReadInfo;
class ObTablet;

// Document length statistics stay here; the index-tree traverser only navigates ranges and cached blocks.
class ObInvIdxFieldAggregator
{
public:
  // A null prefix aggregates the whole FTS tree; Search Text restricts (COL_IDX, PATH).
  // The output owns its Number digits; it remains valid after traversal and can exceed uint64.
  static int aggregate_total_doc_length(
      blocksstable::ObSSTable &sstable,
      const ObITableReadInfo &read_info,
      const blocksstable::ObDatumRowkey *field_prefix,
      ObTablet *tablet,
      blocksstable::ObStorageDatum &total_doc_length);
};
} // namespace storage
} // namespace oceanbase
#endif
