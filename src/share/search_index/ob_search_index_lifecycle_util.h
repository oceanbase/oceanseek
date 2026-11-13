/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SEARCH_INDEX_LIFECYCLE_UTIL_H_
#define OCEANBASE_SHARE_SEARCH_INDEX_LIFECYCLE_UTIL_H_

#include "lib/container/ob_iarray.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
// ObIndexType is an existing enum without a fixed underlying type. Include
// its definition rather than forward-declaring it or changing its ABI.
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObSchemaGetterGuard;
}

// One physical child of a Search Def, described by registered metadata.
// collect_search_children() returns these in DATA, TEXT_MATCH, TEXT_PHRASE order.
struct ObSearchPhysicalChild
{
  ObSearchPhysicalChild()
      : def_table_id_(OB_INVALID_ID), table_id_(OB_INVALID_ID),
        index_type_(schema::INDEX_TYPE_IS_NOT) {}
  uint64_t def_table_id_;          // Owning logical Search Def table ID.
  uint64_t table_id_;              // Physical child table ID, not a tablet ID.
  schema::ObIndexType index_type_; // Schema type: Data, Text MATCH or Text PHRASE.
  TO_STRING_KV(K_(def_table_id), K_(table_id), K_(index_type));
};

// A maintenance target after expanding all selected indexes of a base table.
// Represents a Search child, an async FTS table, or the shared Delete table;
// the logical Search Def itself is never a physical target.
struct ObAsyncPhysicalTarget
{
  ObAsyncPhysicalTarget()
      : table_id_(OB_INVALID_ID), def_table_id_(OB_INVALID_ID),
        index_type_(schema::INDEX_TYPE_IS_NOT), is_delete_(false) {}
  uint64_t table_id_;     // Physical table ID; resolve its tablet per base partition.
  uint64_t def_table_id_; // Owning Search Def, or OB_INVALID_ID for FTS/Delete.
  schema::ObIndexType index_type_;
  bool is_delete_;        // True only for the shared deletion-record table.
  TO_STRING_KV(K_(table_id), K_(def_table_id), K_(index_type), K_(is_delete));
};

// Shared schema utilities for physical-target enumeration.
// Merge, Delete GC and published-SCN queries use these to expand Search Defs into
// the physical tables they maintain. This class does not schedule or execute work.
// Relationships and statuses must all come from the caller's single guard.
// Results contain IDs, never schema pointers that could outlive that guard.
class ObSearchIndexLifecycleUtil
{
public:
  // Enumerate registered children only; do not read their schemas or filter status.
  // DATA is optional; an empty registration list produces an empty result.
  static int collect_search_children(
      const schema::ObTableSchema &def_schema,
      common::ObIArray<ObSearchPhysicalChild> &children);

  // Expand one logical async index registered on base_schema. FTS/Delete return themselves;
  // Search Def returns its registered children, or OB_SCHEMA_EAGAIN if none are registered.
  // Failure clears targets. Callers own schema lookup errors,
  // status filtering, and whether Delete participates in their operation.
  static int expand_async_index(
      const schema::ObTableSchema &base_schema,
      const schema::ObTableSchema &index_schema,
      common::ObIArray<ObAsyncPhysicalTarget> &targets);

  // Lookup a physical target and validate its registered relationship if present.
  // Success with a null schema is preserved for the caller to skip/retry/report.
  static int get_target_schema(
      schema::ObSchemaGetterGuard &guard,
      const schema::ObTableSchema &base_schema,
      const ObAsyncPhysicalTarget &target,
      const schema::ObTableSchema *&schema);

  // Validate Search child types, valid IDs, one common Def and unique IDs/types.
  // Any subset of DATA/TEXT_MATCH/TEXT_PHRASE (including empty) is accepted, since
  // the configured type set is not known here. Malformed children return
  // OB_ERR_UNEXPECTED.
  static int validate_child_types(
      const common::ObIArray<ObSearchPhysicalChild> &children);

};

} // namespace share
} // namespace oceanbase
#endif
