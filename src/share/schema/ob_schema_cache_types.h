/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_CACHE_TYPES_H_
#define OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_CACHE_TYPES_H_

#include "share/ob_define.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

enum ObSchemaCacheGenerationDomain : uint64_t
{
  SCHEMA_CACHE_DOMAIN_NONE = 0,
  SCHEMA_CACHE_DOMAIN_ID_RESOLVE = 1ULL << 0,
  SCHEMA_CACHE_DOMAIN_NAME_RESOLVE = 1ULL << 1,
  SCHEMA_CACHE_DOMAIN_TABLEGROUP = 1ULL << 3,
  SCHEMA_CACHE_DOMAIN_RESOLVE = SCHEMA_CACHE_DOMAIN_ID_RESOLVE
                              | SCHEMA_CACHE_DOMAIN_NAME_RESOLVE,
  SCHEMA_CACHE_DOMAIN_METADATA = SCHEMA_CACHE_DOMAIN_RESOLVE
                               | SCHEMA_CACHE_DOMAIN_TABLEGROUP,
  SCHEMA_CACHE_DOMAIN_ALL = SCHEMA_CACHE_DOMAIN_METADATA
};

// Precise-version objects are immutable and have no cache generation.
// One typed value carries every independently invalidated metadata epoch. Keeping
// the fields named prevents same-type generation parameters from being swapped
// as the publication context crosses schema-refresh layers.
struct ObSchemaCacheGenerations
{
  ObSchemaCacheGenerations() { reset(); }
  explicit ObSchemaCacheGenerations(const uint64_t initial_generation)
    : id_resolve_(initial_generation),
      name_resolve_(initial_generation),
      tablegroup_(initial_generation)
  {}

  void reset()
  {
    id_resolve_ = 0;
    name_resolve_ = 0;
    tablegroup_ = 0;
  }

  bool is_valid() const
  {
    return id_resolve_ > 0
        && name_resolve_ > 0
        && tablegroup_ > 0;
  }

  bool is_disabled() const
  {
    return 0 == id_resolve_
        && 0 == name_resolve_
        && 0 == tablegroup_;
  }

  bool operator==(const ObSchemaCacheGenerations &other) const
  {
    return id_resolve_ == other.id_resolve_
        && name_resolve_ == other.name_resolve_
        && tablegroup_ == other.tablegroup_;
  }

  bool operator!=(const ObSchemaCacheGenerations &other) const
  {
    return !(*this == other);
  }

  TO_STRING_KV(K_(id_resolve), K_(name_resolve), K_(tablegroup));

  uint64_t id_resolve_;
  uint64_t name_resolve_;
  uint64_t tablegroup_;
};

enum ObSchemaCacheRepairAction : uint8_t
{
  SCHEMA_CACHE_REPAIR_APPLIED = 0,
  SCHEMA_CACHE_REPAIR_FALLBACK,
  SCHEMA_CACHE_REPAIR_ACTION_MAX
};

struct ObSchemaCacheRepairResult
{
  ObSchemaCacheRepairResult() { reset(); }
  void reset()
  {
    action_ = SCHEMA_CACHE_REPAIR_APPLIED;
    bump_domains_ = SCHEMA_CACHE_DOMAIN_NONE;
    reason_ = common::OB_SUCCESS;
  }
  bool is_valid() const
  {
    return action_ >= SCHEMA_CACHE_REPAIR_APPLIED
        && action_ < SCHEMA_CACHE_REPAIR_ACTION_MAX
        && 0 == (bump_domains_ & ~static_cast<uint64_t>(SCHEMA_CACHE_DOMAIN_ALL))
        && (SCHEMA_CACHE_REPAIR_APPLIED != action_
            || SCHEMA_CACHE_DOMAIN_NONE == bump_domains_);
  }
  TO_STRING_KV(K_(action), K_(bump_domains), K_(reason));

  ObSchemaCacheRepairAction action_;
  uint64_t bump_domains_;
  int reason_;
};

} // namespace schema
} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_SCHEMA_CACHE_TYPES_H_
