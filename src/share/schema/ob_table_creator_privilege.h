/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_SHARE_SCHEMA_OB_TABLE_CREATOR_PRIVILEGE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_TABLE_CREATOR_PRIVILEGE_H_

#include "share/schema/ob_priv_mgr.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;

class ObTableCreatorPrivilege
{
public:
  static constexpr ObPrivSet PRIVILEGES =
      OB_PRIV_SELECT | OB_PRIV_INSERT | OB_PRIV_UPDATE | OB_PRIV_DELETE;

  static int check_enabled(const uint64_t tenant_id, bool &enabled);
  static bool is_supported_table(const ObTableSchema &schema);
};

} // namespace schema
} // namespace share
} // namespace oceanbase
#endif // OCEANBASE_SHARE_SCHEMA_OB_TABLE_CREATOR_PRIVILEGE_H_
