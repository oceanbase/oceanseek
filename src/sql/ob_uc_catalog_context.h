/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_UC_CATALOG_CONTEXT_H_
#define OCEANBASE_SQL_OB_UC_CATALOG_CONTEXT_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace sql
{

class ObUcCatalogContext
{
public:
  ObUcCatalogContext()
    : access_token_(), child_catalog_id_(common::OB_INVALID_ID), remote_catalog_name_()
  {}
  ~ObUcCatalogContext() = default;

  void reset()
  {
    access_token_.reset();
    child_catalog_id_ = common::OB_INVALID_ID;
    remote_catalog_name_.reset();
  }
  bool has_remote_catalog() const { return common::OB_INVALID_ID != child_catalog_id_; }
  bool is_remote_catalog(const uint64_t catalog_id) const
  { return has_remote_catalog() && child_catalog_id_ == catalog_id; }
  bool matches_identity(const common::ObString &access_token,
                        const common::ObString &remote_catalog_name) const
  {
    return has_remote_catalog()
           && access_token_ == access_token
           && remote_catalog_name_ == remote_catalog_name;
  }
  int set_remote_catalog(common::ObIAllocator &allocator,
                         const common::ObString &access_token,
                         const common::ObString &remote_catalog_name,
                         const uint64_t child_catalog_id);
  const common::ObString &get_access_token() const { return access_token_; }
  uint64_t get_child_catalog_id() const { return child_catalog_id_; }
  const common::ObString &get_remote_catalog_name() const { return remote_catalog_name_; }

private:
  DISALLOW_COPY_AND_ASSIGN(ObUcCatalogContext);
  common::ObString access_token_;
  uint64_t child_catalog_id_;
  common::ObString remote_catalog_name_;
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_OB_UC_CATALOG_CONTEXT_H_
