/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_DROP_RLS_POLICY_RESOLVER_H_
#define OCEANBASE_SQL_OB_DROP_RLS_POLICY_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObDropRlsPolicyResolver : public ObDDLResolver
{
public:
  explicit ObDropRlsPolicyResolver(ObResolverParams &params);
  virtual ~ObDropRlsPolicyResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDropRlsPolicyResolver);
};

}  // namespace sql
}  // namespace oceanbase

#endif // OCEANBASE_SQL_OB_DROP_RLS_POLICY_RESOLVER_H_
