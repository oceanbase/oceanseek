/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_OB_CREATE_RLS_POLICY_RESOLVER_H_
#define OCEANBASE_SQL_OB_CREATE_RLS_POLICY_RESOLVER_H_

#include "sql/resolver/ddl/ob_ddl_resolver.h"

namespace oceanbase
{
namespace sql
{

class ObCreateRlsPolicyResolver : public ObDDLResolver
{
public:
  explicit ObCreateRlsPolicyResolver(ObResolverParams &params);
  virtual ~ObCreateRlsPolicyResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateRlsPolicyResolver);
};

}  // namespace sql
}  // namespace oceanbase

#endif // OCEANBASE_SQL_OB_CREATE_RLS_POLICY_RESOLVER_H_
