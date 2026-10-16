/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_GRANT_PROXY_RESOLVER_H
#define OB_GRANT_PROXY_RESOLVER_H

#include "sql/resolver/dcl/ob_alter_user_proxy_stmt.h"
#include "sql/resolver/dcl/ob_dcl_resolver.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace sql
{
// Resolver for MySQL-mode `GRANT/REVOKE PROXY ON client TO/FROM proxy_list`.
// It reuses ObAlterUserProxyStmt (stmt type T_ALTER_USER_PROXY) so that the
// executor dispatch and privilege check paths of CONNECT THROUGH are shared.
// MVP: no WITH ROLE clause, proxy login activates all default roles of client.
class ObGrantProxyResolver: public ObDCLResolver
{
public:
  explicit ObGrantProxyResolver(ObResolverParams &params);
  virtual ~ObGrantProxyResolver();
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_user_node(const ParseNode *user_node,
                        common::ObString &user_name,
                        common::ObString &host_name);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObGrantProxyResolver);
};

} // end namespace sql
} // end namespace oceanbase

#endif // OB_GRANT_PROXY_RESOLVER_H
