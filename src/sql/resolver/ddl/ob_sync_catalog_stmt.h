/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_SYNC_CATALOG_STMT_H_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_SYNC_CATALOG_STMT_H_

#include "sql/resolver/cmd/ob_system_cmd_stmt.h"

namespace oceanbase
{
namespace sql
{

class ObSyncCatalogStmt : public ObSystemCmdStmt
{
public:
  enum SyncScope
  {
    INVALID_SCOPE = 0,
    TABLE_SCOPE = 1,
    DATABASE_SCOPE = 2,
    CATALOG_SCOPE = 3
  };

  explicit ObSyncCatalogStmt(common::ObIAllocator *name_pool);
  ObSyncCatalogStmt();
  virtual ~ObSyncCatalogStmt() {}

  virtual bool cause_implicit_commit() const { return true; }
  void set_sync_scope(const SyncScope sync_scope) { sync_scope_ = sync_scope; }
  SyncScope get_sync_scope() const { return sync_scope_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_catalog_id(const uint64_t catalog_id)
  {
    catalog_id_ = catalog_id;
  }
  uint64_t get_catalog_id() const
  {
    return catalog_id_;
  }
  void set_catalog_name(const common::ObString &catalog_name)
  {
    catalog_name_ = catalog_name;
  }
  const common::ObString &get_catalog_name() const { return catalog_name_; }
  void set_database_id(const uint64_t database_id) { database_id_ = database_id; }
  uint64_t get_database_id() const { return database_id_; }
  void set_table_id(const uint64_t table_id) { table_id_ = table_id; }
  uint64_t get_table_id() const { return table_id_; }
  void set_database_name(const common::ObString &database_name)
  {
    database_name_ = database_name;
  }
  const common::ObString &get_database_name() const { return database_name_; }
  void set_table_name(const common::ObString &table_name) { table_name_ = table_name; }
  const common::ObString &get_table_name() const { return table_name_; }

  TO_STRING_KV(K_(stmt_type),
               K_(sync_scope),
               K_(tenant_id),
               K_(catalog_id),
               K_(catalog_name),
               K_(database_id),
               K_(table_id),
               K_(database_name),
               K_(table_name));

private:
  SyncScope sync_scope_;
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  common::ObString catalog_name_;
  uint64_t database_id_;
  uint64_t table_id_;
  common::ObString database_name_;
  common::ObString table_name_;
  DISALLOW_COPY_AND_ASSIGN(ObSyncCatalogStmt);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_SYNC_CATALOG_STMT_H_
