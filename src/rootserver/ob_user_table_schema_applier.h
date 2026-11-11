/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_ROOTSERVER_OB_USER_TABLE_SCHEMA_APPLIER_H_
#define OCEANBASE_ROOTSERVER_OB_USER_TABLE_SCHEMA_APPLIER_H_

#include "lib/container/ob_iarray.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
class ObTimeoutCtx;
}
namespace share
{
struct ObUserTableBuiltinLocalIndexEntry;
struct ObUserTableBuiltinSchemaEntry;
namespace schema
{
class ObSchemaGetterGuard;
class ObMultiVersionSchemaService;
class ObTableSchema;
}
}
namespace obrpc
{
struct ObCreateIndexArg;
}
namespace rootserver
{

class ObUserTableSchemaApplier
{
public:
  ObUserTableSchemaApplier();
  ~ObUserTableSchemaApplier();

  int init(share::schema::ObMultiVersionSchemaService &schema_service,
           common::ObMySQLProxy &sql_proxy,
           const common::ObTimeoutCtx &timeout_ctx);
  int apply_builtin_user_table_schemas(const uint64_t tenant_id,
                                       const bool force_local = false);

private:
  int check_inner_stat_() const;
  int get_database_id_(share::schema::ObSchemaGetterGuard &guard,
                       const uint64_t tenant_id,
                       const common::ObString &database_name,
                       uint64_t &database_id) const;
  int build_builtin_table_schema_(const uint64_t tenant_id,
                                  const uint64_t database_id,
                                  const share::ObUserTableBuiltinSchemaEntry &entry,
                                  share::schema::ObTableSchema &table_schema) const;
  int check_table_exists_(share::schema::ObSchemaGetterGuard &guard,
                          const uint64_t tenant_id,
                          const uint64_t database_id,
                          const common::ObString &table_name,
                          bool &exists,
                          const share::schema::ObTableSchema *&exist_schema) const;
  int check_and_get_column_diff_(const share::schema::ObTableSchema &exist_schema,
                                 const share::schema::ObTableSchema &expect_schema,
                                 common::ObIArray<common::ObString> &add_columns,
                                 common::ObIArray<common::ObString> &modify_columns) const;
  int check_outtable_lob_control_schema_(
      const share::schema::ObTableSchema &exist_schema,
      const share::schema::ObTableSchema &expect_schema) const;
  int generate_alter_table_ddl_(const common::ObString &database_name,
                                const common::ObString &table_name,
                                const share::schema::ObTableSchema &expect_schema,
                                const common::ObIArray<common::ObString> &add_columns,
                                const common::ObIArray<common::ObString> &modify_columns,
                                common::ObSqlString &ddl_sql) const;
  int execute_parallel_create_table_(const uint64_t tenant_id,
                                     const common::ObString &database_name,
                                     const share::schema::ObTableSchema &table_schema,
                                     const bool force_local);
  int build_builtin_local_index_arg_(
      const uint64_t tenant_id,
      const common::ObString &database_name,
      const share::schema::ObTableSchema &table_schema,
      const share::ObUserTableBuiltinLocalIndexEntry &entry,
      obrpc::ObCreateIndexArg &arg) const;
  int check_builtin_local_index_(
      share::schema::ObSchemaGetterGuard &guard,
      const share::schema::ObTableSchema &table_schema,
      const share::ObUserTableBuiltinLocalIndexEntry &entry,
      bool &exists) const;
  int check_builtin_local_indexes_(
      const uint64_t tenant_id,
      share::schema::ObSchemaGetterGuard &guard,
      const share::schema::ObTableSchema &table_schema) const;
  int execute_ddl_sql_(const uint64_t tenant_id, const common::ObString &ddl_sql);
  int apply_one_builtin_table_(const uint64_t tenant_id,
                               share::schema::ObSchemaGetterGuard &guard,
                               const share::ObUserTableBuiltinSchemaEntry &entry,
                               const bool force_local);
  bool should_apply_builtin_table_(
      const uint64_t tenant_id,
      const share::ObUserTableBuiltinSchemaEntry &entry) const;

private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  common::ObMySQLProxy *sql_proxy_;
  const common::ObTimeoutCtx *timeout_ctx_;
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_USER_TABLE_SCHEMA_APPLIER_H_
