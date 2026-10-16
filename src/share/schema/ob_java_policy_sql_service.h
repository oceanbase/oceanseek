/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_SHARE_SCHEMA_OB_JAVA_POLICY_SQL_SERVICE_H_
#define OCEANBASE_SHARE_SCHEMA_OB_JAVA_POLICY_SQL_SERVICE_H_

#include "ob_ddl_sql_service.h"

namespace oceanbase
{
namespace share
{
namespace schema
{

class ObSimpleJavaPolicySchema;

class ObJavaPolicySqlService : public ObDDLSqlService
{
public:
  ObJavaPolicySqlService(ObSchemaService &schema_service)
    : ObDDLSqlService(schema_service) {}
  virtual ~ObJavaPolicySqlService() {}

  virtual int create_java_policy(const ObSimpleJavaPolicySchema &java_policy_schema,
                                 const common::ObString &ddl_stmt_str,
                                 common::ObISQLClient &sql_client);

  virtual int drop_java_policy(const ObSimpleJavaPolicySchema &java_policy_schema,
                               const common::ObString &ddl_stmt_str,
                               common::ObISQLClient &sql_client);

  virtual int modify_java_policy(const ObSimpleJavaPolicySchema &java_policy_schema,
                                 const common::ObString &ddl_stmt_str,
                                 common::ObISQLClient &sql_client);

private:
  DISALLOW_COPY_AND_ASSIGN(ObJavaPolicySqlService);
};

} //end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

#endif // OCEANBASE_SHARE_SCHEMA_OB_JAVA_POLICY_SQL_SERVICE_H_
