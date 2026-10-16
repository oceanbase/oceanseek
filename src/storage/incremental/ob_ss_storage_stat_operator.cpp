/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "src/storage/incremental/ob_ss_storage_stat_operator.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace share;
namespace storage
{

int ObSSStorageStatTableOperator::update_tenant_space_usage(
    const ReportType report_type,
    const int64_t report_value)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(report_type));
  } else {
    ObDMLSqlSplicer dml;
    ObDMLExecHelper exec(proxy_, gen_meta_tenant_id(tenant_id));
    int64_t affected_rows = 0;
    const char *report_type_str = NULL;
    switch (report_type) {
      case ReportType::PUBLIC_DATA:
        report_type_str = "Shared Data Bytes";
        break;
      case ReportType::PUBLIC_META:
        report_type_str = "Shared Meta Bytes";
        break;
      case ReportType::PRIVATE_ALL:
        report_type_str = "Local All Bytes";
        break;
      default:
        ret = OB_INVALID_ARGUMENT;
        STORAGE_LOG(WARN, "unexpected", K(ret), K(report_type));
        break;
    }
    if  (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
        ||  OB_FAIL(dml.add_pk_column("storage_type", report_type_str))
        ||  OB_FAIL(dml.add_pk_column("endpoint", "UNUSE"))
        ||  OB_FAIL(dml.add_pk_column("path", "UNUSE"))
        ||  OB_FAIL(dml.add_column("value", report_value))
        ||  OB_FAIL(dml.add_time_column("update_time", ObClockGenerator::getRealClock()))) {
      STORAGE_LOG(WARN, "dml fail to add column", K(ret));
    } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_TENANT_STORAGE_STAT_TNAME, dml, affected_rows))) {
      STORAGE_LOG(WARN, "fail to exec insert", K(ret));
    } else if (affected_rows < 1 || affected_rows > 2) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "affected rows unexpected", K(ret), K(affected_rows), K(tenant_id), K(report_type), K(report_value));
    } else {
      STORAGE_LOG(INFO, "insert_update successful ", K(ret), K(tenant_id), K(report_type), K(report_value));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

