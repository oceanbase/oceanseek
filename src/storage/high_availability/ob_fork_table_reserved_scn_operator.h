/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_FORK_TABLE_RESERVED_SCN_OPERATOR_H_
#define OCEANBASE_STORAGE_OB_FORK_TABLE_RESERVED_SCN_OPERATOR_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

// status 仅用于观测，不能作为调度或释放依据。
enum class ObForkTableReservedScnStatus : int64_t
{
  DOING = 0,
  DONE = 1,
};

struct ObForkTableReservedScnRow final
{
public:
  ObForkTableReservedScnRow();
  ~ObForkTableReservedScnRow() = default;
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(ddl_task_id), K_(tablet_id), K_(meta_scn),
      "status", static_cast<int64_t>(status_));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  int64_t ddl_task_id_;
  common::ObTabletID tablet_id_;
  share::SCN meta_scn_;
  ObForkTableReservedScnStatus status_;
};

class ObForkTableReservedScnOperator final
{
public:
  ObForkTableReservedScnOperator();
  ~ObForkTableReservedScnOperator() = default;
  int init(
      common::ObISQLClient &sql_client,
      const int64_t sql_timeout_us);
  void reset();

  int batch_insert_rows(const common::ObIArray<ObForkTableReservedScnRow> &rows);

  int update_tablet_done(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t ddl_task_id,
      const common::ObTabletID &tablet_id);

  int delete_group(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t ddl_task_id);

  int scan_ls_rows(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      common::ObIArray<ObForkTableReservedScnRow> &rows);

  int get_group_any_row(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t ddl_task_id,
      ObForkTableReservedScnRow &row,
      bool &is_exist);

private:
  static int parse_row_(
      common::sqlclient::ObMySQLResult &result,
      ObForkTableReservedScnRow &row);
  int get_tablet_row_(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t ddl_task_id,
      const common::ObTabletID &tablet_id,
      ObForkTableReservedScnRow &row,
      bool &is_exist);
  int get_real_tenant_id_(
      const uint64_t tenant_id,
      uint64_t &real_tenant_id);
  int get_sql_timeout_us_(int64_t &timeout_us);
  int do_sql_write_(
      const uint64_t tenant_id,
      const char *sql,
      int64_t &affected_rows);
  int do_sql_read_(
      const uint64_t tenant_id,
      const char *sql,
      common::ObMySQLProxy::MySQLResult &result);

private:
  static const int64_t BATCH_INSERT_ROW_LIMIT = 512;
  bool is_inited_;
  common::ObISQLClient *sql_client_;
  int64_t sql_timeout_us_;
  DISALLOW_COPY_AND_ASSIGN(ObForkTableReservedScnOperator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_FORK_TABLE_RESERVED_SCN_OPERATOR_H_
