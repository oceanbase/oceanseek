/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_ROOTSERVER_OB_FORK_TABLE_TABLET_TASK_OPERATOR_H_
#define OCEANBASE_ROOTSERVER_OB_FORK_TABLE_TABLET_TASK_OPERATOR_H_

#include "lib/container/ob_iarray.h"
#include "common/ob_tablet_id.h"
#include "lib/ob_errno.h"

namespace oceanbase {
namespace common {
class ObISQLClient;
namespace sqlclient {
class ObMySQLResult;
}
}

namespace rootserver {

enum class ObForkTableTabletTaskStatus : int64_t {
  INIT = 0,
  SUCCESS = 1,
  FAILED = 2,
  MAX
};

struct ObForkTableTabletTaskInfo final
{
public:
  ObForkTableTabletTaskInfo() { reset(); }
  ~ObForkTableTabletTaskInfo() {}
  void reset();
  bool is_valid() const
  {
    return task_id_ != OB_INVALID_ID && tenant_id_ != OB_INVALID_TENANT_ID
           && src_tablet_id_.is_valid() && dest_tablet_id_.is_valid();
  }
  TO_STRING_KV(K_(task_id), K_(tenant_id), K_(src_tablet_id),
               K_(dest_tablet_id), K_(status), K_(ret_code));
public:
  int64_t task_id_;
  uint64_t tenant_id_;
  common::ObTabletID src_tablet_id_;
  common::ObTabletID dest_tablet_id_;
  int64_t status_;
  int ret_code_;
};

class ObForkTableTabletTaskOperator final
{
public:
  ObForkTableTabletTaskOperator() {}
  ~ObForkTableTabletTaskOperator() {}

  static int batch_insert(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const common::ObIArray<ObForkTableTabletTaskInfo> &infos);

  static int get_all_by_task_id(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      common::ObIArray<ObForkTableTabletTaskInfo> &infos);

  static int get_by_dest_tablet_id(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const common::ObTabletID &dest_tablet_id,
      ObForkTableTabletTaskInfo &info);

  // CAS 语义：仅当行的当前 status 等于 expected_old_status 时才更新。
  // 用于挡住陈旧/并发回包对非期望状态（如已 SUCCESS 终态）行的覆盖。
  static int update_status(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      const common::ObTabletID &dest_tablet_id,
      const ObForkTableTabletTaskStatus expected_old_status,
      const ObForkTableTabletTaskStatus new_status,
      const int new_ret_code);

  static int get_status_summary(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t task_id,
      int64_t &total_cnt,
      int64_t &success_cnt,
      int64_t &fail_cnt);

  static int archive_and_remove_by_task_id(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t task_id);

  static int batch_delete_expired_history(
      common::ObISQLClient &sql_proxy,
      const uint64_t tenant_id,
      const int64_t expire_ts_us,
      const int64_t limit,
      int64_t &affected_rows);

  static int clean_expired_history(common::ObISQLClient &sql_proxy);

  static constexpr int64_t DEFAULT_BATCH_SIZE = 200;
  static constexpr int64_t DEFAULT_DELETE_EXPIRED_HISTORY_LIMIT = 1000;
  static constexpr int64_t DEFAULT_HISTORY_EXPIRATION_US = 7L * 24L * 60L * 60L * 1000L * 1000L; // 7d

private:
  static int fill_info_from_result_(
      common::sqlclient::ObMySQLResult &result,
      const uint64_t exec_tenant_id,
      ObForkTableTabletTaskInfo &info);
};

} // namespace rootserver
} // namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_FORK_TABLE_TABLET_TASK_OPERATOR_H_
