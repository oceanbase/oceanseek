/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_OPERATOR_H_
#define OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_OPERATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "storage/lob/common/ob_outtable_lob_control_key.h"

namespace oceanbase
{
namespace common
{
namespace sqlclient
{
class ObISQLConnection;
}
}
namespace storage
{

enum ObOuttableLobControlStatus : int64_t
{
  OUTTABLE_LOB_CONTROL_DOING = 1,
  OUTTABLE_LOB_CONTROL_FAIL = 2,
  OUTTABLE_LOB_CONTROL_COMPLETE = 3,
};

OB_INLINE bool is_valid_outtable_lob_control_status(const int64_t status)
{
  return status >= OUTTABLE_LOB_CONTROL_DOING && status <= OUTTABLE_LOB_CONTROL_COMPLETE;
}

struct ObOuttableLobControlRow
{
  ObOuttableLobControlRow()
    : key_(), status_(0), last_modify_time_(0), info_()
  {}

  void reset()
  {
    key_.reset();
    status_ = 0;
    last_modify_time_ = 0;
    info_.reset();
  }

  bool is_valid() const
  {
    return key_.is_valid()
        && is_valid_outtable_lob_control_status(status_)
        && info_.length() <= OB_MAX_VARBINARY_LENGTH;
  }

  TO_STRING_KV(K_(key), K_(status), K_(last_modify_time),
      "info_length", info_.length());

  ObOuttableLobControlKey key_;
  int64_t status_;
  int64_t last_modify_time_;
  common::ObString info_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObOuttableLobControlRow);
};

// Each tenant owns its control table. tenant_id is the SQL execution tenant.
class ObOuttableLobControlOperator final
{
public:
  enum class LockMode
  {
    NO_LOCK = 0,
    FOR_UPDATE,
    FOR_UPDATE_NOWAIT
  };

  static constexpr int64_t MAX_MACRO_ID_LENGTH = 128;
  static const char EMPTY_INFO[];

  static int insert(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObOuttableLobControlRow &row,
      int64_t &affected_rows);

  static int insert(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const common::ObIArray<ObOuttableLobControlKey> &keys,
      const int64_t status,
      int64_t &affected_rows);

  static int update(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const ObOuttableLobControlRow &row,
      int64_t &affected_rows);

  static int update(
      common::sqlclient::ObISQLConnection &sql_connection,
      const uint64_t tenant_id,
      const ObOuttableLobControlRow &row,
      int64_t &affected_rows);

  static int delete_row(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      int64_t &affected_rows);

  static int select(
      common::ObISQLClient &sql_client,
      const uint64_t tenant_id,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      const LockMode lock_mode,
      common::ObIAllocator &allocator,
      int64_t &row_scn,
      ObOuttableLobControlRow &row);

  static int select(
      common::sqlclient::ObISQLConnection &sql_connection,
      const uint64_t tenant_id,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      const LockMode lock_mode,
      common::ObIAllocator &allocator,
      int64_t &row_scn,
      ObOuttableLobControlRow &row);

  static int select_status(
      common::sqlclient::ObISQLConnection &sql_connection,
      const uint64_t tenant_id,
      const common::ObIArray<ObOuttableLobControlKey> &keys,
      const bool for_update,
      common::ObIArray<int64_t> &statuses);

  static int update_status(
      common::sqlclient::ObISQLConnection &sql_connection,
      const uint64_t tenant_id,
      const common::ObIArray<ObOuttableLobControlKey> &keys,
      const int64_t status,
      int64_t &affected_rows);

private:
  static int append_keys_condition_(
      const common::ObIArray<ObOuttableLobControlKey> &keys,
      common::ObSqlString &sql);

  static int parse_status_result_(
      common::ObISQLClient::ReadResult &read_result,
      const common::ObIArray<ObOuttableLobControlKey> &keys,
      common::ObIArray<int64_t> &statuses);

  static int build_update_sql_(
      const ObOuttableLobControlRow &row,
      common::ObSqlString &sql);

  static int build_select_sql_(
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      const LockMode lock_mode,
      common::ObSqlString &sql);

  static int parse_select_result_(
      common::ObISQLClient::ReadResult &read_result,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      common::ObIAllocator &allocator,
      int64_t &row_scn,
      ObOuttableLobControlRow &row);

  static int serialize_macro_id_(
      const blocksstable::MacroBlockId &macro_id,
      char *buf,
      const int64_t buf_len,
      common::ObString &serialized_macro_id);

  ObOuttableLobControlOperator() = delete;
  ~ObOuttableLobControlOperator() = delete;
  DISALLOW_COPY_AND_ASSIGN(ObOuttableLobControlOperator);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_OPERATOR_H_
