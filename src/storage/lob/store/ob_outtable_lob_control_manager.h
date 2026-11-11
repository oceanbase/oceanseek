/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_MANAGER_H_
#define OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_MANAGER_H_

#include "storage/lob/store/ob_outtable_lob_control_operator.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace storage
{

class ObOuttableLobControlManager final
{
public:
  ObOuttableLobControlManager();
  ~ObOuttableLobControlManager() = default;

  int init(const uint64_t tenant_id);
  void reset();

  int insert(const ObOuttableLobControlRow &row);
  int insert(const common::ObIArray<ObOuttableLobControlKey> &keys);

  int update_complete(
      common::ObMySQLTransaction &trans,
      const ObOuttableLobControlRow &row);

  int update_complete(
      common::sqlclient::ObISQLConnection &sql_connection,
      const ObOuttableLobControlKey &key);

  int update_complete(
      common::sqlclient::ObISQLConnection &sql_connection,
      const common::ObIArray<ObOuttableLobControlKey> &keys);

  int update_fail(const ObOuttableLobControlRow &row);

  int delete_fail_row(
      common::ObMySQLTransaction &trans,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash);

  int delete_complete_row(
      common::ObMySQLTransaction &trans,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      const int64_t expected_row_scn);

private:
  static constexpr int64_t COMPLETE_BATCH_SIZE = 64;

  struct ObOuttableLobControlKeyCompare
  {
    bool operator()(const ObOuttableLobControlKey &lhs, const ObOuttableLobControlKey &rhs) const;
  };

  int update_complete_batch_(
      common::sqlclient::ObISQLConnection &sql_connection,
      const common::ObIArray<ObOuttableLobControlKey> &keys);

  int select_for_update_(
      common::ObMySQLTransaction &trans,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      common::ObIAllocator &allocator,
      ObOuttableLobControlRow &row);

  int select_for_update_(
      common::sqlclient::ObISQLConnection &sql_connection,
      const blocksstable::MacroBlockId &macro_id,
      const uint64_t hash,
      common::ObIAllocator &allocator,
      ObOuttableLobControlRow &row);

  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObOuttableLobControlManager);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_MANAGER_H_
