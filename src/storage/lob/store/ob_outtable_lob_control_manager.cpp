/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/lob/store/ob_outtable_lob_control_manager.h"

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/utility/ob_sort.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{

ObOuttableLobControlManager::ObOuttableLobControlManager()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), sql_proxy_(nullptr)
{
}

int ObOuttableLobControlManager::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("outtable lob control manager init twice", K(ret), K(tenant_id_));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = GCTX.sql_proxy_;
    is_inited_ = true;
  }
  return ret;
}

void ObOuttableLobControlManager::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  sql_proxy_ = nullptr;
}

int ObOuttableLobControlManager::insert(const ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid() || OUTTABLE_LOB_CONTROL_DOING != row.status_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row for inserting outtable lob control", K(ret), K(row));
  } else {
    common::ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to start outtable lob control insert transaction", K(ret), K(tenant_id_));
    } else {
      if (OB_FAIL(ObOuttableLobControlOperator::insert(trans, tenant_id_, row, affected_rows))) {
        LOG_WARN("failed to insert outtable lob control row", K(ret), K(tenant_id_), K(row));
      }
      const bool commit = OB_SUCCESS == ret;
      const int end_ret = trans.end(commit);
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to end outtable lob control insert transaction",
            K(end_ret), K(commit), K(ret), K(tenant_id_));
        if (OB_SUCCESS == ret) {
          ret = end_ret;
        }
      }
    }
  }
  return ret;
}

int ObOuttableLobControlManager::insert(const common::ObIArray<ObOuttableLobControlKey> &keys)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    if (OB_UNLIKELY(!keys.at(i).is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid key for inserting lob control rows", K(ret), "key", keys.at(i));
    }
  }
  if (OB_SUCC(ret) && !keys.empty()) {
    common::ObMySQLTransaction trans;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to start lob control batch insert transaction", K(ret), K(tenant_id_));
    } else {
      if (OB_FAIL(ObOuttableLobControlOperator::insert(
          trans, tenant_id_, keys, OUTTABLE_LOB_CONTROL_DOING, affected_rows))) {
        LOG_WARN("failed to insert lob control batch", K(ret), K(tenant_id_), K(keys));
      }
      const bool commit = OB_SUCCESS == ret;
      const int end_ret = trans.end(commit);
      if (OB_SUCCESS != end_ret) {
        if (OB_SUCCESS == ret) {
          ret = end_ret;
        }
        LOG_WARN("failed to end lob control batch insert transaction",
            K(end_ret), K(commit), K(ret), K(tenant_id_));
      }
    }
  }
  return ret;
}

int ObOuttableLobControlManager::update_complete(
    common::ObMySQLTransaction &trans,
    const ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("LobCtrlUpdate");
  ObOuttableLobControlRow current_row;
  int64_t affected_rows = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  } else if (OB_UNLIKELY(!trans.is_started())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transaction is not started", K(ret), K(tenant_id_), K(row));
  } else if (OB_UNLIKELY(!row.is_valid() || OUTTABLE_LOB_CONTROL_COMPLETE != row.status_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row for completing outtable lob control", K(ret), K(row));
  } else if (OB_FAIL(select_for_update_(
      trans, row.key_.macro_id_, row.key_.hash_, allocator, current_row))) {
    LOG_WARN("failed to lock outtable lob control row", K(ret), K(tenant_id_), K(row));
  } else if (OUTTABLE_LOB_CONTROL_DOING != current_row.status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("outtable lob control status does not match",
        K(ret), K(tenant_id_), K(row), K(current_row));
  } else if (OB_FAIL(ObOuttableLobControlOperator::update(
      trans, tenant_id_, row, affected_rows))) {
    LOG_WARN("failed to update outtable lob control row", K(ret), K(tenant_id_), K(row));
  }
  return ret;
}

int ObOuttableLobControlManager::update_complete(
    common::sqlclient::ObISQLConnection &sql_connection,
    const ObOuttableLobControlKey &key)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("LobCtrlUpdate");
  ObOuttableLobControlRow current_row;
  int64_t affected_rows = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid key for completing outtable lob control", K(ret), K(key));
  } else if (OB_FAIL(select_for_update_(
      sql_connection, key.macro_id_, key.hash_, allocator, current_row))) {
    LOG_WARN("failed to lock outtable lob control row", K(ret), K(tenant_id_), K(key));
  } else if (OUTTABLE_LOB_CONTROL_DOING != current_row.status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("outtable lob control status does not match",
        K(ret), K(tenant_id_), K(key), K(current_row));
  } else if (FALSE_IT(current_row.status_ = OUTTABLE_LOB_CONTROL_COMPLETE)) {
  } else if (OB_FAIL(ObOuttableLobControlOperator::update(
      sql_connection, tenant_id_, current_row, affected_rows))) {
    LOG_WARN("failed to complete outtable lob control row",
        K(ret), K(tenant_id_), K(current_row));
  }
  return ret;
}

bool ObOuttableLobControlManager::ObOuttableLobControlKeyCompare::operator()(
    const ObOuttableLobControlKey &lhs,
    const ObOuttableLobControlKey &rhs) const
{
  return lhs.macro_id_ < rhs.macro_id_ || (lhs.macro_id_ == rhs.macro_id_ && lhs.hash_ < rhs.hash_);
}

int ObOuttableLobControlManager::update_complete(
    common::sqlclient::ObISQLConnection &sql_connection,
    const common::ObIArray<ObOuttableLobControlKey> &keys)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObOuttableLobControlKey, COMPLETE_BATCH_SIZE> sorted_keys;
  common::ObSEArray<ObOuttableLobControlKey, COMPLETE_BATCH_SIZE> batch_keys;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    if (OB_UNLIKELY(!keys.at(i).is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid key for completing lob control rows", K(ret), "key", keys.at(i));
    } else if (OB_FAIL(sorted_keys.push_back(keys.at(i)))) {
      LOG_WARN("failed to copy lob control key", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && !sorted_keys.empty()) {
    lib::ob_sort(sorted_keys.begin(), sorted_keys.end(), ObOuttableLobControlKeyCompare());
    for (int64_t i = 0; OB_SUCC(ret) && i < sorted_keys.count(); ++i) {
      const ObOuttableLobControlKey &key = sorted_keys.at(i);
      if (i > 0 && key.macro_id_ == sorted_keys.at(i - 1).macro_id_
          && key.hash_ == sorted_keys.at(i - 1).hash_) {
      } else if (OB_FAIL(batch_keys.push_back(key))) {
        LOG_WARN("failed to append lob control batch key", K(ret), K(key));
      } else if (COMPLETE_BATCH_SIZE == batch_keys.count()) {
        if (OB_FAIL(update_complete_batch_(sql_connection, batch_keys))) {
          LOG_WARN("failed to complete lob control batch", K(ret), K(tenant_id_));
        } else {
          batch_keys.reuse();
        }
      }
    }
    if (OB_SUCC(ret) && !batch_keys.empty()
        && OB_FAIL(update_complete_batch_(sql_connection, batch_keys))) {
      LOG_WARN("failed to complete last lob control batch", K(ret), K(tenant_id_));
    }
  }
  return ret;
}

int ObOuttableLobControlManager::update_complete_batch_(
    common::sqlclient::ObISQLConnection &sql_connection,
    const common::ObIArray<ObOuttableLobControlKey> &keys)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSEArray<int64_t, COMPLETE_BATCH_SIZE> statuses;
  if (OB_FAIL(ObOuttableLobControlOperator::select_status(
      sql_connection, tenant_id_, keys, true /*for_update*/, statuses))) {
    LOG_WARN("failed to lock lob control batch", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(statuses.count() != keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected lob control status count", K(ret), K(statuses.count()), K(keys.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < statuses.count(); ++i) {
    if (OUTTABLE_LOB_CONTROL_DOING != statuses.at(i)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("lob control status does not match", K(ret), "key", keys.at(i),
          "status", statuses.at(i));
    }
  }
  if (FAILEDx(ObOuttableLobControlOperator::update_status(
      sql_connection, tenant_id_, keys, OUTTABLE_LOB_CONTROL_COMPLETE, affected_rows))) {
    LOG_WARN("failed to update lob control batch", K(ret), K(tenant_id_));
  } else if (OB_SUCC(ret) && OB_UNLIKELY(affected_rows != keys.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected completed row count", K(ret), K(affected_rows), K(keys.count()));
  }
  return ret;
}

int ObOuttableLobControlManager::update_fail(const ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  } else if (OB_UNLIKELY(!row.is_valid()
      || OUTTABLE_LOB_CONTROL_FAIL != row.status_
      || row.last_modify_time_ <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid row for failing outtable lob control", K(ret), K(row));
  } else {
    common::ObMySQLTransaction trans;
    common::ObArenaAllocator allocator("LobCtrlFail");
    ObOuttableLobControlRow current_row;
    int64_t affected_rows = 0;
    if (OB_FAIL(trans.start(sql_proxy_, tenant_id_))) {
      LOG_WARN("failed to start outtable lob control fail transaction", K(ret), K(tenant_id_));
    } else {
      if (OB_FAIL(select_for_update_(
          trans, row.key_.macro_id_, row.key_.hash_, allocator, current_row))) {
        LOG_WARN("failed to lock outtable lob control row", K(ret), K(tenant_id_), K(row));
      } else if (OUTTABLE_LOB_CONTROL_DOING != current_row.status_
          || current_row.last_modify_time_ != row.last_modify_time_) {
        ret = OB_STATE_NOT_MATCH;
        LOG_INFO("outtable lob control row changed after scan",
            K(ret), K(tenant_id_), K(row), K(current_row));
      } else if (FALSE_IT(current_row.status_ = OUTTABLE_LOB_CONTROL_FAIL)) {
      } else if (OB_FAIL(ObOuttableLobControlOperator::update(
          trans, tenant_id_, current_row, affected_rows))) {
        LOG_WARN("failed to mark outtable lob control row as failed",
            K(ret), K(tenant_id_), K(current_row));
      }
      const bool commit = OB_SUCCESS == ret;
      const int end_ret = trans.end(commit);
      if (OB_SUCCESS != end_ret) {
        LOG_WARN("failed to end outtable lob control fail transaction",
            K(end_ret), K(commit), K(ret), K(tenant_id_));
        if (OB_SUCCESS == ret) {
          ret = end_ret;
        }
      }
    }
  }
  return ret;
}

int ObOuttableLobControlManager::delete_fail_row(
    common::ObMySQLTransaction &trans,
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("LobCtrlDelete");
  ObOuttableLobControlRow current_row;
  int64_t affected_rows = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  } else if (OB_UNLIKELY(!trans.is_started() || !macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for deleting outtable lob control row",
        K(ret), K(tenant_id_), K(macro_id), K(hash));
  } else if (OB_FAIL(select_for_update_(trans, macro_id, hash, allocator, current_row))) {
    LOG_WARN("failed to lock outtable lob control row",
        K(ret), K(tenant_id_), K(macro_id), K(hash));
  } else if (OUTTABLE_LOB_CONTROL_FAIL != current_row.status_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("only failed outtable lob control row can be deleted",
        K(ret), K(tenant_id_), K(current_row));
  } else if (OB_FAIL(ObOuttableLobControlOperator::delete_row(
      trans, tenant_id_, macro_id, hash, affected_rows))) {
    LOG_WARN("failed to delete outtable lob control row",
        K(ret), K(tenant_id_), K(macro_id), K(hash));
  }
  return ret;
}

int ObOuttableLobControlManager::delete_complete_row(
    common::ObMySQLTransaction &trans,
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    const int64_t expected_row_scn)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("LobCtrlDelete");
  ObOuttableLobControlRow current_row;
  int64_t current_row_scn = 0;
  int64_t affected_rows = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  } else if (OB_UNLIKELY(!trans.is_started() || !macro_id.is_valid()
      || expected_row_scn <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument for deleting complete outtable lob control row",
        K(ret), K(tenant_id_), K(macro_id), K(hash), K(expected_row_scn));
  } else if (OB_FAIL(ObOuttableLobControlOperator::select(
      trans, tenant_id_, macro_id, hash, true /*for_update*/, allocator,
      current_row_scn, current_row))) {
    LOG_WARN("failed to lock outtable lob control row",
        K(ret), K(tenant_id_), K(macro_id), K(hash));
  } else if (OUTTABLE_LOB_CONTROL_COMPLETE != current_row.status_
      || expected_row_scn != current_row_scn) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("complete outtable lob control row does not match deletion mark",
        K(ret), K(tenant_id_), K(expected_row_scn), K(current_row_scn), K(current_row));
  } else if (OB_FAIL(ObOuttableLobControlOperator::delete_row(
      trans, tenant_id_, macro_id, hash, affected_rows))) {
    LOG_WARN("failed to delete complete outtable lob control row",
        K(ret), K(tenant_id_), K(macro_id), K(hash));
  }
  return ret;
}

int ObOuttableLobControlManager::select_for_update_(
    common::ObMySQLTransaction &trans,
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    common::ObIAllocator &allocator,
    ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  int64_t row_scn = 0;
  row.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  } else if (OB_FAIL(ObOuttableLobControlOperator::select(
      trans, tenant_id_, macro_id, hash, true /*for_update*/, allocator, row_scn, row))) {
    LOG_WARN("failed to lock outtable lob control row",
        K(ret), K(tenant_id_), K(macro_id), K(hash));
  }
  return ret;
}

int ObOuttableLobControlManager::select_for_update_(
    common::sqlclient::ObISQLConnection &sql_connection,
    const blocksstable::MacroBlockId &macro_id,
    const uint64_t hash,
    common::ObIAllocator &allocator,
    ObOuttableLobControlRow &row)
{
  int ret = OB_SUCCESS;
  int64_t row_scn = 0;
  row.reset();
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("outtable lob control manager is not initialized", K(ret));
  } else if (OB_FAIL(ObOuttableLobControlOperator::select(
      sql_connection, tenant_id_, macro_id, hash, true /*for_update*/, allocator, row_scn, row))) {
    LOG_WARN("failed to lock outtable lob control row",
        K(ret), K(tenant_id_), K(macro_id), K(hash));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
