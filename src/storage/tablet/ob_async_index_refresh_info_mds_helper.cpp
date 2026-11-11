/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX MDS
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"
#include "storage/tablet/ob_async_index_refresh_info_replay_executor.h"
#include "storage/multi_data_source/ob_tablet_mds_info_cache_ctx.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/tablelock/ob_lock_inner_connection_util.h"
#include "storage/tx/ob_multi_data_source.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "observer/ob_inner_sql_connection.h"
#include "share/config/ob_server_config.h"
#include "share/ob_share_util.h"
#include "share/schema/ob_table_schema.h"
#include "storage/tablet/ob_tablet.h"

using namespace oceanbase::storage::mds;

namespace oceanbase
{
namespace storage
{

bool ObTabletAsyncIndexRefreshInfoArg::is_valid() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid() && info_.is_valid()
      && expected_published_version_ >= 0
      && (0 == expected_published_version_
          || (!floor_pin_only_ && !info_.has_async_index() && info_.need_fill_commit_version()));
}

OB_SERIALIZE_MEMBER(ObTabletAsyncIndexRefreshInfoArg, ls_id_, tablet_id_, info_, floor_pin_only_, expected_published_version_);

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_membership(
    const share::schema::ObTableSchema &data_table_schema,
    common::ObMySQLTransaction &trans,
    const int64_t initial_snapshot)
{
  return register_membership_(data_table_schema, trans, initial_snapshot);
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_membership_(
    const share::schema::ObTableSchema &data_table_schema,
    common::ObMySQLTransaction &trans,
    const int64_t initial_snapshot)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObTabletID> tablet_ids;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  const bool has_async_index = data_table_schema.has_async_index();
  refresh_info.set_has_async_index(has_async_index);
  if (has_async_index && 0 == initial_snapshot && !data_table_schema.is_user_hidden_table()) {
    refresh_info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
  }
  if (OB_UNLIKELY(!data_table_schema.is_valid() || initial_snapshot < 0
                  || (initial_snapshot > 0 && !has_async_index))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data table schema for async-index membership", K(ret),
             K(data_table_schema));
  } else if (initial_snapshot > 0 && OB_FAIL(refresh_info.append_refresh_snapshot(initial_snapshot))) {
    LOG_WARN("failed to pin initial build snapshot", K(ret), K(initial_snapshot));
  } else if (OB_FAIL(refresh_info.init_pending_refresh_snapshot())) {
    LOG_WARN("failed to init pending refresh snapshot for async-index membership",
             K(ret), "table_id", data_table_schema.get_table_id());
  } else if (OB_FAIL(data_table_schema.get_tablet_ids(tablet_ids))) {
    LOG_WARN("failed to get data tablet ids for async-index membership", K(ret),
             K(data_table_schema));
  } else if (OB_FAIL(register_tablets_refresh_info_(
                 data_table_schema.get_tenant_id(), tablet_ids,
                 refresh_info, trans))) {
    LOG_WARN("failed to register data-table async-index membership", K(ret),
             "table_id", data_table_schema.get_table_id(), K(tablet_ids),
             K(has_async_index));
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_initial_published_versions(
    const uint64_t tenant_id,
    const common::ObIArray<share::schema::ObTableSchema> &table_schemas,
    const int64_t snapshot_version,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObTabletID> tablet_ids;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || table_schemas.empty()
                  || (snapshot_version != ObTabletAsyncIndexRefreshInfo::USE_COMMIT_VERSION && snapshot_version <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args for async-index initial published versions",
             K(ret), K(tenant_id), "schema_count", table_schemas.count(), K(snapshot_version));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
    const share::schema::ObTableSchema &table_schema = table_schemas.at(i);
    if (OB_UNLIKELY(!table_schema.is_valid() || table_schema.get_tenant_id() != tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid schema for async-index initial published version",
               K(ret), K(tenant_id), K(i), K(table_schema));
    } else if (share::schema::ObTableStateFlag::TABLE_STATE_NORMAL == table_schema.get_table_state_flag()
               && share::schema::is_async_index_refresh_target(table_schema.get_index_type())) {
      common::ObArray<common::ObTabletID> this_tablet_ids;
      if (OB_FAIL(table_schema.get_tablet_ids(this_tablet_ids))) {
        LOG_WARN("failed to get async-index target tablet ids",
                 K(ret), K(i), "table_id", table_schema.get_table_id());
      } else if (OB_UNLIKELY(this_tablet_ids.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async-index target has no tablet",
                 K(ret), K(i), "table_id", table_schema.get_table_id());
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < this_tablet_ids.count(); ++j) {
        if (OB_FAIL(tablet_ids.push_back(this_tablet_ids.at(j)))) {
          LOG_WARN("failed to collect async-index target tablet", K(ret),
                   K(i), K(j), "tablet_id", this_tablet_ids.at(j));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(tablet_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no async-index target found for initial publication",
             K(ret), K(tenant_id), K(snapshot_version), "schema_count", table_schemas.count());
  } else {
    refresh_info.set_published_version(snapshot_version);
    if (OB_FAIL(register_tablets_refresh_info_(tenant_id, tablet_ids, refresh_info, trans))) {
      LOG_WARN("failed to register initial async-index published versions",
               K(ret), K(tenant_id), K(snapshot_version), K(tablet_ids));
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_offline_publication(
    const share::schema::ObTableSchema &data_table_schema,
    const common::ObIArray<share::schema::ObTableSchema> &table_schemas,
    const int64_t base_snapshot,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> index_tablets;
  ObArray<ObTabletID> data_tablets;
  ObArray<ObTabletID> tablets;
  ObTabletAsyncIndexRefreshInfo info;
  info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_VERSION);
  const uint64_t tenant_id = data_table_schema.get_tenant_id();
  for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
    const share::schema::ObTableSchema &schema = table_schemas.at(i);
    if (share::schema::TABLE_STATE_NORMAL == schema.get_table_state_flag()
        && share::schema::is_async_index_refresh_target(schema.get_index_type())) {
      tablets.reuse();
      if (OB_FAIL(schema.get_tablet_ids(tablets))) {
        LOG_WARN("failed to collect offline index tablets", K(ret));
      } else if (OB_FAIL(append(index_tablets, tablets))) {
        LOG_WARN("failed to append offline index tablets", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (index_tablets.empty() || base_snapshot <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(register_tablets_refresh_info_(tenant_id, index_tablets, info, trans, base_snapshot))) {
    LOG_WARN("failed to register offline index publication", K(ret));
  } else if (OB_FAIL(data_table_schema.get_tablet_ids(data_tablets))) {
    LOG_WARN("failed to collect offline data tablets", K(ret));
  } else if (OB_FAIL(info.init_offline_refresh_snapshots(base_snapshot))) {
    LOG_WARN("failed to initialize offline checkpoints", K(ret));
  } else if (OB_FAIL(register_tablets_refresh_info_(tenant_id, data_tablets, info, trans))) {
    LOG_WARN("failed to register offline checkpoints", K(ret));
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_published_version(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t published_version,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid()
      || !tablet_id.is_valid()
      || published_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async-index published version arguments", K(ret),
             K(tenant_id), K(ls_id), K(tablet_id), K(published_version));
  } else {
    refresh_info.set_published_version(published_version);
    common::ObArenaAllocator allocator("AsyncIdxMds");
    if (OB_FAIL(register_refresh_info_(
            tenant_id, ls_id, tablet_id, refresh_info, trans, allocator,
            true /*need_flush_redo_instantly*/))) {
      LOG_WARN("failed to register async-index published version MDS", K(ret),
               K(tenant_id), K(ls_id), K(tablet_id), K(published_version));
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::pin_one_data_tablet(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t floor_pin,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || !ls_id.is_valid()
      || !tablet_id.is_valid()
      || floor_pin <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid maintain data tablet pin arguments", K(ret),
             K(tenant_id), K(ls_id), K(tablet_id), K(floor_pin));
  } else {
    refresh_info.set_has_async_index(true);
    refresh_info.set_published_version(ObTabletAsyncIndexRefreshInfo::INVALID_PUBLISHED_VERSION);
    if (OB_FAIL(refresh_info.append_refresh_snapshot(floor_pin))) {
      LOG_WARN("failed to build floor-pin refresh info", K(ret), K(tenant_id), K(ls_id), K(tablet_id), K(floor_pin));
    } else {
      common::ObArenaAllocator allocator("AsyncIdxPin");
      if (OB_FAIL(register_refresh_info_(
              tenant_id, ls_id, tablet_id, refresh_info, trans, allocator,
              true /*need_flush_redo_instantly*/, true /*floor_pin_only*/))) {
        LOG_WARN("failed to maintain data tablet GC pin", K(ret),
                 K(tenant_id), K(ls_id), K(tablet_id), K(floor_pin));
      }
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::pin_data_tablets(
    const uint64_t tenant_id,
    const share::schema::ObTableSchema &data_table_schema,
    const int64_t floor_pin,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  common::ObArray<common::ObTabletID> data_tablet_ids;
  common::ObArray<std::pair<share::ObLSID, common::ObTabletID>> data_ls_tablets;
  common::ObSEArray<common::ObTabletID, 8> lock_tablet_ids;
  common::ObTimeoutCtx timeout_ctx;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || !data_table_schema.is_valid()
      || floor_pin <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments for data tablet pins maintenance",
             K(ret), K(tenant_id), "table_id", data_table_schema.get_table_id(), K(floor_pin));
  } else if (OB_FAIL(data_table_schema.get_tablet_ids(data_tablet_ids))) {
    LOG_WARN("failed to get data tablet ids for early pin",
             K(ret), "table_id", data_table_schema.get_table_id());
  } else if (OB_FAIL(ObTabletBindingMdsHelper::get_sorted_ls_tablets(tenant_id, data_tablet_ids, data_ls_tablets, trans))) {
    LOG_WARN("failed to resolve data tablet ls-tablet pairs for early pin",
             K(ret), K(tenant_id), K(data_tablet_ids));
  } else if (OB_ISNULL(trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(timeout_ctx, GCONF.rpc_timeout))) {
    LOG_WARN("failed to set floor pin lock timeout", K(ret));
  }
  // Preserve (ls_id, tablet_id) order and acquire all locks before read/merge.
  for (int64_t i = 0; OB_SUCC(ret) && i < data_ls_tablets.count(); ++i) {
    if (OB_FAIL(lock_tablet_ids.push_back(data_ls_tablets.at(i).second))) {
      LOG_WARN("failed to collect floor pin lock tablet", K(ret), K(i), K(data_ls_tablets));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(lock_async_index_refresh_tablets_(
          tenant_id, lock_tablet_ids, timeout_ctx, trans))) {
    LOG_WARN("failed to lock data tablets for floor pin", K(ret), K(lock_tablet_ids));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < data_ls_tablets.count(); ++i) {
    if (OB_FAIL(pin_one_data_tablet(tenant_id, data_ls_tablets.at(i).first, data_ls_tablets.at(i).second, floor_pin, trans))) {
      LOG_WARN("failed to write early floor-pin to data tablet",
               K(ret), K(i), K(floor_pin), "ls_id", data_ls_tablets.at(i).first, "tablet_id", data_ls_tablets.at(i).second);
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_refresh_info_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const ObTabletAsyncIndexRefreshInfo &refresh_info,
    common::ObMySQLTransaction &trans,
    common::ObIAllocator &allocator,
    const bool need_flush_redo_instantly,
    const bool floor_pin_only,
    const int64_t expected_published_version)
{
  int ret = OB_SUCCESS;
  ObTabletAsyncIndexRefreshInfoArg arg;
  observer::ObInnerSQLConnection *conn = nullptr;
  transaction::ObRegisterMdsFlag flag;
  char *buf = nullptr;
  int64_t pos = 0;
  flag.need_flush_redo_instantly_ = need_flush_redo_instantly;
  flag.mds_base_scn_.reset();
  arg.ls_id_ = ls_id;
  arg.tablet_id_ = tablet_id;
  arg.floor_pin_only_ = floor_pin_only;
  arg.expected_published_version_ = expected_published_version;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !arg.ls_id_.is_valid()
                  || !arg.tablet_id_.is_valid()
                  || !refresh_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async-index refresh-info MDS arguments", K(ret),
             K(tenant_id), K(arg), K(refresh_info));
  } else if (OB_FAIL(arg.info_.assign(refresh_info))) {
    LOG_WARN("failed to assign async-index refresh info", K(ret), K(refresh_info));
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(
                           trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid transaction connection", K(ret), K(tenant_id), K(arg));
  } else {
    const int64_t buf_len = arg.get_serialize_size();
    if (OB_UNLIKELY(!arg.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid async-index refresh-info MDS argument", K(ret), K(arg));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate async-index refresh-info buffer",
               K(ret), K(buf_len), K(arg));
    } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize async-index refresh info", K(ret), K(arg));
    } else if (OB_FAIL(conn->register_multi_data_source(
                   tenant_id,
                   arg.ls_id_,
                   transaction::ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
                   buf,
                   pos,
                   flag))) {
      LOG_WARN("failed to register async-index refresh-info MDS", K(ret),
               K(tenant_id), K(arg));
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::lock_async_index_refresh_tablets_(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    const common::ObTimeoutCtx &timeout_ctx,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = nullptr;
  transaction::tablelock::ObLockObjsRequest lock_arg;
  if (tablet_ids.empty()) {
    // No tablets to lock.
  } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid transaction connection", K(ret), K(tenant_id));
  } else if (OB_FAIL(lock_arg.owner_id_.convert_from_value(
                 transaction::tablelock::ObLockOwnerType::DEFAULT_OWNER_TYPE,
                 get_tid_cache()))) {
    LOG_WARN("failed to build async-index refresh lock owner", K(ret));
  } else {
    lock_arg.lock_mode_ = transaction::tablelock::EXCLUSIVE;
    lock_arg.op_type_ = transaction::tablelock::ObTableLockOpType::IN_TRANS_COMMON_LOCK;
    // All refresh objects route to the lock-service LS, irrespective of the
    // data LS used for MDS registration. Preserve the caller's sorted order.
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      transaction::tablelock::ObLockID lock_id;
      if (OB_FAIL(lock_id.set(
              transaction::tablelock::ObLockOBJType::OBJ_TYPE_ASYNC_INDEX_REFRESH,
              tablet_ids.at(i).id()))) {
        LOG_WARN("failed to build async-index refresh lock id", K(ret), K(i), K(tablet_ids));
      } else if (OB_FAIL(lock_arg.objs_.push_back(lock_id))) {
        LOG_WARN("failed to append async-index refresh lock id", K(ret), K(lock_id));
      }
    }
    if (OB_SUCC(ret)) {
      lock_arg.timeout_us_ = timeout_ctx.get_timeout();
      if (OB_UNLIKELY(lock_arg.timeout_us_ <= 0)) {
        ret = OB_TIMEOUT;
        LOG_WARN("async-index refresh lock timed out", K(ret), K(tenant_id), K(lock_arg));
      } else if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_obj(
                     tenant_id, lock_arg, conn))) {
        LOG_WARN("failed to lock async-index refresh tablets", K(ret), K(tenant_id), K(lock_arg));
      }
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_tablets_refresh_info_(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    const ObTabletAsyncIndexRefreshInfo &refresh_info,
    common::ObMySQLTransaction &trans,
    const int64_t expected_published_version)
{
  int ret = OB_SUCCESS;
  common::ObArray<std::pair<share::ObLSID, common::ObTabletID>> ls_tablets;
  common::ObSEArray<common::ObTabletID, 8> lock_tablet_ids;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || tablet_ids.empty()
                  || !refresh_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async-index refresh-info arguments", K(ret), K(tenant_id),
             K(tablet_ids), K(refresh_info));
  } else if (OB_ISNULL(trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid transaction connection", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObTabletBindingMdsHelper::get_sorted_ls_tablets(
                 tenant_id, tablet_ids, ls_tablets, trans))) {
    LOG_WARN("failed to resolve tablets to log streams", K(ret), K(tenant_id),
             K(tablet_ids));
  } else {
    common::ObTimeoutCtx timeout_ctx;
    common::ObArenaAllocator allocator("AsyncIdxMship");
    if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(
            timeout_ctx, GCONF.rpc_timeout))) {
      LOG_WARN("failed to set async-index membership lock timeout", K(ret));
    }

    // Keep the order from get_sorted_ls_tablets(); lock the whole batch before MDS.
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); ++i) {
      if (OB_FAIL(lock_tablet_ids.push_back(ls_tablets.at(i).second))) {
        LOG_WARN("failed to collect async-index membership lock tablet", K(ret), K(i), K(ls_tablets));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(lock_async_index_refresh_tablets_(
            tenant_id, lock_tablet_ids, timeout_ctx, trans))) {
      LOG_WARN("failed to lock async-index membership", K(ret), K(tenant_id), K(lock_tablet_ids));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); ++i) {
      allocator.reuse();
      if (OB_FAIL(register_refresh_info_(
              tenant_id, ls_tablets.at(i).first, ls_tablets.at(i).second,
              refresh_info, trans, allocator,
              false /*need_flush_redo_instantly*/, false /*floor_pin_only*/, expected_published_version))) {
        LOG_WARN("failed to register async-index refresh info", K(ret),
                 K(tenant_id), K(i), K(ls_tablets), K(refresh_info));
      }
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_ls_tablets_membership_(
    const uint64_t tenant_id,
    const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ls_tablets.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async-index membership arguments", K(ret), K(tenant_id),
             K(ls_tablets));
  } else {
    refresh_info.set_has_async_index(true);
    refresh_info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_REFRESH_SNAPSHOT);
    if (OB_FAIL(refresh_info.init_pending_refresh_snapshot())) {
      LOG_WARN("failed to init pending refresh snapshot for async-index membership", K(ret), K(refresh_info));
    } else if (OB_FAIL(register_ls_tablets_refresh_info_(tenant_id, ls_tablets, refresh_info, trans, "AsyncIdxMship"))) {
      LOG_WARN("failed to register async-index membership", K(ret), K(tenant_id), K(ls_tablets), K(refresh_info));
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_ls_tablets_refresh_info_(
    const uint64_t tenant_id,
    const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
    const ObTabletAsyncIndexRefreshInfo &refresh_info,
    common::ObMySQLTransaction &trans,
    const char *allocator_label)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObTabletID, 8> lock_tablet_ids;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ls_tablets.empty()
                  || !refresh_info.is_valid() || OB_ISNULL(allocator_label))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls-tablet refresh info arguments", K(ret), K(tenant_id),
             K(ls_tablets), K(refresh_info), KP(allocator_label));
  } else if (OB_ISNULL(trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid transaction connection", K(ret), K(tenant_id));
  } else {
    common::ObTimeoutCtx timeout_ctx;
    common::ObArenaAllocator allocator(allocator_label);
    if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(
            timeout_ctx, GCONF.rpc_timeout))) {
      LOG_WARN("failed to set ls-tablet refresh info lock timeout", K(ret));
    }
    // ls_tablets is sorted by (ls_id, tablet_id); keep that order for locking.
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); ++i) {
      if (OB_FAIL(lock_tablet_ids.push_back(ls_tablets.at(i).tablet_id_))) {
        LOG_WARN("failed to collect async-index membership lock tablet", K(ret), K(i), K(ls_tablets));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(lock_async_index_refresh_tablets_(
            tenant_id, lock_tablet_ids, timeout_ctx, trans))) {
      LOG_WARN("failed to lock async-index refresh info", K(ret), K(tenant_id), K(lock_tablet_ids));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); ++i) {
      allocator.reuse();
      if (OB_FAIL(register_refresh_info_(
              tenant_id, ls_tablets.at(i).ls_id_, ls_tablets.at(i).tablet_id_,
              refresh_info, trans, allocator,
              false /*need_flush_redo_instantly*/))) {
        LOG_WARN("failed to register ls-tablet refresh info", K(ret),
                 K(tenant_id), K(i), K(ls_tablets), K(refresh_info));
      }
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_ls_tablets_membership(
    const uint64_t tenant_id,
    const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  common::ObArray<obrpc::ObLSTabletPair> sorted_ls_tablets;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ls_tablets.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async-index membership arguments", K(ret), K(tenant_id), K(ls_tablets));
  } else if (OB_FAIL(sorted_ls_tablets.assign(ls_tablets))) {
    LOG_WARN("failed to assign async-index membership ls tablets", K(ret), K(tenant_id), K(ls_tablets));
  } else {
    lib::ob_sort(sorted_ls_tablets.begin(), sorted_ls_tablets.end());
    if (OB_FAIL(register_ls_tablets_membership_(tenant_id, sorted_ls_tablets, trans))) {
      LOG_WARN("failed to register async-index membership for ls tablets", K(ret), K(tenant_id), K(sorted_ls_tablets));
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_ls_tablets_initial_published_versions(
    const uint64_t tenant_id,
    const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  common::ObArray<obrpc::ObLSTabletPair> sorted_ls_tablets;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ls_tablets.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async-index initial published versions arguments", K(ret), K(tenant_id), K(ls_tablets));
  } else if (OB_FAIL(sorted_ls_tablets.assign(ls_tablets))) {
    LOG_WARN("failed to assign ls tablets for initial published versions", K(ret), K(tenant_id), K(ls_tablets));
  } else {
    lib::ob_sort(sorted_ls_tablets.begin(), sorted_ls_tablets.end());
    if (OB_FAIL(register_ls_tablets_initial_published_versions_(tenant_id, sorted_ls_tablets, trans))) {
      LOG_WARN("failed to register initial published versions for ls tablets", K(ret), K(tenant_id), K(sorted_ls_tablets));
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::register_ls_tablets_initial_published_versions_(
    const uint64_t tenant_id,
    const common::ObIArray<obrpc::ObLSTabletPair> &ls_tablets,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObTabletAsyncIndexRefreshInfo refresh_info;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || ls_tablets.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async-index initial published versions arguments", K(ret), K(tenant_id), K(ls_tablets));
  } else {
    refresh_info.set_published_version(ObTabletAsyncIndexRefreshInfo::USE_COMMIT_VERSION);
    if (OB_FAIL(register_ls_tablets_refresh_info_(tenant_id, ls_tablets, refresh_info, trans, "AsyncIdxPub"))) {
      LOG_WARN("failed to register initial published versions", K(ret), K(tenant_id), K(ls_tablets));
    }
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx)
{
  return process(buf, len, share::SCN(), ctx, false);
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::on_replay(const char *buf, const int64_t len, const share::SCN scn, mds::BufferCtx &ctx)
{
  return process(buf, len, scn, ctx, true);
}

int ObTabletAsyncIndexRefreshInfoMdsHelper::process(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx, bool for_replay)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletAsyncIndexRefreshInfoArg arg;
  if (OB_UNLIKELY(nullptr == buf || len <= 0 || (for_replay && !scn.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(len), K(for_replay), K(scn));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("fail to deserialize ObTabletAsyncIndexRefreshInfoArg", K(ret));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("arg is invalid", K(ret), K(arg));
  } else {
    ObLS *ls = nullptr;
    ObLSHandle ls_handle;
    ObLSService *ls_service = MTL(ObLSService *);
    ObTabletMdsInfoCacheCtx &user_ctx = static_cast<ObTabletMdsInfoCacheCtx &>(ctx);
    user_ctx.set_target(ObMdsInfoCacheTarget::ASYNC_INDEX_MEMBERSHIP);
    user_ctx.set_ls_id(arg.ls_id_);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is nullptr", K(ret));
    } else if (OB_FAIL(ls_service->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
      LOG_WARN("fail to get ls", K(ret), K(arg.ls_id_));
    } else if (OB_UNLIKELY(!ls_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls handle is invalid", K(ret));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is nullptr", K(ret));
    } else if (!for_replay) {
      bool should_write = true;
      if (arg.expected_published_version_ > 0) {
        ObTabletHandle handle;
        ObTabletAsyncIndexRefreshInfo committed_info;
        if (OB_FAIL(ls->get_tablet(arg.tablet_id_, handle))) {
          LOG_WARN("failed to get offline publication target", K(ret), K(arg.tablet_id_));
        } else if (OB_FAIL(handle.get_obj()->get_latest_committed_async_index_refresh_info(committed_info))) {
          if (OB_EMPTY_RESULT == ret) {
            ret = OB_EAGAIN;
          }
          LOG_WARN("failed to verify offline logical publication", K(ret), K(arg.tablet_id_));
        } else if (!committed_info.has_published_version()
                   || committed_info.get_published_version() != arg.expected_published_version_) {
          // Refresh commits its data and logical watermark atomically. Empty
          // refreshes advance this watermark without installing a physical INC.
          ret = OB_EAGAIN;
          LOG_WARN("offline index has not reached the common logical boundary", K(ret),
                   K(committed_info), K(arg.info_));
        }
      }
      if (OB_SUCC(ret) && arg.floor_pin_only_) {
        ObTabletHandle tablet_handle;
        ObTablet *tablet = nullptr;
        ObTabletAsyncIndexRefreshInfo current_info;
        if (OB_FAIL(ls->get_tablet_svr()->get_tablet(arg.tablet_id_, tablet_handle, 0,
                ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
          LOG_WARN("fail to get tablet for floor-pin check", K(ret), K(arg.tablet_id_));
        } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet is null for floor-pin check", K(ret), K(arg.tablet_id_));
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_TMP_SUCC(tablet->get_async_index_refresh_info(share::SCN::max_scn(), current_info))) {
            if (!current_info.need_apply_floor_pin(arg.info_)) {
              const common::ObIArray<int64_t> &snapshots = current_info.get_refresh_snapshots();
              const common::ObIArray<int64_t> &new_snapshots = arg.info_.get_refresh_snapshots();
              const int64_t new_pin = new_snapshots.empty() ? 0 : new_snapshots.at(0);
              should_write = false;
              LOG_INFO("[ASYNC_INDEX] floor-pin: skip write, existing pin <= new floor_pin",
                       K(arg.tablet_id_), "current_pin0", snapshots.at(0), K(new_pin));
            } else if (OB_FAIL(current_info.merge_floor_pin(arg.info_))) {
              LOG_WARN("failed to merge floor pin", K(ret), K(arg.tablet_id_));
            } else if (OB_FAIL(arg.info_.assign(current_info))) {
              LOG_WARN("failed to assign merged floor pin", K(ret));
            }
          } else if (OB_EMPTY_RESULT != tmp_ret) {
            ret = tmp_ret;
            LOG_WARN("fail to read async index refresh info for floor-pin check",
                     K(ret), K(arg.tablet_id_));
          }
        }
      }
      if (OB_SUCC(ret) && should_write) {
        if (OB_FAIL(ls->get_tablet_svr()->set_async_index_refresh_info(arg.tablet_id_, arg.info_, user_ctx, 0))) {
          if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
            ret = OB_EAGAIN;
            LOG_INFO("fail to set async index refresh info, try again", K(ret), K(arg));
          } else {
            LOG_WARN("fail to set async index refresh info", K(ret), K(arg));
          }
        }
      }
      if (OB_SUCC(ret)) {
        // Isolate floor pins to apply a pre-replay barrier without affecting ordinary refreshes.
        ctx.set_needs_own_log(arg.floor_pin_only_);
      }
    } else {
      ObTabletAsyncIndexRefreshInfoReplayExecutor replay_executor;
      if (OB_FAIL(replay_executor.init(ctx, scn, arg.info_, arg.floor_pin_only_))) {
        LOG_WARN("fail to init replay executor", K(ret), K(scn));
      } else if (OB_FAIL(replay_executor.execute(scn, arg.ls_id_, arg.tablet_id_))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("fail to replay async index refresh info mds", K(ret), K(arg.ls_id_), K(arg.tablet_id_), K(scn));
        } else {
          LOG_INFO("fail to replay async index refresh info mds, try again", K(ret), K(arg.ls_id_), K(arg.tablet_id_), K(scn));
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
