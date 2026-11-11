/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/async_index/ob_async_index_deps.h"
#include "storage/async_index/ob_async_index_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_async_index_params.h"
#include "share/search_index/ob_search_index_builder_util.h"
#include "share/ob_order_perserving_encoder.h"

#include "storage/tx_storage/ob_ls_service.h"         // ObLSService / ObLSIterator
#include "storage/ls/ob_ls.h"                          // ObLS
#include "storage/tx/ob_trans_service.h"               // ObTransService
#include "share/ob_share_util.h"                       // ObShareUtil
#include "storage/tablet/ob_tablet.h"                  // ObTablet
#include "storage/blocksstable/ob_sstable.h"           // ObSSTable / ObSSTableMetaHandle
#include "storage/memtable/ob_memtable.h"              // memtable::ObMemtable
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"  // ObTabletAsyncIndexRefreshInfoArg
#include "storage/tx/ob_multi_data_source.h"           // ObRegisterMdsFlag / ObTxDataSourceType
#include "observer/ob_inner_sql_connection.h"          // ObInnerSQLConnection
#include "share/ob_server_struct.h"                    // GCTX
#include "lib/mysqlclient/ob_mysql_transaction.h"      // ObMySQLTransaction
#include "storage/tablelock/ob_lock_inner_connection_util.h" // ObInnerConnectionLockUtil
#include "storage/tablelock/ob_table_lock_rpc_struct.h"      // ObLockObjRequest
#include "lib/utility/ob_sort.h"
#include "share/ob_debug_sync.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;
using namespace share::schema;

int get_async_index_refresh_tablet(
    ObLS &ls, const ObTabletID &tablet_id, ObTabletHandle &handle,
    const int64_t transfer_seq, const bool is_probe)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData data;
  mds::MdsWriter writer;
  mds::TwoPhaseCommitState trans_state;
  SCN trans_version;
  if (OB_FAIL(ls.get_tablet(tablet_id, handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    if (is_probe && (OB_TABLET_NOT_EXIST == ret || OB_EAGAIN == ret)) {
      LOG_DEBUG("failed to get tablet for async index refresh", K(ret), K(tablet_id),
                "ls_id", ls.get_ls_id());
    } else {
      LOG_WARN("failed to get tablet for async index refresh", K(ret), K(tablet_id),
               "ls_id", ls.get_ls_id());
    }
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (handle.get_obj()->is_empty_shell()) {
    ret = OB_TABLET_NOT_EXIST;
  } else if (handle.get_obj()->get_tablet_meta().ls_id_ != ls.get_ls_id()
      || (transfer_seq >= 0 && handle.get_obj()->get_transfer_seq() != transfer_seq)) {
    ret = OB_TABLET_TRANSFER_SEQ_NOT_MATCH;
    LOG_INFO("async index refresh tablet location changed", K(ret), K(tablet_id),
             "ls_id", ls.get_ls_id(), K(transfer_seq), "tablet_meta", handle.get_obj()->get_tablet_meta());
  } else if (OB_FAIL(handle.get_obj()->get_latest_tablet_status(data, writer, trans_state, trans_version))) {
    if (is_probe && (OB_TABLET_NOT_EXIST == ret || OB_EAGAIN == ret)) {
      LOG_DEBUG("failed to get tablet status for async index refresh", K(ret), K(tablet_id),
                "ls_id", ls.get_ls_id());
    } else {
      LOG_WARN("failed to get tablet status for async index refresh", K(ret), K(tablet_id),
               "ls_id", ls.get_ls_id());
    }
  } else if (mds::TwoPhaseCommitState::ON_COMMIT != trans_state
      || (ObTabletStatus::NORMAL != data.tablet_status_
          && ObTabletStatus::SPLIT_DST != data.tablet_status_
          && ObTabletStatus::FORK_DST != data.tablet_status_)) {
    // A source tablet may remain locally accessible after transfer out.
    ret = OB_EAGAIN;
    if (is_probe) {
      LOG_DEBUG("tablet is not ready for async index refresh", K(ret), K(tablet_id),
                "ls_id", ls.get_ls_id(), K(data), K(trans_state));
    } else {
      LOG_INFO("tablet is not ready for async index refresh", K(ret), K(tablet_id),
               "ls_id", ls.get_ls_id(), K(data), K(trans_state));
    }
  }
  return ret;
}

// Acquire an exclusive async-refresh fence for every index tablet involved in
// DROP, preventing new refresh tasks from entering the normal table-lock path.
int lock_async_index_refresh_for_drop(
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &index_tablet_ids,
    const int64_t lock_timeout_us,
    common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  observer::ObInnerSQLConnection *conn = nullptr;
  ObSEArray<ObTabletID, 8> sorted_tablet_ids;
  transaction::tablelock::ObLockObjsRequest lock_arg;
  // Copy and sort the ids so concurrent DROP operations acquire multiple
  // refresh fences in the same tablet order.
  if (OB_FAIL(sorted_tablet_ids.assign(index_tablet_ids))) {
    LOG_WARN("failed to collect async index tablet ids", K(ret), K(index_tablet_ids));
  } else {
    lib::ob_sort(sorted_tablet_ids.begin(), sorted_tablet_ids.end());
  }
  lock_arg.lock_mode_ = transaction::tablelock::EXCLUSIVE;
  lock_arg.op_type_ = transaction::tablelock::ObTableLockOpType::IN_TRANS_COMMON_LOCK;
  lock_arg.timeout_us_ = lock_timeout_us;
  lock_arg.lock_priority_ = transaction::tablelock::ObTableLockPriority::HIGH1;
  lock_arg.force_use_priority_ = true;
  // Skip duplicate tablet ids and collect all refresh fences into one request.
  for (int64_t i = 0; OB_SUCC(ret) && i < sorted_tablet_ids.count(); ++i) {
    const ObTabletID &tablet_id = sorted_tablet_ids.at(i);
    if (i > 0 && tablet_id == sorted_tablet_ids.at(i - 1)) {
      continue;
    }
    transaction::tablelock::ObLockID lock_id;
    if (OB_FAIL(lock_id.set(
            transaction::tablelock::ObLockOBJType::OBJ_TYPE_ASYNC_INDEX_REFRESH,
            tablet_id.id()))) {
      LOG_WARN("failed to build async index refresh lock id", K(ret), K(tablet_id));
    } else if (OB_FAIL(lock_arg.objs_.push_back(lock_id))) {
      LOG_WARN("failed to collect async index refresh lock id", K(ret), K(lock_id));
    }
  }
  // Acquire every fence through one table-lock request so priority registration
  // and timeout accounting cover the complete tablet set as one batch.
  if (OB_SUCC(ret) && !lock_arg.objs_.empty()) {
    if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(trans.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection is null", K(ret), K(tenant_id));
    } else if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_obj(
                   tenant_id, lock_arg, conn))) {
      LOG_WARN("failed to fence async index refresh for drop", K(ret), K(tenant_id),
               K(lock_arg), K(lock_timeout_us));
    }
  }
  return ret;
}

// ============================================================================
// calc_min_publish_scn / get_min_index_published_scn
// ============================================================================

int calc_min_publish_scn(
    const ObIArray<SCN> &publish_scns,
    SCN &min_publish_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(publish_scns.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ASYNC_INDEX] empty publish scns", K(ret));
  } else {
    min_publish_scn = publish_scns.at(0);
    for (int64_t i = 1; i < publish_scns.count(); ++i) {
      if (publish_scns.at(i) < min_publish_scn) {
        min_publish_scn = publish_scns.at(i);
      }
    }
  }
  return ret;
}

int get_min_index_published_scn(
    const uint64_t tenant_id,
    ObLS *ls,
    ObIAsyncIndexDepProvider *dep_provider,
    const ObIArray<ObTabletID> &index_tablet_ids,
    SCN &min_publish_scn)
{
  int ret = OB_SUCCESS;
  min_publish_scn = SCN::base_scn();
  ObSEArray<SCN, 8> publish_scns;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(ls)
      || OB_ISNULL(dep_provider)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ASYNC_INDEX] invalid argument", K(ret), K(tenant_id), KP(ls), KP(dep_provider));
  } else if (index_tablet_ids.empty()) {
    // No sibling indexes on this partition; keep the conservative base_scn.
  } else if (OB_FAIL(dep_provider->get_publish_scns(ls, index_tablet_ids, publish_scns))) {
    LOG_WARN("[ASYNC_INDEX] fail to get index publish scns", K(ret), K(index_tablet_ids));
  } else if (OB_UNLIKELY(publish_scns.count() != index_tablet_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ASYNC_INDEX] index publish scn count mismatch", K(ret), K(index_tablet_ids), K(publish_scns));
  } else if (OB_FAIL(calc_min_publish_scn(publish_scns, min_publish_scn))) {
    LOG_WARN("[ASYNC_INDEX] fail to calc min publish scn", K(ret), K(publish_scns));
  }
  return ret;
}

int ObIAsyncIndexDepProvider::get_leader_proposal_id(ObLS *ls, int64_t &proposal_id)
{
  int ret = OB_SUCCESS;
  ObRole role = FOLLOWER;
  proposal_id = -1;
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null LS for async index leader proposal", K(ret));
  } else if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
    LOG_WARN("failed to get async index leader proposal", K(ret), K(ls->get_ls_id()));
  } else if (!is_strong_leader(role)) {
    ret = OB_NOT_MASTER;
    LOG_INFO("async index LS is not leader", K(ret), K(ls->get_ls_id()), K(role), K(proposal_id));
  }
  return ret;
}

int ObIAsyncIndexDepProvider::get_async_index_read_state(
    const uint64_t tenant_id, ObLS *ls, const ObTabletID &tablet_id,
    const ObAsyncIndexReadType read_type, const int64_t transfer_seq,
    ObAsyncIndexReadState &state)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  if (ObAsyncIndexReadType::CHECKPOINTS == read_type) {
    if (OB_FAIL(get_async_index_refresh_info(tenant_id, ls, tablet_id, state.refresh_info_))) {
      LOG_WARN("failed to read async index checkpoints", K(ret), K(tablet_id));
    }
  } else if (ObAsyncIndexReadType::PUBLISHED_VERSION != read_type || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index publication read", K(ret), KP(ls), K(tablet_id), K(read_type));
  } else if (OB_FAIL(get_async_index_refresh_tablet(*ls, tablet_id, tablet_handle, transfer_seq))) {
    LOG_WARN("failed to get async index tablet at expected generation",
             K(ret), K(tablet_id), K(transfer_seq));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_async_index_refresh_start_version(
                 state.published_version_, false /*use_cache*/))) {
    LOG_WARN("failed to read async index publication", K(ret), K(tablet_id));
  }
  return ret;
}

int ObIAsyncIndexDepProvider::check_async_index_read_proposal_(
    ObLS *ls, const int64_t expected_proposal_id)
{
  int ret = OB_SUCCESS;
  int64_t proposal_id = -1;
  if (OB_FAIL(get_leader_proposal_id(ls, proposal_id))) {
    LOG_WARN("failed to check async index read proposal", K(ret), K(expected_proposal_id));
  } else if (proposal_id != expected_proposal_id) {
    ret = OB_NOT_MASTER;
    LOG_INFO("async index read proposal changed", K(ret), K(proposal_id), K(expected_proposal_id));
  }
  return ret;
}

int ObIAsyncIndexDepProvider::read_async_index_state_on_leader(
    const uint64_t tenant_id, ObLS *ls, const int64_t leader_proposal_id,
    const ObTabletID &tablet_id, const ObAsyncIndexReadType read_type,
    const int64_t transfer_seq, ObAsyncIndexReadState &state)
{
  int ret = OB_SUCCESS;
  state.reset();
  if (OB_INVALID_TENANT_ID == tenant_id || !tablet_id.is_valid() || leader_proposal_id < 0
      || (ObAsyncIndexReadType::CHECKPOINTS != read_type
          && ObAsyncIndexReadType::PUBLISHED_VERSION != read_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid async index state read", K(ret), K(tenant_id), K(tablet_id),
             K(leader_proposal_id), K(read_type));
  } else if (OB_FAIL(check_async_index_read_proposal_(ls, leader_proposal_id))) {
    LOG_WARN("leader changed before reading async index state", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_async_index_read_state(
                 tenant_id, ls, tablet_id, read_type, transfer_seq, state))) {
    LOG_WARN("failed to read local async index state", K(ret), K(tablet_id), K(read_type));
  } else {
    if (ObAsyncIndexReadType::CHECKPOINTS == read_type) {
      DEBUG_SYNC(AFTER_ASYNC_INDEX_READ_REFRESH_INFO);
    } else {
      DEBUG_SYNC(AFTER_ASYNC_INDEX_READ_PUBLISH_SCN);
    }
    if (OB_FAIL(check_async_index_read_proposal_(ls, leader_proposal_id))) {
      LOG_WARN("leader changed while reading async index state", K(ret), K(tablet_id));
    }
  }
  if (OB_FAIL(ret)) {
    state.reset();
  }
  return ret;
}

int maintain_async_index_refresh_info(
    const uint64_t tenant_id,
    ObLS *ls,
    const int64_t leader_proposal_id,
    const ObTabletID &data_tablet_id,
    const SCN &new_snapshot,
    const SCN &min_publish_scn,
    ObIAsyncIndexDepProvider *dep_provider,
    ObTabletAsyncIndexRefreshInfo &out_refresh_info,
    const int64_t lock_timeout_us,
    const bool skip_incr_check,
    bool *checkpoint_capacity_full,
    const bool is_manual_refresh,
    const bool skip_if_published)
{
  int ret = OB_SUCCESS;
  out_refresh_info.reset();
  if (nullptr != checkpoint_capacity_full) {
    *checkpoint_capacity_full = false;
  }
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(leader_proposal_id < 0)
      || OB_UNLIKELY(!data_tablet_id.is_valid())
      || OB_UNLIKELY(!new_snapshot.is_valid())
      || OB_ISNULL(dep_provider)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(ls), K(data_tablet_id),
             K(new_snapshot), K(min_publish_scn), KP(dep_provider));
  } else {
    ObArenaAllocator allocator(ObMemAttr(tenant_id, "AsyncIdxMds"));
    ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    ObMySQLTransaction trans;
    observer::ObInnerSQLConnection *conn = nullptr;
    const ObLSID ls_id = (ls != nullptr) ? ls->get_ls_id() : ObLSID();
    DEBUG_SYNC(ASYNC_INDEX_SNAPSHOT_WAIT_RESET);

    // Step 1: start a lightweight internal transaction (skip when ls is null)
    if (OB_ISNULL(ls)) {
      // Test providers emulate leadership; production rejects a null LS on read.
    } else if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", K(ret));
    } else if (OB_FAIL(trans.start(sql_proxy, tenant_id))) {
      LOG_WARN("failed to start trans", K(ret), K(tenant_id));
    } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(
                             trans.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection is null", K(ret));
    }

    // Step 2: try-lock OBJ_TYPE_ASYNC_INDEX_REFRESH with EXCLUSIVE + timeout_us=0
    if (OB_SUCC(ret) && OB_NOT_NULL(conn)) {
      transaction::tablelock::ObTableLockOwnerID owner_id;
      if (OB_FAIL(owner_id.convert_from_value(
              transaction::tablelock::ObLockOwnerType::DEFAULT_OWNER_TYPE,
              get_tid_cache()))) {
        LOG_WARN("failed to get owner id", K(ret));
      } else {
        transaction::tablelock::ObLockObjRequest lock_arg;
        lock_arg.obj_type_ = transaction::tablelock::ObLockOBJType::OBJ_TYPE_ASYNC_INDEX_REFRESH;
        lock_arg.obj_id_ = data_tablet_id.id();
        lock_arg.owner_id_ = owner_id;
        lock_arg.lock_mode_ = transaction::tablelock::EXCLUSIVE;
        lock_arg.op_type_ = transaction::tablelock::ObTableLockOpType::IN_TRANS_COMMON_LOCK;
        lock_arg.timeout_us_ = lock_timeout_us;
        if (OB_FAIL(transaction::tablelock::ObInnerConnectionLockUtil::lock_obj(
                tenant_id, lock_arg, conn))) {
          if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret || OB_TRY_LOCK_ROW_CONFLICT == ret || OB_TIMEOUT == ret) {
            LOG_INFO("maintain_async_index_refresh_info lock conflict, skip this round",
                     K(ret), K(data_tablet_id), K(lock_timeout_us));
            ret = OB_EAGAIN;
          } else {
            LOG_WARN("failed to lock obj for async index refresh", K(ret), K(data_tablet_id));
          }
        }
      }
    }

    // Step 3: read current refresh_info
    ObAsyncIndexReadState read_state;
    ObTabletAsyncIndexRefreshInfo &refresh_info = read_state.refresh_info_;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(dep_provider->read_async_index_state_on_leader(
              tenant_id, ls, leader_proposal_id, data_tablet_id,
              ObAsyncIndexReadType::CHECKPOINTS, -1 /*transfer_seq*/, read_state))) {
        LOG_WARN("failed to get async index refresh info", K(ret), K(data_tablet_id));
      } else if (!refresh_info.is_valid_for_refresh()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("refresh info not ready for refresh", K(ret), K(data_tablet_id), K(refresh_info));
      } else if (is_manual_refresh && refresh_info.get_refresh_snapshots().count()
          > OB_ASYNC_INDEX_MANUAL_REFRESH_CHECKPOINT_THRESHOLD) {
        // 在排他锁内检查，拒绝时不追加 target，也不修改已有 checkpoint。
        ret = OB_OP_NOT_ALLOW;
        LOG_USER_ERROR(OB_OP_NOT_ALLOW,
            "Async index refresh has a backlog. Please retry later. Manual refresh is");
        LOG_WARN("[ASYNC_INDEX] manual refresh throttled by checkpoint backlog", K(ret),
            K(tenant_id), K(ls_id), K(data_tablet_id), K(new_snapshot),
            "checkpoint_count", refresh_info.get_refresh_snapshots().count(),
            "checkpoint_threshold", OB_ASYNC_INDEX_MANUAL_REFRESH_CHECKPOINT_THRESHOLD);
      }
    }

    // Step 4: early-exit check — if new_snapshot does not advance past cur_max
    bool need_update = false;
    if (OB_SUCC(ret)) {
      const ObIArray<int64_t> &snapshots = refresh_info.get_refresh_snapshots();
      const int64_t cur_max = snapshots.empty()
          ? 0 : snapshots.at(snapshots.count() - 1);
      const int64_t new_snapshot_val = async_index_scn_to_snapshot(new_snapshot);
      const int64_t min_publish_snapshot = async_index_scn_to_snapshot(min_publish_scn);
      // Major may skip only after its original proposal has been validated.
      const bool already_published = skip_if_published && min_publish_snapshot >= new_snapshot_val;

      // Step 5: increment detection — probe (cur_max, new_snapshot] for DML
      bool has_incr = false;
      if (!already_published && new_snapshot_val > cur_max) {
        if (skip_incr_check) {
          has_incr = true;
        } else {
          const SCN window_start = (cur_max > 0)
              ? async_index_snapshot_to_scn(cur_max)
              : SCN::base_scn();
          if (OB_FAIL(dep_provider->has_incr_dml(
                  tenant_id, ls, data_tablet_id, window_start, new_snapshot,
                  has_incr))) {
            LOG_WARN("failed to probe incr dml", K(ret), K(data_tablet_id),
                     K(window_start), K(new_snapshot));
          }
        }
      }

      // Step 6: prune retired snapshots (when min_publish_scn > SCN::min_scn())
      if (OB_SUCC(ret) && !already_published) {
        if (!snapshots.empty() && min_publish_snapshot > 0) {
          const int64_t before_cnt = snapshots.count();
          const int64_t before_head = snapshots.at(0);
          if (OB_FAIL(refresh_info.cleanup_retired_snapshots(min_publish_snapshot))) {
            if (nullptr != checkpoint_capacity_full && OB_SIZE_OVERFLOW == ret
                && before_cnt >= MAX_INC_MAJOR_SSTABLE_CNT && min_publish_snapshot < before_head) {
              *checkpoint_capacity_full = true;
            }
            if (nullptr != checkpoint_capacity_full && *checkpoint_capacity_full) {
              if (REACH_TIME_INTERVAL(1000L * 1000L)) {
                LOG_INFO("[ASYNC_INDEX] checkpoint array full while inserting published floor",
                    K(ret), K(tenant_id), K(ls_id), K(data_tablet_id), K(new_snapshot_val),
                    K(min_publish_snapshot), K(before_cnt), K(before_head));
              }
            } else {
              LOG_WARN("failed to cleanup retired snapshots", K(ret), K(tenant_id), K(ls_id),
                  K(data_tablet_id), K(new_snapshot_val), K(min_publish_snapshot), K(before_cnt), K(before_head));
            }
          } else if (snapshots.count() != before_cnt || snapshots.at(0) != before_head) {
            need_update = true;
          }
        } else if (snapshots.empty()
                   && min_publish_snapshot > async_index_scn_to_snapshot(share::SCN::base_scn())
                   && new_snapshot_val > min_publish_snapshot) {
          // anchor the published checkpoint when the array is first created
          if (OB_FAIL(refresh_info.append_refresh_snapshot(min_publish_snapshot))) {
            LOG_WARN("failed to anchor published checkpoint into empty refresh info",
                     K(ret), K(data_tablet_id), K(min_publish_snapshot));
          } else {
            need_update = true;
          }
        }
      }

      // append new_snapshot if increment detected and it advances past cur_max
      if (OB_SUCC(ret) && has_incr) {
        const int64_t last = snapshots.empty() ? 0 : snapshots.at(snapshots.count() - 1);
        if (new_snapshot_val > last) {
          if (OB_FAIL(refresh_info.append_refresh_snapshot(new_snapshot_val))) {
            if (OB_SIZE_OVERFLOW == ret && snapshots.count() >= MAX_INC_MAJOR_SSTABLE_CNT) {
              if (REACH_TIME_INTERVAL(1000L * 1000L)) {
                LOG_INFO("[ASYNC_INDEX] checkpoint array full while appending target",
                    K(ret), K(tenant_id), K(ls_id), K(data_tablet_id), K(new_snapshot_val),
                    K(min_publish_snapshot), K(last), "checkpoint_count", snapshots.count(),
                    "report_capacity_full", nullptr != checkpoint_capacity_full);
              }
              if (nullptr != checkpoint_capacity_full) {
                *checkpoint_capacity_full = true;
              }
            } else {
              LOG_WARN("failed to append refresh snapshot", K(ret), K(data_tablet_id),
                       K(new_snapshot_val), K(refresh_info.get_refresh_snapshots().count()));
            }
          } else {
            need_update = true;
          }
        }
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("[ASYNC_INDEX_REFRESH] maintain refresh info decision",
                 K(tenant_id), K(ls_id), K(data_tablet_id), K(cur_max),
                 K(new_snapshot_val), K(min_publish_scn), K(has_incr),
                 K(need_update), K(snapshots));
      }

      // Step 7: MDS write within the same transaction (if content changed)
      if (OB_SUCC(ret) && need_update) {
        if (OB_ISNULL(conn)) {
          if (OB_FAIL(dep_provider->set_async_index_refresh_info(
                  tenant_id, ls, data_tablet_id, refresh_info))) {
            LOG_WARN("failed to set async index refresh info via dep_provider",
                     K(ret), K(data_tablet_id));
          }
        } else {
          ObTabletAsyncIndexRefreshInfoArg arg;
          arg.ls_id_ = ls_id;
          arg.tablet_id_ = data_tablet_id;
          transaction::ObRegisterMdsFlag flag;
          flag.need_flush_redo_instantly_ = true;
          flag.mds_base_scn_.reset();
          char *buf = nullptr;
          int64_t buf_len = 0;
          int64_t pos = 0;
          if (OB_FAIL(arg.info_.assign(refresh_info))) {
            LOG_WARN("failed to assign refresh info to arg", K(ret), K(data_tablet_id));
          } else if (OB_UNLIKELY(!arg.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid refresh info arg", K(ret), K(arg));
          } else if (FALSE_IT(buf_len = arg.get_serialize_size())) {
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate buf", K(ret), K(buf_len));
          } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
            LOG_WARN("failed to serialize arg", K(ret), K(arg));
          } else if (OB_FAIL(conn->register_multi_data_source(
                         tenant_id, ls_id,
                         transaction::ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
                         buf, buf_len, flag))) {
            LOG_WARN("failed to register async index refresh info mds", K(ret),
                     K(ls_id), K(data_tablet_id));
          }
        }
      }

      // Step 8: output the (possibly updated) refresh_info
      if (OB_SUCC(ret)) {
        if (OB_FAIL(out_refresh_info.assign(refresh_info))) {
          LOG_WARN("failed to assign out refresh info", K(ret), K(data_tablet_id));
        }
      }
    }

    // Always end the transaction (lock is released with the transaction)
    if (trans.is_started()) {
      const bool commit = OB_SUCC(ret);
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(commit))) {
        LOG_WARN("[ASYNC_INDEX] failed to end refresh info maintenance transaction", K(ret), K(tmp_ret),
            K(commit), K(tenant_id), K(ls_id), K(data_tablet_id), K(new_snapshot), K(min_publish_scn));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

// ============================================================================
// Production provider stubs.
//
// These are intentionally minimal placeholders. The real implementations are
// owned by other developers in the async-index project (schema / MDS / storage
// estimation), per specs/spec.md needs boundary. Each stub returns OB_SUCCESS
// with an empty / conservative default so that:
//   - the scheduler links and runs;
//   - an idle node with no async indexes does no real work (low base noise);
//   - no refresh is ever triggered until the real providers are wired in.
// ============================================================================

int ObAsyncIndexDepProvider::get_publish_scns(
    ObLS *ls,
    const ObIArray<ObTabletID> &index_tablet_ids,
    ObIArray<SCN> &publish_scns)
{
  int ret = OB_SUCCESS;
  publish_scns.reset();
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret));
  } else {
    // Empty refreshes advance the committed logical watermark without adding
    // a physical table. Use that watermark when choosing the next refresh start.
    for (int64_t i = 0; OB_SUCC(ret) && i < index_tablet_ids.count(); ++i) {
      const ObTabletID &index_tablet_id = index_tablet_ids.at(i);
      ObTabletHandle tablet_handle;
      int64_t published_version = 0;
      if (OB_FAIL(get_async_index_refresh_tablet(
              *ls, index_tablet_id, tablet_handle))) {
        LOG_WARN("failed to get index tablet", K(ret), K(index_tablet_id));
      } else if (OB_ISNULL(tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is null", K(ret), K(index_tablet_id));
      } else if (OB_FAIL(tablet_handle.get_obj()->get_async_index_refresh_start_version(
                     published_version))) {
        LOG_WARN("failed to get index max published version", K(ret), K(index_tablet_id));
      } else if (OB_FAIL(publish_scns.push_back(
                     async_index_snapshot_to_scn(published_version)))) {
        LOG_WARN("failed to push publish scn", K(ret), K(i), K(index_tablet_id),
                 K(published_version));
      }
    }
  }
  return ret;
}

int ObAsyncIndexDepProvider::get_async_index_refresh_info(
    const uint64_t tenant_id,
    ObLS *ls,
    const ObTabletID &data_tablet_id,
    ObTabletAsyncIndexRefreshInfo &info)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  info.reset();
  const int64_t timeout_us = GCONF.internal_sql_execute_timeout;
  ObTabletHandle tablet_handle;
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(ls->get_tablet(data_tablet_id, tablet_handle,
                                     0 /*timeout_us*/,
                                     ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    // Preserve location and fatal errors; neither is a valid initial empty MDS.
    LOG_WARN("failed to get data tablet for async index refresh", K(ret), K(data_tablet_id));
  } else if (OB_ISNULL(tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(data_tablet_id));
  } else if (!tablet_handle.get_obj()->is_data_complete()) {
    // Transfer/HA/fork may not have installed the source MDS yet. Such absence
    // must not be mistaken for an initial empty record with false membership.
    ret = OB_EAGAIN;
    LOG_INFO("data tablet is incomplete for async index refresh", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_async_index_refresh_info(
                 SCN::max_scn(), info, timeout_us))) {
    if (OB_EMPTY_RESULT == ret) {
      // no Async Refresh Info has ever been written for this tablet: brand new.
      info.reset();
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get async index refresh info from tablet", K(ret), K(data_tablet_id));
    }
  }
  return ret;
}

int ObAsyncIndexDepProvider::set_async_index_refresh_info(
    const uint64_t tenant_id,
    ObLS *ls,
    const ObTabletID &data_tablet_id,
    const ObTabletAsyncIndexRefreshInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret), K(data_tablet_id));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid refresh info", K(ret), K(data_tablet_id), K(info));
  } else {
    // The snapshot array is capped at MAX_INC_MAJOR_SSTABLE_CNT(=64) int64_t
    // entries, so the serialized arg is small; a temp arena suffices.
    ObArenaAllocator allocator(ObMemAttr(tenant_id, "AsyncIdxMds"));
    ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
    ObMySQLTransaction trans;
    observer::ObInnerSQLConnection *conn = nullptr;
    const ObLSID ls_id = ls->get_ls_id();
    ObTabletAsyncIndexRefreshInfoArg arg;
    arg.ls_id_ = ls_id;
    arg.tablet_id_ = data_tablet_id;
    transaction::ObRegisterMdsFlag flag;
    flag.need_flush_redo_instantly_ = true;
    flag.mds_base_scn_.reset();
    char *buf = nullptr;
    int64_t buf_len = 0;
    int64_t pos = 0;
    if (OB_ISNULL(sql_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("sql proxy is null", K(ret));
    } else if (OB_FAIL(arg.info_.assign(info))) {
      LOG_WARN("failed to assign refresh info", K(ret), K(data_tablet_id));
    } else if (OB_UNLIKELY(!arg.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid refresh info arg", K(ret), K(arg));
    } else if (OB_FAIL(trans.start(sql_proxy, tenant_id))) {
      LOG_WARN("failed to start trans", K(ret), K(tenant_id));
    } else if (OB_ISNULL(conn = static_cast<observer::ObInnerSQLConnection *>(
                             trans.get_connection()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("connection is null", K(ret));
    } else if (FALSE_IT(buf_len = arg.get_serialize_size())) {
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate buf", K(ret), K(buf_len));
    } else if (OB_FAIL(arg.serialize(buf, buf_len, pos))) {
      LOG_WARN("failed to serialize arg", K(ret), K(arg));
    } else if (OB_FAIL(conn->register_multi_data_source(
                   tenant_id, ls_id,
                   transaction::ObTxDataSourceType::ASYNC_INDEX_REFRESH_INFO,
                   buf, buf_len, flag))) {
      LOG_WARN("failed to register async index refresh info mds", K(ret), K(ls_id), K(data_tablet_id));
    }
    // always end the transaction we started
    if (trans.is_started()) {
      const bool commit = OB_SUCC(ret);
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(trans.end(commit))) {
        LOG_WARN("failed to end trans", K(tmp_ret), K(commit));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
    // lock conflicts are transient: caller skips this round and retries later.
    if (OB_ERR_EXCLUSIVE_LOCK_CONFLICT == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("set async index refresh info lock conflict, retry next round",
               K(ret), K(data_tablet_id));
    }
  }
  return ret;
}

int ObAsyncIndexDepProvider::has_incr_dml(
    const uint64_t tenant_id,
    ObLS *ls,
    const ObTabletID &data_tablet_id,
    const SCN &begin_scn,
    const SCN &end_scn,
    bool &has_data)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  has_data = false;

  ObTabletHandle tablet_handle;
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(ls->get_tablet(data_tablet_id, tablet_handle,
                                     0 /*timeout_us*/,
                                     ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(data_tablet_id));
  } else {
    ObTablet *tablet = tablet_handle.get_obj();
    if (OB_ISNULL(tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(data_tablet_id));
    } else {
      // 1. Check every non-empty minor SSTable: a transaction may commit after
      //    the SSTable's log SCN range ends.
      ObTabletMemberWrapper<ObTabletTableStore> wrapper;
      if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
        LOG_WARN("failed to fetch table store", K(ret), K(data_tablet_id));
      } else {
        const ObSSTableArray &minor_tables =
            wrapper.get_member()->get_minor_sstables();
        for (int64_t i = 0; OB_SUCC(ret) && !has_data && i < minor_tables.count(); ++i) {
          blocksstable::ObSSTable *sstable = minor_tables[i];
          if (OB_ISNULL(sstable)) {
            continue;
          }
          ObSSTableMetaHandle meta_handle;
          if (OB_FAIL(sstable->get_meta(meta_handle))) {
            LOG_WARN("failed to get sstable meta", K(ret));
            break;
          }
          const int64_t row_count =
              meta_handle.get_sstable_meta().get_row_count();
          if (0 == row_count) {
            continue;
          }
          const int64_t upper_trans = sstable->get_upper_trans_version();
          if (upper_trans > 0
              && upper_trans != INT64_MAX
              && upper_trans <= begin_scn.get_val_for_tx()) {
            continue;
          }
          has_data = true;
        }
      }

      // 2. Check memtables — only if SSTables did not already confirm data.
      //    Use memtable-level metadata (max_end_scn) instead of row-level scan
      //    to avoid the O(N) btree traversal + per-row latch overhead of
      //    check_memtable_has_incr_dml.  This is a conservative check: it may
      //    report false positives (triggering an empty refresh) but never false
      //    negatives (missing real incremental data).
      if (OB_SUCC(ret) && !has_data) {
        ObSEArray<ObITable *, 8> memtables;
        if (OB_FAIL(tablet->get_memtables(memtables))) {
          LOG_WARN("failed to get memtables", K(ret), K(data_tablet_id));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && !has_data && i < memtables.count(); ++i) {
            ObITable *table = memtables.at(i);
            if (OB_ISNULL(table) || !table->is_data_memtable()) {
              continue;
            }
            memtable::ObMemtable *mt = static_cast<memtable::ObMemtable *>(table);
            if (mt->is_empty() || mt->get_physical_row_cnt() == 0) {
              continue;
            }
            if (mt->get_max_end_scn() <= begin_scn) {
              continue;
            }
            if (table->get_start_scn() >= end_scn) {
              continue;
            }
            has_data = true;
          }
        }
      }
    }
  }
  return ret;
}

int ObAsyncIndexDepProvider::get_read_snapshot_version(
    const uint64_t tenant_id,
    SCN &snapshot_scn)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!cached_snapshot_scn_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached snapshot scn is invalid, call refresh_snapshot_scn first", K(ret));
  } else {
    snapshot_scn = cached_snapshot_scn_;
  }
  return ret;
}

int ObAsyncIndexDepProvider::refresh_snapshot_scn()
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", K(ret));
  } else {
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx, DEFAULT_TIMEOUT))) {
      LOG_WARN("failed to set default timeout ctx", K(ret));
    } else if (OB_FAIL(txs->get_read_snapshot_version(
                   timeout_ctx.get_abs_timeout(), cached_snapshot_scn_))) {
      LOG_WARN("failed to get read snapshot version from trans service", K(ret));
    } else {
      LOG_DEBUG("refreshed snapshot scn for async index", K_(cached_snapshot_scn));
    }
  }
  return ret;
}

int ObAsyncIndexDepProvider::get_index_minor_sstable_count(
    const uint64_t tenant_id,
    ObLS *ls,
    const ObTabletID &index_tablet_id,
    int64_t &count)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  count = 0;

  ObTabletHandle tablet_handle;
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret), K(index_tablet_id));
  } else if (OB_FAIL(ls->get_tablet(index_tablet_id, tablet_handle,
                                     0 /*timeout_us*/,
                                     ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get index tablet", K(ret), K(index_tablet_id));
  } else {
    ObTablet *tablet = tablet_handle.get_obj();
    if (OB_ISNULL(tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(index_tablet_id));
    } else {
      ObTabletMemberWrapper<ObTabletTableStore> wrapper;
      if (OB_FAIL(tablet->fetch_table_store(wrapper))) {
        LOG_WARN("failed to fetch table store", K(ret), K(index_tablet_id));
      } else {
        count = wrapper.get_member()->get_inc_major_sstables().count() + wrapper.get_member()->get_inc_major_ddl_sstables().count();
      }
    }
  }
  return ret;
}

int ObAsyncIndexDepProvider::get_index_delete_tablet_id(
    const uint64_t tenant_id,
    const ObTabletID &index_tablet_id,
    ObTabletID &delete_tablet_id)
{
  UNUSEDx(tenant_id, index_tablet_id);
  // TODO(async-index): resolve the delete-buffer table tablet of this index.
  delete_tablet_id.reset();
  return OB_SUCCESS;
}

int ObAsyncIndexDepProvider::collect_leader_ls_ids_(
    ObIArray<ObLSID> &leader_ls_ids)
{
  int ret = OB_SUCCESS;
  leader_ls_ids.reset();
  ObLSService *ls_service = MTL(ObLSService *);
  common::ObSharedGuard<ObLSIterator> ls_iter_guard;
  ObLS *ls = nullptr;
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", K(ret));
  } else if (OB_FAIL(ls_service->get_ls_iter(ls_iter_guard, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get ls iterator", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObRole role = common::INVALID_ROLE;
      int64_t proposal_id = 0;
      if (OB_FAIL(ls_iter_guard.get_ptr()->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next ls", K(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret));
      } else if (OB_FAIL(ls->get_log_handler()->get_role(role, proposal_id))) {
        // a transient role-fetch failure on one ls should not abort the rest
        LOG_WARN("failed to get ls role, skip", K(ret), "ls_id", ls->get_ls_id());
        ret = OB_SUCCESS;
      } else if (!is_strong_leader(role)) {
        // not the leader of this ls: its tablets are handled by the leader node
      } else if (OB_FAIL(leader_ls_ids.push_back(ls->get_ls_id()))) {
        LOG_WARN("failed to add leader ls id", K(ret), "ls_id", ls->get_ls_id());
      }
    }
  }
  return ret;
}

int ObAsyncIndexDepProvider::collect_leader_ls_ids(
    ObIArray<ObLSID> &leader_ls_ids)
{
  return collect_leader_ls_ids_(leader_ls_ids);
}


int ObAsyncIndexDepProvider::get_async_index_data_tablets(
    const uint64_t tenant_id,
    ObIArray<uint64_t> &data_table_ids,
    ObIArray<ObTabletID> &data_tablet_ids)
{
  int ret = OB_SUCCESS;
  data_table_ids.reset();
  data_tablet_ids.reset();
  ObSchemaGetterGuard schema_guard;
  ObSEArray<const ObSimpleTableSchemaV2 *, 16> table_schemas;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_with_async_index_in_tenant(tenant_id, table_schemas))) {
    // Walks the pre-collected "tables carrying async index" set rather than the
    // full tenant table list, so an idle tenant pays essentially nothing.
    LOG_WARN("failed to get async index tables", K(ret), K(tenant_id));
  } else if (table_schemas.empty()) {
    // no async index tables: nothing to do (low base noise)
  } else {
    // Return ALL (data_table_id, data_tablet_id) pairs in the tenant without
    // LS filtering. The caller (check_and_refresh_for_ls_) is responsible for
    // filtering to only the tablets on its own LS via has_tablet(), which is a
    // cheap local memory lookup. This avoids repeated collect_leader_ls_ids_()
    // and location service calls when multiple LSs share the same timer round.
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObSimpleTableSchemaV2 *table_schema = table_schemas.at(i);
      if (OB_ISNULL(table_schema)) {
        LOG_WARN("null table schema in async index infos, skip", K(tenant_id), K(i));
      } else {
        const uint64_t data_table_id = table_schema->get_table_id();
        ObSEArray<ObTabletID, 16> tablet_ids;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(table_schema->get_tablet_ids(tablet_ids))) {
          LOG_WARN("failed to get tablet ids, skip this data table", K(tmp_ret),
                   K(tenant_id), K(data_table_id));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < tablet_ids.count(); ++j) {
            if (OB_FAIL(data_table_ids.push_back(data_table_id))) {
              LOG_WARN("failed to push data table id", K(ret), K(data_table_id));
            } else if (OB_FAIL(data_tablet_ids.push_back(tablet_ids.at(j)))) {
              LOG_WARN("failed to push data tablet id", K(ret), K(data_table_id),
                       "data_tablet_id", tablet_ids.at(j));
              data_table_ids.pop_back();
            }
          }
        }
      }
    }
  }
  return ret;
}

static int parse_async_index_params_(
    const ObTableSchema &data_table_schema,
    ObAsyncIndexParams &async_index_params,
    int64_t &refresh_interval_us)
{
  int ret = OB_SUCCESS;
  refresh_interval_us = ObAsyncIndexParams::DISABLE_REFRESH_INTERVAL;
  if (OB_FAIL(ObAsyncIndexParams::parse(
          data_table_schema.get_async_index_params(), async_index_params))) {
  } else {
    refresh_interval_us =
        ObAsyncIndexParams::DISABLE_REFRESH_INTERVAL == async_index_params.refresh_interval_
        ? ObAsyncIndexParams::DISABLE_REFRESH_INTERVAL
        : async_index_params.refresh_interval_ * 1000000L;
  }
  return ret;
}

int ObAsyncIndexDepProvider::get_async_table_refresh_intervals(
    const uint64_t tenant_id,
    ObIArray<ObAsyncTableRefreshInterval> &table_refresh_intervals)
{
  int ret = OB_SUCCESS;
  table_refresh_intervals.reset();
  ObSchemaGetterGuard schema_guard;
  ObSEArray<const ObSimpleTableSchemaV2 *, 16> table_schemas;
  table_schemas.set_attr(ObMemAttr(tenant_id, "AsyncIdxDeps"));
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schemas_with_async_index_in_tenant(
                 tenant_id, table_schemas))) {
    LOG_WARN("failed to get async index tables", K(ret), K(tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      const ObSimpleTableSchemaV2 *simple_schema = table_schemas.at(i);
      if (OB_ISNULL(simple_schema)) {
        LOG_WARN("null table schema in async index infos, skip", K(tenant_id), K(i));
        continue;
      }
      const uint64_t data_table_id = simple_schema->get_table_id();
      const ObTableSchema *data_table_schema = nullptr;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
        LOG_WARN("failed to get data table schema, skip this table", K(tmp_ret), K(tenant_id), K(data_table_id));
        continue;
      } else if (OB_ISNULL(data_table_schema)) {
        LOG_WARN("data table schema is null, skip", K(tenant_id), K(data_table_id));
        continue;
      } else if (data_table_schema->is_user_hidden_table()) {
        // Only the owning offline DDL may prepare this table before swap.
        continue;
      } else if (data_table_schema->is_ctas_tmp_table()) {
        // A target sampled before the final CTAS DDL can fall below its read
        // lower bound. Only admit periodic refresh after the table is formal.
        LOG_DEBUG("ctas table is not ready for periodic async index refresh, skip",
                  K(tenant_id), K(data_table_id), "session_id", data_table_schema->get_session_id());
        continue;
      }

      ObAsyncIndexParams async_index_params;
      ObAsyncTableRefreshInterval interval;
      interval.data_table_id_ = data_table_id;
      if (OB_TMP_FAIL(parse_async_index_params_(
              *data_table_schema, async_index_params, interval.refresh_interval_us_))) {
        LOG_WARN("failed to parse async index params", K(tmp_ret), K(tenant_id), K(data_table_id),
                 "async_index_params", data_table_schema->get_async_index_params());
      }
      if (OB_FAIL(table_refresh_intervals.push_back(interval))) {
        LOG_WARN("failed to push async table refresh interval", K(ret), K(tenant_id), K(interval));
      }
    }
  }
  return ret;
}

// Resolve only the requested tablet. No full-table tablet/name arrays are built.
static int get_refresh_partition_(const ObTableSchema &schema,
                                  const ObTabletID &tablet_id,
                                  ObIAllocator &allocator,
                                  int64_t &part_idx,
                                  int64_t &subpart_idx,
                                  ObString &name)
{
  int ret = OB_SUCCESS;
  part_idx = subpart_idx = OB_INVALID_INDEX;
  name.reset();
  if (!schema.is_partitioned_table()) {
    if (schema.get_tablet_id() != tablet_id) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("refresh tablet does not match nonpartitioned table", K(ret), K(tablet_id),
          "table_id", schema.get_table_id(), "schema_tablet_id", schema.get_tablet_id());
    }
  } else if (OB_FAIL(schema.get_part_idx_by_tablet(tablet_id, part_idx, subpart_idx))) {
    LOG_WARN("failed to locate refresh partition", K(ret), K(tablet_id));
  } else {
    const ObPartition *part = schema.get_part_array()[part_idx];
    const ObString &part_name = PARTITION_LEVEL_TWO == schema.get_part_level()
        ? part->get_subpart_array()[subpart_idx]->get_part_name() : part->get_part_name();
    if (OB_FAIL(ob_write_string(allocator, part_name, name))) {
      LOG_WARN("failed to copy refresh partition name", K(ret), K(tablet_id), K(part_name));
    }
  }
  return ret;
}

static int build_refresh_partition_(const ObTableSchema &data_schema,
                                    const ObTableSchema &index_schema,
                                    const ObTabletID &data_tablet_id,
                                    const ObTabletID &index_tablet_id,
                                    ObIAllocator &allocator,
                                    ObAsyncIndexDataTableInfo &info)
{
  int ret = OB_SUCCESS;
  int64_t data_part = -1, data_subpart = -1, index_part = -1, index_subpart = -1;
  ObAsyncIndexDataTabletInfo tablet_info;
  tablet_info.data_tablet_id_ = data_tablet_id;
  ObString index_name;
  if (OB_FAIL(get_refresh_partition_(data_schema, data_tablet_id, allocator,
      data_part, data_subpart, tablet_info.partition_name_))) {
    LOG_WARN("failed to resolve data refresh partition", K(ret), K(data_tablet_id),
        "data_table_id", data_schema.get_table_id());
  } else if (OB_FAIL(get_refresh_partition_(index_schema, index_tablet_id, allocator,
      index_part, index_subpart, index_name))) {
    LOG_WARN("failed to resolve index refresh partition",
      K(ret), K(index_tablet_id),
        "index_table_id", index_schema.get_table_id());
  } else if (data_schema.get_part_level() != index_schema.get_part_level()
             || data_part != index_part || data_subpart != index_subpart) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("refresh tablets no longer map to the same partition",
        K(ret), K(data_tablet_id), K(index_tablet_id), K(data_part), K(index_part), K(data_subpart), K(index_subpart));
  } else if (OB_FAIL(info.data_tablet_infos_.push_back(tablet_info))) {
    LOG_WARN("failed to append data refresh partition", K(ret), K(data_tablet_id));
  } else if (OB_FAIL(info.index_infos_.at(0).tablet_ids_.push_back(index_tablet_id))) {
    LOG_WARN("failed to append index refresh tablet", K(ret), K(index_tablet_id));
  } else if (OB_FAIL(info.index_infos_.at(0).partition_names_.push_back(index_name))) {
    LOG_WARN("failed to append index refresh partition name", K(ret), K(index_tablet_id), K(index_name));
  }
  return ret;
}

// Build one ObAsyncIndexDataTableInfo from an already-resolved data table schema.
// Called from both get_async_index_refresh_deps (selected table loop) and
// get_single_table_refresh_info (targeted single-table lookup) to avoid
// duplicating the schema-to-info conversion logic.
// A target index/tablet pair selects the direct-refresh path; the default
// builds all indexes and partitions for background scheduling. Both paths
// share column conversion and deep-copy strings into the supplied allocator.
static int build_data_table_info_from_schema_(
    const uint64_t tenant_id,
    const ObTableSchema *data_table_schema,
    ObSchemaGetterGuard &schema_guard,
    ObIAllocator &allocator,
    ObAsyncIndexDataTableInfo &data_table_info,
    const uint64_t target_index_id = OB_INVALID_ID,
    const ObTabletID &target_data_tablet = ObTabletID(),
    const ObTabletID &target_index_tablet = ObTabletID())
{
  int ret = OB_SUCCESS;
  bool target_index_found = false;
  data_table_info.reset(tenant_id);
  const ObMemAttr attr(tenant_id, "AsyncIdxDeps");
  const uint64_t data_table_id = data_table_schema->get_table_id();
  ObAsyncIndexParams async_index_params;
  int params_ret = parse_async_index_params_(
      *data_table_schema, async_index_params, data_table_info.refresh_interval_us_);
  if (OB_SUCCESS != params_ret) {
    LOG_WARN_RET(params_ret, "failed to parse async index params",
                 K(tenant_id), K(data_table_id),
                 "async_index_params", data_table_schema->get_async_index_params());
  }

  // Get database name
  const ObSimpleDatabaseSchema *db_schema = nullptr;
  if (OB_FAIL(schema_guard.get_database_schema(
                  tenant_id, data_table_schema->get_database_id(), db_schema))) {
    LOG_WARN("failed to get database schema", K(ret), K(tenant_id),
             "database_id", data_table_schema->get_database_id());
  } else if (OB_ISNULL(db_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("database schema is null", K(ret), K(tenant_id), K(data_table_id));
  }

  // Fill basic fields and deep-copy names
  if (OB_FAIL(ret)) {
  } else {
    data_table_info.data_table_id_  = data_table_id;
    data_table_info.schema_version_ = data_table_schema->get_schema_version();
    if (OB_FAIL(ob_write_string(allocator, db_schema->get_database_name_str(),
                                data_table_info.database_name_))) {
      LOG_WARN("failed to copy database name", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, data_table_schema->get_table_name_str(),
                                       data_table_info.data_table_name_))) {
      LOG_WARN("failed to copy data table name", K(ret));
    }
  }

  // Enumerate async indexes and build index_infos_
  // select_columns: walk index column ids in storage order, skip shadow columns.
  // Fulltext index aux columns (word_segment, doc_id, …) are HIDDEN generated
  // columns; filtering is_hidden() would drop them, so we skip shadow columns only.
  // rowkey_columns: same rationale -- skip shadow columns only.
  const ObIArray<ObAuxTableMetaInfo> &aux_infos = data_table_schema->get_simple_index_infos();
  for (int64_t j = 0; OB_SUCC(ret) && !target_index_found && j < aux_infos.count(); ++j) {
    const ObAuxTableMetaInfo &aux_info = aux_infos.at(j);
    const bool is_async_search = is_async_search_def_index(aux_info.index_type_);
    if (!is_async_index(aux_info.index_type_)) {
      continue;
    }
    if (OB_INVALID_ID != target_index_id && !is_async_search
        && target_index_id != aux_info.table_id_) {
      continue;
    }
    uint64_t index_table_id = aux_info.table_id_;
    const ObTableSchema *search_def_schema = nullptr;
    const ObTableSchema *index_schema = nullptr;
    if (is_async_search
        && OB_FAIL(schema_guard.get_table_schema(
               tenant_id, aux_info.table_id_, search_def_schema))) {
      LOG_WARN("failed to get async search def schema", K(ret), K(tenant_id), K(aux_info.table_id_));
      break;
    } else if (is_async_search && OB_ISNULL(search_def_schema)) {
      LOG_WARN("async search def schema is null, skip", K(tenant_id), K(aux_info.table_id_));
      continue;
    } else if (is_async_search
               && OB_FAIL(search_def_schema->get_search_data_index_tid(index_table_id))) {
      LOG_WARN("failed to resolve async search data table", K(ret), K(aux_info.table_id_));
      break;
    } else if (OB_INVALID_ID != target_index_id && target_index_id != index_table_id) {
      continue;
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
      LOG_WARN("failed to get index table schema", K(ret), K(tenant_id), K(index_table_id));
      break;
    } else if (OB_ISNULL(index_schema)) {
      LOG_WARN("index table schema is null, skip", K(tenant_id), K(index_table_id));
      continue;
    } else if (index_schema->is_in_deleting()) {
      LOG_INFO("[ASYNC_INDEX] index is being deleted, skip", K(tenant_id), K(index_table_id));
      continue;
    }

    ObAsyncIndexInfo index_info(tenant_id);
    index_info.index_table_id_  = index_table_id;
    index_info.def_index_table_id_ = is_async_search ? aux_info.table_id_ : OB_INVALID_ID;
    index_info.index_type_ = index_schema->get_index_type();
    index_info.is_delete_index_ = is_async_delete_index(index_info.index_type_);
    index_info.is_refresh_on_error_skip_ =
        OB_SUCCESS == params_ret && async_index_params.is_refresh_on_error_skip();
    if (OB_FAIL(ob_write_string(allocator, index_schema->get_table_name_str(),
                                index_info.index_table_name_))) {
      LOG_WARN("failed to copy index table name", K(ret));
      break;
    }

    // select_columns. Async IVF uses main-table hidden generated columns whose
    // ids/names are copied into the physical index schema, so it follows the
    // same INSERT...SELECT metadata path as async FTS/delete.
    ObSEArray<ObColDesc, 16> index_col_ids;
    index_col_ids.set_attr(attr);
    if (OB_FAIL(index_schema->get_column_ids(index_col_ids))) {
      LOG_WARN("failed to get index column ids", K(ret), K(index_table_id));
      break;
    }
    for (int64_t c = 0; OB_SUCC(ret) && c < index_col_ids.count(); ++c) {
      const uint64_t col_id = index_col_ids.at(c).col_id_;
      if (common::is_shadow_column(col_id)) { continue; }
      const ObColumnSchemaV2 *col = index_schema->get_column_schema(col_id);
      if (OB_ISNULL(col)) { continue; }
      ObString col_name;
      if (OB_FAIL(ob_write_string(allocator, col->get_column_name_str(), col_name))) {
        LOG_WARN("failed to copy column name", K(ret)); break;
      } else if (OB_FAIL(index_info.select_columns_.push_back(col_name))) {
        LOG_WARN("failed to push select column", K(ret)); break;
      }
    }

    // FTS/delete project the same columns into the target. Async Search Data is
    // generated from the user columns stored on the Def schema, while its target
    // columns are the canonical Data columns collected above.
    if (OB_SUCC(ret) && is_async_search) {
      ObSEArray<ObString, 8> source_column_names;
      ObSEArray<ObString, 8> col_comments;
      source_column_names.set_attr(attr);
      col_comments.set_attr(attr);
      if (OB_FAIL(ObSearchIndexBuilderUtil::get_search_index_column_name(
          *data_table_schema, *search_def_schema, source_column_names, col_comments))) {
        LOG_WARN("failed to get async search source columns", K(ret),
            K(data_table_id), K(aux_info.table_id_));
      }
      for (int64_t c = 0; OB_SUCC(ret) && c < source_column_names.count(); ++c) {
        ObString column_name;
        if (OB_FAIL(ob_write_string(allocator, source_column_names.at(c), column_name))) {
          LOG_WARN("failed to copy async search source column", K(ret), K(c));
        } else if (OB_FAIL(index_info.source_columns_.push_back(column_name))) {
          LOG_WARN("failed to push async search source column", K(ret), K(c));
        }
      }
      if (OB_SUCC(ret) && OB_UNLIKELY(index_info.source_columns_.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("async search source columns are empty", K(ret),
            K(data_table_id), K(aux_info.table_id_));
      }
    } else if (OB_SUCC(ret)
               && OB_FAIL(index_info.source_columns_.assign(index_info.select_columns_))) {
      LOG_WARN("failed to assign async index source columns", K(ret));
    }

    // For local indexes that need partition key columns (e.g. heap tables, async FTS on
    // partitioned tables), mirror the logic of ObDDLUtil::generate_build_replica_sql():
    // include any data-table column flagged as a partition key.  Without these columns the
    // DDL plan's calc_part_id_expr_ cannot evaluate the partition function at runtime and
    // every tablet-ID lookup fails with OB_HASH_NOT_EXIST (-4201).
    if (OB_SUCC(ret) && index_schema->need_partition_key_for_build_local_index(*data_table_schema)) {
      ObSEArray<ObColDesc, 16> data_col_ids;
      ObSEArray<uint64_t, 8> routing_column_ids;
      data_col_ids.set_attr(attr);
      routing_column_ids.set_attr(attr);
      if (OB_FAIL(data_table_schema->get_column_ids(data_col_ids))) {
        LOG_WARN("failed to get data table column ids for partition key check", K(ret));
      } else {
        for (int64_t c = 0; OB_SUCC(ret) && c < data_col_ids.count(); ++c) {
          const uint64_t col_id = data_col_ids.at(c).col_id_;
          const ObColumnSchemaV2 *col = data_table_schema->get_column_schema(col_id);
          if (OB_NOT_NULL(col) && col->is_tbl_part_key_column()) {
            bool found = false;
            for (int64_t i = 0; !found && i < routing_column_ids.count(); ++i) {
              found = routing_column_ids.at(i) == col_id;
            }
            if (!found && OB_FAIL(routing_column_ids.push_back(col_id))) {
              LOG_WARN("failed to push partition key column id", K(ret), K(col_id));
            } else if (col->is_generated_column()) {
              ObSEArray<uint64_t, 8> cascaded_column_ids;
              cascaded_column_ids.set_attr(attr);
              if (OB_FAIL(col->get_cascaded_column_ids(cascaded_column_ids))) {
                LOG_WARN("failed to get cascaded partition column ids", K(ret), K(col_id));
              }
              for (int64_t i = 0; OB_SUCC(ret) && i < cascaded_column_ids.count(); ++i) {
                const uint64_t cascaded_column_id = cascaded_column_ids.at(i);
                bool cascaded_found = false;
                for (int64_t j = 0; !cascaded_found && j < routing_column_ids.count(); ++j) {
                  cascaded_found = routing_column_ids.at(j) == cascaded_column_id;
                }
                if (!cascaded_found && OB_FAIL(routing_column_ids.push_back(cascaded_column_id))) {
                  LOG_WARN("failed to push cascaded partition column id", K(ret),
                           K(col_id), K(cascaded_column_id));
                }
              }
            }
          }
        }
        for (int64_t c = 0; OB_SUCC(ret) && c < routing_column_ids.count(); ++c) {
          const uint64_t col_id = routing_column_ids.at(c);
          const ObColumnSchemaV2 *col = data_table_schema->get_column_schema(col_id);
          bool found = false;
          if (OB_ISNULL(col)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition routing column schema is null", K(ret), K(col_id));
          } else {
            for (int64_t ci = 0; ci < index_info.select_columns_.count() && !found; ++ci) {
              found = index_info.select_columns_.at(ci) == col->get_column_name_str();
            }
          }
          if (OB_SUCC(ret) && !found) {
            ObString col_name;
            if (OB_FAIL(ob_write_string(allocator, col->get_column_name_str(), col_name))) {
              LOG_WARN("failed to copy partition routing column name", K(ret));
            } else if (OB_FAIL(index_info.select_columns_.push_back(col_name))) {
              LOG_WARN("failed to push partition routing column", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            bool part_key_found = false;
            for (int64_t pi = 0; pi < index_info.partition_key_columns_.count()
                                 && !part_key_found; ++pi) {
              part_key_found = index_info.partition_key_columns_.at(pi)
                  == col->get_column_name_str();
            }
            if (!part_key_found) {
              ObString part_key_name;
              if (OB_FAIL(ob_write_string(
                      allocator, col->get_column_name_str(), part_key_name))) {
                LOG_WARN("failed to copy partition routing column", K(ret));
              } else if (OB_FAIL(index_info.partition_key_columns_.push_back(part_key_name))) {
                LOG_WARN("failed to push partition routing column", K(ret));
              }
            }
          }
        }
      }
    }

    // rowkey_columns (drives ORDER BY; delete index rowkey doubles as docid)
    if (OB_SUCC(ret)) {
      const ObRowkeyInfo &rowkey_info = index_schema->get_rowkey_info();
      for (int64_t rk = 0; OB_SUCC(ret) && rk < rowkey_info.get_size(); ++rk) {
        ObRowkeyColumn rk_col;
        if (OB_FAIL(rowkey_info.get_column(rk, rk_col))) {
          LOG_WARN("failed to get rowkey column", K(ret), K(rk)); break;
        } else if (common::is_shadow_column(rk_col.column_id_)) {
          continue;
        }
        const ObColumnSchemaV2 *rk_schema = index_schema->get_column_schema(rk_col.column_id_);
        if (OB_ISNULL(rk_schema)) { continue; }
        // Keep scanning to collect all rowkey names while accumulating the newsort capability.
        index_info.enable_newsort_ = index_info.enable_newsort_
            && ObOrderPerservingEncoder::can_encode_sortkey(
                rk_schema->get_data_type(), rk_schema->get_collation_type());
        ObString rk_name;
        if (OB_FAIL(ob_write_string(allocator, rk_schema->get_column_name_str(), rk_name))) {
          LOG_WARN("failed to copy rowkey column name", K(ret)); break;
        } else if (OB_FAIL(index_info.rowkey_columns_.push_back(rk_name))) {
          LOG_WARN("failed to push rowkey column", K(ret)); break;
        }
      }
      if (OB_SUCC(ret)
          && OB_FAIL(index_info.order_columns_.assign(index_info.rowkey_columns_))) {
        LOG_WARN("failed to assign async index order columns", K(ret));
      } else if (OB_SUCC(ret)) {
        const ObColumnSchemaV2 *nested_key_column =
            ObSearchIndexBuilderUtil::get_nested_key_column_schema(*index_schema);
        ObString nested_key_name;
        if (OB_NOT_NULL(nested_key_column)) {
          index_info.enable_newsort_ = index_info.enable_newsort_
              && ObOrderPerservingEncoder::can_encode_sortkey(
                  nested_key_column->get_data_type(), nested_key_column->get_collation_type());
        }
        if (OB_NOT_NULL(nested_key_column)
            && OB_FAIL(ob_write_string(
                allocator, nested_key_column->get_column_name_str(), nested_key_name))) {
          LOG_WARN("failed to copy nested key order column name", K(ret));
        } else if (OB_NOT_NULL(nested_key_column)
                   && OB_FAIL(index_info.order_columns_.push_back(nested_key_name))) {
          LOG_WARN("failed to push nested key order column", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(data_table_info.index_infos_.push_back(index_info))) {
        LOG_WARN("failed to push index info", K(ret));
      } else if (OB_INVALID_ID != target_index_id) {
        target_index_found = true;
        if (OB_FAIL(build_refresh_partition_(*data_table_schema, *index_schema,
            target_data_tablet, target_index_tablet, allocator, data_table_info))) {
          LOG_WARN("failed to build targeted refresh partition", K(ret), K(target_index_id),
              K(target_data_tablet), K(target_index_tablet));
        }
      }
    }
  } // end for each aux index

  if (OB_SUCC(ret) && OB_INVALID_ID != target_index_id && !target_index_found) {
    // Keep the direct-refresh dropped-index error semantics.
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("refresh index is absent from current async index targets", K(ret),
        K(tenant_id), K(data_table_id), K(target_index_id));
  }

  if (OB_SUCC(ret) && OB_INVALID_ID == target_index_id) {
    // Build per-data-tablet info and fill index tablet mappings
    ObSEArray<ObTabletID, 16> data_tablet_ids;
    data_tablet_ids.set_attr(attr);
    if (OB_FAIL(data_table_schema->get_tablet_ids(data_tablet_ids))) {
      LOG_WARN("failed to get data tablet ids", K(ret), K(data_table_id));
    }

    const int64_t index_cnt = data_table_info.index_infos_.count();
    ObSEArray<ObSEArray<ObTabletID, 16>, 4> index_tablet_bufs;
    ObSEArray<const ObTableSchema *, 4> index_schemas;
    index_tablet_bufs.set_attr(attr);
    index_schemas.set_attr(attr);
    // Construct the empty buffers together: growing this outer array after
    // filling its elements would copy their arrays with default allocators.
    if (OB_SUCC(ret) && OB_FAIL(index_tablet_bufs.prepare_allocate(index_cnt))) {
      LOG_WARN("failed to init index tablet buffers", K(ret), K(index_cnt));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < index_cnt; ++j) {
      index_tablet_bufs.at(j).set_attr(attr);
      const uint64_t idx_table_id = data_table_info.index_infos_.at(j).index_table_id_;
      const ObTableSchema *idx_schema = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(tenant_id, idx_table_id, idx_schema))) {
        LOG_WARN("failed to get index schema for tablets", K(ret), K(idx_table_id)); break;
      } else if (OB_ISNULL(idx_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema null for tablet fetch", K(ret), K(idx_table_id)); break;
      } else if (OB_FAIL(idx_schema->get_tablet_ids(index_tablet_bufs.at(j)))) {
        LOG_WARN("failed to get index tablet ids", K(ret), K(idx_table_id)); break;
      } else if (OB_FAIL(index_schemas.push_back(idx_schema))) {
        LOG_WARN("failed to push index schema", K(ret), K(idx_table_id)); break;
      }
    }

    const bool is_partitioned = data_table_schema->is_partitioned_table();
    const ObPartitionLevel part_level = data_table_schema->get_part_level();
    for (int64_t t = 0; OB_SUCC(ret) && t < data_tablet_ids.count(); ++t) {
      ObAsyncIndexDataTabletInfo data_tablet_info;
      data_tablet_info.data_tablet_id_ = data_tablet_ids.at(t);
      if (is_partitioned) {
        ObPartition **part_array = data_table_schema->get_part_array();
        if (PARTITION_LEVEL_ONE == part_level) {
          const int64_t part_num = data_table_schema->get_partition_num();
          if (OB_NOT_NULL(part_array) && t < part_num && OB_NOT_NULL(part_array[t])) {
            ObString part_name;
            if (OB_FAIL(ob_write_string(allocator, part_array[t]->get_part_name(), part_name))) {
              LOG_WARN("failed to copy partition name", K(ret)); break;
            }
            data_tablet_info.partition_name_ = part_name;
          }
        } else if (PARTITION_LEVEL_TWO == part_level) {
          int64_t part_idx = -1;
          int64_t subpart_idx = -1;
          if (OB_FAIL(data_table_schema->get_part_idx_by_tablet(
                  data_tablet_ids.at(t), part_idx, subpart_idx))) {
            LOG_WARN("failed to get part idx by tablet for subpartition", K(ret),
                     K(data_tablet_ids.at(t))); break;
          } else if (OB_ISNULL(part_array) || OB_ISNULL(part_array[part_idx])) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part array or partition is null", K(ret), K(part_idx)); break;
          } else {
            ObSubPartition **subpart_array = part_array[part_idx]->get_subpart_array();
            if (OB_ISNULL(subpart_array) || OB_ISNULL(subpart_array[subpart_idx])) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("subpart array or subpartition is null", K(ret),
                       K(part_idx), K(subpart_idx)); break;
            } else {
              ObString part_name;
              if (OB_FAIL(ob_write_string(allocator,
                      subpart_array[subpart_idx]->get_part_name(), part_name))) {
                LOG_WARN("failed to copy subpartition name", K(ret)); break;
              }
              data_tablet_info.partition_name_ = part_name;
            }
          }
        }
      }
      if (OB_FAIL(ret)) { break; }
      else if (OB_FAIL(data_table_info.data_tablet_infos_.push_back(data_tablet_info))) {
        LOG_WARN("failed to push data tablet info", K(ret)); break;
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < index_tablet_bufs.count(); ++j) {
        const ObSEArray<ObTabletID, 16> &idx_tablets = index_tablet_bufs.at(j);
        if (t >= idx_tablets.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("index tablet array size mismatch", K(ret), K(t),
                   "idx_count", idx_tablets.count());
          break;
        }
        ObAsyncIndexInfo &idx_info = data_table_info.index_infos_.at(j);
        if (OB_FAIL(idx_info.tablet_ids_.push_back(idx_tablets.at(t)))) {
          LOG_WARN("failed to push index tablet id", K(ret)); break;
        }
        // For the index partition name, locate the subpartition name from the
        // index schema itself (local indexes mirror the data table's partition
        // structure but have their own partition names).
        ObString idx_part_name;
        if (is_partitioned) {
          const ObTableSchema *idx_schema = index_schemas.at(j);
          if (PARTITION_LEVEL_ONE == part_level) {
            ObPartition **idx_part_array = idx_schema->get_part_array();
            const int64_t idx_part_num = idx_schema->get_partition_num();
            if (OB_NOT_NULL(idx_part_array) && t < idx_part_num && OB_NOT_NULL(idx_part_array[t])) {
              if (OB_FAIL(ob_write_string(allocator, idx_part_array[t]->get_part_name(), idx_part_name))) {
                LOG_WARN("failed to copy index partition name", K(ret)); break;
              }
            }
          } else if (PARTITION_LEVEL_TWO == part_level) {
            int64_t idx_part_idx = -1;
            int64_t idx_subpart_idx = -1;
            if (OB_FAIL(idx_schema->get_part_idx_by_tablet(
                    idx_tablets.at(t), idx_part_idx, idx_subpart_idx))) {
              LOG_WARN("failed to get index part idx by tablet", K(ret),
                       K(idx_tablets.at(t))); break;
            } else {
              ObPartition **idx_part_array = idx_schema->get_part_array();
              if (OB_ISNULL(idx_part_array) || OB_ISNULL(idx_part_array[idx_part_idx])) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("index part array or partition is null", K(ret),
                         K(idx_part_idx), K(idx_tablets.at(t))); break;
              } else {
                ObSubPartition **idx_subpart_array = idx_part_array[idx_part_idx]->get_subpart_array();
                if (OB_ISNULL(idx_subpart_array) || OB_ISNULL(idx_subpart_array[idx_subpart_idx])) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("index subpart array or subpartition is null", K(ret),
                           K(idx_part_idx), K(idx_subpart_idx), K(idx_tablets.at(t))); break;
                } else if (OB_FAIL(ob_write_string(allocator,
                        idx_subpart_array[idx_subpart_idx]->get_part_name(), idx_part_name))) {
                  LOG_WARN("failed to copy index subpartition name", K(ret)); break;
                }
              }
            }
          }
        }
        if (OB_FAIL(ret)) { break; }
        if (OB_FAIL(idx_info.partition_names_.push_back(idx_part_name))) {
          LOG_WARN("failed to push index partition name", K(ret)); break;
        }
      }
    }
  }
  return ret;
}

void ObAsyncIndexRefreshMetadataCache::reset()
{
  for (int64_t i = 0; i < entries_.count(); ++i) {
    entries_.at(i)->~ObAsyncIndexDataTableInfo();
  }
  entries_.reset();
  allocator_.reset();
  tenant_id_ = OB_INVALID_TENANT_ID;
  data_table_id_ = OB_INVALID_ID;
  data_tablet_id_.reset();
  schema_version_ = 0;
}

int ObAsyncIndexRefreshMetadataCache::get(const uint64_t tenant_id,
    const uint64_t data_table_id, const ObTabletID &data_tablet_id,
    const uint64_t index_table_id, const ObTabletID &index_tablet_id,
    const ObAsyncIndexDataTableInfo *&info)
{
  int ret = OB_SUCCESS;
  info = nullptr;
  ObSchemaGetterGuard guard;
  int64_t version = 0;
  if (OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id
      || OB_INVALID_ID == index_table_id || !data_tablet_id.is_valid() || !index_tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid refresh metadata arguments", K(ret), K(tenant_id), K(data_table_id),
        K(data_tablet_id), K(index_table_id), K(index_tablet_id));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(tenant_id, guard))) {
    LOG_WARN("failed to get refresh metadata schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(guard.get_schema_version(tenant_id, version))) {
    LOG_WARN("failed to get refresh metadata snapshot version", K(ret), K(tenant_id));
  } else if (version <= 0) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("invalid refresh metadata snapshot version", K(ret), K(tenant_id), K(version));
  } else {
    // Use this same guard to build misses. A new guard is required on every call.
    // Tenant snapshot invalidation also covers database and Search Def changes.
    if (tenant_id_ != tenant_id || data_table_id_ != data_table_id
        || data_tablet_id_ != data_tablet_id || schema_version_ != version) {
      reset();
      allocator_.set_attr(ObMemAttr(tenant_id, "AsyncIdxMeta"));
      entries_.set_attr(ObMemAttr(tenant_id, "AsyncIdxMeta"));
      tenant_id_ = tenant_id;
      data_table_id_ = data_table_id;
      data_tablet_id_ = data_tablet_id;
      schema_version_ = version;
    }
    for (int64_t i = 0; nullptr == info && i < entries_.count(); ++i) {
      const ObAsyncIndexInfo &index = entries_.at(i)->index_infos_.at(0);
      if (index.index_table_id_ == index_table_id && index.tablet_ids_.at(0) == index_tablet_id) {
        info = entries_.at(i);
      }
    }
    if (nullptr == info) {
      const ObTableSchema *data_schema = nullptr;
      ObAsyncIndexDataTableInfo *entry = nullptr;
      void *buf = nullptr;
      if (OB_FAIL(guard.get_table_schema(tenant_id, data_table_id, data_schema))) {
        LOG_WARN("failed to get refresh data schema", K(ret), K(data_table_id));
      } else if (OB_ISNULL(data_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("refresh data schema does not exist", K(ret), K(tenant_id), K(data_table_id));
      } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObAsyncIndexDataTableInfo)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate refresh metadata", K(ret), K(tenant_id), K(data_table_id), K(index_table_id));
      } else {
        entry = new (buf) ObAsyncIndexDataTableInfo();
        if (OB_FAIL(build_data_table_info_from_schema_(tenant_id, data_schema, guard,
            allocator_, *entry, index_table_id, data_tablet_id, index_tablet_id))) {
          LOG_WARN("failed to build targeted refresh metadata", K(ret), K(data_table_id), K(index_table_id));
        } else if (OB_FAIL(entries_.push_back(entry))) {
          LOG_WARN("failed to cache refresh metadata", K(ret));
        } else {
          info = entry;
        }
        if (OB_FAIL(ret)) {
          entry->~ObAsyncIndexDataTableInfo();
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObAsyncIndexDepProvider::get_async_index_refresh_deps(
    const uint64_t tenant_id,
    const ObIArray<uint64_t> &data_table_ids,
    ObAsyncIndexRefreshDeps &refresh_deps)
{
  int ret = OB_SUCCESS;
  refresh_deps.reset();
  ObSchemaGetterGuard schema_guard;
  common::ObArenaAllocator &allocator = refresh_deps.allocator_;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K(tenant_id));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(refresh_deps.prepare(tenant_id, data_table_ids.count()))) {
    LOG_WARN("failed to reserve async index refresh dependencies", K(ret), K(tenant_id),
             "table_count", data_table_ids.count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_table_ids.count(); ++i) {
      const uint64_t data_table_id = data_table_ids.at(i);
      const ObTableSchema *data_table_schema = nullptr;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
        LOG_WARN("failed to get data table schema, skip this table", K(tmp_ret), K(tenant_id), K(data_table_id));
      } else if (OB_ISNULL(data_table_schema)) {
        LOG_WARN("data table schema is null, skip", K(tenant_id), K(data_table_id));
      } else {
        ObAsyncIndexDataTableInfo data_table_info(tenant_id);
        if (OB_TMP_FAIL(build_data_table_info_from_schema_(
                tenant_id, data_table_schema, schema_guard, allocator,
                data_table_info))) {
          LOG_WARN("failed to build data table info, skip this table", K(tmp_ret), K(data_table_id));
        } else if (data_table_info.is_valid()
                   && OB_FAIL(refresh_deps.data_table_infos_.push_back(data_table_info))) {
          LOG_WARN("failed to push data table info", K(ret), K(data_table_id));
        }
      }
    }
  }
  LOG_INFO("get selected async index refresh deps done", K(ret), K(tenant_id),
           "selected_table_cnt", data_table_ids.count(),
           "data_table_cnt", refresh_deps.data_table_infos_.count());
  return ret;
}

int ObAsyncIndexDepProvider::get_single_table_refresh_info(
    const uint64_t tenant_id,
    const uint64_t data_table_id,
    common::ObIAllocator &allocator,
    ObAsyncIndexDataTableInfo &data_table_info)
{
  int ret = OB_SUCCESS;
  data_table_info.reset();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == data_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(data_table_id));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
    LOG_WARN("failed to get data table schema", K(ret), K(data_table_id));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data table schema not found", K(ret), K(data_table_id));
  } else if (OB_FAIL(build_data_table_info_from_schema_(
                         tenant_id, data_table_schema, schema_guard, allocator,
                         data_table_info))) {
    LOG_WARN("failed to build data table info", K(ret), K(data_table_id));
  }
  return ret;
}

/**
 * Resolve all available async-index targets of one data partition and return
 * their minimum published SCN. The shared async-delete target is included in
 * the table refresh metadata and therefore participates in the minimum.
 *
 * @param tenant_id Tenant owning the data and async-index tablets.
 * @param ls LS used to read each target tablet's published version.
 * @param data_table_id Data table whose async targets are resolved.
 * @param data_tablet_id Data partition used to select corresponding tablets.
 * @param has_async_index Output true when at least one available target exists.
 * @param min_published_scn Output safety point across all available targets.
 */
int ObAsyncIndexDepProvider::get_partition_min_async_published_scn(
    const uint64_t tenant_id,
    ObLS *ls,
    const uint64_t data_table_id,
    const ObTabletID &data_tablet_id,
    bool &has_async_index,
    SCN &min_published_scn)
{
  int ret = OB_SUCCESS;
  has_async_index = false;
  min_published_scn.reset();
  ObArenaAllocator allocator(ObMemAttr(tenant_id, "AsyncIdxSafe"));
  ObAsyncIndexDataTableInfo data_table_info;
  ObSchemaGetterGuard schema_guard;
  int64_t partition_pos = -1;
  ObSEArray<ObTabletID, 8> index_tablet_ids;
  ObSEArray<SCN, 8> publish_scns;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || OB_INVALID_ID == data_table_id
                  || !data_tablet_id.is_valid())
      || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(ls),
             K(data_table_id), K(data_tablet_id));
  } else if (OB_FAIL(get_single_table_refresh_info(
                 tenant_id, data_table_id, allocator, data_table_info))) {
    LOG_WARN("failed to get async index refresh info", K(ret),
             K(data_table_id), K(data_tablet_id));
  } else if (OB_FAIL(GSCHEMASERVICE.get_tenant_schema_guard(
                 tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(tenant_id));
  } else {
    // All target tablet arrays use the data-table partition ordinal.
    for (int64_t i = 0; i < data_table_info.data_tablet_infos_.count()
                        && partition_pos < 0; ++i) {
      if (data_table_info.data_tablet_infos_.at(i).data_tablet_id_
          == data_tablet_id) {
        partition_pos = i;
      }
    }
    if (OB_UNLIKELY(partition_pos < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data tablet is missing from async index refresh info", K(ret),
               K(data_table_id), K(data_tablet_id), K(data_table_info));
    }
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && i < data_table_info.index_infos_.count(); ++i) {
    const ObAsyncIndexInfo &index_info = data_table_info.index_infos_.at(i);
    const ObTableSchema *index_schema = nullptr;
    if (OB_FAIL(schema_guard.get_table_schema(
            tenant_id, index_info.index_table_id_, index_schema))) {
      LOG_WARN("failed to get async index schema", K(ret), K(index_info));
    } else if (OB_ISNULL(index_schema)) {
      ret = OB_SCHEMA_EAGAIN;
      LOG_WARN("async index schema disappeared while collecting safety point",
               K(ret), K(index_info));
    } else if (INDEX_STATUS_AVAILABLE != index_schema->get_index_status()) {
      // Building or dropping targets cannot define the current query safety point.
      LOG_DEBUG("skip unavailable async index target", K(index_info));
    } else if (OB_UNLIKELY(partition_pos >= index_info.tablet_ids_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("async index tablet mapping is incomplete", K(ret), K(partition_pos),
               K(index_info), K(data_table_id), K(data_tablet_id));
    } else if (OB_FAIL(index_tablet_ids.push_back(
                   index_info.tablet_ids_.at(partition_pos)))) {
      LOG_WARN("failed to collect async index tablet", K(ret), K(index_info));
    }
  }
  if (OB_SUCC(ret)) {
    has_async_index = !index_tablet_ids.empty();
  }
  if (OB_SUCC(ret) && has_async_index
      && OB_FAIL(get_publish_scns(
          ls, index_tablet_ids, publish_scns))) {
    LOG_WARN("failed to get async index publish scns", K(ret),
             K(data_table_id), K(data_tablet_id), K(index_tablet_ids));
  } else if (OB_SUCC(ret) && has_async_index
             && OB_UNLIKELY(publish_scns.count() != index_tablet_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index publish scn count mismatch", K(ret),
             K(index_tablet_ids), K(publish_scns));
  }
  for (int64_t i = 0;
       OB_SUCC(ret) && has_async_index && i < publish_scns.count(); ++i) {
    const SCN &publish_scn = publish_scns.at(i);
    if (OB_UNLIKELY(!publish_scn.is_valid_and_not_min())) {
      // A target without a published baseline is not safe for vector freezing.
      ret = OB_EAGAIN;
      LOG_WARN("async index target has no published baseline", K(ret),
               "index_tablet_id", index_tablet_ids.at(i), K(publish_scn),
               K(data_table_id), K(data_tablet_id));
    } else {
      min_published_scn = min_published_scn.is_valid()
          ? SCN::min(min_published_scn, publish_scn)
          : publish_scn;
      LOG_DEBUG("collect async index published scn", K(data_table_id),
                K(data_tablet_id), "index_tablet_id", index_tablet_ids.at(i),
                K(publish_scn));
    }
  }
  if (OB_SUCC(ret) && has_async_index
      && OB_UNLIKELY(!min_published_scn.is_valid_and_not_min())) {
    ret = OB_EAGAIN;
    LOG_WARN("failed to get valid async index safety point", K(ret),
             K(data_table_id), K(data_tablet_id), K(min_published_scn));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
