/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX MDS

#include "storage/mds_table/ob_tablet_mds_reloc_helper.h"
#include "storage/mds_table/ob_tablet_mds_logical_reloc.h"
#include "storage/mds_table/ob_tablet_mds_reloc_batch_arg.h"
#include "storage/mds_table/ob_tablet_mds_reloc_traits.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/mds_table/ob_mds_multikey_reader.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_info_mds_user_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_mds_reloc_status.h"
#include "storage/tx/ob_tx_log.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/high_availability/ob_fork_table_service.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "storage/high_availability/ob_ss_transfer_backfill_utils.h"
#include "storage/high_availability/ob_ls_migration_handler.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_errno.h"
#include "storage/mds_table/ob_mds_type_registry.h"
#include "storage/ob_relative_table.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_tracepoint.h"

namespace oceanbase
{
namespace storage
{

using namespace mds;

#ifdef ERRSIM
ERRSIM_POINT_DEF(EN_MDS_RELOC_SHARED_CHECKPOINT_READ);
ERRSIM_POINT_DEF(EN_MDS_RELOC_LOCAL_CHECKPOINT_READ);
ERRSIM_POINT_DEF(EN_MDS_RELOC_CHECKPOINT_FLUSH);
#endif


// Unified apply of one materialized reloc row: deserialize [key + value],
// write/replay to all targets.  DummyKey rows use the single-row overload of
// ObMdsTable::write/replay; multi-key rows pass the deserialized key.
template <typename K, typename V>
int ObTabletMdsRelocHelper::apply_reloc_row_(
    const bool for_replay,
    const share::SCN &replay_scn,
    ObMdsTable *mds_table,
    ObTabletMdsRelocBatchArg &arg,
    const ObTabletMdsRelocRowStorage &row,
    MdsCtx &user_ctx,
    common::ObIAllocator &deser_alloc,
    const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  const char *payload = row.ser_payload_.ptr();
  const int64_t payload_len = row.ser_payload_.length();
  // Deserialize the value with or without an allocator depending on whether V
  // records its allocator (ObTabletAutoincSeq routes to the allocator-aware
  // deserialize; see ObMdsUserDataDeserializeNeedsAlloc).
  auto deser_value = [](V &v, common::ObIAllocator &alloc, const char *buf, const int64_t len, int64_t &pos) -> int {
    int ret = OB_SUCCESS;
    if constexpr (storage::ObMdsUserDataDeserializeNeedsAlloc<V>::value) {
      ret = v.deserialize(alloc, buf, len, pos);
    } else {
      UNUSED(alloc);
      ret = v.deserialize(buf, len, pos);
    }
    return ret;
  };
  if (OB_UNLIKELY(nullptr == payload || payload_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("reloc payload too short", KR(ret), K(payload_len));
  } else if (for_replay && OB_UNLIKELY(!replay_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("replay scn invalid", KR(ret));
  } else {
    int64_t pos = 0;
    K key;
    V val;
    if (OB_FAIL(key.mds_deserialize(payload, payload_len, pos))) {
      LOG_WARN("reloc key deserialize failed", KR(ret));
    } else if (OB_FAIL(deser_value(val, deser_alloc, payload, payload_len, pos))) {
      LOG_WARN("reloc value deserialize failed", KR(ret), "mds_unit_id", row.mds_unit_id_);
    } else if (OB_UNLIKELY(pos != payload_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reloc payload tail not consumed", KR(ret), K(pos), K(payload_len));
    } else {
      for (int64_t tgt_i = 0; OB_SUCC(ret) && tgt_i < arg.targets_.count(); ++tgt_i) {
        if (arg.skip_bitmap_.is_skipped(row_idx, tgt_i)) {
          continue;
        }
        const ObTabletMdsRelocTarget &tgt = arg.targets_.at(tgt_i);
        int rv = OB_SUCCESS;
        if constexpr (std::is_same_v<K, mds::DummyKey>) {
          rv = for_replay
              ? mds_table->replay(tgt.tablet_id_, tgt.transfer_scn_, val, row.meta_, user_ctx, replay_scn)
              : mds_table->write(tgt.tablet_id_,
                                 tgt.transfer_scn_,
                                 val,
                                 row.meta_,
                                 *user_ctx.get_store_ctx(),
                                 user_ctx,
                                 true);
        } else {
          rv = for_replay
              ? mds_table->replay(tgt.tablet_id_, tgt.transfer_scn_, key, val, row.meta_, user_ctx, replay_scn)
              : mds_table->write(tgt.tablet_id_,
                                 tgt.transfer_scn_,
                                 key,
                                 val,
                                 row.meta_,
                                 *user_ctx.get_store_ctx(),
                                 user_ctx,
                                 true);
        }
        // A committed duplicate of the same row is tolerated (only the
        // winner's copy stays) and marked in the skip bitmap for retries.
        const bool is_committed_duplicate = (common::OB_ERR_PRIMARY_KEY_DUPLICATE == rv);
        ret = is_committed_duplicate ? common::OB_SUCCESS : rv;
        if (is_committed_duplicate) {
          if (!for_replay) {
            arg.skip_bitmap_.set_skip(row_idx, tgt_i);
          }
        } else if (OB_FAIL(ret)) {
          LOG_WARN("mds_table apply reloc row failed", KR(ret), K(tgt), "mds_unit_id", row.mds_unit_id_);
        }
      }
    }
  }
  return ret;
}

// The marker lives in the dedicated ObTabletMdsRelocStatus unit (NOT the
// tablet-status unit), so tablet-status commit gates never observe an
// uncommitted row owned by the reloc transaction. Generation isolation is
// structural: its row key is (tablet_id, destination reorg SCN). V2 marker
// protocol (mutex INSERT then fence PUT) is defined on ObTabletMdsRelocStatus.
int ObTabletMdsRelocHelper::apply_reloc_marker_(
    const bool for_replay,
    const bool acquire_mutex,
    const share::SCN &replay_scn,
    ObMdsTable *mds_table,
    ObTabletMdsRelocBatchArg &arg,
    MdsCtx &user_ctx)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "RelocMarker"));
  const share::SCN read_snapshot = share::SCN::max_scn();
  const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.targets_.count(); ++i) {
    const ObTabletMdsRelocTarget &tgt = arg.targets_.at(i);
    ObTabletCreateDeleteMdsUserData user_data;
    ObMdsValueMeta meta;
    if (OB_FAIL(mds_table->get<ObTabletCreateDeleteMdsUserData>(
            &allocator,
            tgt.tablet_id_,
            tgt.transfer_scn_,
            read_snapshot,
            timeout_us,
            false /*read_uncommitted*/,
            user_data,
            meta))) {
      if (for_replay && (OB_ITER_END == ret || OB_EMPTY_RESULT == ret)) {
        // Neither CREATE_TABLET_NEW_MDS nor LS_MDS_TABLET_RELOC_BATCH has a replay barrier.
        // A standby tenant may replay a reloc marker before the destination tablet's
        // committed status. Return OB_EAGAIN and retry after the status has been replayed.
        ret = OB_EAGAIN;
        LOG_WARN("wait for committed tablet status before replaying reloc marker",
                 KR(ret), K(tgt), K(replay_scn));
      } else {
        LOG_WARN("read latest committed tablet status for reloc marker failed",
                 KR(ret), K(tgt), K(for_replay), K(replay_scn));
      }
    } else if ((ObTabletMdsRelocType::TRANSFER == arg.reloc_type_
                && ObTabletStatus::TRANSFER_IN != user_data.tablet_status_)
               || (ObTabletMdsRelocType::SPLIT == arg.reloc_type_
                   && ObTabletStatus::SPLIT_DST != user_data.tablet_status_)
               || (ObTabletMdsRelocType::FORK == arg.reloc_type_
                   && ObTabletStatus::FORK_DST != user_data.tablet_status_)) {
      ret = OB_EAGAIN;
      LOG_WARN("reloc type does not match tablet status for marker",
               KR(ret), K(arg.reloc_type_), K(tgt), K(user_data), K(for_replay), K(replay_scn));
    } else {
      ObTabletMdsRelocStatus reloc_done_data;
      reloc_done_data.set_reloc_done();
      if (arg.is_v2()) {
        reloc_done_data.enable_redo_fence();
      }
      // V2's leading write is an INSERT mutex (check_dup).  V1 and the V2 final
      // fence keep DF_PUT so a duplicate committed marker stacks a new version.
      // INSERT conflicts map to OB_ALREADY_DONE / OB_EAGAIN (abort + retry);
      // the retry's done-check short-circuits once the winning reloc's marker
      // has committed:
      //   - OB_ERR_PRIMARY_KEY_DUPLICATE: a committed marker already exists.
      //   - OB_TRY_LOCK_ROW_CONFLICT / OB_TRANSACTION_SET_VIOLATION: a concurrent
      //     reloc holds the uncommitted marker.
      // Replay is single-stream and idempotent by scn, so it uses the plain path.
      const bool check_dup = acquire_mutex;
      const int apply_ret = for_replay
          ? mds_table->replay(tgt.tablet_id_,
                              tgt.transfer_scn_,
                              reloc_done_data,
                              user_ctx,
                              replay_scn)
          : mds_table->write(tgt.tablet_id_,
                             tgt.transfer_scn_,
                             reloc_done_data,
                             *user_ctx.get_store_ctx(),
                             user_ctx,
                             check_dup);
      if (!for_replay && check_dup) {
        ret = handle_marker_write_ret_(acquire_mutex, arg, tgt, replay_scn, apply_ret);
      } else if (OB_SUCCESS != apply_ret) {
        ret = apply_ret;
        LOG_WARN("apply reloc marker failed",
                 KR(ret), K(tgt), K(acquire_mutex), K(arg.is_final_), K(for_replay), K(replay_scn));
      }
    }
    allocator.reuse();
  }
  return ret;
}

int ObTabletMdsRelocHelper::handle_marker_write_ret_(
    const bool acquire_mutex,
    const ObTabletMdsRelocBatchArg &arg,
    const ObTabletMdsRelocTarget &tgt,
    const share::SCN &replay_scn,
    const int apply_ret)
{
  int ret = OB_SUCCESS;
  if (common::OB_ERR_PRIMARY_KEY_DUPLICATE == apply_ret) {
    // A COMMITTED marker already exists for (tablet, transfer_scn): a
    // concurrent reloc has already completed the whole atomic reloc. Stop
    // this loser instead of continuing to payload/pull: after the winner's
    // marker is persisted, the source may already have been invalidated.
    // OB_ALREADY_DONE is an internal control result: it makes the current
    // SQL transaction abort (rolling back any writes already registered by
    // this attempt), then the submitter normalizes it to external success.
    // Do not use OB_EAGAIN here; retrying the same completed reloc can
    // livelock the transfer/balance task.
    ret = OB_ALREADY_DONE;
    LOG_INFO("reloc marker already committed by another reloc; abort loser as done",
             "conflict_ret", apply_ret, K(acquire_mutex), K(tgt), K(replay_scn));
    SERVER_EVENT_ADD(reloc_event_module(arg.reloc_type_), "MDS_RELOC_MARKER_ALREADY_DONE",
        "ls_id", arg.ls_id_.id(),
        "tablet_id", tgt.tablet_id_.id(),
        "transfer_scn", tgt.transfer_scn_.get_val_for_tx(true /*ignore_invalid*/),
        "conflict_ret", apply_ret,
        "is_mutex_batch", acquire_mutex);
  } else if (common::OB_TRY_LOCK_ROW_CONFLICT == apply_ret
      || common::OB_TRANSACTION_SET_VIOLATION == apply_ret) {
    ret = OB_EAGAIN;
    LOG_INFO("reloc marker mutex conflict with uncommitted row; will retry",
             "conflict_ret", apply_ret, K(tgt), K(replay_scn));
    SERVER_EVENT_ADD(reloc_event_module(arg.reloc_type_), "MDS_RELOC_MARKER_CONFLICT",
        "ls_id", arg.ls_id_.id(),
        "tablet_id", tgt.tablet_id_.id(),
        "transfer_scn", tgt.transfer_scn_.get_val_for_tx(true /*ignore_invalid*/),
        "conflict_ret", apply_ret);
  } else if (OB_SUCCESS != apply_ret) {
    ret = apply_ret;
    LOG_WARN("apply reloc marker failed",
             KR(ret), K(tgt), K(acquire_mutex), K(arg.is_final_), K(replay_scn));
  }
  return ret;
}

int ObTabletMdsRelocHelper::apply_rows_(
    const bool for_replay,
    const share::SCN &replay_scn,
    ObTabletMdsRelocBatchArg &arg,
    BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  MdsCtx &user_ctx = static_cast<MdsCtx &>(ctx);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObMdsTable *mds_table = nullptr;
  share::SCN mds_ckpt_scn;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls mds reloc batch arg", KR(ret), K(arg));
  } else if (!for_replay && OB_UNLIKELY(nullptr == user_ctx.get_store_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls mds reloc batch register requires store_ctx on MdsCtx", KR(ret));
  } else if (for_replay && OB_UNLIKELY(nullptr == user_ctx.get_mem_ctx())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls mds reloc batch replay requires mem_ctx on MdsCtx", KR(ret));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("get ls failed", KR(ret), K(arg.ls_id_));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", KR(ret), K(arg.ls_id_));
  } else if (OB_UNLIKELY(ls->get_ls_id() != arg.ls_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls id mismatch", KR(ret), K(arg.ls_id_), K(ls->get_ls_id()));
  } else if (OB_ISNULL(mds_table = ls->get_mds_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mds table is null", KR(ret));
  } else if (for_replay && OB_FAIL(mds_table->get_mds_checkpoint_scn(mds_ckpt_scn))) {
    LOG_WARN("failed to get mds table checkpoint scn", KR(ret));
  } else if (for_replay && replay_scn <= mds_ckpt_scn) {
    ret = OB_NO_NEED_UPDATE;
    LOG_INFO("skip replay for mds table checkpoint", KR(ret), K(arg.ls_id_), K(replay_scn), K(mds_ckpt_scn));
  } else if (for_replay && arg.is_final_ && arg.is_v2()
             && OB_FAIL(probe_source_ready_(arg))) {
    if (OB_EAGAIN != ret) {
      LOG_WARN("probe reloc source before apply failed", KR(ret), K(for_replay), K(replay_scn), K(arg));
    }
    // EAGAIN: return without writing any row.  Each EAGAIN-retry would
    // otherwise materialize rows into the MDS memtable first and only fail at
    // the final pull; the rolled-back rows leave stranded nodes that are
    // reclaimed only when the memtable is released, so a retry loop grows
    // MdsMemstore without bound (see Dima 2026092100119283766).  Probing is
    // read-only, so a not-yet-ready source costs no memtable memory.
  }
  {
    ObRelativeTable relative_table;
    ObMdsRelativeTableGuard relative_table_guard(ctx, relative_table);
    if (OB_SUCC(ret) && arg.is_marker_lock_batch()
        && OB_FAIL(apply_reloc_marker_(
            for_replay, true /*acquire_mutex*/, replay_scn, mds_table, arg, user_ctx))) {
      if (OB_ALREADY_DONE != ret) {
        LOG_WARN("apply reloc marker mutex failed", KR(ret), K(for_replay), K(replay_scn), K(arg));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.rows_.count(); ++i) {
      const ObTabletMdsRelocRowStorage &row = arg.rows_.at(i);
      if (OB_UNLIKELY(nullptr == row.ser_payload_.ptr() || row.ser_payload_.length() <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid reloc payload", KR(ret), K(row));
      } else {
        common::ObArenaAllocator deser_alloc(common::ObMemAttr(MTL_ID(), "RelocApply"));
        bool dispatched = false;
        // Eligibility is checked per row before dispatch: a batch must only
        // carry the units that belong to its reloc type.  A malformed or
        // future batch carrying e.g. a TRANSFER-only unit inside a FORK batch
        // is rejected instead of being applied to the destination.
        bool unit_type_allowed = false;
        bool unit_type_matched = false;
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
        if (OB_SUCC(ret) && !dispatched \
            && row.mds_unit_id_ == static_cast<uint8_t>( \
                   mds_type_id_detail::TypeIdOf<KEY_TYPE, VALUE_TYPE>::value)) { \
          unit_type_matched = true; \
          unit_type_allowed = \
              (ObTabletMdsRelocType::TRANSFER == arg.reloc_type_ && (TRANSFER_RELOC)) \
              || (ObTabletMdsRelocType::SPLIT == arg.reloc_type_ && (SPLIT_RELOC)) \
              || (ObTabletMdsRelocType::FORK == arg.reloc_type_ \
                  && std::is_same<VALUE_TYPE, ObTTLFilterInfo>::value); \
          if (unit_type_allowed && arg.is_v2() && ObIsRelocOpType<KEY_TYPE>::value) { \
            /* V2 submit skips shipping multi-key op rows; do not apply them. */ \
            dispatched = true; \
            ret = OB_INVALID_ARGUMENT; \
            LOG_WARN("v2 reloc batch must not carry materialized multi-key op rows", \
                     KR(ret), K(row.mds_unit_id_), K(arg.reloc_type_)); \
          } else if (unit_type_allowed && ((TRANSFER_RELOC) || (SPLIT_RELOC))) { \
            dispatched = true; \
            ret = apply_reloc_row_<KEY_TYPE, VALUE_TYPE>( \
                for_replay, replay_scn, mds_table, arg, row, user_ctx, deser_alloc, i); \
          } \
        }
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE
        if (OB_UNLIKELY(!unit_type_matched)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unsupported mds_unit_id in reloc batch", KR(ret), K(row.mds_unit_id_));
        } else if (OB_UNLIKELY(!unit_type_allowed)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("mds unit is not eligible for this reloc type", KR(ret),
                   K(row.mds_unit_id_), K(arg.reloc_type_));
        } else if (OB_SUCC(ret) && !dispatched) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("eligible mds unit was not applied in reloc batch", KR(ret),
                   K(row.mds_unit_id_), K(arg.reloc_type_));
        } else if (dispatched && OB_FAIL(ret)) {
          LOG_WARN("apply reloc row failed", KR(ret), K(row.mds_unit_id_));
        }
      }
    }
    // V2 op-semantic pull: before writing the done marker on the final batch,
    // pull the selected multi-key rows from the local source LS replica and
    // apply them to the destination.  May return OB_EAGAIN
    // (source not yet replayed up to the reloc snapshot); replay retries.
    if (OB_SUCC(ret) && arg.is_final_ && arg.is_v2()
        && OB_FAIL(pull_and_apply_op_rows_(
            for_replay, replay_scn, mds_table, arg, user_ctx))) {
      if (OB_EAGAIN != ret) {
        LOG_WARN("pull and apply op rows failed", KR(ret), K(for_replay), K(replay_scn), K(arg));
      }
    }
    if (OB_SUCC(ret) && arg.is_final_
        && OB_FAIL(apply_reloc_marker_(
            for_replay, false /*acquire_mutex*/, replay_scn, mds_table, arg, user_ctx))) {
      if (OB_ALREADY_DONE != ret) {
        LOG_WARN("apply reloc final marker failed", KR(ret), K(for_replay), K(replay_scn), K(arg));
      }
    }
  }
  LOG_INFO("apply reloc batch finish", KR(ret), K(for_replay), K(replay_scn),
           K(arg.rows_.count()), K(arg.is_final_), "is_marker_lock", arg.is_marker_lock_batch(),
           K(arg.ls_id_));
  return ret;
}

int ObTabletMdsRelocHelper::on_register(
    const char *buf,
    const int64_t len,
    BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTabletMdsRelocBatchArg arg;
  int64_t pos = 0;
  if (OB_UNLIKELY(nullptr == buf || len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(buf), K(len));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("deserialize ls mds reloc batch failed", KR(ret));
  } else if (OB_UNLIKELY(len != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch arg tail not consumed", KR(ret), K(len), K(pos));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch arg invalid", KR(ret), K(arg));
  } else if (OB_FAIL(apply_rows_(false /*for_replay*/, share::SCN::invalid_scn(), arg, ctx))) {
    if (OB_ALREADY_DONE != ret) {
      LOG_WARN("apply reloc rows on register failed", KR(ret), K(arg));
    }
  }
  // Partially overwrite the skip-bitmap portion in the redo buffer if any
  // skip bits were set during apply_rows_.  Only the bitmap elements are
  // touched; the rest of the buffer (version/reloc_type/ls_id/targets/rows) is untouched.
  if (OB_SUCC(ret) && arg.skip_bitmap_.has_any_skip()) {
    if (OB_FAIL(arg.skip_bitmap_.rewrite_inplace())) {
      LOG_WARN("rewrite skip bitmap inplace failed", KR(ret));
    }
  }
  return ret;
}

int ObTabletMdsRelocHelper::on_replay(
    const char *buf,
    const int64_t len,
    const share::SCN &scn,
    BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObTabletMdsRelocBatchArg arg;
  ObTransferService *transfer_service = nullptr;
  ObForkTableService *fork_table_service = nullptr;
  int64_t pos = 0;
  if (OB_UNLIKELY(nullptr == buf || len <= 0 || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid replay args", KR(ret), KP(buf), K(len), K(scn));
  } else if (OB_FAIL(arg.deserialize(buf, len, pos))) {
    LOG_WARN("deserialize ls mds reloc batch failed", KR(ret));
  } else if (OB_UNLIKELY(len != pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch arg tail not consumed", KR(ret), K(len), K(pos));
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch arg invalid", KR(ret), K(arg));
  } else if (arg.is_final_
             && ObTabletMdsRelocType::TRANSFER == arg.reloc_type_
             && OB_ISNULL(transfer_service = MTL(ObTransferService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer service is null for reloc final replay", KR(ret));
  } else if (arg.is_final_
             && ObTabletMdsRelocType::FORK == arg.reloc_type_
             && OB_ISNULL(fork_table_service = MTL(ObForkTableService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fork table service is null for reloc final replay", KR(ret));
  } else if (OB_FAIL(apply_rows_(true /*for_replay*/, scn, arg, ctx))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else if (OB_EAGAIN == ret) {
      // OB_EAGAIN is kept as the return code (not normalized to success): the MDS frame in
      // ob_multi_data_source.cpp propagates it, so clog replay retries this batch once the
      // source LS has caught up / the destination tablet status becomes visible.  Only the
      // log level is downgraded, because this wait is expected and a WARN per retry is noise.
      LOG_INFO("apply reloc rows on replay wait source catchup", KR(ret), K(scn), K(arg));
    } else {
      LOG_WARN("apply reloc rows on replay failed", KR(ret), K(scn), K(arg));
    }
  }
  if (OB_SUCC(ret) && arg.is_final_) {
    if (ObTabletMdsRelocType::TRANSFER == arg.reloc_type_) {
      transfer_service->wakeup();
    } else if (ObTabletMdsRelocType::FORK == arg.reloc_type_) {
      fork_table_service->wakeup();
    }
  }
  return ret;
}

// V2 op-semantic pull for one selected multi-key <K,V>: read the MDS rows from
// the LOCAL source tablet at the reloc snapshot (the snapshot carried in the
// batch arg, which may be a transfer, split, or fork snapshot) and apply them
// to the destination tablet, preserving each row's original commit_version (meta).
// Runs on both leader (write) and followers (replay).
template <typename K, typename V>
int ObTabletMdsRelocHelper::pull_apply_op_type_(
    const bool for_replay,
    const share::SCN &replay_scn,
    ObTablet &src_tablet,
    ObMdsTable *dest_mds_table,
    const common::ObTabletID &dest_tablet_id,
    const share::SCN &dest_transfer_scn,
    const share::SCN &snapshot,
    MdsCtx &user_ctx,
    int64_t &pulled_cnt)
{
  int ret = OB_SUCCESS;
  ObTabletMDSInfoReader<K, V> reader;
  common::ObArenaAllocator alloc(common::ObMemAttr(MTL_ID(), "RelocPull"));
  if (OB_ISNULL(dest_mds_table)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest mds table null", KR(ret), K(dest_tablet_id));
  } else if (OB_FAIL(reader.init(src_tablet, share::SCN::max_scn()))) {
    LOG_WARN("init op pull reader failed", KR(ret), K(dest_tablet_id));
  }
  while (OB_SUCC(ret)) {
    // key/val are deep-deserialized from alloc (e.g. ObMediumCompactionInfo
    // frees its members via the saved allocator on destruction), so they must
    // be fully destroyed before alloc.reuse() recycles the arena.  Keep the
    // read/apply in an inner scope, mirroring submit_reloc_rows_by_type.
    {
      K key;
      V val;
      ObMdsValueMeta meta;
      ret = reader.get_next_mds_kv(alloc, key, val, meta);
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("op pull get_next failed", KR(ret), K(dest_tablet_id));
      } else if (!snapshot.is_max() && meta.commit_version_ > snapshot.get_val_for_tx()) {
        // Match V1: select the latest committed version, then filter by the
        // business snapshot. Do not resurrect an older version of this key.
      } else {
        const int rv = for_replay
            ? dest_mds_table->replay(dest_tablet_id, dest_transfer_scn, key, val, meta, user_ctx, replay_scn)
            : dest_mds_table->write(dest_tablet_id, dest_transfer_scn, key, val, meta,
                                    *user_ctx.get_store_ctx(), user_ctx, true /*check_dup*/);
        if (common::OB_ERR_PRIMARY_KEY_DUPLICATE == rv) {
          // Under the marker placeholder mutex there is at most one committed
          // reloc per (tablet, transfer_scn), so a duplicate implies the same
          // row was already applied by a benign double-commit in the
          // upgrade/failover window.  Verify that the committed row matches
          // this pulled snapshot instead of blindly ignoring the conflict.
          // verify_pulled_row_committed_ already logs the concrete failure.
          if (OB_FAIL((verify_pulled_row_committed_<K, V>(
              *dest_mds_table, dest_tablet_id, dest_transfer_scn, key, val, meta)))) {
            if (OB_ERR_UNEXPECTED == ret) {
              LOG_ERROR("duplicate pulled op row does not match the committed row",
                        KR(ret), K(dest_tablet_id), K(dest_transfer_scn), K(key), K(meta));
            } else {
              LOG_WARN("failed to verify duplicate pulled op row",
                       KR(ret), K(dest_tablet_id), K(dest_transfer_scn), K(key), K(meta));
            }
          } else {
            ++pulled_cnt;
          }
        } else if (OB_FAIL(ret = rv)) {
          LOG_WARN("apply pulled op row failed", KR(ret), K(dest_tablet_id), K(dest_transfer_scn),
                   K(key), K(meta));
        } else {
          ++pulled_cnt;
        }
      }
    } // destroy key/value/meta before reusing alloc
    alloc.reuse();
  }
  return ret;
}

// Verify that a row reported as OB_ERR_PRIMARY_KEY_DUPLICATE on write is
// byte-identical to the row already committed on the destination, so an
// unexpected conflict (different value or meta) is surfaced instead of being
// silently swallowed.  Compare the serialized representations instead of the
// Value objects because reloc Value types have no uniform, complete equality
// contract: some do not provide operator==, while others compare only a subset
// of their serialized fields.  The serialized bytes are the canonical
// persisted representation and also exclude allocator/runtime-only state.
// Used by the V2 op-semantic pull path.
template <typename K, typename V>
int ObTabletMdsRelocHelper::verify_pulled_row_committed_(
    ObMdsTable &dest_mds_table,
    const common::ObTabletID &dest_tablet_id,
    const share::SCN &dest_transfer_scn,
    const K &key,
    const V &val,
    const ObMdsValueMeta &meta)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator verify_alloc(common::ObMemAttr(MTL_ID(), "RelocDupChk"));
  V committed_val;
  ObMdsValueMeta committed_meta;
  if (OB_FAIL((dest_mds_table.get<K, V>(
      &verify_alloc,
      dest_tablet_id,
      dest_transfer_scn,
      key,
      share::SCN::max_scn(),
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
      false /*read_uncommitted*/,
      committed_val,
      committed_meta)))) {
    LOG_WARN("read committed row for duplicate check failed", KR(ret),
             K(dest_tablet_id), K(dest_transfer_scn), K(key));
  } else {
    // The pulled row is verified by re-serializing it against the committed
    // row: value bytes and all persisted meta fields must match (both sides
    // come from the same source snapshot).
    const int64_t val_ser_size = val.get_serialize_size();
    const int64_t committed_ser_size = committed_val.get_serialize_size();
    if (OB_UNLIKELY(val_ser_size <= 0 || committed_ser_size <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid serialize size for duplicate pulled op row check",
          KR(ret), K(dest_tablet_id), K(dest_transfer_scn), K(key),
          K(val_ser_size), K(committed_ser_size));
    } else if (OB_UNLIKELY(
        val_ser_size > transaction::ObTxMultiDataSourceLog::MAX_MDS_LOG_SIZE
        || committed_ser_size > transaction::ObTxMultiDataSourceLog::MAX_MDS_LOG_SIZE)) {
      // A single mds row cannot exceed the multi-data-source log budget, so a
      // larger serialized size means the in-memory row is corrupt.  Reject it
      // before the size is used to allocate the comparison buffers.
      // OB_ERR_UNEXPECTED is deliberate: callers classify it as a protocol
      // error and do not turn it into a retryable relocation wait.
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("oversized serialized value for duplicate pulled op row check",
          KR(ret), K(dest_tablet_id), K(dest_transfer_scn), K(key),
          K(val_ser_size), K(committed_ser_size));
    } else {
      char *val_buf = static_cast<char *>(verify_alloc.alloc(val_ser_size));
      char *committed_buf = static_cast<char *>(verify_alloc.alloc(committed_ser_size));
      if (OB_ISNULL(val_buf) || OB_ISNULL(committed_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc verify buffer failed", KR(ret), K(val_ser_size), K(committed_ser_size));
      } else {
        int64_t val_pos = 0;
        int64_t committed_pos = 0;
        if (OB_FAIL(val.serialize(val_buf, val_ser_size, val_pos))) {
          LOG_WARN("serialize pulled value for duplicate check failed", KR(ret));
        } else if (OB_FAIL(committed_val.serialize(committed_buf, committed_ser_size, committed_pos))) {
          LOG_WARN("serialize committed value for duplicate check failed", KR(ret));
        } else if (val_pos != val_ser_size || committed_pos != committed_ser_size) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("serialized value length mismatch during duplicate pulled op row check",
              KR(ret), K(dest_tablet_id), K(dest_transfer_scn), K(key),
              K(val_pos), K(val_ser_size), K(committed_pos), K(committed_ser_size));
        } else if (val_pos != committed_pos
                   || 0 != MEMCMP(val_buf, committed_buf, val_pos)
                   || committed_meta.writer_trans_id_ != meta.writer_trans_id_
                   || committed_meta.commit_version_ != meta.commit_version_
                   || committed_meta.commit_scn_ != meta.commit_scn_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("duplicate pulled op row differs from committed row",
              KR(ret), K(dest_tablet_id), K(dest_transfer_scn), K(key),
              "pulled_meta", meta, "committed_meta", committed_meta);
        } else {
          LOG_INFO("duplicate pulled op row matches committed row; treat as done",
              K(dest_tablet_id), K(dest_transfer_scn), K(key),
              "commit_version", meta.commit_version_);
        }
      }
    }
  }
  return ret;
}

// V2 op-semantic pull, all op-types for every dest target.  Reads from the LOCAL
// source LS replica so each replayer's dest row-set stays self-consistent with
// its own major progress (this is what closes the backup/restore history gap).
// Returns OB_EAGAIN if the source LS replica has not yet replayed up to the
// destination-generation fence or the reloc snapshot (or the source tablet is
// not yet present); replay retries.
int ObTabletMdsRelocHelper::pull_and_apply_op_rows_(
    const bool for_replay,
    const share::SCN &replay_scn,
    ObMdsTable *dest_mds_table,
    ObTabletMdsRelocBatchArg &arg,
    MdsCtx &user_ctx)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle src_ls_handle;
  ObLS *src_ls = nullptr;
  int64_t pulled_cnt = 0;
  const int64_t pull_start_us = ObTimeUtility::fast_current_time();
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service null", KR(ret));
  } else if (ObTabletMdsRelocType::SPLIT == arg.reloc_type_
             && arg.src_ls_id_ != arg.ls_id_) {
    // Split uses min_scn as its destination row key, so it intentionally has
    // no cross-LS replay fence below.  Make the same-LS construction invariant
    // explicit here instead of silently accepting a future cross-LS caller.
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("split reloc source and destination must be in the same ls",
        KR(ret), K(arg.src_ls_id_), "dest_ls_id", arg.ls_id_);
  } else if (OB_FAIL(ls_service->get_ls(arg.src_ls_id_, src_ls_handle, ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret && for_replay) {
      // Source LS replica not created/replayed yet on this replica; retry later.
      ret = OB_EAGAIN;
      LOG_WARN("src ls not ready for op pull, will retry", KR(ret), K(arg.src_ls_id_));
    } else {
      LOG_WARN("get src ls for op pull failed", KR(ret), K(arg.src_ls_id_));
    }
  } else if (OB_ISNULL(src_ls = src_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src ls null", KR(ret), K(arg.src_ls_id_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.targets_.count(); ++i) {
    const ObTabletMdsRelocTarget &tgt = arg.targets_.at(i);
    ObTabletHandle src_tablet_handle;
    if (OB_FAIL(get_ready_source_tablet_(
            for_replay, *src_ls, tgt, arg.read_snapshot_, arg.ls_id_, src_tablet_handle))) {
      LOG_WARN("source tablet not ready for reloc pull", KR(ret), K(tgt));
    } else if (OB_FAIL(pull_target_op_rows_(for_replay, replay_scn,
        *src_tablet_handle.get_obj(), dest_mds_table, arg, tgt, user_ctx, pulled_cnt))) {
      LOG_WARN("pull target op rows failed", KR(ret), K(tgt));
    }
  }
  // Keep successful pulls and unexpected errors observable. Expected replay
  // waits, including partial success followed by OB_EAGAIN on a later target,
  // share a rate limit so every retry does not enqueue a new event.
  const bool emit_event = (OB_EAGAIN == ret)
      ? REACH_TIME_INTERVAL(1_s)
      : (pulled_cnt > 0 || OB_SUCCESS != ret);
  if (emit_event) {
    SERVER_EVENT_ADD(reloc_event_module(arg.reloc_type_), "MDS_RELOC_PULL",
        "src_ls_id", arg.src_ls_id_.id(),
        "dest_ls_id", arg.ls_id_.id(),
        "for_replay", for_replay,
        "pulled_row_cnt", pulled_cnt,
        "cost_us", ObTimeUtility::fast_current_time() - pull_start_us,
        "result", ret);
  }
  return ret;
}

int ObTabletMdsRelocHelper::get_ready_source_tablet_(
    const bool for_replay,
    ObLS &src_ls,
    const ObTabletMdsRelocTarget &tgt,
    const share::SCN &read_snapshot,
    const share::ObLSID &dest_ls_id,
    ObTabletHandle &src_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *src_tablet = nullptr;
  share::SCN actual_reorg_scn;
  bool source_generation_gone = false;
  // ObLS::get_max_decided_scn() delegates to ObLogHandler::get_max_decided_scn(),
  // which returns max(max_replayed_scn, max_applied_scn): a replay/apply
  // boundary rather than a "log decided" one (the transaction layer relies on
  // the same value as its replayed-state watermark).  MDS rows only become
  // readable once their redo has been replayed, so this boundary is the correct
  // cross-LS fence for the pull below.
  share::SCN max_decided_scn;
  if (OB_FAIL(src_ls.get_max_decided_scn(max_decided_scn))) {
    if (for_replay) {
      // This read-only query supplies the source replay/apply watermark.
      // Any failure leaves source readiness unknown, so retry the destination
      // batch without advancing it and recheck the fences below.  In
      // particular, the source LS can enter the LS map before its log handler
      // is initialized or online.  Keep the original error observable even
      // for persistent source failures; a failed readiness query must not
      // permanently mark destination replay fatal.
      const int src_ret = ret;
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1_s)) {
        LOG_WARN("src ls is not readable yet for op pull, will retry", KR(ret),
                 KR(src_ret), K(src_ls.get_ls_id()));
      }
    } else {
      LOG_WARN("get src ls max decided scn failed", KR(ret), K(src_ls.get_ls_id()));
    }
  } else if (max_decided_scn < tgt.transfer_scn_) {
    // Enforce cross-LS replay order for transfer/fork: the source LS must
    // have reached the destination-generation fence before rows are pulled.
    // Split uses min_scn here because its source and destination share one LS
    // log stream, whose replay order already supplies the barrier.
    ret = OB_EAGAIN;
    LOG_WARN("src ls not caught up to transfer scn, will retry", KR(ret),
             K(src_ls.get_ls_id()), K(max_decided_scn), K(tgt));
  } else if (!read_snapshot.is_max() && max_decided_scn < read_snapshot) {
    // A finite reloc snapshot (split/fork) is a commit-version filter. The
    // source replica must have decided that SCN before the MDS reader can
    // observe the selected rows. Transfer uses max_scn and skips this wait.
    ret = OB_EAGAIN;
    LOG_WARN("src ls not caught up to reloc snapshot, will retry", KR(ret),
             K(src_ls.get_ls_id()), K(max_decided_scn), K(read_snapshot), K(tgt));
  } else if (OB_FAIL(src_ls.get_tablet(tgt.src_tablet_id_, src_tablet_handle,
                 0 /*timeout: READ_ALL_COMMITED ignores timeout*/,
                 ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    if ((OB_TABLET_NOT_EXIST == ret || OB_EMPTY_RESULT == ret) && for_replay) {
      // Do not reinterpret permanent absence as success.  Every path that
      // can invalidate the source first waits for the authoritative shared
      // LS_MDS_TABLET checkpoint to cover the V2 final-marker redo.  Hence a
      // replay that is not already skipped by its MDS checkpoint is still in
      // the phase where the source must become available; skipping here
      // could silently lose op rows.
      // Reaching this branch also implies max_decided_scn >= tgt.transfer_scn_
      // (the not-caught-up branches above would have returned first), i.e. the
      // source LS replica has already decided past the fence.  The source
      // tablet's create log is therefore behind us and it can never appear on
      // this replica again (replayed-and-deleted, emptied by the empty-shell
      // task, or skipped by a migration snapshot taken after the deletion).
      // Waiting with OB_EAGAIN would loop forever, so fail the DESTINATION
      // LS's in-flight migration task (no-op when no task is running): the
      // migration-failure flow stops replay (ls offline/disable_replay) and
      // lets RS reschedule this replica, whose fresh snapshot carries an MDS
      // checkpoint that already covers this batch.  Replay itself keeps the
      // EAGAIN semantics and is taken over by the migration failure flow.
      source_generation_gone = true;
      ret = OB_EAGAIN;
      if (REACH_TIME_INTERVAL(1_s)) {
        LOG_WARN("src tablet not ready for op pull, will retry", KR(ret), K(tgt));
      }
    } else {
      LOG_WARN("get src tablet for op pull failed", KR(ret), K(tgt));
    }
  } else if (OB_ISNULL(src_tablet = src_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src tablet null", KR(ret), K(tgt));
  } else if ((actual_reorg_scn = src_tablet->get_reorganization_scn()) != tgt.src_reorg_scn_) {
    // An older source may catch up. Once it is newer, the required generation is gone.
    source_generation_gone = for_replay && actual_reorg_scn > tgt.src_reorg_scn_;
    ret = for_replay ? OB_EAGAIN : OB_STATE_NOT_MATCH;
    LOG_WARN("source tablet generation does not match reloc batch", KR(ret),
        K(for_replay), K(tgt), K(actual_reorg_scn));
  }
  if (source_generation_gone) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(fail_dest_migration_for_absent_src_(dest_ls_id, tgt, max_decided_scn))) {
      LOG_WARN("fail dest migration for absent src failed", KR(tmp_ret), K(dest_ls_id), K(tgt));
    }
  }
  return ret;
}

int ObTabletMdsRelocHelper::fail_dest_migration_for_absent_src_(
    const share::ObLSID &dest_ls_id,
    const ObTabletMdsRelocTarget &tgt,
    const share::SCN &src_max_decided_scn)
{
  int ret = OB_SUCCESS;
  // set_result is idempotent (first-error-wins inside the handler) and is a
  // no-op when no migration task is in flight; rate-limit the lookup and logs
  // because replay retries this batch at high frequency.
  if (REACH_TIME_INTERVAL(1_s)) {
    ObLSService *ls_service = MTL(ObLSService *);
    ObLSHandle dest_ls_handle;
    ObLS *dest_ls = nullptr;
    ObLSMigrationTask task;
    ObLSMigrationHandlerStatus handler_status;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(dest_ls_id, dest_ls_handle,
            ObLSGetMod::STORAGE_MOD))) {
      // dest LS gone (already rescheduled/offlined): nothing to fail
      LOG_INFO("dest ls not available, skip failing migration", KR(ret), K(dest_ls_id));
      ret = OB_SUCCESS;
    } else if (OB_ISNULL(dest_ls = dest_ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dest ls is null", KR(ret), K(dest_ls_id));
    } else if (OB_FAIL(dest_ls->get_ls_migration_handler()
            ->get_migration_task_and_handler_status(task, handler_status))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // Not migrating: no migration task to fail, and calling set_result
        // would be pointless. Replay keeps waiting; RS stall detection is the
        // fallback for this replica.
        ret = OB_SUCCESS;
        LOG_INFO("reloc src permanently absent, but no migration task in flight, "
                 "keep waiting for rs detection", K(dest_ls_id), K(tgt));
      } else {
        // A failed probe must not abort the replay path itself.
        LOG_WARN("get migration task status failed", KR(ret), K(dest_ls_id));
        ret = OB_SUCCESS;
      }
    } else {
      // Request failure of the probed migration task. The
      // locked task-id check also guards completion and replacement after this probe.
      const int32_t fail_result = OB_LOG_REPLAY_ERROR;
      if (OB_FAIL(dest_ls->get_ls_migration_handler()->set_result_for_task(fail_result, task.task_id_))) {
        LOG_WARN("set migration result for absent reloc src failed", KR(ret),
                 K(dest_ls_id), K(tgt));
      } else {
        FLOG_WARN("reloc replay source tablet permanently absent, "
                   "requested failure of the probed dest migration task",
                   K(dest_ls_id), K(tgt), K(src_max_decided_scn),
                   "handler_status", static_cast<int64_t>(handler_status),
                   "task_id", task.task_id_, "requested_result", fail_result);
        SERVER_EVENT_ADD("MDS_RELOC", "RELOC_SRC_ABSENT_REQUEST_FAIL_MIGRATION",
            "dest_ls_id", dest_ls_id.id(),
            "src_tablet_id", tgt.src_tablet_id_.id(),
            "src_max_decided_scn",
            static_cast<int64_t>(src_max_decided_scn.get_val_for_logservice()),
            "handler_status", static_cast<int64_t>(handler_status),
            "task_id", task.task_id_,
            "requested_result", fail_result);
      }
    }
  }
  return ret;
}

int ObTabletMdsRelocHelper::probe_source_ready_(
    const ObTabletMdsRelocBatchArg &arg)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle src_ls_handle;
  ObLS *src_ls = nullptr;
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service null", KR(ret));
  } else if (OB_FAIL(ls_service->get_ls(arg.src_ls_id_, src_ls_handle,
          ObLSGetMod::STORAGE_MOD))) {
    if (OB_LS_NOT_EXIST == ret) {
      // Source LS replica not created/replayed yet on this replica; retry later.
      ret = OB_EAGAIN;
      LOG_WARN("src ls not ready for reloc probe, will retry", KR(ret), K(arg.src_ls_id_));
    } else {
      LOG_WARN("get src ls for reloc probe failed", KR(ret), K(arg.src_ls_id_));
    }
  } else if (OB_ISNULL(src_ls = src_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("src ls null", KR(ret), K(arg.src_ls_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.targets_.count(); ++i) {
      const ObTabletMdsRelocTarget &tgt = arg.targets_.at(i);
      ObTabletHandle src_tablet_handle;
      if (OB_FAIL(get_ready_source_tablet_(
              true /*for_replay*/, *src_ls, tgt, arg.read_snapshot_, arg.ls_id_,
              src_tablet_handle))) {
        // EAGAIN carries the "source not ready yet" semantics (and, for a
        // permanently absent source, the fail-migration escape hatch inside
        // get_ready_source_tablet_ has already been armed); other errors are
        // reported by get_ready_source_tablet_ itself.
        if (OB_EAGAIN != ret) {
          LOG_WARN("probe reloc source tablet failed", KR(ret), K(tgt));
        }
      }
    }
  }
  return ret;
}

int ObTabletMdsRelocHelper::pull_target_op_rows_(
    const bool for_replay,
    const share::SCN &replay_scn,
    ObTablet &src_tablet,
    ObMdsTable *dest_mds_table,
    ObTabletMdsRelocBatchArg &arg,
    const ObTabletMdsRelocTarget &tgt,
    MdsCtx &user_ctx,
    int64_t &pulled_cnt)
{
  int ret = OB_SUCCESS;
  // First select the units needed by this reloc operation, then derive
  // op-semantics from the registered key shape.  if constexpr keeps the
  // multi-key reader from being instantiated for DummyKey units.
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
  if (OB_SUCC(ret)) { \
    if constexpr (ObIsRelocOpType<KEY_TYPE>::value) { \
      /* Which op-types to pull depends on the reloc type; fork carries only TTL. */ \
      const bool should_pull = \
          (ObTabletMdsRelocType::TRANSFER == arg.reloc_type_ && (TRANSFER_RELOC)) \
          || (ObTabletMdsRelocType::SPLIT == arg.reloc_type_ && (SPLIT_RELOC)) \
          || (ObTabletMdsRelocType::FORK == arg.reloc_type_ \
              && std::is_same<VALUE_TYPE, ObTTLFilterInfo>::value); \
      if (should_pull) { \
        ret = pull_apply_op_type_<KEY_TYPE, VALUE_TYPE>( \
            for_replay, replay_scn, src_tablet, dest_mds_table, \
            tgt.tablet_id_, tgt.transfer_scn_, arg.read_snapshot_, user_ctx, pulled_cnt); \
        if (OB_FAIL(ret)) { \
          LOG_WARN("pull-apply op type failed", KR(ret), K(tgt), \
                   "key_type", #KEY_TYPE, "value_type", #VALUE_TYPE); \
        } \
      } \
    } \
  }
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE
  return ret;
}

int ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &is_done)
{
  int ret = OB_SUCCESS;
  ObTabletMdsRelocStatus reloc_status;
  ObMdsValueMeta marker_meta;
  bool marker_exists = false;
  is_done = false;

  if (OB_FAIL(read_tablet_mds_reloc_marker(
      ls, tablet_id, transfer_scn, reloc_status, marker_meta, marker_exists))) {
    LOG_WARN("failed to read tablet mds reloc marker", K(ret), K(tablet_id), K(transfer_scn), KP(ls));
  } else {
    is_done = marker_exists && reloc_status.is_reloc_done();
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("check tablet mds reloc done", K(ret), K(is_done), K(tablet_id), K(transfer_scn),
        K(reloc_status), "ls_id", ls->get_ls_id());
  }
  return ret;
}

int ObTabletMdsRelocHelper::check_tablet_mds_reloc_done(
    const share::ObLSID &dest_ls_id,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &is_done)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  ObLSHandle dest_ls_handle;
  ObLS *dest_ls = nullptr;
  is_done = false;

  if (!dest_ls_id.is_valid() || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service should not be NULL", K(ret), K(dest_ls_id), K(tablet_id));
  } else if (OB_FAIL(ls_service->get_ls(dest_ls_id, dest_ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get local dest ls when checking mds reloc done",
        K(ret), K(dest_ls_id), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(dest_ls = dest_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest ls should not be NULL", K(ret), K(dest_ls_id), K(tablet_id));
  } else if (OB_FAIL(check_tablet_mds_reloc_done(dest_ls, tablet_id, transfer_scn, is_done))) {
    LOG_WARN("failed to read local dest tablet mds reloc marker",
        K(ret), K(dest_ls_id), K(tablet_id), K(transfer_scn));
  }

  if (OB_FAIL(ret)) {
    is_done = false;
  }
  return ret;
}

int ObTabletMdsRelocHelper::get_latest_committed_reloc_status(
    ObMdsTable &mds_table,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const int64_t timeout_us,
    ObTabletMdsRelocStatus &reloc_status,
    ObMdsValueMeta *value_meta)
{
  int ret = OB_SUCCESS;
  ObMdsValueMeta meta;
  if (OB_FAIL(mds_table.get(nullptr, tablet_id, transfer_scn, share::SCN::max_scn(),
          timeout_us, false /*read_uncommitted*/, reloc_status, meta))) {
  } else if (reloc_status.is_reloc_done() && reloc_status.get_redo_scn().is_valid()
      && !reloc_status.has_captured_redo_fence()) {
    // on_redo captures the V2 fence before commit. A committed min/max SCN
    // is a protocol error, not a waitable state or a legacy V1 marker.
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("committed V2 mds reloc marker has no redo fence",
        K(ret), K(tablet_id), K(transfer_scn), K(reloc_status), K(meta));
  }
  if (nullptr != value_meta) {
    value_meta->assign(meta);
  }
  return ret;
}

int ObTabletMdsRelocHelper::read_tablet_mds_reloc_marker(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    ObTabletMdsRelocStatus &reloc_status,
    ObMdsValueMeta &marker_meta,
    bool &marker_exists)
{
  int ret = OB_SUCCESS;
  ObMdsTable *mds_table = nullptr;
  marker_exists = false;

  if (OB_ISNULL(ls) || !tablet_id.is_valid() || !transfer_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ls), K(tablet_id), K(transfer_scn));
  } else if (OB_ISNULL(mds_table = ls->get_mds_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mds table should not be NULL", K(ret), K(tablet_id), K(transfer_scn), KPC(ls));
  } else if (OB_FAIL(get_latest_committed_reloc_status(
      *mds_table, tablet_id, transfer_scn, 5_s, reloc_status, &marker_meta))) {
    if (OB_EMPTY_RESULT == ret || OB_ITER_END == ret || OB_SNAPSHOT_DISCARDED == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get mds reloc status from mds table",
          K(ret), K(tablet_id), K(transfer_scn), KPC(ls));
    }
  } else {
    marker_exists = true;
  }
  return ret;
}

int ObTabletMdsRelocHelper::get_ss_mds_clog_checkpoint_scn(
    const share::ObLSID &ls_id,
    share::SCN &ss_mds_clog_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  lib::ObMemAttr mem_attr(MTL_ID(), "MdsRelocCkpt");
  ObArenaAllocator allocator(mem_attr);
  ObTabletHandle ss_mds_tablet_handle;
  ObTablet *ss_mds_tablet = nullptr;
  ss_mds_clog_checkpoint_scn.reset();
#ifdef ERRSIM
  if (OB_FAIL(OB_E(EN_MDS_RELOC_SHARED_CHECKPOINT_READ, GETTID()) OB_SUCCESS)) {
    // Preserve the injected error without reading shared storage.
  } else
#endif
  if (OB_FAIL(ObSSTransferBackfillUtils::get_ss_tablet(
      ls_id, LS_MDS_TABLET, share::SCN::min_scn(), allocator,
      ss_mds_tablet_handle, true /*read_mark_delete*/))) {
    LOG_WARN("failed to read LS_MDS_TABLET from shared storage", K(ret), K(ls_id));
  } else if (OB_ISNULL(ss_mds_tablet = ss_mds_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared LS_MDS_TABLET should not be NULL", K(ret), K(ls_id), K(ss_mds_tablet_handle));
  } else {
    ss_mds_clog_checkpoint_scn = ss_mds_tablet->get_clog_checkpoint_scn();
  }
  return ret;
}

int ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const share::SCN &ss_mds_clog_checkpoint_scn,
    bool &is_persisted,
    bool *flush_requested)
{
  int ret = OB_SUCCESS;
  bool read_authoritative = false;
  share::SCN marker_redo_scn;
  bool is_marker_ready = false;
  is_persisted = false;

  if (OB_FAIL(get_tablet_mds_reloc_marker_redo_scn_(
      ls, tablet_id, transfer_scn, marker_redo_scn, is_marker_ready))) {
    LOG_WARN("failed to get tablet mds reloc marker redo scn",
        K(ret), K(tablet_id), K(transfer_scn), KP(ls));
  } else if (is_marker_ready) {
    if (!marker_redo_scn.is_valid()) {
      // A V1 marker keeps redo_scn_ invalid even when written by a new binary.
      // V1 only gated source invalidation on reloc_done, so preserve that
      // behavior instead of imposing the V2 persistence fence.
      is_persisted = true;
    } else if (OB_UNLIKELY(!ss_mds_clog_checkpoint_scn.is_valid())) {
      // The caller's prefetch failed (e.g. temporary SS meta unavailability).
      // The 4-arg overload reads the same shared LS_MDS_TABLET meta, so a
      // persistent prefetch failure will fail here too. Retryable errors
      // defer this tablet; protocol errors keep their original code.
      read_authoritative = true;
      if (OB_FAIL(check_tablet_mds_reloc_persisted(
          ls, tablet_id, transfer_scn, is_persisted, flush_requested))) {
        if (OB_EAGAIN != fold_mds_reloc_check_err(ret)) {
          LOG_WARN("fallback to authoritative mds checkpoint read hit nonretryable error",
              K(ret), K(tablet_id), K(transfer_scn), KP(ls));
        } else {
          LOG_WARN("fallback to authoritative mds checkpoint read failed, defer tablet",
              K(ret), K(tablet_id), K(transfer_scn), KP(ls));
          ret = OB_SUCCESS;
          is_persisted = false;
        }
      }
    } else {
      // Reusing an earlier LS-wide checkpoint is conservative: advancement
      // after this read only produces a false negative and another scheduling
      // round.
      is_persisted = ss_mds_clog_checkpoint_scn >= marker_redo_scn;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("check tablet mds reloc persisted with supplied checkpoint",
        K(ret), K(is_persisted), K(is_marker_ready), K(tablet_id), K(transfer_scn),
        K(marker_redo_scn), K(ss_mds_clog_checkpoint_scn),
        "ls_id", ls->get_ls_id());
  }
  if (!read_authoritative) {
    try_flush_mds_table_(ls, ret, is_persisted, flush_requested);
  }
  return ret;
}

int ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &is_persisted,
    bool *flush_requested)
{
  int ret = OB_SUCCESS;
  share::SCN ss_mds_clog_checkpoint_scn;
  share::SCN marker_redo_scn;
  bool is_marker_ready = false;
  is_persisted = false;

  if (OB_FAIL(get_tablet_mds_reloc_marker_redo_scn_(
      ls, tablet_id, transfer_scn, marker_redo_scn, is_marker_ready))) {
    LOG_WARN("failed to get tablet mds reloc marker redo scn",
        K(ret), K(tablet_id), K(transfer_scn), KP(ls));
  } else if (!is_marker_ready) {
    // Avoid an authoritative shared-storage read until the reloc transaction
    // has committed its final marker.
  } else if (!marker_redo_scn.is_valid()) {
    // V1 markers keep redo_scn_ invalid, including markers written by a new
    // binary.  Preserve the V1 completion contract and avoid an unnecessary
    // authoritative SS read.  The marker state is the stable discriminator
    // even after the driving transfer/DDL task has been cleaned up.
    is_persisted = true;
  } else if (OB_FAIL(get_ss_mds_clog_checkpoint_scn(
      ls->get_ls_id(), ss_mds_clog_checkpoint_scn))) {
    LOG_WARN("failed to read authoritative mds checkpoint from shared storage",
        K(ret), K(tablet_id), K(transfer_scn), K(marker_redo_scn), "ls_id", ls->get_ls_id());
  } else {
    // The authoritative shared LS_MDS_TABLET checkpoint must cover the final
    // marker redo before replace mutates the shared tablet.  Local replay
    // safety is established separately by the local checkpoint before refresh.
    is_persisted = ss_mds_clog_checkpoint_scn >= marker_redo_scn;
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("check tablet mds reloc persisted", K(ret), K(is_persisted), K(is_marker_ready),
        K(tablet_id), K(transfer_scn), K(marker_redo_scn), K(ss_mds_clog_checkpoint_scn),
        "ls_id", ls->get_ls_id());
  }
  try_flush_mds_table_(ls, ret, is_persisted, flush_requested);
  return ret;
}


int ObTabletMdsRelocHelper::get_tablet_mds_reloc_marker_redo_scn_(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    share::SCN &marker_redo_scn,
    bool &is_marker_ready)
{
  int ret = OB_SUCCESS;
  ObTabletMdsRelocStatus reloc_status;
  ObMdsValueMeta marker_meta;
  bool marker_exists = false;
  marker_redo_scn.reset();
  is_marker_ready = false;

  if (OB_FAIL(read_tablet_mds_reloc_marker(
      ls, tablet_id, transfer_scn, reloc_status, marker_meta, marker_exists))) {
    LOG_WARN("failed to read tablet mds reloc marker",
        K(ret), K(tablet_id), K(transfer_scn), KP(ls));
  } else if (!marker_exists || !reloc_status.is_reloc_done()) {
    // The relocation transaction has not committed its final marker yet.
  } else if (OB_UNLIKELY(!marker_meta.is_committed())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("done mds reloc marker is not committed",
        K(ret), K(tablet_id), K(transfer_scn), K(reloc_status), K(marker_meta), KPC(ls));
  } else {
    // The reloc status read has validated the marker. V1 carries no
    // fence; V2 carries the final redo captured before commit.
    if (!reloc_status.is_v1_marker()) {
      marker_redo_scn = reloc_status.get_redo_scn();
    }
    is_marker_ready = true;
  }
  return ret;
}

int ObTabletMdsRelocHelper::get_local_mds_checkpoint_scn(
    ObLS *ls,
    share::SCN &local_mds_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ObMdsTable *mds_table = nullptr;
  local_mds_checkpoint_scn.reset();
#ifdef ERRSIM
  if (OB_FAIL(OB_E(EN_MDS_RELOC_LOCAL_CHECKPOINT_READ, GETTID()) OB_SUCCESS)) {
    // Preserve the injected error without reading the local checkpoint.
  } else
#endif
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is NULL", K(ret));
  } else if (OB_ISNULL(mds_table = ls->get_mds_table())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("mds table should not be NULL", K(ret), KPC(ls));
  } else if (OB_FAIL(mds_table->get_mds_checkpoint_scn(local_mds_checkpoint_scn))) {
    LOG_WARN("failed to get local mds checkpoint scn", K(ret), KPC(ls));
  }
  return ret;
}

int ObTabletMdsRelocHelper::check_local_checkpoint_reached(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const share::SCN &local_mds_checkpoint_scn,
    bool &is_reached,
    bool *flush_requested)
{
  int ret = OB_SUCCESS;
  bool read_authoritative = false;
  share::SCN marker_redo_scn;
  bool is_marker_ready = false;
  is_reached = false;

  if (OB_FAIL(get_tablet_mds_reloc_marker_redo_scn_(
      ls, tablet_id, transfer_scn, marker_redo_scn, is_marker_ready))) {
    LOG_WARN("failed to get tablet mds reloc marker redo scn",
        K(ret), K(tablet_id), K(transfer_scn), KP(ls));
  } else if (is_marker_ready) {
    if (!marker_redo_scn.is_valid()) {
      // V1 has no redo fence, so keep its committed-done behavior.
      is_reached = true;
    } else if (OB_UNLIKELY(!local_mds_checkpoint_scn.is_valid())) {
      // The 4-arg overload reads the same local checkpoint. Retryable errors
      // defer this tablet; protocol errors keep their original code.
      read_authoritative = true;
      if (OB_FAIL(check_local_checkpoint_reached(
          ls, tablet_id, transfer_scn, is_reached, flush_requested))) {
        if (OB_EAGAIN != fold_mds_reloc_check_err(ret)) {
          LOG_WARN("fallback to authoritative local mds checkpoint read hit nonretryable error",
              K(ret), K(tablet_id), K(transfer_scn), KP(ls));
        } else {
          LOG_WARN("fallback to authoritative local mds checkpoint read failed, defer tablet",
              K(ret), K(tablet_id), K(transfer_scn), KP(ls));
          ret = OB_SUCCESS;
          is_reached = false;
        }
      }
    } else {
      // A reused LS-wide checkpoint is conservative: a stale value only delays
      // refresh until the next scheduling round.
      is_reached = local_mds_checkpoint_scn >= marker_redo_scn;
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("check tablet mds reloc local checkpoint with supplied checkpoint",
        K(ret), K(is_reached), K(is_marker_ready), K(tablet_id), K(transfer_scn),
        K(marker_redo_scn), K(local_mds_checkpoint_scn),
        "ls_id", ls->get_ls_id());
  }
  if (!read_authoritative) {
    try_flush_mds_table_(ls, ret, is_reached, flush_requested);
  }
  return ret;
}

int ObTabletMdsRelocHelper::check_local_checkpoint_reached(
    ObLS *ls,
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    bool &is_reached,
    bool *flush_requested)
{
  int ret = OB_SUCCESS;
  share::SCN local_mds_checkpoint_scn;
  share::SCN marker_redo_scn;
  bool is_marker_ready = false;
  is_reached = false;

  if (OB_FAIL(get_tablet_mds_reloc_marker_redo_scn_(
      ls, tablet_id, transfer_scn, marker_redo_scn, is_marker_ready))) {
    LOG_WARN("failed to get tablet mds reloc marker redo scn",
        K(ret), K(tablet_id), K(transfer_scn), KP(ls));
  } else if (!is_marker_ready) {
    // The final marker has not committed yet.
  } else if (!marker_redo_scn.is_valid()) {
    // V1 has no redo fence, so keep its committed-done behavior.
    is_reached = true;
  } else if (OB_FAIL(get_local_mds_checkpoint_scn(ls, local_mds_checkpoint_scn))) {
    LOG_WARN("failed to get local mds checkpoint for reloc marker",
        K(ret), K(tablet_id), K(transfer_scn), K(marker_redo_scn), KP(ls));
  } else {
    // Refresh is the local installation boundary.  Requiring the local MDS
    // checkpoint to cover the final marker here makes restart replay skip the
    // reloc before its source can have been removed.
    is_reached = local_mds_checkpoint_scn >= marker_redo_scn;
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("check tablet mds reloc local checkpoint", K(ret), K(is_reached),
        K(is_marker_ready), K(tablet_id), K(transfer_scn), K(marker_redo_scn),
        K(local_mds_checkpoint_scn), "ls_id", ls->get_ls_id());
  }
  try_flush_mds_table_(ls, ret, is_reached, flush_requested);
  return ret;
}

int ObTabletMdsRelocHelper::fold_mds_reloc_check_err(const int check_ret)
{
  int report_ret = check_ret;
  if (OB_SUCCESS != check_ret
      && OB_ERR_UNEXPECTED != check_ret
      && OB_INVALID_ARGUMENT != check_ret
      && OB_SERVER_OUTOF_DISK_SPACE != check_ret
      && OB_DISK_ERROR != check_ret
      && OB_NOT_SUPPORTED != check_ret
      && OB_ERR_DATA_TOO_LONG != check_ret
      && OB_ERR_TOO_LONG_KEY_LENGTH != check_ret) {
    report_ret = OB_EAGAIN;
  }
  return report_ret;
}


void ObTabletMdsRelocHelper::try_flush_mds_table_(
    ObLS *ls,
    const int check_ret,
    const bool is_ready,
    bool *flush_requested)
{
  if (nullptr != ls && !is_ready
      && (nullptr == flush_requested || !*flush_requested)
      && (OB_SUCCESS == check_ret || OB_EAGAIN == fold_mds_reloc_check_err(check_ret))) {
    if (nullptr != flush_requested) {
      *flush_requested = true;
    }
    try_flush_mds_table(ls);
  }
}

void ObTabletMdsRelocHelper::try_flush_mds_table(ObLS *ls)
{
  if (nullptr != ls) {
    int tmp_ret = OB_SUCCESS;
#ifdef ERRSIM
    tmp_ret = OB_E(EN_MDS_RELOC_CHECKPOINT_FLUSH, GETTID()) OB_SUCCESS;
#endif
    if (OB_SUCCESS == tmp_ret) {
      tmp_ret = ls->flush_mds_table();
    }
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN_RET(tmp_ret, "failed to trigger mds flush for reloc", KPC(ls));
    }
  }
}

// Wait for both checkpoints of every V2 destination before reporting completion.
// A missing marker also waits for replay; callers that never submit skip the gate.
void ObTabletMdsRelocHelper::defer_report_until_mds_reloc_persisted(
    ObLS *ls,
    const common::ObIArray<ObTabletID> &dest_tablet_ids,
    const uint64_t data_version,
    const bool is_mds_reloc_submitted,
    const int data_ret,
    int &report_ret)
{
  report_ret = data_ret;
  int tmp_ret = OB_SUCCESS;
  bool is_persisted = true;
  bool is_local_checkpoint_reached = true;
  share::SCN ss_mds_clog_checkpoint_scn;
  share::SCN local_mds_checkpoint_scn;
  const bool should_check_reloc_persistence =
      is_mds_reloc_submitted
      && OB_SUCCESS == data_ret
      && ObTabletSplitMdsReloc::is_reloc_v2_enabled(data_version);
  if (should_check_reloc_persistence) {
    if (OB_ISNULL(ls)) {
      // Null dest LS is a DAG invariant break (handle unset or already
      // reset), not a transient checkpoint wait. OB_EAGAIN is in the DDL
      // retry white list and would re-dispatch until timeout while hiding
      // data_ret. Keep the protocol code so RS sees the concrete failure
      // instead of a retryable build wait, matching fold_mds_reloc_check_err.
      report_ret = OB_ERR_UNEXPECTED;
      LOG_WARN_RET(report_ret, "dest ls is null before reporting tablet split completion",
          K(report_ret), K(data_version), K(dest_tablet_ids));
    } else {
      // Failed prefetches leave an invalid SCN; each V2 marker then retries
      // the authoritative read and classifies its own result in the helper.
      if (OB_TMP_FAIL(ObTabletMdsRelocHelper::get_ss_mds_clog_checkpoint_scn(
          ls->get_ls_id(), ss_mds_clog_checkpoint_scn))) {
        LOG_WARN_RET(tmp_ret, "failed to prefetch shared reloc checkpoint", KPC(ls));
      }
      if (OB_TMP_FAIL(ObTabletMdsRelocHelper::get_local_mds_checkpoint_scn(
          ls, local_mds_checkpoint_scn))) {
        LOG_WARN_RET(tmp_ret, "failed to prefetch local reloc checkpoint", KPC(ls));
      }
      tmp_ret = OB_SUCCESS;
      for (int64_t i = 0; OB_SUCCESS == tmp_ret && is_persisted
          && is_local_checkpoint_reached && i < dest_tablet_ids.count(); ++i) {
        if (OB_TMP_FAIL(ObTabletMdsRelocHelper::check_tablet_mds_reloc_persisted(
                ls, dest_tablet_ids.at(i), share::SCN::min_scn(),
                ss_mds_clog_checkpoint_scn, is_persisted))) {
          report_ret = ObTabletMdsRelocHelper::fold_mds_reloc_check_err(tmp_ret);
          LOG_WARN_RET(tmp_ret, "failed to check tablet split mds reloc persistence before reporting completion",
              K(tmp_ret), K(report_ret), K(i), K(dest_tablet_ids), K(data_version));
        } else if (OB_TMP_FAIL(ObTabletMdsRelocHelper::check_local_checkpoint_reached(
                ls, dest_tablet_ids.at(i), share::SCN::min_scn(),
                local_mds_checkpoint_scn, is_local_checkpoint_reached))) {
          report_ret = ObTabletMdsRelocHelper::fold_mds_reloc_check_err(tmp_ret);
          LOG_WARN_RET(tmp_ret, "failed to check local split mds checkpoint before reporting completion",
              K(tmp_ret), K(report_ret), K(i), K(dest_tablet_ids), K(data_version));
        }
      }
      if (OB_SUCCESS == tmp_ret && (!is_persisted || !is_local_checkpoint_reached)) {
        report_ret = OB_EAGAIN;
        LOG_INFO("defer tablet split completion until mds relocation is persisted locally and shared",
            K(report_ret), K(dest_tablet_ids), K(data_version), K(local_mds_checkpoint_scn),
            K(ss_mds_clog_checkpoint_scn));
      }
    }
  }
}

} // namespace storage
} // namespace oceanbase
