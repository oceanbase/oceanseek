/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX MDS

#include "storage/mds_table/ob_ls_mds_tablet_reloc_mds_helper.h"
#include "storage/mds_table/ob_ls_mds_tablet_reloc_batch_arg.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_info_mds_user_data.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_mds_reloc_status.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/high_availability/ob_fork_table_service.h"
#include "storage/high_availability/ob_transfer_service.h"
#include "share/ob_tablet_autoincrement_param.h"
#include "share/rc/ob_tenant_base.h"
#include "share/ob_errno.h"
#include "storage/mds_table/ob_mds_type_registry.h"
#include "storage/ob_relative_table.h"

namespace oceanbase
{
namespace storage
{

using namespace mds;

namespace ls_mds_reloc_mds_helper_detail
{

OB_INLINE int reloc_skip_pkey_duplicate_ret(const int err)
{
  return (common::OB_ERR_PRIMARY_KEY_DUPLICATE == err) ? common::OB_SUCCESS : err;
}

OB_INLINE bool reloc_was_pkey_duplicate(const int err)
{
  return common::OB_ERR_PRIMARY_KEY_DUPLICATE == err;
}

//
// Overload set: lets apply_dummy_key_reloc_row_<V> stay generic while
// ObTabletAutoincSeq routes to its allocator-aware deserialize.
// Deserialize value with or without allocator depending on the type.
template <typename V>
typename std::enable_if<storage::ObMdsUserDataDeserializeNeedsAlloc<V>::value, int>::type
reloc_deserialize_value_(V &v, common::ObIAllocator &alloc, const char *buf, const int64_t len, int64_t &pos)
{
  return v.deserialize(alloc, buf, len, pos);
}

template <typename V>
typename std::enable_if<!storage::ObMdsUserDataDeserializeNeedsAlloc<V>::value, int>::type
reloc_deserialize_value_(V &v, common::ObIAllocator &, const char *buf, const int64_t len, int64_t &pos)
{
  return v.deserialize(buf, len, pos);
}

// Unified apply: deserialize [key_len + key + value], write/replay to all targets.
// DummyKey (key_len==0): calls mds_table->replay/write(tablet_id, transfer_scn, V, ...)
// Multi-key (key_len>0): calls mds_table->replay/write(tablet_id, transfer_scn, K, V, ...)
template <typename K, typename V>
int apply_reloc_row_(const bool for_replay,
                     const share::SCN &replay_scn,
                     ObMdsTable *mds_table,
                     ObLsMdsTabletRelocBatchArg &arg,
                     const ObLsMdsTabletRelocRowStorage &row,
                     MdsCtx &user_ctx,
                     common::ObIAllocator &deser_alloc,
                     const int64_t row_idx)
{
  int ret = OB_SUCCESS;
  const char *payload = row.ser_payload_.ptr();
  const int64_t payload_len = row.ser_payload_.length();
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
    } else if (OB_FAIL(reloc_deserialize_value_(val, deser_alloc, payload, payload_len, pos))) {
      LOG_WARN("reloc value deserialize failed", KR(ret), "mds_unit_id", row.mds_unit_id_);
    } else if (OB_UNLIKELY(pos != payload_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("reloc payload tail not consumed", KR(ret), K(pos), K(payload_len));
    } else {
      for (int64_t tgt_i = 0; OB_SUCC(ret) && tgt_i < arg.targets_.count(); ++tgt_i) {
        if (arg.skip_bitmap_.is_skipped(row_idx, tgt_i)) {
          continue;
        }
        const ObLsMdsTabletRelocTarget &tgt = arg.targets_.at(tgt_i);
        int rv = OB_SUCCESS;
        if constexpr (std::is_same_v<K, mds::DummyKey>) {
          rv = for_replay
              ? mds_table->replay(tgt.tablet_id_, tgt.transfer_scn_, val, row.meta_, user_ctx, replay_scn)
              : mds_table->write(tgt.tablet_id_, tgt.transfer_scn_, val, row.meta_, *user_ctx.get_store_ctx(), user_ctx, true);
        } else {
          rv = for_replay
              ? mds_table->replay(tgt.tablet_id_, tgt.transfer_scn_, key, val, row.meta_, user_ctx, replay_scn)
              : mds_table->write(tgt.tablet_id_, tgt.transfer_scn_, key, val, row.meta_, *user_ctx.get_store_ctx(), user_ctx, true);
        }
        ret = reloc_skip_pkey_duplicate_ret(rv);
        if (reloc_was_pkey_duplicate(rv)) {
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

// The final marker is written to the dedicated ObTabletMdsRelocStatus
// unit (NOT the tablet-status unit), so tablet-status commit gates never
// observe an uncommitted row owned by the reloc transaction.  Generation
// isolation is structural: the marker row key is (tablet_id, dest reorg scn),
// so a stale final redo can only land on an old-generation row key that new
// generation readers never look at.  The committed tablet-status check below
// remains as a cheap guard so an aborted / superseded destination does not
// accumulate junk marker rows.
int apply_final_marker_(const bool for_replay,
                        const share::SCN &replay_scn,
                        ObMdsTable *mds_table,
                        ObLsMdsTabletRelocBatchArg &arg,
                        MdsCtx &user_ctx)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "RelocMarker"));
  const share::SCN read_snapshot = share::SCN::max_scn();
  const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US;
  for (int64_t i = 0; OB_SUCC(ret) && i < arg.targets_.count(); ++i) {
    const ObLsMdsTabletRelocTarget &tgt = arg.targets_.at(i);
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
        // A standby tenant may replay the reloc final before the destination tablet's
        // committed status. Return OB_EAGAIN and retry after the status has been replayed.
        ret = OB_EAGAIN;
        LOG_WARN("wait for committed tablet status before replaying reloc final",
                 KR(ret), K(tgt), K(replay_scn));
      } else {
        LOG_WARN("read latest committed tablet status for reloc final failed",
                 KR(ret), K(tgt), K(for_replay), K(replay_scn));
      }
    } else if ((ObLsMdsTabletRelocType::TRANSFER == arg.reloc_type_
                && ObTabletStatus::TRANSFER_IN != user_data.tablet_status_)
               || (ObLsMdsTabletRelocType::SPLIT == arg.reloc_type_
                   && ObTabletStatus::SPLIT_DST != user_data.tablet_status_)
               || (ObLsMdsTabletRelocType::FORK == arg.reloc_type_
                   && ObTabletStatus::FORK_DST != user_data.tablet_status_)) {
      ret = OB_EAGAIN;
      LOG_WARN("reloc type does not match tablet status for final marker",
               KR(ret), K(arg.reloc_type_), K(tgt), K(user_data), K(for_replay), K(replay_scn));
    } else {
      ObTabletMdsRelocStatus reloc_done_data;
      reloc_done_data.reloc_done_ = true;
      // A duplicate marker write (reloc resubmitted after a race between the
      // done-check and commit) just stacks a new committed version on the same
      // row; readers only care about the latest committed value.
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
                             user_ctx);
      if (OB_SUCCESS != apply_ret) {
        ret = apply_ret;
        LOG_WARN("apply reloc done final marker failed",
                 KR(ret), K(tgt), K(for_replay), K(replay_scn));
      }
    }
    allocator.reuse();
  }
  return ret;
}

} // namespace ls_mds_reloc_mds_helper_detail

int ObLsMdsTabletRelocMdsHelper::apply_rows_(const bool for_replay,
                                               const share::SCN &replay_scn,
                                               ObLsMdsTabletRelocBatchArg &arg,
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
  }
  {
    ObRelativeTable relative_table;
    ObMdsRelativeTableGuard relative_table_guard(ctx, relative_table);
    for (int64_t i = 0; OB_SUCC(ret) && i < arg.rows_.count(); ++i) {
      const ObLsMdsTabletRelocRowStorage &row = arg.rows_.at(i);
      if (OB_UNLIKELY(nullptr == row.ser_payload_.ptr() || row.ser_payload_.length() <= 0)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid reloc payload", KR(ret), K(row));
      } else {
        common::ObArenaAllocator deser_alloc(common::ObMemAttr(MTL_ID(), "RelocApply"));
        bool dispatched = false;
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
        if (!dispatched && ((TRANSFER_RELOC) || (SPLIT_RELOC))                                  \
            && row.mds_unit_id_ == static_cast<uint8_t>(                                        \
                   mds_type_id_detail::TypeIdOf<KEY_TYPE, VALUE_TYPE>::value)) {                  \
          dispatched = true;                                                                     \
          ret = ls_mds_reloc_mds_helper_detail::apply_reloc_row_<KEY_TYPE, VALUE_TYPE>(          \
              for_replay, replay_scn, mds_table, arg, row, user_ctx, deser_alloc, i);            \
        }
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE
        if (!dispatched) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unsupported mds_unit_id in reloc batch", KR(ret), K(row.mds_unit_id_));
        } else if (OB_FAIL(ret)) {
          LOG_WARN("apply reloc row failed", KR(ret), K(row.mds_unit_id_));
        }
      }
    }
    if (OB_SUCC(ret) && arg.is_final_
        && OB_FAIL(ls_mds_reloc_mds_helper_detail::apply_final_marker_(
            for_replay, replay_scn, mds_table, arg, user_ctx))) {
      LOG_WARN("apply reloc final marker failed", KR(ret), K(for_replay), K(replay_scn), K(arg));
    }
  }
  LOG_INFO("apply reloc batch finish", KR(ret), K(for_replay), K(replay_scn),
           K(arg.rows_.count()), K(arg.is_final_), K(arg.ls_id_));
  return ret;
}

int ObLsMdsTabletRelocMdsHelper::on_register(const char *buf, const int64_t len, BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObLsMdsTabletRelocBatchArg arg;
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
  } else if (0 == arg.rows_.count() && !arg.is_final_) {
    // empty batch: no-op (reserved for heartbeat / padding)
  } else if (OB_FAIL(apply_rows_(false /*for_replay*/, share::SCN::invalid_scn(), arg, ctx))) {
    LOG_WARN("apply reloc rows on register failed", KR(ret), K(arg));
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

int ObLsMdsTabletRelocMdsHelper::on_replay(const char *buf,
                                           const int64_t len,
                                           const share::SCN &scn,
                                           BufferCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObLsMdsTabletRelocBatchArg arg;
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
             && ObLsMdsTabletRelocType::TRANSFER == arg.reloc_type_
             && OB_ISNULL(transfer_service = MTL(ObTransferService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer service is null for reloc final replay", KR(ret));
  } else if (arg.is_final_
             && ObLsMdsTabletRelocType::FORK == arg.reloc_type_
             && OB_ISNULL(fork_table_service = MTL(ObForkTableService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fork table service is null for reloc final replay", KR(ret));
  } else if (0 == arg.rows_.count() && !arg.is_final_) {
    // no-op
  } else if (OB_FAIL(apply_rows_(true /*for_replay*/, scn, arg, ctx))) {
    if (OB_NO_NEED_UPDATE == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("apply reloc rows on replay failed", KR(ret), K(scn), K(arg));
    }
  }
  if (OB_SUCC(ret) && arg.is_final_) {
    if (ObLsMdsTabletRelocType::TRANSFER == arg.reloc_type_) {
      transfer_service->wakeup();
    } else if (ObLsMdsTabletRelocType::FORK == arg.reloc_type_) {
      fork_table_service->wakeup();
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
