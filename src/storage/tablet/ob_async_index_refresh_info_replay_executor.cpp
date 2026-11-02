/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include "storage/tablet/ob_async_index_refresh_info_replay_executor.h"
#include "storage/tx_storage/ob_ls_service.h"
#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::storage::mds;

namespace oceanbase
{
namespace storage
{

int ObTabletAsyncIndexRefreshInfoReplayExecutor::check_tablet_status_for_replay_(
    const ObTabletCreateDeleteMdsUserData &tablet_status,
    const MdsWriter &tablet_status_writer,
    const TwoPhaseCommitState trans_state,
    const MdsWriter &refresh_info_writer)
{
  int ret = OB_SUCCESS;
  // 随表创建时 refresh-info 与 TabletStatus 可能由同一个尚未提交的建表
  // 事务写入。只有 writer 相同才允许继续 replay；其他未提交创建、迁移或
  // 分裂状态仍返回 EAGAIN，不能放宽通用 Tablet 可见性校验。
  if (TwoPhaseCommitState::ON_COMMIT != trans_state) {
    const bool is_uncommitted_create =
        ObTabletStatus::NORMAL == tablet_status.tablet_status_
        && transaction::ObTransVersion::INVALID_TRANS_VERSION
            == tablet_status.create_commit_version_;
    const bool is_same_create_writer =
        is_uncommitted_create
        && tablet_status_writer == refresh_info_writer;
    if (!is_same_create_writer
        && (is_uncommitted_create
            || ObTabletStatus::TRANSFER_IN == tablet_status.tablet_status_
            || ObTabletStatus::SPLIT_DST == tablet_status.tablet_status_)) {
      ret = OB_EAGAIN;
    }
  }
  return ret;
}

ObTabletAsyncIndexRefreshInfoReplayExecutor::ObTabletAsyncIndexRefreshInfoReplayExecutor()
    : logservice::ObTabletReplayExecutor(), user_ctx_(nullptr), scn_(), info_(nullptr),
      floor_pin_only_(false)
{
}

int ObTabletAsyncIndexRefreshInfoReplayExecutor::init(BufferCtx &user_ctx,
                                                 const share::SCN &scn,
                                                 const ObTabletAsyncIndexRefreshInfo &info,
                                                 const bool floor_pin_only)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("async index refresh info replay executor init twice", KR(ret));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", KR(ret), K(scn));
  } else {
    user_ctx_ = &user_ctx;
    scn_ = scn;
    info_ = &info;
    floor_pin_only_ = floor_pin_only;
    is_inited_ = true;
  }
  return ret;
}

int ObTabletAsyncIndexRefreshInfoReplayExecutor::do_replay_(ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  MdsCtx *user_ctx = nullptr;
  ObTablet *tablet = tablet_handle.get_obj();
  ObTabletCreateDeleteMdsUserData tablet_status;
  MdsWriter tablet_status_writer;
  TwoPhaseCommitState trans_state;
  share::SCN trans_version;
  ObTabletAsyncIndexRefreshInfo merged_info;
  const ObTabletAsyncIndexRefreshInfo *replay_info = info_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(user_ctx_) || OB_ISNULL(info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index refresh info replay arguments are null",
        KR(ret), KP_(user_ctx), KP_(info));
  } else if (FALSE_IT(user_ctx = static_cast<MdsCtx *>(user_ctx_))) {
  } else if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", KR(ret), K(tablet_handle));
  } else if (tablet->is_empty_shell()) {
    ret = OB_NO_NEED_UPDATE;
  } else if (OB_FAIL(tablet->get_latest_tablet_status(
      tablet_status, tablet_status_writer, trans_state, trans_version))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_EAGAIN;
    } else {
      LOG_WARN("failed to get tablet status for async index refresh info replay",
          KR(ret), "tablet_id", tablet->get_tablet_id(), K(scn_));
    }
  } else if (OB_FAIL(check_tablet_status_for_replay_(
      tablet_status,
      tablet_status_writer,
      trans_state,
      user_ctx->get_writer()))) {
  } else if (floor_pin_only_) {
    ObLSHandle ls_handle;
    ObLSService *ls_service = MTL(ObLSService *);
    share::SCN mds_checkpoint_scn;
    // A durable floor-pin must be skipped before merging it with newer state.
    // Otherwise an obsolete pin can overflow the current checkpoint array before
    // replay_mds_node_ gets a chance to perform the same durability check.
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls service is null", KR(ret));
    } else if (OB_FAIL(ls_service->get_ls(
        tablet->get_tablet_meta().ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
      LOG_WARN("failed to get ls for floor-pin replay", KR(ret), K_(scn));
    } else if (OB_ISNULL(ls_handle.get_ls())
        || OB_ISNULL(ls_handle.get_ls()->get_mds_table())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls or mds table is null", KR(ret));
    } else if (OB_FAIL(ls_handle.get_ls()->get_mds_table()->get_mds_checkpoint_scn(
        mds_checkpoint_scn))) {
      LOG_WARN("failed to get mds checkpoint for floor-pin replay", KR(ret), K_(scn));
    } else if (scn_ <= mds_checkpoint_scn) {
      ret = OB_NO_NEED_UPDATE;
    }
    ObTabletAsyncIndexRefreshInfo current_info;
    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ret)) {
      // Keep checkpoint lookup errors and the durable-log skip result.
    } else if (OB_TMP_SUCC(tablet->get_async_index_refresh_info(share::SCN::max_scn(), current_info))) {
      if (!current_info.need_apply_floor_pin(*info_)) {
        const common::ObIArray<int64_t> &snapshots = current_info.get_refresh_snapshots();
        const common::ObIArray<int64_t> &new_snapshots = info_->get_refresh_snapshots();
        const int64_t new_pin = new_snapshots.empty() ? 0 : new_snapshots.at(0);
        ret = OB_NO_NEED_UPDATE;
        LOG_INFO("[ASYNC_INDEX] floor-pin replay: skip, existing pin <= new floor_pin",
                 "tablet_id", tablet->get_tablet_id(), "current_pin0", snapshots.at(0), K(new_pin), K_(scn));
      } else if (OB_FAIL(current_info.merge_floor_pin(*info_))) {
        LOG_WARN("failed to merge replay floor pin", K(ret), K_(scn));
      } else if (OB_FAIL(merged_info.assign(current_info))) {
        LOG_WARN("failed to assign replay floor pin", K(ret));
      } else {
        replay_info = &merged_info;
      }
    } else if (OB_EMPTY_RESULT != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("fail to read async index refresh info for floor-pin check in replay",
               KR(ret), K_(scn), "tablet_id", tablet->get_tablet_id());
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(replay_to_mds_table_(tablet_handle, *replay_info, *user_ctx, scn_))) {
      LOG_WARN("fail to replay async index refresh info", KR(ret), K(scn_));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
