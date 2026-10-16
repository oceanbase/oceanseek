/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX MDS
#include "storage/multi_data_source/ob_tablet_mds_info_cache_ctx.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

ObTabletMdsInfoCacheCtx::ObTabletMdsInfoCacheCtx()
  : MdsCtx(),
    target_(Target::INVALID),
    ls_id_(),
    tablet_ids_()
{
  tablet_ids_.set_attr(lib::ObMemAttr(MTL_ID(), "MdsCaTblIDs"));
}

ObTabletMdsInfoCacheCtx::ObTabletMdsInfoCacheCtx(const MdsWriter &writer)
  : MdsCtx(writer),
    target_(Target::INVALID),
    ls_id_(),
    tablet_ids_()
{
  tablet_ids_.set_attr(lib::ObMemAttr(MTL_ID(), "MdsCaTblIDs"));
}

int ObTabletMdsInfoCacheCtx::add_tablet_id(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id", K(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_ids_.push_back(tablet_id))) {
    LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
  }
  return ret;
}

void ObTabletMdsInfoCacheCtx::notify_mds_info_cache_finish_(
    const bool is_commit,
    const int64_t commit_version)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSTabletService *tablet_svr = nullptr;

  if (tablet_ids_.empty()) {
    // 没有需要释放的资源，这是合法路径：回放场景下，如果 mds 数据已经被持久化的
    // tablet meta 覆盖（日志 scn <= ls 的 tablet change checkpoint scn），
    // ObTabletReplayExecutor 会跳过 do_replay_，因此该 ctx 上既没有执行过
    // set_target/set_ls_id，也没有发生过任何 ObMdsInfoCache::on_set（in-flight 加计数）。
    LOG_INFO("tablet_ids is empty, nothing to notify", K(target_), K(is_commit), K_(ls_id));
  } else if (Target::INVALID == target_ || !ls_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("target or ls_id is invalid", K(ret), K(target_), K(is_commit), K_(ls_id), K_(tablet_ids));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("fail to get ls", K(ret), K_(ls_id));
  } else if (OB_ISNULL(tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet svr is null", K(ret), K_(ls_id));
  } else {
    for (int64_t i = 0; i < tablet_ids_.count(); ++i) {
      const common::ObTabletID &tablet_id = tablet_ids_.at(i);
      if (OB_FAIL(tablet_svr->on_mds_info_cache_user_finish(tablet_id, target_, is_commit, commit_version))) {
        LOG_ERROR("on_mds_info_cache_user_finish failed",
                  K(ret), K(tablet_id), K_(ls_id), K(target_), K(is_commit), K(commit_version));
      }
    }
  }
}

void ObTabletMdsInfoCacheCtx::on_commit(const share::SCN &commit_version,
                                        const share::SCN &commit_scn)
{
  // Run base state-machine advancement first.
  MdsCtx::on_commit(commit_version, commit_scn);
  notify_mds_info_cache_finish_(true, commit_version.get_val_for_tx());
}

void ObTabletMdsInfoCacheCtx::on_abort(const share::SCN &abort_scn)
{
  MdsCtx::on_abort(abort_scn);
  notify_mds_info_cache_finish_(false, 0 /*ignored on abort*/);
}

} // namespace mds
} // namespace storage
} // namespace oceanbase
