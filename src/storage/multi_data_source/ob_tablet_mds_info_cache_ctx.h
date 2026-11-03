/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_INFO_CACHE_CTX
#define OCEANBASE_STORAGE_OB_TABLET_MDS_INFO_CACHE_CTX

#include "lib/container/ob_se_array.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/tablet/ob_tablet_mds_data_cache.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

// MdsCtx for truncate, TTL and async-index cache notifications.
// Async-index writes retain the committed cache; commit invalidates it, abort does not.
//
// Carries (ls_id, target, tablet_ids) so that on commit/abort we can find the
// owning tablets and let their ObMdsInfoCache release the in-flight slot
// (and, on commit, advance the cache's max-known-commit-scn). Both ObMdsCtx
// hooks run lock-free at the cache level — we acquire the t3m bucket RLock
// while resolving the live tablet so the dec is serialized with
// update_tablet's bucket WLock, eliminating the orphan-decrement race that
// would otherwise occur if copy_mds_info_cache_from interleaved with a
// concurrent commit.
//
// Persistence: OB_SERIALIZE_MEMBER_TEMP after base MdsCtx (same pattern as
// mds_ctx.h).
class ObTabletMdsInfoCacheCtx : public MdsCtx
{
  OB_UNIS_VERSION(1);
public:
  using Target = storage::ObMdsInfoCacheTarget;

public:
  ObTabletMdsInfoCacheCtx();
  explicit ObTabletMdsInfoCacheCtx(const MdsWriter &writer);
  virtual ~ObTabletMdsInfoCacheCtx() = default;

  void set_target(const Target target) { target_ = target; }
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  // Append one tablet_id whose ObMdsInfoCache::on_set was just invoked under
  // this ctx. Must be called BEFORE the matching on_set so a successful
  // increment is always paired with one mark-done at on_commit/on_abort.
  int add_tablet_id(const common::ObTabletID &tablet_id);

  Target get_target() const { return target_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }

  virtual void on_commit(const share::SCN &commit_version,
                         const share::SCN &commit_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;

  INHERIT_TO_STRING_KV("MdsCtx", MdsCtx,
                       "target", static_cast<uint8_t>(target_),
                       K_(ls_id),
                       K_(tablet_ids));

private:
  Target                                   target_;
  share::ObLSID                            ls_id_;
  // Runtime-only; not serialized. Repopulated on the leader during
  // on_register and on the follower during on_replay before either
  // on_commit or on_abort fires.
  common::ObSEArray<common::ObTabletID, 4> tablet_ids_;

  // Shared by on_commit / on_abort: resolve LS and notify each tablet's cache.
  void notify_mds_info_cache_finish_(const bool is_commit,
                                      const int64_t commit_version_tx_val);

  DISALLOW_COPY_AND_ASSIGN(ObTabletMdsInfoCacheCtx);
};
OB_SERIALIZE_MEMBER_TEMP(inline, (ObTabletMdsInfoCacheCtx, MdsCtx), target_, ls_id_);

} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MDS_INFO_CACHE_CTX
