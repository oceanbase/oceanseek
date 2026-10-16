/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_REPLAY_EXECUTOR_H
#define OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_REPLAY_EXECUTOR_H

#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/tablet/ob_async_index_refresh_info.h"

namespace oceanbase
{
namespace storage
{

class ObTabletAsyncIndexRefreshInfoReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletAsyncIndexRefreshInfoReplayExecutor();
  ~ObTabletAsyncIndexRefreshInfoReplayExecutor() = default;
  int init(mds::BufferCtx &user_ctx, const share::SCN &scn,
           const ObTabletAsyncIndexRefreshInfo &info,
           const bool floor_pin_only = false);
protected:
  int do_replay_(ObTabletHandle &tablet_handle) override;
  bool is_replay_update_tablet_status_() const override
  {
    // 随表创建异步索引会在 CREATE_TABLET 的同一事务注册
    // ASYNC_INDEX_REFRESH_INFO。复用 TABLET_BINDING replay 模式，使建表
    // 状态尚未提交时也能取得 Tablet；do_replay_ 再校验 writer 与状态。
    return true;
  }
  bool is_replay_update_mds_table_() const override
  {
    return true;
  }
  int replay_check_restore_status_(ObTabletHandle &tablet_handle) override
  {
    return logservice::ObTabletReplayExecutor::replay_check_restore_status(
        tablet_handle, false /* update_tx_data */);
  }
private:
  static int check_tablet_status_for_replay_(
      const ObTabletCreateDeleteMdsUserData &tablet_status,
      const mds::MdsWriter &tablet_status_writer,
      const mds::TwoPhaseCommitState trans_state,
      const mds::MdsWriter &refresh_info_writer);

  mds::BufferCtx *user_ctx_;
  share::SCN scn_;
  const ObTabletAsyncIndexRefreshInfo *info_;
  bool floor_pin_only_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_ASYNC_INDEX_REFRESH_INFO_REPLAY_EXECUTOR_H
