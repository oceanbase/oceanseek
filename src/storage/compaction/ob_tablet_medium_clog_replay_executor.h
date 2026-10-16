/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_STORAGE_COMPACTION_TABLET_MEDIUM_CLOG_REPLAY_EXECUTOR_H_
#define OB_STORAGE_COMPACTION_TABLET_MEDIUM_CLOG_REPLAY_EXECUTOR_H_

#include "logservice/replayservice/ob_tablet_replay_executor.h"
#include "storage/compaction/ob_medium_compaction_info.h"

namespace oceanbase
{
namespace compaction
{

class ObTabletMediumClogReplayExecutor final : public logservice::ObTabletReplayExecutor
{
public:
  ObTabletMediumClogReplayExecutor(ObMediumCompactionInfo &medium_info);
  int init(const share::SCN &scn);
protected:
  bool is_replay_update_tablet_status_() const override
  {
    return false;
  }
  int do_replay_(storage::ObTabletHandle &tablet_handle) override;
  virtual bool is_replay_update_mds_table_() const override
  {
    return true;
  }
private:
  ObMediumCompactionInfo &medium_info_;
  share::SCN scn_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_TABLET_MEDIUM_CLOG_REPLAY_EXECUTOR_H_
