/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SERVER_SLOG_FLUSH_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SERVER_SLOG_FLUSH_TASK_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace blocksstable
{
  class ObStorageObjectHandle;
  class MacroBlockId;
}
namespace storage
{

class ObSSMacroCacheFlushInfo;

class ObServerSlogFlushTask : public common::ObTimerTask
{
public:
  explicit ObServerSlogFlushTask(volatile bool &is_stopped);
  virtual ~ObServerSlogFlushTask() = default;
  int start(const int tg_id);
  virtual void runTimerTask() override;
  TO_STRING_KV(K_(is_inited), K_(is_stopped));

private:
  int flush_slog(const ObSSMacroCacheFlushInfo &flush_info);
  int read_file(const ObSSMacroCacheFlushInfo &flush_info,
                const int64_t file_len,
                char *buf,
                blocksstable::ObStorageObjectHandle &read_handle);
  int write_file(const ObSSMacroCacheFlushInfo &flush_info,
                 blocksstable::ObStorageObjectHandle &read_handle,
                 blocksstable::ObStorageObjectHandle &write_handle);
  int erase_from_flush_map(const blocksstable::MacroBlockId &file_id);

private:
  static const int INVALID_TG_ID = -1;
  static const int64_t SCHEDULE_INTERVAL_US = 1 * 1000 * 1000L; // 1s
  bool is_inited_;
  volatile bool &is_stopped_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_TASK_OB_SERVER_SLOG_FLUSH_TASK_H_ */
