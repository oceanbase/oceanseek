/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_TABLET_STAT_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_TABLET_STAT_TASK_H_

#include "lib/task/ob_timer.h"
#include "lib/container/ob_se_array.h"
#include "lib/literals/ob_literals.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"

namespace oceanbase
{
namespace storage
{

class ObSSMacroCacheTabletStatTask : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSMacroCacheTabletStatTask(volatile bool &is_stopped);
  virtual ~ObSSMacroCacheTabletStatTask();
  int init();
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int clean_deleted_tablet_in_map_() const;
  void print_macro_cache_mgr_hit_stat_() const;
  static bool need_clean_deleted_tablet_(
      const int64_t current_time_us,
      const int64_t last_clean_time_us,
      const int64_t clean_interval_us);

  static const int64_t TABLET_STAT_SCHEDULE_INTERVAL_US = 120LL * 1000LL * 1000LL; // 2m

  int64_t last_clean_tablet_time_us_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_MACRO_CACHE_TABLET_STAT_TASK_H_ */