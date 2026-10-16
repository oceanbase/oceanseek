/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_HOT_TABLET_MACRO_CACHE_GC_TASK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_HOT_TABLET_MACRO_CACHE_GC_TASK_H_

#include "storage/shared_storage/macro_cache/task/ob_ss_macro_cache_task_base.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/literals/ob_literals.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace blocksstable
{
  class MacroBlockId;
}
namespace storage
{
class ObSSMacroCacheMgr;

class ObSSHotTabletMacroCacheGCTask : public ObSSMacroCacheTaskBase
{
public:
  explicit ObSSHotTabletMacroCacheGCTask(volatile bool &is_stopped);
  virtual ~ObSSHotTabletMacroCacheGCTask();
  int init(ObSSMacroCacheMgr *macro_cache_mgr);
  int start(const int tg_id);
  void destroy();
  virtual void runTimerTask() override;

private:
  int gc_hot_tablet_macro_cache(
      const int64_t idle_time_us,
      const bool convert_to_auto = false);
  int get_ls_id_epoch_map(common::hash::ObHashMap<share::ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map);
  // 将五类 LS inner tablet 在全部 LS 中的有效引用加入全局引用集合。
  int get_ls_inner_macro_block_set_(
      const common::hash::ObHashMap<share::ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map,
      common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_set);
  int get_macro_block_set(const common::hash::ObHashMap<share::ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map,
                          common::hash::ObHashMap<common::ObTabletID, share::ObLSID> &hot_tablet_map,
                          common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_set);
  int check_cur_tablet_version(const common::ObIArray<int64_t> &tablet_versions,
                               const int64_t current_tablet_version,
                               bool &is_cur_tablet_version_exist);
  int get_macro_blocks(const share::ObLSID &ls_id,
                       const int64_t ls_epoch,
                       const common::ObTabletID &tablet_id,
                       const int64_t tablet_version,
                       const int64_t reorganization_scn,
                       common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_set);
  int get_macro_blocks(const share::ObLSID &ls_id,
                       const int64_t ls_epoch,
                       const common::ObTabletID &tablet_id,
                       const int64_t tablet_version,
                       const int64_t reorganization_scn,
                       const bool is_shared,
                       common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_set);
  int get_macro_block_set_for_single_tablet(const common::hash::ObHashMap<share::ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map,
                                            const common::ObTabletID &tablet_id,
                                            const share::ObLSID &ls_id,
                                            common::hash::ObHashSet<blocksstable::MacroBlockId> &macro_block_set,
                                            bool &is_get);
  int gc_macro_hot_cache(const common::hash::ObHashMap<share::ObLSID, int64_t/*ls_epoch*/> &ls_id_epoch_map,
                         const common::hash::ObHashMap<common::ObTabletID, share::ObLSID> &macro_hot_tablet_map);
private:
  static constexpr int64_t SCHEDULE_INTERVAL_US = 1_min;
  ObSSMacroCacheMgr *macro_cache_mgr_;
  int64_t last_gc_time_us_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_TASK_OB_SS_HOT_TABLET_MACRO_CACHE_GC_TASK_H_ */
