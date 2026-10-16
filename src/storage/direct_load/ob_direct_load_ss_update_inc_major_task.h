/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_DIRECT_LOAD_SS_UPDATE_INC_MAJOR_TASK_H_
#define OCEANBASE_STORAGE_OB_DIRECT_LOAD_SS_UPDATE_INC_MAJOR_TASK_H_

#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/ob_storage_struct.h"

namespace oceanbase
{
namespace storage
{

class ObTabletHandle;
class ObLS;
class ObLSHandle;

class ObDirectLoadSSUpdateIncMajorITask
{
public:
  ObDirectLoadSSUpdateIncMajorITask(ObLS &ls,
                                    const ObTablet &tablet,
                                    const share::ObLSID &ls_id,
                                    const ObTabletID &tablet_id);
  virtual ~ObDirectLoadSSUpdateIncMajorITask() = default;
  virtual int process();
protected:
  virtual int update_tablet_table_store(const blocksstable::ObSSTable *new_inc_major,
                                        const ObIncMajorSSTableAction action) = 0;
protected:
  int get_ls_handle_and_tablet_handle(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle);
  int prepare_new_inc_major(
      blocksstable::ObSSTable &old_inc_major,
      blocksstable::ObSSTable *&new_inc_major,
      ObIncMajorSSTableAction &action);
  int prepare_new_committed_inc_major(
      blocksstable::ObSSTable &old_inc_major,
      const int64_t commit_version,
      blocksstable::ObSSTable *&new_inc_major);
protected:
  ObArenaAllocator allocator_;
  ObLS &ls_;
  const ObTablet &tablet_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
};

class ObDirectLoadSSUpdateLocalIncMajorTask : public ObDirectLoadSSUpdateIncMajorITask
{
public:
  ObDirectLoadSSUpdateLocalIncMajorTask(ObLS &ls,
                                        const ObTablet &tablet,
                                        const share::ObLSID &ls_id,
                                        const ObTabletID &tablet_id);
  virtual ~ObDirectLoadSSUpdateLocalIncMajorTask();
protected:
  int update_tablet_table_store(const blocksstable::ObSSTable *new_inc_major,
                                const ObIncMajorSSTableAction action) override;
};

class ObDirectLoadSSUpdateSharedIncMajorTask : public ObDirectLoadSSUpdateIncMajorITask
{
public:
  ObDirectLoadSSUpdateSharedIncMajorTask(ObLS &ls,
                                         const ObTablet &tablet,
                                         const share::ObLSID &ls_id,
                                         const ObTabletID &tablet_id);
  virtual ~ObDirectLoadSSUpdateSharedIncMajorTask();
protected:
  int update_tablet_table_store(const blocksstable::ObSSTable *new_inc_major,
                                const ObIncMajorSSTableAction action) override;
};

} // namespace storage
} // namespace oceanbase

#endif