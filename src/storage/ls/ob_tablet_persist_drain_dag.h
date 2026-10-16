/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_LS_OB_TABLET_PERSIST_DRAIN_DAG_H_
#define OCEANBASE_STORAGE_LS_OB_TABLET_PERSIST_DRAIN_DAG_H_

#include "common/ob_tablet_id.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace storage
{

// Snapshot tablet_ids collected by ObLSTabletService::flush and drained asynchronously.
struct ObTabletPersistDrainParam : public share::ObIDagInitParam
{
public:
  static constexpr int64_t MAX_TABLET_CNT = 20000;
  static constexpr int64_t BATCH_SIZE = 32;
  static constexpr int64_t MAX_CONCURRENT_TASK_CNT = 2;

  ObTabletPersistDrainParam();
  virtual ~ObTabletPersistDrainParam() { reset(); }
  virtual bool is_valid() const override;
  void reset();
  int assign(const ObTabletPersistDrainParam &other);
  int64_t get_hash() const;
  TO_STRING_KV(K_(ls_id), K_(recycle_scn), "tablet_cnt", tablet_ids_.count());

  share::ObLSID ls_id_;
  share::SCN recycle_scn_;
  common::ObSEArray<common::ObTabletID, 64> tablet_ids_;
};

class ObTabletPersistDrainTask;
class ObTabletPersistDrainDag : public share::ObIDag
{
public:
  ObTabletPersistDrainDag();
  virtual ~ObTabletPersistDrainDag();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  virtual bool ignore_warning() override { return true; }
  const ObTabletPersistDrainParam &get_param() const { return param_; }
  share::ObLSID get_ls_id() const { return param_.ls_id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(param));
private:
  bool is_inited_;
  ObTabletPersistDrainParam param_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletPersistDrainDag);
};

class ObTabletPersistDrainTask : public share::ObITask
{
public:
  ObTabletPersistDrainTask();
  virtual ~ObTabletPersistDrainTask();
  int init(const int64_t idx);
  virtual int generate_next_task(ObITask *&next_task) override;
  virtual int process() override;
private:
  int64_t get_start_idx() const { return idx_ * ObTabletPersistDrainParam::BATCH_SIZE; }
  int64_t get_end_idx() const;
  bool is_inited_;
  ObTabletPersistDrainDag *dag_;
  int64_t idx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletPersistDrainTask);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LS_OB_TABLET_PERSIST_DRAIN_DAG_H_
