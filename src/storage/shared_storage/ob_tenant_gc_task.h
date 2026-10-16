/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_GC_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_GC_H_

#include "lib/task/ob_timer.h"
#include "storage/ob_super_block_struct.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_define.h"

namespace oceanbase
{
namespace storage
{

class ObTenantGCTask : public common::ObTimerTask
{
public:
  ObTenantGCTask(volatile bool &is_stopped);
  virtual ~ObTenantGCTask() = default;
  int init(const int tg_id);
  virtual void runTimerTask() override;
  TO_STRING_KV(K(GC_INTERVAL), K_(is_inited));
private:
  int get_gc_safe_time_val_(
      int64_t &gc_safe_time_val_us) const;
  int process_delete_tenant_shared_data_(
      ObSharedGCTask &task,
      const ObDeleteTenantTaskValue &value) const;
  int process_delete_server_task_(
      ObSharedGCTask &task,
      const ObDeleteServerTaskValue &value) const;
  int claim_task_(ObSharedGCTask &task) const;
  int finish_task_(const ObSharedGCTask &task) const;
  void loop_check_tenant_shared_dir_gc_() const;
  void loop_check_server_dir_gc_() const;
  void loop_check_tenant_private_dir_gc_() const;
  void delete_tenants_(
      ObIArray<storage::ObTenantItem> &items) const;
  int delete_tenant_(
      const storage::ObTenantItem &item) const;
  int delete_inner_table_rows_(const uint64_t tenant_id) const;
  int delete_shared_meta_ctrl_(const uint64_t tenant_id) const;
  //TODO(muwei.ym) wait for interface to FIX IT
  //int delete_shared_meta_() const;
  //int delete_shared_tier_meta_() const;
  //int delete_shared_gc_task_(const uint64_t tenant_id) const;
  int delete_shared_gc_ctrl_info_(const uint64_t tenant_id) const;
private:
  static const int64_t GC_INTERVAL;
  bool is_inited_;
  volatile bool &is_stopped_;
};

struct ObIsDeletedObj
{
  OB_UNIS_VERSION(1);
public:
  static const int64_t MAX_IS_DELETED_OBJ_SIZE;
  int64_t delete_ts_;
  TO_STRING_KV(K_(delete_ts));
};
OB_SERIALIZE_MEMBER_TEMP(inline, ObIsDeletedObj, delete_ts_);

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_TENANT_GC_H_ */
