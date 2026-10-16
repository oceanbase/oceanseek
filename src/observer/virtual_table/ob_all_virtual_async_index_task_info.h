/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_ALL_VIRTUAL_ASYNC_INDEX_TASK_INFO_H_
#define OB_ALL_VIRTUAL_ASYNC_INDEX_TASK_INFO_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_multi_tenant_operator.h"
#include "storage/async_index/ob_async_index_define.h"

namespace oceanbase
{
namespace observer
{

// Per-tablet view of async fulltext index refresh tasks.
//
// Surfaces three segments per tenant: recently finished tasks (from the
// worker's bounded finished_ring_), currently in-flight tasks executing in
// the worker thread pool, and retry-pending tasks parked in each leader LS
// scheduler's retry list. In-flight and retry rows carry end_time_=0 and
// result_=INT_MIN to distinguish them from completed tasks.
class ObAllVirtualAsyncIndexTaskInfo : public common::ObVirtualTableScannerIterator,
                                       public omt::ObMultiTenantOperator
{
public:
  ObAllVirtualAsyncIndexTaskInfo();
  virtual ~ObAllVirtualAsyncIndexTaskInfo();

  virtual int inner_get_next_row(common::ObNewRow *&row) override;
  virtual void reset() override;
  inline void set_addr(const common::ObAddr &addr) { addr_ = addr; }

private:
  virtual bool is_need_process(uint64_t tenant_id) override;
  virtual int process_curr_tenant(common::ObNewRow *&row) override;
  virtual void release_last_tenant() override;

  // Pull a per-tenant snapshot of refresh task stats into snapshot_:
  // finished (finished_ring_), in-flight (task_pool_), and retry (per-LS).
  int prepare_to_read_();

private:
  common::ObAddr addr_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  common::ObSEArray<storage::ObRefreshTaskStat, 32> snapshot_;
  int64_t cursor_;
  bool prepared_;

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualAsyncIndexTaskInfo);
};

} // namespace observer
} // namespace oceanbase

#endif // OB_ALL_VIRTUAL_ASYNC_INDEX_TASK_INFO_H_
