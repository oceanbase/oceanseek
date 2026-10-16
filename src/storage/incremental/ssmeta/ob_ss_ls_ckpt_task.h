/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SS_LS_CKPT_TASK_H_
#define OCEANBASE_STORAGE_OB_SS_LS_CKPT_TASK_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_dag.h"

namespace oceanbase
{
namespace common
{
class ObString;
class ObStringBuffer;
}
namespace sslog
{
class ObSSLogKVPalfAdapter;
}
namespace storage
{

class ObSSLsCkptExecutor final
{
public:
  static const int64_t LS_CKPT_INTERVAL_US = 1800LL * 1000LL * 1000LL; // 30 minutes
  static const int64_t LS_CKPT_SCHEDULE_RETRY_INTERVAL_US = 5LL * 1000LL * 1000LL; // 5s
  static const int64_t LS_CKPT_BOOTSTRAP_POLL_INTERVAL_US = 100LL * 1000LL; // 100ms

  ObSSLsCkptExecutor() : is_inited_(false) {}
  ~ObSSLsCkptExecutor() { destroy(); }

  // The caller serializes lifecycle operations;
  // init()/destroy() must not overlap scheduling, waiting, or cancellation.
  int init();
  // Do not call from an LS checkpoint DAG task: waiting for this DAG's removal would deadlock.
  void cancel_checkpoint_dag_and_wait();
  void destroy();

  // NORMAL only submits the DAG; BOOTSTRAP also polls PALF KV until the checkpoint is published.
  int do_checkpoint(const ObSSLsCkptMode mode);
  int schedule_checkpoint_dag(const ObSSLsCkptMode mode, bool &is_scheduled);

private:
  static bool is_palf_kv_retryable_(const int ret);
  int prepare_ls_ckpt_palf_kv_(
      const share::ObLSID &ls_id,
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter,
      char *key_buf,
      const int64_t key_buf_len,
      common::ObString &kv_key) const;
  int get_ls_ckpt_palf_kv_raw_value_(
      const share::ObLSID &ls_id,
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter,
      common::ObStringBuffer &value_buffer) const;
  int check_ls_ckpt_palf_kv_exists_(
      const share::ObLSID &ls_id,
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const;
  int get_published_ckpt_palf_kv_(
      const share::ObLSID &ls_id,
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const;
  int delete_published_ckpt_palf_kv_(
      const share::ObLSID &ls_id,
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const;
  int wait_before_next_bootstrap_check_() const;
  int delete_old_bootstrap_palf_kv_once_(
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const;
  int clear_old_bootstrap_palf_kv_(
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const;
  int schedule_bootstrap_ckpt_dag_with_retry_(bool &is_scheduled);
  int check_is_sswriter_(bool &is_sswriter, int64_t &sswriter_epoch) const;
  int check_ckpt_dag_exist_(bool &exist) const;
  int confirm_bootstrap_ckpt_published_(
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter,
      bool &is_published) const;
  int check_bootstrap_ckpt_published_once_(
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter,
      bool &is_published) const;
  int wait_bootstrap_ckpt_published_(
      sslog::ObSSLogKVPalfAdapter &palf_kv_adapter) const;
  int wait_bootstrap_ckpt_();

private:
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObSSLsCkptExecutor);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SS_LS_CKPT_TASK_H_
