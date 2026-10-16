/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SS_LS_CKPT_DAG_H_
#define OCEANBASE_STORAGE_OB_SS_LS_CKPT_DAG_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_data.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_writer.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"

namespace oceanbase
{
namespace storage
{

enum class ObSSLsCkptMode : int64_t
{
  BOOTSTRAP = 0,
  NORMAL,
  MAX_MODE
};

class ObSSLsCkptModePolicy final
{
public:
  static constexpr int64_t DEFAULT_BOOTSTRAP_UPLOAD_PARALLELISM = 16;
  static constexpr int64_t NORMAL_UPLOAD_PARALLELISM = 1;

  static bool is_valid(const ObSSLsCkptMode mode);
  static bool need_check_sswriter(const ObSSLsCkptMode mode);
  static bool need_check_existing_object(const ObSSLsCkptMode mode);
  static int64_t get_upload_parallelism(const ObSSLsCkptMode mode);
};

// The DAG owns this context by value; tasks borrow it only during the DAG's lifetime.
class ObSSLsCkptWriteCtx final
{
public:
  ObSSLsCkptWriteCtx();
  ~ObSSLsCkptWriteCtx() = default;
  int init(
      const ObSSLsCkptMode mode,
      const int64_t sswriter_epoch,
      const share::ObLSID &ls_id);
  bool is_valid() const;
  ObSSLsCkptMode get_mode() const { return mode_; }
  int64_t get_upload_parallelism() const;
  int check_lease_valid() const;
  int check_status();
  void record_error(const int error_code);
  int get_first_error() const { return ATOMIC_LOAD(&dag_ret_); }
  void reset_next_upload_idx() { ATOMIC_STORE(&next_upload_idx_, 0); }
  int64_t fetch_next_tablet_idx() { return ATOMIC_FAA(&next_upload_idx_, 1); }
  int64_t get_next_upload_idx() const { return ATOMIC_LOAD(&next_upload_idx_); }
  TO_STRING_KV(K_(is_inited), K_(mode), K_(sswriter_epoch), K_(ls_id), K_(is_scn_reserved),
      K_(upload_parallelism), K_(ckpt_data), "tablet_count", tablet_keys_.count(),
      "next_upload_idx", get_next_upload_idx(), "dag_ret", get_first_error());

public:
  bool is_inited_;
  ObSSLsCkptMode mode_;
  int64_t sswriter_epoch_;
  share::ObLSID ls_id_;
  bool is_scn_reserved_;
  ObSSLsCkptData ckpt_data_;
  common::ObArray<ObSSMetaTabletKey> tablet_keys_;

private:
  int64_t upload_parallelism_;
  int64_t next_upload_idx_;
  int dag_ret_;

  DISALLOW_COPY_AND_ASSIGN(ObSSLsCkptWriteCtx);
};

struct ObSSLsCkptDagParam : public share::ObIDagInitParam
{
public:
  ObSSLsCkptDagParam()
    : mode_(ObSSLsCkptMode::MAX_MODE), sswriter_epoch_(-1), ls_id_()
  {}
  ObSSLsCkptDagParam(
      const ObSSLsCkptMode mode,
      const int64_t sswriter_epoch,
      const share::ObLSID &ls_id)
    : mode_(mode), sswriter_epoch_(sswriter_epoch), ls_id_(ls_id)
  {}
  virtual ~ObSSLsCkptDagParam() = default;
  bool is_valid() const override
  {
    return ObSSLsCkptModePolicy::is_valid(mode_)
        && ls_id_.is_valid()
        && ls_id_.is_sys_ls()
        && (!ObSSLsCkptModePolicy::need_check_sswriter(mode_) || sswriter_epoch_ >= 0);
  }
  TO_STRING_KV(K_(mode), K_(sswriter_epoch), K_(ls_id));

  ObSSLsCkptMode mode_;
  int64_t sswriter_epoch_;
  share::ObLSID ls_id_;
};

class ObSSLsCkptDag final : public share::ObIDag
{
public:
  ObSSLsCkptDag();
  int init_by_param(const share::ObIDagInitParam *param) override;
  int create_first_task() override;
  bool operator==(const share::ObIDag &other) const override;
  uint64_t hash() const override;
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  int report_result() override;
  bool inner_check_can_retry() override { return false; }
  lib::Worker::CompatMode get_compat_mode() const override { return lib::Worker::CompatMode::MYSQL; }
  uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(is_inited), K_(ctx));

private:
  bool is_inited_;
  ObSSLsCkptWriteCtx ctx_;
};

class ObSSLsCkptPrepareTask final : public share::ObITask
{
public:
  ObSSLsCkptPrepareTask();
  int init(ObSSLsCkptWriteCtx &ctx);
  int process() override;
  TO_STRING_KV(K_(is_inited), KP_(ctx));

private:
  int prepare_checkpoint_();
  int create_reserved_meta_scn_(
      share::SCN &reserved_meta_scn) const;
  void build_ckpt_op_init_param_(
      const int64_t op_id,
      ObSSMetaAtomicOpInitParam &init_param) const;
  int collect_tablet_keys_();

private:
  bool is_inited_;
  ObSSLsCkptWriteCtx *ctx_;
};

class ObSSLsCkptUploadTask final : public share::ObITask
{
public:
  ObSSLsCkptUploadTask();
  int init(ObSSLsCkptWriteCtx &ctx, const int64_t worker_idx);
  int process() override;
  int generate_next_task(share::ObITask *&next_task) override;
  TO_STRING_KV(K_(is_inited), K_(next_task_generated), K_(worker_idx), KP_(ctx));

private:
  bool is_inited_;
  bool next_task_generated_;
  int64_t worker_idx_;
  ObSSLsCkptWriteCtx *ctx_;
};

class ObSSLsCkptFinishTask final : public share::ObITask
{
public:
  ObSSLsCkptFinishTask();
  int init(ObSSLsCkptWriteCtx &ctx);
  int process() override;
  TO_STRING_KV(K_(is_inited), KP_(ctx));

private:
  int collect_tablet_meta_object_ids_();
  static int parse_palf_kv_meta_scn_(
      const share::ObLSID &ls_id,
      const common::ObString &value,
      share::SCN &meta_scn);
  static int write_to_palf_kv_(
      const share::ObLSID &ls_id,
      const share::SCN &meta_scn);
  static int remove_reserved_meta_scn_(
      const share::ObLSID &ls_id,
      const share::SCN &meta_scn);
  static int remove_reserved_meta_scns_at_or_before_(
      const share::ObLSID &ls_id,
      const share::SCN &meta_scn_upper_bound);

private:
  bool is_inited_;
  ObSSLsCkptWriteCtx *ctx_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SS_LS_CKPT_DAG_H_
