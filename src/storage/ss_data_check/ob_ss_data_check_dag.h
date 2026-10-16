/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_DAG_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_DAG_H_

#include "storage/ss_garbage_collector/dag/ob_ss_gc_dag_base.h"
#include "storage/ss_data_check/ob_ss_data_check_ctx.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_producer.h"

namespace oceanbase
{
namespace storage
{

struct ObSSDataCheckDagParam final : public share::ObIDagInitParam
{
  ObSSDataCheckDagParam();
  ~ObSSDataCheckDagParam() = default;

  int init(
      const uint64_t tenant_id,
      const share::SCN &snapshot_meta_scn,
      const ObSSDataCheckMode check_mode,
      const int64_t memory_limit,
      const int64_t file_buf_size,
      const int64_t io_expire_timestamp);
  bool is_valid() const;

  uint64_t tenant_id_;
  share::SCN snapshot_meta_scn_;
  ObSSDataCheckMode check_mode_;
  int64_t memory_limit_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;

  TO_STRING_KV(K_(tenant_id), K_(snapshot_meta_scn), K_(check_mode), K_(memory_limit),
      K_(file_buf_size), K_(io_expire_timestamp));
};

// DAG 串联固定 shared-meta 快照的 Expected 收集、归并、LIST 单向包含校验和
// HEALTH/NOT_HEALTH 发布。任一执行错误都会阻止业务状态发布。
class ObSSDataCheckDag final : public ObSSGCDagBase
{
public:
  static constexpr share::ObDagType::ObDagTypeEnum DAG_TYPE =
      share::ObDagType::DAG_TYPE_SS_DATA_CHECK;

  ObSSDataCheckDag();
  virtual ~ObSSDataCheckDag();

  virtual bool operator==(const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(
      compaction::ObIBasicInfoParam *&out_param,
      common::ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual int create_first_task() override;
  virtual int report_result() override;
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return lib::Worker::CompatMode::MYSQL; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }

  ObSSDataCheckCtx *get_ctx() { return ctx_; }
  const ObSSDataCheckCtx *get_ctx() const { return ctx_; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }

  INHERIT_TO_STRING_KV("ObIDag", share::ObIDag,
      K_(is_inited), K_(tenant_id), KP_(ctx));

private:
  int alloc_ctx_();
  void free_ctx_();
  int report_retry_state_(
      const int result,
      const share::ObIDag::ObDagStatus dag_status) const;

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObSSDataCheckCtx *ctx_;
  ObSSDataCheckSharedMetaExpectedSource shared_meta_source_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckDag);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_DAG_H_
