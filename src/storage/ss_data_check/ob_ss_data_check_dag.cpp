/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_data_check/ob_ss_data_check_dag.h"
#include "lib/time/ob_time_utility.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_producer.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_sort_task.h"
#include "storage/ss_data_check/ob_ss_data_check_list_task.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"

namespace oceanbase
{
namespace storage
{

using namespace common;
using namespace share;

ObSSDataCheckDagParam::ObSSDataCheckDagParam()
  : tenant_id_(OB_INVALID_TENANT_ID),
    snapshot_meta_scn_(),
    check_mode_(ObSSDataCheckMode::MAX),
    memory_limit_(0),
    file_buf_size_(0),
    io_expire_timestamp_(-1)
{
}

int ObSSDataCheckDagParam::init(
    const uint64_t tenant_id,
    const share::SCN &snapshot_meta_scn,
    const ObSSDataCheckMode check_mode,
    const int64_t memory_limit,
    const int64_t file_buf_size,
    const int64_t io_expire_timestamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_valid())) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check dag param initialized twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !snapshot_meta_scn.is_valid()
      || !ObSSDataCheckModeUtil::is_enabled(check_mode)
      || memory_limit < ObExternalSortConstant::MIN_MEMORY_LIMIT
      || !ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size)
      || (memory_limit / file_buf_size - 1) / 2 < 2 || io_expire_timestamp < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check dag param", KR(ret), K(tenant_id),
        K(snapshot_meta_scn), K(check_mode), K(memory_limit), K(file_buf_size), K(io_expire_timestamp));
  } else {
    tenant_id_ = tenant_id;
    snapshot_meta_scn_ = snapshot_meta_scn;
    check_mode_ = check_mode;
    memory_limit_ = memory_limit;
    file_buf_size_ = file_buf_size;
    io_expire_timestamp_ = io_expire_timestamp;
  }
  return ret;
}

bool ObSSDataCheckDagParam::is_valid() const
{
  return is_valid_tenant_id(tenant_id_) && snapshot_meta_scn_.is_valid()
      && ObSSDataCheckModeUtil::is_enabled(check_mode_)
      && memory_limit_ >= ObExternalSortConstant::MIN_MEMORY_LIMIT
      && ObSSDataCheckExpectedSortUtil::is_valid_file_buffer_size(file_buf_size_)
      && (memory_limit_ / file_buf_size_ - 1) / 2 >= 2 && io_expire_timestamp_ >= 0;
}

ObSSDataCheckDag::ObSSDataCheckDag()
  : ObSSGCDagBase(DAG_TYPE),
    is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    ctx_(nullptr),
    shared_meta_source_()
{
}

ObSSDataCheckDag::~ObSSDataCheckDag()
{
  shared_meta_source_.reset();
  free_ctx_();
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int ObSSDataCheckDag::alloc_ctx_()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_NOT_NULL(ctx_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check dag context already allocated", KR(ret), KP_(ctx));
  } else if (OB_ISNULL(buf = mtl_malloc(
      sizeof(ObSSDataCheckCtx), ObMemAttr(MTL_ID(), "SSDataCheckCtx")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate data check dag context", KR(ret));
  } else {
    ctx_ = new (buf) ObSSDataCheckCtx();
  }
  return ret;
}

void ObSSDataCheckDag::free_ctx_()
{
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObSSDataCheckCtx();
    mtl_free(ctx_);
    ctx_ = nullptr;
  }
}

bool ObSSDataCheckDag::operator==(const ObIDag &other) const
{
  bool is_equal = false;
  if (this == &other) {
    is_equal = true;
  } else if (get_type() == other.get_type()) {
    const ObSSDataCheckDag &other_dag = static_cast<const ObSSDataCheckDag &>(other);
    is_equal = tenant_id_ == other_dag.tenant_id_;
  }
  return is_equal;
}

uint64_t ObSSDataCheckDag::hash() const
{
  uint64_t hash_value = 0;
  hash_value = murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  hash_value = murmurhash(&type_, sizeof(type_), hash_value);
  return hash_value;
}

int ObSSDataCheckDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  const int first_error = OB_NOT_NULL(ctx_) ? ctx_->get_first_error() : OB_NOT_INIT;
  return ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(), static_cast<int64_t>(tenant_id_),
      static_cast<int64_t>(first_error));
}

int ObSSDataCheckDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "tenant_id=%lu,dag_type=%s",
      tenant_id_, ObIDag::get_dag_type_str(get_type())))) {
    LOG_WARN("failed to fill data check dag key", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObSSDataCheckDag::init_by_param(const ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const ObSSDataCheckDagParam *dag_param = static_cast<const ObSSDataCheckDagParam *>(param);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("data check dag initialized twice", KR(ret), K_(tenant_id));
  } else if (OB_ISNULL(dag_param) || OB_UNLIKELY(!dag_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid data check dag param", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(alloc_ctx_())) {
    LOG_WARN("failed to allocate data check dag context", KR(ret), KPC(dag_param));
  } else if (OB_FAIL(ctx_->init(
      dag_param->tenant_id_, dag_param->snapshot_meta_scn_, dag_param->check_mode_,
      dag_param->memory_limit_, dag_param->file_buf_size_, dag_param->io_expire_timestamp_))) {
    LOG_WARN("failed to initialize data check dag context", KR(ret), KPC(dag_param));
    free_ctx_();
  } else {
    tenant_id_ = dag_param->tenant_id_;
    is_inited_ = true;
  }
  return ret;
}

int ObSSDataCheckDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObSSDataCheckExpectedCollectTask *collect_task = nullptr;
  ObSSDataCheckExpectedMergeStartTask *merge_start_task = nullptr;
  ObSSDataCheckExpectedMergeCompleteTask *merge_complete_task = nullptr;
  ObSSDataCheckListCheckTask *list_check_task = nullptr;
  ObSSDataCheckFinalizeTask *finalize_task = nullptr;
  if (OB_UNLIKELY(!is_inited_) || OB_ISNULL(ctx_) || OB_UNLIKELY(!ctx_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("data check dag is not initialized", KR(ret), K_(is_inited), K_(tenant_id), KP_(ctx));
  } else if (OB_FAIL(alloc_task(collect_task))) {
    LOG_WARN("failed to allocate expected collect task", KR(ret));
  } else if (OB_FAIL(alloc_task(merge_start_task))) {
    LOG_WARN("failed to allocate expected merge start task", KR(ret));
  } else if (OB_FAIL(alloc_task(merge_complete_task))) {
    LOG_WARN("failed to allocate expected merge complete task", KR(ret));
  } else if (OB_FAIL(alloc_task(list_check_task))) {
    LOG_WARN("failed to allocate data check LIST task", KR(ret));
  } else if (OB_FAIL(alloc_task(finalize_task))) {
    LOG_WARN("failed to allocate data check finalize task", KR(ret));
  } else if (OB_FAIL(merge_complete_task->init(ctx_))) {
    LOG_WARN("failed to initialize expected merge complete task", KR(ret));
  } else if (OB_FAIL(list_check_task->init(ctx_))) {
    LOG_WARN("failed to initialize data check LIST task", KR(ret));
  } else if (OB_FAIL(finalize_task->init(ctx_))) {
    LOG_WARN("failed to initialize data check finalize task", KR(ret));
  } else if (OB_FAIL(merge_start_task->init(ctx_, merge_complete_task))) {
    LOG_WARN("failed to initialize expected merge start task", KR(ret));
  } else if (OB_FAIL(collect_task->init(ctx_, &shared_meta_source_))) {
    LOG_WARN("failed to initialize expected collect task", KR(ret));
  } else if (OB_FAIL(collect_task->add_child(*merge_start_task))) {
    LOG_WARN("failed to wire expected collect to merge start", KR(ret));
  } else if (OB_FAIL(merge_start_task->add_child(*merge_complete_task))) {
    LOG_WARN("failed to wire data check expected merge anchors", KR(ret));
  } else if (OB_FAIL(merge_complete_task->add_child(*list_check_task))) {
    LOG_WARN("failed to wire expected merge to LIST check", KR(ret));
  } else if (OB_FAIL(list_check_task->add_child(*finalize_task))) {
    LOG_WARN("failed to wire LIST check to data check finalize", KR(ret));
  } else if (OB_FAIL(add_task(*collect_task))) {
    LOG_WARN("failed to add expected collect task", KR(ret));
  } else if (OB_FAIL(add_task(*merge_start_task))) {
    LOG_WARN("failed to add expected merge start task", KR(ret));
  } else if (OB_FAIL(add_task(*merge_complete_task))) {
    LOG_WARN("failed to add expected merge complete task", KR(ret));
  } else if (OB_FAIL(add_task(*list_check_task))) {
    LOG_WARN("failed to add data check LIST task", KR(ret));
  } else if (OB_FAIL(add_task(*finalize_task))) {
    LOG_WARN("failed to add data check finalize task", KR(ret));
  }
  return ret;
}

int ObSSDataCheckDag::report_result()
{
  const int dag_ret = get_dag_ret();
  const ObDagStatus dag_status = get_dag_status();
  const int first_error = OB_NOT_NULL(ctx_) ? ctx_->get_first_error() : OB_NOT_INIT;
  const int64_t active_task_count = OB_NOT_NULL(ctx_) ? ctx_->get_active_task_count() : 0;
  const bool has_published_status = OB_NOT_NULL(ctx_) && ctx_->has_published_status();
  ObSSDataCheckStatus status = ObSSDataCheckStatus::HEALTH;
  int status_ret = OB_SUCCESS;
  int result = dag_ret;
  if (OB_SUCCESS == result && ObIDag::DAG_STATUS_FINISH != dag_status) {
    result = OB_CANCELED;
  } else if (OB_SUCCESS == result && OB_SUCCESS != first_error) {
    result = first_error;
  } else if (OB_SUCCESS == result && 0 != active_task_count) {
    result = OB_STATE_NOT_MATCH;
  } else if (OB_SUCCESS == result && !has_published_status) {
    result = OB_STATE_NOT_MATCH;
  } else if (OB_SUCCESS == result && OB_SUCCESS != (status_ret = ctx_->get_status(status))) {
    result = status_ret;
    LOG_WARN_RET(status_ret, "failed to get published data check status", K_(tenant_id));
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(report_retry_state_(result, dag_status))) {
    LOG_WARN_RET(tmp_ret, "failed to report data check retry state",
        KR(tmp_ret), K_(tenant_id), K(result), K(dag_status));
  }
  // 业务状态只由 FinalizeTask 发布，本函数仅报告最终执行结果。
  LOG_INFO("finish data check dag", K_(tenant_id), K(dag_ret), K(dag_status),
      K(first_error), K(active_task_count), K(has_published_status), K(status), K(result));
  return OB_SUCCESS;
}

int ObSSDataCheckDag::report_retry_state_(
    const int result,
    const ObIDag::ObDagStatus dag_status) const
{
  int ret = OB_SUCCESS;
  ObTenantDagScheduler *dag_scheduler = MTL(ObTenantDagScheduler *);
  ObSSGCService *gc_service = nullptr;
  if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant dag scheduler is null when reporting data check result",
        KR(ret), K_(tenant_id), K(result), K(dag_status));
  } else if (dag_scheduler->has_set_stop()) {
    LOG_INFO("skip reporting data check retry state after scheduler stopped",
        K_(tenant_id), K(result), K(dag_status));
  } else if (OB_ISNULL(gc_service = MTL(ObSSGCService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared storage gc service is null when reporting data check result",
        KR(ret), K_(tenant_id), K(result), K(dag_status));
  } else {
    const bool need_retry = OB_SUCCESS != result;
    gc_service->report_data_check_dag_result(need_retry, ObTimeUtility::current_time_us());
    LOG_INFO("report data check dag retry state",
        K_(tenant_id), K(result), K(dag_status), K(need_retry));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
