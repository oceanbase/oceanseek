// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE_COMPACTION

#include "storage/compaction_v2/ob_ss_link_major_dag.h"
#include "storage/compaction_v2/ob_ss_link_major_task.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"

namespace oceanbase
{
namespace compaction
{

bool ObLinkSSMajorSSTableDag::operator==(const ObIDag &other) const
{
  bool bret = false;
  if (this == &other) {
    bret = true;
  } else if (get_type() != other.get_type()) {
    bret = false;
  } else {
    const ObLinkSSMajorSSTableDag &o = static_cast<const ObLinkSSMajorSSTableDag&>(other);
    if (o.param_.ls_id_ == param_.ls_id_ 
        && o.param_.tablet_id_ == param_.tablet_id_
        && o.param_.merge_version_ == param_.merge_version_) {
      bret = true;
    }
  }
  return bret;
}

uint64_t ObLinkSSMajorSSTableDag::hash() const
{
  uint64_t hash_value = 0;
  hash_value = param_.tablet_id_.hash();
  hash_value = common::murmurhash(&param_.ls_id_, sizeof(param_.ls_id_), hash_value);
  hash_value = common::murmurhash(&param_.merge_version_, sizeof(param_.merge_version_), hash_value);
  return hash_value;
}

int ObLinkSSMajorSSTableDag::fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param, allocator, get_type(),
                                      static_cast<int64_t>(param_.ls_id_.id()),
                                      static_cast<int64_t>(param_.tablet_id_.id()),
                                      param_.merge_version_))) {
    LOG_WARN("failed to fill info param", K(ret));
  }
  return ret;
}

int ObLinkSSMajorSSTableDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("param is null", K(ret));
  } else if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dag is already inited", K(ret));
  } else {
    const ObTabletMergeDagParam *dag_param = static_cast<const ObTabletMergeDagParam*>(param);
    param_ = *dag_param;
    compat_mode_ = THIS_WORKER.get_compatibility_mode();
    is_inited_ = true;
  }
  return ret;
}

int ObLinkSSMajorSSTableDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%ld tablet_id=%lu merge_version=%ld",
                               param_.ls_id_.id(), param_.tablet_id_.id(), param_.merge_version_))) {
    LOG_WARN("failed to fill dag key", K(ret));
  }
  return ret;
}

int ObLinkSSMajorSSTableDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletSSLinkMajorTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret), KPC(this));
  } else if (OB_FAIL(alloc_task(task, param_.ls_id_, param_.tablet_id_, param_.merge_version_, param_.merge_base_version_, param_.merge_type_, true /* prewarm */, true /* need_report */))) {
    LOG_WARN("fail to create task", KR(ret));
  } else {
    if (OB_FAIL(add_task(*task))) {
      LOG_WARN("fail to add task", KR(ret), KPC(this));
    } else {
      LOG_INFO("success to add link major task", KPC(this), K(param_));
    }
  }
  return ret;
}

} // namespace compaction
} // namespace oceanbase

