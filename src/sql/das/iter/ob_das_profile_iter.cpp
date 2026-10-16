/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_profile_iter.h"
#include "sql/das/search/ob_das_search_context.h"
#include "sql/das/search/ob_i_das_search_op.h"
#include "sql/das/ob_das_scan_op.h"

namespace oceanbase
{
namespace sql
{

int ObDASProfileIter::inner_init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  ObDASProfileIterParam &profile_param = static_cast<ObDASProfileIterParam &>(param);
  tablet_id_ = profile_param.get_tablet_id();
  enable_profile_ = profile_param.is_enable_profile();
  root_profile_id_ = profile_param.get_root_profile_id();
  if (OB_UNLIKELY(!tablet_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet id", KR(ret), K(tablet_id_));
  } else {
    switch (root_profile_id_) {
      case common::ObProfileId::HYBRID_SEARCH: {
        partition_profile_id_ = common::ObProfileId::HYBRID_SEARCH_PARTITION;
        tablet_metric_id_ = common::ObMetricId::HS_TABLET_ID;
        break;
      }
      case common::ObProfileId::VECTOR_SEARCH: {
        partition_profile_id_ = common::ObProfileId::VECTOR_SEARCH_PARTITION;
        tablet_metric_id_ = common::ObMetricId::VEC_TABLET_ID;
        break;
      }
      default: {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("unsupported root profile id", KR(ret), K(root_profile_id_));
        break;
      }
    }
  }
  return ret;
}

int ObDASProfileIter::inner_reuse()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < children_cnt_; ++i) {
    if (OB_NOT_NULL(children_[i])) {
      if (OB_FAIL(children_[i]->reuse())) {
        LOG_WARN("failed to reuse child iter", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObDASProfileIter::inner_release()
{
  return OB_SUCCESS;
}

int ObDASProfileIter::create_partition_profile()
{
  int ret = OB_SUCCESS;
  execution_profile_ = nullptr;
  if (!enable_profile_) {
    partition_profile_ = nullptr;
  } else if (OB_UNLIKELY(!tablet_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet id", KR(ret), K(tablet_id_));
  } else if (OB_NOT_NULL(partition_profile_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition profile already created", KR(ret));
  } else if (OB_NOT_NULL(table_scan_profile_)) {
    common::ObOpProfile<common::ObMetric> *root_profile = nullptr;
    if (OB_FAIL(table_scan_profile_->get_or_register_child(
            root_profile_id_, root_profile))) {
      LOG_WARN("failed to get or register root profile", KR(ret), K(root_profile_id_));
    } else if (OB_FAIL(root_profile->register_child(
            partition_profile_id_, partition_profile_))) {
      LOG_WARN("failed to register partition profile", KR(ret));
    } else {
      /* The vector SQL path has no unified entry point at the sub path,
         so the Vector Iter node is created directly here */
      if (common::ObProfileId::VECTOR_SEARCH == root_profile_id_) {
        if (OB_FAIL(partition_profile_->register_child(
                common::ObProfileId::VECTOR_SEARCH_VEC_ITER,
                execution_profile_))) {
          LOG_WARN("failed to register vector iter profile", KR(ret));
        }
      } else {
        execution_profile_ = partition_profile_;
      }
      if (OB_SUCC(ret)) {
        common::ObProfileSwitcher switcher(partition_profile_);
        SET_METRIC_VAL(tablet_metric_id_, tablet_id_.id());
      }
    }
  }
  return ret;
}

int ObDASProfileIter::do_table_scan()
{
  int ret = OB_SUCCESS;
  table_scan_profile_ = common::get_current_profile();
  int profile_ret = create_partition_profile();
  if (OB_SUCCESS != profile_ret) {
    partition_profile_ = nullptr;
    execution_profile_ = nullptr;
    LOG_WARN("failed to create partition profile", KR(profile_ret));
  }
  if (OB_ISNULL(children_) || OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", KR(ret));
  } else {
    common::ObProfileSwitcher profile_switcher(execution_profile_);
    if (OB_FAIL(children_[0]->do_table_scan())) {
      LOG_WARN("failed to do table scan for child", KR(ret));
    }
  }
  return ret;
}

int ObDASProfileIter::rescan()
{
  int ret = OB_SUCCESS;
  partition_profile_ = nullptr;
  execution_profile_ = nullptr;
  int profile_ret = create_partition_profile();
  if (OB_SUCCESS != profile_ret) {
    partition_profile_ = nullptr;
    execution_profile_ = nullptr;
    LOG_WARN("failed to create partition profile", KR(profile_ret));
  }
  if (OB_ISNULL(children_) || OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", KR(ret));
  } else {
    common::ObProfileSwitcher profile_switcher(execution_profile_);
    if (OB_FAIL(children_[0]->rescan())) {
      LOG_WARN("failed to rescan for child", KR(ret));
    }
  }
  return ret;
}

int ObDASProfileIter::inner_get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(children_) || OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", KR(ret));
  } else {
    common::ObProfileSwitcher profile_switcher(execution_profile_);
    if (OB_FAIL(children_[0]->get_next_row())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from child", KR(ret));
      }
    }
  }
  return ret;
}

int ObDASProfileIter::inner_get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(children_) || OB_ISNULL(children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", KR(ret));
  } else {
    common::ObProfileSwitcher profile_switcher(execution_profile_);
    if (OB_FAIL(children_[0]->get_next_rows(count, capacity))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next rows from child", KR(ret));
      }
    }
  }
  return ret;
}

int ObDASProfileIter::init_runtime_profile(
    common::ObProfileId profile_id,
    common::ObOpProfile<common::ObMetric> *&profile_out)
{
  int ret = OB_SUCCESS;
  profile_out = nullptr;
  common::ObOpProfile<common::ObMetric> *parent_profile = common::get_current_profile();
  if (OB_NOT_NULL(parent_profile)) {
    if (OB_FAIL(parent_profile->register_child(profile_id, profile_out))) {
      LOG_WARN("failed to register profile", KR(ret), K(profile_id));
    }
  }
  return ret;
}

void ObDASProfileIter::clear_evaluated_flag()
{
  if (OB_NOT_NULL(children_) && OB_NOT_NULL(children_[0])) {
    children_[0]->clear_evaluated_flag();
  }
}

}  // namespace sql
}  // namespace oceanbase
