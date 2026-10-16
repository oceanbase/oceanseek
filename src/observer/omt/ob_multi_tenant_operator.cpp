/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER_OMT
#include "observer/omt/ob_multi_tenant_operator.h"
#include "src/share/ob_server_struct.h"
#include "observer/omt/ob_tenant.h"

namespace oceanbase
{
namespace omt
{

ObMultiTenantOperator::ObMultiTenantOperator() :
  inited_(false),
  tenant_idx_(-1),
  tenant_(nullptr)
{}

ObMultiTenantOperator::~ObMultiTenantOperator()
{
  reset();
}

void ObMultiTenantOperator::reset()
{
  if (inited_) {
    // 如果迭代未结束，释放残留的资源
    int ret = OB_SUCCESS;
    if (tenant_idx_ >= 0 && tenant_idx_ < tenant_ids_.size()) {
      uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
      if (tenant_ != nullptr) {
        if (tenant_id != tenant_->id()) {
          LOG_ERROR("ObMultiTenantOperator::reset", K(tenant_id), K(tenant_->id()));
          abort();
        }
        {
          share::ObTenantSwitchGuard guard(tenant_);
          release_last_tenant();
        }
        tenant_->unlock(handle_);
      }
    }
    if (OB_FAIL(ret)) {
      // 无法处理资源释放不了的问题
      abort();
    } else {
      tenant_ids_.reuse();
      tenant_idx_ = -1;
      tenant_ = nullptr;
      inited_ = false;
    }
  }
}

int ObMultiTenantOperator::init()
{
  int ret = OB_SUCCESS;
  ObMultiTenant *omt = GCTX.omt_;
  TenantIdList tenant_list;
  if (omt == nullptr) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("operator init", K(ret));
  } else if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("operator init", K(ret));
  } else {
    if (tenant_ids_.size() != 0 || tenant_idx_ != -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("operator init", K(ret), K(tenant_idx_), K(tenant_ids_));
    } else if (FALSE_IT(omt->get_tenant_ids(tenant_list))) {
    } else {
      for (int i=0; i < tenant_list.size(); i++) {
        uint64_t tenant_id = tenant_list.at(i);
        if (!is_virtual_tenant_id(tenant_id) && is_need_process(tenant_id)) {
          tenant_ids_.push_back(tenant_id);
        }
      }
      inited_ = true;
    }
  }
  return ret;
}

bool ObMultiTenantOperator::is_tenant_deleted_or_deleting(ObTenant *tenant) const
{
  bool is_deleted = false;
  if (OB_NOT_NULL(tenant)) {
    const share::ObUnitInfoGetter::ObUnitStatus unit_status = tenant->get_unit_status();
    const storage::ObTenantCreateStatus create_status = tenant->get_create_status();
    is_deleted = tenant->has_stopped()
        // unit 已从 __all_unit 删除，或本机已开始清理该 unit
        || share::ObUnitInfoGetter::is_unit_will_be_deleted_in_observer(unit_status)
        // unit 已被标记删除，租户即将迁走
        || share::ObUnitInfoGetter::UNIT_MARK_DELETING == unit_status
        // 租户自身的创建/删除状态已进入终止态
        || storage::ObTenantCreateStatus::CREATE_ABORT == create_status
        || storage::ObTenantCreateStatus::DELETING == create_status
        || storage::ObTenantCreateStatus::DELETED == create_status;
  }
  return is_deleted;
}

int ObMultiTenantOperator::execute(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = init();
  }
  if (OB_SUCC(ret)) {
    if (tenant_idx_ == -1) {
      tenant_idx_ = 0;
    }
    while (OB_SUCC(ret)) {
      if (tenant_idx_ >= tenant_ids_.size()) {
        // finish iter
        ret = OB_ITER_END;
      } else {
        uint64_t tenant_id = tenant_ids_.at(tenant_idx_);
        int process_ret = OB_SUCCESS;
        if (tenant_ == nullptr) {
          if (OB_FAIL(GCTX.omt_->get_active_tenant_with_tenant_lock(tenant_id, handle_, tenant_))) {
            LOG_WARN("get_tenant_with_tenant_lock", K(ret), K(tenant_id));
          }
        } else {
          // check iter tenant
          if (tenant_->id() != tenant_id) {
            LOG_ERROR("ObMultiTenantOperator tenant mismatch", K(tenant_ids_), K(tenant_idx_), K(tenant_id), K(tenant_->id()));
            abort();
          }
        }
        if (OB_SUCC(ret)) {
          share::ObTenantSwitchGuard guard(tenant_);
          process_ret = process_curr_tenant(row);
        }
        if (OB_SUCC(ret)) {
          if (process_ret == OB_SUCCESS) {
            // succ do nothing
            break;
          } else if (process_ret == OB_ITER_END) {
            {
              // release last tenant obj
              share::ObTenantSwitchGuard guard(tenant_);
              release_last_tenant();
            }
            tenant_->unlock(handle_);
            tenant_ = nullptr;
            tenant_idx_++;
          } else if (process_ret == OB_OBJECT_NOT_EXIST && is_tenant_deleted_or_deleting(tenant_)) {
            // only skip object-not-exist for a tenant which has been deleted or is
            // being deleted; keep propagating it for a live tenant
            LOG_WARN("skip object not exist error for deleted or deleting tenant", K(tenant_id), K(process_ret),
                     "unit_status", share::ObUnitInfoGetter::get_unit_status_str(tenant_->get_unit_status()),
                     "create_status", tenant_->get_create_status(),
                     "has_stopped", tenant_->has_stopped(), K(lbt()));
            {
              // release last tenant obj
              share::ObTenantSwitchGuard guard(tenant_);
              release_last_tenant();
            }
            tenant_->unlock(handle_);
            tenant_ = nullptr;
            tenant_idx_++;
          } else {
            ret = process_ret;
            LOG_WARN("operator process", K(ret), K(lbt()));
          }
        } else if (ret == OB_TENANT_NOT_IN_SERVER) {
          ret = OB_SUCCESS;
          tenant_idx_++;
        }
      }
    }
  }
  return ret;
}

} // end omt
} // end oceanbase
