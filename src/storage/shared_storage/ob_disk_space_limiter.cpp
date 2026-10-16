/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_disk_space_limiter.h"
#include "deps/oblib/src/lib/utility/ob_print_utils.h"
#include "share/ob_force_print_log.h"
#include "storage/shared_storage/common/ob_ss_common_info.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

/*-----------------------------------------ObDiskSpaceLimiter-----------------------------------------*/
ObDiskSpaceLimiter::ObDiskSpaceLimiter(const int64_t max)
  : max_(max), used_(0), is_first_calibration_done_(true),
    lock_(ObLatchIds::DISK_SPACE_MANAGER_LOCK), parent_(nullptr), child_(nullptr), next_(nullptr)
{
}

ObDiskSpaceLimiter::~ObDiskSpaceLimiter()
{
  if (OB_NOT_NULL(parent_)) {
    parent_->del_child(this);
    parent_ = nullptr;
  }
}

void ObDiskSpaceLimiter::reset()
{
  // detach from parent first (this will also clear our next_ in parent's list and
  // release our used_ from parent)
  if (OB_NOT_NULL(parent_)) {
    parent_->del_child(this);
    parent_ = nullptr;
  }

  // detach all children from this node to avoid dangling parent_ pointers
  // and to release their usage from this node.
  while (has_child()) {
    ObDiskSpaceLimiter *node = nullptr;
    {
      SpinRLockGuard guard(lock_);
      node = child_;
    }
    if (OB_NOT_NULL(node)) {
      del_child(node);
    }
  }

  next_ = nullptr;
  ATOMIC_STORE(&max_, 0);
  ATOMIC_STORE(&used_, 0);
  ATOMIC_STORE(&is_first_calibration_done_, true); // 复位为默认值，避免复用后残留
}

bool ObDiskSpaceLimiter::is_quota_valid_() const
{
  return used_ >= 0 && max_ >= used_;
}

int ObDiskSpaceLimiter::add_child(ObDiskSpaceLimiter *node)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || OB_UNLIKELY(node == this)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is invalid", KR(ret), KP(node));
  } else if (OB_NOT_NULL(node->parent_) || OB_UNLIKELY(!node->is_quota_valid_())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is invalid", KR(ret), KPC(node));
  } else if (OB_FAIL(acquire(node->get_used()))) {
    LOG_WARN("fail to hold child resource", KR(ret), KPC(node), KPC(this));
  } else {
    SpinWLockGuard guard(lock_);
    node->next_ = child_;
    child_ = node;
    node->parent_ = this;
  }
  return ret;
}

void ObDiskSpaceLimiter::del_child(ObDiskSpaceLimiter *node)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(node) && OB_LIKELY(node != this && node->parent_ == this)) {
    if (OB_UNLIKELY(!node->is_quota_valid_())) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "node is invalid", KPC(node));
    }

    SpinWLockGuard guard(lock_);
    ObDiskSpaceLimiter **cur = &child_;
    while (OB_SUCC(ret) && OB_NOT_NULL(*cur) && *cur != node) {
      cur = &((*cur)->next_);
    }

    if (OB_NOT_NULL(*cur)) {
      *cur = (*cur)->next_;
      node->next_ = nullptr;
      node->parent_ = nullptr;
      release(node->get_used());
    }
  }
}

int ObDiskSpaceLimiter::acquire(const int64_t quota)
{
  int ret = OB_SUCCESS;
  if (quota == 0) {
    // do nothing
  } else if (quota < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quota is invalid", KR(ret), K(quota));
  } else {
    if (ATOMIC_AAF(&used_, quota) > get_max()) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      LOG_WARN("fail to acquire resource", KR(ret), K(quota), KPC(this));
    } else if (OB_NOT_NULL(parent_)) {
      ret = parent_->acquire(quota);
    }

    // if fail to acquire resource, rollback the used resource
    if (OB_FAIL(ret)) {
      ATOMIC_AAF(&used_, -quota);
    }
  }
  return ret;
}

void ObDiskSpaceLimiter::release(const int64_t quota)
{
  int ret = OB_SUCCESS;
  if (quota == 0) {
    // do nothing
  } else if (quota < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("quota is invalid", KR(ret), K(quota));
  } else {
    // Atomically decrease used_ by quota and clamp to 0 when quota > current used_.
    int64_t old_used = 0;
    int64_t new_used = 0;
    bool done = false;
    // 首次校准完成前，used_ 由可能过期的 disk_space_meta 播种、不可信，quota > used_ 的 over-release
    // 属预期，降级为 WARN
    const bool is_first_calibration_done = ATOMIC_LOAD(&is_first_calibration_done_);
    while (!done) {
      old_used = ATOMIC_LOAD(&used_);

      // used_ 为负与首次校准无关：合法持久化数据要求 used_ >= 0（反序列化会校验），且 release 用饱和
      // 减法不会由 stale meta 或普通 over-release 制造负数，故负值始终代表下溢/溢出/内存破坏等真实
      // 不变量异常，恒打 ERROR。
      if (OB_UNLIKELY(old_used < 0)) { // In theory used_ should never be negative
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("used_ is negative before release", KR(ret), K(old_used), K(quota), KPC(this));
      }
      
      // If quota is larger than current used_, clamp the result to 0 (saturating subtraction).
      new_used = MAX(0, old_used - quota);

      if (ATOMIC_BCAS(&used_, old_used, new_used)) {
        if (OB_UNLIKELY(quota > old_used)) {
          // Log an unexpected over-release for diagnosis; the counter has been clamped to 0 safely.
          ret = OB_ERR_UNEXPECTED;
          if (is_first_calibration_done) {
            LOG_ERROR("release resource quota is larger than used, clamp to zero",
                KR(ret), K(quota), K(old_used), KPC(this));
          } else {
            LOG_WARN("release resource quota is larger than used before first calibration, clamp to zero",
                KR(ret), K(quota), K(old_used), KPC(this));
          }
        }
        done = true;
      }
    }

    if (OB_NOT_NULL(parent_)) {
      parent_->release(old_used - new_used);
    }
  }
}

bool ObDiskSpaceLimiter::has_child()
{
  SpinRLockGuard guard(lock_);
  return OB_NOT_NULL(child_);
}

bool ObDiskSpaceLimiter::has_children()
{
  SpinRLockGuard guard(lock_);
  return OB_NOT_NULL(child_) && OB_NOT_NULL(child_->next_);
}

int64_t ObDiskSpaceLimiter::get_used() const
{
  return ATOMIC_LOAD(&used_);
}

int64_t ObDiskSpaceLimiter::get_max() const
{
  return ATOMIC_LOAD(&max_);
}

int ObDiskSpaceLimiter::set_max(const int64_t max)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(max <= 0 || max < get_used())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("max is invalid", KR(ret), K(max), KPC(this));
  } else {
    ATOMIC_STORE(&max_, max);
  }
  return ret;
}

int ObDiskSpaceLimiter::update_max(const int64_t delta_max)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(delta_max == 0)) {
    // do nothing
  } else if (delta_max > 0) {
    ATOMIC_AAF(&max_, delta_max);
  } else {
    if (OB_UNLIKELY(ATOMIC_AAF(&max_, delta_max) < get_used())) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      LOG_WARN("fail to update max", KR(ret), K(delta_max), KPC(this));
      ATOMIC_AAF(&max_, -delta_max);
    }
  }
  return ret;
}

void ObDiskSpaceLimiter::force_update_used(const int64_t delta_used)
{
  if (delta_used > 0) {
    ATOMIC_AAF(&used_, delta_used);
    if (OB_NOT_NULL(parent_)) {
      parent_->force_update_used(delta_used);
    }
  } else if (delta_used < 0) {
    release(-delta_used);
  }
}

int ObShareTenantDiskSpaceLimiter::set_tenant_hot_macro_used(
    const uint64_t tenant_id, const int64_t hot_macro_used)
{
  int ret = OB_SUCCESS;
  const int64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(hot_macro_used < 0 || meta_tenant_id != tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hot macro used is invalid", KR(ret), K(hot_macro_used), K(tenant_id_), K(tenant_id));
  } else {
    SpinWLockGuard guard(lock_);
    const int64_t idx = (tenant_id == meta_tenant_id ? 0 : 1);
    tenant_hot_macro_used_[idx] = hot_macro_used;
  }
  return ret;
}

int64_t ObShareTenantDiskSpaceLimiter::get_total_hot_macro_used()
{
  SpinRLockGuard guard(lock_);
  return tenant_hot_macro_used_[0] + tenant_hot_macro_used_[1];
}

/*-----------------------------------------ObShareTenantDiskSpaceLimiterManager-----------------------------------------*/
ObShareTenantDiskSpaceLimiterManager::ObShareTenantDiskSpaceLimiterManager()
  : is_inited_(false),
    share_tenant_limiter_map_()
{
}

ObShareTenantDiskSpaceLimiterManager::~ObShareTenantDiskSpaceLimiterManager()
{
  destroy();
}

void ObShareTenantDiskSpaceLimiterManager::destroy()
{
  int ret = OB_SUCCESS;
  SS_FOREACH_MAP_NODE(share_tenant_limiter_map_, ShareTenantLimiterMap, node_iter, {
    ObShareTenantDiskSpaceLimiter *limiter = node_iter->second;
    if (OB_NOT_NULL(limiter)) {
      LOG_INFO("succ to delete share tenant limiter", KR(ret), K(node_iter->first), KP(limiter));
      OB_DELETE(ObShareTenantDiskSpaceLimiter, MALLOC_LABEL, limiter);
    }
  });
  share_tenant_limiter_map_.destroy();
  is_inited_ = false;
}

int ObShareTenantDiskSpaceLimiterManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("share tenant disk space limiter manager has been inited", KR(ret));
  } else if (OB_FAIL(share_tenant_limiter_map_.create(
      MAP_BUCKET_NUM, ObMemAttr(OB_SERVER_TENANT_ID, MALLOC_LABEL)))) {
    LOG_WARN("fail to create limiter map", KR(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

class RetCBBase
{
public:
  RetCBBase() : ret_(OB_SUCCESS)
  {
  }

  int get_ret() const { return ret_; }

protected:
  int ret_;
};

class AddChildSetCB
{
public:
  AddChildSetCB(ObShareTenantDiskSpaceLimiter *limiter, ObDiskSpaceLimiter *child_node)
      : limiter_(limiter), child_node_(child_node)
  {
  }

  int operator()(const hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(limiter_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else if (OB_FAIL(limiter_->add_child(child_node_))) {
      LOG_WARN("fail to add child", KR(ret), KPC(child_node_), KP(limiter_));
    }
    return ret;
  };

private:
  ObShareTenantDiskSpaceLimiter *limiter_;
  ObDiskSpaceLimiter *child_node_;
};

class AddChildUpdateCB : public RetCBBase
{
public:
  AddChildUpdateCB(ObDiskSpaceLimiter *child_node)
      : RetCBBase(), child_node_(child_node), is_exist_(false)
  {
  }

  void operator()(hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else if (OB_FAIL(limiter->add_child(child_node_))) {
      LOG_WARN("fail to add child", KR(ret), KPC(child_node_), KP(limiter));
    }
    ret_ = ret;
    is_exist_ = true;
  }

  bool is_exist() const { return is_exist_; }

private:
  ObDiskSpaceLimiter *child_node_;
  bool is_exist_;
};

int ObShareTenantDiskSpaceLimiterManager::add_child_tenant_limiter(
    const uint64_t tenant_id, ObDiskSpaceLimiter *node)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is null", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    ObShareTenantDiskSpaceLimiter *limiter = nullptr;
    if (OB_FAIL(create_share_tenant_limiter_(meta_tenant_id, limiter))) {
      LOG_WARN("fail to create share tenant limiter", KR(ret), K(tenant_id));
    } else if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret), K(tenant_id));
    } else {
      AddChildSetCB add_child_set_cb(limiter, node);
      AddChildUpdateCB add_child_update_cb(node);
      if (OB_FAIL(share_tenant_limiter_map_.set_or_update(
          meta_tenant_id, limiter, add_child_set_cb, add_child_update_cb))) {
        LOG_WARN("fail to set refactored", KR(ret), K(tenant_id));
      } else if (OB_FAIL(add_child_update_cb.get_ret())) {
        LOG_WARN("fail to add child", KR(ret), KPC(node), KP(limiter));
      } else {
        LOG_INFO("succ to add child limiter", KR(ret), K(tenant_id), KPC(node), KP(limiter));
      }

      if (OB_FAIL(ret) || add_child_update_cb.is_exist()) {
        if (OB_NOT_NULL(limiter)) {
          OB_DELETE(ObShareTenantDiskSpaceLimiter, MALLOC_LABEL, limiter);
        }
      }
    }
  }
  return ret;
}

class DelChildReadCB : public RetCBBase
{
public:
  DelChildReadCB(ObDiskSpaceLimiter *child_node) : RetCBBase(), child_node_(child_node)
  {
  }

  void operator()(hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else {
      limiter->del_child(child_node_);
    }
    ret_ = ret;
  }

private:
  ObDiskSpaceLimiter *child_node_;
};

void ObShareTenantDiskSpaceLimiterManager::del_child_tenant_limiter(
    const uint64_t tenant_id, ObDiskSpaceLimiter *node)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("node is null", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    const int64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    DelChildReadCB del_child_read_cb(node);
    if (OB_FAIL(share_tenant_limiter_map_.read_atomic(meta_tenant_id, del_child_read_cb))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to del child", KR(ret), K(tenant_id), KPC(node));
      }
    } else if (OB_FAIL(del_child_read_cb.get_ret())) {
      LOG_WARN("fail to del child", KR(ret), K(tenant_id), KPC(node));
    }

    LOG_INFO("finish del child limiter", KR(ret), K(tenant_id), KPC(node));
  }
}

class TryDelShareTenantLimiterDeletePred : public RetCBBase
{
public:
  TryDelShareTenantLimiterDeletePred() : RetCBBase()
  {
  }

  bool operator()(hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    bool can_delete = false;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else {
      can_delete = !(limiter->has_child());
    }
    ret_ = ret;
    return OB_SUCC(ret) && can_delete;
  }
};

void ObShareTenantDiskSpaceLimiterManager::try_del_share_tenant_limiter(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    TryDelShareTenantLimiterDeletePred delete_pred;
    ObShareTenantDiskSpaceLimiter *limiter = nullptr;
    bool is_erased = false;
    if (OB_FAIL(share_tenant_limiter_map_.erase_if(
        meta_tenant_id, delete_pred, is_erased, &limiter))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to erase share tenant limiter", KR(ret), K(tenant_id));
      }
    } else if (OB_FAIL(delete_pred.get_ret())) {
      LOG_WARN("fail to delete share tenant limiter", KR(ret), K(tenant_id));
    }

    if (is_erased && OB_NOT_NULL(limiter)) {
      FLOG_INFO("succ to delete share tenant limiter", KR(ret), K(tenant_id), KP(limiter));
      OB_DELETE(ObShareTenantDiskSpaceLimiter, MALLOC_LABEL, limiter);
    }
  }
}

int ObShareTenantDiskSpaceLimiterManager::create_share_tenant_limiter_(
    const uint64_t tenant_id, ObShareTenantDiskSpaceLimiter *&limiter)
{
  int ret = OB_SUCCESS;
  limiter = nullptr;
  const int64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY(!is_valid_tenant_id(meta_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(limiter =
      OB_NEW(ObShareTenantDiskSpaceLimiter, MALLOC_LABEL, meta_tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(tenant_id));
  } else {
    FLOG_INFO("succ to create share tenant limiter", K(tenant_id), KP(limiter));
  }
  return ret;
}

class GetShareTenantTotalSizeReadCB : public RetCBBase
{
public:
  GetShareTenantTotalSizeReadCB() : RetCBBase(), total_size_(0)
  {
  }

  void operator()(const hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else {
      total_size_ = limiter->get_max();
    }
    ret_ = ret;
  }

  int64_t get_total_size() const { return total_size_; }

private:
  int64_t total_size_;
};

int64_t ObShareTenantDiskSpaceLimiterManager::get_share_tenant_total_size(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t total_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    GetShareTenantTotalSizeReadCB read_cb;
    if (OB_FAIL(share_tenant_limiter_map_.read_atomic(meta_tenant_id, read_cb))) {
      LOG_WARN("fail to get share tenant total size", KR(ret), K(tenant_id));
    } else if (OB_FAIL(read_cb.get_ret())) {
      LOG_WARN("fail to get share tenant total size", KR(ret), K(tenant_id));
    } else {
      total_size = read_cb.get_total_size();
    }
  }
  return total_size;
}

class UpdateShareTenantTotalSizeReadCB : public RetCBBase
{
public:
  UpdateShareTenantTotalSizeReadCB(const int64_t delta_disk_size)
      : RetCBBase(), delta_disk_size_(delta_disk_size)
  {
  }

  void operator()(hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else if (OB_FAIL(limiter->update_max(delta_disk_size_))) {
      LOG_WARN("fail to update max", KR(ret), K(delta_disk_size_));
    }
    ret_ = ret;
  }

private:
  const int64_t delta_disk_size_;
};

int ObShareTenantDiskSpaceLimiterManager::update_share_tenant_total_size(
  const uint64_t tenant_id, const int64_t delta_disk_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id), K(delta_disk_size));
  } else if (OB_LIKELY(delta_disk_size != 0)) {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    UpdateShareTenantTotalSizeReadCB update_cb(delta_disk_size);
    if (OB_FAIL(share_tenant_limiter_map_.read_atomic(meta_tenant_id, update_cb))) {
      LOG_WARN("fail to update share tenant total size", KR(ret), K(tenant_id), K(delta_disk_size));
    } else if (OB_FAIL(update_cb.get_ret())) {
      LOG_WARN("fail to update share tenant total size", KR(ret), K(tenant_id), K(delta_disk_size));
    }
  }
  return ret;
}

class GetShareTenantFreeSizeReadCB : public RetCBBase
{
public:
  GetShareTenantFreeSizeReadCB() : RetCBBase(), free_size_(0)
  {
  }

  void operator()(const hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else {
      free_size_ = limiter->get_free();
    }
    ret_ = ret;
  }

  int64_t get_free_size() const { return free_size_; }

private:
  int64_t free_size_;
};

int64_t ObShareTenantDiskSpaceLimiterManager::get_share_tenant_free_size(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t free_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    GetShareTenantFreeSizeReadCB read_cb;
    if (OB_FAIL(share_tenant_limiter_map_.read_atomic(gen_meta_tenant_id(tenant_id), read_cb))) {
      LOG_WARN("fail to get share tenant free size", KR(ret), K(tenant_id));
    } else if (OB_FAIL(read_cb.get_ret())) {
      LOG_WARN("fail to get share tenant free size", KR(ret), K(tenant_id));
    } else {
      free_size = read_cb.get_free_size();
    }
  }
  return free_size;
}

class SetTenantHotMacroUsedReadCB : public RetCBBase
{
public:
  SetTenantHotMacroUsedReadCB(const uint64_t tenant_id, const int64_t hot_macro_used)
      : RetCBBase(), tenant_id_(tenant_id), hot_macro_used_(hot_macro_used)
  {
  }

  void operator()(hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else if (OB_FAIL(limiter->set_tenant_hot_macro_used(tenant_id_, hot_macro_used_))) {
      LOG_WARN("fail to set tenant hot macro used",
          KR(ret), K(tenant_id_), K(hot_macro_used_), KP(limiter));
    }
    ret_ = ret;
  }

private:
  const uint64_t tenant_id_;
  const int64_t hot_macro_used_;
};

int ObShareTenantDiskSpaceLimiterManager::set_tenant_hot_macro_used(
    const uint64_t tenant_id, const int64_t hot_macro_used)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id), K(hot_macro_used));
  } else if (OB_UNLIKELY(hot_macro_used < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("hot macro used is invalid", KR(ret), K(tenant_id), K(hot_macro_used));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    SetTenantHotMacroUsedReadCB read_cb(tenant_id, hot_macro_used);
    if (OB_FAIL(share_tenant_limiter_map_.read_atomic(meta_tenant_id, read_cb))) {
      LOG_WARN("fail to set tenant hot macro used", KR(ret), K(tenant_id), K(hot_macro_used));
    } else if (OB_FAIL(read_cb.get_ret())) {
      LOG_WARN("fail to set tenant hot macro used", KR(ret), K(tenant_id), K(hot_macro_used));
    }
  }
  return ret;
}

class GetShareTenantTotalHotMacroUsedReadCB : public RetCBBase
{
public:
  GetShareTenantTotalHotMacroUsedReadCB() : RetCBBase(), total_hot_macro_used_(0)
  {
  }

  void operator()(const hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else {
      total_hot_macro_used_ = limiter->get_total_hot_macro_used();
    }
    ret_ = ret;
  }

  int64_t get_total_hot_macro_used() const { return total_hot_macro_used_; }

private:
  int64_t total_hot_macro_used_;
};

int64_t ObShareTenantDiskSpaceLimiterManager::get_share_tenant_total_hot_macro_used(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t total_hot_macro_used = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    GetShareTenantTotalHotMacroUsedReadCB read_cb;
    if (OB_FAIL(share_tenant_limiter_map_.read_atomic(meta_tenant_id, read_cb))) {
      LOG_WARN("fail to get share tenant total hot macro used", KR(ret), K(tenant_id));
    } else if (OB_FAIL(read_cb.get_ret())) {
      LOG_WARN("fail to get share tenant total hot macro used", KR(ret), K(tenant_id));
    } else {
      total_hot_macro_used = read_cb.get_total_hot_macro_used();
    }
  }
  return total_hot_macro_used;
}

class CheckTenantPairReadyReadCB : public RetCBBase
{
public:
  CheckTenantPairReadyReadCB() : RetCBBase(), is_ready_(false)
  {
  }
  
  void operator()(const hash::HashMapPair<uint64_t, ObShareTenantDiskSpaceLimiter *> &entry)
  {
    int ret = OB_SUCCESS;
    ObShareTenantDiskSpaceLimiter *limiter = entry.second;
    if (OB_ISNULL(limiter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("limiter is null", KR(ret));
    } else {
      is_ready_ = limiter->has_children();
    }
    ret_ = ret;
  }
  
  bool is_ready() const { return is_ready_; }
  
private:
  bool is_ready_;
};
  
bool ObShareTenantDiskSpaceLimiterManager::is_share_tenant_disk_space_quota_initialized(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_share_tenant_disk_space_quota_initialized = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("share tenant disk space limiter manager is not initialized", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id)) {
    // no need to check sys tenant
    is_share_tenant_disk_space_quota_initialized = true;
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    CheckTenantPairReadyReadCB read_cb;
    if (OB_FAIL(share_tenant_limiter_map_.read_atomic(meta_tenant_id, read_cb))) {
      LOG_WARN("fail to check tenant pair ready", KR(ret), K(tenant_id));
    } else if (OB_FAIL(read_cb.get_ret())) {
      LOG_WARN("fail to check tenant pair ready", KR(ret), K(tenant_id));
    } else {
      is_share_tenant_disk_space_quota_initialized = read_cb.is_ready();
    }
  }
  
  return is_share_tenant_disk_space_quota_initialized;
}

} // namespace storage
} // namespace oceanbase
