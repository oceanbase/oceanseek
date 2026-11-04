/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_disk_space_manager.h"
#include "lib/container/ob_array_wrap.h"
#include "lib/utility/ob_print_utils.h"
#include "share/config/ob_server_config.h"
#include "share/ob_errno.h"
#include "share/rc/ob_tenant_base.h"
#include "share/rc/ob_tenant_module_init_ctx.h"
#include "share/ob_io_device_helper.h"
#include "share/ob_time_utility2.h"
#include "observer/omt/ob_tenant.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "share/object_storage/ob_zone_storage_table_operation.h"
#include "share/ob_all_server_tracer.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ob_storage_rpc.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"
#include "storage/shared_storage/ob_dir_manager.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::share;

// - use ObTimeUtility2::usec_to_str()
// - if failed, fallback to printing the raw timestamp
// - always make sure the output is null-terminated
void format_ts_us_to_str(const int64_t ts_us, char *buf, const int64_t buf_len)
{
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    // do nothing
  } else if (ts_us <= 0) {
    IGNORE_RETURN snprintf(buf, buf_len, "N/A");
  } else {
    int64_t pos = 0;
    if (OB_SUCCESS != ObTimeUtility2::usec_to_str(ts_us, buf, buf_len, pos)) {
      IGNORE_RETURN snprintf(buf, buf_len, "%ld", ts_us);
    } else if (pos >= buf_len) {
      buf[buf_len - 1] = '\0';
    } else {
      buf[pos] = '\0';
    }
  }
}

// Format bytes into human-readable string with unit (PB/TB/GB/MB/KB/B), always keeping 2 decimals.
// Keeps sign and scales by abs(bytes).
// Examples: 1536 -> "1.50KB", -1536 -> "-1.50KB", 0 -> "0.00B"
void format_bytes_to_str(const int64_t bytes, char *buf, const int64_t buf_len)
{
  static constexpr double KB = 1024LL;
  static constexpr double MB = 1024LL * KB;
  static constexpr double GB = 1024LL * MB;
  static constexpr double TB = 1024LL * GB;
  static constexpr double PB = 1024LL * TB;

  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    // do nothing
  } else {
    const bool is_neg = (bytes < 0);
    const double abs_bytes = static_cast<double>(is_neg ? -bytes : bytes);
    double v = 0.0;
    const char *unit = "B";
    if (abs_bytes >= PB) {
      v = abs_bytes / PB;
      unit = "PB";
    } else if (abs_bytes >= TB) {
      v = abs_bytes / TB;
      unit = "TB";
    } else if (abs_bytes >= GB) {
      v = abs_bytes / GB;
      unit = "GB";
    } else if (abs_bytes >= MB) {
      v = abs_bytes / MB;
      unit = "MB";
    } else if (abs_bytes >= KB) {
      v = abs_bytes / KB;
      unit = "KB";
    } else {
      v = abs_bytes;
      unit = "B";
    }
    
    if (is_neg) {
      IGNORE_RETURN databuff_printf(buf, buf_len, "-%.2f%s", v, unit);
    } else {
      IGNORE_RETURN databuff_printf(buf, buf_len, "%.2f%s", v, unit);
    }
  }
}

/**
 * --------------------------------ObDiskSpaceManager------------------------------------
 */
ObDiskSpaceManager::ObDiskSpaceManager()
  : ObShareTenantDiskSpaceLimiterManager(),
    disk_size_lock_(ObLatchIds::DISK_SPACE_MANAGER_LOCK),
    total_disk_size_(0),
    reserved_disk_size_(0),
    alloc_disk_size_(0),
    hidden_sys_data_disk_config_size_(0),
    sys_tenant_data_disk_size_(0),
    is_hidden_sys_(false),
    data_disk_suggested_size_(0),
    data_disk_suggested_operation_(DataDiskSuggestedOperationType::TYPE::NONE)
{
}

ObDiskSpaceManager::~ObDiskSpaceManager() { destroy(); }

int ObDiskSpaceManager::init(const int64_t total_disk_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("disk space manager has been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(total_disk_size <= DEFAULT_SERVER_TENANT_ID_DISK_SIZE)) {  // total_disk_size must be larger than reserved_disk_size (100MB)
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument, total disk size is smaller than reserved disk size", KR(ret), K(total_disk_size));
  } else if (OB_FAIL(ObShareTenantDiskSpaceLimiterManager::init())) {
    LOG_WARN("fail to init share tenant disk space limiter manager", KR(ret), K(total_disk_size));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    total_disk_size_ = total_disk_size;
    reserved_disk_size_ = DEFAULT_SERVER_TENANT_ID_DISK_SIZE;
    alloc_disk_size_ = 0;
    hidden_sys_data_disk_config_size_ = GMEMCONF.get_hidden_sys_memory() * MEMORY_TO_DATA_DISK_FACTOR;
    sys_tenant_data_disk_size_ = 0; // when create hidden sys, then init this value
    is_hidden_sys_ = false;
    data_disk_suggested_size_ = 0;
    data_disk_suggested_operation_ = DataDiskSuggestedOperationType::TYPE::NONE;
    is_inited_ = true;
    LOG_INFO("succ to init disk space manager", KPC(this));
  }
  return ret;
}

void ObDiskSpaceManager::destroy()
{
  ObMutexGuard guard(disk_size_lock_);
  total_disk_size_ = 0;
  reserved_disk_size_ = 0;
  alloc_disk_size_ = 0;
  hidden_sys_data_disk_config_size_ = 0;
  sys_tenant_data_disk_size_ = 0;
  is_hidden_sys_ = false;
  data_disk_suggested_size_ = 0;
  data_disk_suggested_operation_ = DataDiskSuggestedOperationType::TYPE::NONE;
  ObShareTenantDiskSpaceLimiterManager::destroy();
}

ObDiskSpaceManager &ObDiskSpaceManager::get_instance()
{
  static ObDiskSpaceManager instance;
  return instance;
}

int ObDiskSpaceManager::get_free_cpu_count_(double &free_cpu_count)
{
  int ret = OB_SUCCESS;
  free_cpu_count = 0.0;
  omt::ObTenantNodeBalancer::ServerResource svr_res_assigned;
  if (OB_FAIL(omt::ObTenantNodeBalancer::get_instance().get_server_allocated_resource(
      svr_res_assigned))) {
    LOG_WARN("fail to get server allocated resource", KR(ret));
  } else {
    free_cpu_count =
        MAX(0.0, static_cast<double>(common::get_cpu_count()) - svr_res_assigned.max_cpu_);
  }
  return ret;
}

int64_t ObDiskSpaceManager::cal_data_disk_suggested_size_(
    const int64_t delta_alloc_disk_size) const
{
  int ret = OB_SUCCESS;
  int64_t data_disk_suggested_size = 0;
  double free_cpu_count = 0;
  const int64_t new_alloc_disk_size = alloc_disk_size_ + delta_alloc_disk_size;
  if (OB_UNLIKELY(new_alloc_disk_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(delta_alloc_disk_size),
        K(total_disk_size_), K(alloc_disk_size_), K(reserved_disk_size_));
  } else if (OB_FAIL(get_free_cpu_count_(free_cpu_count))) {
    LOG_WARN("fail to get free cpu count", KR(ret), K(delta_alloc_disk_size),
        K(total_disk_size_), K(alloc_disk_size_), K(reserved_disk_size_));
  } else {
    const double expect_total_disk_size = static_cast<double>(reserved_disk_size_)
        + free_cpu_count * DEFAULT_DISK_SIZE_RESERVED_PERCPU
        + static_cast<double>(new_alloc_disk_size) / DEFAULT_DISK_SIZE_EXPAND_THRESHOLD_PERCENT;
    const int64_t aligned_expect_total_disk_size = upper_align(
        expect_total_disk_size + 1,
        DEFAULT_TENANT_DISK_SIZE_EXPAND_STEP_LENGTH);
    if (aligned_expect_total_disk_size > total_disk_size_) {
      data_disk_suggested_size = aligned_expect_total_disk_size;
    }
  }
  return data_disk_suggested_size;
}

int ObDiskSpaceManager::resize(const int64_t new_total_disk_size)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_total_disk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid total disk size to resize", KR(ret), K(new_total_disk_size));
  } else if (new_total_disk_size < total_disk_size_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("can not make total disk size smaller", KR(ret), K(new_total_disk_size), K_(total_disk_size));
  } else {
    const int64_t old_total_disk_size = total_disk_size_;
    total_disk_size_ = new_total_disk_size;
    const int64_t data_disk_suggested_size = cal_data_disk_suggested_size_();
    update_data_disk_suggested_info_(data_disk_suggested_size, old_total_disk_size, alloc_disk_size_);
    FLOG_INFO("succ to resize total disk size",
        KPC(this), K(old_total_disk_size), K(data_disk_suggested_size));
  }
  return ret;
}

int ObDiskSpaceManager::alloc(const int64_t alloc_size)
{
  int ret = OB_SUCCESS;
  int64_t data_disk_suggested_size = 0;
  ObMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(alloc_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk size to alloc", KR(ret), K(alloc_size));
  } else if (FALSE_IT(data_disk_suggested_size = cal_data_disk_suggested_size_(alloc_size))) {
  } else if (alloc_size > get_free_disk_size()) {
    // ss_cache free_size is not enough, need expand
    update_data_disk_suggested_info_(data_disk_suggested_size, total_disk_size_, alloc_disk_size_);
    ret = OB_SERVER_OUTOF_DISK_SPACE;
    LOG_WARN("fail to alloc disk size", KR(ret), K(alloc_size),
        KPC(this), K(data_disk_suggested_size), K(get_free_disk_size()));
  } else {
    const int64_t old_alloc_disk_size = alloc_disk_size_;
    alloc_disk_size_ += alloc_size;
    if (data_disk_suggested_size > total_disk_size_) {
      update_data_disk_suggested_info_(data_disk_suggested_size, total_disk_size_, old_alloc_disk_size);
    }
    FLOG_INFO("succ to alloc disk size", KPC(this), K(alloc_size), K(data_disk_suggested_size));
  }
  return ret;
}

int ObDiskSpaceManager::free(const int64_t free_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(free_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid disk size to free", KR(ret), K(free_size));
  } else if (free_size > alloc_disk_size_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error, alloc disk size less than 0", KR(ret), K_(alloc_disk_size),
              K_(total_disk_size), K(free_size));
  } else {
    const int64_t old_alloc_disk_size = alloc_disk_size_;
    alloc_disk_size_ -= free_size;
    const int64_t data_disk_suggested_size = cal_data_disk_suggested_size_();
    update_data_disk_suggested_info_(data_disk_suggested_size, total_disk_size_, old_alloc_disk_size);
    FLOG_INFO("succ to free disk size", KPC(this),
        K(old_alloc_disk_size), K(free_size), K(data_disk_suggested_size));
  }
  return ret;
}

int64_t ObDiskSpaceManager::get_ss_cache_max_size_() const
{
  return MAX3(total_disk_size_, GCONF.datafile_size, GCONF.datafile_maxsize);
}

void ObDiskSpaceManager::update_data_disk_suggested_info_(
    const int64_t new_data_disk_suggested_size,
    const int64_t old_total_disk_size,
    const int64_t old_alloc_disk_size)
{
  const DataDiskSuggestedOperationType::TYPE old_data_disk_suggested_operation =
      data_disk_suggested_operation_;
  bool is_suggested_operation_changed = false;
  const int64_t bounded_data_disk_suggested_size =
      MIN(get_ss_cache_max_size_(), new_data_disk_suggested_size);
  if (bounded_data_disk_suggested_size <= total_disk_size_) {
    if (old_data_disk_suggested_operation != DataDiskSuggestedOperationType::TYPE::NONE) {
      is_suggested_operation_changed = true;
    }
    data_disk_suggested_operation_ = DataDiskSuggestedOperationType::TYPE::NONE;
    data_disk_suggested_size_ = 0;
  } else {
    if (bounded_data_disk_suggested_size != data_disk_suggested_size_) {
      is_suggested_operation_changed = true;
      data_disk_suggested_operation_ = DataDiskSuggestedOperationType::TYPE::EXPAND;
      data_disk_suggested_size_ = bounded_data_disk_suggested_size;
    }
  }

  if (is_suggested_operation_changed) {
    SERVER_EVENT_ADD("shared_storage", "data disk suggested operation changed",
      "old_data_disk_suggested_operation",
      DataDiskSuggestedOperationType::get_str(old_data_disk_suggested_operation),
      "old_total_disk_size", old_total_disk_size,
      "old_alloc_disk_size", old_alloc_disk_size,
      "new_data_disk_suggested_operation",
      DataDiskSuggestedOperationType::get_str(data_disk_suggested_operation_),
      "new_total_disk_size", total_disk_size_,
      "new_alloc_disk_size", alloc_disk_size_);
  }
}

int ObDiskSpaceManager::reload_hidden_sys_data_disk_config(const ObServerConfig &server_config)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(disk_size_lock_);
  const int64_t hidden_sys_data_disk_config_size = server_config._hidden_sys_tenant_data_disk_size;
  const uint64_t tenant_id = OB_SYS_TENANT_ID;
  ObUnitInfoGetter::ObTenantConfig new_unit;
  omt::ObTenant *tenant = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if ((0 == hidden_sys_data_disk_config_size) || (hidden_sys_data_disk_config_size == hidden_sys_data_disk_config_size_)) {
    // do nothing
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", KR(ret), K(tenant_id), K(hidden_sys_data_disk_config_size));
  } else if (OB_FAIL(new_unit.assign(tenant->get_unit()))) {
    LOG_WARN("fail to assign old unit failed", KR(ret),
        K(tenant_id), K(hidden_sys_data_disk_config_size), K(tenant->get_unit()));
  } else {
    if (tenant->is_hidden()) {
      new_unit.config_.set_data_disk_size(hidden_sys_data_disk_config_size);
    }
    new_unit.hidden_sys_data_disk_config_size_ = hidden_sys_data_disk_config_size;

    if (OB_FAIL(GCTX.omt_->update_tenant_unit(new_unit))) {
      LOG_WARN("fail to update tenant unit", KR(ret), K(tenant_id), K(new_unit));
    } else {
      hidden_sys_data_disk_config_size_ = hidden_sys_data_disk_config_size;
    }
  }

  FLOG_INFO("finish to update hidden sys tenant data disk size",
      KR(ret), K(hidden_sys_data_disk_config_size), KPC(this));
  return ret;
}

int ObDiskSpaceManager::update_hidden_sys_data_disk_config_size(const int64_t new_hidden_sys_data_disk_config_size)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_hidden_sys_data_disk_config_size <= 0)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid hidden sys default data disk size", KR(ret), K(new_hidden_sys_data_disk_config_size));
  } else {
    hidden_sys_data_disk_config_size_ = new_hidden_sys_data_disk_config_size;
    FLOG_INFO("succ to update hidden sys default data_disk_size", K_(hidden_sys_data_disk_config_size));
  }
  return ret;
}

int ObDiskSpaceManager::update_sys_tenant_data_disk_size(
    const int64_t new_data_disk_size, const bool is_hidden)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(new_data_disk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid hidden sys data disk size", KR(ret), K(new_data_disk_size), K(is_hidden));
  } else if (new_data_disk_size != sys_tenant_data_disk_size_ || is_hidden != is_hidden_sys_) {
    FLOG_INFO("succ to update sys tenant data disk size",
        K(sys_tenant_data_disk_size_), K(new_data_disk_size), K(is_hidden_sys_), K(is_hidden));
    sys_tenant_data_disk_size_ = new_data_disk_size;
    is_hidden_sys_ = is_hidden;
  }
  return ret;
}

int ObDiskSpaceManager::get_used_disk_size(int64_t &used_disk_size)
{
  int ret = OB_SUCCESS;
  used_disk_size = 0;
  ObArray<uint64_t> tenant_ids;
  omt::ObMultiTenant *omt = GCTX.omt_;
  if (OB_ISNULL(omt)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error, omt is nullptr", KR(ret));
  } else if (OB_FAIL(omt->get_mtl_tenant_ids(tenant_ids))) {
    STORAGE_LOG(WARN, "fail to get_mtl_tenant_ids", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); i++) {
    const uint64_t tenant_id = tenant_ids.at(i);
    MTL_SWITCH(tenant_id) {
      int64_t tenant_used_disk_size = 0;
      ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
      if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant disk space manager is null", KR(ret), KP(disk_space_mgr));
      } else if (OB_FAIL(disk_space_mgr->get_used_disk_size(tenant_used_disk_size))) {
        LOG_WARN("fail to get used disk size", KR(ret), K(tenant_used_disk_size));
      } else {
        used_disk_size += tenant_used_disk_size;
      }
    } else {
      if (OB_TENANT_NOT_IN_SERVER == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("tenant is stopped, ingnore", K(tenant_id));
      } else {
        LOG_WARN("fail to switch tenant", KR(ret), K(tenant_id));
      }
    }
  }
  return ret;
}

int ObDiskSpaceManager::get_tenant_pair_unallocated_disk_size(
    const uint64_t tenant_id, int64_t &tenant_pair_unallocated_disk_size)
{
  int ret = OB_SUCCESS;
  tenant_pair_unallocated_disk_size = 0;
  int64_t tenant_pair_config_data_disk_size = 0;
  int64_t tenant_pair_actual_data_disk_size = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTenantDiskSpaceManager::get_tenant_unit_data_disk_size(
      tenant_id, tenant_pair_config_data_disk_size, tenant_pair_actual_data_disk_size))) {
    if (OB_TENANT_NOT_IN_SERVER != ret) {
      LOG_WARN("fail to get tenant config and actual data disk size", KR(ret), K(tenant_id));
    }
  } else if (!is_sys_tenant(tenant_id)) {
    uint64_t other_tenant_id = OB_INVALID_TENANT_ID;
    if (is_meta_tenant(tenant_id)) {
      other_tenant_id = gen_user_tenant_id(tenant_id);
    } else { // user tenant
      other_tenant_id = gen_meta_tenant_id(tenant_id);
    }
    int64_t other_tenant_config_data_disk_size = 0;
    int64_t other_tenant_actual_data_disk_size = 0;
    
    if (OB_FAIL(ObTenantDiskSpaceManager::get_tenant_unit_data_disk_size(
        other_tenant_id, other_tenant_config_data_disk_size, other_tenant_actual_data_disk_size))) {
      if (OB_TENANT_NOT_IN_SERVER != ret) {
        LOG_WARN("fail to get other tenant config and actual data disk size",
            KR(ret), K(other_tenant_id));
      }
    } else {
      tenant_pair_config_data_disk_size += other_tenant_config_data_disk_size;
      tenant_pair_actual_data_disk_size += other_tenant_actual_data_disk_size;
    }
  }
  
  if (OB_SUCC(ret)) {
    // During shrinking, config_data_disk_size can be smaller than actual_data_disk_size.
    tenant_pair_unallocated_disk_size =
        tenant_pair_config_data_disk_size - tenant_pair_actual_data_disk_size;
  }
  return ret;
}

int ObDiskSpaceManager::check_ls_migration_space_full(const ObMigrationOpArg &arg)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  int64_t tenant_pair_unallocated_disk_size = 0;
  bool allow_discard_hot_data_during_migration = true;

  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(arg));
  } else if (OB_FAIL(get_tenant_pair_unallocated_disk_size(
      tenant_id, tenant_pair_unallocated_disk_size))) {
    LOG_WARN("fail to get tenant pair max free disk size", KR(ret), K(tenant_id), K(arg));
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    allow_discard_hot_data_during_migration =
        (tenant_config.is_valid()
        ? tenant_config->_allow_discard_hot_data_during_migration : true);
  }
  
  // choose src server
  ObSEArray<ObAddr, 16> src_server_candidates;
  src_server_candidates.set_attr(lib::ObMemAttr(tenant_id, "SSMigSrcCands"));
  ObAddr leader_addr;
  // `arg.src_` is invalid in ss copy-migration/restore, so we need to determine a source server by ourselves.
  // Prefer the LS leader. In restore scenarios the LS may temporarily have no leader; in that case we fall back to
  // querying `__all_virtual_ls_info` for all replica servers and validate migration space with each of them.
  //
  // Note: `get_ls_leader_addr()` reads from `__all_virtual_ls_info`. When "no leader row" exists:
  // - It may return OB_ERR_NULL_VALUE if the query produces a row with NULL fields (e.g. aggregation).
  // - Or it may return OB_ENTRY_NOT_EXIST if the query returns no row (normal select).
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObStorageInfoOperator::get_ls_leader_addr(
      tenant_id, arg.ls_id_.id(), leader_addr))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_ERR_NULL_VALUE == ret) {
      ret = OB_SUCCESS;
      // In restore scenarios, the LS may temporarily have no leader.
      // `arg.src_` is invalid in ss restore/migration, so we query `__all_virtual_ls_info`
      // to get all replica servers, then validate space with each src server.
      ObSEArray<ObAddr, 16> replica_servers;
      replica_servers.set_attr(lib::ObMemAttr(tenant_id, "SSMigRepSvrs"));
      if (OB_FAIL(ObStorageInfoOperator::get_ls_replica_addrs(
          tenant_id, arg.ls_id_.id(), replica_servers))) {
        LOG_WARN("fail to get ls replica addrs", KR(ret), K(arg));
      } else if (OB_UNLIKELY(replica_servers.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("no replica servers found when ls leader not exist", KR(ret), K(arg));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < replica_servers.count(); ++i) {
          if (OB_FAIL(src_server_candidates.push_back(replica_servers.at(i)))) {
            LOG_WARN("fail to add replica server as candidate",
                KR(ret), K(arg), K(i), K(replica_servers.at(i)));
          }
        }
      }
    } else {
      LOG_WARN("fail to get ls leader addr", KR(ret), K(arg));
    }
  } else { // find leader
    if (OB_FAIL(src_server_candidates.push_back(leader_addr))) {
      LOG_WARN("fail to add ls leader as candidate", KR(ret), K(arg), K(leader_addr));
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < src_server_candidates.count(); ++i) {
      const ObAddr &src_server = src_server_candidates.at(i);
      if (OB_FAIL(do_check_ls_migration_space_with_src_(
          arg.ls_id_, src_server,
          tenant_pair_unallocated_disk_size,
          allow_discard_hot_data_during_migration))) {
        LOG_WARN("fail to check ls migration space with src server",
            KR(ret), K(arg), K(i), K(src_server));
      }
    } 
  }
  return ret;
}

int ObDiskSpaceManager::do_check_ls_migration_space_with_src_(
    const ObLSID &ls_id,
    const ObAddr &src_server,
    const int64_t tenant_pair_unallocated_disk_size,
    const bool allow_discard_hot_data_during_migration)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ObLSService *ls_service = nullptr;
  ObStorageRpc *storage_rpc = nullptr;

  if (OB_UNLIKELY(!ls_id.is_valid() || !src_server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(src_server));
  } else if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant disk space manager is null", KR(ret), K(ls_id), K(src_server));
  } else if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("macro cache mgr is null", KR(ret), K(ls_id), K(src_server));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls service is null", KR(ret), K(ls_id), K(src_server));
  } else if (OB_ISNULL(storage_rpc = ls_service->get_storage_rpc())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage rpc is null", KR(ret), K(ls_id), K(src_server));
  } else {
    ObStorageHASrcInfo src_info;
    src_info.src_addr_ = src_server;
    src_info.cluster_id_ = GCONF.cluster_id;
    ObSSLocalCacheLSInfo src_ls_local_cache_info;
    int64_t macro_cache_min_expand_size = 0;
    int64_t macro_cache_best_expand_size = 0;

    if (OB_FAIL(storage_rpc->get_local_cache_ls_info(
        tenant_id, ls_id, src_info, src_ls_local_cache_info))) {
      LOG_WARN("fail to get src ls local cache info", KR(ret), K(ls_id), K(src_info));
    } else if (OB_UNLIKELY(!src_ls_local_cache_info.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src ls local cache info is invalid",
          KR(ret), K(ls_id), K(src_info), K(src_ls_local_cache_info));
    } else if (OB_FAIL(macro_cache_mgr->cal_expand_size_for_ls_migration(
        src_ls_local_cache_info.macro_cache_ls_info_,
        macro_cache_min_expand_size, macro_cache_best_expand_size))) {
      LOG_WARN("fail to cal macro cache expand size for ls migration",
          KR(ret), K(ls_id), K(src_ls_local_cache_info), KPC(tnt_disk_space_mgr));
    } else if (OB_UNLIKELY(macro_cache_min_expand_size > macro_cache_best_expand_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro cache expand size is invalid",
          KR(ret), K(ls_id), K(src_ls_local_cache_info), KPC(tnt_disk_space_mgr),
          K(macro_cache_min_expand_size), K(macro_cache_best_expand_size));
    }

    // During shrinking, config_data_disk_size can be smaller than actual_data_disk_size.
    const int64_t tenant_pair_max_free_disk_size = MAX(0, tenant_pair_unallocated_disk_size);
    const int64_t server_free_size = OB_SERVER_DISK_SPACE_MGR.get_free_disk_size();

    if (OB_FAIL(ret)) {
    } else if (macro_cache_min_expand_size > tenant_pair_max_free_disk_size) {
      const char *msg = (allow_discard_hot_data_during_migration
          ? "destination cannot hold all hot data, but allow discard hot data during migration, ingore"
          : "destination cannot hold all hot data");
      if (!allow_discard_hot_data_during_migration) {
        ret = OB_SERVER_OUTOF_DISK_SPACE;
      }
      FLOG_WARN(msg, KR(ret), K(allow_discard_hot_data_during_migration),
          K(ls_id), K(src_info), K(src_ls_local_cache_info), KPC(tnt_disk_space_mgr),
          K(macro_cache_min_expand_size), K(macro_cache_best_expand_size),
          K(tenant_pair_max_free_disk_size));
    } else {
      const int64_t expand_size = MIN3(
          server_free_size,
          tenant_pair_max_free_disk_size,
          macro_cache_best_expand_size);
      if (expand_size > 0 && OB_TMP_FAIL(tnt_disk_space_mgr->expand_data_disk_size(expand_size))) {
        FLOG_WARN("fail to expand data disk in advance for ls migration", KR(ret), KR(tmp_ret),
            K(allow_discard_hot_data_during_migration), K(ls_id), K(src_info), K(src_ls_local_cache_info),
            K(macro_cache_min_expand_size), K(macro_cache_best_expand_size),
            K(expand_size), K(server_free_size), K(tenant_pair_max_free_disk_size),
            KPC(tnt_disk_space_mgr));
      } else {
        FLOG_INFO("succ to check ls migration space full", KR(ret),
            K(allow_discard_hot_data_during_migration), K(ls_id), K(src_info), K(src_ls_local_cache_info),
            K(macro_cache_min_expand_size), K(macro_cache_best_expand_size),
            K(expand_size), K(server_free_size), K(tenant_pair_max_free_disk_size),
            KPC(tnt_disk_space_mgr));
      }
    }
  }
  return ret;
}

int ObDiskSpaceManager::cal_tenant_min_shrinkable_disk_size_(
    const uint64_t tenant_id, int64_t &min_shrinkable_disk_size)
{
  int ret = OB_SUCCESS;
  min_shrinkable_disk_size = INT64_MAX;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    MTL_SWITCH(tenant_id)
    {
      ObTenantDiskSpaceManager *tnt_disk_space_mgr = nullptr;
      if (OB_ISNULL(tnt_disk_space_mgr = MTL(ObTenantDiskSpaceManager*))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("other tenant disk space manager is null", KR(ret), K(tenant_id));
      } else if (OB_FAIL(tnt_disk_space_mgr->cal_min_shrinkable_disk_size(
          min_shrinkable_disk_size))) {
        LOG_WARN("fail to cal min shrinkable disk size",
            KR(ret), K(tenant_id), KPC(tnt_disk_space_mgr));
      }
    }
  }
  return ret;
}

// Calculate the minimum data-disk size that a *logical* tenant can shrink to.
// - First, we aggregate per-instance minima:
//     * For sys tenant: use its own per-instance lower bound
//       (reserved dir size + micro-cache file size + minimal macro-cache requirement).
//     * For a normal tenant (meta / user pair): sum the per-instance minima of
//       the meta-tenant and the user-tenant, which share the same underlying
//       disk space limiter.
// - Then, for *all* logical tenants, we enforce a CPU-based floor derived from
//   the user-visible tenant's unit config:
//       min_cpu * DEFAULT_DISK_SIZE_RESERVED_PERCPU
//   (for sys tenant, this is its own min_cpu; for normal tenants this is the
//    user-tenant's min_cpu). This guarantees CPU reservation is counted exactly
//   once per logical tenant.
// The result is used as the global floor when deciding how far data_disk_size
// can be shrunk.
int ObDiskSpaceManager::cal_min_shrinkable_disk_size(
    const uint64_t tenant_id, int64_t &min_shrinkable_disk_size)
{
  int ret = OB_SUCCESS;
  min_shrinkable_disk_size = INT64_MAX;
  int64_t cur_tenant_min_shrinkable_disk_size = INT64_MAX;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || is_virtual_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(cal_tenant_min_shrinkable_disk_size_(
      tenant_id, cur_tenant_min_shrinkable_disk_size))) {
    LOG_WARN("fail to cal min shrinkable disk size", KR(ret), K(tenant_id));
  } else if (is_sys_tenant(tenant_id)) {
    // sys tenant does not have a paired meta/user instance; use its own bound.
    min_shrinkable_disk_size = cur_tenant_min_shrinkable_disk_size;
  } else {
    uint64_t other_tenant_id = OB_INVALID_TENANT_ID;
    if (is_meta_tenant(tenant_id)) {
      other_tenant_id = gen_user_tenant_id(tenant_id);
    } else {
      other_tenant_id = gen_meta_tenant_id(tenant_id);
    }
    int64_t other_tenant_min_shrinkable_disk_size = INT64_MAX;

    if (OB_FAIL(cal_tenant_min_shrinkable_disk_size_(
        other_tenant_id, other_tenant_min_shrinkable_disk_size))) {
      LOG_WARN("fail to cal min shrinkable disk size", KR(ret), K(tenant_id), K(other_tenant_id));
    } else {
      min_shrinkable_disk_size =
          cur_tenant_min_shrinkable_disk_size + other_tenant_min_shrinkable_disk_size;
    }
  }

  share::ObUnitInfoGetter::ObTenantConfig unit;
  const int64_t user_tenant_id = gen_user_tenant_id(tenant_id);
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_unit(user_tenant_id, unit))) {
    LOG_WARN("fail to get tenant unit config", KR(ret), K(user_tenant_id), K(tenant_id));
  } else {
    min_shrinkable_disk_size = MAX(
        min_shrinkable_disk_size,
        unit.config_.min_cpu() * DEFAULT_DISK_SIZE_RESERVED_PERCPU);
    
    if (OB_UNLIKELY(min_shrinkable_disk_size <= 0 || min_shrinkable_disk_size == INT64_MAX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("min shrinkable disk size is invalid",
          KR(ret), K(min_shrinkable_disk_size), K(unit), K(tenant_id));
    }
  }
  return ret;
}

/**
 * --------------------------------ObTenantDiskSpaceManager------------------------------------
 */
ObTenantDiskSpaceManager::ObTenantDiskSpaceManager()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    is_first_calibration_done_(false),
    disk_size_lock_(ObLatchIds::DISK_SPACE_MANAGER_LOCK),
    total_disk_size_(0),
    unit_config_data_disk_size_(0),
    tenant_pair_unallocated_disk_size_(0),
    tenant_pair_unallocated_size_last_sync_us_(0),
    tenant_disk_space_limiter_(INT64_MAX),
    tg_id_(INVALID_TG_ID),
    persist_disk_space_task_(),
    check_expand_disk_task_(),
    expand_diag_(),
    shrink_diag_()
{
  ObSSMacroCacheStat default_stat;
  for (uint8_t i = 0; i < SS_MACRO_CACHE_MAX_TYPE_VAL; ++i) {
    macro_cache_stats_[i] = default_stat;
    macro_cache_stats_[i].cache_type_ = static_cast<ObSSMacroCacheType>(i);
    dir_sizes_[i] = 0;
    dir_nums_[i] = 0;
    free_size_exceeded_sum_[i] = 0;
    shift_size_exceeded_sum_[i] = 0;
    free_dir_size_exceeded_sum_[i] = 0;
    free_dir_num_exceeded_sum_[i] = 0;
  }
}

ObTenantDiskSpaceManager::~ObTenantDiskSpaceManager() { destroy(); }

int ObTenantDiskSpaceManager::init(const uint64_t tenant_id,
                                   const int64_t data_disk_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("tenant disk space manager has been inited.",
        KR(ret), K(tenant_id), K(data_disk_size));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
      || data_disk_size <= RESERVED_SLOG_DISK_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(data_disk_size));
  } else if (OB_FAIL(persist_disk_space_task_.init(tenant_id))) {
    LOG_WARN("fail to init persist disk space task", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_expand_disk_task_.init(tenant_id))) {
    LOG_WARN("fail to init check expand disk task", KR(ret), K(tenant_id));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    total_disk_size_ = data_disk_size;
    unit_config_data_disk_size_ = data_disk_size;
    tenant_pair_unallocated_disk_size_ = 0;
    tenant_pair_unallocated_size_last_sync_us_ = ObTimeUtility::fast_current_time();
    tenant_id_ = tenant_id;
    // 首次校准成功前，limiter 的 used_ 由可能过期的 disk_space_meta 播种、不可信，over-release 属预期，
    // 先置为未校准（over-release 降级为 WARN），待首次校准成功后由 set_first_calibration_done 置回
    tenant_disk_space_limiter_.set_first_calibration_done(false);
    if (OB_FAIL(tenant_disk_space_limiter_.set_max(INT64_MAX))) {
      LOG_WARN("fail to set max", KR(ret), K(tenant_id), K(data_disk_size));
    } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.add_child_tenant_limiter(
        tenant_id, &tenant_disk_space_limiter_))) {
      LOG_WARN("fail to add child", KR(ret), K(tenant_id), K(data_disk_size));
    } else if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.update_share_tenant_total_size(
        tenant_id, get_disk_size_capacity_nolock_()))) {
      LOG_WARN("fail to update share tenant limiter", KR(ret), K(tenant_id), K(total_disk_size_));
    } else {
      is_inited_ = true;
      LOG_INFO("succ to init tenant disk space manager", KPC(this));
    }
  }
  if (IS_NOT_INIT) {
    destroy();
  }
  return ret;
}

int ObTenantDiskSpaceManager::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret));
  } else if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::SSDiskSpaceMgrTimer, tg_id_))) {
    LOG_WARN("fail to create timer thread", KR(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("fail to start timer thread", KR(ret), K(tg_id_));
  } else if (OB_FAIL(persist_disk_space_task_.start(tg_id_))) {
    LOG_WARN("fail to start persist disk space task", KR(ret), K(tg_id_));
  } else if (OB_FAIL(check_expand_disk_task_.start(tg_id_))) {
    LOG_WARN("fail to start check expand disk size task", KR(ret), K(tg_id_));
  } else {
    LOG_INFO("succ to start tenant disk space manager", K(tg_id_));
  }
  return ret;
}

void ObTenantDiskSpaceManager::stop()
{
  if (INVALID_TG_ID != tg_id_) {
    TG_STOP(tg_id_);
  }
}

void ObTenantDiskSpaceManager::wait()
{
  if (INVALID_TG_ID != tg_id_) {
    TG_WAIT(tg_id_);
  }
}

void ObTenantDiskSpaceManager::destroy()
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(disk_size_lock_);
  if (INVALID_TG_ID != tg_id_) {
    TG_DESTROY(tg_id_);
  }
  check_expand_disk_task_.destroy();
  persist_disk_space_task_.destroy();
  tg_id_ = INVALID_TG_ID;
  
  if (IS_INIT) {
    set_macro_cache_used_nolock_(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, 0);
  }
  OB_SERVER_DISK_SPACE_MGR.del_child_tenant_limiter(tenant_id_, &tenant_disk_space_limiter_);
  OB_SERVER_DISK_SPACE_MGR.try_del_share_tenant_limiter(tenant_id_);
  tenant_disk_space_limiter_.reset();
  unit_config_data_disk_size_ = 0;
  tenant_pair_unallocated_disk_size_ = 0;
  tenant_pair_unallocated_size_last_sync_us_ = 0;
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  is_first_calibration_done_ = false;
  total_disk_size_ = 0;
  ObSSMacroCacheStat default_stat;
  for (uint8_t i = 0; i < SS_MACRO_CACHE_MAX_TYPE_VAL; ++i) {
    macro_cache_stats_[i] = default_stat;
    dir_sizes_[i] = 0;
    dir_nums_[i] = 0;
    free_size_exceeded_sum_[i] = 0;
    shift_size_exceeded_sum_[i] = 0;
    free_dir_size_exceeded_sum_[i] = 0;
    free_dir_num_exceeded_sum_[i] = 0;
  }
  expand_diag_.reset();
  shrink_diag_.reset();
}

void ObTenantDiskSpaceManager::set_first_calibration_done(const bool is_first_calibration_done)
{
  ATOMIC_STORE(&is_first_calibration_done_, is_first_calibration_done);
  // 与首次校准状态保持同步：校准成功后恢复 over-release 的 ERROR，未完成时降级为 WARN
  tenant_disk_space_limiter_.set_first_calibration_done(is_first_calibration_done);
}

int ObTenantDiskSpaceManager::mtl_init(ObTenantDiskSpaceManager *&tenant_disk_space_manager)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tenant_disk_space_manager->init(MTL_ID(), MTL_INIT_CTX()->init_data_disk_size_))) {
    LOG_WARN("fail to init tenant disk space manager", KR(ret));
  }
  return ret;
}

void ObTenantDiskSpaceManager::check_disk_space_status()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ObMutexGuard guard(disk_size_lock_);
    check_disk_scaling_status_nolock_();
    // Skip consistency check before first calibration: macro_cache_stats_.used_ may not be meaningful yet.
    if (is_first_calibration_done()) {
      check_variables_consistency_nolock_();
    }
  }
}

void ObTenantDiskSpaceManager::check_disk_scaling_status_nolock_()
{
  // Build shared context once to reduce duplication.
  DiskScalingDiagCommonInfo common;
  IGNORE_RETURN get_tenant_unit_data_disk_size(
      tenant_id_, common.config_data_disk_size_, common.actual_data_disk_size_);
  common.disk_size_capacity_ = get_disk_size_capacity_nolock_();
  common.allocated_macro_cache_size_ =
      static_cast<int64_t>(get_allocated_shared_macro_cache_size_nolock_());
  common.allocated_macro_cache_free_size_ =
      static_cast<int64_t>(get_allocated_shared_macro_cache_free_size_nolock_());
  common.allocated_macro_cache_used_size_ =
      MAX(0, common.allocated_macro_cache_size_ - common.allocated_macro_cache_free_size_);
  common.allocated_macro_cache_usage_pct_ =
      (common.allocated_macro_cache_size_ > 0
           ? (100.0 * static_cast<double>(common.allocated_macro_cache_used_size_)
              / static_cast<double>(common.allocated_macro_cache_size_))
           : 0.0);

  format_bytes_to_str(common.config_data_disk_size_, common.unit_config_disk_str_, sizeof(common.unit_config_disk_str_));
  format_bytes_to_str(common.actual_data_disk_size_, common.unit_actual_disk_str_, sizeof(common.unit_actual_disk_str_));
  format_bytes_to_str(common.disk_size_capacity_, common.disk_cap_str_, sizeof(common.disk_cap_str_));
  format_bytes_to_str(common.allocated_macro_cache_size_, common.alloc_macro_size_str_, sizeof(common.alloc_macro_size_str_));
  format_bytes_to_str(common.allocated_macro_cache_free_size_, common.alloc_macro_free_str_, sizeof(common.alloc_macro_free_str_));
  format_bytes_to_str(common.allocated_macro_cache_used_size_, common.alloc_macro_used_str_, sizeof(common.alloc_macro_used_str_));

  check_expand_status_nolock_(common);
  check_shrink_status_nolock_(common);
}

void ObTenantDiskSpaceManager::check_expand_status_nolock_(const DiskScalingDiagCommonInfo &common)
{
  char diagnose_info[OB_DIAGNOSE_INFO_LENGTH] = { 0 };

  char need_since_str[64] = { 0 };
  char last_attempt_str[64] = { 0 };
  char last_succ_str[64] = { 0 };
  format_ts_us_to_str(expand_diag_.first_need_time_us_, need_since_str, sizeof(need_since_str));
  format_ts_us_to_str(expand_diag_.last_attempt_time_us_, last_attempt_str, sizeof(last_attempt_str));
  format_ts_us_to_str(expand_diag_.last_succ_time_us_, last_succ_str, sizeof(last_succ_str));

  bool is_stuck = false;
  int64_t stuck_us = 0;
  expand_diag_.get_stuck_info(NEED_EXPAND_STUCK_WARN_INTERVAL_US, is_stuck, stuck_us);

  int64_t pos = 0;
  IGNORE_RETURN databuff_printf(diagnose_info, sizeof(diagnose_info), pos,
      "need_since=%s; stuck_us=%ld;"
      " last_attempt=%s; last_succ=%s; last_ret=%d;"
      " unit_config_disk=%s; unit_actual_disk=%s;"
      " disk_cap=%s;"
      " allocated_macro_size=%s; allocated_macro_free=%s; allocated_macro_used=%s; macro_usage_pct=%.2f%%",
      need_since_str, stuck_us,
      last_attempt_str, last_succ_str, expand_diag_.last_ret_,
      common.unit_config_disk_str_, common.unit_actual_disk_str_,
      common.disk_cap_str_,
      common.alloc_macro_size_str_, common.alloc_macro_free_str_, common.alloc_macro_used_str_,
      common.allocated_macro_cache_usage_pct_);

  ADD_LOCAL_CACHE_DIAGNOSE(
      DIAG_MOD_NAME, DIAG_SUBMOD_EXPAND,
      is_stuck ? ObSSLocalCacheModStatus::WARNING_STATUS : ObSSLocalCacheModStatus::NORMAL_STATUS,
      diagnose_info,
      is_stuck ? "need expand but no expansion for long time" : "");

  if (is_stuck) {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "need expand but no expansion for long time",
        K(common.config_data_disk_size_), K(common.actual_data_disk_size_), KPC(this));
  }
}

void ObTenantDiskSpaceManager::check_shrink_status_nolock_(const DiskScalingDiagCommonInfo &common)
{
  int ret = OB_SUCCESS;
  char diagnose_info[OB_DIAGNOSE_INFO_LENGTH] = { 0 };

  const bool need_shrink = (common.config_data_disk_size_ < common.actual_data_disk_size_);
  char need_since_str[64] = { 0 };
  char last_attempt_str[64] = { 0 };
  char last_succ_str[64] = { 0 };
  format_ts_us_to_str(shrink_diag_.first_need_time_us_, need_since_str, sizeof(need_since_str));
  format_ts_us_to_str(shrink_diag_.last_attempt_time_us_, last_attempt_str, sizeof(last_attempt_str));
  format_ts_us_to_str(shrink_diag_.last_succ_time_us_, last_succ_str, sizeof(last_succ_str));

  bool is_stuck = false;
  int64_t stuck_us = 0;
  shrink_diag_.get_stuck_info(NEED_SHRINK_STUCK_WARN_INTERVAL_US, is_stuck, stuck_us);

  int64_t pos = 0;
  IGNORE_RETURN databuff_printf(diagnose_info, sizeof(diagnose_info), pos,
      "need_shrink=%d; need_since=%s; stuck_us=%ld;"
      " last_attempt=%s; last_succ=%s; last_ret=%d;"
      " unit_config_disk=%s; unit_actual_disk=%s;"
      " disk_cap=%s;"
      " allocated_macro_size=%s; allocated_macro_free=%s; allocated_macro_used=%s; macro_usage_pct=%.2f%%",
      need_shrink, need_since_str, stuck_us,
      last_attempt_str, last_succ_str, shrink_diag_.last_ret_,
      common.unit_config_disk_str_, common.unit_actual_disk_str_,
      common.disk_cap_str_,
      common.alloc_macro_size_str_, common.alloc_macro_free_str_, common.alloc_macro_used_str_,
      common.allocated_macro_cache_usage_pct_);

  ADD_LOCAL_CACHE_DIAGNOSE(
      DIAG_MOD_NAME, DIAG_SUBMOD_SHRINK,
      is_stuck ? ObSSLocalCacheModStatus::WARNING_STATUS : ObSSLocalCacheModStatus::NORMAL_STATUS,
      diagnose_info,
      is_stuck ? "need shrink but no shrink for long time" : "");

  if (is_stuck && TC_REACH_TIME_INTERVAL(1_min)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "need shrink but no shrink for long time",
        K(common.config_data_disk_size_), K(common.actual_data_disk_size_), KPC(this));
  }
}

void ObTenantDiskSpaceManager::check_variables_consistency_nolock_()
{
  char diagnose_info[OB_DIAGNOSE_INFO_LENGTH] = { 0 };
  const int64_t now_us = ObTimeUtility::fast_current_time();

  // 1) tenant_pair_unallocated_disk_size_ sync freshness
  const int64_t last_sync_us = tenant_pair_unallocated_size_last_sync_us_;
  const bool is_unalloc_sync_stale =
      (last_sync_us <= 0
       || last_sync_us + TENANT_PAIR_UNALLOCATED_SIZE_ERROR_LOG_THRESHOLD_US < now_us);
  const int64_t unalloc_age_us = (last_sync_us > 0 ? now_us - last_sync_us : -1);

  char last_sync_str[64] = { 0 };
  format_ts_us_to_str(last_sync_us, last_sync_str, sizeof(last_sync_str));

  char unalloc_size_str[32] = { 0 };
  format_bytes_to_str(tenant_pair_unallocated_disk_size_, unalloc_size_str, sizeof(unalloc_size_str));

  // 2) used size delta threshold check (value-based)
  const int64_t used_size_delta = get_used_size_delta_nolock_();
  const int64_t used_size_delta_abs = (used_size_delta >= 0 ? used_size_delta : -used_size_delta);
  const bool is_used_delta_too_large = (used_size_delta_abs > USED_SIZE_DELTA_ERROR_THRESHOLD);

  char used_delta_str[32] = { 0 };
  char used_delta_threshold_str[32] = { 0 };
  format_bytes_to_str(used_size_delta, used_delta_str, sizeof(used_delta_str));
  format_bytes_to_str(USED_SIZE_DELTA_ERROR_THRESHOLD, used_delta_threshold_str, sizeof(used_delta_threshold_str));

  int64_t pos = 0;
  IGNORE_RETURN databuff_printf(diagnose_info, sizeof(diagnose_info), pos,
      "tenant_pair_unallocated_last_sync=%s;"
      " tenant_pair_unallocated_sync_age_us=%ld;"
      " tenant_pair_unallocated_disk_size=%s;"
      " used_size_delta=%s; used_size_delta_threshold=%s",
      last_sync_str, unalloc_age_us, unalloc_size_str,
      used_delta_str, used_delta_threshold_str);

  const bool has_error = (is_unalloc_sync_stale || is_used_delta_too_large);
  const char *extra_info = "";
  if (has_error) {
    if (is_unalloc_sync_stale && is_used_delta_too_large) {
      extra_info = "unallocated size sync stale; used size delta too large";
    } else if (is_unalloc_sync_stale) {
      extra_info = "unallocated size sync stale";
    } else {
      extra_info = "used size delta too large";
    }
  }

  ADD_LOCAL_CACHE_DIAGNOSE(
      DIAG_MOD_NAME, DIAG_SUBMOD_CONSISTENCY,
      has_error ? ObSSLocalCacheModStatus::WARNING_STATUS : ObSSLocalCacheModStatus::NORMAL_STATUS,
      diagnose_info, extra_info);

  if (has_error) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "disk space variables consistency check failed",
        K(is_unalloc_sync_stale), K(is_used_delta_too_large), KPC(this));
  }
}

int ObTenantDiskSpaceManager::alloc_file_size(
    const int64_t alloc_size,
    const ObSSMacroCacheType cache_type,
    const ObDiskSpaceType disk_space_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY((alloc_size <= 0) || (ObSSMacroCacheType::MAX_TYPE == cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(alloc_size), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    const int8_t idx = static_cast<uint8_t>(cache_type);
    ObMutexGuard guard(disk_size_lock_);
    if (OB_FAIL(tenant_disk_space_limiter_.acquire(alloc_size))) {
      if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
        if (TC_REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
          LOG_WARN("fail to alloc file size", KR(ret),
              K_SS_MACRO_CACHE_TYPE(cache_type), K(alloc_size), KPC(this));
        }
      } else {
        LOG_WARN("fail to alloc file size", KR(ret),
            K_SS_MACRO_CACHE_TYPE(cache_type), K(alloc_size), KPC(this));
      }
    } else {
      set_macro_cache_used_nolock_(cache_type, macro_cache_stats_[idx].used_ + alloc_size);
      if (ObDiskSpaceType::DIR == disk_space_type) {
        dir_sizes_[idx] += alloc_size;
        dir_nums_[idx]++;
      }
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::free_file_size(
    const int64_t free_size,
    const ObSSMacroCacheType cache_type,
    const ObDiskSpaceType disk_space_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((free_size <= 0) || (ObSSMacroCacheType::MAX_TYPE == cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(free_size), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    tenant_disk_space_limiter_.release(free_size);
    if (OB_UNLIKELY(free_size > macro_cache_stats_[idx].used_)) {
      const int64_t exceeded_size = free_size - macro_cache_stats_[idx].used_;
      if (is_first_calibration_done()) {
        // After first calibration, used_ is meaningful; treat free - used > threshold as accounting error and record it.
        free_size_exceeded_sum_[idx] += exceeded_size;
        const int64_t threshold = get_free_exceeded_threshold_();
        if (free_size_exceeded_sum_[idx] > threshold) {
          FLOG_ERROR("free size is larger than used size", KR(ret), K(free_size),
              K(macro_cache_stats_[idx].used_), K(exceeded_size), K(threshold),
              K_SS_MACRO_CACHE_TYPE(cache_type), KPC(this));
        } else {
          LOG_WARN("free size is larger than used size", KR(ret), K(free_size),
              K(macro_cache_stats_[idx].used_), K(exceeded_size), K(threshold),
              K_SS_MACRO_CACHE_TYPE(cache_type), KPC(this));
        }
      } else {
        // Before first calibration, used_ may not reflect real usage yet; do not accumulate
        // free_size_exceeded_sum_ or FLOG_ERROR to avoid false alarms; still clamp used_ to 0.
        LOG_WARN("free size is larger than used size", KR(ret), K(free_size),
            K(macro_cache_stats_[idx].used_), K_SS_MACRO_CACHE_TYPE(cache_type), KPC(this));
      }
      set_macro_cache_used_nolock_(cache_type, 0);
    } else {
      set_macro_cache_used_nolock_(cache_type, macro_cache_stats_[idx].used_ - free_size);
    }
    if (ObDiskSpaceType::DIR == disk_space_type) {
      const int64_t dir_num = free_size / ObDirManager::DEFAULT_DIR_SIZE;
      if ((dir_sizes_[idx] - free_size < 0) || (dir_nums_[idx] - dir_num < 0)) {
        const int64_t exceeded_dir_size = (free_size > dir_sizes_[idx]) ? (free_size - dir_sizes_[idx]) : 0;
        const int64_t exceeded_dir_num = (dir_num > dir_nums_[idx]) ? (dir_num - dir_nums_[idx]) : 0;
        if (is_first_calibration_done()) {
          // After first calibration, dir_sizes_/dir_nums_ are meaningful; treat as accounting error.
          if (exceeded_dir_size > 0) {
            free_dir_size_exceeded_sum_[idx] += exceeded_dir_size;
          }
          if (exceeded_dir_num > 0) {
            free_dir_num_exceeded_sum_[idx] += exceeded_dir_num;
          }
          const int64_t threshold = get_free_exceeded_threshold_();
          const int64_t dir_threshold = threshold / ObDirManager::DEFAULT_DIR_SIZE;
          if (free_dir_size_exceeded_sum_[idx] > threshold || free_dir_num_exceeded_sum_[idx] > dir_threshold) {
            FLOG_ERROR("dir size is less than free size", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(free_size),
                K(dir_sizes_[idx]), K(dir_num), K(dir_nums_[idx]), K(exceeded_dir_size), K(exceeded_dir_num),
                K(threshold), K(dir_threshold), KPC(this));
          } else {
            LOG_WARN("dir size is less than free size", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(free_size),
                K(dir_sizes_[idx]), K(dir_num), K(dir_nums_[idx]), K(exceeded_dir_size), K(exceeded_dir_num),
                K(threshold), K(dir_threshold), KPC(this));
          }
        } else {
          // Before first calibration, dir_sizes_/dir_nums_ may not be calibrated yet; do not FLOG_ERROR
          // to avoid false alarms; still clamp and log WARN.
          LOG_WARN("dir size is less than free size", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(free_size), 
            K(dir_sizes_[idx]), K(dir_num), K(dir_nums_[idx]), KPC(this));
        }
      }
      dir_sizes_[idx] = (dir_sizes_[idx] - free_size) > 0 ? (dir_sizes_[idx] - free_size) : 0;
      dir_nums_[idx] = (dir_nums_[idx] - dir_num) > 0 ? (dir_nums_[idx] - dir_num) : 0;
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::shift_file_size(
    const ObSSMacroCacheType ori_cache_type,
    const ObSSMacroCacheType new_cache_type,
    const int64_t file_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret));
  } else if (OB_UNLIKELY((file_size <= 0) || !is_valid_ss_macro_cache_type(ori_cache_type) ||
                         !is_valid_ss_macro_cache_type(new_cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(file_size), K_SS_MACRO_CACHE_TYPE(ori_cache_type),
             K_SS_MACRO_CACHE_TYPE(new_cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    // 1. decrease used size of ori_cache_type
    const uint8_t ori_idx = static_cast<uint8_t>(ori_cache_type);
    if (OB_UNLIKELY(file_size > macro_cache_stats_[ori_idx].used_)) {
      if (is_first_calibration_done()) {
        // 首次校准后 used_ 可信，file_size > used_ 视为记账异常，累计并 FLOG_ERROR
        shift_size_exceeded_sum_[ori_idx] += file_size - macro_cache_stats_[ori_idx].used_;
        FLOG_ERROR("file size is larger than used size", KR(ret),
            K(file_size), K(macro_cache_stats_[ori_idx].used_),
            K_SS_MACRO_CACHE_TYPE(ori_cache_type), K_SS_MACRO_CACHE_TYPE(new_cache_type), KPC(this));
      } else {
        // 首次校准前 used_ 由可能过期的 disk_space_meta 播种、不可信，over-shift 属预期，
        // 不累加 shift_size_exceeded_sum_、降级为 WARN，仅 clamp used_ 到 0
        LOG_WARN("file size is larger than used size before first calibration", KR(ret),
            K(file_size), K(macro_cache_stats_[ori_idx].used_),
            K_SS_MACRO_CACHE_TYPE(ori_cache_type), K_SS_MACRO_CACHE_TYPE(new_cache_type), KPC(this));
      }
      set_macro_cache_used_nolock_(ori_cache_type, 0);
    } else {
      set_macro_cache_used_nolock_(ori_cache_type, macro_cache_stats_[ori_idx].used_ - file_size);
    }
    // 2. increase used size of new_cache_type
    const uint8_t new_idx = static_cast<uint8_t>(new_cache_type);
    set_macro_cache_used_nolock_(new_cache_type, macro_cache_stats_[new_idx].used_ + file_size);
  }
  return ret;
}

void ObTenantDiskSpaceManager::set_macro_cache_used_nolock_(
    const ObSSMacroCacheType cache_type,
    const int64_t new_used_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_ss_macro_cache_type(cache_type) || new_used_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(new_used_size));
  } else {
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    macro_cache_stats_[idx].used_ = new_used_size;
    if (ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cache_type) {
      IGNORE_RETURN OB_SERVER_DISK_SPACE_MGR.set_tenant_hot_macro_used(
          tenant_id_, macro_cache_stats_[idx].used_);
    }
  }
}


int ObTenantDiskSpaceManager::resize_total_disk_size_(const int64_t new_data_disk_size, bool &succ_resize)
{
  int ret = OB_SUCCESS;
  succ_resize = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K_(is_inited));
  } else if (new_data_disk_size == total_disk_size_) {
    // do nothing, tenant data disk size does not changed
  } else if (new_data_disk_size < total_disk_size_) {
    ret = shrink_data_disk_size(total_disk_size_ - new_data_disk_size);
  } else if (new_data_disk_size > total_disk_size_) {
    ret = expand_data_disk_size(new_data_disk_size - total_disk_size_);
  }

  if (OB_SUCC(ret)) {
    succ_resize = true;
  }
  return ret;
}

int ObTenantDiskSpaceManager::get_tenant_unit_data_disk_size(
    const uint64_t tenant_id,
    int64_t &config_data_disk_size,
    int64_t &actual_data_disk_size)
{
  int ret = OB_SUCCESS;
  config_data_disk_size = 0;
  actual_data_disk_size = 0;
  omt::ObTenant *tenant = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || is_virtual_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.omt_->get_tenant(tenant_id, tenant))) {
    LOG_WARN("fail to get tenant", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant is null", KR(ret), KP(tenant));
  } else {
    const share::ObUnitInfoGetter::ObTenantConfig &unit = tenant->get_unit();
    config_data_disk_size = unit.config_.data_disk_size();
    actual_data_disk_size = unit.actual_data_disk_size_;
    if (is_sys_tenant(tenant_id) && !tenant->is_hidden()) {
      config_data_disk_size += OB_SERVER_DISK_SPACE_MGR.get_hidden_sys_data_disk_config_size();
    }

    if (OB_UNLIKELY(config_data_disk_size <= 0 || actual_data_disk_size < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("config or actual data disk size is unexpected", KR(ret),
          K(unit), K(config_data_disk_size), K(actual_data_disk_size), K(tenant_id));
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::need_expand_disk_size(bool &is_need_expand)
{
  int ret = OB_SUCCESS;
  is_need_expand = false;
  int64_t config_data_disk_size = 0;
  int64_t actual_data_disk_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_tenant_unit_data_disk_size(
      tenant_id_, config_data_disk_size, actual_data_disk_size))) {
    LOG_WARN("fail to get tenant config and actual data disk size", KR(ret), K(tenant_id_));
  } else if (config_data_disk_size > actual_data_disk_size) {
    ObMutexGuard guard(disk_size_lock_);
    // if macro_cache_used_size has reached 90% of macro_cache_size, need auto expand tenant data disk size
    // Micro cache expansion eats up free space,
    // so once space is exhausted the macro block cache usage can hit the threshold
    const double macro_cache_size = get_allocated_shared_macro_cache_size_nolock_();
    const double macro_cache_free_size = get_allocated_shared_macro_cache_free_size_nolock_();
    const double disk_size_capacity = get_disk_size_capacity_nolock_();
    const bool is_need_expand_for_macro_cache =
        (macro_cache_free_size <= macro_cache_size * (1.0 - DEFAULT_TENANT_DISK_SIZE_EXPAND_THRESHOLD_PCT));
    is_need_expand = is_need_expand_for_macro_cache;
  }

  if (OB_SUCC(ret)) {
    ObMutexGuard guard(disk_size_lock_);
    expand_diag_.refresh_first_need_time(is_need_expand);
  }
  return ret;
}

int ObTenantDiskSpaceManager::expand_data_disk_size(const int64_t expand_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_INIT) {
    ObMutexGuard guard(disk_size_lock_);
    expand_diag_.mark_attempt();
  }
  int64_t config_data_disk_size = 0;
  int64_t actual_data_disk_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(expand_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(expand_size));
  } else if (OB_FAIL(get_tenant_unit_data_disk_size(
      tenant_id_, config_data_disk_size, actual_data_disk_size))) {
    LOG_WARN("fail to get tenant config and actual data disk size", KR(ret), K(tenant_id_));
  } else if (config_data_disk_size <= actual_data_disk_size) {
    LOG_INFO("tenant data disk size is already reached config value, no need to expand",
        K(tenant_id_), K(config_data_disk_size), K(actual_data_disk_size));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const int64_t actual_expand_size =
        MIN(expand_size, config_data_disk_size - total_disk_size_);
    if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.alloc(actual_expand_size))) {
      LOG_WARN("fail to alloc disk size", KR(ret), K(config_data_disk_size),
          K(expand_size), K(actual_expand_size), K(actual_data_disk_size), KPC(this));
    } else {
      const int64_t delta_capacity = cal_delta_capacity_(actual_expand_size);
      if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.update_share_tenant_total_size(
          tenant_id_, delta_capacity))) {
        LOG_WARN("fail to update share tenant limiter",
            KR(ret), K(config_data_disk_size), KPC(this),
            K(expand_size), K(actual_expand_size), K(actual_data_disk_size), K(delta_capacity));
      } else {
        const int64_t old_data_disk_size = total_disk_size_;
        total_disk_size_ += actual_expand_size;
        SERVER_EVENT_ADD("shared_storage", "auto expand data disk size",
          "tenant_id", tenant_id_,
          "ret", ret,
          "expand_size", actual_expand_size,
          "new_data_disk_size", total_disk_size_);
        FLOG_INFO("succ to auto expand data disk size",
            KPC(this), K(config_data_disk_size), K(actual_data_disk_size),
            K(old_data_disk_size), K(expand_size), K(actual_expand_size));
        expand_diag_.mark_success();
      }

      if (OB_FAIL(ret)) { // if fail, need free disk size
        if (OB_TMP_FAIL(OB_SERVER_DISK_SPACE_MGR.free(actual_expand_size))) {
          LOG_ERROR("fail to free disk size", KR(ret), KR(tmp_ret), K(expand_size),
              K(config_data_disk_size), K(actual_expand_size), K(actual_data_disk_size), KPC(this));
        }
      }
    }
  }

  if (IS_INIT) {
    ObMutexGuard guard(disk_size_lock_);
    expand_diag_.mark_ret(ret);
  }
  return ret;
}

int ObTenantDiskSpaceManager::need_shrink_disk_size(bool &is_need_shrink)
{
  int ret = OB_SUCCESS;
  is_need_shrink = false;
  int64_t config_data_disk_size = 0;
  int64_t actual_data_disk_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_tenant_unit_data_disk_size(
      tenant_id_, config_data_disk_size, actual_data_disk_size))) {
    LOG_WARN("fail to get tenant config and actual data disk size", KR(ret), K(tenant_id_));
  } else if (config_data_disk_size < actual_data_disk_size) {
    is_need_shrink = true;
  }

  if (OB_SUCC(ret)) {
    ObMutexGuard guard(disk_size_lock_);
    shrink_diag_.refresh_first_need_time(is_need_shrink);
  }
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_ERROR_SKIP_SHRINK_DATA_DISK_SIZE)
int ObTenantDiskSpaceManager::shrink_data_disk_size(const int64_t shrink_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_INIT) {
    ObMutexGuard guard(disk_size_lock_);
    shrink_diag_.mark_attempt();
  }
  omt::ObTenant *tenant = nullptr;
  int64_t config_data_disk_size = 0;
  int64_t actual_data_disk_size = 0;
  int64_t min_shrinkable_disk_size = 0;
  if (OB_UNLIKELY(ERRSIM_ERROR_SKIP_SHRINK_DATA_DISK_SIZE)) {
    LOG_WARN("errsim skip shrink data disk size", KR(ret));
  } else if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(shrink_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(shrink_size));
  } else if (OB_FAIL(get_tenant_unit_data_disk_size(
      tenant_id_, config_data_disk_size, actual_data_disk_size))) {
    LOG_WARN("fail to get tenant config and actual data disk size", KR(ret), K(tenant_id_));
  } else if (config_data_disk_size >= actual_data_disk_size) {
    LOG_INFO("tenant have enough disk space, should not shrink data disk size",
        KPC(this), K(config_data_disk_size), K(actual_data_disk_size));
  } else if (OB_FAIL(cal_min_shrinkable_disk_size(min_shrinkable_disk_size))) {
    LOG_WARN("fail to cal min shrinkable disk size", KR(ret),
        KPC(this), K(config_data_disk_size), K(actual_data_disk_size));
  } else if (min_shrinkable_disk_size >= actual_data_disk_size) {
    LOG_INFO("tenant data disk size is already reached min shrinkable disk size, cannot shrink data disk size",
        KPC(this), K(config_data_disk_size), K(min_shrinkable_disk_size), K(actual_data_disk_size));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    min_shrinkable_disk_size = MAX(min_shrinkable_disk_size, config_data_disk_size);
    const int64_t actual_shrink_size =
        MIN(shrink_size, total_disk_size_ - min_shrinkable_disk_size);
    // TODO: @fangdan: 用guard优化
    if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.update_share_tenant_total_size(
        tenant_id_, cal_delta_capacity_(-actual_shrink_size)))) {
      LOG_WARN("fail to update share tenant limiter", KR(ret),
          K(config_data_disk_size), K(actual_data_disk_size),
          K(shrink_size), K(actual_shrink_size), K(min_shrinkable_disk_size), KPC(this));
    } else {
      if (OB_FAIL(OB_SERVER_DISK_SPACE_MGR.free(actual_shrink_size))) {
        LOG_WARN("fail to free disk size", KR(ret),
            K(config_data_disk_size), K(actual_data_disk_size),
            K(shrink_size), K(actual_shrink_size), K(min_shrinkable_disk_size), KPC(this));
      } else {
        const int64_t old_data_disk_size = total_disk_size_;
        total_disk_size_ -= actual_shrink_size;
        SERVER_EVENT_ADD("shared_storage", "auto shrink data disk size",
          "tenant_id", tenant_id_,
          "ret", ret,
          "shrink_size", actual_shrink_size,
          "new_data_disk_size", total_disk_size_);
        FLOG_INFO("succ to auto shrink data disk size", KPC(this),
            K(config_data_disk_size), K(actual_data_disk_size), K(shrink_size),
            K(old_data_disk_size), K(actual_shrink_size), K(min_shrinkable_disk_size));
        shrink_diag_.mark_success();
      }

      if (OB_FAIL(ret)) { // if fail, need update_share_tenant_total_size
        if (OB_TMP_FAIL(OB_SERVER_DISK_SPACE_MGR.update_share_tenant_total_size(
            tenant_id_, cal_delta_capacity_(actual_shrink_size)))) {
          LOG_ERROR("fail to rollback share tenant limiter", KR(ret), KR(tmp_ret),
              K(config_data_disk_size), K(actual_data_disk_size), K(shrink_size),
              K(actual_shrink_size), K(min_shrinkable_disk_size), KPC(this));
        }
      }
    }
  }
  if (IS_INIT) {
    ObMutexGuard guard(disk_size_lock_);
    shrink_diag_.mark_ret(ret);
  }
  return ret;
}

int ObTenantDiskSpaceManager::cal_cur_available_shrink_size(
    const int64_t macro_cache_cur_min_shrinkable_size, int64_t &cur_available_shrink_size)
{
  int ret = OB_SUCCESS;
  cur_available_shrink_size = 0;
  ObMutexGuard guard(disk_size_lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(macro_cache_cur_min_shrinkable_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_cache_cur_min_shrinkable_size));
  } else {
    const int64_t target_size = macro_cache_cur_min_shrinkable_size;
    // Use the shared macro-cache capacity above target_size as a disk-shrink candidate.
    const int64_t raw_available =
        MAX(0, get_allocated_shared_macro_cache_size_nolock_() - target_size);
    const int64_t max_step =
        get_allocated_shared_macro_cache_free_size_nolock_() / 100 * TENANT_DISK_SIZE_MAX_SHRINK_STEP_PCT;
    cur_available_shrink_size = MIN(raw_available, max_step);
    if (OB_UNLIKELY(cur_available_shrink_size <= 0)) {
      // ignore error
      LOG_INFO("cannot shrink disk size",
          K(cur_available_shrink_size), K(raw_available), K(max_step), K(target_size), KPC(this));
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::cal_min_shrinkable_disk_size(int64_t &min_shrinkable_disk_size)
{
  int ret = OB_SUCCESS;
  min_shrinkable_disk_size = INT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    /*
     * Compute the minimum data-disk size for this *single* tenant instance.
     *
     * The lower bound here is:
     *   reserved_dir_size + MIN_REQUIRED_DISK_SIZE.
     *
     * This function intentionally does NOT:
     *   - apply any CPU-based reservation term, nor
     *   - aggregate the meta/user pair for a logical tenant.
     *
     * CPU-based floors (including sys tenant) and meta+user aggregation are
     * handled by the global helper:
     *   ObDiskSpaceManager::cal_min_shrinkable_disk_size(tenant_id, ...).
     */
    const int64_t cannot_shrink_disk_size = get_reserved_disk_size();
    if (OB_UNLIKELY(cannot_shrink_disk_size > unit_config_data_disk_size_)) {
      ret = OB_RESOURCE_UNIT_VALUE_BELOW_LIMIT;
      LOG_WARN("cannot shrink disk size is greater than config data disk size",
          KR(ret), K(cannot_shrink_disk_size), KPC(this));
    } else {
      min_shrinkable_disk_size = MIN(
          unit_config_data_disk_size_,
          cannot_shrink_disk_size + ObSSMacroCacheMgr::MIN_REQUIRED_DISK_SIZE);
      if (OB_UNLIKELY(min_shrinkable_disk_size <= 0 || min_shrinkable_disk_size == INT64_MAX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("min shrinkable disk size is invalid",
            KR(ret), K(min_shrinkable_disk_size), KPC(this));
      }
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::set_config_data_disk_size(const int64_t new_config_data_disk_size)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret));
  } else if (OB_UNLIKELY(new_config_data_disk_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(new_config_data_disk_size));
  } else if (new_config_data_disk_size != unit_config_data_disk_size_) {
    ObMutexGuard guard(disk_size_lock_);
    unit_config_data_disk_size_ = new_config_data_disk_size;
    FLOG_INFO("succ to set config data disk size", KPC(this), K(new_config_data_disk_size));
  }
  return ret;
}

int ObTenantDiskSpaceManager::sync_tenant_pair_unallocated_disk_size()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager is not inited", KR(ret));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    ret = sync_tenant_pair_unallocated_disk_size_nolock_();
  }
  return ret;
}

int ObTenantDiskSpaceManager::sync_tenant_pair_unallocated_disk_size_nolock_()
{
  int ret = OB_SUCCESS;
  int64_t new_tenant_pair_unallocated_disk_size = 0;
  if (OB_FAIL(ObDiskSpaceManager::get_tenant_pair_unallocated_disk_size(
      tenant_id_, new_tenant_pair_unallocated_disk_size))) {
    if (OB_TENANT_NOT_IN_SERVER == ret) {
      // Tenant may be stopping (e.g., unit migration). Treat as a benign case:
      // - do not log (avoid log storm)
      // - refresh last sync time to prevent false alarm
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tenant pair unallocated disk size", KR(ret), KPC(this));
    }
  } else if (new_tenant_pair_unallocated_disk_size != tenant_pair_unallocated_disk_size_) {
    FLOG_INFO("succ to set tenant pair unallocated disk size",
        KPC(this), K(new_tenant_pair_unallocated_disk_size));
    tenant_pair_unallocated_disk_size_ = new_tenant_pair_unallocated_disk_size;
  }

  if (OB_SUCC(ret)) {
    tenant_pair_unallocated_size_last_sync_us_ = ObTimeUtility::fast_current_time();
  }
  return ret;
}

int ObTenantDiskSpaceManager::calibrate_alloc_size_nolock_(
    const int64_t alloc_size,
    const ObSSMacroCacheType cache_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((alloc_size < 0) || (ObSSMacroCacheType::MAX_TYPE == cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(alloc_size), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    FLOG_INFO("begin to calibrate alloc size",
        K(alloc_size), K_SS_MACRO_CACHE_TYPE(cache_type), KPC(this),
        "share_tenant_total_size", OB_SERVER_DISK_SPACE_MGR.get_share_tenant_total_size(tenant_id_));
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    const int64_t delta_used = alloc_size - macro_cache_stats_[idx].used_;
    tenant_disk_space_limiter_.force_update_used(delta_used);
    set_macro_cache_used_nolock_(cache_type, alloc_size);
    FLOG_INFO("finish to calibrate alloc size",
        KR(ret), K(alloc_size), K_SS_MACRO_CACHE_TYPE(cache_type), KPC(this),
        "share_tenant_total_size", OB_SERVER_DISK_SPACE_MGR.get_share_tenant_total_size(tenant_id_));
  }
  return ret;
}

int ObTenantDiskSpaceManager::calibrate_alloc_size(
  const int64_t alloc_size,
  const ObSSMacroCacheType cache_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    ret = calibrate_alloc_size_nolock_(alloc_size, cache_type);
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to calibrate alloc size", KR(ret), K(alloc_size), K_SS_MACRO_CACHE_TYPE(cache_type));
    } else {
      const uint8_t idx = static_cast<uint8_t>(cache_type);
      free_dir_size_exceeded_sum_[idx] = 0;
      free_dir_num_exceeded_sum_[idx] = 0;
      free_size_exceeded_sum_[idx] = 0;
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::calibrate_alloc_size_with_initial(
    const int64_t initial_used_size,
    const int64_t base_target_size,
    const ObSSMacroCacheType cache_type,
    int64_t &final_target_size,
    int64_t &observed_used_size)
{
  int ret = OB_SUCCESS;
  final_target_size = 0;
  observed_used_size = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret));
  } else if (OB_UNLIKELY(initial_used_size < 0 || base_target_size < 0
      || !is_valid_ss_macro_cache_type(cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret),
        K(initial_used_size), K(base_target_size), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    observed_used_size = macro_cache_stats_[idx].used_;
    int64_t target = base_target_size + (observed_used_size - initial_used_size);
    if (OB_UNLIKELY(target < 0)) {
      // target = base_target + (observed - initial). Negative requires
      //   initial - observed > disk_scan + dir_cal
      // With pause_gc active during calibration, most free_file_size sources are
      // blocked:
      //   - background GC paused
      //   - user-triggered tmp file delete -> logical rename (.deleted), no free_file_size
      //   - user-triggered macro/meta delete -> rejected with OB_FILE_DELETE_FAILED
      // The only remaining paths that can shrink used_ are alloc-then-write-failure
      // rollbacks and shift_file_size. Reaching a negative
      // here therefore requires either an unusual burst of write failures, or a
      // stale initial inherited from prior calibration drift. Clamp to 0 (matching
      // disk's view) and continue so exceed_sums still get cleared below, rather
      // than failing with OB_INVALID_ARGUMENT from calibrate_alloc_size_nolock_(<0)
      // which would leave stale exceed_sums.
      LOG_WARN("computed final_target_size is negative, clamp to 0",
          K(base_target_size), K(observed_used_size), K(initial_used_size),
          K_SS_MACRO_CACHE_TYPE(cache_type));
      target = 0;
    }
    final_target_size = target;
    if (OB_FAIL(calibrate_alloc_size_nolock_(final_target_size, cache_type))) {
      LOG_WARN("fail to calibrate alloc size", KR(ret),
          K(base_target_size), K(observed_used_size), K(initial_used_size),
          K(final_target_size), K_SS_MACRO_CACHE_TYPE(cache_type));
    } else {
      free_dir_size_exceeded_sum_[idx] = 0;
      free_dir_num_exceeded_sum_[idx] = 0;
      free_size_exceeded_sum_[idx] = 0;
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::calibrate_dir_info(
  const int64_t dir_num,
  const ObSSMacroCacheType cache_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY((dir_num < 0) || (ObSSMacroCacheType::MAX_TYPE == cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(dir_num), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    dir_nums_[idx] = dir_num;
    dir_sizes_[idx] = dir_num * ObDirManager::DEFAULT_DIR_SIZE;
    FLOG_INFO("finish to calibrate dir size", KR(ret), K(dir_num), "dir_size", dir_sizes_[idx],
              K_SS_MACRO_CACHE_TYPE(cache_type));
  }
  return ret;
}

int ObTenantDiskSpaceManager::calibrate_dir_info_with_initial(
    const int64_t initial_dir_num,
    const int64_t scanned_dir_num,
    const ObSSMacroCacheType cache_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(initial_dir_num < 0 || scanned_dir_num < 0
      || !is_valid_ss_macro_cache_type(cache_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret),
        K(initial_dir_num), K(scanned_dir_num), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    // final = scanned_dir_num + current_dir_num - initial_dir_num
    // so racing dir alloc/free during the scan window is folded in.
    const int64_t delta = scanned_dir_num - initial_dir_num;
    dir_nums_[idx] += delta;
    if (OB_UNLIKELY(dir_nums_[idx] < 0)) {
      dir_nums_[idx] = 0;
    }
    dir_sizes_[idx] = dir_nums_[idx] * ObDirManager::DEFAULT_DIR_SIZE;
    FLOG_INFO("finish to calibrate dir info with initial", KR(ret),
        K(initial_dir_num), K(scanned_dir_num), K(delta),
        "final_dir_num", dir_nums_[idx], "final_dir_size", dir_sizes_[idx],
        K_SS_MACRO_CACHE_TYPE(cache_type));
  }
  return ret;
}

int ObTenantDiskSpaceManager::sync_used_size_to_macro_cache_stats()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("disk space manager has not been inited", KR(ret), K_(is_inited));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    // used_size_delta = limiter_used - sum(macro_cache_stats_.used_)
    const int64_t used_size_delta = get_used_size_delta_nolock_();
    if (OB_UNLIKELY(used_size_delta != 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("used size delta is not 0", KR(ret), KPC(this));
      tenant_disk_space_limiter_.force_update_used(-used_size_delta);
    }
  }
  return ret;
}

int64_t ObTenantDiskSpaceManager::get_total_disk_size()
{
  ObMutexGuard guard(disk_size_lock_);
  return total_disk_size_;
}

int64_t ObTenantDiskSpaceManager::get_allocated_shared_macro_cache_size_nolock_() const
{
  return MAX(0, OB_SERVER_DISK_SPACE_MGR.get_share_tenant_total_size(tenant_id_));
}

int64_t ObTenantDiskSpaceManager::get_tenant_pair_unallocated_disk_capacity_nolock_()
{
  if (IS_INIT) {
    if (OB_UNLIKELY(tenant_pair_unallocated_size_last_sync_us_ + TENANT_PAIR_UNALLOCATED_SIZE_FORCE_SYNC_THRESHOLD_US
        < ObTimeUtility::fast_current_time())) {
      IGNORE_RETURN sync_tenant_pair_unallocated_disk_size_nolock_();
    }
    if (OB_UNLIKELY(tenant_pair_unallocated_size_last_sync_us_ + TENANT_PAIR_UNALLOCATED_SIZE_ERROR_LOG_THRESHOLD_US
        < ObTimeUtility::fast_current_time())) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, 
          "tenant pair unallocated disk size has not been synced for a long time", KPC(this));
    }
  }

  const int64_t tenant_pair_unallocated_disk_capacity =
      cal_delta_capacity_(tenant_pair_unallocated_disk_size_);
  return tenant_pair_unallocated_disk_capacity;
}

int64_t ObTenantDiskSpaceManager::get_unit_config_shared_macro_cache_size()
{
  ObMutexGuard guard(disk_size_lock_);
  return get_allocated_shared_macro_cache_size_nolock_()
      + get_tenant_pair_unallocated_disk_capacity_nolock_();
}

int64_t ObTenantDiskSpaceManager::get_allocated_shared_macro_cache_size()
{
  ObMutexGuard guard(disk_size_lock_);
  return get_allocated_shared_macro_cache_size_nolock_();
}

double ObTenantDiskSpaceManager::get_tenant_macro_cache_usage_ratio_() const
{
  static const double META_TENANT_DATA_DISK_SIZE_PERCENTAGE =
      static_cast<double>(ObUnitResource::META_TENANT_DATA_DISK_SIZE_PERCENTAGE) / 100.0;
  
  double usage_ratio = 1.0;
  if (is_meta_tenant(tenant_id_)) {
    usage_ratio = META_TENANT_DATA_DISK_SIZE_PERCENTAGE;
  } else if (is_user_tenant(tenant_id_)) {
    usage_ratio = 1.0 - META_TENANT_DATA_DISK_SIZE_PERCENTAGE;
  } else {  // sys tenant
    usage_ratio = 1.0;
  }
  return usage_ratio;
}

int64_t ObTenantDiskSpaceManager::get_unit_config_tnt_macro_cache_size_by_usage_ratio()
{
  const double usage_ratio = get_tenant_macro_cache_usage_ratio_();
  const double tenant_macro_cache_size =
      static_cast<double>(get_tenant_macro_cache_used_size())
      + usage_ratio * static_cast<double>(get_unit_config_shared_macro_cache_free_size());
  return MAX(0, static_cast<int64_t>(tenant_macro_cache_size));
}

int64_t ObTenantDiskSpaceManager::get_allocated_tnt_macro_cache_size_by_usage_ratio()
{
  ObMutexGuard guard(disk_size_lock_);
  const double usage_ratio = get_tenant_macro_cache_usage_ratio_();
  const double tenant_macro_cache_size =
      static_cast<double>(get_tenant_macro_cache_used_size_nolock_())
      + usage_ratio * static_cast<double>(get_allocated_shared_macro_cache_free_size_nolock_());
  return MAX(0, static_cast<int64_t>(tenant_macro_cache_size));
}

int64_t ObTenantDiskSpaceManager::get_shared_macro_cache_used_size()
{
  ObMutexGuard guard(disk_size_lock_);
  return MAX(0, get_allocated_shared_macro_cache_size_nolock_() - get_allocated_shared_macro_cache_free_size_nolock_());
}

int64_t ObTenantDiskSpaceManager::get_tenant_macro_cache_used_size_nolock_() const
{
  return MAX(0, tenant_disk_space_limiter_.get_used());
}

int64_t ObTenantDiskSpaceManager::get_tenant_macro_cache_used_size()
{
  ObMutexGuard guard(disk_size_lock_);
  return get_tenant_macro_cache_used_size_nolock_();
}

int64_t ObTenantDiskSpaceManager::get_allocated_shared_macro_cache_free_size_nolock_() const
{
  return MAX(0, OB_SERVER_DISK_SPACE_MGR.get_share_tenant_free_size(tenant_id_));
}

// Callers are externally synchronized by disk_size_lock_, so the value read here is accurate.
int64_t ObTenantDiskSpaceManager::get_free_exceeded_threshold_() const
{
  static const int64_t FREE_EXCEEDED_PERCENTAGE_THRESHOLD = 1;
  static const int64_t FREE_EXCEEDED_MAX_BYTES_THRESHOLD = 1L * 1024L * 1024L * 1024L; // 1GB
  const int64_t threshold_from_usage_ratio = get_allocated_shared_macro_cache_size_nolock_() *
                                              FREE_EXCEEDED_PERCENTAGE_THRESHOLD / 100;
  return MIN(threshold_from_usage_ratio, FREE_EXCEEDED_MAX_BYTES_THRESHOLD);
}

int64_t ObTenantDiskSpaceManager::get_unit_config_shared_macro_cache_free_size()
{
  ObMutexGuard guard(disk_size_lock_);
  return get_allocated_shared_macro_cache_free_size_nolock_()
      + get_tenant_pair_unallocated_disk_capacity_nolock_();
}

// TODO @fangdan: 提供obadmin，支持直接修改allocated，防止扩容出问题，跟不上，影响性能

int ObTenantDiskSpaceManager::get_theoretical_free_disk_size(
    const ObSSMacroCacheType cache_type,
    int64_t &theoretical_free_size)
{
  int ret = OB_SUCCESS;
  theoretical_free_size = 0;
  ObSSMacroCacheStat cache_stat;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_FAIL(get_macro_cache_stat(cache_type, cache_stat))) {
    LOG_WARN("fail to get macro cache stat", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    const int64_t macro_cache_size = get_unit_config_shared_macro_cache_size();
    int64_t occupy_to_min_delta = 0;
    // TODO @fangdan 测试验证，meta租户转auto后,能否淘汰
    if (!is_sys_tenant(tenant_id_) && ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK == cache_type) {
      // For meta or user tenant's HOT_TABLET_MACRO_BLOCK,
      // the "occupy-to-min" target is the share-tenant hot min,
      // and the used size is the share-tenant total hot used (meta + user).
      const int64_t share_min_size = macro_cache_size / 100 * cache_stat.get_min();
      const int64_t share_hot_used =
          OB_SERVER_DISK_SPACE_MGR.get_share_tenant_total_hot_macro_used(tenant_id_);
      occupy_to_min_delta = share_min_size - share_hot_used;
    } else {
      const int64_t cur_min_size =
          get_unit_config_tnt_macro_cache_size_by_usage_ratio() / 100 * cache_stat.get_min();
      occupy_to_min_delta = cur_min_size - cache_stat.used_;
    }

    // Note: if exceed CACHE_EVICT_THRESHOLD, then will evict old hot tablet macro cache, and add
    // new hot tablet macro cache. it is meaningless and wasteful.
    const int64_t free_size = MAX(0,
        get_unit_config_shared_macro_cache_free_size()
        - macro_cache_size / 100 * (100 - ObSSMacroCacheMgr::CACHE_EVICT_THRESHOLD));

    // theoretical_free_size is the larger of:
    // - the gap to reach the min-occupy target (occupy_to_min_delta, for HOT_TABLET_MACRO_BLOCK)
    // - the consumable free space without crossing CACHE_EVICT_THRESHOLD (free_size)
    theoretical_free_size = MAX(occupy_to_min_delta, free_size);
  }
  return ret;
}

// this function is for inner_table/view display
int ObTenantDiskSpaceManager::get_used_disk_size(int64_t &used_disk_size)
{
  int ret = OB_SUCCESS;
  used_disk_size = 0;
  share::ObUnitInfoGetter::ObTenantConfig unit;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tenant disk space manager has not been inited", KR(ret), K_(is_inited));
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("omt is null", KR(ret));
  } else if (OB_FAIL(GCTX.omt_->get_tenant_unit(tenant_id_, unit))) {
    LOG_WARN("fail to get tenant unit config", KR(ret), K(tenant_id_));
  } else {
    const int64_t tenant_config_disk_size = unit.config_.data_disk_size();
    ObMutexGuard guard(disk_size_lock_);
    used_disk_size = tenant_disk_space_limiter_.get_used();
    // because real_sys_tenant's data_disk_size = sys_unit_config + hidden_sys_data_disk_config_size
    // get tenant_used_disk_size must be less than sys_unit_config
    if (OB_SYS_TENANT_ID == tenant_id_) {
      used_disk_size = MIN(used_disk_size, tenant_config_disk_size);
    }
  }
  return ret;
}

int64_t ObTenantDiskSpaceManager::get_used_size_delta_nolock_() const
{
  int64_t used_size_delta = get_tenant_macro_cache_used_size_nolock_();
  for (uint8_t idx = 0; idx < SS_MACRO_CACHE_MAX_TYPE_VAL; idx++) {
    used_size_delta -= macro_cache_stats_[idx].used_;
  }
  return used_size_delta;
}

int ObTenantDiskSpaceManager::update_macro_cache_desc(
    const ObSSMacroCacheType cache_type,
    const ObSSMacroCacheDesc &cache_desc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    macro_cache_stats_[idx].desc_ = cache_desc;
  }
  return ret;
}

int ObTenantDiskSpaceManager::update_macro_cache_stat_info(
    const ObSSMacroCacheType cache_type,
    const ObSSMacroCacheStat &cache_stat,
    const int64_t dir_num,
    const int64_t dir_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const uint8_t idx = static_cast<uint8_t>(cache_type);

    if (OB_FAIL(calibrate_alloc_size_nolock_(cache_stat.used_, cache_type))) {
      LOG_WARN("fail to calibrate alloc size",
          KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type), K(cache_stat), KPC(this));
    } else {
      macro_cache_stats_[idx] = cache_stat;
      dir_nums_[idx] = dir_num;
      dir_sizes_[idx] = dir_size;
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::get_macro_cache_stat(
    const ObSSMacroCacheType cache_type,
    ObSSMacroCacheStat &cache_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    cache_stat = macro_cache_stats_[idx];
  }
  return ret;
}

int ObTenantDiskSpaceManager::get_macro_cache_stats(ObIArray<ObFileStatInfo> &cache_stat_info)
{
  int ret = OB_SUCCESS;
  ObFileStatInfo stat_info;
  cache_stat_info.reuse();
  ObMutexGuard guard(disk_size_lock_);
  for (uint8_t idx = 0; OB_SUCC(ret) && idx < SS_MACRO_CACHE_MAX_TYPE_VAL; ++idx) {
    stat_info.total_size_ = macro_cache_stats_[idx].used_;
    stat_info.dir_size_ = dir_sizes_[idx];
    stat_info.dir_num_ = dir_nums_[idx];
    if (OB_FAIL(cache_stat_info.push_back(stat_info))) {
      LOG_WARN("fail to push back cache stat info", KR(ret), K(idx), K(stat_info));
    }
  }
  return ret;
}

int ObTenantDiskSpaceManager::get_dir_info(const ObSSMacroCacheType cache_type, int64_t &dir_size, int64_t &dir_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObSSMacroCacheType::MAX_TYPE == cache_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cache type", KR(ret), K_SS_MACRO_CACHE_TYPE(cache_type));
  } else {
    ObMutexGuard guard(disk_size_lock_);
    const uint8_t idx = static_cast<uint8_t>(cache_type);
    dir_size = dir_sizes_[idx];
    dir_num = dir_nums_[idx];
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
