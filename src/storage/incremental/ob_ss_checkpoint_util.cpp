// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#include "lib/string/ob_sql_string.h" // for ObSqlString::assign_fmt()
#include "observer/omt/ob_tenant_config_mgr.h"
#include "ob_ss_checkpoint_util.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "storage/tx_storage/ob_checkpoint_service.h"

namespace oceanbase {
namespace storage {
using namespace checkpoint;

#define CKPT_LOG_WRAPPER K(ls_id), K(cur_ckpt_scn), KTIME(cur_ckpt_scn.convert_to_ts()), K(ckpt_record)
void ObCkptUtil::check_ckpt_changed(const bool is_ss_ckpt,
                                    const share::ObLSID ls_id,
                                    const share::SCN cur_ckpt_scn,
                                    CkptChangeRecord &ckpt_record)
{
  int ret = OB_ERR_UNEXPECTED;
  const int64_t current_ts = ObClockGenerator::getClock();
  ObSqlString warn_log;
  const char *ckpt_type = is_ss_ckpt ? "ss" : "local";

  if (cur_ckpt_scn.is_min() || cur_ckpt_scn.is_base_scn()) {
    // no need check
  } else if (ckpt_record.last_checkpoint_scn_ > cur_ckpt_scn) {
    ret = OB_ERR_UNEXPECTED;
    warn_log.assign_fmt("last %s checkpoint scn is greater than current %s checkpoint scn", ckpt_type, ckpt_type);
    STORAGE_LOG(ERROR, warn_log.ptr(), K(ret), CKPT_LOG_WRAPPER);
  } else if (ckpt_record.last_checkpoint_scn_ < cur_ckpt_scn) {
    ckpt_record.last_checkpoint_scn_ = cur_ckpt_scn;
    ckpt_record.changed_ts_ = current_ts;
  } else if (is_standby_ckpt_at_recovery_end_(cur_ckpt_scn)) {
    // Do not count time spent at the standby recovery limit as a checkpoint stall.
    ckpt_record.changed_ts_ = current_ts;
  } else {
    const int64_t DEFAULT_ADVANCE_INTERVAL = 10LL * 60LL * 1000LL * 1000LL;
    int64_t advance_checkpoint_interval = DEFAULT_ADVANCE_INTERVAL;
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      // use config value if config is valid
      advance_checkpoint_interval = tenant_config->_shared_advance_checkpoint_interval;
    }

    const int64_t WARN_INTERVAL = MAX(advance_checkpoint_interval, DEFAULT_ADVANCE_INTERVAL);
    // last checkpoint scn is equal to current ss rec scn
    if (is_ss_ckpt && current_ts - ckpt_record.changed_ts_ > 6 * WARN_INTERVAL) {
      warn_log.assign_fmt("%s checkpoint has not changed for long time", ckpt_type);
      STORAGE_LOG(ERROR, warn_log.ptr(), CKPT_LOG_WRAPPER);
    } else if (current_ts - ckpt_record.changed_ts_ > 3 * WARN_INTERVAL) {
      warn_log.assign_fmt("%s checkpoint has not changed for long time", ckpt_type);
      STORAGE_LOG(WARN, warn_log.ptr(), CKPT_LOG_WRAPPER);
    }
  }
}
#undef CKPT_LOG_WRAPPER

bool ObCkptUtil::is_standby_ckpt_at_recovery_end_(const share::SCN &cur_ckpt_scn)
{
  int ret = OB_SUCCESS;
  bool at_recovery_end = false;
  if (share::is_standby_tenant(MTL_GET_TENANT_ROLE_CACHE())) {
    rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader *);
    share::SCN recovery_until_scn;
    if (OB_ISNULL(tenant_info_loader)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tenant info loader is null", K(ret));
    } else if (OB_FAIL(tenant_info_loader->get_recovery_until_scn(recovery_until_scn))) {
      STORAGE_LOG(WARN, "failed to get recovery until scn", K(ret));
    } else {
      at_recovery_end = cur_ckpt_scn == recovery_until_scn;
    }
  }
  return at_recovery_end;
}

}  // namespace storage

}  // namespace oceanbase