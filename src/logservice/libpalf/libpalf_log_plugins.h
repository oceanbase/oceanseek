/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_LOG_PLUGINS_H_
#define OCEANBASE_LOGSERVICE_LIBPALF_LOG_PLUGINS_H_

#include "logservice/palf/palf_callback.h"

#define LIBPALF_PLUGINS_DELEGATE_PTR(delegate_obj, lock_, func_name)   \
  template <typename ...Args>                                       \
  int func_name(Args &&...args) {                                   \
    int ret = OB_SUCCESS;                                           \
    common::RWLock::RLockGuard guard(lock_);                        \
    if (OB_UNLIKELY(OB_ISNULL(delegate_obj))) {                     \
      ret = OB_NOT_INIT;                                            \
    } else {                                                        \
      ret = delegate_obj->func_name(std::forward<Args>(args)...);   \
    }                                                               \
    return ret;                                                     \
  }

namespace oceanbase
{
namespace palf
{
class PalfMonitorCb;
class PalfReconfigCheckerCb;
}
namespace libpalf
{
class LogPlugins
{
public:
  LogPlugins();
  ~LogPlugins();
  void destroy();
  template<typename T>
  int add_plugin(T *plugin);
  template<typename T>
  int del_plugin(T *plugin);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_set_initial_member_list_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_election_leader_change_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_reconfiguration_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_replica_type_change_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_access_mode_change_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_set_base_lsn_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_enable_sync_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_disable_sync_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_enable_vote_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_disable_vote_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_advance_base_info_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_rebuild_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_flashback_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_truncate_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_role_change_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, add_log_write_stat);
  LIBPALF_PLUGINS_DELEGATE_PTR(palf_monitor_, palf_monitor_lock_, record_parent_child_change_event);
  LIBPALF_PLUGINS_DELEGATE_PTR(reconfig_checker_cb_, reconfig_checker_cb_lock_, check_can_add_member);
  LIBPALF_PLUGINS_DELEGATE_PTR(reconfig_checker_cb_, reconfig_checker_cb_lock_, check_can_change_memberlist);
  palf::PalfMonitorCb *get_palf_monitor() { return palf_monitor_; }
  TO_STRING_KV(KP_(palf_monitor), KP_(reconfig_checker_cb));
private:
  common::RWLock palf_monitor_lock_;
  palf::PalfMonitorCb *palf_monitor_;
  common::RWLock reconfig_checker_cb_lock_;
  palf::PalfReconfigCheckerCb *reconfig_checker_cb_;
};

} // namespace libpalf
} // namespace oceanbase
#endif