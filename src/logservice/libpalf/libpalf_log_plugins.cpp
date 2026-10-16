/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "libpalf_log_plugins.h"

namespace oceanbase
{
namespace libpalf
{

LogPlugins::LogPlugins()
  : palf_monitor_lock_(common::ObLatchIds::OB_LIBPALF_LOG_PLUGINS_PALF_MONITOR_LOCK),
    palf_monitor_(nullptr),
    reconfig_checker_cb_lock_(common::ObLatchIds::OB_LIBPALF_LOG_PLUGINS_RECONFIG_CHECKER_CB_LOCK),
    reconfig_checker_cb_(nullptr)
{}

LogPlugins::~LogPlugins()
{
  destroy();
}

void LogPlugins::destroy()
{
  {
    common::RWLock::WLockGuard guard(palf_monitor_lock_);
    palf_monitor_ = nullptr;
  }
  {
    common::RWLock::WLockGuard guard(reconfig_checker_cb_lock_);
    reconfig_checker_cb_ = nullptr;
  }
}


template<>
int LogPlugins::add_plugin(palf::PalfMonitorCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(palf_monitor_lock_);
  if (OB_ISNULL(plugin)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "Palf plugin is NULL", KP(plugin));
  } else if (OB_NOT_NULL(palf_monitor_)) {
    ret = OB_OP_NOT_ALLOW;
    PALF_LOG(INFO, "Palf plugin is not NULL", KP(plugin), KP_(palf_monitor));
  } else {
    palf_monitor_ = plugin;
    PALF_LOG(INFO, "add_plugin success", KP(plugin));
  }
  return ret;
}

template<>
int LogPlugins::del_plugin(palf::PalfMonitorCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(palf_monitor_lock_);
  if (OB_NOT_NULL(palf_monitor_)) {
    PALF_LOG(INFO, "del_plugin success", KP_(palf_monitor));
    palf_monitor_ = NULL;
  }
  return ret;
}

template<>
int LogPlugins::add_plugin(palf::PalfReconfigCheckerCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(reconfig_checker_cb_lock_);
  if (OB_ISNULL(plugin)) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "Palf plugin is NULL", KP(plugin));
  } else if (OB_NOT_NULL(reconfig_checker_cb_)) {
    ret = OB_OP_NOT_ALLOW;
    PALF_LOG(INFO, "Palf plugin is not NULL", KP(plugin), KP_(reconfig_checker_cb));
  } else {
    reconfig_checker_cb_ = plugin;
    PALF_LOG(INFO, "add_plugin success", KP(plugin));
  }
  return ret;
}

template<>
int LogPlugins::del_plugin(palf::PalfReconfigCheckerCb *plugin)
{
  int ret = OB_SUCCESS;
  common::RWLock::WLockGuard guard(reconfig_checker_cb_lock_);
  if (OB_NOT_NULL(reconfig_checker_cb_)) {
    PALF_LOG(INFO, "del_plugin success", KP_(reconfig_checker_cb));
    reconfig_checker_cb_ = NULL;
  }
  return ret;
}

} // namespace libpalf
} // namespace oceanbase
