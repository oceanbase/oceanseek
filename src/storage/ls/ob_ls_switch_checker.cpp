/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 
#include "ob_ls_switch_checker.h"
#include "ob_ls.h"

namespace oceanbase
{
namespace storage
{

ObLSSwitchChecker::ObLSSwitchChecker(ObLS &ls)
  : ls_(ls),
    record_switch_epoch_(UINT64_MAX)
{
}

bool ObLSSwitchChecker::check_online()
{
  record_switch_epoch_ = ATOMIC_LOAD(&(ls_.switch_epoch_));
  return (record_switch_epoch_ & 1) != 0;
}

bool ObLSSwitchChecker::double_check_online(bool &is_epoch_match) const
{
  const int64_t switch_state = ATOMIC_LOAD(&(ls_.switch_epoch_));
  is_epoch_match = (record_switch_epoch_ == switch_state);
  return (switch_state & 1) != 0;
}

}
}
