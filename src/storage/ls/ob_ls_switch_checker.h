/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_OB_LS_SWITCH_CHECKER_
#define OCEABASE_STORAGE_OB_LS_SWITCH_CHECKER_
#include <stdint.h>

namespace oceanbase
{
namespace storage
{
class ObLS;

class ObLSSwitchChecker
{
public:
  explicit ObLSSwitchChecker(ObLS &ls);
  bool check_online();
  bool double_check_online(bool &is_epoch_match) const;
private:
  ObLS &ls_;
  uint64_t record_switch_epoch_;
};

}
}
#endif
