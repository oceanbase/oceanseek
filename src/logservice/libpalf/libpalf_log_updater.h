/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIBPALF_LOG_UPDATER_H_
#define OCEANBASE_LIBPALF_LOG_UPDATER_H_

#include "lib/task/ob_timer.h"

namespace oceanbase
{
namespace libpalf
{
class LibPalfLogUpdater: public common::ObTimerTask
{
public:
  LibPalfLogUpdater();
  ~LibPalfLogUpdater() { destroy(); }
  int init();
  void destroy();
  virtual void runTimerTask() override final;
private:
  const static int64_t TASK_INTERVAL = 1000000;
private:
  int tg_id_;
  bool is_inited_;
};

} // end namespace libpalf
} // end namespace oceanbase
#endif