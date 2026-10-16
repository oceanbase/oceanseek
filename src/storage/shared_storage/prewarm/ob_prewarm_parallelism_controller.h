/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_PREWARM_PARALLELISM_CONTROLLER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_PREWARM_PARALLELISM_CONTROLLER_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{

class ObPrewarmParController
{
public:
  ObPrewarmParController() : consecutive_succ_cnt_(0) {}
  virtual ~ObPrewarmParController() {}

protected:
  void try_inc_parallelism(const int64_t max_parallelism, int64_t &parallelism);
  void try_dec_parallelism(const int64_t min_parallelism, int64_t &parallelism);
  void try_adjust_parallelism(const bool has_failed_io,
                              const int64_t min_parallelism,
                              const int64_t max_parallelism,
                              int64_t &parallelism);

protected:
  int64_t consecutive_succ_cnt_;
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_PREWARM_PARALLELISM_CONTROLLER_H_ */
