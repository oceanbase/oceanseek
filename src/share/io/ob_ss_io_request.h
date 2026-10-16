/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_SHARED_STORAGE_IO_OB_IO_REQUEST_H_
#define OCEANBASE_SHARE_SHARED_STORAGE_IO_OB_IO_REQUEST_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/io/ob_io_define.h"

namespace oceanbase
{
namespace common
{

class ObSSIORequest : public ObIORequest
{
public:
  ObSSIORequest();
  virtual ~ObSSIORequest();
  virtual void destroy() override;
  virtual void reset() override;
  INHERIT_TO_STRING_KV("ObIORequest", ObIORequest, K_(fd_cache_handle));

private:
  void try_close_device_and_fd();
  virtual int set_fd_cache_handle(const ObIOInfo &info) override;

private:
  storage::ObSSFdCacheHandle fd_cache_handle_; // avoid fd cache to be evicted when io has not finished
};

} /* namespace common */
} /* namespace oceanbase */

#endif /* OCEANBASE_SHARE_SHARED_STORAGE_IO_OB_IO_REQUEST_H_ */
