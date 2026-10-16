/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_IO_HELPER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_IO_HELPER_H_

#include <stdint.h>
#include "share/io/ob_io_define.h"

namespace oceanbase
{
namespace storage
{

class ObSSMicroCacheIOHelper
{
public:
  static int async_read_block(const int64_t offset,
                              const int64_t size,
                              char *buf,
                              ObSSPhyBlockHandle &phy_block_handle,
                              common::ObIOHandle &io_handle);
  static int async_write_block(const int64_t offset,
                               const int64_t size,
                               const char *buf,
                               ObSSPhyBlockHandle &phy_block_handle,
                               common::ObIOHandle &io_handle);
  static int read_block(const int64_t offset,
                        const int64_t size,
                        char *buf,
                        ObSSPhyBlockHandle &phy_block_handle);
  static int write_block(const int64_t offset,
                         const int64_t size,
                         const char *buf,
                         ObSSPhyBlockHandle &phy_block_handle);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_IO_HELPER_H_ */
