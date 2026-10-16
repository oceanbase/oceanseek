/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_COPY_CACHED_BLOCK_READER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_COPY_CACHED_BLOCK_READER_H_

#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/ob_storage_rpc.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"

namespace oceanbase
{
namespace storage
{

class ObCopyCachedBlockReader
{
public:
  ObCopyCachedBlockReader();
  virtual ~ObCopyCachedBlockReader();
  int init(const ObStorageHASrcInfo &src_info,
           const ObHAFetchLocalCacheBlockArg &arg,
           obrpc::ObStorageRpcProxy &svr_rpc_proxy,
           common::ObInOutBandwidthThrottle *bandwidth_throttle);
  int get_next_cached_block(ObHAFetchLocalCacheBlockMeta &meta,
                            blocksstable::ObBufferReader &data);

private:
  int alloc_buffers();
  int fetch_next_buffer_if_need();
  int fetch_next_buffer();
  int extract_meta_and_data(const ObHAFetchLocalCacheBlockHeader &header,
                            ObHAFetchLocalCacheBlockMeta &meta,
                            blocksstable::ObBufferReader &data);

private:
  bool is_inited_;
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_HA_FETCH_LOCAL_CACHE_BLOCK> handle_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  blocksstable::ObBufferReader data_buffer_; // to assemble micro/macro meta and data
  common::ObDataBuffer rpc_buffer_;
  int64_t rpc_buffer_parse_pos_;
  common::ObArenaAllocator allocator_;
  int64_t last_send_time_;
  int64_t data_size_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyCachedBlockReader);
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_COPY_CACHED_BLOCK_READER_H_ */
