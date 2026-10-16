/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_STRUCT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array_serialization.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace blocksstable
{
  class ObBufferReader;
}
namespace storage
{

class ObLSTabletService;

class ObCopyLocalMetaProducer
{
public:
  ObCopyLocalMetaProducer();
  virtual ~ObCopyLocalMetaProducer();
  int init(const ObHACacheJobInfo &job_info,
          const share::ObLSID &ls_id);
  int get_next_macro_metas(common::ObSArray<ObSSMacroPrewarmMeta> &metas);
  int64_t get_cur_idx() const { return cur_idx_; }

private:
  int filter_ls_macro_(const int64_t effective_tablet_id, bool &is_filtered);

private:
  bool is_inited_;
  int64_t start_idx_;
  int64_t end_idx_;
  int64_t cur_idx_;
  ObLSHandle ls_handle_;
  ObLSTabletService *tablet_service_;
};

class ObCopyCachedBlockDataProducer
{
public:
  ObCopyCachedBlockDataProducer();
  virtual ~ObCopyCachedBlockDataProducer();
  int init(const ObHALocalCacheKeys &local_cache_keys);
  int get_next_cached_block_data(ObHAFetchLocalCacheBlockHeader &header,
                                 blocksstable::ObBufferReader &data);

private:
  // macro cache interfaces
  int get_next_macro_block_data(ObHAFetchLocalCacheBlockHeader &header,
                                blocksstable::ObBufferReader &data);
  int prefetch();
  int inner_prefetch(const int64_t handle_idx, bool &is_prefetched);

private:
  static const int64_t MAX_PREFETCH_MACRO_BLOCK_NUM = 2;
  // common variables
  bool is_inited_;
  ObHALocalCacheKeys local_cache_keys_;
  int64_t key_idx_;
  ObArenaAllocator allocator_;
  char *data_buf_;
  int64_t data_buf_size_;

  // variables for macro cache
  blocksstable::ObStorageObjectHandle read_handles_[MAX_PREFETCH_MACRO_BLOCK_NUM];
  int64_t handle_idx_;
  char *read_bufs_[MAX_PREFETCH_MACRO_BLOCK_NUM];
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_HA_PREWARM_STRUCT_H_ */
