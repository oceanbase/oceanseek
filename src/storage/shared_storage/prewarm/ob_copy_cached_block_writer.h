/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_COPY_CACHED_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_COPY_CACHED_BLOCK_WRITER_H_

#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/ob_data_buffer.h"
#include "storage/ob_storage_rpc.h"
#include "storage/shared_storage/prewarm/ob_ha_prewarm_rpc_struct.h"

namespace oceanbase
{
namespace storage
{

class ObCopyCachedBlockReader;

class ObCopyCachedBlockWriter
{
public:
  ObCopyCachedBlockWriter();
  virtual ~ObCopyCachedBlockWriter();
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id, ObCopyCachedBlockReader *reader);
  void reset();
  int process();

private:
  int write_cache(const ObHAFetchLocalCacheBlockMeta &meta,
                  const blocksstable::ObBufferReader &data);
  int write_macro_cache(const ObSSMacroPrewarmMeta &macro_prewarm_meta,
                        const blocksstable::ObBufferReader &data);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObCopyCachedBlockReader *reader_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCopyCachedBlockWriter);
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_COPY_CACHED_BLOCK_WRITER_H_ */
