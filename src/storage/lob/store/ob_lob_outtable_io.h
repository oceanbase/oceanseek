/**
 * Copyright (C) 2026 OceanBase
 * This file contains confidential and proprietary information and may not be used or disclosed without authorization.
 */

#ifndef OCEANBASE_STORAGE_LOB_OUTTABLE_IO_H_
#define OCEANBASE_STORAGE_LOB_OUTTABLE_IO_H_

#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "common/ob_tablet_id.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace blocksstable
{
class ObStorageObjectHandle;
}
namespace storage
{

struct ObLobMetaInfo;

class ObLobOutTableIO final
{
public: // the function about macro id
  static int alloc_macro_id(ObLobInsertContext &insert_ctx, blocksstable::MacroBlockId &macro_id);
  static int alloc_macro_ids(ObLobInsertContext &insert_ctx,
                             const int64_t count,
                             ObIArray<blocksstable::MacroBlockId> &macro_id_array);
  static int serialize_macro_id(common::ObIAllocator &allocator,
                                const blocksstable::MacroBlockId &macro_id,
                                common::ObString &buf);
  static int deserialize_macro_id(const common::ObString &buf, blocksstable::MacroBlockId &macro_id);

public: // the function about write and read
  static int write(const blocksstable::MacroBlockId &macro_id, const common::ObString &data);
  static int async_write(const blocksstable::MacroBlockId &macro_id,
                         const common::ObString &data,
                         blocksstable::ObStorageObjectHandle &object_handle);
  // timeout_ts_us is an absolute deadline in microseconds.
  static int async_write(const blocksstable::MacroBlockId &macro_id,
                         const common::ObString &data,
                         const int64_t timeout_ts_us,
                         blocksstable::ObStorageObjectHandle &object_handle);
  // Keep the allocated buffer valid without resetting or reusing allocator until object_handle.wait() returns.
  // Consume data only after wait succeeds and object_handle.get_data_size() equals size.
  static int async_read(const blocksstable::MacroBlockId &macro_id,
                        const int64_t size,
                        common::ObIAllocator &allocator,
                        common::ObString &data,
                        blocksstable::ObStorageObjectHandle &object_handle);
  // The same lifecycle contract applies; timeout_ts_us is an absolute deadline in microseconds.
  static int async_read(const blocksstable::MacroBlockId &macro_id,
                        const int64_t size,
                        const int64_t timeout_ts_us,
                        common::ObIAllocator &allocator,
                        common::ObString &data,
                        blocksstable::ObStorageObjectHandle &object_handle);
  static int enable_outtable_clog(const uint64_t tenant_id, bool &is_write_clog);
  static int write_clog(ObLobWriteContext &ctx,
                        ObLobQueryIter *iter,
                        const common::ObIArray<ObLobMetaInfo> &metas,
                        const bool is_logging);
  static bool is_retryable_error(const int ret);
  static int is_exist(const blocksstable::MacroBlockId &macro_id,
                      const int64_t ls_id,
                      bool &is_exist);
};

// Concurrent OutTable object uploader; each pending upload owns a private copy of the data.
class ObLobConcurrentPuter final
{
public:
  static constexpr int64_t DEFAULT_CAPACITY = 8;
  static constexpr int64_t MAX_WAIT_US = 1 * 1000 * 1000L;

  ObLobConcurrentPuter();
  ~ObLobConcurrentPuter();

  // timeout_ts_us is an absolute deadline in microseconds.
  int init(const int64_t timeout_ts_us, const int64_t chunk_size, const int64_t chunk_count);
  int submit(const blocksstable::MacroBlockId &macro_id, const common::ObString &data);
  int finish();

  TO_STRING_KV(K_(capacity), K_(head), K_(tail), K_(count), K_(chunk_size), K_(timeout_ts_us), K_(result),
               K_(is_inited));

private:
  struct UploadSlot;

  void reset();
  int submit_slot_(UploadSlot &slot);
  int finish_head_();
  bool can_retry_(const int io_ret, const int64_t wait_us) const;
  void abort_all_();

private:
  common::ObArenaAllocator allocator_;
  common::ObFixedArray<UploadSlot, common::ObIAllocator> slots_;
  int64_t chunk_size_;
  int64_t capacity_;
  int64_t head_;
  int64_t tail_;
  int64_t count_;
  int64_t timeout_ts_us_;
  int result_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObLobConcurrentPuter);
};

// Reads one OutTable chunk; returned data remains valid until the next read or reset.
class ObLobOutTableChunkReader final
{
public:
  ObLobOutTableChunkReader();
  ~ObLobOutTableChunkReader();
  void reset();
  int open(ObLobReadContext *read_ctx);
  int read(const ObLobMetaInfo &meta, common::ObString &data);

  TO_STRING_KV(K_(is_inited), KPC_(read_ctx));

private:
  bool is_inited_;
  ObLobReadContext *read_ctx_;
  common::ObArenaAllocator allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObLobOutTableChunkReader);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LOB_OUTTABLE_IO_H_
