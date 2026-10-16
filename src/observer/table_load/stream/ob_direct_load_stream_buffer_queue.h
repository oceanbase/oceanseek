/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/container/ob_array.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/lock/ob_thread_cond.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace observer
{

// BufferQueue 状态只描述本地 queue 生命周期，不等价于 stream 元数据终态。
enum class ObDirectLoadStreamBufferQueueState
{
  INVALID = 0,
  OPEN,
  CLOSE_REQUESTED,
  SEALED,
  FAILED,
  REMOVED,
};

struct ObDirectLoadStreamBufferQueueKey
{
public:
  ObDirectLoadStreamBufferQueueKey();
  ObDirectLoadStreamBufferQueueKey(const int64_t stream_id, const uint64_t queue_id);
  void reset();
  bool is_valid() const;
  bool operator==(const ObDirectLoadStreamBufferQueueKey &other) const;
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  TO_STRING_KV(K_(stream_id), K_(queue_id));
public:
  // stream_id 必须大于 0；queue_id=0 合法。
  int64_t stream_id_;
  uint64_t queue_id_;
};

class ObIDirectLoadStreamQueueStore
{
public:
  virtual ~ObIDirectLoadStreamQueueStore() = default;
  virtual int init(const uint64_t tenant_id) = 0;
  virtual int append(const common::ObString &chunk, int64_t &offset) = 0;
  virtual int read(const int64_t offset, const int64_t size,
                   common::ObIAllocator &allocator, common::ObString &chunk) = 0;
  virtual int remove() = 0;
};

// 仅 focused 单测提供 store factory；生产路径始终使用 tmp file。
class ObIDirectLoadStreamQueueStoreFactory
{
public:
  virtual ~ObIDirectLoadStreamQueueStoreFactory() = default;
  virtual int create_store(ObIDirectLoadStreamQueueStore *&store) = 0;
};

class ObDirectLoadStreamTmpFileQueueStore final : public ObIDirectLoadStreamQueueStore
{
public:
  ObDirectLoadStreamTmpFileQueueStore();
  ~ObDirectLoadStreamTmpFileQueueStore() override;
  int init(const uint64_t tenant_id) override;
  int append(const common::ObString &chunk, int64_t &offset) override;
  int read(const int64_t offset, const int64_t size,
           common::ObIAllocator &allocator, common::ObString &chunk) override;
  int remove() override;
  TO_STRING_KV(K_(tenant_id), K_(dir_id), K_(fd), K_(next_offset), K_(is_inited));
private:
  uint64_t tenant_id_;
  int64_t dir_id_;
  int64_t fd_;
  int64_t next_offset_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamTmpFileQueueStore);
};

class ObDirectLoadStreamBufferQueue;
class ObDirectLoadStreamAppendHandle
{
public:
  ObDirectLoadStreamAppendHandle();
  ~ObDirectLoadStreamAppendHandle();
  bool is_valid() const;
  TO_STRING_KV(KP_(queue));
private:
  friend class ObDirectLoadStreamBufferQueue;
  void reset();
  ObDirectLoadStreamBufferQueue *queue_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamAppendHandle);
};

class ObDirectLoadStreamBufferQueue
{
public:
  ObDirectLoadStreamBufferQueue();
  ~ObDirectLoadStreamBufferQueue() = default;
  int init(const uint64_t tenant_id, const ObDirectLoadStreamBufferQueueKey &key);
  // focused 单测必须显式注入 mock；生产路径禁止调用本接口。
  int init_with_mock_store_for_test(const uint64_t tenant_id,
                                    const ObDirectLoadStreamBufferQueueKey &key,
                                    ObIDirectLoadStreamQueueStore &mock_store);
  ObDirectLoadStreamBufferQueueState get_state() const;
  int get_first_error(int &first_error) const;
  int begin_append(ObDirectLoadStreamAppendHandle &handle);
  int append_chunk(const ObDirectLoadStreamAppendHandle &handle,
                   const common::ObString &chunk);
  int end_append(ObDirectLoadStreamAppendHandle &handle);
  // 调用返回前，调用方必须始终持有有效的 manager handle。
  int get_next_chunk(common::ObIAllocator &allocator, common::ObString &chunk);
  // 只有 reader 完整消费最后一个 chunk 并再次观察到正常 EOF 才返回成功。
  int check_reader_drain_completed() const;
  int request_close();
  // fatal_ret_ 采用 first-write-wins；本接口只返回调用状态，不返回已保存的首错。
  // 调用方如需首错，应在持有 manager handle 时另行查询 queue 错误状态。
  // OB_ITER_END 会正规化为 OB_ERR_UNEXPECTED，避免 FAILED 被下游解释为正常 EOF。
  int fail(const int error_code);
  // APPEND 在 begin_append() 之前发生错误时调用。
  // 仅将 OPEN queue 标记为 FAILED；关闭中、已关闭或已删除的 queue 保持原状态。
  int fail_if_open(const int error_code);
  int cleanup();
  TO_STRING_KV(K_(tenant_id), K_(key));
private:
  struct ChunkMeta
  {
  public:
    ChunkMeta();
    void reset();
    bool is_valid() const;
    TO_STRING_KV(K_(offset), K_(size));
  public:
    int64_t offset_;
    int64_t size_;
  };

  friend class ObDirectLoadStreamBufferQueueHandle;
  int init_(const uint64_t tenant_id, const ObDirectLoadStreamBufferQueueKey &key,
            ObIDirectLoadStreamQueueStore &store);
  // 调用方已持有 cond_；首次失败时记录有效错误并转 FAILED，后续保留首错，最后广播。
  // OB_SUCCESS/OB_ITER_END 正规化为 OB_ERR_UNEXPECTED。
  void mark_failed_preserving_first_error_without_lock_(const int error_code);
  int64_t get_ready_chunk_count_without_lock_() const;
  void compact_ready_chunks_without_lock_();
  int acquire_ref();
  int acquire_ref_from_handle();
  void release_ref();
private:
  uint64_t tenant_id_; // init 设置，初始化后只读。
  ObDirectLoadStreamBufferQueueKey key_; // init 设置，初始化后只读。
  ObDirectLoadStreamBufferQueueState state_; // 由 cond_ 保护。
  int64_t ref_count_; // 由 cond_ 保护。
  int64_t active_appends_; // 由 cond_ 保护，按 APPEND statement 登记。
  int fatal_ret_; // 由 cond_ 保护，保留第一个致命错误。
  bool reader_observed_eof_; // 由 cond_ 保护，证明 consumer 正常读到 SEALED queue EOF。
  bool is_inited_; // Queue 生命周期来源，仅在单线程 init 阶段修改。
  mutable common::ObThreadCond cond_; // 保护 queue 运行态并承载 wait/broadcast。
  int64_t ready_chunk_head_; // 由 cond_ 保护。
  // 由 cond_ 保护，记录已写入 store 的 chunk meta。
  common::ObArray<ChunkMeta> ready_chunks_;
  ObDirectLoadStreamTmpFileQueueStore tmp_file_store_;
  // 生产路径指向 tmp_file_store_；focused 单测可显式注入 mock。
  ObIDirectLoadStreamQueueStore *store_;
};

class ObDirectLoadStreamBufferQueueHandle
{
public:
  ObDirectLoadStreamBufferQueueHandle();
  ~ObDirectLoadStreamBufferQueueHandle();
  int assign(const ObDirectLoadStreamBufferQueueHandle &other);
  void reset();
  ObDirectLoadStreamBufferQueue *get_queue() const;
  bool is_valid() const;
  TO_STRING_KV(KP_(queue));
private:
  friend class ObDirectLoadStreamBufferQueueManager;
  int set_queue(ObDirectLoadStreamBufferQueue *queue);
  ObDirectLoadStreamBufferQueue *queue_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamBufferQueueHandle);
};

}  // namespace observer
}  // namespace oceanbase
