/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_RAW_KEY_STORE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_RAW_KEY_STORE_H_
#include "storage/ob_parallel_external_sort.h"
#include "sql/engine/basic/ob_ra_datum_store.h"
#include "lib/queue/ob_lighty_queue.h"
#include "storage/blocksstable/vector_index/ob_ivf_vector_ids.h"
#include "lib/allocator/ob_malloc.h"
namespace oceanbase
{
namespace storage
{
struct ObIVFRawOutputKey
{
  OB_UNIS_VERSION(1);
public:
  ObIVFRawOutputKey() : vid_(0), pos_(-1) {}
  ObIVFRawOutputKey(uint64_t vid, int64_t pos) : vid_(vid), pos_(pos) {}
  void reset() { vid_ = 0; pos_ = -1; }
  int64_t get_deep_copy_size() const { return 0; }
  int deep_copy(const ObIVFRawOutputKey &other, char *buf, int64_t len, int64_t &pos)
  { UNUSEDx(buf, len, pos); *this = other; return OB_SUCCESS; }
  uint64_t vid_;
  int64_t pos_;
  TO_STRING_KV(K_(vid), K_(pos));
};
struct ObIVFRawOutputKeyLess
{
  ObIVFRawOutputKeyLess() : result_code_(OB_SUCCESS) {}
  bool operator()(const ObIVFRawOutputKey *a, const ObIVFRawOutputKey *b)
  {
    if (a == nullptr || b == nullptr) { result_code_ = OB_INVALID_ARGUMENT; return false; }
    return a->vid_ < b->vid_;
  }
  int result_code_;
};
// Created only for unordered input. SOAR supplies one key per source row while
// it already owns the vector batch. RAW tasks read disjoint sorted ordinal
// ranges through private readers; no N-element key array is materialized.
class ObIVFRawKeyStore
{
public:
  ObIVFRawKeyStore();
  ~ObIVFRawKeyStore();
  int init(uint64_t tenant_id, int64_t parallelism);
  int append(const share::ObVectorArray<int64_t> &positions, const common::ObIArray<uint64_t> &vids);
  int seal(int64_t expected_count);
  int read(int64_t begin, int64_t end, common::ObIArray<uint64_t> &positions, common::ObIArray<uint64_t> &vids);

private:
  // Spill thresholds, not reservations; pointer arrays and I/O buffers are extra.
  static constexpr int64_t INPUT_MEMORY_LIMIT = 8L << 30;
  static constexpr int64_t MAX_SHARD_MEMORY_LIMIT = 256L << 20;
  struct Shard
  {
    Shard()
      : allocator_(common::ObMemAttr(MTL_ID(), "IVFRawKeys", common::ObCtxIds::VECTOR_CTX_ID)),
        sorter_(allocator_) {}
    TO_STRING_KV(K_(sorter));
    common::ObMalloc allocator_;
    ObIVFRawOutputKeyLess less_;
    ObExternalSort<ObIVFRawOutputKey, ObIVFRawOutputKeyLess> sorter_;
  };
  void clean_up_sorters();
  // A queue entry grants exclusive access to one sorter. Sorting and spill
  // I/O never run inside the queue's synchronization.
  common::LightyQueue free_shards_;
  common::ObMalloc allocator_;
  common::ObArray<Shard *, common::ObWrapperAllocator> shards_;
  ObIVFRawOutputKeyLess less_;
  ObExternalSort<ObIVFRawOutputKey, ObIVFRawOutputKeyLess> sorter_;
  sql::ObRADatumStore keys_;
  int64_t count_;
  int64_t writers_;
  int error_;
  bool initialized_;
  bool sealing_;
  bool sealed_;
};
} // namespace storage
} // namespace oceanbase
#endif
