/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "storage/blocksstable/vector_index/ob_ivf_raw_key_store.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "lib/utility/ob_defer.h"
namespace oceanbase
{
namespace storage
{
using namespace common;
OB_SERIALIZE_MEMBER(ObIVFRawOutputKey, vid_, pos_);
ObIVFRawKeyStore::ObIVFRawKeyStore()
  : allocator_(ObMemAttr(MTL_ID(), "IVFRawKeys", ObCtxIds::VECTOR_CTX_ID)),
    shards_(OB_MALLOC_NORMAL_BLOCK_SIZE, ObWrapperAllocator(allocator_)),
    sorter_(allocator_), count_(0), writers_(0), error_(OB_SUCCESS), initialized_(false),
    sealing_(false), sealed_(false)
{}
ObIVFRawKeyStore::~ObIVFRawKeyStore()
{
  clean_up_sorters();
  keys_.reset();
}
void ObIVFRawKeyStore::clean_up_sorters()
{
  // Transferred iterators borrow shard storage. Destroy the destination first.
  sorter_.clean_up();
  for (int64_t i = 0; i < shards_.count(); ++i) {
    Shard *shard = shards_.at(i);
    shard->sorter_.clean_up();
    shard->~Shard();
    allocator_.free(shard);
  }
  shards_.reset();
  free_shards_.destroy();
}
int ObIVFRawKeyStore::init(uint64_t tenant_id, int64_t parallelism)
{
  int ret = OB_SUCCESS;
  if (initialized_) {
    ret = OB_INIT_TWICE;
  } else if (parallelism <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (parallelism > INPUT_MEMORY_LIMIT / ObExternalSortConstant::MIN_MEMORY_LIMIT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("RAW key sorters exceed input memory budget", K(ret), K(parallelism));
  } else if (OB_FAIL(free_shards_.init(parallelism, "IVFRawKeys", tenant_id))) {
    LOG_WARN("failed to initialize RAW key shard queue", K(ret));
  } else {
    const int64_t file_buf_size = ObExternalSortConstant::DEFAULT_FILE_READ_WRITE_BUFFER;
    const int64_t shard_memory_limit = MIN(MAX_SHARD_MEMORY_LIMIT, INPUT_MEMORY_LIMIT / parallelism);
    // Each input stream uses two read buffers during the final merge.
    const int64_t merge_memory_limit = MAX(ObExternalSortConstant::MIN_MEMORY_LIMIT,
        parallelism * file_buf_size * 2);
    const ObMemAttr attr(tenant_id, "IVFRawKeys", ObCtxIds::VECTOR_CTX_ID);
    allocator_.set_attr(attr);
    less_.result_code_ = OB_SUCCESS;
    if (OB_FAIL(shards_.reserve(parallelism))) {
      LOG_WARN("failed to reserve RAW key sorters", K(ret), K(parallelism));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < parallelism; ++i) {
      Shard *shard = OB_NEWx(Shard, &allocator_);
      if (shard == nullptr) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(shards_.push_back(shard))) {
        shard->~Shard();
        allocator_.free(shard);
      } else {
        shard->allocator_.set_attr(attr);
        ret = shard->sorter_.init(shard_memory_limit, file_buf_size, 0, tenant_id, &shard->less_);
        if (OB_SUCC(ret)) { ret = free_shards_.push(shard); }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(sorter_.init(merge_memory_limit, file_buf_size, 0, tenant_id, &less_))) {
      LOG_WARN("failed to initialize RAW key merge sorter", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(keys_.init(1, tenant_id, ObCtxIds::VECTOR_CTX_ID, "IVFRawKeys"))) {
      LOG_WARN("failed to initialize sorted RAW key store", K(ret));
    }
    if (OB_SUCC(ret)) {
      initialized_ = true;
      LOG_INFO("initialized IVF RAW key sorters", K(parallelism), K(shard_memory_limit), K(merge_memory_limit));
    } else {
      clean_up_sorters();
      keys_.reset();
    }
  }
  return ret;
}
int ObIVFRawKeyStore::append(const share::ObVectorArray<int64_t> &positions, const common::ObIArray<uint64_t> &vids)
{
  if (!initialized_ || positions.count() != vids.count()) { return OB_STATE_NOT_MATCH; }
  ATOMIC_INC(&writers_);
  DEFER(ATOMIC_DEC(&writers_));
  int ret = OB_SUCCESS;
  void *entry = nullptr;
  if (ATOMIC_LOAD(&sealing_) || ATOMIC_LOAD(&sealed_)) { return OB_STATE_NOT_MATCH; }
  while (OB_SUCC(ret) && entry == nullptr) {
    if (OB_SUCCESS != (ret = ATOMIC_LOAD(&error_))) {
    } else if (OB_ENTRY_NOT_EXIST == (ret = free_shards_.pop(entry, 100000))) {
      ret = share::dag_yield();
    }
  }
  if (entry != nullptr) {
    Shard &shard = *static_cast<Shard *>(entry);
    int64_t added = 0;
    ret = ATOMIC_LOAD(&error_);
    for (int64_t i = 0; OB_SUCC(ret) && i < positions.count(); ++i) {
      if (positions.at(i) < 0) { ret = OB_INVALID_ARGUMENT;
      } else if (OB_FAIL(shard.sorter_.add_item(ObIVFRawOutputKey(vids.at(i), positions.at(i))))) {
        LOG_WARN("failed to append IVF RAW output key", K(ret), K(i));
      } else { ++added; }
      if (OB_SUCC(ret) && (i + 1) % 4096 == 0) { ret = share::dag_yield(); }
    }
    ATOMIC_AAF(&count_, added);
    if (OB_FAIL(ret)) { (void)ATOMIC_BCAS(&error_, OB_SUCCESS, ret); }
    const int release_ret = free_shards_.push(entry);
    if (release_ret != OB_SUCCESS) { (void)ATOMIC_BCAS(&error_, OB_SUCCESS, release_ret); }
    if (OB_SUCC(ret)) { ret = ATOMIC_LOAD(&error_); }
  }
  return ret;
}
int ObIVFRawKeyStore::seal(int64_t expected_count)
{
  if (!initialized_ || !ATOMIC_BCAS(&sealing_, false, true)) { return OB_STATE_NOT_MATCH; }
  DEFER(ATOMIC_STORE(&sealing_, false));
  if (ATOMIC_LOAD(&writers_) != 0) { return OB_STATE_NOT_MATCH; }
  if (ATOMIC_LOAD(&sealed_)) { return expected_count == count_ ? OB_SUCCESS : OB_STATE_NOT_MATCH; }
  int ret = ATOMIC_LOAD(&error_);
  if (OB_FAIL(ret)) { return ret; }
  if (count_ != expected_count) { return OB_STATE_NOT_MATCH; }
  const int64_t start = ObTimeUtility::current_time();
  const int64_t sorter_count = shards_.count();
  // The task barrier has drained producers. Keep source storage alive until
  // the existing external-sort merger has consumed its borrowed iterators.
  for (int64_t i = 0; OB_SUCC(ret) && i < sorter_count; ++i) {
    if (OB_FAIL(shards_.at(i)->sorter_.do_sort(false))) {
      LOG_WARN("failed to finish RAW key shard", K(ret), K(i));
    } else if (OB_FAIL(shards_.at(i)->sorter_.transfer_final_sorted_fragment_iter(sorter_))) {
      LOG_WARN("failed to transfer sorted RAW key shard", K(ret), K(i));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(sorter_.do_sort(true))) {
    LOG_WARN("failed to merge RAW key fragments", K(ret));
  }
  const ObIVFRawOutputKey *key = nullptr;
  uint64_t previous = 0;
  int64_t written = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(sorter_.get_next_item(key))) {
      if (ret == OB_ITER_END) { ret = OB_SUCCESS; break; }
    } else if (key == nullptr || key->pos_ < 0 || key->pos_ >= expected_count
        || (written > 0 && previous >= key->vid_)) {
      ret = OB_INVALID_DATA;
    } else {
      ObSEArray<ObDatum, 2> row;
      ObDatum vid(reinterpret_cast<const char *>(&key->vid_), sizeof(key->vid_), false);
      ObDatum pos(reinterpret_cast<const char *>(&key->pos_), sizeof(key->pos_), false);
      if (OB_FAIL(row.push_back(vid)) || OB_FAIL(row.push_back(pos)) || OB_FAIL(keys_.add_row(row))) {
      } else { previous = key->vid_; ++written; }
      if (OB_SUCC(ret) && written % 4096 == 0) { ret = share::dag_yield(); }
    }
  }
  if (OB_SUCC(ret) && written != expected_count) { ret = OB_INVALID_DATA; }
  if (OB_SUCC(ret) && OB_FAIL(keys_.dump(true))) {
  } else if (OB_SUCC(ret) && OB_FAIL(keys_.finish_add_row())) {
  }
  clean_up_sorters();
  if (OB_FAIL(ret)) { keys_.reset(); }
  ATOMIC_STORE(&error_, ret);
  if (OB_SUCC(ret)) { ATOMIC_STORE(&sealed_, true); }
  if (OB_SUCC(ret)) {
    LOG_INFO("prepared IVF PosID RAW output keys", K(count_), "file_bytes", keys_.get_file_size(),
        "shards", sorter_count, "elapsed_us", ObTimeUtility::current_time() - start);
  }
  return ret;
}
int ObIVFRawKeyStore::read(
    int64_t begin, int64_t end, common::ObIArray<uint64_t> &positions, common::ObIArray<uint64_t> &vids)
{
  int ret = OB_SUCCESS;
  positions.reuse(); vids.reuse();
  if (!ATOMIC_LOAD(&sealed_) || begin < 0 || end <= begin || end > count_) { ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(positions.reserve(end - begin)) || OB_FAIL(vids.reserve(end - begin))) {
  } else {
    sql::ObRADatumStore::Reader reader(keys_);
    for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
      const sql::ObRADatumStore::StoredRow *row = nullptr;
      uint64_t vid = 0, pos = 0;
      if (OB_FAIL(reader.get_row(i, row))) {
      } else if (row == nullptr || row->cnt_ != 2 || row->cells()[0].len_ != 8 || row->cells()[1].len_ != 8) {
        ret = OB_INVALID_DATA;
      } else {
        MEMCPY(&vid, row->cells()[0].get_string().ptr(), sizeof(vid));
        MEMCPY(&pos, row->cells()[1].get_string().ptr(), sizeof(pos));
        if (OB_FAIL(positions.push_back(pos)) || OB_FAIL(vids.push_back(vid))) {}
      }
    }
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase
