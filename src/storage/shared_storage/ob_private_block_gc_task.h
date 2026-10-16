/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_PRIVATE_BLOCK_GC_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_PRIVATE_BLOCK_GC_H_

#include "lib/task/ob_timer.h"
#include "storage/shared_storage/ob_block_gc_handler.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "src/storage/shared_storage/ob_block_gc_thread.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
namespace checkpoint
{
class ObTabletGCService;
}

enum ProcessType
{
  GCTabletMetaVersion = 0,
  GCTablet = 1,
  MacroCheck = 2
};

struct ObTabletGCInfo
{
  ObTabletGCInfo()
    : tablet_id_(),
      gc_type_(ProcessType::GCTabletMetaVersion)
  {}
  ObTabletGCInfo(
      const ObTabletID &tablet_id,
      ProcessType gc_type = ProcessType::GCTabletMetaVersion)
    : tablet_id_(tablet_id),
      gc_type_(gc_type)
  {}
  ~ObTabletGCInfo()
  {
    reset();
  }
  void reset()
  {
    tablet_id_.reset();
    gc_type_ = ProcessType::GCTabletMetaVersion;
  }

  bool operator==(const ObTabletGCInfo &rhs) const
  {
    return tablet_id_ == rhs.tablet_id_ && gc_type_ == rhs.gc_type_;
  }

  uint64_t hash() const
  {
    uint64_t hash_val = 0;
    hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
    hash_val = murmurhash(&gc_type_, sizeof(gc_type_), hash_val);
    return hash_val;
  }
  
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }

  TO_STRING_KV(K_(tablet_id), K_(gc_type));

  ObTabletID tablet_id_;
  ProcessType gc_type_;
};

typedef common::hash::ObHashSet<ObTabletGCInfo, common::hash::NoPthreadDefendMode> TabletInfoSet;

template <typename T>
class DoubleQueue
{
public:
  DoubleQueue()
    : lock_(common::ObLatchIds::OB_PRIVATE_BLOCK_GC_DOUBLE_QUEUE_LOCK),
      pos_(0)
  {
    double_queue_[0].set_tenant_id(MTL_ID());
    double_queue_[1].set_tenant_id(MTL_ID());
  }

  ObIArray<T>& drain()
  {
    SpinWLockGuard guard(lock_);
    const int drain_pos = pos_;
    pos_ = (pos_ + 1) % 2;
    double_queue_[pos_].reset();
    return double_queue_[drain_pos];
  }

  int add_item(const T &item)
  {
    int ret = OB_SUCCESS;
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(double_queue_[pos_].push_back(item))) {
      STORAGE_LOG(WARN, "failed to push_back", K(ret), K(item));
    }
    return ret;
  }

  int add_items(const ObIArray<T> &items)
  {
    int ret = OB_SUCCESS;
    SpinWLockGuard guard(lock_);
    const int64_t queue_size = (items.count() + double_queue_[pos_].count()) * sizeof (T);
    if (queue_size > 1000 * 1000 * 1000) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "add items too much", K(ret), K(queue_size), K(items.count()), 
          K(double_queue_[pos_].count()), K(sizeof (T)), K(pos_));
    } else {
      for (int i = 0; i < items.count(); i++) {
        const T &item = items.at(i);
        if (OB_FAIL(double_queue_[pos_].push_back(item))) {
          STORAGE_LOG(WARN, "failed to push_back", K(ret), K(item));
        }
      }
    }
    return ret;
  }
  void reset()
  {
    SpinWLockGuard guard(lock_);
    double_queue_[0].reset();
    double_queue_[1].reset();
    pos_ = 0;
  }

  TO_STRING_KV(K_(pos));
private:
  common::SpinRWLock lock_;
  int pos_;
  ObArray<T> double_queue_[2];
};

class ObPrivateBlockGCThreadGuard;
class ObPrivateBlockGCThread : public ObBlockGCThread<ObPrivateBlockGCThreadGuard>
{
public:
  int init()
  { return ObBlockGCThread<ObPrivateBlockGCThreadGuard>::init(lib::TGDefIDs::ObPrivateBlockGCThread); }
  int start();
  int reload_config();
  virtual ~ObPrivateBlockGCThread() {}
  void handle(void *task);
  static constexpr int64_t DEFAULT_THREAD_NUM = 4;
  static constexpr int64_t MINI_MODE_DEFAULT_THREAD_NUM = 1;
  static constexpr int64_t MAX_BLOCK_GC_TASK_NUM = 1000 * 1000;
private:
  int get_target_thread_count_(int64_t &thread_count);
};

class ObPrivateBlockGCThreadGuard : public ObBlockGCThreadGuard<ObPrivateBlockGCThreadGuard>
{
public:
  ObPrivateBlockGCThreadGuard(
      ObPrivateBlockGCThread &gc_thread,
      ProcessType process_type,
      common::ObArenaAllocator &allocator)
    : ObBlockGCThreadGuard(gc_thread, *this),
      process_type_(process_type),
      allocator_(allocator),
      gc_macro_block_cnt_(0),
      lock_(common::ObLatchIds::OB_PRIVATE_BLOCK_GC_THREAD_GUARD_LOCK)
  {}
  ~ObPrivateBlockGCThreadGuard() {}
  bool is_valid() 
  { return true; }
  common::ObArenaAllocator &get_allocator()
  { return allocator_; }
  void update_gc_macro_block_cnt(const int64_t gc_cnt)
  { ATOMIC_FAA(&gc_macro_block_cnt_, gc_cnt); }
  int64_t get_gc_macro_block_cnt() const
  { return gc_macro_block_cnt_; }
  int update_succ_gc_items(const ObPendingFreeTabletItem &extra_info)
  { 
    SpinWLockGuard guard(lock_);
    return succ_gc_items_.push_back(extra_info); 
  }
  ObArray<ObPendingFreeTabletItem>& get_succ_gc_items()
  { return succ_gc_items_; }
  INHERIT_TO_STRING_KV("ObBlockGCThreadGuard", ObBlockGCThreadGuard<ObPrivateBlockGCThreadGuard>, K_(process_type), K_(gc_macro_block_cnt));

  // todo: add same info in ObPrivateBlockGCHandler
  
  // true: tablet_meta_gc; false: tablet gc
  ProcessType process_type_;
  common::ObArenaAllocator &allocator_;
  int64_t gc_macro_block_cnt_;
  ObArray<ObPendingFreeTabletItem> succ_gc_items_;
  SpinRWLock lock_;
};

class ObLSPrivateBlockGCHandler
{
public:
  static const int64_t PRIVATE_BLOCK_GC_INTERVAL;
  static const int64_t DEFAULT_PRIVATE_TABLET_GC_SAFE_TIME;
  ObLSPrivateBlockGCHandler(ObLS &ls)
    : wait_lock_(common::ObLatchIds::PRIVATE_BLOCK_GC_LOCK),
      gc_tablet_info_queue_(),
      macro_block_check_tablet_info_queue_(),
      next_tablet_gc_ts_(0),
      is_set_stop_(false),
      ls_(ls)
  {}

  // ls operation
  void set_stop() { ATOMIC_STORE(&is_set_stop_, true); }
  void set_start() { ATOMIC_STORE(&is_set_stop_, false); }
  // todo add more check
  bool check_stop() { return ATOMIC_LOAD(&is_set_stop_) == true; }
  bool is_finish() { obsys::ObWLockGuard<> lock(wait_lock_, false); return lock.acquired(); }
  int offline()
  {
    set_stop();
    const int ret = is_finish() ? OB_SUCCESS : OB_EAGAIN;
    STORAGE_LOG(INFO, "block gc handler offline", KR(ret), KPC(this));
    return ret;
  }
  void online()
  {
    set_start();
    STORAGE_LOG(INFO, "block gc handler online", KPC(this));
  }

  // gc info operation
  int report_tablet_id_for_gc_service(const ObTabletGCInfo &tablet_info);

  // gc tablet operation
  void process_tablet_infos_for_macro_block_check();
  static int deduplicate_tablet_info(
      const ObIArray<ObTabletGCInfo> &tablet_infos,
      TabletInfoSet &tablet_info_set);
  void process_tablet_infos_for_tablet_meta_gc();
  void gc_tablets_meta_versions();
  void ls_macro_block_check();
  void gc_tablets();
  void reset()
  {
    gc_tablet_info_queue_.reset();
    macro_block_check_tablet_info_queue_.reset();
    STORAGE_LOG(INFO, "block gc handler reset", KPC(this));
  }

  TO_STRING_KV(K_(gc_tablet_info_queue), K_(macro_block_check_tablet_info_queue), K_(next_tablet_gc_ts), K_(is_set_stop));

  obsys::ObRWLock<> wait_lock_;

private:
  ObIArray<ObTabletGCInfo>& drain_tablet_infos_for_gc_()
  {
    return gc_tablet_info_queue_.drain();
  }

  ObIArray<ObTabletGCInfo>& drain_tablet_infos_for_macro_block_check_()
  {
    return macro_block_check_tablet_info_queue_.drain();
  }

  int gc_tablet_meta_versions_(
      const ObTabletID &tablet_id,
      ObPrivateBlockGCThreadGuard &gc_thread_guard);

  int macro_block_check_(
      const ObTabletID &tablet_id,
      const uint64_t min_macro_block_id,
      const uint64_t max_macro_block_id,
      ObPrivateBlockGCThreadGuard &gc_thread_guard);
  int get_ls_min_macro_block_id_(
      const share::ObLSID &ls_id,
      uint64_t &min_macro_block_id);
private:

  DoubleQueue<ObTabletGCInfo> gc_tablet_info_queue_;
  DoubleQueue<ObTabletGCInfo> macro_block_check_tablet_info_queue_;

  int64_t next_tablet_gc_ts_;
  bool is_set_stop_;
  ObLS &ls_;
};

class ObPrivateBlockGCTask : public common::ObTimerTask
{
public:
  static const int64_t GLOBAL_PRIVATE_BLOCK_GC_INTERVAL;
  ObPrivateBlockGCTask(checkpoint::ObTabletGCService &tablet_gc_service)
    : is_stopped_(false),
      next_loop_tablet_meta_version_gc_ts_(0),
      tablet_gc_service_(tablet_gc_service),
      failed_macro_block_id_queue_(),
      mtl_start_max_block_id_(0),
      observer_start_macro_block_trigger_(false)
  {}
  virtual ~ObPrivateBlockGCTask() {}

  virtual void runTimerTask();
  void loop_check_tablet_version_gc();
  void loop_check_tablet_gc();
  void loop_process_tablet_infos();
  int ls_gc(
    const ObLSItem &ls_item,
    const bool no_delete_tablet = false);
  void loop_check_ls_gc();
  void observer_start_macro_block_check();
  void loop_process_failed_macro_blocks();

  int report_failed_macro_ids(
    ObIArray<blocksstable::MacroBlockId> &block_ids)
  {
    return failed_macro_block_id_queue_.add_items(block_ids);
  }

  void set_is_stopped() { ATOMIC_STORE(&is_stopped_, true); }
  void clear_is_stopped() { ATOMIC_STORE(&is_stopped_, false); }
  bool is_stopped() { return ATOMIC_LOAD(&is_stopped_); }
  uint64_t get_mtl_start_max_block_id() 
  { return mtl_start_max_block_id_; }
  void set_mtl_start_max_block_id(const uint64_t mtl_start_max_block_id) 
  { 
    mtl_start_max_block_id_ = mtl_start_max_block_id; 
    STORAGE_LOG(INFO, "set_mtl_start_max_block_id", KPC(this));
  }
  void set_observer_start_macro_block_id_trigger()
  { 
    ATOMIC_STORE(&observer_start_macro_block_trigger_, true); 
    STORAGE_LOG(INFO, "set_observer_start_macro_block_id_trigger", KPC(this));
  }
  TO_STRING_KV(K_(is_stopped), K_(next_loop_tablet_meta_version_gc_ts), K_(mtl_start_max_block_id), K_(observer_start_macro_block_trigger));
private:
  ObIArray<blocksstable::MacroBlockId>& drain_failed_macro_ids_()
  {
    return failed_macro_block_id_queue_.drain();
  }

private:
  bool is_stopped_;
  int64_t next_loop_tablet_meta_version_gc_ts_;
  checkpoint::ObTabletGCService &tablet_gc_service_;
  DoubleQueue<blocksstable::MacroBlockId> failed_macro_block_id_queue_;
  // mtl init set max_block_id_
  uint64_t mtl_start_max_block_id_;
  // observer start trigger
  bool observer_start_macro_block_trigger_;
};

class ObPrivateBlockGCHandler : public ObBlockGCHandler
{
public:
  // for removed tablet
  ObPrivateBlockGCHandler(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      const int64_t max_tablet_meta_version,
      const int64_t tablet_last_gc_version,
      const int64_t reorganization_scn,
      const GCTabletType gc_type,
      const uint64_t ls_min_block_id,
      const uint64_t ls_max_block_id,
      const ObPendingFreeTabletItem &extra_info,
      const bool &is_set_stop)
    : ObBlockGCHandler(tablet_id, is_set_stop),
      ls_id_(ls_id),
      ls_epoch_(ls_epoch),
      max_tablet_meta_version_(max_tablet_meta_version),
      tablet_last_gc_version_(tablet_last_gc_version),
      reorganization_scn_(reorganization_scn),
      gc_type_(gc_type),
      ls_min_block_id_(ls_min_block_id),
      ls_max_block_id_(ls_max_block_id),
      extra_info_(&extra_info),
      tablet_fingerprint_(0),
      is_transfer_out_deleted_(false)
  {}

  // for gc tablet meta version
  ObPrivateBlockGCHandler(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      const int64_t max_tablet_meta_version,
      const int64_t tablet_last_gc_version,
      const int64_t reorganization_scn,
      const uintptr_t tablet_fingerprint,
      const bool &is_set_stop,
      const bool is_transfer_out_deleted)
    : ObBlockGCHandler(tablet_id, is_set_stop),
      ls_id_(ls_id),
      ls_epoch_(ls_epoch),
      max_tablet_meta_version_(max_tablet_meta_version),
      tablet_last_gc_version_(tablet_last_gc_version),
      reorganization_scn_(reorganization_scn),
      gc_type_(GCTabletType::InvalidType),
      ls_min_block_id_(0),
      ls_max_block_id_(UINT64_MAX),
      extra_info_(NULL),
      tablet_fingerprint_(tablet_fingerprint),
      is_transfer_out_deleted_(is_transfer_out_deleted)
  {}

  // for macro check
  ObPrivateBlockGCHandler(
      const share::ObLSID &ls_id,
      const int64_t ls_epoch,
      const ObTabletID &tablet_id,
      const int64_t max_tablet_meta_version,
      const int64_t tablet_last_gc_version,
      const int64_t reorganization_scn,
      const uint64_t ls_min_block_id,
      const uint64_t ls_max_block_id,
      const uintptr_t tablet_fingerprint,
      const bool &is_set_stop)
    : ObBlockGCHandler(tablet_id, is_set_stop),
      ls_id_(ls_id),
      ls_epoch_(ls_epoch),
      max_tablet_meta_version_(max_tablet_meta_version),
      tablet_last_gc_version_(tablet_last_gc_version),
      reorganization_scn_(reorganization_scn),
      gc_type_(GCTabletType::InvalidType),
      ls_min_block_id_(ls_min_block_id),
      ls_max_block_id_(ls_max_block_id),
      extra_info_(NULL),
      tablet_fingerprint_(tablet_fingerprint),
      is_transfer_out_deleted_(false)
  {}

  int gc_tablet();
  int gc_tablet_meta_versions();
  int macro_block_check();

  /// @brief: for macro check && old tablet version gc
  virtual int list_tablet_meta_version(
    ObIArray<int64_t> &tablet_versions,
    const bool is_macro_check,
    int64_t &gc_end_version);

  /// @brief: for removed tablet
  virtual int list_tablet_meta_version(
    ObIArray<int64_t> &tablet_versions);

  virtual int get_blocks_for_tablet(
    const int64_t tablet_meta_version,
    const bool is_shared,
    ObIArray<blocksstable::MacroBlockId> &block_ids);
  
  virtual int delete_tablet_meta_version(
    int64_t tablet_meta_version);

  virtual int try_delete_tablet_meta_dir();
  virtual int try_delete_tablet_data_dir();

  virtual int get_block_ids_from_dir(
    ObIArray<blocksstable::MacroBlockId> &block_ids);

  virtual int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &collect_check_error_block_op)
  { return OB_SUCCESS; }

  const ObPendingFreeTabletItem* get_extra_info()
  { 
    return extra_info_;
  }
  const share::ObLSID &get_ls_id() { return ls_id_; }

  INHERIT_TO_STRING_KV("ObBlockGCHandler", ObBlockGCHandler, K_(ls_id), K_(ls_epoch), K_(max_tablet_meta_version), 
    K_(tablet_last_gc_version), K_(gc_type), K_(reorganization_scn), K_(ls_max_block_id),
    K_(tablet_fingerprint), K_(is_transfer_out_deleted));

private:
  virtual int update_tablet_last_gc_version_(const int64_t gc_end_version);
  int safe_delete_blocks_for_macro_check_(
      ObIArray<blocksstable::MacroBlockId> &block_ids) override;

private:
  const share::ObLSID ls_id_;
  const int64_t ls_epoch_;
  const int64_t max_tablet_meta_version_;
  const int64_t tablet_last_gc_version_;
  const int64_t reorganization_scn_;
  GCTabletType gc_type_;
  const uint64_t ls_min_block_id_;
  const uint64_t ls_max_block_id_;
  const ObPendingFreeTabletItem *extra_info_;
  const uintptr_t tablet_fingerprint_; // for update tablet last gc version
  const bool is_transfer_out_deleted_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_PRIVATE_BLOCK_GC_H_ */
