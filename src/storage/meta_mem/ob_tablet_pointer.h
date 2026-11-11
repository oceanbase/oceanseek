/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_POINTER
#define OCEANBASE_STORAGE_OB_TABLET_POINTER

#include "lib/lock/ob_mutex.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tablet_attr.h"
#include "storage/multi_data_source/runtime_utility/mds_lock.h"
#include "storage/tablet/ob_tablet_ddl_info.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/ob_i_memtable_mgr.h"
#include "storage/ob_protected_memtable_mgr_handle.h"
#ifdef OB_BUILD_SHARED_STORAGE
// TODO(remove-old-gc-code): #include "storage/incremental/garbage_collector/ob_ss_garbage_collector_rpc.h"
#endif

namespace oceanbase
{
namespace storage
{
class ObTablet;
class ObTabletDDLKvMgr;
class ObTabletPointer;
class ObTabletResidentInfo;
typedef ObMetaObjGuard<ObTabletDDLKvMgr> ObDDLKvMgrHandle;

class ObTabletBasePointer
{
public:
  virtual ~ObTabletBasePointer() = default;
  OB_INLINE const ObMetaDiskAddr &get_addr() const { return phy_addr_; }
  bool is_in_memory() const;
  void set_addr_without_reset_obj(const ObMetaDiskAddr &addr);
  void set_addr_with_reset_obj(const ObMetaDiskAddr &addr);

  virtual bool is_tablet_status_written() const = 0;
  virtual void set_obj(const ObTabletHandle &guard) = 0;
  virtual void set_tablet_status_written() = 0;
  virtual share::SCN get_notify_ss_change_scn() const = 0;
  virtual share::SCN get_ss_change_scn() const = 0;
  virtual share::SCN get_ss_minor_version() const = 0;
  virtual void set_ss_minor_version(const share::SCN &ss_minor_version) = 0;
  virtual ObLS *get_ls() const = 0;

  VIRTUAL_TO_STRING_KV(K_(phy_addr), K_(obj));

protected:
  ObTabletBasePointer();
  virtual void reset_obj() = 0;
  virtual void reset() = 0;

protected:
  ObMetaDiskAddr phy_addr_; // 56B
  ObMetaObj<ObTablet> obj_; // 40B
};

class ObSSTabletDummyPointer final : public ObTabletBasePointer
{
private:
  #define ONLY_LOG(INFO) int ret = OB_SUCCESS; STORAGE_LOG(ERROR, INFO)
  #define LOG_NOT_MY_DUTY_VOID { ONLY_LOG("NOT MY DUTY"); }
public:
  ObSSTabletDummyPointer() = default;
  virtual ~ObSSTabletDummyPointer();

  virtual void set_obj(const ObTabletHandle &guard) override;

  virtual void set_tablet_status_written() override LOG_NOT_MY_DUTY_VOID;
  virtual bool is_tablet_status_written() const override { LOG_NOT_MY_DUTY_VOID; return false; }
  virtual share::SCN get_ss_change_scn() const override { LOG_NOT_MY_DUTY_VOID; return share::SCN::invalid_scn(); }
  virtual share::SCN get_notify_ss_change_scn() const override { LOG_NOT_MY_DUTY_VOID; return share::SCN::invalid_scn(); }
  virtual share::SCN get_ss_minor_version() const override { LOG_NOT_MY_DUTY_VOID; return share::SCN::invalid_scn(); }
  virtual void set_ss_minor_version(const share::SCN &ss_minor_version) override { LOG_NOT_MY_DUTY_VOID; }
  virtual ObLS *get_ls() const override { ONLY_LOG("NOT MY DUTY"); return nullptr; }

private:
  virtual void reset_obj() override;
  virtual void reset() override;
};

class ObTabletPointer final : public ObTabletBasePointer
{
  friend class ObTablet;
  friend class ObTenantMetaMemMgr;
  friend class ObTabletPointerMap;
public:
  static const int64_t META_VERSION_ABORT_INTERNVAL = 500;

public: // self_function
  ObTabletPointer();
  ObTabletPointer(const ObLSHandle &ls_handle,
      const ObMemtableMgrHandle &memtable_mgr_handle);
  virtual ~ObTabletPointer();

public:
  // 必须在 t3m bucket lock 保护下调用。
  int scan_all_tablets_on_chain(const ObFunction<int(ObTablet &)> &op, int64_t &tablet_cnt);
  void set_auto_part_size(const int64_t auto_part_size);
  int64_t get_auto_part_size() const;
  int deep_copy(char *buf, const int64_t buf_len, ObTabletPointer *&value) const;
  int64_t get_deep_copy_size() const;
  virtual ObLS *get_ls() const override;
  virtual void set_tablet_status_written() override;
  virtual bool is_tablet_status_written() const override;
  virtual share::SCN get_ss_change_scn() const override { return attr_.ss_change_scn_.atomic_load(); }
  virtual share::SCN get_notify_ss_change_scn() const override { return attr_.notify_ss_change_scn_.atomic_load(); }
  virtual share::SCN get_ss_minor_version() const override { return attr_.ss_minor_version_.atomic_load(); }
  virtual void set_ss_minor_version(const share::SCN &ss_minor_version) override { attr_.ss_minor_version_.atomic_store(ss_minor_version); }
  int64_t get_next_meta_version() const { return next_meta_version_; }
  int64_t get_last_match_meta_version() const { return attr_.last_match_tablet_meta_version_; }
  /// NOTE: hold T3M::bucket_lock_
  int64_t get_last_gc_version() const { return last_gc_version_; }
  int advance_notify_ss_change_scn(
      const ObTabletID &tablet_id,
      const share::SCN &scn);
  bool need_remove_from_flying() const;
  bool get_initial_state() const;
  ObTabletResidentInfo get_tablet_resident_info(const ObTabletMapKey &key) const;
  bool is_empty_shell() const { return attr_.is_empty_shell(); }
  int64_t get_async_index_checked_leader_epoch() const
  { return ATOMIC_LOAD(&async_index_checked_leader_epoch_); }
  void set_async_index_checked_leader_epoch(const int64_t epoch)
  { ATOMIC_STORE(&async_index_checked_leader_epoch_, epoch); }

  // do not KPC memtable_mgr, may dead lock
  VIRTUAL_TO_STRING_KV(K_(phy_addr), K_(obj), K_(ls_handle), K_(ddl_kv_mgr_handle), K_(attr),
      K_(protected_memtable_mgr_handle), K_(ddl_info), KP_(old_version_chain), K_(flying),
      K_(next_meta_version), K_(last_gc_version), K_(async_index_checked_leader_epoch));
private:
  int get_in_memory_obj(ObTabletHandle &guard);
  int get_attr_for_obj(ObTablet *t);
  int release_memtable_and_mds_table_for_ls_offline(const ObTabletID &tablet_id);
  // load and dump interface
  int acquire_obj(ObTablet *&t);
  int read_from_disk(
      const bool is_full_load,
      common::ObArenaAllocator &allocator,
      char *&r_buf,
      int64_t &r_len,
      ObMetaDiskAddr &addr);
  int deserialize(
      common::ObArenaAllocator &allocator,
      const char *buf,
      const int64_t buf_len,
      ObTablet *t);
  int deserialize(
      const char *buf,
      const int64_t buf_len,
      ObTablet *t);
  int hook_obj(ObTablet *&t, ObTabletHandle &guard);
  int release_obj(ObTablet *&t);
  int dump_meta_obj(ObTabletHandle &guard, void *&free_obj);
  int create_ddl_kv_mgr(const share::ObLSID &ls_id,
                        const ObTabletID &tablet_id,
                        ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                        bool &is_created);
  int set_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int remove_ddl_kv_mgr(const ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int set_tablet_attr(const ObTabletAttr &attr);
  bool is_attr_valid() const { return attr_.is_valid(); }
private:
  void set_obj_pool(ObITenantMetaObjPool &obj_pool);
  bool is_old_version_chain_empty() const { return OB_ISNULL(old_version_chain_); }
  void reset_tablet_status_written();
  share::SCN get_tablet_max_checkpoint_scn() const { return attr_.tablet_max_checkpoint_scn_; }
  void get_ddl_kv_mgr(ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  virtual void reset_obj() override;
  virtual void reset() override;
  virtual void set_obj(const ObTabletHandle &guard) override;
  void get_obj(ObTabletHandle &guard);

  // virtual int wash_obj() override;
  int add_tablet_to_old_version_chain(ObTablet *tablet);
  int remove_tablet_from_old_version_chain(ObTablet *tablet);
  void set_flying(const bool status) { flying_ = status; }
  bool is_flying() const { return flying_; }
  bool need_push_to_flying_() const;
  ObProtectedMemtableMgrHandle* get_protected_memtable_mgr_handle() { return &protected_memtable_mgr_handle_; }
  // do not rely on thess interface
  ObTablet* get_old_version_chain_() const { return old_version_chain_; }
  ObTabletDDLInfo* get_ddl_info_() { return &ddl_info_; }
  int try_alloc_meta_version(int64_t &version);
  int init_next_meta_version();
  /// NOTE: hold T3M::bucket_lock_
  void update_last_gc_version(const int64_t new_gc_version);
  void set_next_meta_version(const int64_t next_meta_version);

private:
  ObLSHandle ls_handle_; // 24B
  ObDDLKvMgrHandle ddl_kv_mgr_handle_; // 48B
  ObProtectedMemtableMgrHandle protected_memtable_mgr_handle_; // 32B
  ObTabletDDLInfo ddl_info_; // 32B
  bool flying_; // 1B
  ObByteLock ddl_kv_mgr_lock_; // 1B
  /// DDL / transfer path: whether tablet-status row has been written (legacy name).
  bool tablet_mds_status_written_;
  ObTablet *old_version_chain_; // 8B
  // the RW operations of tablet_attr are protected by lock guard of tablet_map_
  ObTabletAttr attr_; // 120B
  int64_t next_meta_version_; // 8B(for SS private tablet)
  int64_t last_gc_version_; // 8B(for SS private block gc)
  // Pure memory: survive tablet replacement/washing; LS role changes invalidate the epoch.
  int64_t async_index_checked_leader_epoch_; // 8B
  DISALLOW_COPY_AND_ASSIGN(ObTabletPointer);
};


class ObITabletFilterOp
{
public:
  ObITabletFilterOp()
    :skip_cnt_(0), total_cnt_(0), not_in_mem_cnt_(0), invalid_attr_cnt_(0)
  {}
  virtual ~ObITabletFilterOp();
  int operator()(const ObTabletResidentInfo &info, bool &is_skipped);
  virtual int do_filter(const ObTabletResidentInfo &info, bool &is_skipped) = 0;
  void inc_not_in_memory_cnt() { ++total_cnt_; ++not_in_mem_cnt_; }
  void inc_invalid_attr_cnt() { ++total_cnt_; ++invalid_attr_cnt_; }
private:
  int64_t skip_cnt_;
  int64_t total_cnt_;
  int64_t not_in_mem_cnt_;
  int64_t invalid_attr_cnt_;
  static int64_t total_skip_cnt_;
  static int64_t total_tablet_cnt_;
  static int64_t not_in_mem_tablet_cnt_;
  static int64_t invalid_attr_tablet_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObITabletFilterOp);
};

struct ScanAllVersionTabletsOp
{
#ifdef OB_BUILD_SHARED_STORAGE
  struct GetMinTabletRelyInfo
  {
  public:
    GetMinTabletRelyInfo(share::SCN &min_dep_meta_scn,
                         int64_t &min_major_snapshot,
                         share::SCN &clog_checkpoint_scn,
                         share::SCN &orig_clog_checkpoint_scn,
                         int64_t &multi_version_start,
                         bool &has_memtable,
                         const bool collect_lob_rely_info);
    virtual ~GetMinTabletRelyInfo() {}
    int operator()(ObTablet &);
  public:
    share::SCN &min_dep_meta_scn_;
    int64_t &min_major_snapshot_;
    share::SCN &clog_checkpoint_scn_;
    share::SCN &orig_clog_checkpoint_scn_;
    int64_t &multi_version_start_;
    bool &has_memtable_;
    const bool collect_lob_rely_info_;
  };
#endif
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_POINTER
