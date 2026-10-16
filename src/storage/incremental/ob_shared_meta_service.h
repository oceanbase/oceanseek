/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_META_SERVICE_
#define OCEANBASE_STORAGE_OB_SHARED_META_SERVICE_
#include "lib/oblog/ob_log_module.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_bucket_lock.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/incremental/share/ob_shared_meta_common.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"
#include "storage/incremental/atomic_protocol/ob_tablet_atomic_protocol.h"
#include "logservice/ob_garbage_collector.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "lib/lock/ob_mutex.h" // for mutex
#include "storage/incremental/ob_shared_meta_rpc_common.h"
#include "storage/incremental/ob_ss_usage_reporter.h"
#include "storage/incremental/ob_ss_sstable_gc.h"
#include "lib/container/ob_se_array.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"

namespace oceanbase
{
namespace obrpc
{
class ObCreateLSArg;
}
namespace common
{
class ObTabletID;
class ObArenaAllocator;
}
namespace logservice
{
enum LSGCState;
}
namespace share
{
class ObLSID;
}
namespace blocksstable
{
  class ObIMacroBlockFlushCallback;
}
namespace storage
{
class ObTablet;
class ObTabletMeta;
class ObUpdateTableStoreParam;
class ObBatchUpdateTableStoreParam;
class ObIMetaFunction;
class ObSSMetaCtrlKeyInfo;

template <typename T>
class ObSSMetaIterGuard;

class ObSSTransferSrcTabletBlockInfo;
class ObTablesHandleArray;

enum class ObSSLSCreateMode
{
  NORMAL_CREATE = 0,
  BOOTSTRAP_CREATE
};

class ObSSMetaRetryOp
{
public:
  explicit ObSSMetaRetryOp(int &ret) : ret_(ret) {}
  virtual ~ObSSMetaRetryOp() = default;
  virtual void operator()() = 0;
  int get_ret() const { return ret_; }
protected:
  int &ret_;
};

class ObSSMetaService
{
  using RWLock = common::TCRWLock;
  using RDLockGuard = common::TCRLockGuard;
  using WRLockGuard = common::TCWLockGuard;
  using RDLockGuardWithTimeout = common::TCRWLock::RLockGuardWithTimeout;
  using WRLockGuardWithTimeout = common::TCRWLock::WLockGuardWithTimeout;

public:
  const static int64_t BUCKET_CNT = 10240L;
  const static int64_t DEFAULT_TIMEOUT_US = 10_s;

public:
  ObSSMetaService()
    : is_inited_(false),
      bucket_lock_(),
      meta_rpc_proxy_(),
      ss_usage_report_task_(),
      ss_sstable_gc_processor_() {}
  ~ObSSMetaService() {}
public:
  static int mtl_init(ObSSMetaService* &meta_service);
  int init();
  int start();
  int stop();
  int wait();
  void destroy();
public:
  // ----------------------- COMMON INTERFACE -----------------------------------
  int process_meta_rpc(const ObSharedRpcTransmitArg &arg,
                       ObSharedRpcTransmitResult &res);

  int get_max_committed_meta_scn(share::SCN &scn);

  // 这两个函数仅涉及内存操作，不涉及存储
  void add_diagnose_info(const char* func_name,
                         const char* reason,
                         const int64_t extra_info);
  int free_iterator(ObSSMetaIterator *iter);


  // get raw meta iter while meta_scn <= read_snapshot
  int get_raw_meta_iter(const ObSSMetaReadParamV2 &read_param,
                        const share::SCN &read_snapshot,
                        ObSSMetaIterGuard<ObSSMetaRawRowIter> &iter_guard);
  int get_latest_raw_meta_iter(const ObSSMetaReadParamV2 &read_param,
                               const share::SCN &read_snapshot,
                               ObSSMetaIterGuard<ObSSMetaRawRowIter> &iter_guard);
  // get raw meta iter while meta_scn is in range
  int get_raw_meta_iter(const ObSSMetaReadParamV2 &read_param,
                        const share::ObScnRange &range,
                        ObSSMetaIterGuard<ObSSMetaRawRowIter> &iter_guard);


  int insert_raw_meta(const ObSSRawMetaRow &row);
  // get the latest version raw meta while meta_scn <= read_snapshot
  int get_raw_meta(const ObSSMetaReadParamV2 &read_param,
                   const share::SCN &read_snapshot,
                   ObIAllocator &allocator,
                   ObSSRawMetaRow &row);
  int insert_tablet_meta(const ObSSMetaTabletRow &row);
  int batch_insert_tablet_metas(const common::ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> &rows);
    // may return mark_deleted row
  int read_tablet_meta(const ObSSMetaTabletKey &key,
                       common::ObArenaAllocator &allocator,
                       ObSSMetaTabletRow &row);
  int batch_read_tablet_metas(const common::ObSEArray<ObSSMetaTabletKey, SSMETA_ROW_BATCH_CNT> &keys,
                              common::ObArenaAllocator &allocator,
                              common::ObSEArray<ObSSMetaTabletRow, SSMETA_ROW_BATCH_CNT> &rows);
  // delete a meta row or multi rows by prefix
  // read_param: ObSSMetaDeleteParamV2.read_param_type_:
  // 1.specify the full primary key type such as ObSSMetaReadParamType::TABLET_KEY
  // 2.specify the prefix key type such as ObSSMetaReadParamType::TABLET_PREFIX
  int delete_meta_row(ObSSMetaDeleteParamV2 &delete_param);

  // ----------------------- INTERFACE FOR LS -----------------------------------
  // 这五个函数涉及atomic_op
  // create the LS replica for shared storage.
  // this interface bypasses the meta SSWriter, and writes directly to shared storage.
  // NORMAL_CREATE keeps the local LS checkpoint; BOOTSTRAP_CREATE uses the
  // aggregated shared-storage recovery point to match first-created tablets.
  int create_ls(const obrpc::ObCreateLSArg &arg,
                const ObSSLSCreateMode create_mode);
  // update shared ls metadata.
  // update the ls metadata if I am the ls meta SSWriter;
  // otherwise, send an rpc to ls meta SSWriter to do the work.
  // the ls meta SSWriter will notify all the LS replicas of the update upon successful completion.
  int update_ls_id_meta(const share::ObLSID &ls_id,
                        const int64_t service_type,
                        const int64_t limited_id,
                        const share::SCN &latest_scn,
                        const int64_t timeout_us = DEFAULT_TIMEOUT_US);
  // is_replay = true, means for clog replay situation.
  int update_ls_gc_state(const share::ObLSID &ls_id,
                         const logservice::LSGCState &gc_state,
                         const share::SCN &offline_scn,
                         const int64_t timeout_us = DEFAULT_TIMEOUT_US,
                         const bool is_replay = false);
  // update the shared ls metadata's with function fn.
  // update the ls metadata if I am the ls meta SSWriter;
  // otherwise, send an rpc to ls meta SSWriter to do the work.
  // the ls meta SSWriter will notify all the LS replicas of the update upon successful completion.
  int update_ls_meta(const share::ObLSID &ls_id,
                     const ObMetaUpdateReason update_reason,
                     ObIMetaFunction &fn,
                     const bool check_lease,
                     const int64_t timeout_us = DEFAULT_TIMEOUT_US);

  // read the latest ss ls_meta, may return mark_deleted row
  int get_ls_meta(const share::ObLSID &ls_id,
                  ObSSLSMeta &ls_meta);
  // read ss ls_meta with certain meta_scn, may return mark_deleted row
  int get_ls_meta(const share::ObLSID &ls_id,
                  const share::SCN &meta_scn,
                  ObSSLSMeta &ls_meta);
  // read the latest ss ls_meta while meta_scn <= read_snapshot, may return mark_deleted row
  int get_ls_meta(const ObSSMetaReadParamV2 &read_param,
                  const share::SCN &read_snapshot,
                  ObSSLSMeta &meta);
  // iter all ls_ids of one tenant while meta_scn <= read_snapshot
  int get_ls_id_iter(const share::SCN &read_snapshot,
                     ObSSMetaIterGuard<ObSSMetaLSIDIter> &iter_guard);
  // NEW_ADDED!!! iter all meta_scn of one ls while meta_scn <= read_snapshot
  int get_ls_scn_iter(const ObSSMetaReadParamV2 &read_param,
                      const share::SCN &read_snapshot,
                      ObSSMetaIterGuard<ObSSMetaLSSCNIterator> &iter_guard);
  // NEW_ADDED!!! delete ls_meta with certain meta_scn
  int remove_ls(const share::ObLSID &ls_id, const share::SCN &meta_scn);

  // ----------------------- INTERFACE FOR LS CKPT -----------------------------------

  // get meta_scn from ls_ckpt key which stored in palf_kv.
  // ONLY works when enable_logservice is on, otherwise return OB_NOT_SUPPORTED.
  // return OB_ENTRY_NOT_EXIST if no ls_ckpt has been published yet.
  int get_ls_ckpt_meta_scn(const share::ObLSID &ls_id,
                           share::SCN &meta_scn);

  // read checkpoint payload of the ls from object storage
  int read_ls_ckpt_data(const share::ObLSID &ls_id,
                        const share::SCN &ckpt_meta_scn,
                        ObSSLSMeta &ls_meta,
                        common::ObIArray<blocksstable::MacroBlockId> &tablet_meta_keys);

  // batch delete ls_ckpt from inner table and object_storage(only support sys tenant).
  // 对每个meta_scn先删object_storage再删内表行，不得删除palf_kv当前发布版本。
  int batch_del_ls_ckpt(const share::ObLSID &ls_id, const common::ObIArray<share::SCN> &meta_scns);

  // get all ls_ckpt from inner table while meta_scn <= read_snapshot.
  // read_mark_delete controls whether mark-deleted rows are returned.
  int get_ls_ckpt_iter(const share::ObLSID &ls_id,
                       const share::SCN &read_snapshot,
                       const bool read_mark_delete,
                       ObSSMetaIterGuard<ObSSMetaLSCkptKeyIter> &iter_guard);

  // ----------------------- INTERFACE FOR TABLET -----------------------------------
  // 这四个函数涉及atomic_op
  // create shared tablet.
  // ATTENTION: this should call at the replica with the tablet.
  int create_tablet(const share::ObLSID &ls_id,
                    const common::ObTabletID &tablet_id,
                    const share::SCN &reorgan_scn,
                    const bool check_lease);
  // update shared tablet table store.
  // update the tablet table store and metadata if I am the tablet meta SSWriter;
  // otherwise, send an rpc to tablet meta SSWriter to do the work.
  // the tablet meta SSWriter will notify all the LS replicas of the update upon successful completion.
  // 将新增的sstable与老的sstable相结合生成新的table store
  int update_tablet_table_store(const share::ObLSID &ls_id,
                                const common::ObTabletID &tablet_id,
                                const share::SCN &reorgan_scn,
                                const ObMetaUpdateReason update_reason,
                                ObUpdateTableStoreParam &update_param,
                                const common::ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
                                const bool check_lease);
  // replace all the data mini/minor sstables of the shared tablet table store
  // with one new data minor sstable.
  // ONLY accepts one data minor sstable (param.sstable_), and requires its end_scn
  // bigger than all the existing data mini/minor sstables (start_scn is free).
  // 用传入的一个minor sstable替换table store中所有的数据mini/minor sstable
  int replace_tablet_minor_sstable(const share::ObLSID &ls_id,
                                   const common::ObTabletID &tablet_id,
                                   const share::SCN &reorgan_scn,
                                   const ObMetaUpdateReason update_reason,
                                   ObUpdateTableStoreParam &update_param,
                                   const common::ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
                                   const bool check_lease);
  // all the sstable at ObBatchUpdateTableStoreParam should with the same op id.
  // set the sstable_op_id with -1, if the sstable does not need deal with GC.
  // 完全用新的sstable替换老的sstable，生成新的table store
  int build_tablet_with_batch_tables(const share::ObLSID &ls_id,
                                     const common::ObTabletID &tablet_id,
                                     const share::SCN &reorgan_scn,
                                     const ObMetaUpdateReason update_reason,
                                     const common::ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
                                     const ObBatchUpdateTableStoreParam &update_param);

  // update shared tablet metadata.
  // update the tablet metadata if I am the tablet meta SSWriter;
  // otherwise, send an rpc to tablet meta SSWriter to do the work.
  // the tablet meta SSWriter will notify all the LS replicas of the update upon successful completion.
  int update_tablet_meta(const share::ObLSID &ls_id,
                                   const common::ObTabletID &tablet_id,
                                   const share::SCN &reorgnize_scn,
                                   const ObTabletMeta &tablet_meta);

  // Mark shared tablet deleted by rewriting MetaValue head only
  // (mark_delete_ + deleted_scn_). Does not rewrite tablet body / empty_shell.
  // deleted_scn must be the delete tx commit log scn from mds for user tablets,
  // or LS offline_scn for inner tablets.
  int mark_tablet_deleted(const share::ObLSID &ls_id,
                          const common::ObTabletID &tablet_id,
                          const share::SCN &reorgnize_scn,
                          const share::SCN &deleted_scn);
  // remove the shared tablet.
  // remove the shared tablet directory if I am the tablet meta SSWriter;
  // otherwise, send an rpc to tablet meta SSWriter to do the work.
  // the tablet meta SSWriter will notify all the LS replicas of the update upon successful completion.
  int remove_tablet(const share::ObLSID &ls_id,
                    const common::ObTabletID &tablet_id,
                    const share::SCN &reorgnize_scn,
                    const share::ObScnRange &range);
  int check_tablet_mark_deleted(const share::ObLSID &ls_id,
                         const common::ObTabletID &tablet_id,
                         const share::SCN reorgnize_scn,
                         bool &is_mark_deleted);
  int check_tablet_exist(const share::ObLSID &ls_id,
                         const common::ObTabletID &tablet_id,
                         const share::SCN &reorgnize_scn,
                         bool &exist);
  // get all the tablet id and its ls id and transfer scn of one ls.
  int get_tablet_id_iter(const ObSSMetaReadParamV2 &read_param,
                         const share::SCN snapshot,
                         ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> &iter_guard);
  // get the latest ss tablet, may return mark_deleted row
  int get_tablet(const share::ObLSID &ls_id,
                 const common::ObTabletID &tablet_id,
                 const share::SCN reorgan_scn, // reserved
                 common::ObArenaAllocator &allocator,
                 ObTabletHandle &tablet_hdl);
  // may return mark_deleted row
  int get_tablet(const share::ObLSID &ls_id,
                 const common::ObTabletID &tablet_id,
                 const share::SCN reorgan_scn, // reserved
                 common::ObArenaAllocator &allocator,
                 ObTabletHandle &tablet_hdl,
                 share::SCN &meta_scn);
  // won't return mark_deleted row
  int get_tablet(const share::ObLSID &ls_id,
                 const common::ObTabletID &tablet_id,
                 const share::SCN reorgan_scn,
                 common::ObArenaAllocator &allocator,
                 ObTabletHandle &tablet_hdl,
                 ObSSMetaUpdateMetaInfo &update_meta_info,
                 share::SCN &meta_scn);
  // get the latest tablet while meta_scn <= read_snapshot
  int get_tablet(const ObSSMetaReadParamV2 &read_param,
                 const share::SCN &read_snapshot,
                 common::ObArenaAllocator &allocator,
                 ObTabletHandle &tablet_handle);
  // get tablet with the certain meta_scn, may return mark_deleted row
  int get_tablet(const share::ObLSID &ls_id,
                 const common::ObTabletID &tablet_id,
                 const share::SCN &reorgan_scn,
                 const share::SCN &meta_scn,
                 common::ObArenaAllocator &allocator,
                 ObTabletHandle &tablet_hdl);
  // get one tablet multi version, meta_scn belongs to range.
  int get_tablet_iter(const ObSSMetaReadParamV2 &read_param,
                      const share::ObScnRange &range,
                      ObSSMetaIterGuard<ObSSMetaTabletIter> &iter_guard);
  // get all the latest tablets of one ls, while meta_scn <= read_snapshot
  int get_tablet_iter(const ObSSMetaReadParamV2 &read_param,
                      const share::SCN &read_snapshot,
                      ObSSMetaIterGuard<ObSSMetaTabletIter> &iter_guard);
  // NEW_ADDED!!! get all tablet keys of one ls, while meta_scn <= read_snapshot
  int get_tablet_key_iter(const ObSSMetaReadParamV2 &read_param,
                          const share::SCN &read_snapshot,
                          ObSSMetaIterGuard<ObSSMetaTabletKeyIterator> &iter_guard);
  // LS_PREFIX returns the latest version of each tablet; TABLET_PREFIX returns all versions.
  int get_tablet_tiny_info_iter(const ObSSMetaReadParamV2 &read_param,
                                const share::SCN &read_snapshot,
                                ObSSMetaIterGuard<ObSSMetaTabletTinyInfoIterator> &iter_guard);
  // NEW_ADDED!!! get all tablet scn of one tablet, while meta_scn <= read_snapshot
  int get_tablet_scn_iter(const ObSSMetaReadParamV2 &read_param,
                          const share::SCN &read_snapshot,
                          ObSSMetaIterGuard<ObSSMetaTabletSCNIterator> &iter_guard);
  // may return mark_deleted row
  int get_tablet_latest_version(const share::ObLSID &ls_id,
                                const common::ObTabletID &tablet_id,
                                const share::SCN reorgan_scn,  // reserved
                                share::SCN &meta_scn);
  int get_tablet_macro_ids_iter(const ObSSMetaReadParamV2 &read_param,
                                const common::ObIArray<share::SCN> &meta_scns,
                                ObSSMetaIterGuard<ObSSMetaTabletMacroIDIterator> &iter_guard);
  // TODO @donglou.zl 直接读写raw_meta，可考虑封装；read_param中的read_mark_delete不生效
  int get_tablet_raw_buffer(const ObSSMetaReadParamV2 &read_param,
                            const share::SCN &read_snapshot,
                            common::ObArenaAllocator &allocator,
                            share::SCN &meta_scn,
                            char *&dest_buf,
                            int64_t &dest_buf_len);
private:
  static int gen_initial_ss_checkpoint_scn_(
      const ObSSLSCreateMode create_mode,
      const share::SCN &local_checkpoint_scn,
      const share::SCN &ss_rec_scn,
      share::SCN &initial_checkpoint_scn);
  int create_ls_(ObLS *ls, const ObSSLSCreateMode create_mode);
  int create_ls_inner_tablet_(ObLS *ls);
  int create_tablet_(const share::ObLSID &ls_id,
                     const common::ObTabletID &tablet_id,
                     const share::SCN &reorgan_scn,
                     const ObTabletHandle &local_tablet_hdl,
                     const bool check_lease);
  int persist_tablet_(const uint64_t data_version,
                      const ObTablet *tablet,
                      const ObMetaUpdateReason update_reason,
                      ObSSMetaAtomicTabletOp *tablet_op,
                      blocksstable::ObIMacroBlockFlushCallback *ddl_redo_callback = nullptr,
                      blocksstable::ObIMacroBlockFlushCallback *ddl_finish_callback = nullptr,
                      const ObSSTransferSrcTabletBlockInfo *src_tablet_block_info = nullptr);
  // for first creation, we need a tmp_tablet to call init_for_shared_storage_first_creation
  // get the tablet by this interface
  int acquire_first_created_ss_tablet_(common::ObArenaAllocator &allocator,
                                       const ObTabletHandle &local_tablet_hdl,
                                       ObTabletHandle &tablet_hdl) const;
  // may return mark_deleted row
  // by_prefix = true, get the latest ls_meta while meta_scn <= scn
  // by_prefix = false, get ls_meta with certain meta_scn
  int get_ls_from_ss_(const share::ObLSID &ls_id,
                      const share::SCN &scn,
                      ObSSLSMeta &ls_meta,
                      const bool by_prefix);
  int get_tablet_from_ss_(const share::ObLSID &ls_id,
                          const common::ObTabletID &tablet_id,
                          const share::SCN reorgan_scn,
                          common::ObArenaAllocator &allocator,
                          ObTabletHandle &tablet,
                          share::SCN &meta_scn,
                          const bool read_mark_delete = true);
  int get_tablet_from_ss_(const share::ObLSID &ls_id,
                          const common::ObTabletID &tablet_id,
                          const share::SCN reorgan_scn,
                          common::ObArenaAllocator &allocator,
                          ObTabletHandle &tablet,
                          ObSSMetaUpdateMetaInfo &update_meta_info,
                          share::SCN &meta_scn,
                          const bool read_mark_delete = true);
  int get_tablet_latest_version_from_ss_(const share::ObLSID &ls_id,
                                         const common::ObTabletID &tablet_id,
                                         const share::SCN reorgan_scn,  // reserved
                                         share::SCN &meta_scn);
  int remove_remote_tablet_(const share::ObLSID &ls_id,
                            const common::ObTabletID &tablet_id,
                            const share::SCN &reorgan_scn,
                            const share::ObScnRange &range);
  int check_lease_(const share::ObLSID &ls_id,
                   bool &is_sswriter);
  // retreve the addr of the meta sswriter in this region.
  int get_leader_(const share::ObLSID &ls_id,
                  ObAddr &addr,
                  const int64_t timeout_us = 0);
  int process_ls_meta_update_(const ObUpdateSharedLSMetaArg &arg);
  int forward_request_(const ObAddr &addr,
                       const int64_t rpc_type,
                       const ObSharedMetaUpdateArg &req,
                       ObSharedRpcTransmitResult &res);
  bool need_retry_(const int ret);
  void do_with_retry_(const char *op_name, ObSSMetaRetryOp &op);
  int resolve_create_ret_(const int create_ret,
                          const ObSSMetaTypeEnum meta_type,
                          const share::ObLSID &ls_id,
                          const common::ObTabletID &tablet_id,
                          const share::SCN &reorgan_scn);
  int build_tablet_protocol_init_param_(const ObLSID &ls_id,
                                        const ObTabletID &tablet_id,
                                        const SCN &reorgan_scn,
                                        const ObArray<ObSSMetaCtrlKeyInfo> &keys_info,
                                        ObTabletAtomicProtocolParam &init_param);
  int build_tablet_protocol_init_param_(const ObLSID &ls_id,
                                        const ObTabletID &tablet_id,
                                        const SCN &reorgan_scn,
                                        const ObArray<ObSSMetaCtrlKeyInfo> &keys_info,
                                        const bool is_deleted,
                                        const SCN &deleted_scn,
                                        ObTabletAtomicProtocolParam &init_param);
  void build_tablet_op_init_param_(const int64_t op_id,
                                   const share::ObLSID &ls_id,
                                   const common::ObTabletID &tablet_id,
                                   const share::SCN &reorgan_scn,
                                   ObSSMetaAtomicOpInitParam &init_param);
  void build_ls_op_init_param_(const int64_t op_id,
                               const share::ObLSID &ls_id,
                               ObSSMetaAtomicOpInitParam &init_param);
  int do_update_tablet_table_store_(const share::ObLSID &ls_id,
                                    const common::ObTabletID &tablet_id,
                                    const share::SCN &reorgan_scn,
                                    const ObMetaUpdateReason update_reason,
                                    ObUpdateTableStoreParam &update_param,
                                    const common::ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
                                    const bool check_lease);
  int do_replace_tablet_minor_sstable_(const share::ObLSID &ls_id,
                                       const common::ObTabletID &tablet_id,
                                       const share::SCN &reorgan_scn,
                                       const ObMetaUpdateReason update_reason,
                                       ObUpdateTableStoreParam &update_param,
                                       const common::ObArray<ObSSMetaCtrlKeyInfo> &meta_ctrl_keys,
                                       const bool check_lease);
  int check_can_replace_minor_sstable_(
      const ObTablet &old_tablet,
      const ObUpdateTableStoreParam &param);
  int get_local_empty_major_sstable_(
      ObTableStoreIterator &iter,
      ObTableHandleV2 &local_empty_major) const;

  int update_for_ss_id_meta_(const ObSharedMetaUpdateArg &rpc_req,
                             const share::ObLSID &ls_id,
                             const int64_t service_type,
                             const int64_t limited_id,
                             const share::SCN &latest_scn);
  int get_ls_gc_state_from_ss_(const share::ObLSID &ls_id,
                               logservice::LSGCState &gc_state,
                               share::SCN &offline_scn);
  int check_ls_gc_state_covered_(const share::ObLSID &ls_id,
                                 const logservice::LSGCState &gc_state,
                                 const share::SCN &offline_scn,
                                 bool &is_covered);
  int update_for_ss_gc_state_(const ObSharedMetaUpdateArg &rpc_req,
                              const share::ObLSID &ls_id,
                              const logservice::LSGCState &gc_state,
                              const share::SCN &offline_scn,
                              const bool is_replay);
  int update_for_ss_ls_meta_(const ObSharedMetaUpdateArg &rpc_req,
                             const share::ObLSID &ls_id,
                             ObIMetaFunction &fn);

  int update_for_ss_id_meta_local_(const share::ObLSID &ls_id,
                                   const ObMetaUpdateReason reason,
                                   const bool check_lease,
                                   const int64_t service_type,
                                   const int64_t limited_id,
                                   const share::SCN &latest_scn);
  int update_for_ss_gc_state_local_(const share::ObLSID &ls_id,
                                    const ObMetaUpdateReason reason,
                                    const bool check_lease,
                                    const logservice::LSGCState &gc_state,
                                    const share::SCN &offline_scn);
  int mark_inner_tablet_delete_(const share::ObLSID &ls_id,
                               const common::ObTabletID &tablet_id,
                               const share::SCN &reorgan_scn,
                               const share::SCN &delete_scn);
  int mark_ls_inner_tablets_deleted_(const share::ObLSID &ls_id,
                                     const share::SCN &deleted_scn);
  int update_for_ss_ls_meta_local_(const share::ObLSID &ls_id,
                                   const ObMetaUpdateReason reason,
                                   const bool check_lease,
                                   ObIMetaFunction &fn);
  friend class CreateLsRetryOp;
  friend class CreateLsInnerTabletRetryOp;
  friend class CreateTabletRetryOp;
  friend class UpdateTabletTableStoreRetryOp;
  friend class ReplaceTabletMinorSstableRetryOp;
private:
  bool is_inited_;
  ObBucketLock bucket_lock_;
  obrpc::ObSharedRpcProxy meta_rpc_proxy_;
  ObSSUsageReportTask ss_usage_report_task_;
  // ss sstable gc processor
  ObSSSSTableGCProcessor ss_sstable_gc_processor_;
};

} // namespace storage
} // namespace oceanbase


#endif // OCEANBASE_STORAGE_OB_SHARED_META_SERVICE_
