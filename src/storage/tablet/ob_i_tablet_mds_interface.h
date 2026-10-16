/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_TABLET_OB_TABLET_MDS_PART
#define OCEANBASE_STORAGE_TABLET_OB_TABLET_MDS_PART

#include "lib/ob_errno.h"
#include "common/meta_programming/ob_meta_serialization.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/tablet/ob_tablet_member_wrapper.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/ls/ob_ls_switch_checker.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/mds_table/ob_mds_node_info_for_virtual_table.h"

namespace oceanbase
{
namespace storage
{
class ObMdsTable;
class ObTabletCreateDeleteHelper;
template <typename K, typename T>
class ObMdsRangeQueryIterator;

class ObITabletMdsInterface
{
  friend class ObTabletCreateDeleteHelper;
  friend class ObTenantDirectLoadMgr; // TODO(@gaishun.gs): refactor later
public:
  // new mds
  // Currently, we only support read LATEST multi source data, so please pass MAX_SCN as snapshot.
  // Other value will cause OB_NOT_SUPPOTED error.
  // Snapshot read operation will be implemented after multi source data dumped into macro blocks.
  template <typename T>// general set for dummy key unit
  int set(T &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us = 0);
  template <typename Key, typename Value>// general set for multi key unit
  int set(const Key &key, Value &&data, mds::MdsCtx &ctx, const int64_t lock_timeout_us = 0, const bool check_dup = true);
  // sometimes mds ndoes needed be forcely released, e.g.: ls offline
  template <typename T>
  int is_locked_by_others(bool &is_locked, const mds::MdsWriter &self = mds::MdsWriter()) const;

  int check_tablet_status_written(bool &written) const;
  // belows are wrapper interfaces for default getter for simple data structure
  // specialization get for each module
  int get_latest_tablet_status(ObTabletCreateDeleteMdsUserData &data,
                               mds::MdsWriter &writer,
                               mds::TwoPhaseCommitState &trans_stat,
                               share::SCN &trans_version,
                               const int64_t read_seq = 0) const;
  int get_tablet_status(const share::SCN &snapshot,
                        ObTabletCreateDeleteMdsUserData &data,
                        const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;
  int get_latest_ddl_data(ObTabletBindingMdsUserData &data,
                          mds::MdsWriter &writer,
                          mds::TwoPhaseCommitState &trans_stat,
                          share::SCN &trans_version,
                          const int64_t read_seq = 0) const;
  int get_ddl_data(const share::SCN &snapshot,
                   ObTabletBindingMdsUserData &data,
                   const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;
  int get_autoinc_seq(ObIAllocator &allocator,
                      const share::SCN &snapshot,
                      share::ObTabletAutoincSeq &data,
                      const int64_t timeout = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US) const;


  int get_split_data(ObTabletSplitMdsUserData &data,
                     const int64_t timeout) const;
  int split_partkey_compare(const blocksstable::ObDatumRowkey &rowkey,
                            const ObITableReadInfo &rowkey_read_info,
                            const ObIArray<uint64_t> &partkey_projector,
                            int &cmp_ret,
                            const int64_t timeout) const;
  int fill_virtual_info(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  TO_STRING_KV(KP(this), "is_inited", check_is_inited_(), "ls_id", get_tablet_meta_().ls_id_,
               "tablet_id", get_tablet_id_(), KP(get_tablet_pointer_()));
  /// Pin LS and return `ObMdsTable` for reads on LS_MDS_TABLET (delegates to get_mds_table_for_write_).
  int get_mds_table(ObLSHandle &ls_handle, ObMdsTable *&mds_table) const;
  template <typename K, typename T>
  int mds_range_query(const share::SCN &read_snapshot, ObMdsRangeQueryIterator<K, T> &iter) const;
  /// Point read on MDS storage and consider transfer source fallback.
  template <typename Key, typename Value>
  int get_mds_snapshot_data(const Key &key,
                            Value &value,
                            const share::SCN &snapshot,
                            const int64_t timeout_us,
                            ObIAllocator *alloc = nullptr) const;
protected:// implemented by ObTablet
  // if trans_stat < BEFORE_PREPARE, trans_version is explained as prepare_version(which is MAX).
  // else if trans_stat < ON_PREAPRE, trans_version is explained as prepare_version(which is MIN).
  // else if trans_stat < ON_COMMIT, trans_version is explained as prepare_version(which is a valid data).
  // else if trans_stat == ON_COMMIT, trans_version is explained as commit_version(which is a valid data).
  template <typename T>
  int get_latest(T &value,
                 mds::MdsWriter &writer,// FIXME(zk250686): should not exposed, will be removed later
                 mds::TwoPhaseCommitState &trans_stat,// FIXME(zk250686): should not exposed, will be removed later
                 share::SCN &trans_version,// FIXME(zk250686): should not exposed, will be removed later
                 ObIAllocator *alloc = nullptr,
                 const int64_t read_seq = 0) const;
  template <typename T>
  int get_latest_committed(T &value, ObIAllocator *alloc = nullptr) const;
  template <typename T>
  int get_latest_data_consider_transfer_(
      T &value,
      mds::MdsWriter &writer,
      mds::TwoPhaseCommitState &trans_stat,
      share::SCN &trans_version,
      ObIAllocator *alloc = nullptr,
      const int64_t read_seq = 0) const;
  template <typename T>
  int get_latest_committed_data_consider_transfer_(T &value, ObIAllocator *alloc = nullptr) const;
  template <typename Key, typename Value>
  int get_snapshot_data_consider_transfer_(
      const Key &key,
      Value &value,
      const share::SCN &snapshot,
      const int64_t timeout_us,
      ObIAllocator *alloc = nullptr) const;
  template <typename T>
  int get_snapshot(T &value,
                   const share::SCN snapshot,
                   const int64_t timeout_us,
                   ObIAllocator *alloc = nullptr,
                   const int64_t read_seq = 0) const;
  template <typename Key, typename Value>
  int get_snapshot(const Key &key,
                   Value &value,
                   const share::SCN snapshot,
                   const int64_t timeout_us,
                   ObIAllocator *alloc = nullptr) const;
  // belows are general get interfaces, which could be customized for complicated data structure
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_latest(OP &&read_op,
                 mds::MdsWriter &writer,// FIXME(zk250686): should not exposed, will be removed later
                 mds::TwoPhaseCommitState &trans_stat,// FIXME(zk250686): should not exposed, will be removed later
                 share::SCN &trans_version,// FIXME(zk250686): should not exposed, will be removed later
                 const int64_t read_seq = 0) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_latest_committed(OP &&read_op) const;
  template <typename T, typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(const T &))>
  int get_snapshot(OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us) const;
  template <typename Key, typename Value, typename OP>
  int get_snapshot(const Key &key,
                   OP &&read_op,
                   const share::SCN snapshot,
                   const int64_t timeout_us) const;
  /// LS inner tablets have no create/delete MDS row; synthesize NORMAL committed status for readers.
  /// When \p trans_stat / \p trans_version are null, only \p data is filled.
  static void fill_inner_tablet_status_defaults_(
      ObTabletCreateDeleteMdsUserData &data,
      mds::TwoPhaseCommitState *trans_stat,
      share::SCN *trans_version);
  int fill_empty_shell_tablet_status_from_meta_(
      ObTabletCreateDeleteMdsUserData &data,
      mds::TwoPhaseCommitState *trans_stat = nullptr,
      share::SCN *trans_version = nullptr) const;
  // TODO(@gaishun.gs): remove these virtual functions later
  virtual bool check_is_inited_() const = 0;
  virtual const ObTabletMeta &get_tablet_meta_() const = 0;
  /// Tablet reorganization SCN (transfer_scn attribute); second rowkey column on LS_MDS_TABLET.
  virtual share::SCN get_reorganization_scn() const = 0;
  virtual ObTabletBasePointer *get_tablet_pointer_() const = 0;
  // LS-level MDS on LS_MDS_TABLET. `ls_handle` keeps the LS ref for the caller's stack frame.
  virtual int get_mds_table_for_write_(ObLSHandle &ls_handle, ObMdsTable *&mds_table) const = 0;
  /// Point read on LS_MDS_TABLET via ObMdsTable::get; deserializes into \p out.
  template <typename K, typename V>
  int mds_get_committed_row_(const K &key,
                             const share::SCN &snapshot,
                             const int64_t timeout_us,
                             const bool read_uncommitted,
                             V &out,
                             common::ObIAllocator *alloc) const;
  template <typename K, typename V>
  int mds_get_row_(const K &key,
                   const share::SCN &snapshot,
                   const int64_t timeout_us,
                   const bool read_uncommitted,
                   V &out,
                   common::ObIAllocator *alloc,
                   mds::MdsWriter &writer,
                   mds::TwoPhaseCommitState &trans_stat,
                   share::SCN &trans_version) const;
  int get_tablet_handle_from_this(
    ObTabletHandle &tablet_handle) const;

  template <typename T>
  int replay(T &&mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn);
  int get_src_tablet_handle_and_base_ptr_(ObTabletHandle &tablet_handle,
                                          ObITabletMdsInterface *&base_ptr) const;
  template <typename T>
  int cross_ls_get_latest(const ObITabletMdsInterface *another,
                          T &value,
                          common::ObIAllocator *alloc,
                          mds::MdsWriter &writer,// FIXME(zk250686): should not exposed, will be removed later
                          mds::TwoPhaseCommitState &trans_stat,// FIXME(zk250686): should not exposed, will be removed later
                          share::SCN &trans_version,// FIXME(zk250686): should not exposed, will be removed later
                          const int64_t read_seq = 0) const;
  template <typename Key, typename Value>
  int replay(const Key &key,
             Value &&mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn);
private:
  template <typename Key, typename Value>
  int cross_ls_get_snapshot(const ObITabletMdsInterface *another,
                            const Key &key,
                            Value &value,
                            const share::SCN snapshot,
                            const int64_t timeout_us,
                            common::ObIAllocator *alloc = nullptr) const;
  template <typename T>
  int cross_ls_get_latest_committed(const ObITabletMdsInterface *another,
                                    T &value,
                                    ObIAllocator *alloc = nullptr) const;
  common::ObTabletID get_tablet_id_() const;
  template <typename T>
  int obj_to_string_holder_(const T &obj, ObStringHolder &holder) const;
  template <typename K, typename T>
  int fill_virtual_info_by_obj_(const T &obj, const mds::NodePosition position, ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  template <typename K, typename T>
  int fill_virtual_info_from_mds_table(ObIArray<mds::MdsNodeInfoForVirtualTable> &mds_node_info_array) const;
  template <class T, ENABLE_IF_IS_SAME_CLASS(T, ObTabletCreateDeleteMdsUserData)>
  int check_mds_data_complete_(bool &is_complete) const  { is_complete = true; return OB_SUCCESS; } // Only for tablet_Status, which doesn't need data integrity check.
  template <class T, ENABLE_IF_NOT_SAME_CLASS(T, ObTabletCreateDeleteMdsUserData)>
  int check_mds_data_complete_(bool &is_complete) const;

  // ---- Read-path retry framework (shared by is_locked_by_others / get_latest /
  //      get_latest_committed / get_snapshot).
  //
  // Each public read API constructs one of the *Op nested structs (which carry
  // the caller's typed inputs/outputs and do the actual mds_*_row_ call plus
  // error-code translation and self-logging) and hands it to
  // retry_with_ls_switch_check_, which owns the LS-switch / double-check retry loop.
  //
  // Nested structs are used (instead of free functions + friend) so the op
  // bodies have direct access to private mds_get_*_row_ / get_mds_table_for_write_
  // members.
  template <typename T> struct IsLockedByOthersOp;
  template <typename T> struct GetLatestOp;
  template <typename T> struct GetLatestCommittedOp;
  template <typename Key, typename Value> struct GetSnapshotOp;
  template <typename Op>
  int retry_with_ls_switch_check_(ObLS *ls, Op &op) const;
};

}
}

#ifndef INCLUDE_OB_TABLET_MDS_PART_IPP
#define INCLUDE_OB_TABLET_MDS_PART_IPP
#include "ob_i_tablet_mds_interface.ipp"
#endif

#endif
