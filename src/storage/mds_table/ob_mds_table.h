/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_H

#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "lib/literals/ob_literals.h"
#include "lib/worker.h"
#include "common/meta_programming/ob_type_traits.h"
#include "common/ob_tablet_id.h"
#include "storage/checkpoint/ob_common_checkpoint.h"
#include "storage/multi_data_source/mds_ctx.h"
#include "storage/multi_data_source/compile_utility/compile_mapper.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/mds_table/ob_mds_table_datum_row_converter.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/mds_table/ob_mds_tablet_schema.h"
#include <utility>

namespace oceanbase
{
namespace common
{
class ObTabletID;
class ObIAllocator;
class ObNewRange;
}

namespace storage
{
class ObLS;
class ObTableScanParam;
class ObStoreCtx;
class ObMdsTableScanIterator;
class ObITabletMdsInterface;

// ObMdsTable owns the per-LS MDS_Tablet (LS_MDS_TABLET). Named in the style
// of ObTxTable / ObLockTable — it manages the tablet's lifecycle (create /
// remove / online / offline) and exposes tablet-aware operations for the
// aggregated multi-data-source records of every user tablet in this log
// stream.
//
// Read/write entry points (insert/get/scan, resolver, compaction filter, ...)
// are added in later phases; this class only scaffolds the lifecycle in
// Phase 1.
class ObMdsTable final : public checkpoint::ObCommonCheckpoint
{
  friend class ObITabletMdsInterface;

public:
  ObMdsTable();
  ~ObMdsTable();

  // Registered as MDS_TABLE_TYPE while the MDS table is online. MDS memtables
  // are not tracked by DATA_CHECKPOINT, so this checkpoint owns their rec_scn
  // reporting and clog-recycle flush.
  virtual share::SCN get_rec_scn() override;
  virtual int flush(share::SCN recycle_scn = share::SCN::max_scn(), const int64_t trace_id = 0, bool need_freeze = true) override;
  virtual common::ObTabletID get_tablet_id() const override { return common::LS_MDS_TABLET; }
  virtual bool is_flushing() const override { return false; }

  int init(ObLS *ls);
  int start();
  void stop();
  void destroy();

  int create_tablet(const share::SCN &create_scn);
  int remove_tablet();

  int offline();
  int online();

  int get_mds_checkpoint_scn(share::SCN &mds_ckpt_scn) const;

  // Create the tablet for compat path: callers on an existing LS that has
  // never hosted an MDS_Tablet (e.g. cross-version upgrade before cutover).
  int init_tablet_for_compat();

  class TabletStatusReadEmptyGuard final
  {
  public:
    TabletStatusReadEmptyGuard()
    {
      ObMdsTable::set_tablet_status_read_empty_diag_();
    }
    ~TabletStatusReadEmptyGuard()
    {
      ObMdsTable::clear_tablet_status_read_empty_diag_();
    }
  private:
    DISALLOW_COPY_AND_ASSIGN(TabletStatusReadEmptyGuard);
  };

  // Open scan iterator on LS_MDS_TABLET for one MDS unit type.
  template <typename K, typename V>
  int table_scan(
      common::ObIAllocator &allocator,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const share::SCN &read_snapshot,
      ObMdsTableScanIterator &iter)
  {
    constexpr uint8_t mds_unit_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<K, V>::value);
    return table_scan_by_mds_type_(allocator, tablet_id, transfer_scn, mds_unit_id, read_snapshot, iter);
  }

  // MDS aggregation Stage 2 write path: called from ObITabletMdsInterface::set()
  // (caller must supply non-null ctx.get_store_ctx()). Stores a
  // live ObMdsTransNodeValue<V> in the LS_MDS_TABLET's memtable via the
  // standard ObMemtable::set_ path (mvcc_acc_ctx_.write_flag_.is_mds()). The row key is
  // (tablet_id, transfer_scn, mds_type, mds_key); the value column is left nop —
  // the live V object lives in the MVCC node's buf_ instead.

  // DummyKey (single-row) convenience: same as write<mds::DummyKey, V>(..., DummyKey(), data, ...).
  template <typename V>
  int write(const common::ObTabletID &tablet_id,
            const share::SCN &transfer_scn,
            const V &data,
            ObStoreCtx &store_ctx,
            mds::MdsCtx &mds_ctx,
            const bool check_dup = false)
  {
    return write_impl_(tablet_id, transfer_scn, mds::DummyKey(), data, nullptr, store_ctx, mds_ctx, check_dup);
  }
  template <typename V>
  int write(const common::ObTabletID &tablet_id,
            const share::SCN &transfer_scn,
            const V &data,
            const ObMdsValueMeta &meta,
            ObStoreCtx &store_ctx,
            mds::MdsCtx &mds_ctx,
            const bool check_dup = false)
  {
    return write_impl_(tablet_id, transfer_scn, mds::DummyKey(), data, &meta, store_ctx, mds_ctx, check_dup);
  }

  // Point read on LS_MDS_TABLET: @p key is mds_serialized into mds_key, same as write<K,V>.
  // On success, @p value and @p meta are deserialized from the stored row.
  template <typename K, typename V>
  int get(common::ObIAllocator *allocator,
          const common::ObTabletID &tablet_id,
          const share::SCN &transfer_scn,
          const K &key,
          const share::SCN &read_snapshot,
          const int64_t timeout_us,
          const bool read_uncommitted,
          V &value,
          ObMdsValueMeta &meta)
  {
    value.reset();
    meta.reset();
    return get_mds_kv_(
        allocator, tablet_id, transfer_scn, key, read_snapshot, timeout_us, read_uncommitted, value, meta);
  }

  // DummyKey (single-row) convenience: forwards to get<mds::DummyKey, V>.
  template <typename V>
  int get(common::ObIAllocator *allocator,
          const common::ObTabletID &tablet_id,
          const share::SCN &transfer_scn,
          const share::SCN &read_snapshot,
          const int64_t timeout_us,
          const bool read_uncommitted,
          V &value,
          ObMdsValueMeta &meta)
  {
    return get<mds::DummyKey, V>(
        allocator, tablet_id, transfer_scn, mds::DummyKey(), read_snapshot, timeout_us, read_uncommitted, value, meta);
  }

  // Latest-committed DummyKey point-get with default snapshot/timeout.
  // Only for non-DEEP_SERIALIZEABLE V: convert_row_to_kv deserializes without
  // allocator. Deep types must call get() with a caller-owned allocator that
  // outlives @p value. Use get() also when meta / snapshot / timeout need control.
  template <typename V>
  int get_latest_committed(const common::ObTabletID &tablet_id,
                           const share::SCN &transfer_scn,
                           const int64_t timeout_us,
                           V &value)
  {
    static_assert(!OB_TRAIT_DEEP_SERIALIZEABLE(V),
                  "get_latest_committed cannot deep-deserialize; pass a lasting allocator to get()");
    ObMdsValueMeta meta;
    return get(nullptr /*allocator*/,
               tablet_id,
               transfer_scn,
               share::SCN::max_scn(),
               timeout_us,
               false /*read_uncommitted*/,
               value,
               meta);
  }

  // Multi-key variant: mds_serialize K into mds_key column.
  template <typename K, typename V>
  int write(const common::ObTabletID &tablet_id,
            const share::SCN &transfer_scn,
            const K &key,
            const V &data,
            ObStoreCtx &store_ctx,
            mds::MdsCtx &mds_ctx,
            const bool check_dup = true)
  {
    return write_impl_(tablet_id, transfer_scn, key, data, nullptr, store_ctx, mds_ctx, check_dup);
  }
  template <typename K, typename V>
  int write(const common::ObTabletID &tablet_id,
            const share::SCN &transfer_scn,
            const K &key,
            const V &data,
            const ObMdsValueMeta &meta,
            ObStoreCtx &store_ctx,
            mds::MdsCtx &mds_ctx,
            const bool check_dup = true)
  {
    return write_impl_(tablet_id, transfer_scn, key, data, &meta, store_ctx, mds_ctx, check_dup);
  }

  // MDS redo replay: @p ctx must carry a non-null ObStoreCtx (same contract as set()
  // on the LS aggregate path). Applies redo on LS_MDS_TABLET via replay_mds_node_
  // (mvcc_replay / register_row_replay_cb), not insert_row write path.
  template <typename V>
  int replay(const common::ObTabletID &tablet_id,
             const share::SCN &transfer_scn,
             const V &data,
             mds::MdsCtx &ctx,
             const share::SCN &scn)
  {
    return replay_impl_(tablet_id, transfer_scn, mds::DummyKey(), data, nullptr, ctx, scn);
  }
  template <typename V>
  int replay(const common::ObTabletID &tablet_id,
             const share::SCN &transfer_scn,
             const V &data,
             const ObMdsValueMeta &meta,
             mds::MdsCtx &ctx,
             const share::SCN &scn)
  {
    return replay_impl_(tablet_id, transfer_scn, mds::DummyKey(), data, &meta, ctx, scn);
  }
  template <typename K, typename V>
  int replay(const common::ObTabletID &tablet_id,
             const share::SCN &transfer_scn,
             const K &key,
             const V &mds,
             mds::MdsCtx &ctx,
             const share::SCN &scn)
  {
    return replay_impl_(tablet_id, transfer_scn, key, mds, nullptr, ctx, scn);
  }
  template <typename K, typename V>
  int replay(const common::ObTabletID &tablet_id,
             const share::SCN &transfer_scn,
             const K &key,
             const V &mds,
             const ObMdsValueMeta &meta,
             mds::MdsCtx &ctx,
             const share::SCN &scn)
  {
    return replay_impl_(tablet_id, transfer_scn, key, mds, &meta, ctx, scn);
  }

  TO_STRING_KV(KP(this), K_(is_inited), K_(checkpoint_registered), K_(ls_id), K_(state));

private:
  template <typename K, typename V>
  int write_impl_(const common::ObTabletID &tablet_id,
                  const share::SCN &transfer_scn,
                  const K &key,
                  const V &data,
                  const ObMdsValueMeta *meta,
                  ObStoreCtx &store_ctx,
                  mds::MdsCtx &mds_ctx,
                  const bool check_dup);
  template <typename K, typename V>
  int replay_impl_(const common::ObTabletID &tablet_id,
                   const share::SCN &transfer_scn,
                   const K &key,
                   const V &mds,
                   const ObMdsValueMeta *meta,
                   mds::MdsCtx &ctx,
                   const share::SCN &scn);
  // replay<> templates call this so they need not touch ObLS members (only forward-declared here).
  bool is_mds_replay_ready_() const;
  int write_mds_node_(const common::ObTabletID &tablet_id,
                      const share::SCN &transfer_scn,
                      const uint8_t mds_type_id,
                      const common::ObString &mds_key,
                      const void *mds_value,
                      const ObMdsValueMeta *meta,
                      ObStoreCtx &store_ctx,
                      mds::MdsCtx &mds_ctx,
                      const bool check_dup);
  int replay_mds_node_(const common::ObTabletID &tablet_id,
                       const share::SCN &transfer_scn,
                       const uint8_t mds_type_id,
                       const common::ObString &mds_key,
                       const void *mds_value,
                       const ObMdsValueMeta *meta,
                       mds::MdsCtx &mds_ctx,
                       const share::SCN &replay_scn);
  int get_mds_tablet_handle_(ObTabletHandle &mds_tablet_handle) const;
  int remove_tablet_(const common::ObTabletID &tablet_id);
  int table_scan_by_mds_type_(
      common::ObIAllocator &allocator,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const uint8_t mds_unit_id,
      const share::SCN &read_snapshot,
      ObMdsTableScanIterator &iter);
  static int build_mds_scan_key_range_(
      common::ObIAllocator &allocator,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const uint8_t mds_unit_id,
      void *&key_range_buf,
      common::ObNewRange &key_range);
  struct ObMdsRowReadInfo
  {
    ObMdsRowReadInfo()
      : trans_version_(INT64_MAX),
        row_trans_id_(),
        writer_tx_id_(0),
        is_uncommitted_(false)
    {}
    void reset()
    {
      trans_version_ = INT64_MAX;
      row_trans_id_.reset();
      writer_tx_id_ = 0;
      is_uncommitted_ = false;
    }
    TO_STRING_KV(K_(trans_version), K_(row_trans_id), K_(writer_tx_id), K_(is_uncommitted));
    int64_t trans_version_;
    transaction::ObTransID row_trans_id_;
    int64_t writer_tx_id_;
    bool is_uncommitted_;
  };
  struct ObMdsReadArg
  {
    ObMdsReadArg(
        common::ObIAllocator *allocator,
        const common::ObTabletID &tablet_id,
        const share::SCN &transfer_scn,
        const uint8_t mds_unit_id,
        const common::ObString &udf_key,
        const share::SCN &read_snapshot,
        const int64_t timeout_us,
        const bool read_uncommitted,
        const bool need_row_info,
        const bool need_kv)
      : allocator_(allocator),
        tablet_id_(tablet_id),
        transfer_scn_(transfer_scn),
        mds_unit_id_(mds_unit_id),
        udf_key_(udf_key),
        read_snapshot_(read_snapshot),
        timeout_us_(timeout_us),
        read_uncommitted_(read_uncommitted),
        need_row_info_(need_row_info),
        need_kv_(need_kv)
    {}
    common::ObIAllocator *allocator_;
    common::ObTabletID tablet_id_;
    share::SCN transfer_scn_;
    uint8_t mds_unit_id_;
    common::ObString udf_key_;
    share::SCN read_snapshot_;
    int64_t timeout_us_;
    bool read_uncommitted_;
    bool need_row_info_;
    bool need_kv_;
    bool is_valid() const
    {
      return tablet_id_.is_valid()
          && transfer_scn_.is_valid()
          && read_snapshot_.is_valid()
          && timeout_us_ >= 0
          && (need_row_info_ || need_kv_);
    }
    TO_STRING_KV(K_(tablet_id),
                 K_(transfer_scn),
                 K_(mds_unit_id),
                 K_(udf_key),
                 K_(read_snapshot),
                 K_(timeout_us),
                 K_(read_uncommitted),
                 K_(need_row_info),
                 K_(need_kv));
  };
  typedef int (*ObMdsKvConvertFunc)(
      common::ObIAllocator *allocator,
      const blocksstable::ObDatumRow &row,
      void *key,
      void *value,
      ObMdsValueMeta &meta);
  template <typename K, typename V>
  static int convert_mds_kv_read_result_(
      common::ObIAllocator *allocator,
      const blocksstable::ObDatumRow &row,
      void *key,
      void *value,
      ObMdsValueMeta &meta);
  struct ObMdsReadResult
  {
    explicit ObMdsReadResult(ObMdsRowReadInfo &row_info)
      : row_info_(&row_info),
        key_(nullptr),
        value_(nullptr),
        meta_(nullptr),
        kv_convert_func_(nullptr)
    {}
    template <typename K, typename V>
    ObMdsReadResult(K &key, V &value, ObMdsValueMeta &meta)
      : row_info_(nullptr),
        key_(static_cast<void *>(&key)),
        value_(static_cast<void *>(&value)),
        meta_(&meta),
        kv_convert_func_(convert_mds_kv_read_result_<K, V>)
    {}
    template <typename K, typename V>
    ObMdsReadResult(ObMdsRowReadInfo &row_info, K &key, V &value, ObMdsValueMeta &meta)
      : row_info_(&row_info),
        key_(static_cast<void *>(&key)),
        value_(static_cast<void *>(&value)),
        meta_(&meta),
        kv_convert_func_(convert_mds_kv_read_result_<K, V>)
    {}
    ObMdsRowReadInfo *row_info_;
    void *key_;
    void *value_;
    ObMdsValueMeta *meta_;
    ObMdsKvConvertFunc kv_convert_func_;
    bool is_valid(const ObMdsReadArg &read_arg) const
    {
      const bool row_info_valid = !read_arg.need_row_info_ || OB_NOT_NULL(row_info_);
      const bool kv_valid = !read_arg.need_kv_
          || (OB_NOT_NULL(key_)
              && OB_NOT_NULL(value_)
              && OB_NOT_NULL(meta_)
              && OB_NOT_NULL(kv_convert_func_));
      return row_info_valid && kv_valid;
    }
    TO_STRING_KV(KP_(row_info), KP_(key), KP_(value), KP_(meta), KP_(kv_convert_func));
  };
  // Read the row info (including row_trans_id_ used for lock detection) for a
  // single mds key. Internally forces read_uncommitted=true: row_trans_id_ is
  // only populated when the SSTable scan is configured with iter_uncommitted_row,
  // so this entry point owns that invariant on behalf of callers (e.g. is_locked_by_others).
  template <typename K, typename V>
  int get_mds_row_read_info_(
      common::ObIAllocator *allocator,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const K &key,
      const share::SCN &read_snapshot,
      const int64_t timeout_us,
      ObMdsRowReadInfo &row_info);
  static int fill_mds_row_read_info_(
      const blocksstable::ObDatumRow &row,
      ObMdsRowReadInfo &row_info);
  struct ObMdsSingleMergeResources;
  struct ObMdsSingleReadDiagnoseInfo;
  int setup_single_merge_for_read_(
      const ObMdsReadArg &read_arg,
      common::ObIAllocator &allocator,
      ObMdsSingleMergeResources &resources,
      ObMdsSingleReadDiagnoseInfo &diagnose_info);
  int read_and_verify_single_row_(
      const ObMdsReadArg &read_arg,
      ObMdsReadResult &read_result,
      ObMdsSingleMergeResources &resources,
      ObMdsSingleReadDiagnoseInfo &diagnose_info);
  int read_mds_row_(
      const ObMdsReadArg &read_arg,
      ObMdsReadResult &read_result);
  template <typename K, typename V>
  int get_mds_kv_(
      common::ObIAllocator *allocator,
      const common::ObTabletID &tablet_id,
      const share::SCN &transfer_scn,
      const K &key,
      const share::SCN &read_snapshot,
      const int64_t timeout_us,
      const bool read_uncommitted,
      V &value,
      ObMdsValueMeta &meta);
  int prepare_mds_read_snapshot_(
      const share::SCN &read_snapshot,
      const ObTabletHandle &ls_mds_handle,
      share::SCN &effective_read_snapshot,
      bool &is_select_follower);
  int direct_commit_mds_node_(const common::ObTabletID &tablet_id,
                              const share::SCN &transfer_scn,
                              const uint8_t mds_type_id,
                              const common::ObString &mds_key,
                              const void *mds_value,
                              const share::SCN &log_scn);
  int freeze_ls_mds_tablet_(const int64_t trace_id, bool is_sync);
  int flush_frozen_memtables_(ObTabletHandle &mds_tablet_handle, const int64_t trace_id);

  static void set_tablet_status_read_empty_diag_();
  static void clear_tablet_status_read_empty_diag_();

private:
  // Lifecycle of LS_MDS_TABLET observed by this ObMdsTable:
  //   INIT    : init() succeeded, but LS_MDS_TABLET is not yet created
  //             (create_ls_inner_tablet runs later in ObLSService::create_ls_).
  //             It is legitimate for get_rec_scn() to find no tablet in this state.
  //   ONLINE  : ObLS::online_without_lock() has driven mds_table_.online()
  //             successfully; LS_MDS_TABLET exists and rec_scn is meaningful.
  //   OFFLINE : offline() has been called; memtables released. Tablet may
  //             already be gone, so missing-tablet here is also legitimate.
  enum class MdsTableState : uint8_t
  {
    INIT = 0,
    ONLINE = 1,
    OFFLINE = 2,
  };

  bool is_inited_;
  // Protected by the LS lifecycle lock. A registered checkpoint is always in
  // ONLINE state; online/offline order state publication around registry locks
  // so checkpoint readers never observe a registered inactive object.
  bool checkpoint_registered_;
  share::ObLSID ls_id_;
  ObLS *ls_;
  uint8_t state_;  // MdsTableState; stored as uint8_t for ATOMIC_LOAD/STORE.

  static thread_local bool thread_tablet_status_read_empty_diag_;

  DISALLOW_COPY_AND_ASSIGN(ObMdsTable);
};

template <typename K, typename V>
int ObMdsTable::convert_mds_kv_read_result_(
    common::ObIAllocator *allocator,
    const blocksstable::ObDatumRow &row,
    void *key,
    void *value,
    ObMdsValueMeta &meta)
{
  int ret = common::OB_SUCCESS;
  K *typed_key = static_cast<K *>(key);
  V *typed_value = static_cast<V *>(value);
  if (OB_FAIL(ObMdsTableDatumRowConverter::convert_row_to_kv(allocator, row, *typed_key, *typed_value, meta))) {
    MDS_LOG(WARN, "fail to convert mds datum row to typed value", K(ret));
  }
  return ret;
}

template <typename K, typename V>
inline int ObMdsTable::get_mds_row_read_info_(common::ObIAllocator *allocator,
                                              const common::ObTabletID &tablet_id,
                                              const share::SCN &transfer_scn,
                                              const K &key,
                                              const share::SCN &read_snapshot,
                                              const int64_t timeout_us,
                                              ObMdsRowReadInfo &row_info)
{
  int ret = common::OB_SUCCESS;
  constexpr uint8_t mds_type_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<K, V>::value);
  char key_buf[ObMdsTabletSchemaDef::MDS_KEY_COLUMN_LENGTH];
  int64_t pos = 0;
  row_info.reset();
  if (OB_FAIL(key.mds_serialize(key_buf, static_cast<int64_t>(sizeof(key_buf)), pos))) {
    MDS_LOG(WARN, "failed to serialize mds key", K(ret));
  } else {
    // read_uncommitted is hardcoded true: ObMdsRowReadInfo::row_trans_id_ is
    // sourced from ObDatumRow::get_trans_id(), which only returns a meaningful
    // value when the underlying scan was opened with iter_uncommitted_row.
    ObMdsReadArg read_arg(
        allocator,
        tablet_id,
        transfer_scn,
        mds_type_id,
        common::ObString(static_cast<int32_t>(pos), key_buf),
        read_snapshot,
        timeout_us,
        true/*read_uncommitted*/,
        true/*need_row_info*/,
        false/*need_kv*/);
    ObMdsReadResult read_result(row_info);
    ret = read_mds_row_(read_arg, read_result);
  }
  return ret;
}

template <typename K, typename V>
inline int ObMdsTable::get_mds_kv_(common::ObIAllocator *allocator,
                                   const common::ObTabletID &tablet_id,
                                   const share::SCN &transfer_scn,
                                   const K &key,
                                   const share::SCN &read_snapshot,
                                   const int64_t timeout_us,
                                   const bool read_uncommitted,
                                   V &value,
                                   ObMdsValueMeta &meta)
{
  int ret = common::OB_SUCCESS;
  K key_ret_unused;
  constexpr uint8_t mds_type_id = static_cast<uint8_t>(mds_type_id_detail::TypeIdOf<K, V>::value);
  char key_buf[ObMdsTabletSchemaDef::MDS_KEY_COLUMN_LENGTH];
  int64_t pos = 0;
  if (OB_FAIL(key.mds_serialize(key_buf, static_cast<int64_t>(sizeof(key_buf)), pos))) {
    MDS_LOG(WARN, "failed to serialize mds key", K(ret));
  } else {
    ObMdsReadArg read_arg(
        allocator,
        tablet_id,
        transfer_scn,
        mds_type_id,
        common::ObString(static_cast<int32_t>(pos), key_buf),
        read_snapshot,
        timeout_us,
        read_uncommitted,
        false/*need_row_info*/,
        true/*need_kv*/);
    ObMdsReadResult read_result(key_ret_unused, value, meta);
    ret = read_mds_row_(read_arg, read_result);
  }
  if (OB_FAIL(ret)) {
    if (common::OB_ITER_END != ret && common::OB_EMPTY_RESULT != ret && common::OB_SNAPSHOT_DISCARDED != ret) {
      MDS_LOG(WARN, "fail to read mds kv", K(ret));
    }
  }
  return ret;
}

template <typename K, typename V>
inline int ObMdsTable::write_impl_(const common::ObTabletID &tablet_id,
                                   const share::SCN &transfer_scn,
                                   const K &key,
                                   const V &data,
                                   const ObMdsValueMeta *meta,
                                   ObStoreCtx &store_ctx,
                                   mds::MdsCtx &mds_ctx,
                                   const bool check_dup)
{
  int ret = OB_SUCCESS;
  uint8_t mds_type_id = mds_type_id_detail::TypeIdOf<K, V>::value;
  char key_buf[ObMdsTabletSchemaDef::MDS_KEY_COLUMN_LENGTH];
  int64_t pos = 0;
  if (OB_FAIL(key.mds_serialize(key_buf, sizeof(key_buf), pos))) {
    MDS_LOG(WARN, "failed to serialize mds key", K(ret));
  } else if (OB_FAIL(write_mds_node_(tablet_id,
                                     transfer_scn,
                                     mds_type_id,
                                     common::ObString(pos, key_buf),
                                     static_cast<const void *>(&data),
                                     meta,
                                     store_ctx,
                                     mds_ctx,
                                     check_dup))) {
    MDS_LOG(WARN, "failed to write mds node", K(ret),
            K(tablet_id), K(transfer_scn), K(mds_type_id),
            K(key), K(data), KP(meta), K(store_ctx), K(mds_ctx), K(check_dup));
  }
  return ret;
}

template <typename K, typename V>
inline int ObMdsTable::replay_impl_(const common::ObTabletID &tablet_id,
                                    const share::SCN &transfer_scn,
                                    const K &key,
                                    const V &data,
                                    const ObMdsValueMeta *meta,
                                    mds::MdsCtx &ctx,
                                    const share::SCN &scn)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    MDS_LOG(WARN, "mds table not inited", K(ret));
  } else if (!is_mds_replay_ready_()) {
    ret = common::OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "ls or tablet service is null", K(ret), KP(ls_));
  } else if (OB_ISNULL(ctx.get_mem_ctx())) {
    ret = common::OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "ls mds replay requires non-null ObMemtableCtx on MdsCtx", K(ret), K(tablet_id), K(transfer_scn));
  } else {
    uint8_t mds_type_id = mds_type_id_detail::TypeIdOf<K, V>::value;
    char key_buf[ObMdsTabletSchemaDef::MDS_KEY_COLUMN_LENGTH];
    int64_t pos = 0;
    if (OB_FAIL(key.mds_serialize(key_buf, sizeof(key_buf), pos))) {
      MDS_LOG(WARN, "failed to serialize mds key", K(ret));
    } else if (OB_FAIL(replay_mds_node_(tablet_id,
                                        transfer_scn,
                                        mds_type_id,
                                        common::ObString(pos, key_buf),
                                        static_cast<const void *>(&data),
                                        meta,
                                        ctx,
                                        scn))) {
      MDS_LOG(WARN, "failed to replay mds node", K(ret),
              K(tablet_id), K(transfer_scn), K(mds_type_id),
              K(key), K(data), KP(meta), K(ctx), K(scn));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TABLE_H
