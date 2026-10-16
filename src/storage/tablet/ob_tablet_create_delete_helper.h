/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER
#define OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER

#include <stdint.h>
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/hash/ob_hashset.h"
#include "common/ob_tablet_id.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "share/scn.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/tablet/ob_tablet_mds_data_cache.h"

namespace oceanbase
{
namespace blocksstable
{
class ObSSTable;
}

namespace transaction
{
class ObTransID;
}

namespace storage
{
class ObTabletMapKey;
class ObTabletCreateSSTableParam;
class ObTableHandleV2;
class ObTablet;
class ObTabletCreateDeleteMdsUserData;

class ObTabletCreateDeleteHelper
{
public:
  static int replay_mds_get_tablet( const ObTabletMapKey &key, ObLS *ls, ObTabletHandle &handle);
  static int get_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle,
      const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
      const WashTabletPriority priority = WashTabletPriority::WTP_HIGH);

  // snapshot version is used for multi source data reading,
  // tablet's multi source data will infect its visibility.
  // if snapshot version is MAX_TRANS_VERSION, it means we'll ignore
  // tablet creation/deletion transaction commit version,
  // and the tablet is fully visible as long as it really exists.
  static int check_and_get_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle,
      const int64_t timeout_us,
      const ObMDSGetTabletMode mode,
      const int64_t snapshot_version);
  static int check_status_for_new_mds(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      ObTabletStatusCache &tablet_status_cache);
  static int check_read_snapshot_by_commit_version(
      const ObTablet &tablet,
      const int64_t create_commit_version,
      const int64_t delete_commit_version,
      const int64_t snapshot_version,
      const ObTabletStatus &tablet_status);
  static int check_read_snapshot_for_normal_or_split_dst(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const mds::MdsWriter &writer,
      const mds::TwoPhaseCommitState &trans_state,
      const share::SCN &trans_version);
  static int check_read_snapshot_for_deleted(
      ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const mds::MdsWriter &writer,
      const mds::TwoPhaseCommitState &trans_state,
      const share::SCN &trans_version);
  static int check_read_snapshot_for_transfer_in(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const mds::MdsWriter &writer,
      const mds::TwoPhaseCommitState &trans_state,
      const share::SCN &trans_version);
  static int check_read_snapshot_for_deleted_or_transfer_out(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const mds::MdsWriter &writer,
      const mds::TwoPhaseCommitState &trans_state,
      const share::SCN &trans_version);
  static int check_read_snapshot_for_transfer_out_deleted(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data);
  static int check_read_snapshot_for_split_src(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const mds::TwoPhaseCommitState &trans_state);
  static int check_read_snapshot_for_split_src_deleted(
      const ObTablet &tablet,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const mds::TwoPhaseCommitState &trans_state);
  static int check_read_snapshot_by_commit_version(
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data);
  static int check_for_standby(
      const share::ObLSID &ls_id,
      const transaction::ObTransID &tx_id,
      const share::SCN &snapshot,
      ObTxCommitData::TxDataState &tx_data_state,
      share::SCN &commit_version);
  static int check_read_snapshot_for_finish_transfer_in_tx(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data);
  static int check_read_snapshot_for_create_tx(
      const ObTablet &tablet,
      const int64_t snapshot_version,
      const ObTabletCreateDeleteMdsUserData &user_data,
      const mds::MdsWriter &writer,
      const mds::TwoPhaseCommitState &trans_state,
      const share::SCN &trans_version);
public:
  static int create_tmp_tablet(
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &handle);
  static int prepare_create_msd_tablet();
  static int create_msd_tablet(
      const ObTabletMapKey &key,
      ObTabletHandle &handle);
  static int acquire_tmp_tablet(
      const ObTabletMapKey &key,
      common::ObArenaAllocator &allocator,
      ObTabletHandle &handle);
  static int acquire_tablet_from_pool(
      const ObTabletPoolType &type,
      const ObTabletMapKey &key,
      ObTabletHandle &handle);
  static int acquire_tablet_from_pool_for_ss(
    const ObTabletPoolType &type,
    const ObTabletMapKey &key,
    ObTabletHandle &handle);
  // Attention !!! only used when first creating tablet
  static int create_empty_sstable(
      common::ObArenaAllocator &allocator,
      const ObStorageSchema &storage_schema,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      ObTableHandleV2 &table_handle);
  static int create_empty_co_sstable(
      common::ObArenaAllocator &allocator,
      const ObStorageSchema &storage_schema,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      ObTableHandleV2 &table_handle);
  static int create_shared_empty_sstable(
      common::ObArenaAllocator &allocator,
      const ObStorageSchema &storage_schema,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      ObTableHandleV2 &table_handle);
  static int create_shared_empty_co_sstable(
      common::ObArenaAllocator &allocator,
      const ObStorageSchema &storage_schema,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      ObTableHandleV2 &table_handle);

  template <typename T = blocksstable::ObSSTable>
  static int create_sstable(
      const ObTabletCreateSSTableParam &param,
      common::ObArenaAllocator &allocator,
      ObTableHandleV2 &table_handle);
  template<typename T = blocksstable::ObSSTable>
  static int create_sstable(
      const ObTabletCreateSSTableParam &param,
      common::ObArenaAllocator &allocator,
      T &sstable);
  static bool is_pure_data_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_mixed_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_pure_aux_tablets(const obrpc::ObCreateTabletInfo &info);
  static bool is_pure_hidden_tablets(const obrpc::ObCreateTabletInfo &info);

  static int update_empty_mds_tablet_id_array_if_need(
      const ObTablet &tablet,
      common::ObIArray<common::ObTabletID> &empty_mds_tablet_id_array);
  static int rollback_is_written(
      const share::ObLSID &ls_id,
      const common::ObIArray<common::ObTabletID> &empty_mds_tablet_id_array);

private:
  static int inner_create_empty_sstable(
      common::ObArenaAllocator &allocator,
      const ObStorageSchema &storage_schema,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const bool is_shared,
      ObTableHandleV2 &table_handle);
  static int inner_create_empty_co_sstable(
      common::ObArenaAllocator &allocator,
      const ObStorageSchema &storage_schema,
      const common::ObTabletID &tablet_id,
      const int64_t snapshot_version,
      const bool is_shared,
      ObTableHandleV2 &table_handle);

private:
  class ReadMdsFunctor
  {
  public:
    ReadMdsFunctor(ObTabletCreateDeleteMdsUserData &user_data);
    int operator()(const ObTabletCreateDeleteMdsUserData &data);
  private:
    ObTabletCreateDeleteMdsUserData &user_data_;
  };
  class DummyReadMdsFunctor
  {
  public:
    int operator()(const ObTabletCreateDeleteMdsUserData &) { return common::OB_SUCCESS; }
  };
};

template <typename T>
int ObTabletCreateDeleteHelper::create_sstable(
    const ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &table_handle)
{
  int ret = common::OB_SUCCESS;
  void *buf = allocator.alloc(sizeof(T));
  T *sstable = nullptr;
  if (OB_ISNULL(buf)) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate sstable memory", K(ret));
  } else if (FALSE_IT(sstable = new (buf) T())) {
  } else if (OB_FAIL(create_sstable(param, allocator, *sstable))) {
    STORAGE_LOG(WARN, "fail to create sstable", K(ret));
  } else if (OB_FAIL(table_handle.set_sstable(sstable, &allocator))) {
    STORAGE_LOG(WARN, "fail to set table handle", K(ret), KPC(sstable));
  }
  return ret;
}

template <typename T>
int ObTabletCreateDeleteHelper::create_sstable(
    const ObTabletCreateSSTableParam &param,
    common::ObArenaAllocator &allocator,
    T &sstable)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(param));
  } else if (OB_FAIL(sstable.init(param, &allocator))) {
    STORAGE_LOG(WARN, "fail to init sstable", K(ret), K(param));
  }
  return ret;
}
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_HELPER
