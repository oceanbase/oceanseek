/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_i_tablet_mds_customized_interface.h"
#include "storage/mds_table/ob_mds_multikey_reader.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_ddl_complete_mds_data.h"
#include "storage/tablet/ob_tablet_fork_src_mds_data.h"
namespace oceanbase
{
namespace storage
{

int ObITabletMdsCustomizedInterface::get_ddl_data(ObTabletBindingMdsUserData &ddl_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_latest_committed_data_consider_transfer_(ddl_data))) {
    if (OB_EMPTY_RESULT == ret) {
      ddl_data.set_default_value(); // use default value
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObITabletMdsCustomizedInterface::get_autoinc_seq(share::ObTabletAutoincSeq &inc_seq, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_latest_committed_data_consider_transfer_(inc_seq, &allocator))) {
    if (OB_EMPTY_RESULT == ret) {
      inc_seq.reset(); // use default value
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObITabletMdsCustomizedInterface::get_latest_split_data(
    ObTabletSplitMdsUserData &data,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version,
    const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (get_tablet_meta_().tablet_id_.is_ls_intrinsic_tablet()) {
    ret = OB_EMPTY_RESULT;
  } else {
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (get_tablet_meta_().has_transfer_table()) {
      if (CLICK_FAIL(get_src_tablet_handle_and_base_ptr_(src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(get_tablet_meta_()));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(cross_ls_get_latest(
        src,
        data,
        nullptr,
        writer,
        trans_stat,
        trans_version,
        read_seq))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get latest", K(lbt()));
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_latest_autoinc_seq(
    ObTabletAutoincSeq &data,
    ObIAllocator &allocator,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version,
    const int64_t read_seq) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    ObITabletMdsInterface *src = nullptr;
    ObTabletHandle src_tablet_handle;
    if (get_tablet_meta_().has_transfer_table()) {
      if (CLICK_FAIL(get_src_tablet_handle_and_base_ptr_(src_tablet_handle, src))) {
        MDS_LOG(WARN, "fail to get src tablet handle", K(ret), K(get_tablet_meta_()));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(cross_ls_get_latest(
        src,
        data,
        &allocator,
        writer,
        trans_stat,
        trans_version,
        read_seq))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get latest", K(lbt()));
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_ddl_complete(const share::SCN &snapshot,
                                                      ObIAllocator &allocator,
                                                      ObTabletDDLCompleteMdsUserData &data,
                                                      const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(get_snapshot(
      ObTabletDDLCompleteMdsUserDataKey(ObTabletDDLCompleteMdsUserDataKey::DDL_COMPLETE_TX_ID),
      data,
      snapshot,
      timeout,
      &allocator))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get snapshot", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_inc_major_direct_load_info(
    const share::SCN &snapshot,
    ObIAllocator &allocator,
    const ObTabletDDLCompleteMdsUserDataKey &key,
    ObTabletDDLCompleteMdsUserData &data,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(get_snapshot(key, data, snapshot, timeout, &allocator))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get snapshot", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_split_info_data(const share::SCN &snapshot,
                                                         ObTabletSplitInfoMdsUserData &data,
                                                         const int64_t timeout) const 
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret  = OB_SUCCESS;
  if (CLICK_FAIL(get_snapshot(mds::DummyKey(), data, snapshot, timeout, nullptr))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get snapshot", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_async_index_refresh_info(
    const share::SCN &snapshot,
    ObTabletAsyncIndexRefreshInfo &info,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(info)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (CLICK_FAIL((get_snapshot<mds::DummyKey, ObTabletAsyncIndexRefreshInfo>(
      mds::DummyKey(),
      ReadAsyncIndexRefreshInfoOp(info),
      snapshot,
      timeout)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get async index refresh info snapshot", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_latest_async_index_refresh_info(
    ObTabletAsyncIndexRefreshInfo &info,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version) const
{
  #define PRINT_WRAPPER KR(ret), K(info)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (CLICK_FAIL((get_latest<ObTabletAsyncIndexRefreshInfo>(
      ReadAsyncIndexRefreshInfoOp(info), writer, trans_stat, trans_version)))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG_GET(WARN, "fail to get latest async index refresh info");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_latest_committed_async_index_refresh_info(
    ObTabletAsyncIndexRefreshInfo &info) const
{
  #define PRINT_WRAPPER KR(ret), K(info)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (CLICK_FAIL((get_latest_committed<ObTabletAsyncIndexRefreshInfo>(
      ReadAsyncIndexRefreshInfoOp(info))))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG_GET(WARN, "fail to get latest committed async index refresh info");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_latest_committed_tablet_status(ObTabletCreateDeleteMdsUserData &data) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (get_tablet_meta_().tablet_id_.is_ls_intrinsic_tablet()) {
    fill_inner_tablet_status_defaults_(data, nullptr, nullptr);
  } else if (get_tablet_meta_().is_empty_shell_) {
    if (CLICK_FAIL(fill_empty_shell_tablet_status_from_meta_(data, nullptr, nullptr))) {
      MDS_LOG(WARN, "fail to fill empty shell tablet status from meta", K(ret));
    }
  } else if (CLICK_FAIL(get_latest_committed(data, nullptr))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG_GET(WARN, "fail to get latest committed", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_min_committed_fork_snapshot(
    int64_t &min_fork_snapshot) const
{
  #define PRINT_WRAPPER KR(ret), K(min_fork_snapshot)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  bool iter_end = false;
  common::ObArenaAllocator allocator("ForkSrcMds");
  ObTabletMDSInfoReader<ObTabletForkSrcMdsKey, ObTabletForkSrcMdsData> reader;
  ObTabletForkSrcMdsKey key;
  ObTabletForkSrcMdsData data;
  ObMdsValueMeta meta;
  min_fork_snapshot = INT64_MAX;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_FAIL(reader.init(
                 static_cast<const ObTablet &>(*this), share::SCN::max_scn()))) {
    MDS_LOG_GET(WARN, "fail to init fork src mds reader", K(ret));
  }
  while (OB_SUCC(ret) && !iter_end) {
    key.reset();
    data.reset();
    meta.reset();
    if (OB_FAIL(reader.get_next_mds_kv(allocator, key, data, meta))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        iter_end = true;
      } else {
        MDS_LOG_GET(WARN, "fail to get next fork src mds row", K(ret));
      }
    } else if (data.is_valid()) {
      min_fork_snapshot = MIN(min_fork_snapshot, data.fork_snapshot_version_);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsCustomizedInterface::get_latest_binding_info(
    ObTabletBindingMdsUserData &data,
    mds::MdsWriter &writer,
    mds::TwoPhaseCommitState &trans_stat,
    share::SCN &trans_version) const
{
  #define PRINT_WRAPPER KR(ret), K(data)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_latest(data, writer, trans_stat, trans_version))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "failed to get latest binding info", KR(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}
}
}
