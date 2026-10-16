/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include "storage/tablet/ob_i_tablet_mds_interface.h"
#include "storage/access/ob_table_read_info.h"
#include "common/ob_tablet_id.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
void ObITabletMdsInterface::fill_inner_tablet_status_defaults_(
    ObTabletCreateDeleteMdsUserData &data,
    mds::TwoPhaseCommitState *trans_stat,
    share::SCN *trans_version)
{
  data.tablet_status_ = ObTabletStatus::NORMAL;
  data.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
  data.create_commit_version_ = share::SCN::base_scn().get_val_for_tx();
  if (nullptr != trans_stat) {
    *trans_stat = mds::TwoPhaseCommitState::ON_COMMIT;
  }
  if (nullptr != trans_version) {
    *trans_version = share::SCN::base_scn();
  }
}

int ObITabletMdsInterface::fill_empty_shell_tablet_status_from_meta_(
    ObTabletCreateDeleteMdsUserData &data,
    mds::TwoPhaseCommitState *trans_stat,
    share::SCN *trans_version) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data.assign(get_tablet_meta_().empty_shell_tablet_status_))) {
    MDS_LOG(WARN, "fail to assign empty shell tablet status from meta", K(ret));
  } else {
    if (nullptr != trans_stat) {
      *trans_stat = mds::TwoPhaseCommitState::ON_COMMIT;
    }
    if (nullptr != trans_version) {
      if (transaction::is_effective_trans_version(data.delete_commit_version_)) {
        if (OB_FAIL(trans_version->convert_for_tx(data.delete_commit_version_))) {
          MDS_LOG(WARN, "fail to convert delete commit version for trans_version", K(ret),
              K(data.delete_commit_version_));
        }
      } else {
        // the create transaction aborted
        trans_version->set_min();
      }
    }
  }
  return ret;
}

int ObITabletMdsInterface::get_mds_table(ObLSHandle &ls_handle, ObMdsTable *&mds_table) const
{
  return get_mds_table_for_write_(ls_handle, mds_table);
}

int ObITabletMdsInterface::get_src_tablet_handle_and_base_ptr_(
    ObTabletHandle &tablet_handle,
    ObITabletMdsInterface *&base_ptr) const
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = get_tablet_meta_().transfer_info_.ls_id_;
  const common::ObTabletID &tablet_id = get_tablet_meta_().tablet_id_;
  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  ObTablet *tablet = nullptr;

  if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::TABLET_MOD))) {
    MDS_LOG(WARN, "fail to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "ls is null", K(ret), KP(ls), K(ls_id));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    MDS_LOG(WARN, "fail to get tablet", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "tablet is null", K(ret), K(ls_id), K(tablet_id), K(tablet_handle));
  } else {
    base_ptr = static_cast<ObITabletMdsInterface*>(tablet);
  }

  return ret;
}

int ObITabletMdsInterface::get_tablet_status(
    const share::SCN &snapshot,
    ObTabletCreateDeleteMdsUserData &data,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(snapshot), K(data), K(timeout), K(*this)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (get_tablet_meta_().tablet_id_.is_ls_intrinsic_tablet()) {
    fill_inner_tablet_status_defaults_(data, nullptr, nullptr);
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else if (get_tablet_meta_().is_empty_shell_) {
    if (CLICK_FAIL(fill_empty_shell_tablet_status_from_meta_(data, nullptr, nullptr))) {
      MDS_LOG(WARN, "fail to fill empty shell tablet status from meta", K(ret), K(data), K(get_tablet_meta_()));
    }
  } else if (CLICK_FAIL(get_snapshot(mds::DummyKey(), data, snapshot, timeout))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get snapshot", K(ret));
    }
  } 
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_latest_tablet_status(
    ObTabletCreateDeleteMdsUserData &data,
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
    fill_inner_tablet_status_defaults_(data, &trans_stat, &trans_version);
  } else if (get_tablet_meta_().is_empty_shell_) {
    if (CLICK_FAIL(fill_empty_shell_tablet_status_from_meta_(data, &trans_stat, &trans_version))) {
      MDS_LOG(WARN, "fail to fill empty shell tablet status from meta", K(ret), K(data), K(get_tablet_meta_()));
    }
  } else if (CLICK_FAIL(get_latest(data, writer, trans_stat, trans_version, nullptr, read_seq))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get latest", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_latest_ddl_data(
    ObTabletBindingMdsUserData &data,
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
  } else if (CLICK_FAIL(get_latest_data_consider_transfer_(
      data, writer, trans_stat, trans_version, nullptr, read_seq))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG(WARN, "fail to get latest", K(ret));
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_ddl_data(
    const share::SCN &snapshot,
    ObTabletBindingMdsUserData &data,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data), K(snapshot), K(timeout)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else if (CLICK_FAIL(get_snapshot_data_consider_transfer_(
      mds::DummyKey(), data, snapshot, timeout, nullptr))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG_GET(WARN, "fail to cross ls get snapshot", K(lbt()));
    } else {
      data.set_default_value(); // use default value
      ret = OB_SUCCESS;
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_autoinc_seq(
    ObIAllocator &allocator,
    const share::SCN &snapshot,
    share::ObTabletAutoincSeq &data,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data), K(snapshot), K(timeout)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (OB_UNLIKELY(!snapshot.is_max())) {
    ret = OB_NOT_SUPPORTED;
    MDS_LOG_GET(WARN, "only support read latest data currently");
  } else if (CLICK_FAIL(get_snapshot_data_consider_transfer_(
      mds::DummyKey(), data, snapshot, timeout, &allocator))) {
    if (OB_EMPTY_RESULT != ret) {
      MDS_LOG_GET(WARN, "fail to cross ls get snapshot", K(lbt()));
    } else {
      data.reset(); // use default value
      ret = OB_SUCCESS;
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::get_split_data(
    ObTabletSplitMdsUserData &data,
    const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(data), K(timeout)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else if (get_tablet_id_().is_ls_intrinsic_tablet()) {
    data.reset();
  } else {
    // TODO(lihongqin.lhq): use get_latest_committed and block during 2pc
    const share::SCN snapshot = share::SCN::max_scn();
    if (CLICK_FAIL(get_snapshot_data_consider_transfer_(
        mds::DummyKey(), data, snapshot, timeout, nullptr))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get snapshot", K(lbt()));
      } else {
        data.reset(); // use default value
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
  #undef PRINT_WRAPPER
}

int ObITabletMdsInterface::split_partkey_compare(const blocksstable::ObDatumRowkey &rowkey,
                                                 const ObITableReadInfo &rowkey_read_info,
                                                 const ObIArray<uint64_t> &partkey_projector,
                                                 int &cmp_ret,
                                                 const int64_t timeout) const
{
  #define PRINT_WRAPPER KR(ret), K(rowkey), K(rowkey_read_info)
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!check_is_inited_())) {
    ret = OB_NOT_INIT;
    MDS_LOG_GET(WARN, "not inited");
  } else {
    // TODO(lihongqin.lhq): use get_latest_committed and block during 2pc
    const share::SCN snapshot = share::SCN::max_scn();
    ObTabletSplitMdsUserData split_data;
    if (CLICK_FAIL(get_snapshot_data_consider_transfer_(
        mds::DummyKey(), split_data, snapshot, timeout, nullptr))) {
      if (OB_EMPTY_RESULT != ret) {
        MDS_LOG_GET(WARN, "fail to cross ls get snapshot", K(ret), K(lbt()));
      }
    } else if (CLICK_FAIL(split_data.partkey_compare(
        rowkey, rowkey_read_info, partkey_projector, cmp_ret))) {
      MDS_LOG_GET(WARN, "fail to compare split partkey", K(ret));
    }
  }

  return ret;
  #undef PRINT_WRAPPER 
}

int ObITabletMdsInterface::get_tablet_handle_from_this(
  ObTabletHandle &tablet_handle) const
{
  int ret = OB_SUCCESS;
  const ObTablet *tablet = static_cast<const ObTablet*>(this);
  const share::ObLSID &ls_id = get_tablet_meta_().ls_id_;
  const common::ObTabletID &tablet_id = get_tablet_meta_().tablet_id_;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_FAIL(t3m->build_tablet_handle_for_mds_scan(const_cast<ObTablet*>(tablet), tablet_handle))) {
    MDS_LOG(WARN, "fail to build tablet handle", K(ret), K(ls_id), K(tablet_id));
  } 
  return ret;
}

} // namespace storage
} // namespace oceanbase
