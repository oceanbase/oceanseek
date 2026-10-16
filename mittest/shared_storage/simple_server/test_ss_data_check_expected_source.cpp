/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <algorithm>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <tuple>
#include <vector>

#include "lib/utility/serialization.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "observer/omt/ob_tenant_mtl_helper.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/share/ob_ss_diagnose_mgr.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_iter_wrapper.h"
#include "storage/ss_data_check/ob_ss_data_check_expected_sort_task.h"
#include "storage/ss_data_check/ob_ss_data_check_meta_ref_checker.h"

#define private public
#include "storage/ss_data_check/ob_ss_data_check_expected_producer.h"
#undef private
#include "storage/ss_data_check/ob_ss_data_check_dag.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace storage
{

class ExpectedSourceStopChecker final
    : public ObCheckStopProvider
{
public:
  ExpectedSourceStopChecker()
    : result_(OB_SUCCESS), result_check_idx_(INT64_MAX), call_count_(0)
  {}

  void set_result(
      const int64_t result_check_idx,
      const int result)
  {
    result_check_idx_ = result_check_idx;
    result_ = result;
    call_count_ = 0;
  }

  virtual int check_stop() const override
  {
    ++call_count_;
    return call_count_ >= result_check_idx_ ? result_ : OB_SUCCESS;
  }

  int64_t get_call_count() const { return call_count_; }

private:
  int result_;
  int64_t result_check_idx_;
  mutable int64_t call_count_;
};

class BlockingExpectedSourceStopChecker final
    : public ObCheckStopProvider
{
public:
  BlockingExpectedSourceStopChecker(
      const ObCheckStopProvider &delegate,
      const int64_t block_check_idx)
    : delegate_(delegate),
      block_check_idx_(block_check_idx),
      call_count_(0),
      is_blocked_(false),
      is_released_(false)
  {}

  virtual int check_stop() const override
  {
    {
      std::unique_lock<std::mutex> guard(lock_);
      ++call_count_;
      if (call_count_ == block_check_idx_) {
        is_blocked_ = true;
        condition_.notify_all();
        condition_.wait(guard, [this]() { return is_released_; });
      }
    }
    return delegate_.check_stop();
  }

  bool wait_until_blocked() const
  {
    std::unique_lock<std::mutex> guard(lock_);
    return condition_.wait_for(
        guard, std::chrono::seconds(30), [this]() { return is_blocked_; });
  }

  void release()
  {
    std::lock_guard<std::mutex> guard(lock_);
    is_released_ = true;
    condition_.notify_all();
  }

  int64_t get_call_count() const
  {
    std::lock_guard<std::mutex> guard(lock_);
    return call_count_;
  }

private:
  const ObCheckStopProvider &delegate_;
  const int64_t block_check_idx_;
  mutable std::mutex lock_;
  mutable std::condition_variable condition_;
  mutable int64_t call_count_;
  mutable bool is_blocked_;
  bool is_released_;
};

struct SelectedTabletVersion final
{
  int64_t ls_id_;
  uint64_t tablet_id_;
  uint64_t reorganize_scn_;
  uint64_t meta_scn_;
  bool is_deleted_;

  bool operator<(const SelectedTabletVersion &other) const
  {
    return std::tie(ls_id_, tablet_id_, reorganize_scn_, meta_scn_, is_deleted_)
        < std::tie(other.ls_id_, other.tablet_id_, other.reorganize_scn_,
            other.meta_scn_, other.is_deleted_);
  }

  bool operator==(const SelectedTabletVersion &other) const
  {
    return ls_id_ == other.ls_id_
        && tablet_id_ == other.tablet_id_
        && reorganize_scn_ == other.reorganize_scn_
        && meta_scn_ == other.meta_scn_
        && is_deleted_ == other.is_deleted_;
  }
};

class TestSSDataCheckExpectedSource
    : public unittest::ObSimpleClusterTestBase
{
public:
  static const int64_t FIRST_LS_ID = 8901;
  static const int64_t SECOND_LS_ID = 8902;
  static const int64_t FUTURE_LS_ID = 8903;
  static const uint64_t FIRST_TABLET_ID = 9901001;
  static const uint64_t SECOND_TABLET_ID = 9901002;
  static const uint64_t THIRD_TABLET_ID = 9901003;
  static const uint64_t FUTURE_LS_TABLET_ID = 9901004;

  TestSSDataCheckExpectedSource()
    : unittest::ObSimpleClusterTestBase(
          "test_ss_data_check_expected_source"),
      meta_service_(nullptr),
      own_meta_service_(false),
      diagnose_info_mgr_(nullptr),
      own_diagnose_info_mgr_(false)
  {}
  virtual ~TestSSDataCheckExpectedSource() = default;

  virtual void SetUp() override;
  virtual void TearDown() override;

  static SCN make_scn(const uint64_t value);
  static int build_head_value(
      ObIAllocator &allocator,
      const bool is_deleted,
      ObString &value);
  static bool is_synthetic_ls(const ObLSID &ls_id);
  static int build_tablet_keys(ObIArray<ObSSMetaTabletKey> &keys);

  int insert_ls_meta(
      const ObLSID &ls_id,
      const SCN &meta_scn,
      const ObString &value);
  int insert_tablet_meta(
      const ObSSMetaTabletKey &key,
      const ObString &value);
  int delete_tablet_meta(
      const ObSSMetaTabletKey &key,
      int64_t &affected_rows);
  int cleanup_synthetic_rows();
  int prepare_synthetic_rows();
  int collect_versions(
      ObSSDataCheckSharedMetaExpectedSource &source,
      const ObSSDataCheckMode mode,
      const SCN &snapshot_meta_scn,
      std::vector<SelectedTabletVersion> &versions);

private:
  ObSSMetaService *meta_service_;
  bool own_meta_service_;
  ObSSDiagnoseInfoMgr *diagnose_info_mgr_;
  bool own_diagnose_info_mgr_;
};

const int64_t TestSSDataCheckExpectedSource::FIRST_LS_ID;
const int64_t TestSSDataCheckExpectedSource::SECOND_LS_ID;
const int64_t TestSSDataCheckExpectedSource::FUTURE_LS_ID;
const uint64_t TestSSDataCheckExpectedSource::FIRST_TABLET_ID;
const uint64_t TestSSDataCheckExpectedSource::SECOND_TABLET_ID;
const uint64_t TestSSDataCheckExpectedSource::THIRD_TABLET_ID;
const uint64_t TestSSDataCheckExpectedSource::FUTURE_LS_TABLET_ID;

SCN TestSSDataCheckExpectedSource::make_scn(const uint64_t value)
{
  SCN scn;
  EXPECT_EQ(OB_SUCCESS, scn.convert_for_logservice(value));
  return scn;
}

int TestSSDataCheckExpectedSource::build_head_value(
    ObIAllocator &allocator,
    const bool is_deleted,
    ObString &value)
{
  int ret = OB_SUCCESS;
  const int64_t outer_version = 1;
  ObSSMetaHeadValueInfo head_info;
  ObSSMetaHeadValue head_value;
  char *buf = nullptr;
  int64_t pos = 0;
  int64_t total_size = 0;
  value.reset();
  if (OB_FAIL(head_info.set(
      ObMetaUpdateReason::CREATE_TABLET, is_deleted))) {
    LOG_WARN("failed to set shared meta head info", KR(ret), K(is_deleted));
  } else if (OB_FAIL(head_value.set_meta_head_info(head_info))) {
    LOG_WARN("failed to set shared meta head value", KR(ret));
  } else if (FALSE_IT(total_size = head_value.get_serialize_size()
      + serialization::encoded_length_vi64(outer_version)
      + serialization::OB_SERIALIZE_SIZE_NEED_BYTES)) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(total_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate shared meta head value", KR(ret), K(total_size));
  } else if (OB_FAIL(serialization::encode_vi64(
      buf, total_size, pos, outer_version))) {
    LOG_WARN("failed to encode shared meta outer version", KR(ret));
  } else {
    const int64_t length_pos = pos;
    const int64_t length_size = serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    pos += length_size;
    const int64_t data_pos = pos;
    if (OB_FAIL(head_value.serialize(buf, total_size, pos))) {
      LOG_WARN("failed to serialize shared meta head value", KR(ret));
    } else {
      int64_t tmp_pos = 0;
      if (OB_FAIL(serialization::encode_fixed_bytes_i64(
          buf + length_pos, length_size, tmp_pos, pos - data_pos))) {
        LOG_WARN("failed to encode shared meta head length", KR(ret));
      } else {
        value.assign_ptr(buf, static_cast<int32_t>(pos));
      }
    }
  }
  return ret;
}

bool TestSSDataCheckExpectedSource::is_synthetic_ls(const ObLSID &ls_id)
{
  return FIRST_LS_ID == ls_id.id()
      || SECOND_LS_ID == ls_id.id()
      || FUTURE_LS_ID == ls_id.id();
}

int TestSSDataCheckExpectedSource::build_tablet_keys(
    ObIArray<ObSSMetaTabletKey> &keys)
{
  int ret = OB_SUCCESS;
  struct TabletVersionDesc final
  {
    int64_t ls_id_;
    uint64_t tablet_id_;
    uint64_t reorganize_scn_;
    uint64_t meta_scn_;
  };
  const TabletVersionDesc descriptions[] = {
    {FIRST_LS_ID, FIRST_TABLET_ID, 10, 100},
    {FIRST_LS_ID, FIRST_TABLET_ID, 10, 200},
    {FIRST_LS_ID, FIRST_TABLET_ID, 10, 300},
    {FIRST_LS_ID, SECOND_TABLET_ID, 20, 150},
    {FIRST_LS_ID, SECOND_TABLET_ID, 20, 250},
    {SECOND_LS_ID, THIRD_TABLET_ID, 30, 180},
    {FUTURE_LS_ID, FUTURE_LS_TABLET_ID, 40, 100}
  };
  keys.reset();
  for (int64_t i = 0;
       OB_SUCC(ret) && i < ARRAYSIZEOF(descriptions);
       ++i) {
    const TabletVersionDesc &description = descriptions[i];
    ObSSMetaTabletKey key;
    key.ls_id_ = ObLSID(description.ls_id_);
    key.tablet_id_ = ObTabletID(description.tablet_id_);
    key.reorganization_scn_ = make_scn(description.reorganize_scn_);
    key.meta_scn_ = make_scn(description.meta_scn_);
    if (OB_UNLIKELY(!key.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid synthetic shared meta tablet key", KR(ret), K(key));
    } else if (OB_FAIL(keys.push_back(key))) {
      LOG_WARN("failed to save synthetic shared meta tablet key", KR(ret), K(key));
    }
  }
  return ret;
}

void TestSSDataCheckExpectedSource::SetUp()
{
  unittest::ObSimpleClusterTestBase::SetUp();
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_FALSE(GCTX.is_shared_storage_mode());
  diagnose_info_mgr_ = MTL(ObSSDiagnoseInfoMgr *);
  ASSERT_EQ(nullptr, diagnose_info_mgr_);
  if (OB_ISNULL(diagnose_info_mgr_)) {
    ASSERT_EQ(OB_SUCCESS, mtl_new_default(diagnose_info_mgr_));
    ASSERT_NE(nullptr, diagnose_info_mgr_);
    ObTenantEnv::get_tenant()->set(diagnose_info_mgr_);
    own_diagnose_info_mgr_ = true;
    ASSERT_EQ(OB_SUCCESS,
        ObSSDiagnoseInfoMgr::mtl_init(diagnose_info_mgr_));
  }
  meta_service_ = MTL(ObSSMetaService *);
  ASSERT_EQ(nullptr, meta_service_);
  if (OB_ISNULL(meta_service_)) {
    ASSERT_EQ(OB_SUCCESS, mtl_new_default(meta_service_));
    ASSERT_NE(nullptr, meta_service_);
    ObTenantEnv::get_tenant()->set(meta_service_);
    own_meta_service_ = true;
    ASSERT_EQ(OB_SUCCESS, ObSSMetaService::mtl_init(meta_service_));
  }
  ASSERT_EQ(OB_SUCCESS, cleanup_synthetic_rows());
}

void TestSSDataCheckExpectedSource::TearDown()
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  const int switch_ret = tenant_guard.switch_to(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, switch_ret);
  if (OB_SUCCESS == switch_ret) {
    EXPECT_EQ(OB_SUCCESS, cleanup_synthetic_rows());
    if (own_meta_service_) {
      mtl_destroy_default(meta_service_);
      ObTenantEnv::get_tenant()->set(
          static_cast<ObSSMetaService *>(nullptr));
      own_meta_service_ = false;
    }
    if (own_diagnose_info_mgr_) {
      mtl_destroy_default(diagnose_info_mgr_);
      ObTenantEnv::get_tenant()->set(
          static_cast<ObSSDiagnoseInfoMgr *>(nullptr));
      own_diagnose_info_mgr_ = false;
    }
  }
  unittest::ObSimpleClusterTestBase::TearDown();
}

int TestSSDataCheckExpectedSource::insert_ls_meta(
    const ObLSID &ls_id,
    const SCN &meta_scn,
    const ObString &value)
{
  int ret = OB_SUCCESS;
  ObSSMetaLSKey key;
  char path_buf[OB_MAX_URI_LENGTH] = {};
  int64_t pos = 0;
  key.ls_id_ = ls_id;
  key.meta_scn_ = meta_scn;
  const blocksstable::MacroBlockId macro_id =
      key.convert_to_macro_id(true /*is_in_table*/);
  if (OB_ISNULL(meta_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta service is null", KR(ret));
  } else if (OB_FAIL(blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, path_buf, sizeof(path_buf), pos))) {
    LOG_WARN("failed to format synthetic ls meta key", KR(ret), K(key));
  } else {
    const ObString meta_key(static_cast<int32_t>(pos), path_buf);
    const ObSSRawMetaRow row(
        OB_SYS_TENANT_ID,
        ls_id,
        static_cast<int64_t>(ObSSMetaTypeEnum::LS_META),
        meta_key,
        value);
    if (OB_FAIL(meta_service_->insert_raw_meta(row))) {
      LOG_WARN("failed to insert synthetic ls meta", KR(ret), K(row));
    }
  }
  return ret;
}

int TestSSDataCheckExpectedSource::insert_tablet_meta(
    const ObSSMetaTabletKey &key,
    const ObString &value)
{
  int ret = OB_SUCCESS;
  const ObSSMetaTabletRow row(
      static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META), key, value);
  if (OB_ISNULL(meta_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta service is null", KR(ret));
  } else if (OB_FAIL(meta_service_->insert_tablet_meta(row))) {
    LOG_WARN("failed to insert synthetic tablet meta", KR(ret), K(row));
  }
  return ret;
}

int TestSSDataCheckExpectedSource::delete_tablet_meta(
    const ObSSMetaTabletKey &key,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSSMetaDeleteParamV2 delete_param;
  affected_rows = 0;
  if (OB_ISNULL(meta_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("shared meta service is null", KR(ret));
  } else {
    delete_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_KEY,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        key.ls_id_, key.tablet_id_, key.meta_scn_,
        key.reorganization_scn_, true /*is_in_table*/);
    if (OB_FAIL(meta_service_->delete_meta_row(delete_param))) {
      LOG_WARN("failed to delete synthetic tablet meta", KR(ret), K(key));
    } else {
      affected_rows = delete_param.affected_rows_;
    }
  }
  return ret;
}

int TestSSDataCheckExpectedSource::cleanup_synthetic_rows()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSEArray<ObSSMetaTabletKey, 8> keys;
  if (OB_ISNULL(meta_service_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(build_tablet_keys(keys))) {
    LOG_WARN("failed to build synthetic tablet keys", KR(ret));
  } else {
    for (int64_t i = 0; i < keys.count(); ++i) {
      const ObSSMetaTabletKey &key = keys.at(i);
      int64_t affected_rows = 0;
      tmp_ret = delete_tablet_meta(key, affected_rows);
      if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
        ret = tmp_ret;
        LOG_WARN("failed to delete synthetic tablet meta", KR(ret), K(key));
      }
    }
    const int64_t ls_ids[] = {FIRST_LS_ID, SECOND_LS_ID, FUTURE_LS_ID};
    const uint64_t ls_meta_scns[] = {50, 50, 300};
    for (int64_t i = 0; i < ARRAYSIZEOF(ls_ids); ++i) {
      tmp_ret = meta_service_->remove_ls(
          ObLSID(ls_ids[i]), make_scn(ls_meta_scns[i]));
      if (OB_SUCCESS != tmp_ret && OB_SUCCESS == ret) {
        ret = tmp_ret;
        LOG_WARN("failed to delete synthetic ls meta", KR(ret), K(ls_ids[i]));
      }
    }
  }
  return ret;
}

int TestSSDataCheckExpectedSource::prepare_synthetic_rows()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DCSourceValue");
  ObString live_value;
  ObString deleted_value;
  ObSEArray<ObSSMetaTabletKey, 8> keys;
  if (OB_FAIL(build_head_value(allocator, false, live_value))) {
    LOG_WARN("failed to build live shared meta value", KR(ret));
  } else if (OB_FAIL(build_head_value(allocator, true, deleted_value))) {
    LOG_WARN("failed to build deleted shared meta value", KR(ret));
  } else if (OB_FAIL(build_tablet_keys(keys))) {
    LOG_WARN("failed to build synthetic tablet keys", KR(ret));
  } else if (OB_FAIL(insert_ls_meta(
      ObLSID(FIRST_LS_ID), make_scn(50), live_value))) {
    LOG_WARN("failed to insert first synthetic ls", KR(ret));
  } else if (OB_FAIL(insert_ls_meta(
      ObLSID(SECOND_LS_ID), make_scn(50), live_value))) {
    LOG_WARN("failed to insert second synthetic ls", KR(ret));
  } else if (OB_FAIL(insert_ls_meta(
      ObLSID(FUTURE_LS_ID), make_scn(300), live_value))) {
    LOG_WARN("failed to insert future synthetic ls", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < keys.count(); ++i) {
    const bool is_deleted = FIRST_LS_ID == keys.at(i).ls_id_.id()
        && FIRST_TABLET_ID == keys.at(i).tablet_id_.id()
        && 200 == keys.at(i).meta_scn_.get_val_for_inner_table_field();
    if (OB_FAIL(insert_tablet_meta(
        keys.at(i), is_deleted ? deleted_value : live_value))) {
      LOG_WARN("failed to insert synthetic tablet version",
          KR(ret), K(i), K(keys.at(i)));
    }
  }
  return ret;
}

int TestSSDataCheckExpectedSource::collect_versions(
    ObSSDataCheckSharedMetaExpectedSource &source,
    const ObSSDataCheckMode mode,
    const SCN &snapshot_meta_scn,
    std::vector<SelectedTabletVersion> &versions)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("DCSourceRows");
  ObSEArray<ObSSRawMetaRow, 4> rows;
  ExpectedSourceStopChecker stop_checker;
  versions.clear();
  if (OB_FAIL(source.open(
      OB_SYS_TENANT_ID, snapshot_meta_scn, mode))) {
    LOG_WARN("failed to open data check expected source", KR(ret), K(mode));
  } else if (OB_FAIL(source.open(
      OB_SYS_TENANT_ID, snapshot_meta_scn, mode))) {
    LOG_WARN("failed to reopen identical data check expected source",
        KR(ret), K(mode));
  } else {
    bool is_end = false;
    while (OB_SUCC(ret) && !is_end) {
      rows.reset();
      allocator.reuse();
      if (OB_FAIL(source.get_next_tablet_batch(
          allocator, 2 /*max_tablet_count*/, 1024 * 1024, stop_checker, rows))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          is_end = true;
        } else {
          LOG_WARN("failed to get expected source batch", KR(ret), K(mode));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
          const ObSSRawMetaRow &row = rows.at(i);
          if (is_synthetic_ls(row.ls_id_)) {
            ObLSID parsed_ls_id;
            ObTabletID tablet_id;
            SCN reorganize_scn;
            SCN meta_scn;
            bool is_deleted = false;
            if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(
                row.meta_key_, parsed_ls_id, tablet_id,
                reorganize_scn, meta_scn))) {
              LOG_WARN("failed to parse selected tablet meta key", KR(ret), K(row));
            } else if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(
                row.meta_value_, is_deleted))) {
              LOG_WARN("failed to parse selected tablet delete marker",
                  KR(ret), K(row));
            } else {
              versions.push_back({
                  row.ls_id_.id(),
                  tablet_id.id(),
                  reorganize_scn.get_val_for_inner_table_field(),
                  meta_scn.get_val_for_inner_table_field(),
                  is_deleted});
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    rows.reset();
    allocator.reuse();
    const int end_ret = source.get_next_tablet_batch(
        allocator, 2, 1024 * 1024, stop_checker, rows);
    if (OB_ITER_END != end_ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expected source did not keep iter end",
          KR(ret), K(end_ret), K(mode));
    } else {
      std::sort(versions.begin(), versions.end());
    }
  }
  return ret;
}

TEST_F(TestSSDataCheckExpectedSource, select_versions_at_fixed_snapshot)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, prepare_synthetic_rows());
  const SCN snapshot_meta_scn = make_scn(250);
  ObSSDataCheckSharedMetaExpectedSource source;
  std::vector<SelectedTabletVersion> actual;

  ASSERT_EQ(OB_SUCCESS, collect_versions(
      source, ObSSDataCheckMode::SINGLE, snapshot_meta_scn, actual));
  std::vector<SelectedTabletVersion> expected_single = {
    {FIRST_LS_ID, FIRST_TABLET_ID, 10, 200, true},
    {FIRST_LS_ID, SECOND_TABLET_ID, 20, 250, false},
    {SECOND_LS_ID, THIRD_TABLET_ID, 30, 180, false}
  };
  std::sort(expected_single.begin(), expected_single.end());
  ASSERT_EQ(expected_single, actual);

  source.reset();
  ASSERT_EQ(OB_SUCCESS, collect_versions(
      source, ObSSDataCheckMode::FULL, snapshot_meta_scn, actual));
  std::vector<SelectedTabletVersion> expected_full = {
    {FIRST_LS_ID, FIRST_TABLET_ID, 10, 100, false},
    {FIRST_LS_ID, FIRST_TABLET_ID, 10, 200, true},
    {FIRST_LS_ID, SECOND_TABLET_ID, 20, 150, false},
    {FIRST_LS_ID, SECOND_TABLET_ID, 20, 250, false},
    {SECOND_LS_ID, THIRD_TABLET_ID, 30, 180, false}
  };
  std::sort(expected_full.begin(), expected_full.end());
  ASSERT_EQ(expected_full, actual);
}

TEST_F(TestSSDataCheckExpectedSource,
    full_source_supports_sequential_worker_handoff)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, prepare_synthetic_rows());
  const SCN snapshot_meta_scn = make_scn(250);
  ObSSDataCheckSharedMetaExpectedSource source;
  ExpectedSourceStopChecker stop_checker;
  std::vector<SelectedTabletVersion> actual;
  ASSERT_EQ(OB_SUCCESS, source.open(
      OB_SYS_TENANT_ID, snapshot_meta_scn, ObSSDataCheckMode::FULL));

  const auto append_synthetic_versions = [&actual](
      const ObSEArray<ObSSRawMetaRow, 4> &rows) -> int {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const ObSSRawMetaRow &row = rows.at(i);
      if (is_synthetic_ls(row.ls_id_)) {
        ObLSID parsed_ls_id;
        ObTabletID tablet_id;
        SCN reorganize_scn;
        SCN meta_scn;
        bool is_deleted = false;
        if (OB_FAIL(ObSSMetaInfoUtil::parse_tablet_meta_key(
            row.meta_key_, parsed_ls_id, tablet_id,
            reorganize_scn, meta_scn))) {
          LOG_WARN("failed to parse handed-off tablet meta key", KR(ret), K(row));
        } else if (OB_FAIL(ObSSMetaHeadValue::check_mark_deleted(
            row.meta_value_, is_deleted))) {
          LOG_WARN("failed to parse handed-off tablet delete marker",
              KR(ret), K(row));
        } else {
          actual.push_back({
              row.ls_id_.id(),
              tablet_id.id(),
              reorganize_scn.get_val_for_inner_table_field(),
              meta_scn.get_val_for_inner_table_field(),
              is_deleted});
        }
      }
    }
    return ret;
  };

  // 第一个 Worker 只读取一个 tablet，随后退出并把 Source 交给其他线程。
  int first_worker_ret = OB_SUCCESS;
  int64_t first_batch_row_count = 0;
  std::thread first_worker([&]() {
    int ret = OB_SUCCESS;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(thread_guard);
    ObArenaAllocator allocator("DCHandoffA");
    ObSEArray<ObSSRawMetaRow, 4> rows;
    if (OB_FAIL(thread_guard.switch_to(OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to switch first handoff worker tenant",
          KR(ret));
    } else if (OB_FAIL(source.get_next_tablet_batch(
        allocator, 1 /*每批最多一个 tablet*/, 1024 * 1024, stop_checker, rows))) {
      LOG_WARN("failed to read first handed-off batch", KR(ret));
    } else {
      first_batch_row_count = rows.count();
      ret = append_synthetic_versions(rows);
    }
    first_worker_ret = ret;
  });
  first_worker.join();
  ASSERT_EQ(OB_SUCCESS, first_worker_ret);
  ASSERT_EQ(1, first_batch_row_count);

  // 第二个 Worker 从同一个 Source 接续读取，直到遍历结束。
  int second_worker_ret = OB_SUCCESS;
  bool second_worker_reached_end = false;
  std::thread second_worker([&]() {
    int ret = OB_SUCCESS;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(thread_guard);
    ObArenaAllocator allocator("DCHandoffB");
    ObSEArray<ObSSRawMetaRow, 4> rows;
    if (OB_FAIL(thread_guard.switch_to(OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to switch second handoff worker tenant",
          KR(ret));
    } else {
      while (OB_SUCC(ret) && !second_worker_reached_end) {
        rows.reset();
        allocator.reuse();
        ret = source.get_next_tablet_batch(
            allocator, 1 /*每批最多一个 tablet*/, 1024 * 1024, stop_checker, rows);
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          second_worker_reached_end = true;
        } else if (OB_SUCCESS != ret) {
          LOG_WARN("failed to read remaining handed-off batches",
              KR(ret));
        } else {
          ret = append_synthetic_versions(rows);
        }
      }
    }
    second_worker_ret = ret;
  });
  second_worker.join();
  ASSERT_EQ(OB_SUCCESS, second_worker_ret);
  ASSERT_TRUE(second_worker_reached_end);

  std::sort(actual.begin(), actual.end());
  std::vector<SelectedTabletVersion> expected_full = {
    {FIRST_LS_ID, FIRST_TABLET_ID, 10, 100, false},
    {FIRST_LS_ID, FIRST_TABLET_ID, 10, 200, true},
    {FIRST_LS_ID, SECOND_TABLET_ID, 20, 150, false},
    {FIRST_LS_ID, SECOND_TABLET_ID, 20, 250, false},
    {SECOND_LS_ID, THIRD_TABLET_ID, 30, 180, false}
  };
  std::sort(expected_full.begin(), expected_full.end());
  ASSERT_EQ(expected_full, actual);
}

TEST_F(TestSSDataCheckExpectedSource,
    source_checks_stop_while_reading_batch)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, prepare_synthetic_rows());
  ObSSDataCheckSharedMetaExpectedSource source;
  ExpectedSourceStopChecker stop_checker;
  ObArenaAllocator allocator("DCSourceStop");
  ObSEArray<ObSSRawMetaRow, 4> rows;
  ASSERT_EQ(OB_SUCCESS, source.open(
      OB_SYS_TENANT_ID, make_scn(250), ObSSDataCheckMode::FULL));
  source.ls_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, source.ls_ids_.push_back(ObLSID(FIRST_LS_ID)));
  stop_checker.set_result(3, OB_CANCELED);
  EXPECT_EQ(OB_CANCELED, source.get_next_tablet_batch(
      allocator, 1024, 16L << 20, stop_checker, rows));
  EXPECT_EQ(3, stop_checker.get_call_count());
  EXPECT_TRUE(rows.empty());

  source.reset();
  rows.reset();
  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS, source.open(
      OB_SYS_TENANT_ID, make_scn(250), ObSSDataCheckMode::FULL));
  source.ls_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, source.ls_ids_.push_back(ObLSID(FIRST_LS_ID)));
  stop_checker.set_result(4, OB_CANCELED);
  EXPECT_EQ(OB_CANCELED, source.get_next_tablet_batch(
      allocator, 1024, 16L << 20, stop_checker, rows));
  EXPECT_EQ(4, stop_checker.get_call_count());
  EXPECT_EQ(1, rows.count());
}

TEST_F(TestSSDataCheckExpectedSource,
    source_observes_concurrent_dag_cancel_while_reading_batch)
{
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, prepare_synthetic_rows());
  static const int64_t MEMORY_LIMIT = 8L << 20;
  static const int64_t FILE_BUF_SIZE = 32L << 10;
  ObSSDataCheckSharedMetaExpectedSource source;
  ObSSDataCheckCtx ctx;
  ObArenaAllocator dag_allocator("DCSrcCancel");
  ObSSDataCheckDag dag;
  ObSSDataCheckExpectedCollectTask collect_task;
  ASSERT_EQ(OB_SUCCESS, ctx.init(
      OB_SYS_TENANT_ID, make_scn(250), ObSSDataCheckMode::FULL,
      MEMORY_LIMIT, FILE_BUF_SIZE, 0 /*io_expire_timestamp*/));
  ASSERT_EQ(OB_SUCCESS, dag.basic_init(dag_allocator));
  dag.set_dag_status(ObIDag::DAG_STATUS_NODE_RUNNING);
  collect_task.set_dag(dag);
  ASSERT_EQ(OB_SUCCESS, collect_task.init(&ctx, &source));
  ASSERT_EQ(OB_SUCCESS, source.open(
      OB_SYS_TENANT_ID, make_scn(250), ObSSDataCheckMode::FULL));
  source.ls_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, source.ls_ids_.push_back(ObLSID(FIRST_LS_ID)));

  // 第三次检查位于 SQL 返回一行之后、写入 batch 之前，此时 Worker
  // 仍持有 Source 锁，用确定性屏障模拟另一个 Worker 并发取消 DAG。
  BlockingExpectedSourceStopChecker stop_checker(collect_task, 3);
  int reader_ret = OB_SUCCESS;
  int64_t reader_row_count = -1;
  bool cancel_worker_reached_barrier = false;
  int source_trylock_ret = OB_SUCCESS;
  std::thread reader([&]() {
    int ret = OB_SUCCESS;
    MAKE_TENANT_SWITCH_SCOPE_GUARD(thread_guard);
    ObArenaAllocator allocator("DCSrcReader");
    ObSEArray<ObSSRawMetaRow, 4> rows;
    if (OB_FAIL(thread_guard.switch_to(OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to switch concurrent source reader tenant", KR(ret));
    } else {
      ret = source.get_next_tablet_batch(
          allocator, 1024, 16L << 20, stop_checker, rows);
    }
    reader_row_count = rows.count();
    reader_ret = ret;
  });
  std::thread cancel_worker([&]() {
    cancel_worker_reached_barrier = stop_checker.wait_until_blocked();
    source_trylock_ret = source.lock_.trylock();
    if (OB_SUCCESS == source_trylock_ret) {
      source.lock_.unlock();
    }
    dag.simply_set_stop();
    stop_checker.release();
  });
  cancel_worker.join();
  reader.join();

  EXPECT_TRUE(cancel_worker_reached_barrier);
  EXPECT_EQ(OB_EAGAIN, source_trylock_ret);
  EXPECT_EQ(OB_CANCELED, reader_ret);
  EXPECT_EQ(3, stop_checker.get_call_count());
  EXPECT_EQ(0, reader_row_count);

  // 取消轮不要求从已推进的游标继续读取，但 Source 必须可以安全重置并复用。
  source.reset();
  ExpectedSourceStopChecker healthy_stop_checker;
  ObArenaAllocator verify_allocator("DCSrcReuse");
  ObSEArray<ObSSRawMetaRow, 4> verify_rows;
  ASSERT_EQ(OB_SUCCESS, source.open(
      OB_SYS_TENANT_ID, make_scn(250), ObSSDataCheckMode::FULL));
  source.ls_ids_.reset();
  ASSERT_EQ(OB_SUCCESS, source.ls_ids_.push_back(ObLSID(FIRST_LS_ID)));
  EXPECT_EQ(OB_SUCCESS, source.get_next_tablet_batch(
      verify_allocator, 1, 1024 * 1024, healthy_stop_checker, verify_rows));
  EXPECT_EQ(1, verify_rows.count());
  source.reset();
  EXPECT_EQ(OB_SUCCESS, ctx.cleanup());
}

static blocksstable::MacroBlockId make_filter_test_object(
    const blocksstable::ObStorageObjectType type,
    const blocksstable::ObMacroBlockIdLocation location)
{
  blocksstable::MacroBlockId macro_id(
      blocksstable::MacroBlockId::DEFAULT_FIRST_ID,
      9901001, 10, 200, 1);
  macro_id.set_storage_object_type(static_cast<uint64_t>(type));
  macro_id.set_id_mode(static_cast<uint64_t>(
      blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
  macro_id.set_location(location);
  return macro_id;
}

TEST_F(TestSSDataCheckExpectedSource, filter_shared_storage_objects)
{
  using blocksstable::ObMacroBlockIdLocation;
  using blocksstable::ObStorageObjectType;
  ASSERT_TRUE(ObSSDataCheckExpectedCollectTask::is_checkable_object_(
      make_filter_test_object(
          ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
          ObMacroBlockIdLocation::OBJECT_STORAGE)));
  ASSERT_TRUE(ObSSDataCheckExpectedCollectTask::is_checkable_object_(
      make_filter_test_object(
          ObStorageObjectType::SHARED_TABLET_SUB_META,
          ObMacroBlockIdLocation::OBJECT_STORAGE)));
  ASSERT_FALSE(ObSSDataCheckExpectedCollectTask::is_checkable_object_(
      make_filter_test_object(
          ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
          ObMacroBlockIdLocation::TABLE)));
  ASSERT_FALSE(ObSSDataCheckExpectedCollectTask::is_checkable_object_(
      make_filter_test_object(
          ObStorageObjectType::PRIVATE_DATA_MACRO,
          ObMacroBlockIdLocation::OBJECT_STORAGE)));
  ASSERT_FALSE(ObSSDataCheckExpectedCollectTask::is_checkable_object_(
      make_filter_test_object(
          ObStorageObjectType::MAJOR_PREWARM_DATA,
          ObMacroBlockIdLocation::OBJECT_STORAGE)));
  ASSERT_FALSE(ObSSDataCheckExpectedCollectTask::is_checkable_object_(
      make_filter_test_object(
          ObStorageObjectType::SHARED_TABLET_SUB_META,
          ObMacroBlockIdLocation::TABLE)));
}

TEST_F(TestSSDataCheckExpectedSource,
    meta_ref_checker_uses_exact_row_and_keeps_mark_deleted_reference)
{
  using blocksstable::ObMacroBlockIdLocation;
  using blocksstable::ObStorageObjectType;
  MAKE_TENANT_SWITCH_SCOPE_GUARD(tenant_guard);
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ASSERT_EQ(OB_SUCCESS, prepare_synthetic_rows());
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_service);

  ObSEArray<ObSSMetaTabletKey, 8> keys;
  ASSERT_EQ(OB_SUCCESS, build_tablet_keys(keys));
  ASSERT_GE(keys.count(), 3);
  const ObSSMetaTabletKey &live_key = keys.at(0);
  const ObSSMetaTabletKey &deleted_key = keys.at(1);
  const ObSSMetaTabletKey &newer_key = keys.at(2);
  ASSERT_EQ(FIRST_LS_ID, deleted_key.ls_id_.id());
  ASSERT_EQ(FIRST_TABLET_ID, deleted_key.tablet_id_.id());
  ASSERT_EQ(10,
      deleted_key.reorganization_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(200, deleted_key.meta_scn_.get_val_for_inner_table_field());
  ASSERT_EQ(deleted_key.ls_id_, newer_key.ls_id_);
  ASSERT_EQ(deleted_key.tablet_id_, newer_key.tablet_id_);
  ASSERT_EQ(deleted_key.reorganization_scn_, newer_key.reorganization_scn_);
  ASSERT_EQ(300, newer_key.meta_scn_.get_val_for_inner_table_field());

  ObArenaAllocator allocator("DCMetaRef");
  ObSSMetaTabletRow deleted_row;
  bool is_mark_deleted = false;
  ASSERT_EQ(OB_SUCCESS,
      meta_service->read_tablet_meta(deleted_key, allocator, deleted_row));
  ASSERT_EQ(OB_SUCCESS, ObSSMetaHeadValue::check_mark_deleted(
      deleted_row.value_, is_mark_deleted));
  ASSERT_TRUE(is_mark_deleted);

  const blocksstable::MacroBlockId macro_id = make_filter_test_object(
      ObStorageObjectType::SHARED_MAJOR_DATA_MACRO,
      ObMacroBlockIdLocation::OBJECT_STORAGE);
  ObSSDataCheckExpectedItem live_item;
  ObSSDataCheckExpectedItem deleted_item;
  ObSSDataCheckExpectedItem newer_item;
  ASSERT_EQ(OB_SUCCESS, live_item.init(
      macro_id, live_key.ls_id_, live_key.tablet_id_,
      live_key.reorganization_scn_, live_key.meta_scn_));
  ASSERT_EQ(OB_SUCCESS, deleted_item.init(
      macro_id, deleted_key.ls_id_, deleted_key.tablet_id_,
      deleted_key.reorganization_scn_, deleted_key.meta_scn_));
  ASSERT_EQ(OB_SUCCESS, newer_item.init(
      macro_id, newer_key.ls_id_, newer_key.tablet_id_,
      newer_key.reorganization_scn_, newer_key.meta_scn_));

  ObSSDataCheckMetaRefChecker checker;
  ASSERT_EQ(OB_SUCCESS, checker.init(*meta_service));
  bool reference_exist = false;
  ASSERT_EQ(OB_SUCCESS,
      checker.check_reference_exist(live_item, reference_exist));
  ASSERT_TRUE(reference_exist);
  reference_exist = false;
  ASSERT_EQ(OB_SUCCESS,
      checker.check_reference_exist(deleted_item, reference_exist));
  ASSERT_TRUE(reference_exist);

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, delete_tablet_meta(deleted_key, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ObSSMetaTabletRow absent_row;
  allocator.reuse();
  ASSERT_EQ(OB_ENTRY_NOT_EXIST,
      meta_service->read_tablet_meta(deleted_key, allocator, absent_row));
  reference_exist = true;
  ASSERT_EQ(OB_SUCCESS,
      checker.check_reference_exist(deleted_item, reference_exist));
  ASSERT_FALSE(reference_exist);

  ObSSMetaTabletRow newer_row;
  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS,
      meta_service->read_tablet_meta(newer_key, allocator, newer_row));
  reference_exist = false;
  ASSERT_EQ(OB_SUCCESS,
      checker.check_reference_exist(newer_item, reference_exist));
  ASSERT_TRUE(reference_exist);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
