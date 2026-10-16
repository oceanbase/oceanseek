
/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define protected public
#define private public

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_binding_info.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/tablet/ob_tablet_multi_source_data.h"

namespace oceanbase
{
namespace unittest
{

using namespace storage;
using namespace common;
using namespace share;

#define ALLOC_AND_INIT(allocator, addr, args...)                                  \
  do {                                                                            \
    if (OB_SUCC(ret)) {                                                           \
      if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, addr.ptr_))) {  \
        LOG_WARN("fail to allocate and new object", K(ret));                      \
      } else if (OB_FAIL(addr.get_ptr()->init(allocator, args))) {                \
        LOG_WARN("fail to initialize tablet member", K(ret), K(addr));            \
      }                                                                           \
    }                                                                             \
  } while (false)                                                                 \

class ObTabletMetaV1 final
{
public:
  int64_t get_serialize_size() const;
  int serialize(char *buf, const int64_t len, int64_t &pos) const;

public:
  static const int32_t TABLET_META_VERSION_V1 = 1;

  int32_t version_;
  int32_t length_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID data_tablet_id_;
  common::ObTabletID ref_tablet_id_;
  bool has_next_tablet_;
  share::SCN create_scn_;
  share::SCN start_scn_;
  share::SCN clog_checkpoint_scn_;
  share::SCN ddl_checkpoint_scn_;
  int64_t snapshot_version_;
  int64_t multi_version_start_;
  lib::Worker::CompatMode compat_mode_;
  share::ObTabletAutoincSeq autoinc_seq_;
  ObTabletHAStatus ha_status_;
  ObTabletReportStatus report_status_;
  ObTabletTxMultiSourceDataUnit tx_data_;
  ObTabletBindingInfo ddl_data_;
  ObTabletTableStoreFlag table_store_flag_;
  share::SCN ddl_start_scn_;
  int64_t ddl_snapshot_version_;
  int64_t max_sync_storage_schema_version_;
  int64_t ddl_execution_id_;
  int64_t ddl_cluster_version_;
  int64_t max_serialized_medium_scn_;
  // keep member same to 4.1 !!!
};

int ObTabletMetaV1::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  const int64_t length = get_serialize_size();

  if (OB_UNLIKELY(length > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - new_pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, len, new_pos, version_))) {
    LOG_WARN("failed to serialize tablet meta's version", K(ret), K(len), K(new_pos), K_(version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i32(buf, len, new_pos, length))) {
    LOG_WARN("failed to serialize tablet meta's length", K(ret), K(len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(ls_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ls id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(data_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize data tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ref_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ref tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_bool(buf, len, new_pos, has_next_tablet_))) {
    LOG_WARN("failed to serialize has next tablet", K(ret), K(len), K(new_pos), K_(has_next_tablet));
  } else if (new_pos - pos < length && OB_FAIL(create_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize create scn", K(ret), K(len), K(new_pos), K_(create_scn));
  } else if (new_pos - pos < length && OB_FAIL(start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize start scn", K(ret), K(len), K(new_pos), K_(start_scn));
  } else if (new_pos - pos < length && OB_FAIL(clog_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(clog_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(ddl_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl checkpoint ts", K(ret), K(len), K(new_pos), K_(ddl_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, snapshot_version_))) {
    LOG_WARN("failed to serialize snapshot version", K(ret), K(len), K(new_pos), K_(snapshot_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, multi_version_start_))) {
    LOG_WARN("failed to serialize multi version start", K(ret), K(len), K(new_pos), K_(multi_version_start));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(compat_mode_)))) {
    LOG_WARN("failed to serialize compat mode", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(autoinc_seq_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize auto inc seq", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ha_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ha status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(report_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize report status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(tx_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize multi source data", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl data", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(table_store_flag_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize table store flag", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl start log ts", K(ret), K(len), K(new_pos), K_(ddl_start_scn));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_snapshot_version_))) {
    LOG_WARN("failed to serialize ddl snapshot version", K(ret), K(len), K(new_pos), K_(ddl_snapshot_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_sync_storage_schema_version_))) {
    LOG_WARN("failed to serialize max_sync_storage_schema_version", K(ret), K(len), K(new_pos), K_(max_sync_storage_schema_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_execution_id_))) {
    LOG_WARN("failed to serialize ddl execution id", K(ret), K(len), K(new_pos), K_(ddl_execution_id));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_cluster_version_))) {
    LOG_WARN("failed to serialize ddl cluster version", K(ret), K(len), K(new_pos), K_(ddl_cluster_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_serialized_medium_scn_))) {
    LOG_WARN("failed to serialize max_serialized_medium_scn", K(ret), K(len), K(new_pos), K_(max_serialized_medium_scn));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta's length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length));
  } else {
    pos = new_pos;
  }

  return ret;
}

int64_t ObTabletMetaV1::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i32(version_);
  size += serialization::encoded_length_i32(length_);
  size += ls_id_.get_serialize_size();
  size += tablet_id_.get_serialize_size();
  size += data_tablet_id_.get_serialize_size();
  size += ref_tablet_id_.get_serialize_size();
  size += serialization::encoded_length_bool(has_next_tablet_);
  size += create_scn_.get_fixed_serialize_size();
  size += start_scn_.get_fixed_serialize_size();
  size += clog_checkpoint_scn_.get_fixed_serialize_size();
  size += ddl_checkpoint_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(snapshot_version_);
  size += serialization::encoded_length_i64(multi_version_start_);
  size += serialization::encoded_length_i8(static_cast<int8_t>(compat_mode_));
  size += autoinc_seq_.get_serialize_size();
  size += ha_status_.get_serialize_size();
  size += report_status_.get_serialize_size();
  size += tx_data_.get_serialize_size();
  size += ddl_data_.get_serialize_size();
  size += table_store_flag_.get_serialize_size();
  size += ddl_start_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(ddl_snapshot_version_);
  size += serialization::encoded_length_i64(max_sync_storage_schema_version_);
  size += serialization::encoded_length_i64(ddl_execution_id_);
  size += serialization::encoded_length_i64(ddl_cluster_version_);
  size += serialization::encoded_length_i64(max_serialized_medium_scn_);
  return size;
}

struct ObMigrationTabletParamV1 final
{
public:
  int serialize(char *buf, const int64_t len, int64_t &pos) const;
  int deserialize(const char *buf, const int64_t len, int64_t &pos);
  int64_t get_serialize_size() const;

  // magic_number_ is added to support upgrade from old format(without version and length compatibility)
  // The old format first member is ls_id_(also 8 bytes long), which is not possible be a negative number.
  const static int64_t MAGIC_NUM = -20230111;
  const static int64_t PARAM_VERSION = 1;

  TO_STRING_KV(K_(magic_number),
               K_(version),
               K_(ls_id),
               K_(tablet_id),
               K_(data_tablet_id),
               K_(ref_tablet_id),
               K_(create_scn),
               K_(start_scn),
               K_(clog_checkpoint_scn),
               K_(ddl_checkpoint_scn),
               K_(ddl_snapshot_version),
               K_(ddl_start_scn),
               K_(snapshot_version),
               K_(multi_version_start),
               K_(autoinc_seq),
               K_(compat_mode),
               K_(ha_status),
               K_(report_status),
               K_(tx_data),
               K_(ddl_data),
               K_(storage_schema),
               K_(medium_info_list),
               K_(table_store_flag),
               K_(max_sync_storage_schema_version),
               K_(max_serialized_medium_scn),
               K_(ddl_commit_scn));

public:
  int64_t magic_number_;
  int64_t version_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  common::ObTabletID data_tablet_id_;
  common::ObTabletID ref_tablet_id_;
  share::SCN create_scn_;
  share::SCN start_scn_;              // for migration
  share::SCN clog_checkpoint_scn_;
  share::SCN ddl_checkpoint_scn_;
  int64_t snapshot_version_;
  int64_t multi_version_start_;
  lib::Worker::CompatMode compat_mode_;
  share::ObTabletAutoincSeq autoinc_seq_;
  ObTabletHAStatus ha_status_;
  ObTabletReportStatus report_status_;
  ObTabletTxMultiSourceDataUnit tx_data_;
  ObTabletBindingInfo ddl_data_;
  ObStorageSchema storage_schema_;
  compaction::ObMediumCompactionInfoList medium_info_list_;
  ObTabletTableStoreFlag table_store_flag_;
  share::SCN ddl_start_scn_;
  int64_t ddl_snapshot_version_;
  // max_sync_version may less than storage_schema.schema_version_ when major update schema
  int64_t max_sync_storage_schema_version_;
  int64_t ddl_execution_id_;
  int64_t ddl_data_format_version_;
  int64_t max_serialized_medium_scn_;
  share::SCN ddl_commit_scn_;

  // Add new serialization member before this line, below members won't serialize
  common::ObArenaAllocator allocator_; // for storage schema
};

int ObMigrationTabletParamV1::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t length = 0;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (FALSE_IT(length = get_serialize_size())) {
  } else if (OB_UNLIKELY(length > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, magic_number_))) {
    LOG_WARN("failed to serialize magic number", K(ret), K(len), K(new_pos), K_(magic_number));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, version_))) {
    LOG_WARN("failed to serialize version", K(ret), K(len), K(new_pos), K_(version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, length))) {
    LOG_WARN("failed to serialize length", K(ret), K(len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(ls_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ls id", K(ret), K(len), K(new_pos), K_(ls_id));
  } else if (new_pos - pos < length && OB_FAIL(tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet id", K(ret), K(len), K(new_pos), K_(tablet_id));
  } else if (new_pos - pos < length && OB_FAIL(data_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize data tablet id", K(ret), K(len), K(new_pos), K_(data_tablet_id));
  } else if (new_pos - pos < length && OB_FAIL(ref_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ref tablet id", K(ret), K(len), K(new_pos), K_(ref_tablet_id));
  } else if (new_pos - pos < length && OB_FAIL(create_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(create_scn));
  } else if (new_pos - pos < length && OB_FAIL(start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize start scn", K(ret), K(len), K(new_pos), K_(start_scn));
  } else if (new_pos - pos < length && OB_FAIL(clog_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(clog_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(ddl_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl checkpoint ts", K(ret), K(len), K(new_pos), K_(ddl_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, snapshot_version_))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(snapshot_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, multi_version_start_))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(multi_version_start));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(compat_mode_)))) {
    LOG_WARN("failed to serialize compat mode", K(ret), K(len), K(new_pos), K_(compat_mode));
  } else if (new_pos - pos < length && OB_FAIL(autoinc_seq_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize auto inc seq", K(ret), K(len), K(new_pos), K_(autoinc_seq));
  } else if (new_pos - pos < length && OB_FAIL(ha_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ha status", K(ret), K(len), K(new_pos), K_(ha_status));
  } else if (new_pos - pos < length && OB_FAIL(report_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize report status", K(ret), K(len), K(new_pos), K_(report_status));
  } else if (new_pos - pos < length && OB_FAIL(tx_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize multi source data", K(ret), K(len), K(new_pos), K_(tx_data));
  } else if (new_pos - pos < length && OB_FAIL(ddl_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl data", K(ret), K(len), K(new_pos), K_(ddl_data));
  } else if (new_pos - pos < length && OB_FAIL(storage_schema_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize storage schema", K(ret), K(len), K(new_pos), K_(storage_schema));
  } else if (new_pos - pos < length && OB_FAIL(medium_info_list_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize medium compaction list", K(ret), K(len), K(new_pos), K_(medium_info_list));
  } else if (new_pos - pos < length && OB_FAIL(table_store_flag_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize table store flag", K(ret), K(len), K(new_pos), K_(table_store_flag));
  } else if (new_pos - pos < length && OB_FAIL(ddl_start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl start log ts", K(ret), K(len), K(new_pos), K_(ddl_start_scn));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_snapshot_version_))) {
    LOG_WARN("failed to serialize ddl snapshot version", K(ret), K(len), K(new_pos), K_(ddl_snapshot_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_sync_storage_schema_version_))) {
    LOG_WARN("failed to serialize max_sync_storage_schema_version", K(ret), K(len), K(new_pos), K_(max_sync_storage_schema_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_execution_id_))) {
    LOG_WARN("failed to serialize ddl execution id", K(ret), K(len), K(new_pos), K_(ddl_execution_id));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_data_format_version_))) {
    LOG_WARN("failed to serialize ddl data format version", K(ret), K(len), K(new_pos), K_(ddl_data_format_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_serialized_medium_scn_))) {
    LOG_WARN("failed to serialize max_serialized_medium_scn", K(ret), K(len), K(new_pos), K_(max_serialized_medium_scn));
  } else if (new_pos - pos < length && OB_FAIL(ddl_commit_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl commit scn", K(ret), K(len), K(new_pos), K_(ddl_commit_scn));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length));
  } else {
    pos = new_pos;
  }

  return ret;
}

int64_t ObMigrationTabletParamV1::get_serialize_size() const
{
  int64_t size = 0;
  int64_t length = 0;
  size += serialization::encoded_length_i64(magic_number_);
  size += serialization::encoded_length_i64(version_);
  size += serialization::encoded_length_i64(length);
  size += ls_id_.get_serialize_size();
  size += tablet_id_.get_serialize_size();
  size += data_tablet_id_.get_serialize_size();
  size += ref_tablet_id_.get_serialize_size();
  size += create_scn_.get_fixed_serialize_size();
  size += start_scn_.get_fixed_serialize_size();
  size += clog_checkpoint_scn_.get_fixed_serialize_size();
  size += ddl_checkpoint_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(snapshot_version_);
  size += serialization::encoded_length_i64(multi_version_start_);
  size += serialization::encoded_length_i8(static_cast<int8_t>(compat_mode_));
  size += autoinc_seq_.get_serialize_size();
  size += ha_status_.get_serialize_size();
  size += report_status_.get_serialize_size();
  size += tx_data_.get_serialize_size();
  size += ddl_data_.get_serialize_size();
  size += storage_schema_.get_serialize_size();
  size += medium_info_list_.get_serialize_size();
  size += table_store_flag_.get_serialize_size();
  size += ddl_start_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(ddl_snapshot_version_);
  size += serialization::encoded_length_i64(max_sync_storage_schema_version_);
  size += serialization::encoded_length_i64(ddl_execution_id_);
  size += serialization::encoded_length_i64(ddl_data_format_version_);
  size += serialization::encoded_length_i64(max_serialized_medium_scn_);
  size += ddl_commit_scn_.get_fixed_serialize_size();
  return size;
}

class TestTablet : public ::testing::Test
{
public:
  TestTablet();
  virtual ~TestTablet();
  virtual void SetUp();
  virtual void TearDown();
  void pull_ddl_memtables(ObIArray<ObDDLKV *> &ddl_kvs)
  {
    for (int64_t i = 0; i < ddl_kv_count_; ++i) {
      ASSERT_EQ(OB_SUCCESS, ddl_kvs.push_back(ddl_kvs_[i]));
    }
    std::cout<< "pull ddl memtables:" << ddl_kv_count_ << std::endl;
  }
  void reproducing_bug();
private:
  ObArenaAllocator allocator_;
  ObDDLKV **ddl_kvs_;
  volatile int64_t ddl_kv_count_;
};

TestTablet::TestTablet()
  : ddl_kvs_(nullptr),
    ddl_kv_count_(0)
{
}

TestTablet::~TestTablet()
{
}

void TestTablet::SetUp()
{
  OB_LOG(INFO, "ObTabletMeta", K(sizeof(ObTabletMeta)),
        K(sizeof(ObTabletHAStatus)), K(sizeof(ObTabletReportStatus)), K(sizeof(ObTabletTableStoreFlag)),
        K(sizeof(ObTabletSpaceUsage)), K(sizeof(lib::Worker::CompatMode)));

  const int64_t tablet_size = sizeof(ObTablet);
  const int64_t rowkey_size = sizeof(ObRowkeyReadInfo);
  const int64_t assert_size = tablet_size + rowkey_size;

  OB_LOG(INFO, "ObTablet", K(assert_size), K(tablet_size), K(rowkey_size),
        K(sizeof(ObTabletHandle)),
        K(sizeof(ObTabletComplexAddr<ObTabletMacroInfo>)), K(sizeof(ObTabletPointerHandle)),
        K(sizeof(ObMetaDiskAddr)), K(sizeof(common::SpinRWLock)), K(sizeof(ObTabletStatusCache)),
        K(sizeof(ObDDLInfoCache)), K(sizeof(ObTableStoreCache)));
}

void TestTablet::TearDown()
{
}

TEST_F(TestTablet, test_async_index_refresh_target_flags)
{
  ObTableStoreCache cache;

  ASSERT_FALSE(cache.is_async_index_refresh_target());
  ASSERT_FALSE(cache.is_async_inverted_storage_index());
  ASSERT_FALSE(cache.is_async_delete_index());

  cache.is_async_index_refresh_target_ = true;
  ASSERT_TRUE(cache.is_async_index_refresh_target());
  ASSERT_TRUE(cache.is_async_inverted_storage_index());
  ASSERT_FALSE(cache.is_async_delete_index());

  cache.is_async_delete_index_ = true;
  ASSERT_TRUE(cache.is_async_index_refresh_target());
  ASSERT_FALSE(cache.is_async_inverted_storage_index());
  ASSERT_TRUE(cache.is_async_delete_index());
}

TEST_F(TestTablet, test_serialize_meta_compat)
{
  int ret = OB_SUCCESS;
  ObTabletMetaV1 tablet_meta;
  tablet_meta.version_ = ObTabletMetaV1::TABLET_META_VERSION_V1;
  tablet_meta.ls_id_ = ObLSID(3);
  tablet_meta.tablet_id_ = ObTabletID(1);
  tablet_meta.data_tablet_id_ = ObTabletID(4);
  tablet_meta.ref_tablet_id_ = ObTabletID(1);
  bool has_next_tablet_ = true;
  tablet_meta.create_scn_ = share::SCN::base_scn();
  tablet_meta.start_scn_ = share::SCN::base_scn();
  tablet_meta.clog_checkpoint_scn_ = share::SCN::base_scn();
  tablet_meta.ddl_checkpoint_scn_ = share::SCN::base_scn();
  tablet_meta.snapshot_version_ = 5;
  tablet_meta.multi_version_start_ = 9;
  tablet_meta.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  tablet_meta.autoinc_seq_.set_autoinc_seq_value(allocator_, 1);
  ASSERT_EQ(OB_SUCCESS, tablet_meta.ha_status_.init_status());
  tablet_meta.ddl_start_scn_ = share::SCN::base_scn();
  tablet_meta.ddl_snapshot_version_ = 3;
  tablet_meta.max_sync_storage_schema_version_ = 1;
  tablet_meta.ddl_execution_id_ = 4;
  tablet_meta.ddl_cluster_version_ = 1;
  tablet_meta.max_serialized_medium_scn_ = 5;
  tablet_meta.length_ = tablet_meta.get_serialize_size();
  tablet_meta.tx_data_.tablet_status_ = ObTabletStatus::NORMAL;

  char *buf = nullptr;
  int64_t len = tablet_meta.get_serialize_size();
  buf = reinterpret_cast<char*>(allocator_.alloc(len));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_meta.serialize(buf, len, pos));

  pos = 0;
  ObTablet tablet;
  ObTabletAutoincSeq autoinc_seq;
  ObTabletTxMultiSourceDataUnit tx_data;
  ObTabletBindingInfo ddl_data;
  ASSERT_EQ(OB_SUCCESS,
            tablet.deserialize_meta_v1(allocator_, buf, len, pos, autoinc_seq, tx_data, ddl_data));
  uint64_t autoinc_seq_v1;
  ASSERT_EQ(OB_SUCCESS, tablet_meta.autoinc_seq_.get_autoinc_seq_value(autoinc_seq_v1));
  uint64_t autoinc_seq_v2;
  ASSERT_EQ(OB_SUCCESS, autoinc_seq.get_autoinc_seq_value(autoinc_seq_v2));
  ASSERT_EQ(autoinc_seq_v1, autoinc_seq_v2);
  ASSERT_EQ(tx_data.tablet_status_, ObTabletStatus::NORMAL);
}

TEST_F(TestTablet, test_tablet_meta_mds_start_scn_serialize)
{
  ObTabletMeta tablet_meta;
  ObTabletTableStoreFlag table_store_flag;
  share::ObSplitTabletInfo split_info;
  const share::SCN mds_start_scn = share::SCN::base_scn();
  share::SCN min_minor_meta_scn;
  share::SCN min_inc_major_meta_scn;
  const uint64_t data_version = DATA_VERSION_4_6_2_1;
  ASSERT_EQ(OB_SUCCESS, min_minor_meta_scn.convert_for_tx(20));
  ASSERT_EQ(OB_SUCCESS, min_inc_major_meta_scn.convert_for_tx(30));

  ASSERT_EQ(OB_SUCCESS,
            tablet_meta.init(ObLSID(3), ObTabletID(1), ObTabletID(4),
                             share::SCN::base_scn(), 5, lib::Worker::CompatMode::MYSQL,
                             table_store_flag, 1, share::SCN::base_scn(), split_info,
                             share::ObForkTabletInfo(), false, false, false, false, 0));
  tablet_meta.mds_start_scn_ = mds_start_scn;
  tablet_meta.min_minor_meta_scn_ = min_minor_meta_scn;
  tablet_meta.min_inc_major_meta_scn_ = min_inc_major_meta_scn;

  const int64_t len = tablet_meta.get_serialize_size(data_version);
  char *buf = reinterpret_cast<char *>(allocator_.alloc(len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_meta.serialize(data_version, buf, len, pos));
  ASSERT_EQ(len, pos);

  ObTabletMeta new_tablet_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_tablet_meta.deserialize(buf, len, pos));
  ASSERT_EQ(len, pos);
  ASSERT_EQ(mds_start_scn, new_tablet_meta.mds_start_scn_);
  ASSERT_EQ(min_minor_meta_scn, new_tablet_meta.min_minor_meta_scn_);
  ASSERT_EQ(min_inc_major_meta_scn, new_tablet_meta.min_inc_major_meta_scn_);
}

TEST_F(TestTablet, test_tablet_meta_min_minor_meta_scn)
{
  ObTabletMeta tablet_meta;
  ObTabletTableStoreFlag table_store_flag;
  share::ObSplitTabletInfo split_info;
  share::SCN min_minor_meta_scn;
  share::SCN min_inc_major_meta_scn;
  ASSERT_EQ(OB_SUCCESS, min_minor_meta_scn.convert_for_tx(20));
  ASSERT_EQ(OB_SUCCESS, min_inc_major_meta_scn.convert_for_tx(30));

  ASSERT_EQ(OB_SUCCESS,
            tablet_meta.init(ObLSID(3), ObTabletID(1), ObTabletID(4),
                             share::SCN::base_scn(), 5, lib::Worker::CompatMode::MYSQL,
                             table_store_flag, 1, share::SCN::base_scn(), split_info,
                             share::ObForkTabletInfo(), false, false, false, false, 0));
  ASSERT_TRUE(tablet_meta.is_valid());
  tablet_meta.min_minor_meta_scn_.reset();
  ASSERT_FALSE(tablet_meta.is_valid());
  tablet_meta.min_minor_meta_scn_.set_min();
  ASSERT_FALSE(tablet_meta.is_valid());
  tablet_meta.min_minor_meta_scn_ = min_minor_meta_scn;
  ASSERT_TRUE(tablet_meta.is_valid());
  tablet_meta.min_minor_meta_scn_.set_max();
  ASSERT_TRUE(tablet_meta.is_valid());
  tablet_meta.min_inc_major_meta_scn_.reset();
  ASSERT_FALSE(tablet_meta.is_valid());
  tablet_meta.min_inc_major_meta_scn_.set_min();
  ASSERT_FALSE(tablet_meta.is_valid());
  tablet_meta.min_inc_major_meta_scn_ = min_inc_major_meta_scn;
  ASSERT_TRUE(tablet_meta.is_valid());
  tablet_meta.min_inc_major_meta_scn_.set_max();
  ASSERT_TRUE(tablet_meta.is_valid());

#ifdef OB_BUILD_SHARED_STORAGE
  tablet_meta.min_minor_meta_scn_ = min_minor_meta_scn;
  tablet_meta.min_inc_major_meta_scn_ = min_inc_major_meta_scn;
  ObTabletMeta shared_tablet_meta;
  ASSERT_EQ(OB_SUCCESS, shared_tablet_meta.init_for_shared_storage_first_creation(tablet_meta));
  ASSERT_TRUE(shared_tablet_meta.min_minor_meta_scn_.is_max());
  ASSERT_TRUE(shared_tablet_meta.min_inc_major_meta_scn_.is_max());
#endif
}

TEST_F(TestTablet, test_table_store_cache_inc_major_table_count)
{
  const uint64_t old_data_version = DATA_VERSION_4_6_2_0;
  const uint64_t new_data_version = DATA_VERSION_4_6_2_1;
  ObTableStoreCache cache;
  cache.version_ = ObTableStoreCache::TABLET_STORE_CACHE_VERSION;
  cache.is_inited_ = true;
  cache.min_recycle_scn_.set_min();
  cache.is_async_index_refresh_target_ = true;
  cache.is_async_delete_index_ = true;
  cache.inc_major_table_cnt_ = 0;
  const int64_t merge_info = cache.merge_info_;
  cache.inc_major_table_cnt_ = 2;
  ASSERT_EQ(merge_info, cache.merge_info_);
  cache.inc_major_table_cnt_ = 0;

  const int64_t new_len = cache.get_serialize_size(new_data_version);
  char *buf = reinterpret_cast<char *>(allocator_.alloc(new_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, cache.serialize(new_data_version, buf, new_len, pos));
  ASSERT_EQ(new_len, pos);

  ObTableStoreCache decoded_cache;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_cache.deserialize(buf, new_len, pos));
  ASSERT_EQ(new_len, pos);
  EXPECT_TRUE(decoded_cache.is_async_index_refresh_target_);
  EXPECT_TRUE(decoded_cache.is_async_delete_index_);
  EXPECT_EQ(0, decoded_cache.inc_major_table_cnt_);

  cache.inc_major_table_cnt_ = 2;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, cache.serialize(new_data_version, buf, new_len, pos));
  ASSERT_EQ(new_len, pos);
  ObTableStoreCache count_cache;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, count_cache.deserialize(buf, new_len, pos));
  ASSERT_EQ(new_len, pos);
  EXPECT_EQ(2, count_cache.inc_major_table_cnt_);
  cache.inc_major_table_cnt_ = 0;

  const int64_t old_len = cache.get_serialize_size(old_data_version);
  ASSERT_EQ(new_len - serialization::encoded_length_i64(cache.inc_major_table_cnt_), old_len);
  char *old_buf = reinterpret_cast<char *>(allocator_.alloc(old_len));
  ASSERT_NE(nullptr, old_buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, cache.serialize(old_data_version, old_buf, old_len, pos));
  ASSERT_EQ(old_len, pos);

  ObTableStoreCache old_cache;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_cache.deserialize(old_buf, old_len, pos));
  ASSERT_EQ(old_len, pos);
  EXPECT_TRUE(old_cache.is_async_index_refresh_target_);
  EXPECT_TRUE(old_cache.is_async_delete_index_);
  EXPECT_EQ(1, old_cache.inc_major_table_cnt_);
}

#ifdef OB_BUILD_SHARED_STORAGE
TEST_F(TestTablet, test_ss_min_inc_major_meta_scn)
{
  ObTablet tablet;
  share::SCN reorg_scn;
  share::SCN row_scn;
  ASSERT_EQ(OB_SUCCESS, reorg_scn.convert_for_tx(10));
  ASSERT_EQ(OB_SUCCESS, row_scn.convert_for_tx(20));
  ASSERT_EQ(OB_SUCCESS, tablet.tablet_addr_.set_sslog_tablet_meta_addr(
      ObLSID(1), ObTabletID(200001), reorg_scn, row_scn, 0, 4096));

  tablet.table_store_cache_.is_inited_ = true;
  tablet.table_store_cache_.inc_major_table_cnt_ = 2;
  EXPECT_EQ(2, tablet.get_inc_major_table_count());
  tablet.tablet_meta_.min_inc_major_meta_scn_.set_max();
  ASSERT_EQ(OB_SUCCESS, tablet.set_ss_min_inc_major_meta_scn_());
  EXPECT_EQ(row_scn, tablet.tablet_meta_.min_inc_major_meta_scn_);

  tablet.table_store_cache_.inc_major_table_cnt_ = 0;
  EXPECT_EQ(0, tablet.get_inc_major_table_count());
  ASSERT_EQ(OB_SUCCESS, tablet.set_ss_min_inc_major_meta_scn_());
  EXPECT_TRUE(tablet.tablet_meta_.min_inc_major_meta_scn_.is_max());

  tablet.table_store_cache_.is_inited_ = false;
  ASSERT_EQ(OB_SUCCESS, tablet.set_ss_min_inc_major_meta_scn_());
  EXPECT_EQ(row_scn, tablet.tablet_meta_.min_inc_major_meta_scn_);
}

TEST_F(TestTablet, test_ss_get_min_tablet_rely_info)
{
  ObTablet first_tablet;
  ObTablet second_tablet;
  share::SCN first_minor_meta_scn;
  share::SCN second_minor_meta_scn;
  share::SCN first_inc_major_meta_scn;
  share::SCN second_inc_major_meta_scn;
  ASSERT_EQ(OB_SUCCESS, first_minor_meta_scn.convert_for_tx(300));
  ASSERT_EQ(OB_SUCCESS, second_minor_meta_scn.convert_for_tx(400));
  ASSERT_EQ(OB_SUCCESS, first_inc_major_meta_scn.convert_for_tx(500));
  ASSERT_EQ(OB_SUCCESS, second_inc_major_meta_scn.convert_for_tx(200));
  first_tablet.tablet_meta_.min_minor_meta_scn_ = first_minor_meta_scn;
  first_tablet.tablet_meta_.min_inc_major_meta_scn_ = first_inc_major_meta_scn;
  first_tablet.table_store_cache_.min_major_snapshot_ = 500;
  second_tablet.tablet_meta_.min_minor_meta_scn_ = second_minor_meta_scn;
  second_tablet.tablet_meta_.min_inc_major_meta_scn_ = second_inc_major_meta_scn;
  second_tablet.table_store_cache_.min_major_snapshot_ = 400;

  share::SCN min_tablet_meta_scn;
  int64_t min_major_snapshot = 0;
  ScanAllVersionTabletsOp::GetMinTabletRelyInfo op(min_tablet_meta_scn, min_major_snapshot);
  ASSERT_TRUE(min_tablet_meta_scn.is_max());
  ASSERT_EQ(INT64_MAX, min_major_snapshot);
  ASSERT_EQ(OB_SUCCESS, op(first_tablet));
  ASSERT_EQ(OB_SUCCESS, op(second_tablet));
  ASSERT_EQ(second_inc_major_meta_scn, min_tablet_meta_scn);
  ASSERT_EQ(400, min_major_snapshot);
}
#endif

TEST_F(TestTablet, test_tablet_meta_dependency_scn_compat)
{
  ObTabletMeta tablet_meta;
  ObTabletTableStoreFlag table_store_flag;
  share::ObSplitTabletInfo split_info;
  const uint64_t legacy_data_version = 0;
  const uint64_t old_data_version = DATA_VERSION_4_6_2_0;
  const uint64_t new_data_version = DATA_VERSION_4_6_2_1;

  ASSERT_EQ(OB_SUCCESS,
            tablet_meta.init(ObLSID(3), ObTabletID(1), ObTabletID(4),
                             share::SCN::base_scn(), 5, lib::Worker::CompatMode::MYSQL,
                             table_store_flag, 1, share::SCN::base_scn(), split_info,
                             share::ObForkTabletInfo(), false, false, false, false, 0));
  ASSERT_TRUE(tablet_meta.min_minor_meta_scn_.is_max());

  const int64_t new_len = tablet_meta.get_serialize_size(new_data_version);
  char *buf = reinterpret_cast<char *>(allocator_.alloc(new_len));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_meta.serialize(new_data_version, buf, new_len, pos));
  ASSERT_EQ(new_len, pos);

  ObTabletMeta new_tablet_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_tablet_meta.deserialize(buf, new_len, pos));
  ASSERT_TRUE(new_tablet_meta.min_minor_meta_scn_.is_max());
  ASSERT_TRUE(new_tablet_meta.min_inc_major_meta_scn_.is_max());

  const int64_t old_len = tablet_meta.get_serialize_size(old_data_version);
  ASSERT_EQ(new_len - tablet_meta.min_inc_major_meta_scn_.get_fixed_serialize_size(), old_len);
  char *old_buf = reinterpret_cast<char *>(allocator_.alloc(old_len));
  ASSERT_NE(nullptr, old_buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tablet_meta.serialize(old_data_version, old_buf, old_len, pos));
  ASSERT_EQ(old_len, pos);

  ObTabletMeta old_tablet_meta;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_tablet_meta.deserialize(old_buf, old_len, pos));
  ASSERT_EQ(old_len, pos);
  ASSERT_TRUE(old_tablet_meta.min_inc_major_meta_scn_.is_base_scn());

  const int32_t incomplete_inc_major_meta_scn_length = static_cast<int32_t>(old_len + 1);
  int64_t length_pos = serialization::encoded_length_i32(tablet_meta.version_);
  ASSERT_EQ(OB_SUCCESS,
      serialization::encode_i32(buf, new_len, length_pos, incomplete_inc_major_meta_scn_length));

  ObTabletMeta incomplete_tablet_meta;
  pos = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED, incomplete_tablet_meta.deserialize(buf, new_len, pos));

  const int64_t legacy_len = tablet_meta.get_serialize_size(legacy_data_version);
  char *legacy_buf = reinterpret_cast<char *>(allocator_.alloc(legacy_len));
  ASSERT_NE(nullptr, legacy_buf);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      tablet_meta.serialize(legacy_data_version, legacy_buf, legacy_len, pos));
  ASSERT_EQ(legacy_len, pos);

  const int32_t no_dependency_meta_scn_length = static_cast<int32_t>(
      legacy_len - tablet_meta.min_minor_meta_scn_.get_fixed_serialize_size());
  length_pos = serialization::encoded_length_i32(tablet_meta.version_);
  ASSERT_EQ(OB_SUCCESS,
      serialization::encode_i32(
          legacy_buf, legacy_len, length_pos, no_dependency_meta_scn_length));

  ObTabletMeta shorter_tablet_meta;
  pos = 0;
  ASSERT_EQ(OB_ERR_UNEXPECTED,
      shorter_tablet_meta.deserialize(legacy_buf, legacy_len, pos));
}

TEST_F(TestTablet, test_serialize_mig_param_compat)
{
  int ret = OB_SUCCESS;
  ObMigrationTabletParamV1 mig_param;
  mig_param.magic_number_ = ObMigrationTabletParamV1::MAGIC_NUM;
  mig_param.version_ = ObMigrationTabletParamV1::PARAM_VERSION;
  mig_param.ls_id_ = ObLSID(3);
  mig_param.tablet_id_ = ObTabletID(1);
  mig_param.data_tablet_id_ = ObTabletID(4);
  mig_param.ref_tablet_id_ = ObTabletID(1);
  mig_param.create_scn_ = share::SCN::base_scn();
  mig_param.start_scn_ = share::SCN::base_scn();
  mig_param.clog_checkpoint_scn_ = share::SCN::base_scn();
  mig_param.ddl_checkpoint_scn_ = share::SCN::base_scn();
  mig_param.snapshot_version_ = 9;
  mig_param.multi_version_start_ = 5;
  mig_param.compat_mode_ = lib::Worker::CompatMode::MYSQL;
  mig_param.autoinc_seq_.set_autoinc_seq_value(allocator_, 1);
  ASSERT_EQ(OB_SUCCESS, mig_param.ha_status_.init_status());
  mig_param.tx_data_.tablet_status_ = ObTabletStatus::NORMAL;
  mig_param.ddl_start_scn_ = share::SCN::base_scn();
  mig_param.ddl_snapshot_version_ = 3;
  mig_param.max_sync_storage_schema_version_ = 1;
  mig_param.ddl_execution_id_ = 4;
  mig_param.ddl_data_format_version_ = 6;
  mig_param.max_serialized_medium_scn_ = 5;
  mig_param.ddl_commit_scn_ = share::SCN::base_scn();
  mig_param.storage_schema_.storage_schema_version_ = ObStorageSchema::STORAGE_SCHEMA_VERSION_V3;
  mig_param.storage_schema_.schema_version_ = 1;
  mig_param.storage_schema_.column_cnt_ = 1;
  mig_param.storage_schema_.table_type_ = SYSTEM_TABLE;
  const ObStorageColumnSchema column_schema;
  mig_param.storage_schema_.column_array_.set_allocator(&mig_param.allocator_);
  ASSERT_EQ(OB_SUCCESS, mig_param.storage_schema_.column_array_.reserve(mig_param.storage_schema_.column_cnt_));
  ASSERT_EQ(OB_SUCCESS, mig_param.storage_schema_.column_array_.push_back(column_schema));
  ASSERT_EQ(mig_param.storage_schema_.column_array_.count(), 1);
  mig_param.storage_schema_.is_inited_ = true;

  int8_t head_val = 3;
  int8_t tail_val = 4;
  char *buf = nullptr;
  int64_t len = serialization::encoded_length_i8(head_val) + mig_param.get_serialize_size() + serialization::encoded_length_i8(tail_val);
  buf = reinterpret_cast<char*>(allocator_.alloc(len));
  int64_t pos = 0;
  serialization::encode_i8(buf, len, pos, head_val);
  ASSERT_EQ(OB_SUCCESS, mig_param.serialize(buf, len, pos));
  serialization::encode_i8(buf, len, pos, tail_val);

  ObMigrationTabletParam new_mig_param;
  pos = 0;
  serialization::decode_i8(buf, len, pos, &head_val);
  ASSERT_EQ(head_val, 3);
  ret = new_mig_param.deserialize(buf, len, pos);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  // pos is not advanced after OB_NOT_SUPPORTED, so tail_val is not reachable
  ASSERT_EQ(1, pos);

}

class TestTableStore
{
public:
  int init(ObArenaAllocator &allocator, TestTablet &tablet)
  {
    int ret = OB_SUCCESS;
    ObArray<ObDDLKV *> ddl_kvs;
    tablet.pull_ddl_memtables(ddl_kvs);
    ret = ddl_kvs_.init(allocator, ddl_kvs);
    const int64_t count = ddl_kvs_.count();
    std::cout<< "init table store:" << ddl_kvs.count() << ", " << count <<std::endl;
    STORAGE_LOG(ERROR, "ddl kvs", K(ddl_kvs), K(ddl_kvs_));
    return ret;
  }
  void reproducing_bug(ObArenaAllocator &allocator)
  {
    ObArray<ObDDLKV *> ddl_kvs;
    for (int64_t i = 0; i < 3; ++i) {
      ObDDLKV *ddl_kv = new ObDDLKV();
      ddl_kvs.push_back(ddl_kv);
    }
    ddl_kvs_.init(allocator, ddl_kvs);
    const int64_t count = ddl_kvs_.count();
    std::cout<< "table store reproducing_bug:" << ddl_kvs.count() << ", " << count <<std::endl;
  }
  TO_STRING_KV(K(ddl_kvs_));
private:
  ObDDLKVArray ddl_kvs_;
};

void TestTablet::reproducing_bug()
{
  int ret = OB_SUCCESS;
  ObTabletComplexAddr<TestTableStore> table_store_addr;
  ddl_kvs_ = static_cast<ObDDLKV**>(allocator_.alloc(sizeof(ObDDLKV*) * ObTablet::DDL_KV_ARRAY_SIZE));
  ASSERT_TRUE(nullptr != ddl_kvs_);
  ddl_kvs_[0] = new ObDDLKV();
  ddl_kvs_[1] = new ObDDLKV();
  ddl_kvs_[2] = new ObDDLKV();
  std::cout<< "reproducing_bug 1:" << ddl_kv_count_ << std::endl;
  ddl_kv_count_ = 3;
  std::cout<< "reproducing_bug 2:" << ddl_kv_count_ << std::endl;
  ALLOC_AND_INIT(allocator_, table_store_addr, (*this));
  if (ddl_kv_count_ != table_store_addr.get_ptr()->ddl_kvs_.count()) {
    std::cout<< "reproducing_bug 3:" << ddl_kv_count_ << ", " << table_store_addr.get_ptr()->ddl_kvs_.count() << std::endl;
    // This is defense code. If it runs at here, it must be a bug. And, just abort to preserve the enviroment
    // for debugging. Please remove me, after the problem is found.
    ob_abort();
  }
}

TEST_F(TestTablet, reproducing_bug_53174886)
{
  TestTableStore table_store;
  table_store.reproducing_bug(allocator_);
  reproducing_bug();
}

TEST_F(TestTablet, test_ddl_replay_status_compat)
{
  ObITable::TableType ddl_table_type;
  ObCSReplicaDDLReplayStatus ddl_replay_status;
  ASSERT_EQ(serialization::encoded_length(ddl_table_type), serialization::encoded_length(ddl_replay_status));
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_tablet.log*");
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_file_name("test_tablet.log", true, true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
