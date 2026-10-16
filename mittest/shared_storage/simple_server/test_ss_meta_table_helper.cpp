// owner: donglou.zl
// owner group: storage

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include <gmock/gmock.h>
#define protected public
#define private public
#include "lib/restore/ob_object_storage_base.h"
#include "lib/utility/ob_test_util.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/backup/ob_ss_backup_utils.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_helper.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "src/storage/incremental/ob_sswriter_service.h"
#include "storage/incremental/share/ob_shared_ls_meta.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/ob_cluster_version.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "unittest/storage/schema_utils.h"
#include "unittest/storage/test_tablet_helper.h"

namespace oceanbase
{
char *shared_storage_info = nullptr;
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{
} // namespace sslog

namespace storage
{
static int64_t lease_epoch = 1;

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  UNUSED(key);
  is_sswriter = true;
  epoch = ATOMIC_LOAD(&lease_epoch);
  return OB_SUCCESS;
}

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    const int64_t timeout_us)
{
  UNUSED(key);
  UNUSED(timeout_us);
  sswriter_addr.addr_ = GCTX.self_addr();
  return OB_SUCCESS;
}
} // namespace storage

using namespace unittest;
namespace share
{
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace oceanbase::common;
using namespace oceanbase::storage;
using oceanbase::blocksstable::ObStorageObjectType;

static const uint64_t TEST_TENANT_ID = 1;

class TestSSMetaTableHelper : public unittest::ObSimpleClusterTestBase
{
public:
  TestSSMetaTableHelper()
    : unittest::ObSimpleClusterTestBase("test_ss_meta_table_helper", "50G", "50G", "50G")
  {}

  common::ObISQLClient &get_sql_client()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  static share::SCN make_scn(const int64_t val)
  {
    share::SCN scn;
    scn.convert_for_tx(val);
    return scn;
  }

  static ObString make_str(ObIAllocator &allocator, const char *s)
  {
    const int64_t len = STRLEN(s);
    char *buf = static_cast<char *>(allocator.alloc(len));
    EXPECT_NE(nullptr, buf);
    MEMCPY(buf, s, len);
    return ObString(len, buf);
  }

  static ObString make_meta_key(ObIAllocator &alloc, const char *prefix, const int64_t scn_val,
                                const ObStorageObjectType type = ObStorageObjectType::SHARED_LS_META)
  {
    char tmp[512];
    int64_t pos = 0;
    databuff_printf(tmp, sizeof(tmp), pos, "%s/scn%020ld.T%d", prefix, scn_val,
                    static_cast<int>(type));
    char *buf = static_cast<char *>(alloc.alloc(pos));
    EXPECT_NE(nullptr, buf);
    MEMCPY(buf, tmp, pos);
    return ObString(static_cast<int32_t>(pos), buf);
  }

  static ObString make_tablet_meta_value_with_last_major(
      ObIAllocator &alloc,
      const int64_t last_major_snapshot_version,
      const bool mark_delete)
  {
    ObSSTabletTableStoreMetaInfo table_store_meta_info;
    EXPECT_EQ(OB_SUCCESS, table_store_meta_info.set(
        1 /*major_tables_cnt*/,
        0 /*minor_tables_cnt*/,
        0 /*ddl_tables_cnt*/,
        0 /*meta_major_tables_cnt*/,
        true /*minor_trans_state_determined*/,
        last_major_snapshot_version,
        share::SCN::min_scn(),
        0 /*uncommitted_inc_major_cnt*/));

    ObSSMetaHeadValueInfo head_info;
    head_info.mark_delete_ = mark_delete;
    head_info.update_reason_ = ObMetaUpdateReason::CREATE_TABLET;
    head_info.table_store_meta_info_ = table_store_meta_info;
    ObSSMetaHeadValue head_value;
    head_value.head_info_ = head_info;

    const int64_t head_ser_size = head_value.get_serialize_size();
    const int64_t total = serialization::encoded_length_vi64(1)
                        + serialization::OB_SERIALIZE_SIZE_NEED_BYTES
                        + head_ser_size;
    char *buf = static_cast<char *>(alloc.alloc(total));
    EXPECT_NE(nullptr, buf);
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, serialization::encode_vi64(buf, total, pos, 1));
    const int64_t len_pos = pos;
    pos += serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    const int64_t data_start = pos;
    EXPECT_EQ(OB_SUCCESS, head_value.serialize(buf, total, pos));
    const int64_t data_len = pos - data_start;
    int64_t tmp_pos = 0;
    EXPECT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(
        buf + len_pos, serialization::OB_SERIALIZE_SIZE_NEED_BYTES, tmp_pos, data_len));

    return ObString(static_cast<int32_t>(pos), buf);
  }

  static int rewrite_tablet_meta_value_head(
      ObArenaAllocator &alloc,
      const ObString &src_value,
      const int64_t last_major_snapshot_version,
      ObString &value)
  {
    int ret = OB_SUCCESS;
    int64_t outer_version = 0;
    int64_t outer_content_len = 0;
    int64_t pos = 0;
    ObSSMetaHeadValue head_value;
    int64_t head_pos = 0;
    value.reset();
    if (OB_FAIL(serialization::decode_vi64(src_value.ptr(), src_value.length(),
                                           pos, &outer_version))) {
      LOG_WARN("fail to decode outer version", KR(ret), K(src_value));
    } else if (OB_FAIL(serialization::decode_vi64(src_value.ptr(), src_value.length(),
                                                  pos, &outer_content_len))) {
      LOG_WARN("fail to decode outer content length", KR(ret), K(src_value));
    } else {
      const int64_t header_size = pos;
      const int64_t content_start = pos;
      const int64_t content_end = content_start + outer_content_len;
      if (OB_UNLIKELY(content_end > src_value.length())) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("outer content length overflow", KR(ret), K(src_value), K(content_start), K(outer_content_len));
      } else if (OB_FAIL(head_value.deserialize(src_value.ptr() + content_start,
                                                outer_content_len, head_pos))) {
        LOG_WARN("fail to deserialize head value", KR(ret), K(outer_content_len));
      } else {
        const int64_t payload_start = content_start + head_pos;
        const int64_t payload_len = content_end - payload_start;
        ObSSTabletTableStoreMetaInfo table_store_meta_info;
        if (OB_FAIL(table_store_meta_info.set(
            1 /*major_tables_cnt*/,
            0 /*minor_tables_cnt*/,
            0 /*ddl_tables_cnt*/,
            0 /*meta_major_tables_cnt*/,
            true /*minor_trans_state_determined*/,
            last_major_snapshot_version,
            share::SCN::min_scn(),
            0 /*uncommitted_inc_major_cnt*/))) {
          LOG_WARN("fail to set table store meta info", KR(ret), K(last_major_snapshot_version));
        } else {
          head_value.head_info_.mark_delete_ = false;
          head_value.head_info_.update_reason_ = ObMetaUpdateReason::CREATE_TABLET;
          head_value.head_info_.table_store_meta_info_ = table_store_meta_info;
          const int64_t new_content_len = head_value.get_serialize_size() + payload_len;
          const int64_t new_value_len = header_size + new_content_len;
          char *buf = nullptr;
          int64_t write_pos = 0;
          int64_t tmp_pos = 0;
          if (OB_UNLIKELY(payload_len < 0)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid payload length", KR(ret), K(payload_start), K(content_end));
          } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(new_value_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to alloc rewritten meta value buffer", KR(ret), K(new_value_len));
          } else if (OB_FAIL(serialization::encode_vi64(buf, new_value_len, write_pos, outer_version))) {
            LOG_WARN("fail to encode outer version", KR(ret), K(new_value_len));
          } else if (FALSE_IT(write_pos += serialization::OB_SERIALIZE_SIZE_NEED_BYTES)) {
          } else if (OB_FAIL(head_value.serialize(buf, new_value_len, write_pos))) {
            LOG_WARN("fail to serialize head value", KR(ret), K(new_value_len));
          } else {
            if (payload_len > 0) {
              MEMCPY(buf + write_pos, src_value.ptr() + payload_start, payload_len);
              write_pos += payload_len;
            }
            if (OB_UNLIKELY(write_pos != new_value_len)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("rewritten meta value length mismatch", KR(ret), K(write_pos), K(new_value_len));
            } else if (OB_FAIL(serialization::encode_fixed_bytes_i64(
                buf + serialization::encoded_length_vi64(outer_version),
                serialization::OB_SERIALIZE_SIZE_NEED_BYTES, tmp_pos, new_content_len))) {
              LOG_WARN("fail to encode outer content length", KR(ret), K(new_content_len));
            } else {
              value = ObString(static_cast<int32_t>(write_pos), buf);
            }
          }
        }
      }
    }
    return ret;
  }

  static int delete_tablet_meta_rows(
      ObSSMetaService *meta_svr,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &reorgan_scn)
  {
    int ret = OB_SUCCESS;
    ObSSMetaDeleteParamV2 delete_param;
    if (OB_ISNULL(meta_svr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("meta service is null", KR(ret));
    } else {
      delete_param.set_tablet_level_param(
          ObSSMetaReadParamType::TABLET_PREFIX,
          ObSSMetaReadResultType::READ_WHOLE_ROW,
          ObAtomicProtocolTypeEnum::TABLET_META,
          ls_id, tablet_id, share::SCN::min_scn(), reorgan_scn,
          true /*is_in_table*/);
      ret = meta_svr->delete_meta_row(delete_param);
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      }
    }
    return ret;
  }

  static int insert_row(ObSSMetaBaseOperator &op,
                        const uint64_t tenant_id,
                        const ObLSID &ls_id,
                        const ObSSMetaType &meta_type,
                        const ObString &meta_key,
                        const ObString &meta_value)
  {
    int64_t affected_rows = 0;
    return op.insert_row(tenant_id, ls_id, meta_type, meta_key, meta_value, affected_rows);
  }

  static int delete_row(ObSSMetaBaseOperator &op,
                        const uint64_t tenant_id,
                        const ObLSID &ls_id,
                        const ObSSMetaType &meta_type,
                        const ObString &meta_key)
  {
    int64_t affected_rows = 0;
    return op.delete_row(tenant_id, ls_id, meta_type, meta_key, false, affected_rows);
  }

  // Build a valid ObSSMetaReadParamV2 for tablet-level read
  static void build_tablet_read_param(
      ObSSMetaReadParamV2 &param,
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const share::SCN &reorgan_scn)
  {
    param.reset();
    param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::TABLET_META,
        ls_id, tablet_id, share::SCN::min_scn(), reorgan_scn, true);
  }

  // Build a valid ObSSMetaReadParamV2 for LS-level read
  static void build_ls_read_param(
      ObSSMetaReadParamV2 &param,
      const ObLSID &ls_id)
  {
    param.reset();
    param.set_ls_level_param(
        ObSSMetaReadParamType::LS_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        ObAtomicProtocolTypeEnum::LS_META,
        ls_id, share::SCN::min_scn(), true);
  }
};

// ===========================================================================
// get_meta_info: invalid argument
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_invalid_argument)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator allocator;
  ObSSMetaOpBuffer meta_value;
  share::SCN meta_scn;

  ObSSMetaReadParamV2 invalid_param;
  int ret = ObSSMetaTableHelper::get_meta_info(invalid_param, make_scn(9999),
                                               allocator, meta_value, meta_scn);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// get_meta_info: entry not exist when no data
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_not_exist)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator allocator;
  ObSSMetaOpBuffer meta_value;
  share::SCN meta_scn;

  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ObLSID(8001), ObTabletID(999999), make_scn(0));

  int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(9999),
                                               allocator, meta_value, meta_scn);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// get_meta_info: insert data then read back
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_basic)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8010);
  const ObTabletID tablet_id(200010);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  // serialize an ObSSMetaHeadValue as meta_value
  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::CREATE_TABLET;

  ObSSMetaHeadValue head_value;
  head_value.head_info_ = head_info;

  const int64_t ser_size = head_value.get_serialize_size();
  char *ser_buf = static_cast<char *>(alloc.alloc(ser_size));
  ASSERT_NE(nullptr, ser_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, head_value.serialize(ser_buf, ser_size, pos));
  ObString value_str(static_cast<int32_t>(pos), ser_buf);

  // insert two versions: scn 100 and scn 500
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200010/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META), value_str));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200010/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META), value_str));

  // get_meta_info should return the latest version <= read_snapshot
  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ls_id, tablet_id, make_scn(0));

  ObSSMetaOpBuffer result_buf;
  share::SCN result_scn;
  int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(9999),
                                               alloc, result_buf, result_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(500, result_scn.get_val_for_tx());
  ASSERT_TRUE(result_buf.is_valid());
  ASSERT_GT(result_buf.size_, 0);

  // verify the read data can be deserialized back
  ObSSMetaHeadValue deser_value;
  int64_t deser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, deser_value.deserialize(result_buf.buf_, result_buf.size_, deser_pos));
  ASSERT_EQ(head_info.mark_delete_, deser_value.head_info_.mark_delete_);
  ASSERT_EQ(head_info.update_reason_, deser_value.head_info_.update_reason_);

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200010/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200010/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META));

  share::mtl_free(result_buf.buf_);
}

// ===========================================================================
// get_meta_info: LS_META path uses ls/{ls_id}/meta prefix
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_ls_meta)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8012);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;
  ObSSMetaHeadValue head_value;
  head_value.head_info_ = head_info;

  const int64_t ser_size = head_value.get_serialize_size();
  char *ser_buf = static_cast<char *>(alloc.alloc(ser_size));
  ASSERT_NE(nullptr, ser_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, head_value.serialize(ser_buf, ser_size, pos));
  ObString value_str(static_cast<int32_t>(pos), ser_buf);

  // insert with ls/{ls_id}/meta prefix
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8012/meta", 100), value_str));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8012/meta", 400), value_str));

  // read via get_meta_info with LS_META param
  ObSSMetaReadParamV2 param;
  build_ls_read_param(param, ls_id);

  ObSSMetaOpBuffer result_buf;
  share::SCN result_scn;
  int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(9999),
                                               alloc, result_buf, result_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(400, result_scn.get_val_for_tx());
  ASSERT_TRUE(result_buf.is_valid());

  // verify round-trip
  ObSSMetaHeadValue deser_value;
  int64_t deser_pos = 0;
  ASSERT_EQ(OB_SUCCESS, deser_value.deserialize(result_buf.buf_, result_buf.size_, deser_pos));
  ASSERT_EQ(head_info.mark_delete_, deser_value.head_info_.mark_delete_);
  ASSERT_EQ(head_info.update_reason_, deser_value.head_info_.update_reason_);

  // snapshot filter: snapshot=200 should get scn=100
  {
    ObSSMetaOpBuffer result_buf2;
    share::SCN result_scn2;
    ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(200),
                                             alloc, result_buf2, result_scn2);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(100, result_scn2.get_val_for_tx());
    share::mtl_free(result_buf2.buf_);
  }

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8012/meta", 100));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8012/meta", 400));

  share::mtl_free(result_buf.buf_);
}

// ===========================================================================
// get_meta_info: read_snapshot filters versions
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_snapshot_filter)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8011);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "val");

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200020/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200020/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200020/0/meta", 1000, ObStorageObjectType::SHARED_TABLET_META), v));

  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ls_id, ObTabletID(200020), make_scn(0));

  // read_snapshot=200: should get scn=100
  {
    ObSSMetaOpBuffer result_buf;
    share::SCN result_scn;
    int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(200),
                                                 alloc, result_buf, result_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(100, result_scn.get_val_for_tx());
    share::mtl_free(result_buf.buf_);
  }

  // read_snapshot=500: should get scn=500
  {
    ObSSMetaOpBuffer result_buf;
    share::SCN result_scn;
    int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(500),
                                                 alloc, result_buf, result_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(500, result_scn.get_val_for_tx());
    share::mtl_free(result_buf.buf_);
  }

  // read_snapshot=9999: should get scn=1000 (latest)
  {
    ObSSMetaOpBuffer result_buf;
    share::SCN result_scn;
    int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(9999),
                                                 alloc, result_buf, result_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(1000, result_scn.get_val_for_tx());
    share::mtl_free(result_buf.buf_);
  }

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200020/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200020/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200020/0/meta", 1000, ObStorageObjectType::SHARED_TABLET_META));
}

// ===========================================================================
// get_tablet_scn: invalid argument
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_scn_invalid_argument)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  share::SCN meta_scn;
  ObSSMetaReadParamV2 invalid_param;
  int ret = ObSSMetaTableHelper::get_tablet_scn(invalid_param, make_scn(9999), meta_scn);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// get_tablet_scn: entry not exist
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_scn_not_exist)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  share::SCN meta_scn;
  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ObLSID(8020), ObTabletID(888888), make_scn(0));

  int ret = ObSSMetaTableHelper::get_tablet_scn(param, make_scn(9999), meta_scn);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// get_tablet_scn: basic read
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_scn_basic)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8021);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);

  ObSSMetaHeadValueInfo head_info;
  head_info.mark_delete_ = false;
  head_info.update_reason_ = ObMetaUpdateReason::CREATE_TABLET;
  ObSSMetaHeadValue head_value;
  head_value.head_info_ = head_info;
  const int64_t ser_size = head_value.get_serialize_size();
  char *ser_buf = static_cast<char *>(alloc.alloc(ser_size));
  ASSERT_NE(nullptr, ser_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, head_value.serialize(ser_buf, ser_size, pos));
  ObString value_str(static_cast<int32_t>(pos), ser_buf);

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200030/0/meta", 300, ObStorageObjectType::SHARED_TABLET_META), value_str));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200030/0/meta", 700, ObStorageObjectType::SHARED_TABLET_META), value_str));

  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ls_id, ObTabletID(200030), make_scn(0));
  param.set_read_mark_delete(true);

  // snapshot 500: should return scn 300
  {
    share::SCN meta_scn;
    int ret = ObSSMetaTableHelper::get_tablet_scn(param, make_scn(500), meta_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(300, meta_scn.get_val_for_tx());
  }

  // snapshot 9999: should return scn 700
  {
    share::SCN meta_scn;
    int ret = ObSSMetaTableHelper::get_tablet_scn(param, make_scn(9999), meta_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(700, meta_scn.get_val_for_tx());
  }

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200030/0/meta", 300, ObStorageObjectType::SHARED_TABLET_META));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200030/0/meta", 700, ObStorageObjectType::SHARED_TABLET_META));
}

// ===========================================================================
// get_tablet_last_major_snapshot_version: invalid argument
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_last_major_snapshot_version_invalid_argument)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  int64_t last_major_snapshot_version = 0;
  bool is_mark_delete = true;
  ObSSMetaReadParamV2 invalid_param;
  int ret = ObSSMetaTableHelper::get_tablet_last_major_snapshot_version(
      invalid_param, last_major_snapshot_version, is_mark_delete);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
  ASSERT_EQ(-1, last_major_snapshot_version);
  ASSERT_FALSE(is_mark_delete);
}

// ===========================================================================
// get_tablet_last_major_snapshot_version: entry not exist
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_last_major_snapshot_version_not_exist)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ObLSID(8022), ObTabletID(888889), make_scn(0));

  int64_t last_major_snapshot_version = 0;
  bool is_mark_delete = true;
  int ret = ObSSMetaTableHelper::get_tablet_last_major_snapshot_version(
      param, last_major_snapshot_version, is_mark_delete);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  ASSERT_EQ(-1, last_major_snapshot_version);
  ASSERT_FALSE(is_mark_delete);
}

// ===========================================================================
// get_tablet_last_major_snapshot_version: read latest row and mark-delete flag
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_last_major_snapshot_version_latest)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8023);
  const ObTabletID tablet_id(200032);
  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200032/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200032/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META));

  ObString old_value = make_tablet_meta_value_with_last_major(
      alloc, 1200 /*last_major_snapshot_version*/, false /*mark_delete*/);
  ObString new_value = make_tablet_meta_value_with_last_major(
      alloc, 3600 /*last_major_snapshot_version*/, true /*mark_delete*/);
  {
    int64_t pos = 0;
    ObSSMetaHeadValue head_value;
    ASSERT_EQ(OB_SUCCESS, head_value.deserialize_value_info(new_value.ptr(), new_value.length(), pos));
    ObSSTabletTableStoreMetaInfo table_store_meta_info;
    head_value.get_meta_head_info().get_table_store_meta(table_store_meta_info);
    ASSERT_EQ(1, table_store_meta_info.major_tables_cnt_);
    ASSERT_EQ(0, table_store_meta_info.minor_tables_cnt_);
    ASSERT_EQ(3600, table_store_meta_info.last_major_snapshot_version_);
    ASSERT_TRUE(head_value.get_meta_head_info().is_mark_deleted());
  }

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200032/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META),
      old_value));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200032/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META),
      new_value));

  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ls_id, tablet_id, make_scn(0));
  param.set_read_mark_delete(true);

  int64_t last_major_snapshot_version = -1;
  bool is_mark_delete = false;
  int ret = ObSSMetaTableHelper::get_tablet_last_major_snapshot_version(
      param, last_major_snapshot_version, is_mark_delete);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3600, last_major_snapshot_version);
  ASSERT_TRUE(is_mark_delete);

  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200032/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200032/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META));
}

// ===========================================================================
// get_ls: invalid argument
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_ls_invalid_argument)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObDefaultSSMetaValue<ObSSLSMeta> ls_value;
  share::SCN meta_scn;
  ObSSMetaReadParamV2 invalid_param;

  int ret = ObSSMetaTableHelper::get_ls(invalid_param, make_scn(9999), ls_value, meta_scn);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// get_ls: entry not exist
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_ls_not_exist)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObDefaultSSMetaValue<ObSSLSMeta> ls_value;
  share::SCN meta_scn;

  ObSSMetaReadParamV2 param;
  build_ls_read_param(param, ObLSID(9999));

  int ret = ObSSMetaTableHelper::get_ls(param, make_scn(9999), ls_value, meta_scn);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// get_ls: insert serialized ls meta and read back
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_ls_basic)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8030);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);

  // build and serialize ObDefaultSSMetaValue<ObSSLSMeta>
  ObDefaultSSMetaValue<ObSSLSMeta> orig_value;
  orig_value.head_info_.mark_delete_ = false;
  orig_value.head_info_.update_reason_ = ObMetaUpdateReason::UPDATE_LS_GC_STATE;

  const int64_t ser_size = orig_value.get_serialize_size();
  ASSERT_GT(ser_size, 0);
  char *ser_buf = static_cast<char *>(alloc.alloc(ser_size));
  ASSERT_NE(nullptr, ser_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, orig_value.serialize(ser_buf, ser_size, pos));
  ObString value_str(static_cast<int32_t>(pos), ser_buf);

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8030/meta", 200), value_str));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8030/meta", 600), value_str));

  ObSSMetaReadParamV2 param;
  build_ls_read_param(param, ls_id);

  // read_snapshot=9999: should get scn=600 (latest)
  {
    ObDefaultSSMetaValue<ObSSLSMeta> read_value;
    share::SCN meta_scn;
    int ret = ObSSMetaTableHelper::get_ls(param, make_scn(9999), read_value, meta_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(600, meta_scn.get_val_for_tx());
    ASSERT_EQ(orig_value.head_info_.mark_delete_, read_value.head_info_.mark_delete_);
    ASSERT_EQ(orig_value.head_info_.update_reason_, read_value.head_info_.update_reason_);
  }

  // read_snapshot=300: should get scn=200
  {
    ObDefaultSSMetaValue<ObSSLSMeta> read_value;
    share::SCN meta_scn;
    int ret = ObSSMetaTableHelper::get_ls(param, make_scn(300), read_value, meta_scn);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(200, meta_scn.get_val_for_tx());
  }

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8030/meta", 200));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8030/meta", 600));
}

// ===========================================================================
// get_meta_info (raw row overload): basic read
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_raw_row_basic)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8013);
  const ObTabletID tablet_id(200013);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "raw_row_val");

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200013/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200013/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META), v));

  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ls_id, tablet_id, make_scn(0));

  // read via raw row overload
  ObSSRawMetaRow row;
  int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(9999), alloc, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(row.meta_key_.empty());
  ASSERT_FALSE(row.meta_value_.empty());

  // verify scn in meta_key is the latest (500)
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(row.meta_key_, scn_val));
  ASSERT_EQ(500UL, scn_val);

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200013/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200013/0/meta", 500, ObStorageObjectType::SHARED_TABLET_META));
}

// ===========================================================================
// get_meta_info (raw row overload): LS_META path
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_raw_row_ls_meta)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8014);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "ls_raw_val");

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8014/meta", 200), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8014/meta", 800), v));

  ObSSMetaReadParamV2 param;
  build_ls_read_param(param, ls_id);

  ObSSRawMetaRow row;
  int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(9999), alloc, row);
  ASSERT_EQ(OB_SUCCESS, ret);

  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(row.meta_key_, scn_val));
  ASSERT_EQ(800UL, scn_val);

  // snapshot filter: snapshot=500 should get scn=200
  {
    ObSSRawMetaRow row2;
    ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(500), alloc, row2);
    ASSERT_EQ(OB_SUCCESS, ret);
    uint64_t scn_val2 = 0;
    ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(row2.meta_key_, scn_val2));
    ASSERT_EQ(200UL, scn_val2);
  }

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8014/meta", 200));
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8014/meta", 800));
}

// ===========================================================================
// get_meta_info (raw row overload): not exist
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_raw_row_not_exist)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ObLSID(8015), ObTabletID(999998), make_scn(0));

  ObSSRawMetaRow row;
  int ret = ObSSMetaTableHelper::get_meta_info(param, make_scn(9999), alloc, row);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// get_meta_info (raw row overload): invalid argument
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_meta_info_raw_row_invalid)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  ObSSMetaReadParamV2 invalid_param;
  ObSSRawMetaRow row;
  int ret = ObSSMetaTableHelper::get_meta_info(invalid_param, make_scn(9999), alloc, row);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// delete_meta_info: TABLET_KEY single key delete
// ===========================================================================

TEST_F(TestSSMetaTableHelper, delete_meta_info_tablet_key)
{
  ObTenantSwitchGuard tenant_guard;int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8052);
  const ObTabletID tablet_id(200052);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "key_del_val");

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200052/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200052/0/meta", 200, ObStorageObjectType::SHARED_TABLET_META), v));

  // single key delete: only scn=100 should be removed
  ObSSMetaDeleteParamV2 del_param;
  del_param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_KEY,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id, tablet_id, make_scn(100), make_scn(0), true);

  ret = ObSSMetaTableHelper::delete_meta_info(del_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, del_param.affected_rows_);

  // verify scn=200 survives
  ObSSMetaReadParamV2 read_param;
  build_tablet_read_param(read_param, ls_id, tablet_id, make_scn(0));
  ObSSMetaOpBuffer result_buf;
  share::SCN result_scn;
  ret = ObSSMetaTableHelper::get_meta_info(read_param, make_scn(9999),
                                           alloc, result_buf, result_scn);
  // may succeed or fail depending on whether meta_value is valid for deserialization,
  // but should NOT be OB_ENTRY_NOT_EXIST since scn=200 row still exists
  // Actually get_meta_info returns raw row, so it should succeed
  // But the OpBuffer overload extracts scn - let's use raw row overload
  ObSSRawMetaRow row;
  ret = ObSSMetaTableHelper::get_meta_info(read_param, make_scn(9999), alloc, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(row.meta_key_, scn_val));
  ASSERT_EQ(200UL, scn_val);

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200052/0/meta", 200, ObStorageObjectType::SHARED_TABLET_META));
}

// ===========================================================================
// delete_meta_info: LS_KEY single key delete
// ===========================================================================

TEST_F(TestSSMetaTableHelper, delete_meta_info_ls_key)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8053);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "ls_key_del");

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8053/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8053/meta", 300), v));

  // single key delete: only scn=100 should be removed
  ObSSMetaDeleteParamV2 del_param;
  del_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_KEY,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META,
      ls_id, make_scn(100), true);

  int ret = ObSSMetaTableHelper::delete_meta_info(del_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, del_param.affected_rows_);

  // verify scn=300 survives
  ObSSMetaReadParamV2 read_param;
  build_ls_read_param(read_param, ls_id);
  ObSSRawMetaRow row;
  ret = ObSSMetaTableHelper::get_meta_info(read_param, make_scn(9999), alloc, row);
  ASSERT_EQ(OB_SUCCESS, ret);
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(row.meta_key_, scn_val));
  ASSERT_EQ(300UL, scn_val);

  // cleanup
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8053/meta", 300));
}

// ===========================================================================
// delete_meta_info: CHECKPOINT + LS_KEY single key delete
// ===========================================================================

TEST_F(TestSSMetaTableHelper, delete_meta_info_ls_ckpt_key)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8054);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
  const ObString value = make_str(alloc, "ls_ckpt_key_del");
  const ObString deleted_key = make_meta_key(
      alloc, "ls/8054/meta", 100, ObStorageObjectType::SHARED_LS_CKPT);
  const ObString survived_key = make_meta_key(
      alloc, "ls/8054/meta", 300, ObStorageObjectType::SHARED_LS_CKPT);

  ASSERT_EQ(OB_SUCCESS, insert_row(
      base_op, TEST_TENANT_ID, ls_id, meta_type, deleted_key, value));
  ASSERT_EQ(OB_SUCCESS, insert_row(
      base_op, TEST_TENANT_ID, ls_id, meta_type, survived_key, value));

  ObSSMetaDeleteParamV2 del_param;
  del_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_KEY,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::CHECKPOINT,
      ls_id, make_scn(100), true/*is_in_table*/);

  int ret = ObSSMetaTableHelper::delete_meta_info(del_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, del_param.affected_rows_);

  ObString read_value;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, base_op.read_row(
      TEST_TENANT_ID, ls_id, meta_type, deleted_key, alloc, read_value));
  ASSERT_EQ(OB_SUCCESS, base_op.read_row(
      TEST_TENANT_ID, ls_id, meta_type, survived_key, alloc, read_value));

  ASSERT_EQ(OB_SUCCESS, delete_row(
      base_op, TEST_TENANT_ID, ls_id, meta_type, survived_key));
}

// ===========================================================================
// delete_meta_info: invalid argument
// ===========================================================================

TEST_F(TestSSMetaTableHelper, delete_meta_info_invalid_argument)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaDeleteParamV2 invalid_param;
  int ret = ObSSMetaTableHelper::delete_meta_info(invalid_param);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// delete_meta_info: TABLET_PREFIX delete
// ===========================================================================

TEST_F(TestSSMetaTableHelper, delete_meta_info_tablet_prefix)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8050);
  const ObTabletID tablet_id(200050);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ObString v = make_str(alloc, "del_val");

  // insert two versions for the same tablet
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200050/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200050/0/meta", 200, ObStorageObjectType::SHARED_TABLET_META), v));

  // prefix delete: should remove both versions
  ObSSMetaDeleteParamV2 del_param;
  del_param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ls_id, tablet_id, share::SCN::min_scn(), make_scn(0), true);

  int ret = ObSSMetaTableHelper::delete_meta_info(del_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, del_param.affected_rows_);

  // verify they are gone
  ObSSMetaReadParamV2 read_param;
  build_tablet_read_param(read_param, ls_id, tablet_id, make_scn(0));
  ObSSMetaOpBuffer result_buf;
  share::SCN result_scn;
  ret = ObSSMetaTableHelper::get_meta_info(read_param, make_scn(9999),
                                           alloc, result_buf, result_scn);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// delete_meta_info: LS_PREFIX delete
// ===========================================================================

TEST_F(TestSSMetaTableHelper, delete_meta_info_ls_prefix)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8051);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "ls_del_val");

  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8051/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "ls/8051/meta", 300), v));

  // prefix delete all versions under this LS
  ObSSMetaDeleteParamV2 del_param;
  del_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META,
      ls_id, share::SCN::min_scn(), true);

  int ret = ObSSMetaTableHelper::delete_meta_info(del_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2, del_param.affected_rows_);

  // verify gone
  ObSSMetaReadParamV2 read_param;
  build_ls_read_param(read_param, ls_id);
  ObDefaultSSMetaValue<ObSSLSMeta> ls_value;
  share::SCN result_scn;
  ret = ObSSMetaTableHelper::get_ls(read_param, make_scn(9999), ls_value, result_scn);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// delete_meta_info: TENANT_PREFIX delete
// ===========================================================================

TEST_F(TestSSMetaTableHelper, delete_meta_info_tenant_prefix)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();

  // delete existing ls meta rows, so as to avoid affecting the test
  ObSSMetaDeleteParamV2 del_param;
  del_param.set_tenant_level_param(
      ObSSMetaReadParamType::TENANT_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META, true);
  int ret = ObSSMetaTableHelper::delete_meta_info(del_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType ls_meta_type;
  ls_meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  ObString v = make_str(alloc, "tenant_del");

  // insert LS_META rows under different LSes for the same tenant
  const ObLSID ls_a(8060);
  const ObLSID ls_b(8061);
  const ObLSID ls_c(8062);
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_a, ls_meta_type,
      make_meta_key(alloc, "ls/8060/meta", 100), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_b, ls_meta_type,
      make_meta_key(alloc, "ls/8061/meta", 200), v));
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_c, ls_meta_type,
      make_meta_key(alloc, "ls/8062/meta", 300), v));

  // also insert a TABLET_META row that should NOT be deleted (different meta_type)
  ObSSMetaType tablet_meta_type;
  tablet_meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_a, tablet_meta_type,
      make_meta_key(alloc, "tablet/200060/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META), v));

  // TENANT_PREFIX delete for LS_META: should delete all 3 ls meta rows

  ret = ObSSMetaTableHelper::delete_meta_info(del_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(3, del_param.affected_rows_);

  // verify ls metas are gone
  {
    ObSSMetaReadParamV2 read_param;
    build_ls_read_param(read_param, ls_a);
    ObDefaultSSMetaValue<ObSSLSMeta> ls_value;
    share::SCN result_scn;
    ret = ObSSMetaTableHelper::get_ls(read_param, make_scn(9999), ls_value, result_scn);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }
  {
    ObSSMetaReadParamV2 read_param;
    build_ls_read_param(read_param, ls_b);
    ObDefaultSSMetaValue<ObSSLSMeta> ls_value;
    share::SCN result_scn;
    ret = ObSSMetaTableHelper::get_ls(read_param, make_scn(9999), ls_value, result_scn);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }
  {
    ObSSMetaReadParamV2 read_param;
    build_ls_read_param(read_param, ls_c);
    ObDefaultSSMetaValue<ObSSLSMeta> ls_value;
    share::SCN result_scn;
    ret = ObSSMetaTableHelper::get_ls(read_param, make_scn(9999), ls_value, result_scn);
    ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }

  // verify the TABLET_META row is still present (different meta_type was not affected)
  {
    ObSSMetaReadParamV2 read_param;
    build_tablet_read_param(read_param, ls_a, ObTabletID(200060), make_scn(0));
    ObSSMetaOpBuffer result_buf;
    share::SCN result_scn;
    ret = ObSSMetaTableHelper::get_meta_info(read_param, make_scn(9999),
                                             alloc, result_buf, result_scn);
    ASSERT_NE(OB_ENTRY_NOT_EXIST, ret);
  }

  // cleanup the remaining TABLET_META row
  delete_row(base_op, TEST_TENANT_ID, ls_a, tablet_meta_type,
      make_meta_key(alloc, "tablet/200060/0/meta", 100, ObStorageObjectType::SHARED_TABLET_META));
}

// ===========================================================================
// get_tablet: invalid argument
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_invalid_argument)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator allocator;
  ObTabletHandle tablet_handle;
  ObSSMetaUpdateMetaInfo update_meta_info;
  share::SCN meta_scn;

  ObSSMetaReadParamV2 invalid_param;
  int ret = ObSSMetaTableHelper::get_tablet(invalid_param, make_scn(9999),
                                            allocator, tablet_handle, update_meta_info, meta_scn);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// get_tablet: entry not exist
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_not_exist)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator allocator;
  ObTabletHandle tablet_handle;
  ObSSMetaUpdateMetaInfo update_meta_info;
  share::SCN meta_scn;

  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ObLSID(8040), ObTabletID(777777), make_scn(0));

  int ret = ObSSMetaTableHelper::get_tablet(param, make_scn(9999),
                                            allocator, tablet_handle, update_meta_info, meta_scn);
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ret);
}

// ===========================================================================
// get_tablet_last_major_snapshot_version: with meta_value output
// ===========================================================================

TEST_F(TestSSMetaTableHelper, get_tablet_last_major_snapshot_version_with_meta_value)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8024);
  const ObTabletID tablet_id(200033);
  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200033/0/meta", 300, ObStorageObjectType::SHARED_TABLET_META));

  ObString value = make_tablet_meta_value_with_last_major(
      alloc, 2400 /*last_major_snapshot_version*/, false /*mark_delete*/);
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200033/0/meta", 300, ObStorageObjectType::SHARED_TABLET_META),
      value));

  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ls_id, tablet_id, make_scn(0));
  param.set_read_mark_delete(true);

  int64_t last_major_snapshot_version = -1;
  bool is_mark_delete = true;
  ObSSMetaOpBufferGuard meta_value_guard;
  ObSSMetaOpBuffer &meta_value = meta_value_guard.get_buffer();
  int ret = ObSSMetaTableHelper::get_tablet_last_major_snapshot_version(
      param, last_major_snapshot_version, is_mark_delete, &meta_value);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(2400, last_major_snapshot_version);
  ASSERT_FALSE(is_mark_delete);

  // verify meta_value buffer is populated
  ASSERT_TRUE(meta_value.is_valid());
  ASSERT_GT(meta_value.data_len_, 0);

  // verify the meta_value can be deserialized back
  {
    int64_t pos = 0;
    ObSSMetaHeadValue head_value;
    ASSERT_EQ(OB_SUCCESS, head_value.deserialize_value_info(
        meta_value.buf_, meta_value.data_len_, pos));
    ASSERT_FALSE(head_value.get_meta_head_info().is_mark_deleted());
    ObSSTabletTableStoreMetaInfo table_store_meta_info;
    head_value.get_meta_head_info().get_table_store_meta(table_store_meta_info);
    ASSERT_EQ(2400, table_store_meta_info.last_major_snapshot_version_);
  }

  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200033/0/meta", 300, ObStorageObjectType::SHARED_TABLET_META));
}

// ===========================================================================
// handle_mark_delete: no cached data -> OB_OBJECT_NOT_EXIST
// ===========================================================================

TEST_F(TestSSMetaTableHelper, handle_mark_delete_no_data)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObSSMetaAtomicTabletOp tablet_op;
  int ret = tablet_op.handle_mark_delete(make_scn(1000));
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, ret);

  // invalid deleted_scn should be rejected even when buffer is empty
  ret = tablet_op.handle_mark_delete(share::SCN::invalid_scn());
  ASSERT_EQ(OB_INVALID_ARGUMENT, ret);
}

// ===========================================================================
// handle_mark_delete: basic mark delete with last_major preserved
// ===========================================================================

TEST_F(TestSSMetaTableHelper, handle_mark_delete_basic)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObArenaAllocator alloc;
  auto &proxy = get_sql_client();
  const ObLSID ls_id(8025);
  const ObTabletID tablet_id(200034);
  ObSSMetaBaseOperator base_op;
  ASSERT_EQ(OB_SUCCESS, base_op.init(proxy, OB_ALL_SHARED_META_TNAME));

  ObSSMetaType meta_type;
  meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200034/0/meta", 400, ObStorageObjectType::SHARED_TABLET_META));

  ObString value = make_tablet_meta_value_with_last_major(
      alloc, 1800 /*last_major_snapshot_version*/, false /*mark_delete*/);
  ASSERT_EQ(OB_SUCCESS, insert_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200034/0/meta", 400, ObStorageObjectType::SHARED_TABLET_META),
      value));

  // read via get_tablet_last_major_snapshot_version with meta_value output
  ObSSMetaReadParamV2 param;
  build_tablet_read_param(param, ls_id, tablet_id, make_scn(0));
  param.set_read_mark_delete(true);

  int64_t last_major_snapshot_version = -1;
  bool is_mark_delete = true;
  ObSSMetaOpBufferGuard meta_value_guard;
  ObSSMetaOpBuffer &meta_value = meta_value_guard.get_buffer();
  ASSERT_EQ(OB_SUCCESS, ObSSMetaTableHelper::get_tablet_last_major_snapshot_version(
      param, last_major_snapshot_version, is_mark_delete, &meta_value));
  ASSERT_EQ(1800, last_major_snapshot_version);
  ASSERT_FALSE(is_mark_delete);

  // init ObSSMetaAtomicTabletOp with the read meta_value
  ObSSMetaAtomicOpInitParam op_param;
  op_param.op_id_ = 1;
  op_param.meta_scn_ = make_scn(400);
  op_param.ls_id_ = ls_id;
  op_param.tablet_id_ = tablet_id;
  op_param.reorgan_scn_ = make_scn(0);
  op_param.protocol_type_.type_ = ObAtomicProtocolTypeEnum::TABLET_META;
  op_param.meta_value_ = &meta_value;

  ObSSMetaAtomicTabletOp tablet_op;
  int ret = tablet_op.init(op_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  // call handle_mark_delete with deleted_scn
  const share::SCN deleted_scn = make_scn(12345);
  ret = tablet_op.handle_mark_delete(deleted_scn);
  ASSERT_EQ(OB_SUCCESS, ret);

  // verify cached_buffer_ has mark_delete=true, deleted_scn and last_major preserved
  ASSERT_TRUE(tablet_op.cached_buffer_.is_valid());
  ASSERT_GT(tablet_op.cached_buffer_.data_len_, 0);
  {
    int64_t pos = 0;
    ObSSMetaHeadValue head_value;
    ASSERT_EQ(OB_SUCCESS, head_value.deserialize_value_info(
        tablet_op.cached_buffer_.buf_, tablet_op.cached_buffer_.data_len_, pos));
    ASSERT_TRUE(head_value.get_meta_head_info().is_mark_deleted());
    ASSERT_EQ(deleted_scn, head_value.get_meta_head_info().get_deleted_scn());
    ObSSTabletTableStoreMetaInfo table_store_meta_info;
    head_value.get_meta_head_info().get_table_store_meta(table_store_meta_info);
    ASSERT_EQ(1800, table_store_meta_info.last_major_snapshot_version_);

    bool is_deleted = false;
    SCN got_deleted_scn;
    ASSERT_EQ(OB_SUCCESS, ObSSMetaHeadValue::get_mark_delete_info(
        ObString(tablet_op.cached_buffer_.data_len_, tablet_op.cached_buffer_.buf_),
        is_deleted, got_deleted_scn));
    ASSERT_TRUE(is_deleted);
    ASSERT_EQ(deleted_scn, got_deleted_scn);
  }

  delete_row(base_op, TEST_TENANT_ID, ls_id, meta_type,
      make_meta_key(alloc, "tablet/200034/0/meta", 400, ObStorageObjectType::SHARED_TABLET_META));
}

static blocksstable::MacroBlockId make_in_table_meta_id(
    const ObStorageObjectType type,
    const int64_t second_id,
    const uint64_t meta_scn_or_packed_op_seq,
    const bool is_inner_tablet,
    const int64_t ls_id,
    const int64_t reorganization_scn)
{
  blocksstable::MacroBlockId macro_id;
  macro_id.set_storage_object_type(static_cast<uint64_t>(type));
  macro_id.set_location(blocksstable::ObMacroBlockIdLocation::TABLE);
  macro_id.set_second_id(second_id);
  macro_id.set_ss_third_id(is_inner_tablet, ls_id, reorganization_scn);
  if (ObStorageObjectType::SHARED_TABLET_SUB_META == type) {
    macro_id.set_op_id(static_cast<int64_t>(meta_scn_or_packed_op_seq >> 32));
    macro_id.set_seq_id(static_cast<int64_t>(
        static_cast<uint32_t>(meta_scn_or_packed_op_seq)));
  } else {
    macro_id.set_meta_scn(static_cast<int64_t>(meta_scn_or_packed_op_seq));
    macro_id.set_seq_id(0);
  }
  return macro_id;
}

static void assert_same_meta_id(
    const blocksstable::MacroBlockId &expected,
    const blocksstable::MacroBlockId &actual)
{
  ASSERT_EQ(expected.storage_object_type(), actual.storage_object_type());
  ASSERT_EQ(expected.location(), actual.location());
  ASSERT_EQ(expected.second_id(), actual.second_id());
  ASSERT_EQ(expected.third_id(), actual.third_id());
  ASSERT_EQ(expected.fourth_id(), actual.fourth_id());
  ASSERT_EQ(expected.fifth_id(), actual.fifth_id());
}

static void assert_relative_meta_key(
    const blocksstable::MacroBlockId &macro_id,
    const char *expected_key)
{
  char meta_key[MAX_PATH_SIZE] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, meta_key, sizeof(meta_key), pos));
  ASSERT_STREQ(expected_key, meta_key);
  ASSERT_EQ(STRLEN(expected_key), pos);
  ASSERT_NE('/', meta_key[0]);

  // 覆盖场景：数据库保存裸 meta_key，显式类型重载可直接解析。
  blocksstable::MacroBlockId decoded_id;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
      static_cast<ObStorageObjectType>(macro_id.storage_object_type()),
      meta_key, decoded_id));
  assert_same_meta_id(macro_id, decoded_id);

  // 覆盖场景：iterator/tiered metadata store 使用不显式传类型的重载
  // 解析数据库中的裸 meta_key。
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
      meta_key, decoded_id));
  assert_same_meta_id(macro_id, decoded_id);

  char table_uri[MAX_PATH_SIZE] = {0};
  int64_t table_uri_pos = 0;
  ASSERT_EQ(OB_SUCCESS, databuff_printf(
      table_uri, sizeof(table_uri), table_uri_pos, "%s%s", OB_TABLE_PREFIX, meta_key));

  // 覆盖场景：table device 传入 table:// URI 时，继续复用远端路径解析。
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
      static_cast<ObStorageObjectType>(macro_id.storage_object_type()),
      table_uri, decoded_id));
  assert_same_meta_id(macro_id, decoded_id);

  // 覆盖场景：table device 实际使用的不显式传类型重载也必须支持
  // table:// URI。
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
      table_uri, decoded_id));
  assert_same_meta_id(macro_id, decoded_id);
}

static void assert_relative_meta_path_format(
    const blocksstable::MacroBlockId &macro_id,
    const char *expected_path)
{
  char relative_path[MAX_PATH_SIZE] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      macro_id, relative_path, sizeof(relative_path), pos));
  ASSERT_STREQ(expected_path, relative_path);
  ASSERT_EQ(STRLEN(expected_path), pos);
  ASSERT_NE('/', relative_path[0]);
}

// ===========================================================================
// 测试主题 1：storage object type 的相对路径不带前导 '/'。
//
// 覆盖场景：
// 1. SHARED_TABLET_META：普通 tablet、inner tablet。
// 2. SHARED_TABLET_SUB_META：普通 tablet、inner tablet，且分别覆盖
//    in-table 与 object-storage 两种状态。
// 3. SHARED_LS_CKPT、SHARED_LS_META。
// 4. 每种 in-table 对象都覆盖裸 meta_key/table:// URI，以及显式类型/
//    自动识别类型两组解析入口。
// ===========================================================================
TEST_F(TestSSMetaTableHelper, meta_key_storage_object_type_relative_path_format_and_parse)
{
  // 覆盖场景 1.1：普通 tablet meta 生成无前导 '/' 的裸 meta_key。
  const blocksstable::MacroBlockId tablet_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_TABLET_META,
      200001, 100, false, 0, 2001);
  assert_relative_meta_key(
      tablet_meta_id,
      "tablet/200001/2001/meta/scn00000000000000000100.T20");

  // 覆盖场景 1.2：inner tablet meta 走 ls/{ls_id}/{tablet_name} 格式。
  const blocksstable::MacroBlockId inner_tablet_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_TABLET_META,
      ObTabletID::LS_TX_DATA_TABLET_ID, 101, true, 1001, 0);
  assert_relative_meta_key(
      inner_tablet_meta_id,
      "ls/1001/TX_DATA/meta/scn00000000000000000101.T20");

  // 覆盖场景 1.3：in-table 普通 tablet sub-meta 生成无前导 '/' 的裸 key。
  const blocksstable::MacroBlockId tablet_sub_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_TABLET_SUB_META,
      200001, (1ULL << 32) + 1, false, 0, 0);
  assert_relative_meta_key(
      tablet_sub_meta_id,
      "tablet/200001/0/meta/op1seq0000000001.T21");

  // 覆盖场景 1.4：object-storage 普通 tablet sub-meta 与 in-table
  // 使用相同的无前导 '/' 相对路径格式。
  blocksstable::MacroBlockId object_storage_tablet_sub_meta_id = tablet_sub_meta_id;
  object_storage_tablet_sub_meta_id.set_location(
      blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  assert_relative_meta_path_format(
      object_storage_tablet_sub_meta_id,
      "tablet/200001/0/meta/op1seq0000000001.T21");

  // 覆盖场景 1.5：in-table inner tablet sub-meta 走 ls 目录格式。
  const blocksstable::MacroBlockId inner_tablet_sub_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_TABLET_SUB_META,
      ObTabletID::LS_TX_DATA_TABLET_ID, (2ULL << 32) + 2, true, 1002, 0);
  assert_relative_meta_key(
      inner_tablet_sub_meta_id,
      "ls/1002/TX_DATA/meta/op2seq0000000002.T21");

  // 覆盖场景 1.6：object-storage inner tablet sub-meta 同样不带前导 '/'。
  blocksstable::MacroBlockId object_storage_inner_tablet_sub_meta_id = inner_tablet_sub_meta_id;
  object_storage_inner_tablet_sub_meta_id.set_location(
      blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  assert_relative_meta_path_format(
      object_storage_inner_tablet_sub_meta_id,
      "ls/1002/TX_DATA/meta/op2seq0000000002.T21");

  // 覆盖场景 1.7：LS checkpoint 的相对路径和反向解析。
  const blocksstable::MacroBlockId ls_ckpt_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_LS_CKPT,
      1003, 200, false, 0, 0);
  assert_relative_meta_key(
      ls_ckpt_id,
      "ls/1003/meta/scn00000000000000000200.T30");

  // 覆盖场景 1.8：LS meta 的相对路径和反向解析。
  const blocksstable::MacroBlockId ls_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_LS_META,
      1004, 300, false, 0, 0);
  assert_relative_meta_key(
      ls_meta_id,
      "ls/1004/meta/scn00000000000000000300.T31");
}

// ===========================================================================
// 测试主题 2：__all_shared_meta 的 prefix 和 meta_key 使用无前导 '/'
// 的裸 key；ObSSMetaInfoUtil 按裸 meta_key 调用解析接口。
//
// 覆盖场景：
// 1. 普通 tablet 的有效/无效 reorganization SCN 前缀。
// 2. 四种 inner tablet 名称，且 reorganization SCN 有效/无效时格式一致。
// 3. LS meta 与 LS checkpoint 共用的 LS 前缀。
// 4. 普通 tablet、inner tablet、LS meta 裸 key 的反向解析。
// ===========================================================================
TEST_F(TestSSMetaTableHelper, meta_key_info_util_prefix_and_parse)
{
  // 覆盖场景 2.1：普通 tablet + 有效 reorganization SCN，
  // 使用 tablet/{tablet_id}/{reorganization_scn}/meta。
  ObSqlString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
      ObLSID(1001), ObTabletID(200001), TestSSMetaTableHelper::make_scn(2001), prefix));
  ASSERT_STREQ("tablet/200001/2001/meta", prefix.ptr());

  // 覆盖场景 2.2：普通 tablet + 无效 reorganization SCN，
  // prefix 退化到 tablet/{tablet_id}/，仍然不能带前导 '/'。
  prefix.reset();
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
      ObLSID(1001), ObTabletID(200001), SCN::invalid_scn(), prefix));
  ASSERT_STREQ("tablet/200001/", prefix.ptr());

  // 覆盖场景 2.3：四种 inner tablet 都使用
  // ls/{ls_id}/{tablet_name}/meta；reorganization SCN 不参与路径。
  struct InnerTabletPrefixCase {
    uint64_t tablet_id_;
    const char *tablet_name_;
  };
  const InnerTabletPrefixCase inner_tablet_cases[] = {
    {ObTabletID::LS_TX_CTX_TABLET_ID, "TX_CTX"},
    {ObTabletID::LS_TX_DATA_TABLET_ID, "TX_DATA"},
    {ObTabletID::LS_LOCK_TABLET_ID, "TX_LOCK"},
    {ObTabletID::LS_REORG_INFO_TABLET_ID, "REORG_INFO"},
  };
  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(inner_tablet_cases) / sizeof(inner_tablet_cases[0]));
       ++i) {
    ObSqlString expected_prefix;
    ASSERT_EQ(OB_SUCCESS, expected_prefix.assign_fmt(
        "ls/1002/%s/meta", inner_tablet_cases[i].tablet_name_));

    prefix.reset();
    ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
        ObLSID(1002), ObTabletID(inner_tablet_cases[i].tablet_id_),
        SCN::min_scn(), prefix));
    ASSERT_STREQ(expected_prefix.ptr(), prefix.ptr());

    prefix.reset();
    ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
        ObLSID(1002), ObTabletID(inner_tablet_cases[i].tablet_id_),
        SCN::invalid_scn(), prefix));
    ASSERT_STREQ(expected_prefix.ptr(), prefix.ptr());
  }

  // 覆盖场景 2.4：LS meta 的查询前缀不带前导 '/'。
  ObSSMetaReadParamV2 ls_read_param;
  TestSSMetaTableHelper::build_ls_read_param(ls_read_param, ObLSID(1003));
  prefix.reset();
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_meta_key_prefix(ls_read_param, prefix));
  ASSERT_STREQ("ls/1003/meta", prefix.ptr());

  // 覆盖场景 2.5：LS checkpoint 与 LS meta 共用 SSMETA_LS_PREFIX_FMT，
  // 也必须生成无前导 '/' 的前缀。
  ObSSMetaReadParamV2 checkpoint_read_param;
  checkpoint_read_param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::CHECKPOINT,
      ObLSID(1004), SCN::min_scn(), true /*is_in_table*/);
  prefix.reset();
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_meta_key_prefix(
      checkpoint_read_param, prefix));
  ASSERT_STREQ("ls/1004/meta", prefix.ptr());

  ObLSID ls_id;
  ObTabletID tablet_id;
  SCN reorganization_scn;
  SCN meta_scn;

  // 覆盖场景 2.6：普通 tablet meta 从数据库裸 key 解析。
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::parse_tablet_meta_key(
      ObString::make_string(
          "tablet/200001/2001/meta/scn00000000000000000100.T20"),
      ls_id, tablet_id, reorganization_scn, meta_scn));
  ASSERT_FALSE(ls_id.is_valid());
  ASSERT_EQ(200001UL, tablet_id.id());
  ASSERT_EQ(2001UL, reorganization_scn.get_val_for_inner_table_field());
  ASSERT_EQ(100UL, meta_scn.get_val_for_inner_table_field());

  // 覆盖场景 2.7：inner tablet meta 从数据库裸 key 解析，并恢复 ls_id。
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::parse_tablet_meta_key(
      ObString::make_string(
          "ls/1002/TX_DATA/meta/scn00000000000000000101.T20"),
      ls_id, tablet_id, reorganization_scn, meta_scn));
  ASSERT_EQ(1002, ls_id.id());
  ASSERT_EQ(static_cast<uint64_t>(ObTabletID::LS_TX_DATA_TABLET_ID), tablet_id.id());
  ASSERT_EQ(SCN::min_scn(), reorganization_scn);
  ASSERT_EQ(101UL, meta_scn.get_val_for_inner_table_field());

  // 覆盖场景 2.8：LS meta 从数据库裸 key 解析。
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::parse_ls_meta_key(
      ObString::make_string(
          "ls/1003/meta/scn00000000000000000300.T31"),
      ls_id, meta_scn));
  ASSERT_EQ(1003, ls_id.id());
  ASSERT_EQ(300UL, meta_scn.get_val_for_inner_table_field());

}

// ===========================================================================
// 测试主题 3：ObDirManager 生成无前导 '/' 的扫描目录，并与 tiered
// metadata store/iterator 中的裸 sub-meta key 对齐。
//
// 覆盖场景：
// 1. 普通 tablet 目录与 sub-meta key 可直接做前缀匹配。
// 2. 四种 inner tablet 的相对目录均无前导 '/'。
// 3. inner tablet 目录与 sub-meta key 可直接做前缀匹配。
// ===========================================================================
TEST_F(TestSSMetaTableHelper, meta_key_tiered_metadata_relative_dir_matches_sub_meta_key)
{
  // 覆盖场景 3.1：get_shared_tablet_meta_relative_dir 普通 tablet 分支，
  // 验证生成结果不包含前导 '/'。
  char relative_dir[MAX_PATH_SIZE] = {0};
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_meta_relative_dir(
      relative_dir, sizeof(relative_dir), false, 0, 200001, 2001));
  ASSERT_STREQ("tablet/200001/2001/meta", relative_dir);

  // 覆盖场景 3.2：普通 tablet 的扫描目录与数据库 sub-meta 裸 key 对齐。
  const blocksstable::MacroBlockId tablet_sub_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_TABLET_SUB_META,
      200001, (1ULL << 32) + 1, false, 0, 2001);
  char meta_key[MAX_PATH_SIZE] = {0};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      tablet_sub_meta_id, meta_key, sizeof(meta_key), pos));
  ASSERT_EQ(0, STRNCMP(meta_key, relative_dir, STRLEN(relative_dir)));
  ASSERT_EQ('/', meta_key[STRLEN(relative_dir)]);

  // 覆盖场景 3.3：ObTieredMetadataStore/ObSSTieredMetaTableIterator
  // 获取到裸 meta_key 后可直接解析。
  blocksstable::MacroBlockId decoded_id;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
      ObStorageObjectType::SHARED_TABLET_SUB_META, meta_key, decoded_id));
  assert_same_meta_id(tablet_sub_meta_id, decoded_id);

  // 覆盖场景 3.4：get_shared_tablet_meta_relative_dir 的 inner tablet
  // 分支覆盖全部四种 tablet_name，且 reorganization_scn 不进入目录。
  struct InnerTabletDirCase {
    uint64_t tablet_id_;
    const char *tablet_name_;
  };
  const InnerTabletDirCase inner_tablet_cases[] = {
    {ObTabletID::LS_TX_CTX_TABLET_ID, "TX_CTX"},
    {ObTabletID::LS_TX_DATA_TABLET_ID, "TX_DATA"},
    {ObTabletID::LS_LOCK_TABLET_ID, "TX_LOCK"},
    {ObTabletID::LS_REORG_INFO_TABLET_ID, "REORG_INFO"},
  };
  for (int64_t i = 0;
       i < static_cast<int64_t>(sizeof(inner_tablet_cases) / sizeof(inner_tablet_cases[0]));
       ++i) {
    MEMSET(relative_dir, 0, sizeof(relative_dir));
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_meta_relative_dir(
        relative_dir, sizeof(relative_dir), true, 1002,
        inner_tablet_cases[i].tablet_id_, 777 /*inner tablet 不使用该值*/));
    ObSqlString expected_dir;
    ASSERT_EQ(OB_SUCCESS, expected_dir.assign_fmt(
        "ls/1002/%s/meta", inner_tablet_cases[i].tablet_name_));
    ASSERT_STREQ(expected_dir.ptr(), relative_dir);
  }

  // 覆盖场景 3.5：inner tablet 的扫描目录与数据库 sub-meta 裸 key 对齐。
  MEMSET(relative_dir, 0, sizeof(relative_dir));
  ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_shared_tablet_meta_relative_dir(
      relative_dir, sizeof(relative_dir), true, 1002,
      ObTabletID::LS_TX_DATA_TABLET_ID, 0));
  ASSERT_STREQ("ls/1002/TX_DATA/meta", relative_dir);

  const blocksstable::MacroBlockId inner_tablet_sub_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_TABLET_SUB_META,
      ObTabletID::LS_TX_DATA_TABLET_ID, (2ULL << 32) + 2, true, 1002, 0);
  MEMSET(meta_key, 0, sizeof(meta_key));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::to_relative_remote_path_format(
      inner_tablet_sub_meta_id, meta_key, sizeof(meta_key), pos));
  ASSERT_EQ(0, STRNCMP(meta_key, relative_dir, STRLEN(relative_dir)));
  ASSERT_EQ('/', meta_key[STRLEN(relative_dir)]);

  // 覆盖场景 3.6：inner tablet 裸 key 同样可直接解析。
  ASSERT_EQ(OB_SUCCESS, blocksstable::SSObjUtil::relative_remote_path_to_macro_id(
      ObStorageObjectType::SHARED_TABLET_SUB_META, meta_key, decoded_id));
  assert_same_meta_id(inner_tablet_sub_meta_id, decoded_id);
}

// ===========================================================================
// 测试主题 4：ObSSBackupUtils 直接使用 codec 生成的无前导 '/'
// 相对路径与 backup_set_dir 拼接。
//
// 覆盖场景：普通 tablet、inner tablet、LS 三种目录层级，验证相对路径
// 与最终备份 URI 都不存在重复分隔符。
// ===========================================================================
TEST_F(TestSSMetaTableHelper, meta_key_backup_relative_path_join)
{
  ObBackupPath backup_set_dir;
  ASSERT_EQ(OB_SUCCESS, backup_set_dir.init(
      ObString::make_string("file:///backup/backup_set/")));

  // 覆盖场景 4.1：普通 tablet sub-meta 的相对路径无需额外删除首字符，
  // join 后只有一个目录分隔符。
  const blocksstable::MacroBlockId tablet_sub_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_TABLET_SUB_META,
      200001, (1ULL << 32) + 1, false, 0, 0);
  char relative_path[MAX_PATH_SIZE] = {0};
  ASSERT_EQ(OB_SUCCESS, backup::ObSSBackupUtils::get_macro_block_relative_path(
      tablet_sub_meta_id, relative_path, sizeof(relative_path)));
  ASSERT_STREQ("tablet/200001/0/meta/op1seq0000000001.T21", relative_path);

  ObBackupPath backup_path;
  ASSERT_EQ(OB_SUCCESS, backup::ObSSBackupUtils::get_macro_block_backup_path(
      tablet_sub_meta_id, backup_set_dir, backup_path));
  ASSERT_STREQ(
      "file:///backup/backup_set/tablet/200001/0/meta/op1seq0000000001.T21",
      backup_path.get_ptr());

  // 覆盖场景 4.2：inner tablet sub-meta 的 ls 目录也可直接 join。
  const blocksstable::MacroBlockId inner_tablet_sub_meta_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_TABLET_SUB_META,
      ObTabletID::LS_LOCK_TABLET_ID, (2ULL << 32) + 2, true, 1002, 0);
  MEMSET(relative_path, 0, sizeof(relative_path));
  ASSERT_EQ(OB_SUCCESS, backup::ObSSBackupUtils::get_macro_block_relative_path(
      inner_tablet_sub_meta_id, relative_path, sizeof(relative_path)));
  ASSERT_STREQ("ls/1002/TX_LOCK/meta/op2seq0000000002.T21", relative_path);

  ObBackupPath inner_backup_path;
  ASSERT_EQ(OB_SUCCESS, backup::ObSSBackupUtils::get_macro_block_backup_path(
      inner_tablet_sub_meta_id, backup_set_dir, inner_backup_path));
  ASSERT_STREQ(
      "file:///backup/backup_set/ls/1002/TX_LOCK/meta/op2seq0000000002.T21",
      inner_backup_path.get_ptr());

  // 覆盖场景 4.3：LS checkpoint 的 ls/{ls_id}/meta 目录可直接 join。
  const blocksstable::MacroBlockId ls_ckpt_id = make_in_table_meta_id(
      ObStorageObjectType::SHARED_LS_CKPT,
      1003, 200, false, 0, 0);
  MEMSET(relative_path, 0, sizeof(relative_path));
  ASSERT_EQ(OB_SUCCESS, backup::ObSSBackupUtils::get_macro_block_relative_path(
      ls_ckpt_id, relative_path, sizeof(relative_path)));
  ASSERT_STREQ("ls/1003/meta/scn00000000000000000200.T30", relative_path);

  ASSERT_EQ(OB_SUCCESS, backup::ObSSBackupUtils::get_macro_block_backup_path(
      ls_ckpt_id, backup_set_dir, backup_path));
  ASSERT_STREQ(
      "file:///backup/backup_set/ls/1003/meta/scn00000000000000000200.T30",
      backup_path.get_ptr());
}

} // share
} // oceanbase

int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
