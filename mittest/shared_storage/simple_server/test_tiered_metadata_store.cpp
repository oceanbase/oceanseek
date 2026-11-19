/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>

#define protected public
#define private public
#include "src/storage/tiered_metadata_store/ob_tiered_metadata_store.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_info_iterator.h"
#include "storage/backup/ob_ss_backup_restore_util.h"
#undef private
#undef protected

#include "src/storage/backup/ob_ss_backup_utils.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/ob_test_util.h"
#include "lib/utility/ob_tracepoint.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/ob_remote_io_adapter.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "src/storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "src/storage/tiered_metadata_store/ob_tiered_metadata_object_manager.h"
#include "src/storage/ss_garbage_collector/ob_ss_gc_util.h"
#include "lib/restore/ob_object_storage_base.h"
#include "lib/time/ob_time_utility.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/high_availability/ob_ss_storage_ha_writer.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/io/ob_io_define.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
namespace sslog
{


} // namespace sslog
} // namespace oceanbase

using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::lib;

static const int64_t kDefaultMetaType =
    static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);

class ScopedNamedEvent
{
public:
  ScopedNamedEvent() : name_(nullptr), is_inited_(false) {}
  ~ScopedNamedEvent() { reset(); }

  int init(const char *name, const int error_code, const int64_t occur)
  {
    int ret = OB_SUCCESS;
    EventItem item;
    if (OB_ISNULL(name) || OB_SUCCESS == error_code || occur <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      item.error_code_ = error_code;
      item.occur_ = occur;
      item.trigger_freq_ = 0;
      item.cond_ = GETTID();
      if (OB_FAIL(EventTable::set_event(name, item))) {
      } else {
        name_ = name;
        is_inited_ = true;
      }
    }
    return ret;
  }

  void reset()
  {
    if (is_inited_) {
      EventItem item;
      (void) EventTable::set_event(name_, item);
      name_ = nullptr;
      is_inited_ = false;
    }
  }

private:
  const char *name_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ScopedNamedEvent);
};

class CountingTieredDeleteSQLClient : public ObMySQLProxy
{
public:
  explicit CountingTieredDeleteSQLClient(ObISQLClient &client, const int64_t fail_write_idx = 0)
    : client_(client), fail_write_idx_(fail_write_idx), write_count_(0) {}
  using ObMySQLProxy::write;
  int write(const uint64_t tenant_id, const char *sql,
            const int32_t group_id, int64_t &affected_rows) override
  {
    int ret = OB_SUCCESS;
    affected_rows = 0;
    if (OB_ISNULL(sql)) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      ++write_count_;
      EXPECT_EQ(OBCG_SHARED_META, group_id);
      EXPECT_EQ(gen_meta_tenant_id(oceanbase::share::ObTenantEnv::get_tenant_local()->id()),
                tenant_id);
      if (write_count_ == fail_write_idx_) {
        ret = OB_TIMEOUT;
      } else if (OB_FAIL(client_.write(tenant_id, sql, group_id, affected_rows))) {
      }
    }
    return ret;
  }
  int64_t get_write_count() const { return write_count_; }
private:
  ObISQLClient &client_;
  int64_t fail_write_idx_;
  int64_t write_count_;
  DISALLOW_COPY_AND_ASSIGN(CountingTieredDeleteSQLClient);
};

namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{

class TestTieredMetadataStoreV2 : public ObSimpleClusterTestBase
{
public:
  TestTieredMetadataStoreV2()
      : ObSimpleClusterTestBase("test_tiered_metadata_store_dir", "50G", "50G", "50G")
  {}

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!tenant_created_) {
      OK(create_tenant_with_retry("tt1", "5G", "10G", false/*oracle_mode*/, 1));
      OK(get_tenant_id(tenant_id_));
      ASSERT_NE(0, tenant_id_);
      tenant_created_ = true;
      share::ObTenantSwitchGuard tguard;
      OK(tguard.switch_to(tenant_id_));
      OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
    }
  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

protected:
  MacroBlockId build_sub_meta_in_table_id(const uint64_t tablet_id,
                                          const uint64_t macro_seq,
                                          const uint64_t ls_id = 1001) const
  {
    MacroBlockId id;
    id.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_TABLET_SUB_META));
    id.set_location(ObMacroBlockIdLocation::TABLE);
    id.set_second_id(tablet_id);
    id.set_ss_third_id(ObTabletID(tablet_id).is_ls_inner_tablet(), ls_id, 0/*reorg_scn*/);
    id.set_op_id(1);
    id.set_seq_id(macro_seq);
    return id;
  }

  MacroBlockId inc_seq(const MacroBlockId &id) const
  {
    MacroBlockId res = id;
    res.set_seq_id(id.seq_id() + 1);
    return res;
  }

  static MacroBlockId to_remote_id(const MacroBlockId &in_table_id)
  {
    MacroBlockId remote_id(in_table_id);
    remote_id.set_location(ObMacroBlockIdLocation::OBJECT_STORAGE);
    return remote_id;
  }

  static void write_remote_object(const MacroBlockId &remote_id)
  {
    static char write_buf[1024 * 1024];
    MEMSET(write_buf, 'r', sizeof(write_buf));
    ObStorageObjectWriteInfo write_info;
    write_info.io_desc_.set_wait_event(1);
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = sizeof(write_buf);
    write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.write_strategy_ = ObStorageObjectWriteStrategy::WRITE_THROUGH;

    ObStorageObjectHandle object_handle;
    ASSERT_EQ(OB_SUCCESS, object_handle.set_macro_block_id(remote_id));
    ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info, object_handle));
    ASSERT_EQ(OB_SUCCESS, object_handle.wait());
  }

  static void assert_remote_object_exist(const MacroBlockId &remote_id, const bool expected)
  {
    bool is_exist = false;
    ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::is_exist(
        MTL_ID(), remote_id, GCONF.observer_id, is_exist));
    ASSERT_EQ(expected, is_exist);
  }

  static void delete_remote_object(const MacroBlockId &remote_id)
  {
    const int ret = ObRemoteIoAdapter::del_file(
        MTL_ID(), remote_id, 0/*ls_epoch_id*/, GCONF.observer_id);
    ASSERT_TRUE(OB_SUCCESS == ret || OB_OBJECT_NOT_EXIST == ret);
  }

  common::ObISQLClient &get_sql_client()
  {
    return get_curr_simple_server().get_observer().get_mysql_proxy();
  }

  // Query the meta_type column actually stored in __all_shared_tiered_meta for the row(s)
  // of a given macro_id. Returns the (shared) meta_type value and the number of part rows.
  int query_stored_meta_type(ObTieredMetadataStore &store,
                             const MacroBlockId &macro_id,
                             int64_t &meta_type,
                             int64_t &row_cnt)
  {
    int ret = OB_SUCCESS;
    meta_type = -1;
    row_cnt = 0;
    share::ObLSID ls_id;
    ObSSTieredMetaType unused_type;
    char key_buf[1024];
    int64_t key_len = 0;
    ObSqlString sql;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
    if (OB_FAIL(store.macro_block_id_to_ss_meta_key_(
            macro_id, ls_id, unused_type, key_buf, sizeof(key_buf), key_len))) {
    } else if (OB_FAIL(sql.assign_fmt(
            "SELECT meta_type FROM %s "
            "WHERE tenant_id = %lu AND ls_id = %ld AND meta_key = '%.*s'",
            OB_ALL_SHARED_TIERED_META_TNAME,
            tenant_id_,
            ls_id.id(),
            static_cast<int>(key_len),
            key_buf))) {
    } else {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        if (OB_FAIL(get_sql_client().read(res, exec_tenant_id, sql.ptr()))) {
        } else if (OB_ISNULL(res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          sqlclient::ObMySQLResult *result = res.get_result();
          while (OB_SUCC(ret = result->next())) {
            if (OB_FAIL(result->get_int("meta_type", meta_type))) {
              break;
            }
            ++row_cnt;
          }
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          }
        }
      }
    }
    return ret;
  }

  // 直接向表中插入一个物理 part，用于构造正常写接口不会产生的异常分片数据。
  int insert_raw_part(ObTieredMetadataStore &store,
                      const MacroBlockId &macro_id,
                      const int64_t meta_type,
                      const int64_t part,
                      const char *hex_value)
  {
    int ret = OB_SUCCESS;
    share::ObLSID ls_id;
    ObSSTieredMetaType unused_type;
    char key_buf[1024];
    int64_t key_len = 0;
    int64_t affected_rows = 0;
    ObSqlString sql;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
    if (OB_ISNULL(hex_value)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(store.macro_block_id_to_ss_meta_key_(
            macro_id, ls_id, unused_type, key_buf, sizeof(key_buf), key_len))) {
    } else if (OB_FAIL(sql.assign_fmt(
            "INSERT INTO %s "
            "(tenant_id, ls_id, meta_type, meta_key, part, meta_value) "
            "VALUES (%lu, %ld, %ld, '%.*s', %ld, X'%s')",
            OB_ALL_SHARED_TIERED_META_TNAME,
            tenant_id_,
            ls_id.id(),
            meta_type,
            static_cast<int>(key_len),
            key_buf,
            part,
            hex_value))) {
    } else if (OB_FAIL(get_sql_client().write(
            exec_tenant_id, sql.ptr(), affected_rows))) {
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
    }
    return ret;
  }

  struct LegacyPartSpec
  {
    int64_t length_;
    unsigned char fill_byte_;
  };

  int build_repeated_parts_value(ObIAllocator &allocator,
                                 const LegacyPartSpec *part_specs,
                                 const int64_t part_count,
                                 ObString &value)
  {
    int ret = OB_SUCCESS;
    int64_t total_len = 0;
    value.reset();
    if (OB_ISNULL(part_specs) || part_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
      if (OB_UNLIKELY(part_specs[i].length_ <= 0)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        total_len += part_specs[i].length_;
      }
    }
    if (OB_SUCC(ret)) {
      char *value_buf = static_cast<char *>(allocator.alloc(total_len));
      if (OB_ISNULL(value_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        int64_t offset = 0;
        for (int64_t i = 0; i < part_count; ++i) {
          MEMSET(value_buf + offset,
                 static_cast<int>(part_specs[i].fill_byte_),
                 part_specs[i].length_);
          offset += part_specs[i].length_;
        }
        value = ObString(
            static_cast<ObString::obstr_size_t>(total_len), value_buf);
      }
    }
    return ret;
  }

  // Simulate the legacy writer, which split raw data every 256KB because it
  // applied the 512KB limit to hex characters. All parts are inserted by one
  // SQL statement, matching the production writer's atomic write behavior.
  // Distinct bytes per part make ordering and offset errors observable.
  int insert_legacy_repeated_parts(ObTieredMetadataStore &store,
                                   const MacroBlockId &macro_id,
                                   const int64_t meta_type,
                                   const LegacyPartSpec *part_specs,
                                   const int64_t part_count,
                                   int64_t &affected_rows)
  {
    int ret = OB_SUCCESS;
    share::ObLSID ls_id;
    ObSSTieredMetaType unused_type;
    char key_buf[1024];
    int64_t key_len = 0;
    ObSqlString sql;
    const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id_);
    affected_rows = 0;
    if (OB_ISNULL(part_specs) || part_count <= 0) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(store.macro_block_id_to_ss_meta_key_(
            macro_id, ls_id, unused_type, key_buf, sizeof(key_buf), key_len))) {
    } else if (OB_FAIL(sql.assign_fmt(
            "INSERT INTO %s "
            "(tenant_id, ls_id, meta_type, meta_key, part, meta_value) VALUES ",
            OB_ALL_SHARED_TIERED_META_TNAME))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
      if (OB_FAIL(sql.append_fmt(
              "%s(%lu, %ld, %ld, '%.*s', %ld, REPEAT(X'%02X', %ld))",
              i > 0 ? ", " : "",
              tenant_id_,
              ls_id.id(),
              meta_type,
              static_cast<int>(key_len),
              key_buf,
              i,
              static_cast<unsigned int>(part_specs[i].fill_byte_),
              part_specs[i].length_))) {
      }
    }
    if (OB_SUCC(ret)) {
      ret = get_sql_client().write(
          exec_tenant_id, sql.ptr(), affected_rows);
    }
    return ret;
  }

protected:
  static bool tenant_created_;
  static uint64_t tenant_id_;
};

bool TestTieredMetadataStoreV2::tenant_created_ = false;
uint64_t TestTieredMetadataStoreV2::tenant_id_ = 0;

// ============================================================================
// Test: init
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_init_basic)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  // verify init succeeded by performing a valid operation
  const int64_t tablet_id = 399999;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestTieredMetadataStoreV2, test_init_twice)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));
  ASSERT_EQ(OB_INIT_TWICE, store.init(tenant_id_, &get_sql_client()));
}

TEST_F(TestTieredMetadataStoreV2, test_init_invalid_args)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.init(OB_INVALID_TENANT_ID, &get_sql_client()));

  ObTieredMetadataStore store2;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store2.init(tenant_id_, nullptr));
}

TEST_F(TestTieredMetadataStoreV2, test_macro_block_id_to_path_validation)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  const MacroBlockId valid_id = build_sub_meta_in_table_id(300000, 1);
  const char *expected_path = "tablet/300000/0/meta/op1seq0000000001.T21";
  const int64_t expected_len = STRLEN(expected_path);
  char path[common::MAX_PATH_SIZE] = {0};
  int64_t pos = 0;

  ASSERT_TRUE(valid_id.is_valid());
  ASSERT_EQ(OB_SUCCESS,
      store.macro_block_id_to_path_(valid_id, path, sizeof(path), pos));
  ASSERT_EQ(expected_len, pos);
  ASSERT_STREQ(expected_path, path);

  // 无效版本即使对象字段合法，也不能生成路径。
  MacroBlockId invalid_version_id = valid_id;
  invalid_version_id.ss_version_ = 0;
  ASSERT_FALSE(invalid_version_id.is_valid());
  ASSERT_TRUE(SSObjUtil::is_valid(invalid_version_id));
  pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      store.macro_block_id_to_path_(
          invalid_version_id, path, sizeof(path), pos));

  // 校验空缓冲区和路径末尾空字符所需的空间。
  pos = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT,
      store.macro_block_id_to_path_(
          valid_id, nullptr, common::MAX_PATH_SIZE, pos));

  pos = 0;
  ASSERT_EQ(OB_SIZE_OVERFLOW,
      store.macro_block_id_to_path_(valid_id, path, 0, pos));

  pos = 0;
  ASSERT_EQ(OB_SIZE_OVERFLOW,
      store.macro_block_id_to_path_(valid_id, path, expected_len, pos));

  MEMSET(path, 0, sizeof(path));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS,
      store.macro_block_id_to_path_(valid_id, path, expected_len + 1, pos));
  ASSERT_EQ(expected_len, pos);
  ASSERT_STREQ(expected_path, path);
}

// ============================================================================
// Test: write_meta + read_meta
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_write_and_read_meta)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300001;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  const char *write_data = "hello_tiered_metadata_store_v2";
  ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  const int64_t buf_size = 1024;
  char read_buf[buf_size];
  int64_t actual_size = 0;
  ASSERT_EQ(OB_SUCCESS, store.read_meta(macro_id, read_buf, buf_size, actual_size));
  ASSERT_EQ(meta_value.length(), actual_size);
  ASSERT_EQ(0, MEMCMP(read_buf, write_data, actual_size));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
}

// 覆盖模块：ObTieredMetadataStore、ObTieredMetadataObjectManager 和
// ObTieredMetaTableRowData。
// 测试流程：写入 Tiered 元数据，通过对象管理器分别读取默认格式和不含 rowkey 的
// 完整行格式，最后校验序列化、缓冲区不足及数据不存在的异常分支。
TEST_F(TestTieredMetadataStoreV2, test_row_data_serialization)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300019;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);
  const int64_t meta_type =
      static_cast<int64_t>(ObSSTieredMetaTypeEnum::CO_SSTABLE);
  const char *write_data = "tiered_meta_table_non_rowkey_data";
  const ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS,
            store.write_meta(macro_id, meta_type, meta_value));

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMRowDataTest"));
  ObTieredMetaTableRowData row_data;
  ASSERT_EQ(OB_SUCCESS,
            store.read_meta_row_data(macro_id, allocator, row_data));
  ASSERT_TRUE(row_data.is_valid());
  ASSERT_EQ(meta_type, row_data.meta_type_);
  ASSERT_EQ(meta_value.length(), row_data.meta_value_.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(),
                      row_data.meta_value_.ptr(),
                      meta_value.length()));

  const int64_t payload_size = row_data.get_serialize_size();
  char *payload_buf = static_cast<char *>(allocator.alloc(payload_size));
  ASSERT_NE(nullptr, payload_buf);
  {
    ObObjectStorageTenantGuard storage_guard(
        tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
    int64_t actual_size = -1;
    ASSERT_EQ(OB_SUCCESS,
              ObTieredMetadataObjectManager::
                  read_tiered_metadata_from_table(
                      macro_id,
                      ObObjectReadResultFormat::DEFAULT,
                      payload_buf,
                      payload_size,
                      actual_size));
    ASSERT_EQ(meta_value.length(), actual_size);
    ASSERT_EQ(0, MEMCMP(meta_value.ptr(), payload_buf, actual_size));

    actual_size = -1;
    ASSERT_EQ(OB_IO_ERROR,
              ObTieredMetadataObjectManager::
                  read_tiered_metadata_from_table(
                      macro_id,
                      ObObjectReadResultFormat::DEFAULT,
                      payload_buf,
                      meta_value.length() - 1,
                      actual_size));
    ASSERT_EQ(0, actual_size);

    actual_size = -1;
    ASSERT_EQ(OB_SUCCESS,
              ObTieredMetadataObjectManager::
                  read_tiered_metadata_from_table(
                      macro_id,
                      ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY,
                      payload_buf,
                      payload_size,
                      actual_size));
    ASSERT_EQ(payload_size, actual_size);

    ObTieredMetaTableRowData decoded_row;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS,
              decoded_row.deserialize(payload_buf, actual_size, pos));
    ASSERT_EQ(actual_size, pos);
    ASSERT_TRUE(decoded_row.is_valid());
    ASSERT_EQ(meta_type, decoded_row.meta_type_);
    ASSERT_EQ(meta_value.length(), decoded_row.meta_value_.length());
    ASSERT_EQ(0, MEMCMP(meta_value.ptr(),
                        decoded_row.meta_value_.ptr(),
                        meta_value.length()));

    actual_size = -1;
    ASSERT_EQ(OB_IO_ERROR,
              ObTieredMetadataObjectManager::
                  read_tiered_metadata_from_table(
                      macro_id,
                      ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY,
                      payload_buf,
                      payload_size - 1,
                      actual_size));
    ASSERT_EQ(0, actual_size);

    MacroBlockId missing_id = build_sub_meta_in_table_id(tablet_id, 99);
    actual_size = -1;
    ASSERT_EQ(OB_OBJECT_NOT_EXIST,
              ObTieredMetadataObjectManager::
                  read_tiered_metadata_from_table(
                      missing_id,
                      ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY,
                      payload_buf,
                      payload_size,
                      actual_size));
    ASSERT_EQ(0, actual_size);
  }

  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
}

// 覆盖模块：ObTieredMetadataObjectManager 的读取结果格式参数校验。
// 测试流程：分别传入 MAX 和越界枚举值，验证对象 I/O 错误映射且 actual_size 归零。
TEST_F(TestTieredMetadataStoreV2, test_invalid_read_result_format)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  const int64_t tablet_id = 300026;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);
  char read_buf[128] = {0};
  int64_t actual_size = -1;
  ObObjectStorageTenantGuard storage_guard(
      tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);

  ASSERT_EQ(OB_IO_ERROR,
            ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                macro_id,
                ObObjectReadResultFormat::MAX,
                read_buf,
                sizeof(read_buf),
                actual_size));
  ASSERT_EQ(0, actual_size);

  actual_size = -1;
  ASSERT_EQ(OB_IO_ERROR,
            ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                macro_id,
                static_cast<ObObjectReadResultFormat>(127),
                read_buf,
                sizeof(read_buf),
                actual_size));
  ASSERT_EQ(0, actual_size);
}

// 覆盖模块：ObTieredMetadataObjectManager、ObObjectManager 和 ObTableDevice 的通用对象读写链路。
// 测试流程：通过通用对象接口写入非默认 meta_type，检查物理列后分别读取 value 和完整行数据，
// 验证写入参数及两种读取结果格式均能沿完整 IO 链路正确传递。
TEST_F(TestTieredMetadataStoreV2, test_read_object_result_format)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const uint64_t ls_id = 1001;
  const uint64_t tablet_id = 300023;
  const uint64_t data_seq = 1;
  const MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, data_seq, ls_id);
  DEFER({
    EXPECT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  });

  const int64_t meta_type =
      static_cast<int64_t>(ObSSTieredMetaTypeEnum::SSTABLE_META);
  const char *write_data = "object_manager_tiered_meta_row_data";
  const ObString meta_value(STRLEN(write_data), write_data);

  ObStorageObjectOpt write_opt;
  write_opt.set_ss_tablet_sub_meta_opt(
      ls_id,
      tablet_id,
      1 /* op_id */,
      data_seq,
      ObTabletID(tablet_id).is_ls_inner_tablet(),
      0 /* reorganization_scn */,
      false /* is_object_storage */);
  ObStorageObjectWriteInfo write_info;
  write_info.buffer_ = write_data;
  write_info.offset_ = 0;
  write_info.size_ = meta_value.length();
  write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  write_info.io_desc_.set_unsealed();
  write_info.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
  write_info.mtl_tenant_id_ = MTL_ID();
  write_info.set_ls_epoch_id(0);
  write_info.set_meta_type(meta_type);
  ASSERT_TRUE(write_info.is_valid());

  ObStorageObjectHandle write_handle;
  ASSERT_EQ(OB_SUCCESS,
            ObTieredMetadataObjectManager::write_object(
                write_opt, write_info, write_handle));
  ASSERT_EQ(macro_id, write_handle.get_macro_id());

  int64_t stored_meta_type = -1;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
            query_stored_meta_type(
                store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(1, row_cnt);
  ASSERT_EQ(meta_type, stored_meta_type);

  const int64_t buf_size = 1024;
  char read_buf[buf_size] = { 0 };
  ObStorageObjectReadInfo read_info;
  read_info.macro_block_id_ = macro_id;
  read_info.offset_ = 0;
  read_info.size_ = buf_size;
  read_info.io_desc_.set_wait_event(1);
  read_info.buf_ = read_buf;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  read_info.mtl_tenant_id_ = MTL_ID();
  read_info.set_ls_id(ls_id);
  read_info.set_effective_tablet_id(ObTabletID(tablet_id));

  ObStorageObjectHandle default_handle;
  ASSERT_EQ(OB_SUCCESS,
            ObObjectManager::read_object(
                read_info, default_handle));
  ASSERT_EQ(meta_value.length(), default_handle.get_data_size());
  ASSERT_EQ(0,
            MEMCMP(meta_value.ptr(),
                   default_handle.get_buffer(),
                   meta_value.length()));
  default_handle.reset();

  MEMSET(read_buf, 0, sizeof(read_buf));
  read_info.set_result_format(
      ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY);
  ObStorageObjectHandle row_handle;
  ASSERT_EQ(OB_SUCCESS,
            ObObjectManager::read_object(
                read_info, row_handle));

  ObTieredMetaTableRowData row_data;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            row_data.deserialize(
                row_handle.get_buffer(),
                row_handle.get_data_size(),
                pos));
  ASSERT_EQ(row_handle.get_data_size(), pos);
  ASSERT_TRUE(row_data.is_valid());
  ASSERT_EQ(meta_type, row_data.meta_type_);
  ASSERT_EQ(meta_value.length(), row_data.meta_value_.length());
  ASSERT_EQ(0,
            MEMCMP(meta_value.ptr(),
                   row_data.meta_value_.ptr(),
                   meta_value.length()));
  row_handle.reset();
}

// 覆盖模块：ObTieredMetadataObjectManager、ObTieredMetadataWriteBatchStrategy 和 ObTableDevice 批量写链路。
// 测试流程：构造两个具有不同 meta_type 的表内对象并执行批量写入，逐项校验返回的 macro_id、
// 物理表 meta_type 和回读内容，确认聚合 IO 保留每个对象的独立写入参数。
TEST_F(TestTieredMetadataStoreV2, test_batch_write_object_persists_per_index_meta_type)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const uint64_t ls_id = 1001;
  const uint64_t tablet_id = 300025;
  const int64_t meta_type1 =
      static_cast<int64_t>(ObSSTieredMetaTypeEnum::STORAGE_SCHEMA);
  const int64_t meta_type2 =
      static_cast<int64_t>(ObSSTieredMetaTypeEnum::CO_SSTABLE);
  const char *write_data1 = "batch_object_manager_meta_1";
  const char *write_data2 = "batch_object_manager_meta_2";

  ObArray<ObStorageObjectOpt> write_opts;
  ObArray<ObStorageObjectWriteInfo> write_infos;
  ObArray<MacroBlockId> macro_ids;
  for (int64_t i = 0; i < 2; ++i) {
    const uint64_t data_seq = i + 1;
    ObStorageObjectOpt write_opt;
    write_opt.set_ss_tablet_sub_meta_opt(
        ls_id,
        tablet_id,
        1 /* op_id */,
        data_seq,
        ObTabletID(tablet_id).is_ls_inner_tablet(),
        0 /* reorganization_scn */,
        false /* is_object_storage */);
    ASSERT_EQ(OB_SUCCESS, write_opts.push_back(write_opt));
    ASSERT_EQ(
        OB_SUCCESS,
        macro_ids.push_back(
            build_sub_meta_in_table_id(tablet_id, data_seq, ls_id)));
  }
  DEFER({
    EXPECT_EQ(OB_SUCCESS, store.batch_del_meta(macro_ids));
  });

  ObStorageObjectWriteInfo write_info1;
  write_info1.buffer_ = write_data1;
  write_info1.offset_ = 0;
  write_info1.size_ = STRLEN(write_data1);
  write_info1.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info1.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  write_info1.io_desc_.set_unsealed();
  write_info1.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
  write_info1.mtl_tenant_id_ = MTL_ID();
  write_info1.set_ls_epoch_id(0);
  write_info1.set_meta_type(meta_type1);
  ASSERT_TRUE(write_info1.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_infos.push_back(write_info1));

  ObStorageObjectWriteInfo write_info2;
  write_info2.buffer_ = write_data2;
  write_info2.offset_ = 0;
  write_info2.size_ = STRLEN(write_data2);
  write_info2.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
  write_info2.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
  write_info2.io_desc_.set_unsealed();
  write_info2.io_desc_.set_sys_module_id(ObIOModule::SHARED_BLOCK_RW_IO);
  write_info2.mtl_tenant_id_ = MTL_ID();
  write_info2.set_ls_epoch_id(0);
  write_info2.set_meta_type(meta_type2);
  ASSERT_TRUE(write_info2.is_valid());
  ASSERT_EQ(OB_SUCCESS, write_infos.push_back(write_info2));

  ObTieredMetadataBatchHandle batch_handle;
  ASSERT_EQ(OB_SUCCESS,
            ObTieredMetadataObjectManager::batch_write_object(
                write_opts, write_infos, batch_handle));
  ASSERT_EQ(macro_ids.count(), batch_handle.macro_id_array().count());

  const int64_t meta_types[] = {meta_type1, meta_type2};
  const char *write_data[] = {write_data1, write_data2};
  char read_buf[128] = {0};
  for (int64_t i = 0; i < macro_ids.count(); ++i) {
    ASSERT_EQ(macro_ids.at(i), batch_handle.macro_id_array().at(i));
    int64_t stored_meta_type = -1;
    int64_t row_cnt = 0;
    ASSERT_EQ(OB_SUCCESS,
              query_stored_meta_type(
                  store,
                  macro_ids.at(i),
                  stored_meta_type,
                  row_cnt));
    ASSERT_EQ(1, row_cnt);
    ASSERT_EQ(meta_types[i], stored_meta_type);

    int64_t actual_size = 0;
    ASSERT_EQ(OB_SUCCESS,
              store.read_meta(
                  macro_ids.at(i), read_buf, sizeof(read_buf), actual_size));
    ASSERT_EQ(STRLEN(write_data[i]), actual_size);
    ASSERT_EQ(0, MEMCMP(write_data[i], read_buf, actual_size));
  }
}

// 覆盖模块：ObTieredMetadataStore::write_meta 对 meta_type 物理列的持久化。
// 测试流程：使用非默认的 SSTABLE_META 类型写入单条元数据，查询物理表并校验只有一个 part，
// 且保存的是调用方传入值，防止错误回退为默认 TABLE_STORE 或无效值。
TEST_F(TestTieredMetadataStoreV2, test_write_meta_persists_caller_meta_type)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300020;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  const int64_t expect_meta_type = static_cast<int64_t>(ObSSTieredMetaTypeEnum::SSTABLE_META);
  const char *write_data = "value_with_specific_meta_type";
  ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, expect_meta_type, meta_value));

  int64_t stored_meta_type = -1;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, query_stored_meta_type(store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(1, row_cnt);
  ASSERT_EQ(expect_meta_type, stored_meta_type);

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
}

// 覆盖模块：ObTieredMetadataStore 和 ObTieredMetadataObjectManager 对未知 meta_type 的恢复兼容。
// 测试流程：写入 meta_type=-1，依次校验物理列、普通 value 读取、完整行读取以及序列化结果，
// 确认 Tiered 元数据按 (tenant_id, ls_id, meta_key, part) 定位并在全链路保留未知类型原值。
TEST_F(TestTieredMetadataStoreV2, test_write_meta_accepts_unknown_meta_type_for_restore)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300024;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);
  DEFER({
    EXPECT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  });

  const int64_t unknown_meta_type = -1;
  const char *write_data = "restored_value_without_meta_type";
  ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, unknown_meta_type, meta_value));

  int64_t stored_meta_type = 0;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, query_stored_meta_type(store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(1, row_cnt);
  ASSERT_EQ(unknown_meta_type, stored_meta_type);

  char read_buf[128];
  int64_t actual_size = 0;
  ASSERT_EQ(OB_SUCCESS, store.read_meta(macro_id, read_buf, sizeof(read_buf), actual_size));
  ASSERT_EQ(meta_value.length(), actual_size);
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_buf, actual_size));

  // Tiered 表读取不校验 meta_type，完整行数据应保留恢复写入的原值。
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMRowDataTest"));
  ObTieredMetaTableRowData row_data;
  ASSERT_EQ(OB_SUCCESS,
            store.read_meta_row_data(macro_id, allocator, row_data));
  ASSERT_TRUE(row_data.is_valid());
  ASSERT_EQ(unknown_meta_type, row_data.meta_type_);
  ASSERT_EQ(meta_value.length(), row_data.meta_value_.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), row_data.meta_value_.ptr(), meta_value.length()));

  const int64_t payload_size = row_data.get_serialize_size();
  char *payload_buf = static_cast<char *>(allocator.alloc(payload_size));
  ASSERT_NE(nullptr, payload_buf);
  {
    ObObjectStorageTenantGuard storage_guard(
        tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
    int64_t row_size = 0;
    ASSERT_EQ(OB_SUCCESS,
              ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                  macro_id,
                  ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY,
                  payload_buf,
                  payload_size,
                  row_size));
    ASSERT_EQ(payload_size, row_size);

    ObTieredMetaTableRowData decoded_row;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded_row.deserialize(payload_buf, row_size, pos));
    ASSERT_EQ(row_size, pos);
    ASSERT_TRUE(decoded_row.is_valid());
    ASSERT_EQ(unknown_meta_type, decoded_row.meta_type_);
    ASSERT_EQ(meta_value.length(), decoded_row.meta_value_.length());
    ASSERT_EQ(0, MEMCMP(meta_value.ptr(), decoded_row.meta_value_.ptr(), meta_value.length()));
  }

}

TEST_F(TestTieredMetadataStoreV2, test_read_meta_not_exist)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300002;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 99);

  const int64_t buf_size = 1024;
  char read_buf[buf_size];
  int64_t actual_size = 0;
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, store.read_meta(macro_id, read_buf, buf_size, actual_size));
}

TEST_F(TestTieredMetadataStoreV2, test_read_meta_buf_too_small)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300003;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  const char *write_data = "this_is_a_long_enough_value_for_testing_buffer_overflow";
  ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  const int64_t small_buf_size = 5;
  char small_buf[small_buf_size];
  int64_t actual_size = 0;
  ASSERT_EQ(OB_SIZE_OVERFLOW, store.read_meta(macro_id, small_buf, small_buf_size, actual_size));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
}

TEST_F(TestTieredMetadataStoreV2, test_read_meta_invalid_args)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300004;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  int64_t actual_size = 0;
  // null buf
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.read_meta(macro_id, nullptr, 1024, actual_size));

  // zero buf_size
  char buf[16];
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.read_meta(macro_id, buf, 0, actual_size));

  // invalid macro block id (not SHARED_TABLET_SUB_META type)
  MacroBlockId invalid_id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.read_meta(invalid_id, buf, 16, actual_size));
}

// 覆盖模块：ObSSTieredMetaTableIterator、ObTieredMetadataStore 的分片重组校验，
// 以及 ObTieredMetadataObjectManager 的对外错误映射。
// 测试流程：直接构造缺失或为空的 part 0、part 不连续、后续 part 为空、meta_type 不一致，
// 以及下一逻辑 key 从非零 part 开始的数据；校验底层返回 OB_INVALID_DATA，
// 对象管理器对外返回 OB_IO_ERROR 并将 actual_size 清零。
TEST_F(TestTieredMetadataStoreV2, test_read_corrupt_parts)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300022;
  MacroBlockId orphan_id = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId gap_id = build_sub_meta_in_table_id(tablet_id, 2);
  MacroBlockId type_mismatch_id =
      build_sub_meta_in_table_id(tablet_id, 3);
  MacroBlockId empty_first_id = build_sub_meta_in_table_id(tablet_id, 4);
  MacroBlockId empty_following_id = build_sub_meta_in_table_id(tablet_id, 5);
  MacroBlockId valid_before_orphan_id = build_sub_meta_in_table_id(tablet_id, 6);
  MacroBlockId orphan_after_id = build_sub_meta_in_table_id(tablet_id, 7);
  const MacroBlockId point_read_corrupt_ids[] = {
      orphan_id, gap_id, type_mismatch_id, empty_first_id, empty_following_id};
  const int64_t other_meta_type =
      static_cast<int64_t>(ObSSTieredMetaTypeEnum::STORAGE_SCHEMA);

  ObArray<MacroBlockId> corrupt_ids;
  ASSERT_EQ(OB_SUCCESS, corrupt_ids.push_back(orphan_id));
  ASSERT_EQ(OB_SUCCESS, corrupt_ids.push_back(gap_id));
  ASSERT_EQ(OB_SUCCESS, corrupt_ids.push_back(type_mismatch_id));
  ASSERT_EQ(OB_SUCCESS, corrupt_ids.push_back(empty_first_id));
  ASSERT_EQ(OB_SUCCESS, corrupt_ids.push_back(empty_following_id));
  ASSERT_EQ(OB_SUCCESS, corrupt_ids.push_back(valid_before_orphan_id));
  ASSERT_EQ(OB_SUCCESS, corrupt_ids.push_back(orphan_after_id));
  DEFER({
    EXPECT_EQ(OB_SUCCESS, store.batch_del_meta(corrupt_ids));
  });

  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, orphan_id, kDefaultMetaType, 1, "6F727068616E"));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, gap_id, kDefaultMetaType, 0, "7061727430"));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, gap_id, kDefaultMetaType, 2, "7061727432"));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, type_mismatch_id, kDefaultMetaType, 0, "7061727430"));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, type_mismatch_id, other_meta_type, 1, "7061727431"));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, empty_first_id, kDefaultMetaType, 0, ""));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, empty_following_id, kDefaultMetaType, 0, "7061727430"));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, empty_following_id, kDefaultMetaType, 1, ""));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, valid_before_orphan_id, kDefaultMetaType, 0, "76616C6964"));
  ASSERT_EQ(OB_SUCCESS,
            insert_raw_part(
                store, orphan_after_id, kDefaultMetaType, 1, "6F727068616E"));

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMRowDataTest"));
  ObTieredMetaTableRowData row_data;
  for (int64_t i = 0; i < ARRAYSIZEOF(point_read_corrupt_ids); ++i) {
    allocator.reuse();
    ASSERT_EQ(OB_INVALID_DATA,
              store.read_meta_row_data(
                  point_read_corrupt_ids[i], allocator, row_data));
  }

  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS,
            store.get_tiered_metadata_iter(
                valid_before_orphan_id, orphan_after_id, iter));
  uint64_t out_tenant_id = 0;
  share::ObLSID out_ls_id;
  ObSSMetaBaseType out_meta_type;
  ObString out_key;
  ObString out_value;
  allocator.reuse();
  ASSERT_EQ(OB_INVALID_DATA,
            iter.get_next(
                allocator,
                out_tenant_id,
                out_ls_id,
                out_meta_type,
                out_key,
                out_value));

  {
    ObObjectStorageTenantGuard storage_guard(
        tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
    char read_buf[128] = {0};
    int64_t actual_size = -1;
    ASSERT_EQ(OB_IO_ERROR,
              ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                  empty_following_id,
                  ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY,
                  read_buf,
                  sizeof(read_buf),
                  actual_size));
    ASSERT_EQ(0, actual_size);
  }
}

// ============================================================================
// Test: write_meta error cases
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_write_meta_invalid_args)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300005;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  // empty meta_value
  ObString empty_value;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.write_meta(macro_id, kDefaultMetaType, empty_value));

  // invalid macro block id
  MacroBlockId invalid_id;
  ObString valid_value("some_data");
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.write_meta(invalid_id, kDefaultMetaType, valid_value));
}

TEST_F(TestTieredMetadataStoreV2, test_write_meta_not_inited)
{
  ObTieredMetadataStore store;
  const int64_t tablet_id = 300006;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);
  ObString meta_value("data");
  ASSERT_EQ(OB_NOT_INIT, store.write_meta(macro_id, kDefaultMetaType, meta_value));
}

TEST_F(TestTieredMetadataStoreV2, test_write_meta_duplicate_same_value)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300007;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  const char *write_data = "duplicate_test_value";
  ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  // write same value again should succeed (idempotent)
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
}

TEST_F(TestTieredMetadataStoreV2,
       test_new_writer_multi_part_conflicts_with_legacy_layout)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  MacroBlockId macro_id = build_sub_meta_in_table_id(300107, 1);
  const LegacyPartSpec legacy_parts[] = {
      {256 * 1024, 0x41},
      {256 * 1024, 0x42},
      {256 * 1024, 0x43},
      {256 * 1024, 0x44},
  };
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMLegacyMulti"));
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS,
            build_repeated_parts_value(
                allocator, legacy_parts, ARRAYSIZEOF(legacy_parts), meta_value));
  ASSERT_EQ(1024 * 1024, meta_value.length());

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
      insert_legacy_repeated_parts(
          store, macro_id, kDefaultMetaType,
          legacy_parts, ARRAYSIZEOF(legacy_parts), affected_rows));
  ASSERT_EQ(4, affected_rows);

  // Exercise both store read APIs against the legacy four-part layout before
  // the new writer reaches the conflict-resolution path.
  char *read_buf = static_cast<char *>(allocator.alloc(meta_value.length()));
  ASSERT_NE(nullptr, read_buf);
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            store.read_meta(
                macro_id, read_buf, meta_value.length(), read_size));
  ASSERT_EQ(meta_value.length(), read_size);
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_buf, read_size));

  ObArenaAllocator row_allocator(ObMemAttr(tenant_id_, "TMOldRowRead"));
  ObTieredMetaTableRowData row_data;
  ASSERT_EQ(OB_SUCCESS,
            store.read_meta_row_data(macro_id, row_allocator, row_data));
  ASSERT_TRUE(row_data.is_valid());
  ASSERT_EQ(kDefaultMetaType, row_data.meta_type_);
  ASSERT_EQ(meta_value.length(), row_data.meta_value_.length());
  ASSERT_EQ(0,
            MEMCMP(meta_value.ptr(),
                   row_data.meta_value_.ptr(),
                   meta_value.length()));

  // The new writer proposes two 512KB parts. Its multi-row INSERT conflicts
  // with legacy parts 0 and 1, then compares the reassembled logical value.
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));
  int64_t stored_meta_type = -1;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
      query_stored_meta_type(store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(kDefaultMetaType, stored_meta_type);
  ASSERT_EQ(4, row_cnt);

  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  ASSERT_EQ(OB_SUCCESS,
            query_stored_meta_type(
                store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(0, row_cnt);
}

TEST_F(TestTieredMetadataStoreV2,
       test_legacy_multi_part_retry_against_new_layout)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  MacroBlockId macro_id = build_sub_meta_in_table_id(300108, 1);
  const LegacyPartSpec legacy_parts[] = {
      {256 * 1024, 0x41},
      {256 * 1024, 0x42},
      {256 * 1024, 0x43},
      {256 * 1024, 0x44},
  };
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMNewMulti"));
  ObString meta_value;
  ASSERT_EQ(OB_SUCCESS,
            build_repeated_parts_value(
                allocator, legacy_parts, ARRAYSIZEOF(legacy_parts), meta_value));
  ASSERT_EQ(1024 * 1024, meta_value.length());
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  int64_t stored_meta_type = -1;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
      query_stored_meta_type(store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(2, row_cnt);

  // The legacy four-row INSERT conflicts with existing parts 0 and 1, while
  // parts 2 and 3 are new keys. The whole statement must fail atomically.
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE,
      insert_legacy_repeated_parts(
          store, macro_id, kDefaultMetaType,
          legacy_parts, ARRAYSIZEOF(legacy_parts), affected_rows));
  ASSERT_EQ(OB_SUCCESS,
      query_stored_meta_type(store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(2, row_cnt);

  // This is the unchanged conflict path used by both old and new writers.
  ASSERT_EQ(OB_SUCCESS,
      store.compare_with_existing_value_when_write_conflict_(macro_id, meta_value));
  char *read_buf = static_cast<char *>(allocator.alloc(meta_value.length()));
  ASSERT_NE(nullptr, read_buf);
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS,
      store.read_meta(macro_id, read_buf, meta_value.length(), read_size));
  ASSERT_EQ(meta_value.length(), read_size);
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), read_buf, read_size));

  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  ASSERT_EQ(OB_SUCCESS,
            query_stored_meta_type(
                store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(0, row_cnt);
}

TEST_F(TestTieredMetadataStoreV2,
       test_new_writer_rejects_different_legacy_multi_part_value)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  MacroBlockId macro_id = build_sub_meta_in_table_id(300109, 1);
  const LegacyPartSpec stored_parts[] = {
      {256 * 1024, 0x41},
      {256 * 1024, 0x42},
      {256 * 1024, 0x43},
      {256 * 1024, 0x44},
  };
  const LegacyPartSpec different_parts[] = {
      {256 * 1024, 0x41},
      {256 * 1024, 0x42},
      {256 * 1024, 0x43},
      {256 * 1024, 0x45},
  };
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMMismatch"));
  ObString stored_value;
  ObString different_value;
  ASSERT_EQ(OB_SUCCESS,
            build_repeated_parts_value(
                allocator, stored_parts, ARRAYSIZEOF(stored_parts), stored_value));
  ASSERT_EQ(OB_SUCCESS,
            build_repeated_parts_value(
                allocator,
                different_parts,
                ARRAYSIZEOF(different_parts),
                different_value));
  ASSERT_EQ(1024 * 1024, stored_value.length());
  ASSERT_EQ(stored_value.length(), different_value.length());
  ASSERT_EQ(0, MEMCMP(stored_value.ptr(), different_value.ptr(), 512 * 1024));
  ASSERT_NE(0,
            MEMCMP(stored_value.ptr() + 512 * 1024,
                   different_value.ptr() + 512 * 1024,
                   512 * 1024));

  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS,
      insert_legacy_repeated_parts(
          store, macro_id, kDefaultMetaType,
          stored_parts, ARRAYSIZEOF(stored_parts), affected_rows));
  ASSERT_EQ(4, affected_rows);

  // The first new 512KB part is identical. Only comparing the fully
  // reassembled legacy value can detect the difference in the second part.
  ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH,
            store.write_meta(macro_id, kDefaultMetaType, different_value));

  int64_t stored_meta_type = -1;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
            query_stored_meta_type(
                store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(kDefaultMetaType, stored_meta_type);
  ASSERT_EQ(4, row_cnt);

  char *read_buf = static_cast<char *>(allocator.alloc(stored_value.length()));
  ASSERT_NE(nullptr, read_buf);
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            store.read_meta(
                macro_id, read_buf, stored_value.length(), read_size));
  ASSERT_EQ(stored_value.length(), read_size);
  ASSERT_EQ(0, MEMCMP(stored_value.ptr(), read_buf, read_size));

  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  ASSERT_EQ(OB_SUCCESS,
            query_stored_meta_type(
                store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(0, row_cnt);
}

TEST_F(TestTieredMetadataStoreV2, test_write_meta_duplicate_different_value)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300008;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  const char *write_data_1 = "first_value";
  ObString meta_value_1(STRLEN(write_data_1), write_data_1);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value_1));

  // write different value should fail with condition not match
  const char *write_data_2 = "second_different_value";
  ObString meta_value_2(STRLEN(write_data_2), write_data_2);
  ASSERT_EQ(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH, store.write_meta(macro_id, kDefaultMetaType, meta_value_2));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
}

// ============================================================================
// Test: batch_write_meta
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_batch_write_meta)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300010;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId id2 = inc_seq(id1);
  MacroBlockId id3 = inc_seq(id2);

  ObArray<MacroBlockId> ids;
  ObArray<int64_t> meta_types;
  ObArray<ObString> values;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id1));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id2));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id3));
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(kDefaultMetaType));
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(kDefaultMetaType));
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(kDefaultMetaType));

  const char *data1 = "batch_val_1";
  const char *data2 = "batch_val_22";
  const char *data3 = "batch_val_333";
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data2), data2)));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data3), data3)));

  ASSERT_EQ(OB_SUCCESS, store.batch_write_meta(ids, meta_types, values));

  // verify each write
  const int64_t buf_size = 1024;
  char read_buf[buf_size];
  int64_t actual_size = 0;

  ASSERT_EQ(OB_SUCCESS, store.read_meta(id1, read_buf, buf_size, actual_size));
  ASSERT_EQ(STRLEN(data1), actual_size);
  ASSERT_EQ(0, MEMCMP(read_buf, data1, actual_size));

  ASSERT_EQ(OB_SUCCESS, store.read_meta(id2, read_buf, buf_size, actual_size));
  ASSERT_EQ(STRLEN(data2), actual_size);
  ASSERT_EQ(0, MEMCMP(read_buf, data2, actual_size));

  ASSERT_EQ(OB_SUCCESS, store.read_meta(id3, read_buf, buf_size, actual_size));
  ASSERT_EQ(STRLEN(data3), actual_size);
  ASSERT_EQ(0, MEMCMP(read_buf, data3, actual_size));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.batch_del_meta(ids));
}

// 覆盖模块：ObTieredMetadataStore::batch_write_meta 对逐项 meta_type 的持久化。
// 测试流程：为三个 macro_id 分别设置不同 meta_type 并批量写入，逐项查询物理表，
// 确认每行保存对应下标的类型，而不是复用统一或默认类型。
TEST_F(TestTieredMetadataStoreV2, test_batch_write_meta_persists_per_index_meta_type)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300021;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId id2 = inc_seq(id1);
  MacroBlockId id3 = inc_seq(id2);

  const int64_t type1 = static_cast<int64_t>(ObSSTieredMetaTypeEnum::TABLE_STORE);
  const int64_t type2 = static_cast<int64_t>(ObSSTieredMetaTypeEnum::STORAGE_SCHEMA);
  const int64_t type3 = static_cast<int64_t>(ObSSTieredMetaTypeEnum::CO_SSTABLE);

  ObArray<MacroBlockId> ids;
  ObArray<int64_t> meta_types;
  ObArray<ObString> values;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id1));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id2));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id3));
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(type1));
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(type2));
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(type3));

  const char *data1 = "per_idx_val_1";
  const char *data2 = "per_idx_val_2";
  const char *data3 = "per_idx_val_3";
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data2), data2)));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data3), data3)));

  ASSERT_EQ(OB_SUCCESS, store.batch_write_meta(ids, meta_types, values));

  int64_t stored_type = -1;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, query_stored_meta_type(store, id1, stored_type, row_cnt));
  ASSERT_EQ(1, row_cnt);
  ASSERT_EQ(type1, stored_type);
  ASSERT_EQ(OB_SUCCESS, query_stored_meta_type(store, id2, stored_type, row_cnt));
  ASSERT_EQ(1, row_cnt);
  ASSERT_EQ(type2, stored_type);
  ASSERT_EQ(OB_SUCCESS, query_stored_meta_type(store, id3, stored_type, row_cnt));
  ASSERT_EQ(1, row_cnt);
  ASSERT_EQ(type3, stored_type);

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.batch_del_meta(ids));
}

TEST_F(TestTieredMetadataStoreV2, test_batch_write_meta_invalid_args)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  // empty ids
  ObArray<MacroBlockId> empty_ids;
  ObArray<int64_t> empty_meta_types;
  ObArray<ObString> empty_values;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.batch_write_meta(empty_ids, empty_meta_types, empty_values));

  // count mismatch
  const int64_t tablet_id = 300011;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  ObArray<MacroBlockId> ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id1));

  ObArray<int64_t> meta_types;
  ObArray<ObString> values;
  const char *data1 = "val1";
  const char *data2 = "val2";
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(kDefaultMetaType));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data2), data2)));

  ASSERT_EQ(OB_INVALID_ARGUMENT, store.batch_write_meta(ids, meta_types, values));
}

TEST_F(TestTieredMetadataStoreV2, test_batch_write_meta_with_invalid_id)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  MacroBlockId invalid_id; // default constructed, not valid
  ObArray<MacroBlockId> ids;
  ObArray<int64_t> meta_types;
  ObArray<ObString> values;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(invalid_id));
  const char *data = "some_data";
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(kDefaultMetaType));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString(STRLEN(data), data)));

  ASSERT_EQ(OB_INVALID_ARGUMENT, store.batch_write_meta(ids, meta_types, values));
}

TEST_F(TestTieredMetadataStoreV2, test_batch_write_meta_with_empty_value)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300012;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  ObArray<MacroBlockId> ids;
  ObArray<int64_t> meta_types;
  ObArray<ObString> values;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id1));
  ASSERT_EQ(OB_SUCCESS, meta_types.push_back(kDefaultMetaType));
  ASSERT_EQ(OB_SUCCESS, values.push_back(ObString()));

  ASSERT_EQ(OB_INVALID_ARGUMENT, store.batch_write_meta(ids, meta_types, values));
}

// ============================================================================
// Test: del_meta
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_del_meta)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300020;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  const char *write_data = "delete_me";
  ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  // verify exists
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  // delete
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));

  // verify not exists
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestTieredMetadataStoreV2, test_del_meta_not_exist)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300021;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 88);

  // delete non-existent key should still succeed (affected_rows=0)
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
}

TEST_F(TestTieredMetadataStoreV2, test_del_meta_invalid_args)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  MacroBlockId invalid_id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.del_meta(invalid_id));
}

// ============================================================================
// Test: batch_del_meta
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_batch_del_meta)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300030;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId id2 = inc_seq(id1);

  const char *data1 = "batch_del_1";
  const char *data2 = "batch_del_2";
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id1, kDefaultMetaType, ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id2, kDefaultMetaType, ObString(STRLEN(data2), data2)));

  ObArray<MacroBlockId> ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id1));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id2));

  CountingTieredDeleteSQLClient client(get_sql_client());
  ObTieredMetadataStore deleting_store;
  ASSERT_EQ(OB_SUCCESS, deleting_store.init(tenant_id_, &client));
  ASSERT_EQ(OB_SUCCESS, deleting_store.batch_del_meta(ids));
  ASSERT_EQ(1, client.get_write_count());
  ASSERT_EQ(OB_SUCCESS, deleting_store.batch_del_meta(ids));
  ASSERT_EQ(2, client.get_write_count());

  // verify both deleted
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(id1, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(id2, is_exist));
  ASSERT_FALSE(is_exist);
}

// Count real SQL submissions across a batch boundary and retry a partially completed delete.
TEST_F(TestTieredMetadataStoreV2, test_batch_del_meta_batches_and_retry)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));
  ObArray<MacroBlockId> ids;
  for (int64_t i = 0; i < 257; ++i) {
    ASSERT_EQ(OB_SUCCESS, ids.push_back(build_sub_meta_in_table_id(300031, i + 1)));
  }
  MacroBlockId kept_id = build_sub_meta_in_table_id(300031, 258);
  ASSERT_EQ(OB_SUCCESS, store.batch_del_meta(ids));
  ASSERT_EQ(OB_SUCCESS, store.del_meta(kept_id));

  ObArenaAllocator allocator("BatchDelTest");
  const int64_t value_size = OB_SS_MAX_META_VALUE_LENGTH + 1;
  char *value_buf = static_cast<char *>(allocator.alloc(value_size));
  ASSERT_NE(nullptr, value_buf);
  MEMSET(value_buf, 's', value_size);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(
      ids.at(0), kDefaultMetaType, ObString(value_size, value_buf)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(
      ids.at(127), kDefaultMetaType, ObString::make_string("first_batch_tail")));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(
      ids.at(128), 1000001, ObString::make_string("unknown_type")));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(
      ids.at(255), kDefaultMetaType, ObString::make_string("second_batch_tail")));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(
      ids.at(256), kDefaultMetaType, ObString::make_string("last_batch")));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(
      kept_id, kDefaultMetaType, ObString::make_string("keep")));
  int64_t stored_type = 0;
  int64_t row_count = 0;
  ASSERT_EQ(OB_SUCCESS, query_stored_meta_type(store, ids.at(0), stored_type, row_count));
  ASSERT_EQ(2, row_count);

  // The same key in another LS must survive every batch and retry.
  share::ObLSID ls_id;
  ObSSTieredMetaType meta_type;
  char key_buf[1024];
  int64_t key_len = 0;
  ASSERT_EQ(OB_SUCCESS, store.macro_block_id_to_ss_meta_key_(
      ids.at(0), ls_id, meta_type, key_buf, sizeof(key_buf), key_len));
  const ObString meta_key(key_len, key_buf);
  meta_type.type_val_ = kDefaultMetaType;
  ObSSTieredMetaTableOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(get_sql_client(), OB_ALL_SHARED_TIERED_META_TNAME));
  int64_t affected_rows = 0;
  ASSERT_EQ(OB_SUCCESS, op.insert_row(tenant_id_, ObLSID(1), meta_type,
      meta_key, ObString::make_string("other_ls"), affected_rows));
  ASSERT_EQ(1, affected_rows);

  CountingTieredDeleteSQLClient client(get_sql_client(), 2);
  ObTieredMetadataStore deleting_store;
  ASSERT_EQ(OB_SUCCESS, deleting_store.init(tenant_id_, &client));
  ASSERT_EQ(OB_TIMEOUT, deleting_store.batch_del_meta(ids));
  ASSERT_EQ(2, client.get_write_count());
  ASSERT_EQ(OB_SUCCESS, query_stored_meta_type(store, ids.at(0), stored_type, row_count));
  ASSERT_EQ(0, row_count);
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(ids.at(127), is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(ids.at(128), is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(ids.at(255), is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(ids.at(256), is_exist));
  ASSERT_TRUE(is_exist);

  ASSERT_EQ(OB_SUCCESS, deleting_store.batch_del_meta(ids));
  ASSERT_EQ(5, client.get_write_count());
  for (int64_t i = 0; i < ids.count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, query_stored_meta_type(store, ids.at(i), stored_type, row_count));
    ASSERT_EQ(0, row_count);
  }
  char kept_buf[16];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, store.read_meta(kept_id, kept_buf, sizeof(kept_buf), read_size));
  ASSERT_EQ(ObString::make_string("keep"), ObString(read_size, kept_buf));
  ObString other_ls_value;
  ASSERT_EQ(OB_SUCCESS, op.read_row(tenant_id_, ObLSID(1), meta_type,
      meta_key, allocator, other_ls_value));
  ASSERT_EQ(ObString::make_string("other_ls"), other_ls_value);
  ASSERT_EQ(OB_SUCCESS, op.delete_row(
      tenant_id_, ObLSID(1), meta_type, meta_key, false, affected_rows));
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(OB_SUCCESS, store.del_meta(kept_id));
}

// Meta-tenant and sys-tenant rows share the sys tenant's physical table.
TEST_F(TestTieredMetadataStoreV2, test_batch_del_meta_logical_tenant_isolation)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));
  ObTieredMetadataStore meta_store;
  ObTieredMetadataStore sys_store;
  ASSERT_EQ(OB_SUCCESS, meta_store.init(gen_meta_tenant_id(tenant_id_), &get_sql_client()));
  ASSERT_EQ(OB_SUCCESS, sys_store.init(OB_SYS_TENANT_ID, &get_sql_client()));
  const MacroBlockId id = build_sub_meta_in_table_id(300032, 1);
  ASSERT_EQ(OB_SUCCESS, meta_store.del_meta(id));
  ASSERT_EQ(OB_SUCCESS, sys_store.del_meta(id));
  ASSERT_EQ(OB_SUCCESS, meta_store.write_meta(
      id, kDefaultMetaType, ObString::make_string("meta_value")));
  ASSERT_EQ(OB_SUCCESS, sys_store.write_meta(
      id, kDefaultMetaType, ObString::make_string("sys_value")));
  ObArray<MacroBlockId> ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(id));
  ASSERT_EQ(OB_SUCCESS, meta_store.batch_del_meta(ids));
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, meta_store.is_meta_exist(id, is_exist));
  ASSERT_FALSE(is_exist);
  char value_buf[16];
  int64_t read_size = 0;
  ASSERT_EQ(OB_SUCCESS, sys_store.read_meta(id, value_buf, sizeof(value_buf), read_size));
  ASSERT_EQ(ObString::make_string("sys_value"), ObString(read_size, value_buf));
  ASSERT_EQ(OB_SUCCESS, sys_store.del_meta(id));
}

TEST_F(TestTieredMetadataStoreV2, test_batch_del_meta_empty_ids)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  ObArray<MacroBlockId> empty_ids;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.batch_del_meta(empty_ids));
}

TEST_F(TestTieredMetadataStoreV2, test_batch_del_meta_with_invalid_id)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  MacroBlockId invalid_id;
  ObArray<MacroBlockId> ids;
  ASSERT_EQ(OB_SUCCESS, ids.push_back(invalid_id));

  ASSERT_EQ(OB_INVALID_ARGUMENT, store.batch_del_meta(ids));
}

// ============================================================================
// Test: is_meta_exist
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_is_meta_exist)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300040;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  // should not exist initially
  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);

  // write and verify exists
  const char *write_data = "exist_check";
  ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  // delete and verify not exists
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);
}

TEST_F(TestTieredMetadataStoreV2, test_is_meta_exist_invalid_args)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  MacroBlockId invalid_id;
  bool is_exist = false;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.is_meta_exist(invalid_id, is_exist));
}

// ============================================================================
// Test: list_meta_macro_files
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_list_meta_macro_files)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300050;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId id2 = inc_seq(id1);
  MacroBlockId id3 = inc_seq(id2);

  const char *data1 = "list_val_1";
  const char *data2 = "list_val_22";
  const char *data3 = "list_val_333";
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id1, kDefaultMetaType, ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id2, kDefaultMetaType, ObString(STRLEN(data2), data2)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id3, kDefaultMetaType, ObString(STRLEN(data3), data3)));

  ObArray<MacroBlockId> macro_ids;
  ASSERT_EQ(OB_SUCCESS, store.list_meta_macro_files(false/*is_ls_inner_tablet*/,
                                                    0/*ls_id*/,
                                                    tablet_id,
                                                    0/*reorganization_scn*/,
                                                    macro_ids));
  ASSERT_EQ(3, macro_ids.count());
  ASSERT_EQ(id1, macro_ids.at(0));
  ASSERT_EQ(id2, macro_ids.at(1));
  ASSERT_EQ(id3, macro_ids.at(2));

  // cleanup
  ObArray<MacroBlockId> del_ids;
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id1));
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id2));
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id3));
  ASSERT_EQ(OB_SUCCESS, store.batch_del_meta(del_ids));
}

TEST_F(TestTieredMetadataStoreV2, test_list_meta_macro_files_empty)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300051;
  ObArray<MacroBlockId> macro_ids;
  ASSERT_EQ(OB_SUCCESS, store.list_meta_macro_files(false/*is_ls_inner_tablet*/,
                                                    0/*ls_id*/,
                                                    tablet_id,
                                                    0/*reorganization_scn*/,
                                                    macro_ids));
  ASSERT_EQ(0, macro_ids.count());
}

TEST_F(TestTieredMetadataStoreV2, test_list_meta_macro_files_inner_tablet)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = ObTabletID::LS_TX_DATA_TABLET_ID;
  const int64_t ls_id = 2001;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1, ls_id);
  MacroBlockId id2 = inc_seq(id1);

  const char *data1 = "inner_tablet_val_1";
  const char *data2 = "inner_tablet_val_2";
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id1, kDefaultMetaType, ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id2, kDefaultMetaType, ObString(STRLEN(data2), data2)));

  ObArray<MacroBlockId> macro_ids;
  ASSERT_EQ(OB_SUCCESS, store.list_meta_macro_files(true/*is_ls_inner_tablet*/,
                                                    ls_id,
                                                    tablet_id,
                                                    0/*reorganization_scn*/,
                                                    macro_ids));
  ASSERT_EQ(2, macro_ids.count());
  ASSERT_EQ(id1, macro_ids.at(0));
  ASSERT_EQ(id2, macro_ids.at(1));

  // cleanup
  ObArray<MacroBlockId> del_ids;
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id1));
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id2));
  ASSERT_EQ(OB_SUCCESS, store.batch_del_meta(del_ids));
}

// ============================================================================
// Test: get_tiered_metadata_iter
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_get_tiered_metadata_iter)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300060;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId id2 = inc_seq(id1);
  MacroBlockId id3 = inc_seq(id2);

  const char *data1 = "iter_val_1";
  const char *data2 = "iter_val_22";
  const char *data3 = "iter_val_333";
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id1, kDefaultMetaType, ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id2, kDefaultMetaType, ObString(STRLEN(data2), data2)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id3, kDefaultMetaType, ObString(STRLEN(data3), data3)));

  // iterate from id1 to id3 (inclusive range)
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, store.get_tiered_metadata_iter(id1, id3, iter));

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2Test"));
  uint64_t out_tenant_id = 0;
  share::ObLSID out_ls_id;
  ObSSMetaBaseType out_meta_type;
  ObString meta_key;
  ObString meta_value;

  // first row
  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));
  ASSERT_EQ(STRLEN(data1), meta_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), data1, meta_value.length()));

  // second row
  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));
  ASSERT_EQ(STRLEN(data2), meta_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), data2, meta_value.length()));

  // third row
  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));
  ASSERT_EQ(STRLEN(data3), meta_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), data3, meta_value.length()));

  // end
  allocator.reuse();
  ASSERT_EQ(OB_ITER_END, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));

  // cleanup
  ObArray<MacroBlockId> del_ids;
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id1));
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id2));
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id3));
  ASSERT_EQ(OB_SUCCESS, store.batch_del_meta(del_ids));
}

TEST_F(TestTieredMetadataStoreV2, test_get_tiered_metadata_iter_partial_range)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300061;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId id2 = inc_seq(id1);
  MacroBlockId id3 = inc_seq(id2);

  const char *data1 = "partial_1";
  const char *data2 = "partial_22";
  const char *data3 = "partial_333";
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id1, kDefaultMetaType, ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id2, kDefaultMetaType, ObString(STRLEN(data2), data2)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id3, kDefaultMetaType, ObString(STRLEN(data3), data3)));

  // iterate from id2 to id3 only
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, store.get_tiered_metadata_iter(id2, id3, iter));

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2Test"));
  uint64_t out_tenant_id = 0;
  share::ObLSID out_ls_id;
  ObSSMetaBaseType out_meta_type;
  ObString meta_key;
  ObString meta_value;

  // first row should be id2
  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));
  ASSERT_EQ(STRLEN(data2), meta_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), data2, meta_value.length()));

  // second row should be id3
  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));
  ASSERT_EQ(STRLEN(data3), meta_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), data3, meta_value.length()));

  // end
  allocator.reuse();
  ASSERT_EQ(OB_ITER_END, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));

  // cleanup
  ObArray<MacroBlockId> del_ids;
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id1));
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id2));
  ASSERT_EQ(OB_SUCCESS, del_ids.push_back(id3));
  ASSERT_EQ(OB_SUCCESS, store.batch_del_meta(del_ids));
}

TEST_F(TestTieredMetadataStoreV2, test_get_tiered_metadata_iter_single_key)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300062;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);

  const char *data1 = "single_key_val";
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id1, kDefaultMetaType, ObString(STRLEN(data1), data1)));

  // iterate with start == end
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, store.get_tiered_metadata_iter(id1, id1, iter));

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2Test"));
  uint64_t out_tenant_id = 0;
  share::ObLSID out_ls_id;
  ObSSMetaBaseType out_meta_type;
  ObString meta_key;
  ObString meta_value;

  allocator.reuse();
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));
  ASSERT_EQ(STRLEN(data1), meta_value.length());
  ASSERT_EQ(0, MEMCMP(meta_value.ptr(), data1, meta_value.length()));

  allocator.reuse();
  ASSERT_EQ(OB_ITER_END, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.del_meta(id1));
}

TEST_F(TestTieredMetadataStoreV2, test_get_tiered_metadata_iter_invalid_args)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  MacroBlockId invalid_id;
  const int64_t tablet_id = 300063;
  MacroBlockId valid_id = build_sub_meta_in_table_id(tablet_id, 1);

  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.get_tiered_metadata_iter(invalid_id, valid_id, iter));

  ObSSTieredMetaTableIterator iter2;
  ASSERT_EQ(OB_INVALID_ARGUMENT, store.get_tiered_metadata_iter(valid_id, invalid_id, iter2));
}

// ============================================================================
// Test: raw value crosses one physical-part boundary
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_write_read_large_value)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300080;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);
  DEFER({
    EXPECT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  });

  // The physical VARBINARY column limit is measured in raw bytes.
  const int64_t raw_part_size = OB_SS_MAX_META_VALUE_LENGTH;
  const int64_t large_size = raw_part_size + 1;
  char *large_buf = static_cast<char *>(ob_malloc(large_size, "TMStoreV2Test"));
  ASSERT_NE(nullptr, large_buf);
  DEFER(ob_free(large_buf));
  MEMSET(large_buf, 'X', large_size);
  large_buf[0] = 'A';
  large_buf[large_size - 1] = 'Z';

  ObString meta_value(large_size, large_buf);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  int64_t stored_meta_type = -1;
  int64_t row_cnt = 0;
  ASSERT_EQ(OB_SUCCESS,
            query_stored_meta_type(
                store, macro_id, stored_meta_type, row_cnt));
  ASSERT_EQ(kDefaultMetaType, stored_meta_type);
  ASSERT_EQ(2, row_cnt);

  // read back and verify
  char *read_buf = static_cast<char *>(ob_malloc(large_size, "TMStoreV2Test"));
  ASSERT_NE(nullptr, read_buf);
  DEFER(ob_free(read_buf));
  int64_t actual_size = 0;
  ASSERT_EQ(OB_SUCCESS, store.read_meta(macro_id, read_buf, large_size, actual_size));
  ASSERT_EQ(large_size, actual_size);
  ASSERT_EQ(0, MEMCMP(read_buf, large_buf, large_size));

  // 覆盖 read_meta_row_data 的多 part 重组流程，校验重组后的类型、长度和内容。
  ObArenaAllocator row_allocator(
      ObMemAttr(tenant_id_, "TMRowDataTest"));
  ObTieredMetaTableRowData row_data;
  ASSERT_EQ(OB_SUCCESS,
            store.read_meta_row_data(
                macro_id, row_allocator, row_data));
  ASSERT_EQ(kDefaultMetaType, row_data.meta_type_);
  ASSERT_EQ(large_size, row_data.meta_value_.length());
  ASSERT_EQ(0, MEMCMP(row_data.meta_value_.ptr(), large_buf, large_size));

  // verify via iterator
  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, store.get_tiered_metadata_iter(macro_id, macro_id, iter));

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2Test"));
  uint64_t out_tenant_id = 0;
  share::ObLSID out_ls_id;
  ObSSMetaBaseType out_meta_type;
  ObString out_key;
  ObString out_value;
  ASSERT_EQ(OB_SUCCESS, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, out_key, out_value));
  ASSERT_EQ(large_size, out_value.length());
  ASSERT_EQ(0, MEMCMP(out_value.ptr(), large_buf, large_size));

  allocator.reuse();
  ASSERT_EQ(OB_ITER_END, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, out_key, out_value));

}

/**
 * 概要：验证 Tiered 元数据超过旧 4 MiB 固定缓冲区后，各读取出口仍返回完整数据。
 *
 * 覆盖模块：
 * 1. ObTieredMetadataObjectManager::read_tiered_metadata_from_table：
 *    动态申请结果缓冲区，并适配 DEFAULT 和 ROW_WITHOUT_ROWKEY 两种结果格式。
 * 2. ObTieredMetadataStore / ObTieredMetaTableRowData：
 *    多 part 行重组，以及携带 meta_type 的行格式反序列化。
 * 3. ObSSTabletMetaInfoIterator::read_tiered_meta_value_：
 *    覆盖 dump iterator 按 ObMetaDiskAddr.size 使用 caller buffer 的读取 helper；完整 init/get_next
 *    由 SYS tablet 用例覆盖。
 *
 * 测试流程：
 * 1. 写入 4 MiB + 1 byte 的 value，并在首字节、2 MiB 边界和末字节设置哨兵值。
 * 2. 按 DEFAULT 格式读取，校验长度、容量和全部字节。
 * 3. 按 ROW_WITHOUT_ROWKEY 格式读取并反序列化，校验 meta_type 和原 value。
 * 4. 分别触发非法格式和对象不存在，确认错误码正确且输出 reader 被清空。
 * 5. 构造合法 ObMetaDiskAddr 调用 dump helper，并验证非法 offset、size 不一致时的错误语义。
 * 6. 验证 addr.size 偏小时 helper 保留 OB_SIZE_OVERFLOW，上层转为 OB_INVALID_DATA。
 *
 * 回归判据：若仍使用固定 4 MiB 缓冲区，DEFAULT 对外返回 OB_IO_ERROR；
 * dump helper 则保留内部的 OB_SIZE_OVERFLOW。
 */
TEST_F(TestTieredMetadataStoreV2, test_dynamic_read_large_value)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300081;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);
  const int64_t value_size = 4 * 1024 * 1024 + 1;
  // 构造跨越多个 part 的 value，并在关键边界设置哨兵字节，避免只校验重复填充值。
  char *value_buf = static_cast<char *>(
      ob_malloc(value_size, "TMDynReadTest"));
  ASSERT_NE(nullptr, value_buf);
  DEFER(ob_free(value_buf));
  MEMSET(value_buf, 'X', value_size);
  value_buf[0] = 'A';
  value_buf[OB_DEFAULT_MACRO_BLOCK_SIZE - 1] = 'Y';
  value_buf[value_size - 1] = 'Z';

  const ObString meta_value(value_size, value_buf);
  ASSERT_EQ(OB_SUCCESS,
            store.write_meta(macro_id, kDefaultMetaType, meta_value));
  DEFER({
    EXPECT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  });

  // 先验证调用方最常用的裸 value 格式，结果缓冲区应按真实长度动态申请。
  ObArenaAllocator read_allocator(
      ObMemAttr(tenant_id_, "TMDynReadTest"));
  ObBufferReader data;
  ObObjectStorageTenantGuard storage_guard(
      tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS,
            ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                macro_id,
                ObObjectReadResultFormat::DEFAULT,
                read_allocator,
                data));
  ASSERT_TRUE(data.is_valid());
  ASSERT_GT(data.length(), OB_DEFAULT_MACRO_BLOCK_SIZE);
  ASSERT_EQ(value_size, data.length());
  ASSERT_EQ(value_size, data.capacity());
  ASSERT_EQ(0, MEMCMP(value_buf, data.data(), value_size));

  // 再验证携带 meta_type 的行格式，反序列化后应保留类型和完整 value。
  ObBufferReader serialized_data;
  ASSERT_EQ(OB_SUCCESS,
            ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                macro_id,
                ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY,
                read_allocator,
                serialized_data));
  ASSERT_TRUE(serialized_data.is_valid());
  ASSERT_GT(serialized_data.length(), value_size);
  ASSERT_EQ(serialized_data.length(), serialized_data.capacity());

  ObTieredMetaTableRowData decoded_row;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS,
            decoded_row.deserialize(
                serialized_data.data(), serialized_data.length(), pos));
  ASSERT_EQ(serialized_data.length(), pos);
  ASSERT_TRUE(decoded_row.is_valid());
  ASSERT_EQ(kDefaultMetaType, decoded_row.meta_type_);
  ASSERT_EQ(value_size, decoded_row.meta_value_.length());
  ASSERT_EQ(0,
            MEMCMP(value_buf, decoded_row.meta_value_.ptr(), value_size));

  // 同一对象的固定 buffer 与动态读取结果必须保持一致。
  char *fixed_buf = static_cast<char *>(
      read_allocator.alloc(serialized_data.length()));
  ASSERT_NE(nullptr, fixed_buf);
  int64_t fixed_actual_size = -1;
  ASSERT_EQ(OB_SUCCESS,
            ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                macro_id,
                ObObjectReadResultFormat::DEFAULT,
                fixed_buf,
                serialized_data.length(),
                fixed_actual_size));
  ASSERT_EQ(data.length(), fixed_actual_size);
  ASSERT_EQ(0, MEMCMP(data.data(), fixed_buf, fixed_actual_size));

  fixed_actual_size = -1;
  ASSERT_EQ(OB_SUCCESS,
            ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                macro_id,
                ObObjectReadResultFormat::ROW_WITHOUT_ROWKEY,
                fixed_buf,
                serialized_data.length(),
                fixed_actual_size));
  ASSERT_EQ(serialized_data.length(), fixed_actual_size);
  ASSERT_EQ(0,
            MEMCMP(serialized_data.data(), fixed_buf, fixed_actual_size));

  // 失败路径必须覆盖旧 reader 内容，避免调用方误用上一次成功读取留下的数据。
  ObBufferReader failed_data(value_buf, value_size, value_size);
  ASSERT_TRUE(failed_data.is_valid());
  ASSERT_EQ(OB_IO_ERROR,
            ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                macro_id,
                ObObjectReadResultFormat::MAX,
                read_allocator,
                failed_data));
  ASSERT_EQ(nullptr, failed_data.data());
  ASSERT_EQ(0, failed_data.length());
  ASSERT_EQ(0, failed_data.capacity());
  ASSERT_FALSE(failed_data.is_valid());

  // 对象不存在是正常缺失语义，应保留原错误码，同时仍将输出 reader 清空。
  const MacroBlockId missing_id =
      build_sub_meta_in_table_id(tablet_id, 99);
  failed_data.assign(value_buf, value_size, value_size);
  ASSERT_TRUE(failed_data.is_valid());
  ASSERT_EQ(OB_OBJECT_NOT_EXIST,
            ObTieredMetadataObjectManager::read_tiered_metadata_from_table(
                missing_id,
                ObObjectReadResultFormat::DEFAULT,
                read_allocator,
                failed_data));
  ASSERT_EQ(nullptr, failed_data.data());
  ASSERT_EQ(0, failed_data.length());
  ASSERT_EQ(0, failed_data.capacity());
  ASSERT_FALSE(failed_data.is_valid());

  // 这里定向覆盖 dump 的内部读取 helper；完整 iterator 生命周期在另一用例验证。
  ObArenaAllocator dump_read_allocator(
      ObMemAttr(tenant_id_, "TMDumpReadTest"));
  ObSSTabletMetaInfoIterator dump_iter;
  ObString dump_meta_value;
  ObMetaDiskAddr dump_meta_addr;
  ASSERT_EQ(OB_SUCCESS,
            dump_meta_addr.set_block_addr(
                macro_id,
                0 /*offset*/,
                value_size,
                ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_EQ(OB_SUCCESS,
            dump_iter.read_tiered_meta_value_(
                store, dump_meta_addr, dump_read_allocator, dump_meta_value));
  ASSERT_EQ(value_size, dump_meta_value.length());
  ASSERT_EQ(0, MEMCMP(value_buf, dump_meta_value.ptr(), value_size));

  // dump 地址只支持从对象起始位置读取，非法 offset 需清空输出。
  ObMetaDiskAddr invalid_offset_dump_addr;
  ASSERT_EQ(OB_SUCCESS,
            invalid_offset_dump_addr.set_block_addr(
                macro_id,
                1 /*offset*/,
                value_size,
                ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_EQ(OB_INVALID_DATA,
            dump_iter.read_tiered_meta_value_(
                store,
                invalid_offset_dump_addr,
                dump_read_allocator,
                dump_meta_value));
  ASSERT_TRUE(dump_meta_value.empty());

  // 地址 size 大于真实 value 时应视为数据不一致，并清空原有输出。
  dump_meta_value.assign_ptr(
      value_buf, static_cast<ObString::obstr_size_t>(value_size));
  ObMetaDiskAddr oversized_dump_addr;
  ASSERT_EQ(OB_SUCCESS,
            oversized_dump_addr.set_block_addr(
                macro_id,
                0 /*offset*/,
                value_size + 1,
                ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_EQ(OB_INVALID_DATA,
            dump_iter.read_tiered_meta_value_(
                store,
                oversized_dump_addr,
                dump_read_allocator,
                dump_meta_value));
  ASSERT_TRUE(dump_meta_value.empty());

  ObMetaDiskAddr undersized_dump_addr;
  ASSERT_EQ(OB_SUCCESS,
            undersized_dump_addr.set_block_addr(
                macro_id,
                0 /*offset*/,
                value_size - 1,
                ObMetaDiskAddr::DiskType::RAW_BLOCK));
  ASSERT_EQ(OB_SIZE_OVERFLOW,
            dump_iter.read_tiered_meta_value_(
                store,
                undersized_dump_addr,
                dump_read_allocator,
                dump_meta_value));
  ASSERT_TRUE(dump_meta_value.empty());

  // addr 长度不足表示引用数据不一致，上层不得再当作可忽略的缺失。
  ObSSTabletMetaInfoIterator::SubMetaEmitCtx dump_ctx;
  dump_ctx.tiered_store = &store;
  dump_ctx.tiered_alloc = &dump_read_allocator;
  ASSERT_EQ(OB_INVALID_DATA,
            dump_iter.read_sub_meta_blob_(
                dump_ctx, undersized_dump_addr, dump_meta_value));
  ASSERT_TRUE(dump_meta_value.empty());
}

/**
 * 概要：验证表内 Tiered 大对象能通过 Shared Storage HA 公共链路完成备份和恢复。
 *
 * 覆盖模块：
 * 1. ObCopySSMacroBlockBackupReader / ObCopySSMacroBlockBackupWriter：
 *    从 Tiered 表读取裸 value，并写入带公共头的备份对象。
 * 2. ObBackupStorageInfoOperator / ObBackupIoAdapter：
 *    注册恢复目的端路由，定位备份对象，并校验对象实际长度。
 * 3. ObCopySSMacroBlockRestoreReader / ObCopySSMacroBlockRestoreWriter：
 *    读取备份对象、校验并剥离公共头，再把裸 value 按多 part 形式写回 Tiered 表。
 *
 * 测试流程：
 * 1. 写入 4 MiB + 1 byte 的多 part value，并构造与生产输入一致的 HA macro batch。
 * 2. 注册备份目的端和系统任务，随后走 backup reader/writer 的 init/process。
 * 3. 确认表内对象的占用量统计保持未知值 0，并校验备份对象的公共头和真实长度。
 * 4. 删除源行并确认 part 已清空，再走 restore reader/writer 的 init/process。
 * 5. 回读恢复结果，校验全部字节、恢复后的 meta_type 和多 part 形态。
 *
 * 资源约束：每个副作用成功后立即注册幂等清理；备份对象在写入前预注册清理。
 * 回归判据：restore reader 首次以 2 MiB 读取失败后，若不按实际文件长度重读，
 * restore process 无法完成。
 */
TEST_F(TestTieredMetadataStoreV2, test_large_value_backup_restore_path)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300082;
  const int64_t value_size = 4 * 1024 * 1024 + 1;
  const ObLSID ls_id(1001);
  const MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1, ls_id.id());
  ASSERT_TRUE(macro_id.is_shared_tablet_sub_meta_in_table());
  // 源数据跨越多个 part，并在边界位置设置不同字节，确保恢复后不是仅长度相同。
  char *value_buf = static_cast<char *>(
      ob_malloc(value_size, "TMHABackupTest"));
  ASSERT_NE(nullptr, value_buf);
  DEFER(ob_free(value_buf));
  MEMSET(value_buf, 'B', value_size);
  value_buf[0] = 'A';
  value_buf[OB_DEFAULT_MACRO_BLOCK_SIZE - 1] = 'Y';
  value_buf[value_size - 1] = 'Z';

  ASSERT_EQ(OB_SUCCESS,
            store.write_meta(
                macro_id,
                kDefaultMetaType,
                ObString(value_size, value_buf)));
  DEFER({
    const int cleanup_ret = store.del_meta(macro_id);
    EXPECT_TRUE(OB_SUCCESS == cleanup_ret
                || OB_OBJECT_NOT_EXIST == cleanup_ret);
  });

  // HA reader 按 batch 消费宏块；这里使用真实 LS 和表内 sub-meta macro id。
  ObSSHAMacroBlockInfo macro_info;
  macro_info.ls_id_ = ls_id;
  macro_info.macro_block_id_ = macro_id;
  // 表内 Tiered sub-meta 没有独立物理宏块，生产 iterator 用 0 表示占用量未知。
  // reader 只透传该统计值，真实传输长度由下面的备份对象和恢复内容校验。
  macro_info.occupy_size_ = 0;
  ObSSHAMacroBlockBatch macro_batch;
  ASSERT_EQ(OB_SUCCESS, macro_batch.add_one(macro_info));

  ObBackupDest backup_dest;
  ASSERT_EQ(OB_SUCCESS, backup_dest.set(shared_storage_info));
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = 300082;
  backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  ObBackupDest backup_set_dest;
  ASSERT_EQ(OB_SUCCESS,
            ObBackupPathUtil::construct_backup_set_dest(
                backup_dest, backup_set_desc, backup_set_dest));

  // restore reader 通过该记录查找对象存储路由，测试结束后必须删除对应数据库行。
  const int64_t dest_id = 1;
  const ObStorageIdMod backup_mod(
      dest_id, ObStorageUsedMod::STORAGE_USED_BACKUP);
  ObBackupIOInfo io_info;
  io_info.max_iops_ = backup_set_dest.get_storage_info()->max_iops_;
  io_info.max_bandwidth_ =
      backup_set_dest.get_storage_info()->max_bandwidth_;
  ASSERT_EQ(OB_SUCCESS,
            ObBackupStorageInfoOperator::insert_backup_storage_info(
                *GCTX.sql_proxy_,
                tenant_id_,
                backup_set_dest,
                ObBackupDestType::DEST_TYPE_RESTORE_DATA,
                dest_id,
                io_info,
                true));
  DEFER({
    EXPECT_EQ(OB_SUCCESS,
              ObBackupStorageInfoOperator::remove_backup_storage_info(
                  *GCTX.sql_proxy_, tenant_id_, backup_set_dest));
  });

  // backup/restore writer 会按 task id 检查取消状态，因此注册一个真实的 HA copy task。
  ObDagId dag_id;
  dag_id.init(GCTX.self_addr());
  ObSysTaskStat task;
  task.start_time_ = ObTimeUtility::current_time();
  task.task_id_ = dag_id;
  task.task_type_ = SS_HA_MACRO_COPY_TASK;
  task.tenant_id_ = tenant_id_;
  ASSERT_EQ(OB_SUCCESS, SYS_TASK_STATUS_MGR.add_task(task));
  DEFER({
    EXPECT_EQ(OB_SUCCESS, SYS_TASK_STATUS_MGR.del_task(dag_id));
  });

  // 先注册幂等删除，再执行备份写入；即使 process 中途失败也不会残留测试对象。
  ObBackupPath backup_set_dir;
  ObBackupPath backup_path;
  ASSERT_EQ(OB_SUCCESS,
            ObBackupPathUtil::get_backup_set_dir_path(
                backup_set_dest, backup_set_dir));
  ASSERT_EQ(OB_SUCCESS,
            backup::ObSSBackupUtils::get_macro_block_backup_path(
                macro_id, backup_set_dir, backup_path));
  DEFER({
    const int cleanup_ret = ObBackupIoAdapter::del_file(
        backup_path.get_obstr(), backup_set_dest.get_storage_info());
    EXPECT_TRUE(OB_SUCCESS == cleanup_ret
                || OB_OBJECT_NOT_EXIST == cleanup_ret);
  });

  // 走公开 init/process 链路，验证大对象读取、公共头封装和对象存储写入。
  ObCopySSMacroBlockBackupReader backup_reader;
  ASSERT_EQ(OB_SUCCESS,
            backup_reader.init(
                backup_dest, backup_set_desc, backup_mod, macro_batch));
  ObObjectStorageTenantGuard storage_guard(
      tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  ObCopySSMacroBlockBackupWriter backup_writer;
  ASSERT_EQ(OB_SUCCESS,
            backup_writer.init(
                tenant_id_,
                dag_id,
                backup_dest,
                backup_set_desc,
                backup_mod,
                &backup_reader));
  ASSERT_EQ(OB_SUCCESS, backup_writer.process());
  ASSERT_EQ(0, backup_reader.get_data_size());

  int64_t backup_file_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            ObBackupIoAdapter::get_file_length(
                backup_path.get_obstr(),
                backup_set_dest.get_storage_info(),
                backup_file_size));
  ASSERT_EQ(value_size + sizeof(ObBackupCommonHeader), backup_file_size);
  ASSERT_GT(backup_file_size, OB_DEFAULT_MACRO_BLOCK_SIZE);

  // 恢复前主动删除源行，并确认后续读到的数据只能来自备份对象。
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));
  int64_t stored_meta_type = 0;
  int64_t stored_part_count = 0;
  ASSERT_EQ(OB_SUCCESS,
            query_stored_meta_type(
                store,
                macro_id,
                stored_meta_type,
                stored_part_count));
  ASSERT_EQ(0, stored_part_count);

  // restore reader 解析备份对象，writer 再通过 Tiered store 写回多 part 行。
  ObCopySSMacroBlockRestoreReader restore_reader;
  ASSERT_EQ(OB_SUCCESS,
            restore_reader.init(
                tenant_id_, backup_set_dest, macro_batch));
  ObCopySSMacroBlockRestoreWriter restore_writer;
  ASSERT_EQ(OB_SUCCESS,
            restore_writer.init(
                tenant_id_, ls_id, dag_id, &restore_reader));
  ASSERT_EQ(OB_SUCCESS, restore_writer.process());
  ASSERT_EQ(0, restore_reader.get_data_size());

  // 同时校验内容和存储形态，防止只恢复了长度正确但内容截断的单 part 数据。
  char *restored_buf = static_cast<char *>(
      ob_malloc(value_size, "TMHARestoreTest"));
  ASSERT_NE(nullptr, restored_buf);
  DEFER(ob_free(restored_buf));
  int64_t restored_size = 0;
  ASSERT_EQ(OB_SUCCESS,
            store.read_meta(
                macro_id, restored_buf, value_size, restored_size));
  ASSERT_EQ(value_size, restored_size);
  ASSERT_EQ(0, MEMCMP(value_buf, restored_buf, value_size));
  ASSERT_EQ(OB_SUCCESS,
            query_stored_meta_type(
                store,
                macro_id,
                stored_meta_type,
                stored_part_count));
  ASSERT_EQ(-1, stored_meta_type);
  ASSERT_GT(stored_part_count, 1);
}

/**
 * 概要：直接验证 ObSSBackupRestoreUtil::read_ss_macro_block 能读出超过默认 2 MiB
 * 缓冲区的表内 sub-meta 备份对象。
 *
 * 覆盖模块：
 * 1. ObSSBackupUtils::wrap_with_backup_common_header：
 *    构造与生产备份写路径一致的"公共头 + 裸 value"备份对象内容。
 * 2. ObBackupIoAdapter::write_single_file / del_file：
 *    把备份对象写入备份目的端，并在用例结束时幂等清理。
 * 3. ObSSBackupRestoreUtil::read_ss_macro_block：
 *    首次按 OB_DEFAULT_MACRO_BLOCK_SIZE 读取返回 OB_BUF_NOT_ENOUGH 后，
 *    按实际文件长度重试，读出完整内容。
 *
 * 测试流程：
 * 1. 构造 4 MiB + 1 字节的 value，并在首字节、2 MiB 末尾和尾字节设置哨兵。
 * 2. 包装公共头后写入备份路径（写入前预注册幂等删除）。
 * 3. 直接调用 read_ss_macro_block，确认读出长度与写入文件长度一致。
 * 4. 剥离公共头后逐字节比对原 value。
 *
 * 回归判据：若移除"OB_BUF_NOT_ENOUGH 后按实际文件长度重读"的逻辑，本用例读失败。
 */
TEST_F(TestTieredMetadataStoreV2, test_read_ss_macro_block_large_sub_meta)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  const int64_t tablet_id = 300083;
  const ObLSID ls_id(1001);
  const MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1, ls_id.id());
  ASSERT_TRUE(macro_id.is_shared_tablet_sub_meta_in_table());

  // 4 MiB + 1 字节，约为默认读取缓冲（2 MiB 宏块）的两倍，必然触发首次读取的 OB_BUF_NOT_ENOUGH。
  const int64_t value_size = 4 * 1024 * 1024 + 1;
  char *value_buf = static_cast<char *>(
      ob_malloc(value_size, "TMReadSsMacro"));
  ASSERT_NE(nullptr, value_buf);
  DEFER(ob_free(value_buf));
  MEMSET(value_buf, 'C', value_size);
  value_buf[0] = 'A';
  value_buf[OB_DEFAULT_MACRO_BLOCK_SIZE - 1] = 'Y';
  value_buf[value_size - 1] = 'Z';

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMReadSsMacro"));

  ObBackupDest backup_dest;
  ASSERT_EQ(OB_SUCCESS, backup_dest.set(shared_storage_info));
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = 300083;
  backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  ObBackupDest backup_set_dest;
  ASSERT_EQ(OB_SUCCESS,
            ObBackupPathUtil::construct_backup_set_dest(
                backup_dest, backup_set_desc, backup_set_dest));
  const ObStorageIdMod backup_mod(
      1 /*dest_id*/, ObStorageUsedMod::STORAGE_USED_BACKUP);

  // 备份文件内容为"公共头 + 裸 value"，与生产备份写路径一致。
  ObBufferReader value_data(value_buf, value_size, value_size);
  ObBufferReader wrapped_data;
  ASSERT_EQ(OB_SUCCESS,
            backup::ObSSBackupUtils::wrap_with_backup_common_header(
                macro_id, value_data, allocator, wrapped_data));
  ASSERT_GT(wrapped_data.length(), OB_DEFAULT_MACRO_BLOCK_SIZE);

  ObBackupPath backup_set_dir;
  ObBackupPath backup_path;
  ASSERT_EQ(OB_SUCCESS,
            ObBackupPathUtil::get_backup_set_dir_path(
                backup_set_dest, backup_set_dir));
  ASSERT_EQ(OB_SUCCESS,
            backup::ObSSBackupUtils::get_macro_block_backup_path(
                macro_id, backup_set_dir, backup_path));
  // 先注册幂等删除，再写备份对象，确保中途失败也不残留。
  DEFER({
    const int cleanup_ret = ObBackupIoAdapter::del_file(
        backup_path.get_obstr(), backup_set_dest.get_storage_info());
    EXPECT_TRUE(OB_SUCCESS == cleanup_ret
                || OB_OBJECT_NOT_EXIST == cleanup_ret);
  });

  ObObjectStorageTenantGuard storage_guard(
      tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS,
            ObBackupIoAdapter::write_single_file(
                backup_path.get_obstr(),
                backup_set_dest.get_storage_info(),
                wrapped_data.data(),
                wrapped_data.length(),
                backup_mod));

  // 被测接口：首读 2 MiB 缓冲不足后，应按实际文件长度重读成功。
  ObBufferReader read_buffer;
  ASSERT_EQ(OB_SUCCESS,
            backup::ObSSBackupRestoreUtil::read_ss_macro_block(
                backup_set_dest, macro_id, backup_mod, allocator, read_buffer));
  ASSERT_EQ(wrapped_data.length(), read_buffer.length());

  ASSERT_EQ(OB_SUCCESS,
            backup::ObSSBackupUtils::unwrap_backup_common_header(
                macro_id, read_buffer));
  ASSERT_EQ(value_size, read_buffer.length());
  ASSERT_EQ(0, MEMCMP(value_buf, read_buffer.data(), value_size));
}

/**
 * 概要：直接验证 read_ss_macro_block_ 在读取缓冲区小于备份对象时返回 OB_BUF_NOT_ENOUGH。
 *
 * 覆盖模块：
 * 1. ObBackupIoAdapter::read_single_file：
 *    读完后用 get_file_length 校验完整性，截断即报 OB_BUF_NOT_ENOUGH。
 * 2. ObSSBackupRestoreUtil::read_ss_macro_block_：
 *    使用指定大小的 buffer 单次读取，不做重试，原样返回 OB_BUF_NOT_ENOUGH；
 *    本用例不覆盖公共入口的重试条件。
 *
 * 测试流程：
 * 1. 构造 2 MiB + 1 字节的裸 value，包装公共头后写入备份目的端。
 * 2. 以 OB_DEFAULT_MACRO_BLOCK_SIZE（2 MiB）为 buf_size 直接调用私有 helper，
 *    断言返回 OB_BUF_NOT_ENOUGH 且预先置空的输出 buffer 仍无效，不产生半包数据。
 *
 * 回归判据：若 helper 吞掉截断错误或对截断数据返回成功，本用例失败。
 */
TEST_F(TestTieredMetadataStoreV2, test_read_ss_macro_block_buf_not_enough)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  const int64_t tablet_id = 300084;
  const ObLSID ls_id(1001);
  const MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1, ls_id.id());
  ASSERT_TRUE(macro_id.is_shared_tablet_sub_meta_in_table());

  const int64_t value_size = OB_DEFAULT_MACRO_BLOCK_SIZE + 1;
  char *value_buf = static_cast<char *>(
      ob_malloc(value_size, "TMReadSsMacro"));
  ASSERT_NE(nullptr, value_buf);
  DEFER(ob_free(value_buf));
  MEMSET(value_buf, 'D', value_size);

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMReadSsMacro"));

  ObBackupDest backup_dest;
  ASSERT_EQ(OB_SUCCESS, backup_dest.set(shared_storage_info));
  ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = 300084;
  backup_set_desc.backup_type_.type_ = ObBackupType::FULL_BACKUP;
  ObBackupDest backup_set_dest;
  ASSERT_EQ(OB_SUCCESS,
            ObBackupPathUtil::construct_backup_set_dest(
                backup_dest, backup_set_desc, backup_set_dest));
  const ObStorageIdMod backup_mod(
      1 /*dest_id*/, ObStorageUsedMod::STORAGE_USED_BACKUP);

  ObBufferReader value_data(value_buf, value_size, value_size);
  ObBufferReader wrapped_data;
  ASSERT_EQ(OB_SUCCESS,
            backup::ObSSBackupUtils::wrap_with_backup_common_header(
                macro_id, value_data, allocator, wrapped_data));
  ASSERT_GT(wrapped_data.length(), OB_DEFAULT_MACRO_BLOCK_SIZE);

  ObBackupPath backup_set_dir;
  ObBackupPath backup_path;
  ASSERT_EQ(OB_SUCCESS,
            ObBackupPathUtil::get_backup_set_dir_path(
                backup_set_dest, backup_set_dir));
  ASSERT_EQ(OB_SUCCESS,
            backup::ObSSBackupUtils::get_macro_block_backup_path(
                macro_id, backup_set_dir, backup_path));
  DEFER({
    const int cleanup_ret = ObBackupIoAdapter::del_file(
        backup_path.get_obstr(), backup_set_dest.get_storage_info());
    EXPECT_TRUE(OB_SUCCESS == cleanup_ret
                || OB_OBJECT_NOT_EXIST == cleanup_ret);
  });

  ObObjectStorageTenantGuard storage_guard(
      tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  ASSERT_EQ(OB_SUCCESS,
            ObBackupIoAdapter::write_single_file(
                backup_path.get_obstr(),
                backup_set_dest.get_storage_info(),
                wrapped_data.data(),
                wrapped_data.length(),
                backup_mod));

  // buf_size 取默认 2 MiB，小于备份对象实际长度，helper 必须原样返回 OB_BUF_NOT_ENOUGH，
  // 预先置空的输出 buffer 应保持无效，不向调用方暴露本次截断数据。
  ObBufferReader read_buffer;
  read_buffer.assign(nullptr, 0, 0);
  ASSERT_EQ(OB_BUF_NOT_ENOUGH,
            backup::ObSSBackupRestoreUtil::read_ss_macro_block_(
                backup_path.get_obstr(),
                backup_set_dest.get_storage_info(),
                backup_mod,
                OB_DEFAULT_MACRO_BLOCK_SIZE,
                allocator,
                read_buffer));
  ASSERT_FALSE(read_buffer.is_valid());
}

// ============================================================================
// Test: cross-tablet isolation for list_meta_macro_files
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_list_meta_cross_tablet_isolation)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_a = 300090;
  const int64_t tablet_b = 300091;
  MacroBlockId id_a1 = build_sub_meta_in_table_id(tablet_a, 1);
  MacroBlockId id_a2 = inc_seq(id_a1);
  MacroBlockId id_b1 = build_sub_meta_in_table_id(tablet_b, 1);

  const char *data_a1 = "tablet_a_val_1";
  const char *data_a2 = "tablet_a_val_2";
  const char *data_b1 = "tablet_b_val_1";
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id_a1, kDefaultMetaType, ObString(STRLEN(data_a1), data_a1)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id_a2, kDefaultMetaType, ObString(STRLEN(data_a2), data_a2)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id_b1, kDefaultMetaType, ObString(STRLEN(data_b1), data_b1)));

  // list tablet_a should only return a's entries
  ObArray<MacroBlockId> macro_ids_a;
  ASSERT_EQ(OB_SUCCESS, store.list_meta_macro_files(false, 0, tablet_a, 0, macro_ids_a));
  ASSERT_EQ(2, macro_ids_a.count());
  ASSERT_EQ(id_a1, macro_ids_a.at(0));
  ASSERT_EQ(id_a2, macro_ids_a.at(1));

  // list tablet_b should only return b's entry
  ObArray<MacroBlockId> macro_ids_b;
  ASSERT_EQ(OB_SUCCESS, store.list_meta_macro_files(false, 0, tablet_b, 0, macro_ids_b));
  ASSERT_EQ(1, macro_ids_b.count());
  ASSERT_EQ(id_b1, macro_ids_b.at(0));

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.del_meta(id_a1));
  ASSERT_EQ(OB_SUCCESS, store.del_meta(id_a2));
  ASSERT_EQ(OB_SUCCESS, store.del_meta(id_b1));
}

// ============================================================================
// Test: iterator on empty range
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_get_tiered_metadata_iter_empty_range)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  // use a tablet_id that has no data written
  const int64_t tablet_id = 300100;
  MacroBlockId id_start = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId id_end = inc_seq(id_start);

  ObSSTieredMetaTableIterator iter;
  ASSERT_EQ(OB_SUCCESS, store.get_tiered_metadata_iter(id_start, id_end, iter));

  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2Test"));
  uint64_t out_tenant_id = 0;
  share::ObLSID out_ls_id;
  ObSSMetaBaseType out_meta_type;
  ObString meta_key;
  ObString meta_value;

  ASSERT_EQ(OB_ITER_END, iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value));
}

// ============================================================================
// Test: list after partial delete
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_list_meta_after_partial_delete)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300110;
  MacroBlockId id1 = build_sub_meta_in_table_id(tablet_id, 1);
  MacroBlockId id2 = inc_seq(id1);
  MacroBlockId id3 = inc_seq(id2);

  const char *data1 = "partial_del_1";
  const char *data2 = "partial_del_2";
  const char *data3 = "partial_del_3";
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id1, kDefaultMetaType, ObString(STRLEN(data1), data1)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id2, kDefaultMetaType, ObString(STRLEN(data2), data2)));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(id3, kDefaultMetaType, ObString(STRLEN(data3), data3)));

  // delete the middle one
  ASSERT_EQ(OB_SUCCESS, store.del_meta(id2));

  // list should return id1 and id3
  ObArray<MacroBlockId> macro_ids;
  ASSERT_EQ(OB_SUCCESS, store.list_meta_macro_files(false, 0, tablet_id, 0, macro_ids));
  ASSERT_EQ(2, macro_ids.count());
  ASSERT_EQ(id1, macro_ids.at(0));
  ASSERT_EQ(id3, macro_ids.at(1));

  // is_meta_exist should reflect the deletion
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(id2, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(id1, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(id3, is_exist));
  ASSERT_TRUE(is_exist);

  // cleanup
  ASSERT_EQ(OB_SUCCESS, store.del_meta(id1));
  ASSERT_EQ(OB_SUCCESS, store.del_meta(id3));
}

// ============================================================================
// Test: full lifecycle (write -> exist -> read -> del -> not exist)
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_full_lifecycle)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300070;
  MacroBlockId macro_id = build_sub_meta_in_table_id(tablet_id, 1);

  // 1. not exist initially
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);

  // 2. write
  const char *write_data = "lifecycle_test_data_content_12345";
  ObString meta_value(STRLEN(write_data), write_data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(macro_id, kDefaultMetaType, meta_value));

  // 3. exists
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_TRUE(is_exist);

  // 4. read and verify
  const int64_t buf_size = 1024;
  char read_buf[buf_size];
  int64_t actual_size = 0;
  ASSERT_EQ(OB_SUCCESS, store.read_meta(macro_id, read_buf, buf_size, actual_size));
  ASSERT_EQ(meta_value.length(), actual_size);
  ASSERT_EQ(0, MEMCMP(read_buf, write_data, actual_size));

  // 5. delete
  ASSERT_EQ(OB_SUCCESS, store.del_meta(macro_id));

  // 6. not exist after delete
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(macro_id, is_exist));
  ASSERT_FALSE(is_exist);

  // 7. read returns not exist
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, store.read_meta(macro_id, read_buf, buf_size, actual_size));
}

// ============================================================================
// Test: sys tenant double delete, object-first ordering, retry and errors
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_double_delete_order_and_retry)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(OB_SYS_TENANT_ID, &get_sql_client()));
  ObObjectStorageTenantGuard object_storage_guard(
      OB_SYS_TENANT_ID, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  const char *data = "sys_double_delete_meta";
  const ObString meta_value(STRLEN(data), data);

  auto prepare_both_sides = [&](const MacroBlockId &in_table_id,
                                MacroBlockId &remote_id) {
    ASSERT_EQ(OB_SUCCESS, store.write_meta(in_table_id, kDefaultMetaType, meta_value));
    remote_id = to_remote_id(in_table_id);
    write_remote_object(remote_id);
  };
  auto assert_table_exist = [&](const MacroBlockId &in_table_id, const bool expected) {
    bool is_exist = false;
    ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(in_table_id, is_exist));
    ASSERT_EQ(expected, is_exist);
  };
  auto batch_delete_one = [&](const MacroBlockId &in_table_id) {
    ObSEArray<MacroBlockId, 1> ids;
    EXPECT_EQ(OB_SUCCESS, ids.push_back(in_table_id));
    return ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids);
  };

  // The production switch is enabled: both copies are deleted.
  const MacroBlockId batch_delete_id = build_sub_meta_in_table_id(300301, 1);
  MacroBlockId batch_delete_remote_id;
  prepare_both_sides(batch_delete_id, batch_delete_remote_id);
  ASSERT_EQ(OB_SUCCESS, batch_delete_one(batch_delete_id));
  assert_table_exist(batch_delete_id, false);
  assert_remote_object_exist(batch_delete_remote_id, false);

  // Inject a table-delete error. The remote object must already be gone while
  // the table row remains, which proves object storage is deleted first.
  const MacroBlockId retry_id = build_sub_meta_in_table_id(300302, 2);
  MacroBlockId retry_remote_id;
  prepare_both_sides(retry_id, retry_remote_id);
  {
    ScopedNamedEvent table_delete_error;
    ASSERT_EQ(OB_SUCCESS, table_delete_error.init(
        "EN_SS_TIERED_META_TABLE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_IO_ERROR, batch_delete_one(retry_id));
  }
  assert_remote_object_exist(retry_remote_id, false);
  assert_table_exist(retry_id, true);

  // Retry after remote deletion. OB_OBJECT_NOT_EXIST is normalized to success,
  // then the remaining inner-table row is deleted.
  {
    ScopedNamedEvent remote_not_exist;
    ASSERT_EQ(OB_SUCCESS, remote_not_exist.init(
        "EN_SS_TIERED_META_REMOTE_DELETE_ERROR", OB_OBJECT_NOT_EXIST, 1));
    ASSERT_EQ(OB_SUCCESS, batch_delete_one(retry_id));
  }
  assert_table_exist(retry_id, false);
  assert_remote_object_exist(retry_remote_id, false);

  // A normal remote error must stop before deleting the inner-table row.
  const MacroBlockId remote_error_id = build_sub_meta_in_table_id(300303, 3);
  MacroBlockId remote_error_remote_id;
  prepare_both_sides(remote_error_id, remote_error_remote_id);
  {
    ScopedNamedEvent remote_delete_error;
    ASSERT_EQ(OB_SUCCESS, remote_delete_error.init(
        "EN_SS_TIERED_META_REMOTE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_IO_ERROR, batch_delete_one(remote_error_id));
  }
  assert_table_exist(remote_error_id, true);
  assert_remote_object_exist(remote_error_remote_id, true);
  ASSERT_EQ(OB_SUCCESS, batch_delete_one(remote_error_id));
  assert_table_exist(remote_error_id, false);
  assert_remote_object_exist(remote_error_remote_id, false);

  // The single-delete interface delegates to the same double-delete path.
  const MacroBlockId single_delete_id = build_sub_meta_in_table_id(300304, 4);
  MacroBlockId single_delete_remote_id;
  prepare_both_sides(single_delete_id, single_delete_remote_id);
  ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::del_tiered_metadata(single_delete_id));
  assert_table_exist(single_delete_id, false);
  assert_remote_object_exist(single_delete_remote_id, false);
}

// ============================================================================
// Test: TABLE single-delete preserves the remote copy and skips remote deletion
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_batch_delete_single_route)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ObObjectStorageTenantGuard object_storage_guard(
      tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  ObTieredMetadataStore store;
  const MacroBlockId table_id = build_sub_meta_in_table_id(300306, 6);
  const MacroBlockId remote_id = to_remote_id(table_id);
  const ObString value = ObString::make_string("single_delete_location");
  ObArray<MacroBlockId> ids;
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(table_id, kDefaultMetaType, value));
  ASSERT_NO_FATAL_FAILURE(write_remote_object(remote_id));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(table_id));
  {
    ScopedNamedEvent remote_delete_error;
    ASSERT_EQ(OB_SUCCESS, remote_delete_error.init(
        "EN_SS_TIERED_META_REMOTE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
    ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(table_id, is_exist));
    ASSERT_FALSE(is_exist);
    ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(remote_id, true));
    ids.reuse();
    ASSERT_EQ(OB_SUCCESS, ids.push_back(remote_id));
    ASSERT_EQ(OB_IO_ERROR, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  }
  ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(remote_id, false));
}

// ============================================================================
// Test: an OSS-only batch skips TABLE deletion even for the double-route tenant
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_batch_delete_object_storage_only)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObObjectStorageTenantGuard object_storage_guard(
      OB_SYS_TENANT_ID, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  ObTieredMetadataStore store;
  const MacroBlockId table_id = build_sub_meta_in_table_id(300307, 7);
  const MacroBlockId remote_id = to_remote_id(table_id);
  const ObString value = ObString::make_string("object_storage_location");
  ObArray<MacroBlockId> ids;
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, store.init(OB_SYS_TENANT_ID, &get_sql_client()));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(table_id, kDefaultMetaType, value));
  ASSERT_NO_FATAL_FAILURE(write_remote_object(remote_id));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(remote_id));
  {
    ScopedNamedEvent table_delete_error;
    ASSERT_EQ(OB_SUCCESS, table_delete_error.init(
        "EN_SS_TIERED_META_TABLE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
    ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(remote_id, false));
    ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(table_id, is_exist));
    ASSERT_TRUE(is_exist);
    ids.reuse();
    ASSERT_EQ(OB_SUCCESS, ids.push_back(table_id));
    ASSERT_EQ(OB_IO_ERROR, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  }
  ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(table_id, is_exist));
  ASSERT_FALSE(is_exist);
}

// ============================================================================
// Test: mixed locations and failures remain retryable
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_batch_delete_mixed_locations_and_retry)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));
  ObObjectStorageTenantGuard object_storage_guard(
      OB_SYS_TENANT_ID, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  ObTieredMetadataStore store;
  const MacroBlockId table_id = build_sub_meta_in_table_id(300308, 8);
  const MacroBlockId remote_id = to_remote_id(table_id);
  const MacroBlockId direct_remote_id = to_remote_id(inc_seq(table_id));
  const ObString value = ObString::make_string("mixed_delete_locations");
  ObArray<MacroBlockId> ids;
  bool is_exist = false;
  ASSERT_EQ(OB_SUCCESS, store.init(OB_SYS_TENANT_ID, &get_sql_client()));
  ASSERT_EQ(OB_SUCCESS, store.write_meta(table_id, kDefaultMetaType, value));
  ASSERT_NO_FATAL_FAILURE(write_remote_object(remote_id));
  ASSERT_NO_FATAL_FAILURE(write_remote_object(direct_remote_id));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(direct_remote_id));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(table_id));
  {
    ScopedNamedEvent remote_delete_error;
    ASSERT_EQ(OB_SUCCESS, remote_delete_error.init(
        "EN_SS_TIERED_META_REMOTE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_IO_ERROR, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  }
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(table_id, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(remote_id, true));
  ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(direct_remote_id, true));
  {
    ScopedNamedEvent table_delete_error;
    ASSERT_EQ(OB_SUCCESS, table_delete_error.init(
        "EN_SS_TIERED_META_TABLE_DELETE_ERROR", OB_IO_ERROR, 1));
    ASSERT_EQ(OB_IO_ERROR, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  }
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(table_id, is_exist));
  ASSERT_TRUE(is_exist);
  ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(remote_id, false));
  ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(direct_remote_id, false));
  ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  ASSERT_EQ(OB_SUCCESS, store.is_meta_exist(table_id, is_exist));
  ASSERT_FALSE(is_exist);
  ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
}

// ============================================================================
// Test: validate every input before deleting any object
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_batch_delete_invalid_location_and_type)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
  ObObjectStorageTenantGuard object_storage_guard(
      tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  const MacroBlockId remote_id = to_remote_id(build_sub_meta_in_table_id(300309, 9));
  MacroBlockId invalid_id = remote_id;
  ObArray<MacroBlockId> ids;
  ASSERT_NO_FATAL_FAILURE(write_remote_object(remote_id));
  ASSERT_EQ(OB_SUCCESS, ids.push_back(remote_id));
  invalid_id.set_location(ObMacroBlockIdLocation::CONTROL);
  ASSERT_EQ(OB_SUCCESS, ids.push_back(invalid_id));
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(remote_id, true));
  invalid_id = remote_id;
  invalid_id.set_storage_object_type(
      static_cast<uint64_t>(ObStorageObjectType::SHARED_MINI_DATA_MACRO));
  ids.at(1) = invalid_id;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(remote_id, true));
  ids.pop_back();
  ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::batch_del_tiered_metadata(ids));
  ASSERT_NO_FATAL_FAILURE(assert_remote_object_exist(remote_id, false));
}

// ============================================================================
// Test: sys tenant existence falls back to object storage only in double mode
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_double_write_exist_fallback)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(OB_SYS_TENANT_ID));

  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(OB_SYS_TENANT_ID, &get_sql_client()));
  ObObjectStorageTenantGuard object_storage_guard(
      OB_SYS_TENANT_ID, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
  const MacroBlockId in_table_id = build_sub_meta_in_table_id(300305, 5);
  const MacroBlockId remote_id = to_remote_id(in_table_id);
  write_remote_object(remote_id);

  bool is_exist = true;
  ASSERT_EQ(OB_SUCCESS,
      ObTieredMetadataObjectManager::is_tiered_metadata_exist(in_table_id, is_exist));
  ASSERT_TRUE(is_exist);

  // With a table row present, the remote probe is skipped. The one-shot error
  // remains armed and fires only after the table row is removed.
  const char *data = "sys_exist_short_circuit";
  const ObString meta_value(STRLEN(data), data);
  ASSERT_EQ(OB_SUCCESS, store.write_meta(in_table_id, kDefaultMetaType, meta_value));
  {
    ScopedNamedEvent remote_exist_error;
    ASSERT_EQ(OB_SUCCESS, remote_exist_error.init(
        "EN_SS_TIERED_META_REMOTE_EXIST_ERROR", OB_IO_ERROR, 1));
    is_exist = false;
    ASSERT_EQ(OB_SUCCESS,
        ObTieredMetadataObjectManager::is_tiered_metadata_exist(in_table_id, is_exist));
    ASSERT_TRUE(is_exist);
    ASSERT_EQ(OB_SUCCESS, store.del_meta(in_table_id));
    is_exist = true;
    ASSERT_EQ(OB_IO_ERROR,
        ObTieredMetadataObjectManager::is_tiered_metadata_exist(in_table_id, is_exist));
    ASSERT_FALSE(is_exist);
  }

  delete_remote_object(remote_id);
  is_exist = true;
  ASSERT_EQ(OB_SUCCESS,
      ObTieredMetadataObjectManager::is_tiered_metadata_exist(in_table_id, is_exist));
  ASSERT_FALSE(is_exist);
}

// ============================================================================
// Test: tiered metadata 8192 write / batch delete performance
// ============================================================================

TEST_F(TestTieredMetadataStoreV2, test_batch_del_tiered_metadata_8192_cost)
{
  share::ObTenantSwitchGuard guard;
  OK(guard.switch_to(tenant_id_));

  static const int64_t BATCH_COUNT = 8192;
  ObTieredMetadataStore store;
  ASSERT_EQ(OB_SUCCESS, store.init(tenant_id_, &get_sql_client()));

  const int64_t tablet_id = 300200;
  MacroBlockId base_id = build_sub_meta_in_table_id(tablet_id, 1);

  ObArray<MacroBlockId> macro_ids;
  ObArray<int64_t> meta_types;
  ObArray<ObString> meta_values;
  const char *data = "batch_del_cost_test_value";
  const ObString meta_value(STRLEN(data), data);
  for (int64_t i = 0; i < BATCH_COUNT; ++i) {
    MacroBlockId id = base_id;
    id.set_third_id((1ULL << 32) + 1 + i);
    ASSERT_EQ(OB_SUCCESS, macro_ids.push_back(id));
    ASSERT_EQ(OB_SUCCESS, meta_types.push_back(kDefaultMetaType));
    ASSERT_EQ(OB_SUCCESS, meta_values.push_back(meta_value));
  }

  const int64_t write_start_us = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS, store.batch_write_meta(macro_ids, meta_types, meta_values));
  const int64_t write_cost_us = ObTimeUtility::current_time() - write_start_us;
  fprintf(stderr,
      "[PERF] batch_write_meta count=%ld cost_us=%ld cost_ms=%ld\n",
      BATCH_COUNT, write_cost_us, write_cost_us / 1000);

  {
    ObObjectStorageTenantGuard tablet_meta_guard(
        tenant_id_, ObSSGCUtil::TIERED_METADATA_IO_TIMEOUT_US);
    const int64_t del_start_us = ObTimeUtility::current_time();
    ASSERT_EQ(OB_SUCCESS, ObTieredMetadataObjectManager::batch_del_tiered_metadata(macro_ids));
    const int64_t del_cost_us = ObTimeUtility::current_time() - del_start_us;
    fprintf(stderr,
        "[PERF] batch_del_tiered_metadata count=%ld cost_us=%ld cost_ms=%ld\n",
        BATCH_COUNT, del_cost_us, del_cost_us / 1000);
  }
}

} // namespace unittest
} // namespace oceanbase

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
