/**
 * Copyright (c) 2022 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "gtest/gtest.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "share/ob_get_compat_mode.h"
#include "share/schema/ob_column_schema.h"
#include "ob_log_schema_getter.h"
#include "ob_log_meta_data_struct.h"
#define private public
#include "ob_log_part_mgr.h"
#undef private
#include "ob_log_tenant.h"
#include "ob_log_instance.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;
using namespace share::schema;

namespace oceanbase
{
namespace unittest
{
namespace
{
const uint64_t TENANT_ID = 1002;
const uint64_t DATABASE_ID = 500001;
const uint64_t PRIMARY_TABLE_ID = 603985;
const uint64_t LOB_TABLE_ID = 603987;
const int64_t TIMEOUT = 1000000;

enum PrimaryKind
{
  HIDDEN_INDEX,
  USER_PRIMARY,
  LOB_PRIMARY,
  INVALID_HIDDEN_PRIMARY,
  PRIMARY_KIND_COUNT
};

int init_schema(ObTableSchema &schema, const uint64_t table_id,
    const ObTableType table_type, const char *name)
{
  schema.set_tenant_id(TENANT_ID);
  schema.set_database_id(DATABASE_ID);
  schema.set_schema_version(1);
  schema.set_table_id(table_id);
  schema.set_tablet_id(table_id);
  schema.set_table_type(table_type);
  schema.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
  int ret = schema.set_table_name(ObString::make_string(name));
  if (OB_SUCC(ret)) {
    // Dictionary metadata walks real columns, including the prev/next ID chain.
    ObColumnSchemaV2 column;
    column.set_tenant_id(TENANT_ID);
    column.set_table_id(table_id);
    column.set_column_id(OB_APP_MIN_COLUMN_ID);
    column.set_schema_version(1);
    column.set_data_type(ObIntType);
    column.set_data_length(static_cast<int64_t>(sizeof(int64_t)));
    column.set_rowkey_position(1);
    column.set_nullable(false);
    if (OB_FAIL(column.set_column_name("id"))) {
    } else if (OB_FAIL(schema.add_column(column))) {
    }
  }
  return ret;
}

int init_primary(ObTableSchema &schema, const PrimaryKind kind)
{
  const ObTableType type = HIDDEN_INDEX == kind ? USER_INDEX
      : LOB_PRIMARY == kind ? AUX_LOB_META : USER_TABLE;
  const int ret = init_schema(schema, PRIMARY_TABLE_ID, type, "primary_table");
  if (HIDDEN_INDEX == kind) {
    schema.set_index_type(INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL);
    schema.set_data_table_id(601380);
  }
  if (HIDDEN_INDEX == kind || INVALID_HIDDEN_PRIMARY == kind) {
    schema.set_table_state_flag(TABLE_STATE_HIDDEN_OFFLINE_DDL);
    schema.set_association_table_id(OB_INVALID_ID);
  }
  return ret;
}

struct TableInfoResult
{
  TableInfoResult()
      : tenant_name_(nullptr), database_name_(nullptr), table_name_(nullptr),
        database_id_(OB_INVALID_ID), is_user_table_(true)
  {}
  const char *tenant_name_;
  const char *database_name_;
  const char *table_name_;
  uint64_t database_id_;
  bool is_user_table_;
};

// Only schema retrieval is mocked; table classification uses the real PartMgr.
class LobSchemaGuard : public ObLogSchemaGuard
{
public:
  explicit LobSchemaGuard(const ObTableSchema &primary)
      : primary_(primary), full_schema_fetch_count_(0), name_fetch_count_(0)
  {}

  int get_table_schema(const uint64_t tenant_id, const uint64_t table_id,
      const ObSimpleTableSchemaV2 *&schema, const int64_t timeout) override
  {
    schema = table_id == primary_.get_table_id() ? &primary_ : nullptr;
    return OB_SUCCESS;
  }

  int get_table_schema(const uint64_t tenant_id, const uint64_t table_id,
      const ObTableSchema *&schema, const int64_t timeout) override
  {
    ++full_schema_fetch_count_;
    schema = table_id == primary_.get_table_id() ? &primary_ : nullptr;
    return OB_SUCCESS;
  }

  int get_schema_version(const uint64_t tenant_id, int64_t &version) const override
  {
    version = 1;
    return OB_SUCCESS;
  }

  int get_tenant_schema_info(const uint64_t tenant_id, TenantSchemaInfo &info,
      const int64_t timeout) override
  {
    ++name_fetch_count_;
    info.tenant_id_ = tenant_id;
    info.version_ = 1;
    info.name_ = "test_tenant";
    return OB_SUCCESS;
  }

  int get_database_schema_info(const uint64_t tenant_id, const uint64_t database_id,
      DBSchemaInfo &info, const int64_t timeout) override
  {
    ++name_fetch_count_;
    info.reset(database_id, 1, "test_db");
    return OB_SUCCESS;
  }

  const ObTableSchema &primary_;
  int full_schema_fetch_count_;
  int name_fetch_count_;
};

class ObLogPartMgrLobTest : public ::testing::Test
{
public:
  ObLogPartMgrLobTest() : tenant_(), index_cache_(), part_mgr_(tenant_) {}

  void SetUp() override
  {
    // TabletToTableInfo allocates from the CDC tenant, which has no tenant manager here.
    lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
    ASSERT_NE(nullptr, malloc_allocator);
    const int create_ret = malloc_allocator->create_and_add_tenant_allocator(TENANT_ID);
    ASSERT_TRUE(OB_SUCCESS == create_ret || OB_ENTRY_EXIST == create_ret);
    ASSERT_EQ(OB_SUCCESS, malloc_allocator->set_tenant_limit(TENANT_ID, 1L << 30));
    // Schema construction needs a cached tenant mode without an observer or SQL.
    share::ObCompatModeGetter &compat_getter = share::ObCompatModeGetter::instance();
    const int compat_ret = compat_getter.init_for_obcdc();
    ASSERT_TRUE(OB_SUCCESS == compat_ret || OB_INIT_TWICE == compat_ret);
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
    ASSERT_EQ(OB_SUCCESS, compat_getter.set_tenant_compat_mode(TENANT_ID, compat_mode));
    ASSERT_EQ(OB_SUCCESS, part_mgr_.init(TENANT_ID, 1, false, true, index_cache_));
  }

  void check_result(const int ret, const TableInfoResult &result, const PrimaryKind kind)
  {
    if (HIDDEN_INDEX == kind) {
      EXPECT_EQ(OB_SUCCESS, ret);
      EXPECT_FALSE(result.is_user_table_);
      EXPECT_EQ(nullptr, result.table_name_);
    } else if (USER_PRIMARY == kind) {
      ASSERT_EQ(OB_SUCCESS, ret);
      EXPECT_TRUE(result.is_user_table_);
      EXPECT_STREQ("primary_table", result.table_name_);
      EXPECT_EQ(DATABASE_ID, result.database_id_);
    } else {
      EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
    }
  }

  ObLogTenant tenant_;
  GIndexCache index_cache_;
  ObLogPartMgr part_mgr_;
};

TEST_F(ObLogPartMgrLobTest, OnlineSchemaFiltersIndexLob)
{
  const ObTableType lob_types[] = {AUX_LOB_META, AUX_LOB_PIECE};
  for (const ObTableType lob_type : lob_types) {
    for (int kind_idx = HIDDEN_INDEX; kind_idx < PRIMARY_KIND_COUNT; ++kind_idx) {
      const PrimaryKind kind = static_cast<PrimaryKind>(kind_idx);
      SCOPED_TRACE(::testing::Message() << "lob_type=" << lob_type << ", primary_kind=" << kind);
      ObTableSchema primary;
      ObTableSchema lob;
      ASSERT_EQ(OB_SUCCESS, init_primary(primary, kind));
      ASSERT_EQ(OB_SUCCESS, init_schema(lob, LOB_TABLE_ID, lob_type, "lob_aux"));
      ASSERT_EQ(1, primary.get_column_count());
      ASSERT_EQ(1, lob.get_column_count());
      ASSERT_EQ(1, primary.get_rowkey_info().get_size());
      ASSERT_EQ(1, lob.get_rowkey_info().get_size());
      const ObColumnSchemaV2 *primary_column = primary.get_column_schema(OB_APP_MIN_COLUMN_ID);
      const ObColumnSchemaV2 *lob_column = lob.get_column_schema(OB_APP_MIN_COLUMN_ID);
      ASSERT_NE(nullptr, primary_column);
      ASSERT_NE(nullptr, lob_column);
      ASSERT_EQ(BORDER_COLUMN_ID, primary_column->get_prev_column_id());
      ASSERT_EQ(BORDER_COLUMN_ID, primary_column->get_next_column_id());
      ASSERT_EQ(BORDER_COLUMN_ID, lob_column->get_prev_column_id());
      ASSERT_EQ(BORDER_COLUMN_ID, lob_column->get_next_column_id());
      lob.set_data_table_id(PRIMARY_TABLE_ID);
      LobSchemaGuard schema_guard(primary);
      TableInfoResult result;
      const int ret = part_mgr_.get_table_info_of_table_schema_(schema_guard, &lob,
          result.tenant_name_, result.database_name_, result.table_name_,
          result.database_id_, result.is_user_table_, TIMEOUT);
      check_result(ret, result, kind);
      if (HIDDEN_INDEX == kind) {
        EXPECT_EQ(0, schema_guard.full_schema_fetch_count_);
        EXPECT_EQ(0, schema_guard.name_fetch_count_);
      } else if (INVALID_HIDDEN_PRIMARY == kind) {
        // Invalid offline-DDL metadata must continue to fail.
        EXPECT_EQ(1, schema_guard.full_schema_fetch_count_);
      }
    }
  }
}

TEST_F(ObLogPartMgrLobTest, DataDictionaryFiltersIndexLob)
{
  const ObTableType lob_types[] = {AUX_LOB_META, AUX_LOB_PIECE};
  for (const ObTableType lob_type : lob_types) {
    for (int kind_idx = HIDDEN_INDEX; kind_idx < PRIMARY_KIND_COUNT; ++kind_idx) {
      const PrimaryKind kind = static_cast<PrimaryKind>(kind_idx);
      SCOPED_TRACE(::testing::Message() << "lob_type=" << lob_type << ", primary_kind=" << kind);
      ObDictTenantInfo tenant_info;
      ASSERT_EQ(OB_SUCCESS, tenant_info.init());
      ObTenantSchema tenant_schema;
      tenant_schema.set_tenant_id(TENANT_ID);
      tenant_schema.set_schema_version(1);
      tenant_schema.set_status(TENANT_STATUS_NORMAL);
      tenant_schema.set_compatibility_mode(ObCompatibilityMode::MYSQL_MODE);
      ASSERT_EQ(OB_SUCCESS, tenant_schema.set_tenant_name("test_tenant"));
      ASSERT_EQ(OB_SUCCESS, tenant_info.get_dict_tenant_meta().init(tenant_schema));
      ASSERT_TRUE(tenant_info.get_dict_tenant_meta().is_valid());

      ObDatabaseSchema database;
      database.set_tenant_id(TENANT_ID);
      database.set_database_id(DATABASE_ID);
      database.set_schema_version(1);
      ASSERT_EQ(OB_SUCCESS, database.set_database_name("test_db"));
      datadict::ObDictDatabaseMeta *database_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, tenant_info.alloc_dict_db_meta(database_meta));
      ASSERT_EQ(OB_SUCCESS, database_meta->init(database));
      ASSERT_EQ(OB_SUCCESS, tenant_info.insert_dict_db_meta(database_meta));

      ObTableSchema primary;
      ObTableSchema lob;
      ASSERT_EQ(OB_SUCCESS, init_primary(primary, kind));
      ASSERT_EQ(OB_SUCCESS, init_schema(lob, LOB_TABLE_ID, lob_type, "lob_aux"));
      ASSERT_EQ(1, primary.get_column_count());
      ASSERT_EQ(1, lob.get_column_count());
      ASSERT_EQ(1, primary.get_rowkey_info().get_size());
      ASSERT_EQ(1, lob.get_rowkey_info().get_size());
      const ObColumnSchemaV2 *primary_column = primary.get_column_schema(OB_APP_MIN_COLUMN_ID);
      const ObColumnSchemaV2 *lob_column = lob.get_column_schema(OB_APP_MIN_COLUMN_ID);
      ASSERT_NE(nullptr, primary_column);
      ASSERT_NE(nullptr, lob_column);
      ASSERT_EQ(BORDER_COLUMN_ID, primary_column->get_prev_column_id());
      ASSERT_EQ(BORDER_COLUMN_ID, primary_column->get_next_column_id());
      ASSERT_EQ(BORDER_COLUMN_ID, lob_column->get_prev_column_id());
      ASSERT_EQ(BORDER_COLUMN_ID, lob_column->get_next_column_id());
      lob.set_data_table_id(PRIMARY_TABLE_ID);
      datadict::ObDictTableMeta *primary_meta = nullptr;
      datadict::ObDictTableMeta *lob_meta = nullptr;
      ASSERT_EQ(OB_SUCCESS, tenant_info.alloc_dict_table_meta(primary_meta));
      ASSERT_EQ(OB_SUCCESS, primary_meta->init(primary));
      ASSERT_EQ(OB_SUCCESS, tenant_info.insert_dict_table_meta(primary_meta));
      ASSERT_EQ(OB_SUCCESS, tenant_info.alloc_dict_table_meta(lob_meta));
      ASSERT_EQ(OB_SUCCESS, lob_meta->init(lob));

      TableInfoResult result;
      const int ret = part_mgr_.get_table_info_of_table_meta_(&tenant_info, lob_meta,
          result.tenant_name_, result.database_name_, result.table_name_,
          result.database_id_, result.is_user_table_, TIMEOUT);
      check_result(ret, result, kind);
      ASSERT_EQ(OB_SUCCESS, tenant_info.remove_table_meta(PRIMARY_TABLE_ID));
      ASSERT_EQ(OB_SUCCESS, tenant_info.free_dict_table_meta(lob_meta));
    }
  }
}
} // namespace
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_log_part_mgr.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_log_part_mgr.log", true);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  testing::InitGoogleTest(&argc, argv);
  // Tenant fixtures access TCTX during destruction, so keep the singleton alive.
  if (nullptr == ObLogInstance::get_instance()) {
    return 1;
  }
  const int ret = RUN_ALL_TESTS();
  ObLogInstance::destroy_instance();
  return ret;
}
