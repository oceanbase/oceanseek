/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#define USING_LOG_PREFIX RS
#define private public
#define protected public
#include "observer/ob_server_struct.h"
#include "rootserver/ddl_task/ob_async_index_ddl_util.h"
#include "rootserver/ddl_task/ob_drop_async_vector_index_task.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ob_root_service.h"
#include "share/ob_index_builder_util.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_table_sql_service.h"
#include "storage/tablet/ob_async_index_refresh_info_mds_helper.h"

namespace oceanbase
{
using namespace common;
using namespace rootserver;
using namespace share;
using namespace share::schema;

namespace unittest
{
static const uint64_t TENANT_ID = 1002;
static const uint64_t SOURCE_ID = 500002;
static const uint64_t HIDDEN_ID = 500003;
static const uint64_t ASYNC_ID = 500004;
static const uint64_t NORMAL_ID = 500005;
static const uint64_t GENERATED_COLUMN_ID = 100;

struct RebuildContext
{
  ObTableSchema source_;
  ObTableSchema hidden_;
  ObTableSchema async_index_;
  ObTableSchema normal_index_;
  ObTableSchema pending_schema_;
  ObTableSchema committed_schema_;
  storage::ObTabletAsyncIndexRefreshInfo pending_membership_;
  storage::ObTabletAsyncIndexRefreshInfo committed_membership_;
  ObSArray<ObTableSchema> created_indexes_;
  ObMySQLTransaction *trans_ = nullptr;
  uint64_t next_index_id_ = 600001;
  int64_t updates_ = 0;
  int64_t membership_writes_ = 0;
  int64_t generated_indexes_ = 0;
  int64_t reconstructed_indexes_ = 0;
  int64_t commits_ = 0;
  int64_t rollbacks_ = 0;
  int64_t publications_ = 0;
  int membership_ret_ = OB_SUCCESS;

  int add_generated_column(ObTableSchema &schema)
  {
    int ret = OB_SUCCESS;
    ObColumnSchemaV2 column;
    column.set_tenant_id(TENANT_ID);
    column.set_table_id(HIDDEN_ID);
    column.set_column_id(GENERATED_COLUMN_ID);
    column.set_data_type(ObIntType);
    column.set_is_hidden(true);
    if (OB_FAIL(column.set_column_name("__offline_generated"))) {
    } else if (OB_FAIL(schema.add_column(column))) {
    } else {
      schema.set_max_used_column_id(GENERATED_COLUMN_ID);
    }
    return ret;
  }
};

static RebuildContext *rebuild_ctx = nullptr;
} // namespace unittest

// This executable keeps rebuild/reconstruct/add_new/reconcile and membership construction real.
// Schema queries, schema generators, and transaction/tablet writes use an in-memory backend.
namespace share
{
namespace schema
{
int ObSchemaGetterGuard::get_schema_version(const uint64_t tenant_id, int64_t &version) const
{
  EXPECT_EQ(unittest::TENANT_ID, tenant_id);
  version = 100;
  return OB_SUCCESS;
}

int ObSchemaGetterGuard::get_table_schema(
    const uint64_t tenant_id, const uint64_t table_id, const ObTableSchema *&schema)
{
  int ret = OB_SUCCESS;
  EXPECT_EQ(unittest::TENANT_ID, tenant_id);
  unittest::RebuildContext &ctx = *unittest::rebuild_ctx;
  schema = nullptr;
  if (unittest::SOURCE_ID == table_id) {
    schema = &ctx.source_;
  } else if (unittest::HIDDEN_ID == table_id) {
    schema = &ctx.hidden_;
  } else if (unittest::ASYNC_ID == table_id) {
    schema = &ctx.async_index_;
  } else if (unittest::NORMAL_ID == table_id) {
    schema = &ctx.normal_index_;
  } else {
    ADD_FAILURE() << "unexpected table lookup: " << table_id;
    ret = OB_TABLE_NOT_EXIST;
  }
  return ret;
}

int ObSchemaGetterGuard::get_table_schema(
    const uint64_t, const uint64_t, const ObString &, const bool,
    const ObTableSchema *&schema, const bool, const bool)
{
  schema = nullptr;
  return OB_SUCCESS;
}

int ObSchemaGetterGuard::get_index_schemas_with_data_table_id(
    const uint64_t, const uint64_t, ObIArray<const ObSimpleTableSchemaV2 *> &schemas)
{
  schemas.reuse();
  return OB_SUCCESS;
}

int ObSchemaGetterGuard::check_table_exist(
    const uint64_t, const uint64_t, const ObString &, const bool,
    const CheckTableType, bool &exists, uint64_t *)
{
  exists = false;
  return OB_SUCCESS;
}

int ObSchemaServiceSQLImpl::fetch_new_table_id(const uint64_t tenant_id, uint64_t &table_id)
{
  EXPECT_EQ(unittest::TENANT_ID, tenant_id);
  table_id = unittest::rebuild_ctx->next_index_id_++;
  return OB_SUCCESS;
}
} // namespace schema

int ObIndexBuilderUtil::adjust_expr_index_args(
    obrpc::ObCreateIndexArg &, ObTableSchema &, ObIAllocator &,
    ObIArray<ObColumnSchemaV2 *> &columns)
{
  columns.reuse();
  return OB_SUCCESS;
}
} // namespace share

namespace rootserver
{
int ObDDLService::check_inner_stat() const
{
  return nullptr == unittest::rebuild_ctx ? OB_NOT_INIT : OB_SUCCESS;
}

int ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
    const uint64_t tenant_id, ObSchemaGetterGuard &)
{
  EXPECT_EQ(unittest::TENANT_ID, tenant_id);
  return OB_SUCCESS;
}

int ObDDLSQLTransaction::start(
    ObISQLClient *, const uint64_t &tenant_id, const int64_t &version, bool)
{
  EXPECT_EQ(unittest::TENANT_ID, tenant_id);
  EXPECT_EQ(100, version);
  unittest::rebuild_ctx->trans_ = this;
  in_trans_ = true;
  return OB_SUCCESS;
}

int ObDDLSQLTransaction::end(const bool commit)
{
  int ret = OB_SUCCESS;
  unittest::RebuildContext &ctx = *unittest::rebuild_ctx;
  EXPECT_TRUE(in_trans_);
  if (commit) {
    ++ctx.commits_;
    if (OB_FAIL(ctx.committed_schema_.assign(ctx.pending_schema_))) {
    } else if (OB_FAIL(ctx.committed_membership_.assign(ctx.pending_membership_))) {
    }
  } else {
    ++ctx.rollbacks_;
  }
  in_trans_ = false;
  return ret;
}

int ObDDLOperator::update_table_attribute(
    ObTableSchema &schema, ObMySQLTransaction &trans,
    const ObSchemaOperationType op, const ObString *)
{
  int ret = OB_SUCCESS;
  unittest::RebuildContext &ctx = *unittest::rebuild_ctx;
  EXPECT_EQ(ctx.trans_, &trans);
  EXPECT_TRUE(trans.is_started());
  EXPECT_EQ(OB_DDL_ALTER_TABLE, op);
  EXPECT_EQ(unittest::HIDDEN_ID, schema.get_table_id());
  ++ctx.updates_;
  if (OB_FAIL(ObTableSqlService::check_ddl_allowed(schema))) {
  } else if (OB_FAIL(ctx.pending_schema_.assign(schema))) {
  }
  return ret;
}

int ObDDLService::publish_schema(uint64_t tenant_id)
{
  unittest::RebuildContext &ctx = *unittest::rebuild_ctx;
  EXPECT_EQ(unittest::TENANT_ID, tenant_id);
  EXPECT_EQ(1, ctx.commits_);
  ++ctx.publications_;
  return OB_SUCCESS;
}

int ObDDLService::publish_schema(uint64_t tenant_id, const ObAddrIArray &)
{
  return publish_schema(tenant_id);
}

int ObDDLService::generate_tablet_id(ObTableSchema &schema, ObIDGenerator *)
{
  schema.set_tablet_id(700000 + schema.get_table_id());
  return OB_SUCCESS;
}

int ObIndexBuilder::generate_schema(
    const obrpc::ObCreateIndexArg &arg, ObTableSchema &data_schema,
    const bool, const bool, ObTableSchema &index_schema)
{
  int ret = OB_SUCCESS;
  unittest::RebuildContext &ctx = *unittest::rebuild_ctx;
  ++ctx.generated_indexes_;
  if (OB_FAIL(index_schema.assign(arg.index_schema_))) {
  } else if (OB_FAIL(ctx.add_generated_column(data_schema))) {
  }
  return ret;
}

int ObDDLService::gen_hidden_index_schema_columns(
    const ObTableSchema &, const ObTableSchema &, ObSchemaGetterGuard &,
    const ObIArray<uint64_t> &, const ObColumnNameMap &, const bool,
    ObTableSchema &data_schema, ObTableSchema &, const ObIArray<obrpc::ObColumnSortItem> &,
    const ObIArray<ObString> &, const uint64_t)
{
  ++unittest::rebuild_ctx->reconstructed_indexes_;
  return unittest::rebuild_ctx->add_generated_column(data_schema);
}

int ObDDLService::rebuild_hidden_table_index_and_mlog(
    const uint64_t, ObSchemaGetterGuard &, ObDDLOperator &, ObMySQLTransaction &trans,
    const ObTableSchema &, ObSArray<ObTableSchema> &indexes)
{
  EXPECT_EQ(unittest::rebuild_ctx->trans_, &trans);
  return unittest::rebuild_ctx->created_indexes_.assign(indexes);
}
} // namespace rootserver

namespace storage
{
int ObTabletAsyncIndexRefreshInfoMdsHelper::register_tablets_refresh_info_(
    const uint64_t tenant_id, const ObIArray<ObTabletID> &tablets,
    const ObTabletAsyncIndexRefreshInfo &info, ObMySQLTransaction &trans, const int64_t)
{
  int ret = OB_SUCCESS;
  unittest::RebuildContext &ctx = *unittest::rebuild_ctx;
  EXPECT_EQ(unittest::TENANT_ID, tenant_id);
  EXPECT_EQ(ctx.trans_, &trans);
  EXPECT_TRUE(trans.is_started());
  EXPECT_EQ(1, ctx.updates_);
  EXPECT_EQ(1, tablets.count());
  if (1 == tablets.count()) {
    EXPECT_EQ(ctx.hidden_.get_tablet_id(), tablets.at(0));
  }
  EXPECT_EQ(ctx.pending_schema_.has_async_index(), info.has_async_index());
  ++ctx.membership_writes_;
  if (OB_FAIL(ctx.pending_membership_.assign(info))) {
  } else {
    ret = ctx.membership_ret_;
  }
  return ret;
}
} // namespace storage

namespace unittest
{
class TestHiddenTableIndexRebuild : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ObClusterVersion::get_instance().update_cluster_version(DATA_CURRENT_VERSION);
    ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().init(nullptr));
    lib::Worker::CompatMode mode = lib::Worker::CompatMode::MYSQL;
    ASSERT_EQ(OB_SUCCESS, ObCompatModeGetter::instance().set_tenant_compat_mode(TENANT_ID, mode));
  }
  static void TearDownTestCase() { ObCompatModeGetter::instance().destroy(); }

  void SetUp() override
  {
    rebuild_ctx = &ctx_;
    schema_service_.schema_service_ = &sql_schema_service_;
    ddl_service_.schema_service_ = &schema_service_;
    ddl_service_.sql_proxy_ = &proxy_;
    ASSERT_NO_FATAL_FAILURE(make_schema(ctx_.source_, SOURCE_ID, "source_table"));
    ASSERT_NO_FATAL_FAILURE(make_schema(ctx_.hidden_, HIDDEN_ID, "hidden_table"));
    ctx_.source_.set_association_table_id(HIDDEN_ID);
    ctx_.hidden_.set_table_state_flag(TABLE_STATE_HIDDEN_OFFLINE_DDL);
    ASSERT_FALSE(ctx_.hidden_.get_in_offline_ddl_white_list());
    ASSERT_EQ(OB_OP_NOT_ALLOW, ObTableSqlService::check_ddl_allowed(ctx_.hidden_));
    arg_.table_id_ = SOURCE_ID;
    arg_.hidden_table_id_ = HIDDEN_ID;
    arg_.exec_tenant_id_ = TENANT_ID;
    arg_.alter_table_schema_.set_tenant_id(TENANT_ID);
    arg_.alter_table_schema_.set_table_id(SOURCE_ID);
    arg_.is_alter_columns_ = true;
    arg_.tz_info_wrap_.set_tz_info_offset(0);
  }

  void TearDown() override
  {
    schema_service_.schema_service_ = nullptr;
    rebuild_ctx = nullptr;
  }

  static void make_schema(ObTableSchema &schema, const uint64_t table_id, const char *name,
                           const ObIndexType type = INDEX_TYPE_IS_NOT,
                           const uint64_t key_column_id = OB_APP_MIN_COLUMN_ID)
  {
    schema.set_tenant_id(TENANT_ID);
    schema.set_database_id(500001);
    schema.set_table_id(table_id);
    schema.set_schema_version(100);
    schema.set_tablet_id(700000 + table_id);
    schema.set_table_type(INDEX_TYPE_IS_NOT == type ? USER_TABLE : USER_INDEX);
    schema.set_index_type(type);
    schema.set_index_status(INDEX_STATUS_AVAILABLE);
    schema.set_rowkey_column_num(1);
    schema.set_max_used_column_id(key_column_id);
    ASSERT_EQ(OB_SUCCESS, schema.set_table_name(name));
    ObColumnSchemaV2 column;
    column.set_tenant_id(TENANT_ID);
    column.set_table_id(table_id);
    column.set_column_id(key_column_id);
    column.set_data_type(ObIntType);
    column.set_rowkey_position(1);
    column.set_nullable(false);
    if (INDEX_TYPE_IS_NOT != type) {
      schema.set_data_table_id(SOURCE_ID);
      schema.set_index_column_num(1);
      column.set_index_position(1);
    }
    ASSERT_EQ(OB_SUCCESS, column.set_column_name("id"));
    ASSERT_EQ(OB_SUCCESS, schema.add_column(column));
    ASSERT_TRUE(schema.is_valid());
  }

  void prepare_drop_last_index()
  {
    ObColumnSchemaV2 source_column;
    source_column.set_tenant_id(TENANT_ID);
    source_column.set_table_id(SOURCE_ID);
    source_column.set_column_id(OB_APP_MIN_COLUMN_ID + 1);
    source_column.set_data_type(ObVarcharType);
    source_column.set_data_length(1024);
    source_column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    ASSERT_EQ(OB_SUCCESS, source_column.set_column_name("text_col"));
    ASSERT_EQ(OB_SUCCESS, ctx_.source_.add_column(source_column));
    ctx_.source_.set_max_used_column_id(OB_APP_MIN_COLUMN_ID + 1);
    ASSERT_NO_FATAL_FAILURE(make_schema(ctx_.async_index_, ASYNC_ID, "__idx_500002_async",
        INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, OB_APP_MIN_COLUMN_ID + 1));
    ASSERT_EQ(OB_SUCCESS, ctx_.source_.add_simple_index_info(
        ObAuxTableMetaInfo(ASYNC_ID, USER_INDEX, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL)));
    ctx_.source_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
    ctx_.hidden_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
    ASSERT_EQ(OB_SUCCESS, ctx_.hidden_.set_async_index_params(ObString::make_string("refresh_interval=30s")));
    AlterColumnSchema drop_column;
    drop_column.set_tenant_id(TENANT_ID);
    drop_column.set_table_id(SOURCE_ID);
    drop_column.set_column_id(OB_APP_MIN_COLUMN_ID + 1);
    drop_column.set_data_type(ObVarcharType);
    drop_column.set_data_length(1024);
    drop_column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    drop_column.alter_type_ = OB_DDL_DROP_COLUMN;
    ASSERT_EQ(OB_SUCCESS, drop_column.set_column_name("text_col"));
    ASSERT_EQ(OB_SUCCESS, drop_column.set_origin_column_name(ObString::make_string("text_col")));
    ASSERT_EQ(OB_SUCCESS, arg_.alter_table_schema_.add_alter_column(drop_column, true));
  }

  void assert_committed_membership(const bool has_async_index)
  {
    EXPECT_EQ(1, ctx_.updates_);
    EXPECT_EQ(1, ctx_.membership_writes_);
    EXPECT_EQ(1, ctx_.commits_);
    EXPECT_EQ(0, ctx_.rollbacks_);
    EXPECT_EQ(1, ctx_.publications_);
    EXPECT_TRUE(ctx_.committed_schema_.get_in_offline_ddl_white_list());
    EXPECT_EQ(TABLE_STATE_HIDDEN_OFFLINE_DDL, ctx_.committed_schema_.get_table_state_flag());
    EXPECT_FALSE(ctx_.hidden_.get_in_offline_ddl_white_list());
    EXPECT_EQ(has_async_index, ctx_.committed_schema_.has_async_index());
    EXPECT_EQ(has_async_index, 0 != (ctx_.committed_schema_.get_table_flags() & HAS_ASYNC_INDEX_FLAG));
    EXPECT_EQ(has_async_index, ctx_.committed_membership_.has_async_index());
    EXPECT_EQ(has_async_index, !ctx_.committed_schema_.get_async_index_params().empty());
  }

  int clear_drop_options()
  {
    int ret = OB_SUCCESS;
    auto *old_schema_service = GCTX.schema_service_;
    auto *old_proxy = GCTX.sql_proxy_;
    GCTX.schema_service_ = &schema_service_;
    GCTX.sql_proxy_ = &proxy_;
    DEFER(GCTX.schema_service_ = old_schema_service; GCTX.sql_proxy_ = old_proxy);
    HEAP_VAR(ObRootService, root_service) {
      ret = ObAsyncIndexDDLUtil::clear_async_index_table_options(
          &root_service, TENANT_ID, HIDDEN_ID, 1, ObString());
    }
    return ret;
  }

  int check_can_clear_drop_options(bool &can_clear)
  {
    int ret = OB_SUCCESS;
    auto *old_schema_service = GCTX.schema_service_;
    GCTX.schema_service_ = &schema_service_;
    DEFER(GCTX.schema_service_ = old_schema_service);
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
        TENANT_ID, schema_guard))) {
    } else {
      ret = ObAsyncIndexDDLUtil::check_can_clear_async_index_options(
          TENANT_ID, HIDDEN_ID, schema_guard, can_clear);
    }
    return ret;
  }

  int clear_async_vector_drop_options()
  {
    int ret = OB_SUCCESS;
    auto *old_schema_service = GCTX.schema_service_;
    auto *old_proxy = GCTX.sql_proxy_;
    GCTX.schema_service_ = &schema_service_;
    GCTX.sql_proxy_ = &proxy_;
    DEFER(GCTX.schema_service_ = old_schema_service; GCTX.sql_proxy_ = old_proxy);
    bool has_user_async_index = false;
    HEAP_VAR(ObRootService, root_service) {
      ret = ObAsyncVectorIndexTaskUtil::clear_async_index_table_options(
          TENANT_ID, HIDDEN_ID, ObString(), &root_service, has_user_async_index);
    }
    return ret;
  }

  RebuildContext ctx_;
  ObSchemaServiceSQLImpl sql_schema_service_;
  ObMultiVersionSchemaService schema_service_;
  ObMySQLProxy proxy_;
  ObDDLService ddl_service_;
  obrpc::ObAlterTableArg arg_;
  ObSArray<uint64_t> index_ids_;
};

TEST_F(TestHiddenTableIndexRebuild, dropping_last_logical_index_uses_reconstructed_index_set)
{
  ASSERT_NO_FATAL_FAILURE(prepare_drop_last_index());
  ASSERT_EQ(OB_SUCCESS, ddl_service_.rebuild_hidden_table_index_in_trans(arg_, index_ids_));
  assert_committed_membership(false);
  EXPECT_TRUE(ctx_.hidden_.has_async_index());
  EXPECT_EQ(ObString::make_string("refresh_interval=30s"), ctx_.hidden_.get_async_index_params());
  EXPECT_TRUE(index_ids_.empty());
  EXPECT_TRUE(ctx_.created_indexes_.empty());
  EXPECT_EQ(0, ctx_.reconstructed_indexes_);
}

TEST_F(TestHiddenTableIndexRebuild, adding_first_logical_index_preserves_generated_schema)
{
  obrpc::ObCreateIndexArg create_index;
  create_index.index_action_type_ = obrpc::ObIndexArg::ADD_INDEX;
  create_index.index_type_ = INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL;
  create_index.index_name_ = ObString::make_string("new_async");
  ASSERT_NO_FATAL_FAILURE(make_schema(create_index.index_schema_, 600000,
      "__idx_500002_new_async", INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL));
  ASSERT_EQ(OB_SUCCESS, arg_.index_arg_list_.push_back(&create_index));
  ASSERT_EQ(OB_SUCCESS, ddl_service_.rebuild_hidden_table_index_in_trans(arg_, index_ids_));
  assert_committed_membership(true);
  ASSERT_EQ(1, ctx_.created_indexes_.count());
  EXPECT_EQ(INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL, ctx_.created_indexes_.at(0).get_index_type());
  EXPECT_EQ(INDEX_STATUS_UNAVAILABLE, ctx_.created_indexes_.at(0).get_index_status());
  EXPECT_EQ(1, index_ids_.count());
  EXPECT_EQ(1, ctx_.generated_indexes_);
  EXPECT_EQ(GENERATED_COLUMN_ID, ctx_.committed_schema_.get_max_used_column_id());
  EXPECT_NE(nullptr, ctx_.committed_schema_.get_column_schema(GENERATED_COLUMN_ID));
  EXPECT_EQ(nullptr, ctx_.hidden_.get_column_schema(GENERATED_COLUMN_ID));
}

TEST_F(TestHiddenTableIndexRebuild, rebuilt_indexes_reconcile_without_inherited_whitelist)
{
  ASSERT_NO_FATAL_FAILURE(prepare_drop_last_index());
  ASSERT_NO_FATAL_FAILURE(make_schema(ctx_.normal_index_, NORMAL_ID, "__idx_500003_normal",
      INDEX_TYPE_NORMAL_LOCAL));
  ctx_.normal_index_.set_data_table_id(HIDDEN_ID);
  ASSERT_EQ(OB_SUCCESS, ctx_.hidden_.add_simple_index_info(
      ObAuxTableMetaInfo(NORMAL_ID, USER_INDEX, INDEX_TYPE_NORMAL_LOCAL)));
  ASSERT_EQ(OB_SUCCESS, ddl_service_.rebuild_hidden_table_index_in_trans(arg_, index_ids_));
  assert_committed_membership(false);
  ASSERT_EQ(1, index_ids_.count());
  EXPECT_EQ(NORMAL_ID, index_ids_.at(0));
  EXPECT_TRUE(ctx_.created_indexes_.empty());
  EXPECT_EQ(0, ctx_.reconstructed_indexes_);
  EXPECT_EQ(0, ctx_.generated_indexes_);
}

TEST_F(TestHiddenTableIndexRebuild, rebuilt_async_index_preserves_membership)
{
  const ObString params = ObString::make_string("refresh_interval=30s");
  ASSERT_NO_FATAL_FAILURE(make_schema(ctx_.async_index_, ASYNC_ID, "__idx_500003_async",
      INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL));
  ctx_.async_index_.set_data_table_id(HIDDEN_ID);
  ASSERT_EQ(OB_SUCCESS, ctx_.hidden_.add_simple_index_info(
      ObAuxTableMetaInfo(ASYNC_ID, USER_INDEX, INDEX_TYPE_ASYNC_FTS_INDEX_LOCAL)));
  ctx_.hidden_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  ASSERT_EQ(OB_SUCCESS, ctx_.hidden_.set_async_index_params(params));

  ASSERT_EQ(OB_SUCCESS, ddl_service_.rebuild_hidden_table_index_in_trans(arg_, index_ids_));
  ASSERT_EQ(1, index_ids_.count());
  EXPECT_EQ(ASYNC_ID, index_ids_.at(0));
  EXPECT_TRUE(ctx_.created_indexes_.empty());
  EXPECT_EQ(0, ctx_.reconstructed_indexes_);
  EXPECT_EQ(0, ctx_.generated_indexes_);
  EXPECT_EQ(0, ctx_.updates_);
  EXPECT_EQ(0, ctx_.membership_writes_);
  EXPECT_TRUE(ctx_.hidden_.has_async_index());
  EXPECT_EQ(params, ctx_.hidden_.get_async_index_params());
}

TEST_F(TestHiddenTableIndexRebuild, reconstructed_schema_survives_add_new_and_reconciliation)
{
  ASSERT_NO_FATAL_FAILURE(prepare_drop_last_index());
  ASSERT_NO_FATAL_FAILURE(make_schema(ctx_.normal_index_, NORMAL_ID, "__idx_500002_normal",
      INDEX_TYPE_NORMAL_LOCAL));
  ASSERT_EQ(OB_SUCCESS, ctx_.source_.add_simple_index_info(
      ObAuxTableMetaInfo(NORMAL_ID, USER_INDEX, INDEX_TYPE_NORMAL_LOCAL)));
  ASSERT_EQ(OB_SUCCESS, ddl_service_.rebuild_hidden_table_index_in_trans(arg_, index_ids_));
  assert_committed_membership(false);
  ASSERT_EQ(1, ctx_.created_indexes_.count());
  EXPECT_EQ(INDEX_TYPE_NORMAL_LOCAL, ctx_.created_indexes_.at(0).get_index_type());
  EXPECT_EQ(1, ctx_.reconstructed_indexes_);
  EXPECT_EQ(0, ctx_.generated_indexes_);
  EXPECT_EQ(GENERATED_COLUMN_ID, ctx_.committed_schema_.get_max_used_column_id());
  EXPECT_NE(nullptr, ctx_.committed_schema_.get_column_schema(GENERATED_COLUMN_ID));
}

TEST_F(TestHiddenTableIndexRebuild, membership_failure_rolls_back_caller_transaction)
{
  ASSERT_NO_FATAL_FAILURE(prepare_drop_last_index());
  ctx_.membership_ret_ = OB_TRANS_KILLED;
  EXPECT_EQ(OB_TRANS_KILLED, ddl_service_.rebuild_hidden_table_index_in_trans(arg_, index_ids_));
  EXPECT_EQ(1, ctx_.updates_);
  EXPECT_EQ(1, ctx_.membership_writes_);
  EXPECT_EQ(0, ctx_.commits_);
  EXPECT_EQ(1, ctx_.rollbacks_);
  EXPECT_EQ(0, ctx_.publications_);
  EXPECT_EQ(OB_INVALID_ID, ctx_.committed_schema_.get_table_id());
}

TEST_F(TestHiddenTableIndexRebuild, async_drop_preserves_hidden_offline_table_options)
{
  ctx_.hidden_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  ASSERT_EQ(OB_SUCCESS, ctx_.hidden_.set_async_index_params(ObString::make_string("refresh_interval=30s")));
  bool can_clear = true;
  ASSERT_EQ(OB_SUCCESS, check_can_clear_drop_options(can_clear));
  EXPECT_FALSE(can_clear);
  EXPECT_EQ(0, ctx_.updates_);
  EXPECT_EQ(0, ctx_.membership_writes_);
  EXPECT_EQ(0, ctx_.commits_);
  EXPECT_TRUE(ctx_.hidden_.has_async_index());
  EXPECT_EQ(ObString::make_string("refresh_interval=30s"), ctx_.hidden_.get_async_index_params());
}

TEST_F(TestHiddenTableIndexRebuild, shared_drop_cleanup_rejects_hidden_offline_table)
{
  ctx_.hidden_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  EXPECT_EQ(OB_OP_NOT_ALLOW, clear_drop_options());
  EXPECT_EQ(0, ctx_.membership_writes_);
  EXPECT_EQ(0, ctx_.commits_);
  EXPECT_EQ(1, ctx_.rollbacks_);
}

TEST_F(TestHiddenTableIndexRebuild, shared_drop_cleanup_rejects_offline_original_table)
{
  ctx_.hidden_.set_table_state_flag(TABLE_STATE_OFFLINE_DDL);
  ctx_.hidden_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  EXPECT_EQ(OB_OP_NOT_ALLOW, clear_drop_options());
  EXPECT_EQ(0, ctx_.membership_writes_);
  EXPECT_EQ(0, ctx_.commits_);
  EXPECT_EQ(1, ctx_.rollbacks_);
}

TEST_F(TestHiddenTableIndexRebuild, async_vector_drop_skips_hidden_offline_table_options)
{
  ctx_.hidden_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  const ObString params = ObString::make_string("refresh_interval=30s");
  ASSERT_EQ(OB_SUCCESS, ctx_.hidden_.set_async_index_params(params));
  ASSERT_EQ(OB_OP_NOT_ALLOW, ObTableSqlService::check_ddl_allowed(ctx_.hidden_));

  ASSERT_EQ(OB_SUCCESS, clear_async_vector_drop_options());
  EXPECT_TRUE(ctx_.hidden_.has_async_index());
  EXPECT_EQ(params, ctx_.hidden_.get_async_index_params());
  EXPECT_EQ(0, ctx_.updates_);
  EXPECT_EQ(0, ctx_.membership_writes_);
  EXPECT_EQ(0, ctx_.commits_);
  EXPECT_EQ(0, ctx_.rollbacks_);
  EXPECT_EQ(0, ctx_.publications_);
}

TEST_F(TestHiddenTableIndexRebuild, async_vector_drop_clears_normal_table_options)
{
  ctx_.hidden_.set_table_state_flag(TABLE_STATE_NORMAL);
  ctx_.hidden_.add_table_flag(HAS_ASYNC_INDEX_FLAG);
  ASSERT_EQ(OB_SUCCESS, ctx_.hidden_.set_async_index_params(ObString::make_string("refresh_interval=30s")));

  ASSERT_EQ(OB_SUCCESS, clear_async_vector_drop_options());
  EXPECT_FALSE(ctx_.committed_schema_.has_async_index());
  EXPECT_TRUE(ctx_.committed_schema_.get_async_index_params().empty());
  EXPECT_FALSE(ctx_.committed_membership_.has_async_index());
  EXPECT_EQ(1, ctx_.updates_);
  EXPECT_EQ(1, ctx_.membership_writes_);
  EXPECT_EQ(1, ctx_.commits_);
  EXPECT_EQ(0, ctx_.rollbacks_);
  EXPECT_EQ(1, ctx_.publications_);
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  OB_LOGGER.set_file_name("test_hidden_table_index_rebuild.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
