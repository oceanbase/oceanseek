/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_OPT

#include <cstring>
#include <gtest/gtest.h>
#include "lib/alloc/ob_malloc_allocator.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"
#include "observer/table_load/stream/ob_direct_load_stream_chunk.h"
#include "observer/table_load/stream/ob_direct_load_stream_service.h"
#include "observer/table_load/stream/ob_mock_direct_load_stream_meta_store.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_server_struct.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/px/ob_external_table_granule_handler.h"
#include "sql/engine/table/ob_table_scan_op.h"
#include "sql/engine/table/ob_direct_load_stream_table_row_iter.h"
#include "sql/engine/table/ob_direct_load_stream_scan_task.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"
#include "sql/optimizer/test_optimizer_utils.h"
#include "sql/test_sql_utils.h"
#include "../../observer/table_load/test_direct_load_stream_utils.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

namespace test
{
namespace
{

const int64_t TEST_BATCH_SIZE = 8;
const int64_t TEST_RES_BUF_LEN = 4096;
const uint64_t TEST_SOURCE_TENANT_ID = 1002;

class MockDirectLoadStreamSourceIteratorMtlEnv
{
public:
  MockDirectLoadStreamSourceIteratorMtlEnv()
    : tenant_base_(TEST_SOURCE_TENANT_ID),
      table_load_service_(TEST_SOURCE_TENANT_ID),
      mock_meta_store_(TEST_SOURCE_TENANT_ID),
      old_tenant_(nullptr),
      old_self_addr_(),
      self_addr_(),
      created_allocator_(false),
      next_stream_id_(1000000000),
      is_inited_(false)
  {
  }
  ~MockDirectLoadStreamSourceIteratorMtlEnv()
  {
    destroy();
  }
  int init()
  {
    int ret = OB_SUCCESS;
    bool allocator_existed = false;
    int alloc_ret = OB_SUCCESS;
    if (OB_UNLIKELY(is_inited_)) {
      ret = OB_INIT_TWICE;
    } else {
      allocator_existed = lib::ObMallocAllocator::get_instance()->is_tenant_allocator_exist(
          TEST_SOURCE_TENANT_ID);
      alloc_ret = lib::ObMallocAllocator::get_instance()->create_and_add_tenant_allocator(
          TEST_SOURCE_TENANT_ID);
      if (OB_UNLIKELY(OB_SUCCESS != alloc_ret && OB_ENTRY_EXIST != alloc_ret)) {
        ret = alloc_ret;
      } else if (OB_FAIL(lib::ObMallocAllocator::get_instance()->set_tenant_limit(
                     TEST_SOURCE_TENANT_ID, 1LL << 30))) {
        LOG_WARN("fail to set test tenant memory limit", KR(ret), K(TEST_SOURCE_TENANT_ID));
      } else if (OB_ISNULL(lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
                           TEST_SOURCE_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant ctx allocator is null", KR(ret), K(TEST_SOURCE_TENANT_ID));
      } else if (!self_addr_.set_ip_addr("1.1.1.1", 8888)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to set test self address", KR(ret));
      } else {
        created_allocator_ = !allocator_existed && OB_SUCCESS == alloc_ret;
        old_self_addr_ = GCTX.self_addr();
        GCTX.self_addr_seq_.set_addr(self_addr_);
        old_tenant_ = share::ObTenantEnv::get_tenant();
        tenant_base_.set<ObTableLoadService *>(&table_load_service_);
        share::ObTenantEnv::set_tenant(&tenant_base_);
        ObDirectLoadStreamService &stream_service =
            table_load_service_.get_direct_load_stream_service();
        if (OB_FAIL(mock_meta_store_.init())) {
          LOG_WARN("fail to init source iterator mock stream meta store", KR(ret));
        } else if (OB_FAIL(stream_service.set_meta_store_for_test(mock_meta_store_))) {
          LOG_WARN("fail to inject source iterator mock stream meta store", KR(ret));
        } else if (OB_FAIL(stream_service.enable_local_stream_id_generator_for_test())) {
          LOG_WARN("fail to enable source iterator test stream id generator", KR(ret));
        } else if (OB_FAIL(stream_service.enable_injected_queue_topology_for_test())) {
          LOG_WARN("fail to enable source iterator test stream topology injection", KR(ret));
        } else if (OB_FAIL(stream_service.get_buffer_queue_manager()
                               .set_mock_queue_store_factory_for_test(
                                   get_direct_load_stream_mock_store_factory()))) {
          LOG_WARN("fail to inject source iterator mock queue store factory", KR(ret));
        } else if (OB_FAIL(table_load_service_.init())) {
          LOG_WARN("fail to init source iterator table load service", KR(ret));
        }
        if (OB_FAIL(ret)) {
          table_load_service_.destroy();
          mock_meta_store_.reset();
          share::ObTenantEnv::set_tenant(old_tenant_);
          GCTX.self_addr_seq_.set_addr(old_self_addr_);
          old_tenant_ = nullptr;
        } else {
          is_inited_ = true;
        }
      }
    }
    if (OB_FAIL(ret) && created_allocator_) {
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_SOURCE_TENANT_ID);
      created_allocator_ = false;
    }
    return ret;
  }
  int create_stream_meta(const ObDirectLoadStreamCreateParam &param, int64_t &stream_id)
  {
    int ret = OB_SUCCESS;
    ObDirectLoadStreamMetaCreateOutcome create_outcome =
        DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX;
    stream_id = OB_INVALID_ID;
    if (OB_UNLIKELY(!is_inited_)) {
      ret = OB_NOT_INIT;
    } else {
      stream_id = ++next_stream_id_;
      if (OB_FAIL(mock_meta_store_.create_stream(stream_id, param, create_outcome))) {
        LOG_WARN("fail to create source iterator test stream metadata", KR(ret),
                 K(stream_id), K(param));
      } else if (OB_UNLIKELY(DIRECT_LOAD_STREAM_META_CREATED != create_outcome)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected source iterator test metadata create outcome", KR(ret),
                 K(stream_id), K(create_outcome));
      }
    }
    if (OB_FAIL(ret)) {
      stream_id = OB_INVALID_ID;
    }
    return ret;
  }
  void destroy()
  {
    if (is_inited_) {
      table_load_service_.destroy();
      mock_meta_store_.reset();
      share::ObTenantEnv::set_tenant(old_tenant_);
      GCTX.self_addr_seq_.set_addr(old_self_addr_);
      old_tenant_ = nullptr;
      is_inited_ = false;
    }
    if (created_allocator_) {
      lib::ObMallocAllocator::get_instance()->recycle_tenant_allocator(TEST_SOURCE_TENANT_ID);
      created_allocator_ = false;
    }
  }
  ObTableLoadService &get_table_load_service() { return table_load_service_; }

private:
  share::ObTenantBase tenant_base_;
  ObTableLoadService table_load_service_;
  ObMockDirectLoadStreamMetaStore mock_meta_store_;
  share::ObTenantBase *old_tenant_;
  ObAddr old_self_addr_;
  ObAddr self_addr_;
  bool created_allocator_;
  int64_t next_stream_id_;
  bool is_inited_;
};

class TestExprFrame
{
public:
  TestExprFrame()
    : allocator_("DLSSrcIter"),
      frames_(nullptr),
      frame_(nullptr),
      exprs_(),
      frame_size_(0)
  {
  }
  int init(ObEvalCtx &eval_ctx,
           const int64_t col_count,
           const int64_t batch_size,
           const bool batch_result)
  {
    int ret = OB_SUCCESS;
    int64_t pos = 0;
    frame_size_ = col_count * (sizeof(ObDatum) * batch_size
                  + sizeof(ObEvalInfo) + TEST_RES_BUF_LEN * batch_size);
    if (OB_FAIL(exprs_.reserve(col_count))) {
      LOG_WARN("fail to reserve expr array", KR(ret), K(col_count));
    } else if (OB_ISNULL(frames_ = static_cast<char **>(allocator_.alloc(sizeof(char *))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_ISNULL(frame_ = static_cast<char *>(allocator_.alloc(frame_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMSET(frame_, 0, frame_size_);
      frames_[0] = frame_;
      eval_ctx.frames_ = frames_;
      eval_ctx.set_max_batch_size(batch_size);
      eval_ctx.reuse(batch_size);
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        ObExpr *expr = nullptr;
        if (OB_ISNULL(expr = new (allocator_.alloc(sizeof(ObExpr))) ObExpr())) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          expr->frame_idx_ = 0;
          expr->datum_off_ = pos;
          pos += sizeof(ObDatum) * batch_size;
          expr->eval_info_off_ = pos;
          pos += sizeof(ObEvalInfo);
          expr->res_buf_off_ = pos;
          expr->res_buf_len_ = TEST_RES_BUF_LEN;
          pos += TEST_RES_BUF_LEN * batch_size;
          expr->batch_result_ = batch_result;
          expr->batch_idx_mask_ = batch_result ? UINT64_MAX : 0;
          for (int64_t j = 0; j < batch_size; ++j) {
            ObDatum &datum = expr->locate_expr_datum(eval_ctx, j);
            datum.set_null();
            datum.ptr_ = eval_ctx.frames_[0] + expr->res_buf_off_ + TEST_RES_BUF_LEN * j;
          }
          expr->get_eval_info(eval_ctx).set_evaluated(true);
          expr->get_eval_info(eval_ctx).set_projected(true);
          if (OB_FAIL(exprs_.push_back(expr))) {
            LOG_WARN("fail to push expr", KR(ret));
          }
        }
      }
    }
    return ret;
  }
  ObIArray<ObExpr *> &exprs() { return exprs_; }
  void set_int(ObEvalCtx &eval_ctx, const int64_t row_idx, const int64_t col_idx, const int64_t value)
  {
    ObDatum &datum = exprs_.at(col_idx)->locate_expr_datum(eval_ctx, row_idx);
    datum.set_int(value);
    exprs_.at(col_idx)->get_eval_info(eval_ctx).set_evaluated(true);
    exprs_.at(col_idx)->get_eval_info(eval_ctx).set_projected(true);
  }
  void set_string(ObEvalCtx &eval_ctx, const int64_t row_idx, const int64_t col_idx, const char *value)
  {
    ObDatum &datum = exprs_.at(col_idx)->locate_expr_datum(eval_ctx, row_idx);
    datum.set_string(value, static_cast<uint32_t>(strlen(value)));
    exprs_.at(col_idx)->get_eval_info(eval_ctx).set_evaluated(true);
    exprs_.at(col_idx)->get_eval_info(eval_ctx).set_projected(true);
  }

private:
  ObArenaAllocator allocator_;
  char **frames_;
  char *frame_;
  ObSEArray<ObExpr *, 4> exprs_;
  int64_t frame_size_;
};

} // namespace

class TestDirectLoadStreamSourceIterator : public TestOptimizerUtils
{
public:
  TestDirectLoadStreamSourceIterator() = default;
  virtual ~TestDirectLoadStreamSourceIterator() = default;
  virtual void SetUp()
  {
    schema_file_path_[0] = 0;
    init();
    prepare_user_tenant();
    ASSERT_EQ(OB_SUCCESS, mtl_env_.init());
    prepare_database();
  }
  virtual void TearDown()
  {
    mtl_env_.destroy();
    EXPECT_EQ(OB_SUCCESS, omt::ObTenantTimezoneMgr::get_instance().del_tenant_timezone(
                              TEST_SOURCE_TENANT_ID));
    TestSqlUtils::destroy();
  }

private:
  void prepare_user_tenant()
  {
    ObTenantSchema tenant_schema;
    ObSysVariableSchema sys_variable;
    ObUserInfo login_user;
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::MYSQL;
    int compat_init_ret = OB_SUCCESS;
    tenant_schema.set_tenant_id(TEST_SOURCE_TENANT_ID);
    tenant_schema.set_schema_version(schema_version_);
    tenant_schema.set_locality("auto_locality_strategy");
    ASSERT_EQ(OB_SUCCESS, tenant_schema.add_zone("zone"));
    ASSERT_EQ(OB_SUCCESS, tenant_schema.set_tenant_name("dls_tenant"));
    ASSERT_EQ(OB_SUCCESS, schema_service_->add_tenant_schema(tenant_schema, schema_version_++));

    sys_variable.set_tenant_id(TEST_SOURCE_TENANT_ID);
    sys_variable.set_name_case_mode(OB_ORIGIN_AND_INSENSITIVE);
    sys_variable.set_schema_version(schema_version_);
    ASSERT_EQ(OB_SUCCESS,
              schema_service_->add_sys_variable_schema(sys_variable, schema_version_++));

    login_user.set_tenant_id(session_info_.get_login_tenant_id());
    login_user.set_user_id(session_info_.get_priv_user_id());
    login_user.set_schema_version(schema_version_);
    login_user.set_priv_set(session_info_.get_user_priv_set());
    ASSERT_EQ(OB_SUCCESS, login_user.set_user_name(OB_SYS_USER_NAME));
    ASSERT_EQ(OB_SUCCESS, login_user.set_host(OB_SYS_HOST_NAME));
    ASSERT_EQ(OB_SUCCESS, schema_service_->add_user_schema(login_user, schema_version_++));

    ASSERT_EQ(OB_SUCCESS, schema_service_->get_schema_guard(schema_guard_));
    ASSERT_EQ(OB_SUCCESS,
              omt::ObTenantConfigMgr::get_instance().add_tenant_config(
                  TEST_SOURCE_TENANT_ID));
    compat_init_ret = share::ObCompatModeGetter::instance().init(nullptr);
    ASSERT_TRUE(OB_SUCCESS == compat_init_ret || OB_INIT_TWICE == compat_init_ret);
    ASSERT_EQ(OB_SUCCESS, share::ObCompatModeGetter::instance().set_tenant_compat_mode(
                              TEST_SOURCE_TENANT_ID, compat_mode));
    ASSERT_EQ(OB_SUCCESS, omt::ObTenantTimezoneMgr::get_instance().add_tenant_timezone(
                              TEST_SOURCE_TENANT_ID));

    sys_tenant_id_ = TEST_SOURCE_TENANT_ID;
    ASSERT_EQ(OB_SUCCESS, session_info_.switch_tenant(TEST_SOURCE_TENANT_ID));
    reset_query_ctx_schema_guard();
  }

  void prepare_database()
  {
    ObStmt *stmt = nullptr;
    ASSERT_NO_FATAL_FAILURE(do_load_sql(
        ObString::make_string("create database dls_db"),
        stmt,
        JSON_FORMAT,
        OB_SUCCESS));
    stmt_factory_.destory();
    expr_factory_.destory();
    reset_query_ctx_schema_guard();

    uint64_t database_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS, schema_guard_.get_database_id(
        sys_tenant_id_, ObString::make_string("dls_db"), database_id));
    ASSERT_NE(OB_INVALID_ID, database_id);
    ASSERT_EQ(OB_SUCCESS, session_info_.set_default_database(
        ObString::make_string("dls_db"), CS_TYPE_UTF8MB4_GENERAL_CI));
    session_info_.set_database_id(database_id);
  }

  void reset_query_ctx_schema_guard()
  {
    ASSERT_TRUE(nullptr != stmt_factory_.get_query_ctx());
    stmt_factory_.get_query_ctx()->sql_schema_guard_.set_schema_guard(&schema_guard_);
  }

  int get_target_schema(const char *table_name, const ObTableSchema *&table_schema)
  {
    int ret = OB_SUCCESS;
    table_schema = nullptr;
    if (OB_FAIL(schema_guard_.get_table_schema(sys_tenant_id_,
                                               ObString::make_string("dls_db"),
                                               ObString::make_string(table_name),
                                               false,
                                               table_schema))) {
      LOG_WARN("fail to get target table schema", KR(ret), K(table_name));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("target table schema is null", KR(ret), K(table_name));
    }
    return ret;
  }

  int create_bound_stream(const ObTableSchema &table_schema,
                          int64_t &stream_id,
                          const int64_t queue_count = 1)
  {
    int ret = OB_SUCCESS;
    ObDirectLoadStreamCreateParam create_param;
    table::ObDirectLoadStreamQueueEntry queue_entry;
    stream_id = OB_INVALID_ID;
    if (OB_UNLIKELY(!GCTX.self_addr().is_valid() || queue_count <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid test stream topology", KR(ret), K(GCTX.self_addr()), K(queue_count));
    } else {
      queue_entry.server_addr_ = GCTX.self_addr();
      queue_entry.queue_count_ = queue_count;
      create_param.tenant_id_ = session_info_.get_effective_tenant_id();
      create_param.user_id_ = session_info_.get_priv_user_id();
      create_param.table_id_ = table_schema.get_table_id();
      create_param.target_schema_version_ = table_schema.get_schema_version();
      create_param.expire_ts_ = 6001;
      if (OB_FAIL(create_param.queue_entries_.push_back(queue_entry))) {
        LOG_WARN("fail to push direct load stream queue entry", KR(ret), K(queue_entry));
      } else if (OB_FAIL(ObDirectLoadStreamService::create_direct_load_stream(create_param, stream_id))) {
        LOG_WARN("fail to create direct load stream", KR(ret), K(create_param));
      }
    }
    return ret;
  }

  int create_bound_stream_meta(
      const ObTableSchema &table_schema,
      const ObIArray<table::ObDirectLoadStreamQueueEntry> &queue_entries,
      int64_t &stream_id)
  {
    int ret = OB_SUCCESS;
    ObDirectLoadStreamCreateParam create_param;
    stream_id = OB_INVALID_ID;
    if (OB_UNLIKELY(queue_entries.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("empty source iterator test queue topology", KR(ret));
    } else {
      create_param.tenant_id_ = session_info_.get_effective_tenant_id();
      create_param.user_id_ = session_info_.get_priv_user_id();
      create_param.table_id_ = table_schema.get_table_id();
      create_param.target_schema_version_ = table_schema.get_schema_version();
      create_param.expire_ts_ = 6001;
      if (OB_FAIL(create_param.queue_entries_.assign(queue_entries))) {
        LOG_WARN("fail to copy source iterator test queue topology", KR(ret),
                 K(queue_entries.count()));
      } else if (OB_FAIL(mtl_env_.create_stream_meta(create_param, stream_id))) {
        LOG_WARN("fail to create source iterator test stream metadata", KR(ret),
                 K(create_param));
      }
    }
    return ret;
  }

  int get_local_queue(const int64_t stream_id,
                      ObDirectLoadStreamBufferQueueHandle &queue_handle,
                      const uint64_t queue_id = 0)
  {
    return mtl_env_.get_table_load_service()
        .get_direct_load_stream_service()
        .get_buffer_queue_manager()
        .get_queue(ObDirectLoadStreamBufferQueueKey(stream_id, queue_id), queue_handle);
  }

  int publish_s4_chunk(ObDirectLoadStreamBufferQueue &queue,
                       ObIArray<ObExpr *> &exprs,
                       ObEvalCtx &eval_ctx,
                       const int64_t row_count)
  {
    int ret = OB_SUCCESS;
    ObDirectLoadStreamChunkWriter writer;
    ObString chunk;
    ObDirectLoadStreamAppendHandle append_handle;
    if (OB_FAIL(writer.init(TEST_SOURCE_TENANT_ID))) {
      LOG_WARN("fail to init direct load stream chunk writer", KR(ret));
    } else if (OB_FAIL(writer.append_batch(exprs, eval_ctx, 0, row_count))) {
      LOG_WARN("fail to append rows to stream chunk writer", KR(ret), K(row_count));
    } else if (OB_FAIL(writer.build_chunk(chunk))) {
      LOG_WARN("fail to build stream chunk", KR(ret));
    } else if (OB_FAIL(queue.begin_append(append_handle))) {
      LOG_WARN("fail to begin stream queue append", KR(ret));
    } else {
      ret = queue.append_chunk(append_handle, chunk);
      const int end_ret = queue.end_append(append_handle);
      if (OB_SUCC(ret)) {
        ret = end_ret;
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to publish stream chunk", KR(ret), K(end_ret));
      }
    }
    return ret;
  }

  int build_scan_param(const ObTableSchema &table_schema,
                       const int64_t stream_id,
                       const uint64_t queue_id,
                       const ObAddr &server_addr,
                       const int64_t target_schema_version,
                       ObPushdownOperator &pushdown_op,
                       ExprFixedArray &output_exprs,
                       ExprFixedArray &dependent_exprs,
                       ExprFixedArray &file_column_exprs,
                       ObTableParam &table_param,
                       storage::ObTableScanParam &scan_param)
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObColDesc, 4> stored_columns;
    ObSEArray<uint64_t, 4> output_column_ids;
    ObStoragePushdownFlag pd_pushdown_flag;
    ObDirectLoadStreamScanTask *scan_task = nullptr;
    if (OB_FAIL(table_schema.get_column_ids(stored_columns, false))) {
      LOG_WARN("fail to get stored column layout", KR(ret), K(table_schema));
    } else if (OB_UNLIKELY(stored_columns.count() != output_exprs.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("test expr count does not match stored column layout", KR(ret),
               K(stored_columns.count()), K(output_exprs.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < stored_columns.count(); ++i) {
      const uint64_t column_id = stored_columns.at(i).col_id_;
      if (OB_UNLIKELY(OB_INVALID_ID == column_id)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid test column id", KR(ret), K(i), K(stored_columns.at(i)));
      } else if (OB_FAIL(output_column_ids.push_back(column_id))) {
        LOG_WARN("fail to push output column id", KR(ret), K(column_id));
      } else if (OB_FAIL(scan_param.column_ids_.push_back(column_id))) {
        LOG_WARN("fail to build scan param column id", KR(ret), K(column_id));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(table_param.convert(table_schema, output_column_ids, pd_pushdown_flag))) {
      LOG_WARN("fail to convert table param", KR(ret));
    } else {
      scan_param.tenant_id_ = session_info_.get_effective_tenant_id();
      scan_param.op_ = &pushdown_op;
      scan_param.allocator_ = &allocator_;
      scan_param.table_param_ = &table_param;
      scan_param.output_exprs_ = &output_exprs;
      scan_param.ext_column_dependent_exprs_ = &dependent_exprs;
      scan_param.ext_file_column_exprs_ = &file_column_exprs;
      scan_param.external_file_format_.format_type_ =
          ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT;
      scan_param.external_file_format_.direct_load_stream_format_.stream_id_ = stream_id;
      scan_param.schema_version_ = table_schema.get_schema_version();
      if (OB_UNLIKELY(!server_addr.is_valid()
                      || target_schema_version <= 0
                      || OB_INVALID_ID == queue_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid direct load stream scan task input", KR(ret),
                 K(server_addr), K(target_schema_version), K(queue_id));
      } else if (OB_ISNULL(scan_task = OB_NEWx(ObDirectLoadStreamScanTask, (&allocator_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to allocate direct load stream scan task", KR(ret));
      } else {
        scan_task->stream_id_ = stream_id;
        scan_task->queue_id_ = queue_id;
        scan_task->server_addr_ = server_addr;
        scan_task->target_schema_version_ = target_schema_version;
        if (OB_FAIL(scan_param.scan_tasks_.push_back(scan_task))) {
          LOG_WARN("fail to push direct load stream scan task", KR(ret), KPC(scan_task));
        }
      }
    }
    return ret;
  }

  MockDirectLoadStreamSourceIteratorMtlEnv mtl_env_;
};

// 主路径：复用 S4 Writer 的完整存储行 chunk，并在 chunk 回收后验证变长列深拷贝。
TEST_F(TestDirectLoadStreamSourceIterator, source_iterator_drains_s4_writer_chunk)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64), "
                  "c3 int default 17, c4 varchar(64) default 'tail')");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObDirectLoadStreamBufferQueueHandle queue_handle;
  ObDirectLoadStreamBufferQueue *queue = nullptr;
  ObExecContext input_exec_ctx(allocator_);
  ObEvalCtx input_eval_ctx(input_exec_ctx);
  ObExecContext output_exec_ctx(allocator_);
  ObEvalCtx output_eval_ctx(output_exec_ctx);
  ObPushdownExprSpec pushdown_expr_spec(allocator_);
  ObPushdownOperator pushdown_op(output_eval_ctx, pushdown_expr_spec);
  TestExprFrame input_frame;
  TestExprFrame output_frame;
  ExprFixedArray output_exprs(allocator_);
  ExprFixedArray dependent_exprs(allocator_);
  ExprFixedArray file_column_exprs(allocator_);
  ObTableParam table_param(allocator_);
  storage::ObTableScanParam scan_param;
  ObExternalTableAccessService access_service;
  ObNewRowIterator *iter = nullptr;
  int64_t count = 0;

  pushdown_expr_spec.max_batch_size_ = TEST_BATCH_SIZE;
  ASSERT_EQ(OB_SUCCESS, input_frame.init(input_eval_ctx, 4, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output_frame.init(pushdown_op.get_eval_ctx(), 4,
                                          TEST_BATCH_SIZE, true));
  input_frame.set_int(input_eval_ctx, 0, 0, 7);
  input_frame.set_string(input_eval_ctx, 0, 1, "alpha");
  input_frame.set_int(input_eval_ctx, 0, 2, 17);
  input_frame.set_string(input_eval_ctx, 0, 3, "tail");
  input_frame.set_int(input_eval_ctx, 1, 0, 9);
  input_frame.set_string(input_eval_ctx, 1, 1, "beta-value");
  input_frame.set_int(input_eval_ctx, 1, 2, 17);
  input_frame.set_string(input_eval_ctx, 1, 3, "tail");
  input_frame.set_int(input_eval_ctx, 2, 0, 11);
  input_frame.set_string(input_eval_ctx, 2, 1, "gamma");
  input_frame.set_int(input_eval_ctx, 2, 2, 17);
  input_frame.set_string(input_eval_ctx, 2, 3, "tail");

  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_bound_stream(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, queue_handle));
  ASSERT_NE(nullptr, queue = queue_handle.get_queue());
  ASSERT_EQ(OB_SUCCESS, publish_s4_chunk(*queue, input_frame.exprs(), input_eval_ctx, 3));
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamService::seal_direct_load_stream(stream_id));

  ASSERT_EQ(OB_SUCCESS, output_exprs.assign(output_frame.exprs()));
  // 生产计划中的普通 mocked 列不会生成 dependent expr，这里保持为空以覆盖真实初始化合同。
  ASSERT_TRUE(dependent_exprs.empty());
  ASSERT_EQ(OB_SUCCESS, build_scan_param(*table_schema, stream_id, 0,
                                         GCTX.self_addr(), table_schema->get_schema_version(),
                                         pushdown_op,
                                         output_exprs, dependent_exprs, file_column_exprs,
                                         table_param, scan_param));
  // 外表 ctdef 版本不是 Stream 目标表版本，iterator 不应据此拒绝有效任务。
  scan_param.schema_version_ = table_schema->get_schema_version() + 1;
  ASSERT_EQ(OB_SUCCESS, access_service.table_scan(scan_param, iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_EQ(OB_SUCCESS, iter->get_next_rows(count, TEST_BATCH_SIZE));
  ASSERT_EQ(3, count);
  EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, TEST_BATCH_SIZE));
  EXPECT_EQ(OB_SUCCESS, queue->check_reader_drain_completed());

  // 第二次读取已回收 chunk allocator，此后仍可访问的字符串必须来自输出 frame 深拷贝。
  EXPECT_EQ(7, output_exprs.at(0)->locate_expr_datum(pushdown_op.get_eval_ctx(), 0).get_int());
  EXPECT_EQ(9, output_exprs.at(0)->locate_expr_datum(pushdown_op.get_eval_ctx(), 1).get_int());
  EXPECT_EQ(11, output_exprs.at(0)->locate_expr_datum(pushdown_op.get_eval_ctx(), 2).get_int());
  ASSERT_FALSE(output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 0).is_null());
  ASSERT_FALSE(output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 1).is_null());
  ASSERT_FALSE(output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 2).is_null());
  EXPECT_EQ(0, MEMCMP("alpha",
                      output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 0).ptr_,
                      output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 0).len_));
  EXPECT_EQ(0, MEMCMP("beta-value",
                      output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 1).ptr_,
                      output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 1).len_));
  EXPECT_EQ(0, MEMCMP("gamma",
                      output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 2).ptr_,
                      output_exprs.at(1)->locate_expr_datum(pushdown_op.get_eval_ctx(), 2).len_));
  EXPECT_EQ(17, output_exprs.at(2)->locate_expr_datum(
                    pushdown_op.get_eval_ctx(), 0).get_int());
  EXPECT_EQ(17, output_exprs.at(2)->locate_expr_datum(
                    pushdown_op.get_eval_ctx(), 1).get_int());
  EXPECT_EQ(17, output_exprs.at(2)->locate_expr_datum(
                    pushdown_op.get_eval_ctx(), 2).get_int());
  ASSERT_FALSE(output_exprs.at(3)->locate_expr_datum(
      pushdown_op.get_eval_ctx(), 0).is_null());
  EXPECT_EQ(0, MEMCMP("tail",
                      output_exprs.at(3)->locate_expr_datum(
                          pushdown_op.get_eval_ctx(), 0).ptr_,
                      output_exprs.at(3)->locate_expr_datum(
                          pushdown_op.get_eval_ctx(), 0).len_));
  ASSERT_EQ(OB_SUCCESS, access_service.revert_scan_iter(iter));
}

// PX task 边界：同一 owner 的多个 queue 由 GI 依次下发，iterator 只能切换到下一 queue，
// 不能重新消费已经读完的同一 queue。
TEST_F(TestDirectLoadStreamSourceIterator, source_iterator_switches_between_local_queues)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64))");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObDirectLoadStreamBufferQueueHandle first_queue_handle;
  ObDirectLoadStreamBufferQueueHandle second_queue_handle;
  ObDirectLoadStreamBufferQueue *first_queue = nullptr;
  ObDirectLoadStreamBufferQueue *second_queue = nullptr;
  ObExecContext input_exec_ctx(allocator_);
  ObEvalCtx input_eval_ctx(input_exec_ctx);
  ObExecContext output_exec_ctx(allocator_);
  ObEvalCtx output_eval_ctx(output_exec_ctx);
  ObPushdownExprSpec pushdown_expr_spec(allocator_);
  ObPushdownOperator pushdown_op(output_eval_ctx, pushdown_expr_spec);
  TestExprFrame input_frame;
  TestExprFrame output_frame;
  ExprFixedArray output_exprs(allocator_);
  ExprFixedArray dependent_exprs(allocator_);
  ExprFixedArray file_column_exprs(allocator_);
  ObTableParam first_table_param(allocator_);
  ObTableParam second_table_param(allocator_);
  storage::ObTableScanParam first_scan_param;
  storage::ObTableScanParam second_scan_param;
  ObExternalTableAccessService access_service;
  ObNewRowIterator *iter = nullptr;
  int64_t count = 0;

  pushdown_expr_spec.max_batch_size_ = TEST_BATCH_SIZE;
  ASSERT_EQ(OB_SUCCESS, input_frame.init(input_eval_ctx, 2, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output_frame.init(pushdown_op.get_eval_ctx(), 2,
                                          TEST_BATCH_SIZE, true));
  input_frame.set_int(input_eval_ctx, 0, 0, 7);
  input_frame.set_string(input_eval_ctx, 0, 1, "first");
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_bound_stream(*table_schema, stream_id, 2));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, first_queue_handle, 0));
  ASSERT_EQ(OB_SUCCESS, get_local_queue(stream_id, second_queue_handle, 1));
  ASSERT_NE(nullptr, first_queue = first_queue_handle.get_queue());
  ASSERT_NE(nullptr, second_queue = second_queue_handle.get_queue());
  ASSERT_EQ(OB_SUCCESS, publish_s4_chunk(*first_queue, input_frame.exprs(), input_eval_ctx, 1));
  input_frame.set_int(input_eval_ctx, 0, 0, 9);
  input_frame.set_string(input_eval_ctx, 0, 1, "second");
  ASSERT_EQ(OB_SUCCESS, publish_s4_chunk(*second_queue, input_frame.exprs(), input_eval_ctx, 1));
  ASSERT_EQ(OB_SUCCESS, ObDirectLoadStreamService::seal_direct_load_stream(stream_id));

  ASSERT_EQ(OB_SUCCESS, output_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, build_scan_param(*table_schema, stream_id, 0,
                                         GCTX.self_addr(), table_schema->get_schema_version(),
                                         pushdown_op, output_exprs, dependent_exprs,
                                         file_column_exprs, first_table_param, first_scan_param));
  ASSERT_EQ(OB_SUCCESS, build_scan_param(*table_schema, stream_id, 1,
                                         GCTX.self_addr(), table_schema->get_schema_version(),
                                         pushdown_op, output_exprs, dependent_exprs,
                                         file_column_exprs, second_table_param, second_scan_param));
  ASSERT_EQ(OB_SUCCESS, access_service.table_scan(first_scan_param, iter));
  ASSERT_NE(nullptr, iter);
  ASSERT_EQ(OB_SUCCESS, iter->get_next_rows(count, TEST_BATCH_SIZE));
  ASSERT_EQ(1, count);
  EXPECT_EQ(7, output_exprs.at(0)->locate_expr_datum(
                   pushdown_op.get_eval_ctx(), 0).get_int());
  EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, TEST_BATCH_SIZE));
  EXPECT_EQ(OB_SUCCESS, first_queue->check_reader_drain_completed());

  ASSERT_EQ(OB_SUCCESS, access_service.reuse_scan_iter(false, iter));
  ASSERT_EQ(OB_SUCCESS, access_service.table_rescan(second_scan_param, iter));
  ASSERT_EQ(OB_SUCCESS, iter->get_next_rows(count, TEST_BATCH_SIZE));
  ASSERT_EQ(1, count);
  EXPECT_EQ(9, output_exprs.at(0)->locate_expr_datum(
                   pushdown_op.get_eval_ctx(), 0).get_int());
  EXPECT_EQ(OB_ITER_END, iter->get_next_rows(count, TEST_BATCH_SIZE));
  EXPECT_EQ(OB_SUCCESS, second_queue->check_reader_drain_completed());

  ASSERT_EQ(OB_SUCCESS, access_service.reuse_scan_iter(false, iter));
  EXPECT_EQ(OB_NOT_SUPPORTED, access_service.table_rescan(second_scan_param, iter));
  ASSERT_EQ(OB_SUCCESS, access_service.revert_scan_iter(iter));
}

// 队列边界：metadata 存在但本地 queue 已清理时，iterator 初始化返回队列不存在。
TEST_F(TestDirectLoadStreamSourceIterator, source_iterator_missing_queue_is_error)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64))");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObExecContext output_exec_ctx(allocator_);
  ObEvalCtx output_eval_ctx(output_exec_ctx);
  ObPushdownExprSpec pushdown_expr_spec(allocator_);
  ObPushdownOperator pushdown_op(output_eval_ctx, pushdown_expr_spec);
  TestExprFrame output_frame;
  ExprFixedArray output_exprs(allocator_);
  ExprFixedArray dependent_exprs(allocator_);
  ExprFixedArray file_column_exprs(allocator_);
  ObTableParam table_param(allocator_);
  storage::ObTableScanParam scan_param;
  ObDirectLoadStreamTableRowIterator iter;

  pushdown_expr_spec.max_batch_size_ = TEST_BATCH_SIZE;
  ASSERT_EQ(OB_SUCCESS, output_frame.init(pushdown_op.get_eval_ctx(), 2, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, dependent_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_bound_stream(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, mtl_env_.get_table_load_service()
      .get_direct_load_stream_service()
      .get_buffer_queue_manager()
      .cleanup_queue(ObDirectLoadStreamBufferQueueKey(stream_id, 0)));
  ASSERT_EQ(OB_SUCCESS, build_scan_param(*table_schema, stream_id, 0,
                                         GCTX.self_addr(), table_schema->get_schema_version(),
                                         pushdown_op,
                                         output_exprs, dependent_exprs, file_column_exprs,
                                         table_param, scan_param));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, iter.init(&scan_param));
}

// 任务边界：Stream SOURCE 必须携带 prepare 阶段生成的专用 scan task。
TEST_F(TestDirectLoadStreamSourceIterator, source_iterator_requires_stream_scan_task)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64))");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObExecContext output_exec_ctx(allocator_);
  ObEvalCtx output_eval_ctx(output_exec_ctx);
  ObPushdownExprSpec pushdown_expr_spec(allocator_);
  ObPushdownOperator pushdown_op(output_eval_ctx, pushdown_expr_spec);
  TestExprFrame output_frame;
  ExprFixedArray output_exprs(allocator_);
  ExprFixedArray dependent_exprs(allocator_);
  ExprFixedArray file_column_exprs(allocator_);
  ObTableParam table_param(allocator_);
  storage::ObTableScanParam scan_param;
  ObDirectLoadStreamTableRowIterator iter;

  pushdown_expr_spec.max_batch_size_ = TEST_BATCH_SIZE;
  ASSERT_EQ(OB_SUCCESS, output_frame.init(pushdown_op.get_eval_ctx(), 2, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, dependent_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_bound_stream(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, build_scan_param(*table_schema, stream_id, 0,
                                         GCTX.self_addr(), table_schema->get_schema_version(),
                                         pushdown_op,
                                         output_exprs, dependent_exprs, file_column_exprs,
                                         table_param, scan_param));
  scan_param.scan_tasks_.reuse();
  EXPECT_EQ(OB_INVALID_ARGUMENT, iter.init(&scan_param));
}

// 路由边界：P0 只消费本机 queue，scan task 指向其他 server 时必须拒绝。
TEST_F(TestDirectLoadStreamSourceIterator, source_iterator_rejects_server_mismatch_task)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64))");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObAddr other_addr;
  ObExecContext output_exec_ctx(allocator_);
  ObEvalCtx output_eval_ctx(output_exec_ctx);
  ObPushdownExprSpec pushdown_expr_spec(allocator_);
  ObPushdownOperator pushdown_op(output_eval_ctx, pushdown_expr_spec);
  TestExprFrame output_frame;
  ExprFixedArray output_exprs(allocator_);
  ExprFixedArray dependent_exprs(allocator_);
  ExprFixedArray file_column_exprs(allocator_);
  ObTableParam table_param(allocator_);
  storage::ObTableScanParam scan_param;
  ObDirectLoadStreamTableRowIterator iter;

  ASSERT_TRUE(other_addr.set_ip_addr("2.2.2.2", 8888));
  pushdown_expr_spec.max_batch_size_ = TEST_BATCH_SIZE;
  ASSERT_EQ(OB_SUCCESS, output_frame.init(pushdown_op.get_eval_ctx(), 2, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, dependent_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_bound_stream(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, build_scan_param(*table_schema, stream_id, 0,
                                         other_addr, table_schema->get_schema_version(),
                                         pushdown_op,
                                         output_exprs, dependent_exprs, file_column_exprs,
                                         table_param, scan_param));
  EXPECT_EQ(OB_ERR_UNEXPECTED, iter.init(&scan_param));
}

// Schema 边界：scan task 携带的目标版本必须与 Stream metadata 及当前目标表一致。
TEST_F(TestDirectLoadStreamSourceIterator, source_iterator_rejects_schema_version_mismatch_task)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64))");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObExecContext output_exec_ctx(allocator_);
  ObEvalCtx output_eval_ctx(output_exec_ctx);
  ObPushdownExprSpec pushdown_expr_spec(allocator_);
  ObPushdownOperator pushdown_op(output_eval_ctx, pushdown_expr_spec);
  TestExprFrame output_frame;
  ExprFixedArray output_exprs(allocator_);
  ExprFixedArray dependent_exprs(allocator_);
  ExprFixedArray file_column_exprs(allocator_);
  ObTableParam table_param(allocator_);
  storage::ObTableScanParam scan_param;
  ObDirectLoadStreamTableRowIterator iter;

  pushdown_expr_spec.max_batch_size_ = TEST_BATCH_SIZE;
  ASSERT_EQ(OB_SUCCESS, output_frame.init(pushdown_op.get_eval_ctx(), 2, TEST_BATCH_SIZE, true));
  ASSERT_EQ(OB_SUCCESS, output_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, dependent_exprs.assign(output_frame.exprs()));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_bound_stream(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  ASSERT_EQ(OB_SUCCESS, build_scan_param(*table_schema, stream_id, 0,
                                         GCTX.self_addr(), table_schema->get_schema_version() + 1,
                                         pushdown_op,
                                         output_exprs, dependent_exprs, file_column_exprs,
                                         table_param, scan_param));
  EXPECT_EQ(OB_SCHEMA_ERROR, iter.init(&scan_param));
}

// 任务生成：目标版本来自 Stream metadata，不能与通用外表 ctdef schema version 混用。
TEST_F(TestDirectLoadStreamSourceIterator, source_scan_task_generated_from_stream_meta)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64))");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObExternalFileFormat format;
  ObString format_str;
  ObDASScanCtDef scan_ctdef(allocator_);
  ObSEArray<ObIExtTblScanTask *, 1> scan_tasks;

  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, create_bound_stream(*table_schema, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  format.format_type_ = ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT;
  format.direct_load_stream_format_.stream_id_ = stream_id;
  ASSERT_EQ(OB_SUCCESS, format.to_string_with_alloc(format_str, allocator_));
  scan_ctdef.ref_table_id_ = table_schema->get_table_id();
  scan_ctdef.schema_version_ = table_schema->get_schema_version() + 1;
  scan_ctdef.is_external_table_ = true;
  ASSERT_EQ(OB_SUCCESS, scan_ctdef.external_file_format_str_.store_str(format_str));

  ASSERT_EQ(OB_SUCCESS, prepare_direct_load_stream_scan_task(
      session_info_.get_effective_tenant_id(),
      scan_ctdef,
      allocator_,
      scan_tasks));
  ASSERT_EQ(1, scan_tasks.count());
  ASSERT_NE(nullptr, scan_tasks.at(0));
  ASSERT_TRUE(scan_tasks.at(0)->is_direct_load_stream_scan_task());
  const ObDirectLoadStreamScanTask *scan_task =
      static_cast<const ObDirectLoadStreamScanTask *>(scan_tasks.at(0));
  EXPECT_EQ(stream_id, scan_task->stream_id_);
  EXPECT_EQ(0, scan_task->queue_id_);
  EXPECT_EQ(GCTX.self_addr(), scan_task->server_addr_);
  EXPECT_EQ(table_schema->get_schema_version(), scan_task->target_schema_version_);
}

// 固定 topology 按 metadata 顺序展开；每个 SQC 只发布本机 queue，queue_id 保持全局前缀和。
TEST_F(TestDirectLoadStreamSourceIterator, source_scan_tasks_expand_multi_owner_topology)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64))");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObAddr remote_addr;
  ObExternalFileFormat format;
  ObString format_str;
  ObDASScanCtDef scan_ctdef(allocator_);
  ObSEArray<table::ObDirectLoadStreamQueueEntry, 2> queue_entries;
  ObSEArray<ObIExtTblScanTask *, 5> scan_tasks;

  ASSERT_TRUE(remote_addr.set_ip_addr("2.2.2.2", 9999));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, queue_entries.push_back(
      table::ObDirectLoadStreamQueueEntry(remote_addr, 3)));
  ASSERT_EQ(OB_SUCCESS, queue_entries.push_back(
      table::ObDirectLoadStreamQueueEntry(GCTX.self_addr(), 2)));
  ASSERT_EQ(OB_SUCCESS, create_bound_stream_meta(*table_schema, queue_entries, stream_id));
  ASSERT_TRUE(is_valid_stream_id(stream_id));
  format.format_type_ = ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT;
  format.direct_load_stream_format_.stream_id_ = stream_id;
  ASSERT_EQ(OB_SUCCESS, format.to_string_with_alloc(format_str, allocator_));
  scan_ctdef.ref_table_id_ = table_schema->get_table_id();
  scan_ctdef.schema_version_ = table_schema->get_schema_version() + 1;
  scan_ctdef.is_external_table_ = true;
  ASSERT_EQ(OB_SUCCESS, scan_ctdef.external_file_format_str_.store_str(format_str));

  ASSERT_EQ(OB_SUCCESS, prepare_direct_load_stream_scan_task(
      session_info_.get_effective_tenant_id(), scan_ctdef, allocator_, scan_tasks));
  ASSERT_EQ(2, scan_tasks.count());
  for (int64_t i = 0; i < scan_tasks.count(); ++i) {
    ASSERT_NE(nullptr, scan_tasks.at(i));
    ASSERT_TRUE(scan_tasks.at(i)->is_direct_load_stream_scan_task());
    const ObDirectLoadStreamScanTask *scan_task =
        static_cast<const ObDirectLoadStreamScanTask *>(scan_tasks.at(i));
    EXPECT_EQ(stream_id, scan_task->stream_id_);
    EXPECT_EQ(static_cast<uint64_t>(i + 3), scan_task->queue_id_);
    EXPECT_EQ(GCTX.self_addr(), scan_task->server_addr_);
    EXPECT_EQ(table_schema->get_schema_version(), scan_task->target_schema_version_);
  }
}

// 基线迁移后走实际 handler 入口；空文件/范围仍按本机 queue 生成任务，不随并行度拆分。
TEST_F(TestDirectLoadStreamSourceIterator, source_granule_handler_preserves_local_queue_topology)
{
  do_create_table("create table dls_target(c1 int primary key, c2 varchar(64))");
  reset_query_ctx_schema_guard();
  const ObTableSchema *table_schema = nullptr;
  int64_t stream_id = OB_INVALID_ID;
  ObAddr remote_addr;
  ObExternalFileFormat format;
  ObString format_str;
  ObExecContext exec_ctx(allocator_);
  ObTableScanSpec tsc(allocator_, PHY_TABLE_SCAN);
  ObDASScanCtDef &scan_ctdef = tsc.tsc_ctdef_.scan_ctdef_;
  ObGranulePumpArgs args;
  ObDASTabletLoc tablet;
  ObSEArray<ObDASTabletLoc *, 1> tablets;
  ObSEArray<ObNewRange, 1> ranges;
  ObSEArray<table::ObDirectLoadStreamQueueEntry, 2> queue_entries;
  ObIExternalTableGranuleHandler *handler = nullptr;

  ASSERT_TRUE(remote_addr.set_ip_addr("2.2.2.2", 9999));
  ASSERT_EQ(OB_SUCCESS, get_target_schema("dls_target", table_schema));
  ASSERT_NE(nullptr, table_schema);
  ASSERT_EQ(OB_SUCCESS, queue_entries.push_back(
      table::ObDirectLoadStreamQueueEntry(remote_addr, 3)));
  ASSERT_EQ(OB_SUCCESS, queue_entries.push_back(
      table::ObDirectLoadStreamQueueEntry(GCTX.self_addr(), 2)));
  ASSERT_EQ(OB_SUCCESS, create_bound_stream_meta(*table_schema, queue_entries, stream_id));
  format.format_type_ = ObExternalFileFormat::DIRECT_LOAD_STREAM_FORMAT;
  format.direct_load_stream_format_.stream_id_ = stream_id;
  ASSERT_EQ(OB_SUCCESS, format.to_string_with_alloc(format_str, allocator_));
  scan_ctdef.ref_table_id_ = table_schema->get_table_id();
  scan_ctdef.schema_version_ = table_schema->get_schema_version() + 1;
  scan_ctdef.is_external_table_ = true;
  ASSERT_EQ(OB_SUCCESS, scan_ctdef.external_file_format_str_.store_str(format_str));
  exec_ctx.set_my_session(&session_info_);
  args.ctx_ = &exec_ctx;
  ASSERT_EQ(OB_SUCCESS, tablets.push_back(&tablet));
  ASSERT_EQ(OB_SUCCESS, ObExternalTableGranuleHandlerFactory::create(
      allocator_, scan_ctdef, handler));
  ASSERT_NE(nullptr, handler);

  // 小于和大于 queue 数的并行度，以及 partition granule，都不能改变固定拓扑。
  const int64_t parallelisms[] = {1, 16};
  for (int64_t pass = 0; pass < 2; ++pass) {
    ObSEArray<ObDASTabletLoc *, 2> granule_tablets;
    ObSEArray<ObIExtTblScanTask *, 2> granule_tasks;
    ObSEArray<int64_t, 2> granule_idx;
    args.parallelism_ = parallelisms[pass];
    ASSERT_EQ(OB_SUCCESS, handler->split_granule(
        args, &tsc, ranges, tablets, pass != 0, granule_tablets, granule_tasks, granule_idx));
    ASSERT_EQ(2, granule_tasks.count());
    ASSERT_EQ(2, granule_tablets.count());
    ASSERT_EQ(2, granule_idx.count());
    for (int64_t i = 0; i < granule_tasks.count(); ++i) {
      ASSERT_NE(nullptr, granule_tasks.at(i));
      ASSERT_TRUE(granule_tasks.at(i)->is_direct_load_stream_scan_task());
      const ObDirectLoadStreamScanTask *scan_task =
          static_cast<const ObDirectLoadStreamScanTask *>(granule_tasks.at(i));
      EXPECT_EQ(stream_id, scan_task->stream_id_);
      EXPECT_EQ(static_cast<uint64_t>(i + 3), scan_task->queue_id_);
      EXPECT_EQ(GCTX.self_addr(), scan_task->server_addr_);
      EXPECT_EQ(table_schema->get_schema_version(), scan_task->target_schema_version_);
      EXPECT_EQ(&tablet, granule_tablets.at(i));
      EXPECT_EQ(i, granule_idx.at(i));
    }
  }
}

} // namespace test

int main(int argc, char **argv)
{
  system("rm -f test_direct_load_stream_source_iterator.log*");
  OB_LOGGER.set_file_name("test_direct_load_stream_source_iterator.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
