/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include <gtest/gtest.h>
#include <sstream>

#define private public
#include "storage/async_index/ob_async_index_define.h"
#include "lib/alloc/ob_malloc_allocator.h"
#undef private
#include "common/ob_clock_generator.h"

namespace oceanbase
{
namespace unittest
{

using namespace common;
using namespace storage;
using namespace share;

class TestAsyncIndexDefine : public ::testing::Test
{
public:
  TestAsyncIndexDefine() {}
  virtual ~TestAsyncIndexDefine() {}
  virtual void SetUp() override {}
  virtual void TearDown() override {}
};

TEST_F(TestAsyncIndexDefine, test_refresh_type_enum)
{
  ASSERT_TRUE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_PERIOD));
  ASSERT_TRUE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_CREATE_INDEX));
  ASSERT_FALSE(is_valid_async_refresh_type(ASYNC_REFRESH_TYPE_MAX));
  ASSERT_FALSE(is_valid_async_refresh_type(static_cast<ObAsyncRefreshType>(-1)));

  ASSERT_STREQ("PERIOD", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_PERIOD));
  ASSERT_STREQ("CREATE_INDEX", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_CREATE_INDEX));
  ASSERT_STREQ("UNKNOWN", async_refresh_type_to_str(ASYNC_REFRESH_TYPE_MAX));
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_reset)
{
  ObRefreshTaskStat stat;
  ASSERT_EQ(OB_INVALID_TENANT_ID, stat.tenant_id_);
  ASSERT_FALSE(stat.data_tablet_id_.is_valid());
  ASSERT_FALSE(stat.tablet_id_.is_valid());
  ASSERT_EQ(0, stat.refresh_id_);
  ASSERT_EQ(0, stat.retry_cnt_);
  ASSERT_EQ(ASYNC_REFRESH_TYPE_PERIOD, stat.refresh_type_);
  ASSERT_EQ(0, stat.queue_time_);
  ASSERT_EQ(0, stat.start_time_);
  ASSERT_EQ(0, stat.end_time_);
  ASSERT_EQ(0, stat.affected_rows_);
  ASSERT_FALSE(stat.start_refresh_scn_.is_valid());
  ASSERT_FALSE(stat.target_refresh_scn_.is_valid());
  ASSERT_EQ(OB_SUCCESS, stat.result_);
  ASSERT_EQ(OB_ASYNC_INDEX_MIN_PARALLELISM, stat.refresh_parallelism_);
  ASSERT_EQ(0, stat.schema_version_);
  ASSERT_EQ(0, stat.leader_epoch_);
  ASSERT_FALSE(stat.is_search_index_);
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_is_valid)
{
  ObRefreshTaskStat stat;
  // after reset, stat should be invalid (missing required fields)
  ASSERT_FALSE(stat.is_valid());

  // set required fields
  stat.tenant_id_ = 1001;
  stat.data_tablet_id_ = ObTabletID(200001);
  stat.tablet_id_ = ObTabletID(200002);
  stat.target_refresh_scn_ = SCN::min_scn();
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
  ASSERT_FALSE(stat.is_valid());

  // invalid refresh type
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_MAX;
  ASSERT_FALSE(stat.is_valid());

  // restore and invalidate tenant_id
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_PERIOD;
  stat.tenant_id_ = OB_INVALID_TENANT_ID;
  ASSERT_FALSE(stat.is_valid());
}

TEST_F(TestAsyncIndexDefine, test_retry_delay_calculation)
{
  // first 3 retries: immediate (0 delay)
  ASSERT_EQ(0, calc_async_index_retry_delay_us(0));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(1));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(2));
  ASSERT_EQ(0, calc_async_index_retry_delay_us(3));

  // 4th retry: 5s
  ASSERT_EQ(5L * 1000L * 1000L, calc_async_index_retry_delay_us(4));
  // 5th retry: 10s
  ASSERT_EQ(10L * 1000L * 1000L, calc_async_index_retry_delay_us(5));
  // 6th retry: 20s
  ASSERT_EQ(20L * 1000L * 1000L, calc_async_index_retry_delay_us(6));
  // 7th retry: 40s
  ASSERT_EQ(40L * 1000L * 1000L, calc_async_index_retry_delay_us(7));
  // 8th retry: 80s
  ASSERT_EQ(80L * 1000L * 1000L, calc_async_index_retry_delay_us(8));
  // 9th retry: 160s
  ASSERT_EQ(160L * 1000L * 1000L, calc_async_index_retry_delay_us(9));
  // 10th retry: capped at 5 min = 300s
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US, calc_async_index_retry_delay_us(10));
  // large retry count: still capped
  ASSERT_EQ(OB_ASYNC_INDEX_MAX_RETRY_INTERVAL_US, calc_async_index_retry_delay_us(100));
}

TEST_F(TestAsyncIndexDefine, test_retryable_error_codes)
{
  // A new transaction can recover these failures while reusing the original SQL.
  ASSERT_TRUE(is_async_index_retryable_error(OB_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_RPC_TIMEOUT));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_CONNECT_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_SEND_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_RPC_POST_ERROR));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_ROLLBACKED));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_NEED_ROLLBACK));
  ASSERT_TRUE(is_async_index_retryable_error(OB_TRANS_STMT_NEED_RETRY));

  ASSERT_FALSE(is_async_index_retryable_error(OB_SUCCESS));
  ASSERT_FALSE(is_async_index_retryable_error(OB_SCHEMA_ERROR));
  ASSERT_FALSE(is_async_index_retryable_error(OB_TABLE_NOT_EXIST));
  ASSERT_FALSE(is_async_index_retryable_error(OB_TENANT_NOT_EXIST));
  // stale ob_ddl_schema_version hint: SQL fails at resolve, regenerated next round
  ASSERT_FALSE(is_async_index_retryable_error(OB_DDL_SCHEMA_VERSION_NOT_MATCH));
  ASSERT_FALSE(is_async_index_retryable_error(OB_ERR_UNEXPECTED));

  // index tablet lock conflict: timer path skips retry, next round reschedules
  ASSERT_FALSE(is_async_index_retryable_error(OB_EAGAIN));

  // Errors outside the retry whitelist release the batch for the next timer round.
  ASSERT_FALSE(is_async_index_retryable_error(OB_NOT_MASTER));
  ASSERT_FALSE(is_async_index_retryable_error(OB_SIZE_OVERFLOW));
}

TEST_F(TestAsyncIndexDefine, test_refresh_task_stat_to_string)
{
  ObRefreshTaskStat stat;
  stat.tenant_id_ = 1001;
  stat.data_tablet_id_ = ObTabletID(200001);
  stat.tablet_id_ = ObTabletID(200002);
  stat.refresh_id_ = 42;
  stat.target_refresh_scn_ = SCN::min_scn();
  stat.refresh_type_ = ASYNC_REFRESH_TYPE_CREATE_INDEX;

  const int64_t buf_len = 1024;
  char buf[buf_len];
  int64_t pos = 0;
  pos = stat.to_string(buf, buf_len);
  ASSERT_GT(pos, 0);
  // verify key fields appear in output
  ASSERT_NE(nullptr, strstr(buf, "tenant_id"));
  ASSERT_NE(nullptr, strstr(buf, "CREATE_INDEX"));
}

class TestAsyncIndexDepsMemory : public ::testing::Test
{
protected:
  static constexpr uint64_t TENANT_ID = 1002;
  static constexpr uint64_t OTHER_TENANT_ID = 1004;
  static void SetUpTestCase()
  {
    lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
    for (uint64_t tenant_id : {TENANT_ID, OTHER_TENANT_ID}) {
      if (nullptr == allocator->get_tenant_ctx_allocator(tenant_id, ObCtxIds::DEFAULT_CTX_ID)) {
        ASSERT_EQ(OB_SUCCESS, allocator->create_and_add_tenant_allocator(tenant_id));
      }
      ASSERT_EQ(OB_SUCCESS, allocator->set_tenant_limit(tenant_id, 1L << 30));
    }
  }
  static int64_t used(uint64_t tenant_id, const char *label = "AsyncIdxDeps")
  {
    // Read live objects under the allocator's locks. The public label-usage
    // API reads a periodic memory-dump snapshot and cannot detect a leak in
    // a single assignment/reset without waiting for that background task.
    auto allocator = lib::ObMallocAllocator::get_instance()->get_tenant_ctx_allocator(
        tenant_id, ObCtxIds::DEFAULT_CTX_ID);
    EXPECT_NE(nullptr, allocator);
    int64_t bytes = 0;
    if (nullptr != allocator) {
      allocator->do_cleanup();
      auto &mgr = allocator->obj_mgr_;
      for (int64_t i = 0; i < mgr.parallel_; ++i) {
        for (auto &size_class : mgr.obj_sets_[i].scs) {
          size_class.lock_.lock();
          for (lib::ABlock *head : {size_class.avail_blist_, size_class.full_blist_}) {
            lib::ABlock *block = head;
            if (nullptr != block) {
              do {
                lib::AObject *object = reinterpret_cast<lib::AObject *>(block->data());
                const int64_t cells = lib::AllocHelper::cells_per_block(block->ablock_size_);
                while (true) {
                  if (object->in_use_ && 0 == STRCMP(label, object->label_)) {
                    bytes += object->alloc_bytes_;
                  }
                  if (object->is_large_ || object->is_last(cells)) {
                    break;
                  }
                  object = object->phy_next(object->nobjs_);
                }
                block = block->next_;
              } while (block != head);
            }
          }
          size_class.lock_.unlock();
        }
      }
    }
    return bytes;
  }
  static void fill_index(ObAsyncIndexInfo &info)
  {
    info.index_table_id_ = 200001;
    info.def_index_table_id_ = 200002;
    info.index_type_ = schema::INDEX_TYPE_NORMAL_LOCAL;
    info.index_table_name_ = ObString::make_string("async_index");
    info.enable_newsort_ = true;
    info.is_delete_index_ = true;
    info.is_refresh_on_error_skip_ = true;
    // Overflow every local array, including the 16-entry partition arrays.
    for (int64_t i = 0; i < 17; ++i) {
      ASSERT_EQ(OB_SUCCESS, info.select_columns_.push_back(ObString::make_string("selected")));
      ASSERT_EQ(OB_SUCCESS, info.source_columns_.push_back(ObString::make_string("source")));
      ASSERT_EQ(OB_SUCCESS, info.partition_key_columns_.push_back(ObString::make_string("partition_key")));
      ASSERT_EQ(OB_SUCCESS, info.rowkey_columns_.push_back(ObString::make_string("rowkey")));
      ASSERT_EQ(OB_SUCCESS, info.order_columns_.push_back(ObString::make_string("order_key")));
      ASSERT_EQ(OB_SUCCESS, info.tablet_ids_.push_back(ObTabletID(300000 + i)));
      ASSERT_EQ(OB_SUCCESS, info.partition_names_.push_back(ObString::make_string("p0")));
    }
  }
  static void fill_table(ObAsyncIndexDataTableInfo &info)
  {
    info.data_table_id_ = 100001;
    info.database_name_ = ObString::make_string("db");
    info.data_table_name_ = ObString::make_string("data_table");
    info.schema_version_ = 12345;
    info.refresh_interval_us_ = 3000000;
    for (int64_t i = 0; i < 17; ++i) {
      ObAsyncIndexDataTabletInfo tablet;
      tablet.data_tablet_id_ = ObTabletID(400000 + i);
      tablet.partition_name_ = ObString::make_string("p0");
      tablet.data_ls_id_ = ObLSID(1001);
      ASSERT_EQ(OB_SUCCESS, info.data_tablet_infos_.push_back(tablet));
    }
    ObAsyncIndexInfo index(TENANT_ID);
    fill_index(index);
    // Overflow index_infos_ as well, exercising copies during expansion.
    for (int64_t i = 0; i < 5; ++i) {
      index.index_table_id_ = 200001 + i;
      ASSERT_EQ(OB_SUCCESS, info.index_infos_.push_back(index));
    }
  }
  template <typename T, int64_t N>
  static void check_array(const ObSEArray<T, N> &actual, const ObSEArray<T, N> &expected)
  {
    EXPECT_EQ(TENANT_ID, actual.block_allocator_.get_attr().tenant_id_);
    EXPECT_EQ(lib::ObLabel("AsyncIdxDeps"), actual.block_allocator_.get_attr().label_);
    ASSERT_EQ(expected.count(), actual.count());
    ASSERT_NE(expected.get_data(), actual.get_data());
    for (int64_t i = 0; i < actual.count(); ++i) {
      EXPECT_EQ(expected.at(i), actual.at(i));
    }
  }
  static void check_table(const ObAsyncIndexDataTableInfo &actual, const ObAsyncIndexDataTableInfo &expected)
  {
    EXPECT_EQ(expected.data_table_id_, actual.data_table_id_);
    EXPECT_EQ(expected.database_name_, actual.database_name_);
    EXPECT_EQ(expected.data_table_name_, actual.data_table_name_);
    EXPECT_EQ(expected.schema_version_, actual.schema_version_);
    EXPECT_EQ(expected.refresh_interval_us_, actual.refresh_interval_us_);
    EXPECT_EQ(TENANT_ID, actual.data_tablet_infos_.block_allocator_.get_attr().tenant_id_);
    EXPECT_EQ(TENANT_ID, actual.index_infos_.block_allocator_.get_attr().tenant_id_);
    EXPECT_EQ(lib::ObLabel("AsyncIdxDeps"), actual.data_tablet_infos_.block_allocator_.get_attr().label_);
    EXPECT_EQ(lib::ObLabel("AsyncIdxDeps"), actual.index_infos_.block_allocator_.get_attr().label_);
    ASSERT_EQ(expected.data_tablet_infos_.count(), actual.data_tablet_infos_.count());
    for (int64_t i = 0; i < actual.data_tablet_infos_.count(); ++i) {
      EXPECT_EQ(expected.data_tablet_infos_.at(i).data_tablet_id_, actual.data_tablet_infos_.at(i).data_tablet_id_);
      EXPECT_EQ(expected.data_tablet_infos_.at(i).partition_name_, actual.data_tablet_infos_.at(i).partition_name_);
      EXPECT_EQ(expected.data_tablet_infos_.at(i).data_ls_id_, actual.data_tablet_infos_.at(i).data_ls_id_);
    }
    ASSERT_EQ(expected.index_infos_.count(), actual.index_infos_.count());
    for (int64_t i = 0; i < actual.index_infos_.count(); ++i) {
      const ObAsyncIndexInfo &a = actual.index_infos_.at(i);
      const ObAsyncIndexInfo &e = expected.index_infos_.at(i);
      EXPECT_EQ(e.index_table_id_, a.index_table_id_);
      EXPECT_EQ(e.def_index_table_id_, a.def_index_table_id_);
      EXPECT_EQ(e.index_type_, a.index_type_);
      EXPECT_EQ(e.index_table_name_, a.index_table_name_);
      EXPECT_EQ(e.enable_newsort_, a.enable_newsort_);
      EXPECT_EQ(e.is_delete_index_, a.is_delete_index_);
      EXPECT_EQ(e.is_refresh_on_error_skip_, a.is_refresh_on_error_skip_);
      check_array(a.select_columns_, e.select_columns_);
      check_array(a.source_columns_, e.source_columns_);
      check_array(a.partition_key_columns_, e.partition_key_columns_);
      check_array(a.rowkey_columns_, e.rowkey_columns_);
      check_array(a.order_columns_, e.order_columns_);
      check_array(a.tablet_ids_, e.tablet_ids_);
      check_array(a.partition_names_, e.partition_names_);
    }
  }
};

TEST_F(TestAsyncIndexDepsMemory, reserve_round_capacity_and_rebind_tenant)
{
  RecordProperty("data_table_info_bytes", static_cast<int>(sizeof(ObAsyncIndexDataTableInfo)));
  const int64_t initial_used = used(TENANT_ID);
  const int64_t server_used = used(OB_SERVER_TENANT_ID);
  const int64_t server_array_used = used(OB_SERVER_TENANT_ID, "SeArray");
  ObAsyncIndexRefreshDeps deps(TENANT_ID);
  for (int64_t count : {0, 8, 9, 4096, 4097, 4144}) {
    ASSERT_EQ(OB_SUCCESS, deps.prepare(TENANT_ID, count));
    const int64_t capacity = deps.data_table_infos_.get_capacity();
    EXPECT_EQ(std::max<int64_t>(8, count), capacity);
    if (4144 == count) {
      RecordProperty("reserved_4144_bytes", static_cast<int>(capacity * sizeof(ObAsyncIndexDataTableInfo)));
    }
    for (int64_t i = 0; i < count; ++i) {
      ObAsyncIndexDataTableInfo info(TENANT_ID);
      info.data_table_id_ = i + 1;
      ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.push_back(info));
    }
    EXPECT_EQ(capacity, deps.data_table_infos_.get_capacity());
    EXPECT_EQ(count, deps.data_table_infos_.count());
    ASSERT_NE(nullptr, deps.allocator_.alloc(1024));
    EXPECT_GT(used(TENANT_ID), initial_used);
    EXPECT_EQ(server_used, used(OB_SERVER_TENANT_ID));
    EXPECT_EQ(server_array_used, used(OB_SERVER_TENANT_ID, "SeArray"));
    deps.reset();
    EXPECT_EQ(initial_used, used(TENANT_ID));
  }
  const int64_t other_used = used(OTHER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, deps.prepare(OTHER_TENANT_ID, 9));
  ASSERT_NE(nullptr, deps.allocator_.alloc(1024));
  EXPECT_GT(used(OTHER_TENANT_ID), other_used);
  EXPECT_EQ(initial_used, used(TENANT_ID));
  EXPECT_EQ(OB_INVALID_ARGUMENT, deps.prepare(OB_INVALID_TENANT_ID, 9));
  EXPECT_TRUE(deps.empty());
  EXPECT_EQ(other_used, used(OTHER_TENANT_ID));
  EXPECT_EQ(OB_INVALID_ARGUMENT, deps.prepare(TENANT_ID, -1));
}

TEST_F(TestAsyncIndexDepsMemory, nested_copy_expansion_and_destruction)
{
  const int64_t initial_used = used(TENANT_ID);
  const int64_t server_used = used(OB_SERVER_TENANT_ID);
  const int64_t server_array_used = used(OB_SERVER_TENANT_ID, "SeArray");
  const int64_t server_page_used = used(OB_SERVER_TENANT_ID, "ModulePageAlloc");
  {
    ObAsyncIndexDataTableInfo source(TENANT_ID);
    fill_table(source);
    ObAsyncIndexRefreshDeps deps(TENANT_ID);
    for (int64_t i = 0; i < 9; ++i) {
      ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.push_back(source));
    }
    ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.reserve(20));
    ASSERT_EQ(OB_SUCCESS, source.assign(source));
    ASSERT_EQ(OB_SUCCESS, source.index_infos_.at(0).assign(source.index_infos_.at(0)));
    for (int64_t i = 0; i < deps.data_table_infos_.count(); ++i) {
      check_table(deps.data_table_infos_.at(i), source);
    }
    EXPECT_EQ(server_used, used(OB_SERVER_TENANT_ID));
    EXPECT_EQ(server_array_used, used(OB_SERVER_TENANT_ID, "SeArray"));
    EXPECT_EQ(server_page_used, used(OB_SERVER_TENANT_ID, "ModulePageAlloc"));
    source.reset();
    EXPECT_EQ(ObString::make_string("selected"), deps.data_table_infos_.at(0).index_infos_.at(0).select_columns_.at(16));
  }
  EXPECT_EQ(initial_used, used(TENANT_ID));
}

TEST_F(TestAsyncIndexDepsMemory, reserved_round_nested_arrays_charge_business_tenant)
{
  const int64_t initial_used = used(TENANT_ID);
  const int64_t server_used = used(OB_SERVER_TENANT_ID);
  const int64_t server_array_used = used(OB_SERVER_TENANT_ID, "SeArray");
  const int64_t server_page_used = used(OB_SERVER_TENANT_ID, "ModulePageAlloc");
  // Cover the inner inline-capacity boundary and a representative large round
  // from issues 2026102800120127223 / 2026102800120130573. Eleven indexes fall
  // in the observed 9..12-entry allocation bucket; the exact DDL is unknown.
  for (int64_t index_count : {4, 5, 11}) {
    {
      const int64_t table_count = 11 == index_count ? 4144 : 9;
      ObAsyncIndexDataTableInfo source(TENANT_ID);
      source.database_name_ = ObString::make_string("db");
      source.data_table_name_ = ObString::make_string("data_table");
      ObAsyncIndexDataTabletInfo tablet;
      tablet.data_tablet_id_ = ObTabletID(100001);
      ASSERT_EQ(OB_SUCCESS, source.data_tablet_infos_.push_back(tablet));
      ObAsyncIndexInfo index(TENANT_ID);
      index.index_table_name_ = ObString::make_string("async_index");
      ASSERT_EQ(OB_SUCCESS, index.select_columns_.push_back(ObString::make_string("c1")));
      ASSERT_EQ(OB_SUCCESS, index.tablet_ids_.push_back(ObTabletID(300001)));
      ASSERT_EQ(OB_SUCCESS, index.partition_names_.push_back(ObString()));
      for (int64_t i = 0; i < index_count; ++i) {
        index.index_table_id_ = 200001 + i;
        ASSERT_EQ(OB_SUCCESS, source.index_infos_.push_back(index));
      }
      ObAsyncIndexRefreshDeps deps(TENANT_ID);
      ASSERT_EQ(OB_SUCCESS, deps.prepare(TENANT_ID, table_count));
      for (int64_t i = 0; i < table_count; ++i) {
        source.data_table_id_ = 100001 + i;
        ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.push_back(source));
      }
      source.reset();
      EXPECT_EQ(table_count, deps.data_table_infos_.get_capacity());
      ASSERT_EQ(table_count, deps.data_table_infos_.count());
      for (int64_t i = 0; i < table_count; ++i) {
        const ObAsyncIndexDataTableInfo &copy = deps.data_table_infos_.at(i);
        EXPECT_EQ(100001 + i, copy.data_table_id_);
        ASSERT_EQ(index_count, copy.index_infos_.count());
        EXPECT_EQ(TENANT_ID, copy.index_infos_.block_allocator_.get_attr().tenant_id_);
        EXPECT_EQ(lib::ObLabel("AsyncIdxDeps"), copy.index_infos_.block_allocator_.get_attr().label_);
        EXPECT_EQ(ObString::make_string("c1"), copy.index_infos_.at(index_count - 1).select_columns_.at(0));
      }
      EXPECT_GT(used(TENANT_ID), initial_used);
      EXPECT_EQ(server_used, used(OB_SERVER_TENANT_ID));
      EXPECT_EQ(server_array_used, used(OB_SERVER_TENANT_ID, "SeArray"));
      EXPECT_EQ(server_page_used, used(OB_SERVER_TENANT_ID, "ModulePageAlloc"));
      if (4144 == table_count) {
        RecordProperty("tables_4144_business_dependency_bytes", used(TENANT_ID) - initial_used);
        RecordProperty("tables_4144_server_module_page_alloc_delta",
            used(OB_SERVER_TENANT_ID, "ModulePageAlloc") - server_page_used);
        RecordProperty("copied_index_buffer_bytes", deps.data_table_infos_.at(0).index_infos_.get_data_size());
      }
    }
    EXPECT_EQ(initial_used, used(TENANT_ID));
  }
}

// EN_4 applies to the real allocator, including arrays constructed inside a
// container copy. Always restore the event even when an assertion aborts.
class ScopedAsyncDepsAllocFailure
{
public:
  explicit ScopedAsyncDepsAllocFailure(int64_t frequency)
    : saved_(EventTable::EN_4.item_), saved_signal_handler_(signal(49, SIG_IGN))
  { TP_SET_EVENT(EventTable::EN_4, OB_ALLOCATE_MEMORY_FAILED, 0, frequency); }
  ~ScopedAsyncDepsAllocFailure()
  {
    EventTable::EN_4.item_.set_event(saved_);
    signal(49, saved_signal_handler_);
  }
private:
  EventItem saved_;
  // Allocation-failure diagnostics send observer's memory-dump signal.
  // There is no observer signal handler in this standalone unit test.
  void (*saved_signal_handler_)(int);
};

TEST_F(TestAsyncIndexDepsMemory, allocation_failure_keeps_source_and_releases_partial_copies)
{
  ObAsyncIndexDataTableInfo source(TENANT_ID);
  fill_table(source);
  const int64_t source_used = used(TENANT_ID);
  TP_SWITCH_GUARD(false);
  ObAsyncIndexRefreshDeps deps(TENANT_ID);
  {
    ScopedAsyncDepsAllocFailure failure(1);
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, deps.prepare(TENANT_ID, 4144));
    EXPECT_TRUE(deps.empty());
    EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, deps.data_table_infos_.push_back(source));
    EXPECT_TRUE(deps.empty());
  }
  // Exercise failures at different depths, including after earlier indexes
  // have been fully copied. Every failed assignment must release its prefix.
  for (int64_t i = 0; i < 128; ++i) {
    ObAsyncIndexDataTableInfo copy(OTHER_TENANT_ID);
    int ret = OB_SUCCESS;
    {
      ScopedAsyncDepsAllocFailure failure(8);
      ret = copy.assign(source);
    }
    if (OB_SUCCESS == ret) {
      check_table(copy, source);
      copy.reset();
    } else {
      EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED, ret);
      EXPECT_FALSE(copy.is_valid());
      EXPECT_TRUE(copy.index_infos_.empty());
      EXPECT_TRUE(copy.data_tablet_infos_.empty());
    }
    ASSERT_EQ(source_used, used(TENANT_ID));
  }
  ASSERT_TRUE(source.is_valid());
  ASSERT_EQ(OB_SUCCESS, deps.data_table_infos_.push_back(source));
  check_table(deps.data_table_infos_.at(0), source);
  deps.reset();
  EXPECT_EQ(source_used, used(TENANT_ID));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  if (OB_SUCCESS != oceanbase::common::ObClockGenerator::init()) {
    return 1;
  }
  testing::InitGoogleTest(&argc, argv);
  const int ret = RUN_ALL_TESTS();
  oceanbase::common::ObClockGenerator::destroy();
  return ret;
}
