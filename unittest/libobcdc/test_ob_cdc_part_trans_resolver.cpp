/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX OBLOG

#include <string>

#include "log_generator.h" // must at last of header list
#define private public
#include "logservice/libobcdc/src/ob_cdc_part_trans_resolver.h"
#undef private
#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/lob/common/ob_lob_seq.h"
#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/utility/ob_test_util.h"
#include "storage/tx/ob_trans_log.h"
#include "logservice/logfetcher/ob_log_fetch_stat_info.h"
#include "logservice/libobcdc/src/ob_log_utils.h"
#include "logservice/libobcdc/src/ob_log_ls_fetch_mgr.h"
#include "logservice/libobcdc/src/ob_log_part_progress_controller.h"
#include "logservice/libobcdc/src/ob_log_part_trans_resolver_factory.h"
#include "logservice/libobcdc/src/ob_log_sys_ls_task_handler.h"
#include "logservice/libobcdc/src/ob_log_cluster_id_filter.h"
#include "logservice/libobcdc/src/ob_log_committer.h"
#include "logservice/libobcdc/src/ob_log_instance.h"
#include "logservice/libobcdc/src/ob_log_resource_collector.h"
#include "logservice/libobcdc/src/ob_log_lsn_filter.h"

using namespace oceanbase;
using namespace common;
using namespace libobcdc;
using namespace transaction;
using namespace storage;
using namespace logfetcher;

#define PREPARE_ENV(tenant_id, ls_id, tx_id, cluster_id) \
    bool stop_flag = false; \
    logservice::TenantLSID tls_id(tenant_id, share::ObLSID(ls_id)); \
    EXPECT_TRUE(tls_id.is_valid()); \
    IObCDCPartTransResolver::MissingLogInfo missing_info; \
    logfetcher::TransStatInfo tsi; \
    int64_t start_ts_ns = 1; \
    palf::LSN start_lsn(0); \
    EXPECT_TRUE(start_lsn.is_valid());


#define GET_LS_FETCH_MGR(ls_fetch_mgr) \
    ObLogInstance *instance = ObLogInstance::get_instance(); \
    ObConcurrentFIFOAllocator fifo_allocator; \
    PartProgressController progress_controller; \
    EXPECT_EQ(OB_SUCCESS, progress_controller.init(10)); \
    ObLogPartTransResolverFactory resolver_factory; \
    ObLogTransTaskPool<PartTransTask> task_pool; \
    EXPECT_EQ(OB_SUCCESS, fifo_allocator.init(16 * _G_, 16 * _M_, OB_MALLOC_NORMAL_BLOCK_SIZE)); \
    EXPECT_EQ(OB_SUCCESS, task_pool.init(&fifo_allocator, PREALLOC_POOL_SIZE, true, PREALLOC_PAGE_COUNT)); \
    ObLogEntryTaskPool log_entry_task_pool; \
    EXPECT_EQ(OB_SUCCESS, log_entry_task_pool.init(10/* fixed_log_entry_task_count */)); \
    MockFetcherDispatcher fetcher_dispatcher; \
    MockResourceCollector resource_collector; \
    instance->resource_collector_ = &resource_collector; \
    ObLogSysLsTaskHandler sys_ls_handler; \
    ObLogCommitter committer; \
    EXPECT_EQ(OB_SUCCESS, fetcher_dispatcher.init(&sys_ls_handler, &committer, 0)); \
    ObLogClusterIDFilter cluster_id_filter; \
    const char *cluster_id_black_list = "2147473648"; \
    ObLogLsnFilter lsn_filter; \
    double a = 1.0; \
    void *fetcher = &a; \
    const int64_t source_cluster_id = 1; \
    EXPECT_EQ(OB_SUCCESS, cluster_id_filter.init(cluster_id_black_list, 2147473648, 2147483647)); \
    EXPECT_EQ(OB_SUCCESS, lsn_filter.init("|")); \
    EXPECT_EQ(OB_SUCCESS, resolver_factory.init(task_pool, log_entry_task_pool, fetcher_dispatcher, cluster_id_filter, lsn_filter, source_cluster_id)); \
    EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.init(1, progress_controller, resolver_factory, fetcher));


#define PREPARE_LS_FETCH_CTX() \
    const uint64_t tenant_id = 1002; /*should be user tenant but not meta_tenant or sys_tenant*/ \
    const int64_t ls_id = 1001; \
    const int64_t tx_id = 111111; \
    const uint64_t cluster_id = 1; \
    PREPARE_ENV(tenant_id, ls_id, tx_id, cluster_id); \
    ObLogLSFetchMgr ls_fetch_mgr; \
    GET_LS_FETCH_MGR(ls_fetch_mgr); \
    LSFetchCtx *ls_fetch_ctx = NULL; \
    ObLogFetcherStartParameters start_paras; \
    start_paras.reset(start_ts_ns, start_lsn); \
    logservice::ObLogserviceModelInfo logservice_model_info; \
    EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.add_ls(tls_id, start_paras, false, false, \
        ClientFetchingMode::FETCHING_MODE_INTEGRATED, "|", logservice_model_info)); \
    EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.get_ls_fetch_ctx(tls_id, ls_fetch_ctx)); \
    ObTxLogGenerator log_generator(tenant_id, ls_id, tx_id, cluster_id);


#define GET_PART_TRANS_TASK() \
  ObCDCPartTransResolver *resolver = static_cast<ObCDCPartTransResolver*>(ls_fetch_ctx->get_part_trans_resolver()); \
  PartTransTask *part_trans_task = NULL; \
  EXPECT_EQ(OB_SUCCESS, resolver->obtain_task_(tx_id, part_trans_task, false));


#define DESTROY_OBLOG_INSTANCE() \
    instance->resource_collector_ = NULL; \
    ObLogInstance::destroy_instance();

namespace oceanbase
{
namespace libobcdc
{
class MockFetcherDispatcher : public ObLogFetcherDispatcher
{
  virtual int dispatch(PartTransTask &task, volatile bool &stop_flag) override
  {
    return OB_SUCCESS;
  }
};
class MockResourceCollector : public ObLogResourceCollector
{
  virtual int revert(PartTransTask *task)
  {
    return OB_SUCCESS;
  }
};
}
namespace unittest
{

// Task Pool
static const int64_t PREALLOC_POOL_SIZE = 10 * 1024;
static const int64_t TRANS_TASK_BLOCK_SIZE = 4 * 1024 *1024;
static const int64_t PREALLOC_PAGE_COUNT = 1024;

// test trans count
static const int64_t TRANS_COUNT = 100;
// redo log count
static const int64_t TRANS_REDO_LOG_COUNT = 100;

void call_sort_and_unique_missing_log_ids(IObCDCPartTransResolver::MissingLogInfo &missing_info)
{
  LOG_DEBUG("MISSING LOG [BEGIN]", K(missing_info));
  EXPECT_EQ(OB_SUCCESS, missing_info.sort_and_unique_missing_log_lsn());
  LOG_DEBUG("MISSING LOG [END]", K(missing_info));
}

TEST(ObCDCPartTransResolver, test_misslog_info_basic)
{
  int ret = OB_SUCCESS;
  IObCDCPartTransResolver::MissingLogInfo missing_info;
  ObLogLSNArray &missing_log_id = missing_info.get_miss_redo_lsn_arr();

  // prepare data
  palf::LSN lsn_1(1);
  palf::LSN lsn_2(2);
  palf::LSN lsn_3(3);
  palf::LSN lsn_4(4);
  palf::LSN lsn_5(5);
  palf::LSN record_lsn(6);
  ObLogLSNArray local_lsn_arr;
  local_lsn_arr.reset();
  local_lsn_arr.push_back(lsn_1);
  local_lsn_arr.push_back(lsn_2);
  local_lsn_arr.push_back(lsn_3);
  local_lsn_arr.push_back(lsn_4);

  // 1. one miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());

  // 2. two miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());

  // 3. repeatable miss log with id 1
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());

  // 4. multi repeatable miss log
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_2));
  EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_2));
  call_sort_and_unique_missing_log_ids(missing_info);
  EXPECT_EQ(2, missing_info.get_total_misslog_cnt());
  for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
    EXPECT_EQ(idx+1, missing_log_id.at(idx));
  }

  // // 5. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_missing_log_lsn_arr(local_lsn_arr));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(lsn_1));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(2, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }

  // // 6. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(2, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }


  // // 7. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(4, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }

  // // 8. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(4, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }

  // // 9. multi repeatable miss log
  // missing_info.reset();
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(1));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(2));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(3));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // EXPECT_EQ(OB_SUCCESS, missing_info.push_back_single_miss_log_lsn(4));
  // call_sort_and_unique_missing_log_ids(missing_info);
  // EXPECT_EQ(4, missing_info.get_total_misslog_cnt());
  // for (int64_t idx=0; OB_SUCC(ret) && idx < missing_log_id.count(); ++idx) {
  //   EXPECT_EQ(idx+1, missing_log_id.at(idx));
  // }
}


TEST(ObCDCPartTransResolver, test_ls_fetch_ctx)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  LOG_DEBUG("redo-0", K(log_entry), K(lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  LOG_DEBUG("redo-1", K(log_entry), K(lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  LOG_DEBUG("redo-2", K(log_entry), K(lsn));
  ipalf::ILogEntry offline_ls_entry;
  LSN offline_lsn;
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_ls_offline_log_entry(offline_ls_entry, offline_lsn));
  EXPECT_EQ(OB_INVALID_DATA, ls_fetch_ctx->read_log(offline_ls_entry, offline_lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(offline_ls_entry, offline_lsn, missing_info, tsi, stop_flag));

  EXPECT_EQ(OB_SUCCESS, ls_fetch_mgr.remove_ls(tls_id));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ls_fetch_mgr.get_ls_fetch_ctx(tls_id, ls_fetch_ctx));
  DESTROY_OBLOG_INSTANCE();
}

// Build a shared out-table LOB log body with a branch-aware operation sequence and embedded zero byte.
int build_outtable_log_arg(ObIAllocator &allocator, OuttableTestPayload &arg)
{
  // Keep the piece sequence and LOB identity stable so both resolver tests use the same valid input.
  LOG_INFO("build out-table LOB log argument: initialize piece sequence and identifiers");
  int ret = ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, arg.seq_id_);
  arg.lob_piece_table_id_ = 2002;
  arg.lob_operation_seq_no_ = ObTxSEQ(10 /*seq*/, 3 /*branch*/);
  arg.lob_id_.set_column_idx(3 /*column_idx*/);
  arg.lob_id_.set_tablet_id(1001 /*tablet_id*/);
  arg.lob_id_.set_auto_inc_id(1 /*auto_inc_id*/);

  // Use a valid shared-storage macro ID so serialization reaches the resolver entry under test.
  arg.macro_id_.set_version_v1();
  arg.macro_id_.set_id_mode(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
  arg.macro_id_.set_storage_object_type(static_cast<uint64_t>(
      blocksstable::ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
  arg.macro_id_.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
  arg.macro_id_.set_second_id(1000 /*second_id*/);
  arg.macro_id_.set_third_id(1001 /*third_id*/);
  arg.macro_id_.set_fourth_id(1 /*fourth_id*/);
  arg.macro_id_.set_fifth_id(0 /*fifth_id*/);

  // Preserve the explicit byte length; the embedded zero must not truncate the serialized payload.
  arg.data_.assign_ptr("lob\0data" /*bytes*/, 8 /*length*/);
  arg.byte_len_ = arg.data_.length();
  arg.lob_col_idx_ = arg.lob_id_.get_column_idx();
  LOG_INFO("build out-table LOB log argument completed", KR(ret), K(arg.lob_piece_table_id_),
      K(arg.lob_operation_seq_no_), K(arg.lob_id_), K(arg.macro_id_),
      "seq_id_length", arg.seq_id_.length(), "data_length", arg.data_.length());
  return ret;
}

// Create or reuse the tenant allocator required by the production redo-node allocation path.
int prepare_outtable_tenant_allocator(const uint64_t tenant_id)
{
  lib::ObMallocAllocator *allocator = lib::ObMallocAllocator::get_instance();
  int ret = OB_SUCCESS;
  LOG_INFO("prepare out-table tenant allocator: obtain allocator", K(tenant_id), KP(allocator));
  if (OB_ISNULL(allocator)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    // Tests share the process allocator, so an already registered tenant is expected on later calls.
    ret = allocator->create_and_add_tenant_allocator(tenant_id);
    LOG_INFO("prepare out-table tenant allocator: tenant registration completed", KR(ret), K(tenant_id));
    if (OB_ENTRY_EXIST == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      // Give the small resolver fixture a fixed tenant allocation limit.
      ret = allocator->set_tenant_limit(tenant_id, 1L << 30 /*bytes*/);
      LOG_INFO("prepare out-table tenant allocator: limit configured", KR(ret), K(tenant_id),
          "tenant_limit_bytes", 1L << 30);
    }
  }
  LOG_INFO("prepare out-table tenant allocator completed", KR(ret), K(tenant_id));
  return ret;
}

// ============================================================================
// Out-table LOB log ingestion and missing-log recovery (2 cases)
// ============================================================================

// Real TX_REDO owns the header/native-row/payload bytes, including a 2 MiB object.
// Re-delivery must not create another redo node or fetched-LSN entry.
TEST(ObCDCPartTransResolver, outtable_mutator_redo_owns_binary_and_large_payloads)
{
  PREPARE_LS_FETCH_CTX();
  OK(prepare_outtable_tenant_allocator(tenant_id));
  instance->working_mode_ = WorkingMode::MEMORY_MODE;
  ObArenaAllocator allocator;
  int64_t expected_node_count = 0;
  for (const int64_t payload_size : {int64_t(8), storage::ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE}) {
    OuttableTestPayload payload;
    OK(build_outtable_log_arg(allocator, payload));
    std::string data(payload_size, '\0');
    data.front() = 'A';
    data.back() = 'Z';
    payload.data_.assign_ptr(data.data(), data.size());
    payload.byte_len_ = data.size();
    OK(log_generator.gen_lob_outtable_redo(payload));
    ipalf::ILogEntry entry;
    palf::LSN lsn;
    OK(log_generator.gen_log_entry(entry, lsn));

    ObTxLogBlock block;
    ObTxLogHeader header;
    ObTxRedoLogTempRef temp_ref;
    ObTxRedoLog redo(temp_ref);
    OK(block.init_for_replay(entry.get_data_buf(), entry.get_data_len()));
    OK(block.get_next_log(header));
    EXPECT_EQ(ObTxLogType::TX_REDO_LOG, header.get_tx_log_type());
    OK(block.deserialize_log_body(redo));
    const char *mutator_buf = redo.get_replay_mutator_buf();
    ASSERT_NE(nullptr, mutator_buf);
    ObMemtableMutatorMeta meta;
    int64_t meta_pos = 0;
    OK(meta.deserialize(mutator_buf, redo.get_mutator_size(), meta_pos));
    EXPECT_TRUE(ObTransRowFlag::is_normal_row(meta.get_flags()));
    const std::string expected(mutator_buf + meta_pos, redo.get_mutator_size() - meta_pos);
    if (payload_size > 1024) {
      EXPECT_GT(entry.get_data_len(), static_cast<int64_t>(ObTxAdaptiveLogBuf::NORMAL_LOG_BUF_SIZE));
    }
    OK(ls_fetch_ctx->read_log(entry, lsn, missing_info, tsi, stop_flag));
    GET_PART_TRANS_TASK();
    const int64_t node_count = part_trans_task->get_sorted_redo_list().get_node_number();
    const int64_t fetched_count = part_trans_task->get_sorted_log_entry_info().get_fetched_log_entry_node_arr().count();
    EXPECT_EQ(++expected_node_count, node_count);
    EXPECT_EQ(expected_node_count, fetched_count);
    IObCDCPartTransResolver::MissingLogInfo duplicate;
    duplicate.set_resolving_miss_log();
    OK(ls_fetch_ctx->read_miss_tx_log(entry, lsn, tsi, duplicate));
    EXPECT_EQ(node_count, part_trans_task->get_sorted_redo_list().get_node_number());
    EXPECT_EQ(fetched_count, part_trans_task->get_sorted_log_entry_info().get_fetched_log_entry_node_arr().count());
    MEMSET(const_cast<char *>(entry.get_data_buf()), 0, entry.get_data_len());

    RedoLogMetaNode *owned = nullptr;
    for (RedoNodeIterator it = part_trans_task->get_sorted_redo_list().redo_iter_begin();
        it != part_trans_task->get_sorted_redo_list().redo_iter_end(); it++) {
      if ((*it).get_start_log_lsn() == lsn) { owned = &*it; break; }
    }
    ASSERT_NE(nullptr, owned);
    EXPECT_FALSE(static_cast<DmlRedoLogNode *>(owned)->is_direct_load_inc_log());
    EXPECT_EQ(expected, std::string(owned->get_data(), owned->get_data_len()));
    OuttableTestPayload decoded;
    int64_t pos = 0;
    OK(decode_outtable_test_row(owned->get_data(), owned->get_data_len(), pos, decoded));
    EXPECT_EQ(owned->get_data_len(), pos);
    EXPECT_EQ(payload.lob_piece_table_id_, decoded.lob_piece_table_id_);
    EXPECT_TRUE(payload.lob_operation_seq_no_ == decoded.lob_operation_seq_no_);
    EXPECT_TRUE(payload.lob_id_ == decoded.lob_id_);
    EXPECT_TRUE(payload.macro_id_ == decoded.macro_id_);
    EXPECT_EQ(payload.seq_id_, decoded.seq_id_);
    EXPECT_EQ(data, std::string(decoded.data_.ptr(), decoded.data_.length()));
  }
  DESTROY_OBLOG_INSTANCE();
}

// A missing TX_REDO containing an outtable mutator blocks commit processing until its LSN is registered through refetch.
// Scenario: Generate a LOB entry, then an entry containing REDO, COMMIT_INFO and COMMIT; consume the latter first.
// Steps: Check the reported gap, supply the LOB entry through the missing-log path and reconsume the commit entry.
// Checks: The missing LSN matches the LOB entry, commit reconsumption is requested and succeeds after refetch.
// Scope: This checks LSN completeness and recovery; it does not verify recovered payload contents,
//        issue network fetches or exercise LOB assembly and final CDC output.
TEST(ObCDCPartTransResolver, missing_outtable_mutator_redo_is_refetched_before_commit)
{
  LOG_INFO("========== missing_outtable_mutator_redo_is_refetched_before_commit begin ==========");

  // Generate but withhold the LOB entry so COMMIT_INFO can reveal a known missing LSN.
  LOG_INFO("step 1: prepare a user LS and generate the LOB entry to withhold");
  PREPARE_LS_FETCH_CTX();
  int ret = prepare_outtable_tenant_allocator(tenant_id);
  LOG_INFO("step 1: tenant allocator prepared", KR(ret), K(tenant_id), K(ls_id));
  OK(ret);
  instance->working_mode_ = WorkingMode::MEMORY_MODE;
  ObArenaAllocator allocator;
  OuttableTestPayload arg;
  ret = build_outtable_log_arg(allocator, arg);
  LOG_INFO("step 1: LOB fields prepared", KR(ret), K(arg.lob_piece_table_id_),
      K(arg.lob_operation_seq_no_), "data_len", arg.data_.length());
  OK(ret);
  ret = log_generator.gen_lob_outtable_redo(arg);
  LOG_INFO("step 1: LOB log added to transaction block", KR(ret));
  OK(ret);
  ipalf::ILogEntry lob_entry;
  palf::LSN lob_lsn;
  ret = log_generator.gen_log_entry(lob_entry, lob_lsn);
  LOG_INFO("step 1 complete: LOB entry generated but not consumed", KR(ret), K(lob_lsn));
  OK(ret);

  // The later entry records the preceding LOB LSN and attempts to commit the same transaction.
  LOG_INFO("step 2: generate REDO, COMMIT_INFO and COMMIT in one entry", K(lob_lsn));
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  ipalf::ILogEntry commit_entry;
  palf::LSN commit_lsn;
  ret = log_generator.gen_log_entry(commit_entry, commit_lsn);
  LOG_INFO("step 2 complete: commit entry generated", KR(ret), K(lob_lsn), K(commit_lsn));
  OK(ret);

  // Commit processing must report the withheld LOB entry before it can proceed.
  LOG_INFO("step 3: consume commit entry before the LOB entry", K(commit_lsn));
  ret = ls_fetch_ctx->read_log(commit_entry, commit_lsn, missing_info, tsi, stop_flag);
  LOG_INFO("step 3: inspect missing-log result", KR(ret), K(lob_lsn), K(missing_info),
      "missing_count", missing_info.get_total_misslog_cnt(),
      "need_reconsume", missing_info.need_reconsume_commit_log_entry());
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ret);
  ASSERT_EQ(1, missing_info.get_total_misslog_cnt());
  LOG_INFO("step 3 complete: compare the reported missing LSN", K(lob_lsn),
      "missing_lsn", missing_info.get_miss_redo_lsn_arr().at(0 /*idx*/));
  EXPECT_EQ(lob_lsn, missing_info.get_miss_redo_lsn_arr().at(0 /*idx*/));
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());

  // Supply the exact missing entry manually to exercise the LOB handler in refetch mode.
  IObCDCPartTransResolver::MissingLogInfo refetch_info;
  refetch_info.set_resolving_miss_log();
  LOG_INFO("step 4: supply the missing LOB entry", K(lob_lsn));
  ret = ls_fetch_ctx->read_miss_tx_log(lob_entry, lob_lsn, tsi, refetch_info);
  LOG_INFO("step 4 complete: inspect refetch result", KR(ret), K(refetch_info),
      "missing_count", refetch_info.get_total_misslog_cnt());
  OK(ret);
  EXPECT_EQ(0, refetch_info.get_total_misslog_cnt());

  // Reconsume the same commit entry after the LOB LSN has been registered as fetched.
  IObCDCPartTransResolver::MissingLogInfo reconsume_info;
  reconsume_info.set_reconsuming();
  LOG_INFO("step 5: reconsume commit entry after refetch", K(commit_lsn));
  ret = ls_fetch_ctx->read_log(commit_entry, commit_lsn, reconsume_info, tsi, stop_flag);
  LOG_INFO("step 5 complete: inspect commit reconsumption result", KR(ret), K(reconsume_info),
      "missing_count", reconsume_info.get_total_misslog_cnt());
  OK(ret);
  EXPECT_EQ(0, reconsume_info.get_total_misslog_cnt());

  LOG_INFO("step 6: release test instance");
  DESTROY_OBLOG_INSTANCE();
  LOG_INFO("========== missing_outtable_mutator_redo_is_refetched_before_commit end ==========",
      "has_failure", ::testing::Test::HasFailure());
}

// seq1: redo + commit_info + commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq1)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// seq2: redo | redo + commit_info + commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq2)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq2
TEST(ObCDCPartTransResolver, test_sp_tx_seq2_miss)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;

  log_generator.gen_redo_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry2, lsn2));
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  LOG_DEBUG("read log2", K(lsn), K(lsn2), K(missing_info));
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn, missing_info.get_miss_redo_lsn_arr().at(0));
  missing_info.reset();
  missing_info.set_resolving_miss_log();
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, reconsume_miss_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// seq3: redo + commit_info | commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq3)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry2, lsn2));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
//  GET_PART_TRANS_TASK();
//  LOG_DEBUG("part_trans_task", KPC(part_trans_task), K(lsn), K(log_entry), K(lsn2), K(log_entry2));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(0, missing_info.get_total_misslog_cnt());

  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq3
TEST(ObCDCPartTransResolver, test_sp_tx_seq3_miss)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry, lsn));

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_log();
  EXPECT_EQ(OB_SUCCESS, log_generator.gen_log_entry(log_entry2, lsn2));
  LOG_DEBUG("test_sp_tx_seq3_miss", K(lsn), K(log_entry), K(lsn2), K(log_entry2));
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  missing_info.set_resolving_miss_log();
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, reconsume_miss_info, tsi, stop_flag));
  EXPECT_EQ(0, reconsume_miss_info.get_total_misslog_cnt());

  DESTROY_OBLOG_INSTANCE();
}

// seq4: redo | redo + commit_info | commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq4)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);
  LOG_DEBUG("test_sp_tx_seq4");

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq4(before commit_info_log)
TEST(ObCDCPartTransResolver, test_sp_tx_seq4_miss_1)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn, missing_info.get_miss_redo_lsn_arr().at(0));
  EXPECT_FALSE(missing_info.need_reconsume_commit_log_entry()); // commit_info entry is not miss_log, need reconsume
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, reconsume_miss_info, tsi, stop_flag));
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq4(before commit_log)
TEST(ObCDCPartTransResolver, test_sp_tx_seq4_miss_2)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn2, missing_info.miss_record_or_state_log_lsn_);
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_miss_tx_log(log_entry2, lsn2, tsi, new_miss_log));
  EXPECT_EQ(1, new_miss_log.get_total_misslog_cnt());
  EXPECT_EQ(lsn, new_miss_log.get_miss_redo_lsn_arr().at(0));
  EXPECT_FALSE(new_miss_log.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log_2;
  new_miss_log_2.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log_2));

  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, reconsume_miss_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// seq5: redo | commit_info | commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq5)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// fetch middle of seq5(before log_entry2)
TEST(ObCDCPartTransResolver, test_sp_tx_seq5_miss)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn, missing_info.get_miss_redo_lsn_arr().at(0));
  EXPECT_FALSE(missing_info.need_reconsume_commit_log_entry()); // commit_info entry is not miss_log, need reconsume
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// seq6: redo | rollback_to | redo + commit_info + commit
TEST(ObCDCPartTransResolver, test_sp_tx_seq6)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_rollback_to_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of seq6(before log_entry3)
TEST(ObCDCPartTransResolver, test_sp_tx_seq6_miss)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);

  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_rollback_to_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  EXPECT_EQ(2, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn, missing_info.get_miss_redo_lsn_arr().at(0));
  EXPECT_EQ(lsn2, missing_info.get_miss_redo_lsn_arr().at(1));
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry2, lsn2, tsi, new_miss_log));

  IObCDCPartTransResolver::MissingLogInfo reconsume_miss_info;
  reconsume_miss_info.set_reconsuming();

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, reconsume_miss_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// dist tx is focus on prepare log.
// dist_seq1: redo + commit_info | prepare | commit
TEST(ObCDCPartTransResolver, test_sp_tx_dist)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of dist_seq1(before prepare)
TEST(ObCDCPartTransResolver, test_sp_tx_dist_miss1)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_FALSE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  missing_info.reset();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// fetch from middle of dist_seq1(before commit)
TEST(ObCDCPartTransResolver, test_sp_tx_dist_miss2)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry3;
  palf::LSN lsn3;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry3, lsn3);

  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  EXPECT_EQ(lsn2, missing_info.miss_record_or_state_log_lsn_);
  EXPECT_TRUE(missing_info.need_reconsume_commit_log_entry());
  IObCDCPartTransResolver::MissingLogInfo new_miss_log;
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_miss_tx_log(log_entry2, lsn2, tsi, new_miss_log));
  EXPECT_EQ(1, new_miss_log.get_total_misslog_cnt());
  EXPECT_EQ(lsn, new_miss_log.miss_record_or_state_log_lsn_);
  new_miss_log.reset();
  new_miss_log.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_miss_tx_log(log_entry, lsn, tsi, new_miss_log));
  EXPECT_EQ(0, new_miss_log.get_total_misslog_cnt());
  missing_info.reset();
  missing_info.set_reconsuming();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry3, lsn3, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

// dist tx is focus on prepare log.
// dist_seq2: redo + commit_info + prepare | commit
TEST(ObCDCPartTransResolver, test_sp_tx_dist2)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_redo_log();
  log_generator.gen_commit_info_log();
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// dist tx is focus on prepare log.
// dist_seq3: commit_info + prepare | commit
TEST(ObCDCPartTransResolver, test_sp_tx_dist3)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  log_generator.gen_commit_info_log();
  log_generator.gen_prepare_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry2, lsn2);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

// dist_seq4: commit(may occour while transfer case)
// currently disabled cause transfer is not implied yet. will add case for transfer.
TEST(ObCDCPartTransResolver, DISABLED_test_sp_tx_dist4)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  log_generator.gen_commit_log();
  log_generator.gen_log_entry(log_entry, lsn);

  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  DESTROY_OBLOG_INSTANCE();
}

TEST(ObCDCPartTransResolver, test_sp_tx_record)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry_rc0;
  palf::LSN lsn_rc0;
  ipalf::ILogEntry log_entry1;
  palf::LSN lsn1;
  ipalf::ILogEntry log_entry_rc1;
  palf::LSN lsn_rc1;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry_rc2;
  palf::LSN lsn_rc2;
  // generate and log_entry below
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc0, lsn_rc0);
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry1, lsn1);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc1, lsn_rc1);
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc2, lsn_rc2);
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry_rc0, lsn_rc0, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry1, lsn1, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry_rc1, lsn_rc1, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry_rc2, lsn_rc2, missing_info, tsi, stop_flag));

  DESTROY_OBLOG_INSTANCE();
}

TEST(ObCDCPartTransResolver, test_sp_tx_record_miss)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  ipalf::ILogEntry log_entry_rc0;
  palf::LSN lsn_rc0;
  ipalf::ILogEntry log_entry1;
  palf::LSN lsn1;
  ipalf::ILogEntry log_entry_rc1;
  palf::LSN lsn_rc1;
  ipalf::ILogEntry log_entry2;
  palf::LSN lsn2;
  ipalf::ILogEntry log_entry_rc2;
  palf::LSN lsn_rc2;
  // generate and log_entry below
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry, lsn);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc0, lsn_rc0);
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry1, lsn1);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc1, lsn_rc1);
  log_generator.gen_redo_log();
  log_generator.gen_log_entry(log_entry2, lsn2);
  log_generator.gen_record_log();
  log_generator.gen_log_entry(log_entry_rc2, lsn_rc2);
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry1, lsn1, missing_info, tsi, stop_flag));
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry_rc1, lsn_rc1, missing_info, tsi, stop_flag));
  EXPECT_TRUE(missing_info.miss_record_or_state_log_lsn_.is_valid());
  EXPECT_EQ(1, missing_info.get_total_misslog_cnt());
  LOG_INFO("", K(lsn), K(lsn_rc0), K(lsn1), K(lsn_rc1), K(lsn2), K(lsn_rc2), K(missing_info));
//  EXPECT_EQ(lsn, missing_info.miss_redo_or_state_lsn_arr_.at(0));
  IObCDCPartTransResolver::MissingLogInfo missing_info1;
  missing_info1.set_resolving_miss_log();
  EXPECT_EQ(OB_ITEM_NOT_SETTED, ls_fetch_ctx->read_log(log_entry_rc0, lsn_rc0, missing_info1, tsi, stop_flag));
  EXPECT_EQ(lsn, missing_info1.get_miss_redo_lsn_arr().at(0));
  IObCDCPartTransResolver::MissingLogInfo missing_info2;
  missing_info2.set_resolving_miss_log();
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry, lsn, missing_info2, tsi, stop_flag));
  IObCDCPartTransResolver::MissingLogInfo missing_info3;
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry2, lsn2, missing_info3, tsi, stop_flag));
  EXPECT_EQ(OB_SUCCESS, ls_fetch_ctx->read_log(log_entry_rc2, lsn_rc2, missing_info3, tsi, stop_flag));
  LOG_INFO("missing_infos", K(missing_info), K(missing_info1), K(missing_info2), K(missing_info3));

  DESTROY_OBLOG_INSTANCE();
}

TEST(ObCDCPartTransResolver, test_sp_tx_seq_example)
{
  PREPARE_LS_FETCH_CTX();
  ipalf::ILogEntry log_entry;
  palf::LSN lsn;
  // generate and log_entry below

  DESTROY_OBLOG_INSTANCE();
}

}
}

int main(int argc, char **argv)
{
  // ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  system("rm -f test_ob_cdc_part_trans_resolver.log");
  ObLogger &logger = ObLogger::get_logger();
  bool not_output_obcdc_log = true;
  logger.set_file_name("test_ob_cdc_part_trans_resolver.log", not_output_obcdc_log, false);
  logger.set_log_level(OB_LOG_LEVEL_DEBUG);
  logger.set_mod_log_levels("ALL.*:DEBUG;TLOG.*:DEBUG");
  logger.set_enable_async_log(false);
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
