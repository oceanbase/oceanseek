/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX OBLOG

#include <climits>
#include <map>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/thread/ob_multi_fixed_queue_thread.h"
#include "logservice/libobcdc/src/ob_cdc_lob_ctx.h"
#define private public
#include "logservice/libobcdc/src/ob_cdc_lob_aux_storager.h"
#undef private
#include "logservice/libobcdc/src/ob_cdc_lob_aux_table_parser.h"
#include "logservice/libobcdc/src/ob_cdc_lob_data_merger.h"
#include "logservice/libobcdc/src/ob_log_instance.h"
#include "logservice/libobcdc/src/ob_log_tenant_mgr.h"
#include "logservice/libobcdc/src/ob_log_store_service.h"

namespace oceanbase
{
namespace libobcdc
{
using namespace common;
using namespace storage;

class MockLogErrHandler final : public IObLogErrHandler
{
public:
  void handle_error(const int err_no, const char *fmt, ...) override
  {
    LOG_WARN_RET(err_no, "unexpected lob merger error callback", KCSTRING(fmt));
  }
};

class ObCacheValueTenantMgr final : public ObLogTenantMgr
{
public:
  explicit ObCacheValueTenantMgr(uint64_t tenant_id) : tenant_id_(tenant_id) {}
  int get_tenant_guard(uint64_t tenant_id, ObLogTenantGuard &guard) override
  {
    if (tenant_id != tenant_id_) { return OB_ENTRY_NOT_EXIST; }
    guard.set_tenant(&tenant_);
    return OB_SUCCESS;
  }
  int revert_tenant(ObLogTenant *tenant) override
  { return tenant == &tenant_ ? OB_SUCCESS : OB_INVALID_ARGUMENT; }
private:
  uint64_t tenant_id_;
  ObLogTenant tenant_;
};

// The disk-tier adapter keeps serialized bytes exactly as the production store
// would. Encoding/decoding, tier selection and range cleanup remain real code.
class ObCacheValueStore final : public IObStoreService
{
public:
  int init(const std::string &) override { return OB_SUCCESS; }
  void mark_stop_flag() override {}
  int close() override { return OB_SUCCESS; }
  int put(const std::string &key, const ObSlice &value) override
  { data_[key] = std::string(value.buf_, value.buf_len_); return OB_SUCCESS; }
  int put(void *, const std::string &key, const ObSlice &value) override { return put(key, value); }
  int batch_write(void *, const std::vector<std::string> &, const std::vector<ObSlice> &) override
  { return OB_NOT_SUPPORTED; }
  int get(const std::string &key, std::string &value) override
  {
    const auto it = data_.find(key);
    if (it == data_.end()) { return OB_ENTRY_NOT_EXIST; }
    value = it->second;
    return OB_SUCCESS;
  }
  int get(void *, const std::string &key, std::string &value) override { return get(key, value); }
  int del(const std::string &key) override { data_.erase(key); return OB_SUCCESS; }
  int del(void *, const std::string &key) override { return del(key); }
  int del_range(void *, const std::string &begin, const std::string &end) override
  { data_.erase(data_.lower_bound(begin), data_.lower_bound(end)); return OB_SUCCESS; }
  int compact_range(void *, const std::string &, const std::string &, bool) override { return OB_SUCCESS; }
  int flush(void *) override { return OB_SUCCESS; }
  int create_column_family(const std::string &, void *&) override { return OB_NOT_SUPPORTED; }
  int drop_column_family(void *) override { return OB_SUCCESS; }
  int destory_column_family(void *) override { return OB_SUCCESS; }
  void get_mem_usage(const std::vector<uint64_t>, const std::vector<void *>) override {}
  int get_mem_usage(void *, int64_t &bytes, int64_t &keys) override
  { bytes = 0; keys = data_.size(); return OB_SUCCESS; }
  std::map<std::string, std::string> data_;
};

class ObCacheValueEnvironmentGuard final
{
public:
  ObCacheValueEnvironmentGuard(ObLogInstance &instance, IObLogTenantMgr &tenant_mgr)
      : instance_(instance), saved_mgr_(instance.tenant_mgr_),
        saved_memory_(instance.lob_aux_storager_.enable_memory_)
  { instance_.tenant_mgr_ = &tenant_mgr; }
  ~ObCacheValueEnvironmentGuard()
  {
    instance_.tenant_mgr_ = saved_mgr_;
    instance_.lob_aux_storager_.enable_memory_ = saved_memory_;
  }
private:
  ObLogInstance &instance_;
  IObLogTenantMgr *saved_mgr_;
  bool saved_memory_;
};

class TestCDCLobSummaryCompatibility : public ::testing::Test
{
protected:
  void SetUp() override
  {
    expected_lob_id_.set_column_idx(LOB_COLUMN_IDX);
    expected_lob_id_.set_tablet_id(1001);
    expected_lob_id_.set_auto_inc_id(1);
    expected_summary_seq_id_.assign_ptr(summary_seq_buf_, sizeof(summary_seq_buf_));
    ASSERT_EQ(OB_SUCCESS, ObLobSeqId::get_summary_seq_id(expected_summary_seq_id_));
    ASSERT_TRUE(expected_lob_id_.is_valid());
  }

  void TearDown() override
  {
    merger_.stop();
    merger_.destroy();
    if (nullptr != instance_) {
      instance_->lob_aux_storager_.destroy();
      ObLogInstance::destroy_instance();
      instance_ = nullptr;
    }
  }

  int init_merger_environment(IObStoreService *store_service = nullptr)
  {
    int ret = OB_SUCCESS;
    lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
    if (OB_ISNULL(malloc_allocator)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get malloc allocator failed", KR(ret));
    } else {
      // Merger 的 Piece seq id 使用业务租户 allocator；单测需显式创建租户并设置额度。
      const int create_ret = malloc_allocator->create_and_add_tenant_allocator(TENANT_ID);
      if (OB_SUCCESS != create_ret && OB_ENTRY_EXIST != create_ret) {
        ret = create_ret;
        LOG_WARN("create test tenant allocator failed", KR(ret), K(TENANT_ID));
      } else if (OB_FAIL(malloc_allocator->set_tenant_limit(TENANT_ID, 1L << 30))) {
        LOG_WARN("set test tenant memory limit failed", KR(ret), K(TENANT_ID));
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(instance_ = ObLogInstance::get_instance())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get ObLogInstance failed", KR(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(instance_->lob_aux_storager_.init(store_service))) {
      LOG_WARN("init lob aux storager failed", KR(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(
        merger_.init(1/*thread_num*/, 16/*queue_size*/, err_handler_))) {
      LOG_WARN("init lob data merger failed", KR(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(merger_.start())) {
      LOG_WARN("start lob data merger failed", KR(ret));
    }

    return ret;
  }

  int build_summary_cache_value(
      const char *lob_chunk_type,
      const uint64_t byte_len,
      const uint64_t chunk_count,
      ObIAllocator &allocator,
      ObString &summary_value)
  {
    int ret = OB_SUCCESS;
    ObLobMetaInfo meta_info;
    ObLobMetaExtraInfo extra_info;
    extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
    extra_info.chunk_count_ = chunk_count;
    extra_info.last_write_type_ = ObLobLastWriteType::INSERT;
    if (OB_FAIL(extra_info.lob_chunk_type_.from_string(lob_chunk_type))) {
      LOG_WARN("parse lob chunk type failed", KR(ret), KCSTRING(lob_chunk_type));
    } else if (OB_FAIL(extra_info.serialize(allocator, meta_info.lob_extra_info_))) {
      LOG_WARN("serialize lob summary extra info failed", KR(ret), K(extra_info));
    } else {
      meta_info.byte_len_ = byte_len;
      meta_info.char_len_ = byte_len;
      if (OB_FAIL(ObCDCLobAuxTableParser::encode_summary_value(
          allocator, meta_info, summary_value))) {
        LOG_WARN("encode lob summary cache value failed", KR(ret), K(meta_info));
      }
    }

    return ret;
  }

  int prepare_part_trans_task(
      PartTransTask &part_trans_task,
      const transaction::ObTransID &trans_id,
      const int64_t commit_version)
  {
    int ret = OB_SUCCESS;
    const logservice::TenantLSID tls_id(TENANT_ID, share::ObLSID(LS_ID));
    transaction::ObLSLogInfoArray participants;
    part_trans_task.set_task_info(tls_id, "1002_1001");
    part_trans_task.set_trans_id(trans_id);
    part_trans_task.mark_read_prepare();
    if (OB_FAIL(part_trans_task.commit(CLUSTER_ID, trans_id, commit_version,
        transaction::TransType::SP_TRANS, participants, palf::LSN(commit_version),
        commit_version/*commit_log_submit_ts*/, false/*is_data_dict_mode*/))) {
      LOG_WARN("commit part trans task failed", KR(ret), K(trans_id), K(commit_version));
    }
    return ret;
  }

  int run_merger_with_summary(
      const char *lob_chunk_type,
      const uint64_t byte_len,
      const uint64_t chunk_count,
      const char *piece_data,
      const char *raw_extra_info = nullptr)
  {
    int ret = OB_SUCCESS;
    const int64_t commit_version = next_commit_version_++;
    const transaction::ObTransID trans_id(commit_version);
    const transaction::ObTxSEQ lob_operation_seq_no(10, 0);
    char piece_seq_buf[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
    ObString piece_seq_id(sizeof(piece_seq_buf), piece_seq_buf);
    ObArenaAllocator allocator;
    ObString summary_value;
    ObString expected_piece;
    PartTransTask part_trans_task;
    // PartTransTask::commit() 会分配事务标识字符串，栈构造任务需显式绑定 backing allocator。
    part_trans_task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
    IStmtTask stmt_task(IStmtTask::STMT_TYPE_DML, part_trans_task);
    ObLobDataOutRowCtxList row_ctx_list(allocator);
    ObLobDataGetCtx lob_data_get_ctx;
    const blocksstable::ObDmlRowFlag insert_flag(blocksstable::ObDmlFlag::DF_INSERT);
    bool summary_published = false;
    bool piece_published = false;
    volatile bool stop_flag = false;

    if (OB_FAIL(ObLobSeqId::get_seq_id(1, piece_seq_id))) {
      LOG_WARN("generate lob piece seq id failed", KR(ret));
    } else if (nullptr != piece_data) {
      expected_piece = ObString::make_string(piece_data);
      if (OB_UNLIKELY(1 != chunk_count
          || byte_len != static_cast<uint64_t>(expected_piece.length()))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid test piece data", KR(ret), K(byte_len), K(chunk_count),
            "piece_len", expected_piece.length());
      }
    }

    const ObLobAuxKey summary_key(commit_version, TENANT_ID, trans_id, LOB_META_TABLE_ID,
        expected_lob_id_, lob_operation_seq_no, expected_summary_seq_id_);
    const ObLobAuxKey piece_key(commit_version, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
        expected_lob_id_, lob_operation_seq_no, piece_seq_id);

    if (OB_SUCC(ret) && nullptr != raw_extra_info) {
      // Inject bytes directly so unsupported types reach the real Merger
      // decoder instead of being rejected while constructing the fixture.
      ObLobMetaInfo meta_info;
      meta_info.byte_len_ = byte_len;
      meta_info.char_len_ = byte_len;
      meta_info.lob_extra_info_ = ObString::make_string(raw_extra_info);
      ret = ObCDCLobAuxTableParser::encode_summary_value(allocator, meta_info, summary_value);
    } else if (OB_SUCC(ret) && OB_FAIL(build_summary_cache_value(
        lob_chunk_type, byte_len, chunk_count, allocator, summary_value))) {
      LOG_WARN("build lob summary cache value failed", KR(ret), KCSTRING(lob_chunk_type),
          K(byte_len), K(chunk_count));
    }
    if (OB_SUCC(ret) && OB_FAIL(
        prepare_part_trans_task(part_trans_task, trans_id, commit_version))) {
      LOG_WARN("prepare part trans task failed", KR(ret), K(trans_id), K(commit_version));
    }
    if (OB_SUCC(ret)) {
      row_ctx_list.reset(&stmt_task, TENANT_ID, trans_id,
          LOB_META_TABLE_ID, LOB_PIECE_TABLE_ID, true/*is_ddl*/);
      if (OB_FAIL(lob_data_get_ctx.reset(&row_ctx_list, LOB_COLUMN_IDX, insert_flag,
          expected_lob_id_, lob_operation_seq_no, true/*is_new_col*/))) {
        LOG_WARN("init lob data get context failed", KR(ret), K(expected_lob_id_),
            K(lob_operation_seq_no));
      }
    }
    if (OB_SUCC(ret)) {
      row_ctx_list.add(&lob_data_get_ctx);
      if (OB_FAIL(instance_->lob_aux_storager_.put(
          summary_key, "insert", summary_value.ptr(), summary_value.length()))) {
        LOG_WARN("put lob summary cache failed", KR(ret), K(summary_key),
            "summary_len", summary_value.length());
      } else {
        summary_published = true;
      }
    }
    if (OB_SUCC(ret) && nullptr != piece_data) {
      if (OB_FAIL(instance_->lob_aux_storager_.put(
          piece_key, "insert", expected_piece.ptr(), expected_piece.length()))) {
        LOG_WARN("put lob piece cache failed", KR(ret), K(piece_key),
            "piece_len", expected_piece.length());
      } else {
        piece_published = true;
      }
    }
    if (OB_SUCC(ret)) {
      // DDL 同步入口与 DML Prepare 任务共用 prepare_lob_image_() 的 Summary 校验分支。
      ret = merger_.merge_ddl_lob(row_ctx_list, stop_flag);
      LOG_INFO("merge ddl lob finished", KR(ret), KCSTRING(lob_chunk_type),
          K(byte_len), K(chunk_count));
      if (OB_SUCCESS == ret && nullptr != piece_data
          && 0 != expected_piece.compare(lob_data_get_ctx.get_new_lob_column_value())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("merged lob value mismatch", KR(ret), "expected_len", expected_piece.length(),
            "actual_len", lob_data_get_ctx.get_new_lob_column_value().length());
      }
    }

    if (piece_published) {
      const int piece_del_ret = instance_->lob_aux_storager_.del(piece_key);
      if (OB_SUCCESS != piece_del_ret) {
        LOG_WARN_RET(piece_del_ret, "delete lob piece cache failed", K(piece_key));
      }
      EXPECT_EQ(OB_SUCCESS, piece_del_ret);
    }
    if (summary_published) {
      const int summary_del_ret = instance_->lob_aux_storager_.del(summary_key);
      if (OB_SUCCESS != summary_del_ret) {
        LOG_WARN_RET(summary_del_ret, "delete lob summary cache failed", K(summary_key));
      }
      EXPECT_EQ(OB_SUCCESS, summary_del_ret);
    }
    return ret;
  }

  int build_summary_row(const char *extra_info_json, const uint64_t byte_len)
  {
    int ret = OB_SUCCESS;
    cols_.reset();
    for (int64_t idx = 0; idx < ObLobMetaInfo::COLUMN_CNT; ++idx) {
      col_values_[idx].reset();
      col_values_[idx].column_id_ = OB_APP_MIN_COLUMN_ID + idx;
    }

    const ObString lob_id_string = expected_lob_id_.get_string();
    const ObString extra_info_string = ObString::make_string(extra_info_json);
    col_values_[ObLobMetaUtil::LOB_COL_IDX_COL_ID].value_.set_int(LOB_COLUMN_IDX);
    col_values_[ObLobMetaUtil::LOB_ID_COL_ID].value_.set_varchar(lob_id_string);
    col_values_[ObLobMetaUtil::SEQ_ID_COL_ID].value_.set_varchar(expected_summary_seq_id_);
    col_values_[ObLobMetaUtil::BYTE_LEN_COL_ID].value_.set_uint64(byte_len);
    col_values_[ObLobMetaUtil::CHAR_LEN_COL_ID].value_.set_uint64(byte_len);
    col_values_[ObLobMetaUtil::LOB_INFO_COL_ID].value_.set_varchar(extra_info_string);
    col_values_[ObLobMetaUtil::LOB_BLOCK_ID_COL_ID].value_.set_null();

    for (int64_t idx = 0; OB_SUCC(ret) && idx < ObLobMetaInfo::COLUMN_CNT; ++idx) {
      if (OB_FAIL(cols_.add(&col_values_[idx]))) {
        LOG_WARN("add lob meta column value failed", KR(ret), K(idx));
      }
    }
    return ret;
  }

protected:
  static constexpr int64_t LOB_COLUMN_IDX = 3;
  static constexpr uint64_t TENANT_ID = 1002;
  static constexpr uint64_t LS_ID = 1001;
  static constexpr uint64_t CLUSTER_ID = 1;
  static constexpr uint64_t LOB_META_TABLE_ID = 2001;
  static constexpr uint64_t LOB_PIECE_TABLE_ID = 2002;
  MockLogErrHandler err_handler_;
  ObCDCLobDataMerger merger_;
  ObLogInstance *instance_ = nullptr;
  int64_t next_commit_version_ = 1000;
  ColValueList cols_;
  ColValue col_values_[ObLobMetaInfo::COLUMN_CNT];
  ObLobId expected_lob_id_;
  char summary_seq_buf_[ObLobSeqId::LOB_SEQ_ID_MIN_LEN];
  ObString expected_summary_seq_id_;
};

// 验证历史 ObLobWriteIter::build_summary_() 写出的 INVALID_TYPE Summary
// 能被 CDC 正常解析和发布，并在缓存编解码后完整保留长度、分片数和类型信息。
TEST_F(TestCDCLobSummaryCompatibility, parser_accepts_historical_invalid_type_summary)
{
  const char *extra_info_json =
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"INVALID_TYPE\","
      "\"chunk_count\":2,\"last_write_type\":0}";
  ASSERT_EQ(OB_SUCCESS, build_summary_row(extra_info_json, 8/*byte_len*/));

  ObArenaAllocator allocator;
  bool need_publish = false;
  ObLobId lob_id;
  ObString seq_id;
  ObString cache_value;
  ASSERT_EQ(OB_SUCCESS, ObCDCLobAuxTableParser::parse_lob_aux_table_row(
      ObCDCLobAuxTableType::META, cols_, allocator, need_publish, lob_id, seq_id, cache_value));
  EXPECT_TRUE(need_publish);
  EXPECT_TRUE(expected_lob_id_ == lob_id);
  EXPECT_EQ(0, expected_summary_seq_id_.compare(seq_id));
  EXPECT_FALSE(cache_value.empty());

  uint64_t byte_len = 0;
  uint64_t char_len = 0;
  ObLobMetaExtraInfo extra_info;
  ASSERT_EQ(OB_SUCCESS, ObCDCLobAuxTableParser::decode_summary_value(
      allocator, cache_value, byte_len, char_len, extra_info));
  EXPECT_EQ(8U, byte_len);
  EXPECT_EQ(8U, char_len);
  EXPECT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY, extra_info.extra_info_type_);
  EXPECT_FALSE(extra_info.lob_chunk_type_.is_valid());
  EXPECT_EQ(2U, extra_info.chunk_count_);
  EXPECT_EQ(ObLobLastWriteType::INSERT, extra_info.last_write_type_);
}

TEST_F(TestCDCLobSummaryCompatibility, parser_accepts_out_table_summary)
{
  const char *extra_info_json =
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"OUT_TABLE\","
      "\"chunk_count\":2,\"last_write_type\":0}";
  ASSERT_EQ(OB_SUCCESS, build_summary_row(extra_info_json, 8/*byte_len*/));

  ObArenaAllocator allocator;
  bool need_publish = true;
  ObLobId lob_id;
  ObString seq_id = ObString::make_string("stale-seq-id");
  ObString cache_value = ObString::make_string("stale-cache-value");
  ASSERT_EQ(OB_SUCCESS, ObCDCLobAuxTableParser::parse_lob_aux_table_row(
      ObCDCLobAuxTableType::META, cols_, allocator, need_publish, lob_id, seq_id, cache_value));
  EXPECT_TRUE(need_publish);
  EXPECT_TRUE(expected_lob_id_ == lob_id);
  EXPECT_EQ(0, expected_summary_seq_id_.compare(seq_id));
  uint64_t byte_len = 0;
  uint64_t char_len = 0;
  ObLobMetaExtraInfo extra_info;
  ASSERT_EQ(OB_SUCCESS, ObCDCLobAuxTableParser::decode_summary_value(
      allocator, cache_value, byte_len, char_len, extra_info));
  EXPECT_EQ(8U, byte_len);
  EXPECT_EQ(8U, char_len);
  EXPECT_EQ(2U, extra_info.chunk_count_);
  EXPECT_TRUE(extra_info.lob_chunk_type_.out_table());
}

// 通过真实 ObCDCLobDataMerger 同步拼接路径验证历史 INVALID_TYPE 和当前 IN_TABLE Summary 都会放行，
// 并能从真实 ObCDCLobAuxStorager 读取 Piece 后拼出完整 LOB 数据。
TEST_F(TestCDCLobSummaryCompatibility, merger_accepts_invalid_type_and_in_table_summary)
{
  ASSERT_EQ(OB_SUCCESS, init_merger_environment());
  EXPECT_EQ(OB_SUCCESS,
      run_merger_with_summary("INVALID_TYPE", 8/*byte_len*/, 1/*chunk_count*/, "lob-data"));
  EXPECT_EQ(OB_SUCCESS,
      run_merger_with_summary("IN_TABLE", 8/*byte_len*/, 1/*chunk_count*/, "lob-data"));
}

// Unknown chunk types must fail during decoding in both modes, without fetching
// pieces or padding a malformed Summary.
TEST_F(TestCDCLobSummaryCompatibility, merger_rejects_unknown_chunk_type_in_both_modes)
{
  ASSERT_EQ(OB_SUCCESS, init_merger_environment());
  const char *raw_extra_info =
      "{\"extra_info_type\":1,\"lob_chunk_type\":\"UNKNOWN_CHUNK_TYPE\","
      "\"chunk_count\":1,\"last_write_type\":0}";
  for (const auto mode : {ObUnsupportedLobHandlingMode::ERROR, ObUnsupportedLobHandlingMode::PADDING}) {
    instance_->unsupported_lob_handling_mode_ = mode;
    EXPECT_NE(OB_SUCCESS, run_merger_with_summary(
        "UNKNOWN_CHUNK_TYPE", 8/*byte_len*/, 1/*chunk_count*/, nullptr, raw_extra_info));
  }
}

TEST_F(TestCDCLobSummaryCompatibility, merger_rejects_no_logging_summary_in_both_modes)
{
  ASSERT_EQ(OB_SUCCESS, init_merger_environment());
  for (const auto mode : {ObUnsupportedLobHandlingMode::ERROR, ObUnsupportedLobHandlingMode::PADDING}) {
    instance_->unsupported_lob_handling_mode_ = mode;
    const int64_t commit_version = next_commit_version_++;
    const transaction::ObTransID trans_id(commit_version);
    const transaction::ObTxSEQ lob_operation_seq_no(10, 0);
    ObArenaAllocator allocator;
    PartTransTask part_trans_task;
    part_trans_task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
    ASSERT_EQ(OB_SUCCESS, prepare_part_trans_task(part_trans_task, trans_id, commit_version));
    IStmtTask stmt_task(IStmtTask::STMT_TYPE_DML, part_trans_task);
    ObLobDataOutRowCtxList row_ctx_list(allocator);
    row_ctx_list.reset(&stmt_task, TENANT_ID, trans_id,
        LOB_META_TABLE_ID, LOB_PIECE_TABLE_ID, false/*is_ddl*/);
    ObLobDataGetCtx lob_data_get_ctx;
    const blocksstable::ObDmlRowFlag insert_flag(blocksstable::ObDmlFlag::DF_INSERT);
    ASSERT_EQ(OB_SUCCESS, lob_data_get_ctx.reset(&row_ctx_list, LOB_COLUMN_IDX, insert_flag,
        expected_lob_id_, lob_operation_seq_no, true/*is_new_col*/));
    row_ctx_list.add(&lob_data_get_ctx);
    const ObLobAuxKey summary_key(commit_version, TENANT_ID, trans_id, LOB_META_TABLE_ID,
        expected_lob_id_, lob_operation_seq_no, expected_summary_seq_id_);
    ASSERT_EQ(OB_SUCCESS, instance_->lob_aux_storager_.put_no_logging(summary_key));

    // A marker is valid only for a Piece. Reject it before image initialization,
    // fragment dispatch or completion, even when ordinary unsupported LOBs may pad.
    const int8_t pending_images = lob_data_get_ctx.lob_col_value_handle_done_count_;
    ObLobImagePrepareTask prepare_task(lob_data_get_ctx);
    volatile bool stop_flag = false;
    EXPECT_EQ(OB_ERR_UNEXPECTED, merger_.handle(&prepare_task, 0/*thread_index*/, stop_flag));
    const ObLobColCtx &col_ctx = lob_data_get_ctx.get_lob_col_ctx(true/*is_new_col*/);
    EXPECT_FALSE(col_ctx.is_padding());
    EXPECT_EQ(nullptr, col_ctx.fragment_cb_array_);
    EXPECT_EQ(0U, col_ctx.chunk_count_);
    EXPECT_EQ(0U, col_ctx.get_col_ref_cnt());
    EXPECT_EQ(pending_images, lob_data_get_ctx.lob_col_value_handle_done_count_);
    EXPECT_FALSE(row_ctx_list.is_all_lob_callback_done());
    int64_t task_count = -1;
    merger_.get_task_count(task_count);
    EXPECT_EQ(0, task_count);
    int64_t queue_capacity = 0;
    ASSERT_EQ(OB_SUCCESS, merger_.get_frag_queue_info(task_count, queue_capacity));
    EXPECT_EQ(0, task_count);
    EXPECT_EQ(OB_SUCCESS, instance_->lob_aux_storager_.del(summary_key));
  }
}

// 通过真实 ObCDCLobDataMerger 校验分支验证 INVALID_TYPE 只获得类型兼容，
// 仍必须满足 byte_len 上限、空值与分片数一致性以及 chunk_count 不大于 byte_len 的约束。
TEST_F(TestCDCLobSummaryCompatibility, merger_keeps_invalid_type_summary_bounds)
{
  ASSERT_EQ(OB_SUCCESS, init_merger_environment());
  EXPECT_EQ(OB_SUCCESS,
      run_merger_with_summary("INVALID_TYPE", 0/*byte_len*/, 0/*chunk_count*/, nullptr));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      run_merger_with_summary("INVALID_TYPE", 1/*byte_len*/, 0/*chunk_count*/, nullptr));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      run_merger_with_summary("INVALID_TYPE", 0/*byte_len*/, 1/*chunk_count*/, nullptr));
  EXPECT_EQ(OB_ERR_UNEXPECTED,
      run_merger_with_summary("INVALID_TYPE", 1/*byte_len*/, 2/*chunk_count*/, nullptr));
  EXPECT_EQ(OB_ERR_UNEXPECTED, run_merger_with_summary(
      "INVALID_TYPE", static_cast<uint64_t>(INT64_MAX)/*byte_len*/, 1/*chunk_count*/, nullptr));
}

TEST_F(TestCDCLobSummaryCompatibility, parser_publishes_external_summary_and_ddl_merger_rejects_in_both_modes)
{
  ASSERT_EQ(OB_SUCCESS, init_merger_environment());
  ObArenaAllocator allocator;
  ObLobMetaExtraInfo expected_info;
  expected_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  expected_info.last_write_type_ = ObLobLastWriteType::INSERT;
  expected_info.lob_chunk_type_.set_external_reference();
  expected_info.chunk_count_ = 0;
  expected_info.external_lob_info_.url_ = ObString::make_string("oss://example/object");
  expected_info.external_lob_info_.etag_ = ObString::make_string("object-etag");
  expected_info.external_lob_info_.offset_ = 7;
  expected_info.external_lob_info_.length_ = 8;
  expected_info.external_lob_info_.kind_ = ObExternalBlobKind::REFERENCE;
  ASSERT_TRUE(expected_info.external_lob_info_.is_valid());
  ObString info;
  ASSERT_EQ(OB_SUCCESS, expected_info.serialize(allocator, info));
  const std::string json(info.ptr(), info.length());

  // An external reference has no chunks, even when its descriptor is valid.
  expected_info.chunk_count_ = 1;
  ObString malformed_info;
  ASSERT_EQ(OB_SUCCESS, expected_info.serialize(allocator, malformed_info));
  const std::string malformed_json(malformed_info.ptr(), malformed_info.length());
  expected_info.chunk_count_ = 0;

  for (const auto mode : {ObUnsupportedLobHandlingMode::ERROR, ObUnsupportedLobHandlingMode::PADDING}) {
    instance_->unsupported_lob_handling_mode_ = mode;
    ASSERT_EQ(OB_SUCCESS, build_summary_row(json.c_str(), 8));
    bool need_publish = false;
    ObLobId lob_id;
    ObString seq_id;
    ObString cache_value;

    // Auxiliary parsing publishes valid descriptors independently of the handling mode.
    ASSERT_EQ(OB_SUCCESS, ObCDCLobAuxTableParser::parse_lob_aux_table_row(
        ObCDCLobAuxTableType::META, cols_, allocator,
        need_publish, lob_id, seq_id, cache_value));
    EXPECT_TRUE(need_publish);
    EXPECT_TRUE(expected_lob_id_ == lob_id);
    EXPECT_EQ(0, expected_summary_seq_id_.compare(seq_id));
    EXPECT_FALSE(cache_value.empty());

    uint64_t byte_len = 0;
    uint64_t char_len = 0;
    ObLobMetaExtraInfo decoded_info;
    ASSERT_EQ(OB_SUCCESS, ObCDCLobAuxTableParser::decode_summary_value(
        allocator, cache_value, byte_len, char_len, decoded_info));
    EXPECT_EQ(8U, byte_len);
    EXPECT_EQ(8U, char_len);
    EXPECT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY, decoded_info.extra_info_type_);
    EXPECT_TRUE(decoded_info.lob_chunk_type_.external_reference());
    EXPECT_EQ(0U, decoded_info.chunk_count_);
    EXPECT_EQ(ObLobLastWriteType::INSERT, decoded_info.last_write_type_);
    EXPECT_TRUE(decoded_info.external_lob_info_.is_valid());
    EXPECT_EQ(0, expected_info.external_lob_info_.url_.compare(decoded_info.external_lob_info_.url_));
    EXPECT_EQ(0, expected_info.external_lob_info_.etag_.compare(decoded_info.external_lob_info_.etag_));
    EXPECT_EQ(7U, decoded_info.external_lob_info_.offset_);
    EXPECT_EQ(8U, decoded_info.external_lob_info_.length_);
    EXPECT_EQ(ObExternalBlobKind::REFERENCE, decoded_info.external_lob_info_.kind_);
    EXPECT_TRUE(decoded_info.external_lob_info_.has_length_);

    // DDL still rejects external references in the real Merger, including padding mode.
    EXPECT_EQ(OB_NOT_SUPPORTED, run_merger_with_summary(
        "EXTERNAL_REFERENCE", 8/*byte_len*/, 0/*chunk_count*/, nullptr, json.c_str()));

    ASSERT_EQ(OB_SUCCESS, build_summary_row(malformed_json.c_str(), 8));
    EXPECT_EQ(OB_ERR_UNEXPECTED, ObCDCLobAuxTableParser::parse_lob_aux_table_row(
        ObCDCLobAuxTableType::META, cols_, allocator,
        need_publish, lob_id, seq_id, cache_value));
    EXPECT_FALSE(need_publish);
    EXPECT_TRUE(seq_id.empty());
    EXPECT_TRUE(cache_value.empty());
  }
}

TEST_F(TestCDCLobSummaryCompatibility, typed_cache_preserves_binary_payload_and_no_logging_in_both_tiers)
{
  ObCacheValueStore store;
  ObCacheValueTenantMgr tenant_mgr(TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, init_merger_environment(&store));
  ObCacheValueEnvironmentGuard guard(*instance_, tenant_mgr);
  ObCDCLobAuxStorager &storager = instance_->lob_aux_storager_;
  ObArenaAllocator allocator;
  // A real payload identical to the marker's header must still be a payload.
  const char marker_bytes[] = {static_cast<char>(ObLobAuxReadValueType::NO_LOGGING)};
  const char binary_payload[] = {static_cast<char>(ObLobAuxReadValueType::PAYLOAD),
      'a', '\0', static_cast<char>(ObLobAuxReadValueType::NO_LOGGING)};
  for (int tier = 0; tier < 2; ++tier) {
    storager.enable_memory_ = (0 == tier);
    const int64_t commit_version = 5000 + tier;
    const transaction::ObTransID trans_id(commit_version);
    const ObLobAuxKey payload_key(commit_version, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
        expected_lob_id_, transaction::ObTxSEQ(10, 0), expected_summary_seq_id_);
    const ObLobAuxKey marker_key(commit_version, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
        expected_lob_id_, transaction::ObTxSEQ(11, 0), expected_summary_seq_id_);
    const ObLobAuxKey binary_payload_key(commit_version, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
        expected_lob_id_, transaction::ObTxSEQ(12, 0), expected_summary_seq_id_);
    ASSERT_EQ(OB_SUCCESS, storager.put(payload_key, "test_payload", marker_bytes, sizeof(marker_bytes)));
    ASSERT_EQ(OB_SUCCESS, storager.put(binary_payload_key, "test_binary_payload",
        binary_payload, sizeof(binary_payload)));
    ASSERT_EQ(OB_SUCCESS, storager.put_no_logging(marker_key));
    ObLobAuxReadValue value;
    ASSERT_EQ(OB_SUCCESS, storager.get(allocator, payload_key, value));
    EXPECT_FALSE(value.is_no_logging());
    EXPECT_EQ(std::string(marker_bytes, sizeof(marker_bytes)),
        std::string(value.payload_.ptr(), value.payload_.length()));
    ASSERT_EQ(OB_SUCCESS, storager.get(allocator, binary_payload_key, value));
    EXPECT_FALSE(value.is_no_logging());
    EXPECT_EQ(std::string(binary_payload, sizeof(binary_payload)),
        std::string(value.payload_.ptr(), value.payload_.length()));
    ASSERT_EQ(OB_SUCCESS, storager.get(allocator, marker_key, value));
    EXPECT_TRUE(value.is_no_logging());
    EXPECT_TRUE(value.payload_.empty());
    if (1 == tier) {
      // Read disk entries with memory caching enabled, as after spillover.
      storager.enable_memory_ = true;
      ASSERT_EQ(OB_SUCCESS, storager.get(allocator, marker_key, value));
      EXPECT_TRUE(value.is_no_logging());
    }
  }
  ObCDCLobAuxDataCleanTask clean;
  clean.tenant_id_ = TENANT_ID;
  clean.commit_version_ = 5002;
  ASSERT_EQ(OB_SUCCESS, storager.clean_unused_data(&clean));
  EXPECT_EQ(0, storager.lob_aux_map_.count());
  EXPECT_TRUE(store.data_.empty());
}

TEST_F(TestCDCLobSummaryCompatibility, typed_cache_rejects_malformed_disk_value)
{
  ObCacheValueStore store;
  ObCacheValueTenantMgr tenant_mgr(TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, init_merger_environment(&store));
  ObCacheValueEnvironmentGuard guard(*instance_, tenant_mgr);
  ObCDCLobAuxStorager &storager = instance_->lob_aux_storager_;
  storager.enable_memory_ = false;
  const ObLobAuxKey key(5000, TENANT_ID, transaction::ObTransID(5000), LOB_PIECE_TABLE_ID,
      expected_lob_id_, transaction::ObTxSEQ(10, 0), expected_summary_seq_id_);
  ASSERT_EQ(OB_SUCCESS, storager.put_no_logging(key));
  std::string disk_key;
  ASSERT_EQ(OB_SUCCESS, key.get_key(disk_key));
  ObArenaAllocator allocator;
  ObLobAuxReadValue value;
  const std::string valid_marker = store.data_[disk_key];
  ASSERT_EQ(1U, valid_marker.length());
  EXPECT_EQ(static_cast<char>(ObLobAuxReadValueType::NO_LOGGING), valid_marker[0]);
  store.data_[disk_key][0] = 2; // unknown value type
  EXPECT_EQ(OB_ERR_UNEXPECTED, storager.get(allocator, key, value));
  store.data_[disk_key] = valid_marker + "unexpected-payload";
  EXPECT_EQ(OB_ERR_UNEXPECTED, storager.get(allocator, key, value));
  store.data_[disk_key].assign(1, static_cast<char>(ObLobAuxReadValueType::PAYLOAD));
  EXPECT_EQ(OB_ERR_UNEXPECTED, storager.get(allocator, key, value));
  EXPECT_EQ(OB_INVALID_DATA, ObCDCLobAuxStorager::decode_value_(valid_marker.data(), 0, value));
}

TEST_F(TestCDCLobSummaryCompatibility, summary_validation_rejects_unknown_chunk_type_before_incremental_type)
{
  ASSERT_EQ(OB_SUCCESS, init_merger_environment());
  ObArenaAllocator allocator;
  for (const auto mode : {ObUnsupportedLobHandlingMode::ERROR, ObUnsupportedLobHandlingMode::PADDING}) {
    instance_->unsupported_lob_handling_mode_ = mode;
    for (const auto write_type : {ObLobLastWriteType::APPEND, ObLobLastWriteType::MULTIPART_WRITE}) {
      const std::string json =
          "{\"extra_info_type\":1,\"lob_chunk_type\":\"UNKNOWN_CHUNK_TYPE\","
          "\"chunk_count\":1,\"last_write_type\":"
          + std::to_string(static_cast<int>(write_type)) + "}";
      ASSERT_EQ(OB_SUCCESS, build_summary_row(json.c_str(), 8));
      bool need_publish = false;
      ObLobId lob_id;
      ObString seq_id;
      ObString cache_value;
      EXPECT_NE(OB_SUCCESS, ObCDCLobAuxTableParser::parse_lob_aux_table_row(
          ObCDCLobAuxTableType::META, cols_, allocator, need_publish, lob_id, seq_id, cache_value));
      EXPECT_FALSE(need_publish);
    }
  }
  ObLobMetaExtraInfo extra_info;
  extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
  extra_info.chunk_count_ = 1;
  extra_info.last_write_type_ = ObLobLastWriteType::MULTIPART_WRITE;
  extra_info.lob_chunk_type_.set_out_table();
  EXPECT_EQ(OB_SUCCESS, ObCDCLobAuxTableParser::validate_summary_info(extra_info, 8));
  extra_info.last_write_type_ = ObLobLastWriteType::APPEND;
  EXPECT_EQ(OB_SUCCESS, ObCDCLobAuxTableParser::validate_summary_info(extra_info, 8));
  extra_info.chunk_count_ = 0;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObCDCLobAuxTableParser::validate_summary_info(extra_info, 8));
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  system("rm -f ./test_cdc_lob_summary_compatibility.log*");
  logger.set_file_name("test_cdc_lob_summary_compatibility.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
