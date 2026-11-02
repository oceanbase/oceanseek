/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX OBLOG

#include <climits>
#include <gtest/gtest.h>

#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/page_arena.h"
#include "logservice/libobcdc/src/ob_cdc_lob_aux_table_parser.h"
#include "logservice/libobcdc/src/ob_cdc_lob_data_merger.h"
#include "logservice/libobcdc/src/ob_log_instance.h"

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

  int init_merger_environment()
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
    if (OB_SUCC(ret) && OB_FAIL(instance_->lob_aux_storager_.init(nullptr))) {
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
      const char *piece_data)
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

    if (OB_SUCC(ret) && OB_FAIL(build_summary_cache_value(
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
      // DDL 同步入口与 DML Prepare 任务共用 handle_prepare_task_() 的 Summary 校验分支。
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
      cache_value, byte_len, char_len, extra_info));
  EXPECT_EQ(8U, byte_len);
  EXPECT_EQ(8U, char_len);
  EXPECT_EQ(ObLobMetaExtraInfo::LOB_META_SUMMARY, extra_info.extra_info_type_);
  EXPECT_FALSE(extra_info.lob_chunk_type_.is_valid());
  EXPECT_EQ(2U, extra_info.chunk_count_);
  EXPECT_EQ(ObLobLastWriteType::INSERT, extra_info.last_write_type_);
}

// 验证当前尚未支持的 OUT_TABLE Summary 仍返回 OB_NOT_SUPPORTED，
// 且失败时不会发布 Summary，也不会残留 seq_id 或缓存值。
TEST_F(TestCDCLobSummaryCompatibility, parser_rejects_out_table_summary)
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
  EXPECT_EQ(OB_NOT_SUPPORTED, ObCDCLobAuxTableParser::parse_lob_aux_table_row(
      ObCDCLobAuxTableType::META, cols_, allocator, need_publish, lob_id, seq_id, cache_value));
  EXPECT_FALSE(need_publish);
  EXPECT_TRUE(seq_id.empty());
  EXPECT_TRUE(cache_value.empty());
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

// 通过真实 ObCDCLobDataMerger 校验分支验证尚未支持的 OUT_TABLE Summary
// 都会返回 OB_NOT_SUPPORTED，不会进入 Piece 获取与 LOB 拼接阶段。
TEST_F(TestCDCLobSummaryCompatibility, merger_rejects_unsupported_valid_chunk_types)
{
  ASSERT_EQ(OB_SUCCESS, init_merger_environment());
  EXPECT_EQ(OB_NOT_SUPPORTED,
      run_merger_with_summary("OUT_TABLE", 8/*byte_len*/, 1/*chunk_count*/, nullptr));
}

// 通过真实 ObCDCLobDataMerger 校验分支验证 INVALID_TYPE 只获得类型兼容，
// 仍必须满足 byte_len 上限、空值与分片数一致性以及 chunk_count 不大于 byte_len 的约束。
TEST_F(TestCDCLobSummaryCompatibility, merger_keeps_invalid_type_summary_bounds)
{
  ASSERT_EQ(OB_SUCCESS, init_merger_environment());
  EXPECT_EQ(OB_SUCCESS,
      run_merger_with_summary("INVALID_TYPE", 0/*byte_len*/, 0/*chunk_count*/, nullptr));
  EXPECT_EQ(OB_NOT_SUPPORTED,
      run_merger_with_summary("INVALID_TYPE", 1/*byte_len*/, 0/*chunk_count*/, nullptr));
  EXPECT_EQ(OB_NOT_SUPPORTED,
      run_merger_with_summary("INVALID_TYPE", 0/*byte_len*/, 1/*chunk_count*/, nullptr));
  EXPECT_EQ(OB_NOT_SUPPORTED,
      run_merger_with_summary("INVALID_TYPE", 1/*byte_len*/, 2/*chunk_count*/, nullptr));
  EXPECT_EQ(OB_NOT_SUPPORTED, run_merger_with_summary(
      "INVALID_TYPE", static_cast<uint64_t>(INT64_MAX)/*byte_len*/, 1/*chunk_count*/, nullptr));
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();
  logger.set_file_name("test_cdc_lob_summary_compatibility.cpp.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
