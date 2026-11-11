/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX OBLOG

#include <climits>
#include <functional>
#include <sstream>
#include <string>
#include <vector>
#include <gtest/gtest.h>

#include "lib/alloc/ob_malloc_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/ob_test_util.h"  // OK
#include "lib/thread/ob_multi_fixed_queue_thread.h"
#include "logservice/libobcdc/src/ob_cdc_lob_ctx.h"
#define private public
#include "logservice/libobcdc/src/ob_cdc_lob_aux_table_parser.h"
#include "logservice/libobcdc/src/ob_cdc_lob_aux_storager.h"
#include "logservice/libobcdc/src/ob_cdc_lob_data_merger.h"
#include "logservice/libobcdc/src/ob_log_part_trans_parser.h"
#undef private
#include "logservice/libobcdc/src/ob_log_instance.h"
#include "logservice/libobcdc/src/ob_log_meta_manager.h"
#include "logservice/libobcdc/src/ob_log_store_service.h"
#include "logservice/libobcdc/src/ob_log_tenant_mgr.h"
#include "storage/blocksstable/ob_storage_object_type.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/lob/ob_ext_info_callback.h"
#include "logservice/libobcdc/src/ob_log_all_ddl_operation_schema_info.h"
#include "logservice/libobcdc/src/ob_log_common.h"
#include "share/schema/ob_schema_service.h"

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

// Only tenant lookup and the external store are replaced. Cache lookup, retry,
// dispatcher signaling and stop cleanup use the production merger.
class LobWaitTenantMgr final : public ObLogTenantMgr
{
public:
  explicit LobWaitTenantMgr(const uint64_t tenant_id) : tenant_id_(tenant_id) {}
  int get_tenant_guard(const uint64_t tenant_id, ObLogTenantGuard &guard) override
  {
    int ret = OB_ENTRY_NOT_EXIST;
    if (tenant_id == tenant_id_) {
      guard.set_tenant(&tenant_);
      ret = OB_SUCCESS;
    }
    return ret;
  }
  int revert_tenant(ObLogTenant *tenant) override
  {
    return &tenant_ == tenant ? OB_SUCCESS : OB_INVALID_ARGUMENT;
  }
private:
  uint64_t tenant_id_;
  ObLogTenant tenant_;
};

class MissingLobStore final : public IObStoreService
{
public:
  int init(const std::string &) override { return OB_SUCCESS; }
  void mark_stop_flag() override {}
  int close() override { return OB_SUCCESS; }
  int put(const std::string &, const ObSlice &) override { return OB_NOT_SUPPORTED; }
  int put(void *, const std::string &, const ObSlice &) override { return OB_NOT_SUPPORTED; }
  int batch_write(void *, const std::vector<std::string> &, const std::vector<ObSlice> &) override
  { return OB_NOT_SUPPORTED; }
  int get(const std::string &key, std::string &value) override
  { return get(nullptr/*cf_handle*/, key, value); }
  int get(void *, const std::string &key, std::string &value) override
  {
    EXPECT_EQ(expected_key_, key);
    value.clear();
    ++get_count_;
    if (on_get_) {
      on_get_();
    }
    return OB_ENTRY_NOT_EXIST;
  }
  int del(const std::string &) override { return OB_NOT_SUPPORTED; }
  int del(void *, const std::string &) override { return OB_NOT_SUPPORTED; }
  int del_range(void *, const std::string &, const std::string &) override { return OB_NOT_SUPPORTED; }
  int compact_range(void *, const std::string &, const std::string &, const bool) override
  { return OB_NOT_SUPPORTED; }
  int flush(void *) override { return OB_NOT_SUPPORTED; }
  int create_column_family(const std::string &, void *&) override { return OB_NOT_SUPPORTED; }
  int drop_column_family(void *) override { return OB_NOT_SUPPORTED; }
  int destory_column_family(void *) override { return OB_NOT_SUPPORTED; }
  void get_mem_usage(const std::vector<uint64_t>, const std::vector<void *>) override {}
  int get_mem_usage(void *, int64_t &, int64_t &) override { return OB_NOT_SUPPORTED; }

  std::string expected_key_;
  int64_t get_count_ = 0;
  std::function<void()> on_get_;
};

class TestCDCOfOuttableLob : public ::testing::Test
{
protected:
  void SetUp() override
  {
    // Log before assertions so TearDown can close even a failed setup.
    const ::testing::TestInfo *test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    LOG_INFO("========== out-table LOB case begin ==========", "case_name", test_info->name());
    // Reuse a stable LOB identity and the reserved summary sequence in each case.
    expected_lob_id_.set_column_idx(LOB_COLUMN_IDX);
    expected_lob_id_.set_tablet_id(1001/*tablet_id*/);
    expected_lob_id_.set_auto_inc_id(1/*auto_inc_id*/);
    expected_summary_seq_id_.assign_ptr(summary_seq_buf_, sizeof(summary_seq_buf_));
    OK(ObLobSeqId::get_summary_seq_id(expected_summary_seq_id_));
    LOG_INFO("fixture setup: LOB identity and summary sequence ready",
        K(expected_lob_id_), "summary_seq_len", expected_summary_seq_id_.length());
    ASSERT_TRUE(expected_lob_id_.is_valid());
  }

  void TearDown() override
  {
    // Stop workers before destroying the cache and the singleton they reference.
    LOG_INFO("fixture teardown: stop merger and release auxiliary cache", KP(instance_));
    merger_.stop();
    merger_.destroy();
    if (nullptr != instance_) {
      instance_->lob_aux_storager_.destroy();
      ObLogInstance::destroy_instance();
      instance_ = nullptr;
    }
    const ::testing::TestInfo *test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    LOG_INFO("========== out-table LOB case end ==========",
        "case_name", test_info->name(), "has_failure", HasFailure());
  }

  int init_merger_environment(IObStoreService *store_service = nullptr)
  {
    int ret = OB_SUCCESS;
    // Provision the tenant allocator used by generated piece sequence IDs.
    LOG_INFO("environment step 1: prepare tenant allocator", K(TENANT_ID), KP(store_service));
    lib::ObMallocAllocator *malloc_allocator = lib::ObMallocAllocator::get_instance();
    if (OB_ISNULL(malloc_allocator)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get malloc allocator failed", KR(ret));
    } else {
      // Piece sequence IDs use the business tenant allocator; create it and set its limit explicitly.
      const int create_ret = malloc_allocator->create_and_add_tenant_allocator(TENANT_ID);
      if (OB_SUCCESS != create_ret && OB_ENTRY_EXIST != create_ret) {
        ret = create_ret;
        LOG_WARN("create test tenant allocator failed", KR(ret), K(TENANT_ID));
      } else if (OB_FAIL(malloc_allocator->set_tenant_limit(TENANT_ID, 1L << 30/*bytes*/))) {
        LOG_WARN("set test tenant memory limit failed", KR(ret), K(TENANT_ID));
      }
    }
    LOG_INFO("environment step 1 complete: tenant allocator prepared", KR(ret), K(TENANT_ID));
    // Initialize the real cache while allowing missing-data tests to supply a store double.
    LOG_INFO("environment step 2: initialize instance and auxiliary cache", KR(ret), KP(store_service));
    if (OB_SUCC(ret) && OB_ISNULL(instance_ = ObLogInstance::get_instance())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("get ObLogInstance failed", KR(ret));
    }
    if (OB_SUCC(ret)) {
      instance_->working_mode_ = WorkingMode::MEMORY_MODE;
    }
    if (OB_SUCC(ret) && OB_FAIL(instance_->lob_aux_storager_.init(store_service))) {
      LOG_WARN("init lob aux storager failed", KR(ret));
    }
    LOG_INFO("environment step 2 complete: auxiliary cache initialized", KR(ret), KP(instance_));
    // Keep the production merger available for both synchronous and direct-fragment calls.
    LOG_INFO("environment step 3: initialize and start merger", KR(ret));
    if (OB_SUCC(ret) && OB_FAIL(
        merger_.init(1/*thread_num*/, 16/*queue_size*/, err_handler_))) {
      LOG_WARN("init lob data merger failed", KR(ret));
    }
    if (OB_SUCC(ret) && OB_FAIL(merger_.start())) {
      LOG_WARN("start lob data merger failed", KR(ret));
    }

    LOG_INFO("environment step 3 complete: merger environment ready", KR(ret));
    return ret;
  }

  enum class LobWaitResult { STOP, PUBLISH };

  int run_missing_lob_wait(const LobWaitResult result)
  {
    int ret = OB_SUCCESS;
    const int64_t commit_version = next_commit_version_++;
    const transaction::ObTransID trans_id(commit_version);
    const transaction::ObTxSEQ operation_seq(10/*seq*/, 3/*branch*/);
    ObArenaAllocator allocator;
    PartTransTask task;
    task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
    IStmtTask stmt(IStmtTask::STMT_TYPE_DML, task);
    ObLobDataOutRowCtxList row_ctx_list(allocator);
    ObLobDataGetCtx get_ctx;
    ObString seq_id;
    LobWaitTenantMgr tenant_mgr(TENANT_ID);
    MissingLobStore store;
    IObLogTenantMgr *saved_tenant_mgr = nullptr;
    const std::string expected_data("lob\0data", 8/*count*/);
    bool saw_pending = false;
    bool published = false;
    bool used_stop_fallback = false;
    volatile bool stop_flag = false;
    int publish_ret = OB_SUCCESS;

    // Force cache misses until the production retry loop raises its pending signal.
    LOG_INFO("missing-data step 1: prepare transaction and missing-store scenario",
        K(commit_version), K(operation_seq), "wait_result", static_cast<int>(result),
        "expected_len", expected_data.length());
    if (OB_FAIL(init_merger_environment(&store))) {
      LOG_WARN("init merger environment failed", KR(ret));
    } else {
      saved_tenant_mgr = instance_->tenant_mgr_;
      instance_->tenant_mgr_ = &tenant_mgr;
      ret = ObLobSeqId::get_seq_id(allocator, 1/*idx*/, seq_id);
    }
    const ObLobAuxKey key(commit_version, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
        expected_lob_id_, operation_seq, seq_id);
    if (OB_SUCC(ret) && OB_FAIL(key.get_key(store.expected_key_))) {
      LOG_WARN("get key failed", KR(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(prepare_part_trans_task(task, trans_id, commit_version))) {
      LOG_WARN("prepare part trans task failed", KR(ret));
    }
    if (OB_SUCC(ret)) {
      row_ctx_list.reset(&stmt, TENANT_ID, trans_id,
          LOB_META_TABLE_ID, LOB_PIECE_TABLE_ID, false/*is_ddl*/);
      if (OB_FAIL(get_ctx.reset(&row_ctx_list, LOB_COLUMN_IDX,
          blocksstable::ObDmlRowFlag(blocksstable::ObDmlFlag::DF_INSERT),
          expected_lob_id_, operation_seq, true/*is_new_col*/))) {
        LOG_WARN("reset get ctx failed", KR(ret));
      } else if (OB_FAIL(get_ctx.get_lob_col_ctx(true/*is_new_col*/).init(
          1/*chunk_count*/, expected_data.length(), allocator))) {
        LOG_WARN("init lob col ctx failed", KR(ret));
      } else {
        row_ctx_list.add(&get_ctx);
      }
    }
    LOG_INFO("missing-data step 1 complete: fragment context prepared", KR(ret), K(commit_version));
    if (OB_SUCC(ret)) {
      // React only to observed state changes; do not log every store retry.
      const int64_t test_begin_ts = get_timestamp();
      store.on_get_ = [&]() {
        // Bound the test if signaling regresses; production keeps waiting.
        if (get_timestamp() - test_begin_ts >= 10 * _SEC_) {
          if (!used_stop_fallback) {
            LOG_INFO("missing-data fallback: stop after test deadline", K(commit_version),
                "store_get_count", store.get_count_, KR(publish_ret));
          }
          used_stop_fallback = true;
          stop_flag = true;
        } else if (task.get_pending_lob_format_count() > 0) {
          if (!saw_pending) {
            LOG_INFO("missing-data transition: pending signal observed", K(commit_version),
                "pending_count", task.get_pending_lob_format_count(), "store_get_count", store.get_count_);
          }
          saw_pending = true;
          if (LobWaitResult::STOP == result) {
            if (!stop_flag) {
              LOG_INFO("missing-data transition: request stop after pending signal", K(commit_version));
            }
            stop_flag = true;
          } else if (LobWaitResult::PUBLISH == result && !published) {
            // Publish into the real memory cache only after the merger has
            // asked the dispatcher to make progress; the next retry finds it.
            publish_ret = instance_->lob_aux_storager_.put(
                key, "insert", expected_data.data(), expected_data.length());
            published = OB_SUCCESS == publish_ret;
            if (published) {
              LOG_INFO("missing-data transition: publish delayed piece", KR(publish_ret),
                  K(commit_version), "piece_len", expected_data.length(),
                  "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
            }
          }
        }
      };
      // init() retains the Prepare reference as well as the fragment reference.
      // Processing this fragment must not finish the image or call Formatter.
      ObLobColFragTask fragment_task(get_ctx);
      fragment_task.reset(true/*is_new_col*/, seq_id, 0/*idx*/);
      LOG_INFO("missing-data step 2: enter production fragment retry", K(commit_version));
      ret = merger_.handle_fragment_task_(fragment_task, stop_flag);
      LOG_INFO("missing-data step 2 complete: verify retry exit and pending balance", KR(ret),
          K(commit_version), K(saw_pending), K(published), K(used_stop_fallback), KR(publish_ret),
          "store_get_count", store.get_count_, "pending_count", task.get_pending_lob_format_count());
      EXPECT_GT(store.get_count_, 0);
      EXPECT_FALSE(used_stop_fallback);
      EXPECT_TRUE(saw_pending);
      EXPECT_EQ(0, task.get_pending_lob_format_count());
      EXPECT_EQ(OB_SUCCESS, publish_ret);
      const ObString &fragment = *get_ctx.get_fragment_cb_array(true/*is_new_col*/)[0];
      LOG_INFO("missing-data step 3: verify fragment and retained prepare reference", KR(ret),
          "fragment_len", fragment.length(),
          "col_ref_count", get_ctx.get_lob_col_ctx(true/*is_new_col*/).get_col_ref_cnt());
      if (OB_SUCCESS == ret) {
        EXPECT_TRUE(published);
        EXPECT_EQ(expected_data, std::string(fragment.ptr(), fragment.length()));
        EXPECT_EQ(1U, get_ctx.get_lob_col_ctx(true/*is_new_col*/).get_col_ref_cnt());
      } else {
        EXPECT_EQ(nullptr, fragment.ptr());
        EXPECT_EQ(0, fragment.length());
        EXPECT_EQ(2U, get_ctx.get_lob_col_ctx(true/*is_new_col*/).get_col_ref_cnt());
      }
    }
    // Release only a successfully published piece, preserving the original cleanup order.
    LOG_INFO("missing-data step 4: clean published data and detach doubles", KR(ret), K(published));
    if (published) {
      EXPECT_EQ(OB_SUCCESS, instance_->lob_aux_storager_.memory_del_(key));
    }
    // The instance must not retain pointers to the local test doubles.
    if (nullptr != instance_) {
      instance_->tenant_mgr_ = saved_tenant_mgr;
      instance_->lob_aux_storager_.store_service_ = nullptr;
      LOG_INFO("missing-data step 4 complete: doubles detached", KR(ret), K(commit_version),
          "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
    }
    return ret;
  }

  int build_summary_cache_value(
      const char *lob_chunk_type,
      const uint64_t byte_len,
      const uint64_t chunk_count,
      ObIAllocator &allocator,
      ObString &summary_value,
      const ObLobLastWriteType last_write_type = ObLobLastWriteType::INSERT)
  {
    int ret = OB_SUCCESS;
    ObLobMetaInfo meta_info;
    ObLobMetaExtraInfo extra_info;
    // Encode the production summary format without logging the serialized payload.
    LOG_INFO("summary cache: encode image descriptor", KCSTRING(lob_chunk_type), K(byte_len),
        K(chunk_count), "last_write_type", static_cast<int>(last_write_type));
    extra_info.extra_info_type_ = ObLobMetaExtraInfo::LOB_META_SUMMARY;
    extra_info.chunk_count_ = chunk_count;
    extra_info.last_write_type_ = last_write_type;
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

    LOG_INFO("summary cache: encoding complete", KR(ret), K(byte_len), K(chunk_count),
        "summary_len", summary_value.length());
    return ret;
  }

  int prepare_part_trans_task(
      PartTransTask &part_trans_task,
      const transaction::ObTransID &trans_id,
      const int64_t commit_version,
      const bool is_sys_ls = false)
  {
    int ret = OB_SUCCESS;
    const logservice::TenantLSID tls_id(TENANT_ID, share::ObLSID(is_sys_ls ? 1 : LS_ID));
    transaction::ObLSLogInfoArray participants;
    // Give cache keys a committed transaction version through the real commit path.
    LOG_INFO("transaction setup: prepare and commit participant", K(trans_id), K(commit_version),
        K(is_sys_ls), K(tls_id));
    part_trans_task.set_task_info(tls_id, is_sys_ls ? "1002_1" : "1002_1001");
    part_trans_task.set_trans_id(trans_id);
    part_trans_task.mark_read_prepare();
    if (OB_FAIL(part_trans_task.commit(CLUSTER_ID, trans_id, commit_version,
        transaction::TransType::SP_TRANS, participants, palf::LSN(commit_version),
        commit_version/*commit_log_submit_ts*/, false/*is_data_dict_mode*/))) {
      LOG_WARN("commit part trans task failed", KR(ret), K(trans_id), K(commit_version));
    }
    LOG_INFO("transaction setup complete", KR(ret), K(trans_id), K(commit_version), K(is_sys_ls));
    return ret;
  }

  blocksstable::MacroBlockId make_outtable_macro_id(const int64_t chunk_index)
  {
    // Distinguish chunks with a valid shared out-table object identity.
    blocksstable::MacroBlockId macro_id;
    macro_id.set_version_v1();
    macro_id.set_id_mode(static_cast<uint64_t>(blocksstable::ObMacroBlockIdMode::ID_MODE_SHARE));
    macro_id.set_storage_object_type(static_cast<uint64_t>(
        blocksstable::ObStorageObjectType::SHARED_OUTTABLE_LOB_DATA_MACRO));
    macro_id.set_location(blocksstable::ObMacroBlockIdLocation::OBJECT_STORAGE);
    macro_id.set_second_id(1000/*second_id*/);
    macro_id.set_third_id(1001/*third_id*/);
    macro_id.set_fourth_id(chunk_index + 1/*fourth_id*/);
    macro_id.set_fifth_id(0/*fifth_id*/);
    return macro_id;
  }

  // The fixture acts as the producer: preserve its wire bytes and overwrite them
  // immediately after ingestion to verify the owning DML/DDL node boundary.
  int push_outtable_body(
      PartTransTask &task,
      const palf::LSN &log_lsn,
      const transaction::ObTxLobOutTableDataLog &log,
      const bool append_compat_tail)
  {
    std::vector<char> body(log.get_serialize_size() + (append_compat_tail ? 1 : 0));
    int64_t body_len = 0;
    int ret = log.serialize(body.data(), body.size(), body_len);
    LOG_INFO("raw body step 1: producer serialized LOB", KR(ret), K(log_lsn), K(body_len),
        K(append_compat_tail), "is_sys_ls", task.is_sys_ls_part_trans());
    if (OB_SUCC(ret) && append_compat_tail) {
      // Include an unknown trailing field inside the UNIS envelope. Re-serializing
      // the known fields would drop this byte, whereas copying the raw slice must retain it.
      int64_t header_pos = 0;
      int64_t version = 0;
      int64_t payload_len = 0;
      if (OB_FAIL(serialization::decode_vi64(body.data(), body_len, header_pos, &version))) {
        LOG_WARN("decode version failed", KR(ret), K(body_len), K(header_pos));
      } else {
        const int64_t length_pos = header_pos;
        if (OB_FAIL(serialization::decode_vi64(body.data(), body_len, header_pos, &payload_len))) {
          LOG_WARN("decode payload length failed", KR(ret), K(body_len), K(header_pos));
        } else {
          EXPECT_EQ(body_len, header_pos + payload_len);
          body[body_len++] = 'T';
          int64_t rewrite_pos = length_pos;
          ret = serialization::encode_fixed_bytes_i64(
              body.data(), body.size(), rewrite_pos, payload_len + 1 /*val*/);
        }
      }
      LOG_INFO("raw body step 1: extended compatible envelope", KR(ret), K(version),
          K(payload_len), K(body_len));
    }
    if (OB_SUCC(ret)) {
      const std::vector<char> expected_body(body);
      ret = task.push_lob_outtable_data_log(log_lsn, log, body.data(), body_len);
      // Invalidate the caller-owned wire bytes before inspecting the node.
      MEMSET(body.data(), 0, body.size());
      LOG_INFO("raw body step 2: ingress returned and source was overwritten", KR(ret), K(log_lsn),
          K(body_len), "node_count", task.get_sorted_redo_list().get_node_number());
      if (OB_SUCC(ret)) {
        RedoLogMetaNode *node = nullptr;
        for (RedoNodeIterator it = task.get_sorted_redo_list().redo_iter_begin();
            it != task.get_sorted_redo_list().redo_iter_end(); it++) {
          if ((*it).get_start_log_lsn() == log_lsn) {
            node = &*it;
            break;
          }
        }
        EXPECT_NE(nullptr, node);
        if (nullptr != node) {
          EXPECT_NE(nullptr, node->get_data());
          EXPECT_NE(body.data(), node->get_data());
          EXPECT_EQ(body_len, node->get_data_len());
          if (nullptr != node->get_data() && body_len == node->get_data_len()) {
            const int body_compare = MEMCMP(expected_body.data(), node->get_data(), body_len);
            LOG_INFO("raw body step 3: compare owning node bytes", K(log_lsn), K(body_len),
                K(body_compare), "is_sys_ls", task.is_sys_ls_part_trans());
            EXPECT_EQ(0, body_compare);
          }
          if (task.is_sys_ls_part_trans()) {
            EXPECT_TRUE(static_cast<DdlRedoLogNode *>(node)->is_lob_outtable_data_log());
          } else {
            EXPECT_TRUE(static_cast<DmlRedoLogNode *>(node)->is_lob_outtable_data_log());
          }
        }
      }
    }
    return ret;
  }

  // Feed real typed redo nodes through the parser, then assemble and reclaim the
  // image through the same merger/cache methods used by production. Synchronous
  // prepare avoids requiring a Formatter worker for this focused pipeline test.
  int run_outtable_pipeline(
      const std::vector<std::string> &pieces,
      const uint64_t summary_byte_len,
      const bool parse_pieces = true,
      const blocksstable::ObDmlFlag dml_flag = blocksstable::ObDmlFlag::DF_INSERT,
      const bool is_new_col = true,
      const ObLobLastWriteType last_write_type = ObLobLastWriteType::INSERT)
  {
    int ret = OB_SUCCESS;
    const int64_t commit_version = next_commit_version_++;
    const transaction::ObTransID trans_id(commit_version);
    const transaction::ObTxSEQ operation_seq(10/*seq*/, 3/*branch*/);
    ObArenaAllocator allocator;
    PartTransTask task;
    task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
    IStmtTask stmt(IStmtTask::STMT_TYPE_DML, task);
    ObLobDataOutRowCtxList row_ctx_list(allocator);
    ObLobDataGetCtx get_ctx;
    ObLogPartTransParser parser;
    ObLogTenant tenant;
    GIndexCache gi_cache;
    ObLogPartMgr &part_mgr = static_cast<ObLogPartMgr &>(tenant.get_part_mgr());
    std::vector<ObString> seq_ids;
    std::string expected;
    ObString summary_value;
    volatile bool stop_flag = false;
    const ObLobAuxKey summary_key(commit_version, TENANT_ID, trans_id,
        LOB_META_TABLE_ID, expected_lob_id_, operation_seq, expected_summary_seq_id_);

    // Publish the summary first so each scenario controls only the piece path and image shape.
    LOG_INFO("pipeline step 1: prepare participant and publish summary", K(commit_version),
        "piece_count", pieces.size(), K(summary_byte_len), K(parse_pieces), K(is_new_col),
        "dml_flag", static_cast<int>(dml_flag), "last_write_type", static_cast<int>(last_write_type));
    if (OB_FAIL(part_mgr.init(TENANT_ID, 1/*start_schema_version*/,
        false/*enable_oracle_mode_match_case_sensitive*/, false/*enable_white_black_list*/, gi_cache))) {
      LOG_WARN("init part mgr failed", KR(ret));
    } else if (OB_FAIL(prepare_part_trans_task(task, trans_id, commit_version))) {
      LOG_WARN("prepare part trans task failed", KR(ret));
    } else if (OB_FAIL(build_summary_cache_value("OUT_TABLE", summary_byte_len,
        pieces.size(), allocator, summary_value, last_write_type))) {
      LOG_WARN("build summary cache value failed", KR(ret));
    } else if (OB_FAIL(instance_->lob_aux_storager_.put(
        summary_key, "summary", summary_value.ptr(), summary_value.length()))) {
      LOG_WARN("put summary cache failed", KR(ret));
    }
    LOG_INFO("pipeline step 1 complete: summary publication result", KR(ret), K(commit_version),
        "summary_len", summary_value.length(),
        "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());

    // Ingress tests use typed redo nodes; corruption tests may seed pieces directly in the cache.
    for (size_t i = 0; OB_SUCC(ret) && i < pieces.size(); ++i) {
      LOG_INFO("pipeline step 2: ingest piece", K(commit_version), K(i),
          "piece_len", pieces[i].length(), K(parse_pieces));
      ObString seq_id;
      if (OB_FAIL(ObLobSeqId::get_seq_id(allocator, i + 1, seq_id))) {
      } else {
        seq_ids.push_back(seq_id);
        expected.append(pieces[i]);
        if (parse_pieces) {
          // Deliberately overwrite the source buffers after ingestion. Both
          // seq_id and binary payload must have been copied into the redo node.
          std::string source = pieces[i];
          std::string source_seq(seq_id.ptr(), seq_id.length());
          const blocksstable::MacroBlockId macro_id = make_outtable_macro_id(i);
          const transaction::ObTxLobOutTableDataLog::ConstructArg arg(
              LOB_PIECE_TABLE_ID, operation_seq,
              ObString(source_seq.length(), source_seq.data()), expected_lob_id_,
              macro_id, true /*is_logging*/, ObString(source.length(), source.data()));
          const transaction::ObTxLobOutTableDataLog log(arg);
          ret = push_outtable_body(
              task, palf::LSN(i + 1 /*offset*/), log, false /*append_compat_tail*/);
          source.assign(source.length(), 'x');
          source_seq.assign(source_seq.length(), 'x');
        } else {
          const ObLobAuxKey key(commit_version, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
              expected_lob_id_, operation_seq, seq_id);
          ret = instance_->lob_aux_storager_.put(
              key, "piece", pieces[i].data(), pieces[i].length());
        }
      }
      LOG_INFO("pipeline step 2 complete: piece ingestion result", KR(ret), K(commit_version), K(i),
          "piece_len", pieces[i].length(), "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
    }

    // Verify the serialized fields before parser publication and then overwrite the source body.
    if (OB_SUCC(ret) && parse_pieces) {
      task.get_sorted_redo_list().init_iterator();
      for (size_t i = 0; OB_SUCC(ret) && i < pieces.size(); ++i) {
        DmlRedoLogNode *node = nullptr;
        bool is_last = false;
        LOG_INFO("pipeline step 3: decode and publish typed redo", K(commit_version), K(i));
        if (OB_FAIL(task.next_redo_to_dispatch(node, is_last))) {
        } else if (OB_ISNULL(node)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          EXPECT_TRUE(node->is_lob_outtable_data_log());
          EXPECT_FALSE(node->is_direct_load_inc_log());
          EXPECT_EQ(i + 1 == pieces.size(), is_last);
          transaction::ObTxLobOutTableDataLog::ConstructArg empty_arg;
          transaction::ObTxLobOutTableDataLog decoded(empty_arg);
          int64_t pos = 0;
          if (OB_FAIL(decoded.deserialize(node->get_data(), node->get_data_len(), pos))) {
          } else {
            LOG_INFO("pipeline step 3: verify serialized redo fields", K(commit_version), K(i),
                "body_len", node->get_data_len(), K(pos), K(is_last),
                "piece_len", decoded.get_data().length(), "seq_id_len", decoded.get_seq_id().length());
            EXPECT_EQ(node->get_data_len(), pos);
            EXPECT_EQ(LOB_PIECE_TABLE_ID, decoded.get_lob_aux_table_id());
            EXPECT_TRUE(operation_seq == decoded.get_lob_operation_seq_no());
            EXPECT_TRUE(expected_lob_id_ == decoded.get_lob_id());
            EXPECT_TRUE(make_outtable_macro_id(i) == decoded.get_macro_id());
            EXPECT_EQ(0, seq_ids[i].compare(decoded.get_seq_id()));
            EXPECT_EQ(pieces[i], std::string(decoded.get_data().ptr(), decoded.get_data().length()));
            ret = parser.parse_lob_outtable_data_log_(&tenant, *node, task);
            // The cache must survive release/reuse of the serialized redo body.
            MEMSET(const_cast<char *>(node->get_data()), 0, node->get_data_len());
          }
        }
        LOG_INFO("pipeline step 3 complete: parser publication result", KR(ret), K(commit_version), K(i),
            "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
      }
    }

    // Synchronous prepare exercises production assembly without a Formatter worker.
    if (OB_SUCC(ret)) {
      LOG_INFO("pipeline step 4: assemble requested image", K(commit_version), K(summary_byte_len),
          K(is_new_col), "dml_flag", static_cast<int>(dml_flag));
      row_ctx_list.reset(&stmt, TENANT_ID, trans_id,
          LOB_META_TABLE_ID, LOB_PIECE_TABLE_ID, true/*is_ddl*/);
      if (OB_FAIL(get_ctx.reset(&row_ctx_list, LOB_COLUMN_IDX,
          blocksstable::ObDmlRowFlag(dml_flag), expected_lob_id_, operation_seq, is_new_col))) {
      } else {
        row_ctx_list.add(&get_ctx);
        ObLobImagePrepareTask prepare_task(get_ctx, is_new_col);
        ret = merger_.handle_prepare_task_(prepare_task, true/*is_sync*/, stop_flag);
      }
      LOG_INFO("pipeline step 4 complete: image assembly result", KR(ret), K(commit_version),
          "all_callbacks_done", row_ctx_list.is_all_lob_callback_done());
    }
    // Successful assembly must release summary and pieces using the production image cleanup.
    if (OB_SUCC(ret)) {
      const ObString &actual = get_ctx.get_lob_col_ctx(is_new_col).get_lob_column_value();
      LOG_INFO("pipeline step 5: verify assembled bytes and callbacks", K(commit_version),
          "expected_len", expected.length(), "actual_len", actual.length(),
          "all_callbacks_done", row_ctx_list.is_all_lob_callback_done());
      EXPECT_EQ(expected, std::string(actual.ptr(), actual.length()));
      EXPECT_TRUE(row_ctx_list.is_all_lob_callback_done());
      EXPECT_EQ(OB_SUCCESS, instance_->lob_aux_storager_.del(row_ctx_list, stop_flag));
      LOG_INFO("pipeline step 5: inspect cache after image cleanup", K(commit_version),
          "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
      const char *cached_data = nullptr;
      int64_t cached_len = 0;
      EXPECT_EQ(OB_ENTRY_NOT_EXIST, instance_->lob_aux_storager_.memory_get_(
          summary_key, cached_data, cached_len));
      for (size_t i = 0; i < seq_ids.size(); ++i) {
        const ObLobAuxKey key(commit_version, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
            expected_lob_id_, operation_seq, seq_ids[i]);
        EXPECT_EQ(OB_ENTRY_NOT_EXIST,
            instance_->lob_aux_storager_.memory_get_(key, cached_data, cached_len));
        LOG_INFO("pipeline step 5: piece cache removal checked", K(commit_version), K(i), K(cached_len));
      }
    } else {
      // Failed prepares need transaction-level cleanup; do not require the
      // partially initialized column context to describe all published pieces.
      LOG_INFO("pipeline step 5: clean published keys after expected failure", KR(ret),
          K(commit_version), "piece_key_count", seq_ids.size(),
          "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
      (void)instance_->lob_aux_storager_.memory_del_(summary_key);
      for (const ObString &seq_id : seq_ids) {
        const ObLobAuxKey key(commit_version, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
            expected_lob_id_, operation_seq, seq_id);
        (void)instance_->lob_aux_storager_.memory_del_(key);
      }
    }
    LOG_INFO("pipeline complete: return scenario result", KR(ret), K(commit_version),
        "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
    return ret;
  }

  int parse_outtable_body(
      const transaction::ObTxLobOutTableDataLog::ConstructArg &arg,
      PartTransTask &task,
      ObLogTenant *tenant,
      const int64_t length_adjustment = 0)
  {
    // Preserve the production wire format while allowing truncated or trailing-byte scenarios.
    transaction::ObTxLobOutTableDataLog log(arg);
    std::vector<char> body(log.get_serialize_size() + 1, '\0');
    int64_t pos = 0;
    LOG_INFO("body parser step 1: serialize input", "commit_version", task.get_trans_commit_version(),
        "table_id", arg.lob_aux_table_id_, "piece_len", arg.data_.length(),
        K(length_adjustment), "is_sys_ls", task.is_sys_ls_part_trans());
    int ret = log.serialize(body.data(), body.size(), pos);
    LOG_INFO("body parser step 1 complete: serialized body", KR(ret), K(pos),
        "supplied_body_len", pos + length_adjustment);
    if (OB_SUCC(ret)) {
      // Exercise the same typed payload parser through the node class used by each LS.
      ObLogPartTransParser parser;
      if (task.is_sys_ls_part_trans()) {
        DdlRedoLogNode node;
        node.reset(palf::LSN(1/*offset*/), body.data(), pos + length_adjustment/*size*/,
            pos + length_adjustment/*pos*/, DdlRedoLogNode::LogType::LOB_OUTTABLE_DATA);
        ret = parser.parse_lob_outtable_data_log_(tenant, node, task);
      } else {
        DmlRedoLogNode node;
        node.init_for_data_memory(palf::LSN(1/*offset*/), body.data(),
            pos + length_adjustment/*size*/, pos + length_adjustment/*pos*/);
        node.set_lob_outtable_data_log();
        ret = parser.parse_lob_outtable_data_log_(tenant, node, task);
      }
    }
    LOG_INFO("body parser step 2 complete: typed payload parse result", KR(ret),
        "commit_version", task.get_trans_commit_version(), K(length_adjustment),
        "cache_count", nullptr == instance_ ? -1 : instance_->lob_aux_storager_.lob_aux_map_.count());
    return ret;
  }

  int prepare_ddl_part_trans_task(PartTransTask &task, const int64_t commit_version)
  {
    // Use the SYS LS node path and classify the committed participant as DDL.
    const int ret = prepare_part_trans_task(task, transaction::ObTransID(commit_version),
        commit_version, true/*is_sys_ls*/);
    task.set_type(PartTransTask::TASK_TYPE_DDL_TRANS);
    LOG_INFO("DDL transaction setup complete", KR(ret), K(commit_version),
        "is_sys_ls", task.is_sys_ls_part_trans(), "is_ddl_trans", task.is_ddl_trans());
    return ret;
  }

  // Construct physical macroblock rows, including their two hidden rowkey
  // columns, then use the production schema/locator column decoders.
  int init_macroblock_row(ObIAllocator &allocator, std::vector<ObObj> &values,
      const int64_t rowkey_count, MacroBlockMutatorRow &row)
  {
    int ret = OB_SUCCESS;
    blocksstable::ObDatumRow datum_row;
    ObStoreRowkey rowkey;
    LOG_INFO("macroblock row step 1: allocate physical row", "logical_column_count", values.size(),
        K(rowkey_count), "hidden_rowkey_count", OB_MAX_EXTRA_ROWKEY_COLUMN_NUMBER);
    rowkey.assign(values.data(), rowkey_count);
    if (OB_FAIL(datum_row.init(allocator,
        values.size() + OB_MAX_EXTRA_ROWKEY_COLUMN_NUMBER))) {
    } else {
      // Leave hidden rowkey cells null while copying logical values to their physical positions.
      for (int64_t i = 0; i < datum_row.get_column_count(); ++i) {
        datum_row.storage_datums_[i].set_null();
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int64_t>(values.size()); ++i) {
        const int64_t physical_idx = i < rowkey_count ? i : i + OB_MAX_EXTRA_ROWKEY_COLUMN_NUMBER;
        ret = datum_row.storage_datums_[physical_idx].from_obj_enhance(values[i]);
      }
      if (OB_SUCC(ret)) {
        ret = row.init(allocator, datum_row, rowkey);
      }
    }
    LOG_INFO("macroblock row step 2 complete: physical row initialized", KR(ret),
        "logical_column_count", values.size(), K(rowkey_count));
    return ret;
  }

  int build_summary_row(const char *extra_info_json, const uint64_t byte_len)
  {
    int ret = OB_SUCCESS;
    // Construct real META columns so the parser validates the summary representation.
    LOG_INFO("summary row step 1: populate META columns", K(byte_len), K(expected_lob_id_));
    cols_.reset();
    for (int64_t idx = 0; idx < ObLobMetaInfo::COLUMN_CNT; ++idx) {
      col_values_[idx].reset();
      col_values_[idx].column_id_ = OB_APP_MIN_COLUMN_ID + idx;
    }

    const ObString lob_id_string(
        sizeof(expected_lob_id_), reinterpret_cast<const char *>(&expected_lob_id_));
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
    LOG_INFO("summary row step 2 complete: META columns assembled", KR(ret), K(byte_len),
        "column_count", ObLobMetaInfo::COLUMN_CNT);
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

// Test group overview:
//   A. Log ingestion and LOB image assembly                   (4 cases)
//   B. Payload validation and subscription/rollback filtering (5 cases)
//   C. Waiting for missing pieces and stopping                (2 cases)
//   D. System LS DDL                                          (4 cases)

// ============================================================================
// A. Log ingestion and LOB image assembly (4 cases)
// ============================================================================

// A1. A single 2 MiB binary piece can be enqueued, assembled and removed from the cache.
// Scenario: A piece reaches the OUT_TABLE limit, filled with zero bytes except for distinct first/last characters.
// Steps: Enqueue typed redo, publish through Parser, assemble through Merger, then clean up by LOB image.
// Checks: The helper verifies deep copies in the redo body and cache, full binary content and an empty cache.
// Scope: Exactly 2 MiB; oversized and empty pieces are covered by A4.
TEST_F(TestCDCOfOuttableLob, outtable_insert_two_megabyte_payload_and_cache_cleanup)
{
  // Use binary data at the maximum accepted chunk size.
  OK(init_merger_environment());
  std::string piece(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, '\0');
  piece.front() = 'a';
  piece.back() = 'z';
  LOG_INFO("step 1: prepare maximum-size binary chunk",
      "piece_length", piece.length(), "chunk_limit", ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE);
  // The pipeline checks both owned copies before merging and deleting cache entries.
  const int ret = run_outtable_pipeline({piece}, piece.length());
  LOG_INFO("step 2: maximum-size chunk pipeline completed", K(ret),
      "piece_length", piece.length(), "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
}

// A2. Pieces are assembled in seq_id order, preserving content across boundaries and embedded zero bytes.
// Scenario: Two 2 MiB pieces followed by a 9-byte tail containing an embedded zero byte.
// Steps: Build a summary with the total length and exercise production log ingestion, Parser and Merger.
// Checks: The helper compares assembled bytes, verifies cache deep copies and removes all pieces and summary.
// Scope: Total LOB size exceeds the 2 MiB piece limit; the short tail must not be truncated as a C string.
TEST_F(TestCDCOfOuttableLob, outtable_insert_multiple_binary_chunks_in_order)
{
  // The short binary tail distinguishes byte-wise merging from C-string concatenation.
  OK(init_merger_environment());
  const std::vector<std::string> pieces = {
      std::string(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 'a'),
      std::string(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 'b'),
      std::string("tail\0data", 9)};
  const uint64_t byte_len = pieces[0].length() + pieces[1].length() + pieces[2].length();
  LOG_INFO("step 1: prepare multiple binary chunks", "piece_count", pieces.size(),
      "first_piece_length", pieces[0].length(), "second_piece_length", pieces[1].length(),
      "tail_length", pieces[2].length(), K(byte_len));
  const int ret = run_outtable_pipeline(pieces, byte_len);
  LOG_INFO("step 2: multi-chunk pipeline completed", K(ret), K(byte_len),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
}

// A3. OUT_TABLE summaries with APPEND or MULTIPART_WRITE remain unsupported.
// Scenario: An INSERT new image with a valid 4-byte piece; only the summary write type changes.
// Steps: Invoke Merger with APPEND and MULTIPART_WRITE in turn.
// Checks: Both calls return OB_NOT_SUPPORTED instead of treating incremental writes as full INSERT images.
// Scope: Only the last_write_type restriction is checked.
TEST_F(TestCDCOfOuttableLob, outtable_merger_rejects_append_and_multipart_write)
{
  OK(init_merger_environment());
  // Keep the row image valid and vary only the summary's write operation.
  LOG_INFO("step 1: prepare unsupported write types", "summary_byte_len", 4);
  int ret = run_outtable_pipeline({"data"}, 4 /*summary_byte_len*/, true /*parse_pieces*/,
      blocksstable::ObDmlFlag::DF_INSERT, true /*is_new_col*/, ObLobLastWriteType::APPEND);
  LOG_INFO("step 2: APPEND summary rejected", K(ret),
      "last_write_type", static_cast<int>(ObLobLastWriteType::APPEND));
  EXPECT_EQ(OB_NOT_SUPPORTED, ret);
  ret = run_outtable_pipeline({"data"}, 4 /*summary_byte_len*/, true /*parse_pieces*/,
      blocksstable::ObDmlFlag::DF_INSERT, true /*is_new_col*/, ObLobLastWriteType::MULTIPART_WRITE);
  LOG_INFO("step 3: MULTIPART_WRITE summary rejected", K(ret),
      "last_write_type", static_cast<int>(ObLobLastWriteType::MULTIPART_WRITE));
  EXPECT_EQ(OB_NOT_SUPPORTED, ret);
}

// A4. Log ingestion and Merger independently validate piece lengths; total length must match the summary.
// Scenario: A 2 MiB + 1 piece, an empty piece and two pieces totaling 4 bytes.
// Steps: Submit the oversized piece through ingress, then directly to Merger; use 3- and 5-byte summaries.
// Checks: Ingress returns OB_INVALID_DATA; Merger rejects oversized pieces and mismatched summary lengths.
// Scope: Bypassing ingress tests Merger's rejection of corrupted data independently of the normal input path.
TEST_F(TestCDCOfOuttableLob, outtable_merger_checks_chunk_and_summary_lengths)
{
  OK(init_merger_environment());
  const std::string oversized(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE + 1, 'x');
  // Check the log-ingress upper and lower piece-size boundaries independently.
  LOG_INFO("step 1: check log-ingress piece-size boundaries", "oversized_length", oversized.length(),
      "chunk_limit", ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE);
  int ret = run_outtable_pipeline({oversized}, oversized.length(), true /*parse_pieces*/);
  LOG_INFO("step 1: oversized piece submitted to log ingress", K(ret), "piece_length", oversized.length());
  EXPECT_EQ(OB_INVALID_DATA, ret);
  ret = run_outtable_pipeline({""}, 0 /*summary_byte_len*/);
  LOG_INFO("step 2: empty piece submitted to log ingress", K(ret), "piece_length", 0);
  EXPECT_EQ(OB_INVALID_DATA, ret);

  // Bypass ingress solely to exercise the merger's independent corruption guard.
  ret = run_outtable_pipeline({oversized}, oversized.length(), false /*parse_pieces*/);
  LOG_INFO("step 3: oversized piece reached merger directly", K(ret),
      "piece_length", oversized.length());
  EXPECT_EQ(OB_INVALID_ARGUMENT, ret);

  // Reject summaries both below and above the actual concatenated byte length.
  ret = run_outtable_pipeline({"ab", "cd"}, 3 /*summary_byte_len*/);
  LOG_INFO("step 4: undersized summary checked", K(ret), "actual_byte_len", 4, "summary_byte_len", 3);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
  ret = run_outtable_pipeline({"ab", "cd"}, 5 /*summary_byte_len*/);
  LOG_INFO("step 5: oversized summary checked", K(ret), "actual_byte_len", 4, "summary_byte_len", 5);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ret);
}

// ============================================================================
// B. Payload validation and subscription/rollback filtering (5 cases)
// ============================================================================

// B1. Invalid fields, empty/oversized payloads and invalid serialized lengths must not populate the LOB cache.
// Scenario: Corrupt the table ID, operation seq, LOB ID, macro ID, piece seq_id or data in a valid body.
// Steps: Construct and parse each variant; also increase and decrease the serialized body length by one byte.
// Checks: Serializing an invalid macro ID returns OB_INVALID_ARGUMENT; other invalid fields and trailing bytes
//         return OB_INVALID_DATA. A truncated body fails to parse, and the cache always stays empty.
// Scope: Change one field at a time; an invalid macro ID is rejected during serialization, before Parser.
TEST_F(TestCDCOfOuttableLob, outtable_parser_rejects_invalid_body_without_cache_entries)
{
  // Start from a valid body so each mutation isolates one validation rule.
  OK(init_merger_environment());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  OK(prepare_part_trans_task(task, transaction::ObTransID(2001), 2001 /*commit_version*/));
  ObLogTenant tenant;
  GIndexCache gi_cache;
  ObLogPartMgr &part_mgr = static_cast<ObLogPartMgr &>(tenant.get_part_mgr());
  OK(part_mgr.init(TENANT_ID, 1 /*start_schema_version*/,
      false /*enable_oracle_mode_match_case_sensitive*/, false /*enable_white_black_list*/, gi_cache));
  ObString seq_id;
  OK(ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, seq_id));
  const transaction::ObTxLobOutTableDataLog::ConstructArg valid_arg(
      LOB_PIECE_TABLE_ID, transaction::ObTxSEQ(10 /*seq*/, 3 /*branch*/), seq_id, expected_lob_id_,
      make_outtable_macro_id(0 /*chunk_index*/), true /*is_logging*/, ObString::make_string("payload"));

  LOG_INFO("step 1: valid reference body prepared", K(valid_arg.lob_aux_table_id_),
      K(valid_arg.lob_operation_seq_no_), "data_length", valid_arg.data_.length());
  auto expect_invalid = [&](const transaction::ObTxLobOutTableDataLog::ConstructArg &arg,
      const int expected_ret) {
    const int ret = parse_outtable_body(arg, task, &tenant);
    LOG_INFO("step 2: invalid body validation completed", K(ret), K(expected_ret), K(arg.lob_aux_table_id_),
        K(arg.lob_operation_seq_no_), K(arg.lob_id_), K(arg.macro_id_),
        "seq_id_length", arg.seq_id_.length(), "data_length", arg.data_.length(),
        "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
    EXPECT_EQ(expected_ret, ret);
    EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  };
  // Reset to the valid reference before corrupting the next field.
  auto arg = valid_arg;
  arg.lob_aux_table_id_ = OB_INVALID_ID;
  expect_invalid(arg, OB_INVALID_DATA);
  arg = valid_arg;
  arg.lob_operation_seq_no_.reset();
  expect_invalid(arg, OB_INVALID_DATA);
  arg = valid_arg;
  arg.lob_id_.reset();
  expect_invalid(arg, OB_INVALID_DATA);
  // MacroBlockId::serialize rejects the invalid ID before the CDC parser runs.
  arg = valid_arg;
  arg.macro_id_.reset();
  expect_invalid(arg, OB_INVALID_ARGUMENT);
  arg = valid_arg;
  arg.seq_id_.reset();
  expect_invalid(arg, OB_INVALID_DATA);
  arg = valid_arg;
  arg.data_.reset();
  expect_invalid(arg, OB_INVALID_DATA);
  const std::string oversized(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE + 1, 'x');
  arg = valid_arg;
  arg.data_.assign_ptr(oversized.data(), oversized.length());
  expect_invalid(arg, OB_INVALID_DATA);
  // Serialized-body boundaries must fail without publishing partial data.
  int ret = parse_outtable_body(valid_arg, task, &tenant, 1 /*length_adjustment*/);
  LOG_INFO("step 3: body with trailing byte parsed", K(ret), "length_adjustment", 1,
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_INVALID_DATA, ret);
  ret = parse_outtable_body(valid_arg, task, &tenant, -1 /*length_adjustment*/);
  LOG_INFO("step 4: truncated body parsed", K(ret), "length_adjustment", -1,
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_NE(OB_SUCCESS, ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
}

// B2. Rollback filtering uses the full transaction sequence, distinguishing the same seq in different branches.
// Scenario: Branch 3 rolls back from operation seq 20 to 5; the piece has operation seq 10.
// Steps: Parse the branch 3 piece, then change only the branch to 4 and read back the cached payload.
// Checks: Branch 3 succeeds without caching data; branch 4 is read back in full and explicitly cleaned up.
// Scope: Pass a null tenant on the rollback path to verify rollback precedes subscription checks and caching.
TEST_F(TestCDCOfOuttableLob, outtable_parser_rollback_retains_transaction_branch)
{
  // The numeric operation sequence is identical across both branches.
  OK(init_merger_environment());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  const transaction::ObTransID trans_id(2002);
  OK(prepare_part_trans_task(task, trans_id, 2002 /*commit_version*/));
  OK(task.push_rollback_to_info(palf::LSN(2 /*offset*/),
      transaction::ObTxSEQ(20 /*seq*/, 3 /*branch*/), transaction::ObTxSEQ(5 /*seq*/, 3 /*branch*/)));
  ObString seq_id;
  OK(ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, seq_id));
  transaction::ObTxLobOutTableDataLog::ConstructArg arg(
      LOB_PIECE_TABLE_ID, transaction::ObTxSEQ(10 /*seq*/, 3 /*branch*/), seq_id, expected_lob_id_,
      make_outtable_macro_id(0 /*chunk_index*/), true /*is_logging*/, ObString::make_string("payload"));
  // No tenant is needed for a rolled-back chunk: filtering/publication is skipped.
  int ret = parse_outtable_body(arg, task, nullptr /*tenant*/);
  LOG_INFO("step 1: rolled-back branch parsed without tenant", K(ret), K(arg.lob_operation_seq_no_),
      "rollback_from_seq", 20, "rollback_to_seq", 5, "rollback_branch", 3,
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());

  // Subscription is needed only for the surviving branch.
  ObLogTenant tenant;
  GIndexCache gi_cache;
  ObLogPartMgr &part_mgr = static_cast<ObLogPartMgr &>(tenant.get_part_mgr());
  OK(part_mgr.init(TENANT_ID, 1 /*start_schema_version*/,
      false /*enable_oracle_mode_match_case_sensitive*/, false /*enable_white_black_list*/, gi_cache));
  arg.lob_operation_seq_no_ = transaction::ObTxSEQ(10 /*seq*/, 4 /*branch*/);
  ret = parse_outtable_body(arg, task, &tenant);
  LOG_INFO("step 2: other branch published", K(ret), K(arg.lob_operation_seq_no_),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(ret);
  const ObLobAuxKey key(2002 /*commit_version*/, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
      expected_lob_id_, arg.lob_operation_seq_no_, seq_id);
  const char *data = nullptr;
  int64_t data_len = 0;
  ret = instance_->lob_aux_storager_.get(allocator, key, data, data_len);
  LOG_INFO("step 3: surviving branch read back", K(ret), K(data_len),
      "expected_data_length", arg.data_.length());
  OK(ret);
  EXPECT_EQ("payload", std::string(data, data_len));
  ret = instance_->lob_aux_storager_.del(key);
  LOG_INFO("step 4: surviving branch cache released", K(ret),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
}

// B3. Unsubscribed PIECE payloads are ignored; adding the table to the table cache allows publication.
// Scenario: Subscription whitelist/blacklist filtering is enabled, with the LOB PIECE table initially absent.
// Steps: Parse the payload, add the PIECE table to the table cache and parse the same payload again.
// Checks: The first parse succeeds with an empty cache; subscription adds one entry, then cleanup removes it.
// Scope: Only Parser subscription filtering is covered, not the full construction of schema subscription data.
TEST_F(TestCDCOfOuttableLob, outtable_parser_filters_unsubscribed_data)
{
  // Keep white-list filtering enabled while changing only the subscription cache.
  OK(init_merger_environment());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  const transaction::ObTransID trans_id(2003);
  OK(prepare_part_trans_task(task, trans_id, 2003 /*commit_version*/));
  ObLogTenant tenant;
  GIndexCache gi_cache;
  ObLogPartMgr &part_mgr = static_cast<ObLogPartMgr &>(tenant.get_part_mgr());
  OK(part_mgr.init(TENANT_ID, 1 /*start_schema_version*/,
      false /*enable_oracle_mode_match_case_sensitive*/, true /*enable_white_black_list*/, gi_cache));
  ObString seq_id;
  OK(ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, seq_id));
  const transaction::ObTxLobOutTableDataLog::ConstructArg arg(
      LOB_PIECE_TABLE_ID, transaction::ObTxSEQ(10 /*seq*/, 3 /*branch*/), seq_id, expected_lob_id_,
      make_outtable_macro_id(0 /*chunk_index*/), true /*is_logging*/, ObString::make_string("payload"));
  int ret = parse_outtable_body(arg, task, &tenant);
  LOG_INFO("step 1: unsubscribed piece table parsed", K(ret), K(LOB_PIECE_TABLE_ID),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());

  // Reparse the same body after subscribing its PIECE table.
  OK(part_mgr.insert_table_id_into_cache(LOB_PIECE_TABLE_ID, 3001 /*database_id*/));
  ret = parse_outtable_body(arg, task, &tenant);
  LOG_INFO("step 2: subscribed piece table parsed", K(ret), K(LOB_PIECE_TABLE_ID),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(ret);
  EXPECT_EQ(1, instance_->lob_aux_storager_.lob_aux_map_.count());
  const ObLobAuxKey key(2003 /*commit_version*/, TENANT_ID, trans_id, LOB_PIECE_TABLE_ID,
      expected_lob_id_, arg.lob_operation_seq_no_, seq_id);
  ret = instance_->lob_aux_storager_.del(key);
  LOG_INFO("step 3: subscribed piece cache released", K(ret),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
}

// B4. Valid no-logging TX markers reach filtering; only consumed markers report unsupported CDC.
// Scenario: A marker has a valid identity and no payload; malformed variants invert the flag/data relation.
// Steps: Enqueue the marker, reject malformed variants, then exercise subscription and rollback decisions.
// Checks: Task owns one marker node; malformed bodies return OB_INVALID_DATA; filtered markers succeed;
//         a subscribed marker returns OB_NOT_SUPPORTED. None of these paths populate the piece cache.
TEST_F(TestCDCOfOuttableLob, outtable_no_logging_marker_respects_filtering)
{
  // The same marker must survive ingress so Parser can apply transaction and subscription rules.
  OK(init_merger_environment());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  OK(prepare_part_trans_task(task, transaction::ObTransID(2004 /*id*/), 2004 /*commit_version*/));
  ObLogTenant tenant;
  GIndexCache gi_cache;
  ObLogPartMgr &part_mgr = static_cast<ObLogPartMgr &>(tenant.get_part_mgr());
  OK(part_mgr.init(TENANT_ID, 1 /*start_schema_version*/,
      false /*enable_oracle_mode_match_case_sensitive*/, true /*enable_white_black_list*/, gi_cache));
  ObString seq_id;
  OK(ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, seq_id));
  const transaction::ObTxLobOutTableDataLog::ConstructArg marker(
      LOB_PIECE_TABLE_ID, transaction::ObTxSEQ(10 /*seq*/, 3 /*branch*/), seq_id, expected_lob_id_,
      make_outtable_macro_id(0 /*chunk_index*/), false /*is_logging*/, ObString());
  LOG_INFO("step 1: enqueue marker without payload", K(marker));
  OK(push_outtable_body(task, palf::LSN(1 /*offset*/),
      transaction::ObTxLobOutTableDataLog(marker), false /*append_compat_tail*/));
  ASSERT_EQ(1, task.get_sorted_redo_list().get_node_number());

  // A no-logging flag with data, or a logging flag without data, is corruption even before filtering.
  for (const bool is_logging : {false, true}) {
    auto invalid_arg = marker;
    invalid_arg.is_logging_ = is_logging;
    invalid_arg.data_ = is_logging ? ObString() : ObString::make_string("unexpected payload");
    const int push_ret = push_outtable_body(task, palf::LSN(2 /*offset*/),
        transaction::ObTxLobOutTableDataLog(invalid_arg), false /*append_compat_tail*/);
    const int parse_ret = parse_outtable_body(invalid_arg, task, &tenant);
    LOG_INFO("step 2: malformed marker checked at ingress and parser", K(is_logging),
        K(push_ret), K(parse_ret), "node_count", task.get_sorted_redo_list().get_node_number(),
        "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
    EXPECT_EQ(OB_INVALID_DATA, push_ret);
    EXPECT_EQ(OB_INVALID_DATA, parse_ret);
    EXPECT_EQ(1, task.get_sorted_redo_list().get_node_number());
    EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  }

  // An unsubscribed table must not stop CDC merely because it omits payloads.
  int ret = parse_outtable_body(marker, task, &tenant);
  LOG_INFO("step 3: unsubscribed marker filtered", K(ret), K(marker),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(part_mgr.insert_table_id_into_cache(LOB_PIECE_TABLE_ID, 3001 /*database_id*/));
  ret = parse_outtable_body(marker, task, &tenant);
  LOG_INFO("step 4: subscribed marker rejected explicitly", K(ret), K(marker),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_NOT_SUPPORTED, ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());

  // Rollback precedes both subscription lookup and the no-logging error.
  OK(task.push_rollback_to_info(palf::LSN(3 /*offset*/),
      transaction::ObTxSEQ(20 /*seq*/, 3 /*branch*/), transaction::ObTxSEQ(5 /*seq*/, 3 /*branch*/)));
  ret = parse_outtable_body(marker, task, nullptr /*tenant*/);
  LOG_INFO("step 5: rolled-back marker skipped without tenant", K(ret), K(marker.lob_operation_seq_no_),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
}

// B5. No-logging ExtInfo markers fail explicitly only after their table passes subscription filtering.
// Scenario: An old-image marker carries a valid piece identity and an empty payload.
// Steps: Parse it before and after subscribing the PIECE table, then parse inconsistent flag/data pairs.
// Checks: Filtering succeeds; a consumed marker returns OB_NOT_SUPPORTED; inconsistent bodies return
//         OB_INVALID_DATA. The auxiliary cache remains empty throughout.
TEST_F(TestCDCOfOuttableLob, lob_data_ext_info_no_logging_marker_respects_filtering)
{
  // Use the production ExtInfo decoder and publisher without constructing unrelated mutator rows.
  OK(init_merger_environment());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  OK(prepare_part_trans_task(task, transaction::ObTransID(2005 /*id*/), 2005 /*commit_version*/));
  ObLogTenant tenant;
  GIndexCache gi_cache;
  ObLogPartMgr &part_mgr = static_cast<ObLogPartMgr &>(tenant.get_part_mgr());
  OK(part_mgr.init(TENANT_ID, 1 /*start_schema_version*/,
      false /*enable_oracle_mode_match_case_sensitive*/, true /*enable_white_black_list*/, gi_cache));
  ObLobDataExtInfoLog log;
  log.lob_aux_table_id_ = LOB_PIECE_TABLE_ID;
  log.lob_operation_seq_no_ = transaction::ObTxSEQ(10 /*seq*/, 3 /*branch*/);
  OK(ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, log.seq_id_));
  log.is_logging_ = false;
  ASSERT_TRUE(log.is_valid());
  auto parse_ext_info = [&]() {
    std::vector<char> body(log.get_serialize_size());
    int64_t pos = 0;
    int ret = log.serialize(body.data(), body.size(), pos);
    if (OB_SUCC(ret)) {
      const ObString ext_info(pos, body.data());
      pos = 0;
      ObLogPartTransParser parser;
      ret = parser.publish_lob_data_ext_info_log_(&tenant, expected_lob_id_, ext_info, pos, task);
    }
    return ret;
  };

  int ret = parse_ext_info();
  LOG_INFO("step 1: unsubscribed ExtInfo marker filtered", K(ret), K(log),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(part_mgr.insert_table_id_into_cache(LOB_PIECE_TABLE_ID, 3001 /*database_id*/));
  ret = parse_ext_info();
  LOG_INFO("step 2: subscribed ExtInfo marker rejected explicitly", K(ret), K(log),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_NOT_SUPPORTED, ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());

  // Marker validity is independent of the consumer's ability to reconstruct the LOB.
  for (const bool is_logging : {false, true}) {
    log.is_logging_ = is_logging;
    log.lob_data_ = is_logging ? ObString() : ObString::make_string("unexpected payload");
    ret = parse_ext_info();
    LOG_INFO("step 3: inconsistent ExtInfo flag and payload rejected", K(ret), K(log),
        "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
    EXPECT_EQ(OB_INVALID_DATA, ret);
    EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  }
}

// ============================================================================
// C. Waiting for missing pieces and stopping (2 cases)
// ============================================================================

// C1. Stop a wait for a missing piece and restore the pending count.
// Scenario: The real fragment handler cannot find the piece in either cache or storage.
// Steps: The helper signals stop after the original wait macro registers a pending task.
// Checks: OB_IN_STOP_STATE is returned; the helper checks the wait signal and pending count after exit.
// Scope: Preserve the macro's indefinite wait; the 10-second stop guard only prevents a hung test.
TEST_F(TestCDCOfOuttableLob, missing_lob_data_stop_balances_pending)
{
  // Stop only after the real fragment wait has registered its pending task.
  LOG_INFO("step 1: wait for missing fragment before stopping", "test_stop_guard_seconds", 10);
  const int ret = run_missing_lob_wait(LobWaitResult::STOP);
  LOG_INFO("step 2: stopped fragment wait completed", K(ret));
  EXPECT_EQ(OB_IN_STOP_STATE, ret);
}

// C2. Publish a missing piece during the wait, balancing the pending count and task references.
// Scenario: The fragment handler initially finds no piece; binary data with embedded zeros is cached later.
// Steps: The helper publishes data after pending registration, allowing the original wait macro to succeed.
// Checks: Success, correct data, a zero pending count, and release of the fragment reference.
// Scope: Retain a prepare-reference sentinel to avoid Formatter callbacks; the 10-second guard protects the test.
TEST_F(TestCDCOfOuttableLob, delayed_lob_data_publish_balances_pending)
{
  // Publish only after the original retry macro has observed the missing fragment.
  LOG_INFO("step 1: wait for missing fragment before publishing", "test_stop_guard_seconds", 10);
  const int ret = run_missing_lob_wait(LobWaitResult::PUBLISH);
  LOG_INFO("step 2: delayed fragment publication completed", K(ret));
  EXPECT_EQ(OB_SUCCESS, ret);
}

// ============================================================================
// D. System LS DDL (4 cases)
// ============================================================================

// D1. Accept only DDL PIECE bodies on the system LS and preserve branch identity in rollback filtering.
// Scenario: User PIECE, DDL main, and DDL META tables, plus non-DDL transactions, appear on the system LS.
// Steps: Parse each filtered combination, then DDL PIECE bodies on rolled-back branch 3 and valid branch 4.
// Checks: Filtered or rolled-back bodies are not cached; branch 4 can be read by its full operation sequence
//         number and then removed from the cache.
// Scope: No tenant object is provided; both table ID and transaction type must satisfy the DDL path conditions.
TEST_F(TestCDCOfOuttableLob, ddl_outtable_filters_piece_table_and_full_rollback_sequence)
{
  // SYS LS bodies have no tenant pointer; filtering must rely on task and table identity.
  OK(init_merger_environment());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  OK(prepare_ddl_part_trans_task(task, 3001 /*commit_version*/));
  ObString seq_id;
  OK(ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, seq_id));
  transaction::ObTxLobOutTableDataLog::ConstructArg arg(
      share::OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TID, transaction::ObTxSEQ(10 /*seq*/, 3 /*branch*/),
      seq_id, expected_lob_id_, make_outtable_macro_id(0 /*chunk_index*/), true /*is_logging*/,
      ObString::make_string("ddl chunk"));

  // SYS LS contains other inner-table transactions; only DDL's PIECE table is subscribed.
  const uint64_t filtered_tables[] = {LOB_PIECE_TABLE_ID,
      share::OB_ALL_DDL_OPERATION_TID, share::OB_ALL_DDL_OPERATION_AUX_LOB_META_TID};
  for (const uint64_t table_id : filtered_tables) {
    arg.lob_aux_table_id_ = table_id;
    const int ret = parse_outtable_body(arg, task, nullptr /*tenant*/);
    LOG_INFO("step 1: unrelated SYS LS table filtered", K(ret), K(table_id),
        "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  }
  arg.lob_aux_table_id_ = share::OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TID;
  // The PIECE table alone is insufficient when the transaction is not DDL.
  task.set_type(PartTransTask::TASK_TYPE_DML_TRANS);
  int ret = parse_outtable_body(arg, task, nullptr /*tenant*/);
  LOG_INFO("step 2: non-DDL SYS LS transaction filtered", K(ret), K(arg.lob_aux_table_id_),
      "is_ddl_trans", task.is_ddl_trans(),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  task.set_type(PartTransTask::TASK_TYPE_DDL_TRANS);

  // Preserve the branch when comparing the DDL piece against the rollback range.
  OK(task.push_rollback_to_info(palf::LSN(2 /*offset*/),
      transaction::ObTxSEQ(20 /*seq*/, 3 /*branch*/), transaction::ObTxSEQ(5 /*seq*/, 3 /*branch*/)));
  ret = parse_outtable_body(arg, task, nullptr /*tenant*/);
  LOG_INFO("step 3: rolled-back DDL branch filtered", K(ret), K(arg.lob_operation_seq_no_),
      "rollback_from_seq", 20, "rollback_to_seq", 5, "rollback_branch", 3,
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  arg.lob_operation_seq_no_ = transaction::ObTxSEQ(10 /*seq*/, 4 /*branch*/);
  ret = parse_outtable_body(arg, task, nullptr /*tenant*/);
  LOG_INFO("step 4: other DDL branch published", K(ret), K(arg.lob_operation_seq_no_),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(ret);
  const ObLobAuxKey key(3001 /*commit_version*/, TENANT_ID, task.get_trans_id(), arg.lob_aux_table_id_,
      expected_lob_id_, arg.lob_operation_seq_no_, seq_id);
  const char *data = nullptr;
  int64_t len = 0;
  ret = instance_->lob_aux_storager_.memory_get_(key, data, len);
  LOG_INFO("step 5: surviving DDL piece read back", K(ret), K(len),
      "expected_data_length", arg.data_.length());
  OK(ret);
  EXPECT_EQ("ddl chunk", std::string(data, len));
  ret = instance_->lob_aux_storager_.del(key);
  LOG_INFO("step 6: DDL piece cache released", K(ret),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
}

// D2. Retain unreferenced DDL pieces after a successful scan until the commit-version watermark advances.
// Scenario: One DDL PIECE body has an unknown compatible tail and no main row referencing it.
// Steps: Enqueue the raw body, overwrite its source, scan DDL, then purge at the same and next commit versions.
// Checks: The owning node preserves every wire byte; the scan caches one piece; only a higher watermark removes it.
// Scope: Exercise memory_del_ directly; this does not verify watermark advancement by the background scheduler.
TEST_F(TestCDCOfOuttableLob, ddl_outtable_unreferenced_chunks_use_commit_version_cleanup)
{
  // No main-row locator or summary refers to this otherwise valid DDL piece.
  OK(init_merger_environment());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  OK(prepare_ddl_part_trans_task(task, 3003 /*commit_version*/));
  ObString seq_id;
  OK(ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, seq_id));
  const transaction::ObTxLobOutTableDataLog::ConstructArg arg(
      share::OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TID, transaction::ObTxSEQ(10 /*seq*/, 3 /*branch*/),
      seq_id, expected_lob_id_, make_outtable_macro_id(0 /*chunk_index*/), true /*is_logging*/,
      ObString::make_string("orphan"));
  OK(push_outtable_body(task, palf::LSN(1 /*offset*/),
      transaction::ObTxLobOutTableDataLog(arg), true /*append_compat_tail*/));
  LOG_INFO("step 1: unreferenced DDL piece enqueued", K(arg.lob_aux_table_id_),
      K(arg.lob_operation_seq_no_), "data_length", arg.data_.length(), "commit_version", 3003);
  ObLogMetaManager meta_manager;
  ObLogPartTransParser parser;
  parser.meta_manager_ = &meta_manager;
  volatile bool stop_flag = false;
  LobWaitTenantMgr tenant_mgr(TENANT_ID);
  IObLogTenantMgr *saved_tenant_mgr = instance_->tenant_mgr_;
  instance_->tenant_mgr_ = &tenant_mgr;
  const int ret = parser.parse_ddl_redo_log_(task, false /*is_build_baseline*/, stop_flag);
  instance_->tenant_mgr_ = saved_tenant_mgr;
  LOG_INFO("step 2: DDL scan without main row completed", K(ret),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, instance_->lob_aux_storager_.lob_aux_map_.count());
  // Unreferenced chunks remain after a successful scan as well. Test only the
  // memory purge primitive used by the existing cleanup task.
  int cleanup_ret = instance_->lob_aux_storager_.memory_del_(TENANT_ID, 3003 /*commit_version*/);
  LOG_INFO("step 3: orphan cleanup at equal commit version", K(cleanup_ret), "cleanup_commit_version", 3003,
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, cleanup_ret);
  EXPECT_EQ(1, instance_->lob_aux_storager_.lob_aux_map_.count());
  cleanup_ret = instance_->lob_aux_storager_.memory_del_(TENANT_ID, 3004 /*commit_version*/);
  LOG_INFO("step 4: orphan cleanup beyond transaction commit version", K(cleanup_ret),
      "cleanup_commit_version", 3004,
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_SUCCESS, cleanup_ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
}

// D3. Reconstruct SQL and clear the cache when the DDL main row arrives before the summary and pieces.
// Scenario: The main row has an out-row locator; a 2 MiB first piece plus a short tail makes SQL exceed 2 MiB.
// Steps: Prepare the main row, parse a real META summary, then enqueue two raw PIECE bodies in reverse order.
//        Overwrite the input buffers and verify each owning node before parsing its body.
//        Call parse_ddl_info_ for synchronous assembly in piece sequence order, then clean up by LOB image.
// Checks: The summary and two pieces occupy three cache entries; SQL, the DDL flag and schema version are correct.
//         The merged SQL remains valid after overwriting log body buffers and deleting the cached data.
// Scope: Bypass the full finalizer with a nonempty pending list; BR allocation and output are not covered.
TEST_F(TestCDCOfOuttableLob, ddl_main_row_before_summary_and_two_megabyte_chunks)
{
  // Keep the DDL SQL larger than one piece while reaching the exact per-piece limit.
  LOG_INFO("step 1: initialize DDL transaction and construct a multi-piece SQL image");
  OK(init_merger_environment());
  OK(instance_->global_info_.init());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  OK(prepare_ddl_part_trans_task(task, 3004 /*commit_version*/));
  const transaction::ObTxSEQ operation_seq(10 /*seq*/, 3 /*branch*/);
  const blocksstable::ObDmlRowFlag insert_flag(blocksstable::ObDmlFlag::DF_INSERT);
  // The first fragment reaches the OUT_TABLE limit; the final SQL crosses it.
  std::string first_piece = "CREATE TABLE t (c INT) COMMENT='";
  first_piece.resize(ObLobMetaUtil::LOB_OUTTABLE_BLOCK_DATA_SIZE, 'a');
  const std::vector<std::string> pieces = {first_piece, "tail';"};
  const std::string expected_sql = pieces[0] + pieces[1];

  LOG_INFO("step 1 complete: SQL image constructed", "commit_version", 3004,
      "first_piece_len", pieces[0].length(), "last_piece_len", pieces[1].length(),
      "expected_sql_len", expected_sql.length(), K(operation_seq));

  // Build a locator-bearing main row before any summary or piece is available.
  LOG_INFO("step 2: build and prepare the DDL main row before auxiliary data");
  ObLobDiskLocatorBuilder locator_builder;
  ObLobLocatorV2 locator;
  ObString disk_locator;
  OK(locator_builder.init_outrow(allocator));
  OK(locator_builder.set_lob_id(expected_lob_id_));
  OK(locator_builder.set_seq_no(operation_seq));
  OK(locator_builder.to_locator(locator));
  OK(locator.get_disk_locator(disk_locator));

  ObLogAllDdlOperationSchemaInfo ddl_schema;
  OK(ddl_schema.init());
  const auto &ddl_columns = ddl_schema.get_cols_des_array();
  std::vector<ObObj> ddl_values(ddl_columns.count());
  for (int64_t i = 0; i < ddl_columns.count(); ++i) {
    ObObj &value = ddl_values[i];
    switch (ddl_columns[i].col_id_) {
      case ALL_DDL_OPERATION_TABLE_SCHEMA_VERSION_COLUMN_ID: value.set_int(3004); break;
      case ALL_DDL_OPERATION_TABLE_TENANT_ID_COLUMN_ID:
      case ALL_DDL_OPERATION_TABLE_EXEC_TENANT_ID_COLUMN_ID: value.set_int(TENANT_ID); break;
      case ALL_DDL_OPERATION_TABLE_DATABASE_ID_COLUMN_ID: value.set_int(100); break;
      case ALL_DDL_OPERATION_TABLE_TABLEGROUP_ID_COLUMN_ID: value.set_int(0); break;
      case ALL_DDL_OPERATION_TABLE_TABLE_ID_COLUMN_ID: value.set_int(3001); break;
      case ALL_DDL_OPERATION_TABLE_OPERATION_TYPE_COLUMN_ID:
        value.set_int(share::schema::OB_DDL_CREATE_TABLE); break;
      case ALL_DDL_OPERATION_TABLE_DDL_STMT_STR_COLUMN_ID:
        value.set_varchar(disk_locator); break;
      default: value.set_null(); break;
    }
  }
  MacroBlockMutatorRow ddl_row(allocator, insert_flag, operation_seq);
  OK(init_macroblock_row(allocator, ddl_values,
      ddl_schema.get_table_schema().get_rowkey_column_num(), ddl_row));
  OK(ddl_row.parse_cols(nullptr /*obj2str_helper*/, TENANT_ID,
      share::OB_ALL_DDL_OPERATION_TID, nullptr /*tb_schema_info*/, nullptr /*tz_info_wrap*/,
      false /*enable_output_hidden_primary_key*/, &ddl_schema));
  ASSERT_TRUE(ddl_row.get_lob_ctx_cols().has_out_row_lob());
  ObLogPartTransParser parser;
  parser.inited_ = true;
  ObSEArray<DdlStmtTask *, 16> pending;
  OK(parser.prepare_ddl_stmt_(0 /*row_index*/, ddl_row, task, pending));
  LOG_INFO("step 2 complete: main row is pending without auxiliary data",
      "pending_count", pending.count(), "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  ASSERT_EQ(1, pending.count());
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());

  // Publish a real META summary row after the main row was prepared.
  LOG_INFO("step 3: publish META summary for the pending DDL image",
      "expected_sql_len", expected_sql.length(), "piece_count", pieces.size());
  const char *summary_json = "{\"extra_info_type\":1,\"lob_chunk_type\":\"OUT_TABLE\","
      "\"chunk_count\":2,\"last_write_type\":0}";
  OK(build_summary_row(summary_json, expected_sql.length()));
  const auto &meta_schema = instance_->global_info_.get_lob_aux_meta_table_schema_info();
  const auto &meta_columns = meta_schema.get_cols_des_array();
  std::vector<ObObj> meta_values(meta_columns.count());
  for (int64_t i = 0; i < meta_columns.count(); ++i) {
    const int64_t index = meta_columns[i].col_id_ - OB_APP_MIN_COLUMN_ID;
    ASSERT_GE(index, 0);
    ASSERT_LT(index, ObLobMetaInfo::COLUMN_CNT);
    meta_values[i] = col_values_[index].value_;
  }
  MacroBlockMutatorRow meta_row(allocator, insert_flag, operation_seq);
  OK(init_macroblock_row(allocator, meta_values,
      meta_schema.get_table_schema().get_rowkey_column_num(), meta_row));
  OK(parser.parse_ddl_lob_aux_stmts_(
      share::OB_ALL_DDL_OPERATION_AUX_LOB_META_TID, ObCDCLobAuxTableType::META,
      1 /*row_index*/, meta_row, task));
  LOG_INFO("step 3 complete: summary is cached before pieces",
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  ASSERT_EQ(1, instance_->lob_aux_storager_.lob_aux_map_.count());

  // Reverse physical arrival order: reconstruction must follow chunk seq_id,
  // while the key's operation seq retains its branch and is shared by locator/META.
  LOG_INFO("step 4: enqueue pieces in reverse arrival order", "piece_count", pieces.size());
  for (int64_t i = pieces.size() - 1; i >= 0; --i) {
    LOG_INFO("step 4: enqueue one piece", K(i), "piece_len", pieces[i].length(),
        "log_lsn", palf::LSN(10 - i /*offset*/));
    ObString seq_id;
    OK(ObLobSeqId::get_seq_id(allocator, i + 1, seq_id));
    const transaction::ObTxLobOutTableDataLog::ConstructArg arg(
        share::OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TID, operation_seq, seq_id,
        expected_lob_id_, make_outtable_macro_id(i), true /*is_logging*/,
        ObString(pieces[i].length(), pieces[i].data()));
    OK(push_outtable_body(task, palf::LSN(10 - i /*offset*/),
        transaction::ObTxLobOutTableDataLog(arg), false /*append_compat_tail*/));
  }
  // The parser must copy piece bytes before the serialized redo buffer is reused.
  LOG_INFO("step 5: parse queued pieces and overwrite their source buffers");
  volatile bool stop_flag = false;
  for (RedoNodeIterator it = task.get_sorted_redo_list().redo_iter_begin();
      it != task.get_sorted_redo_list().redo_iter_end(); it++) {
    DdlRedoLogNode &node = static_cast<DdlRedoLogNode &>(*it);
    LOG_INFO("step 5: publish one typed redo body", "log_lsn", node.get_start_log_lsn(),
        "body_len", node.get_data_len(), "is_lob_log", node.is_lob_outtable_data_log());
    ASSERT_TRUE(node.is_lob_outtable_data_log());
    OK(parser.parse_lob_outtable_data_log_(
        nullptr /*tenant*/, node, task));
    MEMSET(const_cast<char *>(node.get_data()), 0, node.get_data_len());
  }
  LOG_INFO("step 5 complete: summary and both pieces are cached",
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(3, instance_->lob_aux_storager_.lob_aux_map_.count());

  // Exercise the real DDL statement endpoint (including synchronous LobMerger),
  // stopping before unrelated binlog-record allocation/output.
  LOG_INFO("step 6: parse the pending statement and synchronously merge its LOB");
  bool contains_ddl = false;
  int64_t schema_version = 0;
  IObCDCLobDataMerger *saved_merger = instance_->lob_data_merger_;
  instance_->lob_data_merger_ = &merger_;
  const int ret = pending[0]->parse_ddl_info_(false /*is_build_baseline*/, contains_ddl, schema_version, stop_flag);
  instance_->lob_data_merger_ = saved_merger;
  LOG_INFO("step 6 complete: verify DDL metadata and reconstructed SQL", KR(ret),
      K(contains_ddl), K(schema_version), "expected_sql_len", expected_sql.length(),
      "actual_sql_len", pending[0]->get_ddl_stmt_str().length());
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(contains_ddl);
  EXPECT_EQ(3004, schema_version);
  EXPECT_EQ(expected_sql, std::string(pending[0]->get_ddl_stmt_str().ptr(),
      pending[0]->get_ddl_stmt_str().length()));
  // Use the same image cleanup as successful DDL finalization; the merged SQL
  // owns its buffer and must remain valid after the cache entries are removed.
  LOG_INFO("step 7: reclaim auxiliary cache while retaining the assembled SQL");
  EXPECT_EQ(OB_SUCCESS, instance_->lob_aux_storager_.del(
      pending[0]->get_lob_ctx_cols(), stop_flag));
  LOG_INFO("step 7 complete: verify cache removal and SQL ownership",
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count(),
      "expected_sql_len", expected_sql.length(), "actual_sql_len", pending[0]->get_ddl_stmt_str().length());
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(expected_sql, std::string(pending[0]->get_ddl_stmt_str().ptr(),
      pending[0]->get_ddl_stmt_str().length()));
  // Release the pending statement after checking that its SQL buffer survived cache cleanup.
  parser.release_pending_ddl_stmts_(pending, task);
}

// D4. System LS filtering precedes no-logging errors, and the DDL scan rejects a required marker.
// Scenario: A no-payload marker first belongs to an unrelated table or a non-DDL transaction.
// Steps: Check both filtered cases, then enqueue it as a DDL PIECE and run the real DDL redo scan.
// Checks: Filtered markers succeed; Task accepts and owns the DDL marker; scanning returns
//         OB_NOT_SUPPORTED without publishing a piece.
TEST_F(TestCDCOfOuttableLob, ddl_outtable_no_logging_marker_respects_filtering)
{
  // System LS filtering uses DDL identity rather than the user-table subscription cache.
  OK(init_merger_environment());
  ObArenaAllocator allocator;
  PartTransTask task;
  task.set_allocator(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator);
  OK(prepare_ddl_part_trans_task(task, 3005 /*commit_version*/));
  ObString seq_id;
  OK(ObLobSeqId::get_seq_id(allocator, 1 /*idx*/, seq_id));
  transaction::ObTxLobOutTableDataLog::ConstructArg marker(
      LOB_PIECE_TABLE_ID, transaction::ObTxSEQ(10 /*seq*/, 3 /*branch*/), seq_id, expected_lob_id_,
      make_outtable_macro_id(0 /*chunk_index*/), false /*is_logging*/, ObString());
  int ret = parse_outtable_body(marker, task, nullptr /*tenant*/);
  LOG_INFO("step 1: unrelated system LS marker filtered", K(ret), K(marker.lob_aux_table_id_),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());

  marker.lob_aux_table_id_ = share::OB_ALL_DDL_OPERATION_AUX_LOB_PIECE_TID;
  task.set_type(PartTransTask::TASK_TYPE_DML_TRANS);
  ret = parse_outtable_body(marker, task, nullptr /*tenant*/);
  LOG_INFO("step 2: non-DDL system LS marker filtered", K(ret),
      "is_ddl_trans", task.is_ddl_trans(), "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  OK(ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());

  // Preserve the marker through DDL ingestion and exercise error propagation through the DDL scan.
  task.set_type(PartTransTask::TASK_TYPE_DDL_TRANS);
  OK(push_outtable_body(task, palf::LSN(1 /*offset*/),
      transaction::ObTxLobOutTableDataLog(marker), false /*append_compat_tail*/));
  ObLogMetaManager meta_manager;
  ObLogPartTransParser parser;
  parser.meta_manager_ = &meta_manager;
  volatile bool stop_flag = false;
  LobWaitTenantMgr tenant_mgr(TENANT_ID);
  IObLogTenantMgr *saved_tenant_mgr = instance_->tenant_mgr_;
  instance_->tenant_mgr_ = &tenant_mgr;
  ret = parser.parse_ddl_redo_log_(task, false /*is_build_baseline*/, stop_flag);
  instance_->tenant_mgr_ = saved_tenant_mgr;
  LOG_INFO("step 3: DDL scan rejected no-logging marker", K(ret),
      "node_count", task.get_sorted_redo_list().get_node_number(),
      "cache_count", instance_->lob_aux_storager_.lob_aux_map_.count());
  EXPECT_EQ(OB_NOT_SUPPORTED, ret);
  EXPECT_EQ(0, instance_->lob_aux_storager_.lob_aux_map_.count());
}

} // namespace libobcdc
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger &logger = oceanbase::common::ObLogger::get_logger();\
  system("rm -f ./test_cdc_of_outtable_lob.log*");
  logger.set_file_name("test_cdc_of_outtable_lob.log", true /*no_redirect_flag*/);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
