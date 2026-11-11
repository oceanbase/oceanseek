/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define USING_LOG_PREFIX STORAGE
#include "mtlenv/mock_tenant_module_env.h"
#include "lib/alloc/alloc_func.h"
#include <gtest/gtest.h>
#include <cstdlib>
#include <memory>
#include <vector>

#define private public
#define protected public
#include "share/ob_cluster_version.h"
#include "share/ob_ddl_common.h"
#include "share/vector_index/ob_ivf_replay_memory_mgr.h"
#include "storage/blocksstable/vector_index/ob_ivf_input_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_tablet_context.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/direct_load/ob_direct_load_batch_datum_rows.h"
#include "storage/direct_load/ob_direct_load_vector.h"
#include "storage/tmp_file/ob_tmp_file_manager.h"
#undef protected
#undef private

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;
namespace
{
constexpr int64_t DIM = 768;
// Only the DDL catalog/task shell is seeded. Writer, Capture, Replay, buffers,
// allocators, map switching and destruction execute their production code.
struct Inputs
{
  ObColumnClusteredDag dag;
  ObISliceWriter *writer = nullptr;
  ObDirectLoadBatchRows sql_rows;
  std::vector<ObDDLTabletContext *> contexts;

  ~Inputs() { destroy_writer(); }
  void destroy_writer()
  {
    if (writer != nullptr) {
      writer->~ObISliceWriter();
      ob_free(writer);
      writer = nullptr;
    }
  }
  void init(const int64_t keys, const bool reshape = false)
  {
    ASSERT_NE(nullptr, MTL(share::ObIVFReplayMemoryMgr *));
    dag.is_inited_ = true;
    dag.direct_load_type_ = SS_IDEM_DIRECT_LOAD_DDL;
    dag.ddl_thread_count_ = 1;
    dag.ddl_task_param_.tenant_data_version_ = DATA_CURRENT_VERSION;
    dag.ddl_task_param_.schema_version_ = 1;
    dag.ddl_task_param_.snapshot_version_ = 10;
    dag.ddl_task_param_.ddl_task_id_ = 1;
    dag.ddl_task_param_.max_batch_size_ = 256;
    auto &schema = dag.ddl_table_schema_;
    schema.table_id_ = 50001;
    schema.table_item_.rowkey_column_num_ = 3;
    schema.table_item_.vec_dim_ = DIM;
    schema.table_item_.is_index_table_ = true;
    schema.table_item_.index_type_ = INDEX_TYPE_ASYNC_IVF_INDEX_LOCAL;
    schema.table_item_.vec_idx_param_ = ObString::make_string("TYPE=DISK_BBQ,DISTANCE=L2,CLUSTER_SIZE=384,LIB=OB");
    for (int64_t i = 0; i < 6; ++i) {
      ObColumnSchemaItem item;
      item.is_valid_ = i != 3 && i != 4;
      item.is_nullable_ = true;
      item.is_rowkey_column_ = i < 3;
      item.col_type_.set_type(i == 5 ? ObVarcharType : (i == 2 ? ObUInt64Type : ObIntType));
      if (i == 5) {
        item.col_type_.set_collation_type(CS_TYPE_BINARY);
        item.col_accuracy_.set_length(DIM * sizeof(float));
      }
      ObColDesc desc;
      desc.col_id_ = i == 2 ? OB_HIDDEN_PK_INCREMENT_COLUMN_ID : OB_APP_MIN_COLUMN_ID + i;
      desc.col_type_ = item.col_type_;
      ASSERT_EQ(OB_SUCCESS, schema.column_items_.push_back(item));
      ASSERT_EQ(OB_SUCCESS, schema.column_descs_.push_back(desc));
    }
    if (reshape) { ASSERT_EQ(OB_SUCCESS, schema.reshape_column_idxs_.push_back(5)); }
    ASSERT_EQ(OB_SUCCESS, dag.tablet_context_map_.create(keys, ObMemAttr(MTL_ID(), "IVFMemProbe")));
    for (int64_t i = 0; i < keys; ++i) {
      auto *ctx = OB_NEWx(ObDDLTabletContext, &dag.arena_);
      ASSERT_NE(nullptr, ctx);
      ASSERT_EQ(OB_SUCCESS, dag.tablet_context_map_.set_refactored(ObTabletID(200001+i), ctx));
      contexts.push_back(ctx);
      ctx->ls_id_ = ObLSID(1);
      ctx->tablet_id_ = ObTabletID(200001+i);
      ctx->slice_count_ = 1;
      ctx->is_inited_ = true;
      ASSERT_EQ(OB_SUCCESS, ctx->init_ivf_context(DIM));
      // Reserve the same zero cache budget as the real tests, without relying
      // on tenant-config bootstrapping in the mock storage environment.
      ASSERT_EQ(OB_SUCCESS, MTL(share::ObIVFReplayMemoryMgr *)->reserve(
          0, 1, ctx->ivf_ctx_->replay_memory_reservation_));
      ctx->ivf_ctx_->capture_slice_count_ = 1;
    }
    ASSERT_EQ(OB_SUCCESS, ObDDLUtil::init_batch_rows(schema, 256, sql_rows));
  }
  void select(int64_t key, bool batch)
  {
    ASSERT_EQ(OB_SUCCESS, ObIVFInputWriter::get_writer(dag, contexts.at(key)->tablet_id_, 0, batch, writer));
  }
};

int append_row(ObISliceWriter &writer, uint64_t vid, const float *payload, int64_t bytes = DIM * sizeof(float))
{
  ObStorageDatum cells[4];
  ObSEArray<ObDatum *, 4> datums;
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < 4; ++i) {
    cells[i].set_int(0);
    ret = datums.push_back(&cells[i]);
  }
  cells[2].set_uint(vid);
  cells[3].set_string(reinterpret_cast<const char *>(payload), bytes);
  return OB_SUCCESS == ret ? writer.append_current_row(datums) : ret;
}

void check_replay(ObDDLTabletContext &context, const std::vector<uint64_t> &vids, int64_t slice_idx = 0)
{
  ASSERT_GT(context.ivf_ctx_->slice_inputs_.count(), slice_idx);
  auto &replay = context.ivf_ctx_->slice_inputs_.at(slice_idx)->replay_;
  ASSERT_TRUE(replay.is_sealed());
  ASSERT_EQ(vids.size(), replay.get_row_count());
  for (int64_t i = 0; i < vids.size(); ++i) {
    uint64_t vid = 0;
    const float *values = nullptr;
    ASSERT_EQ(OB_SUCCESS, replay.get_row_at(i, vid, values));
    ASSERT_EQ(vids[i], vid);
    ASSERT_NE(nullptr, values);
    float expected[DIM] = {};
    expected[0] = static_cast<float>(vid);
    ASSERT_EQ(OB_SUCCESS, context.ivf_ctx_->get_rotator().rotate(DIM, expected));
    for (int64_t d = 0; d < DIM; ++d) {
      EXPECT_FLOAT_EQ(expected[d], values[d]) << "vid=" << vid << ", dim=" << d;
    }
  }
}
} // anonymous namespace

TEST(IVFInputWriter, interleaved_slices_flush_to_their_own_replay)
{
  // Exercise both possible owners of the pending batch at unordered map close.
  for (int64_t final_key : {0L, 1L}) {
    Inputs input;
    ASSERT_NO_FATAL_FAILURE(input.init(2));
    for (int64_t i = 0; i < 3; ++i) {
      ASSERT_NO_FATAL_FAILURE(input.select(i == 2 ? final_key : i, false));
      float payload[DIM] = {};
      payload[0] = 11 + i;
      ASSERT_EQ(OB_SUCCESS, append_row(*input.writer, 11 + i, payload));
      int64_t count = 0;
      ASSERT_EQ(OB_SUCCESS, input.writer->get_row_count(count));
      EXPECT_EQ(i == 2 ? 2 : 1, count);
    }
    ASSERT_EQ(OB_SUCCESS, input.writer->close());
    EXPECT_EQ(OB_SUCCESS, input.writer->close()); // idempotent
    ASSERT_NO_FATAL_FAILURE(check_replay(*input.contexts.at(0),
        final_key == 0 ? std::vector<uint64_t>{11, 13} : std::vector<uint64_t>{11}));
    ASSERT_NO_FATAL_FAILURE(check_replay(*input.contexts.at(1),
        final_key == 1 ? std::vector<uint64_t>{12, 13} : std::vector<uint64_t>{12}));
  }
}

TEST(IVFInputWriter, workers_keep_independent_pending_batches)
{
  Inputs input;
  ASSERT_NO_FATAL_FAILURE(input.init(2));
  ASSERT_NO_FATAL_FAILURE(input.select(0, false));
  ObISliceWriter *second = nullptr;
  ASSERT_EQ(OB_SUCCESS, ObIVFInputWriter::get_writer(
      input.dag, input.contexts.at(1)->tablet_id_, 0, false, second));
  auto destroy = [](ObISliceWriter *writer) { writer->~ObISliceWriter(); ob_free(writer); };
  std::unique_ptr<ObISliceWriter, decltype(destroy)> second_owner(second, destroy);
  float first_payload[DIM] = {21.0F};
  float second_payload[DIM] = {22.0F};
  ASSERT_EQ(OB_SUCCESS, append_row(*input.writer, 21, first_payload));
  ASSERT_EQ(OB_SUCCESS, append_row(*second, 22, second_payload));
  ASSERT_EQ(OB_SUCCESS, input.writer->close());
  ASSERT_NO_FATAL_FAILURE(check_replay(*input.contexts.at(0), {21}));
  auto &second_replay = input.contexts.at(1)->ivf_ctx_->slice_inputs_.at(0)->replay_;
  EXPECT_FALSE(second_replay.is_sealed());
  EXPECT_EQ(0, second_replay.get_row_count());
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, second->get_row_count(count));
  EXPECT_EQ(1, count);
  ASSERT_EQ(OB_SUCCESS, second->close());
  ASSERT_NO_FATAL_FAILURE(check_replay(*input.contexts.at(1), {22}));
}

TEST(IVFInputWriter, flush_failure_does_not_route_pending_rows_to_next_slice)
{
  Inputs input;
  ASSERT_NO_FATAL_FAILURE(input.init(2));
  ASSERT_NO_FATAL_FAILURE(input.select(0, false));
  float short_payload = 1.0F;
  ASSERT_EQ(OB_SUCCESS, append_row(*input.writer, 1, &short_payload, sizeof(short_payload)));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObIVFInputWriter::get_writer(
      input.dag, input.contexts.at(1)->tablet_id_, 0, false, input.writer));
  EXPECT_TRUE(input.contexts.at(1)->ivf_ctx_->slice_inputs_.empty());
  EXPECT_EQ(OB_ERR_UNEXPECTED, input.writer->close());
  EXPECT_EQ(OB_ERR_UNEXPECTED, input.writer->close());
  auto *writer = static_cast<ObIVFInputWriter *>(input.writer);
  EXPECT_EQ(nullptr, writer->input_buffer_);
  EXPECT_EQ(nullptr, writer->active_writer_);
  EXPECT_FALSE(writer->writers_.created());
  // The writer records the DDL error; the PX/DAG scheduler propagates it
  // into the final DAG status outside this focused fixture.
  EXPECT_EQ(OB_ERR_UNEXPECTED, input.dag.ret_code_);
}

TEST(IVFInputWriter, full_buffer_reuse_across_ranges_in_one_tablet)
{
  Inputs input;
  ASSERT_NO_FATAL_FAILURE(input.init(1));
  auto &context = *input.contexts.at(0);
  context.slice_count_ = 2;
  context.ivf_ctx_->capture_slice_count_ = 2;
  ASSERT_NO_FATAL_FAILURE(input.select(0, false));
  std::vector<uint64_t> first_vids;
  for (uint64_t vid = 1; vid <= 257; ++vid) {
    float payload[DIM] = {};
    payload[0] = static_cast<float>(vid);
    ASSERT_EQ(OB_SUCCESS, append_row(*input.writer, vid, payload));
    first_vids.push_back(vid);
  }
  ASSERT_EQ(OB_SUCCESS, ObIVFInputWriter::get_writer(
      input.dag, context.tablet_id_, 1, false, input.writer));
  float second[DIM] = {258.0F};
  ASSERT_EQ(OB_SUCCESS, append_row(*input.writer, 258, second));
  ASSERT_NO_FATAL_FAILURE(input.select(0, false));
  float last[DIM] = {259.0F};
  ASSERT_EQ(OB_SUCCESS, append_row(*input.writer, 259, last));
  first_vids.push_back(259);
  int64_t count = 0;
  ASSERT_EQ(OB_SUCCESS, input.writer->get_row_count(count));
  EXPECT_EQ(258, count);
  ASSERT_EQ(OB_SUCCESS, input.writer->close());
  ASSERT_NO_FATAL_FAILURE(check_replay(context, first_vids, 0));
  ASSERT_NO_FATAL_FAILURE(check_replay(context, {258}, 1));
}

TEST(IVFInputWriter, batch_subranges_survive_source_reuse_and_slice_switch)
{
  // Both ordinary borrowed batches and the conversion fallback must keep the
  // selector boundaries while using a single workspace per receiving worker.
  for (bool reshape : {false, true}) {
    Inputs input;
    ASSERT_NO_FATAL_FAILURE(input.init(2, reshape));
    input.sql_rows.reuse();
    for (uint64_t vid = 30; vid < 36; ++vid) {
      ObStorageDatum cells[4];
      ObSEArray<ObDatum *, 4> datums;
      float payload[DIM] = {};
      payload[0] = static_cast<float>(vid);
      for (int64_t c = 0; c < 4; ++c) {
        cells[c].set_int(0);
        ASSERT_EQ(OB_SUCCESS, datums.push_back(&cells[c]));
      }
      cells[2].set_uint(vid);
      cells[3].set_string(reinterpret_cast<const char *>(payload), sizeof(payload));
      ASSERT_EQ(OB_SUCCESS, input.sql_rows.append_row(datums));
    }
    ObArray<ObIVector *> vectors;
    for (int64_t i = 0; i < input.sql_rows.get_vectors().count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, vectors.push_back(input.sql_rows.get_vectors().at(i)->get_vector()));
    }
    ASSERT_NO_FATAL_FAILURE(input.select(0, true));
    ObBatchSelector first(int64_t(1), int64_t(2));
    ASSERT_EQ(OB_SUCCESS, input.writer->append_current_batch(vectors, first));
    ASSERT_NO_FATAL_FAILURE(input.select(1, true));
    ObBatchSelector second(int64_t(3), int64_t(1));
    ASSERT_EQ(OB_SUCCESS, input.writer->append_current_batch(vectors, second));
    ASSERT_NO_FATAL_FAILURE(input.select(0, true));
    ObBatchSelector third(int64_t(4), int64_t(1));
    ASSERT_EQ(OB_SUCCESS, input.writer->append_current_batch(vectors, third));
    input.sql_rows.reuse(); // writer must no longer depend on this payload
    ASSERT_EQ(OB_SUCCESS, input.writer->close());
    ASSERT_NO_FATAL_FAILURE(check_replay(*input.contexts.at(0), {31, 32, 34}));
    ASSERT_NO_FATAL_FAILURE(check_replay(*input.contexts.at(1), {33}));
  }
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase;
  OB_LOGGER.set_file_name("test_ivf_input_writer.log", true);
  testing::InitGoogleTest(&argc, argv);
  MTL_BIND2(mtl_new_default, share::ObIVFReplayMemoryMgr::mtl_init,
      nullptr, nullptr, nullptr, mtl_destroy_default);
  if (OB_SUCCESS != MockTenantModuleEnv::get_instance().init()) { return 1; }
  OB_LOGGER.set_log_level("INFO");
  THIS_WORKER.set_timeout_ts(INT64_MAX);
  ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  const int ret = RUN_ALL_TESTS();
  MockTenantModuleEnv::get_instance().destroy();
  std::_Exit(ret);
}
