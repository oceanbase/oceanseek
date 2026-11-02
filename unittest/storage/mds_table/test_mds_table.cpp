/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#define UNITTEST_DEBUG
#include <cstring>
#include <gtest/gtest.h>
#define private public
#define protected public

#include "storage/mds_table/ob_mds_type_registry.h"
#include "storage/mds_table/ob_mds_table.h"
#include "storage/mds_table/ob_mds_trans_node_value.h"
#include "storage/mds_table/ob_mds_single_row_mvcc_ctx.h"
#include "storage/mds_table/ob_ls_mds_tablet_reloc_batch_arg.h"
#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/mvcc/ob_mvcc_engine.h"
#include "storage/memtable/mvcc/ob_mvcc_define.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"
#include "storage/tablet/ob_tablet_mds_reloc_status.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase {
namespace unittest {

using namespace common;
using namespace storage;
using namespace memtable;
using namespace transaction;
using namespace mds;

// Minimal user-data type used only in these tests.
struct TestMdsVal
{
  int32_t x_;
  TestMdsVal() : x_(0) {}
  explicit TestMdsVal(int32_t x) : x_(x) {}
  void on_commit(const share::SCN &, const share::SCN &) {}
  int serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    if (pos + 4 > buf_len) return OB_BUF_NOT_ENOUGH;
    memcpy(buf + pos, &x_, 4);
    pos += 4;
    return OB_SUCCESS;
  }
  int deserialize(const char *buf, const int64_t data_len, int64_t &pos)
  {
    if (pos + 4 > data_len) return OB_DESERIALIZE_ERROR;
    memcpy(&x_, buf + pos, 4);
    pos += 4;
    return OB_SUCCESS;
  }
  int64_t get_serialize_size() const { return 4; }
  TO_STRING_KV(K_(x));
};

static constexpr uint16_t TEST_MDS_TYPE = 200; // outside real ObTxDataSourceType range
static constexpr uint16_t TEST_FAILING_MDS_TYPE = 201;

struct FailingAssignMdsVal : public TestMdsVal
{
  FailingAssignMdsVal() : TestMdsVal() {}
  explicit FailingAssignMdsVal(int32_t x) : TestMdsVal(x) {}
  int assign(const FailingAssignMdsVal &) { return OB_ERR_UNEXPECTED; }
};

class UtMdsStubCallback : public ObITransCallback
{
public:
  UtMdsStubCallback() : ObITransCallback(false /*need_submit_log*/) {}
};

class TestMdsTableUnit : public ::testing::Test
{
public:
  TestMdsTableUnit() {}
  virtual ~TestMdsTableUnit() {}
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS,
              (ObMdsTypeRegistry::get_instance().register_type<DummyKey, TestMdsVal>(TEST_MDS_TYPE)));
    ASSERT_EQ(OB_SUCCESS,
              (ObMdsTypeRegistry::get_instance().register_type<DummyKey, FailingAssignMdsVal>(
                  TEST_FAILING_MDS_TYPE)));
  }
  virtual void SetUp() {}
  virtual void TearDown() {}
private:
  DISALLOW_COPY_AND_ASSIGN(TestMdsTableUnit);
};

TEST_F(TestMdsTableUnit, mds_memtable_overrides_data_policies)
{
  ObMemtable data_memtable;
  ObMdsMemtable mds_memtable;
  ObMemtable *memtable = &mds_memtable;

  EXPECT_EQ(share::ObMemstoreResourceKind::DATA, data_memtable.get_memstore_resource_kind());
  EXPECT_TRUE(data_memtable.need_add_to_data_checkpoint());
  EXPECT_FALSE(data_memtable.is_mds_memtable());
  EXPECT_EQ(share::ObMemstoreResourceKind::MDS, memtable->get_memstore_resource_kind());
  EXPECT_FALSE(memtable->need_add_to_data_checkpoint());
  EXPECT_TRUE(memtable->is_mds_memtable());

  EXPECT_FALSE(memtable->is_frozen_memtable());
  mds_memtable.set_is_tablet_freeze();
  EXPECT_TRUE(memtable->is_frozen_memtable());

  data_memtable.set_freeze_state(TabletMemtableFreezeState::READY_FOR_FLUSH);
  mds_memtable.set_freeze_state(TabletMemtableFreezeState::READY_FOR_FLUSH);
  ASSERT_EQ(OB_SUCCESS, data_memtable.key_.scn_range_.end_scn_.convert_for_tx(1));
  ASSERT_EQ(OB_SUCCESS, mds_memtable.key_.scn_range_.end_scn_.convert_for_tx(1));
  EXPECT_FALSE(data_memtable.can_be_minor_merged());
  EXPECT_TRUE(memtable->can_be_minor_merged());
  EXPECT_EQ(checkpoint::ObFreezeCheckpointLocation::OUT, mds_memtable.location_);
  EXPECT_EQ(OB_SUCCESS, memtable->finish_freeze());
  EXPECT_EQ(checkpoint::ObFreezeCheckpointLocation::OUT, mds_memtable.location_);
}

TEST_F(TestMdsTableUnit, inactive_checkpoint_does_not_retain_logs)
{
  ObMdsTable mds_table;
  EXPECT_TRUE(mds_table.get_rec_scn().is_max());

  // Model the interval after init and before online without constructing a
  // complete LS. Inactive checkpoint methods must not dereference the LS.
  mds_table.is_inited_ = true;
  EXPECT_TRUE(mds_table.get_rec_scn().is_max());
  EXPECT_EQ(OB_SUCCESS, mds_table.flush());
}

TEST_F(TestMdsTableUnit, online_error_fail_closes_rec_scn)
{
  // ONLINE with a broken LS pointer must not report SCN::min_scn():
  // ObCheckpointExecutor::get_min_rec_scn ignores min_scn, so an error
  // would silently let the clog checkpoint advance (fail open). The error
  // path must return a sentinel that keeps the checkpoint from advancing.
  ObMdsTable mds_table;
  mds_table.is_inited_ = true;
  mds_table.state_ = static_cast<uint8_t>(ObMdsTable::MdsTableState::ONLINE);
  // ls_ is null, so the error path cannot read the clog checkpoint and
  // falls back to SCN::base_scn().
  const share::SCN rec_scn = mds_table.get_rec_scn();
  EXPECT_FALSE(rec_scn.is_max());
  EXPECT_TRUE(rec_scn.is_valid_and_not_min());
  EXPECT_TRUE(rec_scn <= share::SCN::base_scn());
}

TEST_F(TestMdsTableUnit, register_and_get_ops)
{
  const ObMdsTypeRegistry::TypeOps *ops = ObMdsTypeRegistry::get_instance().get_ops(TEST_MDS_TYPE);
  ASSERT_NE(nullptr, ops);
  EXPECT_TRUE(ops->is_valid());
  EXPECT_EQ(static_cast<int64_t>(sizeof(TestMdsVal)), ops->value_size);
  EXPECT_NE(nullptr, ops->construct);
  EXPECT_NE(nullptr, ops->destruct);
  EXPECT_NE(nullptr, ops->copy_from);
  EXPECT_TRUE(ObMdsTypeRegistry::get_instance().has_on_commit(TEST_MDS_TYPE));
  EXPECT_FALSE(ObMdsTypeRegistry::get_instance().has_on_redo(TEST_MDS_TYPE));
}

TEST_F(TestMdsTableUnit, registry_unregistered_type)
{
  EXPECT_EQ(nullptr, ObMdsTypeRegistry::get_instance().get_ops(0));
  EXPECT_FALSE(ObMdsTypeRegistry::get_instance().has_on_commit(0));

  char raw[64] = {};
  EXPECT_EQ(OB_ERR_UNEXPECTED,
            ObMdsTypeRegistry::get_instance().invoke_construct(0, raw));
}

TEST_F(TestMdsTableUnit, copy_from_roundtrip)
{
  const ObMdsTypeRegistry::TypeOps *ops =
      ObMdsTypeRegistry::get_instance().get_ops(TEST_MDS_TYPE);
  ASSERT_NE(nullptr, ops);

  const int64_t buf_size = ops->value_size;
  char raw[256] = {};
  ASSERT_LE(buf_size, static_cast<int64_t>(sizeof(raw)));

  auto *user_data = reinterpret_cast<TestMdsVal *>(raw);
  ops->construct(user_data);
  EXPECT_EQ(0, user_data->x_);

  TestMdsVal src(42);
  EXPECT_EQ(OB_SUCCESS, ops->copy_from(user_data, &src, nullptr));
  EXPECT_EQ(42, user_data->x_);

  ops->destruct(user_data);
}

TEST_F(TestMdsTableUnit, copy_from_returns_assign_error)
{
  const ObMdsTypeRegistry::TypeOps *ops =
      ObMdsTypeRegistry::get_instance().get_ops(TEST_FAILING_MDS_TYPE);
  ASSERT_NE(nullptr, ops);

  char raw[256] = {};
  ASSERT_LE(ops->value_size, static_cast<int64_t>(sizeof(raw)));
  auto *user_data = reinterpret_cast<FailingAssignMdsVal *>(raw);
  ops->construct(user_data);

  FailingAssignMdsVal src(42);
  EXPECT_EQ(OB_ERR_UNEXPECTED, ops->copy_from(user_data, &src, nullptr));
  EXPECT_EQ(0, user_data->x_);

  ops->destruct(user_data);
}

TEST_F(TestMdsTableUnit, build_mds_tx_node_returns_copy_error)
{
  ObArenaAllocator allocator;
  ObMvccEngine engine;
  engine.engine_allocator_ = &allocator;

  ObTxNodeArg arg;
  FailingAssignMdsVal val(99);
  arg.set_mds(ObTransID(1),
              static_cast<uint8_t>(TEST_FAILING_MDS_TYPE),
              &val,
              nullptr /*mds_meta*/,
              blocksstable::ObDmlFlag::DF_INSERT,
              /*memstore_version*/ 0,
              ObTxSEQ(1, 0),
              /*write_epoch*/ 0,
              /*column_cnt*/ 1);

  ObMvccTransNode *node = nullptr;
  EXPECT_EQ(OB_ERR_UNEXPECTED, engine.build_mds_tx_node_(arg, node));
  EXPECT_EQ(nullptr, node);
}

TEST_F(TestMdsTableUnit, reloc_skip_bitmap_rejects_wrong_word_count)
{
  ObRelocSkipBitmap bitmap;
  ASSERT_EQ(OB_SUCCESS, bitmap.init(2, 65));
  bitmap.set_skip(1, 64);
  EXPECT_TRUE(bitmap.is_skipped(1, 64));

  char buf[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, bitmap.serialize(buf, sizeof(buf), pos));

  ObRelocSkipBitmap decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos, 2, 65));
  EXPECT_TRUE(decoded.is_skipped(1, 64));

  ObRelocSkipBitmap small_bitmap;
  ASSERT_EQ(OB_SUCCESS, small_bitmap.init(1, 1));
  char small_buf[32] = {};
  int64_t small_pos = 0;
  ASSERT_EQ(OB_SUCCESS, small_bitmap.serialize(small_buf, sizeof(small_buf), small_pos));

  ObRelocSkipBitmap malformed;
  int64_t malformed_pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, malformed.deserialize(small_buf, small_pos, malformed_pos, 2, 65));
  EXPECT_FALSE(malformed.is_inited());
}

TEST_F(TestMdsTableUnit, reloc_batch_empty_final_roundtrip)
{
  ObLsMdsTabletRelocBatchArg batch;
  batch.ls_id_ = share::ObLSID(1);
  ObLsMdsTabletRelocTarget target;
  target.tablet_id_ = ObTabletID(1001);
  target.transfer_scn_.convert_for_tx(100);
  ASSERT_EQ(OB_SUCCESS, batch.targets_.push_back(target));

  EXPECT_FALSE(batch.is_valid());
  batch.is_final_ = true;
  EXPECT_FALSE(batch.is_valid());
  batch.reloc_type_ = ObLsMdsTabletRelocType::SPLIT;
  EXPECT_TRUE(batch.is_valid());
  batch.reloc_type_ = ObLsMdsTabletRelocType::FORK;
  EXPECT_TRUE(batch.is_valid());

  char buf[1024] = {};
  int64_t pos = 0;
  ASSERT_LE(batch.get_serialize_size(), static_cast<int64_t>(sizeof(buf)));
  ASSERT_EQ(OB_SUCCESS, batch.serialize(buf, sizeof(buf), pos));
  EXPECT_EQ(batch.get_serialize_size(), pos);

  ObLsMdsTabletRelocBatchArg decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_TRUE(decoded.is_valid());
  EXPECT_TRUE(decoded.is_final_);
  EXPECT_EQ(ObLsMdsTabletRelocType::FORK, decoded.reloc_type_);
  EXPECT_EQ(0, decoded.rows_.count());
  EXPECT_FALSE(decoded.skip_bitmap_.has_any_skip());

  decoded.reset_rows();
  EXPECT_FALSE(decoded.is_final_);
  EXPECT_EQ(ObLsMdsTabletRelocType::FORK, decoded.reloc_type_);
  decoded.reset();
  EXPECT_EQ(ObLsMdsTabletRelocType::INVALID, decoded.reloc_type_);
}

TEST_F(TestMdsTableUnit, reloc_batch_final_coexists_with_skip_bitmap)
{
  ObLsMdsTabletRelocBatchArg batch;
  batch.ls_id_ = share::ObLSID(1);
  batch.reloc_type_ = ObLsMdsTabletRelocType::TRANSFER;
  ObLsMdsTabletRelocTarget target;
  target.tablet_id_ = ObTabletID(1001);
  target.transfer_scn_.convert_for_tx(100);
  ASSERT_EQ(OB_SUCCESS, batch.targets_.push_back(target));

  char payload = 'x';
  ObLsMdsTabletRelocRowStorage row;
  row.reset();
  row.mds_unit_id_ = 1;
  row.ser_payload_.assign_ptr(&payload, sizeof(payload));
  const int64_t estimated_size =
      batch.estimate_serialize_size_after_push_row(row.get_serialize_size());
  ASSERT_EQ(OB_SUCCESS, batch.push_row(row));
  ASSERT_EQ(OB_SUCCESS, batch.skip_bitmap_.init(batch.rows_.count(), batch.targets_.count()));
  batch.is_final_ = true;
  EXPECT_EQ(batch.get_serialize_size(), estimated_size);

  char buf[1024] = {};
  int64_t pos = 0;
  ASSERT_LE(batch.get_serialize_size(), static_cast<int64_t>(sizeof(buf)));
  ASSERT_EQ(OB_SUCCESS, batch.serialize(buf, sizeof(buf), pos));

  ObLsMdsTabletRelocBatchArg decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_TRUE(decoded.is_final_);
  EXPECT_EQ(ObLsMdsTabletRelocType::TRANSFER, decoded.reloc_type_);
  EXPECT_FALSE(decoded.skip_bitmap_.is_skipped(0, 0));

  decoded.skip_bitmap_.set_skip(0, 0);
  ASSERT_EQ(OB_SUCCESS, decoded.skip_bitmap_.rewrite_inplace());
  ObLsMdsTabletRelocBatchArg decoded_after_rewrite;
  int64_t rewrite_decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_after_rewrite.deserialize(buf, pos, rewrite_decode_pos));
  EXPECT_EQ(pos, rewrite_decode_pos);
  EXPECT_TRUE(decoded_after_rewrite.is_final_);
  EXPECT_EQ(ObLsMdsTabletRelocType::TRANSFER, decoded_after_rewrite.reloc_type_);
  EXPECT_TRUE(decoded_after_rewrite.skip_bitmap_.is_skipped(0, 0));
}

TEST_F(TestMdsTableUnit, mds_reloc_status_lifecycle)
{
  ObTabletMdsRelocStatus data;
  EXPECT_FALSE(data.reloc_done_);
  EXPECT_FALSE(data.is_valid());
  data.reloc_done_ = true;
  EXPECT_TRUE(data.is_valid());

  char buf[1024] = {};
  int64_t pos = 0;
  ASSERT_LE(data.get_serialize_size(), static_cast<int64_t>(sizeof(buf)));
  ASSERT_EQ(OB_SUCCESS, data.serialize(buf, sizeof(buf), pos));

  ObTabletMdsRelocStatus decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_TRUE(decoded.reloc_done_);

  ObTabletMdsRelocStatus assigned;
  ASSERT_EQ(OB_SUCCESS, assigned.assign(decoded));
  EXPECT_TRUE(assigned.reloc_done_);
  char print_buf[1024] = {};
  EXPECT_GT(assigned.to_string(print_buf, sizeof(print_buf)), 0);
  EXPECT_NE(nullptr, std::strstr(print_buf, "reloc_done"));
  assigned.reset();
  EXPECT_FALSE(assigned.reloc_done_);
}

TEST_F(TestMdsTableUnit, registry_serialize_roundtrip)
{
  ObMdsTypeRegistry &reg = ObMdsTypeRegistry::get_instance();
  char raw[256] = {};
  auto *user_data = reinterpret_cast<TestMdsVal *>(raw);
  ASSERT_EQ(OB_SUCCESS, reg.invoke_construct(TEST_MDS_TYPE, user_data));
  user_data->x_ = 7;

  char ser_buf[64] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, reg.invoke_serialize(TEST_MDS_TYPE, user_data, ser_buf, sizeof(ser_buf), pos));
  EXPECT_GT(pos, 0);

  char raw2[256] = {};
  auto *user_data2 = reinterpret_cast<TestMdsVal *>(raw2);
  ASSERT_EQ(OB_SUCCESS, reg.invoke_construct(TEST_MDS_TYPE, user_data2));
  int64_t dpos = 0;
  ASSERT_EQ(OB_SUCCESS, reg.invoke_deserialize(TEST_MDS_TYPE, user_data2, ser_buf, pos, dpos, nullptr));
  EXPECT_EQ(7, user_data2->x_);

  share::SCN commit_version;
  share::SCN commit_scn;
  commit_version.convert_for_tx(100);
  commit_scn.convert_for_tx(200);
  ASSERT_EQ(OB_SUCCESS, reg.invoke_on_commit(TEST_MDS_TYPE, user_data, commit_version, commit_scn));

  reg.invoke_destruct(TEST_MDS_TYPE, user_data);
  reg.invoke_destruct(TEST_MDS_TYPE, user_data2);
}

TEST_F(TestMdsTableUnit, registry_value_to_string)
{
  char raw[256] = {};
  auto *user_data = reinterpret_cast<TestMdsVal *>(raw);
  ASSERT_EQ(OB_SUCCESS, ObMdsTypeRegistry::get_instance().invoke_construct(TEST_MDS_TYPE, user_data));
  user_data->x_ = 99;

  char str_buf[128] = {};
  const ObMdsTypeRegistry::TypeOps *ops =
      ObMdsTypeRegistry::get_instance().get_ops(TEST_MDS_TYPE);
  ASSERT_NE(nullptr, ops);
  EXPECT_GT(ops->value_to_string(user_data, str_buf, sizeof(str_buf)), 0);

  ObMdsTypeRegistry::get_instance().invoke_destruct(TEST_MDS_TYPE, user_data);
}

TEST_F(TestMdsTableUnit, tx_node_arg_mds_value)
{
  ObTxNodeArg arg;
  TestMdsVal val(99);

  arg.set_mds(ObTransID(1),
              static_cast<uint8_t>(TEST_MDS_TYPE),
              &val,
              nullptr /*mds_meta*/,
              blocksstable::ObDmlFlag::DF_INSERT,
              /*memstore_version*/ 0,
              ObTxSEQ(1, 0),
              /*write_epoch*/ 0,
              /*column_cnt*/ 1);

  EXPECT_TRUE(arg.is_mds_write_);
  EXPECT_EQ(static_cast<uint8_t>(TEST_MDS_TYPE), arg.mds_type_);
  EXPECT_EQ(static_cast<const void *>(&val), arg.mds_value_);
  EXPECT_EQ(nullptr, arg.mds_meta_);
  EXPECT_EQ(nullptr, arg.data_);

  arg.reset();
  EXPECT_FALSE(arg.is_mds_write_);
  EXPECT_EQ(nullptr, arg.mds_value_);
}

TEST_F(TestMdsTableUnit, tx_node_arg_no_mds_value)
{
  ObTxNodeArg arg;
  arg.set_mds(ObTransID(1),
              1,
              nullptr,
              nullptr /*mds_meta*/,
              blocksstable::ObDmlFlag::DF_INSERT,
              0,
              ObTxSEQ(1, 0),
              0,
              1);
  EXPECT_TRUE(arg.is_mds_write_);
  EXPECT_EQ(nullptr, arg.mds_value_);
  EXPECT_EQ(nullptr, arg.mds_meta_);
}

TEST_F(TestMdsTableUnit, meta_mds_type_layout)
{
  ObMdsValueMeta meta;
  meta.mds_type_ = 123;
  EXPECT_EQ(123u, meta.mds_type_);
}

TEST_F(TestMdsTableUnit, mvcc_ctx_trans_end_null_row_cb)
{
  share::SCN scn;
  scn.set_min();

  {
    MdsSingleRowMvccCtx ctx;
    ctx.inited_ = true;
    EXPECT_EQ(OB_SUCCESS, ctx.trans_end(/*commit=*/true, scn, scn));
  }
  {
    MdsSingleRowMvccCtx ctx;
    ctx.inited_ = true;
    EXPECT_EQ(OB_SUCCESS, ctx.trans_end(/*commit=*/false, scn, scn));
  }
}

TEST_F(TestMdsTableUnit, mvcc_ctx_trans_end_after_trans_clear)
{
  MdsSingleRowMvccCtx ctx;
  ctx.inited_ = true;
  ASSERT_EQ(OB_SUCCESS, ctx.trans_clear());
  EXPECT_EQ(&ctx.head_, ctx.head_.get_next());

  share::SCN scn;
  scn.set_min();
  EXPECT_EQ(OB_SUCCESS, ctx.trans_end(/*commit=*/true, scn, scn));
}

TEST_F(TestMdsTableUnit, mvcc_ctx_append_callback_null_arg)
{
  MdsSingleRowMvccCtx ctx;
  ctx.inited_ = true;
  EXPECT_EQ(OB_INVALID_ARGUMENT, ctx.append_callback(nullptr));
  EXPECT_EQ(&ctx.head_, ctx.head_.get_next());
}

TEST_F(TestMdsTableUnit, mvcc_ctx_append_callback_rejects_second)
{
  MdsSingleRowMvccCtx ctx;
  ctx.inited_ = true;
  UtMdsStubCallback cb1;
  UtMdsStubCallback cb2;
  ASSERT_EQ(OB_SUCCESS, ctx.append_callback(&cb1));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ctx.append_callback(&cb2));
  EXPECT_EQ(&cb1, ctx.head_.get_next());
}

TEST_F(TestMdsTableUnit, mvcc_ctx_trans_end_commit_duplicate)
{
  MdsSingleRowMvccCtx ctx;
  ctx.inited_ = true;
  UtMdsStubCallback cb;
  share::SCN scn;
  scn.convert_for_tx(1);
  cb.set_scn(scn);

  ASSERT_EQ(OB_SUCCESS, ctx.append_callback(&cb));
  share::SCN commit_version;
  share::SCN final_scn;
  commit_version.convert_for_tx(10);
  final_scn.convert_for_tx(20);
  ASSERT_EQ(OB_SUCCESS, ctx.trans_end(/*commit=*/true, commit_version, final_scn));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ctx.trans_end(/*commit=*/true, commit_version, final_scn));
}

TEST_F(TestMdsTableUnit, mvcc_ctx_alloc_row_callback_slot)
{
  MdsSingleRowMvccCtx ctx;
  ctx.inited_ = true;
  void *slot = ctx.alloc_mvcc_row_callback();
  ASSERT_NE(nullptr, slot);
  EXPECT_EQ(static_cast<void *>(slot), static_cast<void *>(ctx.mvcc_row_cb_mem_));
  EXPECT_TRUE(ctx.row_cb_lent_);
}

TEST_F(TestMdsTableUnit, mvcc_ctx_batch_append_callback_unsupported)
{
  MdsSingleRowMvccCtx ctx;
  ctx.inited_ = true;
  EXPECT_EQ(OB_ERR_UNEXPECTED, ctx.append_callback(nullptr, nullptr, 0));
}

// End-to-end ObMdsTable::write/replay on a real ObLS, and replay_scn vs
// clog_checkpoint (OB_NO_NEED_UPDATE) are covered by mittest:
//   test_mds_table_scan, test_mds_compat, test_mds_table_checkpoint.

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
