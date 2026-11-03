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
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/mds_table/ob_mds_trans_node_value.h"
#include "storage/mds_table/ob_mds_single_row_mvcc_ctx.h"
#include "storage/mds_table/ob_tablet_mds_reloc_batch_arg.h"
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
static constexpr uint16_t RELOC_MDS_TYPE = static_cast<uint16_t>(
    mds_type_id_detail::TypeIdOf<mds::DummyKey, ObTabletMdsRelocStatus>::value);

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
    ASSERT_EQ(OB_SUCCESS,
              (ObMdsTypeRegistry::get_instance().register_type<DummyKey, ObTabletMdsRelocStatus>(
                  RELOC_MDS_TYPE)));
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
  ObTabletMdsRelocSkipBitmap bitmap;
  ASSERT_EQ(OB_SUCCESS, bitmap.init(2, 65));
  bitmap.set_skip(1, 64);
  EXPECT_TRUE(bitmap.is_skipped(1, 64));

  char buf[128] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, bitmap.serialize(buf, sizeof(buf), pos));

  ObTabletMdsRelocSkipBitmap decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos, 2, 65));
  EXPECT_TRUE(decoded.is_skipped(1, 64));

  ObTabletMdsRelocSkipBitmap small_bitmap;
  ASSERT_EQ(OB_SUCCESS, small_bitmap.init(1, 1));
  char small_buf[32] = {};
  int64_t small_pos = 0;
  ASSERT_EQ(OB_SUCCESS, small_bitmap.serialize(small_buf, sizeof(small_buf), small_pos));

  ObTabletMdsRelocSkipBitmap malformed;
  int64_t malformed_pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, malformed.deserialize(small_buf, small_pos, malformed_pos, 2, 65));
  EXPECT_FALSE(malformed.is_inited());
}

TEST_F(TestMdsTableUnit, reloc_batch_empty_final_roundtrip)
{
  ObTabletMdsRelocBatchArg batch;
  batch.ls_id_ = share::ObLSID(1);
  ObTabletMdsRelocTarget target;
  target.tablet_id_ = ObTabletID(1001);
  target.transfer_scn_.convert_for_tx(100);
  ASSERT_EQ(OB_SUCCESS, batch.targets_.push_back(target));

  EXPECT_FALSE(batch.is_valid());
  batch.is_final_ = true;
  EXPECT_FALSE(batch.is_valid());
  batch.reloc_type_ = ObTabletMdsRelocType::SPLIT;
  EXPECT_TRUE(batch.is_valid());
  batch.reloc_type_ = ObTabletMdsRelocType::FORK;
  EXPECT_TRUE(batch.is_valid());

  char buf[1024] = {};
  int64_t pos = 0;
  ASSERT_LE(batch.get_serialize_size(), static_cast<int64_t>(sizeof(buf)));
  ASSERT_EQ(OB_SUCCESS, batch.serialize(buf, sizeof(buf), pos));
  EXPECT_EQ(batch.get_serialize_size(), pos);

  ObTabletMdsRelocBatchArg decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_TRUE(decoded.is_valid());
  EXPECT_TRUE(decoded.is_final_);
  EXPECT_EQ(ObTabletMdsRelocType::FORK, decoded.reloc_type_);
  EXPECT_EQ(0, decoded.rows_.count());
  EXPECT_FALSE(decoded.skip_bitmap_.has_any_skip());

  decoded.reset_rows();
  EXPECT_FALSE(decoded.is_final_);
  EXPECT_EQ(ObTabletMdsRelocType::FORK, decoded.reloc_type_);
  decoded.reset();
  EXPECT_EQ(ObTabletMdsRelocType::INVALID, decoded.reloc_type_);
}

TEST_F(TestMdsTableUnit, reloc_batch_rejects_unknown_version)
{
  for (const int64_t version : {int64_t(0), int64_t(3), INT64_MAX}) {
    ObTabletMdsRelocBatchArg batch;
    batch.version_ = version;
    EXPECT_FALSE(batch.is_v2());
    EXPECT_FALSE(batch.is_valid());
    char buf[1024] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, batch.serialize(buf, sizeof(buf), pos));
    ObTabletMdsRelocBatchArg decoded;
    int64_t decode_pos = 0;
    EXPECT_EQ(OB_NOT_SUPPORTED, decoded.deserialize(buf, pos, decode_pos));
  }
}

TEST_F(TestMdsTableUnit, reloc_marker_rejects_invalid_redo_fence)
{
  ObTabletMdsRelocStatus marker;
  marker.set_reloc_done();
  EXPECT_TRUE(marker.is_v1_marker());
  EXPECT_FALSE(marker.is_v2_marker_pending_fence());
  share::SCN invalid;
  share::SCN redo;
  ASSERT_EQ(OB_SUCCESS, redo.convert_for_tx(123));
  marker.on_redo(redo);
  EXPECT_FALSE(marker.get_redo_scn().is_valid());
  marker.enable_redo_fence();
  EXPECT_TRUE(marker.is_v2_marker_pending_fence());
  EXPECT_FALSE(marker.is_v1_marker());
  marker.on_redo(invalid);
  EXPECT_TRUE(marker.get_redo_scn().is_min());
  EXPECT_FALSE(marker.has_captured_redo_fence());
  marker.on_redo(redo);
  EXPECT_TRUE(marker.has_captured_redo_fence());
  EXPECT_FALSE(marker.is_v1_marker());
  EXPECT_FALSE(marker.is_v2_marker_pending_fence());
  for (const share::SCN &bad : {invalid, share::SCN::min_scn(), share::SCN::max_scn()}) {
    marker.on_redo(bad);
    EXPECT_EQ(redo, marker.get_redo_scn());
    EXPECT_TRUE(marker.has_captured_redo_fence());
  }
}

TEST_F(TestMdsTableUnit, reloc_batch_final_coexists_with_skip_bitmap)
{
  ObTabletMdsRelocBatchArg batch;
  batch.ls_id_ = share::ObLSID(1);
  batch.reloc_type_ = ObTabletMdsRelocType::TRANSFER;
  ObTabletMdsRelocTarget target;
  target.tablet_id_ = ObTabletID(1001);
  target.transfer_scn_.convert_for_tx(100);
  ASSERT_EQ(OB_SUCCESS, batch.targets_.push_back(target));

  char payload = 'x';
  ObTabletMdsRelocRowStorage row;
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

  ObTabletMdsRelocBatchArg decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_TRUE(decoded.is_final_);
  EXPECT_EQ(ObTabletMdsRelocType::TRANSFER, decoded.reloc_type_);
  EXPECT_FALSE(decoded.skip_bitmap_.is_skipped(0, 0));

  decoded.skip_bitmap_.set_skip(0, 0);
  ASSERT_EQ(OB_SUCCESS, decoded.skip_bitmap_.rewrite_inplace());
  ObTabletMdsRelocBatchArg decoded_after_rewrite;
  int64_t rewrite_decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_after_rewrite.deserialize(buf, pos, rewrite_decode_pos));
  EXPECT_EQ(pos, rewrite_decode_pos);
  EXPECT_TRUE(decoded_after_rewrite.is_final_);
  EXPECT_EQ(ObTabletMdsRelocType::TRANSFER, decoded_after_rewrite.reloc_type_);
  EXPECT_TRUE(decoded_after_rewrite.skip_bitmap_.is_skipped(0, 0));
}

// Legacy V1 wire shapes (before the tail fields were appended) reproduced
// with the same UNIS macro machinery an old binary used.  These let the tests
// below exercise both upgrade directions of the same-version tail-append
// compatibility relied on by ObTabletMdsRelocStatus / ObTabletMdsRelocTarget.
namespace legacy_v1
{
struct LegacyRelocStatusV1
{
  OB_UNIS_VERSION(1);
  LegacyRelocStatusV1() : reloc_done_(false) {}
  TO_STRING_KV(K_(reloc_done));
  bool reloc_done_;
};
OB_SERIALIZE_MEMBER(LegacyRelocStatusV1, reloc_done_);

struct LegacyRelocTargetV1
{
  OB_UNIS_VERSION(1);
  LegacyRelocTargetV1() : tablet_id_(), transfer_scn_() {}
  TO_STRING_KV(K_(tablet_id), K_(transfer_scn));
  common::ObTabletID tablet_id_;
  share::SCN transfer_scn_;
};
OB_SERIALIZE_MEMBER(LegacyRelocTargetV1, tablet_id_, transfer_scn_);
} // namespace legacy_v1

// Legacy V1 wire compatibility for the reloc marker: a V1-encoded payload
// (reloc_done_ only, no appended redo_scn_) must deserialize successfully
// under the new binary with redo_scn_ left at its default (invalid) value,
// and a legacy reader must still decode the new-format payload.  OB_UNIS_DECODE
// stops at the object length header; this test pins that behavior for the
// marker type whose layout gained a tail field without bumping UNIS_VERSION.
TEST_F(TestMdsTableUnit, mds_reloc_status_legacy_v1_payload_compat)
{
  // Old-binary bytes for {reloc_done_ = true}.
  legacy_v1::LegacyRelocStatusV1 legacy;
  legacy.reloc_done_ = true;
  char legacy_buf[64] = {};
  int64_t legacy_pos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.serialize(legacy_buf, sizeof(legacy_buf), legacy_pos));
  const int64_t legacy_len = legacy_pos;

  // New binary reads the legacy payload: deserialization must succeed and
  // leave redo_scn_ invalid (the recognized "legacy marker" shape).
  ObTabletMdsRelocStatus decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(legacy_buf, legacy_len, decode_pos));
  EXPECT_EQ(legacy_len, decode_pos);
  EXPECT_TRUE(decoded.is_reloc_done());
  EXPECT_FALSE(decoded.get_redo_scn().is_valid());
  EXPECT_TRUE(decoded.is_valid());

  // New-format bytes for the same marker state plus a redo SCN.
  share::SCN redo_scn;
  ASSERT_EQ(OB_SUCCESS, redo_scn.convert_for_tx(456));
  ObTabletMdsRelocStatus current;
  current.set_reloc_done();
  current.enable_redo_fence();
  current.on_redo(redo_scn);
  char new_buf[64] = {};
  int64_t new_pos = 0;
  ASSERT_EQ(OB_SUCCESS, current.serialize(new_buf, sizeof(new_buf), new_pos));
  // Same UNIS version, strictly longer payload (the appended SCN field).
  EXPECT_GT(new_pos, legacy_len);
  EXPECT_EQ(0, MEMCMP(legacy_buf, new_buf, 1));

  // A legacy reader of the new payload decodes the old fields and skips the
  // trailing SCN via the object length header.
  legacy_v1::LegacyRelocStatusV1 as_old_reader;
  int64_t old_reader_pos = 0;
  ASSERT_EQ(OB_SUCCESS, as_old_reader.deserialize(new_buf, new_pos, old_reader_pos));
  EXPECT_EQ(new_pos, old_reader_pos);
  EXPECT_TRUE(as_old_reader.reloc_done_);
}

// Legacy V1 wire compatibility for the reloc target: a V1-encoded target
// (tablet_id_ + transfer_scn_ only) must deserialize under the new binary with
// src_tablet_id_ / src_reorg_scn_ left invalid, and a legacy reader must still
// decode the new-format payload (V2 fields are skipped via the length header).
TEST_F(TestMdsTableUnit, reloc_target_legacy_v1_payload_compat)
{
  ObTabletID tablet_id(200001);
  share::SCN transfer_scn;
  ASSERT_EQ(OB_SUCCESS, transfer_scn.convert_for_tx(1100));

  // Old-binary bytes for {tablet_id_, transfer_scn_}.
  legacy_v1::LegacyRelocTargetV1 legacy;
  legacy.tablet_id_ = tablet_id;
  legacy.transfer_scn_ = transfer_scn;
  char legacy_buf[128] = {};
  int64_t legacy_pos = 0;
  ASSERT_EQ(OB_SUCCESS, legacy.serialize(legacy_buf, sizeof(legacy_buf), legacy_pos));
  const int64_t legacy_len = legacy_pos;

  // New-format bytes: the same two fields plus the appended V2-only fields.
  ObTabletMdsRelocTarget v2_target;
  v2_target.tablet_id_ = tablet_id;
  v2_target.transfer_scn_ = transfer_scn;
  v2_target.src_tablet_id_ = ObTabletID(200002);
  ASSERT_EQ(OB_SUCCESS, v2_target.src_reorg_scn_.convert_for_tx(900));
  char v2_buf[128] = {};
  int64_t v2_pos = 0;
  ASSERT_EQ(OB_SUCCESS, v2_target.serialize(v2_buf, sizeof(v2_buf), v2_pos));
  // Same UNIS version, strictly longer payload (the appended fields).
  EXPECT_GT(v2_pos, legacy_len);
  EXPECT_EQ(0, MEMCMP(legacy_buf, v2_buf, 1));

  // New reader on old data: succeeds, V2-only fields stay invalid.
  ObTabletMdsRelocTarget decoded_v1;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_v1.deserialize(legacy_buf, legacy_len, decode_pos));
  EXPECT_EQ(legacy_len, decode_pos);
  EXPECT_EQ(tablet_id, decoded_v1.tablet_id_);
  EXPECT_EQ(transfer_scn, decoded_v1.transfer_scn_);
  EXPECT_FALSE(decoded_v1.src_tablet_id_.is_valid());
  EXPECT_FALSE(decoded_v1.src_reorg_scn_.is_valid());

  // Old reader on new data: decodes the V1 fields, skips the appended fields.
  legacy_v1::LegacyRelocTargetV1 as_old_reader;
  int64_t old_reader_pos = 0;
  ASSERT_EQ(OB_SUCCESS, as_old_reader.deserialize(v2_buf, v2_pos, old_reader_pos));
  EXPECT_EQ(v2_pos, old_reader_pos);
  EXPECT_EQ(tablet_id, as_old_reader.tablet_id_);
  EXPECT_EQ(transfer_scn, as_old_reader.transfer_scn_);
}

TEST_F(TestMdsTableUnit, mds_reloc_status_lifecycle)
{
  ObTabletMdsRelocStatus data;
  EXPECT_FALSE(data.is_reloc_done());
  EXPECT_FALSE(data.get_redo_scn().is_valid());
  EXPECT_FALSE(data.is_valid());
  data.set_reloc_done();
  share::SCN redo_scn;
  ASSERT_EQ(OB_SUCCESS, redo_scn.convert_for_tx(123));
  ObMdsTypeRegistry &registry = ObMdsTypeRegistry::get_instance();
  EXPECT_TRUE(registry.has_on_redo(RELOC_MDS_TYPE));
  // A V1 marker written by the new binary still receives the generic MDS redo
  // callback, but must not acquire the V2 persistence fence.
  ASSERT_EQ(OB_SUCCESS, registry.invoke_on_redo(RELOC_MDS_TYPE, &data, redo_scn));
  EXPECT_FALSE(data.get_redo_scn().is_valid());
  data.enable_redo_fence();
  EXPECT_TRUE(data.get_redo_scn().is_min());
  // Illegal redo SCNs must leave the parked min fence unchanged. on_redo
  // is an in-place seqlock patch and does not log or rewrite the field,
  // so the persistence gate can still observe min SCN.
  share::SCN invalid_scn;
  ASSERT_EQ(OB_SUCCESS, registry.invoke_on_redo(RELOC_MDS_TYPE, &data, invalid_scn));
  EXPECT_TRUE(data.get_redo_scn().is_min());
  ASSERT_EQ(OB_SUCCESS, registry.invoke_on_redo(RELOC_MDS_TYPE, &data, share::SCN::min_scn()));
  EXPECT_TRUE(data.get_redo_scn().is_min());
  ASSERT_EQ(OB_SUCCESS, registry.invoke_on_redo(RELOC_MDS_TYPE, &data, share::SCN::max_scn()));
  EXPECT_TRUE(data.get_redo_scn().is_min());
  ASSERT_EQ(OB_SUCCESS, registry.invoke_on_redo(RELOC_MDS_TYPE, &data, redo_scn));
  EXPECT_TRUE(data.is_valid());
  EXPECT_TRUE(data.has_captured_redo_fence());
  EXPECT_EQ(redo_scn, data.get_redo_scn());

  char buf[1024] = {};
  int64_t pos = 0;
  ASSERT_LE(data.get_serialize_size(), static_cast<int64_t>(sizeof(buf)));
  ASSERT_EQ(OB_SUCCESS, data.serialize(buf, sizeof(buf), pos));

  ObTabletMdsRelocStatus decoded;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_TRUE(decoded.is_reloc_done());
  EXPECT_EQ(redo_scn, decoded.get_redo_scn());

  ObTabletMdsRelocStatus assigned;
  ASSERT_EQ(OB_SUCCESS, assigned.assign(decoded));
  EXPECT_TRUE(assigned.is_reloc_done());
  EXPECT_EQ(redo_scn, assigned.get_redo_scn());
  char print_buf[1024] = {};
  EXPECT_GT(assigned.to_string(print_buf, sizeof(print_buf)), 0);
  EXPECT_NE(nullptr, std::strstr(print_buf, "reloc_done"));
  assigned.reset();
  EXPECT_FALSE(assigned.is_reloc_done());
  EXPECT_FALSE(assigned.get_redo_scn().is_valid());
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
