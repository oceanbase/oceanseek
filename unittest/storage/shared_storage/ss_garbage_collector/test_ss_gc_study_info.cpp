/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#define private public
#include "storage/ss_garbage_collector/ob_ss_mark_sweep_gc_rpc.h"
#include "storage/ss_garbage_collector/task/ob_ss_mark_sweep_gc_gen_task.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/tablet/ob_tablet.h"
#undef private
#include "storage/ob_storage_schema.h"

namespace oceanbase
{
namespace storage
{

struct ObSSGCTabletStudyResultV1 final
{
  OB_UNIS_VERSION(1);

public:
  ObSSGCTabletStudyResultV1()
    : gc_tablet_(), min_minor_meta_scn_(), min_major_snapshot_(0)
  {}

  ObSSGCTablet gc_tablet_;
  share::SCN min_minor_meta_scn_;
  int64_t min_major_snapshot_;
};

OB_SERIALIZE_MEMBER(ObSSGCTabletStudyResultV1,
    gc_tablet_, min_minor_meta_scn_, min_major_snapshot_);

// Previous LOB checkpoint payload without the original checkpoint; wire version remains 1.
struct ObSSGCTabletStudyResultBeforeOrig final
{
  OB_UNIS_VERSION(1);
public:
  ObSSGCTablet gc_tablet_;
  share::SCN min_dep_meta_scn_;
  int64_t min_major_snapshot_ = 0;
  share::SCN clog_checkpoint_scn_;
  int64_t multi_version_start_ = 0;
};

OB_SERIALIZE_MEMBER(ObSSGCTabletStudyResultBeforeOrig,
    gc_tablet_, min_dep_meta_scn_, min_major_snapshot_, clog_checkpoint_scn_, multi_version_start_);

// Previous scan-input payload; checkpoint fields were appended without changing the wire version.
struct ObSSGCTabletWithMetaScnBeforeCheckpoint final
{
  OB_UNIS_VERSION(1);
public:
  ObSSGCTablet gc_tablet_;
  share::SCN meta_scn_;
};

OB_SERIALIZE_MEMBER(ObSSGCTabletWithMetaScnBeforeCheckpoint, gc_tablet_, meta_scn_);

} // namespace storage

namespace unittest
{

using namespace common;
using namespace share;
using namespace storage;

class TestSSGCStudyInfo : public ::testing::Test
{
public:
  TestSSGCStudyInfo() {}
  virtual ~TestSSGCStudyInfo() {}
};

TEST_F(TestSSGCStudyInfo, set_boundary)
{
  ObSSGCTabletStudyInfo info;
  ASSERT_TRUE(info.clog_checkpoint_scn_.is_max());
  ASSERT_EQ(INT64_MAX, info.multi_version_start_);
  ASSERT_TRUE(info.orig_clog_checkpoint_scn_.is_max());
  ASSERT_FALSE(info.is_cold());

  info.set_to_max();
  ASSERT_TRUE(info.min_dep_meta_scn_.is_max());
  ASSERT_EQ(INT64_MAX, info.min_major_snapshot_);
  ASSERT_TRUE(info.clog_checkpoint_scn_.is_max());
  ASSERT_EQ(INT64_MAX, info.multi_version_start_);
  ASSERT_TRUE(info.orig_clog_checkpoint_scn_.is_max());
  ASSERT_FALSE(info.is_cold());

  info.set_to_min();
  ASSERT_TRUE(info.min_dep_meta_scn_.is_min());
  ASSERT_EQ(0, info.min_major_snapshot_);
  ASSERT_TRUE(info.clog_checkpoint_scn_.is_min());
  ASSERT_EQ(0, info.multi_version_start_);
  ASSERT_TRUE(info.orig_clog_checkpoint_scn_.is_min());
  ASSERT_FALSE(info.is_cold());
}

TEST_F(TestSSGCStudyInfo, coldness_requires_checkpoint_advancement)
{
  SCN original;
  SCN effective;
  ASSERT_EQ(OB_SUCCESS, original.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, effective.convert_for_tx(200));
  ObSSGCTabletStudyInfo info(SCN::base_scn(), 10, original, 20);
  ASSERT_FALSE(info.is_cold());
  info.clog_checkpoint_scn_ = effective;
  ASSERT_TRUE(info.is_cold());
  info.clog_checkpoint_scn_.set_min();
  ASSERT_FALSE(info.is_cold());
  info.clog_checkpoint_scn_.set_max();
  ASSERT_FALSE(info.is_cold());
  info.clog_checkpoint_scn_ = effective;
  info.orig_clog_checkpoint_scn_.set_max();
  ASSERT_FALSE(info.is_cold());
  info.orig_clog_checkpoint_scn_.set_min();
  ASSERT_FALSE(info.is_cold());
  info.orig_clog_checkpoint_scn_ = effective;
  info.clog_checkpoint_scn_ = original;
  ASSERT_FALSE(info.is_cold());

  obrpc::ObSSMarkSweepGCStudyP processor;
  info.clog_checkpoint_scn_ = original;
  info.orig_clog_checkpoint_scn_ = original;
  ASSERT_FALSE(processor.update_cold_lob_rely_info_(true, 1, false, original, info));
  ASSERT_EQ(original, info.orig_clog_checkpoint_scn_);
  ASSERT_EQ(original, info.clog_checkpoint_scn_);
}

TEST_F(TestSSGCStudyInfo, serialize_round_trip)
{
  SCN checkpoint_scn;
  ASSERT_EQ(OB_SUCCESS, checkpoint_scn.convert_for_tx(100));
  ObSSGCTabletStudyInfo info(SCN::base_scn(), 10, checkpoint_scn, 20);
  info.orig_clog_checkpoint_scn_ = SCN::base_scn();
  ObSSGCTabletStudyResult src(ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()),
      info, ObSSGCTabletType::LOB_META);
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, sizeof(buf), pos));

  const int64_t data_len = pos;
  ObSSGCTabletStudyResult dst;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(src.gc_tablet_, dst.gc_tablet_);
  ASSERT_EQ(src.study_info_.min_dep_meta_scn_, dst.study_info_.min_dep_meta_scn_);
  ASSERT_EQ(src.study_info_.min_major_snapshot_, dst.study_info_.min_major_snapshot_);
  ASSERT_EQ(checkpoint_scn, dst.study_info_.clog_checkpoint_scn_);
  ASSERT_EQ(20, dst.study_info_.multi_version_start_);
  ASSERT_EQ(SCN::base_scn(), dst.study_info_.orig_clog_checkpoint_scn_);
  ASSERT_TRUE(dst.study_info_.is_cold());
  ASSERT_EQ(ObSSGCTabletType::LOB_META, dst.table_type_.type_);
}

TEST_F(TestSSGCStudyInfo, deserialize_v1_payload)
{
  ObSSGCTabletStudyResultV1 old_result;
  old_result.gc_tablet_ = ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  old_result.min_minor_meta_scn_ = SCN::base_scn();
  old_result.min_major_snapshot_ = 10;
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_result.serialize(buf, sizeof(buf), pos));

  const int64_t data_len = pos;
  ObSSGCTabletStudyResult new_result;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_result.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(old_result.gc_tablet_, new_result.gc_tablet_);
  ASSERT_EQ(old_result.min_minor_meta_scn_, new_result.study_info_.min_dep_meta_scn_);
  ASSERT_EQ(old_result.min_major_snapshot_, new_result.study_info_.min_major_snapshot_);
  ASSERT_TRUE(new_result.study_info_.clog_checkpoint_scn_.is_max());
  ASSERT_EQ(INT64_MAX, new_result.study_info_.multi_version_start_);
  ASSERT_TRUE(new_result.study_info_.orig_clog_checkpoint_scn_.is_max());
  ASSERT_FALSE(new_result.study_info_.is_cold());
  ASSERT_EQ(ObSSGCTabletType::UNKNOWN, new_result.table_type_.type_);
}

TEST_F(TestSSGCStudyInfo, v1_deserialize_new_payload)
{
  SCN checkpoint_scn;
  ASSERT_EQ(OB_SUCCESS, checkpoint_scn.convert_for_tx(100));
  ObSSGCTabletStudyInfo info(SCN::base_scn(), 10, checkpoint_scn, 20);
  ObSSGCTabletStudyResult new_result(ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn()),
      info, ObSSGCTabletType::UNKNOWN);
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_result.serialize(buf, sizeof(buf), pos));

  const int64_t data_len = pos;
  ObSSGCTabletStudyResultV1 old_result;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_result.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(new_result.gc_tablet_, old_result.gc_tablet_);
  ASSERT_EQ(new_result.study_info_.min_dep_meta_scn_, old_result.min_minor_meta_scn_);
  ASSERT_EQ(new_result.study_info_.min_major_snapshot_, old_result.min_major_snapshot_);
}

TEST_F(TestSSGCStudyInfo, merge_study_info)
{
  SCN checkpoint_scn_80;
  SCN checkpoint_scn_100;
  ASSERT_EQ(OB_SUCCESS, checkpoint_scn_80.convert_for_tx(80));
  ASSERT_EQ(OB_SUCCESS, checkpoint_scn_100.convert_for_tx(100));
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  ObSSGCTabletStudyResultArray server_result;
  ObSSGCTabletStudyResultArray final_result;
  hash::ObHashMap<ObSSGCTablet, int64_t> tablet_index_map;
  ASSERT_EQ(OB_SUCCESS, tablet_index_map.create(8, ObMemAttr(OB_SYS_TENANT_ID, "SSGCStudyUT")));

  ASSERT_EQ(OB_SUCCESS, server_result.push_back(
      ObSSGCTabletStudyResult(gc_tablet, SCN::base_scn(), 10, checkpoint_scn_100, 100)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_EQ(1, final_result.count());
  ASSERT_EQ(checkpoint_scn_100, final_result.at(0).study_info_.clog_checkpoint_scn_);
  ASSERT_EQ(100, final_result.at(0).study_info_.multi_version_start_);

  server_result.reuse();
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(ObSSGCTabletStudyResult(
          gc_tablet, SCN::base_scn(), 10, SCN::max_scn(), INT64_MAX)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_EQ(checkpoint_scn_100, final_result.at(0).study_info_.clog_checkpoint_scn_);
  ASSERT_EQ(100, final_result.at(0).study_info_.multi_version_start_);

  server_result.reuse();
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(
      ObSSGCTabletStudyResult(gc_tablet, SCN::base_scn(), 10, checkpoint_scn_80, 80)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_EQ(checkpoint_scn_80, final_result.at(0).study_info_.clog_checkpoint_scn_);
  ASSERT_EQ(80, final_result.at(0).study_info_.multi_version_start_);

  server_result.reuse();
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(
      ObSSGCTabletStudyResult(gc_tablet, SCN::base_scn(), 10, SCN::min_scn(), 0)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_TRUE(final_result.at(0).study_info_.clog_checkpoint_scn_.is_min());
  ASSERT_EQ(0, final_result.at(0).study_info_.multi_version_start_);

  server_result.reuse();
  const ObSSGCTablet another_generation(ObLSID(1), ObTabletID(200001), checkpoint_scn_80);
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(ObSSGCTabletStudyResult(
          another_generation, SCN::base_scn(), 10, checkpoint_scn_100, 100)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_EQ(2, final_result.count());
}

TEST_F(TestSSGCStudyInfo, classify_lob_rely_table_type)
{
  obrpc::ObSSMarkSweepGCStudyP processor;
  ObTablet tablet;
  ObSSGCTabletType table_type;
  bool need_collect = false;
  // No schema is attached: known cached types must not trigger a schema load.
  tablet.table_store_cache_.table_type_ = schema::AUX_LOB_META;
  ASSERT_EQ(OB_SUCCESS, processor.need_collect_lob_rely_info_(tablet, need_collect, table_type));
  ASSERT_TRUE(need_collect);
  ASSERT_EQ(ObSSGCTabletType::LOB_META, table_type.type_);

  tablet.table_store_cache_.table_type_ = schema::AUX_OUTTABLE_LOB_CONTROL;
  ASSERT_EQ(OB_SUCCESS, processor.need_collect_lob_rely_info_(tablet, need_collect, table_type));
  ASSERT_TRUE(need_collect);
  ASSERT_EQ(ObSSGCTabletType::LOB_CTRL, table_type.type_);

  tablet.table_store_cache_.table_type_ = schema::USER_TABLE;
  ASSERT_EQ(OB_SUCCESS, processor.need_collect_lob_rely_info_(tablet, need_collect, table_type));
  ASSERT_FALSE(need_collect);
  ASSERT_EQ(ObSSGCTabletType::NORMAL, table_type.type_);

  tablet.table_store_cache_.table_type_ = schema::AUX_LOB_PIECE;
  ASSERT_EQ(OB_SUCCESS, processor.need_collect_lob_rely_info_(tablet, need_collect, table_type));
  ASSERT_FALSE(need_collect);
  ASSERT_EQ(ObSSGCTabletType::NORMAL, table_type.type_);

  tablet.table_store_cache_.table_type_ = schema::USER_INDEX;
  ASSERT_EQ(OB_SUCCESS, processor.need_collect_lob_rely_info_(tablet, need_collect, table_type));
  ASSERT_FALSE(need_collect);
  ASSERT_EQ(ObSSGCTabletType::NORMAL, table_type.type_);

  // An unknown type without a readable schema must fail instead of skipping LOB dependencies.
  table_type = ObSSGCTabletType::LOB_META;
  need_collect = true;
  tablet.table_store_cache_.reset();
  ASSERT_NE(OB_SUCCESS, processor.need_collect_lob_rely_info_(tablet, need_collect, table_type));
  ASSERT_FALSE(need_collect);
  ASSERT_EQ(ObSSGCTabletType::UNKNOWN, table_type.type_);
}

TEST_F(TestSSGCStudyInfo, classify_gc_table_type_from_storage_schema)
{
  ObStorageSchema storage_schema;
  ObSSGCTabletType table_type;
  storage_schema.table_type_ = share::schema::ObTableType::AUX_LOB_META;
  ASSERT_EQ(OB_SUCCESS, ObSSGCTabletType::get_ss_gc_tablet_type(storage_schema, table_type));
  ASSERT_EQ(ObSSGCTabletType::LOB_META, table_type.type_);

  storage_schema.table_type_ = share::schema::ObTableType::AUX_OUTTABLE_LOB_CONTROL;
  ASSERT_EQ(OB_SUCCESS, ObSSGCTabletType::get_ss_gc_tablet_type(storage_schema, table_type));
  ASSERT_EQ(ObSSGCTabletType::LOB_CTRL, table_type.type_);

  storage_schema.table_type_ = share::schema::ObTableType::USER_TABLE;
  ASSERT_EQ(OB_SUCCESS, ObSSGCTabletType::get_ss_gc_tablet_type(storage_schema, table_type));
  ASSERT_EQ(ObSSGCTabletType::NORMAL, table_type.type_);

  storage_schema.table_type_ = share::schema::ObTableType::AUX_LOB_PIECE;
  ASSERT_EQ(OB_SUCCESS, ObSSGCTabletType::get_ss_gc_tablet_type(storage_schema, table_type));
  ASSERT_EQ(ObSSGCTabletType::NORMAL, table_type.type_);
}

TEST_F(TestSSGCStudyInfo, merge_known_and_unknown_table_types)
{
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  const ObSSGCTabletType types[] = {
      ObSSGCTabletType::NORMAL, ObSSGCTabletType::LOB_META, ObSSGCTabletType::LOB_CTRL};
  for (const ObSSGCTabletType type : types) {
    ObSSGCTabletStudyInfo info;
    info.set_to_max();
    ObSSGCTabletStudyResultArray server_result;
    ObSSGCTabletStudyResultArray final_result;
    hash::ObHashMap<ObSSGCTablet, int64_t> index_map;
    ASSERT_EQ(OB_SUCCESS, index_map.create(8, ObMemAttr(OB_SYS_TENANT_ID, "SSGCStudyUT")));
    // An absent tablet or old server must not hide a type reported by another server.
    ASSERT_EQ(OB_SUCCESS, server_result.push_back(
        ObSSGCTabletStudyResult(gc_tablet, info, ObSSGCTabletType::UNKNOWN)));
    ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
        server_result, index_map, final_result));
    ASSERT_EQ(ObSSGCTabletType::UNKNOWN, final_result.at(0).table_type_.type_);
    server_result.at(0).table_type_ = type;
    ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
        server_result, index_map, final_result));
    ASSERT_EQ(type, final_result.at(0).table_type_);
    server_result.at(0).table_type_ = ObSSGCTabletType::UNKNOWN;
    ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
        server_result, index_map, final_result));
    ASSERT_EQ(type, final_result.at(0).table_type_);
    server_result.at(0).table_type_ = type;
    ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
        server_result, index_map, final_result));
    server_result.at(0).table_type_ = type.is_lob_meta()
        ? ObSSGCTabletType::NORMAL : ObSSGCTabletType::LOB_META;
    ASSERT_EQ(OB_ERR_UNEXPECTED, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
        server_result, index_map, final_result));
  }
}

TEST_F(TestSSGCStudyInfo, generated_gc_inputs_preserve_table_type)
{
  volatile bool stopped = false;
  ObSSMarkSweepGCGenTask task(stopped);
  ObSSGCTabletStudyResultArray results;
  ObSSGCTabletWithMetaScnArray inputs;
  ObSSGCTabletStudyInfo info;
  info.set_to_max();
  const ObSSGCTabletType types[] = {ObSSGCTabletType::UNKNOWN, ObSSGCTabletType::NORMAL,
      ObSSGCTabletType::LOB_META, ObSSGCTabletType::LOB_CTRL};
  for (int64_t i = 0; i < 4; ++i) {
    ASSERT_EQ(OB_SUCCESS, results.push_back(ObSSGCTabletStudyResult(
        ObSSGCTablet(ObLSID(1), ObTabletID(200001 + i), SCN::base_scn()), info, types[i])));
  }
  ASSERT_EQ(OB_SUCCESS, task.calc_min_meta_scn_(SCN::base_scn(), results, inputs));
  ASSERT_EQ(4, inputs.count());
  for (int64_t i = 0; i < inputs.count(); ++i) {
    EXPECT_EQ(types[i], inputs.at(i).table_type_);
    EXPECT_EQ(results.at(i).gc_tablet_, inputs.at(i).gc_tablet_);
  }
}

TEST_F(TestSSGCStudyInfo, update_cold_lob_rely_info)
{
  obrpc::ObSSMarkSweepGCStudyP processor;
  SCN tablet_checkpoint_scn;
  SCN ls_checkpoint_scn;
  ASSERT_EQ(OB_SUCCESS, tablet_checkpoint_scn.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, ls_checkpoint_scn.convert_for_tx(200));
  ObSSGCTabletStudyInfo study_info(SCN::base_scn(), 10, tablet_checkpoint_scn, 20);

  ASSERT_TRUE(processor.update_cold_lob_rely_info_(
      true, 1, false, ls_checkpoint_scn, study_info));
  ASSERT_EQ(ls_checkpoint_scn, study_info.clog_checkpoint_scn_);
  ASSERT_EQ(tablet_checkpoint_scn, study_info.orig_clog_checkpoint_scn_);
  ASSERT_TRUE(study_info.is_cold());
  ASSERT_EQ(20, study_info.multi_version_start_);

  study_info.clog_checkpoint_scn_ = tablet_checkpoint_scn;
  ASSERT_FALSE(processor.update_cold_lob_rely_info_(
      false, 1, false, ls_checkpoint_scn, study_info));
  ASSERT_EQ(tablet_checkpoint_scn, study_info.clog_checkpoint_scn_);
  ASSERT_FALSE(study_info.is_cold());

  ASSERT_FALSE(processor.update_cold_lob_rely_info_(
      true, 2, false, ls_checkpoint_scn, study_info));
  ASSERT_EQ(tablet_checkpoint_scn, study_info.clog_checkpoint_scn_);
  ASSERT_FALSE(study_info.is_cold());

  ASSERT_FALSE(processor.update_cold_lob_rely_info_(
      true, 1, true, ls_checkpoint_scn, study_info));
  ASSERT_EQ(tablet_checkpoint_scn, study_info.clog_checkpoint_scn_);
  ASSERT_FALSE(study_info.is_cold());
  ASSERT_EQ(20, study_info.multi_version_start_);
}

TEST_F(TestSSGCStudyInfo, original_checkpoint_compatible_with_previous_payload)
{
  ObSSGCTabletStudyResultBeforeOrig old_result;
  old_result.gc_tablet_ = ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  old_result.min_dep_meta_scn_ = SCN::base_scn();
  old_result.min_major_snapshot_ = 10;
  ASSERT_EQ(OB_SUCCESS, old_result.clog_checkpoint_scn_.convert_for_tx(100));
  old_result.multi_version_start_ = 20;
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_result.serialize(buf, sizeof(buf), pos));
  int64_t data_len = pos;
  ObSSGCTabletStudyResult new_result;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_result.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(old_result.clog_checkpoint_scn_, new_result.study_info_.clog_checkpoint_scn_);
  ASSERT_TRUE(new_result.study_info_.orig_clog_checkpoint_scn_.is_max());
  ASSERT_FALSE(new_result.study_info_.is_cold());

  new_result.study_info_.orig_clog_checkpoint_scn_ = SCN::base_scn();
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_result.serialize(buf, sizeof(buf), pos));
  data_len = pos;
  ObSSGCTabletStudyResultBeforeOrig decoded_old;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_old.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(old_result.gc_tablet_, decoded_old.gc_tablet_);
  ASSERT_EQ(old_result.clog_checkpoint_scn_, decoded_old.clog_checkpoint_scn_);
  ASSERT_EQ(old_result.multi_version_start_, decoded_old.multi_version_start_);
}

TEST_F(TestSSGCStudyInfo, merge_cold_and_incomplete_results)
{
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  ObSSGCTabletStudyResultArray server_result;
  ObSSGCTabletStudyResultArray final_result;
  hash::ObHashMap<ObSSGCTablet, int64_t> tablet_index_map;
  ASSERT_EQ(OB_SUCCESS, tablet_index_map.create(8, ObMemAttr(OB_SYS_TENANT_ID, "SSGCStudyUT")));
  SCN first_raw;
  SCN second_raw;
  SCN first_effective;
  SCN second_effective;
  ASSERT_EQ(OB_SUCCESS, first_raw.convert_for_tx(20));
  ASSERT_EQ(OB_SUCCESS, second_raw.convert_for_tx(10));
  ASSERT_EQ(OB_SUCCESS, first_effective.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, second_effective.convert_for_tx(200));
  ObSSGCTabletStudyInfo info(SCN::base_scn(), 10, first_effective, 20);
  info.orig_clog_checkpoint_scn_ = first_raw;
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(
      ObSSGCTabletStudyResult(gc_tablet, info, ObSSGCTabletType::UNKNOWN)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_TRUE(final_result.at(0).study_info_.is_cold());
  ASSERT_EQ(first_raw, final_result.at(0).study_info_.orig_clog_checkpoint_scn_);

  server_result.reuse();
  info.orig_clog_checkpoint_scn_ = second_raw;
  info.clog_checkpoint_scn_ = second_effective;
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(
      ObSSGCTabletStudyResult(gc_tablet, info, ObSSGCTabletType::UNKNOWN)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_EQ(second_raw, final_result.at(0).study_info_.orig_clog_checkpoint_scn_);
  ASSERT_EQ(first_effective, final_result.at(0).study_info_.clog_checkpoint_scn_);
  ASSERT_TRUE(final_result.at(0).study_info_.is_cold());

  server_result.reuse();
  info.orig_clog_checkpoint_scn_ = second_effective;
  ASSERT_FALSE(info.is_cold());
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(
      ObSSGCTabletStudyResult(gc_tablet, info, ObSSGCTabletType::UNKNOWN)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_TRUE(final_result.at(0).study_info_.is_cold());
  ASSERT_EQ(second_raw, final_result.at(0).study_info_.orig_clog_checkpoint_scn_);
  ASSERT_EQ(first_effective, final_result.at(0).study_info_.clog_checkpoint_scn_);

  server_result.reuse();
  info.clog_checkpoint_scn_.set_min();
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(
      ObSSGCTabletStudyResult(gc_tablet, info, ObSSGCTabletType::UNKNOWN)));
  info.clog_checkpoint_scn_ = second_effective;
  ASSERT_EQ(OB_SUCCESS, server_result.push_back(
      ObSSGCTabletStudyResult(gc_tablet, info, ObSSGCTabletType::UNKNOWN)));
  ASSERT_EQ(OB_SUCCESS, ObSSGCStudyUtil::merge_server_meta_scn_infos_(
      server_result, tablet_index_map, final_result));
  ASSERT_TRUE(final_result.at(0).study_info_.clog_checkpoint_scn_.is_min());
  ASSERT_EQ(second_raw, final_result.at(0).study_info_.orig_clog_checkpoint_scn_);
  ASSERT_FALSE(final_result.at(0).study_info_.is_cold());
}

TEST_F(TestSSGCStudyInfo, incomplete_tablet_cannot_use_ls_checkpoint)
{
  obrpc::ObSSMarkSweepGCStudyP processor;
  SCN raw_checkpoint;
  SCN ls_checkpoint;
  ASSERT_EQ(OB_SUCCESS, raw_checkpoint.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, ls_checkpoint.convert_for_tx(200));
  ObSSGCTabletStudyInfo info(SCN::base_scn(), 10, raw_checkpoint, 20);
  ObTablet tablet;
  tablet.tablet_meta_.clog_checkpoint_scn_ = raw_checkpoint;
  tablet.tablet_meta_.multi_version_start_ = 20;
  ASSERT_EQ(OB_SUCCESS, tablet.tablet_meta_.ha_status_.init_status());
  tablet.tablet_meta_.split_info_.set_data_incomplete(true);
  bool has_memtable = false;
  ScanAllVersionTabletsOp::GetMinTabletRelyInfo op(
      info.min_dep_meta_scn_, info.min_major_snapshot_, info.clog_checkpoint_scn_,
      info.orig_clog_checkpoint_scn_, info.multi_version_start_, has_memtable, true);
  ASSERT_EQ(OB_SUCCESS, op(tablet));
  ASSERT_FALSE(processor.update_cold_lob_rely_info_(true, 1, false, ls_checkpoint, info));
  ASSERT_EQ(raw_checkpoint, info.orig_clog_checkpoint_scn_);
  ASSERT_TRUE(info.clog_checkpoint_scn_.is_min());
  ASSERT_FALSE(info.is_cold());
  ASSERT_EQ(20, info.multi_version_start_);

  // A MIN boundary must not be raised by the cold-tablet helper.
  ASSERT_FALSE(processor.update_cold_lob_rely_info_(true, 1, false, ls_checkpoint, info));
  ASSERT_EQ(raw_checkpoint, info.orig_clog_checkpoint_scn_);
  ASSERT_TRUE(info.clog_checkpoint_scn_.is_min());
  ASSERT_FALSE(info.is_cold());
}

TEST_F(TestSSGCStudyInfo, collect_completeness_across_tablet_versions)
{
  ObTablet complete_tablet;
  ObTablet incomplete_tablet;
  SCN raw_checkpoint;
  SCN first_checkpoint;
  SCN last_checkpoint;
  ASSERT_EQ(OB_SUCCESS, raw_checkpoint.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, first_checkpoint.convert_for_tx(200));
  ASSERT_EQ(OB_SUCCESS, last_checkpoint.convert_for_tx(50));
  incomplete_tablet.tablet_meta_.clog_checkpoint_scn_ = raw_checkpoint;
  ASSERT_EQ(OB_SUCCESS, complete_tablet.tablet_meta_.ha_status_.init_status());
  ASSERT_EQ(OB_SUCCESS, incomplete_tablet.tablet_meta_.ha_status_.init_status());
  ASSERT_TRUE(complete_tablet.is_data_complete());

  for (int mode = 0; mode < 2; ++mode) {
    incomplete_tablet.tablet_meta_.split_info_.set_data_incomplete(0 == mode);
    incomplete_tablet.tablet_meta_.fork_info_.reset();
    if (1 == mode) {
      incomplete_tablet.tablet_meta_.fork_info_.set_fork_snapshot_version(100);
      incomplete_tablet.tablet_meta_.fork_info_.set_src_tablet_id(ObTabletID(200001));
      incomplete_tablet.tablet_meta_.fork_info_.set_data_complete(false);
    }
    SCN min_dep;
    int64_t major_snapshot = 0;
    SCN checkpoint;
    SCN orig_checkpoint;
    int64_t multi_version_start = 0;
    bool has_memtable = false;
    ScanAllVersionTabletsOp::GetMinTabletRelyInfo op(
        min_dep, major_snapshot, checkpoint, orig_checkpoint,
        multi_version_start, has_memtable, true);
    ASSERT_TRUE(checkpoint.is_max());
    ASSERT_TRUE(orig_checkpoint.is_max());
    complete_tablet.tablet_meta_.clog_checkpoint_scn_ = first_checkpoint;
    ASSERT_EQ(OB_SUCCESS, op(complete_tablet));
    ASSERT_EQ(first_checkpoint, checkpoint);
    ASSERT_EQ(first_checkpoint, orig_checkpoint);
    ASSERT_EQ(OB_SUCCESS, op(incomplete_tablet));
    ASSERT_TRUE(checkpoint.is_min());
    ASSERT_EQ(raw_checkpoint, orig_checkpoint);
    complete_tablet.tablet_meta_.clog_checkpoint_scn_ = last_checkpoint;
    ASSERT_EQ(OB_SUCCESS, op(complete_tablet));
    ASSERT_TRUE(checkpoint.is_min());
    ASSERT_EQ(last_checkpoint, orig_checkpoint);

    ScanAllVersionTabletsOp::GetMinTabletRelyInfo reverse_op(
        min_dep, major_snapshot, checkpoint, orig_checkpoint,
        multi_version_start, has_memtable, true);
    ASSERT_EQ(OB_SUCCESS, reverse_op(complete_tablet));
    ASSERT_EQ(OB_SUCCESS, reverse_op(incomplete_tablet));
    complete_tablet.tablet_meta_.clog_checkpoint_scn_ = first_checkpoint;
    ASSERT_EQ(OB_SUCCESS, reverse_op(complete_tablet));
    ASSERT_TRUE(checkpoint.is_min());
    ASSERT_EQ(last_checkpoint, orig_checkpoint);

    ScanAllVersionTabletsOp::GetMinTabletRelyInfo non_lob_op(
        min_dep, major_snapshot, checkpoint, orig_checkpoint,
        multi_version_start, has_memtable, false);
    ASSERT_EQ(OB_SUCCESS, non_lob_op(incomplete_tablet));
    ASSERT_TRUE(checkpoint.is_max());
    ASSERT_TRUE(orig_checkpoint.is_max());
  }
}

TEST_F(TestSSGCStudyInfo, scan_input_checkpoint_defaults)
{
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  ObSSGCTabletWithMetaScn input(gc_tablet, SCN::base_scn(), ObSSGCTabletType::UNKNOWN);
  ASSERT_TRUE(input.clog_checkpoint_scn_.is_max());
  ASSERT_TRUE(input.orig_clog_checkpoint_scn_.is_max());
  ASSERT_FALSE(input.is_cold());
  ASSERT_EQ(ObSSGCTabletType::UNKNOWN, input.table_type_.type_);
}

TEST_F(TestSSGCStudyInfo, scan_input_checkpoint_round_trip)
{
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  SCN original;
  SCN effective;
  ASSERT_EQ(OB_SUCCESS, original.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, effective.convert_for_tx(200));
  ObSSGCTabletWithMetaScn src(gc_tablet, SCN::base_scn(), ObSSGCTabletType::LOB_CTRL, effective,
      original);
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(buf, sizeof(buf), pos));
  const int64_t data_len = pos;
  ObSSGCTabletWithMetaScn dst;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(src.gc_tablet_, dst.gc_tablet_);
  ASSERT_EQ(src.meta_scn_, dst.meta_scn_);
  ASSERT_EQ(effective, dst.clog_checkpoint_scn_);
  ASSERT_EQ(original, dst.orig_clog_checkpoint_scn_);
  ASSERT_TRUE(dst.is_cold());
  ASSERT_EQ(ObSSGCTabletType::LOB_CTRL, dst.table_type_.type_);
}

TEST_F(TestSSGCStudyInfo, scan_input_deserialize_previous_payload)
{
  ObSSGCTabletWithMetaScnBeforeCheckpoint old_input;
  old_input.gc_tablet_ = ObSSGCTablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  old_input.meta_scn_ = SCN::base_scn();
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_input.serialize(buf, sizeof(buf), pos));
  const int64_t data_len = pos;
  ObSSGCTabletWithMetaScn new_input;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_input.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(old_input.gc_tablet_, new_input.gc_tablet_);
  ASSERT_EQ(old_input.meta_scn_, new_input.meta_scn_);
  ASSERT_TRUE(new_input.clog_checkpoint_scn_.is_max());
  ASSERT_TRUE(new_input.orig_clog_checkpoint_scn_.is_max());
  ASSERT_FALSE(new_input.is_cold());
  ASSERT_EQ(ObSSGCTabletType::UNKNOWN, new_input.table_type_.type_);
}

TEST_F(TestSSGCStudyInfo, previous_scan_input_reader_skips_checkpoint_fields)
{
  const ObSSGCTablet gc_tablet(ObLSID(1), ObTabletID(200001), SCN::base_scn());
  SCN original;
  SCN effective;
  ASSERT_EQ(OB_SUCCESS, original.convert_for_tx(100));
  ASSERT_EQ(OB_SUCCESS, effective.convert_for_tx(200));
  ObSSGCTabletWithMetaScn new_input(gc_tablet, SCN::base_scn(), ObSSGCTabletType::UNKNOWN,
      effective, original);
  char buf[1024];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, new_input.serialize(buf, sizeof(buf), pos));
  const int64_t data_len = pos;
  ObSSGCTabletWithMetaScnBeforeCheckpoint old_input;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, old_input.deserialize(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(new_input.gc_tablet_, old_input.gc_tablet_);
  ASSERT_EQ(new_input.meta_scn_, old_input.meta_scn_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
