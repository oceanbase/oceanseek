/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <type_traits>
#define private public
#include "sql/engine/expr/ob_expr_vec_ivf_center_id.h"
#include "sql/das/ob_ivf_cid_util.h"
#include "sql/das/ob_das_dml_ctx_define.h"
#undef private
#include "sql/engine/dml/ob_dml_service.h"

namespace oceanbase
{
namespace sql
{

static_assert(!std::is_pointer<decltype(ObIVFDMLCtDef::protocol_version_)>::value,
              "IVF CtDef protocol metadata must not contain an expression pointer");
static_assert(!std::is_pointer<decltype(ObIVFDMLCtDef::cid_store_idx_)>::value,
              "IVF CtDef CID slot must be a serialized ordinal");

static void mark_as_ivf_centroid_ctdef(ObDASInsCtDef &ctdef)
{
  // Routing requires both the table id and centroid role. Model the actual
  // codegen result instead of using index_tid_ alone.
  ctdef.table_param_.get_data_table_ref().index_type_ =
      share::schema::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL;
}

static void init_logical_insert_main(ObDASInsCtDef &ctdef,
                                     ObDASInsRtDef &rtdef)
{
  ctdef.logical_table_aux_type_ = share::LT_AUX_PRIMARY;
  ctdef.lt_namespace_id_ = 11;
  ctdef.lt_logical_table_id_ = 22;
  rtdef.lt_rt_.namespace_id_ = 11;
  rtdef.lt_rt_.logical_table_id_ = 22;
  rtdef.lt_rt_.enabled_ = true;
}

static void init_logical_ivf_aux(ObDASInsCtDef &ctdef,
                                 const int64_t cid_store_idx,
                                 const uint64_t centroid_table_id)
{
  ctdef.logical_table_aux_type_ = share::LT_AUX_IVF_CID_ROWID;
  ctdef.lt_namespace_id_ = 11;
  ctdef.lt_logical_table_id_ = 22;
  ASSERT_EQ(common::OB_SUCCESS,
            ctdef.ivf_dml_ctdef_.init_v2(
                cid_store_idx, 2, 9, centroid_table_id,
                share::VIDA_L2, 4));
  ctdef.has_ivf_dml_ctdef_ = 1;
}

TEST(TestExprVecIVFCenterIDDeferred, deferred_protocol_requires_dml_codegen_flag)
{
  ObExpr expr;
  expr.extra_ = ObExprVecIVFCenterID::pack_extra(
      ObIvfCentroidResolveMode::SPFRESH,
      false /*enable_deferred*/);
  EXPECT_EQ(ObIvfCentroidResolveMode::SPFRESH,
            ObExprVecIVFCenterID::get_resolve_mode(expr));
  EXPECT_FALSE(ObExprVecIVFCenterID::is_deferred_enabled(expr));

  expr.extra_ = ObExprVecIVFCenterID::pack_extra(
      ObIvfCentroidResolveMode::SPFRESH,
      true /*enable_deferred*/);
  EXPECT_EQ(ObIvfCentroidResolveMode::SPFRESH,
            ObExprVecIVFCenterID::get_resolve_mode(expr));
  EXPECT_TRUE(ObExprVecIVFCenterID::is_deferred_enabled(expr));
}

TEST(TestExprVecIVFCenterIDDeferred, placeholder_is_exactly_16_bytes)
{
  const char placeholder[] = "IVFCTDEFDEFERRED";
  static_assert(sizeof(placeholder) - 1
                    == ObExprVecIVFCenterID::CENTER_ID_BYTE_LENGTH,
                "placeholder must keep the persisted CID width");
  EXPECT_TRUE(ObExprVecIVFCenterID::is_deferred_placeholder(
      common::ObString(sizeof(placeholder) - 1, placeholder)));
  EXPECT_FALSE(ObExprVecIVFCenterID::is_deferred_placeholder(
      common::ObString(sizeof(placeholder) - 2, placeholder)));
  char changed[ObExprVecIVFCenterID::CENTER_ID_BYTE_LENGTH];
  MEMCPY(changed, placeholder, sizeof(changed));
  changed[0] = 'X';
  EXPECT_FALSE(ObExprVecIVFCenterID::is_deferred_placeholder(
      common::ObString(sizeof(changed), changed)));
}

TEST(TestExprVecIVFCenterIDDeferred, old_ctdef_and_unknown_protocol_are_disabled)
{
  ObIVFDMLCtDef meta;
  EXPECT_FALSE(meta.is_valid()); // old coordinator: appended field is absent/default

  meta.protocol_version_ = 1; // retired expression-pointer protocol
  meta.cid_store_idx_ = 3;
  meta.vector_store_idx_ = 4;
  meta.vector_subschema_id_ = 1;
  meta.centroid_table_id_ = 1001;
  meta.distance_algorithm_ = static_cast<uint8_t>(share::VIDA_L2);
  EXPECT_FALSE(meta.is_valid());

  meta.protocol_version_ = ObIVFDMLCtDef::IVF_CTDEF_VERSION;
  EXPECT_TRUE(meta.is_valid());

  meta.protocol_version_ = ObIVFDMLCtDef::IVF_CTDEF_VERSION + 1;
  EXPECT_FALSE(meta.is_valid());
  meta.reset();
  EXPECT_FALSE(meta.is_valid());
}

TEST(TestExprVecIVFCenterIDDeferred, ctdef_v2_roundtrip_has_no_expression_pointer)
{
  ObIVFDMLCtDef src;
  src.protocol_version_ = ObIVFDMLCtDef::IVF_CTDEF_VERSION;
  src.cid_store_idx_ = 7;
  src.vector_store_idx_ = 2;
  src.vector_subschema_id_ = 9;
  src.centroid_table_id_ = 50001;
  src.distance_algorithm_ = static_cast<uint8_t>(share::VIDA_COS);
  ASSERT_TRUE(src.is_valid());

  char buf[256];
  int64_t pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, src.serialize(buf, sizeof(buf), pos));
  const int64_t serialized_len = pos;

  ObIVFDMLCtDef dst;
  pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, dst.deserialize(buf, serialized_len, pos));
  EXPECT_TRUE(dst.is_valid());
  EXPECT_EQ(src.protocol_version_, dst.protocol_version_);
  EXPECT_EQ(src.cid_store_idx_, dst.cid_store_idx_);
  EXPECT_EQ(src.vector_store_idx_, dst.vector_store_idx_);
  EXPECT_EQ(src.vector_subschema_id_, dst.vector_subschema_id_);
  EXPECT_EQ(src.centroid_table_id_, dst.centroid_table_id_);
  EXPECT_EQ(src.distance_algorithm_, dst.distance_algorithm_);
}

TEST(TestExprVecIVFCenterIDDeferred, legacy_skipped_ivf_field_uses_v0_default)
{
  // The insert-specific rowkey fields follow the base CtDef in the same UNIS
  // buffer. A legacy base must not consume those bytes as IVF metadata.
  common::ObArenaAllocator allocator("IVFCtDefCompat");
  ObDASInsCtDef src(allocator);
  ASSERT_EQ(common::OB_SUCCESS, src.table_rowkey_cids_.init(1));
  ASSERT_EQ(common::OB_SUCCESS, src.table_rowkey_cids_.push_back(12345));
  ASSERT_FALSE(src.has_ivf_dml_ctdef_);
  char buf[64 * 1024];
  int64_t pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, src.serialize(buf, sizeof(buf), pos));

  ObDASInsCtDef dst(allocator);
  int64_t decode_pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, dst.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_FALSE(dst.has_ivf_dml_ctdef_);
  EXPECT_EQ(0, dst.ivf_dml_ctdef_.protocol_version_);
  EXPECT_FALSE(dst.ivf_dml_ctdef_.is_valid());
  ASSERT_EQ(1, dst.table_rowkey_cids_.count());
  EXPECT_EQ(12345, dst.table_rowkey_cids_.at(0));
}

TEST(TestExprVecIVFCenterIDDeferred, derived_ctdef_v2_roundtrip_uses_presence_flag)
{
  common::ObArenaAllocator allocator("IVFCtDefV2");
  ObDASInsCtDef src(allocator);
  ASSERT_EQ(common::OB_SUCCESS,
            src.ivf_dml_ctdef_.init_v2(1, 2, 9, 50001, share::VIDA_COS, 4));
  src.has_ivf_dml_ctdef_ = 1;
  ASSERT_EQ(common::OB_SUCCESS, src.table_rowkey_cids_.init(1));
  ASSERT_EQ(common::OB_SUCCESS, src.table_rowkey_cids_.push_back(67890));
  char buf[64 * 1024];
  int64_t pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, src.serialize(buf, sizeof(buf), pos));

  ObDASInsCtDef dst(allocator);
  int64_t decode_pos = 0;
  ASSERT_EQ(common::OB_SUCCESS, dst.deserialize(buf, pos, decode_pos));
  EXPECT_EQ(pos, decode_pos);
  EXPECT_TRUE(dst.has_ivf_dml_ctdef_);
  EXPECT_TRUE(dst.ivf_dml_ctdef_.is_valid());
  EXPECT_EQ(src.ivf_dml_ctdef_.centroid_table_id_,
            dst.ivf_dml_ctdef_.centroid_table_id_);
  ASSERT_EQ(1, dst.table_rowkey_cids_.count());
  EXPECT_EQ(67890, dst.table_rowkey_cids_.at(0));
}

TEST(TestExprVecIVFCenterIDDeferred, nonzero_unsupported_protocol_fails_atomically)
{
  for (const uint8_t version : {static_cast<uint8_t>(1),
                                static_cast<uint8_t>(ObIVFDMLCtDef::IVF_CTDEF_VERSION + 1)}) {
    ObIVFDMLCtDef src;
    src.protocol_version_ = version;
    src.cid_store_idx_ = 1;
    src.vector_store_idx_ = 2;
    src.vector_subschema_id_ = 3;
    src.centroid_table_id_ = 4;
    src.distance_algorithm_ = static_cast<uint8_t>(share::VIDA_L2);
    char buf[128];
    int64_t pos = 0;
    ASSERT_EQ(common::OB_SUCCESS, src.serialize(buf, sizeof(buf), pos));
    ObIVFDMLCtDef dst;
    int64_t decode_pos = 0;
    EXPECT_EQ(common::OB_NOT_SUPPORTED,
              dst.deserialize(buf, pos, decode_pos));
    EXPECT_EQ(0, dst.protocol_version_);
    EXPECT_FALSE(dst.is_valid());
  }
}

TEST(TestExprVecIVFCenterIDDeferred, ctdef_v2_rejects_invalid_assignment_metadata)
{
  ObIVFDMLCtDef meta;
  meta.protocol_version_ = ObIVFDMLCtDef::IVF_CTDEF_VERSION;
  meta.cid_store_idx_ = 3;
  meta.vector_store_idx_ = 4;
  meta.vector_subschema_id_ = 1;
  meta.centroid_table_id_ = 1001;
  meta.distance_algorithm_ = static_cast<uint8_t>(share::VIDA_L2);
  ASSERT_TRUE(meta.is_valid());

  meta.cid_store_idx_ = common::OB_INVALID_INDEX;
  EXPECT_FALSE(meta.is_valid());
  meta.cid_store_idx_ = 3;
  meta.vector_store_idx_ = 3;
  EXPECT_FALSE(meta.is_valid());
  meta.vector_store_idx_ = 4;
  meta.vector_subschema_id_ = UINT16_MAX;
  EXPECT_FALSE(meta.is_valid());
  meta.vector_subschema_id_ = 1;
  meta.centroid_table_id_ = common::OB_INVALID_ID;
  EXPECT_FALSE(meta.is_valid());
  meta.centroid_table_id_ = 1001;
  meta.distance_algorithm_ = static_cast<uint8_t>(share::VIDA_MAX);
  EXPECT_FALSE(meta.is_valid());
}

TEST(TestExprVecIVFCenterIDDeferred, ctdef_slots_are_checked_against_stored_row)
{
  ObIVFDMLCtDef meta;
  meta.protocol_version_ = ObIVFDMLCtDef::IVF_CTDEF_VERSION;
  meta.cid_store_idx_ = 3;
  meta.vector_store_idx_ = 4;
  meta.vector_subschema_id_ = 1;
  meta.centroid_table_id_ = 1001;
  meta.distance_algorithm_ = static_cast<uint8_t>(share::VIDA_L2);
  EXPECT_FALSE(meta.is_valid_for_row_count(0));
  EXPECT_FALSE(meta.is_valid_for_row_count(4));
  EXPECT_TRUE(meta.is_valid_for_row_count(5));
}

TEST(TestExprVecIVFCenterIDDeferred, codegen_metadata_builder_rejects_missing_overlapping_and_oob_slots)
{
  ObIVFDMLCtDef meta;
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            meta.init_v2(common::OB_INVALID_INDEX, 1, 2, 100, share::VIDA_L2, 3));
  EXPECT_FALSE(meta.is_valid());
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            meta.init_v2(1, 1, 2, 100, share::VIDA_L2, 3));
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            meta.init_v2(1, 3, 2, 100, share::VIDA_L2, 3));
  EXPECT_EQ(common::OB_SUCCESS,
            meta.init_v2(1, 2, 9, 100, share::VIDA_COS, 3));
  EXPECT_EQ(9, meta.vector_subschema_id_);
  EXPECT_EQ(100, meta.centroid_table_id_);
  EXPECT_EQ(share::VIDA_COS, meta.distance_algorithm_);
}

TEST(TestExprVecIVFCenterIDDeferred, multiple_ivf_indexes_keep_independent_metadata_and_delete_stays_legacy)
{
  ObIVFDMLCtDef index1;
  ObIVFDMLCtDef index2;
  ObIVFDMLCtDef delete_meta;
  ASSERT_EQ(common::OB_SUCCESS,
            index1.init_v2(1, 2, 11, 1001, share::VIDA_L2, 5));
  ASSERT_EQ(common::OB_SUCCESS,
            index2.init_v2(3, 4, 12, 1002, share::VIDA_COS, 5));
  EXPECT_NE(index1.cid_store_idx_, index2.cid_store_idx_);
  EXPECT_NE(index1.vector_subschema_id_, index2.vector_subschema_id_);
  EXPECT_NE(index1.centroid_table_id_, index2.centroid_table_id_);
  EXPECT_NE(index1.distance_algorithm_, index2.distance_algorithm_);
  EXPECT_EQ(ObIVFDMLCtDef::DISABLED_PROTOCOL_VERSION,
            delete_meta.protocol_version_);
  EXPECT_FALSE(delete_meta.is_valid());
}

TEST(TestExprVecIVFCenterIDDeferred, deferred_protocol_obeys_min_data_version_gate)
{
  EXPECT_FALSE(ObExprVecIVFCenterID::is_deferred_protocol_available(
      DATA_VERSION_4_6_2_1));
  EXPECT_FALSE(ObExprVecIVFCenterID::is_deferred_protocol_available(
      DATA_VERSION_4_6_3_0 - 1));
  EXPECT_TRUE(ObExprVecIVFCenterID::is_deferred_protocol_available(
      DATA_VERSION_4_6_3_0));
  EXPECT_TRUE(ObExprVecIVFCenterID::is_deferred_protocol_available(
      DATA_VERSION_4_6_3_0 + 1));
}

template <typename CtDef>
static void check_domain_ivf_upgrade_ctdef_roundtrip()
{
  common::ObArenaAllocator allocator("IVFUpgrade");
  for (const uint64_t version : {DATA_VERSION_4_6_2_1,
                                 DATA_VERSION_4_6_3_0 - 1,
                                 DATA_VERSION_4_6_3_0,
                                 DATA_VERSION_4_6_3_0 + 1}) {
    SCOPED_TRACE(version);
    CtDef src(allocator);
    src.logical_table_aux_type_ = share::LT_AUX_IVF_CID_ROWID;
    src.lt_namespace_id_ = 11;
    src.lt_logical_table_id_ = 22;
    const bool deferred = ObExprVecIVFCenterID::is_deferred_protocol_available(version);
    ASSERT_EQ(version >= DATA_VERSION_4_6_3_0, deferred);
    if (deferred) {
      ASSERT_EQ(common::OB_SUCCESS,
                src.ivf_dml_ctdef_.init_v2(1, 2, 9, 50001, share::VIDA_L2, 4));
      src.has_ivf_dml_ctdef_ = 1;
    }
    char buf[64 * 1024];
    int64_t pos = 0;
    ASSERT_EQ(common::OB_SUCCESS, src.serialize(buf, sizeof(buf), pos));
    EXPECT_EQ(pos, src.get_serialize_size());
    CtDef dst(allocator);
    int64_t decoded = 0;
    ASSERT_EQ(common::OB_SUCCESS, dst.deserialize(buf, pos, decoded));
    EXPECT_EQ(pos, decoded);
    EXPECT_EQ(deferred, static_cast<bool>(dst.has_ivf_dml_ctdef_));
    EXPECT_EQ(deferred, dst.ivf_dml_ctdef_.is_valid());
    EXPECT_EQ(11, dst.lt_namespace_id_);
    EXPECT_EQ(22, dst.lt_logical_table_id_);
    if (deferred) {
      EXPECT_EQ(ObIVFDMLCtDef::IVF_CTDEF_VERSION, dst.ivf_dml_ctdef_.protocol_version_);
      EXPECT_EQ(1, dst.ivf_dml_ctdef_.cid_store_idx_);
      EXPECT_EQ(2, dst.ivf_dml_ctdef_.vector_store_idx_);
      EXPECT_EQ(50001, dst.ivf_dml_ctdef_.centroid_table_id_);
    } else {
      EXPECT_EQ(ObIVFDMLCtDef::DISABLED_PROTOCOL_VERSION,
                dst.ivf_dml_ctdef_.protocol_version_);
    }
  }
}

TEST(TestExprVecIVFCenterIDDeferred, domain_ivf_insert_ctdef_upgrade_roundtrip)
{
  check_domain_ivf_upgrade_ctdef_roundtrip<ObDASInsCtDef>();
}

TEST(TestExprVecIVFCenterIDDeferred, domain_ivf_update_ctdef_upgrade_roundtrip)
{
  check_domain_ivf_upgrade_ctdef_roundtrip<ObDASUpdCtDef>();
}

TEST(TestExprVecIVFCenterIDDeferred, cid_transport_rejects_null_wrong_width_and_materialized_placeholder)
{
  ObDatum datum;
  datum.set_null();
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            ObDASWriteBuffer::validate_ivf_cid_transport(datum, true));

  char bytes[2 * ObExprVecIVFCenterID::CENTER_ID_BYTE_LENGTH] = {};
  datum.set_string(common::ObString(5, bytes));
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            ObDASWriteBuffer::validate_ivf_cid_transport(datum, true));
  datum.set_string(common::ObString(32, bytes));
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            ObDASWriteBuffer::validate_ivf_cid_transport(datum, true));

  const char placeholder[] = "IVFCTDEFDEFERRED";
  datum.set_string(common::ObString(
      ObExprVecIVFCenterID::CENTER_ID_BYTE_LENGTH, placeholder));
  EXPECT_EQ(common::OB_SUCCESS,
            ObDASWriteBuffer::validate_ivf_cid_transport(datum, true));
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            ObDASWriteBuffer::validate_ivf_cid_transport(datum, false));

  MEMSET(bytes, 'X', ObExprVecIVFCenterID::CENTER_ID_BYTE_LENGTH);
  datum.set_string(common::ObString(
      ObExprVecIVFCenterID::CENTER_ID_BYTE_LENGTH, bytes));
  EXPECT_EQ(common::OB_SUCCESS,
            ObDASWriteBuffer::validate_ivf_cid_transport(datum, false));
}

TEST(TestExprVecIVFCenterIDDeferred, related_tablet_routing_matches_exact_centroid_table)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDASInsCtDef aux1(allocator);
  ObDASInsCtDef centroid1(allocator);
  ObDASInsCtDef centroid2(allocator);
  aux1.index_tid_ = 100;
  centroid1.index_tid_ = 200;
  centroid2.index_tid_ = 300;
  mark_as_ivf_centroid_ctdef(centroid1);
  mark_as_ivf_centroid_ctdef(centroid2);
  DASCtDefFixedArray ctdefs(allocator);
  common::ObTabletIDFixedArray tablets(allocator);
  ASSERT_EQ(common::OB_SUCCESS, ctdefs.init(3));
  ASSERT_EQ(common::OB_SUCCESS, tablets.init(3));
  ASSERT_EQ(common::OB_SUCCESS, ctdefs.push_back(&aux1));
  ASSERT_EQ(common::OB_SUCCESS, ctdefs.push_back(&centroid1));
  ASSERT_EQ(common::OB_SUCCESS, ctdefs.push_back(&centroid2));
  ASSERT_EQ(common::OB_SUCCESS, tablets.push_back(common::ObTabletID(1000)));
  ASSERT_EQ(common::OB_SUCCESS, tablets.push_back(common::ObTabletID(2000)));
  ASSERT_EQ(common::OB_SUCCESS, tablets.push_back(common::ObTabletID(3000)));

  common::ObTabletID result;
  ASSERT_EQ(common::OB_SUCCESS,
            resolve_ivf_centroid_tablet(ctdefs, tablets, 300, result));
  EXPECT_EQ(common::ObTabletID(3000), result);
  ASSERT_EQ(common::OB_SUCCESS,
            resolve_ivf_centroid_tablet(ctdefs, tablets, 200, result));
  EXPECT_EQ(common::ObTabletID(2000), result);
}

TEST(TestExprVecIVFCenterIDDeferred, related_tablet_routing_rejects_misaligned_missing_and_duplicate)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDASInsCtDef centroid(allocator);
  ObDASInsCtDef duplicate(allocator);
  centroid.index_tid_ = 200;
  duplicate.index_tid_ = 200;
  mark_as_ivf_centroid_ctdef(centroid);
  mark_as_ivf_centroid_ctdef(duplicate);
  DASCtDefFixedArray ctdefs(allocator);
  common::ObTabletIDFixedArray tablets(allocator);
  ASSERT_EQ(common::OB_SUCCESS, ctdefs.init(2));
  ASSERT_EQ(common::OB_SUCCESS, tablets.init(2));
  ASSERT_EQ(common::OB_SUCCESS, ctdefs.push_back(&centroid));
  ASSERT_EQ(common::OB_SUCCESS, tablets.push_back(common::ObTabletID(2000)));
  common::ObTabletID result;

  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            resolve_ivf_centroid_tablet(ctdefs, tablets, 201, result));
  ASSERT_EQ(common::OB_SUCCESS, ctdefs.push_back(&duplicate));
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            resolve_ivf_centroid_tablet(ctdefs, tablets, 200, result));
  ASSERT_EQ(common::OB_SUCCESS, tablets.push_back(common::ObTabletID(2001)));
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            resolve_ivf_centroid_tablet(ctdefs, tablets, 200, result));
}

TEST(TestExprVecIVFCenterIDDeferred, multi_tablet_batch_resolves_centroid_per_das_task)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDASInsCtDef task1_centroid(allocator);
  ObDASInsCtDef task2_centroid(allocator);
  task1_centroid.index_tid_ = 200;
  task2_centroid.index_tid_ = 200;
  mark_as_ivf_centroid_ctdef(task1_centroid);
  mark_as_ivf_centroid_ctdef(task2_centroid);
  DASCtDefFixedArray task1_ctdefs(allocator);
  DASCtDefFixedArray task2_ctdefs(allocator);
  common::ObTabletIDFixedArray task1_tablets(allocator);
  common::ObTabletIDFixedArray task2_tablets(allocator);
  ASSERT_EQ(common::OB_SUCCESS, task1_ctdefs.init(1));
  ASSERT_EQ(common::OB_SUCCESS, task2_ctdefs.init(1));
  ASSERT_EQ(common::OB_SUCCESS, task1_tablets.init(1));
  ASSERT_EQ(common::OB_SUCCESS, task2_tablets.init(1));
  ASSERT_EQ(common::OB_SUCCESS, task1_ctdefs.push_back(&task1_centroid));
  ASSERT_EQ(common::OB_SUCCESS, task2_ctdefs.push_back(&task2_centroid));
  ASSERT_EQ(common::OB_SUCCESS,
            task1_tablets.push_back(common::ObTabletID(21001)));
  ASSERT_EQ(common::OB_SUCCESS,
            task2_tablets.push_back(common::ObTabletID(22001)));

  common::ObTabletID task1_result;
  common::ObTabletID task2_result;
  ASSERT_EQ(common::OB_SUCCESS,
            resolve_ivf_centroid_tablet(task1_ctdefs, task1_tablets,
                                        200, task1_result));
  ASSERT_EQ(common::OB_SUCCESS,
            resolve_ivf_centroid_tablet(task2_ctdefs, task2_tablets,
                                        200, task2_result));
  EXPECT_EQ(common::ObTabletID(21001), task1_result);
  EXPECT_EQ(common::ObTabletID(22001), task2_result);
  EXPECT_NE(task1_result, task2_result);
}

TEST(TestExprVecIVFCenterIDDeferred, data_version_transition_changes_fresh_codegen_protocol)
{
  // A cached/prepared plan cannot reuse one protocol choice across the version
  // gate. This fixes fresh-codegen expectations; mixed-version tests verify
  // old-plan invalidation.
  const uint64_t old_version = DATA_VERSION_4_6_2_1;
  const uint64_t new_version = DATA_VERSION_4_6_3_0;
  const bool old_plan_deferred =
      ObExprVecIVFCenterID::is_deferred_protocol_available(old_version);
  const bool new_plan_deferred =
      ObExprVecIVFCenterID::is_deferred_protocol_available(new_version);
  EXPECT_FALSE(old_plan_deferred);
  EXPECT_TRUE(new_plan_deferred);
  EXPECT_NE(old_plan_deferred, new_plan_deferred);
}

TEST(TestExprVecIVFCenterIDDeferred, cache_miss_rows_choose_persisted_center_id)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  float center0[] = {0.0F, 0.0F};
  float center1[] = {10.0F, 10.0F};
  float vector[] = {9.0F, 8.0F};
  common::ObSEArray<share::ObSpfreshCentroidRow, 2> rows;
  share::ObSpfreshCentroidRow row0;
  row0.center_id_ = 101;
  row0.vector_ = reinterpret_cast<const char *>(center0);
  row0.vector_len_ = sizeof(center0);
  share::ObSpfreshCentroidRow row1;
  row1.center_id_ = 202;
  row1.vector_ = reinterpret_cast<const char *>(center1);
  row1.vector_len_ = sizeof(center1);
  ASSERT_EQ(common::OB_SUCCESS, rows.push_back(row0));
  ASSERT_EQ(common::OB_SUCCESS, rows.push_back(row1));

  int64_t center_id = 0;
  ASSERT_EQ(common::OB_SUCCESS,
            ObIVFCidUtil::find_nearest_center_from_persistent_rows(
                vector, 2, rows, share::VIDA_L2, allocator, center_id));
  EXPECT_EQ(202, center_id);
}

TEST(TestExprVecIVFCenterIDDeferred, cache_miss_empty_rows_use_spfresh_bootstrap_center)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  float vector[] = {1.0F, 2.0F};
  common::ObSEArray<share::ObSpfreshCentroidRow, 1> rows;
  int64_t center_id = 999;
  EXPECT_EQ(common::OB_SUCCESS,
            ObIVFCidUtil::find_nearest_center_from_persistent_rows(
                vector, 2, rows, share::VIDA_L2, allocator, center_id));
  EXPECT_EQ(0, center_id);
}

TEST(TestExprVecIVFCenterIDDeferred, cache_miss_dimension_mismatch_is_rejected)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  float center[] = {0.0F, 0.0F, 0.0F};
  float vector[] = {1.0F, 2.0F};
  common::ObSEArray<share::ObSpfreshCentroidRow, 1> rows;
  share::ObSpfreshCentroidRow row;
  row.center_id_ = 7;
  row.vector_ = reinterpret_cast<const char *>(center);
  row.vector_len_ = sizeof(center);
  ASSERT_EQ(common::OB_SUCCESS, rows.push_back(row));
  int64_t center_id = 0;
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            ObIVFCidUtil::find_nearest_center_from_persistent_rows(
                vector, 2, rows, share::VIDA_L2, allocator, center_id));
}

TEST(TestExprVecIVFCenterIDDeferred, cache_miss_invalid_vector_and_centroid_are_rejected)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  float vector[] = {1.0F, 2.0F};
  common::ObSEArray<share::ObSpfreshCentroidRow, 1> rows;
  share::ObSpfreshCentroidRow row;
  row.center_id_ = 7;
  row.vector_ = nullptr;
  row.vector_len_ = sizeof(vector);
  ASSERT_EQ(common::OB_SUCCESS, rows.push_back(row));

  int64_t center_id = 99;
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            ObIVFCidUtil::find_nearest_center_from_persistent_rows(
                nullptr, 2, rows, share::VIDA_L2, allocator, center_id));
  EXPECT_EQ(0, center_id);
  EXPECT_EQ(common::OB_INVALID_ARGUMENT,
            ObIVFCidUtil::find_nearest_center_from_persistent_rows(
                vector, 0, rows, share::VIDA_L2, allocator, center_id));
  EXPECT_EQ(0, center_id);
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            ObIVFCidUtil::find_nearest_center_from_persistent_rows(
                vector, 2, rows, share::VIDA_L2, allocator, center_id));
  EXPECT_EQ(0, center_id);
}

TEST(TestExprVecIVFCenterIDDeferred, insert_adaptor_rejects_misaligned_related_arrays_before_write)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDASInsCtDef main_ctdef(allocator);
  ObDASInsCtDef aux_ctdef(allocator);
  ObDASInsRtDef main_rtdef;
  ObDASWriteBuffer write_buffer;
  ObDASDMLIterator iter(&main_ctdef, write_buffer, allocator);
  ObDASIndexDMLAdaptor<DAS_OP_TABLE_INSERT, ObDASDMLIterator> adaptor;
  DASCtDefFixedArray related_ctdefs(allocator);
  DASRtDefFixedArray related_rtdefs(allocator);
  common::ObTabletIDFixedArray related_tablets(allocator);
  ASSERT_EQ(common::OB_SUCCESS, related_ctdefs.init(1));
  ASSERT_EQ(common::OB_SUCCESS, related_rtdefs.init(1));
  ASSERT_EQ(common::OB_SUCCESS, related_tablets.init(1));
  ASSERT_EQ(common::OB_SUCCESS, related_ctdefs.push_back(&aux_ctdef));

  init_logical_insert_main(main_ctdef, main_rtdef);
  adaptor.ctdef_ = &main_ctdef;
  adaptor.rtdef_ = &main_rtdef;
  adaptor.related_ctdefs_ = &related_ctdefs;
  adaptor.related_rtdefs_ = &related_rtdefs;
  adaptor.related_tablet_ids_ = &related_tablets;

  // conflict-fetch and regular INSERT share this entry point. Misalignment
  // must fail before any primary/auxiliary row can persist a placeholder.
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            adaptor.materialize_logical_ivf_rows(write_buffer));
  EXPECT_EQ(0, write_buffer.get_row_cnt());
}

TEST(TestExprVecIVFCenterIDDeferred, insert_adaptor_rejects_unsupported_and_escaped_ivf_metadata)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDASInsCtDef main_ctdef(allocator);
  ObDASInsCtDef aux_ctdef(allocator);
  ObDASInsRtDef main_rtdef;
  ObDASInsRtDef aux_rtdef;
  ObDASWriteBuffer write_buffer;
  ObDASDMLIterator iter(&main_ctdef, write_buffer, allocator);
  ObDASIndexDMLAdaptor<DAS_OP_TABLE_INSERT, ObDASDMLIterator> adaptor;
  DASCtDefFixedArray related_ctdefs(allocator);
  DASRtDefFixedArray related_rtdefs(allocator);
  common::ObTabletIDFixedArray related_tablets(allocator);
  ASSERT_EQ(common::OB_SUCCESS, related_ctdefs.init(1));
  ASSERT_EQ(common::OB_SUCCESS, related_rtdefs.init(1));
  ASSERT_EQ(common::OB_SUCCESS, related_tablets.init(1));
  ASSERT_EQ(common::OB_SUCCESS, related_ctdefs.push_back(&aux_ctdef));
  ASSERT_EQ(common::OB_SUCCESS, related_rtdefs.push_back(&aux_rtdef));
  ASSERT_EQ(common::OB_SUCCESS,
            related_tablets.push_back(common::ObTabletID(3001)));

  init_logical_insert_main(main_ctdef, main_rtdef);
  adaptor.ctdef_ = &main_ctdef;
  adaptor.rtdef_ = &main_rtdef;
  adaptor.related_ctdefs_ = &related_ctdefs;
  adaptor.related_rtdefs_ = &related_rtdefs;
  adaptor.related_tablet_ids_ = &related_tablets;

  aux_ctdef.ivf_dml_ctdef_.protocol_version_ = 1;
  EXPECT_EQ(common::OB_NOT_SUPPORTED,
            adaptor.materialize_logical_ivf_rows(write_buffer));

  init_logical_ivf_aux(aux_ctdef, 1, 2001);
  aux_ctdef.logical_table_aux_type_ = share::LT_AUX_PRIMARY;
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            adaptor.materialize_logical_ivf_rows(write_buffer));

  // The deferred protocol is IVF_FLAT-only in this release. SQ8 must fail at
  // the adaptor boundary before the empty write buffer reaches storage.
  aux_ctdef.logical_table_aux_type_ = share::LT_AUX_IVF_CID_ROWID;
  aux_ctdef.table_param_.get_data_table_ref().index_type_ =
      share::schema::INDEX_TYPE_VEC_IVFSQ8_ROWKEY_CID_LOCAL;
  EXPECT_EQ(common::OB_NOT_SUPPORTED,
            adaptor.materialize_logical_ivf_rows(write_buffer));
  EXPECT_EQ(0, write_buffer.get_row_cnt());
}

TEST(TestExprVecIVFCenterIDDeferred, sibling_ivf_metadata_must_be_identical_for_one_cid_slot)
{
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDASInsCtDef main_ctdef(allocator);
  ObDASInsCtDef cid_rowid(allocator);
  ObDASInsCtDef rowid_cid(allocator);
  ObDASInsRtDef main_rtdef;
  ObDASInsRtDef cid_rowid_rtdef;
  ObDASInsRtDef rowid_cid_rtdef;
  ObDASWriteBuffer write_buffer;
  ObDASDMLIterator iter(&main_ctdef, write_buffer, allocator);
  ObDASIndexDMLAdaptor<DAS_OP_TABLE_INSERT, ObDASDMLIterator> adaptor;
  DASCtDefFixedArray related_ctdefs(allocator);
  DASRtDefFixedArray related_rtdefs(allocator);
  common::ObTabletIDFixedArray related_tablets(allocator);
  ASSERT_EQ(common::OB_SUCCESS, related_ctdefs.init(2));
  ASSERT_EQ(common::OB_SUCCESS, related_rtdefs.init(2));
  ASSERT_EQ(common::OB_SUCCESS, related_tablets.init(2));
  ASSERT_EQ(common::OB_SUCCESS, related_ctdefs.push_back(&cid_rowid));
  ASSERT_EQ(common::OB_SUCCESS, related_ctdefs.push_back(&rowid_cid));
  ASSERT_EQ(common::OB_SUCCESS, related_rtdefs.push_back(&cid_rowid_rtdef));
  ASSERT_EQ(common::OB_SUCCESS, related_rtdefs.push_back(&rowid_cid_rtdef));
  ASSERT_EQ(common::OB_SUCCESS,
            related_tablets.push_back(common::ObTabletID(3001)));
  ASSERT_EQ(common::OB_SUCCESS,
            related_tablets.push_back(common::ObTabletID(3002)));

  init_logical_insert_main(main_ctdef, main_rtdef);
  init_logical_ivf_aux(cid_rowid, 1, 2001);
  init_logical_ivf_aux(rowid_cid, 1, 2002);
  rowid_cid.logical_table_aux_type_ = share::LT_AUX_IVF_ROWID_CID;
  adaptor.ctdef_ = &main_ctdef;
  adaptor.rtdef_ = &main_rtdef;
  adaptor.related_ctdefs_ = &related_ctdefs;
  adaptor.related_rtdefs_ = &related_rtdefs;
  adaptor.related_tablet_ids_ = &related_tablets;

  // Siblings sharing a CID slot must reuse identical assignment metadata. A
  // different centroid table is rejected before the first write.
  EXPECT_EQ(common::OB_ERR_UNEXPECTED,
            adaptor.materialize_logical_ivf_rows(write_buffer));
  EXPECT_EQ(0, write_buffer.get_row_cnt());
}

TEST(TestExprVecIVFCenterIDDeferred, lazy_materialization_is_once_per_write_attempt)
{
  ObDASWriteBuffer buffer;
  int calls = 0;
  auto prepare = [](void *ctx, ObDASWriteBuffer &) -> int {
    ++*static_cast<int *>(ctx);
    return common::OB_SUCCESS;
  };
  EXPECT_EQ(common::OB_SUCCESS, buffer.ensure_ivf_materialized());
  {
    ObDASWriteBuffer::IvfMaterializationGuard guard(buffer, &calls, prepare);
    EXPECT_EQ(0, calls);
    EXPECT_EQ(common::OB_SUCCESS, buffer.ensure_ivf_materialized());
    // Main batch consumption and both auxiliary rewinds share one result.
    EXPECT_EQ(common::OB_SUCCESS, buffer.ensure_ivf_materialized());
    EXPECT_EQ(common::OB_SUCCESS, buffer.ensure_ivf_materialized());
    EXPECT_EQ(1, calls);
  }
  {
    ObDASWriteBuffer::IvfMaterializationGuard guard(buffer, &calls, prepare);
    EXPECT_EQ(common::OB_SUCCESS, buffer.ensure_ivf_materialized());
    EXPECT_EQ(2, calls);
  }
}

TEST(TestExprVecIVFCenterIDDeferred, failed_materialization_is_not_reentered)
{
  ObDASWriteBuffer buffer;
  int calls = 0;
  auto prepare = [](void *ctx, ObDASWriteBuffer &) -> int {
    ++*static_cast<int *>(ctx);
    return common::OB_EAGAIN;
  };
  ObDASWriteBuffer::IvfMaterializationGuard guard(buffer, &calls, prepare);
  EXPECT_EQ(common::OB_EAGAIN, buffer.ensure_ivf_materialized());
  EXPECT_EQ(common::OB_EAGAIN, buffer.ensure_ivf_materialized());
  common::ObArenaAllocator allocator(common::ObModIds::TEST);
  ObDASInsCtDef ctdef(allocator);
  ObDASDMLIterator iter(&ctdef, buffer, allocator);
  blocksstable::ObDatumRow *row = nullptr;
  int64_t row_count = 1;
  EXPECT_EQ(common::OB_EAGAIN, iter.get_next_row(row));
  EXPECT_EQ(common::OB_EAGAIN, iter.get_next_rows(row, row_count));
  EXPECT_EQ(nullptr, row);
  EXPECT_EQ(0, row_count);
  EXPECT_EQ(1, calls);
}

TEST(TestExprVecIVFCenterIDDeferred, overlapping_guard_preserves_original_owner)
{
  ObDASWriteBuffer buffer;
  int calls = 0;
  auto prepare = [](void *ctx, ObDASWriteBuffer &) -> int {
    ++*static_cast<int *>(ctx);
    return common::OB_SUCCESS;
  };
  ObDASWriteBuffer::IvfMaterializationGuard owner(buffer, &calls, prepare);
  ASSERT_TRUE(owner.is_registered());
  {
    ObDASWriteBuffer::IvfMaterializationGuard duplicate(buffer, &calls, prepare);
    EXPECT_FALSE(duplicate.is_registered());
    EXPECT_EQ(common::OB_STATE_NOT_MATCH, duplicate.ensure());
  }
  EXPECT_EQ(common::OB_SUCCESS, buffer.ensure_ivf_materialized());
  EXPECT_EQ(1, calls);
}

TEST(TestExprVecIVFCenterIDDeferred, write_attempt_restores_only_newly_materialized_cid)
{
  ObDASWriteBuffer buffer;
  char cid_bytes[ObExprVecIVFCenterID::CENTER_ID_BYTE_LENGTH] = {};
  ObDatum cid;
  cid.set_string(common::ObString(sizeof(cid_bytes), cid_bytes));
  {
    ObDASWriteBuffer::IvfMaterializationGuard guard(buffer, nullptr, nullptr);
    ASSERT_EQ(common::OB_SUCCESS, guard.remember_placeholder(cid));
    EXPECT_FALSE(ObExprVecIVFCenterID::is_deferred_placeholder(cid.get_string()));
  }
  EXPECT_EQ(16, cid.get_string().length());
  EXPECT_TRUE(ObExprVecIVFCenterID::is_deferred_placeholder(cid.get_string()));
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
