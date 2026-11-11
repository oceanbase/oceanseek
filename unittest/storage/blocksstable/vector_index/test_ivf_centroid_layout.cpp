/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include <gtest/gtest.h>
#include <cstring>
#include <vector>
#include <utility>
#include "lib/allocator/page_arena.h"
#include "storage/blocksstable/vector_index/ob_ivf_centroid_layout.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace blocksstable;

TEST(TestIvfCentroidLayout, payload_count_is_fixed_width_and_supports_unaligned_rows)
{
  const float values[] = {1.25F, -3.0F, 0.5F};
  const int64_t bytes = ObIvfCentroidPayload::get_size(3);
  ASSERT_EQ(20, bytes);
  char storage[21];
  char *buf = storage + 1;
  const float *decoded = nullptr;
  uint32_t count = 0;
  ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::encode(buf, bytes, values, 3, UINT32_MAX));
  ObIvfCentroidPayloadHeader header;
  MEMCPY(&header, buf, sizeof(header));
  EXPECT_EQ(ObIvfCentroidPayloadHeader::VERSION, header.version_);
  EXPECT_EQ(sizeof(header), header.header_size_);
  EXPECT_EQ(UINT32_MAX, header.child_count_);
  ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(ObString(bytes, buf), 3, decoded, count));
  EXPECT_EQ(UINT32_MAX, count);
  EXPECT_EQ(buf + sizeof(header), reinterpret_cast<const char *>(decoded));
  EXPECT_EQ(0, MEMCMP(values, decoded, sizeof(values)));
  EXPECT_EQ(OB_INVALID_DATA, ObIvfCentroidPayload::decode(ObString(bytes - 1, buf), 3, decoded, count));
  ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::encode(buf, bytes, values, 3, 0));
  ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(ObString(bytes, buf), 3, decoded, count));
  EXPECT_EQ(0, count);
}

TEST(TestIvfCentroidLayout, payload_header_validates_version_size_and_extensions)
{
  const float values[] = {1.25F, -3.0F, 0.5F};
  const int64_t dim = 3;
  const int64_t bytes = ObIvfCentroidPayload::get_size(dim);
  char storage[25] = {};
  char *buf = storage + 1; // The header and vector may both be unaligned.
  const float *vector = nullptr;
  uint32_t count = 0;
  ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::encode(buf, bytes, values, dim, 17));
  ObIvfCentroidPayloadHeader header(17);
  for (const uint16_t version : {uint16_t(0), uint16_t(2)}) {
    header.version_ = version;
    MEMCPY(buf, &header, sizeof(header));
    EXPECT_EQ(OB_INVALID_DATA, ObIvfCentroidPayload::decode(ObString(bytes, buf), dim, vector, count));
    EXPECT_EQ(nullptr, vector);
    EXPECT_EQ(0, count);
  }
  header.version_ = ObIvfCentroidPayloadHeader::VERSION;
  for (const uint16_t size : {uint16_t(0), uint16_t(7), uint16_t(9), uint16_t(UINT16_MAX)}) {
    header.header_size_ = size;
    MEMCPY(buf, &header, sizeof(header));
    EXPECT_EQ(OB_INVALID_DATA, ObIvfCentroidPayload::decode(ObString(bytes, buf), dim, vector, count));
  }
  header.header_size_ = sizeof(header) + sizeof(uint32_t);
  MEMCPY(buf, &header, sizeof(header));
  MEMCPY(buf + header.header_size_, values, sizeof(values));
  ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::decode(
      ObString(bytes + sizeof(uint32_t), buf), dim, vector, count));
  EXPECT_EQ(17, count);
  EXPECT_EQ(buf + header.header_size_, reinterpret_cast<const char *>(vector));
  EXPECT_EQ(0, MEMCMP(values, vector, sizeof(values)));
  EXPECT_EQ(OB_INVALID_DATA, ObIvfCentroidPayload::decode(
      ObString(bytes + sizeof(uint32_t) - 1, buf), dim, vector, count));
}

TEST(TestIvfCentroidLayout, child_count_accumulation_checks_before_narrowing)
{
  uint32_t count = 0;
  ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::add_child_count(count, UINT32_MAX - 1));
  ASSERT_EQ(OB_SUCCESS, ObIvfCentroidPayload::add_child_count(count, 1));
  EXPECT_EQ(UINT32_MAX, count);
  EXPECT_EQ(OB_SIZE_OVERFLOW, ObIvfCentroidPayload::add_child_count(count, 1));
  EXPECT_EQ(UINT32_MAX, count);
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObIvfCentroidPayload::add_child_count(count, -1));
  EXPECT_EQ(UINT32_MAX, count);
  count = 0;
  EXPECT_EQ(OB_SIZE_OVERFLOW, ObIvfCentroidPayload::add_child_count(count, INT64_MAX));
  EXPECT_EQ(0, count);
  static_assert(sizeof(decltype(std::declval<share::ObIvfSSTableCentCache>().get_child_count(0))) == 4,
      "centroid cache child counts must remain 32-bit");
}

TEST(TestIvfCentroidLayout, stored_identity_is_independent_of_group_membership)
{
  ObIvfSSTableMeta meta;
  meta.centroid_count_ = 3;
  int64_t self = -1, group = -1;
  EXPECT_EQ(1, meta.leaf_storage_cid(0));
  EXPECT_EQ(OB_INVALID_DATA, meta.decode_centroid_key(0, 0, self, group));
  ASSERT_EQ(OB_SUCCESS, meta.decode_centroid_key(0, 3, self, group));
  EXPECT_EQ(3, self);
  EXPECT_EQ(0, group);
  EXPECT_EQ(OB_INVALID_DATA, meta.decode_centroid_key(3, 0, self, group));
  EXPECT_EQ(OB_INVALID_DATA, meta.decode_centroid_key(1, 2, self, group));
  EXPECT_EQ(OB_INVALID_DATA, meta.decode_centroid_key(0, 4, self, group));
  meta.centroid_layout_ = IVF_CENTROID_HIERARCHICAL;
  meta.parent_centroid_count_ = 2;
  EXPECT_EQ(3, meta.leaf_storage_cid(0));
  ASSERT_EQ(OB_SUCCESS, meta.decode_centroid_key(0, 2, self, group));
  EXPECT_EQ(2, self);
  EXPECT_EQ(0, group);
  EXPECT_EQ(OB_INVALID_DATA, meta.decode_centroid_key(0, 3, self, group));
  for (int64_t p = 1; p <= 2; ++p) {
    ASSERT_EQ(OB_SUCCESS, meta.decode_centroid_key(p, 4, self, group));
    EXPECT_EQ(4, self);
    EXPECT_EQ(p, group);
  }
  EXPECT_EQ(OB_INVALID_DATA, meta.decode_centroid_key(1, 2, self, group));
  EXPECT_EQ(OB_INVALID_DATA, meta.decode_centroid_key(3, 4, self, group));
  EXPECT_EQ(OB_INVALID_DATA, meta.decode_centroid_key(2, 6, self, group));
}

TEST(TestIvfCentroidLayout, grouping_preserves_leaf_order_and_soar_replica_identity)
{
  ObArenaAllocator arena;
  ObArray<float> leaves;
  const int64_t k = 96, dim = 3;
  for (int64_t j = 0; j < k; ++j) {
    ASSERT_EQ(OB_SUCCESS, leaves.push_back(static_cast<float>(j)));
    ASSERT_EQ(OB_SUCCESS, leaves.push_back(static_cast<float>(j % 7)));
    ASSERT_EQ(OB_SUCCESS, leaves.push_back(static_cast<float>(j % 11)));
  }
  const std::vector<float> before(leaves.begin(), leaves.end());
  share::ObHcaTrainParam train;
  share::ObHcaSoarParam soar;
  ObIvfCentroidLayout layout(arena);
  ASSERT_EQ(OB_SUCCESS, layout.build(leaves, dim, train, soar, nullptr, k + 1, 8));
  EXPECT_FALSE(layout.is_hierarchical());
  EXPECT_EQ(k, layout.center_row_count());
  // The boundary is inclusive: equality selects the grouped layout.
  ASSERT_EQ(OB_SUCCESS, layout.build(leaves, dim, train, soar, nullptr, k, 8));
  ASSERT_TRUE(layout.is_ready());
  ASSERT_TRUE(layout.is_hierarchical());
  ASSERT_GT(layout.parent_count(), 1);
  EXPECT_EQ(0, MEMCMP(before.data(), leaves.get_data(), before.size() * sizeof(float)));
  std::vector<int64_t> occurrences(k, 0);
  int64_t rows = 0, replicas = 0;
  for (int64_t p = 0; p < layout.parent_count(); ++p) {
    EXPECT_GT(layout.group_count(p), 0);
    int64_t previous = -1;
    for (int64_t pos = layout.group_begin(p); pos < layout.group_end(p); ++pos) {
      const int64_t j = layout.member(pos);
      ASSERT_GT(j, previous);
      ASSERT_LT(j, k);
      EXPECT_TRUE(layout.primary_parent(j) == p + 1 || layout.secondary_parent(j) == p + 1);
      ++occurrences[j];
      previous = j;
      ++rows;
    }
  }
  for (int64_t j = 0; j < k; ++j) {
    EXPECT_EQ(layout.parent_count() + 1 + j, layout.leaf_cid(j));
    const bool secondary = layout.secondary_parent(j) > 0;
    EXPECT_EQ(secondary ? 2 : 1, occurrences[j]);
    EXPECT_NE(layout.primary_parent(j), layout.secondary_parent(j));
    replicas += secondary;
  }
  EXPECT_EQ(k + replicas, rows);
  EXPECT_EQ(layout.parent_count() + rows, layout.center_row_count());
}

TEST(TestIvfCentroidLayout, production_threshold_and_degenerate_groups)
{
  EXPECT_EQ(300000, IVF_CENTROID_HIERARCHY_THRESHOLD);
  EXPECT_EQ(384, IVF_PARENT_CLUSTER_SIZE);
  ObArenaAllocator arena;
  ObArray<float> leaves;
  for (int64_t j = 0; j < 12; ++j) { ASSERT_EQ(OB_SUCCESS, leaves.push_back(1.0F)); }
  ObIvfCentroidLayout layout(arena);
  share::ObHcaTrainParam train;
  share::ObHcaSoarParam soar;
  ASSERT_EQ(OB_SUCCESS, layout.build(leaves, 1, train, soar, nullptr, 12, 4));
  EXPECT_EQ(1, layout.parent_count());
  EXPECT_EQ(12, layout.group_count(0));
  EXPECT_EQ(13, layout.center_row_count());
  train.max_local_k_ = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, layout.build(leaves, 1, train, soar, nullptr, 12, 4));
  EXPECT_FALSE(layout.is_ready());
}

TEST(TestIvfCentroidLayout, borrowed_contiguous_buffer_preserves_layout_and_input)
{
  const int64_t k = 96, dim = 3;
  std::vector<float> buffer(k * dim);
  ObArray<float> reference;
  for (int64_t j = 0; j < k; ++j) {
    const float vector[] = {static_cast<float>(j), static_cast<float>(j % 7), static_cast<float>(j % 11)};
    for (int64_t d = 0; d < dim; ++d) {
      buffer[j * dim + d] = vector[d];
      ASSERT_EQ(OB_SUCCESS, reference.push_back(vector[d]));
    }
  }
  ObArenaAllocator arena;
  ObIvfCentroidLayout expected(arena), actual(arena);
  share::ObHcaTrainParam train;
  share::ObHcaSoarParam soar;
  ASSERT_EQ(OB_SUCCESS, expected.build(reference, dim, train, soar, nullptr, k, 8));
  {
    // The wrapper owns no vector memory and may disappear immediately after build.
    const ObArrayWrap<float> borrowed(buffer.data(), buffer.size());
    ASSERT_EQ(buffer.data(), borrowed.get_data());
    ASSERT_EQ(OB_SUCCESS, actual.build(borrowed, dim, train, soar, nullptr, k, 8));
  }
  ASSERT_EQ(expected.parent_count(), actual.parent_count());
  ASSERT_EQ(expected.center_row_count(), actual.center_row_count());
  for (int64_t j = 0; j < k; ++j) {
    EXPECT_EQ(expected.primary_parent(j), actual.primary_parent(j));
    EXPECT_EQ(expected.secondary_parent(j), actual.secondary_parent(j));
    EXPECT_EQ(expected.leaf_cid(j), actual.leaf_cid(j));
  }
  for (int64_t p = 0; p < actual.parent_count(); ++p) {
    EXPECT_EQ(0, MEMCMP(expected.parent_vector(p), actual.parent_vector(p), dim * sizeof(float)));
    ASSERT_EQ(expected.group_count(p), actual.group_count(p));
    for (int64_t offset = 0; offset < actual.group_count(p); ++offset) {
      EXPECT_EQ(expected.member(expected.group_begin(p) + offset), actual.member(actual.group_begin(p) + offset));
    }
  }
  EXPECT_EQ(0, MEMCMP(reference.get_data(), buffer.data(), buffer.size() * sizeof(float)));
  const float parent_value = actual.parent_vector(0)[0];
  buffer[0] = -1000.0F;
  EXPECT_FLOAT_EQ(parent_value, actual.parent_vector(0)[0]);
  ObArrayWrap<float> missing(nullptr, k * dim);
  EXPECT_EQ(OB_INVALID_ARGUMENT, actual.build(missing, dim, train, soar, nullptr, k, 8));
  EXPECT_FALSE(actual.is_ready());
}

TEST(TestIvfCentroidLayout, cache_key_separates_root_and_child_groups)
{
  storage::ObITable::TableKey table;
  table.table_type_ = storage::ObITable::IVF_INDEX_SSTABLE;
  table.tablet_id_ = common::ObTabletID(123);
  table.version_range_.base_version_ = 0;
  table.version_range_.snapshot_version_ = 100;
  const share::ObIvfCacheMgrKey root(table), child(table, 1), other(table, 2), copy(table, 1);
  EXPECT_TRUE(root.is_valid());
  EXPECT_TRUE(child.is_valid());
  EXPECT_FALSE(root == child);
  EXPECT_FALSE(child == other);
  EXPECT_TRUE(child == copy);
  EXPECT_EQ(child.hash(), copy.hash());
  EXPECT_FALSE(share::ObIvfCacheMgrKey(table, -1).is_valid());
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
