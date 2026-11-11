/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <vector>
#define private public
#define protected public
#include "share/vector_index/ob_vector_index_segment.h"
#include "share/vector_index/ob_vector_index_merge_task.h"
#include "share/vector_index/ob_vector_index_aux_table_handler.h"
#undef private
#undef protected
#include "lib/allocator/page_arena.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "share/scn.h"
#include "share/ob_cluster_version.h"
#include "storage/ob_storage_rpc.h"
#include "storage/tx/ob_trans_define_v4.h"

namespace oceanbase {
namespace common {

class TestVectorIndexSnapMeta : public ::testing::Test
{
public:
  TestVectorIndexSnapMeta() {}
  ~TestVectorIndexSnapMeta() {}
  void SetUp() override
  {
    allocator_.reset();
    ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_1);
  }
  void TearDown() override
  {
    ODV_MGR.set_mock_data_version(0);
  }

protected:
  int build_bridge(const int64_t from_base_scn,
                   share::ObVectorIndexSegmentMeta &meta,
                   share::ObVectorIndexSegmentHandle &segment)
  {
    int ret = OB_SUCCESS;
    meta.seg_type_ = share::ObVectorIndexSegmentType::SNAP_META_BRIDGE;
    meta.index_type_ = share::ObVectorIndexAlgorithmType::VIAT_MAX;
    meta.scn_ = from_base_scn;
    meta.has_segment_meta_row_ = true;
    meta.blocks_cnt_ = 1;
    if (OB_FAIL(share::ObVectorIndexSegment::create_bitmap_only(segment, TEST_TENANT_ID, allocator_))) {
    } else if (OB_ISNULL(meta.vbitmap_ = OB_NEWx(
        share::ObVectorIndexRoaringBitMap, &allocator_, &allocator_, TEST_TENANT_ID))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(meta.vbitmap_->init(true, true))) {
    }
    return ret;
  }

  int serialize_bridge(const share::ObVectorIndexSegmentMeta &meta,
                       const share::ObVectorIndexSegmentHandle &segment,
                       ObString &data)
  {
    int ret = OB_SUCCESS;
    const share::ObVectorIndexSnapMetaHeader header;
    const int64_t size = share::ObVectorIndexSnapMetaHeader::HEAD_LEN
        + meta.get_serialize_size() + segment->get_serialize_meta_size(meta.vbitmap_);
    char *buf = static_cast<char *>(allocator_.alloc(size));
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, reinterpret_cast<const char *>(&header),
          share::ObVectorIndexSnapMetaHeader::HEAD_LEN);
      pos += share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
      if (OB_FAIL(meta.serialize(buf, size, pos))) {
      } else if (OB_FAIL(segment->serialize_meta(buf, size, pos, meta.vbitmap_))) {
      } else if (pos != size) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        data.assign_ptr(buf, size);
      }
    }
    return ret;
  }

  ObArenaAllocator allocator_{"TestSnapMeta"};
  static const uint64_t TEST_TENANT_ID = 1001;
};

class BitmapTrackingAllocator : public ObIAllocator
{
public:
  explicit BitmapTrackingAllocator(ObIAllocator &allocator) : allocator_(allocator) {}
  void *alloc(const int64_t size) override
  {
    void *ptr = allocator_.alloc(size);
    if (nullptr != ptr) { ++live_; }
    return ptr;
  }
  void *alloc(const int64_t size, const ObMemAttr &) override { return alloc(size); }
  void free(void *ptr) override
  {
    if (nullptr != ptr) { --live_; }
    allocator_.free(ptr);
  }
  int64_t live_ = 0;
private:
  ObIAllocator &allocator_;
};

TEST_F(TestVectorIndexSnapMeta, insert_prune_watermark_without_delete_candidates)
{
  share::ObVecIdxSnapTableSegMergeOp op(TEST_TENANT_ID);
  transaction::ObTxReadSnapshot snapshot;
  share::SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_inner_table_field(200));
  snapshot.init_special_read(scn);
  share::ObVectorIndexMeta old_meta;
  share::ObVectorIndexMeta new_meta;
  old_meta.last_bitmap_update_scn_ = 100;
  new_meta.last_bitmap_update_scn_ = old_meta.last_bitmap_update_scn_;
  // No delete plan: this is the early return that used to lose insert reclamation.
  ASSERT_EQ(OB_SUCCESS, op.before_write_meta(nullptr, nullptr, snapshot, old_meta, new_meta, 0));
  EXPECT_EQ(100, new_meta.last_bitmap_update_scn_);
  op.set_insert_bitmap_pruned(true);
  ASSERT_EQ(OB_SUCCESS, op.before_write_meta(nullptr, nullptr, snapshot, old_meta, new_meta, 0));
  EXPECT_EQ(200, new_meta.last_bitmap_update_scn_);
  // A later publication with no actual pruning keeps the inherited watermark.
  old_meta.last_bitmap_update_scn_ = new_meta.last_bitmap_update_scn_;
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_inner_table_field(300));
  snapshot.init_special_read(scn);
  op.set_insert_bitmap_pruned(false);
  ASSERT_EQ(OB_SUCCESS, op.before_write_meta(nullptr, nullptr, snapshot, old_meta, new_meta, 0));
  EXPECT_EQ(200, new_meta.last_bitmap_update_scn_);
  // Publishing an empty deletion plan has the same pure-insert behavior.
  share::ObVectorIndexRoaringBitMap removable(&allocator_, TEST_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, removable.init(true, true));
  op.set_bitmap_prune_plan(&old_meta, &removable);
  op.set_insert_bitmap_pruned(true);
  ASSERT_EQ(OB_SUCCESS, op.before_write_meta(nullptr, nullptr, snapshot, old_meta, new_meta, 0));
  EXPECT_EQ(300, new_meta.last_bitmap_update_scn_);
}

TEST_F(TestVectorIndexSnapMeta, truncated_bitmap_releases_partial_decode)
{
  share::ObVectorIndexSegmentMeta source;
  share::ObVectorIndexSegmentHandle graph;
  ASSERT_EQ(OB_SUCCESS, build_bridge(100, source, graph));
  roaring::api::roaring64_bitmap_add(source.vbitmap_->insert_bitmap_, 1);
  roaring::api::roaring64_bitmap_add(source.vbitmap_->delete_bitmap_, 2);
  const int64_t size = graph->get_serialize_meta_size(source.vbitmap_);
  char *buf = static_cast<char *>(allocator_.alloc(size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, graph->serialize_meta(buf, size, pos, source.vbitmap_));
  BitmapTrackingAllocator allocator(allocator_);
  {
    share::ObVectorIndexSegmentMeta target;
    ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexSegment::create_bitmap_only(
        target.segment_handle_, TEST_TENANT_ID, allocator));
    pos = 0;
    EXPECT_NE(OB_SUCCESS, target.segment_handle_->deserialize_meta(buf, size - 1, pos, target.vbitmap_));
  }
  EXPECT_EQ(0, allocator.live_);
}

TEST_F(TestVectorIndexSnapMeta, bitmap_versions_release_independently_of_shared_graph)
{
  BitmapTrackingAllocator allocator(allocator_);
  share::ObVectorIndexSegmentHandle graph;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexSegment::create_bitmap_only(graph, TEST_TENANT_ID, allocator_));
  share::ObVectorIndexSegmentMeta old_meta;
  old_meta.segment_handle_ = graph;
  old_meta.vbitmap_ = OB_NEWx(share::ObVectorIndexRoaringBitMap, &allocator, &allocator, TEST_TENANT_ID);
  ASSERT_NE(nullptr, old_meta.vbitmap_);
  ASSERT_EQ(OB_SUCCESS, old_meta.vbitmap_->init(true, true));
  roaring::api::roaring64_bitmap_add(old_meta.vbitmap_->delete_bitmap_, 1);
  share::ObVectorIndexSegmentMeta old_query = old_meta;
  share::ObVectorIndexSegmentMeta current = old_meta;
  share::ObVectorIndexSegmentMeta next = old_meta;
  for (auto *meta : {&current, &next}) {
    auto *bitmap = OB_NEWx(share::ObVectorIndexRoaringBitMap, &allocator, &allocator, TEST_TENANT_ID);
    ASSERT_NE(nullptr, bitmap);
    ASSERT_EQ(OB_SUCCESS, bitmap->init(true, true));
    meta->set_vbitmap(bitmap);
    share::ObVectorIndexRoaringBitMap::destroy(bitmap);
  }
  EXPECT_EQ(3, allocator.live_);
  EXPECT_NE(current.vbitmap_, next.vbitmap_);
  EXPECT_TRUE(roaring::api::roaring64_bitmap_contains(old_query.vbitmap_->delete_bitmap_, 1));
  EXPECT_FALSE(roaring::api::roaring64_bitmap_contains(next.vbitmap_->delete_bitmap_, 1));
  old_meta.release();
  EXPECT_EQ(3, allocator.live_); // The old query still owns B0.
  old_query.release();
  EXPECT_EQ(2, allocator.live_);
  current = next; // Release B1, share B2; no bitmap chain.
  EXPECT_EQ(1, allocator.live_);
  next.release();
  EXPECT_EQ(1, allocator.live_);
  EXPECT_EQ(graph.get(), current.segment_handle_.get());
  current.release();
  EXPECT_EQ(0, allocator.live_);
  EXPECT_TRUE(graph.is_valid()); // Graph retention must not retain any bitmap.
}

TEST_F(TestVectorIndexSnapMeta, serialize_deserialize_roundtrip)
{
  share::ObVectorIndexSegmentMeta src_meta;
  share::ObVectorIndexSegmentHandle src_segment;
  ASSERT_EQ(OB_SUCCESS, build_bridge(100, src_meta, src_segment));
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->insert_bitmap_, 1);
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->insert_bitmap_, 5);
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->insert_bitmap_, 100);
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->delete_bitmap_, 3);
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->delete_bitmap_, 7);

  // serialize
  const share::ObVectorIndexSnapMetaHeader src_header;
  int64_t ser_size = share::ObVectorIndexSnapMetaHeader::HEAD_LEN
      + src_meta.get_serialize_size() + src_segment->get_serialize_meta_size(src_meta.vbitmap_);
  ASSERT_GT(ser_size, 0);
  char *buf = static_cast<char*>(allocator_.alloc(ser_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  MEMCPY(buf + pos, reinterpret_cast<const char*>(&src_header),
      share::ObVectorIndexSnapMetaHeader::HEAD_LEN);
  pos += share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, src_meta.serialize(buf, ser_size, pos));
  ASSERT_EQ(OB_SUCCESS, src_segment->serialize_meta(buf, ser_size, pos, src_meta.vbitmap_));
  ASSERT_EQ(pos, ser_size);

  // deserialize into a reused object to ensure keyless bridge metadata clears stale segment keys.
  share::ObVectorIndexSegmentMeta dst_meta;
  const ObString stale_key = ObString::make_string("stale_segment_key");
  ASSERT_EQ(OB_SUCCESS, dst_meta.deep_copy_seg_key(stale_key, stale_key));
  share::ObVectorIndexSegmentHandle dst_segment;
  share::ObVectorIndexSnapMetaHeader dst_header;
  int64_t dpos = 0;
  MEMCPY(reinterpret_cast<char*>(&dst_header), buf,
      share::ObVectorIndexSnapMetaHeader::HEAD_LEN);
  dpos += share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
  ASSERT_TRUE(dst_header.is_valid());
  ASSERT_EQ(OB_SUCCESS, dst_meta.deserialize(buf, ser_size, dpos));
  ASSERT_EQ(share::ObVectorIndexSegmentType::SNAP_META_BRIDGE, dst_meta.seg_type_);
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexSegment::create_bitmap_only(dst_segment, TEST_TENANT_ID, allocator_));
  ASSERT_EQ(OB_SUCCESS, dst_segment->deserialize_meta(buf, ser_size, dpos, dst_meta.vbitmap_));
  ASSERT_EQ(dpos, ser_size);

  // A bridge has no data-block key range, unlike a regular segment meta.
  ASSERT_EQ(dst_meta.scn_, 100);
  ASSERT_TRUE(dst_meta.start_key_.empty());
  ASSERT_TRUE(dst_meta.end_key_.empty());

  // verify bitmap content
  ASSERT_NE(nullptr, dst_meta.vbitmap_->insert_bitmap_);
  ASSERT_NE(nullptr, dst_meta.vbitmap_->delete_bitmap_);
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_meta.vbitmap_->insert_bitmap_, 1));
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_meta.vbitmap_->insert_bitmap_, 5));
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_meta.vbitmap_->insert_bitmap_, 100));
  ASSERT_FALSE(roaring::api::roaring64_bitmap_contains(dst_meta.vbitmap_->insert_bitmap_, 2));
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_meta.vbitmap_->delete_bitmap_, 3));
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_meta.vbitmap_->delete_bitmap_, 7));
  ASSERT_FALSE(roaring::api::roaring64_bitmap_contains(dst_meta.vbitmap_->delete_bitmap_, 1));

  ASSERT_EQ(roaring::api::roaring64_bitmap_get_cardinality(dst_meta.vbitmap_->insert_bitmap_), 3);
  ASSERT_EQ(roaring::api::roaring64_bitmap_get_cardinality(dst_meta.vbitmap_->delete_bitmap_), 2);
}

TEST_F(TestVectorIndexSnapMeta, regular_segment_empty_keys_are_rejected)
{
  share::ObVectorIndexSegmentMeta src_meta;
  src_meta.seg_type_ = share::ObVectorIndexSegmentType::BASE;
  src_meta.index_type_ = share::ObVectorIndexAlgorithmType::VIAT_HNSW;
  src_meta.scn_ = 100;
  src_meta.blocks_cnt_ = 1;

  const int64_t ser_size = src_meta.get_serialize_size();
  char *buf = static_cast<char *>(allocator_.alloc(ser_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src_meta.serialize(buf, ser_size, pos));
  ASSERT_EQ(ser_size, pos);

  share::ObVectorIndexSegmentMeta dst_meta;
  pos = 0;
  ASSERT_NE(OB_SUCCESS, dst_meta.deserialize(buf, ser_size, pos));
}

TEST_F(TestVectorIndexSnapMeta, deserialize_truncated_payload)
{
  share::ObVectorIndexSegmentMeta src_meta;
  share::ObVectorIndexSegmentHandle src_segment;
  ASSERT_EQ(OB_SUCCESS, build_bridge(100, src_meta, src_segment));
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->insert_bitmap_, 1);

  const share::ObVectorIndexSnapMetaHeader src_header;
  const int64_t ser_size = share::ObVectorIndexSnapMetaHeader::HEAD_LEN
      + src_meta.get_serialize_size() + src_segment->get_serialize_meta_size(src_meta.vbitmap_);
  char *buf = static_cast<char*>(allocator_.alloc(ser_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  MEMCPY(buf + pos, reinterpret_cast<const char*>(&src_header),
      share::ObVectorIndexSnapMetaHeader::HEAD_LEN);
  pos += share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, src_meta.serialize(buf, ser_size, pos));
  ASSERT_EQ(OB_SUCCESS, src_segment->serialize_meta(buf, ser_size, pos, src_meta.vbitmap_));

  share::ObVectorIndexSegmentMeta dst_meta;
  int64_t dpos = share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, dst_meta.deserialize(buf, ser_size - 1, dpos));
  share::ObVectorIndexSegmentHandle dst_segment;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexSegment::create_bitmap_only(dst_segment, TEST_TENANT_ID, allocator_));
  ASSERT_NE(OB_SUCCESS, dst_segment->deserialize_meta(buf, ser_size - 1, dpos, dst_meta.vbitmap_));
}

TEST_F(TestVectorIndexSnapMeta, reject_invalid_bridge_envelope)
{
  share::ObVectorIndexSegmentMeta src_meta;
  share::ObVectorIndexSegmentHandle src_segment;
  ASSERT_EQ(OB_SUCCESS, build_bridge(100, src_meta, src_segment));
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->insert_bitmap_, 1);

  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, serialize_bridge(src_meta, src_segment, serialized));

  share::ObVectorIndexSegmentMeta dst_meta;
  share::ObVectorIndexSegmentHandle dst_segment;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexSegmentMeta::deserialize_snap_meta_bridge(
      serialized, TEST_TENANT_ID, allocator_, dst_meta, dst_segment));

  char *invalid = static_cast<char *>(allocator_.alloc(serialized.length() + 1));
  ASSERT_NE(nullptr, invalid);
  MEMCPY(invalid, serialized.ptr(), serialized.length());

  share::ObVectorIndexSnapMetaHeader *header =
      reinterpret_cast<share::ObVectorIndexSnapMetaHeader *>(invalid);
  header->version_ = share::ObVectorIndexSnapMetaHeader::CURRENT_VERSION + 1;
  ObString invalid_data(serialized.length(), invalid);
  ASSERT_NE(OB_SUCCESS, share::ObVectorIndexSegmentMeta::deserialize_snap_meta_bridge(
      invalid_data, TEST_TENANT_ID, allocator_, dst_meta, dst_segment));

  MEMCPY(invalid, serialized.ptr(), serialized.length());
  header->layout_type_ = static_cast<uint64_t>(share::ObVectorIndexSnapMetaLayoutType::INVALID);
  ASSERT_NE(OB_SUCCESS, share::ObVectorIndexSegmentMeta::deserialize_snap_meta_bridge(
      invalid_data, TEST_TENANT_ID, allocator_, dst_meta, dst_segment));

  MEMCPY(invalid, serialized.ptr(), serialized.length());
  header->entry_count_ = share::ObVectorIndexSnapMetaHeader::CURRENT_ENTRY_COUNT + 1;
  ASSERT_NE(OB_SUCCESS, share::ObVectorIndexSegmentMeta::deserialize_snap_meta_bridge(
      invalid_data, TEST_TENANT_ID, allocator_, dst_meta, dst_segment));

  ObString truncated(serialized.length() - 1, serialized.ptr());
  ASSERT_NE(OB_SUCCESS, share::ObVectorIndexSegmentMeta::deserialize_snap_meta_bridge(
      truncated, TEST_TENANT_ID, allocator_, dst_meta, dst_segment));

  MEMCPY(invalid, serialized.ptr(), serialized.length());
  invalid[serialized.length()] = 0;
  ObString trailing(serialized.length() + 1, invalid);
  ASSERT_NE(OB_SUCCESS, share::ObVectorIndexSegmentMeta::deserialize_snap_meta_bridge(
      trailing, TEST_TENANT_ID, allocator_, dst_meta, dst_segment));
}

TEST_F(TestVectorIndexSnapMeta, reject_bitmap_declared_length_with_trailing_data)
{
  share::ObVectorIndexSegmentMeta src_meta;
  share::ObVectorIndexSegmentHandle src_segment;
  ASSERT_EQ(OB_SUCCESS, build_bridge(100, src_meta, src_segment));
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->insert_bitmap_, 1);
  roaring::api::roaring64_bitmap_add(src_meta.vbitmap_->delete_bitmap_, 2);

  ObString serialized;
  ASSERT_EQ(OB_SUCCESS, serialize_bridge(src_meta, src_segment, serialized));
  const int64_t malformed_len = serialized.length() + 1;
  char *malformed_buf = static_cast<char *>(allocator_.alloc(malformed_len));
  ASSERT_NE(nullptr, malformed_buf);
  MEMCPY(malformed_buf, serialized.ptr(), serialized.length());
  malformed_buf[serialized.length()] = 0;

  // Locate the final delete bitmap's declared size and its enclosing UNIS payload length.
  int64_t parse_pos = share::ObVectorIndexSnapMetaHeader::HEAD_LEN
      + src_meta.get_serialize_size() + share::ObVectorIndexSegmentHeader::HEAD_LEN;
  bool has_ibitmap = true;
  bool has_vbitmap = false;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(
      malformed_buf, malformed_len, parse_pos, has_ibitmap));
  ASSERT_FALSE(has_ibitmap);
  ASSERT_EQ(OB_SUCCESS, serialization::decode(
      malformed_buf, malformed_len, parse_pos, has_vbitmap));
  ASSERT_TRUE(has_vbitmap);

  int64_t bitmap_version = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(
      malformed_buf, malformed_len, parse_pos, bitmap_version));
  ASSERT_EQ(1, bitmap_version);
  const int64_t bitmap_payload_size_pos = parse_pos;
  int64_t bitmap_payload_size = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(
      malformed_buf, malformed_len, parse_pos, bitmap_payload_size));
  ASSERT_EQ(serialization::OB_SERIALIZE_SIZE_NEED_BYTES,
      parse_pos - bitmap_payload_size_pos);

  int64_t insert_bitmap_size = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(
      malformed_buf, malformed_len, parse_pos, insert_bitmap_size));
  ASSERT_GT(insert_bitmap_size, 0);
  parse_pos += insert_bitmap_size;
  const int64_t delete_bitmap_size_pos = parse_pos;
  int64_t delete_bitmap_size = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(
      malformed_buf, malformed_len, parse_pos, delete_bitmap_size));
  ASSERT_GT(delete_bitmap_size, 0);
  ASSERT_EQ(serialized.length(), parse_pos + delete_bitmap_size);

  // Make both envelopes consume the appended byte. A prefix-only roaring decoder would
  // accept this, so strict decoding must compare the portable encoded size with the declaration.
  char encoded_size[16];
  int64_t encoded_size_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(
      encoded_size, sizeof(encoded_size), encoded_size_pos, delete_bitmap_size + 1));
  ASSERT_EQ(parse_pos - delete_bitmap_size_pos, encoded_size_pos);
  MEMCPY(malformed_buf + delete_bitmap_size_pos, encoded_size, encoded_size_pos);
  int64_t fixed_size_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(
      malformed_buf + bitmap_payload_size_pos,
      serialization::OB_SERIALIZE_SIZE_NEED_BYTES,
      fixed_size_pos,
      bitmap_payload_size + 1));
  ASSERT_EQ(serialization::OB_SERIALIZE_SIZE_NEED_BYTES, fixed_size_pos);

  share::ObVectorIndexSegmentMeta dst_meta;
  share::ObVectorIndexSegmentHandle dst_segment;
  ObString malformed(malformed_len, malformed_buf);
  ASSERT_NE(OB_SUCCESS, share::ObVectorIndexSegmentMeta::deserialize_snap_meta_bridge(
      malformed, TEST_TENANT_ID, allocator_, dst_meta, dst_segment));
}

TEST_F(TestVectorIndexSnapMeta, empty_bridge_bitmap_roundtrip)
{
  share::ObVectorIndexSegmentMeta src_meta;
  share::ObVectorIndexSegmentHandle src_segment;
  ASSERT_EQ(OB_SUCCESS, build_bridge(100, src_meta, src_segment));
  ASSERT_EQ(0, roaring::api::roaring64_bitmap_get_cardinality(
      src_meta.vbitmap_->insert_bitmap_));
  ASSERT_EQ(0, roaring::api::roaring64_bitmap_get_cardinality(
      src_meta.vbitmap_->delete_bitmap_));

  const int64_t ser_size = src_segment->get_serialize_meta_size(src_meta.vbitmap_);
  char *buf = static_cast<char *>(allocator_.alloc(ser_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src_segment->serialize_meta(buf, ser_size, pos, src_meta.vbitmap_));
  ASSERT_EQ(ser_size, pos);

  share::ObVectorIndexSegmentMeta dst_meta;
  share::ObVectorIndexSegmentHandle dst_segment;
  ASSERT_EQ(OB_SUCCESS,
      share::ObVectorIndexSegment::create_bitmap_only(
          dst_segment, TEST_TENANT_ID, allocator_));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst_segment->deserialize_meta(buf, ser_size, pos, dst_meta.vbitmap_));
  ASSERT_EQ(ser_size, pos);
  ASSERT_NE(nullptr, dst_meta.vbitmap_);
  ASSERT_NE(nullptr, dst_meta.vbitmap_->insert_bitmap_);
  ASSERT_NE(nullptr, dst_meta.vbitmap_->delete_bitmap_);
  ASSERT_EQ(0, roaring::api::roaring64_bitmap_get_cardinality(
      dst_meta.vbitmap_->insert_bitmap_));
  ASSERT_EQ(0, roaring::api::roaring64_bitmap_get_cardinality(
      dst_meta.vbitmap_->delete_bitmap_));
}

TEST_F(TestVectorIndexSnapMeta, rowkey_classification)
{
  // _meta_data key (root)
  ObString meta_key("!500010_meta_data");
  ASSERT_TRUE(meta_key.suffix_match("_meta_data"));
  ASSERT_FALSE(share::ObVectorIndexSegmentMeta::is_vector_index_snap_meta_key(meta_key));

  // One fixed child row per tablet.
  ObString snap_key("!500010_meta_data_snap");
  ASSERT_FALSE(snap_key.suffix_match("_meta_data"));
  ASSERT_TRUE(share::ObVectorIndexSegmentMeta::is_vector_index_snap_meta_key(snap_key));

  // segment data key
  ObString seg_key("500010_12345_hnsw_data_part00000");
  ASSERT_FALSE(seg_key.suffix_match("_meta_data"));
  ASSERT_FALSE(share::ObVectorIndexSegmentMeta::is_vector_index_snap_meta_key(seg_key));

  // edge case: empty key
  ObString empty_key;
  ASSERT_FALSE(empty_key.suffix_match("_meta_data"));
  ASSERT_FALSE(share::ObVectorIndexSegmentMeta::is_vector_index_snap_meta_key(empty_key));

  ObString malformed_key("!tablet_meta_data_snap");
  ASSERT_FALSE(share::ObVectorIndexSegmentMeta::is_vector_index_snap_meta_key(malformed_key));
}

TEST_F(TestVectorIndexSnapMeta, get_snap_meta_key)
{
  ObString key;
  ObTabletID tablet_id(500010);
  ASSERT_EQ(OB_SUCCESS,
      share::ObVectorIndexSegmentMeta::get_vector_index_snap_meta_key(allocator_, tablet_id, key));
  ASSERT_EQ(0, key.compare("!500010_meta_data_snap"));
  ASSERT_TRUE(key.prefix_match("!"));
  ASSERT_TRUE(share::ObVectorIndexSegmentMeta::is_vector_index_snap_meta_key(key));

  // The child-row key is stable across base merges.
  ObString key2;
  ASSERT_EQ(OB_SUCCESS,
      share::ObVectorIndexSegmentMeta::get_vector_index_snap_meta_key(allocator_, tablet_id, key2));
  ASSERT_EQ(0, key.compare(key2));
}

TEST_F(TestVectorIndexSnapMeta, key_ordering)
{
  // This guards the generated key shape; task 2.4 verifies the real No.5 collation.
  ObString meta_key("!500010_meta_data");
  ObString snap_key("!500010_meta_data_snap");
  ObString seg_key("500010_12345_hnsw_data_part00000");

  ASSERT_LT(meta_key.compare(snap_key), 0);
  ASSERT_LT(snap_key.compare(seg_key), 0);
}

TEST_F(TestVectorIndexSnapMeta, snap_meta_header_validation)
{
  share::ObVectorIndexSnapMetaHeader header;
  ASSERT_TRUE(header.is_valid());
  ASSERT_EQ(sizeof(int64_t), share::ObVectorIndexSnapMetaHeader::HEAD_LEN);

  header.version_ = 2;
  ASSERT_FALSE(header.is_valid());
  header.version_ = share::ObVectorIndexSnapMetaHeader::CURRENT_VERSION;
  header.layout_type_ = static_cast<uint64_t>(share::ObVectorIndexSnapMetaLayoutType::INVALID);
  ASSERT_FALSE(header.is_valid());
  header.layout_type_ = static_cast<uint64_t>(share::ObVectorIndexSnapMetaLayoutType::INLINE_SINGLE);
  header.entry_count_ = 2;
  ASSERT_FALSE(header.is_valid());
  header.entry_count_ = share::ObVectorIndexSnapMetaHeader::CURRENT_ENTRY_COUNT;
  header.reserved_ = 1;
  ASSERT_FALSE(header.is_valid());
}

TEST_F(TestVectorIndexSnapMeta, root_meta_roundtrip_without_snap_meta_ref)
{
  share::ObVectorIndexMeta src;
  // A committed root meta may contain no segment descriptors, but its SCN is valid.
  src.header_.scn_ = 100;
  src.set_persistent();

  ObString serde_data;
  ASSERT_EQ(OB_SUCCESS, src.serialize(allocator_, serde_data));
  const char *data = serde_data.ptr() + sizeof(ObLobCommon);
  const int64_t data_len = serde_data.length() - sizeof(ObLobCommon);
  share::ObVectorIndexMeta dst;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(data, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_EQ(100, dst.header_.scn_);
  ASSERT_FALSE(dst.has_snap_meta_ref());
  ASSERT_EQ(0, dst.base_segment_count());
  ASSERT_EQ(0, dst.incr_segment_count());
}

TEST_F(TestVectorIndexSnapMeta, zero_scn_meta_can_be_deserialized_but_is_invalid)
{
  share::ObVectorIndexMeta src;
  src.set_persistent();

  ObString serde_data;
  ASSERT_EQ(OB_SUCCESS, src.serialize(allocator_, serde_data));
  const char *data = serde_data.ptr() + sizeof(ObLobCommon);
  const int64_t data_len = serde_data.length() - sizeof(ObLobCommon);
  share::ObVectorIndexMeta dst;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(data, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_FALSE(dst.is_valid());
}

TEST_F(TestVectorIndexSnapMeta, max_base_scn)
{
  share::ObVectorIndexMeta meta;
  ASSERT_EQ(0, meta.max_base_scn());

  share::ObVectorIndexSegmentMeta base_meta;
  base_meta.scn_ = 100;
  ASSERT_EQ(OB_SUCCESS, meta.bases_.push_back(base_meta));
  base_meta.scn_ = 300;
  ASSERT_EQ(OB_SUCCESS, meta.bases_.push_back(base_meta));
  base_meta.scn_ = 200;
  ASSERT_EQ(OB_SUCCESS, meta.bases_.push_back(base_meta));

  ASSERT_EQ(300, meta.max_base_scn());
}

TEST_F(TestVectorIndexSnapMeta, root_meta_roundtrip_with_snap_meta_ref)
{
  share::ObVectorIndexMeta src;
  src.header_.scn_ = 200;
  src.set_persistent();
  src.set_snap_meta_ref();
  ASSERT_TRUE(src.has_snap_meta_ref());

  ObString serde_data;
  ASSERT_EQ(OB_SUCCESS, src.serialize(allocator_, serde_data));
  const char *data = serde_data.ptr() + sizeof(ObLobCommon);
  const int64_t data_len = serde_data.length() - sizeof(ObLobCommon);
  share::ObVectorIndexMeta dst;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(data, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ASSERT_TRUE(dst.has_snap_meta_ref());
  ASSERT_EQ(200, dst.header_.scn_);

  src.clear_snap_meta_ref();
  ASSERT_FALSE(src.has_snap_meta_ref());
  ObString serde_data_without_ref;
  ASSERT_EQ(OB_SUCCESS, src.serialize(allocator_, serde_data_without_ref));
  ASSERT_EQ(serde_data.length(), serde_data_without_ref.length());

  share::ObVectorIndexMeta without_ref;
  int64_t without_ref_pos = 0;
  const char *without_ref_data = serde_data_without_ref.ptr() + sizeof(ObLobCommon);
  const int64_t without_ref_len = serde_data_without_ref.length() - sizeof(ObLobCommon);
  ASSERT_EQ(OB_SUCCESS, without_ref.deserialize(without_ref_data, without_ref_len, without_ref_pos));
  ASSERT_EQ(without_ref_len, without_ref_pos);
  ASSERT_FALSE(without_ref.has_snap_meta_ref());
}

// Keep the 4.6.3.0 segment wire fields frozen here: using the current segment
// decoder would not test whether an old reader skips bitmap_scn_ correctly.
struct LegacyVectorSegmentMeta : public share::ObVectorIndexSegmentMeta
{
  OB_UNIS_VERSION(1);
};
OB_SERIALIZE_MEMBER(LegacyVectorSegmentMeta, seg_type_, index_type_, flags_, scn_,
                    start_key_, end_key_, blocks_cnt_, index_param_);

TEST_F(TestVectorIndexSnapMeta, root_v1_preserves_legacy_rpc_layout)
{
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  share::ObVectorIndexMeta root;
  root.header_.scn_ = 42;
  root.set_persistent();
  share::ObVectorIndexSegmentMeta segment;
  segment.seg_type_ = share::ObVectorIndexSegmentType::BASE;
  segment.index_type_ = share::ObVectorIndexAlgorithmType::VIAT_IPIVF_SQ;
  segment.bitmap_scn_ = 42;
  segment.scn_ = 10;
  ASSERT_EQ(OB_SUCCESS, segment.deep_copy_seg_key(ObString("start"), ObString("end")));
  ASSERT_EQ(OB_SUCCESS, root.add_base_seg_meta(segment));
  segment.seg_type_ = share::ObVectorIndexSegmentType::FREEZE_PERSIST;
  segment.index_type_ = share::ObVectorIndexAlgorithmType::VIAT_IPIVF;
  segment.scn_ = 20;
  ASSERT_EQ(OB_SUCCESS, root.add_incr_seg_meta(segment));
  char buf[4096] = {};
  const int64_t start = 7;
  int64_t pos = start;
  ASSERT_EQ(OB_SUCCESS, root.serialize(buf, sizeof(buf), pos));
  const int64_t root_end = pos;
  EXPECT_EQ(root.get_serialize_size(), root_end - start);
  share::SCN boundary;
  ASSERT_EQ(OB_SUCCESS, boundary.convert_for_tx(123456));
  ASSERT_EQ(OB_SUCCESS, boundary.serialize(buf, sizeof(buf), pos));

  // An old reader knows only the header, flags and segment arrays.
  share::ObVectorIndexMeta legacy;
  MEMCPY(&legacy.header_, buf + start, share::ObVectorIndexMetaHeader::HEAD_LEN);
  ASSERT_EQ(static_cast<int64_t>(share::ObVectorIndexMetaHeader::VERSION_V1), legacy.header_.version_);
  int64_t read_pos = start + share::ObVectorIndexMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, read_pos, legacy.flags_));
  for (const auto expected_type : {share::ObVectorIndexAlgorithmType::VIAT_IPIVF_SQ,
                                   share::ObVectorIndexAlgorithmType::VIAT_IPIVF}) {
    int64_t count = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, read_pos, count));
    ASSERT_EQ(1, count);
    LegacyVectorSegmentMeta old_segment;
    ASSERT_EQ(OB_SUCCESS, old_segment.deserialize(buf, pos, read_pos));
    EXPECT_EQ(expected_type, old_segment.index_type_);
    EXPECT_EQ(ObString("start"), old_segment.start_key_);
    EXPECT_EQ(ObString("end"), old_segment.end_key_);
    EXPECT_EQ(0, old_segment.bitmap_scn_);
    if (expected_type == share::ObVectorIndexAlgorithmType::VIAT_IPIVF_SQ) {
      ASSERT_EQ(OB_SUCCESS, legacy.add_base_seg_meta(old_segment));
    } else {
      ASSERT_EQ(OB_SUCCESS, legacy.add_incr_seg_meta(old_segment));
    }
  }
  ASSERT_EQ(root_end, read_pos);
  EXPECT_EQ(1, legacy.base_segment_count());
  EXPECT_EQ(1, legacy.incr_segment_count());
  share::SCN received;
  ASSERT_EQ(OB_SUCCESS, received.deserialize(buf, pos, read_pos));
  EXPECT_EQ(boundary, received);
  EXPECT_EQ(pos, read_pos);
}

TEST_F(TestVectorIndexSnapMeta, root_data_version_gate_and_legacy_reload)
{
  for (const uint64_t data_version : {DATA_VERSION_4_6_2_1, DATA_VERSION_4_6_3_0,
                                    DATA_VERSION_4_6_3_1}) {
    ODV_MGR.set_mock_data_version(data_version);
    share::ObVectorIndexMeta root;
    root.header_.scn_ = 42;
    root.set_persistent();
    ObString data;
    ASSERT_EQ(OB_SUCCESS, root.serialize(allocator_, data));
    const int64_t wire_size = data.length() - sizeof(ObLobCommon);
    EXPECT_EQ(root.get_serialize_size(), wire_size);
    // A new reader must decode the wire version even after compatible advances.
    ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_1);
    share::ObVectorIndexMeta decoded;
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(data.ptr() + sizeof(ObLobCommon), wire_size, pos));
    EXPECT_EQ(wire_size, pos);
    const int64_t expected_version = data_version < DATA_VERSION_4_6_3_1
        ? share::ObVectorIndexMetaHeader::VERSION_V1 : share::ObVectorIndexMetaHeader::VERSION_V2;
    EXPECT_EQ(expected_version, decoded.header_.version_);
    EXPECT_EQ(0, decoded.last_bitmap_update_scn_);
    // Rewriting legacy metadata after upgrade promotes the wire format to V2.
    ObString rewritten;
    ASSERT_EQ(OB_SUCCESS, decoded.serialize(allocator_, rewritten));
    share::ObVectorIndexMeta promoted;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, promoted.deserialize(rewritten.ptr() + sizeof(ObLobCommon),
        rewritten.length() - sizeof(ObLobCommon), pos));
    EXPECT_EQ(static_cast<int64_t>(share::ObVectorIndexMetaHeader::VERSION_V2), promoted.header_.version_);
  }
}

TEST_F(TestVectorIndexSnapMeta, root_version_lookup_failure_falls_back_to_v1)
{
  // This standalone test process has no initialized tenant data-version manager.
  ODV_MGR.set_mock_data_version(0);
  uint64_t data_version = 0;
  ASSERT_NE(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  for (const int64_t bitmap_scn : {0, 99}) {
    share::ObVectorIndexMeta root;
    root.header_.scn_ = 42;
    root.last_bitmap_update_scn_ = bitmap_scn;
    root.set_persistent();
    char buf[256] = {};
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, root.serialize(buf, sizeof(buf), pos));
    share::ObVectorIndexMeta decoded;
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
    EXPECT_EQ(static_cast<int64_t>(share::ObVectorIndexMetaHeader::VERSION_V1), decoded.header_.version_);
    // The chosen fallback is wire-compatible with V1, not a lossless V2 rewrite.
    EXPECT_EQ(0, decoded.last_bitmap_update_scn_);
    EXPECT_EQ(pos, read_pos);
  }
}

TEST_F(TestVectorIndexSnapMeta, root_upgrade_between_size_and_encode_requires_retry)
{
  share::ObVectorIndexMeta root;
  root.header_.scn_ = 42;
  root.set_persistent();
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_0);
  const int64_t old_size = root.get_serialize_size();
  char buf[256] = {};
  ODV_MGR.set_mock_data_version(DATA_VERSION_4_6_3_1);
  int64_t pos = 0;
  EXPECT_NE(OB_SUCCESS, root.serialize(buf, old_size, pos));
  // The failed encoding is not a valid result; retry with the new size.
  pos = 0;
  const int64_t new_size = root.get_serialize_size();
  ASSERT_GT(new_size, old_size);
  ASSERT_EQ(OB_SUCCESS, root.serialize(buf, new_size, pos));
  EXPECT_EQ(new_size, pos);
  share::ObVectorIndexMeta decoded;
  int64_t read_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
  EXPECT_EQ(static_cast<int64_t>(share::ObVectorIndexMetaHeader::VERSION_V2), decoded.header_.version_);
}

TEST_F(TestVectorIndexSnapMeta, root_v2_preserves_rpc_scn_at_nonzero_offset)
{
  for (const int64_t bitmap_scn : {99, 0}) {
    share::ObVectorIndexMeta root;
    root.header_.scn_ = 42;
    root.set_persistent();
    root.last_bitmap_update_scn_ = bitmap_scn;
    if (bitmap_scn == 0) {
      root.header_.version_ = share::ObVectorIndexMetaHeader::VERSION_V2;
    }
    char buf[256] = {};
    const int64_t start = 7;
    int64_t pos = start;
    ASSERT_EQ(OB_SUCCESS, root.serialize(buf, sizeof(buf), pos));
    const int64_t root_end = pos;
    EXPECT_EQ(root.get_serialize_size(), root_end - start);
    share::SCN boundary;
    ASSERT_EQ(OB_SUCCESS, boundary.convert_for_tx(123456));
    ASSERT_EQ(OB_SUCCESS, boundary.serialize(buf, sizeof(buf), pos));

    share::ObVectorIndexMeta decoded;
    int64_t read_pos = start;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
    EXPECT_EQ(static_cast<int64_t>(share::ObVectorIndexMetaHeader::VERSION_V2), decoded.header_.version_);
    EXPECT_EQ(bitmap_scn, decoded.last_bitmap_update_scn_);
    EXPECT_EQ(root_end, read_pos);
    share::SCN received;
    ASSERT_EQ(OB_SUCCESS, received.deserialize(buf, pos, read_pos));
    EXPECT_EQ(boundary, received);
    EXPECT_EQ(pos, read_pos);
  }
}

TEST_F(TestVectorIndexSnapMeta, migration_rpc_root_data_version_compatibility)
{
  using namespace share;
  for (const uint64_t data_version : {DATA_VERSION_4_6_3_0, DATA_VERSION_4_6_3_1}) {
    ODV_MGR.set_mock_data_version(data_version);
    uint64_t actual_data_version = 0;
    ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), actual_data_version));
    ASSERT_EQ(data_version, actual_data_version);
    obrpc::ObFetchVectorIndexSegmentMetasRes sent;
    sent.adaptor_handle_id_ = 7;
    sent.vec_index_meta_.header_.scn_ = 42;
    sent.vec_index_meta_.set_persistent();
    ObVectorIndexSegmentMeta segment;
    segment.seg_type_ = ObVectorIndexSegmentType::BASE;
    segment.scn_ = 10;
    ASSERT_EQ(OB_SUCCESS, segment.deep_copy_seg_key(ObString("start"), ObString("end")));
    ASSERT_EQ(OB_SUCCESS, sent.vec_index_meta_.add_base_seg_meta(segment));
    segment.seg_type_ = ObVectorIndexSegmentType::FREEZE_PERSIST;
    segment.scn_ = 20;
    ASSERT_EQ(OB_SUCCESS, sent.vec_index_meta_.add_incr_seg_meta(segment));
    ASSERT_EQ(OB_SUCCESS, sent.vec_index_right_boundary_scn_.convert_for_tx(123456));
    std::vector<char> buf(sent.get_serialize_size());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, sent.serialize(buf.data(), buf.size(), pos));
    EXPECT_EQ(buf.size(), pos);

    obrpc::ObFetchVectorIndexSegmentMetasRes received;
    int64_t read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, received.deserialize(buf.data(), pos, read_pos));
    EXPECT_EQ(pos, read_pos);
    EXPECT_EQ(sent.vec_index_right_boundary_scn_, received.vec_index_right_boundary_scn_);
    EXPECT_EQ(2, received.vec_index_meta_.segment_count());
    const int64_t expected_version = data_version < DATA_VERSION_4_6_3_1
        ? ObVectorIndexMetaHeader::VERSION_V1 : ObVectorIndexMetaHeader::VERSION_V2;
    EXPECT_EQ(expected_version, received.vec_index_meta_.header_.version_);

    // Decode the real response envelope and then follow the legacy root decoder.
    int64_t unis_version = 0;
    int64_t body_len = 0;
    int64_t handle_id = 0;
    read_pos = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf.data(), pos, read_pos, unis_version));
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf.data(), pos, read_pos, body_len));
    EXPECT_EQ(pos - read_pos, body_len);
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf.data(), pos, read_pos, handle_id));
    EXPECT_EQ(sent.adaptor_handle_id_, handle_id);
    ObVectorIndexMeta legacy;
    MEMCPY(&legacy.header_, buf.data() + read_pos, ObVectorIndexMetaHeader::HEAD_LEN);
    read_pos += ObVectorIndexMetaHeader::HEAD_LEN;
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf.data(), pos, read_pos, legacy.flags_));
    ASSERT_EQ(OB_SUCCESS, legacy.deserialize_seg_metas(buf.data(), pos, read_pos));
    SCN legacy_boundary;
    const int ret = legacy_boundary.deserialize(buf.data(), pos, read_pos);
    if (data_version < DATA_VERSION_4_6_3_1) {
      ASSERT_EQ(OB_SUCCESS, ret);
      EXPECT_EQ(sent.vec_index_right_boundary_scn_, legacy_boundary);
      EXPECT_EQ(pos, read_pos);
    } else {
      EXPECT_TRUE(OB_SUCCESS != ret || legacy_boundary != sent.vec_index_right_boundary_scn_);
    }
  }
}

TEST_F(TestVectorIndexSnapMeta, root_v2_requires_a_v2_capable_receiver)
{
  share::ObVectorIndexMeta root;
  root.header_.scn_ = 42;
  root.set_persistent();
  char buf[256] = {};
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, root.serialize(buf, sizeof(buf), pos));
  const int64_t root_end = pos;
  share::SCN boundary;
  ASSERT_EQ(OB_SUCCESS, boundary.convert_for_tx(123456));
  ASSERT_EQ(OB_SUCCESS, boundary.serialize(buf, sizeof(buf), pos));

  // A V1 decoder does not consume the new root tail, even with an outer UNIS envelope.
  // Same-data-version builds without V2 support cannot be protected by the gate.
  share::ObVectorIndexMeta legacy;
  int64_t read_pos = share::ObVectorIndexMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, read_pos, legacy.flags_));
  ASSERT_EQ(OB_SUCCESS, legacy.deserialize_seg_metas(buf, pos, read_pos));
  ASSERT_LT(read_pos, root_end);
  share::SCN received;
  const int ret = received.deserialize(buf, pos, read_pos);
  EXPECT_TRUE(OB_SUCCESS != ret || received != boundary);
}

TEST_F(TestVectorIndexSnapMeta, root_v2_skips_future_tail_before_rpc_scn)
{
  share::ObVectorIndexMeta root;
  root.header_.scn_ = 42;
  root.set_persistent();
  root.last_bitmap_update_scn_ = 99;
  char buf[256] = {};
  const int64_t start = 7;
  int64_t pos = start;
  ASSERT_EQ(OB_SUCCESS, root.serialize(buf, sizeof(buf), pos));
  // Locate the length field through the stable legacy prefix.
  share::ObVectorIndexMeta prefix;
  int64_t length_pos = start + share::ObVectorIndexMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, length_pos, prefix.flags_));
  ASSERT_EQ(OB_SUCCESS, prefix.deserialize_seg_metas(buf, pos, length_pos));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(buf, sizeof(buf), pos, 987654321));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_bool(buf, sizeof(buf), pos, true));
  const int64_t root_end = pos;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, sizeof(buf), length_pos, root_end - start));
  share::SCN boundary;
  ASSERT_EQ(OB_SUCCESS, boundary.convert_for_tx(123456));
  ASSERT_EQ(OB_SUCCESS, boundary.serialize(buf, sizeof(buf), pos));

  share::ObVectorIndexMeta decoded;
  int64_t read_pos = start;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
  EXPECT_EQ(99, decoded.last_bitmap_update_scn_);
  EXPECT_EQ(root_end, read_pos);
  share::SCN received;
  ASSERT_EQ(OB_SUCCESS, received.deserialize(buf, pos, read_pos));
  EXPECT_EQ(boundary, received);
  EXPECT_EQ(pos, read_pos);
}

TEST_F(TestVectorIndexSnapMeta, root_v2_rejects_invalid_length_and_truncated_scn)
{
  share::ObVectorIndexMeta root;
  root.header_.scn_ = 42;
  root.set_persistent();
  root.last_bitmap_update_scn_ = 99;
  char buf[256] = {};
  const int64_t start = 7;
  int64_t pos = start;
  ASSERT_EQ(OB_SUCCESS, root.serialize(buf, sizeof(buf), pos));
  share::ObVectorIndexMeta prefix;
  int64_t length_pos = start + share::ObVectorIndexMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, length_pos, prefix.flags_));
  ASSERT_EQ(OB_SUCCESS, prefix.deserialize_seg_metas(buf, pos, length_pos));
  const int64_t size = pos - start;
  // Negative length, length before its own end, beyond the buffer, and no SCN.
  const int64_t bad_sizes[] = {-1, length_pos - start, size + 1, size - 1};
  for (const int64_t bad_size : bad_sizes) {
    int64_t write_pos = length_pos;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, sizeof(buf), write_pos, bad_size));
    share::ObVectorIndexMeta decoded;
    int64_t read_pos = start;
    EXPECT_NE(OB_SUCCESS, decoded.deserialize(buf, pos, read_pos));
  }
  ASSERT_EQ(OB_SUCCESS, serialization::encode_i64(buf, sizeof(buf), length_pos, size));
  share::ObVectorIndexMeta decoded;
  int64_t read_pos = start;
  EXPECT_NE(OB_SUCCESS, decoded.deserialize(buf, pos - 1, read_pos));
}

} // namespace common
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  system("rm -f test_vector_index_snap_meta.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_vector_index_snap_meta.log", true, true);
  return RUN_ALL_TESTS();
}
