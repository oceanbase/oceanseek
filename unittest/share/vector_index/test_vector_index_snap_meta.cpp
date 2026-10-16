/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#define private public
#define protected public
#include "share/vector_index/ob_vector_index_segment.h"
#undef private
#undef protected
#include "lib/allocator/page_arena.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"

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
  }
  void TearDown() override {}

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
    } else if (OB_ISNULL(segment->vbitmap_ = OB_NEWx(
        share::ObVectorIndexRoaringBitMap, &allocator_, &allocator_, TEST_TENANT_ID))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if (OB_FAIL(segment->vbitmap_->init(true, true))) {
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
        + meta.get_serialize_size() + segment->get_serialize_meta_size();
    char *buf = static_cast<char *>(allocator_.alloc(size));
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(buf, reinterpret_cast<const char *>(&header),
          share::ObVectorIndexSnapMetaHeader::HEAD_LEN);
      pos += share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
      if (OB_FAIL(meta.serialize(buf, size, pos))) {
      } else if (OB_FAIL(segment->serialize_meta(buf, size, pos))) {
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

TEST_F(TestVectorIndexSnapMeta, serialize_deserialize_roundtrip)
{
  share::ObVectorIndexSegmentMeta src_meta;
  share::ObVectorIndexSegmentHandle src_segment;
  ASSERT_EQ(OB_SUCCESS, build_bridge(100, src_meta, src_segment));
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->insert_bitmap_, 1);
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->insert_bitmap_, 5);
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->insert_bitmap_, 100);
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->delete_bitmap_, 3);
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->delete_bitmap_, 7);

  // serialize
  const share::ObVectorIndexSnapMetaHeader src_header;
  int64_t ser_size = share::ObVectorIndexSnapMetaHeader::HEAD_LEN
      + src_meta.get_serialize_size() + src_segment->get_serialize_meta_size();
  ASSERT_GT(ser_size, 0);
  char *buf = static_cast<char*>(allocator_.alloc(ser_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  MEMCPY(buf + pos, reinterpret_cast<const char*>(&src_header),
      share::ObVectorIndexSnapMetaHeader::HEAD_LEN);
  pos += share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, src_meta.serialize(buf, ser_size, pos));
  ASSERT_EQ(OB_SUCCESS, src_segment->serialize_meta(buf, ser_size, pos));
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
  ASSERT_EQ(OB_SUCCESS, dst_segment->deserialize_meta(buf, ser_size, dpos));
  ASSERT_EQ(dpos, ser_size);

  // A bridge has no data-block key range, unlike a regular segment meta.
  ASSERT_EQ(dst_meta.scn_, 100);
  ASSERT_TRUE(dst_meta.start_key_.empty());
  ASSERT_TRUE(dst_meta.end_key_.empty());

  // verify bitmap content
  ASSERT_NE(nullptr, dst_segment->vbitmap_->insert_bitmap_);
  ASSERT_NE(nullptr, dst_segment->vbitmap_->delete_bitmap_);
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_segment->vbitmap_->insert_bitmap_, 1));
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_segment->vbitmap_->insert_bitmap_, 5));
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_segment->vbitmap_->insert_bitmap_, 100));
  ASSERT_FALSE(roaring::api::roaring64_bitmap_contains(dst_segment->vbitmap_->insert_bitmap_, 2));
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_segment->vbitmap_->delete_bitmap_, 3));
  ASSERT_TRUE(roaring::api::roaring64_bitmap_contains(dst_segment->vbitmap_->delete_bitmap_, 7));
  ASSERT_FALSE(roaring::api::roaring64_bitmap_contains(dst_segment->vbitmap_->delete_bitmap_, 1));

  ASSERT_EQ(roaring::api::roaring64_bitmap_get_cardinality(dst_segment->vbitmap_->insert_bitmap_), 3);
  ASSERT_EQ(roaring::api::roaring64_bitmap_get_cardinality(dst_segment->vbitmap_->delete_bitmap_), 2);
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
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->insert_bitmap_, 1);

  const share::ObVectorIndexSnapMetaHeader src_header;
  const int64_t ser_size = share::ObVectorIndexSnapMetaHeader::HEAD_LEN
      + src_meta.get_serialize_size() + src_segment->get_serialize_meta_size();
  char *buf = static_cast<char*>(allocator_.alloc(ser_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  MEMCPY(buf + pos, reinterpret_cast<const char*>(&src_header),
      share::ObVectorIndexSnapMetaHeader::HEAD_LEN);
  pos += share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, src_meta.serialize(buf, ser_size, pos));
  ASSERT_EQ(OB_SUCCESS, src_segment->serialize_meta(buf, ser_size, pos));

  share::ObVectorIndexSegmentMeta dst_meta;
  int64_t dpos = share::ObVectorIndexSnapMetaHeader::HEAD_LEN;
  ASSERT_EQ(OB_SUCCESS, dst_meta.deserialize(buf, ser_size - 1, dpos));
  share::ObVectorIndexSegmentHandle dst_segment;
  ASSERT_EQ(OB_SUCCESS, share::ObVectorIndexSegment::create_bitmap_only(dst_segment, TEST_TENANT_ID, allocator_));
  ASSERT_NE(OB_SUCCESS, dst_segment->deserialize_meta(buf, ser_size - 1, dpos));
}

TEST_F(TestVectorIndexSnapMeta, reject_invalid_bridge_envelope)
{
  share::ObVectorIndexSegmentMeta src_meta;
  share::ObVectorIndexSegmentHandle src_segment;
  ASSERT_EQ(OB_SUCCESS, build_bridge(100, src_meta, src_segment));
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->insert_bitmap_, 1);

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
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->insert_bitmap_, 1);
  roaring::api::roaring64_bitmap_add(src_segment->vbitmap_->delete_bitmap_, 2);

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
      src_segment->vbitmap_->insert_bitmap_));
  ASSERT_EQ(0, roaring::api::roaring64_bitmap_get_cardinality(
      src_segment->vbitmap_->delete_bitmap_));

  const int64_t ser_size = src_segment->get_serialize_meta_size();
  char *buf = static_cast<char *>(allocator_.alloc(ser_size));
  ASSERT_NE(nullptr, buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src_segment->serialize_meta(buf, ser_size, pos));
  ASSERT_EQ(ser_size, pos);

  share::ObVectorIndexSegmentHandle dst_segment;
  ASSERT_EQ(OB_SUCCESS,
      share::ObVectorIndexSegment::create_bitmap_only(
          dst_segment, TEST_TENANT_ID, allocator_));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst_segment->deserialize_meta(buf, ser_size, pos));
  ASSERT_EQ(ser_size, pos);
  ASSERT_NE(nullptr, dst_segment->vbitmap_);
  ASSERT_NE(nullptr, dst_segment->vbitmap_->insert_bitmap_);
  ASSERT_NE(nullptr, dst_segment->vbitmap_->delete_bitmap_);
  ASSERT_EQ(0, roaring::api::roaring64_bitmap_get_cardinality(
      dst_segment->vbitmap_->insert_bitmap_));
  ASSERT_EQ(0, roaring::api::roaring64_bitmap_get_cardinality(
      dst_segment->vbitmap_->delete_bitmap_));
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
