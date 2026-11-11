/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_STRUCT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_STRUCT_H_

#include <cstdint>
#include <cstddef>
#include <type_traits>
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace blocksstable
{

// IVF SSTable row type column. Disk rows are ordered by (type, cid, vid).
enum ObIvfSSTableRowkeyCol
{
  IVF_ROWKEY_COL_TYPE = 0,
  IVF_ROWKEY_COL_CID = 1,
  IVF_ROWKEY_COL_VID = 2,
  IVF_ROWKEY_COL_COUNT,
};

enum ObIvfSSTableRowType
{
  IVF_ROW_TYPE_INVALID = 0,
  IVF_ROW_TYPE_CENTROID = 1,
  IVF_ROW_TYPE_CODEBOOK = 2,
  IVF_ROW_TYPE_LOW_PRECISION_VECTOR = 3,
  IVF_ROW_TYPE_HIGH_PRECISION_VECTOR = 4,
  IVF_ROW_TYPE_RAW_VECTOR = 5,
  IVF_ROW_TYPE_MAX = 6,
};

static constexpr int64_t IVF_RAW_VECTOR_CID = 0;
static constexpr int32_t IVF_SSTABLE_FORMAT_VERSION = 1;
static constexpr int64_t IVF_CENTROID_HIERARCHY_THRESHOLD = 300000;
static constexpr int64_t IVF_PARENT_CLUSTER_SIZE = 384;

enum ObIvfCentroidLayoutType : int32_t
{
  IVF_CENTROID_FLAT = 0,
  IVF_CENTROID_HIERARCHICAL = 1,
};

// Fixed on-disk prefix, followed by FP32 coordinates. Like the other storage
// headers, fields use the supported platforms' little-endian representation.
// Counts describe direct children: center rows for parents, postings for leaves.
struct ObIvfCentroidPayloadHeader final
{
  static constexpr uint16_t VERSION = 1;
  explicit ObIvfCentroidPayloadHeader(uint32_t child_count = 0)
    : version_(VERSION), header_size_(sizeof(ObIvfCentroidPayloadHeader)), child_count_(child_count)
  {}
  uint16_t version_;
  uint16_t header_size_;
  uint32_t child_count_;
};

static_assert(std::is_standard_layout<ObIvfCentroidPayloadHeader>::value
    && std::is_trivially_copyable<ObIvfCentroidPayloadHeader>::value,
    "centroid payload header must have a copyable fixed layout");
static_assert(sizeof(ObIvfCentroidPayloadHeader) == 8, "centroid payload header must be 8 bytes");
static_assert(offsetof(ObIvfCentroidPayloadHeader, version_) == 0, "unexpected payload version offset");
static_assert(offsetof(ObIvfCentroidPayloadHeader, header_size_) == 2, "unexpected payload size offset");
static_assert(offsetof(ObIvfCentroidPayloadHeader, child_count_) == 4, "unexpected child count offset");

struct ObIvfCentroidPayload final
{
  static int64_t get_size(const int64_t dim)
  { return dim * static_cast<int64_t>(sizeof(float)) + sizeof(ObIvfCentroidPayloadHeader); }
  // Check before narrowing an external row count or accumulating one leaf's
  // postings. On error the existing counter remains unchanged.
  static int add_child_count(uint32_t &count, int64_t delta);
  static int encode(char *buf, int64_t size, const float *vector,
      int64_t dim, uint32_t child_count);
  static int decode(const common::ObString &payload, int64_t dim,
      const float *&vector, uint32_t &child_count);
};

struct ObIvfFastScanTransformContext final
{
  ObIvfFastScanTransformContext() : dim_(0), format_version_(0) {}
  ObIvfFastScanTransformContext(const int32_t dim, const int32_t format_version)
      : dim_(dim), format_version_(format_version) {}
  bool is_valid() const
  {
    return dim_ > 0 && IVF_SSTABLE_FORMAT_VERSION == format_version_;
  }
  TO_STRING_KV(K_(dim), K_(format_version));

  int32_t dim_;
  int32_t format_version_;
};

// IVF SSTable 扩展元数据。
// 不在本头文件包含 share/vector_index/ob_vector_index_util.h：该头会拉入 rootserver/ddl，
// 再回到 ObTabletCreateSSTableParam，形成循环包含。
// index_type_ / dist_type_ 存的是 share::ObVectorIndexAlgorithmType /
// share::ObVectorIndexDistAlgorithm 的整型值；index_type_ 只接受 VIAT_DISK_BBQ。
struct ObIvfSSTableMeta final
{
  OB_UNIS_VERSION(1);

public:
  ObIvfSSTableMeta() { reset(); }
  ~ObIvfSSTableMeta() = default;
  OB_INLINE void reset()
  {
    flags_ = 0;
    index_type_ = UINT16_MAX;
    dist_type_ = INT32_MAX;
    dim_ = 0;
    format_version_ = 0;
    nbits_ = 0;
    fht_algo_version_ = 0;
    centroid_count_ = 0;
    vector_count_ = 0;
    centroid_layout_ = IVF_CENTROID_FLAT;
    parent_centroid_count_ = 0;
  }
  OB_INLINE void set_empty()
  {
    reset();
    is_empty_ = true;
  }
  bool is_valid() const;
  OB_INLINE bool is_empty() const { return is_empty_; }
  OB_INLINE bool is_hierarchical() const { return centroid_layout_ == IVF_CENTROID_HIERARCHICAL; }
  OB_INLINE int64_t root_centroid_count() const
  { return is_hierarchical() ? parent_centroid_count_ : centroid_count_; }
  OB_INLINE int64_t leaf_cid_base() const
  { return is_hierarchical() ? parent_centroid_count_ + 1 : 1; }
  OB_INLINE int64_t leaf_storage_cid(const int64_t idx) const
  { return leaf_cid_base() + idx; }
  // Every center row stores its parent in cid and its own identity in vid.
  // Parent 0 is the virtual root for FLAT centers and hierarchical top-level parents.
  int decode_centroid_key(int64_t key_cid, int64_t key_vid,
      int64_t &self_cid, int64_t &group_id) const;
  int assign(common::ObIAllocator &allocator, const ObIvfSSTableMeta &other);

  TO_STRING_KV(K_(flags), K_(is_empty), K_(index_type), K_(dist_type), K_(dim),
      K_(format_version), K_(nbits), K_(fht_algo_version), K_(centroid_count), K_(vector_count),
      K_(centroid_layout), K_(parent_centroid_count));

public:
  union {
    int64_t flags_;
    struct {
      int64_t is_empty_ : 1;
      int64_t reserved_ : 63;
    };
  };
  uint16_t index_type_;
  int32_t dist_type_;
  int32_t dim_;
  int32_t format_version_;
  int32_t nbits_;
  int32_t fht_algo_version_;
  // Unique leaves only; excludes parent rows and SOAR center replicas.
  int64_t centroid_count_;
  int64_t vector_count_;
  int32_t centroid_layout_;
  int64_t parent_centroid_count_;
};

struct ObIvfSSTableRowDesc
{
  ObIvfSSTableRowDesc();
  void reset();
  int init(int32_t format_version);
  bool is_valid() const;
  bool is_valid_for_row(const int64_t column_count) const;
  TO_STRING_KV(K_(row_type_col_idx), K_(centroid_id_col_idx),
      K_(vector_id_col_idx), K_(trans_version_col_idx),
      K_(sql_sequence_col_idx), K_(payload_col_idx));

  int16_t row_type_col_idx_;
  int16_t centroid_id_col_idx_;
  int16_t vector_id_col_idx_;
  int16_t trans_version_col_idx_;
  int16_t sql_sequence_col_idx_;
  int16_t payload_col_idx_;
};

} // namespace blocksstable
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SSTABLE_STRUCT_H_
