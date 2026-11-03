/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_SEARCH_INDEX_GENERATOR_H_
#define OCEANBASE_SHARE_OB_SEARCH_INDEX_GENERATOR_H_

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "share/datum/ob_datum.h"
#include "share/vector/ob_i_vector.h"
#include "share/search_index/ob_search_index_encoder.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "ob_search_index_config_filter.h"

namespace oceanbase
{
namespace common
{
class ObCollectionArrayType;
}

namespace share
{

enum ObSearchIndexRowTarget { SEARCH_DATA = 0, SEARCH_TEXT_MATCH = 1, SEARCH_TEXT_PHRASE = 2 };

// Generated search-index tuple: [column_idx, path, value, doc_id] or
//                               [column_idx, path, value, doc_id, nested_key] for nested input.
// The fifth column is a rowkey in synchronous search; async search folds it into a posting value.
struct ObSearchIndexRow
{
public:
  // for json/array null
  explicit ObSearchIndexRow(uint64_t column_idx, uint64_t doc_id)
    : column_idx_(column_idx), path_(), value_(), doc_id_(doc_id),
      nested_key_(), has_nested_key_(false)
  {
    path_.set_null();
    value_.set_null();
    nested_key_.set_null();
  }

  // for scalar type, the path is null
  explicit ObSearchIndexRow(uint64_t column_idx, ObString &value, uint64_t doc_id)
    : column_idx_(column_idx), path_(), value_(value.ptr(), value.length(), false), doc_id_(doc_id),
      nested_key_(), has_nested_key_(false)
  {
    path_.set_null();
    nested_key_.set_null();
  }

  // array column null / placeholder: path = type meta (e.g. ARRAY(INT)), value null
  explicit ObSearchIndexRow(uint64_t column_idx, uint64_t doc_id, const ObString &path)
    : column_idx_(column_idx),
      path_(path.ptr(), path.length(), false),
      value_(),
      doc_id_(doc_id),
      nested_key_(), has_nested_key_(false)
  {
    value_.set_null();
    nested_key_.set_null();
  }

  explicit ObSearchIndexRow(uint64_t column_idx, ObString &path, ObString &value, uint64_t doc_id)
    : column_idx_(column_idx),
      path_(path.ptr(), path.length(), false),
      value_(value.ptr(), value.length(), false),
      doc_id_(doc_id),
      nested_key_(), has_nested_key_(false)
  {
    nested_key_.set_null();
  }

  ObSearchIndexRow() : column_idx_(OB_INVALID_ID), path_(), value_(), doc_id_(OB_INVALID_ID),
                       nested_key_(), has_nested_key_(false)
  {
    nested_key_.set_null();
  }

  TO_STRING_KV(K_(column_idx), K_(path), K_(value), K_(doc_id), K_(has_nested_key));

  int build_datum_row(common::ObIAllocator &allocator, blocksstable::ObDatumRow &datum_row);
  uint64_t column_idx_;
  ObDatum path_;
  ObDatum value_;
  uint64_t doc_id_;
  ObDatum nested_key_;     // encoded big-endian array indices; null when path is not under nested path
  bool    has_nested_key_; // true when the generated tuple includes the fifth nested-key column
};

// Key for document-level (path, value, nested_key) deduplication.
// nested_key is included so that the same (path, value) from different nested object instances
// are not incorrectly deduplicated.
struct PathValueKey
{
  common::ObString path_;
  common::ObString value_;
  common::ObString nested_key_; // empty when not a nested index
};

struct PathValueKeyHash
{
  int operator()(const PathValueKey &k, uint64_t &res) const
  {
    uint64_t h = 0;
    if (k.path_.ptr() != nullptr && k.path_.length() > 0) {
      h = common::murmurhash(k.path_.ptr(), k.path_.length(), h);
    }
    if (k.value_.ptr() != nullptr && k.value_.length() > 0) {
      h = common::murmurhash(k.value_.ptr(), k.value_.length(), h);
    }
    if (k.nested_key_.ptr() != nullptr && k.nested_key_.length() > 0) {
      h = common::murmurhash(k.nested_key_.ptr(), k.nested_key_.length(), h);
    }
    res = h;
    return common::OB_SUCCESS;
  }
};

struct PathValueKeyEqual
{
  bool operator()(const PathValueKey &a, const PathValueKey &b) const
  {
    bool bret = true;
    if (a.path_.length() != b.path_.length() || a.value_.length() != b.value_.length()
        || a.nested_key_.length() != b.nested_key_.length()) {
      bret = false;
    } else if (a.path_.length() > 0 && (a.path_.ptr() == nullptr || b.path_.ptr() == nullptr ||
         MEMCMP(a.path_.ptr(), b.path_.ptr(), a.path_.length()) != 0)) {
      bret = false;
    } else if (a.value_.length() > 0 && (a.value_.ptr() == nullptr || b.value_.ptr() == nullptr ||
         MEMCMP(a.value_.ptr(), b.value_.ptr(), a.value_.length()) != 0)) {
      bret = false;
    } else if (a.nested_key_.length() > 0 && (a.nested_key_.ptr() == nullptr || b.nested_key_.ptr() == nullptr ||
         MEMCMP(a.nested_key_.ptr(), b.nested_key_.ptr(), a.nested_key_.length()) != 0)) {
      bret = false;
    }
    return bret;
  }
};

typedef ObSEArray<ObSearchIndexRow, 16> ObSearchIndexRows;
typedef hash::ObHashSet<PathValueKey, hash::NoPthreadDefendMode,
                        PathValueKeyHash, PathValueKeyEqual> PathValueDedupSet;

// Search index row generator: generate search index rows for multiple columns
class ObSearchIndexRowGenerator {
private:
  // Generator: generate search index rows for a single column
  class Generator {
  public:
    Generator(ObIAllocator &allocator,
              ObIAllocator &inner_allocator,
              const bool has_nested_key = false)
      : allocator_(allocator),
        inner_allocator_(inner_allocator),
        column_idx_(OB_INVALID_ID),
        enc_param_(),
        obj_meta_(),
        need_normalize_encoding_(false),
        orig_cs_type_(CS_TYPE_INVALID),
        arr_type_(nullptr),
        elem_generator_(nullptr),
        config_filter_(nullptr),
        array_index_path_(),
        nested_key_stack_(OB_MALLOC_NORMAL_BLOCK_SIZE, inner_allocator_),
        has_nested_key_(has_nested_key),
        has_nested_paths_(false) {}
    virtual ~Generator() {}

    int init(const uint64_t column_idx, const ObObjMeta &obj_meta,
             const common::ObCollectionArrayType *arr_type,
             const ObSearchIndexConfigFilter *config_filter = nullptr);
    int generate_rows(const int64_t doc_id, const ObDatum& datum, ObSearchIndexRows &rows);
    int generate_rows(const ObIVector &doc_id_vec, const ObIVector &data_vec,
                      const int64_t batch_size, ObSearchIndexRows &rows);

    TO_STRING_KV(K_(column_idx), K_(enc_param), K_(obj_meta));

  private:
    int init_array_generator(const common::ObCollectionArrayType *arr_type);
    int generate_json_rows(const int64_t doc_id, const ObDatum& datum, ObSearchIndexRows &rows);
    int generate_array_rows(const int64_t doc_id, const ObDatum& datum, ObSearchIndexRows &rows);
    int generate_rows_for_array_obj(const int64_t doc_id,
                                    common::ObIArrayType *arr_obj,
                                    ObSearchIndexRows &rows);
    int generate_rows(const int64_t doc_id, ObIJsonBase *j_base,
                      ObIArray<ObSearchIndexPathEncoder::JsonPathItem> &path_items,
                      ObSearchIndexRows &rows,
                      PathValueDedupSet &dedup_set);
    int push_back_row(ObSearchIndexRows &rows, ObSearchIndexRow &row);

  private:
    ObIAllocator &allocator_;
    ObIAllocator &inner_allocator_;
    uint64_t column_idx_;
    ObEncParam enc_param_;
    ObObjMeta obj_meta_;
    // When the column's collation is not supported by encode_from_string_varlen
    // (e.g. UTF-16, UTF-16LE, latin1, or utf8mb4_unicode_ci), we normalize the
    // string to a supported charset/collation before encoding.
    bool need_normalize_encoding_;
    ObCollationType orig_cs_type_;

    // for array type
    common::ObCollectionArrayType *arr_type_;
    Generator *elem_generator_;
    const ObSearchIndexConfigFilter *config_filter_;
    common::ObString array_index_path_;
    // nested_key stack: each entry is the array index of the current nested element.
    // pushed/popped in generate_rows() when entering/leaving a J_ARRAY declared in NESTED_PATHS.
    common::ObSEArray<uint32_t, 4, common::ObIAllocator&> nested_key_stack_;
    bool has_nested_key_;   // true when the index schema has a nested_key column
    bool has_nested_paths_; // true when this column config has NESTED_PATHS
  };

public:
  explicit ObSearchIndexRowGenerator()
    : row_allocator_(common::ObMemAttr(MTL_ID(), "SearchIdxRow")),
      inner_allocator_(common::ObMemAttr(MTL_ID(), "SearchIdxGen")),
      config_filters_(inner_allocator_),
      generators_(inner_allocator_), row_projector_(inner_allocator_), row_projector_ptr_(nullptr),
      has_nested_key_(false)
      {}
  virtual ~ObSearchIndexRowGenerator() { reset(); }

  void reset();

  // true when any indexed column has NESTED_PATHS, i.e. the generated row owns the extra
  // NESTED_KEY column and the search_data table is expected to have SEARCH_INDEX_KEY_CNT_WITH_NK
  // rowkey columns.
  OB_INLINE bool has_nested_key() const { return has_nested_key_; }

  int init(const common::ObIArray<int32_t> &included_cid_idxes,
           const common::ObIArray<ObObjMeta> &included_obj_metas,
           const common::ObIArray<int64_t> &row_projector,
           const common::ObIArray<ObString> &column_comments,
           bool rowkey_only,
           const common::ObIArray<common::ObCollectionArrayType*> *arr_types);

  int generate_rows(const sql::ObChunkDatumStore::StoredRow *store_row,
                        blocksstable::ObDatumRow *&rows, int64_t& row_cnt);
  int generate_rows(const blocksstable::ObDatumRow &store_row,
                        blocksstable::ObDatumRow *&rows, int64_t& row_cnt);
  int set_row_projector(const common::ObFixedArray<int64_t, ObIAllocator> *row_projector);
private:
  // row allocator for search index rows
  common::ObArenaAllocator row_allocator_;
  // inner allocator for generators and row_projector structures
  common::ObArenaAllocator inner_allocator_;
  // config filters created in init() via placement new on inner_allocator_; explicitly
  // destructed in reset() so their matcher arenas/hashmaps are reclaimed.
  common::ObFixedArray<ObSearchIndexConfigFilter*, ObIAllocator> config_filters_;
  common::ObFixedArray<Generator*, ObIAllocator> generators_;
  common::ObFixedArray<int64_t, ObIAllocator> row_projector_;
  const common::ObFixedArray<int64_t, ObIAllocator> *row_projector_ptr_;
  bool has_nested_key_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_SEARCH_INDEX_GENERATOR_H_
