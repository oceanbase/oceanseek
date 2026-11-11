/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/vector_index/ob_ivf_sorter.h"

#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_array.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/basic/ob_temp_row_store.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "sql/engine/sort/ob_sort_vec_op_chunk.h"
#include "sql/engine/sort/ob_storage_sort_vec_impl.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

OB_SERIALIZE_MEMBER(ObIVFSortItem, row_type_, cid_, vid_, payload_);

int ObIVFSortItem::deep_copy(const ObIVFSortItem &src, char *buf,
                             const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t payload_len = src.payload_.length();
  if (OB_UNLIKELY(payload_len < 0 || pos < 0 || len - pos < payload_len
      || (payload_len > 0
          && (OB_ISNULL(buf) || OB_ISNULL(src.payload_.ptr()))))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF sort item deep copy arguments",
             K(ret), K(src), KP(buf), K(len), K(pos));
  } else {
    row_type_ = src.row_type_;
    cid_ = src.cid_;
    vid_ = src.vid_;
    if (payload_len > 0) {
      MEMCPY(buf + pos, src.payload_.ptr(), payload_len);
      payload_.assign_ptr(buf + pos, static_cast<int32_t>(payload_len));
      pos += payload_len;
    } else {
      payload_.reset();
    }
  }
  return ret;
}

namespace
{
static const int64_t IVF_SORT_BATCH_SIZE = 4096;
static const int64_t IVF_SORT_KEY_COLUMN_COUNT = 3;
static const int64_t IVF_SORT_ADDON_COLUMN_COUNT = 1;
static const int64_t IVF_SORT_ROW_TYPE_COLUMN = 0;
static const int64_t IVF_SORT_CID_COLUMN = 1;
static const int64_t IVF_SORT_VID_COLUMN = 2;
static const int64_t IVF_SORT_PAYLOAD_COLUMN = 0;

using ObIVFStorageSortRow = sql::ObSortKeyStore<true>;
using ObIVFStorageSortChunk =
    sql::ObSortVecOpChunk<ObIVFStorageSortRow, true>;

class ObIVFStorageSortCompare
{
public:
  ObIVFStorageSortCompare() : result_code_(OB_SUCCESS), row_meta_(nullptr) {}

  int init(const sql::RowMeta &row_meta)
  {
    int ret = OB_SUCCESS;
    if (IVF_SORT_KEY_COLUMN_COUNT != row_meta.col_cnt_) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      result_code_ = OB_SUCCESS;
      row_meta_ = &row_meta;
    }
    return ret;
  }

  int get_error_code() const { return result_code_; }
  void fallback_to_disable_encode_sortkey() {}

  int compare(
      const ObIVFStorageSortRow *left,
      const ObIArray<ObDatum> &right_datums)
  {
    int cmp = 0;
    if (OB_SUCCESS != result_code_) {
    } else if (OB_ISNULL(left) || OB_ISNULL(row_meta_)
        || right_datums.count() < IVF_SORT_KEY_COLUMN_COUNT) {
      result_code_ = OB_INVALID_ARGUMENT;
    } else {
      const ObDatum left_type =
          left->get_datum(*row_meta_, IVF_SORT_ROW_TYPE_COLUMN);
      const ObDatum left_cid =
          left->get_datum(*row_meta_, IVF_SORT_CID_COLUMN);
      const ObDatum left_vid =
          left->get_datum(*row_meta_, IVF_SORT_VID_COLUMN);
      const ObDatum &right_type =
          right_datums.at(IVF_SORT_ROW_TYPE_COLUMN);
      const ObDatum &right_cid = right_datums.at(IVF_SORT_CID_COLUMN);
      const ObDatum &right_vid = right_datums.at(IVF_SORT_VID_COLUMN);
      if (OB_UNLIKELY(left_type.is_null() || right_type.is_null()
          || left_cid.is_null() || right_cid.is_null()
          || left_vid.is_null() || right_vid.is_null())) {
        result_code_ = OB_INVALID_DATA;
      } else if (left_type.get_int() != right_type.get_int()) {
        cmp = left_type.get_int() < right_type.get_int() ? -1 : 1;
      } else if (left_cid.get_int() != right_cid.get_int()) {
        cmp = left_cid.get_int() < right_cid.get_int() ? -1 : 1;
      } else if (left_vid.get_uint() != right_vid.get_uint()) {
        cmp = left_vid.get_uint() < right_vid.get_uint() ? -1 : 1;
      }
    }
    return cmp;
  }

  bool operator()(
      const ObIVFStorageSortRow *left,
      const ObIArray<ObDatum> &right_datums)
  {
    return compare(left, right_datums) < 0;
  }

  bool operator()(
      const ObIArray<ObDatum> &left_datums,
      const ObIVFStorageSortRow *right)
  {
    return compare(right, left_datums) > 0;
  }

  bool operator()(
      const ObIVFStorageSortRow *left,
      const ObIVFStorageSortRow *right)
  {
    bool less = false;
    if (OB_SUCCESS != result_code_) {
    } else if (OB_ISNULL(left) || OB_ISNULL(right)
        || OB_ISNULL(row_meta_)) {
      result_code_ = OB_INVALID_ARGUMENT;
      LOG_WARN_RET(result_code_, "invalid IVF storage-sort row",
          KP(left), KP(right), KP_(row_meta));
    } else {
      const ObDatum left_type =
          left->get_datum(*row_meta_, IVF_SORT_ROW_TYPE_COLUMN);
      const ObDatum right_type =
          right->get_datum(*row_meta_, IVF_SORT_ROW_TYPE_COLUMN);
      const ObDatum left_cid =
          left->get_datum(*row_meta_, IVF_SORT_CID_COLUMN);
      const ObDatum right_cid =
          right->get_datum(*row_meta_, IVF_SORT_CID_COLUMN);
      const ObDatum left_vid =
          left->get_datum(*row_meta_, IVF_SORT_VID_COLUMN);
      const ObDatum right_vid =
          right->get_datum(*row_meta_, IVF_SORT_VID_COLUMN);
      if (OB_UNLIKELY(left_type.is_null() || right_type.is_null()
          || left_cid.is_null() || right_cid.is_null()
          || left_vid.is_null() || right_vid.is_null()
          || left_type.len_ != sizeof(int64_t)
          || right_type.len_ != sizeof(int64_t)
          || left_cid.len_ != sizeof(int64_t)
          || right_cid.len_ != sizeof(int64_t)
          || left_vid.len_ != sizeof(uint64_t)
          || right_vid.len_ != sizeof(uint64_t))) {
        result_code_ = OB_INVALID_DATA;
        LOG_WARN_RET(result_code_, "invalid IVF storage-sort key payload");
      } else if (left_type.get_int() != right_type.get_int()) {
        less = left_type.get_int() < right_type.get_int();
      } else if (left_cid.get_int() != right_cid.get_int()) {
        less = left_cid.get_int() < right_cid.get_int();
      } else {
        less = left_vid.get_uint() < right_vid.get_uint();
      }
    }
    return less;
  }

  bool operator()(
      const ObIVFStorageSortChunk *left,
      const ObIVFStorageSortChunk *right)
  {
    bool less = false;
    if (OB_SUCCESS != result_code_) {
    } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
      result_code_ = OB_INVALID_ARGUMENT;
      LOG_WARN_RET(result_code_, "invalid IVF storage-sort chunk",
          KP(left), KP(right));
    } else {
      // ObBinaryHeap is a max-heap; reverse the row comparison so its top is
      // the smallest IVF key during the external merge.
      less = operator()(right->sk_row_, left->sk_row_);
    }
    return less;
  }

private:
  int result_code_;
  const sql::RowMeta *row_meta_;
};

using ObIVFStorageSortImpl = sql::ObStorageVecSortImpl<
    ObIVFStorageSortCompare, ObIVFStorageSortRow, true>;
} // namespace

class ObIVFSorterImpl
{
public:
  explicit ObIVFSorterImpl(const uint64_t tenant_id)
    : tenant_id_(tenant_id),
      mem_context_(nullptr),
      meta_allocator_(ObMemAttr(tenant_id, "IVFSortMeta")),
      input_allocator_(ObMemAttr(tenant_id, "IVFSortInput")),
      payload_allocator_(ObMemAttr(tenant_id, "IVFSortPayload")),
      key_row_meta_(&meta_allocator_),
      addon_row_meta_(&meta_allocator_),
      compare_(),
      monitor_info_(),
      sort_impl_(nullptr),
      key_vectors_(input_allocator_),
      addon_vectors_(input_allocator_),
      row_type_vector_(nullptr),
      cid_vector_(nullptr),
      vid_vector_(nullptr),
      payload_vector_(nullptr),
      selector_(nullptr),
      batch_row_count_(0),
      output_vectors_(nullptr),
      output_addon_vectors_(nullptr),
      output_row_count_(0),
      output_row_idx_(0),
      current_item_(),
      is_inited_(false),
      is_sorted_(false)
  {}

  ~ObIVFSorterImpl() { reset(); }

  int init()
  {
    int ret = OB_SUCCESS;
    ObSEArray<sql::ColMetaInfo, IVF_SORT_KEY_COLUMN_COUNT> key_columns;
    ObSEArray<sql::ColMetaInfo, IVF_SORT_ADDON_COLUMN_COUNT> addon_columns;
    lib::ContextParam context_param;
    const int64_t tempstore_read_alignment_size =
        sql::ObTempBlockStore::get_read_alignment_size_config(tenant_id_);
    if (is_inited_) {
      ret = OB_INIT_TWICE;
    } else if (OB_INVALID_TENANT_ID == tenant_id_) {
      ret = OB_INVALID_ARGUMENT;
    }
    for (int64_t i = 0; OB_SUCC(ret)
        && i < IVF_SORT_KEY_COLUMN_COUNT; ++i) {
      if (OB_FAIL(key_columns.push_back(
          sql::ColMetaInfo(true /* is_fixed */, sizeof(int64_t))))) {
        LOG_WARN("failed to build IVF sort-key metadata", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(addon_columns.push_back(
        sql::ColMetaInfo(false /* is_fixed */, 0 /* fixed_length */)))) {
      LOG_WARN("failed to build IVF payload metadata", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(key_row_meta_.init(
        key_columns, ObIVFStorageSortRow::get_extra_size(true),
        true /* reorder_fixed_expr */, &meta_allocator_))) {
      LOG_WARN("failed to initialize IVF sort-key row metadata", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(addon_row_meta_.init(
        addon_columns, ObIVFStorageSortRow::get_extra_size(false),
        true /* reorder_fixed_expr */, &meta_allocator_))) {
      LOG_WARN("failed to initialize IVF payload row metadata", K(ret));
    } else if (OB_SUCC(ret) && OB_FAIL(compare_.init(key_row_meta_))) {
      LOG_WARN("failed to initialize IVF storage-sort comparator", K(ret));
    }

    context_param
        .set_mem_attr(ObMemAttr(
            tenant_id_, "IVFStorageSort", ObCtxIds::WORK_AREA))
        .set_properties(lib::ALLOC_THREAD_SAFE);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ROOT_CONTEXT->CREATE_CONTEXT(
                   mem_context_, context_param))) {
      LOG_WARN("failed to create IVF storage-sort memory context", K(ret));
    } else if (OB_ISNULL(mem_context_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("IVF storage-sort memory context is null", K(ret), K_(tenant_id));
    } else if (OB_ISNULL(sort_impl_ = OB_NEWx(
        ObIVFStorageSortImpl, &mem_context_->get_malloc_allocator(),
        mem_context_, monitor_info_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF storage-sort implementation",
          K(ret), K_(tenant_id));
    } else if (OB_FAIL(sort_impl_->init(
        &compare_, &key_row_meta_, &addon_row_meta_, IVF_SORT_BATCH_SIZE,
        ObMemAttr(tenant_id_, "IVFSortRows", ObCtxIds::WORK_AREA),
        NONE_COMPRESSOR, false /* enable_trunc */,
        tempstore_read_alignment_size, tenant_id_,
        false /* enable_encode_sortkey */, nullptr /* chunk_toolkit */))) {
      LOG_WARN("failed to initialize IVF storage sort", K(ret));
    } else if (OB_FAIL(prepare_input_vectors_())) {
      LOG_WARN("failed to prepare IVF storage-sort input vectors", K(ret));
    } else {
      is_inited_ = true;
    }
    return ret;
  }

  int append_row(const ObIVFBuildRow &row)
  {
    int ret = OB_SUCCESS;
    char *payload = nullptr;
    if (!is_inited_ || is_sorted_ || row.payload_.empty()
        || batch_row_count_ < 0
        || batch_row_count_ >= IVF_SORT_BATCH_SIZE) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_ISNULL(payload = static_cast<char *>(
        payload_allocator_.alloc(row.payload_.length())))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      MEMCPY(payload, row.payload_.ptr(), row.payload_.length());
      row_type_vector_->set_int(batch_row_count_, row.row_type_);
      cid_vector_->set_int(batch_row_count_, row.cid_);
      vid_vector_->set_uint(batch_row_count_, row.vid_);
      payload_vector_->set_string(batch_row_count_, payload,
          static_cast<uint32_t>(row.payload_.length()));
      selector_[batch_row_count_] =
          static_cast<uint16_t>(batch_row_count_);
      ++batch_row_count_;
      if (IVF_SORT_BATCH_SIZE == batch_row_count_
          && OB_FAIL(flush_batch_())) {
        LOG_WARN("failed to flush IVF storage-sort input batch", K(ret));
      }
    }
    return ret;
  }

  int finish()
  {
    int ret = OB_SUCCESS;
    if (!is_inited_ || is_sorted_) {
      ret = OB_STATE_NOT_MATCH;
    } else if (batch_row_count_ > 0 && OB_FAIL(flush_batch_())) {
      LOG_WARN("failed to flush final IVF storage-sort batch", K(ret));
    } else if (OB_FAIL(sort_impl_->sort())) {
      LOG_WARN("failed to sort IVF storage rows", K(ret));
    } else if (OB_SUCCESS != compare_.get_error_code()) {
      ret = compare_.get_error_code();
    } else {
      is_sorted_ = true;
      // Stored rows and output vectors belong to sort_impl_. Input staging is
      // no longer needed once the final batch has been flushed and sorted.
      release_input_buffers_();
    }
    return ret;
  }

  int get_next_item(const ObIVFSortItem *&item)
  {
    int ret = OB_SUCCESS;
    item = nullptr;
    if (!is_inited_ || !is_sorted_) {
      ret = OB_NOT_INIT;
    } else if (output_row_idx_ >= output_row_count_) {
      output_row_count_ = 0;
      output_row_idx_ = 0;
      if (OB_FAIL(sort_impl_->get_next_batch(
          output_row_count_, output_vectors_, output_addon_vectors_))) {
        LOG_WARN("failed to get next IVF storage-sort batch", K(ret));
      } else if (output_row_count_ <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("IVF storage sorter returned an empty batch", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_ISNULL(output_vectors_) || OB_ISNULL(output_addon_vectors_)
          || output_vectors_->count() != IVF_SORT_KEY_COLUMN_COUNT
          || output_addon_vectors_->count()
              != IVF_SORT_ADDON_COLUMN_COUNT
          || output_row_idx_ < 0
          || output_row_idx_ >= output_row_count_) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const char *payload = nullptr;
        ObLength payload_len = 0;
        output_addon_vectors_->at(IVF_SORT_PAYLOAD_COLUMN)->get_payload(
            output_row_idx_, payload, payload_len);
        if (OB_ISNULL(payload) || 0 == payload_len
            || payload_len > INT32_MAX) {
          ret = OB_INVALID_DATA;
        } else {
          current_item_.row_type_ =
              output_vectors_->at(IVF_SORT_ROW_TYPE_COLUMN)->get_int(
                  output_row_idx_);
          current_item_.cid_ =
              output_vectors_->at(IVF_SORT_CID_COLUMN)->get_int(
                  output_row_idx_);
          current_item_.vid_ =
              output_vectors_->at(IVF_SORT_VID_COLUMN)->get_uint(
                  output_row_idx_);
          current_item_.payload_.assign_ptr(
              const_cast<char *>(payload), static_cast<int32_t>(payload_len));
          ++output_row_idx_;
          item = &current_item_;
        }
      }
    }
    return ret;
  }

private:
  int prepare_input_vectors_()
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(
        VEC_FIXED, VEC_TC_INTEGER, input_allocator_, row_type_vector_))) {
      LOG_WARN("failed to create IVF row-type vector", K(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(
        VEC_FIXED, VEC_TC_INTEGER, input_allocator_, cid_vector_))) {
      LOG_WARN("failed to create IVF centroid-ID vector", K(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(
        VEC_FIXED, VEC_TC_UINTEGER, input_allocator_, vid_vector_))) {
      LOG_WARN("failed to create IVF vector-ID vector", K(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::new_vector(
        VEC_DISCRETE, VEC_TC_RAW, input_allocator_, payload_vector_))) {
      LOG_WARN("failed to create IVF payload vector", K(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(
        row_type_vector_, IVF_SORT_BATCH_SIZE, input_allocator_))) {
      LOG_WARN("failed to prepare IVF row-type vector", K(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(
        cid_vector_, IVF_SORT_BATCH_SIZE, input_allocator_))) {
      LOG_WARN("failed to prepare IVF centroid-ID vector", K(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(
        vid_vector_, IVF_SORT_BATCH_SIZE, input_allocator_))) {
      LOG_WARN("failed to prepare IVF vector-ID vector", K(ret));
    } else if (OB_FAIL(ObDirectLoadVectorUtils::prepare_vector(
        payload_vector_, IVF_SORT_BATCH_SIZE, input_allocator_))) {
      LOG_WARN("failed to prepare IVF payload vector", K(ret));
    } else if (OB_FAIL(key_vectors_.reserve(IVF_SORT_KEY_COLUMN_COUNT))) {
      LOG_WARN("failed to reserve IVF sort-key vectors", K(ret));
    } else if (OB_FAIL(addon_vectors_.reserve(
        IVF_SORT_ADDON_COLUMN_COUNT))) {
      LOG_WARN("failed to reserve IVF sort-addon vectors", K(ret));
    } else if (OB_FAIL(key_vectors_.push_back(row_type_vector_))) {
      LOG_WARN("failed to append IVF row-type sort key", K(ret));
    } else if (OB_FAIL(key_vectors_.push_back(cid_vector_))) {
      LOG_WARN("failed to append IVF centroid-ID sort key", K(ret));
    } else if (OB_FAIL(key_vectors_.push_back(vid_vector_))) {
      LOG_WARN("failed to append IVF vector-ID sort key", K(ret));
    } else if (OB_FAIL(addon_vectors_.push_back(payload_vector_))) {
      LOG_WARN("failed to append IVF payload sort addon", K(ret));
    } else if (OB_ISNULL(selector_ = static_cast<uint16_t *>(
        input_allocator_.alloc(
            IVF_SORT_BATCH_SIZE * sizeof(uint16_t))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate IVF sort selector",
          K(ret), K_(tenant_id), K(IVF_SORT_BATCH_SIZE));
    }
    return ret;
  }

  int flush_batch_()
  {
    int ret = OB_SUCCESS;
    int64_t in_memory_row_size = 0;
    // add_batch retains the stored rows, not these temporary pointer arrays.
    ObArray<ObIVFStorageSortRow *> row_ptrs;
    row_ptrs.set_attr(ObMemAttr(tenant_id_, "IVFSortInput"));
    if (batch_row_count_ <= 0 || batch_row_count_ > IVF_SORT_BATCH_SIZE) {
      ret = OB_STATE_NOT_MATCH;
    } else if (OB_FAIL(row_ptrs.prepare_allocate(2 * batch_row_count_))) {
      LOG_WARN("failed to allocate IVF sort batch row pointers", K(ret), K_(batch_row_count));
    } else if (OB_FAIL(sort_impl_->add_batch(
        key_vectors_, &addon_vectors_, selector_, batch_row_count_,
        row_ptrs.get_data(), row_ptrs.get_data() + batch_row_count_, in_memory_row_size))) {
      LOG_WARN("failed to add IVF storage-sort vector batch",
          K(ret), K_(batch_row_count));
    } else {
      batch_row_count_ = 0;
      payload_allocator_.reuse();
    }
    return ret;
  }

  void release_input_buffers_()
  {
    key_vectors_.reset();
    addon_vectors_.reset();
    row_type_vector_ = nullptr;
    cid_vector_ = nullptr;
    vid_vector_ = nullptr;
    payload_vector_ = nullptr;
    selector_ = nullptr;
    batch_row_count_ = 0;
    input_allocator_.reset();
    payload_allocator_.reset();
  }

  void reset()
  {
    if (OB_NOT_NULL(sort_impl_) && OB_NOT_NULL(mem_context_)) {
      ObIAllocator &allocator = mem_context_->get_malloc_allocator();
      sort_impl_->~ObIVFStorageSortImpl();
      allocator.free(sort_impl_);
      sort_impl_ = nullptr;
    }
    if (OB_NOT_NULL(mem_context_)) {
      DESTROY_CONTEXT(mem_context_);
      mem_context_ = nullptr;
    }
    output_vectors_ = nullptr;
    output_addon_vectors_ = nullptr;
    release_input_buffers_();
    key_row_meta_.reset();
    addon_row_meta_.reset();
    meta_allocator_.reset();
    is_inited_ = false;
    is_sorted_ = false;
  }

private:
  uint64_t tenant_id_;
  lib::MemoryContext mem_context_;
  ObArenaAllocator meta_allocator_;
  ObArenaAllocator input_allocator_;
  ObArenaAllocator payload_allocator_;
  sql::RowMeta key_row_meta_;
  sql::RowMeta addon_row_meta_;
  ObIVFStorageSortCompare compare_;
  ObMonitorNode monitor_info_;
  ObIVFStorageSortImpl *sort_impl_;
  ObFixedArray<ObIVector *, ObIAllocator> key_vectors_;
  ObFixedArray<ObIVector *, ObIAllocator> addon_vectors_;
  ObIVector *row_type_vector_;
  ObIVector *cid_vector_;
  ObIVector *vid_vector_;
  ObIVector *payload_vector_;
  uint16_t *selector_;
  int64_t batch_row_count_;
  ObIArray<ObIVector *> *output_vectors_;
  ObIArray<ObIVector *> *output_addon_vectors_;
  int64_t output_row_count_;
  int64_t output_row_idx_;
  ObIVFSortItem current_item_;
  bool is_inited_;
  bool is_sorted_;

  DISALLOW_COPY_AND_ASSIGN(ObIVFSorterImpl);
};

ObIVFSorter::ObIVFSorter()
    : is_inited_(false),
      is_sorted_(false),
      impl_(nullptr),
      row_count_(0),
      has_previous_key_(false),
      previous_row_type_(0),
      previous_cid_(0),
      previous_vid_(0)
{}

ObIVFSorter::~ObIVFSorter()
{
  reset();
}

int ObIVFSorter::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("IVF sorter has already been initialized", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF sorter tenant", K(ret), K(tenant_id));
  } else if (OB_ISNULL(buf = ob_malloc(
      sizeof(ObIVFSorterImpl), ObMemAttr(tenant_id, "IVFSorterImpl")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate IVF sorter implementation",
        K(ret), K(tenant_id), K(sizeof(ObIVFSorterImpl)));
  } else if (FALSE_IT(impl_ = new (buf) ObIVFSorterImpl(tenant_id))) {
  } else if (OB_FAIL(impl_->init())) {
    LOG_WARN("failed to initialize IVF storage sorter", K(ret), K(tenant_id));
  } else {
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(impl_)) {
    impl_->~ObIVFSorterImpl();
    ob_free(impl_);
    impl_ = nullptr;
  }
  return ret;
}

int ObIVFSorter::append_row(const ObIVFBuildRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(impl_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_sorted_ || row.row_type_ <= 0 || row.cid_ < 0
      || row.payload_.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid IVF build row", K(ret), K(row), K_(is_sorted));
  } else if (OB_FAIL(impl_->append_row(row))) {
    LOG_WARN("failed to append IVF row to storage sorter", K(ret), K(row));
  } else {
    ++row_count_;
  }
  return ret;
}

int ObIVFSorter::finish()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_ || OB_ISNULL(impl_))) {
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(is_sorted_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(impl_->finish())) {
    LOG_WARN("failed to finish IVF storage sorter", K(ret), K_(row_count));
  } else {
    is_sorted_ = true;
  }
  return ret;
}

int ObIVFSorter::get_next_item(const ObIVFSortItem *&item)
{
  int ret = OB_SUCCESS;
  item = nullptr;
  if (OB_UNLIKELY(!is_inited_ || !is_sorted_ || OB_ISNULL(impl_))) {
    ret = OB_NOT_INIT;
  } else if (0 == row_count_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(impl_->get_next_item(item))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to read IVF storage-sort item", K(ret));
    }
  } else if (OB_ISNULL(item)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (has_previous_key_
      && previous_row_type_ == item->row_type_
      && previous_cid_ == item->cid_
      && previous_vid_ == item->vid_) {
    ret = OB_ERR_PRIMARY_KEY_DUPLICATE;
    LOG_WARN("duplicate IVF physical row key", K(ret), KPC(item));
  } else {
    has_previous_key_ = true;
    previous_row_type_ = item->row_type_;
    previous_cid_ = item->cid_;
    previous_vid_ = item->vid_;
  }
  return ret;
}

void ObIVFSorter::reset()
{
  if (OB_NOT_NULL(impl_)) {
    impl_->~ObIVFSorterImpl();
    ob_free(impl_);
    impl_ = nullptr;
  }
  is_inited_ = false;
  is_sorted_ = false;
  row_count_ = 0;
  has_previous_key_ = false;
  previous_row_type_ = 0;
  previous_cid_ = 0;
  previous_vid_ = 0;
}

} // namespace storage
} // namespace oceanbase
