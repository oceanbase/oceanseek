/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SORTER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SORTER_H_

#include "storage/blocksstable/vector_index/ob_ivf_build_struct.h"

namespace oceanbase
{
namespace storage
{

struct ObIVFSortItem
{
  OB_UNIS_VERSION(1);
public:
  ObIVFSortItem()
      : row_type_(0), cid_(0), vid_(0), payload_()
  {}
  int64_t get_deep_copy_size() const { return payload_.length(); }
  int deep_copy(const ObIVFSortItem &src, char *buf,
                const int64_t len, int64_t &pos);
  void reset()
  {
    row_type_ = 0;
    cid_ = 0;
    vid_ = 0;
    payload_.reset();
  }
  TO_STRING_KV(K_(row_type), K_(cid), K_(vid), K_(payload));

  int64_t row_type_;
  int64_t cid_;
  uint64_t vid_;
  common::ObString payload_;
};

// Compact training samples use (node, original ordinal) as their sort key.
// The existing external sorter supplies an explicit payload memory budget.
struct ObIVFClusterSampleCompare
{
  ObIVFClusterSampleCompare() : result_code_(OB_SUCCESS) {}
  bool operator()(const ObIVFSortItem *left, const ObIVFSortItem *right)
  {
    bool less = false;
    if (nullptr == left || nullptr == right) {
      result_code_ = OB_INVALID_ARGUMENT;
    } else {
      less = left->cid_ != right->cid_ ? left->cid_ < right->cid_ : left->vid_ < right->vid_;
    }
    return less;
  }
  int result_code_;
};

class ObIVFSorterImpl;

class ObIVFSorter : public ObIVFBuildRowSink
{
public:
  ObIVFSorter();
  virtual ~ObIVFSorter();
  int init(const uint64_t tenant_id);
  virtual int append_row(const ObIVFBuildRow &row) override;
  int finish();
  // The returned item is owned by the sorter and remains valid only until the
  // next get_next_item() call. A repeated full key is rejected here.
  int get_next_item(const ObIVFSortItem *&item);
  int64_t get_row_count() const { return row_count_; }
  // Release sorted runs after the consumer has crossed its output barrier.
  void reset();

private:
  bool is_inited_;
  bool is_sorted_;
  ObIVFSorterImpl *impl_;
  int64_t row_count_;
  bool has_previous_key_;
  int64_t previous_row_type_;
  int64_t previous_cid_;
  uint64_t previous_vid_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFSorter);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_SORTER_H_
