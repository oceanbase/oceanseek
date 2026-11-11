/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_INPUT_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_INPUT_WRITER_H_

#include "storage/ddl/ob_tablet_slice_writer.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace storage
{
class ObColumnClusteredDag;

// Range EXCHANGE may interleave input slices after SORT is removed. Retain
// each tablet/slice writer until the receiving PX worker reaches EOF.
class ObIVFInputWriter final : public ObISliceWriter
{
public:
  ObIVFInputWriter();
  virtual ~ObIVFInputWriter();
  static int get_writer(ObColumnClusteredDag &dag,
                        const common::ObTabletID &tablet_id,
                        const int64_t slice_idx,
                        const bool is_append_batch,
                        ObISliceWriter *&writer);
  int append_current_row(const common::ObIArray<common::ObDatum *> &datums) override;
  int append_current_batch(const common::ObIArray<common::ObIVector *> &vectors,
                           share::ObBatchSelector &selector) override;
  int get_row_count(int64_t &row_count) override;
  int close() override;
  INHERIT_TO_STRING_KV("SliceWriter", ObISliceWriter, K_(is_inited), K_(is_closed), KP_(active_writer));
private:
  class TabletWriter;
  struct InputBuffer;
  struct SliceKey
  {
    SliceKey() : tablet_id_(), slice_idx_(-1) {}
    SliceKey(const common::ObTabletID &tablet_id, const int64_t slice_idx)
      : tablet_id_(tablet_id), slice_idx_(slice_idx) {}
    uint64_t hash() const { return common::murmurhash(&slice_idx_, sizeof(slice_idx_), tablet_id_.hash()); }
    int hash(uint64_t &value) const { value = hash(); return common::OB_SUCCESS; }
    bool operator==(const SliceKey &other) const
    { return tablet_id_ == other.tablet_id_ && slice_idx_ == other.slice_idx_; }
    TO_STRING_KV(K_(tablet_id), K_(slice_idx));
    common::ObTabletID tablet_id_;
    int64_t slice_idx_;
  };
  typedef common::hash::ObHashMap<SliceKey, TabletWriter *,
      common::hash::NoPthreadDefendMode> WriterMap;
  int init(ObColumnClusteredDag &dag, const bool is_append_batch);
  int select_slice(const common::ObTabletID &tablet_id, const int64_t slice_idx);
  void release_writers();
private:
  ObColumnClusteredDag *dag_;
  WriterMap writers_;
  TabletWriter *active_writer_;
  InputBuffer *input_buffer_;
  bool is_inited_;
  bool is_append_batch_;
  bool is_closed_;
  int close_ret_;
  DISALLOW_COPY_AND_ASSIGN(ObIVFInputWriter);
};

} // namespace storage
} // namespace oceanbase
#endif
