/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_TABLET_CONTEXT_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_TABLET_CONTEXT_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_array.h"
#include "lib/lock/ob_mutex.h"
#include "share/rc/ob_tenant_base.h"
#include "share/vector_index/ob_ivf_replay_memory_mgr.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "storage/blocksstable/vector_index/ob_ivf_replay_input.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable_struct.h"

namespace oceanbase
{
namespace storage
{

class ObIVFDDLDagCtx;
class ObIVFRefreshMonitor;

struct ObIVFSliceInput final
{
  explicit ObIVFSliceInput(const int64_t slice_idx)
    : slice_idx_(slice_idx), allocator_(common::ObMemAttr(MTL_ID(), "IVFSlice")), replay_(allocator_) {}
  TO_STRING_KV(K_(slice_idx), K(replay_.is_sealed()), K(replay_.get_row_count()));
  int64_t slice_idx_;
  common::ObArenaAllocator allocator_;
  ObIVFReplayInput replay_;
};

struct ObIVFTabletContext final
{
  ObIVFTabletContext() : mutex_(common::ObLatchIds::DDL_TABLET_CONTEXT_LOCK),
      slice_inputs_(), capture_slice_count_(0), replay_memory_reservation_(),
      rotator_(), rotation_dim_(0), ivf_meta_(),
      is_built_(false), ivf_ddl_dag_ctx_(nullptr), refresh_monitor_(nullptr) {}
  ~ObIVFTabletContext();
  // Call only after capture/build readers have drained. Preserve the IVF
  // metadata and rotator used by the subsequent physical merge.
  void release_capture_inputs();
  int init_rotator(common::ObIAllocator &allocator, const int64_t dim);
  int register_slice(ObIVFSliceInput *input);
  int ensure_capture_budget(const int64_t capture_slice_count, int64_t &slice_memory_limit);
  const quantization::ObFhtKacRotator &get_rotator() const { return rotator_; }
  int64_t get_rotation_dim() const { return rotation_dim_; }
  TO_STRING_KV(K_(slice_inputs), K_(rotation_dim), K_(ivf_meta), K_(is_built));
  lib::ObMutex mutex_;
  common::ObArray<ObIVFSliceInput *> slice_inputs_;
  // Snapshot input slice count before INITIAL_BUILD switches to output slices.
  int64_t capture_slice_count_;
  share::ObIVFReplayMemoryReservation replay_memory_reservation_;
  quantization::ObFhtKacRotator rotator_;
  int64_t rotation_dim_;
  blocksstable::ObIvfSSTableMeta ivf_meta_;
  bool is_built_;
  // Owns the per-tablet INITIAL_BUILD runtime. DAG tasks only borrow it.
  ObIVFDDLDagCtx *ivf_ddl_dag_ctx_;
  ObIVFRefreshMonitor *refresh_monitor_; // owning reference, shared by capture/build tasks
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_VECTOR_INDEX_OB_IVF_TABLET_CONTEXT_H_
