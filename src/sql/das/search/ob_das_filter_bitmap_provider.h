/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_FILTER_BITMAP_PROVIDER_H_
#define OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_FILTER_BITMAP_PROVIDER_H_

#include "sql/das/search/ob_das_search_define.h"

namespace oceanbase
{
namespace sql
{

class ObDASSearchCtx;
class ObIDASSearchOp;
class ObDASBitmapOp;
class ObFastBitmap;

enum ObDASFilterBitmapProviderScope
{
  DAS_FILTER_BITMAP_QUERY_SCOPE = 0,
  DAS_FILTER_BITMAP_GROUP_SCOPE
};
static const int64_t DAS_FILTER_BITMAP_QUERY_SCOPE_SLOT = -1;

// Owner of one immutable materialized filter bitmap. A query-scope provider is
// shared by all BMM consumers of one logical query. A Group-scope provider keeps
// the async Group binding used by its source. Serial providers are materialized
// on first View consumption; parallel PREPARE work builds the shared bitmap.
// Consumers always own independent views/iterators over the provider bitmap.
// Materialization failures are terminal for this runtime: a consumed Source
// batch or a partially opened Source cannot be retried without rebuilding it.
class ObDASFilterBitmapProvider
{
public:
  ObDASFilterBitmapProvider(
      ObDASSearchCtx &search_ctx,
      const ObIDASSearchRtDef *owner_rtdef,
      const ObDASFilterBitmapProviderScope scope,
      const int64_t group_slot,
      ObIDASSearchOp *source_op,
      const int64_t provider_id)
    : search_ctx_(search_ctx),
      owner_rtdef_(owner_rtdef),
      scope_(scope),
      group_slot_(group_slot),
      source_op_(source_op),
      bitmap_(nullptr),
      rowid_buf_(nullptr),
      rowid_buf_capacity_(0),
      provider_id_(provider_id),
      source_opened_(false),
      prepared_(false),
      owns_bitmap_(false),
      rescan_pending_(false),
      materialization_ret_(common::OB_SUCCESS)
  { }
  ~ObDASFilterBitmapProvider();

  int prepare(const bool is_rescan);
  int reset_for_rescan();
  int ensure_prepared();
  int build_shared_bitmap(common::ObIAllocator &bitmap_alloc, ObFastBitmap *&out_bitmap);
  int set_external_bitmap(ObFastBitmap *bitmap);
  int create_view(ObDASSearchCtx &search_ctx, ObDASBitmapOp *&view_op);

  OB_INLINE const ObIDASSearchRtDef *get_owner_rtdef() const { return owner_rtdef_; }
  OB_INLINE ObDASFilterBitmapProviderScope get_scope() const { return scope_; }
  OB_INLINE int64_t get_group_slot() const { return group_slot_; }
  OB_INLINE ObFastBitmap *get_bitmap() const { return prepared_ ? bitmap_ : nullptr; }
  OB_INLINE int64_t get_provider_id() const { return provider_id_; }
  OB_INLINE bool is_prepared() const { return prepared_; }
  TO_STRING_KV(K_(provider_id), KP_(owner_rtdef), K_(scope), K_(group_slot),
               KP_(source_op), KP_(bitmap),
               K_(source_opened), K_(prepared), K_(owns_bitmap),
               K_(rescan_pending), K_(materialization_ret));

private:
  int prepare_source(const bool is_rescan);
  int allocate_bitmap(common::ObIAllocator &allocator, ObFastBitmap *&bitmap);
  int fill_bitmap(ObIDASSearchOp &source_op, ObFastBitmap &bitmap);
  int fill_bitmap_from_rowids(ObIDASSearchOp &source_op, ObFastBitmap &bitmap);

private:
  ObDASSearchCtx &search_ctx_;
  const ObIDASSearchRtDef *owner_rtdef_;
  ObDASFilterBitmapProviderScope scope_;
  int64_t group_slot_;
  ObIDASSearchOp *source_op_;
  ObFastBitmap *bitmap_;
  uint64_t *rowid_buf_;
  int64_t rowid_buf_capacity_;
  int64_t provider_id_;
  bool source_opened_;
  bool prepared_;
  bool owns_bitmap_;
  bool rescan_pending_;
  int materialization_ret_;
};

} // namespace sql
} // namespace oceanbase

#endif // OBDEV_SRC_SQL_DAS_SEARCH_OB_DAS_FILTER_BITMAP_PROVIDER_H_
