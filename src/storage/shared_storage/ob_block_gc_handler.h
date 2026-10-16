/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_BLOCK_GC_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_BLOCK_GC_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "deps/oblib/src/common/ob_tablet_id.h"
#include "lib/hash/ob_hashset.h"
#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObPendingFreeTabletItem;

typedef common::hash::ObHashSet<blocksstable::MacroBlockId, common::hash::NoPthreadDefendMode> BlockIDSet;

struct GCBaseOP
{
public:
  GCBaseOP(
    const BlockIDSet *exist_block_set)
    : block_id_set_(exist_block_set)
  {}
  virtual ~GCBaseOP() { result_block_id_set_.destroy(); }
  GCBaseOP(GCBaseOP&) = delete;
  GCBaseOP& operator=(const GCBaseOP&) = delete;
  const BlockIDSet &get_result_block_id_set() { return result_block_id_set_; }
  virtual int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> &result_block_ids)
  { return OB_NOT_SUPPORTED; }

protected:
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> *result_block_ids);

  virtual void add_or_print_debug_block(const blocksstable::MacroBlockId &block_id) {}
private:
  const BlockIDSet *block_id_set_;
  BlockIDSet result_block_id_set_;
};

struct BlockCollectOP : public GCBaseOP
{
  BlockCollectOP()
    : GCBaseOP(NULL)
  {}
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids)
  { return GCBaseOP::operator()(block_ids, NULL); }
  virtual ~BlockCollectOP() {}
};

struct GCTabletMetaVersionOP : public GCBaseOP
{
public:
  GCTabletMetaVersionOP(
    const BlockIDSet &exist_block_set,
    const BlockIDSet &blocks_for_error_check)
    : GCBaseOP(&exist_block_set),
      blocks_for_error_check_(blocks_for_error_check)
  {}
  virtual ~GCTabletMetaVersionOP() 
  { print_debug_block(); }
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> &result_block_ids);
  void print_debug_block();
  void add_or_print_debug_block(const blocksstable::MacroBlockId &block_id);

private:
  const BlockIDSet &blocks_for_error_check_;
  ObArray<blocksstable::MacroBlockId> debug_block_ids_;
};

struct GCTabletOP : public GCBaseOP
{
  GCTabletOP()
    : GCBaseOP(NULL)
  {}
  virtual ~GCTabletOP() {}
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> &result_block_ids)
  { return GCBaseOP::operator()(block_ids, &result_block_ids); }
};

struct MacroBlockCheckOP
{
public:
  MacroBlockCheckOP(
    const BlockIDSet &exist_block_set)
    : block_id_set_(exist_block_set)
  {}
  MacroBlockCheckOP(MacroBlockCheckOP&) = delete;
  MacroBlockCheckOP& operator=(const MacroBlockCheckOP&) = delete;

  // result_block_ids = block_ids - block_id_set
  int operator()(
      const ObIArray<blocksstable::MacroBlockId> &block_ids,
      ObIArray<blocksstable::MacroBlockId> &result_block_ids) const;
  VIRTUAL_TO_STRING_KV(K_(block_id_set));
protected:
  virtual int filter_block(
      const blocksstable::MacroBlockId &block_id,
      bool &need_filter) const
  { 
    need_filter = false; 
    return OB_SUCCESS;
  }
private:
  const BlockIDSet &block_id_set_;
};

struct PrivateMacroBlockCheckOP : public MacroBlockCheckOP
{
public:
  PrivateMacroBlockCheckOP(
    const BlockIDSet &exist_block_set,
    const uint64_t min_block_id,
    const uint64_t max_block_id)
    : MacroBlockCheckOP(exist_block_set),
      min_block_id_(min_block_id),
      max_block_id_(max_block_id)
  {}
  PrivateMacroBlockCheckOP(MacroBlockCheckOP&) = delete;
  PrivateMacroBlockCheckOP& operator=(const MacroBlockCheckOP&) = delete;

  INHERIT_TO_STRING_KV("MacroBlockCheckOP", MacroBlockCheckOP, K_(max_block_id), K_(min_block_id));
protected:
  virtual int filter_block(
      const blocksstable::MacroBlockId &block_id,
      bool &need_filter) const;
private:
  const uint64_t min_block_id_;
  const uint64_t max_block_id_;
};

int delete_macro_blocks(
    ObIArray<blocksstable::MacroBlockId> &block_ids);

class ObBlockGCHandler
{
public:
  ObBlockGCHandler(
      const ObTabletID &tablet_id,
      const bool &is_set_stop)
    : tablet_id_(tablet_id),
      is_set_stop_(is_set_stop),
      gc_macro_block_cnt_(0)
  {}
  virtual ~ObBlockGCHandler() {}
  bool is_valid()
  { return tablet_id_.is_valid(); }

  // gc tablet operation
  int gc_tablet_meta_versions(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      const bool is_transfer_out_deleted);
  int gc_tablet(
      const ObIArray<int64_t> &tablet_versions,
      const GCTabletType gc_type,
      // for macro block check in private dir
      const uint64_t min_block_id = 0,
      const uint64_t max_block_id = INT64_MAX);

  // virutal interface
  virtual int get_blocks_for_tablet(
    const int64_t tablet_meta_version,
    const bool is_shared,
    ObIArray<blocksstable::MacroBlockId> &block_ids) = 0;
  virtual int delete_tablet_meta_version(
    int64_t tablet_meta_version) = 0;
  virtual int try_delete_tablet_meta_dir() = 0;
  virtual int try_delete_tablet_data_dir() = 0;
  virtual int get_block_ids_from_dir(
    ObIArray<blocksstable::MacroBlockId> &block_ids) = 0;
  virtual int get_blocks_for_error_block_check(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &collect_check_block_op) = 0;
  virtual int delete_macro_blocks(
      ObIArray<blocksstable::MacroBlockId> &block_ids);

  int macro_block_check(
      const BlockIDSet &block_set,
      const uint64_t min_block_id,
      const uint64_t max_block_id);

  int build_macro_block(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      BlockCollectOP &get_gc_blocks_op);
  bool check_stop();


  VIRTUAL_TO_STRING_KV(K_(tablet_id));

protected:
  virtual int safe_delete_blocks_for_macro_check_(
      ObIArray<blocksstable::MacroBlockId> &block_ids);
    
private:
  int gc_blocks_for_tablet_meta_versions_(
      const ObIArray<int64_t> &tablet_versions,
      const int64_t min_retain_tablet_meta_version,
      const bool is_shared);
  int delete_tablet_meta_versions_(
      const ObIArray<int64_t> &tablet_versions,
      int64_t gc_tablet_meta_ts);
  int gc_blocks_(
    const ObIArray<int64_t> &tablet_versions,
    const int64_t gc_tablet_meta_ts,
    const bool is_shared,
    GCBaseOP &op);

  virtual int delete_macro_cache_(const ObIArray<blocksstable::MacroBlockId> &block_ids);

  int64_t get_min_retain_tablet_meta_version_(
    const ObIArray<int64_t> &tablet_versions,
    int64_t min_retain_tablet_meta_ts)
  {
    int64_t ret = INT64_MAX;
    for (int64_t i = 0; i < tablet_versions.count(); i++) {
      const int64_t tablet_version = tablet_versions.at(i);
      if (tablet_version >= min_retain_tablet_meta_ts
          && tablet_version < ret) {
        ret = tablet_version;
      }
    }
    if (INT64_MAX == ret) {
      // if all version is less min_retain_tablet_meta_ts, 
      // min_retain_tablet_meta_version is max version of all version.
      ret = -1;
      for (int64_t i = 0; i < tablet_versions.count(); i++) {
        const int64_t tablet_version = tablet_versions.at(i);
        if (tablet_version > ret) {
          ret = tablet_version;
        }
      }
    }
    return ret;
  }

protected:
  const common::ObTabletID tablet_id_;
  const bool &is_set_stop_;
  // for statistics
public:
  int64_t gc_macro_block_cnt_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_BLOCK_GC_H_ */
