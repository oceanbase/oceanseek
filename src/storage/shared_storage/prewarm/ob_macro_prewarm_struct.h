/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MACRO_PREWARM_STRUCT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MACRO_PREWARM_STRUCT_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "lib/hash/ob_hashmap.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/shared_storage/prewarm/ob_prewarm_parallelism_controller.h"
#include "storage/shared_storage/prewarm/ob_ss_base_prewarmer.h"
#include "share/storage_cache_policy/ob_storage_cache_common.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/ob_ss_local_cache_stat.h"
namespace oceanbase
{
namespace blocksstable
{
  class ObSSTable;
  class ObMacroIdIterator;
  class ObSSTableMetaHandle;
  class ObSSTableSecMetaIterator;
  struct ObDatumRange;
}
namespace compaction
{
  enum class ObSSMajorPrewarmLevel : uint8_t;
}
namespace storage
{
class ObTabletHandle;

struct ObSSTableMacroPrewarmInfo final
{
public:
  ObSSTableMacroPrewarmInfo()
    : iterated_macro_cnt_(0), skipped_macro_cnt_(0), prewarmed_macro_cnt_(0)
  {}
  ~ObSSTableMacroPrewarmInfo() {}
  void reset()
  {
    iterated_macro_cnt_ = 0;
    skipped_macro_cnt_ = 0;
    prewarmed_macro_cnt_ = 0;
  }
  TO_STRING_KV(K_(iterated_macro_cnt), K_(skipped_macro_cnt), K_(prewarmed_macro_cnt));

public:
  int64_t iterated_macro_cnt_; // count of macros iterated from ObMacroIdIterator
  int64_t skipped_macro_cnt_; // count of macros skipped due to already exist in local cache
  int64_t prewarmed_macro_cnt_; // count of macros prewarmed to local cache
};

enum class ObPrewarmMacroType : uint8_t
{
  META_MACRO = 0,
  DATA_MACRO = 1,
  MAX_TYPE = 2
};

static const char *get_prewarm_macro_type_str(const ObPrewarmMacroType type)
{
  static const char *type_str_map_[static_cast<uint8_t>(ObPrewarmMacroType::MAX_TYPE) + 1] = {
    "META_MACRO",
    "DATA_MACRO",
    "MAX_TYPE"
  };
  const char *type_str = type_str_map_[static_cast<uint8_t>(ObPrewarmMacroType::MAX_TYPE)];
  if (OB_LIKELY((type >= ObPrewarmMacroType::META_MACRO)) ||
                (type <= ObPrewarmMacroType::MAX_TYPE)) {
    type_str = type_str_map_[static_cast<uint8_t>(type)];
  }
  return type_str;
}

class ObSSTableMacroPrewarmer : public ObPrewarmParController, public ObSSBasePrewarmer
{
public:
  ObSSTableMacroPrewarmer(
      const blocksstable::ObSSTable *sstable, volatile bool &is_stop,
      const ObSSLocalCachePrewarmType prewarm_type);
  virtual ~ObSSTableMacroPrewarmer();
  int do_prewarm(const ObTabletHandle &tablet_handle);
  int prewarm_single_macro(const blocksstable::MacroBlockId &macro_id, const uint64_t effective_tablet_id);
  TO_STRING_KV(KP_(sstable), K_(effective_tablet_id), K(prewarm_type_), K_(parallelism));

private:
  int handle_hot_tablet(const compaction::ObSSMajorPrewarmLevel prewarm_level,
                        const ObTabletHandle &tablet_handle);
  int handle_non_hot_tablet(const compaction::ObSSMajorPrewarmLevel prewarm_level,
                            const ObTabletHandle &tablet_handle);
  int prewarm_macros_with_retry(const ObPrewarmMacroType type,
                                const ObTabletHandle &tablet_handle);
  int prewarm_macros(const ObPrewarmMacroType type,
                     const ObTabletHandle &tablet_handle,
                     ObSSTableMacroPrewarmInfo &prewarm_info);
  // return ObMacroIdIterator for meta macro, which iterates macro_id.
  // return ObSSTableSecMetaIterator for data macro, which iterates macro_id and logic_version.
  int get_macro_iter(const ObPrewarmMacroType type,
                     const ObTabletHandle &tablet_handle,
                     blocksstable::ObSSTableMetaHandle &meta_handle,
                     blocksstable::ObMacroIdIterator &macro_id_iter,
                     common::ObArenaAllocator &allocator,
                     blocksstable::ObDatumRange &datum_range,
                     blocksstable::ObSSTableSecMetaIterator *&sec_meta_iter);
  // for meta macro, return macro_id.
  // for data macro, return macro_id and macro's data macro block meta.
  int get_next_macro(const ObPrewarmMacroType type,
                     blocksstable::ObMacroIdIterator &macro_id_iter,
                     blocksstable::ObSSTableSecMetaIterator *sec_meta_iter,
                     blocksstable::MacroBlockId &macro_id,
                     blocksstable::ObDataMacroBlockMeta &data_macro_block_meta);
  int check_if_need_prewarm(const ObPrewarmMacroType type,
                            const blocksstable::MacroBlockId &macro_id,
                            const blocksstable::ObDataMacroBlockMeta &data_macro_block_meta,
                            const ObPolicyStatusCalculator &policy_status_calculator,
                            bool &is_need_prewarm);
  // Init policy_status_calculator for MACRO_HOT tablet
  int init_policy_status_calculator(ObPolicyStatusCalculator &policy_status_calculator);
  // For MACRO_HOT tablet, handle existing macro block cache type based on macro policy status
  int handle_exist_macro_cache_type_for_macro_hot(
      const blocksstable::MacroBlockId &macro_id,
      const blocksstable::ObDataMacroBlockMeta &data_macro_block_meta,
      const ObPolicyStatusCalculator &policy_status_calculator);
  int check_if_logic_version_in_one_day(const int64_t logic_version, bool &is_in_one_day);
  int aio_read_from_object_storage(const blocksstable::MacroBlockId &macro_id);
  // `prewarm_into_micro`: false means prewarm into macro cache, true means prewarm into micro cache.
  int batch_write_to_local_cache(ObSSTableMacroPrewarmInfo &prewarm_info);
  int single_write_to_macro_cache(const int64_t cur_read_idx, ObSSTableMacroPrewarmInfo &prewarm_info);
  int single_write_to_local_cache(const int64_t cur_read_idx, ObSSTableMacroPrewarmInfo &prewarm_info);
  int try_modify_macro_cache_type_for_macro_hot(const blocksstable::MacroBlockId &macro_id);
  int batch_wait_read_handles();
  int single_wait_write_handle(const int64_t cur_write_idx, ObSSTableMacroPrewarmInfo &prewarm_info);
  void reset_io_resources();
  int alloc_file_size_for_write(const blocksstable::MacroBlockId &macro_id, const int64_t file_size) const;
  // do not prewarm data macro when ramaining disk space is smaller than 10%
  int check_if_prewarm_data_macro(const bool is_hot_tablet,
                                  const compaction::ObSSMajorPrewarmLevel prewarm_level,
                                  bool &is_prewarm_data_macro);
  bool is_hot_block_(const blocksstable::MacroBlockId &macro_id) const;

private:
  static const int64_t ONE_DAY_NS = 24 * 60 * 60 * 1000L * 1000L * 1000L;
  static const int64_t MIN_PARALLELISM = 1;
  static const int64_t MAX_PARALLELISM = 8;
  static const int64_t DEFAULT_MACRO_POLICY_STATUS_MAP_SIZE = 128;
  volatile bool &is_stop_;
  const blocksstable::ObSSTable *sstable_;
  uint64_t effective_tablet_id_;
  ObSSLocalCachePrewarmType prewarm_type_;
  int64_t parallelism_;
  blocksstable::ObStorageObjectHandle read_handles_[MAX_PARALLELISM]; // TODO(binifei.bnf): refactor same logic
  blocksstable::ObStorageObjectHandle write_handles_[MAX_PARALLELISM];
  int64_t write_sizes_[MAX_PARALLELISM]; // record write size, for free disk space on write fail
  int64_t read_handle_idx_;
  int64_t write_handle_idx_;
  common::ObArenaAllocator buf_allocator_;
  bool is_hot_or_macro_hot_tablet_;
  common::hash::ObHashMap<blocksstable::MacroBlockId, PolicyStatus> macro_policy_status_map_;
};


} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_PREWARM_OB_MACRO_PREWARM_STRUCT_H_ */
