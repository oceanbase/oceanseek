/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_FLUSH_MAP_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_FLUSH_MAP_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/future/ob_future.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

class ObSSMacroCacheFlushStatus
{
public:
  ObSSMacroCacheFlushStatus();
  ObSSMacroCacheFlushStatus(const uint8_t func_type, const bool is_flushing);
  ObSSMacroCacheFlushStatus(const ObSSMacroCacheFlushStatus &other);
  virtual ~ObSSMacroCacheFlushStatus();
  ObSSMacroCacheFlushStatus &operator=(const ObSSMacroCacheFlushStatus &other);
  OB_INLINE uint8_t get_func_type() const { return func_type_; }
  OB_INLINE bool get_is_flushing() const { return is_flushing_; }
  OB_INLINE int64_t get_timestamp_us() const { return timestamp_us_; }
  OB_INLINE void set_flushing() { is_flushing_ = true; }
  TO_STRING_KV(K_(func_type), K_(is_flushing));

private:
  uint8_t func_type_;
  bool is_flushing_;
  // for error log about flush entries, whose @is_flushing_=true but stay in ObSSMacroCacheFlushMap
  // for a long time (e.g., more than 1 hour).
  int64_t timestamp_us_;
};


class ObSSMacroCacheFlushInfo
{
public:
  ObSSMacroCacheFlushInfo();
  ObSSMacroCacheFlushInfo(const blocksstable::MacroBlockId &macro_id,
                          const ObSSMacroCacheFlushStatus &flush_status);
  virtual ~ObSSMacroCacheFlushInfo();
  OB_INLINE void set_macro_id(const blocksstable::MacroBlockId &macro_id) { macro_id_ = macro_id; }
  OB_INLINE const blocksstable::MacroBlockId &get_macro_id() const { return macro_id_; }
  OB_INLINE void set_flush_status(const ObSSMacroCacheFlushStatus &flush_status) { flush_status_ = flush_status; }
  OB_INLINE const ObSSMacroCacheFlushStatus &get_flush_status() const { return flush_status_; }
  bool is_valid() const;
  TO_STRING_KV(K_(macro_id), K_(flush_status));

private:
  blocksstable::MacroBlockId macro_id_;
  ObSSMacroCacheFlushStatus flush_status_;
};


class ObSSMacroCacheFlushMap
{
public:
  explicit ObSSMacroCacheFlushMap(const bool use_promise);
  virtual ~ObSSMacroCacheFlushMap();
  int create(int64_t bucket_num, const lib::ObMemAttr &bucket_attr);
  int destroy();
  int set_refactored(const blocksstable::MacroBlockId &macro_id,
                     const ObSSMacroCacheFlushStatus &flush_status);
  int set_refactored(const blocksstable::MacroBlockId &macro_id,
                     const ObSSMacroCacheFlushStatus &flush_status,
                     const ObPromise<int> &promise);
  int erase_refactored(const blocksstable::MacroBlockId &macro_id);
  int set_promise(const blocksstable::MacroBlockId &macro_id, const int ret_code);
  int64_t size() const;
  int get_next_flush_entry(ObSSMacroCacheFlushInfo &flush_info);

private:
  typedef common::hash::ObHashMap<blocksstable::MacroBlockId, ObSSMacroCacheFlushStatus> FLUSH_MAP;
  typedef common::hash::ObHashMap<blocksstable::MacroBlockId, ObPromise<int>> PROMISE_MAP;
  static const int64_t STAY_IN_MAP_THRESHOLD_US = 600 * 1000L * 1000L; // 10min
  FLUSH_MAP flush_map_;
  int64_t bucket_pos_;
  PROMISE_MAP promise_map_;
  bool use_promise_;
  DISALLOW_COPY_AND_ASSIGN(ObSSMacroCacheFlushMap);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_FLUSH_MAP_H_ */
