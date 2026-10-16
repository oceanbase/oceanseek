/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_CENTROID_COUNTER_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_CENTROID_COUNTER_H_

#include "lib/container/ob_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_define.h"

namespace oceanbase
{
namespace share
{

// Per-center counter entry
struct ObSpfreshCentCountEntry
{
public:
  ObSpfreshCentCountEntry() : center_id_(OB_INVALID_ID), count_(0) {}
  ObSpfreshCentCountEntry(uint64_t center_id, int64_t count)
    : center_id_(center_id), count_(count) {}

  uint64_t center_id_;
  int64_t count_;

  TO_STRING_KV(K_(center_id), K_(count));
};

// Per-center counter for SPFresh
// Used to track posting_list_length for each centroid
class ObSpfreshCentroidCounter
{
public:
  ObSpfreshCentroidCounter();
  ~ObSpfreshCentroidCounter();

  int init(uint64_t tenant_id);
  void destroy();

  // Atomically increase center's count
  // @param [out] new_count The new count after increment
  int inc_center_count(uint64_t center_id, int64_t delta, int64_t &new_count);

  // Atomically decrease center's count
  // @param [out] new_count The new count after decrement
  int dec_center_count(uint64_t center_id, int64_t delta, int64_t &new_count);

  // Get center's count
  // @param [out] count The current count
  int get_center_count(uint64_t center_id, int64_t &count) const;

  // Set center's count (for initialization or sync)
  int set_center_count(uint64_t center_id, int64_t count);

  // Reset all counters
  void reset();

  bool is_inited() const { return is_inited_; }

  TO_STRING_KV(K_(is_inited), K_(tenant_id), "counter_count", counter_map_.size());

private:
  bool is_inited_;
  uint64_t tenant_id_;
  ObSpinLock lock_;
  // map from center_id to count
  common::hash::ObHashMap<uint64_t, int64_t> counter_map_;
  DISALLOW_COPY_AND_ASSIGN(ObSpfreshCentroidCounter);
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_SPFRESH_CENTROID_COUNTER_H_