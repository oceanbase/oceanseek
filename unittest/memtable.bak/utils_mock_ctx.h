/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef  OCEANBASE_UNITTEST_MEMTABLE_MOCK_CTX_H_
#define  OCEANBASE_UNITTEST_MEMTABLE_MOCK_CTX_H_

#include "memtable/ob_memtable_interface.h"
#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

struct ObMtCtxMembers
{
  int64_t start_snapshot_;
  int64_t stop_snapshot_;
  uint32_t ctx_descriptor_;
  int64_t stmt_remainder_time_;
  int64_t base_snapshot_;
  int64_t redo_log_id_;
  char trace_log_buffer_[65536];
  int64_t tlb_pos_;
  CharArena allocator_;
  ObSEArray<ObITransCallback*, 64> scbs_;
  ObSEArray<ObITransCallback*, 64> tcbs_;
};

class ObMtCtx : public ObIMemtableCtx, public ObMtCtxMembers
{
public:
  ObMtCtx()
  {
  }
  ~ObMtCtx()
  {
  }
public:
  void fill_trace_log(const char *fmt, ...)
  {
    va_list args;
    va_start(args, fmt);
    tlb_pos_ += vsnprintf(&trace_log_buffer_[tlb_pos_], sizeof(trace_log_buffer_) - tlb_pos_, fmt, args);
    va_end(args);
  }
  int64_t get_start_snapshot() const { return start_snapshot_; }
  int64_t get_stop_snapshot() const { return stop_snapshot_; }
  uint32_t get_ctx_descriptor() const { return ctx_descriptor_; }
  int64_t get_stmt_remainder_time() const { return stmt_remainder_time_; }
  int64_t get_base_snapshot() const { return base_snapshot_; }
  int64_t get_redo_log_id() const { return redo_log_id_; }
  void *alloc(const int64_t size) { return allocator_.alloc(size); }
  int register_stack_callback(ObITransCallback *callback)
  {
    int ret = OB_SUCCESS;
    scbs_.push_back(callback);
    return ret;
  }
  int register_trans_callback(ObITransCallback *callback)
  {
    int ret = OB_SUCCESS;
    tcbs_.push_back(callback);
    return ret;
  }
  int fill_redo_log(
      const uint64_t table_id,
      const uint64_t index_id,
      const int64_t rowkey_len,
      const common::ObIArray<uint64_t> &column_ids,
      const storage::ObStoreRow &row)
  {
    int ret = OB_SUCCESS;
    UNUSED(table_id);
    UNUSED(index_id);
    UNUSED(rowkey_len);
    UNUSED(column_ids);
    UNUSED(row);
    return ret;
  }
public:
  void stack_begin()
  {
    assert(0 == scbs_.count());
  }
  void stack_end(const bool commit)
  {
    if (!commit) {
      for (int64_t i = scbs_.count() - 1; i >= 0; i--) {
        if (NULL != scbs_.at(i)) {
          scbs_.at(i)->callback(commit);
        }
      }
      scbs_.reset();
    }
  }
  void trans_end(const bool commit)
  {
    for (int64_t i = scbs_.count() - 1; i >= 0; i--) {
      if (NULL != scbs_.at(i)) {
        scbs_.at(i)->callback(commit);
      }
    }
    scbs_.reset();
    for (int64_t i = tcbs_.count() - 1; i >= 0; i--) {
      if (NULL != tcbs_.at(i)) {
        tcbs_.at(i)->callback(commit);
      }
    }
    tcbs_.reset();
  }
  void trans_publish()
  {
  }
};

}
}

#endif //OCEANBASE_UNITTEST_MEMTABLE_MOCK_CTX_H_
