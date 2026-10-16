/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/page_arena.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"
#include "observer/table_load/stream/ob_direct_load_stream_chunk.h"
#include "sql/engine/table/ob_external_table_access_service.h"

namespace oceanbase
{
namespace sql
{

class ObDirectLoadStreamTableRowIterator : public ObExternalTableRowIterator
{
public:
  ObDirectLoadStreamTableRowIterator();
  virtual ~ObDirectLoadStreamTableRowIterator();
  int init(const storage::ObTableScanParam *scan_param) override;
  int rescan(const storage::ObTableScanParam *scan_param);
  int get_next_row() override;
  int get_next_rows(int64_t &count, int64_t capacity) override;
  virtual int get_next_row(common::ObNewRow *&row) override;
  void reset() override;
private:
  int init_access_exprs_(const storage::ObTableScanParam *scan_param);
  int open_next_chunk_();
  int init_scan_task_(const storage::ObTableScanParam *scan_param);
  void close_current_chunk_();
  void destroy_();
private:
  observer::ObDirectLoadStreamBufferQueueHandle queue_handle_;
  observer::ObDirectLoadStreamChunkReader chunk_reader_;
  common::ObArenaAllocator chunk_allocator_;
  common::ObString current_chunk_;
  uint64_t tenant_id_;
  int64_t stream_id_;
  int64_t target_schema_version_;
  uint64_t queue_id_;
  bool reader_opened_;
  bool is_inited_;
};

}  // namespace sql
}  // namespace oceanbase
