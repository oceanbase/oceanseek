/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/allocator/page_arena.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"

namespace oceanbase
{
namespace sql
{
class ObEvalCtx;
class ObExpr;
}
namespace observer
{

/*
 * Stream chunk 使用一段连续内存，格式如下：
 *
 * +------------------------------------------------------+
 * | ChunkHeader                                          |
 * | magic_ | codec_version_ | row_count_ | payload_size_ |
 * +------------------------------------------------------+
 * | Payload                                              |
 * | StoredRow[0] | StoredRow[1] | ... | StoredRow[N-1]   |
 * +------------------------------------------------------+
 *
 * 每个 StoredRow 也是连续内存：
 *
 * +------+-----------+----------------+------------------+
 * | cnt_ | row_size_ | ObDatum[cnt_] | varlen data       |
 * +------+-----------+----------------+------------------+
 *
 * N 等于 row_count_，payload_size_ 是所有 StoredRow 的总字节数。StoredRow
 * 在 Payload 中紧密排列，row_size_ 包含行头、ObDatum 数组和变长数据。chunk
 * 总长度为 sizeof(ObDirectLoadStreamChunkHeader) + payload_size_。Reader 必须
 * 按 row_size_ 遍历完整 Payload，且实际行数等于 N。非 NULL ObDatum 的 ptr_
 * 存储相对当前 StoredRow 起始地址的 offset；NULL datum 的 ptr_ 和 len_ 均为 0。
 */
struct ObDirectLoadStreamChunkHeader
{
public:
  static const int32_t MAGIC = 0x444c5343; // DLSC
  static const int32_t CODEC_VERSION = 1;
public:
  ObDirectLoadStreamChunkHeader();
  void reset();
  bool is_valid() const;
  static int decode(const char *buf,
                    const int64_t size,
                    ObDirectLoadStreamChunkHeader &header);
  int encode(char *buf, const int64_t size) const;
  TO_STRING_KV(K_(magic), K_(codec_version), K_(row_count), K_(payload_size));
public:
  int32_t magic_;
  int32_t codec_version_;
  int64_t row_count_;
  int64_t payload_size_;
};

class ObDirectLoadStreamChunkWriter
{
public:
  static const int64_t DEFAULT_MAX_PAYLOAD_SIZE = 8L << 20;
public:
  ObDirectLoadStreamChunkWriter();
  ~ObDirectLoadStreamChunkWriter();
  int init(const uint64_t tenant_id,
           const int64_t max_payload_size = DEFAULT_MAX_PAYLOAD_SIZE);
  void reset();
  void reuse();
  bool is_inited() const;
  int append_row(const common::ObIArray<sql::ObExpr *> &exprs,
                 sql::ObEvalCtx &eval_ctx);
  int append_batch(const common::ObIArray<sql::ObExpr *> &exprs,
                   sql::ObEvalCtx &eval_ctx,
                   const int64_t start_idx,
                   const int64_t row_count);
  // chunk 引用 Writer 内部 buffer；继续写入、reuse() 或 reset() 后不得再访问。
  int build_chunk(common::ObString &chunk);
  int64_t get_row_count() const;
  TO_STRING_KV(K_(tenant_id), K_(row_count), K_(payload_size),
               K_(max_payload_size), K_(is_inited));
private:
  int ensure_chunk_buffer_();
  int append_row_at_(const common::ObIArray<sql::ObExpr *> &exprs,
                     sql::ObEvalCtx &eval_ctx,
                     const int64_t batch_idx);
private:
  uint64_t tenant_id_;
  common::ObArenaAllocator chunk_allocator_;
  char *chunk_buffer_;
  int64_t row_count_;
  int64_t payload_size_;
  int64_t max_payload_size_;
  bool is_inited_;
};

class ObDirectLoadStreamChunkReader
{
public:
  ObDirectLoadStreamChunkReader();
  ~ObDirectLoadStreamChunkReader();
  int init(const uint64_t tenant_id);
  void reset();
  void reuse();
  int open(const char *chunk_buf,
           const int64_t chunk_size,
           const int64_t expected_col_count);
  int get_next_row(const common::ObIArray<sql::ObExpr *> &exprs,
                   sql::ObEvalCtx &eval_ctx);
  int get_next_batch(const common::ObIArray<sql::ObExpr *> &exprs,
                     sql::ObEvalCtx &eval_ctx,
                     const int64_t max_row_count,
                     int64_t &read_row_count);
  int64_t get_row_count() const;
  int64_t get_payload_size() const;
  TO_STRING_KV(K_(tenant_id), K_(header), K_(expected_col_count),
               K_(read_pos), K_(read_row_count), K_(is_opened));
private:
  bool is_inited() const;
  int validate_payload_() const;
  int validate_stored_row_(const sql::ObChunkDatumStore::StoredRow &row,
                           const int64_t remain_size) const;
  int materialize_current_row_(const common::ObIArray<sql::ObExpr *> &exprs,
                               sql::ObEvalCtx &eval_ctx,
                               const int64_t output_idx);
private:
  uint64_t tenant_id_;
  ObDirectLoadStreamChunkHeader header_;
  const char *payload_;
  int64_t expected_col_count_;
  int64_t read_pos_;
  int64_t read_row_count_;
  bool is_opened_;
};

}  // namespace observer
}  // namespace oceanbase
