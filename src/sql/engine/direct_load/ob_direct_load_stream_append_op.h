/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_DIRECT_LOAD_STREAM_APPEND_OP_H_
#define OB_DIRECT_LOAD_STREAM_APPEND_OP_H_

#include "sql/engine/dml/ob_table_insert_op.h"
#include "observer/table_load/stream/ob_direct_load_stream_buffer_queue.h"
#include "observer/table_load/stream/ob_direct_load_stream_chunk.h"
#include "observer/table_load/stream/ob_direct_load_stream_meta_store.h"

namespace oceanbase
{
namespace sql
{

class ObDirectLoadStreamAppendSpec : public ObTableInsertSpec
{
  OB_UNIS_VERSION_V(1);
public:
  ObDirectLoadStreamAppendSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObTableInsertSpec(alloc, type),
      stream_id_(OB_INVALID_ID),
      target_schema_version_(OB_INVALID_VERSION),
      target_column_ids_(alloc)
  {
  }

  INHERIT_TO_STRING_KV("ObTableInsertSpec", ObTableInsertSpec,
                       K_(stream_id),
                       K_(target_schema_version),
                       K_(target_column_ids));

  int64_t stream_id_;
  int64_t target_schema_version_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> target_column_ids_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamAppendSpec);
};

class ObDirectLoadStreamAppendOp : public ObTableInsertOp
{
public:
  ObDirectLoadStreamAppendOp(ObExecContext &ctx, const ObOpSpec &spec, ObOpInput *input);
  virtual ~ObDirectLoadStreamAppendOp();
  virtual int rescan() override;
protected:
  virtual int inner_open() override;
  virtual int inner_close() override;
  virtual int write_row_to_das_buffer() override;
  virtual int write_rows_post_proc(int last_errno) override;
private:
  int get_primary_insert_ctdef_(const ObInsCtDef *&primary_ins_ctdef) const;
  int get_primary_insert_defs_(const ObInsCtDef *&primary_ins_ctdef,
                               ObInsRtDef *&primary_ins_rtdef);
  int check_append_shape_(const ObInsCtDef &primary_ins_ctdef) const;
  int check_stream_meta_(const observer::ObDirectLoadStreamMeta &meta,
                         const ObInsCtDef &primary_ins_ctdef,
                         bool &need_finish_stream) const;
  int check_target_table_runtime_(const ObInsCtDef &primary_ins_ctdef) const;
  int init_append_context_(const ObInsCtDef &primary_ins_ctdef);
  int check_append_transaction_() const;
  int begin_queue_append_();
  int end_queue_append_(const int result_code);
  int begin_remote_append_(const observer::ObDirectLoadStreamMeta &meta);
  int append_remote_chunk_(const common::ObString &chunk);
  int finish_remote_append_(const int result_code);
  int finish_append_(const int result_code);
  bool is_append_registered_() const;
  int append_current_row_(const ObInsCtDef &primary_ins_ctdef,
                          ObInsRtDef &primary_ins_rtdef);
  int flush_chunk_();
  int mark_stream_failed_(const int ret_code, const char *error_msg, const bool require_queue_touched = true);
  int try_fail_stream_if_open_(const int error_code);
private:
  table::ObDirectLoadStreamQueueDesc queue_desc_;
  observer::ObDirectLoadStreamBufferQueueHandle queue_handle_;
  observer::ObDirectLoadStreamAppendHandle append_handle_;
  observer::ObDirectLoadStreamChunkWriter chunk_writer_;
  uint64_t statement_id_;
  int64_t absolute_deadline_;
  bool remote_append_started_;
  int64_t appended_row_count_;
  bool queue_touched_;
  bool stream_failed_reported_;
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamAppendOp);
};

class ObDirectLoadStreamAppendOpInput : public ObTableInsertOpInput
{
  OB_UNIS_VERSION_V(1);
public:
  ObDirectLoadStreamAppendOpInput(ObExecContext &ctx, const ObOpSpec &spec)
    : ObTableInsertOpInput(ctx, spec)
  {}
private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadStreamAppendOpInput);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OB_DIRECT_LOAD_STREAM_APPEND_OP_H_ */
