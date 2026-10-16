/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITER_H_

#include "storage/blocksstable/ob_dag_macro_block_writer.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
struct ObMacroDataSeq;
class ObDagSliceMacroFlusher;
class ObMacroSeqGenerator;
}

namespace storage
{
class ObStorageSchema;
class ObDDLIndependentDag;
struct ObDDLTabletContext;
class ObMacroMetaStoreMananger;
enum class ObDirectLoadTabletTarget : int8_t;

struct ObLobMacroWriteParam
{
  ObLobMacroWriteParam()
    : lob_col_idx_(OB_INVALID_INDEX_INT64),
      lob_id_interval_start_(0),
      lob_id_interval_end_(0),
      macro_seq_generator_(nullptr)
  {}
  bool is_valid() const
  {
    return lob_col_idx_ >= 0
        && lob_id_interval_start_ <= lob_id_interval_end_
        && nullptr != macro_seq_generator_;
  }
  TO_STRING_KV(K_(lob_col_idx), K_(lob_id_interval_start),
               K_(lob_id_interval_end), KP_(macro_seq_generator));

  int64_t lob_col_idx_;
  uint64_t lob_id_interval_start_;
  uint64_t lob_id_interval_end_;
  blocksstable::ObMacroSeqGenerator *macro_seq_generator_;
};

class ObCgMacroBlockWriter
{
public:
  ObCgMacroBlockWriter();
  ~ObCgMacroBlockWriter();
  int init(
      const ObWriteMacroParam &param,
      const ObDirectLoadTabletTarget target,
      const ObITable::TableKey &table_key,
      const ObMacroDataSeq &start_sequence,
      const int64_t row_offset);
  int init_lob(
      const ObWriteMacroParam &param,
      const ObDirectLoadTabletTarget target,
      const ObITable::TableKey &table_key,
      const ObMacroDataSeq &start_sequence,
      const int64_t row_offset,
      const ObLobMacroWriteParam &lob_write_param);
  void reset();
  int append_row(const blocksstable::ObDatumRow &cg_row);
  int append_batch(const blocksstable::ObBatchDatumRows &cg_rows);
  int close(ObDagSliceMacroFlusher *macro_block_flusher = nullptr);
  bool is_inited() const { return is_inited_; }
  int64_t get_last_macro_seq() const { return macro_block_writer_.get_last_macro_seq(); }
  inline int64_t get_macro_data_size() const { return macro_block_writer_.get_macro_data_size(); }
  int try_finish_last_micro_block() { return macro_block_writer_.try_finish_last_micro_block(); }
  TO_STRING_KV(K(is_inited_), K(macro_block_writer_), KP(ddl_redo_callback_), K(index_builder_), K(data_desc_));
  
private:
  DISABLE_COPY_ASSIGN(ObCgMacroBlockWriter);
  int inner_init(
      const ObWriteMacroParam &param,
      const ObDirectLoadTabletTarget target,
      const ObITable::TableKey &table_key,
      const ObMacroDataSeq &start_sequence,
      const int64_t row_offset,
      const ObLobMacroWriteParam *lob_write_param);

private:
  bool is_inited_;
  blocksstable::ObWholeDataStoreDesc data_desc_;
  blocksstable::ObSSTableIndexBuilder index_builder_;
  ObIMacroBlockFlushCallback *ddl_redo_callback_;
  blocksstable::ObDagSliceMacroBlockWriter macro_block_writer_;
};

} // end namespace storage
} // end namespace oceanbase

#endif //OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITER_H_
