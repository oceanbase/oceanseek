/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "storage/ddl/ob_ddl_inc_log_basic.h"
#include "storage/ob_i_table.h"
#include "lib/allocator/page_arena.h"
#include "share/scn.h"

namespace oceanbase
{

namespace blocksstable
{
class ObSSTable;
}

namespace storage
{
class ObStorageSchema;

class ObDDLIncStartLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncStartLog();
  ~ObDDLIncStartLog();
  int init(const ObDDLIncLogBasic &log_basic,
          const bool has_cs_replica,
          const ObStorageSchema *storage_schema = nullptr);
  bool is_valid() const;
  void reset();
  const ObDDLIncLogBasic &get_log_basic() const { return log_basic_; }
  bool has_cs_replica() const { return has_cs_replica_; }
  const ObStorageSchema *get_storage_schema() const { return storage_schema_; }
  TO_STRING_KV(K_(log_basic), K_(has_cs_replica), KP_(storage_schema));
private:
  int set_storage_schema(const ObStorageSchema &other);
private:
  ObDDLIncLogBasic log_basic_;
  bool has_cs_replica_;
  ObStorageSchema *storage_schema_;
  ObArenaAllocator allocator_;
};

class ObDDLIncCommitLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncCommitLog();
  ~ObDDLIncCommitLog();
  int init(const ObDDLIncLogBasic &log_basic, const bool is_rollback, const share::SCN start_scn = share::SCN::base_scn());
  int set_ss_inc_major(const blocksstable::ObSSTable *data_inc_major,
                       const blocksstable::ObSSTable *lob_meta_inc_major,
                       const blocksstable::ObSSTable *lob_piece_inc_major,
                       const bool need_check_ddl_checkpoint);
  bool is_valid() const { return log_basic_.is_valid(); }
  const ObDDLIncLogBasic &get_log_basic() const { return log_basic_; }
  bool is_rollback() const { return is_rollback_; }
  bool is_co_sstable() const { return is_co_sstable_; }
  bool need_check_ddl_checkpoint() const { return need_check_ddl_checkpoint_; }
  ObITable::TableType get_table_type() const { return table_type_; }
  const ObString &get_data_inc_major_buffer() const { return data_inc_major_buffer_; }
  const ObString &get_lob_meta_inc_major_buffer() const { return lob_meta_inc_major_buffer_; }
  const ObString &get_lob_piece_inc_major_buffer() const { return lob_piece_inc_major_buffer_; }
  share::SCN get_start_scn() const { return start_scn_; }
  TO_STRING_KV(K_(log_basic), K_(is_rollback), K_(is_co_sstable), K_(data_inc_major_buffer),
      K_(lob_meta_inc_major_buffer), K_(lob_piece_inc_major_buffer), K_(start_scn),
      K_(need_check_ddl_checkpoint), K_(table_type));
private:
  ObArenaAllocator allocator_;
  ObDDLIncLogBasic log_basic_;
  bool is_rollback_;
  bool is_co_sstable_;
  ObString data_inc_major_buffer_;
  ObString lob_meta_inc_major_buffer_;
  ObString lob_piece_inc_major_buffer_;
  share::SCN start_scn_;
  bool need_check_ddl_checkpoint_;
  ObITable::TableType table_type_;
};

} // namespace storage
} // namespace oceanbase
