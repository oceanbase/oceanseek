/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_TIERED_METADATA_STORE_
#define OCEANBASE_SHARED_STORAGE_TIERED_METADATA_STORE_

#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/container/ob_iarray.h"
#include "lib/ob_define.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"
#include "storage/tiered_metadata_store/ob_tiered_metadata_row_data.h"

namespace oceanbase
{
namespace storage
{

class ObTieredMetadataStore final
{
public:
  ObTieredMetadataStore()
    : is_inited_(false),
      tenant_id_(common::OB_INVALID_TENANT_ID),
      sql_client_(nullptr),
      operator_() {}

  ~ObTieredMetadataStore() {}

public:
  int init(const uint64_t tenant_id, common::ObISQLClient *sql_client);
  
  int read_meta(
      const blocksstable::MacroBlockId &macro_block_id,
      char *buf,
      const int64_t buf_size,
      int64_t &actual_size);
  int read_meta_row_data(
      const blocksstable::MacroBlockId &macro_block_id,
      common::ObIAllocator &allocator,
      ObTieredMetaTableRowData &row_data);

  int write_meta(
      const blocksstable::MacroBlockId &macro_block_id,
      const int64_t meta_type,
      const common::ObString &meta_value);
  int batch_write_meta(
      const common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids,
      const common::ObIArray<int64_t> &meta_types,
      const common::ObIArray<common::ObString> &meta_values);

  int del_meta(const blocksstable::MacroBlockId &macro_block_id);
  int batch_del_meta(
      const common::ObIArray<blocksstable::MacroBlockId> &macro_block_ids);

  int is_meta_exist(
      const blocksstable::MacroBlockId &macro_block_id,
      bool &is_exist);

  // list all sub-meta macro files under a given tablet directory
  int list_meta_macro_files(
      const bool is_ls_inner_tablet,
      const int64_t ls_id,
      const int64_t tablet_id,
      const int64_t reorganization_scn,
      common::ObIArray<blocksstable::MacroBlockId> &macro_ids);

  // get iterator for [start_macro_block_id, end_macro_block_id] inclusive range
  int get_tiered_metadata_iter(
      const blocksstable::MacroBlockId &start_macro_block_id,
      const blocksstable::MacroBlockId &end_macro_block_id,
      ObSSTieredMetaTableIterator &iter);

private:
  int pre_check_();
  bool is_valid_macro_block_id_(const blocksstable::MacroBlockId &macro_block_id) const;
  int macro_block_id_to_ss_meta_key_(
      const blocksstable::MacroBlockId &macro_block_id,
      share::ObLSID &ls_id,
      ObSSTieredMetaType &meta_type,
      char *key_buf,
      const int64_t key_buf_size,
      int64_t &key_len);
  int macro_block_id_to_path_(
      const blocksstable::MacroBlockId &macro_block_id,
      char *buf,
      const int64_t buf_size,
      int64_t &pos);
  int compare_with_existing_value_when_write_conflict_(
      const blocksstable::MacroBlockId &macro_block_id,
      const common::ObString &meta_value);

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObISQLClient *sql_client_;
  ObSSTieredMetaTableOperator operator_;

  DISALLOW_COPY_AND_ASSIGN(ObTieredMetadataStore);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_TIERED_METADATA_STORE_
