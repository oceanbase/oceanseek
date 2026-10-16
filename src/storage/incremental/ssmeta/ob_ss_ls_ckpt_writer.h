/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_LS_CKPT_WRITER_H_
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_LS_CKPT_WRITER_H_

#include "lib/string/ob_string.h"
#include "share/ob_ls_id.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_data.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"

namespace oceanbase
{
namespace common
{
class ObArenaAllocator;
}

namespace blocksstable
{
class MacroBlockId;
class ObSSTable;
}

namespace storage
{

class ObSSTableArray;
class ObSSTieredMetaTableIterator;
class ObTablet;
class ObTabletMacroInfo;
class ObTabletTableStore;
class ObTieredMetadataStore;

// SYS_LS checkpoint 的写入入口，负责收集 LS meta、上传 tablet meta 及其
// 行存 sub-meta，并最终写入 checkpoint 对象。
class ObSSLsCkptWriter final
{
public:
  explicit ObSSLsCkptWriter(const bool need_check_existing_object);

  // Public interfaces for DAG stages: Prepare collects LS meta, Upload uploads one tablet,
  // and Finish builds the list from tablet keys and writes the checkpoint root object.
  int collect_ls_meta(
      const share::ObLSID &ls_id,
      const share::SCN &meta_scn,
      ObSSLSMeta &ls_meta) const;
  int upload_tablet_meta(const ObSSMetaTabletKey &tablet_key);
  int write_checkpoint_object(
      const share::ObLSID &ls_id,
      const ObSSLsCkptData &ckpt_data) const;

private:
  int write_tablet_meta_(
      const ObSSMetaTabletKey &tablet_key,
      const common::ObString &tablet_meta_value,
      ObTieredMetadataStore &tiered_store,
      common::ObArenaAllocator &allocator,
      blocksstable::MacroBlockId &obj_macro_id);
  void init_write_tablet_context_(
      const ObSSMetaTabletKey &tablet_key,
      ObTieredMetadataStore &tiered_store,
      common::ObArenaAllocator &allocator);
  void reset_write_tablet_context_();
  // 从原始 tablet meta value 构造后续改写和序列化所需的 tablet_meta_，
  // 同时完成 data version 解析、tablet 身份校验和 macro info 加载。
  int build_tablet_meta_(const common::ObString &tablet_meta_value);
  int rewrite_tablet_meta_(
      const common::ObString &tablet_meta_value,
      common::ObString &rewritten_value);
  int serialize_tablet_meta_(common::ObString &rewritten_value);
  int upload_sub_meta_and_redirect_tablet_();
  int precheck_and_upload_table_store_(
      const blocksstable::MacroBlockId &table_store_macro_id);
  int upload_table_store_(
      const blocksstable::MacroBlockId &table_store_macro_id);
  int upload_sstables_and_rewrite_table_store_(
      const common::ObString &table_store_value,
      common::ObString &rewritten_value);
  int serialize_table_store_(
      const ObTabletTableStore &table_store,
      common::ObString &rewritten_value);
  int check_table_store_sstables_(
      const ObTabletTableStore &table_store) const;
  int check_object_storage_table_store_() const;
  int get_tiered_meta_(
      const blocksstable::MacroBlockId &in_table_macro_id,
      ObSSTieredMetaTableIterator &iter,
      common::ObString &meta_value) const;
  int upload_and_redirect_sstables_(const ObSSTableArray &sstable_array);
  int upload_and_redirect_sstable_(blocksstable::ObSSTable &sstable);
  int upload_tiered_meta_(
      const blocksstable::MacroBlockId &in_table_macro_id);
  int validate_referenced_sub_meta_() const;
  int redirect_tablet_to_object_storage_();

private:
  bool need_check_existing_object_;
  ObSSMetaTabletKey tablet_key_;
  ObSSTabletMetaValue tablet_meta_;
  ObTablet *tablet_;
  ObTabletMacroInfo *macro_info_;
  ObTieredMetadataStore *tiered_store_;
  common::ObArenaAllocator *allocator_;

  DISALLOW_COPY_AND_ASSIGN(ObSSLsCkptWriter);
};

} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_LS_CKPT_WRITER_H_
