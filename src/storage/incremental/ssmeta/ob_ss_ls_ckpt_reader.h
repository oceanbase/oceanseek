/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SS_LS_CKPT_READER_H_
#define OCEANBASE_STORAGE_OB_SS_LS_CKPT_READER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/hash/ob_hashset.h"
#include "lib/string/ob_string.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/incremental/ssmeta/ob_ss_ls_ckpt_data.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/sslog/ob_sslog_kv_palf_adapter.h"

namespace oceanbase
{
namespace storage
{

class ObTabletHandle;

// SYS_LS checkpoint 的读取入口。
//
// 读取链路不依赖 __all_shared_meta：既可以从 PALF_KV 获取当前已发布
// 的 checkpoint MacroBlockId，也可以从已知的 MacroBlockId 直接读取。
// 两条入口最终都加载 ObSSLsCkptData 和其中的 tablet meta 对象。
// checkpoint writer 已经把 tablet、table store 以及 SSTable/CG 中的表内
// sub-meta 地址转换成对象存储地址，reader 只负责读取并反序列化，不再修改
// 共享元数据对象。
class ObSSLsCkptReader final
{
public:
  ObSSLsCkptReader();
  ~ObSSLsCkptReader();

  int init();
  void destroy();

  // 读取 ls_id 当前已发布的 checkpoint。任何失败都会清空之前的加载状态。
  int load(const share::ObLSID &ls_id);

  // 读取已知对象地址对应的 checkpoint，不访问 PALF_KV。
  // 任何失败都会清空之前的加载状态。
  int load_checkpoint(
      const share::ObLSID &ls_id,
      const blocksstable::MacroBlockId &ckpt_macro_id);

  bool is_loaded() const { return is_loaded_; }
  // load 成功后返回本次固定的 checkpoint；未加载或失败时返回无效地址。
  const blocksstable::MacroBlockId &get_ckpt_macro_id() const { return ckpt_macro_id_; }
  const ObSSLSMeta &get_ls_meta() const { return ckpt_data_.ls_meta_; }
  const share::SCN &get_ckpt_scn() const { return ckpt_data_.ckpt_scn_; }
  const share::SCN &get_reserved_meta_scn() const { return ckpt_data_.reserved_meta_scn_; }
  const common::ObIArray<blocksstable::MacroBlockId> &get_tablet_meta_macro_ids() const
  {
    return ckpt_data_.tablet_meta_keys_;
  }

  // 读取 checkpoint 中的一个 tablet meta，构建可继续读取 OSS sub-meta 的 handle。
  int read_tablet(const blocksstable::MacroBlockId &tablet_meta_macro_id,
                  common::ObArenaAllocator &allocator,
                  ObTabletHandle &tablet_handle,
                  ObSSUpdateTabletLog &update_log) const;

  TO_STRING_KV(K_(is_inited), K_(is_loaded), K_(ckpt_macro_id), K_(ckpt_data));

private:
  int ensure_palf_kv_inited_();
  int read_published_ckpt_macro_id_(
      const share::ObLSID &ls_id,
      blocksstable::MacroBlockId &ckpt_macro_id);
  static int read_object_(const blocksstable::MacroBlockId &macro_id,
                          common::ObArenaAllocator &allocator,
                          common::ObString &object_data);
  int load_checkpoint_object_(
      const share::ObLSID &ls_id,
      const blocksstable::MacroBlockId &ckpt_macro_id);
  int validate_ckpt_payload_(
      const share::ObLSID &ls_id,
      const blocksstable::MacroBlockId &ckpt_macro_id) const;
  int parse_tablet_meta_key_(
      const share::ObLSID &ls_id,
      const blocksstable::MacroBlockId &tablet_meta_macro_id,
      ObSSMetaTabletKey &tablet_key) const;
  int build_tablet_meta_set_();
  void reset_loaded_data_();
  bool contains_tablet_meta_(
      const blocksstable::MacroBlockId &tablet_meta_macro_id) const;

private:
  bool is_inited_;
  bool palf_kv_inited_;
  bool is_loaded_;
  sslog::ObSSLogKVPalfAdapter palf_kv_adapter_;
  blocksstable::MacroBlockId ckpt_macro_id_;
  ObSSLsCkptData ckpt_data_;
  common::hash::ObHashSet<blocksstable::MacroBlockId,
                          common::hash::NoPthreadDefendMode> tablet_meta_set_;

  DISALLOW_COPY_AND_ASSIGN(ObSSLsCkptReader);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SS_LS_CKPT_READER_H_
