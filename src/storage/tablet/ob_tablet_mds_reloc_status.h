/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_MDS_RELOC_STATUS_H_
#define OCEANBASE_STORAGE_OB_TABLET_MDS_RELOC_STATUS_H_

#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"

namespace oceanbase
{
namespace storage
{

// 源端 MDS reloc 对本 reorganization 代际目的端 tablet 的状态（DummyKey 单行）。
//
// - 独立于 TabletStatus unit：reloc 事务 final batch（apply_final_marker_）写本类型，
//   不再往 ObTabletCreateDeleteMdsUserData 上叠加未提交版本，因此 tablet status 的
//   提交门禁（latest MDS TwoPhaseCommitState 判定）不会被 reloc 干扰。
// - 代际语义由 LS_MDS_TABLET rowkey 第二列（reorganization scn）承担：
//   marker 行写在 (tablet_id, dest_reorg_scn) 下，stale final redo 只会落在
//   旧代际 rowkey 上，新代际读者结构上不可见，无需显式清零。
// - 行不存在（OB_EMPTY_RESULT）即视为本代际 reloc 未完成。
// - 老代际行随 (tablet_id, transfer_scn) group 由 LS mds tablet GC 通用路径回收。
class ObTabletMdsRelocStatus
{
  OB_UNIS_VERSION(1);
public:
  ObTabletMdsRelocStatus() : reloc_done_(false) {}
  ~ObTabletMdsRelocStatus() = default;

  // 当前只会以 reloc_done_ = true 的形态写入；后续可扩展更多状态字段
  bool is_valid() const { return reloc_done_; }
  void reset() { reloc_done_ = false; }
  int assign(const ObTabletMdsRelocStatus &other)
  {
    reloc_done_ = other.reloc_done_;
    return common::OB_SUCCESS;
  }

  TO_STRING_KV(K_(reloc_done));

  bool reloc_done_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MDS_RELOC_STATUS_H_
