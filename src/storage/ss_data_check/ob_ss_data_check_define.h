/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_DEFINE_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_DEFINE_H_

#include "lib/string/ob_string.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/garbage_collector/ob_mark_sweep_tmp_file_mgr.h"

namespace oceanbase
{
namespace storage
{

enum class ObSSDataCheckStatus : uint8_t
{
  HEALTH = 0,
  NOT_HEALTH
};

// OFF 关闭后续巡检调度；SINGLE 覆盖租户全部逻辑 tablet，但每个 tablet
// 只选择快照内最新版本；FULL 覆盖快照内全部 tablet meta 版本。
enum class ObSSDataCheckMode : uint8_t
{
  SINGLE = 0,
  FULL,
  OFF,
  MAX
};

class ObSSDataCheckModeUtil final
{
public:
  static bool is_valid(const ObSSDataCheckMode mode);
  static bool is_enabled(const ObSSDataCheckMode mode);
  static int parse(
      const common::ObString &mode_str,
      ObSSDataCheckMode &mode);
  static const char *to_string(const ObSSDataCheckMode mode);
};

// Expected item 只持久化宏块标识和 shared meta 来源信息。
// 排序使用的相对路径由外排流水线按需生成，不写入临时文件。
class ObSSDataCheckExpectedItem final
{
  static const int32_t DATA_CHECK_EXPECTED_ITEM_VERSION = 1;
  OB_UNIS_VERSION(DATA_CHECK_EXPECTED_ITEM_VERSION);
public:
  ObSSDataCheckExpectedItem();
  ~ObSSDataCheckExpectedItem() = default;

  int init(
      const blocksstable::MacroBlockId &macro_id,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const share::SCN &reorganize_scn,
      const share::SCN &meta_scn);
  void reset();
  bool is_valid() const;
  int assign(const ObSSDataCheckExpectedItem &other);

  bool is_same_object(const ObSSDataCheckExpectedItem &other) const;
  bool operator==(const ObSSDataCheckExpectedItem &other) const;
  bool operator!=(const ObSSDataCheckExpectedItem &other) const;

  int64_t get_deep_copy_size() const;
  int deep_copy(
      const ObSSDataCheckExpectedItem &src,
      char *buf,
      const int64_t buf_len,
      int64_t &pos);

  const blocksstable::MacroBlockId &get_macro_id() const { return macro_id_; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  const share::SCN &get_reorganize_scn() const { return reorganize_scn_; }
  const share::SCN &get_meta_scn() const { return meta_scn_; }

  TO_STRING_KV(K_(macro_id), K_(ls_id), K_(tablet_id),
               K_(reorganize_scn), K_(meta_scn));

private:
  blocksstable::MacroBlockId macro_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorganize_scn_;
  share::SCN meta_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckExpectedItem);
};

// Expected item 支持自描述，因此 ObFragmentReaderV2 可使用默认 item 初始化，
// 无需持久化样例 key。
class ObSSDataCheckSortedFileInfo final
{
public:
  ObSSDataCheckSortedFileInfo();
  ~ObSSDataCheckSortedFileInfo();

  int init(const ObMarkSweepTmpFileHandle &file_handle, const int64_t item_count);
  int assign(const ObSSDataCheckSortedFileInfo &other);
  void reset();
  bool is_valid() const;

  int64_t get_fd() const { return file_handle_.get_fd(); }
  int64_t get_dir_id() const { return file_handle_.get_dir_id(); }
  uint64_t get_tenant_id() const { return file_handle_.get_tenant_id(); }
  int64_t get_item_count() const { return item_count_; }
  const ObMarkSweepTmpFileHandle &get_file_handle() const { return file_handle_; }

  TO_STRING_KV(K_(file_handle), K_(item_count));

private:
  ObMarkSweepTmpFileHandle file_handle_;
  int64_t item_count_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckSortedFileInfo);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_DEFINE_H_
