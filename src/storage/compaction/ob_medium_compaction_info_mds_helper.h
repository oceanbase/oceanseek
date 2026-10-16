/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_COMPACTION_OB_MEDIUM_COMPACTION_INFO_MDS_HELPER_H_
#define OCEANBASE_STORAGE_COMPACTION_OB_MEDIUM_COMPACTION_INFO_MDS_HELPER_H_

#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/compaction/ob_medium_compaction_info.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace share
{
class SCN;
}

namespace common
{
class ObMySQLTransaction;
}

namespace storage
{
namespace mds
{
struct BufferCtx;
}

struct ObTabletMediumCompactionInfoMdsItem
{
  OB_UNIS_VERSION(1);
public:
  explicit ObTabletMediumCompactionInfoMdsItem(common::ObIAllocator &allocator);
  ~ObTabletMediumCompactionInfoMdsItem();
  void reset();
  bool is_valid() const;
  TO_STRING_KV(KP_(allocator), K_(tablet_id), K_(medium_info));

public:
  common::ObIAllocator *allocator_;
  common::ObTabletID tablet_id_;
  compaction::ObMediumCompactionInfo medium_info_;
};

struct ObTabletMediumCompactionInfoMdsArg final
{
  OB_UNIS_VERSION(1);
public:
  ObTabletMediumCompactionInfoMdsArg();
  ~ObTabletMediumCompactionInfoMdsArg();
  void reset();
  bool is_valid() const;
  int64_t get_tablet_count() const { return items_.count(); }
  int add_item(
      const common::ObTabletID &tablet_id,
      const compaction::ObMediumCompactionInfo &medium_info);
  TO_STRING_KV(K_(ls_id), K_(items));

public:
  // 该字段原名为 tenant_id_。租户恢复时 MDS 数据会回放到新租户，但该字段仍是源租户 ID，
  // 不能用于当前租户的事务启动和 MDS 注册，相关操作应使用 MTL_ID()。
  // 将其重命名为 unused_ 以避免误用，但是不能直接删除，否则会改变 MDS 参数的序列化布局，
  // 破坏已有数据的反序列化兼容性。
  uint64_t unused_;
  share::ObLSID ls_id_;
  common::ObSArray<ObTabletMediumCompactionInfoMdsItem *> items_;
  common::ObArenaAllocator allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObTabletMediumCompactionInfoMdsArg);
};

class ObTabletMediumCompactionInfoMdsHelper
{
public:
  static int on_register(const char *buf, const int64_t len, mds::BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN &scn, mds::BufferCtx &ctx);
  static int register_mds(const ObTabletMediumCompactionInfoMdsArg &arg, common::ObMySQLTransaction &trans);
  static int submit_medium_compaction_infos(const ObTabletMediumCompactionInfoMdsArg &arg);

private:
  static int process_batch(
      const ObTabletMediumCompactionInfoMdsArg &arg,
      const share::SCN &scn,
      mds::BufferCtx &ctx,
      const bool for_replay);
  static int process_one(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const compaction::ObMediumCompactionInfo &medium_info,
      const share::SCN &scn,
      mds::BufferCtx &ctx,
      const bool for_replay);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_COMPACTION_OB_MEDIUM_COMPACTION_INFO_MDS_HELPER_H_
