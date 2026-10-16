/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_INCREMENTAL_OB_SS_MINI_MERGE_CTX_H_
#define OCEANBASE_STORAGE_INCREMENTAL_OB_SS_MINI_MERGE_CTX_H_
#include "storage/compaction/ob_tablet_merge_ctx.h"
namespace oceanbase
{
namespace compaction
{
class ObTabletSSMiniMergeCtx : public ObTabletMiniMergeCtx
{
public:
  ObTabletSSMiniMergeCtx(ObTabletMergeDagParam &param, common::ObArenaAllocator &allocator);
  ~ObTabletSSMiniMergeCtx() {}
protected:
  virtual int get_merge_tables(ObGetMergeTablesResult &merge_tables_result) override;
  virtual void after_update_tablet(ObTabletHandle &new_tablet_handle) override;
private:
  void register_upload_task_(ObTabletHandle &new_tablet_handle);
private:
  ObSSTableUploadRegHandle upload_register_handle_;
};
} // namespace compaction
} // namespace oceanbase
#endif
