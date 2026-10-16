/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DDL_REFRESH_H_
#define OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DDL_REFRESH_H_

#include "share/scn.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace obrpc
{
struct ObAsyncIndexRefreshArg;
}
namespace storage
{
class ObLS;
class ObAsyncIndexService;
class ObIAsyncIndexDepProvider;
class ObTabletAsyncIndexRefreshInfo;

// Storage operations used by one DDL refresh attempt. Implementations release
// their transactions before returning so capacity recovery can run safely.
class ObIAsyncIndexDDLRefreshOp
{
public:
  virtual ~ObIAsyncIndexDDLRefreshOp() {}
  virtual int maintain_refresh_info(ObTabletAsyncIndexRefreshInfo &info, bool &capacity_full) = 0;
  virtual int read_refresh_info(ObTabletAsyncIndexRefreshInfo &info) = 0;
  virtual int refresh_index(const int64_t snapshot, const int64_t index) = 0;
  // 默认使用系统时间；测试替身可控制时间，确定性验证刷新途中到达 deadline。
  virtual int64_t current_time_us() const;
  VIRTUAL_TO_STRING_KV("op", "async_index_ddl_refresh");
};

// Executes local storage operations for one refresh RPC. The caller owns the
// LS handle and RPC refresh ownership for the entire operation's lifetime.
class ObAsyncIndexDDLRefreshOp : public ObIAsyncIndexDDLRefreshOp
{
public:
  ObAsyncIndexDDLRefreshOp(
      ObLS &ls,
      ObAsyncIndexService &service,
      ObIAsyncIndexDepProvider &dep_provider,
      const obrpc::ObAsyncIndexRefreshArg &arg,
      const share::SCN &target_scn);
  virtual ~ObAsyncIndexDDLRefreshOp() {}
  virtual int maintain_refresh_info(ObTabletAsyncIndexRefreshInfo &info, bool &capacity_full) override;
  virtual int read_refresh_info(ObTabletAsyncIndexRefreshInfo &info) override;
  virtual int refresh_index(const int64_t snapshot, const int64_t index) override;
  virtual int64_t to_string(char *buf, const int64_t buf_len) const override;
private:
  ObLS &ls_;
  ObAsyncIndexService &service_;
  ObIAsyncIndexDepProvider &dep_provider_;
  const obrpc::ObAsyncIndexRefreshArg &arg_;
  const share::SCN target_scn_;
};

// 容量不足时最多消费两个已提交 checkpoint，再尝试维护并登记目标。
// 目标已登记后按 checkpoint 顺序持续刷新到覆盖目标，不限制为两个 checkpoint。
// 容量或刷新竞争等可重试错误交给 DDL 调度器处理；整个过程受同一 RPC deadline 约束。
// 即使只有一个异步索引，也必须推进到 DDL 所需的目标版本，不能按索引数量跳过。
int advance_async_index_ddl_refresh(
    const int64_t target_snapshot,
    const int64_t index_count,
    const int64_t deadline_us,
    ObIAsyncIndexDDLRefreshOp &refresh_op);

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_ASYNC_INDEX_OB_ASYNC_INDEX_DDL_REFRESH_H_
