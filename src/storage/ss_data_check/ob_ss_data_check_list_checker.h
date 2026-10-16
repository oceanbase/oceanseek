/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_CHECKER_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_CHECKER_H_

#include "common/storage/ob_device_common.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/restore/ob_storage_info.h"
#include "share/ob_check_stop_provider.h"
#include "storage/ss_data_check/ob_ss_data_check_ctx.h"
#include "storage/ss_data_check/ob_ss_data_check_list_sorter.h"
#include "storage/ss_data_check/ob_ss_data_check_meta_ref_checker.h"

namespace oceanbase
{
namespace storage
{

class ObSSDataCheckListCheckUtil final
{
public:
  static int get_parent_prefix(
      const common::ObString &relative_path,
      char *prefix_buf,
      const int64_t prefix_buf_len,
      common::ObString &prefix);
  static bool has_exact_parent_prefix(
      const common::ObString &relative_path,
      const common::ObString &prefix);
};

class ObISSDataCheckObjectLister
{
public:
  virtual ~ObISSDataCheckObjectLister() = default;
  virtual int list_files(
      const common::ObString &relative_prefix,
      common::ObBaseDirEntryOperator &op) = 0;
};

// 将租户共享存储根目录和相对前缀拼成绝对 URI，再调用通用 LIST 接口。
// 该类不实现 LIST 流控；后续由通用 LIST 接口统一提供流控能力。
class ObSSDataCheckObjectLister final : public ObISSDataCheckObjectLister
{
public:
  ObSSDataCheckObjectLister();
  ~ObSSDataCheckObjectLister() = default;

  int init(
      const common::ObString &tenant_root_uri,
      const common::ObObjectStorageInfo *storage_info);
  int list_files(
      const common::ObString &relative_prefix,
      common::ObBaseDirEntryOperator &op) override;
  void reset();
  bool is_valid() const;

private:
  bool is_inited_;
  char tenant_root_uri_[common::OB_MAX_URI_LENGTH];
  const common::ObObjectStorageInfo *storage_info_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckObjectLister);
};

struct ObSSDataCheckListCompareStat final
{
  ObSSDataCheckListCompareStat();
  void reset();
  bool is_valid() const;

  int64_t expected_count_;
  int64_t found_count_;
  int64_t missing_count_;
  int64_t referenced_missing_count_;
  int64_t unreferenced_missing_count_;
  int64_t listed_object_count_;
  int64_t listed_prefix_count_;

  TO_STRING_KV(K_(expected_count), K_(found_count), K_(missing_count),
      K_(referenced_missing_count), K_(unreferenced_missing_count),
      K_(listed_object_count), K_(listed_prefix_count));
};

// 对最终 Expected 有序文件和经过有界外排排序的 LIST 结果做流式归并比较。
// 对 LIST 中缺失的项，只反查 ExpectedItem 保留的精确 shared meta 来源。
class ObSSDataCheckListChecker final
{
public:
  ObSSDataCheckListChecker();
  ~ObSSDataCheckListChecker() = default;

  int init(
      const uint64_t tenant_id,
      const int64_t file_buf_size,
      const int64_t io_expire_timestamp,
      ObISSDataCheckObjectLister &object_lister,
      const ObISSDataCheckMetaRefChecker &meta_ref_checker,
      const share::ObCheckStopProvider &stop_checker,
      ObSSDataCheckCtx &ctx);
  // 调用方必须持有 Context 的 active task 引用，并将返回码传给
  // dec_active_task_count()；失败后不能在同一 Context 上透明重试。
  int check(
      const ObSSDataCheckSortedFileInfo &expected_file,
      const int64_t expected_item_count,
      ObSSDataCheckListCompareStat &stat);
  void reset();
  bool is_valid() const;

private:
  // 该 key 精确对应一行 shared tablet meta，不包含宏块维度。
  class MetaRefKey final
  {
  public:
    MetaRefKey();
    explicit MetaRefKey(const ObSSDataCheckExpectedItem &item);
    bool is_valid() const;
    bool operator==(const MetaRefKey &other) const;
    uint64_t hash() const;
    int hash(uint64_t &hash_value) const;

    TO_STRING_KV(K_(ls_id), K_(tablet_id),
        K_(reorganize_scn), K_(meta_scn));

  private:
    share::ObLSID ls_id_;
    common::ObTabletID tablet_id_;
    share::SCN reorganize_scn_;
    share::SCN meta_scn_;
  };

  class MetaRefCache final : public ObISSDataCheckMetaRefChecker
  {
  public:
    MetaRefCache();
    ~MetaRefCache();

    int init(
        const uint64_t tenant_id,
        const int64_t expected_item_count,
        const ObISSDataCheckMetaRefChecker &meta_ref_checker);
    int check_reference_exist(
        const ObSSDataCheckExpectedItem &item,
        bool &is_exist) const override;
    void reset();
    bool is_valid() const;

  private:
    // 缓存只在单次串行 check() 内使用，无需并发保护。
    typedef common::hash::ObHashMap<MetaRefKey, bool,
        common::hash::NoPthreadDefendMode> ResultMap;

    bool is_inited_;
    uint64_t tenant_id_;
    int64_t bucket_count_;
    const ObISSDataCheckMetaRefChecker *meta_ref_checker_;
    mutable ResultMap result_map_;
    DISALLOW_COPY_AND_ASSIGN(MetaRefCache);
  };

  class ListStopChecker final : public share::ObCheckStopProvider
  {
  public:
    ListStopChecker(
        const share::ObCheckStopProvider &stop_checker,
        const int64_t io_expire_timestamp);
    virtual int check_stop() const override;

  private:
    const share::ObCheckStopProvider &stop_checker_;
    const int64_t io_expire_timestamp_;
    DISALLOW_COPY_AND_ASSIGN(ListStopChecker);
  };

  class ExpectedCursor final
  {
  public:
    explicit ExpectedCursor(const int64_t page_size);
    int init(
        const ObSSDataCheckSortedFileInfo &expected_file,
        const int64_t expected_count,
        const uint64_t tenant_id,
        const int64_t io_expire_timestamp,
        const int64_t file_buf_size);
    int advance();
    bool has_current() const;
    const ObSSDataCheckExpectedSortItem &current() const;
    bool is_complete() const;

  private:
    ObSSDataCheckExpectedPathIterator iterator_;
    const ObSSDataCheckExpectedSortItem *current_;
    int64_t expected_count_;
    int64_t fetched_count_;
    bool is_end_;
    char previous_path_buf_[common::OB_MAX_URI_LENGTH];
    DISALLOW_COPY_AND_ASSIGN(ExpectedCursor);
  };

  class PrefixCompareOp final : public common::ObBaseDirEntryOperator
  {
  public:
    PrefixCompareOp(
        const common::ObString &prefix,
        ExpectedCursor &cursor,
        const ObISSDataCheckMetaRefChecker &meta_ref_checker,
        const share::ObCheckStopProvider &stop_checker,
        ObSSDataCheckCtx &ctx,
        ObSSDataCheckListCompareStat &stat);
    virtual int func(const dirent *entry) override;
    int drain_missing();

  private:
    int compare_one_actual_(const common::ObString &actual_name);
    int handle_missing_();

  private:
    common::ObString prefix_;
    ExpectedCursor &cursor_;
    const ObISSDataCheckMetaRefChecker &meta_ref_checker_;
    const share::ObCheckStopProvider &stop_checker_;
    ObSSDataCheckCtx &ctx_;
    ObSSDataCheckListCompareStat &stat_;
    char previous_name_buf_[common::OB_MAX_URI_LENGTH];
    common::ObString previous_name_;
    DISALLOW_COPY_AND_ASSIGN(PrefixCompareOp);
  };

  bool is_inited_;
  uint64_t tenant_id_;
  int64_t file_buf_size_;
  int64_t io_expire_timestamp_;
  ObISSDataCheckObjectLister *object_lister_;
  const ObISSDataCheckMetaRefChecker *meta_ref_checker_;
  const share::ObCheckStopProvider *stop_checker_;
  ObSSDataCheckCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckListChecker);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_LIST_CHECKER_H_
