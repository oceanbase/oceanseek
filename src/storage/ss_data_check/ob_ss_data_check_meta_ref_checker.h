/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_META_REF_CHECKER_H_
#define OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_META_REF_CHECKER_H_

#include "storage/ss_data_check/ob_ss_data_check_define.h"

namespace oceanbase
{
namespace storage
{

class ObSSMetaService;

// 该抽象用于单测注入 shared meta 反查结果，生产路径使用
// ObSSDataCheckMetaRefChecker。
class ObISSDataCheckMetaRefChecker
{
public:
  virtual ~ObISSDataCheckMetaRefChecker() = default;
  virtual int check_reference_exist(
      const ObSSDataCheckExpectedItem &item,
      bool &is_exist) const = 0;
};

// 仅检查 ExpectedItem 中保留的精确 shared meta 来源行是否存在，
// 不解析行内的 mark-delete 标记，也不搜索同一宏块的其他引用关系。
class ObSSDataCheckMetaRefChecker final : public ObISSDataCheckMetaRefChecker
{
public:
  ObSSDataCheckMetaRefChecker();
  ~ObSSDataCheckMetaRefChecker() = default;

  int init(ObSSMetaService &meta_service);
  int check_reference_exist(
      const ObSSDataCheckExpectedItem &item,
      bool &is_exist) const override;
  void reset();
  bool is_valid() const;

private:
  bool is_inited_;
  ObSSMetaService *meta_service_;
  DISALLOW_COPY_AND_ASSIGN(ObSSDataCheckMetaRefChecker);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_SS_DATA_CHECK_OB_SS_DATA_CHECK_META_REF_CHECKER_H_
