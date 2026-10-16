/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOCAL_LOOKUP_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOCAL_LOOKUP_ITER_H_

#include "sql/das/iter/ob_das_lookup_iter.h"
#include "storage/access/ob_dml_param.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ObLogicalTableHelper;

struct ObDASLocalLookupIterParam : public ObDASLookupIterParam
{
public:
  ObDASLocalLookupIterParam()
    : ObDASLookupIterParam(false /*local lookup*/),
      trans_desc_(nullptr),
      snapshot_(nullptr)
  {}
  transaction::ObTxDesc *trans_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  virtual bool is_valid() const override
  {
    return true;
  }
};

class ObDASScanCtDef;
class ObDASScanRtDef;
class ObDASFuncLookupIter;
class ObDASSearchCtx;
class ObDASLocalLookupIter : public ObDASLookupIter
{
public:
  ObDASLocalLookupIter(const ObDASIterType type = ObDASIterType::DAS_ITER_LOCAL_LOOKUP)
    : ObDASLookupIter(type),
      trans_info_array_(),
      lookup_param_(),
      lookup_tablet_id_(),
      lookup_ls_id_(),
      trans_desc_(nullptr),
      snapshot_(nullptr),
      async_search_ctx_(nullptr),
      is_first_lookup_(true),
      lt_enabled_(false),
      lt_physical_data_table_id_(OB_INVALID_ID)
  {}
  virtual ~ObDASLocalLookupIter() {}

  storage::ObTableScanParam &get_lookup_param() { return lookup_param_; }
  void set_tablet_id(const ObTabletID &tablet_id) { lookup_tablet_id_ = tablet_id; }
  void set_ls_id(const share::ObLSID &ls_id) { lookup_ls_id_ = ls_id; }
  void set_async_search_ctx(ObDASSearchCtx *async_search_ctx)
  {
    async_search_ctx_ = async_search_ctx;
  }
  void set_logical_table_redirect(bool enabled, uint64_t physical_data_table_id) {
    lt_enabled_ = enabled;
    lt_physical_data_table_id_ = physical_data_table_id;
  }
  int init_scan_param(storage::ObTableScanParam &param, const ObDASScanCtDef *ctdef, ObDASScanRtDef *rtdef);

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void reset_lookup_state() override;

  virtual int add_rowkey() override;
  virtual int add_rowkeys(int64_t count) override;
  virtual int do_index_lookup() override;
  virtual int check_index_lookup() override;
protected:
  int init_rowkey_exprs_for_compat();
  int apply_async_index_read_snapshot_(storage::ObTableScanParam &param);

protected:
  ObSEArray<ObDatum *, 4> trans_info_array_;
   // Local lookup das task could rescan multiple times during execution, lookup_tablet_id_ and
   // lookup_ls_id_ store the lookup parameter for this time.
  storage::ObTableScanParam lookup_param_;
  ObTabletID lookup_tablet_id_;
  share::ObLSID lookup_ls_id_;
  transaction::ObTxDesc *trans_desc_;
  transaction::ObTxReadSnapshot *snapshot_;
  ObDASSearchCtx *async_search_ctx_;
  bool is_first_lookup_;
  bool lt_enabled_;
  uint64_t lt_physical_data_table_id_;
};

}  // namespace sql
}  // namespace oceanbase


#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_LOOKUP_ITER_H_ */
