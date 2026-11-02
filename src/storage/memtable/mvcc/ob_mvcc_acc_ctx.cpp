/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_mvcc_acc_ctx.h"
#include "storage/access/ob_mds_filter_mgr.h"
#include "storage/memtable/ob_memtable.h"

namespace oceanbase
{
using namespace transaction;
namespace memtable
{

int ObMvccMdsFilter::init(ObMvccMdsFilter &mds_filter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mds_filter.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", KR(ret), K(mds_filter));
  } else {
    mds_filter_mgr_ = mds_filter.mds_filter_mgr_;
    read_info_ = mds_filter.read_info_;
  }
  return ret;
}

bool ObMvccMdsFilter::is_mds_filter_empty() const
{
  return nullptr == mds_filter_mgr_ || mds_filter_mgr_->is_empty();
}

void ObMvccAccessCtx::leave_mds_on_register()
{
  if (nullptr != pinned_mds_memtable_) {
    pinned_mds_memtable_->dec_mds_pin_cnt();
    pinned_mds_memtable_->dec_write_ref();
    pinned_mds_memtable_ = nullptr;
  }
  in_mds_on_register_ = false;
}

int ObMvccAccessCtx::get_write_seq(ObTxSEQ &seq) const
{
  int ret = OB_SUCCESS;
  // for update uk or pk, set branch part to 0, in orer to let tx-callback fall into single list
  if (write_flag_.is_update_uk()) {
    const int branch = tx_scn_.get_branch();
    if (branch == 0) {
      seq = tx_scn_;
    } else if (OB_UNLIKELY(ObTxDesc::is_alloced_branch_id(branch))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "external branch not support concurrent update uk / pk", K(ret), KPC(this));
    } else {
      seq = ObTxSEQ(tx_scn_.get_seq(), 0);
    }
  } else {
    seq = tx_scn_;
  }
  return ret;
}
} // memtable
} // oceanbase
