/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_TX_VERSION_MGR_
#define OCEANBASE_TRANSACTION_OB_TX_VERSION_MGR_

#include <stdint.h>
#include "lib/atomic/ob_atomic.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/time/ob_time_utility.h"
#include "share/scn.h"

namespace oceanbase
{
namespace transaction
{

class ObTxVersionMgr
{
public:
  ObTxVersionMgr()
  {
    max_commit_ts_.set_base();
    max_elr_commit_ts_.set_base();
    max_read_ts_.set_base();
  }
  ~ObTxVersionMgr() {}
public:
  void update_max_commit_ts(const share::SCN &ts, const bool elr)
  {
    if (!elr) {
      max_commit_ts_.inc_update(ts);
    } else {
      max_elr_commit_ts_.inc_update(ts);
    }
#ifdef ENABLE_DEBUG_LOG
    TRANS_LOG(TRACE, "update max commit ts", K(ts), K(elr));
#endif
  }
  // A registered read must leave room for a finite, strictly greater version.
  static bool is_valid_read_snapshot(const share::SCN &ts)
  {
    return ts.is_valid()
        && ts.get_val_for_tx() < static_cast<int64_t>(share::OB_MAX_SCN_TS_NS - 1);
  }
  [[nodiscard]] int update_max_read_ts(const share::SCN &ts)
  {
    int ret = common::OB_SUCCESS;
    if (OB_UNLIKELY(!is_valid_read_snapshot(ts))) {
      ret = common::OB_INVALID_ARGUMENT;
      if (TC_REACH_TIME_INTERVAL(1_s)) {
        TRANS_LOG(ERROR, "INVALID_READ_SNAPSHOT: refuse to register read watermark",
                  K(ret), K(ts), KP(this));
      }
    } else {
      max_read_ts_.inc_update(ts);
    }
#ifdef ENABLE_DEBUG_LOG
    TRANS_LOG(TRACE, "update max read ts", K(ret), K(ts));
#endif
    return ret;
  }
  share::SCN get_max_commit_ts(const bool elr) const
  {
    share::SCN max_commit_ts = max_commit_ts_.atomic_get();
    if (elr) {
      const share::SCN max_elr_commit_ts = max_elr_commit_ts_.atomic_get();
      max_commit_ts = share::SCN::max(max_commit_ts, max_elr_commit_ts);
    }
#ifdef ENABLE_DEBUG_LOG
    TRANS_LOG(TRACE, "get max commit ts", K(max_commit_ts), K(elr));
#endif
    return max_commit_ts;
  }
  share::SCN get_max_read_ts() const
  {
    const share::SCN max_read_ts = share::SCN::scn_inc(max_read_ts_);
#ifdef ENABLE_DEBUG_LOG
    TRANS_LOG(TRACE, "get max read ts", K(max_read_ts));
#endif
    return max_read_ts;
  }
private:
  share::SCN max_commit_ts_ CACHE_ALIGNED;
  share::SCN max_elr_commit_ts_ CACHE_ALIGNED;
  share::SCN max_read_ts_ CACHE_ALIGNED;
};

}
}//end of namespace oceanbase

#endif //OCEANBASE_TRANSACTION_OB_TX_VERSION_MGR_
