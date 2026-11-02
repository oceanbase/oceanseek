// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OB_SS_CHECKPOINT_UTIL_H_
#define OB_SS_CHECKPOINT_UTIL_H_

#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "common/ob_clock_generator.h"

namespace oceanbase {
namespace storage {

struct CkptChangeRecord {
public:
  CkptChangeRecord() : last_checkpoint_scn_(share::SCN::min_scn()), changed_ts_(0) {}
  ~CkptChangeRecord() {}
  void reset()
  {
    last_checkpoint_scn_ = share::SCN::min_scn();
    changed_ts_ = 0;
  }

  TO_STRING_KV(K(last_checkpoint_scn_), KTIME(changed_ts_));

public:
  share::SCN last_checkpoint_scn_;
  int64_t changed_ts_;
};

class ObCkptUtil {
public:
  static void check_ckpt_changed(const bool is_ss_ckpt,
                                 const share::ObLSID ls_id,
                                 const share::SCN cur_ckpt_scn,
                                 CkptChangeRecord &ckpt_record);

private:
  static bool is_standby_ckpt_at_recovery_end_(const share::SCN &cur_ckpt_scn);
};

}  // namespace storage

}  // namespace oceanbase
#endif
