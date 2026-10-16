// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OB_SS_INC_META_CHECKPOINT_H_
#define OB_SS_INC_META_CHECKPOINT_H_

#include "storage/incremental/ob_ss_inc_checkpoint.h"

namespace oceanbase {
namespace logservice {
class ObICheckpointSubHandler;
}

namespace storage {
class ObLS;

#define REGISTER_SS_INC_META_CKPT(INC_META_TYPE, INC_META)                                 \
  if (OB_FAIL(ret)) {                                                                      \
  } else if (OB_FAIL(inc_meta_ckpt_.register_ss_inc_meta_ckpt(INC_META_TYPE, INC_META))) { \
    LOG_WARN("register ss inc meta failed", KR(ret));                                      \
  }

#define UNREGISTER_SS_INC_META_CKPT(INC_META_TYPE, INC_META) \
  (void)inc_meta_ckpt_.unregister_ss_inc_meta_ckpt(INC_META_TYPE, INC_META)

class ObSSIncMetaCheckpoint : public ObSSIncCheckpoint {
public:
  ObSSIncMetaCheckpoint(ObLS &ls) : is_inited_(false), ls_(ls)
  {
    MEMSET(inc_meta_ckpts_, 0, sizeof(logservice::ObICheckpointSubHandler *) * SSIncMetaType::MAX_INC_META_TYPE);
  }
  virtual share::SCN get_ss_rec_scn() override;

  int init();
  void reset();
  int register_ss_inc_meta_ckpt(SSIncMetaType inc_meta_type, logservice::ObICheckpointSubHandler *inc_meta_ckpt);
  void unregister_ss_inc_meta_ckpt(SSIncMetaType inc_meta_type, logservice::ObICheckpointSubHandler *inc_meta_ckpt);
  const char *get_meta_ckpt_string(int64_t type_idx);

private:

private:
  bool is_inited_;
  ObLS &ls_;
  logservice::ObICheckpointSubHandler *inc_meta_ckpts_[SSIncMetaType::MAX_INC_META_TYPE];
};

}  // namespace storage
}  // namespace oceanbase

#endif