// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0

#ifndef OB_SS_CHECKPOINT_EXECUTOR_H_
#define OB_SS_CHECKPOINT_EXECUTOR_H_

#include "lib/lock/ob_small_spin_lock.h"
#include "storage/incremental/ob_ss_inc_checkpoint.h"
#include "storage/incremental/ob_ss_checkpoint_util.h"

namespace oceanbase {
namespace storage {
class ObLS;
class ObSSIncCheckpoint;

#define REGISTER_SS_INC_CKPT(INC_CKPT_TYPE, INC_CKPT)                                       \
  if (OB_FAIL(ret)) {                                                                       \
  } else if (OB_FAIL(ss_checkpoint_executor_.register_inc_ckpt(INC_CKPT_TYPE, INC_CKPT))) { \
    LOG_WARN("register ss inc ckpt failed", KR(ret));                                       \
  }

#define UNREGISTER_SS_INC_CKPT(INC_CKPT_TYPE, INC_CKPT) \
  (void)ss_checkpoint_executor_.unregister_inc_ckpt(INC_CKPT_TYPE, INC_CKPT);

class ObSSCheckpointExecutor {
public:
  ObSSCheckpointExecutor(ObLS &ls)
      : is_inited_(false), is_online_(false), is_updating_ckpt_(false), byte_lock_(common::ObLatchIds::SS_CHECKPOINT_EXECUTOR_LOCK), ls_(ls), ckpt_change_record_()
  {
    MEMSET(ss_inc_ckpt_, 0, sizeof(ObSSIncCheckpoint *) * SSIncCheckpointType::MAX_SS_INC_CHECKPOINT_TYPE);
  }
  ~ObSSCheckpointExecutor() {}

  int init();
  int online();
  int offline();
  void destroy();
  void reset();
  int get_ss_rec_scn(share::SCN &ss_rec_scn);

  int register_inc_ckpt(const SSIncCheckpointType inc_ckpt_type, ObSSIncCheckpoint *inc_ckpt);
  void unregister_inc_ckpt(const SSIncCheckpointType inc_ckpt_type, ObSSIncCheckpoint *inc_ckpt);

private:
  bool is_inited_;
  bool is_online_;
  bool is_updating_ckpt_;
  common::ObByteLock byte_lock_;
  ObLS &ls_;
  ObSSIncCheckpoint *ss_inc_ckpt_[SSIncCheckpointType::MAX_SS_INC_CHECKPOINT_TYPE];
  CkptChangeRecord ckpt_change_record_;
};

}  // namespace storage

}  // namespace oceanbase
#endif
