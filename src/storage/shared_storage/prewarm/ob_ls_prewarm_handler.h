/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_SHARED_STORAGE_PREWARM_OB_LS_PREWARM_HANDLER_
#define OCEABASE_STORAGE_SHARED_STORAGE_PREWARM_OB_LS_PREWARM_HANDLER_

#include "share/ob_define.h"
#include "share/ob_ls_id.h"
#include "logservice/ob_log_base_type.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
class ObLS;

class ObSSLSPreWarmHandler : public logservice::ObIReplaySubHandler,
                             public logservice::ObIRoleChangeSubHandler,
                             public logservice::ObICheckpointSubHandler
{
public:
  ObSSLSPreWarmHandler();
  virtual ~ObSSLSPreWarmHandler();
  int init(ObLS *ls);
  int stop();
  void wait();
  void destroy();
  // for role change
  virtual void switch_to_follower_forcedly() override;
  virtual int switch_to_leader() override;
  virtual int switch_to_follower_gracefully() override;
  virtual int resume_leader() override;
  // for replay
  virtual int replay(const void *buffer,
                     const int64_t nbytes,
                     const palf::LSN &lsn,
                     const share::SCN &scn) override final;
  // for checkpoint
  virtual share::SCN get_rec_scn() override final { return share::SCN::max_scn(); }
  virtual int flush(share::SCN &scn) override final;
  // for offline and online
  int offline();
  int online();
  ObLS *get_ls() const { return ls_; }

private:
  int switch_role_to(const bool is_leader);

private:
  bool is_inited_;
  bool is_ls_stop_;
  bool is_leader_;
  ObLS *ls_;
  int64_t ls_id_;
  DISALLOW_COPY_AND_ASSIGN(ObSSLSPreWarmHandler);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEABASE_STORAGE_SHARED_STORAGE_PREWARM_OB_LS_PREWARM_HANDLER_ */
