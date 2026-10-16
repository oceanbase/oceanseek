/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_CTX_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_CTX_H

#include "buffer_ctx.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "mds_writer.h"
#include "runtime_utility/common_define.h"
#include "runtime_utility/mds_lock.h"
#include "storage/tx/ob_tx_seq.h"
namespace oceanbase
{
namespace storage
{
namespace mds
{
class MdsTableHandle;

class MdsCtx : public BufferCtx
{
  OB_UNIS_VERSION(1);
public:
  MdsCtx();
  explicit MdsCtx(const MdsWriter &writer,
                  const transaction::ObTxSEQ start_seq = transaction::ObTxSEQ::MIN_VAL());
  virtual ~MdsCtx();
  MdsCtx(const MdsCtx &) = delete;
  MdsCtx(MdsCtx &&) = delete;
  MdsCtx &operator=(const MdsCtx &) = delete;
  MdsCtx &operator=(MdsCtx &&) = delete;
  int assign(const MdsCtx &);
  int set_writer(const MdsWriter &writer);
  void set_seq_no(const transaction::ObTxSEQ seq_no);
  int inc_seq_no();
  transaction::ObTxSEQ get_seq_no() const;
  bool can_write() const;
  VIRTUAL_TO_STRING_KV(K_(writer),
                         K_(register_mds_memtable_write_cnt),
                         K(obj_to_string(state_)),
                         K_(seq_no));
  // LS_MDS_TABLET aggregate write path: increments register_mds_memtable_write_cnt_; on_redo
  // checks it equals sync_flipped_cnt from mds_sync_log_succ then clears.
  void register_mds_memtable_write();
  virtual const MdsWriter get_writer() const override;
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void before_prepare() override;
  virtual void on_prepare(const share::SCN &prepare_version) override;
  virtual void on_commit(const share::SCN &commit_version,
                         const share::SCN &commit_scn) override;
  virtual void on_abort(const share::SCN &abort_scn) override;
  virtual void single_log_commit(const share::SCN commit_version, const share::SCN commit_scn);
  virtual void single_log_abort();
private:
  void advance_state_if_behind_(TwoPhaseCommitState target);
private:
  int64_t register_mds_memtable_write_cnt_;
  TwoPhaseCommitState state_;
  MdsLock lock_;
protected:
  MdsWriter writer_;
  transaction::ObTxSEQ seq_no_;
};
OB_SERIALIZE_MEMBER_TEMP(inline, MdsCtx, writer_);
}
}
}
#endif
