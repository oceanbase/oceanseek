/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/tx/ob_trans_id.h"
#include "storage/tx/ob_tx_seq.h"
#include "lib/hash/ob_hashset.h"

namespace oceanbase
{
namespace storage
{
class ObStorageSchema;
class ObDDLKVHandle;
class ObDDLKV;
class ObSSTableArray;
struct ObDDLWriteStat;
class ObDDLTabletContext;
class ObDDLIndependentDag;

class ObDDLIncLoadUtil final
{
public:
  static int prepare_inc_major_direct_load(ObDDLIndependentDag &dag,
                                           const share::ObLSID &ls_id,
                                           const ObTabletID &tablet_id);
  static int start_inc_major_direct_load(ObDDLIndependentDag &dag,
                                         const share::ObLSID &ls_id,
                                         const ObTabletID &tablet_id);
  static int start_inc_major_direct_load(ObDDLIndependentDag &dag,
                                         ObDDLTabletContext &tablet_ctx);
  static int prepare_inc_major_direct_load_one_tablet(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      hash::ObHashSet<transaction::ObTransID> &trans_ids,
      bool &is_prepared);
private:
  static const int64_t MAX_INC_MAJOR_DIRECT_LOAD_CNT = 48;
  static int record_inc_major_trans_id(const transaction::ObTransID &trans_id,
                                       hash::ObHashSet<transaction::ObTransID> &trans_ids,
                                       bool &is_prepared);
  static int record_inc_major_start_info_to_mds(
      const ObStorageSchema &storage_schema,
      const ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const transaction::ObTransID &trans_id,
      const int64_t data_format_version,
      const int64_t snapshot_version,
      const share::SCN start_scn,
      common::ObIAllocator &allocator);
  static int search_inc_major_sstables(const ObSSTableArray &sstables,
                                       hash::ObHashSet<transaction::ObTransID> &trans_ids,
                                       bool &is_prepared);
  static int search_inc_major_ddlkvs(const common::ObIArray<ObDDLKV *> &ddl_kvs,
                                     hash::ObHashSet<transaction::ObTransID> &trans_ids,
                                     bool &is_prepared);
};

class ObDDLIncStartTask final : public share::ObITask
{
public:
  ObDDLIncStartTask(const int64_t tablet_idx);
  int process() override;

private:
  int generate_next_task(ObITask *&next_task) override;
private:
  int64_t tablet_idx_;
};

class ObDDLIncCommitTask final : public share::ObITask
{
public:
  ObDDLIncCommitTask(const int64_t tablet_idx);
  ObDDLIncCommitTask(const ObTabletID &tablet_id);
  int process() override;

private:
  int generate_next_task(ObITask *&next_task) override;
  int record_inc_major_commit_info_to_mds(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id,
      const transaction::ObTransID &trans_id,
      const share::SCN &commit_scn,
      const ObDDLWriteStat &write_stat);

private:
  int64_t tablet_idx_;
  ObTabletID tablet_id_;
};

class ObDDLIncWaitDumpTask final : public share::ObITask
{
public:
  ObDDLIncWaitDumpTask(const share::ObLSID &ls_id,
                       const ObTabletID &tablet_id,
                       const transaction::ObTransID &trans_id,
                       const transaction::ObTxSEQ &seq_no);
  int process() override;
private:
  void schedule_ddl_merge_dag(ObDDLKVHandle &first_ddl_kv);
private:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  transaction::ObTransID trans_id_;
  transaction::ObTxSEQ seq_no_;
};

class ObDDLIncPrepareTask final : public share::ObITask
{
public:
  ObDDLIncPrepareTask();
  int process() override;
private:
  int process_one_tablet(const share::ObLSID &ls_id,
                         const ObTabletID &tablet_id);
private:
  int64_t tablet_idx_;
  bool is_prepared_;
  hash::ObHashSet<transaction::ObTransID> trans_ids_;
};

} // namespace storage
} // namespace oceanbase
