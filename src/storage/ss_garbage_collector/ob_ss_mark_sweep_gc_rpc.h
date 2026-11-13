/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_MARK_SWEEP_GC_RPC_H_
#define OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_MARK_SWEEP_GC_RPC_H_

#include "lib/container/ob_array_serialization.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "common/ob_learner_list.h"
#include "common/ob_member.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "share/config/ob_server_config.h"
#include "share/ob_ls_id.h"
#include "share/ob_server_struct.h"
#include "storage/ss_garbage_collector/ob_ss_gc_define.h"

namespace oceanbase
{
namespace storage
{
class ObLS;
class ObStorageSchema;

class ObSSMarkSweepGCStudyReq final
{
public:
  static const int32_t SS_MARK_SWEEP_GC_STUDY_REQ_VERSION = 1;
  OB_UNIS_VERSION(SS_MARK_SWEEP_GC_STUDY_REQ_VERSION);
public:
  ObSSMarkSweepGCStudyReq() : tablet_list_() {}
  ~ObSSMarkSweepGCStudyReq() {}
  TO_STRING_KV(K_(tablet_list));
public:
  common::ObSArray<ObSSGCTabletWithDelScn> tablet_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepGCStudyReq);
};

class ObSSMarkSweepGCStudyRes final
{
public:
  static const int32_t SS_MARK_SWEEP_GC_STUDY_RES_VERSION = 1;
  OB_UNIS_VERSION(SS_MARK_SWEEP_GC_STUDY_RES_VERSION);
public:
  ObSSMarkSweepGCStudyRes() : tablet_meta_scn_list_() {}
  ~ObSSMarkSweepGCStudyRes() {}
  TO_STRING_KV(K_(tablet_meta_scn_list));
public:
  common::ObSArray<ObSSGCTabletStudyResult> tablet_meta_scn_list_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepGCStudyRes);
};

}  // namespace storage


namespace obrpc
{
class ObSSMarkSweepGCStudyProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObSSMarkSweepGCStudyProxy);
  RPC_S(PR5 study_min_tablet_meta_scn,
        OB_STUDY_MIN_TABLET_META_SCN,
        (storage::ObSSMarkSweepGCStudyReq),
        storage::ObSSMarkSweepGCStudyRes);
};

class ObSSMarkSweepGCStudyP
  : public ObRpcProcessor<ObSSMarkSweepGCStudyProxy::ObRpc<OB_STUDY_MIN_TABLET_META_SCN>>
{
public:
  ObSSMarkSweepGCStudyP() {}
  virtual ~ObSSMarkSweepGCStudyP() {}

public:
  virtual int process();

private:
  int study_(const common::ObIArray<storage::ObSSGCTabletWithDelScn> &tablet_list,
             common::ObIArray<storage::ObSSGCTabletStudyResult> &tablet_meta_scn_list);
  int handle_ls_not_exist_(
      const storage::ObSSGCTablet &cur_gc_tablet,
      common::ObIArray<storage::ObSSGCTabletStudyResult> &tablet_meta_scn_list);
  int handle_tablet_not_exist_(
      const storage::ObSSGCTablet &cur_gc_tablet,
      const share::SCN &deleted_scn,
      common::ObIArray<storage::ObSSGCTabletStudyResult> &tablet_meta_scn_list);
  int handle_tablet_exist_(
      const share::SCN &ls_clog_checkpoint_scn,
      const storage::ObTabletHandle &tablet_handle,
      const storage::ObSSGCTablet &cur_gc_tablet,
      const int64_t ls_epoch,
      common::ObIArray<storage::ObSSGCTabletStudyResult> &tablet_meta_scn_list);
  int append_tablet_meta_scn_(
      const storage::ObSSGCTablet &cur_gc_tablet,
      const storage::ObSSGCTabletStudyInfo &study_info,
      const storage::ObSSGCTabletType table_type,
      common::ObIArray<storage::ObSSGCTabletStudyResult> &tablet_meta_scn_list);
  int check_allow_tablet_version_gc_(const storage::ObTabletHandle &tablet_handle,
                                     bool &allow_tablet_version_gc);
  int check_tablets_belong_to_same_ls_(
      const common::ObIArray<storage::ObSSGCTabletWithDelScn> &tablet_list);
  bool need_collect_lob_rely_info_(
      const storage::ObStorageSchema &storage_schema);
  bool update_cold_lob_rely_info_(
      const bool collect_lob_rely_info,
      const int64_t tablet_cnt,
      const bool has_memtable,
      const share::SCN &ls_clog_checkpoint_scn,
      storage::ObSSGCTabletStudyInfo &study_info);
  DISALLOW_COPY_AND_ASSIGN(ObSSMarkSweepGCStudyP);
};

}  // namespace obrpc

namespace storage
{
class ObSSGCStudyUtil
{
public:
  static int do_mark_sweep_gc_study(const share::ObLSID &ls_id,
                                    const ObSSGCTabletWithDelScnArray &gc_tablets,
                                    ObSSGCTabletStudyResultArray &tablet_meta_scn_arr,
                                    const int64_t timeout_us);
  static int mark_sweep_gc_study(const share::ObLSID &ls_id,
                                 const ObSSGCTabletWithDelScnArray &gc_tablets,
                                 ObSSGCTabletStudyResultArray &tablet_meta_scn_arr,
                                 bool &study_need_retry);

private:
  static int collect_ls_servers_(const share::ObLSID &ls_id,
                                 common::hash::ObHashSet<common::ObAddr> &ls_servers);
  static int fetch_ls_member_and_learner_list_(const share::ObLSID &ls_id,
                                               common::ObMemberList &member_list,
                                               common::GlobalLearnerList &learner_list);
  static int try_handle_ls_deleted_(const share::ObLSID &ls_id,
                                    const share::SCN &ls_deleted_scn,
                                    const ObSSGCTabletWithDelScnArray &gc_tablets,
                                    ObSSGCTabletStudyResultArray &tablet_meta_scn_arr,
                                    bool &ls_already_deleted);
  static int merge_server_meta_scn_infos_(const common::ObIArray<ObSSGCTabletStudyResult> &cur_tablet_meta_scn_arr,
                                          common::hash::ObHashMap<ObSSGCTablet, int64_t> &tablet_index_map,
                                          ObSSGCTabletStudyResultArray &final_tablet_meta_scn_arr);
  static int batch_process_server_tablets_(const common::ObAddr &server,
                                           const ObSSGCTabletWithDelScnArray &gc_tablets,
                                           common::hash::ObHashMap<ObSSGCTablet, int64_t> &tablet_index_map,
                                           ObSSGCTabletStudyResultArray &tablet_meta_scn_arr);
  static int send_batch_rpc_request_(const common::ObAddr &server,
                                     const ObSSGCTabletWithDelScnArray &batch_tablets,
                                     obrpc::ObSSMarkSweepGCStudyProxy &proxy,
                                     common::hash::ObHashMap<ObSSGCTablet, int64_t> &tablet_index_map,
                                     ObSSGCTabletStudyResultArray &tablet_meta_scn_arr,
                                     int64_t &rpc_cost_us);

public:
  static const int64_t OB_SS_MARK_SWEEP_GC_STUDY_RPC_TIMEOUT = 10_s;
  static const int64_t MIN_BATCH_SIZE = 1;
  static const int64_t MAX_BATCH_SIZE = 10000;
  static const int64_t BATCH_SIZE_SHRINK_FACTOR = 10;
  static const int64_t BATCH_SIZE_GROW_FACTOR = 2;
  static const int64_t BATCH_SIZE_GROW_SUCC_THRESHOLD = 3;
  // grow only when rpc_cost_us * RATIO < timeout (i.e., latency < timeout / RATIO)
  static const int64_t BATCH_SIZE_SAFE_LATENCY_RATIO = 2;
};
}  // namespace storage

}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_GARBAGE_COLLECTOR_OB_SS_MARK_SWEEP_GC_RPC_H_
