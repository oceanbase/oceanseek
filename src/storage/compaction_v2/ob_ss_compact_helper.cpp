// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#define USING_LOG_PREFIX STORAGE
#include "ob_ss_compact_helper.h"
#include "share/compaction/ob_shared_storage_compaction_util.h"
#include "storage/compaction_v2/ob_ss_link_major_task.h"
#include "storage/compaction_v2/ob_ss_link_inc_major_task.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/incremental/atomic_protocol/ob_sstable_atomic_protocol.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"
#include "storage/compaction_v2/ob_ss_skip_major_merge_dag.h"
#include "storage/compaction_v2/ob_ss_link_major_dag.h"
#include "storage/incremental/ob_inc_sstable_upload_task.h"

namespace oceanbase
{
namespace share
{
using namespace compaction;

int SSCompactHelper::check_is_sswriter(const share::ObLSID &ls_id,
                                       const common::ObTabletID &tablet_id,
                                       int64_t &sswriter_epoch)
{
  int ret = OB_SUCCESS;
  ObSSWriterKey key(ObSSWriterType::COMPACTION, ls_id, tablet_id);
  bool is_sswriter = false;
  if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key, is_sswriter, sswriter_epoch))) {
    LOG_WARN("check lease fail", K(ret));
  } else if (!is_sswriter) {
    ret = OB_NOT_MASTER;
    LOG_WARN("current is not sswriter", K(ret));
  }
  return ret;
}
int SSCompactHelper::check_is_sswriter(const share::ObLSID &ls_id,
                                       const common::ObTabletID &tablet_id)
{
  int64_t epoch = 0;
  return check_is_sswriter(ls_id, tablet_id, epoch);
}

int SSCompactHelper::link_major(const share::ObLSID &ls_id,
                                const common::ObTabletID &tablet_id,
                                const ObMergeType merge_type,
                                const int64_t snapshot_version,
                                const int64_t merge_base_version,
                                const bool prewarm,
                                const bool report)
{
  ObTabletSSLinkMajorTask task(ls_id, tablet_id, snapshot_version, merge_base_version, merge_type, prewarm, report);
  return task.process();
}

// create major sstable atomic protocol with gc rely infos
int SSCompactHelper::start_major_task(const share::ObLSID &ls_id,
                                      const common::ObTabletID &tablet_id,
                                      const share::SCN &transfer_scn,
                                      const int64_t snapshot_version,
                                      const int64_t seq_step_cnt,
                                      const int64_t cg_cnt,
                                      storage::ObSSTableAtomicProtocol &sstable_protocol)
{
#define LOG_CTX K(ret), K(ls_id), K(tablet_id), K(transfer_scn), K(snapshot_version), \
    K(seq_step_cnt), K(cg_cnt)
  int ret = OB_SUCCESS;
  ObAtomicProtocolType protocol_type;
  protocol_type.type_ = ObAtomicProtocolTypeEnum::MAJOR_SSTABLE;
  ObSSMetaCtrlObjectInfo object_info;
  bool need_create = true;
  share::SCN snapshot_scn;
  snapshot_scn.convert_for_logservice(static_cast<uint64_t>(snapshot_version));
  if (OB_FAIL(object_info.init_by_major(
      snapshot_scn, MACRO_STEP_SIZE, seq_step_cnt, cg_cnt))) {
    LOG_WARN("init object info fail", LOG_CTX);
  } else if (OB_FAIL(sstable_protocol.init(protocol_type, MTL_ID(), ls_id, tablet_id, transfer_scn, object_info))) {
    LOG_WARN("init sstable protocol fail", LOG_CTX);
  } else if (OB_FAIL(sstable_protocol.load_if_exist(snapshot_version, need_create))) {
    LOG_WARN("load sstable protocol if exist fail", LOG_CTX);
  } else if (!need_create) {
    LOG_INFO("major task already exists", LOG_CTX);
  } else {
    int64_t MAX_SLEEP_US = 30_s;
    int retry_cnt = 0;
    const int64_t start_ts = ObTimeUtility::current_time();
    ObRandom random;
    random.seed(start_ts + GCTX.get_server_id());
    do {
      if (OB_FAIL(sstable_protocol.create(false/*need_check_sswriter*/))) {
        if (OB_NEED_RETRY == ret) {
          if (ObTimeUtility::current_time() - start_ts > MAX_SLEEP_US) {
            LOG_WARN("create major task timeout", LOG_CTX, K(MAX_SLEEP_US), K(retry_cnt));
            break;
          }
          retry_cnt++;
          int64_t sleep_us = random.get(10_ms, 100_ms);
          LOG_INFO("conflict, need retry", LOG_CTX, K(retry_cnt), K(sleep_us));
          ob_usleep(sleep_us);
        } else if (OB_TASK_EXPIRED == ret) {
          LOG_INFO("newer major task already exists, current create is expired", LOG_CTX);
        } else {
          LOG_WARN("create sstable protocol fail", LOG_CTX);
        }
      } else {
        LOG_INFO("create major task success", LOG_CTX);
      }
    } while (OB_NEED_RETRY == ret);
  }
#undef LOG_CTX
  return ret;
}
// mark major sstable atomic protocol finish
int SSCompactHelper::finish_major_task(const share::ObLSID &ls_id,
                                       const common::ObTabletID &tablet_id,
                                       const share::SCN &transfer_scn,
                                       const int64_t snapshot_version,
                                       storage::ObSSTableAtomicProtocol &sstable_protocol)
{
  int ret = OB_SUCCESS;
  bool need_create = true;
  if (!sstable_protocol.is_inited()) {
    ObAtomicProtocolType protocol_type;
    protocol_type.type_ = ObAtomicProtocolTypeEnum::MAJOR_SSTABLE;
    if (OB_FAIL(sstable_protocol.init_for_load(protocol_type, MTL_ID(), ls_id, tablet_id, transfer_scn))) {
      LOG_WARN("init sstable protocol for load fail", K(ret), K(ls_id), K(tablet_id), K(transfer_scn), K(snapshot_version));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(sstable_protocol.load_if_exist(snapshot_version, need_create))) {
    LOG_WARN("load sstable protocol if exist fail", K(ret), K(ls_id), K(tablet_id), K(transfer_scn), K(snapshot_version));
  } else if (need_create) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current version not exist", K(ret), K(ls_id), K(tablet_id), K(transfer_scn), K(snapshot_version));
  } else if (sstable_protocol.is_status_complete()) {
    LOG_INFO("major task already finished", K(ls_id), K(tablet_id), K(snapshot_version));
  } else if (sstable_protocol.is_status_abandon()) {
    LOG_INFO("major task already abandoned", K(ls_id), K(tablet_id), K(snapshot_version));
  } else if (sstable_protocol.is_status_start()) {
    int tmp_ret = OB_SUCCESS;
    ObSSMetaAtomicSSTableOp finish_op;
    if (OB_TMP_FAIL(sstable_protocol.finish(finish_op))) {
      LOG_WARN("finish sstable protocol fail", K(tmp_ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("major task status is unexpected", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(sstable_protocol));
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("ignore, finish op is best effort", K(ret));
    ret = OB_SUCCESS;
  }
  return ret;
}

int SSCompactHelper::start_inc_major_compaction_task(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const int64_t snapshot_version,
    const int64_t seq_step_cnt,
    const int64_t cg_cnt,
    int64_t &op_id,
    storage::ObSSTableAtomicProtocol &sstable_protocol)
{
  int ret = OB_SUCCESS;
  ObAtomicProtocolType protocol_type;
  ObSSMetaCtrlObjectInfo object_info;
  share::SCN snapshot_scn;
  op_id = -1;
  protocol_type.type_ = ObAtomicProtocolTypeEnum::INC_MAJOR_COMPACTION_SSTABLE;
  snapshot_scn.convert_for_logservice(static_cast<uint64_t>(snapshot_version));
  if (OB_FAIL(object_info.init_by_inc_major(
      snapshot_scn, MACRO_STEP_SIZE, seq_step_cnt, cg_cnt))) {
    LOG_WARN("init inc major compaction object info fail", K(ret),
        K(snapshot_version), K(seq_step_cnt), K(cg_cnt));
  } else if (OB_FAIL(sstable_protocol.init(
      protocol_type, MTL_ID(), ls_id, tablet_id, transfer_scn, object_info))) {
    LOG_WARN("init inc major compaction protocol fail", K(ret), K(ls_id), K(tablet_id),
        K(transfer_scn), K(object_info));
  } else if (OB_FAIL(sstable_protocol.create(true/*检查 SS writer*/))) {
    LOG_WARN("create inc major compaction protocol fail", K(ret), K(ls_id), K(tablet_id),
        K(transfer_scn), K(object_info));
  } else if (OB_FAIL(sstable_protocol.get_op_id(op_id))) {
    LOG_WARN("get inc major compaction op id fail", K(ret), K(ls_id), K(tablet_id),
        K(transfer_scn));
  }
  return ret;
}

int SSCompactHelper::finish_inc_major_compaction_task(
    storage::ObSSTableAtomicProtocol &sstable_protocol)
{
  int ret = OB_SUCCESS;
  if (!sstable_protocol.is_inited()) {
    ret = OB_NOT_INIT;
    LOG_WARN("inc major compaction protocol is not initialized", K(ret));
  } else if (sstable_protocol.is_status_complete()) {
    LOG_INFO("inc major compaction task already finished");
  } else if (!sstable_protocol.is_status_start()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("inc major compaction protocol status is unexpected", K(ret), K(sstable_protocol));
  } else {
    ObSSMetaAtomicSSTableOp finish_op;
    if (OB_FAIL(sstable_protocol.finish(finish_op))) {
      LOG_WARN("finish inc major compaction protocol fail", K(ret), K(sstable_protocol));
    }
  }
  return ret;
}

int SSCompactHelper::check_shared_major_exist(const share::ObLSID &ls_id,
                                              const common::ObTabletID &tablet_id,
                                              const share::SCN &transfer_scn,
                                              const int64_t snapshot_version,
                                              ObArenaAllocator &allocator,
                                              ObTabletHandle &tablet_handle,
                                              ObTabletMemberWrapper<ObTabletTableStore> &table_store_wrapper,
                                              blocksstable::ObSSTable *&last_major)
{
  int ret = OB_SUCCESS;
  last_major = NULL;
  if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_id,
                                                 tablet_id,
                                                 transfer_scn,
                                                 allocator,
                                                 tablet_handle))) {
    LOG_WARN("get shared tablet fail", K(ret));
  } else if (!tablet_handle.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared tablet is invalid", K(ret));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else {
    const ObSSTableArray &sstables = table_store_wrapper.get_member()->get_major_sstables();
    bool found = false;
    for(int i = 0; !found && i < sstables.count(); i ++) {
      if (sstables[i]->get_snapshot_version() == snapshot_version) {
        LOG_INFO("found major", KPC(sstables[i]), K(snapshot_version));
        found = true;
      }
      last_major = sstables[i];
    }
    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      int64_t last_snapshot_version = last_major ? last_major->get_snapshot_version() : 0;
      LOG_WARN("can not found target major", K(ret), K(snapshot_version), K(last_snapshot_version));
    }
  }
  return ret;
}

int SSCompactHelper::create_or_update_table_store(
  const share::ObLSID &ls_id,
  const ObTabletID &tablet_id,
  const share::SCN &transfer_scn,
  const ObMetaUpdateReason update_reason,
  ObUpdateTableStoreParam &param,
  storage::ObSSTableAtomicProtocol &sstable_protocol,
  const bool need_add_ctrl_key,
  const bool check_lease)
{
  int ret = OB_SUCCESS;
  bool retry = false;
  int64_t major_snapshot = 0;
  if (OB_ISNULL(param.sstable_) || (!param.sstable_->is_major_sstable()
                                    && !param.sstable_->is_inc_major_type_sstable())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("param.sstable_ is null or not major sstable or not inc major sstable", K(ret), KPC(param.sstable_));
  } else {
    ObSSMetaCtrlKeyInfo ctrl_key_info;
    ObArray<ObSSMetaCtrlKeyInfo> meta_ctrl_keys;
    if (need_add_ctrl_key) {
      if (OB_FAIL(sstable_protocol.get_ctrl_key_info(ctrl_key_info))) {
        LOG_WARN("get ctrl key info fail", K(ret));
      } else if (OB_FAIL(meta_ctrl_keys.push_back(ctrl_key_info))) {
        LOG_WARN("push back ctrl key fail", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      do {
        retry = false;
        if (OB_FAIL(MTL(ObSSMetaService*)->update_tablet_table_store(
            ls_id, tablet_id, transfer_scn, update_reason, param, meta_ctrl_keys, check_lease))) {
          LOG_WARN("update tablet table store v2 fail", K(ret));
          if (OB_TABLET_NOT_EXIST == ret) {
            if (OB_FAIL(MTL(ObSSMetaService*)->create_tablet(ls_id, tablet_id, transfer_scn, true/*check_lease*/))) {
              LOG_WARN("create tablet fail v2", K(ret));
            } else {
              retry = true;
            }
          }
        }
      } while (retry);
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("update shared tablet table store success", K(ls_id), K(tablet_id), K(transfer_scn), K(update_reason));
  }
  return ret;
}

int SSCompactHelper::link_inc_major(const share::ObLSID &ls_id,
                                    const common::ObTabletID &tablet_id,
                                    const int64_t compaction_scn,
                                    const transaction::ObTransID &trans_id,
                                    const transaction::ObTxSEQ &seq_no,
                                    const share::SCN &ddl_checkpoint_scn,
                                    bool &is_linked)
{
  ObTabletSSLinkIncMajorTask task(ls_id,
                                  tablet_id,
                                  compaction_scn,
                                  trans_id,
                                  seq_no,
                                  ddl_checkpoint_scn);
  return task.process(is_linked);
}

bool SSCompactHelper::check_ls_dag_need_cancel(const ObIDag &dag, const ObLSID &ls_id, bool &need_cancel)
{
  need_cancel = false;
  bool recognized = true;
  switch (dag.get_type()) {
  case ObDagType::DAG_TYPE_INC_SSTABLE_UPLOAD: {
    need_cancel = (ls_id == static_cast<const storage::ObIncSSTableUploadDag&>(dag).get_ls_id());
    break;
  }
  case ObDagType::DAG_TYPE_UPDATE_SKIP_MAJOR: {
    need_cancel = (ls_id == static_cast<const ObSSSkipMajorMergeDag&>(dag).get_param().ls_id_);
    break;
  }
  case ObDagType::DAG_TYPE_LINK_MAJOR_SSTABLE: {
    need_cancel = (ls_id == static_cast<const ObLinkSSMajorSSTableDag&>(dag).get_ls_id());
    break;
  }
  default:
    recognized = false;
    break;
  }
  return recognized;
}

} // namespace compaction
} // namespace oceanbase
