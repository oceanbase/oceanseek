/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SS_TRANSFER_BACKFILL_DAG_NET_
#define OCEANBASE_STORAGE_SS_TRANSFER_BACKFILL_DAG_NET_

#include "storage/reorganization_info_table/ob_tablet_reorg_info_table_operation.h"
#include "storage/incremental/ob_inc_ss_define.h"
#include "share/scn.h"
#include "share/ob_storage_ha_diagnose_struct.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "storage/high_availability/ob_storage_ha_dag.h"
#include "ob_ss_transfer_backfill_struct.h"
#include "storage/high_availability/ob_tablet_backfill_tx.h"

namespace oceanbase {
using namespace share;
namespace storage
{

class ObSSTransferBackfillDagNet: public share::ObIDagNet
{
public:
  ObSSTransferBackfillDagNet();
  virtual ~ObSSTransferBackfillDagNet() {};
  virtual int init_by_param(const share::ObIDagInitParam *param) override;
  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx();
  virtual int deal_with_cancel() override;
  virtual bool is_ha_dag_net() const override { return true; }
  virtual storage::ObIHADagNetCtx *get_dag_net_ctx() override { return &ctx_; }

  ObSSTransferBackfillTXCtx *get_ctx() { return &ctx_; }
  virtual share::ObLSID get_ls_id() const override { return ctx_.get_ls_id(); }
  const ObSSTransferBackfillType &get_backfill_type() const { return ctx_.backfill_type_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(ctx));
private:
  bool is_inited_;
  ObSSTransferBackfillTXCtx ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSTransferBackfillDagNet);
};

class ObSSStartTransferBackfillDag: public ObStorageHADag
{
public:
  ObSSStartTransferBackfillDag();
  virtual ~ObSSStartTransferBackfillDag() {};
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
private:
  bool is_inited_;
  ObSSTransferBackfillTXCtx *ctx_;
  share::ObIDagNet *dag_net_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSStartTransferBackfillDag);
};

class ObSSFinishTransferBackfillDag: public ObStorageHADag
{
public:
  ObSSFinishTransferBackfillDag();
  virtual ~ObSSFinishTransferBackfillDag();
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  int init(
    const share::ObTaskId &task_id,
    const share::ObLSID &ls_id,
    common::ObArray<ObTabletBackfillInfo> &tablet_info_array,
    ObIHADagNetCtx *ha_dag_net_ctx);
  ObBackfillTXCtx *get_backfill_tx_ctx() { return &backfill_tx_ctx_; }
  ObIHADagNetCtx *get_ha_dag_net_ctx() { return ha_dag_net_ctx_; }
  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
  bool is_inited_;
private:
  ObBackfillTXCtx backfill_tx_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSFinishTransferBackfillDag);
};

class ObSSStartTransferBackfillTask: public share::ObITask
{
public:
  ObSSStartTransferBackfillTask();
  virtual ~ObSSStartTransferBackfillTask() {}
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSStartTransferBackfillTask"), KP(this), KPC(ctx_));
private:
  static ObITaskType get_task_type_() { return ObITaskType::TASK_TYPE_MAX; }
  int inner_process_();
  template <typename DagType>
  int generate_tablet_backfill_dags_();
private:
  bool is_inited_;
  ObSSTransferBackfillTXCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObSSStartTransferBackfillTask);
};

//TODO(yuxiang) dag has different init function, re-think it
template <typename DagType>
int ObSSStartTransferBackfillTask::generate_tablet_backfill_dags_()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  DagType *tablet_backfill_dag = nullptr;
  ObSSFinishTransferBackfillDag *finish_backfill_dag = nullptr;
  ObTenantDagScheduler *scheduler = nullptr;
  ObIDagNet *dag_net = nullptr;
  ObBackfillTXCtx *backfill_tx_ctx = nullptr;
  storage::ObTabletBackfillInfo tablet_info;
  ObSSStartTransferBackfillDag *backfill_tx_dag = nullptr;
  const bool emergency = true;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "start transfer backfill tx task do not init", K(ret));
  } else if (ObDagType::DAG_TYPE_SS_START_TRANSFER_BACKFILL_TX != this->get_dag()->get_type()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag type is not match", K(ret), "dag_type", this->get_dag()->get_type());
  } else if (OB_ISNULL(backfill_tx_dag = static_cast<ObSSStartTransferBackfillDag *>(this->get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "start transfer backfill tx dag should not be NULL", K(ret), KP(backfill_tx_dag));
  } else if (OB_ISNULL(dag_net = backfill_tx_dag->get_dag_net())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag net should not be NULL", K(ret), KP(dag_net));
  } else if (OB_ISNULL(scheduler = MTL(ObTenantDagScheduler*))) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
  } else {
    if (OB_FAIL(scheduler->alloc_dag(finish_backfill_dag, true/*is_ha_dag*/))) {
      STORAGE_LOG(WARN, "failed to alloc finish backfill tx transfer dag ", K(ret));
    } else if (OB_FAIL(finish_backfill_dag->init(ctx_->dag_net_id_, ctx_->get_ls_id(), ctx_->tablet_infos_, ctx_))) {
      STORAGE_LOG(WARN, "failed to init data tablets transfer dag", K(ret), K(*ctx_));
    } else if (OB_ISNULL(backfill_tx_ctx = finish_backfill_dag->get_backfill_tx_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "backfill tx ctx should not be NULL", K(ret), KP(backfill_tx_ctx));
    } else if (backfill_tx_ctx->is_empty()) {
      if (OB_FAIL(this->get_dag()->add_child(*finish_backfill_dag))) {
        STORAGE_LOG(WARN, "failed to add finish backfill tx dag as chilid", K(ret), K(*ctx_));
      }
    } else {
      if (OB_FAIL(backfill_tx_ctx->get_tablet_info(tablet_info))) {
        STORAGE_LOG(WARN, "failed to get tablet id", K(ret), KPC(ctx_));
      } else if (OB_FAIL(scheduler->alloc_dag(tablet_backfill_dag, true/*is_ha_dag*/))) {
        STORAGE_LOG(WARN, "failed to alloc tablet backfill tx  dag ", K(ret));
      } else if (OB_FAIL(tablet_backfill_dag->init(ctx_->task_id_, ctx_->get_ls_id(), tablet_info, ctx_, backfill_tx_ctx, &ctx_->tablets_table_mgr_))) {
        STORAGE_LOG(WARN, "failed to init tablet backfill tx dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(this->get_dag()->add_child(*tablet_backfill_dag))) {
        STORAGE_LOG(WARN, "failed to add tablet backfill tx dag as chilid", K(ret), K(*ctx_));
      } else if (OB_FAIL(tablet_backfill_dag->create_first_task())) {
        STORAGE_LOG(WARN, "failed to create first task", K(ret));
      } else if (OB_FAIL(tablet_backfill_dag->add_child(*finish_backfill_dag))) {
        STORAGE_LOG(WARN, "failed to add child dag", K(ret), K(*ctx_));
      } else if (OB_FAIL(finish_backfill_dag->create_first_task())) {
        STORAGE_LOG(WARN, "failed to create first task", K(ret));
      } else if (OB_FAIL(scheduler->add_dag(tablet_backfill_dag, emergency))) {
        STORAGE_LOG(WARN, "failed to add tablet backfill tx dag", K(ret), K(*tablet_backfill_dag));
        if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
          STORAGE_LOG(WARN, "Fail to add task", K(ret));
          ret = OB_EAGAIN;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(scheduler->add_dag(finish_backfill_dag, emergency))) {
      STORAGE_LOG(WARN, "failed to add finish backfill tx dag", K(ret), K(*finish_backfill_dag));
      if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
        STORAGE_LOG(WARN, "Fail to add task", K(ret));
        ret = OB_EAGAIN;
      }
      if (OB_NOT_NULL(tablet_backfill_dag)) {
        if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(tablet_backfill_dag))) {
          STORAGE_LOG(WARN, "failed to cancel tablet backfill tx dag", K(tmp_ret), KPC(backfill_tx_dag));
        } else {
          tablet_backfill_dag = nullptr;
        }
      }
    } else {
      STORAGE_LOG(INFO, "[SS_TRANSFER_BACKFILL] succeed to schedule ss tablet backfill dag and ss finish backfill dag",
          KPC(tablet_backfill_dag), KPC(finish_backfill_dag));
      tablet_backfill_dag = nullptr;
      finish_backfill_dag = nullptr;
    }

    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(finish_backfill_dag)) {
        scheduler->free_dag(*finish_backfill_dag);
        finish_backfill_dag = nullptr;
      }

      if (OB_NOT_NULL(tablet_backfill_dag)) {
        scheduler->free_dag(*tablet_backfill_dag);
        tablet_backfill_dag = nullptr;
      }
    }
  }

  STORAGE_LOG(INFO, "[SS_TRANSFER_BACKFILL] finish transfer backfill tx task", K(ret), KPC(ctx_));

  return ret;
}

class ObSSFinishTransferBackfillTask: public share::ObITask
{
public:
  ObSSFinishTransferBackfillTask();
  virtual ~ObSSFinishTransferBackfillTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSSFinishTransferBackfillTask"), KP(this));
private:
  int finish_upload_(ObSSTransferBackfillTXCtx &ctx);
private:
  bool is_inited_;
  ObTransferBackfillTXBaseCtx *ha_dag_net_ctx_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSFinishTransferBackfillTask);
};

} //storage
} //oceanbase
#endif
