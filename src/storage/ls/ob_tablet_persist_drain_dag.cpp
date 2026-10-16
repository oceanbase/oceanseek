/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ls/ob_tablet_persist_drain_dag.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace storage
{

ObTabletPersistDrainParam::ObTabletPersistDrainParam()
  : ls_id_(),
    recycle_scn_(),
    tablet_ids_()
{
  tablet_ids_.set_attr(ObMemAttr(MTL_ID(), "PersistDrainIds"));
}

bool ObTabletPersistDrainParam::is_valid() const
{
  return ls_id_.is_valid()
      && recycle_scn_.is_valid()
      && tablet_ids_.count() > 0
      && tablet_ids_.count() <= MAX_TABLET_CNT;
}

void ObTabletPersistDrainParam::reset()
{
  ls_id_.reset();
  recycle_scn_.reset();
  tablet_ids_.reset();
}

int ObTabletPersistDrainParam::assign(const ObTabletPersistDrainParam &other)
{
  int ret = OB_SUCCESS;
  if (this == &other) {
  } else if (OB_FAIL(tablet_ids_.assign(other.tablet_ids_))) {
    LOG_WARN("failed to assign tablet ids", KR(ret));
  } else {
    ls_id_ = other.ls_id_;
    recycle_scn_ = other.recycle_scn_;
  }
  return ret;
}

int64_t ObTabletPersistDrainParam::get_hash() const
{
  int64_t hash_val = 0;
  hash_val = common::murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  return hash_val;
}

ObTabletPersistDrainDag::ObTabletPersistDrainDag()
  : ObIDag(ObDagType::DAG_TYPE_TABLET_FIRST_PERSIST),
    is_inited_(false),
    param_()
{
}

ObTabletPersistDrainDag::~ObTabletPersistDrainDag()
{
}

int ObTabletPersistDrainDag::init_by_param(const share::ObIDagInitParam *init_param)
{
  int ret = OB_SUCCESS;
  const ObTabletPersistDrainParam *param = nullptr;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("dag init twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(param = static_cast<const ObTabletPersistDrainParam *>(init_param))
             || OB_UNLIKELY(!param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid param", KR(ret), KPC(param));
  } else if (OB_FAIL(param_.assign(*param))) {
    LOG_WARN("failed to assign param", KR(ret), KPC(param));
  } else {
    set_max_concurrent_task_cnt(ObTabletPersistDrainParam::MAX_CONCURRENT_TASK_CNT);
    is_inited_ = true;
  }
  return ret;
}

int ObTabletPersistDrainDag::create_first_task()
{
  int ret = OB_SUCCESS;
  ObTabletPersistDrainTask *task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag not inited", KR(ret));
  } else if (OB_FAIL(alloc_task(task))) {
    LOG_WARN("failed to alloc task", KR(ret));
  } else if (OB_FAIL(task->init(0))) {
    LOG_WARN("failed to init first task", KR(ret));
  } else if (OB_FAIL(add_task(*task))) {
    LOG_WARN("failed to add first task", KR(ret));
  }
  return ret;
}

bool ObTabletPersistDrainDag::operator == (const ObIDag &other) const
{
  bool is_same = true;
  if (this == &other) {
  } else if (get_type() != other.get_type()) {
    is_same = false;
  } else {
    const ObTabletPersistDrainDag &o = static_cast<const ObTabletPersistDrainDag &>(other);
    is_same = (param_.ls_id_ == o.param_.ls_id_);
  }
  return is_same;
}

uint64_t ObTabletPersistDrainDag::hash() const
{
  return static_cast<uint64_t>(param_.get_hash());
}

int ObTabletPersistDrainDag::fill_info_param(
    compaction::ObIBasicInfoParam *&out_param,
    ObIAllocator &allocator) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("dag not inited", KR(ret));
  } else if (OB_FAIL(ADD_DAG_WARN_INFO_PARAM(out_param,
                                             allocator,
                                             get_type(),
                                             param_.ls_id_.id(),
                                             static_cast<int64_t>(param_.tablet_ids_.count())))) {
    LOG_WARN("failed to fill info param", KR(ret), K_(param));
  }
  return ret;
}

int ObTabletPersistDrainDag::fill_dag_key(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(databuff_printf(buf, buf_len, "ls_id=%ld tablet_cnt=%ld",
                              param_.ls_id_.id(),
                              param_.tablet_ids_.count()))) {
    LOG_WARN("failed to fill dag key", KR(ret), K_(param));
  }
  return ret;
}

ObTabletPersistDrainTask::ObTabletPersistDrainTask()
  : ObITask(ObITask::TASK_TYPE_TABLET_FIRST_PERSIST),
    is_inited_(false),
    dag_(nullptr),
    idx_(0)
{
}

ObTabletPersistDrainTask::~ObTabletPersistDrainTask()
{
}

int ObTabletPersistDrainTask::init(const int64_t idx)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("task init twice", KR(ret));
  } else if (OB_ISNULL(get_dag())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dag is null", KR(ret));
  } else {
    dag_ = static_cast<ObTabletPersistDrainDag *>(get_dag());
    if (OB_UNLIKELY(!dag_->get_param().is_valid()
                    || idx < 0
                    || idx * ObTabletPersistDrainParam::BATCH_SIZE
                       >= dag_->get_param().tablet_ids_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid task idx", KR(ret), K(idx), K(dag_->get_param()));
    } else {
      idx_ = idx;
      is_inited_ = true;
    }
  }
  return ret;
}

int64_t ObTabletPersistDrainTask::get_end_idx() const
{
  const int64_t cnt = dag_->get_param().tablet_ids_.count();
  const int64_t end = (idx_ + 1) * ObTabletPersistDrainParam::BATCH_SIZE;
  return end < cnt ? end : cnt;
}

int ObTabletPersistDrainTask::generate_next_task(ObITask *&next_task)
{
  int ret = OB_SUCCESS;
  next_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not inited", KR(ret));
  } else if ((idx_ + 1) * ObTabletPersistDrainParam::BATCH_SIZE
             >= dag_->get_param().tablet_ids_.count()) {
    ret = OB_ITER_END;
  } else {
    ObTabletPersistDrainTask *task = nullptr;
    if (OB_FAIL(dag_->alloc_task(task))) {
      LOG_WARN("failed to alloc next task", KR(ret));
    } else if (OB_FAIL(task->init(idx_ + 1))) {
      LOG_WARN("failed to init next task", KR(ret));
    } else {
      next_task = task;
    }
  }
  return ret;
}

int ObTabletPersistDrainTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("task not inited", KR(ret));
  } else {
    const ObTabletPersistDrainParam &param = dag_->get_param();
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    const int64_t start_idx = get_start_idx();
    const int64_t end_idx = get_end_idx();
    int64_t handled_cnt = 0;
    const int64_t start_ts = ObTimeUtility::fast_current_time();

    if (OB_FAIL(MTL(ObLSService *)->get_ls(param.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
      LOG_WARN("failed to get ls", KR(ret), K(param));
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", KR(ret), K(param));
    } else if (OB_ISNULL(ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet service is null", KR(ret), K(param));
    }

    for (int64_t i = start_idx; OB_SUCC(ret) && i < end_idx; ++i) {
      if (dag_->has_set_stop()) {
        ret = OB_CANCELED;
        LOG_INFO("persist drain dag stopped", KR(ret), K(param), K(i), K(handled_cnt));
      } else if (OB_FAIL(share::dag_yield())) {
        LOG_WARN("dag yield failed or canceled", KR(ret), K(param));
      } else {
        const ObTabletID &tablet_id = param.tablet_ids_.at(i);
        ObTabletHandle persist_handle;
        // Design: each snapshot id must call persist_if_in_memory once;
        // failure still counts as handled.
        if (OB_TMP_FAIL(ls->get_tablet_svr()->persist_if_in_memory(tablet_id, persist_handle))) {
          LOG_WARN("persist_if_in_memory failed, still count as handled",
                   KR(tmp_ret), K(param), K(tablet_id), K(i));
        }
        ++handled_cnt;
      }
    }

    const int64_t cost = ObTimeUtility::fast_current_time() - start_ts;
    LOG_INFO("persist drain task finished", KR(ret), K(param), K(start_idx), K(end_idx),
             K(handled_cnt), K(cost));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
