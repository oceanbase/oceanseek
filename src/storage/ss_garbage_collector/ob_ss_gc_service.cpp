/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/container/ob_se_array.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_force_print_log.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::common::hash;
using namespace oceanbase::share;

namespace oceanbase
{
namespace storage
{

bool ObSSGCService::TabletStudyKey::is_valid() const
{
  return tablet_id_.is_valid() && transfer_scn_.is_valid();
}

bool ObSSGCService::TabletStudyKey::operator==(const TabletStudyKey &other) const
{
  return tablet_id_ == other.tablet_id_ && transfer_scn_ == other.transfer_scn_;
}

int ObSSGCService::TabletStudyKey::hash(uint64_t &hash_val) const
{
  hash_val = 0;
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  hash_val = murmurhash(&transfer_scn_, sizeof(transfer_scn_), hash_val);
  return OB_SUCCESS;
}

ObSSGCService::LSStudyInfoCtx::LSStudyInfoCtx()
  : is_inited_(false),
    ls_epoch_(-1),
    tablet_study_info_map_()
{
}

ObSSGCService::LSStudyInfoCtx::~LSStudyInfoCtx()
{
  destroy();
}

int ObSSGCService::LSStudyInfoCtx::init(const uint64_t tenant_id, const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ls study info context init twice", KR(ret), K(tenant_id), K(ls_epoch));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || ls_epoch < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(tenant_id), K(ls_epoch));
  } else if (OB_FAIL(tablet_study_info_map_.create(
                 TABLET_STUDY_INFO_MAP_BUCKET_NUM,
                 ObMemAttr(tenant_id, "SSGCLSTabMap")))) {
    LOG_WARN("fail to create tablet study info map", KR(ret), K(tenant_id), K(ls_epoch));
  } else {
    ls_epoch_ = ls_epoch;
    is_inited_ = true;
  }
  return ret;
}

void ObSSGCService::LSStudyInfoCtx::destroy()
{
  tablet_study_info_map_.destroy();
  ls_epoch_ = -1;
  is_inited_ = false;
}

ObSSGCService::ObSSGCService()
  : is_inited_(false),
    is_stopped_(true),
    tenant_id_(OB_INVALID_TENANT_ID),
    task_runner_(),
    tablet_study_info_lock_(ObLatchIds::OB_MARK_SWEEP_LOCK),
    last_del_ls_map_cleanup_time_us_(0)
{
}

ObSSGCService::~ObSSGCService()
{
}

int ObSSGCService::mtl_init(ObSSGCService *&gc_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gc_service->init(MTL_ID()))) {
    LOG_WARN("fail to init gc service", KR(ret));
  }
  return ret;
}

int ObSSGCService::init(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t DEL_LS_MAP_BUCKET_NUM = 32;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("gc service init twice", KR(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(task_runner_.init(tenant_id))) {
    LOG_WARN("fail to init gc task runner", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ls_study_info_ctx_map_.create(
                 LS_STUDY_INFO_CTX_MAP_BUCKET_NUM,
                 ObMemAttr(tenant_id, "SSGCLSCtxMap")))) {
    LOG_WARN("fail to create ls study info context map", KR(ret), K(tenant_id));
  } else if (OB_FAIL(del_ls_with_undel_tablet_map_.create(
                 DEL_LS_MAP_BUCKET_NUM, ObMemAttr(tenant_id, "SSGCDelLsMap")))) {
    LOG_WARN("fail to create del_ls_with_undel_tablet_map", KR(ret), K(tenant_id));
  } else {
    last_del_ls_map_cleanup_time_us_ = 0;
    tenant_id_ = tenant_id;
    is_inited_ = true;
  }
  return ret;
}

int ObSSGCService::start()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gc service is not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(task_runner_.start())) {
    LOG_WARN("fail to start gc task runner", KR(ret), K_(tenant_id));
  } else {
    ATOMIC_STORE(&is_stopped_, false);
    FLOG_INFO("succ to start gc service");
  }
  return ret;
}

void ObSSGCService::stop()
{
  FLOG_INFO("start to stop gc service", K_(tenant_id));
  ATOMIC_STORE(&is_stopped_, true);
  task_runner_.stop();
  FLOG_INFO("finish to stop gc service", K_(tenant_id));
}

void ObSSGCService::wait()
{
  FLOG_INFO("start to wait gc service", K_(tenant_id));
  task_runner_.wait();
  FLOG_INFO("finish to wait gc service", K_(tenant_id));
}

void ObSSGCService::destroy()
{
  FLOG_INFO("start to destroy gc service", K_(tenant_id));
  last_del_ls_map_cleanup_time_us_ = 0;
  del_ls_with_undel_tablet_map_.destroy();
  destroy_ls_study_info_ctxs_();
  tablet_info_map_.destroy();
  task_runner_.destroy();
  tenant_id_ = OB_INVALID_TENANT_ID;
  ATOMIC_STORE(&is_stopped_, true);
  is_inited_ = false;
  FLOG_INFO("finish to destroy gc service", K_(tenant_id));
}

bool ObSSGCService::need_retry_mark_sweep_dag() const
{
  return task_runner_.need_retry_mark_sweep_dag();
}

void ObSSGCService::set_need_retry_mark_sweep_dag(const bool need_retry)
{
  task_runner_.set_need_retry_mark_sweep_dag(need_retry);
}

void ObSSGCService::report_data_check_dag_result(
    const bool need_retry,
    const int64_t finish_ts)
{
  task_runner_.report_data_check_dag_result(need_retry, finish_ts);
}

int ObSSGCService::check_and_update_local_tablet_study_info(
    const ObSSGCTablet &gc_tablet,
    const ObSSGCTabletStudyInfo &study_info,
    const int64_t ls_epoch)
{
  int ret = OB_SUCCESS;
  LSStudyInfoCtx *pending_study_ctx = nullptr;
  LSStudyInfoCtx *retired_study_ctx = nullptr;
  bool need_prepare_study_ctx = false;
  // 第一次调用持锁检查当前 ctx。
  // 同 epoch 时直接更新；ctx 不存在或 epoch 落后时，只返回锁外准备请求。
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gc service is not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(update_ls_study_info_ctx_with_lock_(
                 gc_tablet, study_info, ls_epoch, retired_study_ctx, need_prepare_study_ctx))) {
    LOG_WARN("fail to check and update local tablet study info", KR(ret),
        K(gc_tablet), K(study_info), K(ls_epoch));
  } else if (need_prepare_study_ctx) {
    // 第一次调用只返回准备请求；这里在锁外创建固定 bucket 的 ctx。
    // 新 ctx 预先写入首个 StudyKey，尚未发布到 outer map。
    // 插入前重新持锁复查当前 ctx。
    // 状态匹配才发布新 ctx；同 epoch ctx 已被并发发布时，
    // 在其中插入或合并本次 Study 结果。
    if (OB_FAIL(prepare_ls_study_info_ctx_(
            gc_tablet, study_info, ls_epoch, pending_study_ctx))) {
      LOG_WARN("fail to prepare ls study info context", KR(ret),
          K(gc_tablet), K(study_info), K(ls_epoch));
    } else if (OB_FAIL(insert_ls_study_info_ctx_with_lock_(
                   gc_tablet, study_info, ls_epoch, pending_study_ctx, retired_study_ctx))) {
      LOG_WARN("fail to insert ls study info context", KR(ret),
          K(gc_tablet), K(study_info), K(ls_epoch));
    }
  }
  destroy_ls_study_info_ctx_(pending_study_ctx);
  destroy_ls_study_info_ctx_(retired_study_ctx);
  return ret;
}

int ObSSGCService::update_ls_study_info_ctx_with_lock_(
    const ObSSGCTablet &gc_tablet,
    const ObSSGCTabletStudyInfo &study_info,
    const int64_t ls_epoch,
    LSStudyInfoCtx *&retired_study_ctx,
    bool &need_prepare_study_ctx)
{
  int ret = OB_SUCCESS;
  int ctx_hash_ret = OB_SUCCESS;
  LSStudyInfoCtx *study_ctx = nullptr;
  need_prepare_study_ctx = false;
  if (OB_UNLIKELY(!gc_tablet.is_valid()
      || !gc_tablet.transfer_scn_.is_valid()
      || !study_info.min_dep_meta_scn_.is_valid_and_not_min()
      || study_info.min_major_snapshot_ < 0
      || ls_epoch < 0
      || OB_NOT_NULL(retired_study_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(gc_tablet), K(study_info), K(ls_epoch),
        KP(retired_study_ctx));
  } else {
    // map 比较更新、终态删除和后台清理共用同一把锁。
    lib::ObMutexGuard guard(tablet_study_info_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("fail to lock tablet study info", KR(ret), K(gc_tablet));
    } else {
      ctx_hash_ret = ls_study_info_ctx_map_.get_refactored(gc_tablet.ls_id_, study_ctx);
      if (OB_HASH_NOT_EXIST == ctx_hash_ret) {
        need_prepare_study_ctx = study_info.has_finite();
      } else if (OB_SUCCESS != ctx_hash_ret) {
        ret = ctx_hash_ret;
        LOG_WARN("fail to get ls study info context", KR(ret), K(gc_tablet), K(ls_epoch));
      } else if (OB_ISNULL(study_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls study info context is null", KR(ret), K(gc_tablet), K(ls_epoch));
      } else if (study_ctx->get_ls_epoch() > ls_epoch) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tablet study ls epoch reverted", KR(ret),
            K(gc_tablet), K(ls_epoch), "saved_ls_epoch", study_ctx->get_ls_epoch());
      } else if (study_ctx->get_ls_epoch() < ls_epoch) {
        if (study_info.has_finite()) {
          need_prepare_study_ctx = true;
        } else if (OB_FAIL(erase_ls_study_info_ctx_without_lock_(
                       gc_tablet.ls_id_, study_ctx, retired_study_ctx))) {
          LOG_WARN("fail to erase old ls study info context", KR(ret), K(gc_tablet));
        }
      } else if (OB_FAIL(check_and_update_tablet_study_info_without_lock_(
                     gc_tablet, study_info, study_ctx->get_tablet_study_info_map()))) {
        LOG_WARN("fail to check and update tablet study info", KR(ret),
            K(gc_tablet), K(study_info), K(ls_epoch));
      }
    }
  }
  return ret;
}

int ObSSGCService::insert_ls_study_info_ctx_with_lock_(
    const ObSSGCTablet &gc_tablet,
    const ObSSGCTabletStudyInfo &study_info,
    const int64_t ls_epoch,
    LSStudyInfoCtx *&pending_study_ctx,
    LSStudyInfoCtx *&retired_study_ctx)
{
  int ret = OB_SUCCESS;
  int ctx_hash_ret = OB_SUCCESS;
  int overwrite = 0;
  LSStudyInfoCtx *study_ctx = nullptr;
  bool need_insert_study_ctx = false;
  if (OB_UNLIKELY(!gc_tablet.is_valid()
      || !gc_tablet.transfer_scn_.is_valid()
      || !study_info.min_dep_meta_scn_.is_valid_and_not_min()
      || study_info.min_major_snapshot_ < 0
      || ls_epoch < 0
      || !study_info.has_finite()
      || OB_ISNULL(pending_study_ctx)
      || pending_study_ctx->get_ls_epoch() != ls_epoch
      || OB_NOT_NULL(retired_study_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(gc_tablet), K(study_info), K(ls_epoch),
        KP(pending_study_ctx), KP(retired_study_ctx));
  } else {
    lib::ObMutexGuard guard(tablet_study_info_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("fail to lock tablet study info", KR(ret), K(gc_tablet));
    } else {
      ctx_hash_ret = ls_study_info_ctx_map_.get_refactored(gc_tablet.ls_id_, study_ctx);
      if (OB_HASH_NOT_EXIST == ctx_hash_ret) {
        need_insert_study_ctx = true;
      } else if (OB_SUCCESS != ctx_hash_ret) {
        ret = ctx_hash_ret;
        LOG_WARN("fail to get ls study info context", KR(ret), K(gc_tablet), K(ls_epoch));
      } else if (OB_ISNULL(study_ctx)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("ls study info context is null", KR(ret), K(gc_tablet), K(ls_epoch));
      } else if (study_ctx->get_ls_epoch() > ls_epoch) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("tablet study ls epoch reverted", KR(ret),
            K(gc_tablet), K(ls_epoch), "saved_ls_epoch", study_ctx->get_ls_epoch());
      } else if (study_ctx->get_ls_epoch() < ls_epoch) {
        need_insert_study_ctx = true;
        overwrite = 1;
      } else if (OB_FAIL(check_and_update_tablet_study_info_without_lock_(
                     gc_tablet, study_info, study_ctx->get_tablet_study_info_map()))) {
        LOG_WARN("fail to check and update tablet study info", KR(ret),
            K(gc_tablet), K(study_info), K(ls_epoch));
      }
      if (OB_SUCC(ret) && need_insert_study_ctx) {
        if (OB_FAIL(ls_study_info_ctx_map_.set_refactored(
                gc_tablet.ls_id_, pending_study_ctx, overwrite))) {
          LOG_WARN("fail to insert ls study info context", KR(ret),
              K(gc_tablet), K(ls_epoch), K(overwrite));
        } else {
          // outer map 接管新 ctx，旧 ctx 交给调用方锁外销毁。
          retired_study_ctx = study_ctx;
          pending_study_ctx = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObSSGCService::create_ls_study_info_ctx_(const int64_t ls_epoch, LSStudyInfoCtx *&study_ctx)
{
  int ret = OB_SUCCESS;
  LSStudyInfoCtx *new_ctx = nullptr;
  if (OB_UNLIKELY(ls_epoch < 0
      || !is_valid_tenant_id(tenant_id_)
      || OB_NOT_NULL(study_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_epoch), K_(tenant_id), KP(study_ctx));
  } else if (OB_ISNULL(new_ctx = static_cast<LSStudyInfoCtx *>(ob_malloc(
          sizeof(LSStudyInfoCtx), ObMemAttr(tenant_id_, "SSGCLSCtx"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate ls study info context", KR(ret), K(ls_epoch));
  } else {
    new (new_ctx) LSStudyInfoCtx();
    if (OB_FAIL(new_ctx->init(tenant_id_, ls_epoch))) {
      LOG_WARN("fail to init ls study info context", KR(ret), K(ls_epoch));
    } else {
      study_ctx = new_ctx;
      new_ctx = nullptr;
    }
  }
  destroy_ls_study_info_ctx_(new_ctx);
  return ret;
}

int ObSSGCService::prepare_ls_study_info_ctx_(
    const ObSSGCTablet &gc_tablet,
    const ObSSGCTabletStudyInfo &study_info,
    const int64_t ls_epoch,
    LSStudyInfoCtx *&pending_study_ctx)
{
  int ret = OB_SUCCESS;
  LSStudyInfoCtx *new_ctx = nullptr;
  const TabletStudyKey study_key(gc_tablet.tablet_id_, gc_tablet.transfer_scn_);
  if (OB_UNLIKELY(!gc_tablet.is_valid()
      || !gc_tablet.transfer_scn_.is_valid()
      || !study_info.min_dep_meta_scn_.is_valid_and_not_min()
      || study_info.min_major_snapshot_ < 0
      || ls_epoch < 0
      || !study_info.has_finite()
      || OB_NOT_NULL(pending_study_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(gc_tablet), K(study_info), K(ls_epoch),
        KP(pending_study_ctx));
  } else if (OB_FAIL(create_ls_study_info_ctx_(ls_epoch, new_ctx))) {
    LOG_WARN("fail to create ls study info context", KR(ret), K(gc_tablet), K(ls_epoch));
  } else if (OB_FAIL(new_ctx->get_tablet_study_info_map().set_refactored(
                 study_key, study_info, 0 /* 不覆盖 */))) {
    LOG_WARN("fail to insert tablet study info", KR(ret), K(gc_tablet), K(study_info));
  } else {
    pending_study_ctx = new_ctx;
    new_ctx = nullptr;
  }
  destroy_ls_study_info_ctx_(new_ctx);
  return ret;
}

int ObSSGCService::check_and_update_tablet_study_info_without_lock_(
    const ObSSGCTablet &gc_tablet,
    const ObSSGCTabletStudyInfo &study_info,
    TabletStudyInfoMap &tablet_study_info_map)
{
  int ret = OB_SUCCESS;
  ObSSGCTabletStudyInfo last_study_info;
  ObSSGCTabletStudyInfo merged_study_info;
  const TabletStudyKey study_key(gc_tablet.tablet_id_, gc_tablet.transfer_scn_);
  bool need_update = false;
  const bool has_finite_rely = study_info.has_finite_rely();
  const bool has_finite_major = study_info.has_finite_major();
  const int hash_ret = tablet_study_info_map.get_refactored(study_key, last_study_info);
  if (OB_HASH_NOT_EXIST == hash_ret) {
    if (study_info.has_finite()
        && OB_FAIL(tablet_study_info_map.set_refactored(
            study_key, study_info, 0 /* 不覆盖 */))) {
      LOG_WARN("fail to insert tablet study info", KR(ret), K(gc_tablet), K(study_info));
    }
  } else if (OB_SUCCESS != hash_ret) {
    ret = hash_ret;
    LOG_WARN("fail to get tablet study info", KR(ret), K(gc_tablet));
  } else if (OB_UNLIKELY(
      !last_study_info.min_dep_meta_scn_.is_valid_and_not_min()
      || last_study_info.min_major_snapshot_ < 0
      || !last_study_info.has_finite())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid saved tablet study info", KR(ret), K(gc_tablet), K(last_study_info));
  } else if (has_finite_rely
      && last_study_info.has_finite_rely()
      && last_study_info.min_dep_meta_scn_ > study_info.min_dep_meta_scn_) {
    // 位点回退破坏 Study 单调性，保留原高水位并返回错误。
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet study rely meta scn reverted", KR(ret), K(gc_tablet),
        K(last_study_info), K(study_info));
  } else if (has_finite_major
      && last_study_info.has_finite_major()
      && last_study_info.min_major_snapshot_ > study_info.min_major_snapshot_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet study major snapshot reverted", KR(ret), K(gc_tablet),
        K(last_study_info), K(study_info));
  } else {
    // 先校验回退再合并，确保任一字段回退时不会提交另一个字段的推进。
    merged_study_info = last_study_info;
    if (has_finite_rely
        && (!merged_study_info.has_finite_rely()
            || merged_study_info.min_dep_meta_scn_ < study_info.min_dep_meta_scn_)) {
      merged_study_info.min_dep_meta_scn_ = study_info.min_dep_meta_scn_;
      need_update = true;
    }
    if (has_finite_major
        && (!merged_study_info.has_finite_major()
            || merged_study_info.min_major_snapshot_ < study_info.min_major_snapshot_)) {
      merged_study_info.min_major_snapshot_ = study_info.min_major_snapshot_;
      need_update = true;
    }
    if (need_update && OB_FAIL(tablet_study_info_map.set_refactored(
                           study_key, merged_study_info, 1 /* 覆盖 */))) {
      LOG_WARN("fail to update tablet study info", KR(ret),
          K(gc_tablet), K(study_info), K(merged_study_info));
    }
  }
  return ret;
}

int ObSSGCService::erase_empty_ls_study_info_ctx_without_lock_(
    const ObLSID &ls_id,
    LSStudyInfoCtx *&study_ctx,
    LSStudyInfoCtx *&retired_study_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || OB_ISNULL(study_ctx)
      || OB_NOT_NULL(retired_study_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id),
        KP(study_ctx), KP(retired_study_ctx));
  } else if (study_ctx->get_tablet_study_info_map().size() > 0) {
    // 非空 context 继续保留。
  } else if (OB_FAIL(erase_ls_study_info_ctx_without_lock_(
                 ls_id, study_ctx, retired_study_ctx))) {
    LOG_WARN("fail to erase empty ls study info context", KR(ret), K(ls_id));
  }
  return ret;
}

int ObSSGCService::erase_ls_study_info_ctx_without_lock_(
    const ObLSID &ls_id,
    LSStudyInfoCtx *&study_ctx,
    LSStudyInfoCtx *&retired_study_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || OB_ISNULL(study_ctx)
      || OB_NOT_NULL(retired_study_ctx))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id),
        KP(study_ctx), KP(retired_study_ctx));
  } else if (OB_FAIL(ls_study_info_ctx_map_.erase_refactored(ls_id))) {
    LOG_WARN("fail to erase ls study info context", KR(ret), K(ls_id));
  } else {
    retired_study_ctx = study_ctx;
    study_ctx = nullptr;
  }
  return ret;
}

int ObSSGCService::erase_local_tablet_study_info_without_lock_(
    const ObSSGCTablet &gc_tablet,
    const int64_t ls_epoch,
    LSStudyInfoCtx *&retired_study_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LSStudyInfoCtx *study_ctx = nullptr;
  const TabletStudyKey study_key(gc_tablet.tablet_id_, gc_tablet.transfer_scn_);
  bool can_erase_empty_ctx = false;
  const int hash_ret = ls_study_info_ctx_map_.get_refactored(gc_tablet.ls_id_, study_ctx);
  if (OB_HASH_NOT_EXIST == hash_ret) {
  } else if (OB_SUCCESS != hash_ret) {
    ret = hash_ret;
    LOG_WARN("fail to get ls study info context", KR(ret), K(gc_tablet), K(ls_epoch));
  } else if (OB_ISNULL(study_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls study info context is null", KR(ret), K(gc_tablet), K(ls_epoch));
  } else if (study_ctx->get_ls_epoch() > ls_epoch) {
    // 迟到的旧实例请求不能删除新实例的位点。
  } else if (study_ctx->get_ls_epoch() < ls_epoch) {
    // 新实例的删除请求同时使旧实例全部位点失效。
    if (OB_FAIL(erase_ls_study_info_ctx_without_lock_(
            gc_tablet.ls_id_, study_ctx, retired_study_ctx))) {
      LOG_WARN("fail to erase old ls study info context", KR(ret),
          K(gc_tablet), K(ls_epoch));
    }
  } else if (OB_FAIL(study_ctx->get_tablet_study_info_map().erase_refactored(study_key))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      can_erase_empty_ctx = true;
    }
  } else {
    can_erase_empty_ctx = true;
  }
  if (OB_SUCC(ret) && can_erase_empty_ctx) {
    if (OB_TMP_FAIL(erase_empty_ls_study_info_ctx_without_lock_(
            gc_tablet.ls_id_, study_ctx, retired_study_ctx))) {
      LOG_WARN("fail to erase empty ls study info context", KR(tmp_ret), K(gc_tablet));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObSSGCService::check_local_tablet_study_info_stale_(
    const ObSSGCTablet &gc_tablet,
    const int64_t ls_epoch,
    bool &is_stale) const
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = nullptr;
  ObTabletResidentInfo resident_info;
  const ObTabletMapKey tablet_key(gc_tablet.ls_id_, gc_tablet.tablet_id_);
  is_stale = false;
  if (OB_ISNULL(t3m = MTL(ObTenantMetaMemMgr *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObTenantMetaMemMgr is NULL", KR(ret), K(gc_tablet));
  } else if (OB_FAIL(t3m->get_tablet_resident_info(tablet_key, resident_info))) {
    if (OB_ENTRY_NOT_EXIST == ret || OB_HASH_NOT_EXIST == ret) {
      // T3M pointer 缺失时可以回收该 StudyKey。
      // 若并发的迟到 Study 再次写入，由后续周期继续清理。
      ret = OB_SUCCESS;
      is_stale = true;
    } else {
      LOG_WARN("fail to get tablet resident info", KR(ret), K(gc_tablet));
    }
  } else {
    const SCN current_reorganization_scn = resident_info.attr_.get_reorganization_scn();
    const int64_t current_ls_epoch = resident_info.addr_.get_epoch();
    if (OB_UNLIKELY(!resident_info.is_valid()
        || !current_reorganization_scn.is_valid()
        || ls_epoch < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tablet study state", KR(ret),
          K(gc_tablet), K(resident_info), K(ls_epoch));
    } else if (current_ls_epoch > ls_epoch) {
      // 当前 tablet 已属于更新的 LS 实例，旧实例基线可以回收。
      is_stale = true;
    } else if (current_ls_epoch < ls_epoch) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("tablet ls epoch reverted", KR(ret),
          K(gc_tablet), K(ls_epoch), K(current_ls_epoch));
    } else if (current_reorganization_scn > gc_tablet.transfer_scn_) {
      // 当前代际已经推进，旧 StudyKey 不会再次成为本地有效代际。
      is_stale = true;
    } else {
      // current_reorganization_scn <= gc_tablet.transfer_scn_
      // 当前代际未超过 StudyKey 代际，
      // 不能证明 StudyKey 已经过期，继续保留。
    }
  }
  return ret;
}

int ObSSGCService::check_and_erase_stale_local_tablet_study_info_(
    const TabletStudySnapshot &study_snapshot,
    bool &is_erased)
{
  int ret = OB_SUCCESS;
  bool is_stale = false;
  LSStudyInfoCtx *study_ctx = nullptr;
  LSStudyInfoCtx *retired_study_ctx = nullptr;
  ObSSGCTabletStudyInfo study_info;
  const ObSSGCTablet &gc_tablet = study_snapshot.gc_tablet_;
  const TabletStudyKey study_key(gc_tablet.tablet_id_, gc_tablet.transfer_scn_);
  is_erased = false;
  {
    lib::ObMutexGuard guard(tablet_study_info_lock_);
    if (OB_FAIL(guard.get_ret())) {
      LOG_WARN("fail to lock tablet study info", KR(ret), K(gc_tablet));
    } else if (OB_FAIL(ls_study_info_ctx_map_.get_refactored(
                   gc_tablet.ls_id_, study_ctx))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get ls study info context", KR(ret), K(gc_tablet));
      }
    } else if (OB_ISNULL(study_ctx)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("ls study info context is null", KR(ret), K(gc_tablet));
    } else if (study_ctx->get_ls_epoch() != study_snapshot.ls_epoch_) {
      // 快照后已切换 LS 实例，不能删除当前基线。
    } else if (OB_FAIL(study_ctx->get_tablet_study_info_map().get_refactored(
                   study_key, study_info))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get tablet study info", KR(ret), K(gc_tablet));
      }
    } else if (OB_FAIL(check_local_tablet_study_info_stale_(
                   gc_tablet, study_ctx->get_ls_epoch(), is_stale))) {
      LOG_WARN("fail to recheck local tablet study info", KR(ret), K(gc_tablet));
    } else if (is_stale
        && OB_FAIL(study_ctx->get_tablet_study_info_map().erase_refactored(study_key))) {
      LOG_WARN("fail to erase stale local tablet study info", KR(ret), K(gc_tablet));
    } else {
      is_erased = is_stale;
    }
    if (OB_SUCC(ret) && is_erased) {
      if (OB_FAIL(erase_empty_ls_study_info_ctx_without_lock_(
              gc_tablet.ls_id_, study_ctx, retired_study_ctx))) {
        LOG_WARN("fail to erase empty ls study info context", KR(ret), K(gc_tablet));
      }
    }
  }
  destroy_ls_study_info_ctx_(retired_study_ctx);
  return ret;
}

int ObSSGCService::get_tablet_study_snapshots_without_lock_(
    ObIArray<TabletStudySnapshot> &study_snapshots) const
{
  int ret = OB_SUCCESS;
  ObSSGCTablet gc_tablet;
  LSStudyInfoCtxMap::const_iterator ls_iter = ls_study_info_ctx_map_.begin();
  for (; OB_SUCC(ret) && ls_iter != ls_study_info_ctx_map_.end(); ++ls_iter) {
    const ObLSID &ls_id = ls_iter->first;
    const LSStudyInfoCtx *study_ctx = ls_iter->second;
    if (OB_UNLIKELY(!ls_id.is_valid() || OB_ISNULL(study_ctx)
        || study_ctx->get_ls_epoch() < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("invalid ls study info context", KR(ret),
          K(ls_id), KP(study_ctx));
    } else {
      const TabletStudyInfoMap &tablet_study_info_map = study_ctx->get_tablet_study_info_map();
      TabletStudyInfoMap::const_iterator tablet_iter = tablet_study_info_map.begin();
      for (; OB_SUCC(ret) && tablet_iter != tablet_study_info_map.end(); ++tablet_iter) {
        const TabletStudyKey &study_key = tablet_iter->first;
        const ObSSGCTabletStudyInfo &study_info = tablet_iter->second;
        gc_tablet = ObSSGCTablet(ls_id, study_key.tablet_id_, study_key.transfer_scn_);
        if (OB_UNLIKELY(!study_key.is_valid()
            || !study_info.min_dep_meta_scn_.is_valid_and_not_min()
            || study_info.min_major_snapshot_ < 0
            || !study_info.has_finite())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("invalid saved tablet study info", KR(ret),
              K(ls_id), K(study_key), K(study_info));
        } else if (OB_FAIL(study_snapshots.push_back(
                       TabletStudySnapshot(gc_tablet, study_ctx->get_ls_epoch())))) {
          LOG_WARN("fail to save tablet study snapshot", KR(ret), K(gc_tablet));
        }
      }
    }
  }
  return ret;
}

int ObSSGCService::cleanup_stale_local_tablet_study_info()
{
  const int64_t DEFAULT_STUDY_SNAPSHOT_CNT = 1024;
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t cleaned_cnt = 0;
  ObSEArray<TabletStudySnapshot, OB_DEFAULT_SE_ARRAY_COUNT> study_snapshots;
  study_snapshots.set_attr(ObMemAttr(tenant_id_, "SSGCStudySnap"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gc service is not init", KR(ret), K_(tenant_id));
  } else if (OB_FAIL(study_snapshots.reserve(DEFAULT_STUDY_SNAPSHOT_CNT))) {
    LOG_WARN("fail to reserve tablet study snapshots", KR(ret), K_(tenant_id),
        K(DEFAULT_STUDY_SNAPSHOT_CNT));
  } else {
    {
      // 短锁内只复制 key 和 LS epoch，
      // 锁外初筛后仅对候选 key 加锁复查并删除。
      lib::ObMutexGuard guard(tablet_study_info_lock_);
      if (OB_FAIL(guard.get_ret())) {
        LOG_WARN("fail to lock tablet study info", KR(ret), K_(tenant_id));
      } else if (OB_FAIL(get_tablet_study_snapshots_without_lock_(study_snapshots))) {
        LOG_WARN("fail to get tablet study snapshots", KR(ret), K_(tenant_id));
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; i < study_snapshots.count(); ++i) {
        bool is_stale = false;
        bool is_erased = false;
        const TabletStudySnapshot &study_snapshot = study_snapshots.at(i);
        if (OB_TMP_FAIL(check_local_tablet_study_info_stale_(
                study_snapshot.gc_tablet_, study_snapshot.ls_epoch_, is_stale))) {
          // 状态不确定时保留位点，下一轮继续核对。
          LOG_WARN("fail to check local tablet study info", KR(tmp_ret), K(study_snapshot));
          ret = COVER_SUCC(tmp_ret);
        } else if (is_stale
            && OB_TMP_FAIL(check_and_erase_stale_local_tablet_study_info_(
                   study_snapshot, is_erased))) {
          LOG_WARN("fail to check and erase stale local tablet study info",
              KR(tmp_ret), K(study_snapshot));
          ret = COVER_SUCC(tmp_ret);
        } else if (is_erased) {
          ++cleaned_cnt;
        }
      }
    }
    if (cleaned_cnt > 0) {
      LOG_INFO("finish to cleanup stale local tablet study info", K_(tenant_id),
          K(cleaned_cnt), "snapshot_cnt", study_snapshots.count());
    }
  }
  return ret;
}

void ObSSGCService::destroy_ls_study_info_ctx_(LSStudyInfoCtx *&study_ctx)
{
  if (OB_NOT_NULL(study_ctx)) {
    study_ctx->~LSStudyInfoCtx();
    ob_free(study_ctx);
    study_ctx = nullptr;
  }
}

void ObSSGCService::destroy_ls_study_info_ctxs_()
{
  LSStudyInfoCtxMap::iterator iter = ls_study_info_ctx_map_.begin();
  for (; iter != ls_study_info_ctx_map_.end(); ++iter) {
    LSStudyInfoCtx *study_ctx = iter->second;
    if (OB_ISNULL(study_ctx)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "ls study info context is null", K(iter->first));
    } else {
      destroy_ls_study_info_ctx_(study_ctx);
    }
  }
  ls_study_info_ctx_map_.destroy();
}

int ObSSGCService::mark_del_ls_with_undel_tablet(
    const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  const int64_t now_us = ObTimeUtility::current_time_us();
  int64_t first_observed_us = now_us;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gc service is not init", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id));
  } else if (OB_FAIL(del_ls_with_undel_tablet_map_.set_refactored(ls_id, now_us, 0/*overwrite*/))) {
    if (OB_HASH_EXIST == ret) {
      // already marked, keep the original first_observed_us.
      // Note: get_refactored here and erase_refactored in cleanup_stale_del_ls_with_undel_tablet
      // run in one thread, so there is no race condition and we can safely get_refactored here.
      ret = OB_SUCCESS;
      if (OB_FAIL(del_ls_with_undel_tablet_map_.get_refactored(ls_id, first_observed_us))) {
        LOG_WARN("fail to get first observed time", KR(ret), K(ls_id));
      }
    } else {
      LOG_WARN("fail to set del_ls_with_undel_tablet_map", KR(ret), K(ls_id), K(now_us));
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t undel_duration_us = now_us - first_observed_us;
    if (undel_duration_us >= DEL_LS_WITH_UNDEL_TABLET_ERROR_THRESHOLD_US) {
      LOG_ERROR("ls has been deleted, but some tablet not mark deleted for over 1 day",
          K(ls_id), K(undel_duration_us));
    }
  }
  return ret;
}

int ObSSGCService::erase_del_ls_with_undel_tablet(const ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gc service is not init", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", KR(ret), K(ls_id));
  } else if (OB_FAIL(del_ls_with_undel_tablet_map_.erase_refactored(ls_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // not recorded, nothing to erase
    } else {
      LOG_WARN("fail to erase del_ls_with_undel_tablet_map", KR(ret), K(ls_id));
    }
  }
  return ret;
}

int ObSSGCService::cleanup_stale_del_ls_with_undel_tablet()
{
  int ret = OB_SUCCESS;
  const int64_t now_us = ObTimeUtility::current_time_us();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("gc service is not init", KR(ret), K_(tenant_id));
  } else if ((now_us - last_del_ls_map_cleanup_time_us_) < DEL_LS_MAP_CLEANUP_INTERVAL_US) {
    // do nothing, only run every DEL_LS_MAP_CLEANUP_INTERVAL_US
  } else {
    // collect stale ls_id first, then erase (avoid mutating map during iteration)
    ObSEArray<ObLSID, 16> stale_ls_ids;
    ObHashMap<ObLSID, int64_t>::const_iterator iter = del_ls_with_undel_tablet_map_.begin();
    for (; OB_SUCC(ret) && (iter != del_ls_with_undel_tablet_map_.end()); ++iter) {
      const int64_t first_observed_us = iter->second;
      if ((now_us - first_observed_us) >= DEL_LS_MAP_CLEANUP_INTERVAL_US) {
        if (OB_FAIL(stale_ls_ids.push_back(iter->first))) {
          LOG_WARN("fail to push back stale ls_id", KR(ret), K(iter->first));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && (i < stale_ls_ids.count()); ++i) {
      if (OB_FAIL(del_ls_with_undel_tablet_map_.erase_refactored(stale_ls_ids.at(i)))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to erase stale ls_id", KR(ret), K(stale_ls_ids.at(i)));
        }
      }
    }
    if (OB_SUCC(ret)) {
      last_del_ls_map_cleanup_time_us_ = now_us;
      FLOG_INFO("finish to cleanup stale del_ls_with_undel_tablet records", K_(tenant_id),
          "cleaned_cnt", stale_ls_ids.count(), "remain_cnt", del_ls_with_undel_tablet_map_.size(),
          K(stale_ls_ids));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
