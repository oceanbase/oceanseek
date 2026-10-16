/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_ss_diagnose.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/ob_server_struct.h"
#include "storage/incremental/share/ob_ss_diagnose_mgr.h"
// TODO(remove-old-gc-code): #include "storage/incremental/garbage_collector/ob_ss_garbage_collector_service.h"
#include "storage/incremental/ob_ss_minor_compaction.h"


using namespace oceanbase::share;
namespace oceanbase
{
namespace storage
{

const char *ObSSDiagnoseInfo::ObDiagnoseTypeStr[DIA_TYPE_MAX] = {
    "INVALID",
    "SS_META_READ_WRITE",
    "SS_GC",
    "SS_UPLOAD",
    "SS_MINOR",
    "SS_MAJOR",
    "SS_ATTACH",
    "SS_SSTABLE_GC",
    "SS_SCHE_UPLOAD",
    "SS_CKPT"
};

const char * ObSSDiagnoseInfo::ss_diagnose_type_to_str(const int64_t diagnose_type)
{
  STATIC_ASSERT(DIA_TYPE_MAX == ARRAYSIZEOF(ObDiagnoseTypeStr), "diagnose type str len is mismatch");
  const char *str = "";
  if (diagnose_type >= DIA_TYPE_MAX || diagnose_type < DIA_TYPE_INVALID) {
    str = "invalid_status";
  } else {
    str = ObDiagnoseTypeStr[diagnose_type];
  }
  return str;
}

int ObSSDiagnoseIterator::get_diagnose_info(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObSSDiagnoseGetter diagnose_mgr;
  void * buf = nullptr;
  if (NULL == (buf = allocator_.alloc(sizeof(ObSSDiagnoseInfo) * MAX_DIAGNOSE_INFO_CNT))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc info array", K(ret));
  } else if (FALSE_IT(info_array_ = new (buf) ObSSDiagnoseInfo[MAX_DIAGNOSE_INFO_CNT])) {
  } else if (OB_FAIL(diagnose_mgr.init(&allocator_, info_array_, MAX_DIAGNOSE_INFO_CNT))) {
    LOG_WARN("failed to init diagnose info mgr", K(ret));
  } else if (OB_FAIL(diagnose_mgr.diagnose(tenant_id))) {
    LOG_WARN("failed to diagnose ss info", K(ret));
  } else {
    cnt_ = diagnose_mgr.get_cnt();
  }
  return ret;
}

int ObSSDiagnoseIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObSSDiagnoseIterator has been opened", K(ret));
  } else if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_diagnose_info(tenant_id))) {
    LOG_WARN("failed to get diagnose info", K(ret));
  } else {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

void ObSSDiagnoseIterator::reset()
{
  if (OB_NOT_NULL(info_array_)) {
    allocator_.free(info_array_);
    info_array_ = nullptr;
  }
  cnt_ = 0;
  cur_idx_ = 0;
  is_opened_ = false;
}

int ObSSDiagnoseIterator::get_next_info(ObSSDiagnoseInfo &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cur_idx_ >= cnt_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(info_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array is null", K(ret));
  } else {
    info = info_array_[cur_idx_++];
  }
  return ret;
}

ObSSDiagnoseGetter::ObSSDiagnoseGetter()
 : is_inited_(false),
   info_array_(nullptr),
   max_cnt_(0),
   idx_(0)
  {
  }

void ObSSDiagnoseGetter::reset()
{
  info_array_ = nullptr;
  max_cnt_ = 0;
  idx_ = 0;
  is_inited_ = false;
}

int ObSSDiagnoseGetter::init(
    common::ObIAllocator *allocator,
    ObSSDiagnoseInfo *info_array,
    const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObSSDiagnoseGetter has already been initiated", K(ret));
  } else if (OB_UNLIKELY(nullptr == info_array || max_cnt <= 0 || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(info_array), K(max_cnt));
  } else {
    info_array_ = info_array;
    max_cnt_ = max_cnt;
    is_inited_ = true;
  }
  if (!is_inited_) {
    reset();
  }
  return ret;
}

int ObSSDiagnoseGetter::diagnose(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
  if (OB_SYS_TENANT_ID == tenant_id) {
    GCTX.omt_->get_tenant_ids(all_tenants);
  } else if (OB_FAIL(all_tenants.push_back(tenant_id))) {
    LOG_WARN("failed to push back tenant_id", K(ret), K(tenant_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
    uint64_t tenant_id = all_tenants[i];
    if (!is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
      MTL_SWITCH(tenant_id) {
        (void) diagnose_ss_meta_service_();
        (void) diagnose_ss_upload_();
        (void) diagnose_ss_minor_();
        (void) diagnose_ss_major_();
        (void) diagnose_ss_checkpoint_();
        (void) diagnose_ss_notify_();
        (void) diagnose_ss_gc_();
      } else {
        if (OB_TENANT_NOT_IN_SERVER != ret) {
          STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
          continue;
        }
      }
    }
  }
  return ret;
}
void ObSSDiagnoseGetter::diagnose_ss_meta_service_()
{
  MTL(ObSSDiagnoseInfoMgr *)->get_diagnose_info(ObSSDiagnoseInfo::DIA_TYPE_META_READ_WRITE, info_array_, idx_, max_cnt_);
}

void ObSSDiagnoseGetter::diagnose_ss_upload_()
{
  MTL(ObSSDiagnoseInfoMgr *)->get_diagnose_info(ObSSDiagnoseInfo::DIA_TYPE_SSTABLE_UPLOAD, info_array_, idx_, max_cnt_);
  MTL(ObSSDiagnoseInfoMgr *)->get_diagnose_info(ObSSDiagnoseInfo::DIA_TYPE_SCHE_UPLOAD, info_array_, idx_, max_cnt_);
}

void ObSSDiagnoseGetter::diagnose_ss_minor_()
{
  MTL(ObSSDiagnoseInfoMgr *)->get_diagnose_info(ObSSDiagnoseInfo::DIA_TYPE_SSTABLE_MINOR, info_array_, idx_, max_cnt_);
}

void ObSSDiagnoseGetter::diagnose_ss_major_()
{
  MTL(ObSSDiagnoseInfoMgr *)->get_diagnose_info(ObSSDiagnoseInfo::DIA_TYPE_SSTABLE_MAJOR, info_array_, idx_, max_cnt_);
}

void ObSSDiagnoseGetter::diagnose_ss_checkpoint_()
{

  MTL(ObSSDiagnoseInfoMgr *)->get_diagnose_info(ObSSDiagnoseInfo::DIA_TYPE_SS_CKPT, info_array_, idx_, max_cnt_);
}

void ObSSDiagnoseGetter::diagnose_ss_notify_()
{}

void ObSSDiagnoseGetter::diagnose_ss_gc_()
{
  MTL(ObSSDiagnoseInfoMgr *)->get_diagnose_info(ObSSDiagnoseInfo::DIA_TYPE_SS_GC, info_array_, idx_, max_cnt_);
}
}//storage
}//oceanbase
