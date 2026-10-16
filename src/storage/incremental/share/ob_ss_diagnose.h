/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SRC_STORAGE_SS_OB_SS_DIAGNOSE_H_
#define SRC_STORAGE_SS_OB_SS_DIAGNOSE_H_

#include "lib/list/ob_dlist.h"
#include "lib/oblog/ob_log_print_kv.h"
#include "share/scheduler/ob_diagnose_config.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{
struct ObSSDiagnoseInfo
{
  ObSSDiagnoseInfo()
    : diagnose_type_(DIA_TYPE_INVALID),
      tenant_id_(0),
      ls_id_(0),
      tablet_id_(0),
      timestamp_(0)
  {}
  enum ObDiagnoseType
  {
    DIA_TYPE_INVALID = 0,
    DIA_TYPE_META_READ_WRITE = 1,
    DIA_TYPE_SS_GC = 2,
    DIA_TYPE_SSTABLE_UPLOAD = 3,
    DIA_TYPE_SSTABLE_MINOR = 4,
    DIA_TYPE_SSTABLE_MAJOR = 5,
    DIA_TYPE_SSTABLE_ATTACH = 6,
    DIA_TYPE_SSTABLE_GC = 7,
    DIA_TYPE_SCHE_UPLOAD = 8,
    DIA_TYPE_SS_CKPT = 9,
    DIA_TYPE_MAX
  };
  bool is_valid() const { return diagnose_type_ > DIA_TYPE_INVALID && diagnose_type_ < DIA_TYPE_MAX; }
  void copy(const ObSSDiagnoseInfo &src)
  {
    diagnose_type_ = src.diagnose_type_;
    tenant_id_ = src.tenant_id_;
    ls_id_ = src.ls_id_;
    tablet_id_ = src.tablet_id_;
    timestamp_ = src.timestamp_;
    MEMCPY(diagnose_info_, src.diagnose_info_, sizeof(diagnose_info_));
  }
  const static char *ObDiagnoseTypeStr[DIA_TYPE_MAX];
  static const char * ss_diagnose_type_to_str(const int64_t diagnose_type);
  TO_STRING_KV("diagnose_type", ss_diagnose_type_to_str(diagnose_type_), K_(tenant_id), K_(ls_id), K_(tablet_id),
               K_(timestamp), K_(diagnose_info));

  int64_t diagnose_type_;
  uint64_t tenant_id_;
  int64_t ls_id_;
  int64_t tablet_id_;
  int64_t timestamp_;
  char diagnose_info_[common::OB_DIAGNOSE_INFO_LENGTH];
};

class ObSSDiagnoseIterator
{
public:
  ObSSDiagnoseIterator()
   : allocator_("CompDiagnose"),
     info_array_(nullptr),
     cnt_(0),
     cur_idx_(0),
     is_opened_(false)
  {
  }
  virtual ~ObSSDiagnoseIterator() { reset(); }
  int open(const int64_t tenant_id);
  int get_next_info(ObSSDiagnoseInfo &info);
  void reset();

private:
  int get_diagnose_info(const int64_t tenant_id);
private:
  const int64_t MAX_DIAGNOSE_INFO_CNT = 1000;
  ObArenaAllocator allocator_;
  ObSSDiagnoseInfo *info_array_;
  int64_t cnt_;
  int64_t cur_idx_;
  bool is_opened_;
};

class ObSSDiagnoseGetter
{
public:
  ObSSDiagnoseGetter();
  ~ObSSDiagnoseGetter() { reset(); }
  void reset();
  int init(common::ObIAllocator *allocator, ObSSDiagnoseInfo *info_array, const int64_t max_cnt);
  int diagnose(const int64_t tenant_id);
  int64_t get_cnt() { return idx_; }
  OB_INLINE bool can_add_diagnose_info() { return idx_ < max_cnt_; }
private:
  void diagnose_ss_meta_service_();
  void diagnose_ss_upload_();
  void diagnose_ss_minor_();
  void diagnose_ss_major_();
  void diagnose_ss_checkpoint_();
  void diagnose_ss_notify_();
  void diagnose_ss_gc_();
private:
  bool is_inited_;
  ObSSDiagnoseInfo *info_array_;
  int64_t max_cnt_;
  int64_t idx_;
};

}//storage
}//oceanbase

#endif
