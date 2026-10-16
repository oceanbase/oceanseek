/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_LS_META_
#define OCEANBASE_STORAGE_OB_SHARED_LS_META_
#include "storage/ls/ob_ls_meta.h"
#include "logservice/palf/palf_base_info.h"

namespace oceanbase
{
namespace palf {
struct LSN;
}

namespace storage
{
class ObSSLSMeta;

class ObIMetaFunction
{
  OB_UNIS_VERSION_V(1);
public:
  enum MetaFuncType
  {
    FUNC_TYPE_INVALID   = 0,
    FUNC_TYPE_TEST      = 1,
    UPDATE_SS_CKPT_FUNC = 2,
    FUNC_TYPE_RESTORE = 3,    
    FUNC_TYPE_MAX,
  };
  ObIMetaFunction(MetaFuncType func_type)
    : type_(func_type) {}
  virtual ~ObIMetaFunction() {}
  virtual int operator()(ObSSLSMeta &ls_meta) = 0;
  virtual int64_t get_func_code() const
  { return type_; }
  static bool need_transmit_functor_buf(const int64_t func_code)
  {
    return FUNC_TYPE_RESTORE == func_code;
  }
  TO_STRING_KV(K_(type));
public:
  MetaFuncType type_;
};

class ObTestMetaFunction : public ObIMetaFunction
{
public:
  ObTestMetaFunction()
    : ObIMetaFunction(ObIMetaFunction::MetaFuncType::FUNC_TYPE_INVALID) {}
  ~ObTestMetaFunction() {}
  virtual int operator()(ObSSLSMeta &ls_meta);
};

class ObSSLSMeta : public ObLSMeta
{
  // for shared storage
  OB_UNIS_VERSION_V(1);
public:
  ObSSLSMeta() : ObLSMeta(), palf_meta_() {}
  ObSSLSMeta(
      const ObSSLSMeta &ls_meta)
    : ObLSMeta(ls_meta) 
  {
    (void)ls_meta.get_palf_meta(palf_meta_);
  }

  virtual ~ObSSLSMeta() {}

  // init shared ls meta with a local ls meta.
  int init_for_ss(const ObLSMeta &ls_meta);
  // update the shared ls meta in memory.
  // param: ls_meta, the meta want to persist
  //        updated, return if updated
  int update_for_ss(const ObSSLSMeta &ls_meta, bool &updated);
  int update_for_ss_gc_state(const logservice::LSGCState &gc_state,
                             const SCN &scn,
                             bool &updated);
  int update_for_ss_id_meta(const int64_t service_type,
                            const int64_t limited_id,
                            const SCN &latest_scn,
                            bool &updated);
  int update_for_ss_ls_meta(ObIMetaFunction &fn,
                            bool &updated);
  share::SCN get_ss_checkpoint_scn()
  {
    ObReentrantRLockGuard guard(rw_lock_);
    return ss_checkpoint_scn_;
  }
  palf::LSN get_ss_checkpoint_lsn()
  {
    ObReentrantRLockGuard guard(rw_lock_);
    return ss_checkpoint_lsn_;
  }
  int get_ss_checkpoint(share::SCN &ss_checkpoint_scn, palf::LSN &ss_checkpoint_lsn);
  int set_ss_checkpoint(const share::SCN &ss_checkpoint_scn, const palf::LSN &ss_checkpoint_lsn);
  share::SCN get_ss_checkpoint_scn() const
  { return ss_checkpoint_scn_; }
  palf::LSN get_ss_base_lsn() const
  { return ss_checkpoint_lsn_; }
  int set_palf_meta(const palf::PalfBaseInfo &palf_meta);
  int get_palf_meta(palf::PalfBaseInfo &palf_meta) const;
  share::SCN get_acquire_scn() const
  { return share::SCN::min_scn(); }
  int set_transfer_meta(const share::SCN &transfer_scn, const ObLSTransferMetaInfo &transfer_meta_info);
  int convert_local_ls_meta()
  {
    if (ss_checkpoint_scn_.is_min() || !ss_checkpoint_scn_.is_valid()) {
      tablet_change_checkpoint_scn_ = ss_checkpoint_scn_;
    } else {
      tablet_change_checkpoint_scn_ = share::SCN::scn_dec(ss_checkpoint_scn_);
    }
    return OB_SUCCESS;
  }

  INHERIT_TO_STRING_KV("ObLSMeta", ObLSMeta, K_(palf_meta));
private:
  int update_for_ss_(const ObSSLSMeta &ls_meta, bool &updated);

private:
  palf::PalfBaseInfo palf_meta_;
};

} // namespace storage
} // namespace oceanbase
#endif
