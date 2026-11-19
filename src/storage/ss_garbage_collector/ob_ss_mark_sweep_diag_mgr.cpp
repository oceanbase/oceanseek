/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ss_garbage_collector/ob_ss_mark_sweep_diag_mgr.h"
#include "lib/allocator/page_arena.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/serialization.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "observer/ob_server_struct.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/config/ob_server_config.h"
#ifdef ERRSIM
#include "storage/ss_garbage_collector/ob_ss_gc_util.h"
#endif
#include "share/ob_cluster_version.h"
#include "share/ob_debug_sync.h"
#include "share/io/ob_io_define.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/blocksstable/ob_object_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_ss_io_common_op.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "storage/ss_garbage_collector/ob_ss_gc_service.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_table_operator.h"
#include "storage/ss_garbage_collector/shared_gc_task/ob_shared_gc_task_util.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

// ====================================================================================
// ObSSMarkSweepDiagIndex
// ====================================================================================
ObSSMarkSweepDiagIndex::ObSSMarkSweepDiagIndex()
  : record_lens_()
{
  record_lens_.set_attr(ObMemAttr(MTL_ID(), "MSGcDiagIdx"));
}

OB_SERIALIZE_MEMBER(ObSSMarkSweepDiagIndex, record_lens_);

// ====================================================================================
// ObSSMarkSweepDiagTabletMacros / ObSSMarkSweepTabletMetaScns (decode output)
// ====================================================================================
ObSSMarkSweepDiagTabletMacros::ObSSMarkSweepDiagTabletMacros()
  : tablet_(), macro_ids_()
{
  macro_ids_.set_attr(ObMemAttr(MTL_ID(), "MSGcDiagMac"));
}

ObSSMarkSweepTabletMetaScns::ObSSMarkSweepTabletMetaScns()
  : tablet_(), meta_scns_()
{
  meta_scns_.set_attr(ObMemAttr(MTL_ID(), "MSGcDiagScn"));
}

ObSSMarkSweepDiagStudyResults::ObSSMarkSweepDiagStudyResults()
  : tablet_(), study_results_()
{
  study_results_.set_attr(ObMemAttr(MTL_ID(), "MSGcDiagStudy"));
}

ObSSLobCtrlGCDiagRows::ObSSLobCtrlGCDiagRows()
  : rows_()
{
  rows_.set_attr(ObMemAttr(MTL_ID(), "LobGcDiagCtrl"));
}

// ====================================================================================
// ObSSMarkSweepDiagTailer (fixed-length serialization)
// ====================================================================================
int ObSSMarkSweepDiagTailer::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY((buf_len - pos) < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(pos), "size", get_serialize_size());
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, magic_))) {
    LOG_WARN("fail to encode magic", KR(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, version_))) {
    LOG_WARN("fail to encode version", KR(ret));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, index_offset_))) {
    LOG_WARN("fail to encode index_offset", KR(ret));
  }
  return ret;
}

int ObSSMarkSweepDiagTailer::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len - pos) < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(data_len), K(pos), "size", get_serialize_size());
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &magic_))) {
    LOG_WARN("fail to decode magic", KR(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &version_))) {
    LOG_WARN("fail to decode version", KR(ret));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &index_offset_))) {
    LOG_WARN("fail to decode index_offset", KR(ret));
  }
  return ret;
}

int64_t ObSSMarkSweepDiagTailer::get_serialize_size() const
{
  // Fixed-width encoding of three i64. serialization::encode_i64 emits a fixed 8 bytes
  // (no varint), so this is a constant independent of the field values.
  return serialization::encoded_length_i64(0) * 3;
}

// ====================================================================================
// ObSSMarkSweepDiagMgr::ObSSMarkSweepDiagFile
// ====================================================================================
ObSSMarkSweepDiagMgr::ObSSMarkSweepDiagFile::ObSSMarkSweepDiagFile()
  : device_handle_(nullptr), fd_(), offset_(0), is_opened_(false), is_finished_(false), index_(),
    lock_(ObLatchIds::OB_MARK_SWEEP_LOCK)
{
}

void ObSSMarkSweepDiagMgr::ObSSMarkSweepDiagFile::reset()
{
  device_handle_ = nullptr;
  fd_.reset();
  offset_ = 0;
  is_opened_ = false;
  is_finished_ = false;
  index_.reset();
}

// ====================================================================================
// ObSSMarkSweepDiagMgr
// ====================================================================================
ObSSMarkSweepDiagMgr::ObSSMarkSweepDiagMgr()
  : is_inited_(false), is_diag_enabled_(false), is_diag_task_prepared_(false), has_diag_failed_(false), 
    first_fail_ret_(OB_SUCCESS), tenant_id_(OB_INVALID_TENANT_ID), cluster_id_(0), round_id_(0),
    bucket_cnt_(0), diag_file_cnt_(0), diag_file_(nullptr), allocator_()
{
}

ObSSMarkSweepDiagMgr::~ObSSMarkSweepDiagMgr()
{
  reset();
}

int ObSSMarkSweepDiagMgr::init(
    const uint64_t tenant_id,
    const int64_t round_id,
    const int64_t bucket_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObSSMarkSweepDiagMgr init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || (round_id <= 0) || (bucket_cnt <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(round_id), K(bucket_cnt));
  } else {
    const int64_t diag_file_cnt = GROUP_CNT * TYPE_CNT * bucket_cnt;
    allocator_.set_attr(ObMemAttr(tenant_id, "MSGcDiagMgr"));
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObSSMarkSweepDiagFile) * diag_file_cnt))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc diag files", KR(ret), K(diag_file_cnt));
    } else {
      // ObSpinLock is non-copyable, so each ObSSMarkSweepDiagFile must be constructed in place.
      diag_file_ = static_cast<ObSSMarkSweepDiagFile *>(buf);
      for (int64_t i = 0; i < diag_file_cnt; ++i) {
        new (&diag_file_[i]) ObSSMarkSweepDiagFile();
        if (is_study_results_file_(i, bucket_cnt)) {
          // Study indexes are charged to the target tenant, independent of the caller's tenant context.
          diag_file_[i].index_.set_attr(ObMemAttr(tenant_id, "MSGcDiagIdx"));
        }
      }
      tenant_id_ = tenant_id;
      cluster_id_ = GCONF.cluster_id;
      round_id_ = round_id;
      bucket_cnt_ = bucket_cnt;
      diag_file_cnt_ = diag_file_cnt;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::prepare_diag_task(
    const uint64_t tenant_id,
    const int64_t round_id)
{
  int ret = OB_SUCCESS;
  const uint64_t cluster_version = GET_MIN_CLUSTER_VERSION();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || round_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid diagnose round", KR(ret), K(tenant_id), K(round_id));
  } else if (cluster_version < CLUSTER_VERSION_4_6_3_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cluster version does not support diagnose tasks", KR(ret), K(cluster_version));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskUtil::generate_mark_sweep_gc_diag_task(
      tenant_id, round_id, *GCTX.sql_proxy_))) {
    // Use the auto-commit SQL proxy; start diagnose upload only after the ledger commit succeeds.
    LOG_WARN("fail to register diagnose task", KR(ret), K(tenant_id), K(round_id));
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::check_diag_task()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTask"));
  ObSharedGCTask task;
  const ObSharedGCTaskKey key(tenant_id_, ObSharedGCTaskType::MARK_SWEEP_GC_DIAG,
      round_id_, 0/*task_id*/);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("diagnose manager not inited", KR(ret));
  } else if (is_diag_task_prepared_) {
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_6_3_0) {
    // Legacy DEL/KEPT diagnostics do not require a round marker during rolling upgrade.
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else if (OB_FAIL(ObSharedGCTaskTableOperator::get_task(
      key, allocator, *GCTX.sql_proxy_, task))) {
    LOG_WARN("fail to read diagnose task", KR(ret), K(key));
  } else {
    is_diag_task_prepared_ = true;
  }
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret);
  }
  return ret;
}

void ObSSMarkSweepDiagMgr::reset()
{
  // Release any still-open diag files to avoid leaking the multipart upload / device fd.
  if (OB_NOT_NULL(diag_file_)) {
    abort();
    for (int64_t i = 0; i < diag_file_cnt_; ++i) {
      diag_file_[i].~ObSSMarkSweepDiagFile();
    }
    diag_file_ = nullptr;
  }
  allocator_.reset();
  is_inited_ = false;
  is_diag_enabled_ = false;
  is_diag_task_prepared_ = false;
  has_diag_failed_ = false;
  first_fail_ret_ = OB_SUCCESS;
  tenant_id_ = OB_INVALID_TENANT_ID;
  cluster_id_ = 0;
  round_id_ = 0;
  bucket_cnt_ = 0;
  diag_file_cnt_ = 0;
}

bool ObSSMarkSweepDiagMgr::is_study_results_file_(
    const int64_t file_idx,
    const int64_t bucket_cnt)
{
  return file_idx >= 0 && bucket_cnt > 0
      && static_cast<int64_t>(ObMarkSweepGroupType::KEPT)
         == file_idx / bucket_cnt / TYPE_CNT
      && static_cast<int64_t>(ObSSMarkSweepDiagFileType::STUDY_RESULTS)
         == (file_idx / bucket_cnt) % TYPE_CNT;
}

int64_t ObSSMarkSweepDiagMgr::diag_file_idx_(
    const ObMarkSweepGroupType group,
    const ObSSMarkSweepDiagFileType type,
    const int64_t bucket) const
{
  return ((static_cast<int64_t>(group) * TYPE_CNT) + static_cast<int64_t>(type))
         * bucket_cnt_
         + bucket;
}

int64_t ObSSMarkSweepDiagMgr::calc_bucket_(const ObSSGCTablet &tablet) const
{
  return static_cast<int64_t>(tablet.hash() % static_cast<uint64_t>(bucket_cnt_));
}

int64_t ObSSMarkSweepDiagMgr::calc_lob_bucket_(const MacroBlockId &macro_id) const
{
  return static_cast<int64_t>(macro_id.hash() % static_cast<uint64_t>(bucket_cnt_));
}

bool ObSSMarkSweepDiagMgr::is_lob_ctrl_type(const ObSSMarkSweepDiagFileType type)
{
  return ObSSMarkSweepDiagFileType::LOB_CTRL_CANDIDATE == type
      || ObSSMarkSweepDiagFileType::LOB_CTRL_DIFF == type;
}

void ObSSMarkSweepDiagMgr::mark_diag_failed(const int ret_code)
{
  // Sticky: set the flag and keep only the FIRST failure's ret code. Called from
  // multiple SortTask threads, so set the ret code via CAS (OB_SUCCESS -> ret_code).
  ATOMIC_STORE(&has_diag_failed_, true);
  ATOMIC_VCAS(&first_fail_ret_, OB_SUCCESS, ret_code);
}

int ObSSMarkSweepDiagMgr::get_storage_dest_and_id(
    ObBackupDest &storage_dest,
    uint64_t &storage_id)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObDeviceConfig, device_config) {
    if (OB_FAIL(ObDeviceConfigMgr::get_instance().get_device_config(
                ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config))) {
      LOG_WARN("fail to get device config", KR(ret));
    } else if (OB_UNLIKELY(!device_config.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid device config", KR(ret), K(device_config));
    } else if (OB_FAIL(storage_dest.set(device_config.path_, device_config.endpoint_,
                       device_config.access_info_, device_config.extension_))) {
      LOG_WARN("fail to set storage dest", KR(ret), K(device_config));
    } else if (FALSE_IT(storage_id = device_config.storage_id_)) {
    } else if (OB_UNLIKELY(!storage_dest.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage dest", KR(ret), K(storage_dest));
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::build_diag_root_dir(
    const uint64_t tenant_id,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  char *root_dir = nullptr;
  const int64_t cluster_id = GCONF.cluster_id;
  if (OB_ISNULL(buf) || OB_UNLIKELY((buf_len <= 0) || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(tenant_id));
  } else if (OB_FAIL(OB_DIR_MGR.get_object_storage_root_dir(root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, "%s/%s_%ld/%s_%lu/%s",
                                     root_dir,
                                     CLUSTER_DIR_STR, cluster_id,
                                     TENANT_DIR_STR, tenant_id,
                                     MARK_SWEEP_GC_DIAG_DIR_STR))) {
    LOG_WARN("fail to databuff printf diag root dir", KR(ret), K(cluster_id), K(tenant_id));
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::get_diag_macro_id(
    const int64_t round_id,
    const ObMarkSweepGroupType group,
    const ObSSMarkSweepDiagFileType type,
    const int64_t bucket,
    MacroBlockId &macro_id)
{
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  opt.set_shared_mark_sweep_gc_diag_opt(round_id, static_cast<int64_t>(group),
                                        static_cast<int64_t>(type), bucket);
  if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, macro_id))) {
    LOG_WARN("fail to get object id", KR(ret), K(opt), K(macro_id));
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::ensure_open_(
    const ObMarkSweepGroupType group,
    const ObSSMarkSweepDiagFileType type,
    const int64_t bucket,
    ObSSMarkSweepDiagFile &diag_file)
{
  int ret = OB_SUCCESS;
  if (diag_file.is_opened_) {
    // already opened
  } else {
    ObSSIOCommonOp ss_io_common_op;
    ObIOInfo io_info;
    io_info.tenant_id_ = tenant_id_;
    MacroBlockId macro_id;
    if (OB_FAIL(get_diag_macro_id(round_id_, group, type, bucket, macro_id))) {
      LOG_WARN("fail to get diag macro id", KR(ret), K(round_id_), K(bucket), K(group), K(type));
    } else if (OB_FAIL(ss_io_common_op.get_object_device_and_fd(
                       OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER,
                       macro_id, 0 /*ls_epoch_id*/, io_info))) {
      LOG_WARN("fail to get object device and fd", KR(ret), K(macro_id));
    } else {
      diag_file.device_handle_ = io_info.fd_.device_handle_;
      diag_file.fd_ = io_info.fd_;
      diag_file.offset_ = 0;
      diag_file.is_opened_ = true;
      diag_file.index_.reset();
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::append_record_(
    ObSSMarkSweepDiagFile &diag_file,
    const char *buf,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  ObIOHandle io_handle;
  if (OB_UNLIKELY(!diag_file.is_opened_) || OB_ISNULL(diag_file.device_handle_) ||
      OB_ISNULL(buf) || OB_UNLIKELY(len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(diag_file), KP(buf), K(len));
  } else if (OB_FAIL(io_adapter.async_upload_data(*diag_file.device_handle_, diag_file.fd_,
                     buf, diag_file.offset_, len, io_handle))) {
    LOG_WARN("fail to async upload data", KR(ret), K(diag_file), K(len));
  } else if (OB_FAIL(io_handle.wait())) {
    LOG_WARN("fail to wait upload data", KR(ret), K(diag_file), K(len));
  } else if (OB_FAIL(diag_file.index_.add_record_len(len))) {
    LOG_WARN("fail to add record len", KR(ret), K(len));
  } else {
    diag_file.offset_ += len;
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_record_(
    const ObMarkSweepGroupType group,
    const ObSSMarkSweepDiagFileType type,
    const ObSSGCTablet &tablet,
    const char *buf,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  const int64_t bucket = calc_bucket_(tablet);
  const int64_t idx = diag_file_idx_(group, type, bucket);
  if (OB_UNLIKELY((idx < 0) || (idx >= diag_file_cnt_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid diag file idx", KR(ret), K(idx), K(diag_file_cnt_), K(group), K(type), K(bucket));
  } else {
    ObSSMarkSweepDiagFile &diag_file = diag_file_[idx];
    ObSpinLockGuard guard(diag_file.lock_);
    if (OB_FAIL(ensure_open_(group, type, bucket, diag_file))) {
      LOG_WARN("fail to ensure open diag file", KR(ret), K(group), K(type), K(bucket));
    } else if (OB_FAIL(append_record_(diag_file, buf, len))) {
      LOG_WARN("fail to append record", KR(ret), K(group), K(type), K(bucket), K(tablet));
      abort_(diag_file);
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_record_(
    const ObSSMarkSweepDiagFileType type,
    const int64_t bucket,
    const char *buf,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  const ObMarkSweepGroupType group = ObMarkSweepGroupType::DEL;
  const int64_t idx = diag_file_idx_(group, type, bucket);
  if (OB_UNLIKELY(type < ObSSMarkSweepDiagFileType::LOB_META_KEPT
      || type >= ObSSMarkSweepDiagFileType::MAX
      || bucket < 0 || bucket >= bucket_cnt_
      || idx < 0 || idx >= diag_file_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob gc diag record", KR(ret), K(type), K(bucket), K(idx), K(diag_file_cnt_));
  } else {
    ObSSMarkSweepDiagFile &diag_file = diag_file_[idx];
    ObSpinLockGuard guard(diag_file.lock_);
    if (OB_FAIL(ensure_open_(group, type, bucket, diag_file))) {
      LOG_WARN("failed to open lob gc diag file", KR(ret), K(type), K(bucket));
    } else if (OB_FAIL(append_record_(diag_file, buf, len))) {
      LOG_WARN("failed to append lob gc diag record", KR(ret), K(type), K(bucket));
      abort_(diag_file);
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_tablet_macros(
    const ObMarkSweepGroupType group,
    const ObSSGCTablet &tablet,
    const ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet", KR(ret), K(tablet));
  } else if (macro_ids.empty()) {
    // do nothing
  } else {
    const int64_t macro_cnt = macro_ids.count();
    // payload = ObSSGCTablet + macro_cnt(i64) + MacroBlockId[macro_cnt]
    int64_t buf_len = tablet.get_serialize_size();
    buf_len += serialization::encoded_length_i64(macro_cnt);
    for (int64_t i = 0; i < macro_cnt; ++i) {
      buf_len += macro_ids.at(i).get_serialize_size();
    }
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagMac"));
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc record buffer", KR(ret), K(buf_len));
    } else if (OB_FAIL(tablet.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize tablet", KR(ret), K(tablet));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, macro_cnt))) {
      LOG_WARN("fail to encode macro_cnt", KR(ret), K(macro_cnt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < macro_cnt); ++i) {
        if (OB_FAIL(macro_ids.at(i).serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize macro id", KR(ret), K(i), K(macro_ids.at(i)));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(write_record_(group, ObSSMarkSweepDiagFileType::MACROS, tablet, buf, pos))) {
      LOG_WARN("fail to write macros record", KR(ret), K(group), K(tablet));
    }
  }
  // Mark sticky diag failure on ANY failure path above (alloc / serialize / write_record_).
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret);
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_tablet_meta_scns(
    const ObMarkSweepGroupType group,
    const ObSSGCTablet &tablet,
    const ObIArray<SCN> &meta_scns)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet", KR(ret), K(tablet));
  } else if (meta_scns.empty()) {
    // do nothing
  } else {
    const int64_t meta_scn_cnt = meta_scns.count();
    // payload = ObSSGCTablet + meta_scn_cnt(i64) + SCN[meta_scn_cnt]
    int64_t buf_len = tablet.get_serialize_size();
    buf_len += serialization::encoded_length_i64(meta_scn_cnt);
    for (int64_t i = 0; i < meta_scn_cnt; ++i) {
      buf_len += meta_scns.at(i).get_serialize_size();
    }
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagMeta"));
    char *buf = nullptr;
    int64_t pos = 0;
    if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc record buffer", KR(ret), K(buf_len));
    } else if (OB_FAIL(tablet.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize tablet", KR(ret), K(tablet));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, meta_scn_cnt))) {
      LOG_WARN("fail to encode meta_scn_cnt", KR(ret), K(meta_scn_cnt));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && (i < meta_scn_cnt); ++i) {
        if (OB_FAIL(meta_scns.at(i).serialize(buf, buf_len, pos))) {
          LOG_WARN("fail to serialize scn", KR(ret), K(i), K(meta_scns.at(i)));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(write_record_(group, ObSSMarkSweepDiagFileType::TABLET_META_SCNS,
                                     tablet, buf, pos))) {
      LOG_WARN("fail to write tablet_meta_scns record", KR(ret), K(group), K(tablet));
    }
  }
  // Mark sticky diag failure on ANY failure path above (alloc / serialize / write_record_).
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret);
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_lob_meta_kept_record_(
    const ObSSGCTablet &tablet,
    const ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  const int64_t item_count = macro_ids.count();
  int64_t buf_len = tablet.get_serialize_size() + serialization::encoded_length_i64(item_count);
  for (int64_t i = 0; i < item_count; ++i) {
    buf_len += macro_ids.at(i).get_serialize_size();
  }
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "LobGcDiagMeta"));
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_UNLIKELY(item_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob meta gc diag rows", KR(ret), K(item_count));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate lob meta gc diag record", KR(ret), K(buf_len));
  } else if (OB_FAIL(tablet.serialize(buf, buf_len, pos))) {
    LOG_WARN("failed to serialize lob meta gc diag tablet", KR(ret), K(tablet));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, item_count))) {
    LOG_WARN("failed to encode lob meta gc diag row count", KR(ret), K(item_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < item_count; ++i) {
      if (OB_FAIL(macro_ids.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize lob meta gc diag row", KR(ret), K(i), K(macro_ids.at(i)));
      }
    }
    if (FAILEDx(write_record_(
        ObSSMarkSweepDiagFileType::LOB_META_KEPT, calc_bucket_(tablet), buf, pos))) {
      LOG_WARN("failed to write lob meta gc diag record", KR(ret), K(tablet), K(item_count));
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_lob_ctrl_record_(
    const ObSSMarkSweepDiagFileType type,
    const int64_t bucket,
    const ObIArray<ObSSOuttableLobCtrlCandidate> &rows)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = rows.count();
  int64_t buf_len = serialization::encoded_length_i64(row_count);
  for (int64_t i = 0; i < row_count; ++i) {
    buf_len += rows.at(i).get_serialize_size();
  }
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "LobGcDiagCtrl"));
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_UNLIKELY(!is_lob_ctrl_type(type) || row_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl gc diag rows", KR(ret), K(type), K(row_count));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate lob ctrl gc diag record", KR(ret), K(buf_len));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, row_count))) {
    LOG_WARN("failed to encode lob ctrl gc diag row count", KR(ret), K(row_count));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      if (OB_FAIL(rows.at(i).serialize(buf, buf_len, pos))) {
        LOG_WARN("failed to serialize lob ctrl gc diag row", KR(ret), K(i), K(rows.at(i)));
      }
    }
    if (FAILEDx(write_record_(type, bucket, buf, pos))) {
      LOG_WARN("failed to write lob ctrl gc diag record", KR(ret), K(type), K(bucket), K(row_count));
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_lob_meta_kept(
    const ObSSGCTablet &tablet,
    const ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob meta gc diag tablet", KR(ret), K(tablet));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_ids.count(); ++i) {
      if (OB_UNLIKELY(!macro_ids.at(i).is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lob meta gc diag macro id", KR(ret), K(i), K(macro_ids.at(i)));
      }
    }
    if (!macro_ids.empty()) {
      if (FAILEDx(write_lob_meta_kept_record_(tablet, macro_ids))) {
        LOG_WARN("failed to write lob meta gc diag rows", KR(ret), K(tablet));
      }
    }
  }
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret);
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_lob_ctrl_rows(
    const ObSSMarkSweepDiagFileType type,
    const ObIArray<ObSSOuttableLobCtrlCandidate> &rows)
{
  int ret = OB_SUCCESS;
  typedef ObSEArray<ObSSOuttableLobCtrlCandidate, 16> ObLobCtrlDiagBucket;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "LobGcDiagCtrl"));
  ObLobCtrlDiagBucket **buckets = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_lob_ctrl_type(type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl gc diag type", KR(ret), K(type));
  } else if (rows.empty()) {
  } else if (OB_ISNULL(buckets = static_cast<ObLobCtrlDiagBucket **>(
      allocator.alloc(sizeof(ObLobCtrlDiagBucket *) * bucket_cnt_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate lob ctrl gc diag buckets", KR(ret), K_(bucket_cnt));
  } else {
    MEMSET(buckets, 0, sizeof(ObLobCtrlDiagBucket *) * bucket_cnt_);
    for (int64_t i = 0; OB_SUCC(ret) && i < rows.count(); ++i) {
      const ObSSOuttableLobCtrlCandidate &row = rows.at(i);
      if (OB_UNLIKELY(!row.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid lob ctrl gc diag row", KR(ret), K(i), K(row));
      } else {
        const int64_t bucket = calc_lob_bucket_(row.macro_id_);
        if (OB_ISNULL(buckets[bucket])) {
          void *buf = allocator.alloc(sizeof(ObLobCtrlDiagBucket));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate lob ctrl gc diag bucket", KR(ret), K(bucket));
          } else {
            buckets[bucket] = new (buf) ObLobCtrlDiagBucket();
            buckets[bucket]->set_attr(ObMemAttr(tenant_id_, "LobGcDiagCtrl"));
          }
        }
        if (FAILEDx(buckets[bucket]->push_back(row))) {
          LOG_WARN("failed to append lob ctrl gc diag row", KR(ret), K(bucket), K(row));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_cnt_; ++i) {
      if (OB_NOT_NULL(buckets[i]) && OB_FAIL(write_lob_ctrl_record_(type, i, *buckets[i]))) {
        LOG_WARN("failed to write lob ctrl gc diag bucket", KR(ret), K(type), K(i));
      }
    }
  }
  if (OB_NOT_NULL(buckets)) {
    for (int64_t i = 0; i < bucket_cnt_; ++i) {
      if (OB_NOT_NULL(buckets[i])) {
        buckets[i]->~ObLobCtrlDiagBucket();
      }
    }
  }
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret);
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_lob_ctrl_file(
    const ObSSMarkSweepDiagFileType type,
    const ObSSLobCtrlGCScanFile &file,
    const int64_t file_buf_size)
{
  int ret = OB_SUCCESS;
  const int64_t row_batch_size = 1024;
  int64_t read_count = 0;
  ObFragmentReaderV2<ObSSOuttableLobCtrlCandidate> reader(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObArray<ObSSOuttableLobCtrlCandidate> rows;
  rows.set_attr(ObMemAttr(tenant_id_, "LobGcDiagCtrl"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(!is_lob_ctrl_type(type) || !file.is_valid() || file_buf_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob ctrl gc diag file", KR(ret), K(type), K(file), K(file_buf_size));
  } else if (OB_FAIL(reader.init_for_pread(file.get_fd(), file.get_dir_id(),
      0 /*expire_timestamp*/, tenant_id_, file.sample_item_, file_buf_size))) {
    LOG_WARN("failed to init lob ctrl gc diag reader", KR(ret), K(type), K(file));
  } else if (OB_FAIL(reader.open())) {
    LOG_WARN("failed to open lob ctrl gc diag reader", KR(ret), K(type), K(file));
  } else {
    while (OB_SUCC(ret)) {
      const ObSSOuttableLobCtrlCandidate *row = nullptr;
      if (OB_ITER_END == (ret = reader.get_next_item(row))) {
        ret = OB_SUCCESS;
        if (OB_UNLIKELY(read_count != file.item_count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("lob ctrl gc diag row count mismatch", KR(ret), K(type),
              K(read_count), K(file.item_count_));
        } else if (!rows.empty() && OB_FAIL(write_lob_ctrl_rows(type, rows))) {
          LOG_WARN("failed to write last lob ctrl gc diag batch", KR(ret), K(type), K(read_count));
        }
        break;
      } else if (OB_FAIL(ret)) {
        LOG_WARN("failed to read lob ctrl gc diag row", KR(ret), K(type), K(read_count));
      } else if (OB_ISNULL(row) || OB_UNLIKELY(!row->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob ctrl gc diag row", KR(ret), K(type), K(read_count), KPC(row));
      } else if (OB_UNLIKELY(read_count >= file.item_count_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected extra lob ctrl gc diag row", KR(ret), K(type),
            K(read_count), K(file.item_count_));
      } else if (OB_FAIL(rows.push_back(*row))) {
        LOG_WARN("failed to retain lob ctrl gc diag row", KR(ret), K(type), K(read_count));
      } else {
        ++read_count;
        if (rows.count() == row_batch_size) {
          if (OB_FAIL(write_lob_ctrl_rows(type, rows))) {
            LOG_WARN("failed to write lob ctrl gc diag batch", KR(ret), K(type), K(read_count));
          } else {
            rows.reuse();
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret);
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::append_records_(
    ObSSMarkSweepDiagFile &diag_file,
    const char *buf,
    const int64_t len,
    const ObIArray<int64_t> &record_lens)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  ObIOHandle io_handle;
  if (OB_UNLIKELY(!diag_file.is_opened_) || OB_ISNULL(diag_file.device_handle_)
      || OB_ISNULL(buf) || OB_UNLIKELY(len <= 0 || record_lens.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch argument", KR(ret), K(diag_file), KP(buf), K(len));
  } else if (OB_UNLIKELY(diag_file.offset_ > INT64_MAX - len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("study data length overflow", KR(ret), K(diag_file), K(len));
  } else if (OB_FAIL(io_adapter.async_upload_data(*diag_file.device_handle_, diag_file.fd_,
      buf, diag_file.offset_, len, io_handle))) {
    LOG_WARN("fail to async upload study records", KR(ret), K(diag_file), K(len));
  } else if (OB_FAIL(io_handle.wait())) {
    LOG_WARN("fail to wait upload study records", KR(ret), K(diag_file), K(len));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < record_lens.count(); ++i) {
      if (OB_FAIL(diag_file.index_.add_record_len(record_lens.at(i)))) {
        LOG_WARN("fail to add study record length", KR(ret), K(i), K(record_lens.at(i)));
      }
    }
    if (OB_SUCC(ret)) {
      diag_file.offset_ += len;
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_tablets_study_results(
    const int64_t bucket,
    const char *buf,
    const int64_t len,
    const ObIArray<int64_t> &record_lens)
{
  int ret = OB_SUCCESS;
  int64_t total_len = 0;
  if (!is_ready_record()) {
    // When disabled or not yet inited, create no object and record no diagnose error.
  } else if (OB_UNLIKELY(0 != bucket || bucket >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid study bucket", KR(ret), K(bucket), K_(bucket_cnt));
  } else {
    const int64_t idx = diag_file_idx_(ObMarkSweepGroupType::KEPT,
        ObSSMarkSweepDiagFileType::STUDY_RESULTS, bucket);
    ObSSMarkSweepDiagFile &diag_file = diag_file_[idx];
    ObSpinLockGuard guard(diag_file.lock_);
    if (OB_UNLIKELY(diag_file.is_finished_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("study bucket is finished", KR(ret), K(bucket));
    } else if (OB_ISNULL(buf) || OB_UNLIKELY(len <= 0 || record_lens.empty())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid study batch", KR(ret), K(bucket), KP(buf), K(len));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < record_lens.count(); ++i) {
        const int64_t record_len = record_lens.at(i);
        if (OB_UNLIKELY(record_len <= 0 || record_len > len - total_len)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid study record length", KR(ret), K(i), K(record_len), K(total_len));
        } else {
          total_len += record_len;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(total_len != len)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("study batch length mismatch", KR(ret), K(total_len), K(len));
      } else if (OB_FAIL(ensure_open_(ObMarkSweepGroupType::KEPT,
          ObSSMarkSweepDiagFileType::STUDY_RESULTS, bucket, diag_file))) {
        LOG_WARN("fail to open study bucket", KR(ret), K(bucket));
      } else if (OB_FAIL(append_records_(diag_file, buf, len, record_lens))) {
        LOG_WARN("fail to append study bucket", KR(ret), K(bucket), K(len));
      }
    }
    if (OB_FAIL(ret)) {
      // Data and index must both be valid; after failure the current bucket must not be reopened.
      abort_(diag_file);
      diag_file.is_finished_ = true;
    }
  }
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret);
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::complete_study_results(const int64_t bucket)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (OB_UNLIKELY(0 != bucket || bucket >= bucket_cnt_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid study bucket", KR(ret), K(bucket), K_(bucket_cnt));
  } else {
    const int64_t idx = diag_file_idx_(ObMarkSweepGroupType::KEPT,
        ObSSMarkSweepDiagFileType::STUDY_RESULTS, bucket);
    ObSSMarkSweepDiagFile &diag_file = diag_file_[idx];
    ObSpinLockGuard guard(diag_file.lock_);
    if (OB_UNLIKELY(diag_file.is_finished_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_WARN("study bucket is finished", KR(ret), K(bucket));
    } else if (diag_file.is_opened_ && OB_FAIL(complete_(diag_file))) {
      LOG_WARN("fail to complete study bucket", KR(ret), K(bucket));
      abort_(diag_file);
    }
    // Release only the current bucket's resources; do not clear the manager's first diagnose failure.
    diag_file.index_.reset();
    diag_file.is_finished_ = true;
  }
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret);
  }
  return ret;
}

// Serialize and append Index + Tailer, then complete the multipart upload.
int ObSSMarkSweepDiagMgr::complete_(ObSSMarkSweepDiagFile &diag_file)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "MSGcDiagTail"));
  const int64_t index_offset = diag_file.offset_;
  const int64_t index_size = diag_file.index_.get_serialize_size();
  ObSSMarkSweepDiagTailer tailer(index_offset);
  const int64_t tailer_size = tailer.get_serialize_size();
  const int64_t buf_len = index_size + tailer_size;
  char *buf = nullptr;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc index+tailer buffer", KR(ret), K(buf_len));
  } else if (OB_FAIL(diag_file.index_.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize index", KR(ret), K(diag_file));
  } else if (OB_UNLIKELY(pos != index_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index serialize size mismatch", KR(ret), K(pos), K(index_size));
  } else if (OB_FAIL(tailer.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize tailer", KR(ret), K(tailer));
  } else {
    // Append Index + Tailer as one trailing part, then complete the multipart upload.
    ObBackupIoAdapter io_adapter;
    ObIOHandle io_handle;
    if (OB_FAIL(io_adapter.async_upload_data(*diag_file.device_handle_, diag_file.fd_,
                buf, diag_file.offset_, buf_len, io_handle))) {
      LOG_WARN("fail to async upload index+tailer", KR(ret), K(diag_file));
    } else if (OB_FAIL(io_handle.wait())) {
      LOG_WARN("fail to wait upload index+tailer", KR(ret), K(diag_file));
    } else if (OB_FAIL(io_adapter.complete(*diag_file.device_handle_, diag_file.fd_))) {
      LOG_WARN("fail to complete multipart upload", KR(ret), K(diag_file));
    } else {
      diag_file.offset_ += buf_len;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(io_adapter.close_device_and_fd(diag_file.device_handle_, diag_file.fd_))) {
        LOG_WARN("fail to close device and fd", KR(tmp_ret), K(diag_file));
      }
      diag_file.device_handle_ = nullptr;
      diag_file.fd_.reset();
      diag_file.is_opened_ = false;
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::complete()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else {
    // Finish every opened diag file. A single failure is recorded but does not
    // stop the others (best-effort diagnostic side-channel).
    for (int64_t i = 0; i < diag_file_cnt_; ++i) {
      ObSSMarkSweepDiagFile &diag_file = diag_file_[i];
      ObSpinLockGuard guard(diag_file.lock_);
      if (!diag_file.is_opened_) {
        // nothing written for this (group, type, bucket), skip.
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(complete_(diag_file))) {
          LOG_WARN("fail to write index+tailer and complete", KR(tmp_ret), K(i), K(diag_file));
          abort_(diag_file);
          mark_diag_failed(tmp_ret);
          ret = (OB_SUCC(ret) ? tmp_ret : ret);
        }
      }
      if (is_study_results_file_(i, bucket_cnt_)) {
        diag_file.index_.reset();
        diag_file.is_finished_ = true;
      }
    }
  }
  return ret;
}

void ObSSMarkSweepDiagMgr::abort_(ObSSMarkSweepDiagFile &diag_file)
{
  if (diag_file.is_opened_ && OB_NOT_NULL(diag_file.device_handle_)) {
    int tmp_ret = OB_SUCCESS;
    ObBackupIoAdapter io_adapter;
    if (OB_TMP_FAIL(io_adapter.abort(*diag_file.device_handle_, diag_file.fd_))) {
      LOG_WARN_RET(tmp_ret, "fail to abort multipart upload", K(diag_file));
    }
    if (OB_TMP_FAIL(io_adapter.close_device_and_fd(diag_file.device_handle_, diag_file.fd_))) {
      LOG_WARN_RET(tmp_ret, "fail to close device and fd", K(diag_file));
    }
  }
  diag_file.reset();
}

void ObSSMarkSweepDiagMgr::abort()
{
  if (OB_NOT_NULL(diag_file_)) {
    for (int64_t i = 0; i < diag_file_cnt_; ++i) {
      ObSSMarkSweepDiagFile &diag_file = diag_file_[i];
      ObSpinLockGuard guard(diag_file.lock_);
      abort_(diag_file);
      if (is_study_results_file_(i, bucket_cnt_)) {
        diag_file.is_finished_ = true;
      }
    }
  }
}

struct ObSSMarkSweepDiagMgr::StudyDiagBuffer
{
  explicit StudyDiagBuffer(const uint64_t tenant_id)
    : buf_(nullptr), capacity_(0), len_(0), nodes_(), record_lens_()
  {
    nodes_.set_attr(ObMemAttr(tenant_id, "SSGCStudyNode"));
    record_lens_.set_attr(ObMemAttr(tenant_id, "SSGCStudyIndex"));
  }
  ~StudyDiagBuffer() { reset(); }
  void reset()
  {
    if (OB_NOT_NULL(buf_)) {
      ob_free(buf_);
      buf_ = nullptr;
    }
    capacity_ = 0;
    len_ = 0;
    nodes_.reset();
    record_lens_.reset();
  }
  char *buf_;
  int64_t capacity_;
  int64_t len_;
  ObArray<ObSSMarkSweepDiagStudyNode> nodes_;
  ObArray<int64_t> record_lens_;
private:
  DISALLOW_COPY_AND_ASSIGN(StudyDiagBuffer);
};

ObSSMarkSweepDiagMgr::StudyDiagStat::StudyDiagStat()
  : first_fail_ret_(OB_SUCCESS), first_fail_stage_("none"), input_result_count_(-1),
    input_tablet_count_(-1), append_record_count_(0), publish_cost_us_(0)
{
}

void ObSSMarkSweepDiagMgr::StudyDiagStat::record_failure(
    const int ret_code,
    const char *stage)
{
  if (OB_SUCCESS != ret_code && OB_SUCCESS == first_fail_ret_) {
    first_fail_ret_ = ret_code;
    first_fail_stage_ = stage;
  }
}

int ObSSMarkSweepDiagMgr::validate_study_diag_(
    const ObSSGCStudyDiagInfo &diag_info,
    StudyDiagStat &stat)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSSGCStudyDiagServerResult *> &servers = diag_info.get_study_results();
  int64_t result_count = 0;
  int64_t tablet_count = 0;
  for (int64_t begin = 0; OB_SUCC(ret) && begin < servers.count(); ) {
    int64_t end = begin + 1;
    if (OB_ISNULL(servers.at(begin))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("null study diagnose server result", KR(ret), K(begin));
    } else {
      const ObSSGCStudyDiagServerResult &first = *servers.at(begin);
      while (end < servers.count() && OB_NOT_NULL(servers.at(end))
          && servers.at(end)->ls_id_ == first.ls_id_) {
        ++end;
      }
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        if (OB_UNLIKELY(servers.at(i)->tablet_results_.count() != first.tablet_results_.count()
            || result_count > INT64_MAX - servers.at(i)->tablet_results_.count())) {
          ret = OB_INVALID_DATA;
          LOG_WARN("inconsistent study diagnose node arrays", KR(ret), K(i), K(begin), K(end));
        } else {
          result_count += servers.at(i)->tablet_results_.count();
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < first.tablet_results_.count(); ++i) {
        const ObSSGCTablet &tablet = first.tablet_results_.at(i).gc_tablet_;
        for (int64_t j = begin + 1; OB_SUCC(ret) && j < end; ++j) {
          if (OB_UNLIKELY(!(tablet == servers.at(j)->tablet_results_.at(i).gc_tablet_))) {
            ret = OB_INVALID_DATA;
            LOG_WARN("study diagnose tablet identity does not match",
                KR(ret), K(i), K(j), K(tablet));
          }
        }
      }
      if (OB_SUCC(ret)) {
        tablet_count += first.tablet_results_.count();
      }
    }
    begin = end;
  }
  if (OB_SUCC(ret)) {
    stat.input_result_count_ = result_count;
    stat.input_tablet_count_ = tablet_count;
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::serialize_study_diag_record_(
    const ObSSGCStudyDiagInfo &diag_info,
    const StudyDiagTabletIndex &index,
    StudyDiagBuffer &buffer,
    StudyDiagStat &stat)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t record_len = 0;
  const ObIArray<ObSSGCStudyDiagServerResult *> &servers = diag_info.get_study_results();
  ObSSMarkSweepDiagStudyNode node;
  buffer.nodes_.reuse();
  if (OB_UNLIKELY(index.server_idx_ < 0 || index.server_idx_ >= servers.count()
      || index.tablet_idx_ < 0) || OB_ISNULL(servers.at(index.server_idx_))) {
    tmp_ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid study diagnose tablet index", KR(tmp_ret), K(index), K(servers.count()));
  } else {
    const ObSSGCStudyDiagServerResult &first = *servers.at(index.server_idx_);
    for (int64_t i = index.server_idx_; OB_SUCC(tmp_ret) && i < servers.count(); ++i) {
      if (OB_ISNULL(servers.at(i))) {
        tmp_ret = OB_INVALID_DATA;
        LOG_WARN("null study diagnose server result", KR(tmp_ret), K(i));
      } else if (servers.at(i)->ls_id_ != first.ls_id_) {
        // All nodes of the current LS are done; later LSes belong to the next tablet.
        break;
      } else if (OB_UNLIKELY(index.tablet_idx_ >= servers.at(i)->tablet_results_.count())) {
        tmp_ret = OB_INVALID_DATA;
        LOG_WARN("invalid study diagnose tablet position", KR(tmp_ret), K(index), K(i));
      } else {
        node.server_ = servers.at(i)->server_;
        node.study_info_ = servers.at(i)->tablet_results_.at(index.tablet_idx_).study_info_;
        if (OB_TMP_FAIL(buffer.nodes_.push_back(node))) {
          LOG_WARN("fail to build study diagnose node results", KR(tmp_ret), K(first.ls_id_), K(i));
        }
      }
    }
  }
  if (OB_SUCCESS == tmp_ret) {
    const ObSSGCTablet &tablet = servers.at(index.server_idx_)
        ->tablet_results_.at(index.tablet_idx_).gc_tablet_;
    if (OB_TMP_FAIL(get_study_record_size(tablet, buffer.nodes_, record_len))) {
      LOG_WARN("fail to size study diagnose record", KR(tmp_ret), K(tablet));
    } else if (OB_UNLIKELY(record_len > INT32_MAX)) {
      // Readers hold one record as ObString; lengths that ObString cannot represent must not be written.
      tmp_ret = OB_SIZE_OVERFLOW;
      LOG_WARN("study diagnose record is too large", KR(tmp_ret), K(record_len), K(tablet));
    } else if (record_len > STUDY_BATCH_BYTES && buffer.len_ > 0) {
      // Commit the current batch first; oversized records are encoded alone in the next batch.
      ret = OB_BUF_NOT_ENOUGH;
    } else if (OB_UNLIKELY(buffer.len_ > INT64_MAX - record_len)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("study diagnose batch length overflow", KR(ret), K(buffer.len_), K(record_len));
    } else {
      const int64_t capacity = MAX(STUDY_BATCH_BYTES, buffer.len_ + record_len);
      if (capacity > buffer.capacity_) {
        char *buf = static_cast<char *>(ob_malloc(capacity, ObMemAttr(tenant_id_, "SSGCStudyBuf")));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to allocate study diagnose batch buffer", KR(ret), K(capacity));
        } else {
          if (buffer.len_ > 0) {
            MEMCPY(buf, buffer.buf_, buffer.len_);
          }
          if (OB_NOT_NULL(buffer.buf_)) {
            ob_free(buffer.buf_);
          }
          buffer.buf_ = buf;
          buffer.capacity_ = capacity;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_TMP_FAIL(encode_study_record(tablet, buffer.nodes_, buffer.buf_, buffer.capacity_, buffer.len_))) {
        LOG_WARN("fail to encode study diagnose record", KR(tmp_ret), K(tablet));
      } else if (OB_FAIL(buffer.record_lens_.push_back(record_len))) {
        // Abort the file when data and length arrays disagree; do not skip this batch and keep appending.
        LOG_WARN("fail to save study diagnose record length", KR(ret), K(record_len));
      }
    }
  }
  stat.record_failure(tmp_ret, "write");
  return ret;
}

int ObSSMarkSweepDiagMgr::serialize_study_diag_batch_(
    const ObSSGCStudyDiagInfo &diag_info,
    StudyDiagTabletIndex &next_tablet,
    StudyDiagBuffer &buffer,
    StudyDiagStat &stat)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObSSGCStudyDiagServerResult *> &servers = diag_info.get_study_results();
  buffer.len_ = 0;
  buffer.record_lens_.reuse();
  if (OB_UNLIKELY(next_tablet.server_idx_ < 0 || next_tablet.server_idx_ > servers.count()
      || next_tablet.tablet_idx_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid study diagnose batch position", KR(ret), K(next_tablet));
  }
  while (OB_SUCC(ret) && next_tablet.server_idx_ < servers.count()
      && buffer.len_ < STUDY_BATCH_BYTES) {
    if (OB_ISNULL(servers.at(next_tablet.server_idx_))) {
      ret = OB_INVALID_DATA;
      LOG_WARN("null study diagnose server result", KR(ret), K(next_tablet));
    } else {
      const ObSSGCStudyDiagServerResult &first = *servers.at(next_tablet.server_idx_);
      if (next_tablet.tablet_idx_ >= first.tablet_results_.count()) {
        // Current LS is fully walked (may be empty); skip the remaining nodes.
        ++next_tablet.server_idx_;
        while (next_tablet.server_idx_ < servers.count()
            && OB_NOT_NULL(servers.at(next_tablet.server_idx_))
            && servers.at(next_tablet.server_idx_)->ls_id_ == first.ls_id_) {
          ++next_tablet.server_idx_;
        }
        next_tablet.tablet_idx_ = 0;
      } else if (OB_FAIL(serialize_study_diag_record_(diag_info, next_tablet, buffer, stat))) {
        if (OB_BUF_NOT_ENOUGH == ret && buffer.len_ > 0) {
          // Current record is not consumed yet; return the pending batch first.
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to serialize study diagnose batch", KR(ret), K(next_tablet));
        }
      } else {
        ++next_tablet.tablet_idx_;
      }
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::write_study_diag_(
    const uint64_t tenant_id,
    const int64_t round_id,
    const ObSSGCStudyDiagInfo &diag_info,
    StudyDiagStat &stat)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time_us();
  const int64_t bucket = 0;
  const int64_t bucket_cnt = 1;
  ObSSGCService *gc_service = MTL_WITH_CHECK_TENANT(ObSSGCService *, tenant_id);
  StudyDiagTabletIndex next_tablet;
  StudyDiagBuffer buffer(tenant_id);
  // Null-check only here; later stop checks run only while ret succeeds, so they need not re-check null.
  if (OB_ISNULL(gc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gc service is null", KR(ret), K(tenant_id), K(round_id));
  } else if (gc_service->is_stopped()) {
    ret = OB_SERVICE_STOPPED;
    LOG_INFO("stop writing study diagnose", KR(ret), K(tenant_id), K(round_id));
  } else if (OB_FAIL(init(tenant_id, round_id, bucket_cnt))) {
    LOG_WARN("fail to init study diagnose manager", KR(ret), K(round_id));
  } else if (OB_FAIL(check_diag_task())) {
    LOG_WARN("fail to check study diagnose task", KR(ret), K(round_id));
  } else {
    set_diag_enabled(true);
  }
  stat.record_failure(ret, "prepare");
  while (OB_SUCC(ret) && next_tablet.server_idx_ < diag_info.get_study_results().count()) {
    if (gc_service->is_stopped()) {
      ret = OB_SERVICE_STOPPED;
      LOG_INFO("stop publishing study diagnose", KR(ret), K(tenant_id), K(round_id));
    } else if (OB_FAIL(serialize_study_diag_batch_(diag_info, next_tablet, buffer, stat))) {
      LOG_WARN("fail to serialize study diagnose batch", KR(ret), K(round_id));
    } else if (buffer.len_ > 0 && OB_FAIL(write_tablets_study_results(
        bucket, buffer.buf_, buffer.len_, buffer.record_lens_))) {
      LOG_WARN("fail to write study diagnose batch", KR(ret), K(round_id));
    } else if (buffer.len_ > 0) {
      bool should_debug_sync = true;
      stat.append_record_count_ += buffer.record_lens_.count();
#ifdef ERRSIM
      should_debug_sync = ObSSGCUtil::is_errsim_target(tenant_id);
#endif
      if (OB_SUCC(ret) && should_debug_sync) {
        DEBUG_SYNC(AFTER_SS_MARK_SWEEP_STUDY_DIAG_BATCH);
      }
    }
    stat.record_failure(ret, "write");
  }
  // All appends have completed; free the batch buffer before finalize and keep only the file length index.
  buffer.reset();
  if (OB_FAIL(ret)) {
    abort();
  } else if (gc_service->is_stopped()) {
    ret = OB_SERVICE_STOPPED;
    stat.record_failure(ret, "complete");
    LOG_INFO("stop completing study diagnose", KR(ret), K(tenant_id), K(round_id));
    abort();
  } else if (OB_FAIL(complete_study_results(bucket))) {
    stat.record_failure(ret, "complete");
    LOG_WARN("fail to complete study diagnose file", KR(ret), K(round_id));
  }
  stat.publish_cost_us_ = ObTimeUtility::current_time_us() - start_us;
  if (OB_SUCC(ret) && OB_SUCCESS != stat.first_fail_ret_) {
    ret = stat.first_fail_ret_;
  }
  return ret;
}

void ObSSMarkSweepDiagMgr::report_study_diag_(
    const uint64_t tenant_id,
    const int64_t round_id,
    const SCN &gc_meta_scn,
    const int input_ret,
    const bool can_publish,
    const StudyDiagStat &stat)
{
  int ret = OB_SUCCESS;
  char extra_info[1024] = {};
  int64_t pos = 0;
  const bool has_diag_failed = OB_SUCCESS != stat.first_fail_ret_;
  const char *publish_status = has_diag_failed ? "FAILED"
      : (can_publish ? "SUCCESS" : "SKIPPED");
  if (OB_FAIL(databuff_printf(extra_info, sizeof(extra_info), pos,
      "input_ret=%d,input_result_count=%ld,input_tablet_count=%ld,append_record_count=%ld,"
      "first_fail_stage=%s,publish_cost_us=%ld,gc_meta_scn_valid=%d,trace_id=%s", input_ret,
      stat.input_result_count_, stat.input_tablet_count_, stat.append_record_count_,
      stat.first_fail_stage_, stat.publish_cost_us_, gc_meta_scn.is_valid(),
      ObCurTraceId::get_trace_id_str()))) {
    LOG_WARN("fail to format study diagnose event", KR(ret), K(round_id));
  } else if (OB_FAIL(SERVER_EVENT_ADD("SHARED_GC", "FINISH_MARK_SWEEP_STUDY_DIAG",
      "tenant_id", tenant_id, "round_id", round_id, "gc_meta_scn", gc_meta_scn,
      "has_diag_failed", has_diag_failed, "diag_first_fail_ret", stat.first_fail_ret_,
      "publish_status", publish_status, extra_info))) {
    LOG_WARN("fail to submit study diagnose event", KR(ret), K(round_id));
  }
}

// Call flow:
// publish_study_diag()
//   -> validate_study_diag_()         // Validate input and collect statistics once.
//   -> write_study_diag_()            // Write Study results to object storage.
//       -> serialize_study_diag_batch_()
//           -> serialize_study_diag_record_()
//               -> Build all node results for the current tablet.
//   -> report_study_diag_()
int ObSSMarkSweepDiagMgr::publish_study_diag(
    const uint64_t tenant_id,
    const int64_t round_id,
    const SCN &gc_meta_scn,
    const int input_ret,
    const ObSSGCStudyDiagInfo &diag_info)
{
  int ret = OB_SUCCESS;
  StudyDiagStat stat;
  // Study failure always yields a non-success input_ret, so publish is gated only on input_ret.
  const bool can_publish = (OB_SUCCESS == input_ret);
  stat.record_failure(diag_info.get_first_fail_ret(), diag_info.get_first_fail_stage());
  if (OB_SUCCESS == stat.first_fail_ret_
      && OB_FAIL(validate_study_diag_(diag_info, stat))) {
    LOG_WARN("fail to validate study diagnose", KR(ret), K(round_id));
    stat.record_failure(ret, "validate");
  }
  if (can_publish && OB_SUCCESS == stat.first_fail_ret_) {
    if (OB_FAIL(write_study_diag_(tenant_id, round_id, diag_info, stat))) {
      LOG_WARN("fail to write study diagnose", KR(ret), K(round_id));
      stat.record_failure(ret, "write");
    }
  }
  if (has_diag_failed()) {
    stat.record_failure(get_first_fail_ret(), "complete");
  }
  ret = stat.first_fail_ret_;
  report_study_diag_(tenant_id, round_id, gc_meta_scn, input_ret, can_publish, stat);
  return ret;
}

// ====================================================================================
// ObSSGCStudyDiagInfo
// ====================================================================================
ObSSGCStudyDiagInfo::ObSSGCStudyDiagInfo(const uint64_t tenant_id)
  : tenant_id_(tenant_id), first_fail_ret_(OB_SUCCESS), first_fail_stage_("none"), study_results_()
{
  study_results_.set_attr(ObMemAttr(tenant_id_, "SSGCStudyDiag"));
}

ObSSGCStudyDiagInfo::~ObSSGCStudyDiagInfo()
{
  reset();
}

void ObSSGCStudyDiagInfo::clear_study_results_()
{
  for (int64_t i = 0; i < study_results_.count(); ++i) {
    ObSSGCStudyDiagServerResult *result = study_results_.at(i);
    result->~ObSSGCStudyDiagServerResult();
    ob_free(result);
  }
  study_results_.reset();
}

void ObSSGCStudyDiagInfo::reset()
{
  clear_study_results_();
  first_fail_ret_ = OB_SUCCESS;
  first_fail_stage_ = "none";
}

void ObSSGCStudyDiagInfo::mark_diag_failed(
    const int ret_code,
    const char *stage)
{
  if (OB_SUCCESS != ret_code && OB_SUCCESS == first_fail_ret_) {
    first_fail_ret_ = ret_code;
    first_fail_stage_ = OB_ISNULL(stage) ? "unknown" : stage;
    clear_study_results_();
  }
}

void ObSSGCStudyDiagInfo::reset_ls_study_results(const ObLSID &ls_id)
{
  // Nodes of the same LS are stored contiguously; roll back only this failed study, not prior successful LSes.
  while (!study_results_.empty() && study_results_.at(study_results_.count() - 1)->ls_id_ == ls_id) {
    ObSSGCStudyDiagServerResult *result = study_results_.at(study_results_.count() - 1);
    result->~ObSSGCStudyDiagServerResult();
    ob_free(result);
    study_results_.pop_back();
  }
}

int ObSSGCStudyDiagInfo::save_study_result(
    const ObAddr &server,
    const ObSSGCTabletWithDelScnArray &batch_tablets,
    const ObIArray<ObSSGCTabletStudyResult> &study_results)
{
  int ret = OB_SUCCESS;
  ObSSGCStudyDiagServerResult *saved = nullptr;
  if (OB_SUCCESS != first_fail_ret_) {
  } else if (OB_UNLIKELY(batch_tablets.empty() || study_results.count() != batch_tablets.count()
                         || !server.is_valid())) {
    ret = OB_INVALID_DATA;
  } else {
    const ObLSID &ls_id = batch_tablets.at(0).gc_tablet_.ls_id_;
    if (!study_results_.empty()) {
      saved = study_results_.at(study_results_.count() - 1);
      if (saved->ls_id_ != ls_id || saved->server_ != server) {
        saved = nullptr;
      }
    }
    if (nullptr == saved) {
      void *buf = ob_malloc(sizeof(ObSSGCStudyDiagServerResult),
          ObMemAttr(tenant_id_, "SSGCStudyDiag"));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        saved = new (buf) ObSSGCStudyDiagServerResult();
        saved->ls_id_ = ls_id;
        saved->server_ = server;
        saved->tablet_results_.set_attr(ObMemAttr(tenant_id_, "SSGCStudyDiag"));
        if (OB_FAIL(study_results_.push_back(saved))) {
          saved->~ObSSGCStudyDiagServerResult();
          ob_free(saved);
          saved = nullptr;
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < study_results.count(); ++i) {
      // Only validate diagnose index association; do not feed diagnose errors into RPC retry or GC merge.
      if (OB_UNLIKELY(!(study_results.at(i).gc_tablet_ == batch_tablets.at(i).gc_tablet_)
                     || study_results.at(i).gc_tablet_.ls_id_ != ls_id)) {
        ret = OB_INVALID_DATA;
      } else if (OB_FAIL(saved->tablet_results_.push_back(study_results.at(i)))) {
        LOG_WARN("fail to save raw study result", KR(ret), K(server), K(ls_id), K(i));
      }
    }
  }
  if (OB_FAIL(ret)) {
    mark_diag_failed(ret, "collect");
    LOG_WARN("disable study diagnose after saving raw result failed", KR(ret), K(server));
  }
  return ret;
}

// Study collector bound on the current thread; whether other threads enable diagnose is irrelevant here.
thread_local ObSSGCStudyDiagInfo *g_cur_study_diag_info = nullptr;

ObSSGCStudyDiagInfo::Scope::Scope(ObSSGCStudyDiagInfo *diag_info)
  : prev_diag_info_(nullptr), is_bound_(nullptr != diag_info)
{
  if (is_bound_) {
    prev_diag_info_ = g_cur_study_diag_info;
    g_cur_study_diag_info = diag_info;
  }
}

ObSSGCStudyDiagInfo::Scope::~Scope()
{
  if (is_bound_) {
    g_cur_study_diag_info = prev_diag_info_;
  }
}

ObSSGCStudyDiagInfo *ObSSGCStudyDiagInfo::get_cur_diag_info()
{
  return g_cur_study_diag_info;
}

void ObSSGCStudyDiagInfo::try_reset_ls_study_results(const ObLSID &ls_id)
{
  ObSSGCStudyDiagInfo *diag_info = get_cur_diag_info();
  if (OB_NOT_NULL(diag_info)) {
    diag_info->reset_ls_study_results(ls_id);
  }
}

void ObSSGCStudyDiagInfo::try_save_study_result(
    const ObAddr &server,
    const ObSSGCTabletWithDelScnArray &batch_tablets,
    const ObIArray<ObSSGCTabletStudyResult> &study_results)
{
  int ret = OB_SUCCESS;
  ObSSGCStudyDiagInfo *diag_info = get_cur_diag_info();
  if (OB_NOT_NULL(diag_info)
      && OB_FAIL(diag_info->save_study_result(server, batch_tablets, study_results))) {
    LOG_WARN("fail to save study result, ignore", KR(ret), K(server),
        "batch_tablet_cnt", batch_tablets.count());
  }
}

// ====================================================================================
// ObSSGCStudyDiagGuard
// ====================================================================================
bool ObSSGCStudyDiagGuard::check_diag_enabled_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_enabled = false;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  if (!tenant_config.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tenant config for study diagnose, ignore", KR(ret), K(tenant_id));
  } else if (!tenant_config->_enable_shared_mark_sweep_gc_diagnose) {
  } else {
    is_enabled = GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_6_3_0;
  }
  return is_enabled;
}

ObSSGCStudyDiagGuard::ObSSGCStudyDiagGuard(const uint64_t tenant_id, const int64_t round_id)
  : is_enabled_(check_diag_enabled_(tenant_id)),
    is_finished_(false),
    tenant_id_(tenant_id),
    round_id_(round_id),
    diag_info_(tenant_id),
    scope_(is_enabled_ ? &diag_info_ : nullptr)
{
}

int ObSSGCStudyDiagGuard::prepare()
{
  int ret = OB_SUCCESS;
  if (is_finished_ || !is_enabled_) {
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::prepare_diag_task(tenant_id_, round_id_))) {
    LOG_WARN("fail to prepare round diagnose task, ignore", KR(ret),
        K_(tenant_id), K_(round_id));
    diag_info_.mark_diag_failed(ret, "prepare");
  }
  return ret;
}

int ObSSGCStudyDiagGuard::finish(const SCN &gc_meta_scn, const int round_ret)
{
  int ret = OB_SUCCESS;
  if (!is_finished_) {
    is_finished_ = true;
    if (is_enabled_) {
      ObSSMarkSweepDiagMgr diag_mgr;
      if (OB_FAIL(diag_mgr.publish_study_diag(tenant_id_, round_id_, gc_meta_scn, round_ret, diag_info_))) {
        LOG_WARN("fail to publish study diag", KR(ret), K_(tenant_id), K_(round_id), K(gc_meta_scn), K(round_ret));
      }
      diag_mgr.reset();
      diag_info_.reset();
    }
  }
  return ret;
}

// ====================================================================================
// read helpers
// ====================================================================================
int ObSSMarkSweepDiagMgr::parse_tailer_and_index(
    const ObString &uri,
    const ObBackupDest &storage_dest,
    const uint64_t storage_id,
    ObIAllocator &allocator,
    ObIArray<int64_t> &record_lens,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  int64_t file_len = 0;
  ObSSMarkSweepDiagTailer tailer;
  const int64_t tailer_size = tailer.get_serialize_size();
  record_lens.reset();
  data_len = 0;
  if (OB_UNLIKELY(uri.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(uri));
  } else if (OB_FAIL(ObBackupIoAdapter::get_file_length(uri, storage_dest.get_storage_info(),
                                                        file_len))) {
    LOG_WARN("fail to get file length", KR(ret), K(uri));
  } else if (OB_UNLIKELY(file_len < tailer_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("file len shorter than tailer len", KR(ret), K(uri), K(file_len), K(tailer_size));
  } else {
    const ObStorageIdMod storage_id_mod(storage_id, ObStorageUsedMod::STORAGE_USED_DATA);
    int64_t read_size = 0;
    // 1. read fixed-length tailer
    char *tailer_buf = static_cast<char *>(allocator.alloc(tailer_size));
    int64_t pos = 0;
    if (OB_ISNULL(tailer_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc tailer buffer", KR(ret), K(tailer_size));
    } else if (OB_FAIL(ObBackupIoAdapter::pread(uri, storage_dest.get_storage_info(),
                       tailer_buf, tailer_size, file_len - tailer_size /*offset*/,
                       read_size, storage_id_mod))) {
      LOG_WARN("fail to pread tailer", KR(ret), K(uri), K(file_len), K(tailer_size));
    } else if (OB_UNLIKELY(read_size != tailer_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("short read tailer", KR(ret), K(uri), K(read_size), K(tailer_size));
    } else if (OB_FAIL(tailer.deserialize(tailer_buf, tailer_size, pos))) {
      LOG_WARN("fail to deserialize tailer", KR(ret), K(uri));
    } else if (OB_UNLIKELY(!tailer.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid tailer", KR(ret), K(uri), K(tailer));
    }
    // 2. read + parse the variable-length index
    const int64_t index_offset = OB_SUCC(ret) ? tailer.get_index_offset() : 0;
    const int64_t index_size = file_len - tailer_size - index_offset;
    ObSSMarkSweepDiagIndex index;
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY((index_offset < 0) || (index_size < 0)
                           || (index_offset + index_size + tailer_size != file_len))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid index layout", KR(ret), K(uri), K(file_len), K(index_offset),
               K(index_size), K(tailer_size));
    } else if (index_size > 0) {
      char *index_buf = static_cast<char *>(allocator.alloc(index_size));
      int64_t index_pos = 0;
      if (OB_ISNULL(index_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc index buffer", KR(ret), K(index_size));
      } else if (OB_FAIL(ObBackupIoAdapter::pread(uri, storage_dest.get_storage_info(),
                         index_buf, index_size, index_offset /*offset*/, read_size, storage_id_mod))) {
        LOG_WARN("fail to pread index", KR(ret), K(uri), K(index_offset), K(index_size));
      } else if (OB_UNLIKELY(read_size != index_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("short read index", KR(ret), K(uri), K(read_size), K(index_size));
      } else if (OB_FAIL(index.deserialize(index_buf, index_size, index_pos))) {
        LOG_WARN("fail to deserialize index", KR(ret), K(uri));
      }
    }
    // 3. return per-record lengths and the data region length (no data read here)
    if (OB_SUCC(ret)) {
      const ObIArray<int64_t> &record_lens_in_index = index.get_record_lens();
      int64_t total_record_len = 0;
      for (int64_t i = 0; OB_SUCC(ret) && (i < record_lens_in_index.count()); ++i) {
        const int64_t record_len = record_lens_in_index.at(i);
        if (OB_UNLIKELY((record_len <= 0) || ((total_record_len + record_len) > index_offset))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid record_len", KR(ret), K(i), K(record_len), K(total_record_len), K(index_offset));
        } else if (OB_FAIL(record_lens.push_back(record_len))) {
          LOG_WARN("fail to push back record_len", KR(ret), K(i));
        } else {
          total_record_len += record_len;
        }
      }
      if (OB_SUCC(ret)) {
        data_len = index_offset;
      }
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::decode_macros_record(
    const ObString &record,
    ObSSMarkSweepDiagTabletMacros &tablet_macros)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t macro_cnt = 0;
  tablet_macros.reset();
  if (OB_FAIL(tablet_macros.tablet_.deserialize(record.ptr(), record.length(), pos))) {
    LOG_WARN("fail to deserialize tablet", KR(ret));
  } else if (OB_FAIL(serialization::decode_i64(record.ptr(), record.length(), pos, &macro_cnt))) {
    LOG_WARN("fail to decode macro_cnt", KR(ret));
  } else {
    MacroBlockId macro_id;
    for (int64_t i = 0; OB_SUCC(ret) && (i < macro_cnt); ++i) {
      macro_id.reset();
      if (OB_FAIL(macro_id.deserialize(record.ptr(), record.length(), pos))) {
        LOG_WARN("fail to deserialize macro id", KR(ret), K(i));
      } else if (OB_FAIL(tablet_macros.macro_ids_.push_back(macro_id))) {
        LOG_WARN("fail to push back macro id", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::decode_meta_scns_record(
    const ObString &record,
    ObSSMarkSweepTabletMetaScns &tablet_meta_scns)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t meta_scn_cnt = 0;
  tablet_meta_scns.reset();
  if (OB_FAIL(tablet_meta_scns.tablet_.deserialize(record.ptr(), record.length(), pos))) {
    LOG_WARN("fail to deserialize tablet", KR(ret));
  } else if (OB_FAIL(serialization::decode_i64(record.ptr(), record.length(), pos, &meta_scn_cnt))) {
    LOG_WARN("fail to decode meta_scn_cnt", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && (i < meta_scn_cnt); ++i) {
      SCN meta_scn;
      if (OB_FAIL(meta_scn.deserialize(record.ptr(), record.length(), pos))) {
        LOG_WARN("fail to deserialize meta_scn", KR(ret), K(i));
      } else if (OB_FAIL(tablet_meta_scns.meta_scns_.push_back(meta_scn))) {
        LOG_WARN("fail to push back meta_scn", KR(ret), K(i));
      }
    }
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::get_study_record_size(
    const ObSSGCTablet &tablet,
    const ObIArray<ObSSMarkSweepDiagStudyNode> &study_results,
    int64_t &size)
{
  int ret = OB_SUCCESS;
  size = 0;
  if (OB_UNLIKELY(!tablet.is_valid() || study_results.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid study record", KR(ret), K(tablet), K(study_results.count()));
  } else {
    size = tablet.get_serialize_size() + serialization::encoded_length_i64(study_results.count());
    for (int64_t i = 0; OB_SUCC(ret) && i < study_results.count(); ++i) {
      const ObSSMarkSweepDiagStudyNode &node = study_results.at(i);
      const int64_t node_size = node.server_.get_serialize_size()
          + node.study_info_.min_dep_meta_scn_.get_serialize_size()
          + serialization::encoded_length_i64(node.study_info_.min_major_snapshot_);
      if (OB_UNLIKELY(!node.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid study node", KR(ret), K(i), K(node));
      } else if (OB_UNLIKELY(node_size > INT64_MAX - size)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("study record size overflow", KR(ret), K(size), K(node_size));
      } else {
        size += node_size;
      }
    }
  }
  if (OB_FAIL(ret)) {
    size = 0;
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::encode_study_record(
    const ObSSGCTablet &tablet,
    const ObIArray<ObSSMarkSweepDiagStudyNode> &study_results,
    char *buf,
    const int64_t buf_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int64_t start_pos = pos;
  int64_t record_size = 0;
  if (OB_ISNULL(buf) || OB_UNLIKELY(pos < 0 || buf_len < pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid study buffer", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(get_study_record_size(tablet, study_results, record_size))) {
    LOG_WARN("fail to get study record size", KR(ret), K(tablet));
  } else if (OB_UNLIKELY(record_size > buf_len - pos)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("study buffer is too small", KR(ret), K(record_size), K(buf_len), K(pos));
  } else if (OB_FAIL(tablet.serialize(buf, buf_len, pos))) {
    LOG_WARN("fail to serialize study tablet", KR(ret), K(tablet));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, study_results.count()))) {
    LOG_WARN("fail to encode study node count", KR(ret), K(study_results.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < study_results.count(); ++i) {
      const ObSSMarkSweepDiagStudyNode &node = study_results.at(i);
      if (OB_FAIL(node.server_.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize study server", KR(ret), K(i));
      } else if (OB_FAIL(node.study_info_.min_dep_meta_scn_.serialize(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize study minor scn", KR(ret), K(i));
      } else if (OB_FAIL(serialization::encode_i64(
          buf, buf_len, pos, node.study_info_.min_major_snapshot_))) {
        LOG_WARN("fail to encode study major snapshot", KR(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(pos - start_pos != record_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("study record size mismatch", KR(ret), K(pos), K(start_pos), K(record_size));
  }
  if (OB_FAIL(ret)) {
    pos = start_pos;
  }
  return ret;
}

int ObSSMarkSweepDiagMgr::decode_study_record(
    const ObString &rec,
    ObSSMarkSweepDiagStudyResults &out)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t node_count = 0;
  ObSSMarkSweepDiagStudyNode node;
  // Each node has at least a fixed-width major snapshot; do not pre-size arrays from the on-file count.
  const int64_t min_node_size = serialization::encoded_length_i64(0);
  out.reset();
  if (OB_ISNULL(rec.ptr()) || OB_UNLIKELY(rec.length() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid study record buffer", KR(ret), K(rec.length()));
  } else if (OB_FAIL(out.tablet_.deserialize(rec.ptr(), rec.length(), pos))) {
    LOG_WARN("fail to deserialize study tablet", KR(ret));
  } else if (OB_UNLIKELY(!out.tablet_.is_valid())) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid study tablet", KR(ret), K(out.tablet_));
  } else if (OB_FAIL(serialization::decode_i64(rec.ptr(), rec.length(), pos, &node_count))) {
    LOG_WARN("fail to decode study node count", KR(ret));
  } else if (OB_UNLIKELY(node_count <= 0 || pos > rec.length()
      || node_count > (rec.length() - pos) / min_node_size)) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid study node count", KR(ret), K(node_count), K(pos), K(rec.length()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < node_count; ++i) {
      node.reset();
      if (OB_FAIL(node.server_.deserialize(rec.ptr(), rec.length(), pos))) {
        LOG_WARN("fail to deserialize study server", KR(ret), K(i));
      } else if (OB_FAIL(node.study_info_.min_dep_meta_scn_.deserialize(
          rec.ptr(), rec.length(), pos))) {
        LOG_WARN("fail to deserialize study minor scn", KR(ret), K(i));
      } else if (OB_FAIL(serialization::decode_i64(
          rec.ptr(), rec.length(), pos, &node.study_info_.min_major_snapshot_))) {
        LOG_WARN("fail to decode study major snapshot", KR(ret), K(i));
      } else if (OB_UNLIKELY(!node.is_valid())) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("invalid study node", KR(ret), K(i), K(node));
      } else if (OB_FAIL(out.study_results_.push_back(node))) {
        LOG_WARN("fail to append decoded study node", KR(ret), K(i));
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(pos != rec.length())) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("study record has trailing bytes", KR(ret), K(pos), K(rec.length()));
  }
  if (OB_FAIL(ret)) {
    out.reset();
  }
  return ret;
}


int ObSSMarkSweepDiagMgr::decode_lob_ctrl_record(
    const ObString &record,
    ObSSLobCtrlGCDiagRows &rows)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t row_count = 0;
  rows.reset();
  if (OB_FAIL(serialization::decode_i64(record.ptr(), record.length(), pos, &row_count))) {
    LOG_WARN("failed to decode lob ctrl gc diag row count", KR(ret));
  } else if (OB_UNLIKELY(row_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob ctrl gc diag row count", KR(ret), K(row_count));
  } else {
    ObSSOuttableLobCtrlCandidate row;
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
      row.reset();
      if (OB_FAIL(row.deserialize(record.ptr(), record.length(), pos))) {
        LOG_WARN("failed to deserialize lob ctrl gc diag row", KR(ret), K(i));
      } else if (OB_UNLIKELY(!row.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid lob ctrl gc diag row", KR(ret), K(i), K(row));
      } else if (OB_FAIL(rows.rows_.push_back(row))) {
        LOG_WARN("failed to append lob ctrl gc diag row", KR(ret), K(i));
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(pos != record.length())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected lob ctrl gc diag record length", KR(ret), K(pos), K(record.length()));
    }
  }
  return ret;
}

// ====================================================================================
// ObSSMarkSweepDiagFileReader
// ====================================================================================
ObSSMarkSweepDiagFileReader::ObSSMarkSweepDiagFileReader()
  : is_inited_(false),
    storage_dest_(),
    storage_id_(OB_INVALID_ID),
    index_allocator_(ObMemAttr(OB_SERVER_TENANT_ID, "MSGcDiagRdr")),
    record_lens_(),
    data_len_(0),
    next_record_idx_(0),
    next_offset_(0)
{
  uri_[0] = '\0';
}

void ObSSMarkSweepDiagFileReader::reset()
{
  is_inited_ = false;
  storage_dest_.reset();
  storage_id_ = OB_INVALID_ID;
  uri_[0] = '\0';
  record_lens_.reset();
  index_allocator_.reset();
  data_len_ = 0;
  next_record_idx_ = 0;
  next_offset_ = 0;
}

int ObSSMarkSweepDiagFileReader::init(
    const uint64_t tenant_id,
    const int64_t round_id,
    const ObMarkSweepGroupType group,
    const ObSSMarkSweepDiagFileType type,
    const int64_t bucket)
{
  int ret = OB_SUCCESS;
  char *root_dir = nullptr;
  int64_t pos = 0;
  MacroBlockId macro_id;
  reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || round_id < 0 || bucket < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(round_id), K(bucket));
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::get_diag_macro_id(round_id, group, type, bucket, macro_id))) {
    LOG_WARN("fail to get diag macro id", KR(ret), K(round_id), K(group), K(type), K(bucket));
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::get_storage_dest_and_id(storage_dest_, storage_id_))) {
    LOG_WARN("fail to get storage dest and id", KR(ret));
  } else if (OB_FAIL(OB_DIR_MGR.get_object_storage_root_dir(root_dir))) {
    LOG_WARN("fail to get object storage root dir", KR(ret));
  } else if (OB_FAIL(SSObjUtil::to_remote_path_format(macro_id, uri_, sizeof(uri_), pos,
                     root_dir, GCONF.cluster_id, tenant_id, 0/*tenant_epoch_id*/,
                     0/*server_id*/, 0/*ls_id*/, 0/*ls_epoch_id*/))) {
    LOG_WARN("fail to build diag file uri", KR(ret), K(macro_id), K(tenant_id));
  } else if (OB_FAIL(ObSSMarkSweepDiagMgr::parse_tailer_and_index(ObString(uri_), storage_dest_,
                     storage_id_, index_allocator_, record_lens_, data_len_))) {
    LOG_WARN("fail to parse tailer and index", KR(ret), K(uri_));
  } else {
    next_record_idx_ = 0;
    next_offset_ = 0;
    is_inited_ = true;
  }
  return ret;
}

int ObSSMarkSweepDiagFileReader::next_record_batch(
    ObIAllocator &allocator,
    ObIArray<ObString> &records)
{
  int ret = OB_SUCCESS;
  records.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("reader not inited", KR(ret));
  } else if (next_record_idx_ >= record_lens_.count()) {
    ret = OB_ITER_END;
    LOG_TRACE("end of batch", KR(ret), K_(next_record_idx), K(record_lens_.count()));
  } else {
    // accumulate whole records until adding the next one would exceed MAX_BATCH_BYTES;
    // a single record larger than the cap still forms its own batch.
    const int64_t batch_begin_idx = next_record_idx_;
    const int64_t batch_begin_offset = next_offset_;
    int64_t batch_bytes = 0;
    bool is_complete = false;
    while ((next_record_idx_ < record_lens_.count()) && !is_complete) {
      const int64_t record_len = record_lens_.at(next_record_idx_);
      if ((batch_bytes > 0) && ((batch_bytes + record_len) > MAX_BATCH_BYTES)) {
        is_complete = true;
      } else {
        batch_bytes += record_len;
        next_offset_ += record_len;
        ++next_record_idx_;
      }
    }
    if (OB_UNLIKELY((batch_bytes <= 0) || ((batch_begin_offset + batch_bytes) > data_len_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid batch range", KR(ret), K(batch_begin_offset), K(batch_bytes), K_(data_len));
    } else {
      const ObStorageIdMod storage_id_mod(storage_id_, ObStorageUsedMod::STORAGE_USED_DATA);
      int64_t read_size = 0;
      char *data_buf = static_cast<char *>(allocator.alloc(batch_bytes));
      if (OB_ISNULL(data_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc batch buffer", KR(ret), K(batch_bytes));
      } else if (OB_FAIL(ObBackupIoAdapter::pread(ObString(uri_), storage_dest_.get_storage_info(),
                         data_buf, batch_bytes, batch_begin_offset /*offset*/, read_size, storage_id_mod))) {
        LOG_WARN("fail to pread batch", KR(ret), K_(uri), K(batch_begin_offset), K(batch_bytes));
      } else if (OB_UNLIKELY(read_size != batch_bytes)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected read size", KR(ret), K_(uri), K(read_size), K(batch_bytes));
      } else {
        int64_t cur_len = 0;
        for (int64_t i = batch_begin_idx; OB_SUCC(ret) && (i < next_record_idx_); ++i) {
          const int64_t record_len = record_lens_.at(i);
          if (OB_FAIL(records.push_back(ObString(record_len, data_buf + cur_len)))) {
            LOG_WARN("fail to push back record", KR(ret), K(i));
          } else {
            cur_len += record_len;
          }
        }
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
