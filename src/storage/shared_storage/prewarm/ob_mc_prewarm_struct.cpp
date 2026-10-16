/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_mc_prewarm_struct.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/prewarm/ob_ss_local_cache_prewarm_service.h"
#include "storage/compaction/ob_major_pre_warmer.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "storage/blocksstable/ob_storage_object_rw_info.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/prewarm/ob_macro_prewarm_struct.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/column_store/ob_column_oriented_sstable.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::compaction;
using namespace oceanbase::lib;
using namespace oceanbase::share;

static constexpr char MAJOR_COMPACTION_PREWARM_TAG[] = "MC_PREWARM";

/***************************ObHotMicroInfo**********************/
OB_SERIALIZE_MEMBER(ObHotMicroInfo, offset_, size_, logic_micro_id_, micro_crc_);

ObHotMicroInfo::ObHotMicroInfo() : offset_(0), size_(0), logic_micro_id_(), micro_crc_(0)
{
}

ObHotMicroInfo::ObHotMicroInfo(
    const int64_t offset,
    const int64_t size,
    const ObLogicMicroBlockId &logic_micro_id,
    const int64_t micro_crc)
  : offset_(offset), size_(size), logic_micro_id_(logic_micro_id), micro_crc_(micro_crc)
{
}


/***************************ObHotMacroInfo**********************/
OB_SERIALIZE_MEMBER(ObHotMacroInfo, macro_id_, hot_micro_infos_);

ObHotMacroInfo::ObHotMacroInfo() : macro_id_(), hot_micro_infos_()
{
  hot_micro_infos_.set_attr(ObMemAttr(MTL_ID(), MAJOR_COMPACTION_PREWARM_TAG));
}


/***************************ObHotTabletInfo**********************/
OB_SERIALIZE_MEMBER(ObHotTabletInfo, hot_macro_infos_);

ObHotTabletInfo::ObHotTabletInfo() : hot_macro_infos_()
{
  hot_macro_infos_.set_attr(ObMemAttr(MTL_ID(), MAJOR_COMPACTION_PREWARM_TAG));
}

void ObHotTabletInfo::reset()
{
  hot_macro_infos_.reuse();
}

int ObHotTabletInfo::push_back(
  const blocksstable::ObMicroBlockDesc &micro_block_desc,
  ObHotTabletInfoBaseWriter &writer)
{
  int ret = OB_SUCCESS;
  const int64_t macro_cnt = count();
  if (OB_ISNULL(micro_block_desc.header_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "micro block desc header is null", KR(ret), K(micro_block_desc));
  } else {
    ObHotMicroInfo micro_info(micro_block_desc.block_offset_,
        micro_block_desc.buf_size_ + micro_block_desc.header_->header_size_,
        micro_block_desc.logic_micro_id_,
        (micro_block_desc.logic_micro_id_.is_valid() ? micro_block_desc.header_->data_checksum_ : 0));
    if ((macro_cnt > 0)
        && (micro_block_desc.macro_id_ == hot_macro_infos_.at(macro_cnt - 1).macro_id_)) {
      // TODO need to limit micro info count?
      if (OB_FAIL(hot_macro_infos_.at(macro_cnt - 1).push_back(micro_info))) {
        STORAGE_LOG(WARN, "failed to push back micro info", KR(ret), K(micro_info));
      }
    } else {
      ObHotMacroInfo macro_info;
      macro_info.macro_id_ = micro_block_desc.macro_id_;
      if (macro_cnt >= 8) {
        if (OB_FAIL(writer.append(hot_macro_infos_))) {
          STORAGE_LOG(WARN, "failed to write info", KR(ret), K(micro_info));
        } else {
          hot_macro_infos_.reuse();
        }
      }
      if (FAILEDx(macro_info.push_back(micro_info))) {
        STORAGE_LOG(WARN, "failed to push back micro info", KR(ret), K(micro_info));
      } else if (OB_FAIL(hot_macro_infos_.push_back(macro_info))) {
        STORAGE_LOG(WARN, "failed to push back macro info", KR(ret), K(micro_info));
      }
    }
  }
  return ret;
}

/***************************ObHotTabletInfoIndex**********************/
OB_SERIALIZE_MEMBER(ObHotTabletInfoIndex, sizes_);

ObHotTabletInfoIndex::ObHotTabletInfoIndex() : sizes_()
{
  sizes_.set_attr(ObMemAttr(MTL_ID(), MAJOR_COMPACTION_PREWARM_TAG));
}

void ObHotTabletInfoIndex::reset()
{
  sizes_.reuse();
}

int64_t ObHotTabletInfoIndex::get_total_size() const
{
  int64_t total_size = 0;
  const int64_t cnt = sizes_.count();
  for (int64_t i = 0; i < cnt; ++i) {
    total_size += sizes_.at(i);
  }
  return total_size;
}


/***************************ObHotTabletInfoBaseWriter**********************/
ObHotTabletInfoBaseWriter::ObHotTabletInfoBaseWriter(
    const int64_t tablet_id,
    const int64_t compaction_scn,
    const SCN &reorganization_scn)
  : hot_tablet_info_(), hot_micro_cnt_(0), index_(), tablet_id_(tablet_id),
    compaction_scn_(compaction_scn), reorganization_scn_(reorganization_scn), object_handle_(),
    last_serialize_size_(0), device_handle_(nullptr), fd_(), is_opened_(false),
    lock_(ObLatchIds::MC_PREWARM_LOCK)
{
  int ret = OB_SUCCESS;
  // check if current tablet is hot tablet, which will affect following logic:
  // 1. hot tablet no need to write MAJOR_PREWARM_DATA, as hot tablet will be prewarmed by iterating macro_info.
  // 2. !hot tablet need to write MAJOR_PREWARM_DATA, as !hot tablet will be prewarmed according to MAJOR_PREWARM_DATA.
  if (OB_FAIL(ObSSObjectAccessUtil::get_tablet_policy_status(tablet_id_, policy_status_))) {
    LOG_WARN("fail to get tablet policy status", KR(ret), K(tablet_id));
    policy_status_ = PolicyStatus::AUTO;
  }
}

ObHotTabletInfoBaseWriter::~ObHotTabletInfoBaseWriter()
{
  if (is_opened_ && OB_NOT_NULL(device_handle_)) {
    close_device_handle_and_fd();
  }
}

int ObHotTabletInfoBaseWriter::append(const ObIArray<ObHotMacroInfo> &hot_macro_infos)
{
  int ret = OB_SUCCESS;
  if (policy_status_ == PolicyStatus::HOT || policy_status_ == PolicyStatus::MACRO_HOT) { // 1. hot tablet
    // hot tablet no need to write MAJOR_PREWARM_DATA, as hot tablet will be prewarmed by iterating macro_info.
  } else { // 2. !hot tablet
    ObMutexGuard guard(lock_);
    if (OB_FAIL(::append(hot_tablet_info_.hot_macro_infos_, hot_macro_infos))) {
      LOG_WARN("fail to append hot macro infos", KR(ret));
    } else {
      const int64_t hot_macro_info_cnt = hot_macro_infos.count();
      for (int64_t i = 0; (i < hot_macro_info_cnt); ++i) {
        hot_micro_cnt_ += hot_macro_infos.at(i).hot_micro_infos_.count();
      }
      if (hot_micro_cnt_ >= FLUSH_THRESHOLD) {
        if (OB_FAIL(upload_hot_tablet_info())) {
          LOG_WARN("fail to upload hot tablet info", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObHotTabletInfoBaseWriter::complete()
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObHotTabletInfoBaseWriter::complete");
  ObBackupIoAdapter io_adapter;
  // if there exists residual hot tablet info, upload these residual hot tablet info
  if ((hot_micro_cnt_ > 0) && OB_FAIL(upload_hot_tablet_info())) {
    LOG_WARN("fail to upload hot tablet info", KR(ret));
  }
  time_guard.click("upload_hot_tablet_info");
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(wait_if_need())) {
    LOG_WARN("fail to wait if need", KR(ret), K_(tablet_id), K_(compaction_scn));
  }
  time_guard.click("wait_if_need");
  if (OB_FAIL(ret)) {
  } else if (OB_NOT_NULL(device_handle_)
             && (OB_FAIL(io_adapter.complete(*device_handle_, fd_)))) {
    LOG_WARN("fail to complete", KR(ret), KP_(device_handle), K_(fd));
  }
  time_guard.click("complete");
  if (OB_FAIL(ret) && OB_NOT_NULL(device_handle_)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(io_adapter.abort(*device_handle_, fd_))) {
      LOG_WARN("fail to abort", KR(tmp_ret), KP_(device_handle), K_(fd));
    }
  }
  time_guard.click("abort");
  // when hot tablet info does not exist, both prewarm_data and prewarm_index files are not uploaded.
  // when hot tablet info exist, both prewarm_data and prewarm_index files are uploaded.
  // here, if there exists hot tablet info index, upload these hot tablet info index.
  if (OB_SUCC(ret) && (index_.sizes_.count() > 0) && OB_FAIL(upload_hot_tablet_info_index())) {
    LOG_WARN("fail to upload hot tablet info index", KR(ret));
  }
  time_guard.click("upload_hot_tablet_info_index");
  close_device_handle_and_fd();
  time_guard.click("close_device_handle_and_fd");
  LOG_INFO("hot tablet info writer complete", KR(ret), K_(tablet_id), K_(compaction_scn), K(time_guard));
  return ret;
}

int ObHotTabletInfoBaseWriter::upload_hot_tablet_info()
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObHotTabletInfoBaseWriter::upload_hot_tablet_info");
  // if previous multipart upload failed, ignore ret and go on processing next multipart upload
  IGNORE_RETURN wait_if_need();
  time_guard.click("wait_if_need");

  ObBackupIoAdapter io_adapter;
  int64_t pos = 0;
  ObMemAttr attr(MTL_ID(), MAJOR_COMPACTION_PREWARM_TAG);
  ObArenaAllocator allocator(attr);
  ObFileCommonHeader header;
  const int64_t header_length = header.get_serialize_size();
  const int64_t payload_length = hot_tablet_info_.get_serialize_size();
  const int64_t buf_len = header_length + payload_length;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(buf_len));
  } else if (OB_FAIL(ObFileCommonHeaderUtil::serialize<ObHotTabletInfo>(hot_tablet_info_,
                                             ObHotTabletInfo::OB_HOT_TABLET_INFO_VERSION,
                                             buf, buf_len, pos))) {
    LOG_WARN("fail to serialize hot tablet info", KR(ret), K_(hot_tablet_info), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(pos != buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pos", KR(ret), K(pos), K(buf_len));
  } else if (!is_opened_ && OB_FAIL(open_device_handle_and_fd())) {
    LOG_WARN("fail to open device handle and fd", KR(ret));
  } else if (!is_opened_ || OB_ISNULL(device_handle_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("device handle is not opened or is null", KR(ret), K_(is_opened), KP_(device_handle));
  } else {
    const int64_t offset = index_.get_total_size();
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(get_data_object_type(), tablet_id_, compaction_scn_,
                                         reorganization_scn_.get_val_for_sql());
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle_))) {
      LOG_WARN("fail to alloc object", KR(ret));
    } else if (OB_FAIL(io_adapter.async_upload_data(*device_handle_, fd_, buf, offset, buf_len,
                                                    object_handle_.get_io_handle()))) {
      LOG_WARN("fail to async upload data", KR(ret), KP_(device_handle), K_(fd), KP(buf), K(offset), K(buf_len));
    } else {
      last_serialize_size_ = buf_len;
      // clear hot micro infos, which are already serialized into buf
      hot_tablet_info_.reset();
      hot_micro_cnt_ = 0;
    }
  }
  time_guard.click("async_upload_data");

  abort_on_fail(ret);
  time_guard.click("abort_on_fail");
  LOG_INFO("finish to upload hot tablet info", KR(ret), K_(tablet_id), K_(compaction_scn), K(buf_len), K(time_guard));
  return ret;
}

int ObHotTabletInfoBaseWriter::write_hot_tablet_info_index(char *buf, const int64_t buf_len, ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  object_handle.reset();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = buf;
    write_info.offset_ = 0;
    write_info.size_ = buf_len;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    ObSSObjectStorageWriter ss_object_storage_writer;
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(get_index_object_type(), tablet_id_, compaction_scn_,
                                        reorganization_scn_.get_val_for_sql());
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
      LOG_WARN("fail to alloc object", KR(ret), K(storage_opt));
    } else if (OB_FAIL(ss_object_storage_writer.aio_write(write_info, object_handle))) {
      LOG_WARN("fail to aio write", KR(ret), K(write_info), K(object_handle));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("fail to wait", KR(ret));
    }
  }
  return ret;
}

int ObHotTabletInfoBaseWriter::upload_hot_tablet_info_index()
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMemAttr attr(MTL_ID(), MAJOR_COMPACTION_PREWARM_TAG);
  ObArenaAllocator allocator(attr);
  ObFileCommonHeader header;
  const int64_t header_length = header.get_serialize_size();
  const int64_t payload_length = index_.get_serialize_size();
  const int64_t buf_len = header_length + payload_length;
  char *buf = nullptr;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret), K(buf_len));
  } else if (OB_FAIL(ObFileCommonHeaderUtil::serialize<ObHotTabletInfoIndex>(index_,
                                             ObHotTabletInfoIndex::OB_HOT_TABLET_INFO_INDEX_VERSION,
                                             buf, buf_len, pos))) {
    LOG_WARN("fail to serialize hot tablet info", KR(ret), K_(index), KP(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(pos != buf_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected pos", KR(ret), K(pos), K(buf_len));
  } else {
    ObStorageObjectHandle object_handle;
    ObTenantFileManager *tenant_file_manager = MTL(ObTenantFileManager*);
    if (OB_FAIL(write_hot_tablet_info_index(buf, buf_len, object_handle))) {
      LOG_WARN("fail to write hot tablet info index", KR(ret));
      ret = OB_SUCCESS; // ignore ret
      // maybe retry major compation multiple times, and write hot tablet info index multiple times(which is
      // generated by multiple compaction threads concurrently, and thus may be different).
      // delete remote file, retry write hot tablet info index.
      if (OB_ISNULL(tenant_file_manager)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant file manager is null", KR(ret), KP(tenant_file_manager));
      } else if (OB_FAIL(tenant_file_manager->delete_remote_file(object_handle.get_macro_id()))) {
        LOG_WARN("fail to delete remote file", KR(ret), "file_id", object_handle.get_macro_id());
      } else if (OB_FAIL(write_hot_tablet_info_index(buf, buf_len, object_handle))) {
        LOG_WARN("fail to write hot tablet info index again", KR(ret));
      }
    }
  }
  LOG_INFO("finish to upload hot tablet info index", KR(ret), K_(tablet_id), K_(compaction_scn), K(buf_len));
  return ret;
}

int ObHotTabletInfoBaseWriter::open_device_handle_and_fd()
{
  int ret = OB_SUCCESS;
  ObSSIOCommonOp ss_io_common_op;
  ObIOInfo io_info;
  io_info.tenant_id_ = MTL_ID();
  ObStorageObjectHandle object_handle;
  ObStorageObjectOpt storage_opt;
  storage_opt.set_ss_major_prewarm_opt(get_data_object_type(), tablet_id_, compaction_scn_,
                                       reorganization_scn_.get_val_for_sql());
  if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
    LOG_WARN("fail to alloc object", KR(ret));
  } else if (OB_FAIL(ss_io_common_op.get_object_device_and_fd(
                     ObStorageAccessType::OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER,
                     object_handle.get_macro_id(), 0/*ls_epoch_id*/, io_info))) {
    LOG_WARN("fail to get object device and fd", KR(ret), "macro_id", object_handle.get_macro_id());
  } else {
    device_handle_ = io_info.fd_.device_handle_;
    fd_ = io_info.fd_;
    is_opened_ = true;
  }
  return ret;
}

void ObHotTabletInfoBaseWriter::close_device_handle_and_fd()
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(is_opened_)) {
    ObBackupIoAdapter io_adapter;
    if (OB_FAIL(io_adapter.close_device_and_fd(device_handle_, fd_))) {
      LOG_WARN("fail to close device and fd", KR(ret), KP_(device_handle), K_(fd));
    }
    device_handle_ = nullptr;
    fd_.reset();
    is_opened_ = false;
  }
}

int ObHotTabletInfoBaseWriter::wait_if_need()
{
  int ret = OB_SUCCESS;
  if (object_handle_.is_valid()) {
    // wait multipart upload
    if (OB_FAIL(object_handle_.wait())) {
      LOG_WARN("fail to wait", KR(ret), K_(tablet_id), K_(compaction_scn));
    } else if (OB_FAIL(index_.sizes_.push_back(last_serialize_size_))) {
      LOG_WARN("fail to push back", KR(ret), K_(last_serialize_size));
    } else {
      object_handle_.reset();
    }

    abort_on_fail(ret);
  }
  return ret;
}

void ObHotTabletInfoBaseWriter::abort_on_fail(const int ret)
{
  if (OB_SUCCESS != ret) {
    int tmp_ret = OB_SUCCESS;
    ObBackupIoAdapter io_adapter;
    if (OB_NOT_NULL(device_handle_) && OB_TMP_FAIL(io_adapter.abort(*device_handle_, fd_))) {
      LOG_WARN("fail to abort", KR(tmp_ret), KP_(device_handle), K_(fd));
    }
    close_device_handle_and_fd();
    index_.reset();
    object_handle_.reset();
  }
}


/***************************ObHotTabletInfoWriter**********************/
int ObHotTabletInfoWriter::complete()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_TMP_FAIL(data_writer_.complete())) {
    ret = (OB_SUCC(ret) ? tmp_ret : ret);
    LOG_WARN("fail to complete data writer", KR(ret), KR(tmp_ret), K_(data_writer));
  }
  if (OB_TMP_FAIL(meta_writer_.complete())) {
    ret = (OB_SUCC(ret) ? tmp_ret : ret);
    LOG_WARN("fail to complete meta writer", KR(ret), KR(tmp_ret), K_(meta_writer));
  }
  return ret;
}


/***************************ObHotTabletInfoReader**********************/
ObHotTabletInfoReader::ObHotTabletInfoReader(
    const int64_t compaction_scn,
    const share::SCN &reorganization_scn,
    const ObTabletHandle &tablet_handle,
    const blocksstable::ObSSTable &sstable)
  : tablet_id_(ObTabletID::INVALID_TABLET_ID), compaction_scn_(compaction_scn),
    reorganization_scn_(reorganization_scn), tablet_handle_(tablet_handle), sstable_(sstable),
    micro_buf_allocator_(), object_handles_(), write_handles_(),
    handle_idx_(-1), write_handle_idx_(-1),
    read_parallelism_(AGGREGATED_READ_PARALLELISM_MAX), aggregated_read_cnt_(0), prewarmed_macro_cnt_(0), skipped_macro_cnt_(0),
    fail_wait_handle_cnt_(0)
{
  micro_buf_allocator_.set_attr(ObMemAttr(MTL_ID(), MAJOR_COMPACTION_PREWARM_TAG));
}

ObHotTabletInfoReader::~ObHotTabletInfoReader()
{
  reset_io_resources();
}

int ObHotTabletInfoReader::load_hot_macro_infos()
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  ObTablet *tablet = nullptr;
  if (OB_ISNULL(tablet = tablet_handle_.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", KR(ret));
  } else if (FALSE_IT(tablet_id_ = tablet->get_tablet_id().id())) {
  } else if (OB_UNLIKELY(ObTabletID::INVALID_TABLET_ID == tablet_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid tablet id", KR(ret));
  } else if (OB_FAIL(handle_hot_tablet())) {
    LOG_WARN("fail to prewarm tablet", KR(ret), K(tablet_id_));
  }
  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  LOG_INFO("finish to load hot macro infos", KR(ret), K(cost_us), K_(tablet_id), K_(compaction_scn),
           K_(prewarmed_macro_cnt), K_(skipped_macro_cnt), K_(fail_wait_handle_cnt));
  return ret;
}

int ObHotTabletInfoReader::handle_hot_tablet()
{
  int ret = OB_SUCCESS;
  bool is_stop_prewarm = false;
  if (!sstable_.is_co_sstable()) {
    if (OB_FAIL(prewarm_sstable_(sstable_))) {
      LOG_WARN("fail to prewarm sstable", KR(ret), K(tablet_id_));
    }
  } else if (OB_FAIL(check_if_stop_prewarm(is_stop_prewarm))) {
    LOG_WARN("fail to check if stop CO sstable prewarm", KR(ret), K(tablet_id_));
  } else if (is_stop_prewarm) {
    LOG_INFO("skip CO sstable prewarm", K(tablet_id_));
  } else {
    ObArray<ObSSTableWrapper> cg_table_wrappers;
    cg_table_wrappers.set_attr(ObMemAttr(MTL_ID(), "MCPrewarmCG"));
    const ObCOSSTableV2 &co_sstable = static_cast<const ObCOSSTableV2 &>(sstable_);
    if (OB_FAIL(co_sstable.get_all_tables(cg_table_wrappers))) {
      LOG_WARN("fail to get all CO sstables", KR(ret), K(tablet_id_));
    }
    const int64_t cg_count = cg_table_wrappers.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_count; ++i) {
      ObSSTableWrapper &cg_wrapper = cg_table_wrappers.at(i);
      ObSSTable *cg_sstable = cg_wrapper.get_sstable();
      if (OB_ISNULL(cg_sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("CG sstable is null", KR(ret), K(i), K(cg_count), K(tablet_id_), KP(cg_sstable));
      } else {
        const bool is_top_level_sstable = (cg_sstable == &sstable_);
        if (is_top_level_sstable) {
          // The top-level SSTable can load its meta through get_meta() without a wrapper meta handle.
        } else if (OB_FAIL(cg_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
          LOG_WARN("fail to get loaded CG sstable", KR(ret), K(i), K(cg_count), K(tablet_id_));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_ISNULL(cg_sstable)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("loaded CG sstable is null", KR(ret), K(i), K(cg_count), K(tablet_id_), KP(cg_sstable));
        } else if (OB_UNLIKELY(!cg_sstable->is_column_store_sstable())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected CG sstable", KR(ret), K(i), K(cg_count), K(tablet_id_),
              K(cg_sstable->get_key()));
        } else if (OB_FAIL(prewarm_sstable_(*cg_sstable))) {
          LOG_WARN("fail to prewarm CG sstable", KR(ret), K(i), K(cg_count), K(tablet_id_),
              K(cg_sstable->get_key()));
        }
      }
    }
    LOG_INFO("finish to prewarm CO sstables", KR(ret), K(tablet_id_), K(cg_count));
  }
  return ret;
}

int ObHotTabletInfoReader::prewarm_sstable_(const blocksstable::ObSSTable &sstable)
{
  int ret = OB_SUCCESS;
  bool is_canceled = false;
  ObSSTableMacroPrewarmer prewarmer(
      &sstable, is_canceled, ObSSLocalCachePrewarmType::MAJOR_COMPACTION);
  if (OB_FAIL(prewarmer.do_prewarm(tablet_handle_))) {
    LOG_WARN("fail to do prewarm", KR(ret));
  }
  return ret;
}

int ObHotTabletInfoReader::handle_non_hot_tablet()
{
  int ret = OB_SUCCESS;
  bool is_stop_prewarm = false;
  if (OB_FAIL(check_if_stop_prewarm(is_stop_prewarm))) {
    LOG_WARN("fail to check if stop prewarm", KR(ret));
  } else if (is_stop_prewarm) {
    LOG_INFO("stop prewarm", K_(tablet_id), K_(compaction_scn));
  } else if (OB_FAIL(load_hot_macro_infos(ObStorageObjectType::MAJOR_PREWARM_META,
                                          ObStorageObjectType::MAJOR_PREWARM_META_INDEX))) {
    LOG_WARN("fail to load hot macro infos", KR(ret));
  } else if (OB_FAIL(load_hot_macro_infos(
      ObStorageObjectType::MAJOR_PREWARM_DATA,
      ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX))) {
    LOG_WARN("fail to load hot macro infos", KR(ret));
  }
  return ret;
}

int ObHotTabletInfoReader::load_hot_macro_infos(
    const ObStorageObjectType data_object_type,
    const ObStorageObjectType index_object_type)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  ObHotTabletInfoIndex index;
  if (OB_UNLIKELY(!(((ObStorageObjectType::MAJOR_PREWARM_DATA == data_object_type) &&
                    (ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX == index_object_type)) ||
                   ((ObStorageObjectType::MAJOR_PREWARM_META == data_object_type) &&
                    (ObStorageObjectType::MAJOR_PREWARM_META_INDEX == index_object_type))))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid object type", KR(ret), "storage object type", STI(data_object_type), "storage object type", STI(index_object_type));
  } else if (OB_FAIL(check_if_index_file_exist(index_object_type, is_exist))) {
    LOG_WARN("fail to check if index file exist", KR(ret), "storage object type", STI(index_object_type));
  } else if (!is_exist) {
    // do nothing
    LOG_INFO("index file not exist", K_(tablet_id), K_(compaction_scn));
  } else if (OB_FAIL(read_index_file(index_object_type, index))) {
    LOG_WARN("fail to read index file", KR(ret), K_(tablet_id), K_(compaction_scn), "storage object type", STI(index_object_type));
  } else if (OB_FAIL(read_data_file(data_object_type, index))) {
    LOG_WARN("fail to read data file", KR(ret), K_(tablet_id), K_(compaction_scn), "storage object type", STI(data_object_type));
  }
  return ret;
}

bool ObHotTabletInfoReader::is_vaild_data_object_type(
    const ObStorageObjectType data_object_type) const
{
  return ((ObStorageObjectType::MAJOR_PREWARM_DATA == data_object_type) ||
          (ObStorageObjectType::MAJOR_PREWARM_META == data_object_type));
}

bool ObHotTabletInfoReader::is_vaild_index_object_type(
    const ObStorageObjectType index_object_type) const
{
  return ((ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX == index_object_type) ||
          (ObStorageObjectType::MAJOR_PREWARM_META_INDEX == index_object_type));
}

int ObHotTabletInfoReader::check_if_index_file_exist(
    const ObStorageObjectType object_type,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_UNLIKELY(!is_vaild_index_object_type(object_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), "storage object type", STI(object_type));
  } else {
    ObStorageObjectHandle object_handle;
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(object_type, tablet_id_, compaction_scn_,
                                         reorganization_scn_.get_val_for_sql());
    ObTenantFileManager *file_manager = nullptr;
    if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
      LOG_WARN("fail to alloc object", KR(ret));
    } else if (OB_ISNULL(file_manager = MTL(ObTenantFileManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("file manager is null", KR(ret), "tenant_id", MTL_ID());
    } else if (OB_FAIL(file_manager->is_exist_file(object_handle.get_macro_id(), 0/*ls_epoch_id*/, is_exist))) {
      LOG_WARN("fail to check is exist file", KR(ret), "macro_id", object_handle.get_macro_id());
    }
  }
  return ret;
}

int ObHotTabletInfoReader::read_index_file(
    const ObStorageObjectType object_type,
    ObHotTabletInfoIndex &index)
{
  int ret = OB_SUCCESS;
  index.reset();
  char *buf = nullptr;
  ObMemAttr attr(MTL_ID(), MAJOR_COMPACTION_PREWARM_TAG);
  ObArenaAllocator allocator(attr);
  ObStorageObjectHandle object_handle;
  ObStorageObjectOpt storage_opt;
  if (OB_UNLIKELY(!is_vaild_index_object_type(object_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), "storage object type", STI(object_type));
  } else if (FALSE_IT(storage_opt.set_ss_major_prewarm_opt(object_type, tablet_id_, compaction_scn_,
                                                           reorganization_scn_.get_val_for_sql()))) {
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(ObHotTabletInfoBaseWriter::INDEX_BUF_SIZE)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), LITERAL_K(ObHotTabletInfoBaseWriter::INDEX_BUF_SIZE));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
    LOG_WARN("fail to alloc object", KR(ret), K(storage_opt));
  } else {
    int64_t pos = 0;
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = object_handle.get_macro_id();
    read_info.buf_ = buf;
    read_info.offset_ = 0;
    read_info.size_ = ObHotTabletInfoBaseWriter::INDEX_BUF_SIZE;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.mtl_tenant_id_ = MTL_ID();
    ObSSObjectStorageReader ss_object_storage_reader;
    if (OB_FAIL(ss_object_storage_reader.aio_read(read_info, object_handle))) {
      LOG_WARN("fail to aio read", KR(ret), K(read_info), K(object_handle));
    } else if (OB_FAIL(object_handle.wait())) {
      LOG_WARN("fail to wait", KR(ret));
    } else if (OB_FAIL(ObFileCommonHeaderUtil::deserialize<ObHotTabletInfoIndex>(buf,
                       object_handle.get_data_size(), pos, index))) {
      LOG_WARN("fail to deserialize index", KR(ret), "data_size", object_handle.get_data_size(), K(pos));
    }
  }
  LOG_INFO("finish to read index file", KR(ret), "storage object type", STI(object_type),
           K_(tablet_id), K_(compaction_scn), K(index));
  return ret;
}

int ObHotTabletInfoReader::read_data_file(
    const ObStorageObjectType object_type,
    const ObHotTabletInfoIndex &index)
{
  int ret = OB_SUCCESS;
  const int64_t start_us = ObTimeUtility::current_time();
  int64_t cur_offset = 0;
  const int64_t cnt = index.sizes_.count();
  int64_t total_micro_cnt = 0;
  int64_t physic_micro_cnt = 0;
  int64_t logic_micro_cnt = 0;
  bool is_stop_prewarm = false;
  if (OB_UNLIKELY(!is_vaild_data_object_type(object_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid object type", KR(ret), "storage object type", STI(object_type));
  }
  for (int64_t i = 0; (i < cnt) && !is_stop_prewarm && OB_SUCC(ret); ++i) {
    const int64_t size = index.sizes_.at(i);
    ObHotTabletInfo hot_tablet_info;
    char *buf = nullptr;
    ObMemAttr attr(MTL_ID(), MAJOR_COMPACTION_PREWARM_TAG);
    ObArenaAllocator allocator(attr);
    ObStorageObjectHandle object_handle;
    ObStorageObjectOpt storage_opt;
    storage_opt.set_ss_major_prewarm_opt(object_type, tablet_id_, compaction_scn_,
                                         reorganization_scn_.get_val_for_sql());
    if (OB_FAIL(check_if_stop_prewarm(is_stop_prewarm))) {
      LOG_WARN("fail to check if stop prewarm", KR(ret));
    } else if (is_stop_prewarm) {
      LOG_INFO("stop prewarm", K_(tablet_id), K_(compaction_scn));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(size));
    } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.alloc_object(storage_opt, object_handle))) {
      LOG_WARN("fail to alloc object", KR(ret), K(storage_opt));
    } else {
      int64_t pos = 0;
      ObStorageObjectReadInfo read_info;
      read_info.macro_block_id_ = object_handle.get_macro_id();
      read_info.buf_ = buf;
      read_info.offset_ = cur_offset;
      read_info.size_ = size;
      read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
      read_info.mtl_tenant_id_ = MTL_ID();
      ObSSObjectStorageReader ss_object_storage_reader;
      int64_t tmp_total_micro_cnt = 0;
      int64_t tmp_physic_micro_cnt = 0;
      int64_t tmp_logic_micro_cnt = 0;
      if (OB_FAIL(ss_object_storage_reader.aio_read(read_info, object_handle))) {
        LOG_WARN("fail to aio write", KR(ret), K(read_info), K(object_handle));
      } else if (OB_FAIL(object_handle.wait())) {
        LOG_WARN("fail to wait", KR(ret));
      } else if (OB_FAIL(ObFileCommonHeaderUtil::deserialize<ObHotTabletInfo>(buf, size, pos, hot_tablet_info))) {
        LOG_WARN("fail to deserialize hot tablet info", KR(ret), KP(buf), K(size), K(pos));
      } else if (OB_FAIL(load_hot_macro_block_data(hot_tablet_info, tmp_total_micro_cnt,
          tmp_physic_micro_cnt, tmp_logic_micro_cnt, is_stop_prewarm))) {
        LOG_WARN("fail to load hot macro block data", KR(ret), K(hot_tablet_info));
      }
      
      if (OB_SUCC(ret)) {
        total_micro_cnt += tmp_total_micro_cnt;
        physic_micro_cnt += tmp_physic_micro_cnt;
        logic_micro_cnt += tmp_logic_micro_cnt;
      }
    }
    cur_offset += size;
    // OB_SERVER_OUTOF_DISK_SPACE means should stop prewarm.
    if (OB_SERVER_OUTOF_DISK_SPACE != ret) {
      ret = OB_SUCCESS; // ignore ret, so as to process next part of data_file
    }
  }

  if (OB_SERVER_OUTOF_DISK_SPACE == ret) {
    ret = OB_SUCCESS; // ignore ret of OB_SERVER_OUTOF_DISK_SPACE, which is by-design
  }

  const int64_t cost_us = ObTimeUtility::current_time() - start_us;
  FLOG_INFO("finish to read data file", KR(ret), K(cost_us), "storage object type", STI(object_type),
            K_(tablet_id), K_(compaction_scn), K(total_micro_cnt), K(physic_micro_cnt),
            K(logic_micro_cnt), K_(prewarmed_macro_cnt), K_(skipped_macro_cnt),
            K_(fail_wait_handle_cnt));
  return ret;
}

int ObHotTabletInfoReader::get_micro_count_from_tablet_info_(
    const ObHotTabletInfo &hot_tablet_info,
    int64_t &total_micro_cnt,
    int64_t &physic_micro_cnt,
    int64_t &logic_micro_cnt)
{
  int ret = OB_SUCCESS;
  total_micro_cnt = 0;
  physic_micro_cnt = 0;
  logic_micro_cnt = 0;
  const ObHotTabletInfo::HotMacroInfoArr &hot_macro_infos = hot_tablet_info.hot_macro_infos_;
  const int64_t hot_macro_cnt = hot_macro_infos.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < hot_macro_cnt; ++i) {
    const ObHotMacroInfo::HotMicroInfoArr &cur_hot_micro_infos =
        hot_macro_infos.at(i).hot_micro_infos_;
    const int64_t cur_hot_micro_info_cnt = cur_hot_micro_infos.count();
    total_micro_cnt += cur_hot_micro_info_cnt;
    for (int64_t j = 0; OB_SUCC(ret) && j < cur_hot_micro_info_cnt; ++j) {
      if (cur_hot_micro_infos.at(j).logic_micro_id_.is_valid()) {
        logic_micro_cnt++;
      } else {
        physic_micro_cnt++;
      }
    }
  }
  return ret;
}

int ObHotTabletInfoReader::load_hot_macro_block_data(
    const ObHotTabletInfo &hot_tablet_info,
    int64_t &total_micro_cnt,
    int64_t &physic_micro_cnt,
    int64_t &logic_micro_cnt,
    bool &is_stop_prewarm)
{
  int ret = OB_SUCCESS;
  is_stop_prewarm = false;
  if (OB_FAIL(get_micro_count_from_tablet_info_(
      hot_tablet_info, total_micro_cnt, physic_micro_cnt, logic_micro_cnt))) {
    LOG_WARN("fail to get micro count from tablet info", KR(ret),
        K(hot_tablet_info));
  }

  const int64_t hot_macro_info_cnt = hot_tablet_info.hot_macro_infos_.count();
  for (int64_t i = 0; (i < hot_macro_info_cnt) && !is_stop_prewarm && OB_SUCC(ret); ++i) {
    const ObHotMacroInfo &hot_macro_info = hot_tablet_info.hot_macro_infos_.at(i);
    bool has_space = false;
    if (OB_FAIL(check_if_stop_prewarm(is_stop_prewarm))) {
      LOG_WARN("fail to check if stop prewarm", KR(ret));
    } else if (is_stop_prewarm) {
      LOG_INFO("stop prewarm", K_(tablet_id), K_(compaction_scn));
    } else if (OB_FAIL(check_if_macro_cache_prewarm_space_enough(has_space))) {
      LOG_WARN("fail to check macro cache prewarm space", KR(ret));
    } else if (!has_space) {
      skipped_macro_cnt_ += hot_macro_info_cnt - i;
      is_stop_prewarm = true;
      LOG_INFO("stop major compaction prewarm because macro cache reaches prewarm threshold",
               K_(tablet_id), K_(compaction_scn), K(i), K(hot_macro_info_cnt));
    } else if (OB_FAIL(async_read_macro_block(hot_macro_info))) {
      LOG_WARN("fail to async read macro block", KR(ret), K(hot_macro_info));
    }
    if (TC_REACH_TIME_INTERVAL(60 * 1000 * 1000L)) { // 1min
      FLOG_INFO("loading hot macro block data", KR(ret), K_(tablet_id), K_(compaction_scn),
               K(total_micro_cnt), K(physic_micro_cnt), K(logic_micro_cnt),
               K_(read_parallelism), K_(prewarmed_macro_cnt), K_(skipped_macro_cnt));
    }
    ret = OB_SUCCESS; // ignore ret, so as to process next hot macro info
  }

  // wait residual obejct handles
  if (OB_SUCC(ret)) {
    if ((handle_idx_ >= 0) && OB_FAIL(wait_all_object_handles())) {
      LOG_WARN("fail to wait all object handles", KR(ret), K_(handle_idx));
    }
  }
  return ret;
}

int ObHotTabletInfoReader::async_write_hot_policy_macro_into_local_cache(ObStorageObjectHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_handle.is_valid() || !read_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(read_handle));
  } else {
    PrewarmLocalCacheType ss_prewarm_local_cache_type = PrewarmLocalCacheType::SS_PREWARM_MAX_TYPE;
    check_prewarm_cache_type(read_handle.get_macro_id(), read_handle.get_buffer(),
        read_handle.get_data_size(), false/*is_hot_tablet*/, ss_prewarm_local_cache_type);
    if (ObSSPrewarmLocalCacheType::is_skipped(ss_prewarm_local_cache_type)) {
      // do nothing
    } else if (ObSSPrewarmLocalCacheType::is_macro_cache(ss_prewarm_local_cache_type)) {
      if (OB_FAIL(async_write_hot_policy_macro_(read_handle))) {
        LOG_WARN("fail to async write hot policy macro", KR(ret), K(read_handle));
      }
    }
  }
  return ret;
}

int ObHotTabletInfoReader::async_write_hot_policy_macro_(ObStorageObjectHandle &read_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!read_handle.is_valid() || !read_handle.is_finished())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(read_handle));
  } else {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = read_handle.get_buffer();
    write_info.offset_ = 0;
    write_info.size_ = read_handle.get_data_size();
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000L;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.set_is_write_cache(false); // prewarm macro is read cache
    write_info.set_effective_tablet_id(tablet_id_);

    write_handle_idx_ = (write_handle_idx_ + 1) % TOTAL_READ_PARALLELISM;
    if (OB_UNLIKELY(write_handles_[write_handle_idx_].is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret),
          K(write_handle_idx_), K(write_handles_[write_handle_idx_]), K(read_handle));
    } else if (OB_FAIL(ObSSObjectAccessUtil::async_write_file_to_macro_cache(
                       true/*use_effective_tablet_id*/, read_handle.get_macro_id(),
                       write_info, write_handles_[write_handle_idx_]))) {
      LOG_WARN("fail to async write local file", KR(ret),
          K(write_info), K(write_handle_idx_), K(read_handle));
    }
  }
  return ret;
}

int ObHotTabletInfoReader::async_read_macro_block(
    const ObHotMacroInfo &hot_macro_info)
{
  int ret = OB_SUCCESS;
  const int64_t macro_block_size = OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  char *buf = nullptr;
  const MacroBlockId &macro_id = hot_macro_info.macro_id_;
  if (OB_UNLIKELY(!macro_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro id", KR(ret), K(macro_id));
  } else if (OB_ISNULL(buf = static_cast<char *>(micro_buf_allocator_.alloc(macro_block_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", KR(ret), K(macro_block_size), K(macro_id));
  } else {
    ObStorageObjectReadInfo read_info;
    read_info.macro_block_id_ = macro_id;
    read_info.offset_ = 0;
    read_info.size_ = macro_block_size;
    read_info.io_desc_.set_wait_event(ObWaitEventIds::OBJECT_STORAGE_READ);
    read_info.buf_ = buf;
    read_info.mtl_tenant_id_ = MTL_ID();
    read_info.io_timeout_ms_ = OB_IO_MANAGER.get_object_storage_io_timeout_ms(read_info.mtl_tenant_id_);

    ObSSObjectStorageReader object_storage_reader;
    handle_idx_ = (handle_idx_ + 1) % TOTAL_READ_PARALLELISM;
    if (object_handles_[handle_idx_].is_valid()) { // defence
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("object handle already in use", KR(ret), K_(handle_idx));
    } else if (FALSE_IT(++aggregated_read_cnt_)) {
    } else if (OB_FAIL(object_storage_reader.aio_read(read_info, object_handles_[handle_idx_]))) {
      LOG_WARN("fail to aio read whole macro block", KR(ret), K(read_info));
    }

    if (OB_FAIL(ret)) {
      reset_io_resources();
    } else if (read_parallelism_ == aggregated_read_cnt_) {
      if (OB_FAIL(wait_all_object_handles())) {
        LOG_WARN("fail to wait all object handles", KR(ret));
      }
    }
  }
  return ret;
}

int ObHotTabletInfoReader::wait_all_object_handles()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; (i <= handle_idx_) && OB_SUCC(ret); ++i) {
    if (object_handles_[i].is_empty()) {
      // do nothing.
    } else if (OB_FAIL(object_handles_[i].wait())) {
      fail_wait_handle_cnt_++;
      LOG_WARN("fail to wait", KR(ret), K_(fail_wait_handle_cnt));
    } else if (OB_FAIL(async_write_hot_policy_macro_into_local_cache(object_handles_[i]))) {
      LOG_WARN("fail to write macro block to local cache", KR(ret), K(i), K(object_handles_[i]));
    }
    ret = OB_SUCCESS; // ignore ret, and process next handle
  }

  if (FAILEDx(wait_all_write_handles())) {
    LOG_WARN("fail to wait all write handles", KR(ret), K(write_handle_idx_));
  }
  reset_io_resources();
  return ret;
}

int ObHotTabletInfoReader::wait_all_write_handles()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; (i <= write_handle_idx_) && OB_SUCC(ret); ++i) {
    if (write_handles_[i].is_empty()) {
      // do nothing.
    } else if (OB_FAIL(write_handles_[i].wait())) {
      fail_wait_handle_cnt_++;
      LOG_WARN("fail to wait write handle", KR(ret), K_(fail_wait_handle_cnt), K(i), K(write_handles_[i]));
    } else {
      prewarmed_macro_cnt_++;
      inc_local_cache_prewarm_stat(
          ObSSLocalCachePrewarmType::MAJOR_COMPACTION,
          1/*delta_cnt*/, write_handles_[i].get_data_size());
    }
    ret = OB_SUCCESS; // ignore ret, and process next write handle
  }
  return ret;
}

int ObHotTabletInfoReader::check_if_macro_cache_prewarm_space_enough(bool &is_enough)
{
  int ret = OB_SUCCESS;
  is_enough = false;
  ObTenantDiskSpaceManager *disk_space_mgr = nullptr;
  if (OB_ISNULL(disk_space_mgr = MTL(ObTenantDiskSpaceManager *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk space mgr is null", KR(ret));
  } else {
    const int64_t macro_cache_size = disk_space_mgr->get_unit_config_shared_macro_cache_size();
    const int64_t macro_cache_used_size = disk_space_mgr->get_shared_macro_cache_used_size();
    if (OB_UNLIKELY((macro_cache_size <= 0) || (macro_cache_used_size < 0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected macro cache size or macro cache used size", KR(ret),
               K(macro_cache_size), K(macro_cache_used_size));
    } else {
      const int64_t macro_cache_evict_threshold_size =
          macro_cache_size / 100 * ObSSMacroCacheMgr::CACHE_EVICT_THRESHOLD;
      const int64_t prewarm_reserve_size = MIN(100L * 1024L * 1024L, macro_cache_evict_threshold_size);
      const int64_t prewarm_threshold_size = macro_cache_evict_threshold_size - prewarm_reserve_size;
      is_enough = (macro_cache_used_size < prewarm_threshold_size);
      LOG_TRACE("check macro cache prewarm space", K(is_enough),
                K(macro_cache_size), K(macro_cache_used_size), K(macro_cache_evict_threshold_size),
                K(prewarm_threshold_size), K(prewarm_reserve_size));
    }
  }
  return ret;
}

int ObHotTabletInfoReader::check_if_stop_prewarm(bool &is_stop)
{
  int ret = OB_SUCCESS;
  is_stop = false;
  PolicyStatus policy_status = PolicyStatus::MAX_STATUS;
  ObSSLocalCachePrewarmService *prewarm_service = nullptr;
  if (OB_FAIL(ObSSObjectAccessUtil::get_tablet_policy_status(tablet_id_, policy_status))) {
    LOG_WARN("fail to get tablet policy status", KR(ret), K(tablet_id_));
  } else if (PolicyStatus::COLD == policy_status) {
    is_stop = true;
  } else if (PolicyStatus::HOT == policy_status || PolicyStatus::MACRO_HOT == policy_status) {
    is_stop = false;
  } else if (PolicyStatus::AUTO == policy_status) {
    if (OB_ISNULL(prewarm_service = MTL(ObSSLocalCachePrewarmService *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("local cache prewarm service is null", KR(ret), K(tablet_id_));
    } else {
      is_stop = (ObSSMajorPrewarmLevel::PREWARM_NONE_LEVEL == prewarm_service->get_major_prewarm_level());
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet policy status", KR(ret), K(tablet_id_), K(policy_status));
  }
  return ret;
}

void ObHotTabletInfoReader::reset_io_resources()
{
  for (int64_t i = 0; i < TOTAL_READ_PARALLELISM; ++i) {
    object_handles_[i].reset();
    write_handles_[i].reset();
  }
  handle_idx_ = -1;
  write_handle_idx_ = -1;
  aggregated_read_cnt_ = 0;
  micro_buf_allocator_.clear();
}


} /* namespace storage */
} /* namespace oceanbase */
