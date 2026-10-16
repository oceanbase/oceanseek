/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "lib/charset/ob_charset.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "ob_tablet_id.h"
#include "object/ob_object.h"
#include "storage/access/ob_table_read_info.h"
#include "ob_lob_locator.h"
#include "observer/ob_server.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/lob/ob_lob_manager.h"
#include "storage/lob/common/ob_lob_context.h"


namespace oceanbase
{
using namespace common;
using namespace blocksstable;
namespace storage
{

//==================== ObMemLobLocatorBuilder ====================
ObMemLobLocatorBuilder::ObMemLobLocatorBuilder()
{
  reset();
}

ObMemLobLocatorBuilder::~ObMemLobLocatorBuilder()
{
  reset();
}

void ObMemLobLocatorBuilder::reset()
{
  is_inited_ = false;
  allocator_ = nullptr;
  input_.reset(); 
  input_wrapper_.reset();
  tx_read_snapshot_ = nullptr;
  read_snapshot_ls_id_ = ObLSID::INVALID_LS_ID;
  fb_snapshot_ = share::SCN::invalid_scn();
  rowkey_.reset();
  locator_size_ = 0;
  extern_body_size_ = 0;
  disk_locator_size_ = 0;
  read_snapshot_size_ = 0;
  payload_offset_ = 0;
  payload_size_ = 0;
  has_extern_ = false;
  has_location_info_ = false;
  has_retry_info_ = false;
  has_read_snapshot_ = false;
  has_row_key_ = false;
  need_payload_ = false;
}

int ObMemLobLocatorBuilder::init(
    common::ObIAllocator &allocator,
    const common::ObString &input,
    const bool need_data)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "mem lob locator builder is init twice", KR(ret), K(input.length()), K(need_data));
  } else if (OB_FAIL(input_wrapper_.init(input.ptr(), input.length()))) {
    LOG_WARN("failed init input wrapper", KR(ret), K(input.length()), K(input.size()));
  } else if (OB_UNLIKELY(!input_wrapper_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input wrapper", KR(ret), K(input_wrapper_));
  } else if (input_wrapper_.is_outrow() && need_data) {
    need_payload_ = true;
  }

  if (OB_FAIL(ret)) {
  } else {
    allocator_ = &allocator;
    input_ = input;
    is_inited_ = true;
  }
  return ret;
}

int ObMemLobLocatorBuilder::set_extern_info(
    const uint64_t table_id, 
    const uint64_t column_idx,
    const ObCollationType &cs_type)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "mem lob locator builder is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_id(table_id) || !is_valid_id(column_idx)
    || (cs_type <= CS_TYPE_INVALID || cs_type >= CS_TYPE_MAX))) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid table info", KR(ret), K(table_id), K(column_idx));
  } else {
    extern_header_.table_id_ = table_id;
    extern_header_.column_idx_ = column_idx;
    extern_header_.cs_type_ = cs_type;
    has_extern_ = true;
  }
 
  // 若 input 是 outrow，则优先取 lob id 中的 column_idx_
  // 这里是临时的解决方案，Column Store 读路径中的 columns_index 会硬编码为 0，导致读路径失效
  if (OB_FAIL(ret)) {
  } else if (input_wrapper_.is_outrow()) {
    ObLobId lob_id;
    if (OB_FAIL(input_wrapper_.get_lob_id(lob_id))) {
      LOG_WARN("failed to get lob id from input_wrapper", KR(ret), K(input_wrapper_), KPC(this));
    } else {
      extern_header_.column_idx_ = lob_id.get_column_idx();
    } 
  }
  return ret;
}

int ObMemLobLocatorBuilder::set_location_info(
    const uint64_t tablet_id,
    const int64_t ls_id,
    const ObAddr &addr)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "mem lob locator builder is not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_id(tablet_id) || ls_id == ObLSID::INVALID_LS_ID || !addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid lob location info", KR(ret), K(tablet_id), K(ls_id), K(addr));
  } else {
    location_info_.tablet_id_ = tablet_id;
    location_info_.ls_id_ = ls_id;
    location_info_.addr_ = addr;
    has_location_info_ = true;
  }
  return ret;
}

int ObMemLobLocatorBuilder::set_retry_info(
    const bool leader,
    const bool read_latest,
    const uint64_t timeout)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "mem lob locator builder is not init", KR(ret));
  } else {
    retry_info_.select_leader_ = leader;
    retry_info_.read_latest_ = read_latest;
    retry_info_.timeout_ = timeout;
    has_retry_info_ = true;
  }
  return ret;
}

int ObMemLobLocatorBuilder::set_read_snapshot(
    transaction::ObTxReadSnapshot &tx_read_snapshot,
    const int64_t ls_id,
    const share::SCN &fb_snapshot,
    const int64_t tx_seq_base,
    bool read_latest)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "mem lob locator builder is not init", KR(ret));
  } else if (OB_UNLIKELY(ls_id == ObLSID::INVALID_LS_ID)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid ls id to set read snapshot", KR(ret), K(ls_id), K(fb_snapshot));
  } else {
    tx_read_snapshot_ = &tx_read_snapshot;
    read_snapshot_ls_id_ = ls_id;
    fb_snapshot_ = fb_snapshot;
    has_read_snapshot_ = true;
  }

  if (OB_FAIL(ret)) {
  } else if (read_latest) {
    if (OB_FAIL(tx_read_snapshot_->refresh_seq_no(tx_seq_base))) {
      LOG_WARN("refresh_seq_no fail", K(ret), K(tx_read_snapshot), K(tx_seq_base), K(read_latest));
    }
  }
  return ret;
}

int ObMemLobLocatorBuilder::set_rowkey(const common::ObString &rowkey_str)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "mem lob locator builder is not init", KR(ret));
  } else if (OB_UNLIKELY(rowkey_str.length() > UINT16_MAX)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "rowkey size overflow", KR(ret), K(rowkey_str.length()));
  } else {
    rowkey_ = rowkey_str;
    has_row_key_ = true;
  }
  return ret;
}

int ObMemLobLocatorBuilder::build(common::ObLobLocatorV2 &locator)
{
  int ret = OB_SUCCESS;
  locator.reset();
  char *buf = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "mem lob locator builder is not init", KR(ret));
  } else if (OB_FAIL(calc_size_())) {
    STORAGE_LOG(WARN, "failed to calc size", KR(ret));
  } else if (OB_UNLIKELY(locator_size_ > OB_MAX_LONGTEXT_LENGTH)) {
    ret = OB_SIZE_OVERFLOW;
    STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_));
  } else if (OB_ISNULL(buf = reinterpret_cast<char *>(allocator_->alloc(locator_size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory for mem lob locator", KR(ret), K(locator_size_));
  } else {
    MEMSET(buf, 0, locator_size_);
    locator.assign_buffer(buf, locator_size_);

    int64_t offset = 0;

    if (offset + sizeof(ObMemLobCommon) > locator_size_) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_));
    } else {
      ObMemLobCommon *mem_lob_common = new (buf + offset) ObMemLobCommon(ObMemLobType::PERSISTENT_LOB);
      offset += sizeof(ObMemLobCommon);
      if (OB_FAIL(prepare_mem_lob_common_(*mem_lob_common))) {
        STORAGE_LOG(WARN, "failed to prepare mem lob common", KR(ret), K(*mem_lob_common));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (has_extern_) {
      // fill extern header
      if (offset + sizeof(ObMemLobExternHeader) > locator_size_) {
        ret = OB_SIZE_OVERFLOW;
        STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_));
      } else {
        ObMemLobExternHeader *mem_lob_extern_header = new (buf + offset) ObMemLobExternHeader();
        offset += sizeof(ObMemLobExternHeader);
        if (OB_FAIL(prepare_extern_header_(*mem_lob_extern_header))) {
          STORAGE_LOG(WARN, "failed to prepare extern header", KR(ret), K(*mem_lob_extern_header));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (has_location_info_) {
        if (offset + sizeof(ObMemLobLocationInfo) > locator_size_) {
          ret = OB_SIZE_OVERFLOW;
          STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_));
        } else {
          new (buf + offset) ObMemLobLocationInfo(location_info_);
          offset += sizeof(ObMemLobLocationInfo);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (has_retry_info_) {
        if (offset + sizeof(ObMemLobRetryInfo) > locator_size_) {
          ret = OB_SIZE_OVERFLOW;
          STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_));
        } else {
          new (buf + offset) ObMemLobRetryInfo(retry_info_);
          offset += sizeof(ObMemLobRetryInfo);
        }
      }

      if (OB_FAIL(ret)) {
      } else if (has_read_snapshot_) {
        if (offset + sizeof(ObMemLobReadSnapshot) + read_snapshot_size_ > locator_size_) {
          ret = OB_SIZE_OVERFLOW;
          STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_));
        } else {
          ObMemLobReadSnapshot *mem_lob_read_snapshot = new (buf + offset) ObMemLobReadSnapshot();
          offset += sizeof(ObMemLobReadSnapshot) + read_snapshot_size_;
          if (OB_FAIL(prepare_read_snapshot_(read_snapshot_size_, *mem_lob_read_snapshot))) {
            STORAGE_LOG(WARN, "failed to prepare read snapshot", KR(ret), K(*mem_lob_read_snapshot));
          }
        }
      }

      if (OB_FAIL(ret)) {
      } else if (has_row_key_) {
        if (offset + rowkey_.length() > locator_size_) {
          ret = OB_SIZE_OVERFLOW;
          STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_));
        } else {
          MEMCPY(buf + offset, rowkey_.ptr(), rowkey_.length());
          offset += rowkey_.length();
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      // fill disk locator
      if (offset + disk_locator_size_ > locator_size_) {
        ret = OB_SIZE_OVERFLOW;
        STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_));
      } else if (input_.length() > 0) {
        MEMCPY(buf + offset, input_.ptr(), disk_locator_size_);
        offset += input_.length();
      }

      // fill payload
      if (need_payload_) {
        if (offset + payload_size_ > locator_size_) {
          ret = OB_SIZE_OVERFLOW;
          STORAGE_LOG(WARN, "mem lob locator size overflow", KR(ret), K(locator_size_), K(offset), K(payload_size_));
        } else if (OB_FAIL(prepare_payload_(buf + offset))) {
          STORAGE_LOG(WARN, "failed to prepare payload", KR(ret), KPC(this));
        } else {
          offset += payload_size_;
        }
      }

      if (OB_SUCC(ret) && OB_UNLIKELY(offset != locator_size_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected mem lob locator size", KR(ret), K(offset), K(locator_size_));
        locator.reset();
      }
    }
  }
  return ret;
}

int ObMemLobLocatorBuilder::calc_size_()
{
  int ret = OB_SUCCESS;
  payload_offset_ = 0;
  payload_size_ = 0;
  locator_size_ = sizeof(common::ObMemLobCommon);
  if (has_extern_) {
    locator_size_ += sizeof(common::ObMemLobExternHeader);
    extern_body_size_ = 0;
    if (has_location_info_) {
      locator_size_ += sizeof(common::ObMemLobLocationInfo);
      extern_body_size_ += sizeof(common::ObMemLobLocationInfo);
    }
    if (has_retry_info_) {
      locator_size_ += sizeof(common::ObMemLobRetryInfo);
      extern_body_size_ += sizeof(common::ObMemLobRetryInfo);
    }
    if (has_read_snapshot_) {
      locator_size_ += sizeof(common::ObMemLobReadSnapshot);
      extern_body_size_ += sizeof(common::ObMemLobReadSnapshot);
      if (OB_ISNULL(tx_read_snapshot_)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "unexpected null tx read snapshot", KR(ret));
      } else {
        read_snapshot_size_ = tx_read_snapshot_->get_serialize_size_for_lob(share::ObLSID(read_snapshot_ls_id_), fb_snapshot_);
        locator_size_ += read_snapshot_size_;
        extern_body_size_ += read_snapshot_size_;
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      if (has_row_key_) {
        locator_size_ += rowkey_.length();
        extern_body_size_ += rowkey_.length();
      }
      if (OB_UNLIKELY(extern_body_size_ > MAX_EXTERN_BODY_SIZE)) {
        ret = OB_SIZE_OVERFLOW;
        STORAGE_LOG(WARN, "extern body size overflow", KR(ret), K(extern_body_size_));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    locator_size_ += input_.length();
    disk_locator_size_ = input_.length();
  }
  if (OB_FAIL(ret)) {
  } else if (need_payload_) {
    payload_offset_ = locator_size_;
    ObLobLocatorV2 disk_locator(input_);
    ObLobManager *lob_mngr = MTL(ObLobManager *);
    ObLobReadContext read_ctx;
    uint64_t byte_len = 0;
    if (OB_UNLIKELY(input_.length() == 0 || !disk_locator.is_valid() || !disk_locator.is_lob_disk_locator() || disk_locator.is_inrow_disk_lob_locator())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "only outrow lob locator may need payload", KR(ret), K(input_), K(need_payload_));
    } else if (OB_ISNULL(lob_mngr)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "null lob manager", KR(ret));
    } else if (OB_FAIL(build_query_param_(read_ctx, disk_locator))) {
      STORAGE_LOG(WARN, "failed to build query param", KR(ret), KPC(this));
    } else if (OB_FAIL(lob_mngr->get_byte_length(read_ctx, byte_len))) {
      STORAGE_LOG(WARN, "failed to get lob byte length", KR(ret), K(read_ctx));
    } else {
      payload_size_ = static_cast<int64_t>(byte_len);
    }
    locator_size_ += payload_size_;
  }
  return ret;
}

int ObMemLobLocatorBuilder::build_query_param_(
    ObLobReadContext &read_ctx,
    common::ObLobLocatorV2 &disk_locator) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!has_extern_ || !has_location_info_ || !has_read_snapshot_ || OB_ISNULL(tx_read_snapshot_))) {
    // querying outrow data depends on extern/location info and read snapshot
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "outrow query needs extern/location/read snapshot", KR(ret), KPC(this));
  } else if (OB_FAIL(read_ctx.snapshot_.assign(*tx_read_snapshot_))) {
    STORAGE_LOG(WARN, "failed to assign read snapshot", KR(ret), KPC(this));
  } else {
    read_ctx.allocator_ = allocator_;
    read_ctx.coll_type_ = extern_header_.cs_type_;
    read_ctx.store_column_idx_ = extern_header_.column_idx_;
    read_ctx.offset_ = 0;
    read_ctx.len_ = UINT64_MAX;
    read_ctx.timeout_ = has_retry_info_ ? static_cast<int64_t>(retry_info_.timeout_) : THIS_WORKER.get_timeout_ts();
    read_ctx.ls_id_ = share::ObLSID(location_info_.ls_id_);
    read_ctx.tablet_id_ = ObTabletID(location_info_.tablet_id_);
    read_ctx.addr_ = location_info_.addr_;
    read_ctx.src_loc_ = disk_locator;
    if (OB_FAIL(read_ctx.src_loc_wrapper_.init(read_ctx.src_loc_.ptr_, read_ctx.src_loc_.size_))) {
      STORAGE_LOG(WARN, "failed to init src loc wrapper for query", KR(ret), K(read_ctx.src_loc_));
    }
  }
  return ret;
}

int ObMemLobLocatorBuilder::prepare_payload_(char *payload_buf) const
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager *);
  ObLobLocatorV2 disk_locator(input_);
  ObLobReadContext read_ctx;
  ObString buffer;
  buffer.assign_buffer(payload_buf, static_cast<ObString::obstr_size_t>(payload_size_));
  if (payload_size_ == 0) {
    // empty outrow lob, nothing to read
  } else if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "null lob manager", KR(ret));
  } else if (OB_FAIL(build_query_param_(read_ctx, disk_locator))) {
    STORAGE_LOG(WARN, "failed to build query param", KR(ret), KPC(this));
  } else if (OB_FAIL(lob_mngr->query(read_ctx, buffer))) {
    STORAGE_LOG(WARN, "failed to query lob data", KR(ret), K(read_ctx));
  } else if (OB_UNLIKELY(buffer.length() != payload_size_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected payload data length", KR(ret), K(buffer.length()), K(payload_size_));
  }
  return ret;
}

int ObMemLobLocatorBuilder::prepare_mem_lob_common_(common::ObMemLobCommon &mem_lob_common) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "mem lob locator builder is not init", KR(ret));
  } else {
    ObLobLocatorV2 disk_locator(input_);
    if (OB_UNLIKELY(!disk_locator.is_valid() || !disk_locator.is_lob_disk_locator())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid disk locator", KR(ret), K(disk_locator));
    } else {
      mem_lob_common.lob_common_ = *disk_locator.get_lob_common();
      mem_lob_common.lob_common_.is_mem_loc_ = 1;
    }
    if (OB_FAIL(ret)) {
    } else {
      mem_lob_common.disk_locator_size_ = disk_locator_size_;
      mem_lob_common.set_has_extern(has_extern_);
      mem_lob_common.set_has_payload(need_payload_);
      if (need_payload_) {
        mem_lob_common.payload_offset_ = payload_offset_;
        mem_lob_common.payload_size_ = payload_size_;
      }
    }
  }
  return ret;
}

int ObMemLobLocatorBuilder::prepare_read_snapshot_(
    const int64_t read_snapshot_size,
    common::ObMemLobReadSnapshot &read_snapshot) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret), KPC(this));
  } else {
    read_snapshot.size_ = read_snapshot_size;
    if (OB_FAIL(tx_read_snapshot_->serialize_for_lob(share::ObLSID(read_snapshot_ls_id_), 
                                                    fb_snapshot_,
                                                    read_snapshot.buffer_, 
                                                    read_snapshot_size, 
                                                    pos))) {
      STORAGE_LOG(WARN, "failed to serialize read snapshot for lob", KR(ret), K(read_snapshot_size));
    } else if (OB_UNLIKELY(pos != read_snapshot_size)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "unexpected read snapshot size", KR(ret), K(pos), K(read_snapshot_size));
    }
  }
  return ret;
}

int ObMemLobLocatorBuilder::prepare_extern_header_(common::ObMemLobExternHeader &extern_header) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", KR(ret), KPC(this));
  } else {
    extern_header = extern_header_;
    extern_header.body_size_ = extern_body_size_;
    extern_header.has_location_info_ = has_location_info_ ? 1 : 0;
    extern_header.has_retry_info_ = has_retry_info_ ? 1 : 0;
    extern_header.has_read_snapshot_ = has_read_snapshot_ ? 1 : 0;
    extern_header.has_row_key_ = has_row_key_ ? 1 : 0;
    extern_header.rowkey_size_ = rowkey_.length();
  }
  return ret;
}


//================================ ObLobLocatorHelper ====================================
ObLobLocatorHelper::ObLobLocatorHelper()
  : table_id_(OB_INVALID_ID),
    tablet_id_(OB_INVALID_ID),
    ls_id_(ObLSID::INVALID_LS_ID),
    tx_read_snapshot_(),
    fb_snapshot_(),
    rowid_version_(ObURowIDData::INVALID_ROWID_VERSION),
    rowid_project_(nullptr),
    rowid_objs_(),
    locator_allocator_(ObModIds::OB_LOB_READER, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    rowkey_str_(),
    is_inited_(false),
    scan_flag_(),
    tx_seq_base_(-1)
{
}

ObLobLocatorHelper::~ObLobLocatorHelper()
{
}

void ObLobLocatorHelper::reset()
{
  table_id_ = OB_INVALID_ID;
  tablet_id_ = OB_INVALID_ID;
  ls_id_ = ObLSID::INVALID_LS_ID;
  tx_read_snapshot_.reset();
  fb_snapshot_.reset();
  rowid_version_ = ObURowIDData::INVALID_ROWID_VERSION;
  rowid_project_ = nullptr;
  rowid_objs_.reset();
  locator_allocator_.reset();
  rowkey_str_.reset();
  is_inited_ = false;
  scan_flag_.reset();
  tx_seq_base_ = -1;
}

int ObLobLocatorHelper::init(const ObTableScanParam &scan_param,
                             const ObStoreCtx &ctx,
                             const share::ObLSID &ls_id,
                             const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableParam &table_param = *scan_param.table_param_;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobLocatorHelper init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(!table_param.use_lob_locator() || snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObLobLocatorHelper", K(ret), K(table_param), K(snapshot_version));
  } else if (table_param.get_rowid_version() != ObURowIDData::INVALID_ROWID_VERSION
            && table_param.get_rowid_projector().empty()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected empty rowid projector", K(ret), K(table_param));
  } else {
    if (table_param.get_rowid_version() != ObURowIDData::INVALID_ROWID_VERSION
              && table_param.get_rowid_projector().empty()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpected empty rowid projector", K(ret), K(table_param));
    } else {
      rowid_version_ = table_param.get_rowid_version();
      rowid_project_ = &table_param.get_rowid_projector();
      table_id_ = table_param.get_table_id();
      tablet_id_ = scan_param.tablet_id_.id();
      ls_id_ = ls_id.id();
      scan_flag_ = scan_param.scan_flag_;
      tx_seq_base_ = scan_param.tx_seq_base_;
      fb_snapshot_ = scan_param.fb_snapshot_;
      if (OB_FAIL(tx_read_snapshot_.assign(scan_param.snapshot_))) {
        LOG_WARN("assign snapshot fail", K(ret), K(scan_param.snapshot_));
      } else if (snapshot_version != ctx.mvcc_acc_ctx_.snapshot_.version().get_val_for_tx()) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "snapshot version mismatch",
          K(snapshot_version), K(tx_read_snapshot_), K(ctx.mvcc_acc_ctx_.snapshot_));
      } else {
        is_inited_ = true;
      }
    }
  }

  return ret;
}

int ObLobLocatorHelper::init(const uint64_t table_id,
                            const uint64_t tablet_id,
                            const ObStoreCtx &ctx,
                            const share::ObLSID &ls_id,
                            const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObLobLocatorHelper init twice", K(ret), K(*this));
  } else if (OB_UNLIKELY(snapshot_version <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid argument to init ObLobLocatorHelper", K(ret), K(ls_id), K(snapshot_version));
  } else if (OB_FAIL(tx_read_snapshot_.build_snapshot_for_lob(ctx.mvcc_acc_ctx_.snapshot_, ls_id))) {
    STORAGE_LOG(WARN, "build_snapshot_for_lob fail", K(ret), K(ls_id), K(snapshot_version), K(ctx.mvcc_acc_ctx_.snapshot_));
  } else {
    rowid_version_ = ObURowIDData::INVALID_ROWID_VERSION;
    rowid_project_ = NULL;
    // table id只用来判断是不是systable, 这个接口创建的locator不会构造真正的rowid
    table_id_ = table_id;
    tablet_id_ = tablet_id;
    ls_id_ = ls_id.id();
    is_inited_ = true;
    // OB_ASSERT(snapshot_version == ctx.mvcc_acc_ctx_.snapshot_.version_);
    // snapshot_version mismatch in test_multi_version_sstable_single_get
  }

  return ret;
}

int ObLobLocatorHelper::prepare_lob_read_snapshot_(
    const transaction::ObTxSnapshot *read_snapshot,
    const share::ObLSID &ls_id,
    LobReadSnapshotParam &param)
{
  int ret = OB_SUCCESS;
  if (nullptr == read_snapshot) {
    param.snapshot_ = &tx_read_snapshot_;
    param.fb_snapshot_ = fb_snapshot_;
    param.read_latest_ = scan_flag_.read_latest_;
  } else if (OB_FAIL(param.historical_snapshot_.build_snapshot_for_lob(*read_snapshot, ls_id))) {
    STORAGE_LOG(WARN, "Failed to build historical LOB snapshot", KR(ret));
  } else {
    // Historical OLD rows must read LOB data at the same snapshot as their disk locator.
    param.snapshot_ = &param.historical_snapshot_;
    param.fb_snapshot_.set_invalid();
    param.read_latest_ = false;
  }
  return ret;
}

int ObLobLocatorHelper::fill_lob_locator_v2(
    ObDatumRow &row,
    const ObTableAccessContext &access_ctx,
    const ObTableAccessParam &access_param,
    const transaction::ObTxSnapshot *read_snapshot)
{
  int ret = OB_SUCCESS;
  const ObITableReadInfo *read_info = access_param.iter_param_.get_read_info();
  LobReadSnapshotParam snapshot_param;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobLocatorHelper is not init", K(ret), K(*this));
  } else if (OB_UNLIKELY(!row.is_valid() || !access_ctx.is_valid() || !access_param.is_valid()) || OB_ISNULL(read_info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(row), K(access_ctx), K(access_param));
  } else if (OB_FAIL(prepare_lob_read_snapshot_(read_snapshot, access_ctx.ls_id_, snapshot_param))) {
    STORAGE_LOG(WARN, "Failed to prepare LOB read snapshot", KR(ret));
  } else {
    const ObIArray<ObColumnParam *> *cols_param = access_param.iter_param_.get_col_params();
    const ObColDescIArray *col_descs = access_param.iter_param_.get_out_col_descs();
    const common::ObIArray<int32_t> *cols_project = access_param.iter_param_.out_cols_project_;
    bool is_systable = is_sys_table(access_param.iter_param_.table_id_);

    if (cols_param->count() != row.count_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Invalid col count", KR(ret), K(row), KPC(cols_param));
    } else if (OB_FAIL(build_rowid_obj(row, rowkey_str_, false, *col_descs, *cols_project, access_param.iter_param_.tablet_id_))) {
      STORAGE_LOG(WARN, "Failed to build rowid obj", K(ret), K(rowkey_str_));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
        blocksstable::ObStorageDatum &datum = row.storage_datums_[i];
        ObObjMeta datum_meta = cols_param->at(i)->get_meta_type();
        int64_t store_col_idx = OB_INVALID_INDEX;
        ObLobLocatorV2 locator;
        ObMemLobLocatorBuilder locator_builder;
        ObLobDiskLocatorWrapper disk_locator;
        if (datum_meta.is_lob_storage()) {
          if (datum.is_null() || datum.is_nop()) {
          } else if (OB_FAIL(disk_locator.init(datum.ptr_, datum.len_))) {
            LOG_WARN("failed to init disk locator", KR(ret), K(datum));
          } else if (disk_locator.is_inrow()) {
            // inrow can skip build mem lob locator
          } else if (OB_UNLIKELY(i >= read_info->get_columns_index().count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("index out of length", KR(ret), K(i), K(read_info), "count", i >= read_info->get_columns_index().count());
          } else if (FALSE_IT(store_col_idx = read_info->get_columns_index().at(i))) {
          } else if (OB_UNLIKELY(store_col_idx == OB_INVALID_INDEX)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid store col idx", KR(ret), K(i), K(store_col_idx));
          } else {
            if (OB_FAIL(locator_builder.init(locator_allocator_,
                                             datum.get_string(),
                                             is_systable))) {
              STORAGE_LOG(WARN, "failed to init locator builder", KR(ret), K(locator));
            } else if (OB_FAIL(locator_builder.set_extern_info(access_param.iter_param_.table_id_, store_col_idx, datum_meta.get_collation_type()))) {
              STORAGE_LOG(WARN, "failed to set extern info", KR(ret), K(locator));
            } else if (OB_FAIL(locator_builder.set_location_info(access_param.iter_param_.tablet_id_.id(), ls_id_, MYADDR))) {
              STORAGE_LOG(WARN, "failed to set location info", KR(ret), K(locator));
            } else if (OB_FAIL(locator_builder.set_retry_info(!scan_flag_.is_select_follower_, snapshot_param.read_latest_, access_ctx.timeout_))) {
              STORAGE_LOG(WARN, "failed to set retry info", KR(ret), K(locator));
            } else if (OB_FAIL(locator_builder.set_read_snapshot(*snapshot_param.snapshot_, ls_id_, snapshot_param.fb_snapshot_,
                                                                 tx_seq_base_, snapshot_param.read_latest_))) {
              STORAGE_LOG(WARN, "failed to set read snapshot", KR(ret), K(locator));
            } else if (OB_FAIL(locator_builder.set_rowkey(rowkey_str_))) {
              STORAGE_LOG(WARN, "failed to set rowkey", KR(ret), K(locator));
            }

            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(locator_builder.build(locator))) {
              STORAGE_LOG(WARN, "failed to build locator", KR(ret), K(locator));
            } else {
              datum.set_string(locator.ptr_, locator.size_);
              STORAGE_LOG(DEBUG, "succeed to load lob obj", K(datum), K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLobLocatorHelper::fill_lob_locator_v2(
    common::ObDatum &datum,
    const ObColumnParam &col_param,
    const ObTableIterParam &iter_param,
    const ObTableAccessContext &access_ctx,
    const int32_t col_offset)
{
  int ret = OB_SUCCESS;
  const ObObjMeta &datum_meta = col_param.get_meta_type();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObLobLocatorHelper is not init", K(ret), K(*this));
  } else if (!datum_meta.is_lob_storage() || datum.is_null() || datum.is_nop() || datum.get_lob_data().in_row_) {
  } else {
    const ObITableReadInfo *read_info = iter_param.get_read_info();
    if (OB_ISNULL(read_info) || OB_UNLIKELY(col_offset < 0 || col_offset >= read_info->get_columns_index().count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("read_info is nullptr", KR(ret), KP(read_info));
    } else {
      int32_t store_col_idx = read_info->get_columns_index().at(col_offset);
      ObLobLocatorV2 locator;
      rowkey_str_.reset();
      ObMemLobLocatorBuilder locator_builder;
      // TODO @fengyun why systable need payload data when lob is outrow
      bool is_systable = is_sys_table(iter_param.table_id_);
      if (OB_UNLIKELY(store_col_idx == OB_INVALID_INDEX)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid store col idx", KR(ret), K(store_col_idx));
      } else if (OB_FAIL(locator_builder.init(locator_allocator_,
                                              datum.get_string(),
                                              is_systable))) {
        STORAGE_LOG(WARN, "failed to init locator builder", KR(ret), K(locator));
      } else if (OB_FAIL(locator_builder.set_extern_info(iter_param.table_id_, store_col_idx, datum_meta.get_collation_type()))) {
        STORAGE_LOG(WARN, "failed to set extern info", KR(ret), K(locator));
      } else if (OB_FAIL(locator_builder.set_location_info(iter_param.tablet_id_.id(), ls_id_, MYADDR))) {
        STORAGE_LOG(WARN, "failed to set location info", KR(ret), K(locator));
      } else if (OB_FAIL(locator_builder.set_retry_info(!scan_flag_.is_select_follower_, scan_flag_.read_latest_, access_ctx.timeout_))) {
        STORAGE_LOG(WARN, "failed to set retry info", KR(ret), K(locator));
      } else if (OB_FAIL(locator_builder.set_read_snapshot(tx_read_snapshot_, ls_id_, fb_snapshot_, tx_seq_base_, scan_flag_.read_latest_))) {
        STORAGE_LOG(WARN, "failed to set read snapshot", KR(ret), K(locator));
      }

      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(locator_builder.build(locator))) {
        STORAGE_LOG(WARN, "failed to build locator", KR(ret), K(locator));
      } else {
        datum.set_string(locator.ptr_, locator.size_);
      }
    }
  }
  return ret;
}

int ObLobLocatorHelper::fuse_mem_lob_header(ObObj &def_obj, uint64_t col_id, bool is_systable)
{
  int ret = OB_SUCCESS;
  if (!(def_obj.is_lob_storage()) || def_obj.is_nop_value() || def_obj.is_null()) {
  } else {
    // must be called after fill_lob_locator, should not reuse/reset locator_allocator_ or rowkey_str_
    ObLobLocatorV2 locator;
    ObMemLobLocatorBuilder locator_builder;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      STORAGE_LOG(WARN, "ObLobLocatorHelper is not init", K(ret), K(*this));
    } else if (OB_UNLIKELY(!is_valid_id(col_id))) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "Invalid argument to fuse lob header", K(ret), K(col_id));
    } else if (OB_FAIL(locator_builder.init(locator_allocator_,
                                            def_obj.get_string(),
                                            is_systable))) {
      STORAGE_LOG(WARN, "failed to init locator builder", KR(ret), K(locator));
    } else if (OB_FAIL(locator_builder.build(locator))) {
      STORAGE_LOG(WARN, "failed to build default lob locator", KR(ret), K(def_obj));
    } else {
      def_obj.set_string(def_obj.get_type(), locator.ptr_, locator.size_);
    }
  }
  return ret;
}

int ObLobLocatorHelper::build_rowid_obj(
    ObDatumRow &row,
    common::ObString &rowid_str,
    bool is_projected_row,
    const ObColDescIArray &col_descs,
    const common::ObIArray<int32_t> &out_project,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  rowid_objs_.reset();
  rowid_str.reset();
  if (rowid_version_ == ObURowIDData::INVALID_ROWID_VERSION) {
    // use empty string for rowid
  } else if (OB_ISNULL(rowid_project_) || rowid_project_->empty()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected null or empty rowid project", K(ret), K(*this));
  } else {
    ObObj tmp_obj;
    if (is_projected_row) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowid_project_->count(); i++) {
        int64_t idx = rowid_project_->at(i);
        if (OB_UNLIKELY(idx < 0 || idx >= row.count_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected column project idx", K(ret), K(idx));
        } else if (OB_FAIL(row.storage_datums_[idx].to_obj(tmp_obj, col_descs.at(idx).col_type_))) {
          STORAGE_LOG(WARN, "Failed to transform datum to obj", K(ret));
        } else if (OB_FAIL(rowid_objs_.push_back(tmp_obj))) {
          STORAGE_LOG(WARN, "Failed to push back rowid object", K(ret), K(idx), K(row));
        }
      }
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowid_project_->count(); i++) {
        int64_t idx = rowid_project_->at(i);
        if (OB_UNLIKELY(idx < 0 || idx >= out_project.count())) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected column project idx", K(ret), K(idx), K(out_project));
        } else if (FALSE_IT(idx = out_project.at(idx))) {
        } else if (OB_UNLIKELY(idx < 0 || idx >= row.count_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "Unexpected column project idx", K(ret), K(idx), K(row));
        } else if (OB_FAIL(row.storage_datums_[idx].to_obj(tmp_obj, col_descs.at(idx).col_type_))) {
          STORAGE_LOG(WARN, "Failed to transform datum to obj", K(ret));
        } else if (OB_FAIL(rowid_objs_.push_back(tmp_obj))) {
          STORAGE_LOG(WARN, "Failed to push back rowid object", K(ret), K(idx), K(row));
        }
      }
    }
    // append tablet id to build heap table rowid
    if (OB_SUCC(ret) && (ObURowIDData::HEAP_TABLE_ROWID_VERSION == rowid_version_ || ObURowIDData::EXT_HEAP_TABLE_ROWID_VERSION == rowid_version_)) {
      tmp_obj.set_int(tablet_id.id());
      if (OB_FAIL(rowid_objs_.push_back(tmp_obj))) {
        LOG_WARN("failed to push back tablet id", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      common::ObURowIDData rowid;
      // rowid_objs_只包含真实rowkey，因此PK格式中的生成分区列数为0。
      int64_t rowid_buf_size = 0;
      int64_t rowid_str_size = 0;
      char *rowid_buf = nullptr;
      if (OB_FAIL(rowid.set_rowid_content(
          rowid_objs_, rowid_version_, rowid_objs_.count(), locator_allocator_))) {
        STORAGE_LOG(WARN, "Failed to set rowid content", K(ret), K(*this));
      } else if (FALSE_IT(rowid_buf_size = rowid.needed_base64_buffer_size())) {
      } else if (OB_ISNULL(rowid_buf = reinterpret_cast<char *>(locator_allocator_.alloc(rowid_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "Failed to alloc memory for rowid base64 str", K(ret), K(rowid_buf_size));
      } else if (OB_FAIL(rowid.get_base64_str(rowid_buf, rowid_buf_size, rowid_str_size))) {
        STORAGE_LOG(WARN, "Failed to get rowid base64 string", K(ret));
      } else {
        rowid_str.assign_ptr(rowid_buf, rowid_str_size);
      }
    }
  }
  STORAGE_LOG(DEBUG, "build rowid for lob locator", K(ret), K(rowid_str));

  return ret;
}


//==================== ObLobDiskLocatorWrapper ====================
int ObLobDiskLocatorWrapper::init(const char *ptr, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ptr_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KP(ptr), K(len), KPC(this));
  } else if (OB_ISNULL(ptr) || OB_UNLIKELY(len < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(ptr), K(len));
  } else if (FALSE_IT(lob_common_ = reinterpret_cast<const ObLobCommon *>(ptr))) {
  } else if (OB_UNLIKELY(lob_common_->is_mem_loc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("can not init with mem locator", KR(ret), K(lob_common_));
  } else if (lob_common_->in_row_) {
    ptr_ = ptr;
    len_ = len;
    inrow_data_ = lob_common_->buffer_;
    inrow_data_len_ = len_ - static_cast<int64_t>(sizeof(ObLobCommon));
  } else {
    if (OB_UNLIKELY(len != DISK_LOB_OUTROW_FULL_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid outrow disk locator size", KR(ret), KP(ptr), K(len), K(DISK_LOB_OUTROW_FULL_SIZE));
    } else if (OB_FAIL(lob_id_.from_string(
                   ObString(sizeof(ObLobId), lob_common_->buffer_)))) {
      LOG_WARN("failed to parse lob id from disk locator", KR(ret), KP(ptr), K(len));
    } else {
      ptr_ = ptr;
      len_ = len;
      int64_t raw_seq_no = 0;
      MEMCPY(&raw_seq_no, lob_common_->buffer_ + sizeof(ObLobId), sizeof(raw_seq_no));
      seq_no_ = transaction::ObTxSEQ::cast_from_int(raw_seq_no);
    }
  }
  return ret;
}

void ObLobDiskLocatorWrapper::reset()
{
  ptr_ = nullptr;
  len_ = 0;
  lob_common_ = nullptr;
  lob_id_.reset();
  seq_no_.reset();
  inrow_data_ = nullptr;
  inrow_data_len_ = 0;
}

bool ObLobDiskLocatorWrapper::is_valid() const {
  // 索引表的 outrow lob 的 seq_no 可能无效，因此不做校验
  return nullptr != lob_common_ &&
         ((lob_common_->in_row_ && inrow_data_len_ >= 0) ||
          (!lob_common_->in_row_ && nullptr != ptr_));
}

int ObLobDiskLocatorWrapper::get_lob_id(ObLobId &lob_id) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid()) || is_inrow()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(this));
  } else {
    lob_id = lob_id_;
  }
  return ret;
}

//==================== ObLobDiskLocatorBuilder ====================
void ObLobDiskLocatorBuilder::reset()
{
  ptr_ = nullptr;
  len_ = 0;
  lob_common_ = nullptr;
  seq_no_.reset();
  inrow_data_ = nullptr;
  inrow_data_len_ = 0;
}

int ObLobDiskLocatorBuilder::init_outrow(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const int64_t locator_len = DISK_LOB_OUTROW_FULL_SIZE;
  if (OB_NOT_NULL(ptr_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(ptr_ = static_cast<char*>(allocator.alloc(locator_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf fail", K(ret), K(locator_len));
  } else {
    MEMSET(ptr_, 0, locator_len);
    len_ = locator_len;
    lob_common_ = new(ptr_) ObLobCommon();
    lob_common_->in_row_ = 0;
    seq_no_.reset();
  }
  return ret;
}

int ObLobDiskLocatorBuilder::init_inrow(
    common::ObIAllocator &allocator,
    const char *inrow_data,
    const int64_t inrow_data_len)
{
  int ret = OB_SUCCESS;
  const int64_t locator_len = static_cast<int64_t>(sizeof(ObLobCommon)) + inrow_data_len;
  if (OB_NOT_NULL(ptr_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(inrow_data_len < 0 || (inrow_data_len > 0 && OB_ISNULL(inrow_data)))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid inrow data", K(ret), KP(inrow_data), K(inrow_data_len));
  } else if (OB_ISNULL(ptr_ = static_cast<char*>(allocator.alloc(locator_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf fail", K(ret), K(locator_len));
  } else {
    MEMSET(ptr_, 0, locator_len);
    len_ = locator_len;
    lob_common_ = new(ptr_) ObLobCommon();
    lob_common_->in_row_ = 1;
    inrow_data_ = lob_common_->buffer_;
    inrow_data_len_ = inrow_data_len;
    if (inrow_data_len > 0) {
      MEMCPY(inrow_data_, inrow_data, inrow_data_len);
    }
  }
  return ret;
}

int ObLobDiskLocatorBuilder::set_lob_id(const ObLobId &lob_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this), K(lob_id));
  } else if (OB_UNLIKELY(lob_common_->in_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob id is invalid", K(ret), KPC(this));
  } else {
    const ObString lob_id_string = lob_id.get_string();
    MEMCPY(lob_common_->buffer_, lob_id_string.ptr(), lob_id_string.length());
  }
  return ret;
}

int ObLobDiskLocatorBuilder::set_seq_no(const transaction::ObTxSEQ &seq_no)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this), K(seq_no));
  } else if (OB_UNLIKELY(lob_common_->in_row_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("seq no is invalid", K(ret), KPC(this));
  } else {
    const uint64_t raw_seq_no = seq_no.cast_to_int();
    MEMCPY(lob_common_->buffer_ + sizeof(ObLobId), &raw_seq_no, sizeof(raw_seq_no));
    seq_no_ = seq_no;
  }
  return ret;
}

int ObLobDiskLocatorBuilder::to_locator(ObLobLocatorV2 &locator) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ptr_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(len_ < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too small len", KR(ret), KPC(this));
  } else {
    locator.assign_buffer(ptr_, len_);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
