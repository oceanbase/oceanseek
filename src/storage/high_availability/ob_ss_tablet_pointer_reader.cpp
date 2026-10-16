/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_ss_tablet_pointer_reader.h"
#include "storage/incremental/ob_shared_meta_service.h"

using namespace oceanbase;
using namespace common;
using namespace share;
using namespace storage;


ObSSTabletPointerReader::ObSSTabletPointerReader()
  : is_inited_(false),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    reorg_scn_(),
    row_scn_(),
    offset_(0),
    size_(0)
{
}

int ObSSTabletPointerReader::init(const ObMetaDiskAddr &load_addr)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ss tablet pointer reader init twice", K(ret));
  } else if (!load_addr.is_valid() || !load_addr.is_sslog()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("init ss tablet pointer reader get invalid argument", K(ret), K(load_addr));
  } else if (OB_FAIL(load_addr.get_sslog_tablet_meta_addr(ls_id_, tablet_id_, reorg_scn_, row_scn_))) {
    LOG_WARN("failed to get sslog tablet meta addr", K(ret), K(load_addr));
  } else {
    offset_ = load_addr.offset();
    size_ = load_addr.size();
    tenant_id_ = MTL_ID();
    is_inited_ = true;
  }
  return ret;
}

int ObSSTabletPointerReader::get_tablet_raw_buffer(
    common::ObIAllocator &allocator,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet pointer reader do not init", K(ret));
  } else {
    //TODO(muwei.ym) wait for yanyuan provide a local iterator to get mulit version row
    //single version iter for sslog keep multi version start
    if (OB_FAIL(try_read_from_single_version_iter_(allocator, tablet_raw_buf, tablet_raw_buf_len))) {
      LOG_WARN("failed to try read from single version iter", K(ret), K(ls_id_), K(tablet_id_), K(row_scn_));
    }

    if (OB_INVALID_QUERY_TIMESTAMP == ret) {
      //overwrite ret
      //multi version iter for sslog do not keep multi version start
      if (OB_FAIL(try_read_from_multi_version_iter_(allocator, tablet_raw_buf, tablet_raw_buf_len))) {
        LOG_WARN("failed to try read from multi version iter", K(ret), K(ls_id_), K(tablet_id_), K(row_scn_));
      }
    }
  }
  return ret;
}

int ObSSTabletPointerReader::get_local_sslog_readable_scn_(share::SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  readable_scn.reset();
  uint64_t real_tenant_id = 0;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ss tablet pointer reader do not init", K(ret));
  } else {
    real_tenant_id = gen_sslog_tenant_id(tenant_id_, ls_id_);
    if (OB_INVALID_ID == real_tenant_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("real tenant is is invalid, unexpected", K(ret), K(tenant_id_), K(ls_id_), K(real_tenant_id));
    } else {
      MTL_SWITCH(real_tenant_id) {
        if (OB_FAIL(MTL(ObSSMetaService *)->get_max_committed_meta_scn(readable_scn))) {
          STORAGE_LOG(WARN, "failed to get max committed meta scn", K(ret));
        } else if (!readable_scn.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(WARN, "readable scn should not be invalid", K(ret));
        } else {
          //max decided scn may smaller than row scn because get max decided scn is async operation
          readable_scn = readable_scn < row_scn_ ? row_scn_ : readable_scn;
        }
      }
    }
  }
  return ret;
}

int ObSSTabletPointerReader::get_tablet_version_iter(
    const share::SCN &readable_scn,
    ObSSMetaIterGuard<ObSSMetaTabletIter> &tablet_iter_guard)
{
#define PRINT_WRAPPER KR(ret), K(readable_scn), K(ls_id_), K(tablet_id_), K(reorg_scn_), K(row_scn_)
  int ret = OB_SUCCESS;
  ObSSMetaService *ss_meta_srv = nullptr;
  share::ObScnRange range;
  range.start_scn_ = row_scn_;
  range.end_scn_ = readable_scn;
  ObSSMetaReadParamV2 read_param;
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_PREFIX,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    ObAtomicProtocolTypeEnum::TABLET_META,
                                    ls_id_,
                                    tablet_id_,
                                    SCN::min_scn() /*meta_scn*/,
                                    reorg_scn_,
                                    true /*is_in_table*/);
  read_param.set_read_mark_delete(true);

  if (OB_UNLIKELY(!read_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("read_param of tablet_iter is invalid", PRINT_WRAPPER);
  } else if (OB_ISNULL(ss_meta_srv = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObSSMetaService should not be null", PRINT_WRAPPER);
  } else if (OB_FAIL(ss_meta_srv->get_tablet_iter(read_param, range, tablet_iter_guard))) {
    LOG_WARN("get tablet from ObSSMetaService failed", PRINT_WRAPPER);
  }

  if (OB_FAIL(ret) && ret != OB_INVALID_QUERY_TIMESTAMP && ret != OB_TABLET_NOT_EXIST) {
    int tmp_ret = ret;
    ret = OB_IO_ERROR;
    LOG_WARN("get tablet failed, convert to io error", K(tmp_ret), PRINT_WRAPPER);
  }
#undef PRINT_WRAPPER
  return ret;
}

int ObSSTabletPointerReader::get_tablet_raw_buffer_(
    common::ObIAllocator &allocator,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len,
    ObSSMetaIterGuard<ObSSMetaTabletIter> &tablet_iter_guard)
{
#define PRINT_WRAPPER KR(ret), K(ls_id_), K(tablet_id_), K(reorg_scn_), K(row_scn_)
  int ret = OB_SUCCESS;
  ObSSMetaTabletIter *tablet_iter = nullptr;
  ObArenaAllocator tmp_allocator;
  if (OB_FAIL(tablet_iter_guard.get_iter(tablet_iter))) {
    LOG_WARN("get tablet_iter from iter_guard failed", PRINT_WRAPPER);
  } else if (OB_ISNULL(tablet_iter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet iter should not be NULL", K(ret), PRINT_WRAPPER);
  } else {
    ObTabletHandle tablet_handle;
    ObSSMetaUpdateMetaInfo update_meta_info;
    share::SCN reorganization_scn;
    share::SCN meta_scn;
    bool found = false;
    while (OB_SUCC(ret)) {
      tmp_allocator.reset();
      tablet_handle.reset();
      update_meta_info = ObSSMetaUpdateMetaInfo();
      reorganization_scn.reset();
      meta_scn.reset();
      if (OB_FAIL(tablet_iter->get_next(tmp_allocator, tablet_handle, update_meta_info, reorganization_scn, meta_scn))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next tablet", K(ret), PRINT_WRAPPER);
        }
      } else if (meta_scn == row_scn_) {
        // Found the matching version, now get its raw buffer
        char *tmp_tablet_raw_buf = nullptr;
        int64_t tmp_tablet_raw_buf_len = 0;
        SCN sslog_row_scn;
        ObSSMetaReadParamV2 read_param;
        ObSSMetaService *ss_meta_svr = nullptr;
        if (OB_ISNULL(ss_meta_svr = MTL(ObSSMetaService *))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null ObSSMetaService", K(ret));
        } else if (FALSE_IT(read_param.set_tablet_level_param(
                              ObSSMetaReadParamType::TABLET_PREFIX,
                              ObSSMetaReadResultType::READ_WHOLE_ROW,
                              ObAtomicProtocolTypeEnum::TABLET_META,
                              ls_id_,
                              tablet_id_,
                              SCN::min_scn() /*meta_scn*/,
                              reorg_scn_,
                              true /*is_in_table*/))) {
        } else if (FALSE_IT(read_param.set_read_mark_delete(true))) {
        } else if (OB_FAIL(ss_meta_svr->get_tablet_raw_buffer(read_param,
                                                                row_scn_,
                                                                tmp_allocator,
                                                                sslog_row_scn,
                                                                tmp_tablet_raw_buf,
                                                                tmp_tablet_raw_buf_len))) {
          LOG_WARN("failed to get tablet raw buffer", K(ret), PRINT_WRAPPER);
        } else if (OB_FAIL(inner_get_tablet_raw_buffer_(allocator, tmp_tablet_raw_buf,
            tmp_tablet_raw_buf_len, tablet_raw_buf, tablet_raw_buf_len))) {
          LOG_WARN("failed to do inner get tablet raw buffer", K(ret), KP(tmp_tablet_raw_buf), K(tmp_tablet_raw_buf_len));
        } else {
          found = true;
          break;
        }
      }
    }

    if (OB_FAIL(ret) && ret != OB_INVALID_QUERY_TIMESTAMP) {
      int tmp_ret = ret;
      ret = OB_IO_ERROR;
      LOG_WARN("get tablet failed, convert to io error", K(tmp_ret), PRINT_WRAPPER);
    }

    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet pointer in ss do not exist, unexpected", K(ret), PRINT_WRAPPER);
    }
  }
#undef PRINT_WRAPPER
  return ret;
}

int ObSSTabletPointerReader::inner_get_tablet_raw_buffer_(
    common::ObIAllocator &allocator,
    const char *src_buf,
    const int64_t &src_buf_len,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src_buf) || src_buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner get tablet raw buffer get invalid argument", K(ret), KP(src_buf), K(src_buf_len));
  } else if (OB_UNLIKELY(offset_ + size_ > src_buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("addr size overflow", K(ret), K(offset_), K(size_), K(src_buf_len));
  } else if (OB_ISNULL(tablet_raw_buf = static_cast<char*>(allocator.alloc(size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc memory failed", K(ret), K(size_), K(offset_), K(src_buf_len));
  } else {
    MEMCPY(tablet_raw_buf, src_buf + offset_, size_);
    tablet_raw_buf_len = static_cast<int64_t>(size_);
  }
  return ret;
}

int ObSSTabletPointerReader::try_read_from_single_version_iter_(
    common::ObIAllocator &allocator,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len)
{
#define PRINT_WRAPPER KR(ret), K(ls_id_), K(tablet_id_), K(reorg_scn_), K(row_scn_)
  int ret = OB_SUCCESS;
  ObSSMetaReadParamV2 read_param;
  ObSSMetaService *ss_meta_svr = nullptr;
  tablet_raw_buf = nullptr;
  tablet_raw_buf_len = 0;
  SCN sslog_row_scn;
  ObArenaAllocator tmp_allocator;
  char *tmp_tablet_raw_buf = nullptr;
  int64_t tmp_tablet_raw_buf_len = 0;

  if (OB_ISNULL(ss_meta_svr = MTL(ObSSMetaService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ObSSMetaService", K(ret), KP(ss_meta_svr));
  } else if (FALSE_IT(read_param.set_tablet_level_param(
                        ObSSMetaReadParamType::TABLET_PREFIX,
                        ObSSMetaReadResultType::READ_WHOLE_ROW,
                        ObAtomicProtocolTypeEnum::TABLET_META,
                        ls_id_,
                        tablet_id_,
                        SCN::min_scn() /*meta_scn*/,
                        reorg_scn_,
                        true /*is_in_table*/))) {
  } else if (FALSE_IT(read_param.set_read_mark_delete(true))) {
  } else if (OB_FAIL(ss_meta_svr->get_tablet_raw_buffer(read_param,
                                                        row_scn_,
                                                        tmp_allocator,
                                                        /*out*/sslog_row_scn,
                                                        /*out*/tmp_tablet_raw_buf,
                                                        /*out*/tmp_tablet_raw_buf_len))) {
    LOG_WARN("failed to get tablet raw buffer", K(ret), K(read_param), PRINT_WRAPPER);
  }

  if (OB_FAIL(ret) && ret != OB_INVALID_QUERY_TIMESTAMP && ret != OB_TABLET_NOT_EXIST) {
    int tmp_ret = ret;
    ret = OB_IO_ERROR;
    LOG_WARN("get tablet failed, convert to io error", K(tmp_ret), PRINT_WRAPPER);
  }

  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_UNLIKELY(sslog_row_scn != row_scn_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected sslog row scn", K(ret), K(sslog_row_scn), PRINT_WRAPPER);
    } else if (OB_TMP_FAIL(inner_get_tablet_raw_buffer_(allocator, 
                                                        tmp_tablet_raw_buf,
                                                        tmp_tablet_raw_buf_len, 
                                                        tablet_raw_buf, 
                                                        tablet_raw_buf_len))) {
      ret = OB_IO_ERROR;
      LOG_WARN("failed to do inner get tablet raw buffer", K(ret), 
                                                           K(tmp_ret), 
                                                           KP(tmp_tablet_raw_buf), 
                                                           K(tmp_tablet_raw_buf_len));
    }
  }
#undef PRINT_WRAPPER
  return ret;
}

int ObSSTabletPointerReader::try_read_from_multi_version_iter_(
    common::ObIAllocator &allocator,
    char *&tablet_raw_buf,
    int64_t &tablet_raw_buf_len)
{
  int ret = OB_SUCCESS;
  share::SCN readable_scn;
  ObSSMetaIterGuard<ObSSMetaTabletIter> tablet_iter_guard;
  tablet_raw_buf = nullptr;
  tablet_raw_buf_len = 0;

  if (OB_FAIL(get_local_sslog_readable_scn_(readable_scn))) {
    LOG_WARN("failed to get local sslog readable scn", K(ret), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(get_tablet_version_iter(readable_scn, tablet_iter_guard))) {
    LOG_WARN("failed to get tablet version iter", K(ret), K(readable_scn), K(ls_id_), K(tablet_id_), K(reorg_scn_), K(row_scn_));
  } else if (OB_FAIL(get_tablet_raw_buffer_(allocator, tablet_raw_buf, tablet_raw_buf_len, tablet_iter_guard))) {
    LOG_WARN("failed to get tabelt raw buffer", K(ret), K(readable_scn), K(ls_id_), K(tablet_id_), K(reorg_scn_), K(row_scn_));
  }
  return ret;
}

