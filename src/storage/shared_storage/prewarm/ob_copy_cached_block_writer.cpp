/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/prewarm/ob_copy_cached_block_writer.h"
#include "storage/shared_storage/prewarm/ob_copy_cached_block_reader.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_ss_local_cache_service.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::lib;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;

ObCopyCachedBlockWriter::ObCopyCachedBlockWriter()
  : is_inited_(false), tenant_id_(OB_INVALID_TENANT_ID), ls_id_(), reader_(nullptr)
{
}

ObCopyCachedBlockWriter::~ObCopyCachedBlockWriter()
{
  reset();
}

void ObCopyCachedBlockWriter::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  ls_id_.reset();
  reader_ = nullptr;
  is_inited_ = false;
}

int ObCopyCachedBlockWriter::init(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    ObCopyCachedBlockReader *reader)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObCopyCachedBlockWriter init twice", KR(ret));
  } else if (OB_UNLIKELY((OB_INVALID_TENANT_ID == tenant_id) || !ls_id.is_valid()) ||
             OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ls_id), KP(reader));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    reader_ = reader;
    is_inited_ = true;
  }
  return ret;
}

int ObCopyCachedBlockWriter::process()
{
  int ret = OB_SUCCESS;
  ObHAFetchLocalCacheBlockMeta meta;
  ObBufferReader data(nullptr, 0, 0);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCopyCachedBlockWriter not init", KR(ret));
  } else {
    bool is_iter_end = false;
    while (OB_SUCC(ret) && !is_iter_end) {
      meta.reset();
      if (OB_FAIL(reader_->get_next_cached_block(meta, data))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS; // ignore ret
          is_iter_end = true;
          LOG_INFO("get next cached block end", KR(ret), K_(tenant_id), K_(ls_id));
        } else {
          LOG_WARN("fail to get next cached block", KR(ret), K_(tenant_id), K_(ls_id));
        }
      } else if (OB_FAIL(write_cache(meta, data))) {
        LOG_WARN("fail to write cache", KR(ret), K_(tenant_id), K_(ls_id), K(meta), K(data));
      }
    }
  }
  return ret;
}

int ObCopyCachedBlockWriter::write_cache(
    const ObHAFetchLocalCacheBlockMeta &meta,
    const ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta", KR(ret), K(meta));
  } else if (ObHACacheJobType::MACRO_CACHE_JOB_TYPE == meta.job_type_) {
    if (OB_FAIL(write_macro_cache(meta.macro_prewarm_meta_, data))) {
      LOG_WARN("fail to write macro cache", KR(ret), K(meta), K(data));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected job type", KR(ret), K(meta));
  }
  return ret;
}

int ObCopyCachedBlockWriter::write_macro_cache(
    const ObSSMacroPrewarmMeta &macro_prewarm_meta,
    const ObBufferReader &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_prewarm_meta.is_valid() || OB_ISNULL(data.data()) ||
                  (data.length() < 0) || (data.length() > data.capacity()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(macro_prewarm_meta), K(data));
  } else if (is_macro_read_cache_disabled()) {
    LOG_TRACE("macro read cache is disabled, no need to prewarm", K(macro_prewarm_meta));
  } else {
    ObStorageObjectWriteInfo write_info;
    write_info.buffer_ = data.data();
    write_info.offset_ = 0;
    write_info.size_ = macro_prewarm_meta.size_;
    write_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
    write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
    write_info.mtl_tenant_id_ = MTL_ID();
    write_info.set_is_write_cache(false); // prewarm macro is read cache
    write_info.set_effective_tablet_id(macro_prewarm_meta.effective_tablet_id_);
    ObStorageObjectHandle write_handle;
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(ObSSObjectAccessUtil::async_write_file_to_macro_cache(
        true/*use_effective_tablet_id*/, macro_prewarm_meta.macro_id_, write_info, write_handle))) {
      LOG_WARN("fail to async write file to macro cache", KR(tmp_ret), K(write_info),
               K(macro_prewarm_meta), K(write_handle));
    } else if (OB_TMP_FAIL(write_handle.wait())) {
      LOG_WARN("fail to wait write handle", KR(tmp_ret), K(write_handle));
    } else {
      inc_local_cache_prewarm_stat(
          ObSSLocalCachePrewarmType::MIGRATION, 1/*delta_cnt*/, write_info.size_);
    }
  }
  return ret;
}

} /* namespace storage */
} /* namespace oceanbase */
