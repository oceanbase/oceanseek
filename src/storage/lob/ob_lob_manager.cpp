/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/ob_define.h"
#define USING_LOG_PREFIX STORAGE

#include "lib/allocator/page_arena.h"
#include "lib/ob_errno.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/worker.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "share/ob_device_manager.h"
#include "storage/lob/access/ob_lob_iterator.h"
#include "storage/lob/common/ob_lob_context.h"
#include "storage/lob/common/ob_lob_util.h"

#include "ob_lob_manager.h"
#include "observer/ob_server.h"
#include "storage/lob/common/ob_lob_location.h"
#include "storage/lob/ob_lob_handler.h"
#include "storage/lob/common/ob_lob_locator.h"
#include "storage/lob/ob_lob_tablet_dml.h"
#include "storage/lob/store/ob_lob_persistent_reader_cache.h"

namespace oceanbase
{
namespace storage
{

const ObLobCommon ObLobManager::ZERO_LOB = ObLobCommon();

int ObLobManager::mtl_new(ObLobManager *&m) {
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  auto attr = SET_USE_500("LobManager");
  m = OB_NEW(ObLobManager, attr, tenant_id);
  if (OB_ISNULL(m)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(tenant_id));
  }
  return ret;
}

void ObLobManager::mtl_destroy(ObLobManager *&m)
{
  if (OB_UNLIKELY(nullptr == m)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "meta mem mgr is nullptr", KP(m));
  } else {
    OB_DELETE(ObLobManager, oceanbase::ObModIds::OMT_TENANT, m);
    m = nullptr;
  }
}

int ObLobManager::mtl_init(ObLobManager *&m)
{
  return m->init();
}

int ObLobManager::init()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  lib::ObMemAttr mem_attr(tenant_id, "LobAllocator", ObCtxIds::LOB_CTX_ID);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLobManager init twice.", K(ret));
  } else if (OB_FAIL(allocator_.init(lib::ObMallocAllocator::get_instance(), OB_MALLOC_MIDDLE_BLOCK_SIZE, mem_attr))) {
    LOG_WARN("init allocator failed.", K(ret));
  } else if (OB_FAIL(throttle_tool_.init(&ext_info_log_allocator_))) {
    LOG_WARN("init throttle_tool fail", K(ret));
  } else if (OB_FAIL(ext_info_log_allocator_.init(&throttle_tool_))) {
    LOG_WARN("init ext info log fail", K(ret));
  } else if (OB_FAIL(meta_manager_.init(&adaptor_))) {
    LOG_WARN("init lob meta manager fail", K(ret));
  } else if (OB_FAIL(piece_manager_.init(&adaptor_))) {
    LOG_WARN("init lob piece manager fail", K(ret));
  } else {
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

int ObLobManager::start()
{
  int ret = OB_SUCCESS;
  // TODO
  return ret;
}

int ObLobManager::stop()
{
  STORAGE_LOG(INFO, "[LOB]stop");
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLobManager is not initialized", K(ret));
  } else {
    // TODO
    // 1. 触发LobOperator中内存数据的异步flush
    // 2. 清理临时LOB
  }
  return ret;
}

void ObLobManager::wait()
{
  STORAGE_LOG(INFO, "[LOB]wait");
  // TODO
  // 1. 等待LobOperator中内存数据的异步flush完成
}

void ObLobManager::destroy()
{
  STORAGE_LOG(INFO, "[LOB]destroy");
  // TODO
  // 1. LobOperator.destroy()
  allocator_.reset();
  is_inited_ = false;
}

// Only use for default lob col val
int ObLobManager::fill_lob_header(ObIAllocator &allocator, ObString &data, ObString &out)
{
  int ret = OB_SUCCESS;
  void *buf = allocator.alloc(data.length() + sizeof(ObLobCommon));
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for lob data", K(ret), K(data));
  } else {
    ObLobCommon *lob_data = new(buf) ObLobCommon();
    MEMCPY(lob_data->buffer_, data.ptr(), data.length());
    out.assign_ptr(reinterpret_cast<char *>(buf), data.length() + sizeof(ObLobCommon));
  }
  return ret;
}

// Only use for default lob col val.
int ObLobManager::fill_lob_header(
    ObIAllocator &allocator,
    ObStorageDatum &datum)
{
  int ret = OB_SUCCESS;
  if (datum.is_null() || datum.is_nop_value()) {
  } else {
    ObString data = datum.get_string();
    ObString out;
    if (OB_FAIL(ObLobManager::fill_lob_header(allocator, data, out))) {
      LOG_WARN("failed to fill lob header for column.", K(data));
    } else {
      datum.set_string(out);
    }
  }
  return ret;
}

// Only use for default lob col val
int ObLobManager::fill_lob_header(ObIAllocator &allocator,
    const ObIArray<share::schema::ObColDesc> &column_ids,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    if (column_ids.at(i).col_type_.is_lob_storage()) {
      if (OB_FAIL(fill_lob_header(allocator, datum_row.storage_datums_[i]))) {
        LOG_WARN("failed to fill lob header for column.", K(i), K(column_ids), K(datum_row));
      }
    }
  }
  return ret;
}

int ObLobManager::normalize_to_disk_inrow_locator(ObObj &obj)
{
  int ret = OB_SUCCESS;
  if (obj.is_null() || obj.is_nop_value() || !obj.is_lob_storage()) {
    // nothing to do
  } else {
    ObLobLocatorV2 locator(obj.get_string());
    if (locator.is_lob_disk_locator()) {
      // Case 1: already disk locator inrow - validate and pass through
      const ObLobCommon *disk_loc = reinterpret_cast<const ObLobCommon *>(obj.get_string().ptr());
      if (OB_ISNULL(disk_loc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null disk locator", K(ret), K(obj));
      } else if (OB_UNLIKELY(!disk_loc->in_row_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("default value should not be outrow disk locator", K(ret), K(obj));
      }
    } else {
      // Case 2: mem locator - extract the embedded disk locator and replace buffer
      ObString disk_loc_buf;
      if (OB_FAIL(locator.get_disk_locator(disk_loc_buf))) {
        LOG_WARN("get disk locator failed", K(ret), K(obj));
      } else if (OB_UNLIKELY(disk_loc_buf.length() < static_cast<int32_t>(sizeof(ObLobCommon)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("disk locator buffer too small", K(ret), K(obj), K(disk_loc_buf.length()));
      } else {
        const ObLobCommon *disk_loc = reinterpret_cast<const ObLobCommon *>(disk_loc_buf.ptr());
        if (OB_UNLIKELY(!disk_loc->in_row_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("default value should not be outrow", K(ret), K(obj));
        } else {
          obj.set_string(obj.get_type(), disk_loc_buf);
        }
      }
    }
  }
  return ret;
}

int ObLobManager::equal(ObLobLocatorV2 &lob_left,
                        ObLobLocatorV2 &lob_right,
                        ObLobCompareParams &cmp_params,
                        bool &result)
{
  INIT_SUCC(ret);
  uint64_t old_len = 0;
  uint64_t new_len = 0;
  int64_t cmp_res = 0;
  if (OB_FAIL(get_byte_length(lob_left, old_len))) {
    LOG_WARN("fail to get old byte len", K(ret), K(lob_left));
  } else if (OB_FAIL(get_byte_length(lob_right, new_len))) {
    LOG_WARN("fail to get new byte len", K(ret), K(lob_right));
  } else if (new_len != old_len) {
    result = false;
  } else if (lob_left.has_inrow_data() && lob_right.has_inrow_data()) {
    // do both inrow check
    ObString left_str;
    ObString right_str;
    if (OB_FAIL(lob_left.get_inrow_data(left_str))) {
      LOG_WARN("fail to get old inrow data", K(ret), K(lob_left));
    } else if (OB_FAIL(lob_right.get_inrow_data(right_str))) {
      LOG_WARN("fail to get new inrow data", K(ret), K(lob_left));
    } else {
      result = (0 == MEMCMP(left_str.ptr(), right_str.ptr(), left_str.length()));
    }
  } else if (OB_FAIL(compare(lob_left, lob_right, cmp_params, cmp_res))) {
    LOG_WARN("fail to compare lob", K(ret), K(lob_left), K(lob_right));
  } else {
    result = (0 == cmp_res);
  }
  return ret;
}

int ObLobManager::compare(ObLobLocatorV2 &lob_left,
                          ObLobLocatorV2 &lob_right,
                          ObLobCompareParams &cmp_params,
                          int64_t &result) {
  INIT_SUCC(ret);
  ObArenaAllocator tmp_allocator("LobCmp", OB_MALLOC_MIDDLE_BLOCK_SIZE, MTL_ID());
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get lob manager handle null.", K(ret));
  } else if(!lob_left.is_valid() || !lob_right.is_valid()) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid lob. should have lob locator", K(ret));
  } else {
    ObLobReadContext read_ctx_left;
    ObLobReadContext read_ctx_right;
    if (OB_FAIL(build_lob_read_context(read_ctx_left, tmp_allocator, cmp_params.collation_left_,
                cmp_params.offset_left_, cmp_params.compare_len_, cmp_params.timeout_, lob_left,
                nullptr/*tx_desc*/, nullptr/*lob_access_ctx*/))) {
      LOG_WARN("fail to build read context left", K(ret), K(lob_left), K(cmp_params));
    } else if(OB_FAIL(build_lob_read_context(read_ctx_right, tmp_allocator, cmp_params.collation_right_,
                cmp_params.offset_right_, cmp_params.compare_len_, cmp_params.timeout_, lob_right,
                nullptr/*tx_desc*/, nullptr/*lob_access_ctx*/))) {
      LOG_WARN("fail to build read context right", K(ret), K(lob_right));
    } else if(OB_FAIL(compare(read_ctx_left, read_ctx_right, result))) {
      LOG_WARN("fail to compare lob", K(ret), K(lob_left), K(lob_right), K(cmp_params));
    }
  }
  return ret;
}

int ObLobManager::compare(ObLobReadContext &param_left,
                          ObLobReadContext &param_right,
                          int64_t &result) {
  INIT_SUCC(ret);
  result = 0;
  common::ObCollationType collation_left = param_left.coll_type_;
  common::ObCollationType collation_right = param_right.coll_type_;
  common::ObCollationType cmp_collation = collation_left;
  ObIAllocator *tmp_allocator = param_left.allocator_;
  ObLobQueryIter *iter_left = nullptr;
  ObLobQueryIter *iter_right = nullptr;
  if(OB_ISNULL(tmp_allocator)) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid alloctor param", K(ret), K(param_left));
  } else if((collation_left == CS_TYPE_BINARY && collation_right != CS_TYPE_BINARY)
            || (collation_left != CS_TYPE_BINARY && collation_right == CS_TYPE_BINARY)) {
    ret = OB_ERR_ARG_INVALID;
    LOG_WARN("invalid collation param", K(ret), K(param_left), K(param_right));
  } else if (OB_FAIL(query(param_left, iter_left))) {
    LOG_WARN("query param left by iter failed.", K(ret), K(param_left));
  } else if (OB_FAIL(query(param_right, iter_right))) {
    LOG_WARN("query param right by iter failed.", K(ret), K(param_right));
  } else {
    uint64_t read_buff_size = OB_MALLOC_MIDDLE_BLOCK_SIZE; // 64KB
    char *read_buff = nullptr;
    char *charset_convert_buff_ptr = nullptr;
    bool need_convert_charset = (collation_left != CS_TYPE_BINARY);
    uint64_t charset_convert_buff_size = need_convert_charset ?
                                         read_buff_size * ObCharset::CharConvertFactorNum : 0;

    if (OB_ISNULL((read_buff = static_cast<char*>(tmp_allocator->alloc(read_buff_size * 2))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc read buffer failed.", K(ret), K(read_buff_size));
    } else if (need_convert_charset &&
               OB_ISNULL((charset_convert_buff_ptr = static_cast<char*>(tmp_allocator->alloc(charset_convert_buff_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc charset convert buffer failed.", K(ret), K(charset_convert_buff_size));
    } else {
      ObDataBuffer charset_convert_buff(charset_convert_buff_ptr, charset_convert_buff_size);
      ObString read_buffer_left;
      ObString read_buffer_right;
      read_buffer_left.assign_buffer(read_buff, read_buff_size);
      read_buffer_right.assign_buffer(read_buff + read_buff_size, read_buff_size);

      // compare right after charset convert
      ObString convert_buffer_right;
      convert_buffer_right.assign_ptr(nullptr, 0);

      while (OB_SUCC(ret) && result == 0) {
        if (read_buffer_left.length() == 0) {
          // reset buffer and read next block
          read_buffer_left.assign_buffer(read_buff, read_buff_size);
          if (OB_FAIL(iter_left->get_next_row(read_buffer_left))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next buffer for left lob.", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          }
        }

        if (OB_SUCC(ret) && convert_buffer_right.length() == 0) {
          read_buffer_right.assign_buffer(read_buff + read_buff_size, read_buff_size);
          charset_convert_buff.set_data(charset_convert_buff_ptr, charset_convert_buff_size);
          convert_buffer_right.assign_ptr(nullptr, 0);

          if (OB_FAIL(iter_right->get_next_row(read_buffer_right))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("failed to get next buffer for right lob", K(ret));
            } else {
              ret = OB_SUCCESS;
            }
          } else if (need_convert_charset) {
            // convert right lob to left charset if necessary
            if(OB_FAIL(ObExprUtil::convert_string_collation(
                                  read_buffer_right, collation_right,
                                  convert_buffer_right, cmp_collation,
                                  charset_convert_buff))) {
                LOG_WARN("fail to convert string collation", K(ret),
                          K(read_buffer_right), K(collation_right),
                          K(convert_buffer_right), K(cmp_collation));
            }
          } else {
            convert_buffer_right.assign_ptr(read_buffer_right.ptr(), read_buffer_right.length());
          }
        }
        if (OB_SUCC(ret)) {
          if (read_buffer_left.length() == 0 && convert_buffer_right.length() == 0) {
            result = 0;
            ret = OB_ITER_END;
          } else if (read_buffer_left.length() == 0 && convert_buffer_right.length() > 0) {
            result = -1;
          } else if (read_buffer_left.length() > 0 && convert_buffer_right.length() == 0) {
            result = 1;
          } else {
            uint64_t cmp_len = read_buffer_left.length() > convert_buffer_right.length() ?
                                    convert_buffer_right.length() : read_buffer_left.length();
            ObString substr_lob_left;
            ObString substr_lob_right;
            substr_lob_left.assign_ptr(read_buffer_left.ptr(), cmp_len);
            substr_lob_right.assign_ptr(convert_buffer_right.ptr(), cmp_len);
            result = common::ObCharset::strcmp(cmp_collation, substr_lob_left, substr_lob_right);
            if (result > 0) {
              result = 1;
            } else if (result < 0) {
              result = -1;
            }

            read_buffer_left.assign_ptr(read_buffer_left.ptr() + cmp_len, read_buffer_left.length() - cmp_len);
            convert_buffer_right.assign_ptr(convert_buffer_right.ptr() + cmp_len, convert_buffer_right.length() - cmp_len);
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }
    if (OB_NOT_NULL(read_buff)) {
      tmp_allocator->free(read_buff);
    }
    if (OB_NOT_NULL(charset_convert_buff_ptr)) {
      tmp_allocator->free(charset_convert_buff_ptr);
    }
  }
  if (OB_NOT_NULL(iter_left)) {
    iter_left->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter_left);
  }
  if (OB_NOT_NULL(iter_right)) {
    iter_right->reset();
    OB_DELETE(ObLobQueryIter, "unused", iter_right);
  }
  return ret;
}

int ObLobManager::build_lob_read_context(
    ObLobReadContext &read_ctx,
    ObIAllocator &allocator,
    ObCollationType coll_type,
    uint64_t offset,
    uint64_t len,
    int64_t timeout,
    ObLobLocatorV2 &lob,
    transaction::ObTxDesc *tx_desc,
    ObLobAccessCtx *lob_access_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(read_ctx.build(lob, allocator, offset, len, tx_desc))) {
    LOG_WARN("build read context from locator failed",
             K(ret), K(lob), K(coll_type), K(offset), K(len), K(timeout));
  } else {
    // Transactions need the LOCATION provider; auxiliary table scan decides reader reuse.
    read_ctx.access_ctx_ = lob_access_ctx;
    read_ctx.coll_type_ = coll_type;
    read_ctx.timeout_ = timeout;
    if (lob.has_inrow_data()) {
      int64_t byte_size = 0;
      if (OB_FAIL(lob.get_lob_inrow_data_byte_len(byte_size))) {
        LOG_WARN("failed to get lob byte length", KR(ret), K(lob));
      } else {
        read_ctx.byte_size_ = byte_size;
      }
    } else if (OB_FAIL(get_byte_length(read_ctx, read_ctx.byte_size_))) {
      LOG_WARN("failed to get outrow lob byte length", K(ret), K(read_ctx));
    }
  }
  return ret;
}

int ObLobManager::build_lob_write_context(ObLobWriteContext &write_ctx,
                                          ObIAllocator &allocator,
                                          ObCollationType coll_type,
                                          uint64_t offset,
                                          uint64_t len,
                                          int64_t timeout,
                                          ObLobLocatorV2 &lob)
{
  int ret = OB_SUCCESS;
  ObLobReadContext source;
  if (OB_FAIL(build_lob_read_context(source, allocator, coll_type, offset, len,
                                     timeout, lob, write_ctx.tx_desc_, nullptr/*lob_access_ctx*/))) {
    LOG_WARN("failed to build source lob context", K(ret), K(lob));
  } else if (OB_FAIL(write_ctx.assign_source(source))) {
    LOG_WARN("failed to assign source lob context", K(ret), K(source));
  }
  return ret;
}

// 基于 locator 去查询 lob 内容，如果 locator 中包含 inrow data，则可直接获取
// 若不包含，locator 需要是 mem locator，并且包含必要的 external 信息来调用 query 查询
// 需注意 data 要提前分配内存
int ObLobManager::query(ObLobLocatorV2 &loc, ObString &data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!loc.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid locator", KR(ret), K(loc));
  } else if (loc.has_inrow_data()) {
    if (OB_FAIL(loc.get_inrow_data(data))) {
      LOG_WARN("failed to get inrow data", KR(ret), K(loc));
    }
  } else {
    ObLobReadContext read_ctx;
    ObArenaAllocator allocator;
    if (OB_FAIL(read_ctx.build(loc, allocator, 0, UINT64_MAX))) {
      LOG_WARN("failed to build read context", KR(ret), K(loc));
    } else if (OB_FAIL(query(read_ctx, data))) {
      LOG_WARN("failed to query lob", KR(ret), K(read_ctx));
    }
  }
  return ret;
}

int ObLobManager::query(ObLobReadContext &read_ctx, ObString &buffer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (read_ctx.src_loc_.has_inrow_data()) {
    ObString data;
    if (OB_FAIL(read_ctx.src_loc_.get_inrow_data(data))) {
      LOG_WARN("faield to get inrow data", KR(ret), K(read_ctx.src_loc_), K(read_ctx));
    } else {
      uint64_t offset = read_ctx.offset_;
      uint64_t length = read_ctx.len_;
      uint64_t byte_offset = 0;
      uint64_t byte_length = 0;

      if (read_ctx.is_char()) {
        ObLobUtil::convert_char_range_to_byte_range(
            read_ctx.coll_type_, data.ptr(), data.length(), offset, length, byte_offset, byte_length);
      } else {
        ObLobUtil::adjust_range(data.length(), offset, length);
        byte_offset = offset;
        byte_length = length;
      }

      if (OB_UNLIKELY(data.length() < byte_offset + byte_length)) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("data length is not enough.", K(ret), K(read_ctx.src_loc_), K(byte_offset), K(byte_length));
      } else if (read_ctx.inrow_read_nocopy_) {
        buffer.assign_ptr(data.ptr() + byte_offset, byte_length);
      } else if (buffer.write(data.ptr() + byte_offset, byte_length) != byte_length) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to write buffer to output_data.", K(ret), K(byte_offset), K(byte_length));
      }
    }
  } else {
    ObLobQueryDataHandler handler(read_ctx, buffer);
    if (OB_FAIL(handler.init(&meta_manager_))) {
      LOG_WARN("failed to init handler", KR(ret), K(read_ctx));
    } else if (OB_FAIL(handler.execute())) {
      LOG_WARN("failed execute handler", KR(ret), K(read_ctx));
    } else {
      EVENT_INC(ObStatEventIds::OUTROW_LOB_CNT);
    }
  }
  return ret;
}

int ObLobManager::query(ObLobReadContext &read_ctx, ObLobQueryIter *&iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (read_ctx.src_loc_.has_inrow_data()) {
    ObString data;
    if (OB_FAIL(read_ctx.src_loc_.get_inrow_data(data))) {
      LOG_WARN("failed get inrow data", KR(ret), K(read_ctx));
    } else {
      uint64_t offset = read_ctx.offset_;
      uint64_t length = read_ctx.len_;
      uint64_t byte_offset = 0;
      uint64_t byte_length = 0;

      if (0 == offset && length >= static_cast<uint64_t>(data.length())) {
        byte_offset = 0;
        byte_length = data.length();
      } else if (read_ctx.is_char()) {
        ObLobUtil::convert_char_range_to_byte_range(
            read_ctx.coll_type_, data.ptr(), data.length(), offset, length, byte_offset, byte_length);
      } else {
        ObLobUtil::adjust_range(data.length(), offset, length);
        byte_offset = offset;
        byte_length = length;
      }

      ObLobInRowQueryIter *inrow_iter = OB_NEW(ObLobInRowQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter"));
      if (OB_ISNULL(inrow_iter)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed alloc memory", KR(ret), KP(inrow_iter));
      } else if (OB_FAIL(inrow_iter->open(
          data, byte_offset, byte_length, read_ctx.coll_type_, read_ctx.scan_backward_))) {
        LOG_WARN("failed open inrow iter", KR(ret), K(byte_offset), K(byte_length), K(read_ctx));
      } else {
        iter = inrow_iter;
      }
    }
  } else {
    ObLobQueryIterHandler handler(read_ctx);
    if (OB_FAIL(handler.init(&meta_manager_))) {
      LOG_WARN("failed init handler", KR(ret), K(read_ctx));
    } else if (OB_FAIL(handler.execute())) {
      LOG_WARN("failed execute handler", KR(ret), K(read_ctx));
    } else {
      iter = handler.result_;
      EVENT_INC(ObStatEventIds::OUTROW_LOB_CNT);
    }
  }
  return ret;
}

int ObLobManager::query(ObString &str, ObLobQueryIter *&iter)
{
  int ret = OB_SUCCESS;
  ObLobInRowQueryIter *inrow_iter = OB_NEW(ObLobInRowQueryIter, ObMemAttr(MTL_ID(), "LobQueryIter"));
  if (OB_ISNULL(inrow_iter)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed alloc inrow iter", KR(ret));
  } else if (OB_FAIL(inrow_iter->open(str, 0, str.length(), CS_TYPE_BINARY, false))) {
    LOG_WARN("failed open inrow iter", KR(ret), K(str.length()));
  } else {
    iter = inrow_iter;
  }
  return ret;
}

int ObLobManager::insert_before(ObLobInsertContext &ctx, bool &outrow)
{
  int ret = OB_SUCCESS;
  outrow = false;
  uint64_t byte_len = 0;
  bool is_inrow_lob = false;
  if (ctx.src_loc_.is_mem_external_lob()) {
    // external lob do not need a true byte_length
  } else if (OB_FAIL(get_byte_length(ctx.src_loc_, byte_len))) {
    LOG_WARN("fail to get lob byte len", KR(ret), K(ctx));
  } else if (FALSE_IT(ctx.byte_size_ = byte_len)) {
  } else if (byte_len <= ctx.get_inrow_threshold()) {
    is_inrow_lob = true;
  }
  if (OB_FAIL(ret)) {
  } else if (is_inrow_lob) {
    // inrow case
  } else if (ctx.is_mlog_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "columns more than lob inrow threshold in materialized view log is");
    LOG_WARN("columns more than lob inrow threshold in materialized view log is not supported", KR(ret), K(byte_len), K(ctx.get_inrow_threshold()), K(lbt()));
  } else if (ctx.is_index_table_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "outrow lob in index table");
    LOG_WARN("outrow lob in index table is not supported", KR(ret), K(ctx));
  } else if (OB_FAIL(prepare_outline_lob_locator_(ctx, ObLobId()))) {
    LOG_WARN("failed to prepare outrow locator", KR(ret), K(ctx));
  } else {
    outrow = true;
  }
  return ret;
}

int ObLobManager::update_before(ObLobUpdateContext &ctx, bool &outrow)
{
  int ret = OB_SUCCESS;
  outrow = false;
  uint64_t byte_len = 0;
  bool is_inrow_lob = false;
  if (ctx.src_loc_.is_mem_external_lob()) {
    // external lob do not need a true byte_length
  } else if (OB_FAIL(get_byte_length(ctx.src_loc_, byte_len))) {
    LOG_WARN("fail to get lob byte len", KR(ret), K(ctx));
  } else if (FALSE_IT(ctx.byte_size_ = byte_len)) {
  } else if (byte_len <= ctx.get_inrow_threshold()) {
    is_inrow_lob = true;
  }
  if (OB_FAIL(ret)) {
  } else if (is_inrow_lob) {
    // new value is inrow (null->inrow or inrow->inrow), nothing to do.
    // old outrow -> new inrow is forbidden by the upper layer, so old must be inrow/null here.
  } else if (ctx.is_mlog_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "columns more than lob inrow threshold in materialized view log is");
    LOG_WARN("columns more than lob inrow threshold in materialized view log is not supported", KR(ret), K(byte_len), K(ctx.get_inrow_threshold()), K(lbt()));
  } else if (ctx.is_index_table_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "outrow lob in index table");
    LOG_WARN("outrow lob in index table is not supported", KR(ret), K(ctx));
  } else if (OB_FAIL(prepare_outline_lob_locator_(ctx, ctx.lob_id_))) {
    // ctx.lob_id_ valid: outrow->outrow, reuse old lob id;
    // ctx.lob_id_ invalid: null/inrow->outrow, alloc new lob id.
    LOG_WARN("failed to prepare outrow locator", KR(ret), K(ctx));
  } else {
    outrow = true;
  }
  return ret;
}

int ObLobManager::append_before(ObLobAppendContext &ctx, bool &outrow)
{
  int ret = OB_SUCCESS;
  const ObMemLobExternHeader *extern_header = nullptr;
  ObString append_data;
  ObString old_data;

  uint64_t old_byte_len = 0;
  const bool old_is_outrow = ctx.old_loc_.is_outline_lob_disk_locator();
  ObLobReadContext read_ctx;
  ObLobMetaExtraInfo extra_info;
  ObLobDiskLocatorBuilder loc_builder;
  outrow = false;
  if (OB_FAIL(ctx.delta_loc_.get_append_delta(extern_header, append_data))) {
    LOG_WARN("failed to parse append delta", KR(ret), K(ctx));
  } else if (ctx.is_main_table_rowkey_col_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "LOB_WRITEAPPEND on a rowkey column");
    LOG_WARN("lob append on rowkey column is not supported", KR(ret), K(ctx));
  } else if (ctx.old_loc_.has_inrow_data()) {
    if (OB_FAIL(ctx.old_loc_.get_inrow_data(old_data))) {
      LOG_WARN("failed to get old inrow data", KR(ret), K(ctx));
    } else {
      old_byte_len = old_data.length();
    }
  } else if (OB_FAIL(ctx.build_read_context(read_ctx))) {
    LOG_WARN("failed to build append read context", KR(ret), K(ctx));
  } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_6_3_1) {
    ObLobMetaInfo summary;
    if (OB_ISNULL(read_ctx.allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("lob summary allocator is null", KR(ret), K(read_ctx.tablet_id_));
    } else if (OB_FAIL(get_lob_summary_info(read_ctx, summary))) {
      LOG_WARN("failed to read append target summary", KR(ret), K(read_ctx.tablet_id_), K(read_ctx.store_column_idx_));
    } else if (OB_FAIL(extra_info.deserialize(*read_ctx.allocator_, summary.lob_extra_info_))) {
      LOG_WARN("failed to deserialize append target summary", KR(ret), K(summary.lob_id_));
    } else if (OB_UNLIKELY(ObLobMetaExtraInfo::LOB_META_SUMMARY != extra_info.extra_info_type_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid append target summary type", KR(ret), K(extra_info.extra_info_type_));
    } else if (OB_UNLIKELY(extra_info.is_external_lob())) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "appending an external LOB");
      LOG_WARN("appending an external lob is not supported", KR(ret), K(ctx.tablet_id_),
              "append_length", append_data.length());
    } else {
      old_byte_len = summary.byte_len_;
    }
  } else if (OB_FAIL(get_byte_length(read_ctx, old_byte_len))) {
    LOG_WARN("failed to get old outrow lob length", KR(ret), K(read_ctx));
  }

  if (OB_FAIL(ret)) {
  } else if (append_data.empty()) {
    ctx.dest_loc_ = ctx.old_loc_;
  }

  const uint64_t append_byte_len = static_cast<uint64_t>(append_data.length());
  uint64_t total_byte_len = 0;
  if (OB_FAIL(ret) || append_data.empty()) {
  } else if (OB_UNLIKELY(old_byte_len > LOB_WRITEAPPEND_MAX_BYTE_LEN
                         || append_byte_len > LOB_WRITEAPPEND_MAX_BYTE_LEN - old_byte_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_USER_ERROR(OB_SIZE_OVERFLOW);
    LOG_WARN("LOB_WRITEAPPEND exceeds maximum accumulated LOB size",
             KR(ret), K(old_byte_len), K(append_byte_len),
             K(LOB_WRITEAPPEND_MAX_BYTE_LEN));
  } else if (FALSE_IT(total_byte_len = old_byte_len + append_byte_len)) {
  } else if (FALSE_IT(ctx.byte_size_ = total_byte_len)) {
  } else if (!old_is_outrow
             && total_byte_len <= static_cast<uint64_t>(ctx.get_inrow_threshold())) {
    char *combined = nullptr;
    if (OB_ISNULL(combined = static_cast<char *>(ctx.allocator_->alloc(total_byte_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate inrow append buffer", KR(ret), K(total_byte_len));
    } else {
      if (!old_data.empty()) {
        MEMCPY(combined, old_data.ptr(), old_data.length());
      }
      MEMCPY(combined + old_data.length(), append_data.ptr(), append_data.length());
      if (OB_FAIL(loc_builder.init_inrow(*ctx.allocator_, combined, total_byte_len))) {
        LOG_WARN("failed to build appended inrow locator", KR(ret), K(ctx));
      } else if (OB_FAIL(loc_builder.to_locator(ctx.dest_loc_))) {
        LOG_WARN("failed to get appended inrow locator", KR(ret), K(ctx));
      }
    }
  } else if (ctx.is_mlog_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "outrow LOB_WRITEAPPEND in materialized view log");
    LOG_WARN("outrow lob append in mlog is not supported", KR(ret), K(ctx));
  } else if (ctx.is_index_table_) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "outrow LOB_WRITEAPPEND in index table");
    LOG_WARN("outrow lob append in index table is not supported", KR(ret), K(ctx));
  } else if (OB_FAIL(prepare_outline_lob_locator_(ctx, ctx.lob_id_))) {
    LOG_WARN("failed to prepare append destination locator", KR(ret), K(ctx));
  } else {
    outrow = true;
  }
  return ret;
}

int ObLobManager::insert(ObLobInsertContext &ctx)
{
  int ret = OB_SUCCESS;
  ObLobInserter inserter;
  if (OB_FAIL(inserter.init(&ctx, &meta_manager_, &piece_manager_))) {
    STORAGE_LOG(WARN, "failed to init inserter", KR(ret), K(ctx));
  } else if (OB_FAIL(inserter.insert())) {
    STORAGE_LOG(WARN, "failed to insert", KR(ret), K(ctx));
  }
  return ret;
}

int ObLobManager::append(ObLobAppendContext &ctx)
{
  int ret = OB_SUCCESS;
  ObLobAppender appender;
  if (OB_FAIL(appender.init(&ctx, &meta_manager_, &piece_manager_))) {
    STORAGE_LOG(WARN, "failed to init lob appender", KR(ret), K(ctx));
  } else if (OB_FAIL(appender.append())) {
    STORAGE_LOG(WARN, "failed to append lob", KR(ret), K(ctx));
  }
  return ret;
}

int ObLobManager::del(ObLobDeleteContext &del_ctx)
{
  int ret = OB_SUCCESS;
  ObLobDeleter deleter;
  if (OB_FAIL(deleter.init(&del_ctx, &meta_manager_, &piece_manager_))) {
    LOG_WARN("failed to init eraser", KR(ret), K(del_ctx));
  } else if (OB_FAIL(deleter.del())) {
    LOG_WARN("failed to erase", KR(ret), K(del_ctx));
  }
  return ret;
}

int ObLobManager::get_byte_length(ObLobLocatorV2 &loc, uint64_t &byte_len)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  ObExternalBlobInfo external_info;
  if (OB_UNLIKELY(!loc.is_valid() || loc.is_outline_lob_disk_locator())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid locator", KR(ret), K(loc));
  } else if (loc.has_inrow_data()) {
    if (OB_FAIL(loc.get_lob_inrow_data_byte_len(len))) {
      LOG_WARN("failed to get lob byte length", KR(ret), K(loc));
    } else {
      byte_len = len;
    }
  } else if (loc.is_mem_external_lob()) {
    // TODO by renhe.wz: without access_ctx here, can not do head for external lob object
    // DML uses declared length or 0 if absent; write preparation never accesses remote objects.
    if (OB_FAIL(loc.get_external_lob_info(external_info))) {
      LOG_WARN("failed to get external lob info", KR(ret), K(loc.size_));
    } else {
      byte_len = external_info.length_;
    }
  } else {
    ObArenaAllocator allocator("LobQryLength", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObLobReadContext read_ctx;
    ObLobManager *lob_mgr = nullptr;
    if (OB_ISNULL(lob_mgr = MTL(ObLobManager *))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get lob manager", KR(ret), KP(lob_mgr));
    } else if (OB_FAIL(read_ctx.build(loc, allocator, 0, UINT64_MAX))) {
      LOG_WARN("failed to build read context", KR(ret), K(loc));
    } else if (OB_FAIL(lob_mgr->get_byte_length(read_ctx, byte_len))) {
      LOG_WARN("failed to get lob byte length", KR(ret), K(read_ctx));
    }
  }
  return ret;
}

int ObLobManager::get_char_length(ObLobReadContext &read_ctx, uint64_t &char_len)
{
  int ret = OB_SUCCESS;
  ObLobQueryLengthHandler handler(read_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(read_ctx));
  } else if (OB_UNLIKELY(!read_ctx.src_loc_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to check source lob locator", KR(ret), K(read_ctx.tablet_id_),
             "locator_size", read_ctx.src_loc_.size_);
  } else if (read_ctx.src_loc_.is_mem_external_lob()) {
    // External is always binary, so character length equals byte length.
    if (OB_FAIL(get_external_lob_byte_length_(read_ctx, char_len))) {
      LOG_WARN("get_external_lob_byte_length_ failed", KR(ret), KPC(&read_ctx), K(char_len));
    }
  } else if (OB_FAIL(handler.init(&meta_manager_, ObLobQueryLengthHandler::CHAR_LENGTH))) {
    LOG_WARN("failed to init handler", KR(ret), K(read_ctx));
  } else if (OB_FAIL(handler.execute())) {
    LOG_WARN("failed to execute handler", KR(ret), K(read_ctx));
  } else {
    char_len = handler.result_;
  }
  return ret;
}

int ObLobManager::get_byte_length(ObLobReadContext &read_ctx, uint64_t &byte_len)
{
  int ret = OB_SUCCESS;
  ObLobQueryLengthHandler handler(read_ctx);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(read_ctx));
  } else if (OB_UNLIKELY(!read_ctx.src_loc_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to check source lob locator", KR(ret), K(read_ctx.tablet_id_),
             "locator_size", read_ctx.src_loc_.size_);
  } else if (read_ctx.src_loc_.is_mem_external_lob()) {
    if (OB_FAIL(get_external_lob_byte_length_(read_ctx, byte_len))) {
      LOG_WARN("get_external_lob_byte_length_ failed", KR(ret), KPC(&read_ctx), K(byte_len));
    }
  } else if (OB_FAIL(handler.init(&meta_manager_, ObLobQueryLengthHandler::BYTE_LENGTH))) {
    LOG_WARN("failed to init handler", KR(ret), K(read_ctx));
  } else if (OB_FAIL(handler.execute())) {
    LOG_WARN("failed to execute handler", KR(ret), K(read_ctx));
  } else {
    byte_len = handler.result_;
  }
  return ret;
}

int ObLobManager::get_external_lob_byte_length_(
    const ObLobReadContext &read_ctx,
    uint64_t &byte_len)
{
  int ret = OB_SUCCESS;
  ObExternalBlobInfo info;
  ObString head_etag;
  if (OB_UNLIKELY(read_ctx.src_loc_.is_external_temp_lob())) {
    ret = OB_NOT_SUPPORTED;
    // Reject direct reads of temporary external LOBs and report the error to the user.
    // Check at shared read entry points to avoid duplicating checks across SQL functions.
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "reading a temporary external LOB");
    LOG_WARN("reading a temporary external lob is not supported", KR(ret),
             K(read_ctx.tablet_id_), "locator_size", read_ctx.src_loc_.size_);
  } else if (UINT64_MAX != read_ctx.byte_size_) {
    // Reuse the length resolved for this read without changing the source locator.
    byte_len = read_ctx.byte_size_;
  } else if (OB_FAIL(read_ctx.src_loc_.get_external_lob_info(info))) {
    LOG_WARN("failed to get external lob descriptor", KR(ret),
             K(read_ctx.tablet_id_), "locator_size", read_ctx.src_loc_.size_);
  } else if (info.has_length_) {
    byte_len = info.length_;
  } else {
    share::ObBackupStorageInfo access_info;
    common::ObIODFileStat statbuf(common::get_storage_io_tenant_id());
    int64_t remaining_us;
    if (OB_ISNULL(read_ctx.access_ctx_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "reading external LOB without SQL access context");
      LOG_WARN("external lob access provider is unavailable for length query", KR(ret),
               K(read_ctx.tablet_id_), KP(read_ctx.access_ctx_));
    } else if (OB_FAIL(read_ctx.access_ctx_->get_external_access_info(info.url_, access_info))) {
      LOG_WARN("failed to resolve external lob LOCATION", KR(ret),
               K(read_ctx.tablet_id_), "url_length", info.url_.length());
    } else if (OB_FAIL(THIS_WORKER.check_status())) {
      LOG_WARN("external LOB interrupted before HEAD", KR(ret), K(read_ctx.tablet_id_));
    } else if (FALSE_IT(remaining_us = read_ctx.timeout_
                                       - common::ObTimeUtility::current_time_us())) {
    } else if (OB_UNLIKELY(remaining_us <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("external LOB HEAD timed out", KR(ret), K(read_ctx.tablet_id_), K(read_ctx.timeout_));
    } else if (OB_FAIL(ObExternalIoAdapter::get_file_stat(info.url_, &access_info,
                       statbuf, static_cast<uint64_t>(remaining_us)))) {
      LOG_WARN("external LOB HEAD failed", KR(ret), K(read_ctx.tablet_id_), K(remaining_us));
    } else if (OB_UNLIKELY(statbuf.size_ < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid external object length", KR(ret), K(statbuf.size_), K(read_ctx.tablet_id_));
    } else if (FALSE_IT(head_etag = ObString(statbuf.etag_.length(), statbuf.etag_.ptr()))) {
    // HEAD responses may include double quotes around the ETag.
    } else if (!info.etag_.empty()
               && !info.etag_.compare_equal(ObLobUtil::split_etag_quotes(head_etag))) {
      ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
      LOG_USER_ERROR(OB_OBJECT_STORAGE_CONDITION_NOT_MATCH, "ETag mismatch while reading external LOB");
      LOG_WARN("external object etag does not match", KR(ret), K(read_ctx.tablet_id_), K(info.etag_), K(statbuf.etag_));
    } else {
      byte_len = static_cast<uint64_t>(statbuf.size_);
    }
  }
  return ret;
}

int ObLobManager::get_lob_summary_info(
    ObLobReadContext &read_ctx,
    ObLobMetaInfo &lob_info)
{
  int ret = OB_SUCCESS;
  ObLobQueryInfoHandler handler(read_ctx, lob_info);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("lob manager is not initialized", KR(ret), K(read_ctx));
  } else if (OB_FAIL(handler.init(&meta_manager_))) {
    LOG_WARN("initialize lob query info handler failed", KR(ret), K(read_ctx));
  } else if (OB_FAIL(handler.execute())) {
    LOG_WARN("execute lob query info handler failed", KR(ret), K(read_ctx));
  }
  return ret;
}

int ObLobManager::get_meta_summary_extra_info(
    ObLobReadContext &read_ctx,
    ObLobMetaExtraInfo &extra_info)
{
  int ret = OB_SUCCESS;
  ObLobMetaInfo summary;
  extra_info.reset();
  if (OB_ISNULL(read_ctx.allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("lob summary allocator is null", KR(ret), KP(read_ctx.allocator_), K(read_ctx.tablet_id_));
  } else if (OB_FAIL(get_lob_summary_info(read_ctx, summary))) {
    LOG_WARN("failed to read lob summary", KR(ret), K(read_ctx.tablet_id_), K(read_ctx.store_column_idx_));
  } else if (OB_FAIL(extra_info.deserialize(*read_ctx.allocator_, summary.lob_extra_info_))) {
    LOG_WARN("failed to deserialize lob summary", KR(ret), K(summary.lob_id_),
             "extra_info_length", summary.lob_extra_info_.length());
  } else if (OB_UNLIKELY(ObLobMetaExtraInfo::LOB_META_SUMMARY != extra_info.extra_info_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob summary type", KR(ret), K(summary.lob_id_), K(extra_info.extra_info_type_));
  }
  return ret;
}

int ObLobManager::prepare_outline_lob_locator_(
    ObLobInsertContext &insert_ctx,
    const ObLobId &reuse_lob_id)
{
  int ret = OB_SUCCESS;
  ObLobDiskLocatorBuilder loc_builder;
  if (OB_FAIL(loc_builder.init_outrow(*insert_ctx.allocator_))) {
    LOG_WARN("prepare_locator fail", K(ret), K(insert_ctx));
  } else if (OB_FAIL(prepare_lob_id_(insert_ctx, loc_builder, reuse_lob_id))) {
    LOG_WARN("prepare_lob_id fail", K(ret), K(insert_ctx));
  } else if (OB_FAIL(prepare_seq_no_(insert_ctx, loc_builder))) {
    LOG_WARN("failed prepare seq no", KR(ret), K(insert_ctx), K(loc_builder));
  } else if (OB_FAIL(loc_builder.to_locator(insert_ctx.dest_loc_))) {
    LOG_WARN("to locator fail", K(ret), K(loc_builder), K(insert_ctx));
  } else {
    LOG_DEBUG("prepare disk lob locator success", K(loc_builder));
  }
  return ret;
}

int ObLobManager::prepare_lob_id_(ObLobInsertContext &insert_ctx,
                                  ObLobDiskLocatorBuilder &locator_builder,
                                  const ObLobId &reuse_lob_id)
{
  int ret = OB_SUCCESS;
  ObLobId lob_id = reuse_lob_id;
  if (lob_id.is_valid()) {
    // outrow -> outrow update, reuse old lob id
  } else if (OB_FAIL(alloc_lob_id_(insert_ctx, lob_id))) {
    LOG_WARN("alloc lob id fail", K(ret), K(insert_ctx));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(locator_builder.set_lob_id(lob_id))) {
    LOG_WARN("set lob id fail", K(ret), K(lob_id), K(locator_builder), K(insert_ctx));
  } else {
    LOG_DEBUG("prepare lob id success", K(lob_id), K(locator_builder));
  }
  return ret;
}

int ObLobManager::alloc_lob_id_(ObLobInsertContext &insert_ctx, ObLobId &lob_id)
{
  int ret = OB_SUCCESS;
  lob_id.reset();
  lob_id.set_column_idx(insert_ctx.store_column_idx_);
  if (insert_ctx.is_table_without_pk_) {
    if (OB_ISNULL(insert_ctx.data_row_) || OB_UNLIKELY(insert_ctx.data_row_->get_column_count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data_row is null for heap table lob alloc", K(ret), K(insert_ctx));
    } else {
      lob_id.set_tablet_id(insert_ctx.tablet_id_.id());
      lob_id.set_auto_inc_id(insert_ctx.data_row_->storage_datums_[0].get_uint64());
    }
  } else {
    uint64_t tablet_inc_id = 0;
    // TODO by renhe: fetch_tablet_inc_id may alloc tablet_inc_id from splited new tablet_id,
    //                instead of insert_ctx.lob_meta_tablet_id_.id()
    if (OB_FAIL(ObLobUtil::fetch_tablet_inc_id(insert_ctx, tablet_inc_id))) {
      LOG_WARN("get tablet inc id failed", K(ret), K(insert_ctx));
    } else if (!insert_ctx.lob_meta_tablet_id_.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("lob_meta_tablet_id is invalid", K(ret), K(insert_ctx));
    } else {
      lob_id.set_tablet_id(insert_ctx.lob_meta_tablet_id_.id());
      lob_id.set_auto_inc_id(tablet_inc_id);
    }
  }
  return ret;
}

int ObLobManager::prepare_seq_no_(ObLobInsertContext &insert_ctx, ObLobDiskLocatorBuilder &locator_builder)
{
  int ret = OB_SUCCESS;
  transaction::ObTxSEQ seq_no;
  if (OB_UNLIKELY(!insert_ctx.is_valid()) || OB_ISNULL(insert_ctx.tx_desc_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(insert_ctx), K(locator_builder));
  } else if (OB_FAIL(insert_ctx.tx_desc_->inc_and_get_tx_seq(insert_ctx.parent_seq_no_.get_branch(), 1, seq_no))) {
    LOG_WARN("failed alloc seq_no", KR(ret), K(seq_no), K(insert_ctx));
  } else if (OB_FAIL(locator_builder.set_seq_no(seq_no))) {
    LOG_WARN("failed set seq_no", KR(ret), K(insert_ctx), K(locator_builder), K(seq_no));
  }
  return ret;
}

int ObLobManager::get_table_param(const ObTableParam *&table_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_manager_.get_table_param(table_param))) {
    LOG_WARN("get table param fail", K(ret), K(table_param));
  }
  return ret;
}

int ObLobManager::get_table_dml_param(const ObTableDMLParam *&table_dml_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta_manager_.get_table_dml_param(table_dml_param))) {
    LOG_WARN("get table dml param fail", K(ret), K(table_dml_param));
  }
  return ret;
}
} // storage
} // oceanbase
