/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "lib/utility/ob_macro_utils.h"
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/incremental/ssmeta/ob_ss_meta_atomic_op.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_helper.h"
#include "storage/incremental/ssmeta/ob_ss_meta_base_operator.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_define.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

/*-----------------------------------------ObSSMetaAtomicBaseOp-----------------------------------------*/
int ObSSMetaAtomicBaseOp::init(const ObSSMetaAtomicOpInitParam &param)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObSSMetaOpBufferGuard buffer_guard;
  if (OB_FAIL(alloc_meta_op_buffer(param.protocol_type_, buffer_guard))) {
    LOG_WARN("fail to alloc buffer", KR(ret), K(param));
  } else if (FALSE_IT(cached_meta_.set_buffer(buffer_guard.get_buffer()))) {
  } else if (OB_NOT_NULL(param.meta_value_) && param.meta_value_->has_data()) {
    if (OB_FAIL(copy_to_cached_meta_(param.meta_value_->buf_, param.meta_value_->data_len_))) {
      LOG_WARN("fail to copy meta_value to cached meta", KR(ret), K(param));
    }
  } else if (OB_TMP_FAIL(get_meta_value(param))) {
    if (OB_OBJECT_NOT_EXIST != tmp_ret) {
      ret = tmp_ret;
      LOG_WARN("fail to get meta_value", KR(tmp_ret), K(param));
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    op_id_ = param.op_id_;
    protocol_type_ = param.protocol_type_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    reorgan_scn_ = param.reorgan_scn_;
  }
  return ret;
}

int ObSSMetaAtomicBaseOp::copy_to_cached_meta_(const char *src_buf, const int64_t src_len)
{
  int ret = OB_SUCCESS;
  ObSSMetaOpBuffer &buf = cached_meta_.get_buffer();
  if (buf.size_ < src_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("cached meta buffer too small", KR(ret), K(buf.size_), K(src_len));
  } else {
    MEMCPY(buf.buf_, src_buf, src_len);
    buf.data_len_ = src_len;
  }
  return ret;
}

void ObSSMetaAtomicBaseOp::reset()
{
  op_id_ = 0;
  meta_scn_.reset();
  ls_id_.reset();
  tablet_id_.reset();
  reorgan_scn_.reset();
  protocol_type_.reset();
  op_type_ = ObSSMetaAtomicOpType::INVALID_OP;
  if (NULL != cached_buffer_.buf_) {
    mtl_free(cached_buffer_.buf_);
  }
  cached_buffer_.reset();
  cached_meta_.reset();
}

int ObSSMetaAtomicBaseOp::get_meta_max_size(int64_t &max_size)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_meta_op_buf_size(protocol_type_, max_size))) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported type", K_(protocol_type));
  }
  return ret;
}

int ObSSMetaAtomicBaseOp::get_meta_value(const ObSSMetaAtomicOpInitParam &param)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc("GetMetaVal");
  ObSSMetaOpBuffer result_buf;
  SCN meta_scn;

  const bool is_ls_type =
      (param.protocol_type_.type_ == ObAtomicProtocolTypeEnum::LS_META
       || param.protocol_type_.type_ == ObAtomicProtocolTypeEnum::CHECKPOINT);

  ObSSMetaReadParamV2 read_param;
  if (is_ls_type) {
    read_param.set_ls_level_param(
        ObSSMetaReadParamType::LS_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        param.protocol_type_.type_,
        param.ls_id_, SCN::min_scn(), true/*is_in_table*/);
  } else {
    read_param.set_tablet_level_param(
        ObSSMetaReadParamType::TABLET_PREFIX,
        ObSSMetaReadResultType::READ_WHOLE_ROW,
        param.protocol_type_.type_,
        param.ls_id_, param.tablet_id_,
        SCN::min_scn(), param.reorgan_scn_, true/*is_in_table*/);
  }
  read_param.set_read_mark_delete(true);

  if (OB_FAIL(ObSSMetaTableHelper::get_meta_info(
          read_param, SCN::max_scn(), alloc, result_buf, meta_scn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_OBJECT_NOT_EXIST;
    }
    LOG_WARN("fail to get meta info", KR(ret), K(param));
  } else {
    if (OB_FAIL(copy_to_cached_meta_(result_buf.buf_, result_buf.data_len_))) {
      LOG_WARN("fail to copy result to cached meta", KR(ret), K(result_buf));
    }
    mtl_free(result_buf.buf_);
  }
  return ret;
}

int ObSSMetaAtomicBaseOp::process(ObMySQLTransaction &trans, const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  int64_t start_time_us = ObTimeUtility::current_time_us();
  MacroBlockId macro_id;
  ObSSMetaType meta_type;
  char path_buf[SSMETA_PATH_MAX_LEN] = {0};
  int64_t pos = 0;
  ObSSMetaTableOperator op;
  int64_t affected_rows = 0;

  if (OB_UNLIKELY(!meta_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta_scn is invalid", KR(ret), K(meta_scn), KPC(this));
  } else if (OB_UNLIKELY(!cached_buffer_.has_data())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cached_buffer has no data to write", KR(ret), KPC(this));
  } else if (ObAtomicProtocolTypeEnum::LS_META == protocol_type_.type_) {
    ObSSMetaLSKey ls_key;
    ls_key.ls_id_ = ls_id_;
    ls_key.meta_scn_ = meta_scn;
    macro_id = ls_key.convert_to_macro_id(true/*is_in_table*/);
  } else if (ObAtomicProtocolTypeEnum::CHECKPOINT == protocol_type_.type_) {
    ObSSMetaLSCkptKey ckpt_key;
    ckpt_key.ls_id_ = ls_id_;
    ckpt_key.meta_scn_ = meta_scn;
    macro_id = ckpt_key.convert_to_macro_id(true/*is_in_table*/);
  } else {
    ObSSMetaTabletKey tablet_key;
    tablet_key.ls_id_ = ls_id_;
    tablet_key.tablet_id_ = tablet_id_;
    tablet_key.reorganization_scn_ = reorgan_scn_;
    tablet_key.meta_scn_ = meta_scn;
    macro_id = tablet_key.convert_to_macro_id(true/*is_in_table*/);
  }

  if (OB_FAIL(ret)) {
  } else if (ObAtomicProtocolTypeEnum::LS_META == protocol_type_.type_) {
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_META);
  } else if (ObAtomicProtocolTypeEnum::CHECKPOINT == protocol_type_.type_) {
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::LS_CKPT);
  } else {
    meta_type.type_val_ = static_cast<int64_t>(ObSSMetaTypeEnum::TABLET_META);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(
             macro_id, path_buf, sizeof(path_buf), pos))) {
    LOG_WARN("fail to format meta_key path", KR(ret), K(macro_id));
  } else if (OB_FAIL(op.init(trans, OB_ALL_SHARED_META_TNAME))) {
    LOG_WARN("fail to init ss meta operator", KR(ret));
  } else {
    ObString meta_key(pos, path_buf);
    ObString meta_value(cached_buffer_.data_len_, cached_buffer_.buf_);
    if (OB_FAIL(op.insert_row(MTL_ID(), ls_id_, meta_type, meta_key, meta_value, affected_rows))) {
      LOG_WARN("fail to insert meta row", KR(ret), K(meta_key), K_(ls_id), K_(tablet_id), K(meta_scn));
    }
  }
  const int64_t cost_time_us = ObTimeUtility::current_time_us() - start_time_us;
  if (cost_time_us >= 100_ms) {
    LOG_WARN("cost too much time in atomic_op", KR(ret), K_(op_id), K_(ls_id), K_(tablet_id),
      K(meta_scn), K(affected_rows), K(meta_type), K(cost_time_us));
  } else {
    LOG_TRACE("ObSSMetaAtomicBaseOp::process", KR(ret), K_(op_id), K_(ls_id), K_(tablet_id),
      K(meta_scn), K(affected_rows), K(meta_type), K(cost_time_us), K_(cached_buffer));
  }
  return ret;
}

/*-----------------------------------------ObSSMetaAtomicTabletOp-----------------------------------------*/
int ObSSMetaAtomicTabletOp::handle_mark_delete(const share::SCN &deleted_scn)
{
  return mark_delete_head_only_(deleted_scn);
}

int ObSSMetaAtomicTabletOp::mark_delete_head_only_(const share::SCN &deleted_scn)
{
  int ret = OB_SUCCESS;
  ObSSMetaOpBuffer &meta_buffer = cached_meta_.get_buffer();
  if (OB_UNLIKELY(!deleted_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid deleted_scn", KR(ret), K(deleted_scn), KPC(this));
  } else if (OB_UNLIKELY(!meta_buffer.has_data())) {
    ret = OB_OBJECT_NOT_EXIST;
    STORAGE_LOG(TRACE, "no cached meta", KPC(this));
  } else {
    // Buffer layout (ObDefaultSSMetaValue::serialize):
    //   [outer_ver (vi64)][outer_len (fixed 5B)][ ObSSMetaHeadValue::serialize() | meta_value_ 编码 ]
    //                                            \___________ head 区 __________/\__ 保持不动 __/
    // We must NOT deserialize the full ObSSTabletMetaValue, because runtime-only
    // fields (data_version_ / macro_info_ ...) would be lost and re-serialization
    // would corrupt the payload. We also must NOT patch the head in-place: when the
    // buffer was written by an older version, re-serializing the head with the current
    // (possibly larger) ObSSMetaHeadValueInfo would silently overrun and corrupt the
    // trailing meta_value_ bytes. Instead we parse the head to locate the exact
    // boundary, rewrite the (possibly resized) head, copy the original meta_value_
    // bytes verbatim, and fix up the outer length.
    const char *ori_buf = meta_buffer.buf_;
    const int64_t ori_len = meta_buffer.data_len_;
    int64_t pos = 0;
    int64_t outer_version = 0;
    int64_t outer_content_len = 0;
    if (OB_FAIL(serialization::decode(ori_buf, ori_len, pos, outer_version))) {
      LOG_WARN("fail to decode outer version", KR(ret), K(ori_len));
    } else if (OB_FAIL(serialization::decode(ori_buf, ori_len, pos, outer_content_len))) {
      LOG_WARN("fail to decode outer len", KR(ret), K(ori_len), K(pos));
    } else {
      const int64_t header_size = pos;                          // outer version + len envelope
      const int64_t content_end = header_size + outer_content_len; // end of head + meta_value
      ObSSMetaHeadValue head;
      int64_t head_pos = 0;
      if (OB_UNLIKELY(content_end > ori_len || outer_content_len < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid outer content len", KR(ret), K(ori_len), K(header_size),
                 K(outer_content_len));
      } else if (OB_FAIL(head.deserialize(ori_buf + header_size, outer_content_len, head_pos))) {
        LOG_WARN("fail to deserialize head value", KR(ret), K(outer_content_len));
      } else {
        const int64_t old_head_end = header_size + head_pos;     // end of head 区 in ori_buf
        const int64_t meta_value_len = content_end - old_head_end; // bytes to keep verbatim
        head.head_info_.mark_delete_ = true;
        head.head_info_.set_deleted_scn(deleted_scn);
        const int64_t new_content_len = head.get_serialize_size() + meta_value_len;
        const int64_t new_buf_len = header_size + new_content_len;
        char *new_buf = NULL;
        if (OB_UNLIKELY(meta_value_len < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid meta value len", KR(ret), K(old_head_end), K(content_end));
        } else if (OB_ISNULL(new_buf = static_cast<char *>(
                       share::mtl_malloc(new_buf_len, "SSMetaOpBuf")))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc buf for mark_delete", KR(ret), K(new_buf_len));
        } else {
          int64_t wpos = 0;
          if (OB_FAIL(serialization::encode_vi64(new_buf, new_buf_len, wpos, outer_version))) {
            LOG_WARN("fail to encode outer version", KR(ret), K(new_buf_len));
          } else if (OB_FAIL(serialization::encode_fixed_bytes_i64(
                         new_buf, new_buf_len, wpos, new_content_len))) {
            LOG_WARN("fail to encode outer len", KR(ret), K(new_buf_len), K(new_content_len));
          } else if (OB_FAIL(head.serialize(new_buf, new_buf_len, wpos))) {
            LOG_WARN("fail to serialize head value", KR(ret), K(new_buf_len));
          } else {
            if (meta_value_len > 0) {
              MEMCPY(new_buf + wpos, ori_buf + old_head_end, meta_value_len);
              wpos += meta_value_len;
            }
            // The outer header size only depends on outer_version (unchanged) and the
            // fixed-width len, so the rewritten buffer must end exactly at new_buf_len.
            if (OB_UNLIKELY(wpos != new_buf_len)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("mark_delete buf length mismatch", KR(ret), K(wpos), K(new_buf_len));
            }
          }
          if (OB_SUCC(ret)) {
            ObSSMetaOpBuffer new_meta_buf(new_buf, new_buf_len);
            new_meta_buf.data_len_ = new_buf_len;
            cached_buffer_.assign(new_meta_buf);
          } else {
            share::mtl_free(new_buf);
          }
        }
      }
    }
  }
  return ret;
}

/*-----------------------------------------ObSSMetaAtomicSSTableOp-----------------------------------------*/
int ObSSMetaAtomicSSTableOp::process(ObMySQLTransaction &trans, const share::SCN &meta_scn)
{
  int ret = OB_SUCCESS;
  UNUSED(trans);
  UNUSED(meta_scn);
  return ret;
}

} // storage
} // oceanbase
