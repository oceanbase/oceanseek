/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_ATOMIC_OP_H_
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_ATOMIC_OP_H_

#include "lib/utility/ob_macro_utils.h"
#include "common/ob_tablet_id.h"
#include "lib/string/ob_string_holder.h"
#include "share/ob_ls_id.h"
#include "lib/atomic/ob_atomic.h"
#include "share/scn.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol_define.h"
#include "storage/incremental/atomic_protocol/ob_atomic_protocol.h"

namespace oceanbase
{
namespace storage
{
class ObSSTabletMetaValue;

// 适用于ls、tablet meta等场景，适配AtomicProtocol的ssmeta表的操作op
class ObSSMetaAtomicBaseOp : public ObIAtomicProtocolOP
{
public:
  ObSSMetaAtomicBaseOp(ObSSMetaAtomicOpType op_type = ObSSMetaAtomicOpType::INVALID_OP)
    : op_id_(-1), meta_scn_(), ls_id_(), tablet_id_(), reorgan_scn_(),
      protocol_type_(), op_type_(op_type), cached_buffer_(), cached_meta_()
  {}
  virtual ~ObSSMetaAtomicBaseOp() { reset(); }

  void reset();
  int init(const ObSSMetaAtomicOpInitParam &param);

  virtual int process(common::ObMySQLTransaction &trans, const share::SCN &meta_scn) override;

  // meta_info --(serialize)-> cached_meta_
  template<typename MetaInfo>
  int write_meta_info(const MetaInfo &meta_info,
                      ObFunction<int(const MetaInfo&, ObSSMetaOpBuffer &)> serialize_func = ObSSMetaAtomicBaseOp::serialize_meta_info<MetaInfo>);
  
  // cached_meta_ --(deserialize)-> meta_info
  template<typename MetaInfo>
  int read_meta_info(MetaInfo &meta_info,
                     ObFunction<int(const ObSSMetaOpBuffer &, MetaInfo&)> deserialize_func = common_deserialize_meta_info<MetaInfo>);
  // default implementation is call meta_info deserailize function
  // specialization this template function to implement your own deserialization
  template<typename MetaInfo>
  static int deserialize_meta_info(const ObSSMetaOpBuffer &content_buf, MetaInfo &meta_info);
  // serialize meta obj to meta content buf
  // default implementation is call meta_info serialize function
  // specialization this template function to implement your own serialization
  template<typename MetaInfo>
  static int serialize_meta_info(const MetaInfo &meta_info, ObSSMetaOpBuffer &content_buf);

  void set_op_id(const int64_t op_id) { op_id_ = op_id; }
  int64_t get_op_id() const { return op_id_; }
  const ObSSMetaOpBuffer &get_cached_buffer() const { return cached_buffer_; }
  ObAtomicProtocolType get_protocol_type() const { return protocol_type_; }
  ObSSMetaAtomicOpType get_op_type() const { return op_type_; }
  TO_STRING_KV(K_(op_id), K_(meta_scn),
               K_(ls_id), K_(tablet_id), K_(reorgan_scn),
               K_(protocol_type), K_(op_type), K_(cached_buffer));

protected:
  int get_meta_max_size(int64_t &max_size);
  int get_meta_value(const ObSSMetaAtomicOpInitParam &param);
  int copy_to_cached_meta_(const char *src_buf, int64_t src_len);

protected:
  int64_t op_id_;
  share::SCN meta_scn_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN reorgan_scn_;
  ObAtomicProtocolType protocol_type_;
  ObSSMetaAtomicOpType op_type_;
  ObSSMetaOpBuffer cached_buffer_; // new added meta_content will be written into this
  ObSSMetaOpBufferGuard cached_meta_; // read old meta_content from ss and write into this
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaAtomicBaseOp);
};

template<typename MetaInfo>
int ObSSMetaAtomicBaseOp::write_meta_info(
    const MetaInfo &meta_info,
    ObFunction<int(const MetaInfo&, ObSSMetaOpBuffer&)> serialize_func)
{
  #define PRINT_WRAPPER K(ret), K(meta_info), KPC(this)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t len = meta_info.get_serialize_size();
  int64_t meta_max_size = 0;
  char *buf = static_cast<char *>(share::mtl_malloc(len, "SSMetaOpBuf"));
  ObSSMetaOpBuffer meta_info_buf(buf, len);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc memory", K(len), PRINT_WRAPPER);
  } else if (OB_FAIL(get_meta_max_size(meta_max_size))) {
    STORAGE_LOG(WARN, "fail to get meta max size", PRINT_WRAPPER);
  } else if (meta_max_size < len) {
    ret = OB_NOT_SUPPORTED;
    STORAGE_LOG(WARN, "too large meta size", K(meta_max_size), K(len), PRINT_WRAPPER);
  } else if (OB_FAIL(serialize_func(meta_info, meta_info_buf))) {
    STORAGE_LOG(WARN, "fail to serialize", K(len), PRINT_WRAPPER);
  } else {
    cached_buffer_.assign(meta_info_buf);
    STORAGE_LOG(INFO, "finish to write meta info", PRINT_WRAPPER, K_(cached_buffer));
  }
  if (OB_FAIL(ret)) {
    share::mtl_free(buf);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template<typename MetaInfo>
int ObSSMetaAtomicBaseOp::read_meta_info(
    MetaInfo &meta_info,
    ObFunction<int(const ObSSMetaOpBuffer &, MetaInfo&)> deserialize_func)
{
  int ret = OB_SUCCESS;
  ObSSMetaOpBuffer &meta_buffer = cached_meta_.get_buffer();
  if (OB_LIKELY(meta_buffer.has_data())) {
    if (OB_FAIL(deserialize_func(meta_buffer, meta_info))) {
      STORAGE_LOG(WARN, "fail to deserialize cached meta", K(ret), K(meta_buffer));
    } else {
      STORAGE_LOG(INFO, "succ to get cached meta", K(meta_buffer));
    }
  } else {
    ret = OB_OBJECT_NOT_EXIST;
    STORAGE_LOG(TRACE, "no cached meta", KPC(this));
  }
  return ret;
}

template<typename MetaInfo>
int ObSSMetaAtomicBaseOp::deserialize_meta_info(const ObSSMetaOpBuffer &content_buffer, MetaInfo &meta_info)
{
  #define PRINT_WRAPPER K(ret), K(content_buffer), K(meta_info)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(meta_info.deserialize(content_buffer.buf_, content_buffer.size_, pos))) {
    STORAGE_LOG(WARN, "deserialize task info fail", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

template<typename MetaInfo>
int ObSSMetaAtomicBaseOp::serialize_meta_info(const MetaInfo &meta_info, ObSSMetaOpBuffer &content_buffer)
{
  #define PRINT_WRAPPER K(ret), K(content_buffer), K(meta_info)
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(meta_info.serialize(content_buffer.buf_, content_buffer.size_, pos))) {
    STORAGE_LOG(WARN, "fail to serialize meta", PRINT_WRAPPER);
  } else {
    content_buffer.data_len_ = pos;
  }
  return ret;
  #undef PRINT_WRAPPER
}

class ObSSMetaAtomicLSOp : public ObSSMetaAtomicBaseOp
{
public:
  ObSSMetaAtomicLSOp()
    : ObSSMetaAtomicBaseOp(ObSSMetaAtomicOpType::LS_OP)
  {}

  virtual ~ObSSMetaAtomicLSOp() {}
};

class ObSSMetaAtomicLSCkptOp : public ObSSMetaAtomicBaseOp
{
public:
  ObSSMetaAtomicLSCkptOp()
    : ObSSMetaAtomicBaseOp(ObSSMetaAtomicOpType::LS_OP)
  {}

  virtual ~ObSSMetaAtomicLSCkptOp() {}
};

class ObSSMetaAtomicTabletOp : public ObSSMetaAtomicBaseOp
{
public:
  ObSSMetaAtomicTabletOp()
    : ObSSMetaAtomicBaseOp(ObSSMetaAtomicOpType::TABLET_OP)
  {}

  virtual ~ObSSMetaAtomicTabletOp() {}
  int handle_mark_delete(const share::SCN &deleted_scn);
private:
  int mark_delete_head_only_(const share::SCN &deleted_scn);
};

class ObSSMetaAtomicSSTableOp : public ObSSMetaAtomicBaseOp
{
public:
  ObSSMetaAtomicSSTableOp()
    : ObSSMetaAtomicBaseOp(ObSSMetaAtomicOpType::SSTABLE_OP)
  {}

  virtual ~ObSSMetaAtomicSSTableOp() {}

  virtual int process(common::ObMySQLTransaction &trans, const share::SCN &meta_scn) override;
};

} // end storage
} // end oceanbase

#endif /* OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_ATOMIC_OP_H_ */
