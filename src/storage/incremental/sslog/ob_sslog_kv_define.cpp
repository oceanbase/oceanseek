/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/json/ob_yson.h"
#include "src/share/config/ob_server_config.h"
#include <cstdint>
#include "src/storage/incremental/sslog/ob_sslog_kv_define.h"

namespace oceanbase
{

namespace sslog
{

OB_SERIALIZE_MEMBER(ObSSLogKVRowInnerKey, key_version_, type_);

OB_SERIALIZE_MEMBER(ObSSLogKVRowInnerValue,
                    row_version_,
                    tx_version_,
                    tx_state_,
                    tx_id_,
                    flag_.flag_val_);

OB_SERIALIZE_MEMBER(ObSSLogKVRowUserValue, meta_val_, extra_info_);

OB_SERIALIZE_MEMBER(ObSSLogKVRowPhysicalValue, user_val_, inner_val_, next_key_);

DEFINE_SERIALIZE(ObSSLogKVRowUserKey)
{
  int ret = OB_SUCCESS;
  bool encode_last_member = false;
  int64_t start_pos = pos;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, cluster_id_))) {
      STORAGE_LOG(WARN, "encode cluster_id failed", K(ret), K(buf_len), K(pos), K(cluster_id_));
    } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, encode_last_member))) {
      STORAGE_LOG(WARN, "encode last member flag for cluster_id failed", K(ret), K(buf_len), K(pos),
                  K(encode_last_member));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, region_id_))) {
      STORAGE_LOG(WARN, "encode region_id_ failed", K(ret), K(buf_len), K(pos), K(region_id_));
    } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, encode_last_member))) {
      STORAGE_LOG(WARN, "encode last member flag for region_id_ failed", K(ret), K(buf_len), K(pos),
                  K(encode_last_member));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, static_cast<int64_t>(meta_type_)))) {
      STORAGE_LOG(WARN, "encode meta_type_ failed", K(ret), K(buf_len), K(pos), K(meta_type_));
    } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, encode_last_member))) {
      STORAGE_LOG(WARN, "encode last member flag for meta_type_ failed", K(ret), K(buf_len), K(pos),
                  K(encode_last_member));
    }
  }

  if (OB_SUCC(ret)) {
    encode_last_member = true;
    if (meta_key_.length() + 1 + pos > buf_len) {
      ret = OB_SIZE_OVERFLOW;
      STORAGE_LOG(WARN, "buf is not enough for meta_key", K(ret), K(buf_len), K(pos),
                  K(meta_key_.length()));
    } else {
      memcpy(buf + pos, meta_key_.ptr(), meta_key_.length());
      *(buf + pos + meta_key_.length()) = 0;
      pos = pos + meta_key_.length() + 1;
      // TRANS_LOG(INFO, "serialize meta_key_ without size", K(ret), K(meta_key_),
      //           K(meta_key_.length()), K(pos));
      if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, encode_last_member))) {
        STORAGE_LOG(WARN, "encode last member flag for meta_key_ failed", K(ret), K(buf_len),
                    K(pos), K(encode_last_member));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t occupy_bytes = pos - start_pos;
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, occupy_bytes))) {
      STORAGE_LOG(WARN, "encode last member flag for occupy_bytes failed", K(ret), K(buf_len),
                  K(start_pos), K(pos), K(encode_last_member), K(occupy_bytes));
    } else {
      // STORAGE_LOG(INFO, "encode last member flag for occupy_bytes", K(ret), K(buf_len),
      //             K(start_pos), K(pos), K(encode_last_member), K(occupy_bytes));
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ObSSLogKVRowUserKey)
{
  int ret = OB_SUCCESS;

  bool encode_last_member = false;
  int64_t start_pos = pos;

  int64_t tmp_i64 = 0;
  if (OB_SUCC(ret) && !encode_last_member) {

    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tmp_i64))) {
      STORAGE_LOG(WARN, "decode cluster_id failed", K(ret), K(data_len), K(pos), K(tmp_i64));
    } else if (OB_FALSE_IT(cluster_id_ = static_cast<uint64_t>(tmp_i64))) {
    } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &encode_last_member))) {
      STORAGE_LOG(WARN, "decode last member flag for cluster_id failed", K(ret), K(data_len),
                  K(pos), K(encode_last_member));
    }
  }

  if (OB_SUCC(ret) && !encode_last_member) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tmp_i64))) {
      STORAGE_LOG(WARN, "decode region_id_ failed", K(ret), K(data_len), K(pos), K(tmp_i64));
    } else if (OB_FALSE_IT(region_id_ = static_cast<uint64_t>(tmp_i64))) {
    } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &encode_last_member))) {
      STORAGE_LOG(WARN, "decode last member flag for region_id_ failed", K(ret), K(data_len),
                  K(pos), K(encode_last_member));
    }
  }

  if (OB_SUCC(ret) && !encode_last_member) {
    if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &tmp_i64))) {
      STORAGE_LOG(WARN, "decode tmp_i64 failed", K(ret), K(data_len), K(pos), K(tmp_i64));
    } else if (OB_FALSE_IT(meta_type_ = static_cast<ObSSLogMetaType>(tmp_i64))) {
    } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &encode_last_member))) {
      STORAGE_LOG(WARN, "decode last member flag for meta_type_ failed", K(ret), K(data_len),
                  K(pos), K(encode_last_member));
    }
  }

  if (OB_SUCC(ret) && !encode_last_member) {
    int64_t tail = pos;
    while (*(buf + tail) != 0) {
      tail++;

      if (tail > data_len) {
        ret = OB_SIZE_OVERFLOW;
        STORAGE_LOG(WARN, "decode meta_key_ failed", K(ret), K(pos), K(data_len), K(tail));
      }
    }

    if (OB_SUCC(ret)) {
      meta_key_.assign_ptr(buf + pos, tail - pos);
      pos = tail + 1;

      TRANS_LOG(DEBUG, "deserialize meta_key_ without size", K(ret), K(meta_key_),
                K(meta_key_.length()), K(pos), K(tail));
      if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &encode_last_member))) {
        STORAGE_LOG(WARN, "decode last member flag for meta_key_ failed", K(ret), K(data_len),
                    K(pos), K(encode_last_member));
      }
    }
  }

  if (OB_SUCC(ret)) {
    int64_t occupy_bytes = 0;
    int64_t tmp_pos = pos;
    int64_t decode_bytes = 0;
    do {
      decode_bytes = pos - start_pos;
      tmp_pos = pos;
      if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &occupy_bytes))) {
        STORAGE_LOG(WARN, "decode occupy_bytes failed, retry to next byte", K(ret), K(data_len),
                    K(start_pos), K(pos), K(tmp_pos), K(encode_last_member), K(occupy_bytes),
                    K(decode_bytes));
        pos++;
      } else if (decode_bytes != occupy_bytes) {
        ret = OB_EAGAIN;
        STORAGE_LOG(WARN, "unexpected decode bytes, need retry", K(ret), KPC(this), K(start_pos),
                    K(pos), K(tmp_pos), K(occupy_bytes), K(decode_bytes));
        pos++;
      } else {
        STORAGE_LOG(DEBUG, "decode last member flag for occupy_bytes", K(ret), K(data_len),
                    K(start_pos), K(tmp_pos), K(pos), K(encode_last_member), K(occupy_bytes),
                    K(decode_bytes));
        pos = tmp_pos;
      }
    } while (ret != OB_SUCCESS && ret != OB_DESERIALIZE_ERROR);
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSSLogKVRowUserKey)
{
  int64_t size = 0;

  bool encode_last_member = false;

  size += serialization::encoded_length_i64(cluster_id_);
  size += serialization::encoded_length_bool(encode_last_member);
  size += serialization::encoded_length_i64(region_id_);
  size += serialization::encoded_length_bool(encode_last_member);
  size += serialization::encoded_length_i64(static_cast<int64_t>(meta_type_));
  size += serialization::encoded_length_bool(encode_last_member);
  size += meta_key_.length() + 1;
  size += serialization::encoded_length_bool(encode_last_member);
  size += serialization::encoded_length_i64(size);

  return size;
}

int ObSSLogKVRowUserKey::get_practical_end_pos(int64_t &end_pos) const
{
  int ret = OB_SUCCESS;

  bool encode_last_member = false;

  end_pos += serialization::encoded_length_i64(cluster_id_);
  end_pos += serialization::encoded_length_bool(encode_last_member);
  end_pos += serialization::encoded_length_i64(region_id_);
  end_pos += serialization::encoded_length_bool(encode_last_member);
  end_pos += serialization::encoded_length_i64(static_cast<int64_t>(meta_type_));
  end_pos += serialization::encoded_length_bool(encode_last_member);
  end_pos += meta_key_.length();
  return ret;
}

int ObSSLogKVRowUserKey::get_common_key_end_pos(int64_t & end_pos) const
{
  int ret = OB_SUCCESS;

  bool encode_last_member = false;

  end_pos += serialization::encoded_length_i64(cluster_id_);
  end_pos += serialization::encoded_length_bool(encode_last_member);
  end_pos += serialization::encoded_length_i64(region_id_);
  end_pos += serialization::encoded_length_bool(encode_last_member);

  return ret;
}

DEFINE_SERIALIZE(ObSSLogKVRowPhysicalKey)
{
  int ret = OB_SUCCESS;

  bool encode_last_member = false;
  int64_t start_pos = pos;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(user_key_.serialize(buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "encode user_key_ failed", K(ret), K(buf_len), K(pos), K(user_key_));
    } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, encode_last_member))) {
      STORAGE_LOG(WARN, "encode last member flag for cluster_id failed", K(ret), K(buf_len), K(pos),
                  K(encode_last_member));
    }
  }

  if (OB_SUCC(ret)) {
    encode_last_member = true;
    if (OB_FAIL(inner_key_.serialize(buf, buf_len, pos))) {
      STORAGE_LOG(WARN, "encode inner_key_ failed", K(ret), K(buf_len), K(pos), K(inner_key_));
    } else if (OB_FAIL(serialization::encode_bool(buf, buf_len, pos, encode_last_member))) {
      STORAGE_LOG(WARN, "encode last member flag for region_id_ failed", K(ret), K(buf_len), K(pos),
                  K(encode_last_member));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t occupy_bytes = pos - start_pos;
    if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, occupy_bytes))) {
      STORAGE_LOG(WARN, "encode last member flag for occupy_bytes failed", K(ret), K(buf_len),
                  K(pos), K(encode_last_member));
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObSSLogKVRowPhysicalKey)
{
  int ret = OB_SUCCESS;
  bool encode_last_member = false;
  int64_t start_pos = pos;

  if (OB_SUCC(ret) && !encode_last_member) {

    if (OB_FAIL(user_key_.deserialize(buf, data_len, pos))) {
      STORAGE_LOG(WARN, "decode user_key_ failed", K(ret), K(data_len), K(pos), K(user_key_));
    } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &encode_last_member))) {
      STORAGE_LOG(WARN, "decode last member flag for cluster_id failed", K(ret), K(data_len),
                  K(pos), K(encode_last_member));
    }
  }

  if (OB_SUCC(ret) && !encode_last_member) {
    if (OB_FAIL(inner_key_.deserialize(buf, data_len, pos))) {
      STORAGE_LOG(WARN, "decode region_id_ failed", K(ret), K(data_len), K(pos), K(inner_key_));
    } else if (OB_FAIL(serialization::decode_bool(buf, data_len, pos, &encode_last_member))) {
      STORAGE_LOG(WARN, "decode last member flag for region_id_ failed", K(ret), K(data_len),
                  K(pos), K(encode_last_member));
    }
  }

  if (OB_SUCC(ret)) {
    int64_t occupy_bytes = 0;
    int64_t tmp_pos = pos;
    int64_t decode_bytes = 0;
    do {
      decode_bytes = pos - start_pos;
      tmp_pos = pos;
      if (OB_FAIL(serialization::decode_i64(buf, data_len, tmp_pos, &occupy_bytes))) {
        STORAGE_LOG(WARN, "decode occupy_bytes failed, retry to next byte", K(ret), K(data_len),
                    K(start_pos), K(pos), K(tmp_pos), K(encode_last_member), K(occupy_bytes),
                    K(decode_bytes));
        pos++;
      } else if (decode_bytes != occupy_bytes) {
        ret = OB_EAGAIN;
        STORAGE_LOG(WARN, "unexpected decode bytes, need retry", K(ret), KPC(this), K(data_len),
                    K(start_pos), K(pos), K(tmp_pos), K(occupy_bytes), K(decode_bytes));
        pos++;
      } else {
        pos = tmp_pos;
      }
    } while (ret != OB_SUCCESS && ret != OB_SIZE_OVERFLOW);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObSSLogKVRowPhysicalKey)
{
  int64_t size = 0;

  bool encode_last_member = false;

  size += user_key_.get_serialize_size();
  size += serialization::encoded_length_bool(encode_last_member);
  size += inner_key_.get_serialize_size(); 
  size += serialization::encoded_length_bool(encode_last_member);
  size += serialization::encoded_length_i64(size);

  return size;
}

int ObSSLogKVRowPhysicalKey::get_user_key_end_pos(int64_t &end_pos) const
{
  int ret = OB_SUCCESS;

  ret = user_key_.get_practical_end_pos(end_pos);

  return ret;
}

int ObSSLogKVRowPhysicalKey::get_common_key_end_pos(int64_t & end_pos) const
{
  int ret = OB_SUCCESS;

  ret = user_key_.get_common_key_end_pos(end_pos);

  return ret;
}

int ObSSLogKVRowPhysicalBuf::init(const int64_t key_size, const int64_t val_size)
{
  int ret = OB_SUCCESS;

  if (is_inited()) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init physical buf twice", K(ret), KPC(this));
  } else if (OB_FALSE_IT(allocator_.set_tenant_id(MTL_ID()))) {
  } else if (OB_FAIL(key_buf_.reserve(key_size))) {
    STORAGE_LOG(WARN, "reserve key_buf faild", K(ret), K(key_size), KPC(this));
  } else if (OB_FAIL(val_buf_.reserve(val_size))) {
    STORAGE_LOG(WARN, "reserve val_buf faild", K(ret), K(val_size), KPC(this));
  }

  return ret;
}

void ObSSLogKVRowPhysicalBuf::reset()
{
  key_buf_.reset();
  val_buf_.reset();
}

void ObSSLogKVRowPhysicalBuf::reuse()
{
  key_buf_.reuse();
  val_buf_.reuse();
}

void ObSSLogKVTempCache::reset()
{
  for(int i =0 ; i < key_buf_cache_.count(); i++)
  {
    allocator_.free(key_buf_cache_[i].ptr());
    allocator_.free(val_buf_cache_[i].ptr());
  }
  key_buf_cache_.reset();
  val_buf_cache_.reset();

  cur_key_buf_.reset();
  cur_val_buf_.reset();
}

int ObSSLogKVTempCache::store_into_cache()
{
  int ret = OB_SUCCESS;
  char *tmp_key, *tmp_val;
  tmp_key = tmp_val = nullptr;
  if (cur_key_buf_.empty() || cur_val_buf_.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(cur_key_buf_), K(cur_val_buf_));
  } else {
    tmp_key = static_cast<char *>(allocator_.alloc(cur_key_buf_.length()));
    tmp_val = static_cast<char *>(allocator_.alloc(cur_val_buf_.length()));

    if (tmp_key == nullptr || tmp_val == nullptr) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc kv tmp buf failed", K(ret), KP(tmp_key), KP(tmp_val));

      if (OB_NOT_NULL(tmp_key)) {
        share::mtl_free(tmp_key);
      }

      if (OB_NOT_NULL(tmp_val)) {
        share::mtl_free(tmp_val);
      }
    } else {
      MEMCPY(tmp_key, cur_key_buf_.ptr(), cur_key_buf_.length());
      MEMCPY(tmp_val, cur_val_buf_.ptr(), cur_val_buf_.length());
    }

    if (OB_SUCC(ret)) {
      bool push_key_succ = false;
      bool push_val_succ = false;
      ObString key_buf_str, val_buf_str;
      key_buf_str.assign_ptr(tmp_key, cur_key_buf_.length());
      val_buf_str.assign_ptr(tmp_val, cur_val_buf_.length());
      if (OB_FAIL(key_buf_cache_.push_back(key_buf_str))) {
        STORAGE_LOG(WARN, "push back into key_buf_cache_ failed", K(ret), K(key_buf_str));
      } else if (OB_FALSE_IT(push_key_succ = true)) {
      } else if (OB_FAIL(val_buf_cache_.push_back(val_buf_str))) {
        STORAGE_LOG(WARN, "push back into val_buf_cache_ failed", K(ret), K(val_buf_str));
      } else if (OB_FALSE_IT(push_val_succ = true)) {
      }

      if (OB_FAIL(ret)) {
        if (push_key_succ) {
          key_buf_cache_.pop_back();
        }

        if (push_val_succ) {
          val_buf_cache_.pop_back();
        }
      }
    }

    if(OB_SUCC(ret))
    {
      cur_key_buf_.reuse();
      cur_val_buf_.reuse();
    }
  }

  return ret;
}

int ObSSLogTableLogicalRow::init(ObPalfKVAdpaterInterface *interface,
                                 const ObSSLogMetaType &meta_type,
                                 const ObString &meta_key,
                                 bool is_prefix_read)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), KPC(this));
  } else if (OB_FAIL(physical_buf_.init())) {
    STORAGE_LOG(WARN, "init physical buf failed", K(ret), K(this));
  } else {
    physical_key_.user_key_.cluster_id_ = GCONF.cluster_id;
    // physical_key_.user_key_.region_id_ = ;
    physical_key_.user_key_.meta_type_ = meta_type;
    physical_key_.user_key_.meta_key_ = meta_key;

    int64_t tmp_pos = 0;
    if (OB_FAIL(physical_key_.serialize(physical_buf_.get_key_buf(),
                                        ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN, tmp_pos))) {
      STORAGE_LOG(WARN, "serialize physical key failed", K(ret), KPC(this));
    } else {
      physical_buf_.set_key_buf_pos(tmp_pos);
      physical_key_.reset();
      tmp_pos = 0;
      if (OB_FAIL(physical_key_.deserialize(physical_buf_.get_key_buf(),
                                            physical_buf_.get_key_buf_pos(), tmp_pos))) {
        STORAGE_LOG(WARN, "deserialize physical_key_ failed", K(ret), KPC(this));
      } else {
        interface_ = interface;
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSSLogTableLogicalRow::init_for_read(ObPalfKVAdpaterInterface *interface,
                                          const ObSSLogMetaType &type,
                                          const ObString &key)
{
  int ret = OB_SUCCESS;

  if (is_inited()) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), KPC(this));
  } else {
    physical_key_.user_key_.cluster_id_ = GCONF.cluster_id;
    // physical_key_.user_key_.region_id_ = ;
    physical_key_.user_key_.meta_type_ = type;
    physical_key_.user_key_.meta_key_ = key;

    int64_t tmp_pos = 0;
    if (OB_FAIL(physical_buf_.init(physical_key_.get_serialize_size() + 10, 10))) {
      STORAGE_LOG(WARN, "init physical buf failed", K(ret), K(this));
    } else if (OB_FAIL(physical_key_.serialize(physical_buf_.get_key_buf(),
                                               ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN,
                                               tmp_pos))) {
      STORAGE_LOG(WARN, "serialize physical key failed", K(ret), KPC(this));
    } else {
      physical_buf_.set_key_buf_pos(tmp_pos);
      physical_key_.reset();
      tmp_pos = 0;
      if (OB_FAIL(physical_key_.deserialize(physical_buf_.get_key_buf(),
                                            physical_buf_.get_key_buf_pos(), tmp_pos))) {
        STORAGE_LOG(WARN, "deserialize physical_key_ failed", K(ret), KPC(this));
      } else {
        interface_ = interface;
      }
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObSSLogTableLogicalRow::init(ObPalfKVAdpaterInterface *interface,
                                 bool is_prefix_read,
                                 const int64_t key_size,
                                 const int64_t val_size)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret), KPC(this));
  } else if (OB_FAIL(physical_buf_.init(key_size, val_size))) {
    STORAGE_LOG(WARN, "init physical buf failed", K(ret), K(this));
  } else {
    interface_ = interface;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObPrefixFirstVersionBaseIterator::construct_common_prefix_()
{
  int ret = OB_SUCCESS;

  char *tmp_key_buf = nullptr;

  if (OB_NOT_NULL(common_prefix_.ptr())) {
    // do nothing
  } else if (OB_ISNULL(tmp_key_buf = static_cast<char *>(
                           allocator_.alloc(ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "alloc the memory of tmp_key_buf failed", K(ret), KP(tmp_key_buf));
  } else {
    ObSSLogKVRowPhysicalKey phy_key;
    phy_key.user_key_.cluster_id_ = GCONF.cluster_id;
    int64_t tmp_pos = 0;
    int64_t common_key_pos = 0;
    if (OB_FAIL(
            phy_key.serialize(tmp_key_buf, ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN, tmp_pos))) {
      STORAGE_LOG(WARN, "serialize common key failed", K(ret), KP(tmp_key_buf), K(tmp_pos),
                  K(phy_key));
    } else if (OB_FAIL(phy_key.get_common_key_end_pos(common_key_pos))) {
      STORAGE_LOG(WARN, "get common key end_pos failed", K(ret), KP(tmp_key_buf), K(tmp_pos),
                  K(common_key_pos), K(phy_key));
    } else if (OB_FALSE_IT(common_prefix_.assign(tmp_key_buf, common_key_pos))) {
    }
  }

  return ret;
}

int ObPrefixFirstVersionBaseIterator::get_next(ObString &user_key_buf,
                                               ObString &key_buf,
                                               ObString &val_buf)
{
  int ret = OB_SUCCESS;
  ObString tmp_key_buf;
  ObString tmp_val_buf;

  ObSSLogKVRowPhysicalKey tmp_key;
  int64_t tmp_pos = 0;
  int64_t user_key_end_pos = 0;

  if (OB_FAIL(construct_common_prefix_())) {
    STORAGE_LOG(WARN, "construct common prefix failed", K(ret), K(common_prefix_));
  } else {

    do {
      tmp_pos = 0;
      // user_key_end_pos = 0;
      if (OB_FAIL(inner_get_next_kv_(tmp_key_buf, tmp_val_buf))) {
        STORAGE_LOG(WARN, "inner get next kv failed", K(ret), K(tmp_key_buf), K(tmp_val_buf));
      } else if (OB_FAIL(tmp_key.deserialize(tmp_key_buf.ptr(), tmp_key_buf.length(), tmp_pos))) {
        STORAGE_LOG(WARN, "deserialize the next kv failed", K(ret), K(tmp_key_buf), K(tmp_pos),
                    K(tmp_key));
      } else if (!tmp_key.is_first_version()) {
        ret = OB_EAGAIN;
        STORAGE_LOG(DEBUG, "Skip the non-first version", K(ret), K(tmp_key_buf), K(tmp_key));
        } else if (OB_FAIL(tmp_key.get_user_key_end_pos(user_key_end_pos))) {
          STORAGE_LOG(WARN, "get the user_key_end_pos failed", K(ret), K(tmp_key_buf),
          K(tmp_key),
                      K(user_key_end_pos));
      } else {
        key_buf = tmp_key_buf;
        val_buf = tmp_val_buf;
        user_key_buf.assign_ptr(tmp_key_buf.ptr(), user_key_end_pos);

        STORAGE_LOG(INFO, "Get the first version", K(ret), K(key_buf.length()), K(val_buf), K(user_key_buf.length()),
                    K(tmp_key));
      }

    } while (OB_EAGAIN == ret);
  }

  return ret;
}

int ObUserKeyAllVersionBaseIterator::gc_unused_version()
{
  int ret = OB_SUCCESS;
  ObSSLogKVRowPhysicalKey phy_key;
  int64_t tmp_pos = 0;
  if (!construct_finished_) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "the verison list has not been ready", K(ret), KPC(this));
  } else if (physical_key_buffer_.empty() || unused_kv_list_.is_empty()) {
    STORAGE_LOG(INFO, "empty version or physical_key_buffer, no need to gc", K(ret),
                K(physical_key_buffer_), K(unused_kv_list_.get_size()));
  } else if (OB_FAIL(phy_key.deserialize(physical_key_buffer_.ptr(), physical_key_buffer_.length(),
                                         tmp_pos))) {
    STORAGE_LOG(WARN, "deserialize physical_key failed", K(ret), K(physical_key_buffer_),
                K(tmp_pos), K(phy_key));
  } else {
    const uint64_t cur_time = ObTimeUtility::fast_current_time();
    DLIST_FOREACH_REMOVESAFE(cur, unused_kv_list_)
    {
      phy_key.inner_key_ = cur->cur_key_;
      tmp_pos = 0;
      if (cur->src_ == ObSSLogKVVersionKeyPair::VersionSource::UNUSED
          && cur_time < cur->commit_version_.get_val_for_gts() / 1000 + delay_unused_kv_gc_time_) {
        STORAGE_LOG(INFO, "[PALF KV GC] Skip the unused kv", K(ret), K(phy_key),
                    K(physical_key_buffer_), KPC(cur));
      } else if (OB_FAIL(phy_key.serialize(physical_key_buffer_.ptr(),
                                           physical_key_buffer_.capacity(), tmp_pos))) {
        STORAGE_LOG(WARN, "serialize the physical_key buffer failed", K(ret),
                    K(physical_key_buffer_), K(tmp_pos), K(phy_key));
      } else {
        physical_key_buffer_.set_length(tmp_pos);
        if (OB_ISNULL(interface_)) {
          ret = OB_ERR_UNEXPECTED;
          STORAGE_LOG(ERROR, "unexpected palf kv interface", K(ret), KP(interface_), K(phy_key));
        } else if (OB_FAIL(interface_->delete_kv(physical_key_buffer_.string()))) {
          STORAGE_LOG(WARN, "delete unused kv failed", K(ret), K(phy_key), K(physical_key_buffer_),
                      KPC(cur));
        } else {
          deleted_kv_cnt_++;
          STORAGE_LOG(INFO, "[PALF KV GC] delete unused kv successfully", K(ret), K(phy_key),
                      K(physical_key_buffer_), KPC(cur));
        }
      }
    }
  }

  return ret;
}

int ObUserKeyAllVersionBaseIterator::gc_old_version(const share::SCN &max_gc_scn)
{
  int ret = OB_SUCCESS;

  ObSSLogKVRowPhysicalKey phy_key;
  int64_t tmp_pos = 0;
  if (!construct_finished_) {
    ret = OB_EAGAIN;
    STORAGE_LOG(WARN, "the verison list has not been ready", K(ret), KPC(this));
  } else if (physical_key_buffer_.empty() || version_kv_list_.is_empty()) {
    STORAGE_LOG(INFO, "empty version or physical_key_buffer, no need to gc", K(ret),
                K(physical_key_buffer_), K(version_kv_list_.get_size()));
  } else if (OB_FAIL(phy_key.deserialize(physical_key_buffer_.ptr(), physical_key_buffer_.length(),
                                         tmp_pos))) {
    STORAGE_LOG(WARN, "deserialize physical_key failed", K(ret), K(physical_key_buffer_),
                K(tmp_pos), K(phy_key));
  } else {
    ObSSLogKVVersionKeyPair *target_version_ptr = nullptr;
    ObSSLogKVVersionKeyPair *delete_first_version_ptr = nullptr;
    uint64_t before_gc_version_cnt = version_kv_list_.get_size();

    DLIST_FOREACH_REMOVESAFE(cur, version_kv_list_)
    {
      phy_key.inner_key_ = cur->cur_key_;
      tmp_pos = 0;
      if (cur->commit_version_ >= max_gc_scn) {
        target_version_ptr = cur;
        break;
      } else {
        if (cur->cur_key_.is_first_version()) {
          delete_first_version_ptr = cur;
        } else if (cur->next_key_.is_last_version() || cur == version_kv_list_.get_last()) {
          STORAGE_LOG(INFO, "[PALF KV GC] remain the last version", K(ret), K(phy_key.user_key_),
                      K(max_gc_scn), KPC(cur), KPC(delete_first_version_ptr));
        } else {
          version_kv_list_.remove(cur);
          cur->src_ = ObSSLogKVVersionKeyPair::VersionSource::OLD;
          unused_kv_list_.add_last(cur);
          STORAGE_LOG(INFO, "[PALF KV GC] prepare to delete version kv", K(ret), K(phy_key.user_key_),
                      K(max_gc_scn), KPC(cur), KPC(delete_first_version_ptr));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (version_kv_list_.get_size() < 2 && !version_kv_list_.get_first()->next_key_.is_last_version()) {
        STORAGE_LOG(ERROR, "find the last version failed", K(ret), K(version_kv_list_.get_size()),
                    KPC(version_kv_list_.get_last()));
      } else if (before_gc_version_cnt > 2 && target_version_ptr == nullptr) {
        target_version_ptr = version_kv_list_.get_last();
      }
    }

    if (OB_SUCC(ret) && target_version_ptr != nullptr && delete_first_version_ptr != nullptr) {
      ObStringBuffer physical_val_buffer(&allocator_);
      phy_key.inner_key_.set_as_first_version();
      ObSSLogKVRowPhysicalValue physical_val;
      physical_val.inner_val_.set_row_auto_commit(target_version_ptr->cur_key_.key_version_);
      physical_val.inner_val_.set_deleted();
      physical_val.next_key_ = target_version_ptr->cur_key_;

      int64_t min_capacity = physical_val.get_serialize_size() + 128;

      if (OB_FAIL(physical_val_buffer.reserve(min_capacity))) {
        STORAGE_LOG(WARN, "reserve memory for serialize failed", K(ret), K(min_capacity),
                    K(physical_val_buffer));
      } else if (OB_FAIL(phy_key.serialize(physical_key_buffer_.ptr(),
                                           physical_key_buffer_.capacity(), tmp_pos))) {
        STORAGE_LOG(WARN, "serialize the physical_key buffer failed", K(ret),
                    K(physical_key_buffer_), K(tmp_pos), K(phy_key));
      } else if (OB_FALSE_IT(physical_key_buffer_.set_length(tmp_pos))) {
      } else if (OB_FALSE_IT(tmp_pos = 0)) {
      } else if (OB_FAIL(physical_val.serialize(physical_val_buffer.ptr(),
                                                physical_val_buffer.capacity(), tmp_pos))) {
        STORAGE_LOG(WARN, "serialize first version value failed", K(ret), K(physical_val_buffer),
                    K(tmp_pos), K(physical_val));
      } else if (OB_FALSE_IT(physical_val_buffer.set_length(tmp_pos))) {
      } else if (OB_FAIL(interface_->set(physical_key_buffer_.string(),
                                         physical_val_buffer.string()))) {
        STORAGE_LOG(WARN, "set the empty first version failed", K(ret), K(physical_key_buffer_),
                    K(physical_val_buffer), K(phy_key));
      } else {
        STORAGE_LOG(INFO, "linke the first version and traget version successfully", K(ret),
                    K(phy_key), K(physical_val), KPC(delete_first_version_ptr),
                    KPC(target_version_ptr), K(min_version_scn_));
      }
    }
  }
  return ret;
}

int ObUserKeyAllVersionBaseIterator::construct_version_list()
{
  int ret = OB_SUCCESS;

  reset_version_kv_();

  int64_t tmp_pos = 0;
  ObString tmp_key_buf;
  ObString tmp_val_buf;
  ObSSLogKVRowPhysicalKey tmp_key;
  ObSSLogKVRowPhysicalValue tmp_val;
  ObSSLogKVRowPhysicalKey last_key_bk;

  do {
    tmp_pos = 0;
    ObSSLogKVVersionKeyPair *kv_version_ptr = nullptr;
    last_key_bk = tmp_key;
    tmp_key.reset();
    tmp_val.reset();

    if (OB_FAIL(inner_get_next_kv_(tmp_key_buf, tmp_val_buf))) {
      STORAGE_LOG(WARN, "inner get next kv failed", K(ret), K(tmp_key_buf), K(tmp_val_buf));
    } else if (OB_FAIL(tmp_key.deserialize(tmp_key_buf.ptr(), tmp_key_buf.length(), tmp_pos))) {
      STORAGE_LOG(WARN, "deserialize the next key failed", K(ret), K(tmp_key_buf), K(tmp_pos),
                  K(tmp_key));
    } else if (OB_FALSE_IT(tmp_pos = 0)) {
    } else if (OB_FAIL(tmp_val.deserialize(tmp_val_buf.ptr(), tmp_val_buf.length(), tmp_pos))) {
      STORAGE_LOG(WARN, "deserialize the next val failed", K(ret), K(tmp_val_buf), K(tmp_pos),
                  K(tmp_val));
    } else if (OB_ISNULL(kv_version_ptr = static_cast<ObSSLogKVVersionKeyPair *>(
                             allocator_.alloc(sizeof(ObSSLogKVVersionKeyPair))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc the memory of a kv version failed", K(ret), KP(kv_version_ptr));
    } else if (OB_FAIL(physical_key_buffer_.reserve(MAX(500 * 1024L, tmp_key_buf.length())))) {
      STORAGE_LOG(WARN, "reserve the memory of key buffer", K(ret), K(physical_key_buffer_),
                  K(tmp_key_buf));
    } else if (physical_key_buffer_.empty() && OB_FAIL(physical_key_buffer_.append(tmp_key_buf))) {
      STORAGE_LOG(WARN, "append tmp key buf fail", K(ret), K(physical_key_buffer_), K(tmp_key_buf));
    } else {
      new (kv_version_ptr) ObSSLogKVVersionKeyPair();
      kv_version_ptr->cur_key_ = tmp_key.inner_key_;
      kv_version_ptr->commit_version_ = tmp_val.inner_val_.tx_version_;
      kv_version_ptr->next_key_ = tmp_val.next_key_;
      kv_version_ptr->src_ = ObSSLogKVVersionKeyPair::VersionSource::UNUSED;
      min_version_scn_ = share::SCN::min(min_version_scn_, kv_version_ptr->commit_version_);

      ObSSLogKVVersionKeyPair *next_version_ptr = nullptr;

      DLIST_FOREACH(cur, unused_kv_list_)
      {
        if (cur->commit_version_ >= kv_version_ptr->commit_version_) {
          next_version_ptr = cur;
          break;
        }
      }

      if (next_version_ptr != nullptr) {
        unused_kv_list_.add_before(next_version_ptr, kv_version_ptr);
      } else {
        unused_kv_list_.add_last(kv_version_ptr);
      }

      STORAGE_LOG(DEBUG, "add kv version", K(ret), KP(kv_version_ptr), KP(next_version_ptr),
                  K(unused_kv_list_.get_size()), KPC(kv_version_ptr), KPC(next_version_ptr),
                  K(tmp_key), K(tmp_val));
    }
    if (OB_SUCC(ret)) {
      ret = OB_EAGAIN;
    } else if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else if (OB_NOT_NULL(kv_version_ptr)) {
      allocator_.free(kv_version_ptr);
      kv_version_ptr = nullptr;
    }
  } while (OB_EAGAIN == ret);

  uint64_t before_first_version_cnt = 0;
  ObSSLogKVVersionKeyPair *last_version_ptr = nullptr;
  uint64_t skip_version_cnt = 0;

  if (OB_SUCC(ret) && !unused_kv_list_.is_empty()) {
    DLIST_FOREACH_REMOVESAFE(cur, unused_kv_list_)
    {
      // STORAGE_LOG(INFO, "print cur", K(ret), KP(cur), KPC(cur));
      if (version_kv_list_.is_empty()) {
        if (!cur->cur_key_.is_first_version()) {
          STORAGE_LOG(INFO, "The first version was not found, skip it", K(ret), KPC(cur));
          min_need_gc_scn_ = share::SCN::min(min_need_gc_scn_, cur->commit_version_);
          before_first_version_cnt++;
        } else {
          STORAGE_LOG(INFO, "find the first version", K(ret), KPC(cur));
          unused_kv_list_.remove(cur);
          cur->src_ = ObSSLogKVVersionKeyPair::VersionSource::MVCC;
          version_kv_list_.add_first(cur);
        }
      } else {
        min_need_gc_scn_ = share::SCN::min(min_need_gc_scn_, cur->commit_version_);
        if (version_kv_list_.get_last()->next_key_.key_version_ == cur->cur_key_.key_version_) {
          STORAGE_LOG(INFO, "find the next version", K(ret), KPC(cur),
                      KPC(version_kv_list_.get_last()));
          if (cur->next_key_.is_last_version()) {
            last_version_ptr = cur;
          }
          unused_kv_list_.remove(cur);
          cur->src_ = ObSSLogKVVersionKeyPair::VersionSource::MVCC;
          version_kv_list_.add_last(cur);
        } else {
          skip_version_cnt++;
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    construct_finished_ = true;
  }

  if (OB_SUCC(ret) && version_kv_list_.get_size() >= 2 && last_version_ptr == nullptr) {
    STORAGE_LOG(ERROR, "The last version is not found", K(ret), K(last_key_bk.user_key_),
                KPC(last_version_ptr));
  }
  STORAGE_LOG(INFO, "construct version kv", K(ret), K(last_key_bk.user_key_),
              K(version_kv_list_.get_size()), K(unused_kv_list_.get_size()), K(min_version_scn_),
              K(skip_version_cnt), K(before_first_version_cnt), KPC(last_version_ptr),
              K(construct_finished_));

  return ret;
}

void ObUserKeyAllVersionBaseIterator::reset_version_kv_()
{
  int ret = OB_SUCCESS;
  DLIST_REMOVE_ALL(cur, unused_kv_list_) { allocator_.free(cur); }
  DLIST_REMOVE_ALL(cur, version_kv_list_) { allocator_.free(cur); }
  construct_finished_ = false;
}

int ObPhysicalRowInnerTool::generate_base_new_version(const ObSSLogKVRowPhysicalKey &old_key,
                                                      const ObSSLogKVRowPhysicalValue &old_value,
                                                      ObSSLogKVRowPhysicalKey &new_key,
                                                      ObSSLogKVRowPhysicalValue &new_value)
{
  int ret = OB_SUCCESS;

  new_key = old_key;
  new_value = old_value;
  new_key.inner_key_.key_version_ = old_value.inner_val_.row_version_;
  new_value.inner_val_.set_row_auto_commit(share::SCN::invalid_scn());
  new_value.next_key_.set_as_last_version();

  return ret;
}

int ObPhysicalRowInnerTool::insert_new_version(ObSSLogKVRowPhysicalKey &new_key,
                                               ObSSLogKVRowPhysicalValue &new_value,
                                               ObSSLogKVRowPhysicalBuf &new_buf,
                                               ObPalfKVAdpaterInterface *interface)
{
  int ret = OB_SUCCESS;
  share::SCN row_version;
  int64_t tmp_pos = 0;
  new_buf.reuse();

  if (!new_buf.is_inited() || (nullptr == interface) || !new_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(new_key), K(new_value), KP(interface));
  } else if (OB_FAIL(interface->get_gts(row_version))) {
    STORAGE_LOG(WARN, "get gts failed", K(ret), K(row_version), K(new_key), K(new_value));
  } else if (OB_FALSE_IT(new_value.inner_val_.set_row_auto_commit(row_version))) {
  } else if (!new_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(new_key), K(new_value), KP(interface));
  } else if (OB_FAIL(new_key.serialize(new_buf.get_key_buf(),
                                       ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN, tmp_pos))) {
    STORAGE_LOG(WARN, "serialize new key failed", K(ret), K(new_key), K(new_buf), K(tmp_pos));
  } else if (OB_FALSE_IT(new_buf.set_key_buf_pos(tmp_pos))) {
  } else if (OB_FALSE_IT(tmp_pos = 0)) {
  } else if (OB_FAIL(new_value.serialize(new_buf.get_val_buf(),
                                         ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN, tmp_pos))) {
    STORAGE_LOG(WARN, "serialize new value failed", K(ret), K(new_value), K(new_buf));
  } else {
    new_buf.set_val_buf_pos(tmp_pos);

    ObString new_version_key;
    ObString new_version_val;
    new_version_key.assign_ptr(new_buf.get_key_buf(), new_buf.get_key_buf_pos());
    new_version_val.assign_ptr(new_buf.get_val_buf(), new_buf.get_val_buf_pos());
    if (OB_FAIL(interface->put(new_version_key, new_version_val))) {
      STORAGE_LOG(WARN, "insert into new version failed", K(ret), K(new_buf), K(new_key),
                  K(new_value));
      if (OB_ENTRY_EXIST == ret) {
        ret = OB_EAGAIN;
        STORAGE_LOG(WARN, "new version is existed, need retry", K(ret), K(new_buf), K(new_key),
                    K(new_value));
        interface->inc_unused_kv_cnt();
      }
    } else {
      STORAGE_LOG(INFO, "insert into new version successfully", K(ret), K(new_buf), K(new_key),
                  K(new_value));
    }
  }

  return ret;
}

int ObPhysicalRowInnerTool::link_next_version(const ObSSLogKVRowPhysicalKey &old_key,
                                              const ObSSLogKVRowPhysicalValue &old_value,
                                              const ObSSLogKVRowPhysicalBuf &old_buf,
                                              const ObSSLogKVRowPhysicalBuf &new_version_buf,
                                              ObSSLogKVRowPhysicalBuf &old_version_buf,
                                              ObPalfKVAdpaterInterface *interface)
{
  int ret = OB_SUCCESS;

  int64_t tmp_pos = 0;
  old_version_buf.reuse();

  ObSSLogKVRowPhysicalValue next_key_value;
  ObString old_key_buf, old_val_buf, next_key_val_buf, watch_key_buf;
  next_key_value = old_value;
  next_key_value.next_key_ = old_key.inner_key_;
  next_key_value.next_key_.key_version_ = old_value.inner_val_.row_version_;
  bool expected = false;

  if (!old_version_buf.is_inited() || !new_version_buf.is_inited() || !old_buf.is_inited()
      || (nullptr == interface) || !old_key.is_valid() || !old_value.is_valid()
      || !next_key_value.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(old_version_buf), K(new_version_buf),
                K(old_buf), K(old_key), K(old_value), K(next_key_value), KP(interface));
  } else if (OB_FAIL(next_key_value.serialize(old_version_buf.get_val_buf(),
                                              ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN, tmp_pos))) {
    STORAGE_LOG(WARN, "serialize new value failed", K(ret), K(old_key), K(next_key_value),
                K(old_version_buf));
  } else {
    old_version_buf.set_val_buf_pos(tmp_pos);

    old_key_buf.assign_ptr(old_buf.get_key_buf(), old_buf.get_key_buf_pos());
    old_val_buf.assign_ptr(old_buf.get_val_buf(), old_buf.get_val_buf_pos());
    next_key_val_buf.assign_ptr(old_version_buf.get_val_buf(), old_version_buf.get_val_buf_pos());
    watch_key_buf.assign_ptr(new_version_buf.get_key_buf(), new_version_buf.get_key_buf_pos());

    if (OB_FAIL(interface->watch_cas(old_key_buf, watch_key_buf, old_val_buf, next_key_val_buf,
                                     expected))) {
      STORAGE_LOG(WARN, "watch cas KV failed", K(ret), K(old_key), K(old_value), K(next_key_value),
                  K(old_buf), K(old_version_buf), K(expected));
    } else if (!expected) {
      ret = OB_EAGAIN;
      STORAGE_LOG(WARN, "watch cas KV faild, need retry", K(ret), K(old_key), K(old_value),
                  K(next_key_value), K(old_buf), K(old_version_buf), K(expected));
    } else {
      STORAGE_LOG(INFO, "watch cas KV for update next_key successfully", K(ret), K(old_key),
                  K(old_value), K(next_key_value), K(old_buf), K(old_version_buf), K(expected));
    }
  }

  if (OB_FAIL(ret)) {
    interface->inc_unused_kv_cnt();
  }

  return ret;
}

int ObPhysicalRowInnerTool::gc_data_version_kv(ObPalfKVAdpaterInterface *interface,
                                               ObPrefixFirstVersionBaseIterator *first_version_iter,
                                               ObUserKeyAllVersionBaseIterator *all_version_iter,
                                               const share::SCN &max_gc_scn)
{
  int ret = OB_SUCCESS;

  STORAGE_LOG(INFO, "[PALF KV GC] start to gc data version kv", K(ret), KP(interface),
              KP(first_version_iter), KP(all_version_iter), K(max_gc_scn));
  int64_t row_count = 0;
  uint64_t total_version_kv_count = 0;
  uint64_t unused_version_kv_count = 0;
  uint64_t deleted_kv_count = 0;
  share::SCN need_gc_min_kv_scn_before_gc;
  share::SCN real_min_kv_scn_before_gc;
  uint64_t need_gc_min_scn_row_kv_count = 0;
  uint64_t real_min_scn_row_kv_count = 0;
  need_gc_min_kv_scn_before_gc.set_max();
  real_min_kv_scn_before_gc.set_max();

  if (OB_ISNULL(interface) || OB_ISNULL(first_version_iter) || OB_ISNULL(all_version_iter)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(interface), K(first_version_iter),
                K(all_version_iter), K(max_gc_scn));
  } else {
    ObString user_key_buf;
    ObString key_buf;
    ObString val_buf;
    first_version_iter->reset();
    while (OB_SUCC(first_version_iter->get_next(user_key_buf, key_buf, val_buf))) {
      all_version_iter->reset();
      all_version_iter->set_base_param(user_key_buf, interface);
      if (OB_FAIL(all_version_iter->construct_version_list())) {
        STORAGE_LOG(WARN, "construct version list failed", K(ret), K(user_key_buf), K(max_gc_scn));
      } else if (!max_gc_scn.is_valid_and_not_min()) {
      } else if (OB_FAIL(all_version_iter->gc_old_version(max_gc_scn))) {
        STORAGE_LOG(WARN, "gc old version failed", K(ret), K(user_key_buf), K(max_gc_scn));
      }
      if (OB_SUCC(ret) && OB_FAIL(all_version_iter->gc_unused_version())) {
        STORAGE_LOG(WARN, "gc unused version failed", K(ret), K(user_key_buf), K(max_gc_scn));
      }
      row_count++;
      total_version_kv_count += all_version_iter->get_version_kv_size();
      unused_version_kv_count += all_version_iter->get_unused_kv_size();
      deleted_kv_count += all_version_iter->get_deleted_kv_cnt();
      // min_kv_version_before_gc = share::SCN::min(min_kv_version_before_gc,
      // all_version_iter->get_min_version_scn());
      if (real_min_kv_scn_before_gc > all_version_iter->get_min_version_scn()) {
        real_min_kv_scn_before_gc = all_version_iter->get_min_version_scn();
        real_min_scn_row_kv_count = all_version_iter->get_version_kv_size();
      }
      if (need_gc_min_kv_scn_before_gc > all_version_iter->get_min_need_gc_scn()
          && all_version_iter->get_version_kv_size() > 2) {
        need_gc_min_kv_scn_before_gc = all_version_iter->get_min_need_gc_scn();
        need_gc_min_scn_row_kv_count = all_version_iter->get_version_kv_size();
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  STORAGE_LOG(INFO, "[PALF KV GC] finish to gc data version kv", K(ret), KP(interface),
              KP(first_version_iter), KP(all_version_iter), K(max_gc_scn), K(row_count),
              K(total_version_kv_count), K(unused_version_kv_count), K(deleted_kv_count),
              K(need_gc_min_kv_scn_before_gc), K(need_gc_min_scn_row_kv_count),
              K(real_min_kv_scn_before_gc), K(real_min_scn_row_kv_count));

  return ret;
}

int ObPhysicalRowUpdateFunctor::operator()(const ObSSLogKVRowPhysicalKey &old_key,
                                           const ObSSLogKVRowPhysicalValue &old_value,
                                           const ObSSLogKVRowPhysicalBuf &old_buf)
{
  int ret = OB_SUCCESS;

  ObSSLogKVRowPhysicalKey new_key;
  ObSSLogKVRowPhysicalValue new_value;

  if (old_value.next_key_.is_last_version()) {

    ObSSLogKVRowPhysicalBuf new_version_buf;
    ObSSLogKVRowPhysicalBuf old_version_buf;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(old_version_buf.init())) {
        STORAGE_LOG(WARN, "init old_version_buf failed", K(ret), K(old_version_buf));
      } else if (OB_FAIL(new_version_buf.init())) {
        STORAGE_LOG(WARN, "init new_version_buf failed", K(ret), K(new_version_buf));
      }
    }

    if (OB_SUCC(ret)
        && OB_FAIL(ObPhysicalRowInnerTool::generate_base_new_version(old_key, old_value, new_key,
                                                                     new_value))) {
      STORAGE_LOG(WARN, "generate base new version failed", K(ret), K(old_key), K(old_value),
                  K(new_key), K(new_value));
    }

    if (OB_SUCC(ret) && param_.check_extra_info_) {
      if (0 != old_value.user_val_.extra_info_.compare(param_.extra_info_)) {
        ret = OB_ENTRY_NOT_EXIST;
      }
      STORAGE_LOG(INFO, "check extra_info for update", K(ret), K(old_key), K(old_value), K(param_));
    }

    if (OB_SUCC(ret)) {

      if (param_.only_update_extra_info_) {
        new_value.user_val_.meta_val_ = old_value.user_val_.meta_val_;
      } else {
        new_value.user_val_.meta_val_ = meta_value_;
      }
      new_value.user_val_.extra_info_ = extra_info_;

      if (OB_FAIL(ObPhysicalRowInnerTool::insert_new_version(new_key, new_value, new_version_buf,
                                                             interface_))) {
        STORAGE_LOG(WARN, "insert new version into kv failed", K(ret), K(old_key), K(old_value),
                    K(new_key), K(new_value));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPhysicalRowInnerTool::link_next_version(
              old_key, old_value, old_buf, new_version_buf, old_version_buf, interface_))) {
        STORAGE_LOG(WARN, "link next version failed", K(ret), K(old_key), K(old_value), K(new_key),
                    K(new_value), K(new_version_buf), K(old_version_buf));
      }
    }
  } else {
    STORAGE_LOG(INFO, "[SSLogKV] iter next version to update", K(ret), K(old_key), K(old_value),
                K(old_buf));
  }
  return ret;
}

int ObPhysicalRowDeleteFunctor::operator()(const ObSSLogKVRowPhysicalKey &old_key,
                                           const ObSSLogKVRowPhysicalValue &old_value,
                                           const ObSSLogKVRowPhysicalBuf &old_buf)
{
  int ret = OB_SUCCESS;

  ObSSLogKVRowPhysicalKey new_key;
  ObSSLogKVRowPhysicalValue new_value;

  if (old_value.next_key_.is_last_version()) {

    ObSSLogKVRowPhysicalBuf new_version_buf;
    ObSSLogKVRowPhysicalBuf old_version_buf;
    if (OB_SUCC(ret)) {
      if (OB_FAIL(old_version_buf.init())) {
        STORAGE_LOG(WARN, "init old_version_buf failed", K(ret), K(old_version_buf));
      } else if (OB_FAIL(new_version_buf.init())) {
        STORAGE_LOG(WARN, "init new_version_buf failed", K(ret), K(new_version_buf));
      }
    }

    if (OB_SUCC(ret)
        && OB_FAIL(ObPhysicalRowInnerTool::generate_base_new_version(old_key, old_value, new_key,
                                                                     new_value))) {
      STORAGE_LOG(WARN, "generate base new version failed", K(ret), K(old_key), K(old_value),
                  K(new_key), K(new_value));
    }

    if (OB_SUCC(ret)) {
      new_value.inner_val_.set_deleted();

      if (OB_FAIL(ObPhysicalRowInnerTool::insert_new_version(new_key, new_value, new_version_buf,
                                                             interface_))) {
        STORAGE_LOG(WARN, "insert new version into kv failed", K(ret), K(old_key), K(old_value),
                    K(new_key), K(new_value));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObPhysicalRowInnerTool::link_next_version(
              old_key, old_value, old_buf, new_version_buf, old_version_buf, interface_))) {
        STORAGE_LOG(WARN, "link next version failed", K(ret), K(old_key), K(old_value), K(new_key),
                    K(new_value), K(new_version_buf), K(old_version_buf));
      }
    }
  } else {
    STORAGE_LOG(INFO, "[SSLogKV] iter next version to delete", K(ret), K(old_key), K(old_value),
                K(old_buf));
  }

  return ret;
}

int ObSSLogKVResultRow::VersionVal::init(const ObSSLogKVRowUserValue &user_val,
                                         const share::SCN val_version,
                                         const share::SCN key_suffix,
                                         const share::SCN next_key_suffix,
                                         common::ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (!user_val.is_valid() || !val_version.is_valid_and_not_min() || !key_suffix.is_valid()
      || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(user_val_), K(val_version), K(key_suffix),
                K(next_key_suffix), KP(allocator));
  } else {
    val_buf_.set_allocator(allocator);
    int64_t tmp_pos = 0;
    if (OB_FAIL(val_buf_.reserve(user_val.get_serialize_size() + 10))) {
      STORAGE_LOG(WARN, "reserve the buf of a user_val failed", K(ret), K(val_buf_.length()),
                  K(user_val.get_serialize_size()));
    } else if (OB_FAIL(user_val.serialize(val_buf_.ptr(), val_buf_.capacity(), tmp_pos))) {
      STORAGE_LOG(WARN, "serialize the user_val failed", K(ret), K(val_buf_.length()), K(user_val));
    } else if (OB_FALSE_IT(val_buf_.set_length(tmp_pos))) {
    } else if (OB_FALSE_IT(tmp_pos = 0)) {
    } else if (OB_FAIL(user_val_.deserialize(val_buf_.ptr(), val_buf_.length(), tmp_pos))) {
      STORAGE_LOG(WARN, "deserialize into val_buf failed", K(ret), K(val_buf_.length()),
                  K(user_val));
    } else {
      val_version_ = val_version;
      key_suffix_ = key_suffix;
      next_key_suffix_ = next_key_suffix;
    }
  }

  return ret;
}

int ObSSLogKVResultRow::init(const ObSSLogKVRowUserKey user_key, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (!user_key.is_valid() || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(user_key), KP(allocator));
  } else {
    allocator_ = allocator;
    key_buf_.set_allocator(allocator);
    int64_t tmp_pos = 0;
    if (OB_FAIL(key_buf_.reserve(user_key.get_serialize_size() + 10))) {
      STORAGE_LOG(WARN, "reserve the key buf failed", K(ret), K(user_key),
                  K(user_key.get_serialize_size()), K(key_buf_.length()));
    } else if (OB_FAIL(user_key.serialize(key_buf_.ptr(), key_buf_.capacity(), tmp_pos))) {
      STORAGE_LOG(WARN, "serialize user_key failed", K(ret), K(user_key), K(key_buf_), K(tmp_pos));
    } else if (OB_FALSE_IT(key_buf_.set_length(tmp_pos))) {
    } else if (OB_FALSE_IT(tmp_pos = 0)) {
    } else if (OB_FAIL(user_key_.deserialize(key_buf_.ptr(), key_buf_.length(), tmp_pos))) {
      STORAGE_LOG(WARN, "deserialize the user_key_ failed", K(ret), K(key_buf_.length()),
                  K(tmp_pos), K(user_key_));
    } else {
      reset_val_();
    }
  }

  return ret;
}

int ObSSLogKVResultRow::copy_and_insert(const ObSSLogKVRowPhysicalKey &phy_key,
                                        const ObSSLogKVRowPhysicalValue &phy_val)
{
  int ret = OB_SUCCESS;

  if (phy_val.inner_val_.is_deleted()) {
    STORAGE_LOG(INFO, "skip the deleted version", K(ret), K(phy_key), K(phy_val));
  } else if (!phy_key.is_valid() || !phy_val.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(phy_key), K(phy_val));
  } else {
    VersionVal *tmp_version = static_cast<VersionVal *>(allocator_->alloc(sizeof(VersionVal)));
    bool alloc_failed = false;
    const int64_t val_count = val_.count();
    if (OB_ISNULL(tmp_version)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      alloc_failed = true;
      STORAGE_LOG(WARN, "alloc a version failed", K(ret), KP(tmp_version), K(phy_key));
    } else if (OB_FALSE_IT(new (tmp_version) VersionVal())) {
      // do nothing
    } else if (OB_FAIL(tmp_version->init(phy_val.user_val_, phy_val.inner_val_.tx_version_,
                                         phy_key.inner_key_.key_version_,
                                         phy_val.next_key_.key_version_, allocator_))) {
      STORAGE_LOG(WARN, "init the VersionVal failed", K(ret), K(phy_key), K(phy_val));
      //} else if (val_count > 0 && val_[val_count - 1]->val_version_ >= tmp_version->val_version_)
      //{
      //  ret = OB_ERR_UNEXPECTED;
      //  STORAGE_LOG(ERROR, "invalid new version", K(ret), K(val_count), KPC(val_[val_count - 1]),
      //              KPC(tmp_version));
    } else if (OB_FAIL(val_.push_back(tmp_version))) {
      STORAGE_LOG(WARN, "push back the version val failed", K(ret), KPC(tmp_version),
                  K(val_.count()));
    } else {
      int64_t before_index = -1;
      for (int64_t i = val_.count() - 2; i >= 0; i--) {
        if (val_[i]->val_version_ <= tmp_version->val_version_) {
          before_index = i;
          break;
        }
      }

      if (before_index + 1 < val_.count() - 1) {
        memmove(&val_[before_index + 2], &val_[before_index + 1],
                sizeof(VersionVal *) * (val_.count() - before_index - 2));
        val_[before_index + 1] = tmp_version;
      }
    }

    if (OB_FAIL(ret) && !alloc_failed) {
      tmp_version->~VersionVal();
      allocator_->free(tmp_version);
      tmp_version = nullptr;
    }
  }

  return ret;
}

int ObSSLogKVResultRow::deep_copy_version(const VersionVal *src_version, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(src_version) || OB_ISNULL(allocator) || OB_ISNULL(allocator_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(src_version), KP(allocator), KP(allocator_));
  } else {
    VersionVal *new_version = static_cast<VersionVal *>(allocator_->alloc(sizeof(VersionVal)));
    if (OB_ISNULL(new_version)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc new version val failed", K(ret));
    } else {
      new (new_version) VersionVal();
      
      if (OB_FAIL(new_version->init(src_version->user_val_,
                                    src_version->val_version_,
                                    src_version->key_suffix_,
                                    src_version->next_key_suffix_,
                                    allocator_))) {
        STORAGE_LOG(WARN, "init new version val failed", K(ret));
        new_version->~VersionVal();
        allocator_->free(new_version);
      } else if (OB_FAIL(val_.push_back(new_version))) {
        STORAGE_LOG(WARN, "push back new version failed", K(ret));
        new_version->~VersionVal();
        allocator_->free(new_version);
      }
    }
  }

  return ret;
}

void ObSSLogKVResultRow::print_version_val() const
{
  int ret = OB_SUCCESS;
  for (int version_j = 0; version_j < val_.count(); version_j++) {
    STORAGE_LOG(INFO, "print version", K(ret), K(user_key_), K(version_j), KPC(val_[version_j]));
  }
}

bool ObSSLogTableLogicalRow::is_inited() const { return physical_buf_.is_inited(); }

int ObSSLogTableLogicalRow::insert_new_row(const ObString meta_value, const ObString &extra_info)
{
  int ret = OB_SUCCESS;
  share::SCN row_version;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the logical row is not inited", K(ret));
  } else if (!physical_key_.user_key_.is_valid() || nullptr == interface_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid physical_key_", K(ret), K(physical_key_.user_key_), KP(interface_));
  } else if (OB_FAIL(interface_->get_gts(row_version))) {
    STORAGE_LOG(WARN, "get gts failed", K(ret), K(row_version), KPC(this));
  } else {
    physical_key_.inner_key_.set_as_new_row();
    physical_val_.user_val_.meta_val_ = meta_value;
    physical_val_.user_val_.extra_info_ = extra_info;
    physical_val_.inner_val_.set_row_auto_commit(row_version);
    physical_val_.next_key_.reset();
    physical_val_.next_key_.type_ = physical_key_.inner_key_.type_;

    int64_t tmp_pos = 0;
    if (OB_FAIL(physical_key_.serialize(physical_buf_.get_key_buf(),
                                        ObSSLogKVRowPhysicalBuf::MAX_KEY_BUF_LEN, tmp_pos))) {
      STORAGE_LOG(WARN, "serialize physical key failed", K(ret), KPC(this));
    } else {
      physical_buf_.set_key_buf_pos(tmp_pos);
      physical_key_.reset();
      tmp_pos = 0;
      if (OB_FAIL(physical_key_.deserialize(physical_buf_.get_key_buf(),
                                            physical_buf_.get_key_buf_pos(), tmp_pos))) {
        STORAGE_LOG(WARN, "deserialize physical_key_ failed", K(ret), KPC(this));
      }
    }

    tmp_pos = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(physical_val_.serialize(physical_buf_.get_val_buf(),
                                               ObSSLogKVRowPhysicalBuf::MAX_VAL_BUF_LEN,
                                               tmp_pos))) {
      STORAGE_LOG(WARN, "serialize physical value failed", K(ret), KPC(this));
    } else {
      physical_buf_.set_val_buf_pos(tmp_pos);
      physical_val_.reset();
      tmp_pos = 0;
      if (OB_FAIL(physical_val_.deserialize(physical_buf_.get_val_buf(),
                                            physical_buf_.get_val_buf_pos(), tmp_pos))) {
        STORAGE_LOG(WARN, "deserialize physical_val_ failed", K(ret), KPC(this));
      }
    }

    if (OB_SUCC(ret)) {
      ObString key, val;
      key.assign_ptr(physical_buf_.get_key_buf(), physical_buf_.get_key_buf_pos());
      val.assign_ptr(physical_buf_.get_val_buf(), physical_buf_.get_val_buf_pos());
      if (OB_FAIL(interface_->put(key, val))) {
        STORAGE_LOG(WARN, "insert new row into KV failed", K(ret), K(key), K(val), K(physical_key_),
                    K(physical_val_));
      }
    }
  }

  return ret;
}

int ObSSLogTableLogicalRow::update_row(const ObSSLogWriteParam &param,
                                       const ObString &meta_value,
                                       const ObString &extra_info)
{

  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the logical row is not inited", K(ret));
  } else if (!physical_key_.user_key_.is_valid() || nullptr == interface_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid physical_key_", K(ret), K(physical_key_.user_key_), KP(interface_));
  } else {
    ObPhysicalRowUpdateFunctor update_fn(interface_, param, meta_value, extra_info);
    if (OB_FAIL(iter_row_version_(update_fn))) {
      STORAGE_LOG(WARN, "iter for udpate KV failed", K(ret), K(param), K(meta_value), K(extra_info),
                  KPC(this));
    }
  }

  return ret;
}

int ObSSLogTableLogicalRow::delete_row()
{
  int ret = OB_SUCCESS;
  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the logical row is not inited", K(ret));
  } else if (!physical_key_.user_key_.is_valid() || nullptr == interface_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid physical_key_", K(ret), K(physical_key_.user_key_), KP(interface_));
  } else {
    ObPhysicalRowDeleteFunctor delete_fn(interface_);
    if (OB_FAIL(iter_row_version_(delete_fn))) {
      STORAGE_LOG(WARN, "iter for delete KV failed", K(ret), KPC(this));
    }
  }

  return ret;

}

int ObSSLogTableLogicalRow::read_row(const share::SCN start_snapshot,
                                     const share::SCN end_snapshot,
                                     ObSSLogKVResultRowArray &result_row_arr,
                                     common::ObIAllocator *allocator,
                                     bool is_prefix_read,
                                     bool include_oldest_version)
{
  int ret = OB_SUCCESS;

  if (!ObSSLogKVResultSet::snapshot_range_is_valid(start_snapshot, end_snapshot) || !is_inited()
      || !physical_key_.user_key_.is_valid() || nullptr == interface_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(start_snapshot), K(end_snapshot), KPC(this));
  }

  // list_key
  if (OB_SUCC(ret)
      && OB_FAIL(construct_version_by_list_(start_snapshot, end_snapshot, result_row_arr, allocator,
                                            is_prefix_read, include_oldest_version))) {
    STORAGE_LOG(WARN, "construct data version by list failed", K(ret), K(start_snapshot),
                K(end_snapshot), K(result_row_arr.count()), KP(allocator), K(is_prefix_read));
  }

  return ret;
}

int ObSSLogTableLogicalRow::construct_version_by_list_(const share::SCN start_snapshot,
                                                       const share::SCN end_snapshot,
                                                       ObSSLogKVResultRowArray &result_array,
                                                       common::ObIAllocator *allocator,
                                                       const bool is_prefix_read,
                                                       const bool include_oldest_version)
{
  int ret = OB_SUCCESS;

  ObString list_key;
  ObSSLogKVTempCache kvs(MTL_ID());
  int64_t MAX_RESULT_ROW_COUNT = 1;

  STORAGE_LOG(DEBUG, "start to construct version list", K(ret), K(start_snapshot), K(end_snapshot),
              K(is_prefix_read));

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the logical row is not inited", K(ret));
  } else if (!physical_key_.user_key_.is_valid() || nullptr == interface_
             || start_snapshot > end_snapshot) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid physical_key_", K(ret), K(physical_key_.user_key_),
                K(start_snapshot), K(end_snapshot), KP(interface_));
  }

  if (OB_SUCC(ret)) {
    int64_t user_key_end_pos = 0;
    if (OB_FAIL(physical_key_.get_user_key_end_pos(user_key_end_pos))) {
      STORAGE_LOG(WARN, "get user_key_end_pos failed", K(ret), K(physical_key_),
                  K(user_key_end_pos));
    } else {
      list_key.assign_ptr(physical_buf_.get_key_buf(), user_key_end_pos);
      if (is_prefix_read) {
        MAX_RESULT_ROW_COUNT = INT64_MAX;
      } else {
        MAX_RESULT_ROW_COUNT = 1;
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObSSLogKVTempCache kvs(MTL_ID());
    ObSEArray<ObPhyVersionNode, 1> phy_node_arr;
    if (OB_FAIL(interface_->list(list_key, 0, kvs))) {
      STORAGE_LOG(WARN, "list palf kv failed", K(ret), K(list_key.length()),
                  K(physical_key_.user_key_), K(kvs.count()));
    } else if (OB_FAIL(phy_node_arr.reserve(kvs.count()))) {
      STORAGE_LOG(WARN, "reserve phy node failed", K(ret), K(kvs.count()),
                  K(phy_node_arr.get_capacity()));
    } else {
      ObSSLogKVRowPhysicalKey tmp_physical_key;
      ObSSLogKVRowPhysicalValue tmp_physical_val;
      ObSSLogTableLogicalRow::ObPhyVersionArray phy_version_arr;
      ObString tmp_key_buf;
      ObString tmp_val_buf;

      for (int64_t i = 0; OB_SUCC(ret) && i < kvs.count(); i++) {
        tmp_physical_key.reset();
        tmp_physical_val.reset();
        tmp_key_buf.reset();
        tmp_val_buf.reset();
        int64_t tmp_key_pos, tmp_val_pos;
        tmp_key_pos = tmp_val_pos = 0;
        int64_t result_row_index = -1;

        if (OB_FAIL(kvs.key_at(i, tmp_key_buf))) {
          STORAGE_LOG(WARN, "get key str failed", K(ret), K(i), K(tmp_key_buf));
        } else if (OB_FAIL(kvs.val_at(i, tmp_val_buf))) {
          STORAGE_LOG(WARN, "get val str failed", K(ret), K(i), K(tmp_val_buf));
        } else if (OB_FAIL(tmp_physical_key.deserialize(tmp_key_buf.ptr(), tmp_key_buf.length(),
                                                        tmp_key_pos))) {
          STORAGE_LOG(WARN, "deserialize physical_key_ failed", K(ret), K(i), K(tmp_physical_key),
                      K(tmp_key_buf));
        } else if (OB_FAIL(tmp_physical_val.deserialize(tmp_val_buf.ptr(), tmp_val_buf.length(),
                                                        tmp_val_pos))) {
          STORAGE_LOG(WARN, "deserialize physical_val_ failed", K(ret), K(i), K(tmp_physical_val),
                      K(tmp_val_buf));
        } else if (OB_FAIL(find_result_row_(physical_key_.user_key_, tmp_physical_key,
                                            MAX_RESULT_ROW_COUNT, allocator, phy_version_arr,
                                            result_array, result_row_index))) {
          STORAGE_LOG(WARN, "find result row failed", K(ret), K(tmp_physical_key), K(i));
        } else if (result_row_index < 0) {
          // do nothing
        } else if (OB_FAIL(phy_node_arr.push_back(
                       ObPhyVersionNode(tmp_physical_key, tmp_physical_val)))) {
          STORAGE_LOG(WARN, "push back a physical node", K(ret), K(tmp_physical_key),
                      K(tmp_physical_val), K(phy_node_arr.count()));
        } else if (OB_FAIL(link_next_version_(phy_version_arr, result_row_index,
                                              &phy_node_arr[phy_node_arr.count() - 1]))) {
          STORAGE_LOG(WARN, "link the next version failed", K(ret), K(result_row_index),
                      K(phy_node_arr.count()), K(phy_node_arr[phy_node_arr.count() - 1]));
        }
      }

      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(filter_result_val_(phy_version_arr, result_array, start_snapshot,
                                            end_snapshot, is_prefix_read,
                                            include_oldest_version))) {
        STORAGE_LOG(WARN, "filter the result val failed", K(ret), K(result_array.count()),
                    K(is_prefix_read), K(include_oldest_version), K(start_snapshot),
                    K(end_snapshot));
      } else {

        lib::ob_sort(result_array.begin(), result_array.end(),
                     ObSSLogKVResultRow::ObSSLogRowValueComp());
      }
    }
  }

  return ret;
}

int ObSSLogTableLogicalRow::find_result_row_(const ObSSLogKVRowUserKey &user_key,
                                             const ObSSLogKVRowPhysicalKey &phy_key,
                                             const int64_t max_resut_row_index,
                                             common::ObIAllocator *allocator,
                                             ObPhyVersionArray &phy_version_array,
                                             ObSSLogKVResultRowArray &result_array,
                                             int64_t &result_row_index)
{
  int ret = OB_SUCCESS;

  result_row_index = -1;

  if (ObSSLogKVResultSet::contain_user_key(phy_key.user_key_, result_array, result_row_index)) {
    // do nothing
  } else if (result_array.count() >= max_resut_row_index) {
    result_row_index = -1;
  } else if (max_resut_row_index == 1 && !phy_key.user_key_.equal_to(user_key)) {
    result_row_index = -1;
  } else {
    ObSSLogKVResultRow *result_row_ptr =
        static_cast<ObSSLogKVResultRow *>(allocator->alloc(sizeof(ObSSLogKVResultRow)));
    if (OB_ISNULL(result_row_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "alloc the memory of a result_row failed", K(ret), KP(result_row_ptr));
    } else {
      new (result_row_ptr) ObSSLogKVResultRow();
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(phy_version_array.push_back(nullptr))) {
      STORAGE_LOG(WARN, "reserve phy_version_array failed", K(ret), K(result_array.count()));
    } else if (OB_FAIL(result_array.push_back(result_row_ptr))) {
      STORAGE_LOG(WARN, "reserve result_array failed", K(ret), K(result_array.count()));
    } else {
      result_row_index = result_array.count() - 1;
      if (OB_FAIL(result_array[result_row_index]->init(phy_key.user_key_, allocator))) {
        STORAGE_LOG(WARN, "init result row failed", K(ret), K(phy_key), K(result_row_index),
                    K(result_array.count()));
      }
    }

    if (OB_FAIL(ret)) {
      int tmp_ret = OB_SUCCESS;
      if (result_row_index >= 0) {
        if (OB_TMP_FAIL(result_array.remove(result_row_index))) {
          STORAGE_LOG(ERROR, "remove the result_row failed", K(ret), K(tmp_ret),
                      K(result_row_index), K(result_array.count()));
        }
      }

      if (OB_NOT_NULL(result_row_ptr)) {
        result_row_ptr->~ObSSLogKVResultRow();
        allocator->free(result_row_ptr);
      }
    }
  }

  STORAGE_LOG(DEBUG, "find result_row", K(ret), K(phy_key), K(user_key),
              K(max_resut_row_index), K(result_row_index), K(result_array.count()),
              K(phy_version_array.count()));

  return ret;
}

int ObSSLogTableLogicalRow::link_next_version_(ObPhyVersionArray &phy_version_array,
                                               const int64_t result_row_index,
                                               ObPhyVersionNode *node)
{
  int ret = OB_SUCCESS;

  if (result_row_index < 0 || result_row_index >= phy_version_array.count() || OB_ISNULL(node)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(result_row_index), K(phy_version_array.count()),
                KPC(node));
  } else if (phy_version_array[result_row_index] != nullptr
             && !phy_version_array[result_row_index]->phy_key_.user_key_.equal_to(
                 node->phy_key_.user_key_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "a invalid node with a different user_key", K(ret), K(result_row_index),
                K(phy_version_array[result_row_index]), KPC(node));

  } else if (phy_version_array[result_row_index] == nullptr
             || phy_version_array[result_row_index]->phy_val_.inner_val_.tx_version_
                    > node->phy_val_.inner_val_.tx_version_) {
    node->next_ = phy_version_array[result_row_index];
    phy_version_array[result_row_index] = node;
  } else {
    ObPhyVersionNode *prev, *cur;
    prev = cur = nullptr;
    prev = phy_version_array[result_row_index];
    cur = prev->next_;
    while (OB_NOT_NULL(cur) && OB_SUCC(ret)) {
      if (node->phy_val_.inner_val_.tx_version_ < cur->phy_val_.inner_val_.tx_version_) {
        break;
      } else {
        prev = cur;
        cur = cur->next_;
      }
    }

    prev->add_next_node(node);
  }

  return ret;
}

int ObSSLogTableLogicalRow::filter_result_val_(const ObPhyVersionArray &phy_version_array,
                                               ObSSLogKVResultRowArray &result_array,
                                               const share::SCN &start_snapshot,
                                               const share::SCN &end_snapshot,
                                               const bool is_prefix_read,
                                               const bool include_oldest_version)
{
  int ret = OB_SUCCESS;

  if (phy_version_array.count() != result_array.count() || !start_snapshot.is_valid()
      || !end_snapshot.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(phy_version_array.count()),
                K(result_array.count()), K(start_snapshot), K(end_snapshot));
  } else {
    for (int64_t row_index = 0; row_index < result_array.count() && OB_SUCC(ret); row_index++) {
      ObPhyVersionNode *cur, *prev, *max_before_start;
      prev = nullptr;
      cur = phy_version_array[row_index];

      while (OB_NOT_NULL(cur) && OB_SUCC(ret)) {
        if (OB_NOT_NULL(prev) && !(prev->phy_val_.next_key_ == cur->phy_key_.inner_key_)) {
          STORAGE_LOG(INFO, "Skip a version that is not on the version list", K(ret), KPC(prev),
                      KPC(cur));
          cur = cur->next_;
        } else if (cur->phy_val_.inner_val_.tx_version_ >= start_snapshot
                   && cur->phy_val_.inner_val_.tx_version_ <= end_snapshot) {
          if (result_array[row_index]->get_version_count() == 0 && OB_NOT_NULL(prev)
              && include_oldest_version) {
            if (OB_FAIL(result_array[row_index]->copy_and_insert(prev->phy_key_, prev->phy_val_))) {
              STORAGE_LOG(WARN, "copy and insert a oldest version failed", K(ret), KPC(prev),
                          K(row_index));
            }
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_FAIL(
                         result_array[row_index]->copy_and_insert(cur->phy_key_, cur->phy_val_))) {
            STORAGE_LOG(WARN, "copy and insert a version failed", K(ret), KPC(cur), K(row_index));
          } else {
            prev = cur;
            cur = cur->next_;
          }
        } else if (cur->phy_val_.inner_val_.tx_version_ > end_snapshot) {
          cur = nullptr;
          break;
        } else if (cur->phy_val_.inner_val_.tx_version_ < start_snapshot) {
            prev = cur;
            cur = cur->next_;
        }
        // STORAGE_LOG(DEBUG, "iter val", K(ret),K(row_index),KPC(prev),KPC(cur), K(start_snapshot),K(end_snapshot));
      }

      if (result_array[row_index]->get_version_count() == 0 && start_snapshot == end_snapshot
          && OB_NOT_NULL(prev) && prev->phy_val_.inner_val_.tx_version_ <= end_snapshot) {
        if (OB_FAIL(result_array[row_index]->copy_and_insert(prev->phy_key_, prev->phy_val_))) {
          STORAGE_LOG(WARN, "copy and insert a lastest version failed", K(ret), KPC(prev),
                      K(row_index), K(start_snapshot), K(end_snapshot));
        } else {
          STORAGE_LOG(WARN, "copy and insert a lastest version", K(ret), KPC(prev), K(row_index),
                      K(start_snapshot), K(end_snapshot));
        }

      }
    }
  }

  return ret;
}

int ObSSLogKVResultSet::init(ObPalfKVAdpaterInterface *interface,
                             const ObSSLogMetaType &type,
                             const ObString &meta_key,
                             const share::SCN start_snapshot,
                             const share::SCN end_snapshot,
                             ObIAllocator *allocator,
                             bool read_prefix,
                             bool include_oldest_version)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(interface) || (!start_snapshot_.is_valid() && !end_snapshot.is_valid())
      || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(interface), K(start_snapshot),
                K(end_snapshot), KP(allocator), K(read_prefix), K(type), K(meta_key));
  } else if (logic_row_.is_inited()) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init the result set failed", K(ret), K(logic_row_), K(type), K(meta_key));
  } else if (OB_FAIL(logic_row_.init_for_read(interface, type, meta_key))) {
    STORAGE_LOG(WARN, "init the logic row failed", K(ret), KP(interface), K(type), K(meta_key));
  } else {
    start_snapshot_ = start_snapshot;
    end_snapshot_ = end_snapshot;
    result_rows_.reset();
    is_prefix_read_ = read_prefix;
    include_oldest_version_before_start_ = include_oldest_version;
    reset_index();
    allocator_ = allocator;

    if (OB_FAIL(logic_row_.read_row(start_snapshot_, end_snapshot_, result_rows_, allocator_,
                                    is_prefix_read_, include_oldest_version_before_start_))) {
      STORAGE_LOG(WARN, "read row from the palf_kv failed", K(ret), KPC(this));
    } else {
      print_version_list_();
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  }

  return ret;
}

int ObSSLogKVResultSet::get_next(const ObSSLogKVRowUserKey *&user_key,
                                 const ObSSLogKVResultRow::VersionVal *&version_val)
{
  int ret = OB_SUCCESS;

  const bool is_multi_version_read = end_snapshot_ > start_snapshot_;

  if (!is_inited()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "the result set is not inited", K(ret), KPC(this));
  } else if (is_multi_version_read) {
    ret = get_next_version_(user_key, version_val);
  } else {
    ret = get_next_row_latest_version_(user_key, version_val);
  }

  return ret;
}

int ObSSLogKVResultSet::init_for_batch_read(const share::SCN start_snapshot,
                                             const share::SCN end_snapshot,
                                             ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;

  if (!snapshot_range_is_valid(start_snapshot, end_snapshot) || OB_ISNULL(allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), K(start_snapshot), K(end_snapshot), KP(allocator));
  } else {
    reset();
    start_snapshot_ = start_snapshot;
    end_snapshot_ = end_snapshot;
    allocator_ = allocator;
    is_prefix_read_ = false;
    include_oldest_version_before_start_ = false;
    reset_index();
    // Note: logic_row_ is not initialized for batch read
  }

  return ret;
}

int ObSSLogKVResultSet::merge_result_rows(const ObSSLogKVResultRowArray &result_rows)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(allocator_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "result set is not inited", K(ret));
  } else {
    // Deep copy each result row
    for (int i = 0; OB_SUCC(ret) && i < result_rows.count(); i++) {
      const ObSSLogKVResultRow *src_row = result_rows[i];
      if (OB_NOT_NULL(src_row)) {
        // Allocate new result row using the same pattern as find_result_row_
        ObSSLogKVResultRow *new_row = static_cast<ObSSLogKVResultRow *>(
            allocator_->alloc(sizeof(ObSSLogKVResultRow)));
        if (OB_ISNULL(new_row)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "alloc new result row failed", K(ret), K(i));
        } else {
          // Placement new
          new (new_row) ObSSLogKVResultRow();
          // Initialize the new row with the source row's user key
          if (OB_FAIL(new_row->init(*src_row->get_user_key(), allocator_))) {
            STORAGE_LOG(WARN, "init new result row failed", K(ret), K(i));
          } else {
            // Deep copy all version values using the new method
            for (int j = 0; OB_SUCC(ret) && j < src_row->get_version_count(); j++) {
              const ObSSLogKVResultRow::VersionVal *src_version = src_row->at(j);
              if (OB_NOT_NULL(src_version) && OB_FAIL(new_row->deep_copy_version(src_version, allocator_))) {
                STORAGE_LOG(WARN, "deep copy version failed", K(ret), K(i), K(j));
              }
            }
            
            // If version copy succeeded, add the new row to result_rows_
            if (OB_SUCC(ret)) {
              if (OB_FAIL(result_rows_.push_back(new_row))) {
                STORAGE_LOG(WARN, "push back new row failed", K(ret), K(i));
              }
            }
          }
          // Clean up on failure
          if (OB_FAIL(ret)) {
            new_row->~ObSSLogKVResultRow();
            allocator_->free(new_row);
          }
        }
      }
    }
  }

  return ret;
}

int ObSSLogKVResultSet::get_next_version_(const ObSSLogKVRowUserKey *&user_key,
                                          const ObSSLogKVResultRow::VersionVal *&version_val)
{
  int ret = OB_EAGAIN;

  while (OB_EAGAIN == ret) {
    if (row_handle_index_ >= result_rows_.count()) {
      ret = OB_ITER_END;
      STORAGE_LOG(INFO, "the result row index is out of range", K(ret), K(row_handle_index_),
                  K(version_handle_index_));
    } else if ((version_handle_index_ < INT64_MAX
                && (version_handle_index_ >= result_rows_[row_handle_index_]->get_version_count()
                    || version_handle_index_ < 0))
               || (version_handle_index_ == INT64_MAX
                   && result_rows_[row_handle_index_]->get_version_count() == 0)) {
      row_handle_index_++;
      version_handle_index_ = INT64_MAX;
      ret = OB_EAGAIN;
      STORAGE_LOG(INFO, "the version index  is out of range, iter the next row", K(ret),
                  K(row_handle_index_), K(version_handle_index_));
    } else {
      if (version_handle_index_ == INT64_MAX)
        version_handle_index_ = result_rows_[row_handle_index_]->get_version_count() - 1;
      user_key = result_rows_[row_handle_index_]->get_user_key();
      version_val = result_rows_[row_handle_index_]->at(version_handle_index_);
      version_handle_index_--;
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObSSLogKVResultSet::get_next_row_latest_version_(
    const ObSSLogKVRowUserKey *&user_key,
    const ObSSLogKVResultRow::VersionVal *&version_val)
{
  int ret = OB_EAGAIN;

  while (OB_EAGAIN == ret) {
    // STORAGE_LOG(INFO, "retry next", K(ret), K(row_handle_index_), K(version_handle_index_));
    if (row_handle_index_ >= result_rows_.count()) {
      ret = OB_ITER_END;
      STORAGE_LOG(INFO, "the result row index is out of range", K(ret), K(row_handle_index_),
                  K(version_handle_index_));
    } else if ((version_handle_index_ < INT64_MAX
                && (version_handle_index_ >= result_rows_[row_handle_index_]->get_version_count()
                    || version_handle_index_ < 0))
               || (version_handle_index_ == INT64_MAX
                   && result_rows_[row_handle_index_]->get_version_count() == 0)) {
      row_handle_index_++;
      version_handle_index_ = 0;
      ret = OB_EAGAIN;
      STORAGE_LOG(INFO, "the version index  is out of range, iter the next row", K(ret),
                  K(row_handle_index_), K(version_handle_index_));
    } else {
      version_handle_index_ = result_rows_[row_handle_index_]->get_version_count() - 1;
      user_key = result_rows_[row_handle_index_]->get_user_key();
      version_val = result_rows_[row_handle_index_]->at(version_handle_index_);
      row_handle_index_++;
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

void ObSSLogKVResultSet::print_version_list_() const
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(INFO, "Start to print result set", KP(this), KPC(this));
  for (int row_i = 0; row_i < result_rows_.count(); row_i++) {
    STORAGE_LOG(INFO, " Print result row", K(row_i), K(result_rows_[row_i]));
    result_rows_[row_i]->print_version_val();
  }
  STORAGE_LOG(INFO, "End to print result set", KP(this), KPC(this));
}

bool ObSSLogKVResultSet::contain_user_key(const ObSSLogKVRowUserKey &user_key,
                                          const ObSSLogKVResultRowArray &result_rows,
                                          int64_t &index)
{
  bool is_contain = false;
  index = -1;

  for (int i = 0; i < result_rows.count(); i++) {
    if (OB_NOT_NULL(result_rows[i]) && result_rows[i]->is_same_key(user_key)) {
      is_contain = true;
      index = i;
      break;
    }
  }

  return is_contain;
}

} // namespace sslog

} // namespace oceanbase
