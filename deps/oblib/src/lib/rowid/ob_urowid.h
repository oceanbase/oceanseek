/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_UROWID_H_
#define OB_UROWID_H_

#include "lib/alloc/alloc_assist.h"
#include "lib/encode/ob_base64_encode.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_macro_utils.h"
#include "common/object/ob_obj_type.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace common
{
class ObObj;
class ObIAllocator;
template <class T>
class ObIArray;
class ObTabletID;

struct ObURowIDData
{
  OB_UNIS_VERSION(1);
friend class TestURowID;
private:
  typedef int (*get_pk_val_func)(const uint8_t *, const int64_t, int64_t &, ObObj &);
  typedef int (*set_pk_val_func)(const ObObj &, uint8_t *, const int64_t, int64_t &);
  static get_pk_val_func inner_get_funcs_[ObMaxType];
  static set_pk_val_func inner_set_funcs_[ObMaxType];
public:
  enum ObURowIDDataVersion {
    INVALID_ROWID_VERSION = 0,
    PK_ROWID_VERSION = 1,
    NO_PK_ROWID_VERSION = 2, // 已废弃，仅保留历史版本号，禁止编解码
    LOB_NO_PK_ROWID_VERSION = 3, // 已废弃，仅保留历史版本号，禁止编解码
    EXTERNAL_TABLE_ROWID_VERSION = 4,

    HEAP_TABLE_ROWID_VERSION = 128,     // 0x80
    EXT_HEAP_TABLE_ROWID_VERSION = 160, // 0xA0
  };

  int64_t rowid_len_;
  /*
   *  external table logical rowid (EXTERNAL_TABLE_ROWID_VERSION)
   *  +-----------+---------+---------+--------------+
   *  | 1 byte    | dba len | 1 byte  | pk_len bytes |
   *  +-----------+---------+---------+--------------+
   *  | dba len   |  dba    | version |  pk_content  |
   *  +-----------+---------+---------+--------------+
   *
   *  primary key logical rowid (PK_ROWID_VERSION = 1)
   *  +-----------+---------+---------+--------------+-------------------+
   *  | 1 byte    | dba len | 1 byte  | 1 byte       | rowkey buf        |
   *  +-----------+---------+---------+--------------+-------------------+
   *  | dba len   | dba     | version | rowkey count | rowkey content    |
   *  +-----------+---------+---------+--------------+-------------------+
   *  | 2 bytes                         | generated partition column buf |
   *  +---------------------------------+--------------------------------+
   *  | generated partition column count | generated partition content |
   *  +---------------------------------+--------------------------------+
   *  rowkey count取值范围为1～64，生成分区列数不超过表的列数上限OB_MAX_COLUMN_NUMBER, 范围为0～4224。
   *  OB_MAX_COLUMN_NUMBER = OB_USER_ROW_MAX_COLUMNS_COUNT + 2 * OB_USER_MAX_ROWKEY_COLUMN_NUMBER = 4096 + 2 * 64 = 4224;
   *  所有逻辑ROWID中的字符串值采用同一种编码：
   *  [obj type: 1 byte][collation id: 2 bytes][length: 4 bytes][payload]，
   *  其中collation id同时确定字符集和排序规则。
   *
   *  heap-organized table rowid
   *  +--------------+-----------+----------------+
   *  |    3 bit     |   37 bit  |     40 bit     |
   *  +--------------+-----------+----------------+
   *  | version >> 5 | tablet_id | auto_increment |
   *  +--------------+-----------+----------------+
   * 
   *  extended heap-organized table rowid
   *  +--------------+-----------+----------------+
   *  |    3 bit     |   61 bit  |     64 bit     |
   *  +--------------+-----------+----------------+
   *  | version >> 5 | tablet_id | auto_increment |
   *  +--------------+-----------+----------------+
   */
  const uint8_t *rowid_content_;

  ObURowIDData()
    : rowid_len_(0), rowid_content_(NULL) {}

  ObURowIDData(int64_t len, const uint8_t *content)
    : rowid_len_(len), rowid_content_(content) {}

  int compare(const ObURowIDData &other) const;
  bool operator ==(const ObURowIDData &other) const;
  bool operator !=(const ObURowIDData &other) const;
  bool operator >(const ObURowIDData &other) const;
  bool operator <(const ObURowIDData &other) const;
  bool operator <=(const ObURowIDData &other) const;
  bool operator >=(const ObURowIDData &other) const;

  inline bool is_physical_rowid() const
  {
    uint8_t version = get_version();
    return HEAP_TABLE_ROWID_VERSION == version || EXT_HEAP_TABLE_ROWID_VERSION == version;
  }

  int64_t needed_base64_buffer_size() const;
  static int64_t needed_base64_buffer_size(const int64_t content_size,
                                           const bool is_physical_rowid);
  static int64_t needed_max_base64_buffer_size(const int64_t content_size);
  static bool is_variable_length_pk_type(const ObObjType type);
  static int64_t get_max_serialized_pk_value_size(const ObObjType type,
                                                  const int64_t data_length);
  int get_base64_str(char *buf, const int64_t buf_len, int64_t &pos) const;
  static int decode2urowid(const char *input, const int64_t input_len,
                           ObIAllocator &alloc, ObURowIDData &urowid_data);
  static int build_invalid_rowid_obj(ObIAllocator &alloc, ObObj *&rowid_obj);

  static constexpr int64_t HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS = 32;
  static constexpr int64_t EXT_HEAP_TABLE_ROWID_NON_EMBEDED_TABLET_ID_BITS = 56;

  static constexpr int64_t HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE = 10;
  static constexpr int64_t EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE = 16;
  static constexpr int64_t LOGICAL_ROWID_HEADER_SIZE = 2;
  // dba长度、版本和真实主键列数各占一个字节，生成分区列数占两个字节。
  static constexpr int64_t PK_LOGICAL_ROWID_FIXED_SIZE = 3 + sizeof(int16_t);
  static_assert(INT16_MAX >= OB_MAX_COLUMN_NUMBER,
                "generated partition column count exceeds rowid format capacity");

  static constexpr int64_t HEAP_TABLE_ROWID_STR_BUF_LEN = ObBase64Encoder::needed_encoded_length(
      ObURowIDData::HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE);
  static constexpr int64_t EXT_HEAP_TABLE_ROWID_STR_BUF_LEN = ObBase64Encoder::needed_encoded_length(
      ObURowIDData::EXT_HEAP_ORGANIZED_TABLE_ROWID_CONTENT_BUF_SIZE);

  // 主键逻辑ROWID还可能包含生成分区列，调用方必须显式传入真实主键列数。
  int set_rowid_content(const ObIArray<ObObj> &pk_vals,
                        const int64_t version,
                        const int64_t real_pk_count,
                        ObIAllocator &allocator,
                        const int64_t dba_len = 0,
                        const uint8_t* guess_dba = NULL);

  // ROWID还可能编码生成分区列；返回真正属于主键的列数。
  int64_t get_real_pk_count(const ObIArray<ObObj> &all_pk_vals) const
  {
    int64_t cnt = 0;
    uint8_t version = get_version();
    if (INVALID_ROWID_VERSION == version) {
    } else if (PK_ROWID_VERSION == version) {
      const int64_t off = get_real_pk_count_offset();
      if (off < rowid_len_ && is_valid_pk_count(rowid_content_[off])
          && rowid_content_[off] <= all_pk_vals.count()) {
        cnt = rowid_content_[off];
      }
    } else {
      cnt = all_pk_vals.count();
    }
    return cnt;
  }
  int get_pk_vals(ObIArray<ObObj> &pk_vals) const;

  int get_tablet_id_for_heap_organized_table(common::ObTabletID &tablet_id) const;
  int get_rowkey_for_heap_organized_table(ObIArray<ObObj> &rowkey) const;

  static bool is_valid_pk_count(const int64_t real_pk_count)
  {
    return real_pk_count > 0 && real_pk_count <= OB_USER_MAX_ROWKEY_COLUMN_NUMBER;
  }

  static bool is_valid_generated_partition_column_count(const int64_t generated_column_count)
  {
    return generated_column_count >= 0
        && generated_column_count <= OB_MAX_COLUMN_NUMBER;
  }

  static int64_t needed_content_buf_size(const common::ObIArray<ObObj> &pk_vals, const int64_t version);

  uint8_t get_version() const;

  DECLARE_TO_STRING;
private:
  class ObPkValueIterator
  {
  public:
    explicit ObPkValueIterator(const ObURowIDData &rowid);
    int init();
    int get_next_value(ObObj &value);
    int64_t get_rowkey_count() const { return rowkey_count_; }

  private:
    int decode_value(ObObj &value);

  private:
    const ObURowIDData &rowid_; // 当前待解析的主键逻辑ROWID
    int64_t pos_; // 当前解析位置
    int64_t rowkey_count_; // ROWID中声明的真实主键列数
    int64_t rowkey_idx_; // 下一个待解析的真实主键列下标
    int64_t generated_column_count_; // ROWID中声明的生成分区列数，未读取时为-1
    int64_t generated_column_idx_; // 下一个待解析的生成分区列下标
  };

  inline uint8_t get_guess_dba_len() const
  {
    // 1 byte for guess dba len
    OB_ASSERT(rowid_len_ >= 1);
    return rowid_content_[0];
  }

  inline int64_t get_pk_version_offset() const
  {
    return get_guess_dba_len() + 1;
  }

  inline int64_t get_pk_content_offset() const
  {
    const int64_t version_offset = get_pk_version_offset();
    return version_offset + (version_offset < rowid_len_
        && PK_ROWID_VERSION == rowid_content_[version_offset] ? 2 : 1);
  }

  inline int64_t get_real_pk_count_offset() const
  {
    return get_pk_version_offset() + 1;
  }

  inline int64_t get_buf_len() const { return rowid_len_; }
  inline ObObjType get_pk_type(int64_t &pos) const
  {
    OB_ASSERT(pos + 1 <= get_buf_len());
    return static_cast<ObObjType>(rowid_content_[pos++]);
  }


  static bool is_valid_version(int64_t v);
  static bool is_string_pk_type(const ObObjType type);
  int check_is_valid_urowid() const;
  
  static int set_heap_organized_table_rowid_content(const ObIArray<ObObj> &args,
                                                    uint8_t *buf,
                                                    const int64_t buf_len,
                                                    int64_t &pos);
  static int set_ext_heap_organized_table_rowid_content(const ObIArray<ObObj> &args,
                                                        uint8_t *buf,
                                                        const int64_t buf_len,
                                                        int64_t &pos);
  int parse_heap_organized_table_rowid(uint64_t &tablet_id, uint64_t &auto_inc) const;
  int parse_ext_heap_organized_table_rowid(uint64_t &tablet_id, uint64_t &auto_inc) const;
  int parse_physical_rowid(uint64_t &tablet_id, uint64_t &auto_inc) const;

  int get_pk_value(ObObjType obj_type, int64_t &offset, ObObj &pk_val) const;

  template<ObObjType type>
  static int inner_get_pk_value(const uint8_t *rowid_buf, const int64_t rowid_buf_len,
                                int64_t &offset, ObObj &pk_val)
  {
    UNUSED(rowid_buf);
    UNUSED(rowid_buf_len);
    UNUSED(offset);
    UNUSED(pk_val);
    return OB_NOT_SUPPORTED;
  }

  template<ObObjType type>
  static int inner_set_pk_value(const ObObj &pk_val, uint8_t *buffer, const int64_t buf_len,
                                int64_t &pos)
  {
    UNUSED(pk_val);
    UNUSED(buffer);
    UNUSED(buf_len);
    UNUSED(pos);
    return OB_NOT_SUPPORTED;
  }

  static int get_string_pk_value(const ObObjType obj_type,
                                 const uint8_t *rowid_buf,
                                 const int64_t rowid_buf_len,
                                 int64_t &pos,
                                 ObObj &pk_val);
  static int set_string_pk_value(const ObObj &pk_val,
                                 uint8_t *buffer,
                                 const int64_t buf_len,
                                 int64_t &pos);
  static int64_t get_obj_size(const ObObj &pk_val);

  inline void reset()
  {
    rowid_len_ = 0;
    rowid_content_ = NULL;
  }
};
} // end namespace common
} // end namespace oceanbase

#endif // !OB_UROWID_H_
