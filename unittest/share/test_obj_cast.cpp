/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include "share/object/ob_obj_cast.h"
#include "lib/charset/ob_charset.h"
#include "lib/container/ob_se_array.h"
#include "lib/rowid/ob_urowid.h"
#include <gtest/gtest.h>


namespace oceanbase
{
namespace common
{
using namespace number;

class TestObjCast : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

  ObArenaAllocator allocator_;
};

void TestObjCast::SetUp()
{
  const lib::ObMemAttr attr(common::OB_SYS_TENANT_ID, ObModIds::OB_NUMBER);
  int ret = ObNumberConstValue::init(allocator_, attr);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestObjCast, test_number_range_check_mysql_old)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::MYSQL);
  ObNumber zero_number;
  zero_number.set_zero();
  ObObj obj1;
  obj1.set_number(zero_number);
  const ObObj *res_obj = &obj1;
  ObObjCastParams params;
  params.allocator_v2_ = &allocator_;
  params.cast_mode_ |= CM_WARN_ON_FAIL;
  int64_t get_range_beg = ObTimeUtility::current_time();
  for (int16_t precision = OB_MIN_DECIMAL_PRECISION; OB_SUCC(ret) && precision <= ObNumber::MAX_PRECISION; ++precision) {
    for (int16_t scale = 0; OB_SUCC(ret) && precision >= scale && scale <= ObNumber::MAX_SCALE; ++scale) {
      ObAccuracy accuracy(precision, scale);
      ret = number_range_check(params, accuracy, obj1, obj1, res_obj, params.cast_mode_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  int64_t get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "test_number_range_check_mysql_old(%d) cost time: %f", (ObNumber::MAX_PRECISION + 1) * (ObNumber::MAX_SCALE + 1) / 2, (double)get_range_cost / (double)1000);
}

TEST_F(TestObjCast, test_number_range_check_mysql_new)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::MYSQL);
  ObNumber zero_number;
  zero_number.set_zero();
  ObObj obj1;
  obj1.set_number(zero_number);
  const ObObj *res_obj = &obj1;
  ObObjCastParams params;
  params.allocator_v2_ = &allocator_;
  params.cast_mode_ |= CM_WARN_ON_FAIL;
  int64_t get_range_beg = ObTimeUtility::current_time();
  for (int16_t precision = OB_MIN_DECIMAL_PRECISION; OB_SUCC(ret) && precision <= ObNumber::MAX_PRECISION; ++precision) {
    for (int16_t scale = 0; OB_SUCC(ret) && precision >= scale && scale <= ObNumber::MAX_SCALE; ++scale) {
      ObAccuracy accuracy(precision, scale);
      ret = number_range_check_v2(params, accuracy, obj1, obj1, res_obj, params.cast_mode_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  int64_t get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "test_number_range_check_mysql_new(%d) cost time: %f", (ObNumber::MAX_PRECISION + 1) * (ObNumber::MAX_SCALE + 1) / 2, (double)get_range_cost / (double)1000);
}

TEST_F(TestObjCast, test_number_range_check_oracle_old)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::ORACLE);
  ObNumber zero_number;
  zero_number.set_zero();
  ObObj obj1;
  obj1.set_number(zero_number);
  const ObObj *res_obj = &obj1;
  ObObjCastParams params;
  params.allocator_v2_ = &allocator_;
  params.cast_mode_ |= CM_WARN_ON_FAIL;
  int64_t get_range_beg = ObTimeUtility::current_time();
  for (int16_t precision = OB_MIN_NUMBER_PRECISION; OB_SUCC(ret) && precision <= OB_MAX_NUMBER_PRECISION; ++precision) {
    for (int16_t scale = ObNumber::MIN_SCALE; OB_SUCC(ret) && scale <= ObNumber::MAX_SCALE; ++scale) {
      ObAccuracy accuracy(precision, scale);
      ret = number_range_check_for_oracle(params, accuracy, obj1, obj1, res_obj, params.cast_mode_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  int64_t get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "test_number_range_check_oracle_old(%ld) cost time: %f", (OB_MAX_NUMBER_PRECISION) * (ObNumberConstValue::MAX_ORACLE_SCALE_SIZE + 1), (double)get_range_cost / (double)1000);
}

TEST_F(TestObjCast, test_number_range_check_oracle_new)
{
  int ret = OB_SUCCESS;
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::ORACLE);
  ObNumber zero_number;
  zero_number.set_zero();
  ObObj obj1;
  obj1.set_number(zero_number);
  const ObObj *res_obj = &obj1;
  ObObjCastParams params;
  params.allocator_v2_ = &allocator_;
  params.cast_mode_ |= CM_WARN_ON_FAIL;
  int64_t get_range_beg = ObTimeUtility::current_time();
  for (int16_t precision = OB_MIN_NUMBER_PRECISION; OB_SUCC(ret) && precision <= OB_MAX_NUMBER_PRECISION; ++precision) {
    for (int16_t scale = ObNumber::MIN_SCALE; OB_SUCC(ret) && scale <= ObNumber::MAX_SCALE; ++scale) {
      ObAccuracy accuracy(precision, scale);
      ret = number_range_check_v2(params, accuracy, obj1, obj1, res_obj, params.cast_mode_);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }
  int64_t get_range_cost = ObTimeUtility::current_time() - get_range_beg;
  _OB_LOG(INFO, "test_number_range_check_oracle_new(%ld) cost time: %f", (OB_MAX_NUMBER_PRECISION) * (ObNumberConstValue::MAX_ORACLE_SCALE_SIZE + 1), (double)get_range_cost / (double)1000);
}

TEST_F(TestObjCast, test_mysql_text_to_rowid_with_full_data)
{
  lib::CompatModeGuard tmp_mode(lib::Worker::CompatMode::MYSQL);
  const int64_t pk_length = 2048;
  char *pk_buf = static_cast<char *>(allocator_.alloc(pk_length));
  ASSERT_NE(nullptr, pk_buf);
  MEMSET(pk_buf, 'a', pk_length);

  ObObj pk_obj;
  pk_obj.set_varchar(ObString(pk_length, pk_buf));
  pk_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ObSEArray<ObObj, 1> pk_objs;
  ASSERT_EQ(OB_SUCCESS, pk_objs.push_back(pk_obj));

  ObURowIDData expected_rowid;
  ASSERT_EQ(OB_SUCCESS, expected_rowid.set_rowid_content(
      pk_objs, ObURowIDData::PK_ROWID_VERSION, pk_objs.count(), allocator_));
  const int64_t base64_buf_size = expected_rowid.needed_base64_buffer_size();
  char *base64_buf = static_cast<char *>(allocator_.alloc(base64_buf_size));
  ASSERT_NE(nullptr, base64_buf);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, expected_rowid.get_base64_str(base64_buf, base64_buf_size, pos));
  ASSERT_GT(pos, 1000);

  const int64_t lob_size = sizeof(ObLobCommon) + pos;
  char *lob_buf = static_cast<char *>(allocator_.alloc(lob_size));
  ASSERT_NE(nullptr, lob_buf);
  new(lob_buf) ObLobCommon();
  MEMCPY(lob_buf + sizeof(ObLobCommon), base64_buf, pos);
  ObObj text_obj;
  text_obj.set_string(ObLongTextType, ObString(lob_size, lob_buf));
  text_obj.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);

  ObCastCtx cast_ctx(&allocator_, NULL, 0, CM_NONE, CS_TYPE_INVALID, NULL);
  ObObj rowid_obj;
  ASSERT_EQ(OB_SUCCESS, ObObjCaster::to_type(ObURowIDType, cast_ctx, text_obj, rowid_obj));
  ASSERT_EQ(ObURowIDType, rowid_obj.get_type());
  ASSERT_TRUE(expected_rowid == rowid_obj.get_urowid());
}

TEST_F(TestObjCast, test_pk_rowid_version_and_real_pk_count)
{
  ObObj value;
  ObSEArray<ObObj, 2> values;
  value.set_int(1);
  ASSERT_EQ(OB_SUCCESS, values.push_back(value));
  value.set_int(2);
  ASSERT_EQ(OB_SUCCESS, values.push_back(value));

  ObURowIDData one_real_pk;
  ASSERT_EQ(OB_SUCCESS, one_real_pk.set_rowid_content(
      values, ObURowIDData::PK_ROWID_VERSION, 1, allocator_));
  ASSERT_EQ(ObURowIDData::PK_ROWID_VERSION, one_real_pk.get_version());
  ASSERT_EQ(ObURowIDData::needed_content_buf_size(
      values, ObURowIDData::PK_ROWID_VERSION), one_real_pk.rowid_len_);
  ASSERT_EQ(0, one_real_pk.rowid_content_[0]);
  ASSERT_EQ(ObURowIDData::PK_ROWID_VERSION, one_real_pk.rowid_content_[1]);
  ASSERT_EQ(1, one_real_pk.rowid_content_[2]);
  const int64_t generated_count_offset = 3 + sizeof(uint8_t) + sizeof(int64_t);
  ASSERT_LT(generated_count_offset + 1, one_real_pk.rowid_len_);
  ASSERT_EQ(0, one_real_pk.rowid_content_[generated_count_offset]);
  ASSERT_EQ(1, one_real_pk.rowid_content_[generated_count_offset + 1]);

  ObSEArray<ObObj, 2> decoded_values;
  ASSERT_EQ(OB_SUCCESS, one_real_pk.get_pk_vals(decoded_values));
  ASSERT_EQ(2, decoded_values.count());
  ASSERT_EQ(1, one_real_pk.get_real_pk_count(decoded_values));

  ObURowIDData two_real_pks;
  ASSERT_EQ(OB_SUCCESS, two_real_pks.set_rowid_content(
      values, ObURowIDData::PK_ROWID_VERSION, 2, allocator_));
  ASSERT_EQ(0, two_real_pks.rowid_content_[two_real_pks.rowid_len_ - 2]);
  ASSERT_EQ(0, two_real_pks.rowid_content_[two_real_pks.rowid_len_ - 1]);
  ASSERT_EQ(-1, one_real_pk.compare(two_real_pks));
  ASSERT_EQ(1, two_real_pks.compare(one_real_pk));

  ObSEArray<ObObj, 2> larger_generated_values;
  value.set_int(1);
  ASSERT_EQ(OB_SUCCESS, larger_generated_values.push_back(value));
  value.set_int(100);
  ASSERT_EQ(OB_SUCCESS, larger_generated_values.push_back(value));
  ObURowIDData larger_generated_value;
  ASSERT_EQ(OB_SUCCESS, larger_generated_value.set_rowid_content(
      larger_generated_values, ObURowIDData::PK_ROWID_VERSION, 1, allocator_));
  // 比较仍按[主键值, 生成分区列值]的逻辑顺序进行，不能先比较两个分段的长度。
  ASSERT_EQ(1, larger_generated_value.compare(two_real_pks));
  ASSERT_EQ(-1, two_real_pks.compare(larger_generated_value));

  ASSERT_TRUE(ObURowIDData::is_valid_pk_count(1));
  ASSERT_FALSE(ObURowIDData::is_valid_pk_count(0));

  ObURowIDData invalid_count;
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_count.set_rowid_content(
      values, ObURowIDData::PK_ROWID_VERSION, 0, allocator_));
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_count.set_rowid_content(
      values, ObURowIDData::PK_ROWID_VERSION, 3, allocator_));

  ObSEArray<ObObj, 1> generated_values_256;
  for (int64_t i = 0; i < 257; ++i) {
    value.set_int(i);
    ASSERT_EQ(OB_SUCCESS, generated_values_256.push_back(value));
  }
  ObURowIDData generated_count_256;
  ASSERT_EQ(OB_SUCCESS, generated_count_256.set_rowid_content(
      generated_values_256, ObURowIDData::PK_ROWID_VERSION, 1, allocator_));
  ASSERT_EQ(1, generated_count_256.rowid_content_[generated_count_offset]);
  ASSERT_EQ(0, generated_count_256.rowid_content_[generated_count_offset + 1]);
  decoded_values.reset();
  ASSERT_EQ(OB_SUCCESS, generated_count_256.get_pk_vals(decoded_values));
  ASSERT_EQ(257, decoded_values.count());

  ObSEArray<ObObj, 1> too_many_generated_values;
  for (int64_t i = 0;
       i < OB_MAX_COLUMN_NUMBER + 2;
       ++i) {
    value.set_int(i);
    ASSERT_EQ(OB_SUCCESS, too_many_generated_values.push_back(value));
  }
  ASSERT_EQ(OB_INVALID_ARGUMENT, invalid_count.set_rowid_content(
      too_many_generated_values, ObURowIDData::PK_ROWID_VERSION, 1, allocator_));

  uint8_t invalid_content[64] = {0};
  ASSERT_LE(one_real_pk.rowid_len_, static_cast<int64_t>(sizeof(invalid_content)));
  MEMCPY(invalid_content, one_real_pk.rowid_content_, one_real_pk.rowid_len_);
  invalid_content[generated_count_offset] = 0;
  invalid_content[generated_count_offset + 1] = 2;
  ObURowIDData excessive_generated_count(one_real_pk.rowid_len_, invalid_content);
  decoded_values.reset();
  ASSERT_EQ(OB_INVALID_ROWID, excessive_generated_count.get_pk_vals(decoded_values));

  invalid_content[generated_count_offset] = 0;
  invalid_content[generated_count_offset + 1] = 0;
  ObURowIDData unexpected_generated_value(one_real_pk.rowid_len_, invalid_content);
  decoded_values.reset();
  ASSERT_EQ(OB_INVALID_ROWID, unexpected_generated_value.get_pk_vals(decoded_values));

  ObURowIDData missing_generated_count(two_real_pks.rowid_len_ - 1,
                                       two_real_pks.rowid_content_);
  decoded_values.reset();
  ASSERT_EQ(OB_INVALID_ROWID, missing_generated_count.get_pk_vals(decoded_values));

  ASSERT_LE(two_real_pks.rowid_len_, static_cast<int64_t>(sizeof(invalid_content)));
  MEMCPY(invalid_content, two_real_pks.rowid_content_, two_real_pks.rowid_len_);
  const int64_t invalid_generated_count_value = OB_MAX_COLUMN_NUMBER + 1;
  invalid_content[two_real_pks.rowid_len_ - 2] =
      static_cast<uint8_t>((invalid_generated_count_value >> 8) & 0xff);
  invalid_content[two_real_pks.rowid_len_ - 1] =
      static_cast<uint8_t>(invalid_generated_count_value & 0xff);
  ObURowIDData invalid_generated_count(two_real_pks.rowid_len_, invalid_content);
  decoded_values.reset();
  ASSERT_EQ(OB_INVALID_ROWID, invalid_generated_count.get_pk_vals(decoded_values));

  invalid_content[two_real_pks.rowid_len_ - 2] = 0xff;
  invalid_content[two_real_pks.rowid_len_ - 1] = 0xff;
  ObURowIDData negative_generated_count(two_real_pks.rowid_len_, invalid_content);
  decoded_values.reset();
  ASSERT_EQ(OB_INVALID_ROWID, negative_generated_count.get_pk_vals(decoded_values));
}

TEST_F(TestObjCast, test_reject_legacy_pk_rowid_string_layout)
{
  // 旧格式没有真实主键列数字节，且字符串 collation 仅占 1 字节。
  static const char legacy_rowid[] = "*AAEWLQEAAABh";
  ObURowIDData rowid;
  ASSERT_EQ(OB_INVALID_ROWID,
            ObURowIDData::decode2urowid(
                legacy_rowid, sizeof(legacy_rowid) - 1, allocator_, rowid));
  ASSERT_EQ(0, rowid.rowid_len_);
  ASSERT_EQ(nullptr, rowid.rowid_content_);
}

TEST_F(TestObjCast, test_reject_deprecated_logical_rowid_versions)
{
  ObObj value;
  value.set_int(1);
  ObSEArray<ObObj, 1> values;
  ASSERT_EQ(OB_SUCCESS, values.push_back(value));

  const int64_t deprecated_versions[] = {
    ObURowIDData::NO_PK_ROWID_VERSION,
    ObURowIDData::LOB_NO_PK_ROWID_VERSION
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(deprecated_versions); ++i) {
    ObURowIDData rowid;
    ASSERT_EQ(OB_ERR_UNEXPECTED, rowid.set_rowid_content(
        values, deprecated_versions[i], values.count(), allocator_));
  }

  static const char deprecated_no_pk_rowid[] = "*AAI=";
  static const char deprecated_lob_rowid[] = "*AAM=";
  ObURowIDData decoded_rowid;
  ASSERT_EQ(OB_INVALID_ROWID, ObURowIDData::decode2urowid(
      deprecated_no_pk_rowid, sizeof(deprecated_no_pk_rowid) - 1, allocator_, decoded_rowid));
  ASSERT_EQ(OB_INVALID_ROWID, ObURowIDData::decode2urowid(
      deprecated_lob_rowid, sizeof(deprecated_lob_rowid) - 1, allocator_, decoded_rowid));
}

TEST_F(TestObjCast, test_logical_rowid_uses_same_string_format)
{
  const ObCollationType coll_type = CS_TYPE_UTF8MB4_ZH_0900_AS_CS;
  ObObj value;
  ObSEArray<ObObj, 3> values;
  value.set_varchar("varchar");
  value.set_collation_type(coll_type);
  ASSERT_EQ(OB_SUCCESS, values.push_back(value));
  value.set_raw("raw", 3);
  ASSERT_EQ(OB_SUCCESS, values.push_back(value));
  value.set_lob_value(ObLongTextType, "longtext", 8);
  value.set_collation_type(coll_type);
  ASSERT_EQ(OB_SUCCESS, values.push_back(value));

  const int64_t versions[] = {
    ObURowIDData::PK_ROWID_VERSION,
    ObURowIDData::EXTERNAL_TABLE_ROWID_VERSION
  };
  for (int64_t i = 0; i < ARRAYSIZEOF(versions); ++i) {
    ObURowIDData rowid;
    ASSERT_EQ(OB_SUCCESS, rowid.set_rowid_content(
        values, versions[i], values.count(), allocator_));

    const int64_t first_value_offset = ObURowIDData::PK_ROWID_VERSION == versions[i] ? 3 : 2;
    uint16_t encoded_coll_type = CS_TYPE_INVALID;
    ASSERT_LT(first_value_offset + static_cast<int64_t>(sizeof(encoded_coll_type)),
              rowid.rowid_len_);
    ASSERT_EQ(ObVarcharType, rowid.rowid_content_[first_value_offset]);
    MEMCPY(&encoded_coll_type,
           rowid.rowid_content_ + first_value_offset + sizeof(uint8_t),
           sizeof(encoded_coll_type));
    ASSERT_EQ(coll_type, static_cast<ObCollationType>(encoded_coll_type));

    ObSEArray<ObObj, 3> decoded_values;
    ASSERT_EQ(OB_SUCCESS, rowid.get_pk_vals(decoded_values));
    // 外表 ROWID 的首个字段是分区 ID，get_pk_vals() 按既有语义不返回该字段。
    const int64_t value_start_idx = ObURowIDData::EXTERNAL_TABLE_ROWID_VERSION == versions[i]
        ? 1 : 0;
    ASSERT_EQ(values.count() - value_start_idx, decoded_values.count());
    for (int64_t j = 0; j < decoded_values.count(); ++j) {
      ASSERT_EQ(values.at(j + value_start_idx).get_type(), decoded_values.at(j).get_type());
      ASSERT_EQ(values.at(j + value_start_idx).get_collation_type(),
                decoded_values.at(j).get_collation_type());
      ASSERT_EQ(0, values.at(j + value_start_idx).get_string().compare(
          decoded_values.at(j).get_string()));
    }
  }
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  if (oceanbase::common::OB_SUCCESS != oceanbase::common::ObCharset::init_charset()) {
    return 1;
  }
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
