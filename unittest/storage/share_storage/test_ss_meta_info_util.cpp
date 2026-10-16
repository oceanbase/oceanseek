/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#define private public
#define protected public

#include "lib/ob_errno.h"
#include "storage/incremental/ssmeta/ob_ss_meta_info_util.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/shared_storage/ob_file_helper.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

class TestSSMetaInfoUtil : public ::testing::Test
{
public:
  TestSSMetaInfoUtil() = default;
  ~TestSSMetaInfoUtil() = default;
};

/*-----------------------------------------build_ls_ckpt_key-----------------------------------------*/

// 覆盖模块：ObSSMetaInfoUtil::build_ls_ckpt_key 的正常构造和边界容量路径。
// 测试流程：输入合法 LS ID，验证生成的 palf-kv key 内容及返回长度；
//           再使用恰好能容纳完整 key 和结尾 '\0' 的缓冲区验证边界成功。
TEST_F(TestSSMetaInfoUtil, build_ls_ckpt_key)
{
  char buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  int64_t key_len = -1;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::build_ls_ckpt_key(
      ObLSID(1001), buf, sizeof(buf), key_len));
  ASSERT_STREQ("LS_CKPT_1001", buf);
  ASSERT_EQ(static_cast<int64_t>(sizeof("LS_CKPT_1001") - 1), key_len);

  char exact_buf[sizeof("LS_CKPT_1")] = {0};
  key_len = -1;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::build_ls_ckpt_key(
      ObLSID(1), exact_buf, sizeof(exact_buf), key_len));
  ASSERT_STREQ("LS_CKPT_1", exact_buf);
  ASSERT_EQ(static_cast<int64_t>(sizeof("LS_CKPT_1") - 1), key_len);
}

// 覆盖模块：ObSSMetaInfoUtil::build_ls_ckpt_key 的入参校验。
// 测试流程：依次传入无效 LS ID、空缓冲区和零长度缓冲区；
//           验证均返回 OB_INVALID_ARGUMENT，并将 key_len 重置为 0。
TEST_F(TestSSMetaInfoUtil, build_ls_ckpt_key_invalid_argument)
{
  char buf[SS_LS_CKPT_KEY_BUF_LEN] = {0};
  int64_t key_len = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::build_ls_ckpt_key(
      ObLSID(), buf, sizeof(buf), key_len));
  ASSERT_EQ(0, key_len);

  key_len = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::build_ls_ckpt_key(
      ObLSID(1001), nullptr, sizeof(buf), key_len));
  ASSERT_EQ(0, key_len);

  key_len = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::build_ls_ckpt_key(
      ObLSID(1001), buf, 0, key_len));
  ASSERT_EQ(0, key_len);
}

// 覆盖模块：ObSSMetaInfoUtil::build_ls_ckpt_key 的缓冲区容量检查。
// 测试流程：缓冲区比完整 key 和结尾 '\0' 所需容量少一个字节，
//           验证返回 OB_SIZE_OVERFLOW。
TEST_F(TestSSMetaInfoUtil, build_ls_ckpt_key_small_buffer)
{
  char small_buf[sizeof("LS_CKPT_1") - 1] = {0};
  int64_t key_len = -1;
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObSSMetaInfoUtil::build_ls_ckpt_key(
      ObLSID(1), small_buf, sizeof(small_buf), key_len));
}

/*-----------------------------------------find_marker-----------------------------------------*/

TEST_F(TestSSMetaInfoUtil, find_marker_basic)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/scn00000000000000000100.T20");
  const char *pos = ObSSMetaInfoUtil::find_marker(key, SSMETA_SCN_MARKER);
  ASSERT_NE(nullptr, pos);
  ASSERT_EQ(0, MEMCMP(pos, "/scn", 4));
}

TEST_F(TestSSMetaInfoUtil, find_marker_not_found)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/ver00000000000000000100.T20");
  const char *pos = ObSSMetaInfoUtil::find_marker(key, SSMETA_SCN_MARKER);
  ASSERT_EQ(nullptr, pos);
}

TEST_F(TestSSMetaInfoUtil, find_marker_empty_key)
{
  ObString key;
  const char *pos = ObSSMetaInfoUtil::find_marker(key, SSMETA_SCN_MARKER);
  ASSERT_EQ(nullptr, pos);
}

TEST_F(TestSSMetaInfoUtil, find_marker_at_beginning)
{
  ObString key = ObString::make_string("/scn00000000000000000001.T20");
  const char *pos = ObSSMetaInfoUtil::find_marker(key, SSMETA_SCN_MARKER);
  ASSERT_NE(nullptr, pos);
  ASSERT_EQ(key.ptr(), pos);
}

TEST_F(TestSSMetaInfoUtil, find_marker_custom_marker)
{
  ObString key = ObString::make_string("prefix/custom_mark12345");
  const char *pos = ObSSMetaInfoUtil::find_marker(key, "/custom_mark");
  ASSERT_NE(nullptr, pos);
}

/*-----------------------------------------extract_prefix-----------------------------------------*/

TEST_F(TestSSMetaInfoUtil, extract_prefix_basic)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/scn00000000000000000100.T20");
  ObString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_prefix(key, prefix));
  ObString expected = ObString::make_string("tablet/200001/0/meta");
  ASSERT_EQ(expected.length(), prefix.length());
  ASSERT_EQ(0, MEMCMP(expected.ptr(), prefix.ptr(), expected.length()));
}

TEST_F(TestSSMetaInfoUtil, extract_prefix_no_marker)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/ver00000000000000000100.T20");
  ObString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_prefix(key, prefix));
  ASSERT_EQ(key.length(), prefix.length());
  ASSERT_EQ(0, MEMCMP(key.ptr(), prefix.ptr(), key.length()));
}

TEST_F(TestSSMetaInfoUtil, extract_prefix_empty_key)
{
  ObString key;
  ObString prefix;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::extract_prefix(key, prefix));
}

TEST_F(TestSSMetaInfoUtil, extract_prefix_null_marker)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/scn00000000000000000100.T20");
  ObString prefix;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::extract_prefix(key, prefix, nullptr));
}

/*-----------------------------------------extract_scn_value-----------------------------------------*/

TEST_F(TestSSMetaInfoUtil, extract_scn_value_basic)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/scn00000000000000000100.T20");
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(key, scn_val));
  ASSERT_EQ(100UL, scn_val);
}

TEST_F(TestSSMetaInfoUtil, extract_scn_value_large)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/scn18446744073709551615.T20");
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(key, scn_val));
  ASSERT_EQ(UINT64_MAX, scn_val);
}

TEST_F(TestSSMetaInfoUtil, extract_scn_value_zero)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/scn00000000000000000000.T20");
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(key, scn_val));
  ASSERT_EQ(0UL, scn_val);
}

TEST_F(TestSSMetaInfoUtil, extract_scn_value_marker_not_found)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/ver00000000000000000100.T20");
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, ObSSMetaInfoUtil::extract_scn_value(key, scn_val));
}

TEST_F(TestSSMetaInfoUtil, extract_scn_value_empty_key)
{
  ObString key;
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::extract_scn_value(key, scn_val));
}

TEST_F(TestSSMetaInfoUtil, extract_scn_value_null_marker)
{
  ObString key = ObString::make_string("tablet/200001/0/meta/scn00000000000000000100.T20");
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::extract_scn_value(key, scn_val, nullptr));
}

TEST_F(TestSSMetaInfoUtil, extract_scn_value_too_short)
{
  // marker found but not enough digits after it
  ObString key = ObString::make_string("x/scn123");
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObSSMetaInfoUtil::extract_scn_value(key, scn_val));
}

TEST_F(TestSSMetaInfoUtil, extract_scn_value_non_numeric)
{
  // 20 chars after marker but contains non-digit
  ObString key = ObString::make_string("x/scn0000000000000000abcd.T20");
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_INVALID_DATA, ObSSMetaInfoUtil::extract_scn_value(key, scn_val));
}

TEST_F(TestSSMetaInfoUtil, extract_scn_value_custom_marker)
{
  ObString key = ObString::make_string("prefix/ver00000000000000000042.T1");
  uint64_t scn_val = 0;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::extract_scn_value(key, scn_val, "/ver"));
  ASSERT_EQ(42UL, scn_val);
}

/*-----------------------------------------parse_tablet_meta_key-----------------------------------------*/

TEST_F(TestSSMetaInfoUtil, parse_tablet_meta_key_basic)
{
  // format: tablet/{tablet_id}/{reorgan_scn}/meta/scn{meta_scn_20digits}.T{type}
  // type for SHARED_TABLET_META (in_table) = 20
  ObString key = ObString::make_string("tablet/200001/0/meta/scn00000000000000000100.T20");
  ObTabletID tablet_id;
  SCN reorgan_scn;
  SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::parse_tablet_meta_key(key, tablet_id, reorgan_scn, meta_scn));
  ASSERT_EQ(ObTabletID(200001), tablet_id);
  ASSERT_EQ(0UL, reorgan_scn.get_val_for_logservice());
  ASSERT_EQ(100UL, meta_scn.get_val_for_logservice());
}

TEST_F(TestSSMetaInfoUtil, parse_tablet_meta_key_with_reorgan_scn)
{
  ObString key = ObString::make_string("tablet/300001/999/meta/scn00000000000000050000.T20");
  ObTabletID tablet_id;
  SCN reorgan_scn;
  SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::parse_tablet_meta_key(key, tablet_id, reorgan_scn, meta_scn));
  ASSERT_EQ(ObTabletID(300001), tablet_id);
  ASSERT_EQ(999UL, reorgan_scn.get_val_for_logservice());
  ASSERT_EQ(50000UL, meta_scn.get_val_for_logservice());
}

TEST_F(TestSSMetaInfoUtil, parse_tablet_meta_key_empty)
{
  ObString key;
  ObTabletID tablet_id;
  SCN reorgan_scn;
  SCN meta_scn;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::parse_tablet_meta_key(key, tablet_id, reorgan_scn, meta_scn));
}

TEST_F(TestSSMetaInfoUtil, parse_tablet_meta_key_too_long)
{
  char buf[600];
  memset(buf, 'a', sizeof(buf));
  buf[599] = '\0';
  ObString key = ObString::make_string(buf);
  ObTabletID tablet_id;
  SCN reorgan_scn;
  SCN meta_scn;
  ASSERT_EQ(OB_SIZE_OVERFLOW, ObSSMetaInfoUtil::parse_tablet_meta_key(key, tablet_id, reorgan_scn, meta_scn));
}

/*-----------------------------------------gen_meta_key_prefix-----------------------------------------*/

TEST_F(TestSSMetaInfoUtil, gen_meta_key_prefix_tablet)
{
  ObSSMetaReadParamV2 param;
  param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ObLSID(1001), ObTabletID(200001), SCN::min_scn(), SCN::min_scn(), true);

  ObSqlString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_meta_key_prefix(param, prefix));
  ASSERT_STREQ("tablet/200001/0/meta", prefix.ptr());
}

TEST_F(TestSSMetaInfoUtil, gen_meta_key_prefix_tablet_with_reorgan_scn)
{
  ObSSMetaReadParamV2 param;
  SCN reorgan_scn;
  reorgan_scn.convert_for_logservice(12345);
  param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ObLSID(1001), ObTabletID(300002), SCN::min_scn(), reorgan_scn, true);

  ObSqlString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_meta_key_prefix(param, prefix));
  ASSERT_STREQ("tablet/300002/12345/meta", prefix.ptr());
}

TEST_F(TestSSMetaInfoUtil, gen_meta_key_prefix_ls_meta)
{
  ObSSMetaReadParamV2 param;
  param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::LS_META,
      ObLSID(8001), SCN::min_scn(), true);

  ObSqlString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_meta_key_prefix(param, prefix));
  ASSERT_STREQ("ls/8001/meta", prefix.ptr());
}

TEST_F(TestSSMetaInfoUtil, gen_meta_key_prefix_checkpoint)
{
  ObSSMetaReadParamV2 param;
  param.set_ls_level_param(
      ObSSMetaReadParamType::LS_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::CHECKPOINT,
      ObLSID(8002), SCN::min_scn(), true);

  ObSqlString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_meta_key_prefix(param, prefix));
  ASSERT_STREQ("ls/8002/meta", prefix.ptr());
}

TEST_F(TestSSMetaInfoUtil, gen_meta_key_prefix_invalid_param)
{
  ObSSMetaReadParamV2 invalid_param;
  ObSqlString prefix;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::gen_meta_key_prefix(invalid_param, prefix));
}

// inner tablet 通过 gen_meta_key_prefix 应生成 ls/{ls_id}/{tablet_name}/meta
TEST_F(TestSSMetaInfoUtil, gen_meta_key_prefix_inner_tablet)
{
  ObSSMetaReadParamV2 param;
  param.set_tablet_level_param(
      ObSSMetaReadParamType::TABLET_PREFIX,
      ObSSMetaReadResultType::READ_WHOLE_ROW,
      ObAtomicProtocolTypeEnum::TABLET_META,
      ObLSID(1001), ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID),
      SCN::min_scn(), SCN::min_scn(), true);

  ObSqlString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_meta_key_prefix(param, prefix));
  ASSERT_STREQ("ls/1001/TX_DATA/meta", prefix.ptr());
}

/*-----------------------------------------gen_tablet_meta_key_prefix-----------------------------------------*/

TEST_F(TestSSMetaInfoUtil, gen_tablet_meta_key_prefix_normal)
{
  ObSqlString prefix;
  SCN reorgan_scn;
  reorgan_scn.convert_for_logservice(12345);
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
      ObLSID(1001), ObTabletID(300002), reorgan_scn, prefix));
  ASSERT_STREQ("tablet/300002/12345/meta", prefix.ptr());
}

// 普通 tablet 且 reorgan_scn 为 min_scn（合法）时，前缀含 reorgan_scn=0
TEST_F(TestSSMetaInfoUtil, gen_tablet_meta_key_prefix_normal_min_scn)
{
  ObSqlString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
      ObLSID(1001), ObTabletID(200001), SCN::min_scn(), prefix));
  ASSERT_STREQ("tablet/200001/0/meta", prefix.ptr());
}

// 普通 tablet 且 reorgan_scn 非法时，前缀退化为 tablet/{tablet_id}/
TEST_F(TestSSMetaInfoUtil, gen_tablet_meta_key_prefix_normal_invalid_scn)
{
  ObSqlString prefix;
  ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
      ObLSID(1001), ObTabletID(200001), SCN::invalid_scn(), prefix));
  ASSERT_STREQ("tablet/200001/", prefix.ptr());
}

// inner tablet：与 reorgan_scn 是否合法无关，始终为 ls/{ls_id}/{tablet_name}/meta
TEST_F(TestSSMetaInfoUtil, gen_tablet_meta_key_prefix_inner_all_names)
{
  struct {
    uint64_t tablet_id_;
    const char *name_;
  } cases[] = {
    {ObTabletID::LS_TX_CTX_TABLET_ID,     "TX_CTX"},
    {ObTabletID::LS_TX_DATA_TABLET_ID,    "TX_DATA"},
    {ObTabletID::LS_LOCK_TABLET_ID,       "TX_LOCK"},
    {ObTabletID::LS_REORG_INFO_TABLET_ID, "REORG_INFO"},
  };
  for (int64_t i = 0; i < 4; ++i) {
    // reorgan_scn 合法
    {
      ObSqlString prefix;
      ObSqlString expected;
      ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
          ObLSID(2001), ObTabletID(cases[i].tablet_id_), SCN::min_scn(), prefix));
      ASSERT_EQ(OB_SUCCESS, expected.assign_fmt("ls/2001/%s/meta", cases[i].name_));
      ASSERT_STREQ(expected.ptr(), prefix.ptr());
    }
    // reorgan_scn 非法
    {
      ObSqlString prefix;
      ObSqlString expected;
      ASSERT_EQ(OB_SUCCESS, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
          ObLSID(2001), ObTabletID(cases[i].tablet_id_), SCN::invalid_scn(), prefix));
      ASSERT_EQ(OB_SUCCESS, expected.assign_fmt("ls/2001/%s/meta", cases[i].name_));
      ASSERT_STREQ(expected.ptr(), prefix.ptr());
    }
  }
}

// inner tablet 缺少合法 ls_id 时报错
TEST_F(TestSSMetaInfoUtil, gen_tablet_meta_key_prefix_inner_invalid_ls)
{
  ObSqlString prefix;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
      ObLSID(), ObTabletID(ObTabletID::LS_TX_DATA_TABLET_ID), SCN::min_scn(), prefix));
}

TEST_F(TestSSMetaInfoUtil, gen_tablet_meta_key_prefix_invalid_tablet)
{
  ObSqlString prefix;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSSMetaInfoUtil::gen_tablet_meta_key_prefix(
      ObLSID(1001), ObTabletID(), SCN::min_scn(), prefix));
}

/*-----------------------------------------parse_tablet_meta_key (with ls_id)-----------------------------------------*/

TEST_F(TestSSMetaInfoUtil, parse_tablet_meta_key_inner_tablet)
{
  // inner tablet: ls/{ls_id}/{tablet_name}/meta/scn{20}.T20
  ObString key = ObString::make_string("ls/1002/TX_DATA/meta/scn00000000000000000100.T20");
  ObLSID ls_id;
  ObTabletID tablet_id;
  SCN reorgan_scn;
  SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tablet_id, reorgan_scn, meta_scn));
  ASSERT_EQ(1002, ls_id.id());
  ASSERT_EQ(static_cast<uint64_t>(ObTabletID::LS_TX_DATA_TABLET_ID), tablet_id.id());
  ASSERT_TRUE(reorgan_scn.is_valid());
  ASSERT_EQ(100UL, meta_scn.get_val_for_logservice());
}

TEST_F(TestSSMetaInfoUtil, parse_tablet_meta_key_normal_with_ls_out)
{
  // 普通 tablet 不解析 ls_id
  ObString key = ObString::make_string("tablet/300001/999/meta/scn00000000000000050000.T20");
  ObLSID ls_id;
  ObTabletID tablet_id;
  SCN reorgan_scn;
  SCN meta_scn;
  ASSERT_EQ(OB_SUCCESS,
      ObSSMetaInfoUtil::parse_tablet_meta_key(key, ls_id, tablet_id, reorgan_scn, meta_scn));
  ASSERT_FALSE(ls_id.is_valid());
  ASSERT_EQ(300001UL, tablet_id.id());
  ASSERT_EQ(999UL, reorgan_scn.get_val_for_logservice());
  ASSERT_EQ(50000UL, meta_scn.get_val_for_logservice());
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_meta_info_util.log*");
  OB_LOGGER.set_file_name("test_ss_meta_info_util.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
