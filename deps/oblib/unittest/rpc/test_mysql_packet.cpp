/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/obp20_extra_info.h"

using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace oceanbase::obmysql;

class ObFakeMySQLPacket : public ObMySQLPacket
{
public:
  ObFakeMySQLPacket() : content_len_(0) {}
  int64_t content_len_;
protected:
  virtual int serialize(char *start, const int64_t len, int64_t &pos) const
  {
    int ret = OB_SUCCESS;
    if ((NULL == start) || (content_len_ < 0)) {
      ret = OB_INVALID_ARGUMENT;
    } else if ((len - pos) < content_len_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      for (int i = 0; i < content_len_; i++) {
        start[pos + i] = 'a';
      }
      pos += content_len_;
    }
    return ret;
  }
};

class TestMySQLPacket : public ::testing::Test
{
public:
  TestMySQLPacket()
  {
  }

  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};


TEST_F(TestMySQLPacket, common)
{
  ObFakeMySQLPacket fake_pkt;
  fake_pkt.set_seq(255);
  fake_pkt.content_len_ = 1;
  int64_t pkt_count = 0;
  int64_t pos = 0;
  int ret = OB_SUCCESS;
  int64_t len = 100;
  char *buf = new char[len];
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, pkt_count);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 1 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 1;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  EXPECT_EQ(0, pkt_count);
  EXPECT_EQ(0, pos);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 1 + 4 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 1;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(2, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 1 + 4 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 1;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(2, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 2 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 2;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = 16 * 1024 *1024 - 1 + 1 + 4 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = 16 * 1024 * 1024 - 1 + 1;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(2, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = (16 * 1024 * 1024 - 1 + 4) * 5 + 4;
  buf = new char[len];
  fake_pkt.content_len_ = (16 * 1024 * 1024 - 1) * 5;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(6, pkt_count);
  EXPECT_EQ(len, pos);
  delete []buf;
  buf = NULL;

  len = (16 * 1024 * 1024 - 1 + 4);
  buf = new char[len];
  fake_pkt.content_len_ = (16 * 1024 * 1024 - 1) * 5;
  pkt_count = 0;
  pos = 0;
  ret = fake_pkt.encode(buf, len, pos, pkt_count);
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, ret);
  EXPECT_EQ(0, pkt_count);
  EXPECT_EQ(0, pos);
  delete []buf;
  buf = NULL;
}


int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

TEST_F(TestMySQLPacket, proxy_database_decode_and_copy)
{
  char packet[128] = {};
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, ObProtoTransUtil::store_type_and_len(
      packet, sizeof(packet), size, PROXY_ONE_WAY_SYNC_INFO, 11));
  ASSERT_EQ(OB_SUCCESS, ObProtoTransUtil::store_str(
      packet, sizeof(packet), size, "sales", 5, PROXY_SYNC_DATABASE_TYPE));
  Obp20ProxyOneWaySyncDecoder decoder;
  Ob20ExtraInfo decoded;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoder.deserialize(packet, size, pos, decoded));
  EXPECT_EQ(size, pos);
  ASSERT_EQ(5, decoded.get_total_len());
  EXPECT_EQ(ObString::make_string("sales"), decoded.get_sql_database());

  char owned[5];
  Ob20ExtraInfo copied;
  ASSERT_EQ(OB_SUCCESS, copied.assign(decoded, owned, sizeof(owned)));
  MEMSET(packet, 'x', sizeof(packet));
  EXPECT_EQ(ObString::make_string("sales"), copied.get_sql_database());
  copied.reset();
  EXPECT_FALSE(copied.exist_sql_database());
}

TEST_F(TestMySQLPacket, proxy_database_rejects_truncated_container)
{
  char packet[128] = {};
  int64_t size = 0;
  // The parent has only one byte; following packet bytes cannot complete its TLV.
  ASSERT_EQ(OB_SUCCESS, ObProtoTransUtil::store_type_and_len(
      packet, sizeof(packet), size, PROXY_ONE_WAY_SYNC_INFO, 1));
  ASSERT_EQ(OB_SUCCESS, ObProtoTransUtil::store_str(
      packet, sizeof(packet), size, "sales", 5, PROXY_SYNC_DATABASE_TYPE));
  Obp20ProxyOneWaySyncDecoder decoder;
  Ob20ExtraInfo decoded;
  int64_t pos = 0;
  EXPECT_NE(OB_SUCCESS, decoder.deserialize(packet, size, pos, decoded));
  EXPECT_LE(pos, 7);
  EXPECT_FALSE(decoded.exist_sql_database());
}

TEST_F(TestMySQLPacket, proxy_database_skips_unknown_subkey)
{
  char packet[128] = {};
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, ObProtoTransUtil::store_type_and_len(
      packet, sizeof(packet), size, PROXY_ONE_WAY_SYNC_INFO, 18));
  ASSERT_EQ(OB_SUCCESS, ObProtoTransUtil::store_str(packet, sizeof(packet), size, "x", 1, 99));
  ASSERT_EQ(OB_SUCCESS, ObProtoTransUtil::store_str(
      packet, sizeof(packet), size, "sales", 5, PROXY_SYNC_DATABASE_TYPE));
  Obp20ProxyOneWaySyncDecoder decoder;
  Ob20ExtraInfo decoded;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoder.deserialize(packet, size, pos, decoded));
  EXPECT_EQ(size, pos);
  EXPECT_EQ(ObString::make_string("sales"), decoded.get_sql_database());
}
