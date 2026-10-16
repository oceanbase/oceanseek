/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include "lib/compress/ob_compressor_pool.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/config/ob_config_helper.h"
#include "sql/dtl/ob_dtl_flow_control.h"
#include "sql/dtl/ob_dtl_rpc_proxy.h"
#include "sql/dtl/ob_dtl.h"
#include "sql/dtl/ob_dtl_channel_agent.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_diagnostic_info_container.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace sql::dtl;

static const char *const CODECS[] = {
  "none", "lz4_1.0", "snappy_1.0", "zstd_1.3.8", "zstd_1.5.7"
};

template <ObRpcPacketCode PCODE, class Args>
class DtlPacketDecoder : public ObDtlRpcProxy::Processor<PCODE>
{
public:
  int process() override { return OB_SUCCESS; }
  int decode(const ObRpcPacket &packet)
  {
    this->rpc_pkt_ = &packet;
    return this->deserialize();
  }
  const Args &args() const { return this->arg_; }
};

class TestDtlCompression : public ::testing::Test
{
protected:
  void SetUp() override
  {
    old_checksum_level_ = get_rpc_checksum_check_level();
    set_rpc_checksum_check_level(ObRpcCheckSumCheckLevel::FORCE);
  }
  void TearDown() override { set_rpc_checksum_check_level(old_checksum_level_); }

  template <class Args>
  void encode(const Args &args, const char *codec, ObRpcPacket &decoded_packet)
  {
    ObCompressorType type = INVALID_COMPRESSOR;
    ASSERT_EQ(OB_SUCCESS, ObDtlFlowControl::select_compressor_type(true, codec, type));
    ObCompressor *compressor = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(type, compressor));
    const int64_t size = args.get_serialize_size();
    char *plain = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, plain);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, args.serialize(plain, size, pos));
    ASSERT_EQ(size, pos);
    int64_t overflow = 0;
    ASSERT_EQ(OB_SUCCESS, compressor->get_max_overflow_size(size, overflow));
    char *compressed = static_cast<char *>(allocator_.alloc(size + overflow));
    ASSERT_NE(nullptr, compressed);
    int64_t compressed_size = 0;
    ASSERT_EQ(OB_SUCCESS, compressor->compress(plain, size, compressed, size + overflow, compressed_size));
    ObRpcPacket packet;
    if (NONE_COMPRESSOR != type && compressed_size < size) {
      packet.set_content(compressed, compressed_size);
      packet.set_compressor_type(type);
      packet.set_original_len(size);
    } else {
      packet.set_content(plain, size);
      packet.set_compressor_type(INVALID_COMPRESSOR);
    }
    packet.calc_checksum();
    char *wire = static_cast<char *>(allocator_.alloc(size + overflow + 1024));
    ASSERT_NE(nullptr, wire);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, packet.encode(wire, size + overflow + 1024, pos));
    ASSERT_EQ(OB_SUCCESS, decoded_packet.decode(wire, pos));
    ASSERT_EQ(packet.get_compressor_type(), decoded_packet.get_compressor_type());
    ASSERT_EQ(packet.get_original_len(), decoded_packet.get_original_len());
  }

  void expect_buffer(const ObDtlLinkedBuffer &expected, const ObDtlLinkedBuffer &actual)
  {
    ASSERT_EQ(expected.size(), actual.size());
    ASSERT_EQ(expected.seq_no(), actual.seq_no());
    ASSERT_EQ(expected.tenant_id(), actual.tenant_id());
    ASSERT_EQ(0, MEMCMP(expected.buf(), actual.buf(), expected.size()));
  }
  ObArenaAllocator allocator_;
  ObRpcCheckSumCheckLevel old_checksum_level_;
};

TEST_F(TestDtlCompression, whitelist_and_selection)
{
  for (int64_t i = 0; i < ARRAYSIZEOF(CODECS); ++i) {
    SCOPED_TRACE(CODECS[i]);
    EXPECT_TRUE(ObConfigDtlCompressFuncChecker::is_valid_compress_func(CODECS[i]));
    ObCompressorType expected = INVALID_COMPRESSOR;
    ObCompressorType actual = INVALID_COMPRESSOR;
    ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor_type(CODECS[i], expected));
    EXPECT_EQ(OB_SUCCESS, ObDtlFlowControl::select_compressor_type(true, CODECS[i], actual));
    EXPECT_EQ(expected, actual);
    EXPECT_EQ(OB_SUCCESS, ObDtlFlowControl::select_compressor_type(false, CODECS[i], actual));
    EXPECT_EQ(NONE_COMPRESSOR, actual);
  }
  const char *invalid[] = {nullptr, "", "lz4", "zlib_1.0", "lz4_1.9.1",
      "zstd_1.0", "ZSTD_1.0", "stream_zstd_1.3.8", "stream_zstd_1.5.7",
      "stream_lz4_1.0", "zstd_1.3.8_qat", " lz4_1.0", "snappy_1.0 ", "zstd_1.3.8 "};
  for (int64_t i = 0; i < ARRAYSIZEOF(invalid); ++i) {
    EXPECT_FALSE(ObConfigDtlCompressFuncChecker::is_valid_compress_func(invalid[i]));
    ObCompressorType type = LZ4_COMPRESSOR;
    EXPECT_EQ(OB_INVALID_ARGUMENT, ObDtlFlowControl::select_compressor_type(true, invalid[i], type));
    EXPECT_EQ(LZ4_COMPRESSOR, type);
  }
  ObCompressorType type = INVALID_COMPRESSOR;
  EXPECT_EQ(OB_SUCCESS, ObDtlFlowControl::select_compressor_type(false, nullptr, type));
  EXPECT_EQ(NONE_COMPRESSOR, type);
  EXPECT_EQ(OB_SUCCESS, ObDtlFlowControl::select_compressor_type(true, "LZ4_1.0", type));
  EXPECT_EQ(LZ4_COMPRESSOR, type);
  EXPECT_TRUE(ObConfigDtlCompressFuncChecker::is_valid_compress_func("ZSTD_1.3.8"));
  EXPECT_EQ(OB_SUCCESS, ObDtlFlowControl::select_compressor_type(true, "ZSTD_1.3.8", type));
  EXPECT_EQ(ZSTD_1_3_8_COMPRESSOR, type);
  EXPECT_EQ(OB_SUCCESS, ObDtlFlowControl::select_compressor_type(true, "ZSTD_1.5.7", type));
  EXPECT_EQ(ZSTD_1_5_7_COMPRESSOR, type);
}

TEST_F(TestDtlCompression, tiny_and_incompressible_binary_roundtrip)
{
  const int64_t sizes[] = {1, 4, 64, 4096, 65536, 1048576};
  // NONE is a no-op (it reports the length without copying), covered by raw RPC packets above.
  for (int64_t i = 1; i < ARRAYSIZEOF(CODECS); ++i) {
    SCOPED_TRACE(CODECS[i]);
    ObCompressor *compressor = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(CODECS[i], compressor));
    for (int64_t j = 0; j < ARRAYSIZEOF(sizes); ++j) {
      SCOPED_TRACE(sizes[j]);
      const int64_t size = sizes[j];
      int64_t overflow = 0;
      ASSERT_EQ(OB_SUCCESS, compressor->get_max_overflow_size(size, overflow));
      char *plain = static_cast<char *>(allocator_.alloc(size));
      char *encoded = static_cast<char *>(allocator_.alloc(size + overflow));
      char *decoded = static_cast<char *>(allocator_.alloc(size));
      ASSERT_NE(nullptr, plain);
      ASSERT_NE(nullptr, encoded);
      ASSERT_NE(nullptr, decoded);
      uint32_t state = 42;
      for (int64_t k = 0; k < size; ++k) {
        state ^= state << 13;
        state ^= state >> 17;
        state ^= state << 5;
        plain[k] = static_cast<char>(state & 0xff);
      }
      int64_t encoded_size = 0;
      int64_t decoded_size = 0;
      ASSERT_EQ(OB_SUCCESS, compressor->compress(plain, size, encoded, size + overflow, encoded_size));
      ASSERT_EQ(OB_SUCCESS, compressor->decompress(encoded, encoded_size, decoded, size, decoded_size));
      EXPECT_EQ(size, decoded_size);
      EXPECT_EQ(0, MEMCMP(plain, decoded, size));
      // The RPC sender must keep raw bytes when compression is not beneficial.
      EXPECT_GE(encoded_size, size);
    }
  }
}

TEST_F(TestDtlCompression, single_batch_control_and_broadcast_packet_roundtrip)
{
  char payload[65536];
  MEMSET(payload, 'x', sizeof(payload));
  ObDtlLinkedBuffer buffer(payload, sizeof(payload), sizeof(payload));
  buffer.seq_no() = 2;
  buffer.tenant_id() = 1001;
  // Decode alternating codecs without consulting a receiver-side tenant configuration.
  for (int64_t i = 0; i < ARRAYSIZEOF(CODECS); ++i) {
    SCOPED_TRACE(CODECS[i]);
    ObDtlSendArgs single(42, buffer);
    ObRpcPacket single_packet;
    ASSERT_NO_FATAL_FAILURE(encode(single, CODECS[i], single_packet));
    DtlPacketDecoder<OB_DTL_SEND, ObDtlSendArgs> single_decoder;
    ASSERT_EQ(OB_SUCCESS, single_decoder.decode(single_packet));
    EXPECT_EQ(42, single_decoder.args().chid_);
    expect_buffer(buffer, single_decoder.args().buffer_);
    for (int64_t count = 1; count <= 2; ++count) {
      ObDtlBatchSendArgs batch;
      for (int64_t j = 0; j < count; ++j) {
        ObDtlSendArgRef ref = {42 + j, &buffer};
        ASSERT_EQ(OB_SUCCESS, batch.arg_refs_.push_back(ref));
      }
      ObRpcPacket packet;
      ASSERT_NO_FATAL_FAILURE(encode(batch, CODECS[i], packet));
      DtlPacketDecoder<OB_DTL_BATCH_SEND, ObDtlBatchSendArgs> decoder;
      ASSERT_EQ(OB_SUCCESS, decoder.decode(packet));
      ASSERT_EQ(count, decoder.args().args_.count());
      for (int64_t j = 0; j < count; ++j) {
        EXPECT_EQ(42 + j, decoder.args().args_.at(j).chid_);
        expect_buffer(buffer, decoder.args().args_.at(j).buffer_);
      }
    }
    ObDtlBatchSendArgs control;
    control.is_pure_control_msg_ = true;
    ASSERT_EQ(OB_SUCCESS, control.batch_buffer_.shallow_copy(buffer));
    control.batch_buffer_.set_data_msg(false);
    ObRpcPacket control_packet;
    ASSERT_NO_FATAL_FAILURE(encode(control, CODECS[i], control_packet));
    DtlPacketDecoder<OB_DTL_BATCH_SEND, ObDtlBatchSendArgs> control_decoder;
    ASSERT_EQ(OB_SUCCESS, control_decoder.decode(control_packet));
    EXPECT_TRUE(control_decoder.args().is_pure_control_msg_);
    EXPECT_FALSE(control_decoder.args().batch_buffer_.is_data_msg());
    expect_buffer(buffer, control_decoder.args().batch_buffer_);
    ObDtlBCSendArgs broadcast;
    ObDtlLinkedBuffer empty;
    ASSERT_EQ(OB_SUCCESS, broadcast.args_.push_back(ObDtlSendArgs(42, empty)));
    ASSERT_EQ(OB_SUCCESS, broadcast.args_.push_back(ObDtlSendArgs(43, empty)));
    ASSERT_EQ(OB_SUCCESS, broadcast.bc_buffer_.shallow_copy(buffer));
    ObRpcPacket broadcast_packet;
    ASSERT_NO_FATAL_FAILURE(encode(broadcast, CODECS[i], broadcast_packet));
    DtlPacketDecoder<OB_DTL_BC_SEND, ObDtlBCSendArgs> broadcast_decoder;
    ASSERT_EQ(OB_SUCCESS, broadcast_decoder.decode(broadcast_packet));
    ASSERT_EQ(2, broadcast_decoder.args().args_.count());
    expect_buffer(buffer, broadcast_decoder.args().bc_buffer_);
  }
}

TEST_F(TestDtlCompression, reject_invalid_compressed_packets)
{
  char payload[65536];
  MEMSET(payload, 'x', sizeof(payload));
  ObDtlLinkedBuffer buffer(payload, sizeof(payload), sizeof(payload));
  buffer.seq_no() = 2;
  ObDtlSendArgs args(42, buffer);
  for (int64_t i = 1; i < ARRAYSIZEOF(CODECS); ++i) {
    SCOPED_TRACE(CODECS[i]);
    ObRpcPacket packet;
    ASSERT_NO_FATAL_FAILURE(encode(args, CODECS[i], packet));
    ASSERT_NE(INVALID_COMPRESSOR, packet.get_compressor_type());
    const int32_t original_len = packet.get_original_len();
    packet.set_original_len(0);
    DtlPacketDecoder<OB_DTL_SEND, ObDtlSendArgs> zero_length;
    EXPECT_NE(OB_SUCCESS, zero_length.decode(packet));
    packet.set_original_len(original_len + 1);
    DtlPacketDecoder<OB_DTL_SEND, ObDtlSendArgs> wrong_length;
    EXPECT_NE(OB_SUCCESS, wrong_length.decode(packet));
    packet.set_original_len(original_len);
    packet.set_content(packet.get_cdata(), packet.get_clen() / 2);
    packet.calc_checksum();
    DtlPacketDecoder<OB_DTL_SEND, ObDtlSendArgs> truncated;
    EXPECT_NE(OB_SUCCESS, truncated.decode(packet));
    packet.set_compressor_type(static_cast<ObCompressorType>(127));
    DtlPacketDecoder<OB_DTL_SEND, ObDtlSendArgs> unsupported;
    EXPECT_NE(OB_SUCCESS, unsupported.decode(packet));
  }
}

class CompressionTestChannel : public ObDtlRpcChannel
{
public:
  explicit CompressionTestChannel(const ObAddr &peer)
      : ObDtlRpcChannel(1, 42, peer, DtlChannelType::RPC_CHANNEL) {}
  int prepare()
  {
    is_inited_ = true;
    return msg_response_.init(false);
  }
};

class TestDtlCompressionSender : public ::testing::Test
{
protected:
  TestDtlCompressionSender() : transport_(&eio_, &handler_) {}
  void SetUp() override
  {
    ObDiagnosticInfoContainer *container = ObDiagnosticInfoContainer::get_global_di_container();
    if (!container->is_inited()) {
      ASSERT_EQ(OB_SUCCESS, container->init(1, true));
    }
    MEMSET(&eio_, 0, sizeof(eio_));
    MEMSET(&handler_, 0, sizeof(handler_));
    saved_proxy_ = DTL.get_rpc_proxy();
    DTL.get_rpc_proxy() = ObDtlRpcProxy();
    ASSERT_TRUE(peer_.set_ip_addr("127.0.0.1", 2882));
    ASSERT_EQ(OB_SUCCESS, DTL.get_rpc_proxy().init(&transport_, peer_));
    DTL.get_rpc_proxy().set_transport_impl(0);
  }
  void TearDown() override { DTL.get_rpc_proxy() = saved_proxy_; }
  int64_t compression_attempts()
  {
    ObDiagnosticInfo *info = ObLocalDiagnosticInfo::get();
    if (nullptr == info) {
      return -1;
    }
    ObStatEventAddStat *stat = info->get_add_stat_stats().get(ObStatEventIds::RPC_COMPRESS_ORIGINAL_PACKET_CNT);
    return nullptr == stat ? -1 : stat->get_stat_value();
  }
  // No IO threads: exercise the real serialization/compression path, then fail
  // at post_session before any network operation. Count compression attempts.
  easy_io_t eio_;
  easy_io_handler_pt handler_;
  rpc::frame::ObReqTransport transport_;
  ObDtlRpcProxy saved_proxy_;
  ObAddr peer_;
};

TEST_F(TestDtlCompressionSender, single_control_batch_and_broadcast_apply_selection)
{
  ObBackGroundSessionGuard diagnostic_guard(1, 0);
  ASSERT_TRUE(lib::is_diagnose_info_enabled());
  char payload[65536];
  MEMSET(payload, 'x', sizeof(payload));
  for (int64_t i = 0; i < ARRAYSIZEOF(CODECS); ++i) {
    SCOPED_TRACE(CODECS[i]);
    ObCompressorType type = INVALID_COMPRESSOR;
    ASSERT_EQ(OB_SUCCESS, ObDtlFlowControl::select_compressor_type(true, CODECS[i], type));
    for (int64_t path = 0; path < 3; ++path) {
      SCOPED_TRACE(path);
      ObDtlLinkedBuffer buffer(payload, sizeof(payload), sizeof(payload));
      buffer.seq_no() = 2;
      buffer.tenant_id() = 1;
      buffer.timeout_ts() = ObTimeUtility::current_time() + 10000000;
      ObDtlLinkedBuffer *ptr = &buffer;
      const int64_t before = compression_attempts();
      ASSERT_GE(before, 0);
      if (path < 2) {
        CompressionTestChannel channel(peer_);
        ASSERT_EQ(OB_SUCCESS, channel.prepare());
        channel.set_compression_type(type);
        if (path == 1) {
          buffer.set_batch_send();
          buffer.set_data_msg(false);
        }
        ASSERT_EQ(OB_INVALID_ARGUMENT, channel.send_message(ptr));
      } else {
        ObDtlBcastService service(1, type);
        service.server_addr_ = peer_;
        service.bcast_ch_count_ = 1;
        service.active_chs_count_ = 1;
        SendMsgResponse response;
        ASSERT_EQ(OB_SUCCESS, response.init(false));
        ASSERT_EQ(OB_SUCCESS, service.resps_.push_back(&response));
        ASSERT_EQ(OB_SUCCESS, service.peer_ids_.push_back(43));
        ASSERT_EQ(OB_INVALID_ARGUMENT, service.send_message(ptr, false));
      }
      EXPECT_EQ(NONE_COMPRESSOR == type ? 0 : 1, compression_attempts() - before);
    }
  }
}
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("WARN");
  return RUN_ALL_TESTS();
}
