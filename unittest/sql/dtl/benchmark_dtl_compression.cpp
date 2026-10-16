/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <stdio.h>
#include <time.h>
#include "lib/compress/ob_compressor_pool.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
using namespace common;

static int64_t monotonic_ns()
{
  struct timespec now;
  clock_gettime(CLOCK_MONOTONIC, &now);
  return now.tv_sec * 1000000000L + now.tv_nsec;
}

// This is a codec microbenchmark of generated TPC CSV bytes, not a SQL benchmark.
TEST(DtlCompressionBenchmark, tpc_samples)
{
  const char *directory = getenv("DTL_COMPRESSION_DATA_DIR");
  if (nullptr == directory) {
    directory = DTL_COMPRESSION_DEFAULT_DATA_DIR;
  }
  const char *files[] = {"tpch_lineitem.csv", "tpch_orders.csv", "tpch_customer.csv",
      "tpcds_store_sales.csv", "tpcds_item.csv", "tpcds_customer.csv"};
  const char *codecs[] = {"none", "lz4_1.0", "snappy_1.0", "zstd_1.3.8", "zstd_1.5.7"};
  const int64_t blocks[] = {4096, 65536, 1048576};
  const int64_t repeats = 5;
  printf("dataset,codec,block_bytes,input_bytes,codec_bytes,payload_bytes,raw_fallback_blocks,"
         "payload_pct,compress_mib_s,decompress_mib_s,repeats\n");
  for (int64_t f = 0; f < ARRAYSIZEOF(files); ++f) {
    SCOPED_TRACE(files[f]);
    ObArenaAllocator allocator;
    char path[4096];
    ASSERT_LT(snprintf(path, sizeof(path), "%s/%s", directory, files[f]), sizeof(path));
    FILE *input = fopen(path, "rb");
    ASSERT_NE(nullptr, input) << path;
    ASSERT_EQ(0, fseek(input, 0, SEEK_END));
    const int64_t size = ftell(input);
    ASSERT_GT(size, 0);
    ASSERT_LE(size, 256 * 1024 * 1024);
    rewind(input);
    char *data = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, data);
    const int64_t read_size = fread(data, 1, size, input);
    ASSERT_EQ(0, fclose(input));
    ASSERT_EQ(size, read_size);
    for (int64_t c = 0; c < ARRAYSIZEOF(codecs); ++c) {
      SCOPED_TRACE(codecs[c]);
      ObCompressor *compressor = nullptr;
      ASSERT_EQ(OB_SUCCESS, ObCompressorPool::get_instance().get_compressor(codecs[c], compressor));
      for (int64_t b = 0; b < ARRAYSIZEOF(blocks); ++b) {
        const int64_t block = blocks[b];
        if (NONE_COMPRESSOR == compressor->get_compressor_type()) {
          // NONE is a no-op, not a memcpy codec. There is no codec throughput to time.
          printf("%s,%s,%ld,%ld,%ld,%ld,%ld,100.00,NA,NA,0\n", files[f], codecs[c],
              block, size, size, size, (size + block - 1) / block);
          continue;
        }
        ObArenaAllocator buffers;
        int64_t overflow = 0;
        ASSERT_EQ(OB_SUCCESS, compressor->get_max_overflow_size(block, overflow));
        char *encoded = static_cast<char *>(buffers.alloc(block + overflow));
        char *decoded = static_cast<char *>(buffers.alloc(block));
        ASSERT_NE(nullptr, encoded);
        ASSERT_NE(nullptr, decoded);
        int64_t compressed_bytes = 0;
        int64_t payload_bytes = 0;
        int64_t fallback_blocks = 0;
        int64_t compress_ns = 0;
        int64_t decompress_ns = 0;
        for (int64_t offset = 0; offset < size; offset += block) {
          const int64_t length = MIN(block, size - offset);
          int64_t encoded_size = 0;
          int64_t decoded_size = 0;
          // Warm the codec before timing; validate outside the timed region.
          ASSERT_EQ(OB_SUCCESS, compressor->compress(data + offset, length, encoded,
              block + overflow, encoded_size));
          ASSERT_EQ(OB_SUCCESS, compressor->decompress(encoded, encoded_size, decoded,
              block, decoded_size));
          ASSERT_EQ(length, decoded_size);
          ASSERT_EQ(0, MEMCMP(data + offset, decoded, length));
          compressed_bytes += encoded_size;
          payload_bytes += MIN(length, encoded_size);
          fallback_blocks += encoded_size >= length ? 1 : 0;
          int ret = OB_SUCCESS;
          int64_t start = monotonic_ns();
          for (int64_t r = 0; OB_SUCC(ret) && r < repeats; ++r) {
            ret = compressor->compress(data + offset, length, encoded, block + overflow, encoded_size);
          }
          compress_ns += monotonic_ns() - start;
          ASSERT_EQ(OB_SUCCESS, ret);
          start = monotonic_ns();
          for (int64_t r = 0; OB_SUCC(ret) && r < repeats; ++r) {
            ret = compressor->decompress(encoded, encoded_size, decoded, block, decoded_size);
          }
          decompress_ns += monotonic_ns() - start;
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_EQ(length, decoded_size);
          ASSERT_EQ(0, MEMCMP(data + offset, decoded, length));
        }
        ASSERT_GT(compress_ns, 0);
        ASSERT_GT(decompress_ns, 0);
        printf("%s,%s,%ld,%ld,%ld,%ld,%ld,%.2f,%.2f,%.2f,%ld\n", files[f], codecs[c],
            block, size, compressed_bytes, payload_bytes, fallback_blocks,
            100.0 * payload_bytes / size,
            1e9 * repeats * size / (1048576.0 * compress_ns),
            1e9 * repeats * size / (1048576.0 * decompress_ns), repeats);
      }
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
