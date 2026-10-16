/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>

#include "lib/checksum/ob_crc64.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"
#include "logservice/restoreservice/ob_remote_data_generator.h"
#include "logservice/palf/log_group_buffer.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_entry_header.h"
#include "share/ob_cluster_version.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "palf_ffi.h"
#endif

namespace oceanbase
{
namespace logservice
{
namespace unittest
{

using namespace common;
using namespace palf;

// Append a complete group entry to buf, laid out as
//   [LogGroupEntryHeader][LogEntryHeader][data]
// with a valid header checksum, data checksum and accumulated checksum chain, so that the
// palf iterator accepts it.
static int append_group_entry(char *buf,
                              const int64_t buf_len,
                              int64_t &pos,
                              const int64_t log_id,
                              const share::SCN &scn,
                              int64_t &accum_checksum)
{
  int ret = OB_SUCCESS;
  const char data[] = "remote-data-buffer-16";
  const int64_t data_len = sizeof(data);
  LogGroupEntryHeader group_header;
  LogEntryHeader log_entry_header;
  const int64_t group_header_size = group_header.get_serialize_size();
  const int64_t log_header_size = log_entry_header.get_serialize_size();
  const int64_t entry_size = group_header_size + log_header_size + data_len;
  char *entry_begin = buf + pos;
  LogWriteBuf write_buf;
  LSN committed_lsn(0);
  const bool is_raw_write = false;
  const bool is_padding_log = false;
  const int64_t proposal_id = 1;
  int64_t data_checksum = 0;
  int64_t log_header_pos = 0;
  int64_t group_header_pos = 0;
  if (OB_UNLIKELY(buf_len - pos < entry_size)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (FALSE_IT(MEMCPY(entry_begin + group_header_size + log_header_size, data, data_len))) {
  } else if (OB_FAIL(log_entry_header.generate_header(
      entry_begin + group_header_size + log_header_size, data_len, scn))) {
  } else if (OB_FAIL(log_entry_header.serialize(entry_begin + group_header_size,
      log_header_size, log_header_pos))) {
  } else if (OB_FAIL(write_buf.push_back(entry_begin, entry_size))) {
  } else if (OB_FAIL(group_header.generate(is_raw_write, is_padding_log, write_buf,
      log_header_size + data_len, scn, log_id, committed_lsn, proposal_id, data_checksum))) {
  } else {
    accum_checksum = common::ob_crc64(accum_checksum, &data_checksum, sizeof(data_checksum));
    group_header.update_accumulated_checksum(accum_checksum);
    group_header.update_header_checksum();
    if (OB_FAIL(group_header.serialize(entry_begin, group_header_size, group_header_pos))) {
    } else {
      pos += entry_size;
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
// libpalf stores a CRC64-derived checksum in flag[15:0].  Its checksum is calculated over
// the big-endian serialized group header with flag[15:0] cleared, rather than over the
// in-memory C++ struct.  A padding entry keeps this fixture small while still exercising
// header validation and the accumulated-checksum state in GroupEntryIterator.
static int append_libpalf_padding_group_entry(char *buf,
                                              const int64_t buf_len,
                                              int64_t &pos,
                                              const LSN &entry_lsn,
                                              const int64_t log_id,
                                              const share::SCN &scn,
                                              const uint64_t accum_checksum)
{
  int ret = OB_SUCCESS;
  constexpr uint16_t GROUP_ENTRY_MAGIC = 0x4752;
  constexpr uint16_t GROUP_ENTRY_VERSION = 1;
  constexpr uint64_t PADDING_FLAG = 1ULL << 63;
  constexpr uint64_t HEADER_CRC_MASK = 0xffff;
  const uint8_t padding_body = 0;
  const int64_t header_size = sizeof(libpalf::LibPalfLogGroupEntryHeader);
  const int64_t entry_size = header_size + sizeof(padding_body);
  libpalf::LibPalfLogGroupEntryHeader header(
      GROUP_ENTRY_MAGIC,
      GROUP_ENTRY_VERSION,
      sizeof(padding_body),
      1 /*proposal_id*/,
      entry_lsn.val_ + entry_size,
      scn.get_val_for_logservice(),
      accum_checksum,
      log_id,
      PADDING_FLAG);
  libpalf::LibPalfLogGroupEntry entry(header, &padding_body);
  char checksum_buf[sizeof(libpalf::LibPalfLogGroupEntryHeader) + sizeof(padding_body)] = {};
  size_t checksum_pos = 0;
  size_t serialize_pos = 0;
  if (OB_UNLIKELY(buf_len - pos < entry_size)) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_entry_serialize(
      &entry, entry_lsn.val_, checksum_buf, sizeof(checksum_buf), &checksum_pos)))) {
  } else if (OB_UNLIKELY(checksum_pos != static_cast<size_t>(entry_size))) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    entry.header.flag |= common::ob_crc64(checksum_buf, header_size) & HEADER_CRC_MASK;
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_entry_serialize(
        &entry, entry_lsn.val_, buf + pos, buf_len - pos, &serialize_pos)))) {
    } else if (OB_UNLIKELY(serialize_pos != static_cast<size_t>(entry_size))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      pos += entry_size;
    }
  }
  return ret;
}
#endif

// Native PALF rewind; the libpalf format is covered separately below.
static void run_palf_rewind_case(const LSN &buf_start_lsn = LSN(0))
{
  char buf[4096];
  int64_t pos = 0;
  int64_t accum_checksum = 0;
  share::SCN first_scn;
  share::SCN second_scn;
  ASSERT_EQ(OB_SUCCESS, first_scn.convert_for_logservice(1000));
  ASSERT_EQ(OB_SUCCESS, second_scn.convert_for_logservice(2000));
  ASSERT_EQ(OB_SUCCESS, append_group_entry(buf, sizeof(buf), pos, 1, first_scn, accum_checksum));
  const int64_t first_entry_size = pos;
  ASSERT_EQ(OB_SUCCESS, append_group_entry(buf, sizeof(buf), pos, 2, second_scn, accum_checksum));

  RemoteDataBuffer<ipalf::IGroupEntry> data_buffer;
  data_buffer.iter_.set_logservice_mode(false);
  ASSERT_EQ(OB_SUCCESS, data_buffer.set(buf_start_lsn, buf, pos));

  ipalf::IGroupEntry entry(false /*enable_logservice*/);
  LSN lsn;
  const char *entry_buf = NULL;
  int64_t entry_size = 0;
  ASSERT_EQ(OB_SUCCESS, data_buffer.next(entry, lsn, entry_buf, entry_size));
  EXPECT_EQ(buf_start_lsn, lsn);
  EXPECT_EQ(first_scn, entry.get_scn());

  ASSERT_EQ(OB_SUCCESS, data_buffer.rewind(lsn));
  entry.reset();
  ASSERT_EQ(OB_SUCCESS, data_buffer.next(entry, lsn, entry_buf, entry_size));
  EXPECT_EQ(buf_start_lsn, lsn);
  EXPECT_EQ(first_scn, entry.get_scn());

  entry.reset();
  ASSERT_EQ(OB_SUCCESS, data_buffer.next(entry, lsn, entry_buf, entry_size));
  EXPECT_EQ(buf_start_lsn + first_entry_size, lsn);
  EXPECT_EQ(second_scn, entry.get_scn());

  ASSERT_EQ(OB_SUCCESS, data_buffer.rewind(buf_start_lsn + first_entry_size));
  entry.reset();
  ASSERT_EQ(OB_SUCCESS, data_buffer.next(entry, lsn, entry_buf, entry_size));
  EXPECT_EQ(buf_start_lsn + first_entry_size, lsn);
  EXPECT_EQ(second_scn, entry.get_scn());

  entry.reset();
  EXPECT_EQ(OB_ITER_END, data_buffer.next(entry, lsn, entry_buf, entry_size));
}

TEST(TestRemoteDataBuffer, rewind_in_memory_entry)
{
  run_palf_rewind_case();
}

TEST(TestRemoteDataBuffer, rewind_in_memory_entry_with_unaligned_start_lsn)
{
  run_palf_rewind_case(LSN(4096));
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
TEST(TestRemoteDataBuffer, rewind_libpalf_entry_in_middle_of_memory_buffer)
{
  char buf[4096];
  int64_t pos = 0;
  const LSN start_lsn(4096);
  share::SCN first_scn;
  share::SCN second_scn;
  share::SCN third_scn;
  ASSERT_EQ(OB_SUCCESS, first_scn.convert_for_logservice(1000));
  ASSERT_EQ(OB_SUCCESS, second_scn.convert_for_logservice(2000));
  ASSERT_EQ(OB_SUCCESS, third_scn.convert_for_logservice(3000));

  // The first entry is the checksum-chain anchor of this in-memory range.  Padding entries
  // have data CRC 0, so the following accumulated checksums use CRC64 over eight zero bytes.
  uint64_t accum_checksum = 1;
  ASSERT_EQ(OB_SUCCESS, append_libpalf_padding_group_entry(
      buf, sizeof(buf), pos, start_lsn, 1, first_scn, accum_checksum));
  const int64_t first_entry_size = pos;
  const uint64_t padding_data_crc = 0;
  accum_checksum = common::ob_crc64(
      accum_checksum, &padding_data_crc, sizeof(padding_data_crc));
  ASSERT_EQ(OB_SUCCESS, append_libpalf_padding_group_entry(
      buf, sizeof(buf), pos, start_lsn + pos, 2, second_scn, accum_checksum));
  const int64_t second_entry_size = pos - first_entry_size;
  accum_checksum = common::ob_crc64(
      accum_checksum, &padding_data_crc, sizeof(padding_data_crc));
  ASSERT_EQ(OB_SUCCESS, append_libpalf_padding_group_entry(
      buf, sizeof(buf), pos, start_lsn + pos, 3, third_scn, accum_checksum));

  RemoteDataBuffer<ipalf::IGroupEntry> data_buffer;
  data_buffer.iter_.set_logservice_mode(true);
  ASSERT_EQ(OB_SUCCESS, data_buffer.set(start_lsn, buf, pos));

  ipalf::IGroupEntry entry(true /*enable_logservice*/);
  LSN lsn;
  const char *entry_buf = NULL;
  int64_t entry_size = 0;
  ASSERT_EQ(OB_SUCCESS, data_buffer.next(entry, lsn, entry_buf, entry_size));
  EXPECT_EQ(start_lsn, lsn);
  EXPECT_EQ(first_scn, entry.get_scn());
  EXPECT_EQ(first_entry_size, entry_size);

  const LSN second_lsn = start_lsn + first_entry_size;
  ASSERT_EQ(OB_SUCCESS, data_buffer.next(entry, lsn, entry_buf, entry_size));
  EXPECT_EQ(second_lsn, lsn);
  EXPECT_EQ(second_scn, entry.get_scn());
  EXPECT_EQ(second_entry_size, entry_size);

  // Resetting starts a new accumulated-checksum chain at the second entry.  The second
  // entry must still pass its own integrity check, and the third entry must chain from it.
  ASSERT_EQ(OB_SUCCESS, data_buffer.rewind(second_lsn));
  ASSERT_EQ(OB_SUCCESS, data_buffer.next(entry, lsn, entry_buf, entry_size));
  EXPECT_EQ(second_lsn, lsn);
  EXPECT_EQ(second_scn, entry.get_scn());
  ASSERT_EQ(OB_SUCCESS, data_buffer.next(entry, lsn, entry_buf, entry_size));
  EXPECT_EQ(second_lsn + second_entry_size, lsn);
  EXPECT_EQ(third_scn, entry.get_scn());
  EXPECT_EQ(OB_ITER_END, data_buffer.next(entry, lsn, entry_buf, entry_size));
}
#endif

TEST(TestRemoteDataBuffer, rewind_invalid_argument)
{
  char buf[4096];
  int64_t pos = 0;
  int64_t accum_checksum = 0;
  share::SCN scn;
  ASSERT_EQ(OB_SUCCESS, scn.convert_for_logservice(1000));
  ASSERT_EQ(OB_SUCCESS, append_group_entry(buf, sizeof(buf), pos, 1, scn, accum_checksum));

  const LSN start_lsn(1024);
  RemoteDataBuffer<ipalf::IGroupEntry> data_buffer;
  data_buffer.iter_.set_logservice_mode(false);
  EXPECT_EQ(OB_INVALID_ARGUMENT, data_buffer.rewind(start_lsn));

  ASSERT_EQ(OB_SUCCESS, data_buffer.set(start_lsn, buf, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, data_buffer.rewind(LSN()));
  EXPECT_EQ(OB_INVALID_ARGUMENT, data_buffer.rewind(LSN(start_lsn.val_ - 1)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, data_buffer.rewind(LSN(start_lsn.val_ + pos)));
  EXPECT_EQ(OB_SUCCESS, data_buffer.rewind(start_lsn));
}

TEST(TestRemoteFetchStat, use_stage_specific_mean_denominators)
{
  ObRemoteFetchTaskStat fetch_stat;
  fetch_stat.fetch_log_size_ = 64;
  fetch_stat.gen_ts_ = 100;
  fetch_stat.start_fetch_ts_ = 110;
  fetch_stat.finish_fetch_ts_ = 130;

  ObRemoteFetchStat stat;
  stat.update_fetch_stat(fetch_stat);
  stat.update_fetch_stat(fetch_stat);
  stat.update_submit_stat(true /*is_first_submit_round*/, 30, 10);
  stat.update_submit_stat(false /*is_first_submit_round*/, 0, 20);
  stat.update_submit_stat(true /*is_first_submit_round*/, 50, 30);

  const ObRemoteFetchStat mean_stat = stat.calc_mean();
  EXPECT_EQ(1, mean_stat.fetch_task_count_);
  EXPECT_EQ(64, mean_stat.fetch_log_size_);
  EXPECT_EQ(10, mean_stat.gen_to_fetch_time_);
  EXPECT_EQ(20, mean_stat.fetch_log_time_);
  EXPECT_EQ(1, mean_stat.submit_task_count_);
  EXPECT_EQ(1, mean_stat.submit_round_count_);
  EXPECT_EQ(40, mean_stat.fetch_to_submit_time_);
  EXPECT_EQ(20, mean_stat.submit_log_time_);

  // A rate-limited task can continue submitting in a later reporting window where no new
  // pre_read occurs. Submit means must remain visible when the physical-fetch count is zero.
  ObRemoteFetchStat submit_only_stat;
  submit_only_stat.update_submit_stat(true /*is_first_submit_round*/, 30, 10);
  submit_only_stat.update_submit_stat(false /*is_first_submit_round*/, 0, 20);
  const ObRemoteFetchStat submit_only_mean = submit_only_stat.calc_mean();
  EXPECT_EQ(30, submit_only_mean.fetch_to_submit_time_);
  EXPECT_EQ(15, submit_only_mean.submit_log_time_);

  // A wall-clock rollback must not hide the physical read. Only unreliable durations
  // are dropped; task count and bytes remain observable.
  ObRemoteFetchStat rollback_stat;
  fetch_stat.gen_ts_ = 200;
  fetch_stat.start_fetch_ts_ = 190;
  fetch_stat.finish_fetch_ts_ = 180;
  rollback_stat.update_fetch_stat(fetch_stat);
  EXPECT_EQ(1, rollback_stat.fetch_task_count_);
  EXPECT_EQ(64, rollback_stat.fetch_log_size_);
  EXPECT_EQ(0, rollback_stat.gen_to_fetch_time_);
  EXPECT_EQ(0, rollback_stat.fetch_log_time_);
}

} // namespace unittest
} // namespace logservice
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  oceanbase::common::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_CURRENT_VERSION);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
