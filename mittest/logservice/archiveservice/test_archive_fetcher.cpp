/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define private public
#include "logservice/archiveservice/ob_archive_fetcher.h"
#include "logservice/archiveservice/ob_archive_round_mgr.h"
#include "logservice/archiveservice/ob_ls_mgr.h"
#undef private
#include <gtest/gtest.h>

namespace oceanbase
{
namespace unittest
{
using namespace archive;
using namespace share;

class TestArchiveFetcher : public ::testing::Test
{
public:
  TestArchiveFetcher() : key_(1, 1002, 1), id_(1001), ls_task_(nullptr) {}

  void SetUp() override
  {
    round_mgr_.key_ = key_;
    round_mgr_.update_log_archive_status(ObArchiveRoundState::Status::DOING);
    ASSERT_EQ(OB_SUCCESS, ls_mgr_.ls_map_.init("ArchiveLSMap"));
    ls_mgr_.inited_ = true;
    ls_mgr_.round_mgr_ = &round_mgr_;
    ASSERT_EQ(OB_SUCCESS, ls_mgr_.ls_map_.alloc_value(ls_task_));
    ASSERT_NE(nullptr, ls_task_);
    ls_task_->station_ = ArchiveWorkStation(key_, ObArchiveLease(1, 0, 0));
    ASSERT_EQ(OB_SUCCESS, ls_mgr_.ls_map_.insert_and_get(id_, ls_task_));
    fetcher_.ls_mgr_ = &ls_mgr_;
    fetcher_.round_mgr_ = &round_mgr_;
  }

  void TearDown() override
  {
    if (nullptr != ls_task_) {
      ls_mgr_.ls_map_.revert(ls_task_);
      ls_task_ = nullptr;
    }
    ls_mgr_.ls_map_.reset();
  }

protected:
  ArchiveKey key_;
  ObLSID id_;
  ObArchiveRoundMgr round_mgr_;
  ObArchiveLSMgr ls_mgr_;
  ObArchiveFetcher fetcher_;
  ObLSArchiveTask *ls_task_;
};

TEST_F(TestArchiveFetcher, retryable_errors_do_not_interrupt_round)
{
  const int errors[] = {OB_LOGSERVICE_RPC_ERROR, OB_EAGAIN, OB_ALLOCATE_MEMORY_FAILED};
  for (const int error : errors) {
    SCOPED_TRACE(error);
    ls_task_->dest_.has_encount_error_ = false;
    round_mgr_.update_log_archive_status(ObArchiveRoundState::Status::DOING);
    EXPECT_TRUE(fetcher_.is_retry_ret_(error));
    // Exercise the error handler as well as task retention: both must agree.
    fetcher_.handle_log_fetch_ret_(id_, key_, error);
    EXPECT_FALSE(ls_task_->dest_.has_encount_error_);
    EXPECT_TRUE(round_mgr_.is_in_archive_status(key_));
  }
}

TEST_F(TestArchiveFetcher, permanent_errors_still_interrupt_round)
{
  const int errors[] = {OB_INVALID_DATA, OB_CHECKSUM_ERROR, OB_ERR_OUT_OF_LOWER_BOUND};
  for (const int error : errors) {
    SCOPED_TRACE(error);
    ls_task_->dest_.has_encount_error_ = false;
    round_mgr_.update_log_archive_status(ObArchiveRoundState::Status::DOING);
    EXPECT_FALSE(fetcher_.is_retry_ret_(error));
    fetcher_.handle_log_fetch_ret_(id_, key_, error);
    EXPECT_TRUE(ls_task_->dest_.has_encount_error_);
    ArchiveKey key;
    ObArchiveRoundState state;
    round_mgr_.get_archive_round_info(key, state);
    EXPECT_EQ(key_, key);
    EXPECT_TRUE(state.is_interrupted());
  }
}

TEST_F(TestArchiveFetcher, stale_tasks_do_not_interrupt_round)
{
  const int errors[] = {OB_LOG_ARCHIVE_LEADER_CHANGED, OB_ENTRY_NOT_EXIST, OB_LS_NOT_EXIST};
  for (const int error : errors) {
    SCOPED_TRACE(error);
    EXPECT_FALSE(fetcher_.is_retry_ret_(error));
    fetcher_.handle_log_fetch_ret_(id_, key_, error);
    EXPECT_FALSE(ls_task_->dest_.has_encount_error_);
    EXPECT_TRUE(round_mgr_.is_in_archive_status(key_));
  }
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_archive_fetcher.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
