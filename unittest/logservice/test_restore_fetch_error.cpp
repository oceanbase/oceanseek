/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#define private public
#define protected public
#include "logservice/restoreservice/ob_log_restore_handler.h"
#undef protected
#undef private

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::logservice;
using namespace oceanbase::share;
using namespace oceanbase::palf;
using namespace oceanbase::ipalf;

namespace oceanbase
{
namespace unittest
{
class MockIPalfHandle : public ipalf::IPalfHandle
{
public:
  MockIPalfHandle()
    : is_valid_(true),
      end_scn_(),
      end_lsn_(),
      palf_id_(1)
  {
    end_scn_.set_min();
    end_lsn_.reset();
  }
  virtual ~MockIPalfHandle() {}

  void set_end_scn(const share::SCN &scn) { end_scn_ = scn; }
  void set_end_lsn(const palf::LSN &lsn) { end_lsn_ = lsn; }
  void set_valid(bool valid) { is_valid_ = valid; }

  virtual bool is_valid() const override { return is_valid_; }
  virtual bool operator==(const IPalfHandle &rhs) const override { return false; }

  virtual int append(const PalfAppendOptions &opts,
                     const void *buffer,
                     const int64_t nbytes,
                     const share::SCN &ref_scn,
                     palf::LSN &lsn,
                     share::SCN &scn) override
  { return OB_NOT_SUPPORTED; }

  virtual int raw_write(const PalfAppendOptions &opts,
                        const palf::LSN &lsn,
                        const void *buffer,
                        const int64_t nbytes) override
  { return OB_NOT_SUPPORTED; }

  virtual int raw_read(const palf::LSN &lsn,
                       void *buffer,
                       const int64_t nbytes,
                       int64_t &read_size,
                       palf::LogIOContext &io_ctx) override
  { return OB_NOT_SUPPORTED; }

  virtual int seek(const palf::LSN &lsn, palf::PalfBufferIterator &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const palf::LSN &lsn, palf::PalfGroupBufferIterator &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const palf::LSN &lsn, ipalf::IPalfIterator<ILogEntry> &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const palf::LSN &lsn, ipalf::IPalfIterator<IGroupEntry> &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const share::SCN &scn, palf::PalfGroupBufferIterator &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const share::SCN &scn, palf::PalfBufferIterator &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const share::SCN &scn, ipalf::IPalfIterator<ILogEntry> &iter) override
  { return OB_NOT_SUPPORTED; }
  virtual int seek(const share::SCN &scn, ipalf::IPalfIterator<IGroupEntry> &iter) override
  { return OB_NOT_SUPPORTED; }

  virtual int locate_by_scn_coarsely(const share::SCN &scn, palf::LSN &result_lsn) override
  { return OB_NOT_SUPPORTED; }
  virtual int locate_by_lsn_coarsely(const palf::LSN &lsn, share::SCN &result_scn) override
  { return OB_NOT_SUPPORTED; }

  virtual int enable_sync() override { return OB_SUCCESS; }
  virtual int disable_sync() override { return OB_SUCCESS; }
  virtual bool is_sync_enabled() const override { return true; }
  virtual int advance_base_lsn(const palf::LSN &lsn) override { return OB_SUCCESS; }
  virtual int flashback(const int64_t mode_version, const share::SCN &flashback_scn,
                        const int64_t timeout_us) override
  { return OB_NOT_SUPPORTED; }

  virtual int get_begin_lsn(palf::LSN &lsn) const override
  { lsn.reset(); return OB_SUCCESS; }
  virtual int get_begin_scn(share::SCN &scn) const override
  { scn.set_min(); return OB_SUCCESS; }
  virtual int get_base_lsn(palf::LSN &lsn) const override
  { lsn.reset(); return OB_SUCCESS; }
  virtual int get_base_info(const palf::LSN &lsn, palf::PalfBaseInfo &palf_base_info) override
  { return OB_NOT_SUPPORTED; }

  virtual int get_end_lsn(palf::LSN &lsn) const override
  {
    lsn = end_lsn_;
    return OB_SUCCESS;
  }
  virtual int get_end_scn(share::SCN &scn) const override
  {
    scn = end_scn_;
    return OB_SUCCESS;
  }
  virtual int get_max_lsn(palf::LSN &lsn) const override
  { lsn = end_lsn_; return OB_SUCCESS; }
  virtual int get_max_scn(share::SCN &scn) const override
  { scn = end_scn_; return OB_SUCCESS; }

  virtual int get_role(common::ObRole &role, int64_t &proposal_id,
                       bool &is_pending_state) const override
  {
    role = common::LEADER;
    proposal_id = 1;
    is_pending_state = false;
    return OB_SUCCESS;
  }
  virtual int get_palf_id(int64_t &palf_id) const override
  { palf_id = palf_id_; return OB_SUCCESS; }
  virtual int get_palf_epoch(int64_t &palf_epoch) const override
  { palf_epoch = 1; return OB_SUCCESS; }
  virtual int get_election_leader(common::ObAddr &addr) const override
  { return OB_NOT_SUPPORTED; }
  virtual int advance_election_epoch_and_downgrade_priority(
      const int64_t proposal_id,
      const int64_t downgrade_priority_time_us,
      const char *reason) override
  { return OB_NOT_SUPPORTED; }
  virtual int change_leader_to(const common::ObAddr &dst_addr) override
  { return OB_NOT_SUPPORTED; }
  virtual int change_access_mode(const int64_t proposal_id,
                                 const int64_t mode_version,
                                 const ipalf::AccessMode &access_mode,
                                 const share::SCN &ref_scn) override
  { return OB_NOT_SUPPORTED; }
  virtual int get_access_mode(int64_t &mode_version, ipalf::AccessMode &access_mode) const override
  { return OB_NOT_SUPPORTED; }
  virtual int get_access_mode(ipalf::AccessMode &access_mode) const override
  { return OB_NOT_SUPPORTED; }
  virtual int get_access_mode_version(int64_t &mode_version) const override
  { return OB_NOT_SUPPORTED; }
  virtual int get_access_mode_ref_scn(int64_t &mode_version,
                                      ipalf::AccessMode &access_mode,
                                      share::SCN &ref_scn) const override
  { return OB_NOT_SUPPORTED; }
  virtual int register_file_size_cb(palf::PalfFSCb *fs_cb) override
  { return OB_SUCCESS; }
  virtual int unregister_file_size_cb() override
  { return OB_SUCCESS; }
  virtual int register_role_change_cb(palf::PalfRoleChangeCb *rc_cb) override
  { return OB_SUCCESS; }
  virtual int unregister_role_change_cb() override
  { return OB_SUCCESS; }
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  virtual int register_refresh_priority_cb() override
  { return OB_SUCCESS; }
  virtual int unregister_refresh_priority_cb() override
  { return OB_SUCCESS; }
  virtual int set_allow_election_without_memlist(const bool allow) override
  { return OB_SUCCESS; }
#endif
  virtual int set_election_priority(palf::election::ElectionPriority *priority) override
  { return OB_SUCCESS; }
  virtual int reset_election_priority() override
  { return OB_SUCCESS; }
  virtual int set_location_cache_cb(palf::PalfLocationCacheCb *lc_cb) override
  { return OB_SUCCESS; }
  virtual int reset_location_cache_cb() override
  { return OB_SUCCESS; }
  virtual int set_locality_cb(palf::PalfLocalityInfoCb *locality_cb) override
  { return OB_SUCCESS; }
  virtual int reset_locality_cb() override
  { return OB_SUCCESS; }
  virtual int set_reconfig_checker_cb(palf::PalfReconfigCheckerCb *reconfig_checker) override
  { return OB_SUCCESS; }
  virtual int reset_reconfig_checker_cb() override
  { return OB_SUCCESS; }
  virtual int stat(palf::PalfStat &palf_stat) const override
  { return OB_NOT_SUPPORTED; }
  virtual int diagnose(palf::PalfDiagnoseInfo &diagnose_info) const override
  { return OB_NOT_SUPPORTED; }
#ifdef OB_BUILD_ARBITRATION
  virtual int get_arbitration_member(common::ObMember &arb_member) const override
  { return OB_NOT_SUPPORTED; }
#endif

private:
  bool is_valid_;
  share::SCN end_scn_;
  palf::LSN end_lsn_;
  int64_t palf_id_;
};

class TestRestoreFetchError : public ::testing::Test
{
public:
  void SetUp() override
  {
    handler_.is_inited_ = true;
    handler_.is_in_stop_state_ = false;
    handler_.id_ = 1003;
    handler_.role_ = LEADER;
    handler_.proposal_id_ = 2;
    handler_.context_.issue_version_ = 10;
    handler_.palf_handle_ = &palf_;
    handler_.parent_ = &source_;
    source_.upper_limit_scn_.convert_for_logservice(200);
    SCN end_scn;
    end_scn.convert_for_logservice(100);
    palf_.set_end_scn(end_scn);
    palf_.set_end_lsn(LSN(1000));
  }

  void TearDown() override
  {
    // The fixture owns these objects; do not let the handler free them.
    handler_.parent_ = nullptr;
    handler_.palf_handle_ = nullptr;
  }

  void report(const int64_t proposal_id = 2,
              const int64_t version = 10,
              const LSN &lsn = LSN(1000))
  {
    handler_.mark_fetch_error(trace_id_, proposal_id, version,
                             OB_ARCHIVE_ROUND_NOT_CONTINUOUS, lsn);
  }

  MockIPalfHandle palf_;
  ObRemoteLocationParent source_{ObLSID(1003)};
  ObLogRestoreHandler handler_;
  ObTaskId trace_id_;
};

TEST_F(TestRestoreFetchError, stale_error_after_role_change)
{
  // Reproduce the old task reporting exactly at the new leader's continuation
  // LSN after its source has been removed; the LSN-only guard cannot reject it.
  handler_.parent_ = nullptr;
  handler_.switch_role(FOLLOWER, 2);
  handler_.switch_role(LEADER, 3);
  report(2, 10, LSN(1000));
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);

  // Reestablishing the source must not revive the old proposal/version. A
  // current task exactly at the continuation LSN is still a fatal error.
  handler_.parent_ = &source_;
  handler_.context_.issue_version_ = 11;
  report(2, 10, LSN(1000));
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
  report(3, 11, LSN(1001));
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
  report(3, 11, LSN(1000));
  EXPECT_EQ(OB_ARCHIVE_ROUND_NOT_CONTINUOUS, handler_.context_.error_context_.ret_code_);
}

TEST_F(TestRestoreFetchError, stale_proposal_with_current_source)
{
  handler_.proposal_id_ = 3;
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
  report(3, 10);
  EXPECT_EQ(OB_ARCHIVE_ROUND_NOT_CONTINUOUS, handler_.context_.error_context_.ret_code_);
}

TEST_F(TestRestoreFetchError, missing_or_service_source)
{
  handler_.parent_ = nullptr;
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
  ObRemoteSerivceParent service_source(ObLSID(1003));
  handler_.parent_ = &service_source;
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
  handler_.parent_ = &source_;
}

TEST_F(TestRestoreFetchError, stale_source_version)
{
  handler_.context_.issue_version_ = 11;
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
  report(2, 11);
  EXPECT_EQ(OB_ARCHIVE_ROUND_NOT_CONTINUOUS, handler_.context_.error_context_.ret_code_);
}

TEST_F(TestRestoreFetchError, uninitialized_handler_ignores_fetch_error)
{
  handler_.is_inited_ = false;
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
}

TEST_F(TestRestoreFetchError, restore_already_reached_target)
{
  SCN end_scn;
  end_scn.convert_for_logservice(200);
  palf_.set_end_scn(end_scn);
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
}

TEST_F(TestRestoreFetchError, source_already_reached_target)
{
  source_.to_end_ = true;
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
}

TEST_F(TestRestoreFetchError, valid_gap_is_reported)
{
  report();
  EXPECT_EQ(OB_ARCHIVE_ROUND_NOT_CONTINUOUS, handler_.context_.error_context_.ret_code_);
  EXPECT_EQ(LSN(1000), handler_.context_.error_context_.err_lsn_);
  EXPECT_EQ(ObLogRestoreErrorContext::ErrorType::FETCH_LOG, handler_.context_.error_context_.error_type_);
}

TEST_F(TestRestoreFetchError, recycled_archive_remains_a_restore_failure)
{
  handler_.mark_fetch_error(trace_id_, 2, 10, OB_ARCHIVE_LOG_RECYCLED, LSN(1000));
  EXPECT_EQ(OB_ARCHIVE_LOG_RECYCLED, handler_.context_.error_context_.ret_code_);
  EXPECT_EQ(ObLogRestoreErrorContext::ErrorType::FETCH_LOG, handler_.context_.error_context_.error_type_);
  EXPECT_EQ(LSN(1000), handler_.context_.error_context_.err_lsn_);
}

TEST_F(TestRestoreFetchError, follower_and_stopped_handler_ignore_error)
{
  handler_.role_ = FOLLOWER;
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
  handler_.role_ = LEADER;
  handler_.is_in_stop_state_ = true;
  report();
  EXPECT_EQ(OB_SUCCESS, handler_.context_.error_context_.ret_code_);
}

TEST_F(TestRestoreFetchError, generic_error_reporting_is_preserved)
{
  handler_.parent_ = nullptr;
  handler_.mark_error(trace_id_, OB_CHECKSUM_ERROR, LSN(1000),
      ObLogRestoreErrorContext::ErrorType::SUBMIT_LOG);
  EXPECT_EQ(OB_CHECKSUM_ERROR, handler_.context_.error_context_.ret_code_);
  EXPECT_EQ(LSN(1000), handler_.context_.error_context_.err_lsn_);
  EXPECT_EQ(ObLogRestoreErrorContext::ErrorType::SUBMIT_LOG,
      handler_.context_.error_context_.error_type_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_restore_fetch_error.log", true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
