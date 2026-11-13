/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <functional>
#include <stdexcept>
#include <gtest/gtest.h>
#include "lib/worker.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#define private public
#include "rpc/frame/ob_req_queue_thread.h"
#undef private

namespace oceanbase
{
namespace rpc
{
namespace frame
{
using namespace common;

class ErrsimTestReqHandler : public ObiReqQHandler
{
public:
  int onThreadCreated(obsys::CThread *) override { return OB_SUCCESS; }
  int onThreadDestroy(obsys::CThread *) override { return OB_SUCCESS; }
  bool handlePacketQueue(ObRequest *, void *) override
  {
    ++calls_;
    observed_type_ = lib::Worker::get_module_type().type_;
    if (callback_) {
      callback_();
    }
    return true;
  }

  int64_t calls_ = 0;
  ObErrsimModuleType::TYPE observed_type_ = ObErrsimModuleType::ERRSIM_MODULE_MAX;
  std::function<void()> callback_;
};

class TestReqErrsimModule : public ::testing::Test
{
public:
  void SetUp() override
  {
    saved_type_ = lib::Worker::get_module_type();
    lib::Worker::set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_NONE));
    ASSERT_EQ(OB_SUCCESS, queue_.init());
    queue_.set_qhandler(&handler_);
    ObAddr host;
    ASSERT_TRUE(host.set_ip_addr("127.0.0.1", 2882));
    queue_.set_host(host);
  }

  void TearDown() override
  {
    lib::Worker::set_module_type(saved_type_);
    ObCurTraceId::reset();
  }

protected:
  ObReqQueue queue_{16};
  ErrsimTestReqHandler handler_;
  ObErrsimModuleType saved_type_;
};

TEST_F(TestReqErrsimModule, rpc_then_mysql_on_same_thread)
{
  obrpc::ObRpcPacket packet;
  packet.set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
  ObRequest rpc_request(ObRequest::OB_RPC);
  rpc_request.set_packet(&packet);
  ASSERT_EQ(OB_SUCCESS, queue_.process_task(&rpc_request));
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, handler_.observed_type_);
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, lib::Worker::get_module_type().type_);

  obmysql::ObMySQLRawPacket mysql_packet;
  ObRequest mysql_request(ObRequest::OB_MYSQL);
  mysql_request.set_packet(&mysql_packet);
  ASSERT_EQ(OB_SUCCESS, queue_.process_task(&mysql_request));
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, handler_.observed_type_);
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, lib::Worker::get_module_type().type_);
  ASSERT_EQ(2, handler_.calls_);
}

TEST_F(TestReqErrsimModule, outtable_lob_rpc_restores_calling_context)
{
  lib::Worker::set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
  obrpc::ObRpcPacket packet;
  packet.set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_OUTTABLE_LOB));
  ObRequest request(ObRequest::OB_RPC);
  request.set_packet(&packet);
  ASSERT_EQ(OB_SUCCESS, queue_.process_task(&request));
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_OUTTABLE_LOB, handler_.observed_type_);
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, lib::Worker::get_module_type().type_);
}

TEST_F(TestReqErrsimModule, mysql_isolates_and_restores_calling_context)
{
  lib::Worker::set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
  obmysql::ObMySQLRawPacket packet;
  ObRequest request(ObRequest::OB_MYSQL);
  request.set_packet(&packet);
  ASSERT_EQ(OB_SUCCESS, queue_.process_task(&request));
  ASSERT_EQ(1, handler_.calls_);
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, handler_.observed_type_);
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, lib::Worker::get_module_type().type_);
}

TEST_F(TestReqErrsimModule, nested_request_restores_rpc_context)
{
  ErrsimTestReqHandler nested_handler;
  ObReqQueue nested_queue(16);
  ASSERT_EQ(OB_SUCCESS, nested_queue.init());
  nested_queue.set_qhandler(&nested_handler);
  handler_.callback_ = [&]() {
    ObRequest nested_request(ObRequest::OB_TASK);
    EXPECT_EQ(OB_SUCCESS, nested_queue.process_task(&nested_request));
    EXPECT_EQ(1, nested_handler.calls_);
    EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, nested_handler.observed_type_);
    EXPECT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, lib::Worker::get_module_type().type_);
  };
  obrpc::ObRpcPacket packet;
  packet.set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
  ObRequest request(ObRequest::OB_RPC);
  request.set_packet(&packet);
  ASSERT_EQ(OB_SUCCESS, queue_.process_task(&request));
  ASSERT_EQ(1, handler_.calls_);
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_NONE, lib::Worker::get_module_type().type_);
}

TEST_F(TestReqErrsimModule, exception_restores_calling_context)
{
  lib::Worker::set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION));
  handler_.callback_ = []() {
    lib::Worker::set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_ALL));
    throw std::runtime_error("request handler failed");
  };
  obrpc::ObRpcPacket packet;
  packet.set_module_type(ObErrsimModuleType(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER));
  ObRequest request(ObRequest::OB_RPC);
  request.set_packet(&packet);
  EXPECT_THROW(queue_.process_task(&request), std::runtime_error);
  ASSERT_EQ(1, handler_.calls_);
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_TRANSFER, handler_.observed_type_);
  ASSERT_EQ(ObErrsimModuleType::ERRSIM_MODULE_MIGRATION, lib::Worker::get_module_type().type_);
}

} // namespace frame
} // namespace rpc
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("ERROR");
  return RUN_ALL_TESTS();
}
