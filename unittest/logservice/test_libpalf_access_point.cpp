/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <unistd.h>
#include <atomic>
#include <memory>
#include <string>
#include <thread>
#include "lib/checksum/ob_crc64.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/task/ob_timer.h"
#include "common/ob_clock_generator.h"
#include "share/ob_master_key_getter.h"
#define private public
#include "logservice/libpalf/libpalf_env_ffi_instance.h"
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace libpalf;
using Buffer = LibPalfEnvFFIInstance::LogserviceAccessPointBuf;

class TestLibPalfAccessPoint : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, share::ObMasterKeyGetter::instance().init(nullptr));
    ASSERT_EQ(OB_SUCCESS, share::ObMasterKeyGetter::instance().set_root_key(
        OB_SYS_TENANT_ID, obrpc::RootKeyType::NORMAL, ObString::make_string("0123456789abcdef"),
        false /* need_dump */, false /* need_verify */));
  }
  static void TearDownTestCase()
  {
    share::ObMasterKeyGetter::instance().destroy();
  }
};

static std::string make_url(const std::string &endpoint, const char *mode = "path_style")
{
  return "s3://test-bucket/log?host=" + endpoint +
      "&access_id=test-ak&access_key=test-secret&s3_region=us-east-1&logservice_cluster_id=42&addressing_model=" + mode;
}

static int encrypt_url(const std::string &url, Buffer &encrypted)
{
  int64_t length = 0;
  return LibPalfEnvFFIInstance::encrypt_logservice_access_point(
      url.c_str(), url.size(), encrypted.ptr(), encrypted.capacity(), length);
}

TEST_F(TestLibPalfAccessPoint, preserves_url_parameters_through_encryption)
{
  auto encrypted = std::make_unique<Buffer>();
  auto decrypted = std::make_unique<Buffer>();
  for (const char *mode : {"path_style", "virtual_hosted_style"}) {
    ASSERT_EQ(OB_SUCCESS, encrypt_url(make_url("http://oss.example.test:8082", mode), *encrypted));
    EXPECT_EQ(nullptr, strstr(encrypted->ptr(), "test-secret"));
    int64_t length = 0;
    ASSERT_EQ(OB_SUCCESS, LibPalfEnvFFIInstance::decrypt_logservice_access_point(
        encrypted->ptr(), encrypted->size(), decrypted->ptr(), decrypted->capacity(), length));
    for (const char *field : {"host=http://oss.example.test:8082", "access_id=test-ak",
        "access_key=test-secret", "s3_region=us-east-1", "logservice_cluster_id=42"}) {
      EXPECT_NE(nullptr, strstr(decrypted->ptr(), field));
    }
    EXPECT_NE(nullptr, strstr(decrypted->ptr(), (std::string("addressing_model=") + mode).c_str()));
    EXPECT_EQ(length, decrypted->size() + 1);
    // SQL resolution converts the parameter in place.
    ASSERT_EQ(OB_SUCCESS, decrypted->assign(make_url("http://127.0.0.1:8082", mode).c_str()));
    ASSERT_EQ(OB_SUCCESS, LibPalfEnvFFIInstance::encrypt_logservice_access_point(
        decrypted->ptr(), decrypted->size(), decrypted->ptr(), decrypted->capacity(), length));
    ASSERT_EQ(OB_SUCCESS, LibPalfEnvFFIInstance::decrypt_logservice_access_point(
        decrypted->ptr(), decrypted->size(), decrypted->ptr(), decrypted->capacity(), length));
    EXPECT_NE(nullptr, strstr(decrypted->ptr(), "access_key=test-secret"));
    EXPECT_NE(nullptr, strstr(decrypted->ptr(), (std::string("addressing_model=") + mode).c_str()));
  }
}

TEST_F(TestLibPalfAccessPoint, rejects_invalid_cluster_parameter_and_addressing_mode)
{
  auto encrypted = std::make_unique<Buffer>();
  const std::string url = make_url("http://127.0.0.1:8082");
  EXPECT_NE(OB_SUCCESS, encrypt_url(url + "&logservice_cluster_id=43", *encrypted));
  EXPECT_NE(OB_SUCCESS, encrypt_url(make_url("http://127.0.0.1:8082", "invalid"), *encrypted));
  std::string missing = url;
  missing.replace(missing.find("logservice_cluster_id="), strlen("logservice_cluster_id="), "other_cluster_id=");
  EXPECT_NE(OB_SUCCESS, encrypt_url(missing, *encrypted));
}

// Serve a checksummed ACCESS_POINT through real HTTP and the linked libpalf FFI.
// No write operation is accepted, matching the client's discovery permissions.
class AccessPointServer
{
public:
  AccessPointServer() : socket_(::socket(AF_INET, SOCK_STREAM, 0)), stop_(false), healthy_(true), invalid_request_(false), requests_(0)
  {
    sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    EXPECT_EQ(0, ::bind(socket_, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)));
    EXPECT_EQ(0, ::listen(socket_, 16));
    socklen_t size = sizeof(addr);
    EXPECT_EQ(0, ::getsockname(socket_, reinterpret_cast<sockaddr *>(&addr), &size));
    port_ = ntohs(addr.sin_port);
    worker_ = std::thread([this]() { run(); });
  }
  ~AccessPointServer()
  {
    stop_ = true;
    ::shutdown(socket_, SHUT_RDWR);
    ::close(socket_);
    worker_.join();
  }
  std::string endpoint(bool domain) const
  {
    return std::string(domain ? "http://localhost:" : "http://127.0.0.1:") + std::to_string(port_);
  }
  void set_healthy(bool healthy) { healthy_ = healthy; }
  bool invalid_request() const { return invalid_request_; }
  int requests() const { return requests_; }
private:
  void run()
  {
    const uint32_t version = htonl(1);
    const uint64_t cluster = htobe64(42);
    const char *rpc = "127.0.0.1:3882";
    const char *http = "127.0.0.1:3883";
    const char *release = "1.3.4";
    uint64_t checksum = ob_crc64(&version, sizeof(version));
    checksum = ob_crc64(checksum, rpc, strlen(rpc));
    checksum = ob_crc64(checksum, http, strlen(http));
    checksum = ob_crc64(checksum, &cluster, sizeof(cluster));
    checksum = ob_crc64(checksum, release, strlen(release));
    const std::string body = "{\"version\":1,\"checksum\":" + std::to_string(checksum) +
        ",\"lm_leader\":\"127.0.0.1:3882\",\"lm_leader_http\":\"127.0.0.1:3883\","
        "\"cluster_id\":42,\"min_oblogservice_version\":\"1.3.4\"}";
    while (!stop_) {
      const int client = ::accept(socket_, nullptr, nullptr);
      if (client < 0) { continue; }
      timeval timeout = {2, 0};
      ::setsockopt(client, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
      std::string request;
      char buf[4096];
      while (request.find("\r\n\r\n") == std::string::npos) {
        const ssize_t size = ::recv(client, buf, sizeof(buf), 0);
        if (size <= 0) { break; }
        request.append(buf, size);
      }
      ++requests_;
      if (request.find("GET /test-bucket/log/42/ACCESS_POINT ") != 0) { invalid_request_ = true; }
      const std::string payload = healthy_ ? body : "denied";
      const std::string response = std::string(healthy_ ? "HTTP/1.1 200 OK\r\n" : "HTTP/1.1 403 Forbidden\r\n") +
          "Content-Length: " + std::to_string(payload.size()) + "\r\nConnection: close\r\n\r\n" + payload;
      ::send(client, response.data(), response.size(), MSG_NOSIGNAL);
      ::close(client);
    }
  }
  int socket_;
  uint16_t port_;
  std::atomic<bool> stop_;
  std::atomic<bool> healthy_;
  std::atomic<bool> invalid_request_;
  std::atomic<int> requests_;
  std::thread worker_;
};

TEST_F(TestLibPalfAccessPoint, synchronous_update_preserves_state_and_retries_on_reload)
{
  AccessPointServer old_server;
  AccessPointServer new_server;
  const std::string old_url = make_url(old_server.endpoint(false));
  auto instance = std::make_unique<LibPalfEnvFFIInstance>();
  const LibPalfEnvFFI *ffi = nullptr;
  ASSERT_EQ(LibPalfErrno::Success, libpalf_open_env(old_url.c_str(), LibPalfAccessPointType::ObjectStorage,
      "127.0.0.1:0", -1, &ffi));
  // Bypass observer bootstrap, which owns the process address and USSL pipe.
  instance->libpalf_env_ffi_ = ffi;
  instance->is_inited_ = true;
  ASSERT_EQ(OB_SUCCESS, instance->set_logservice_access_point(old_url.c_str()));
  auto candidate = std::make_unique<Buffer>();
  auto current = std::make_unique<Buffer>();
  ASSERT_EQ(OB_SUCCESS, encrypt_url(make_url(new_server.endpoint(true)), *candidate));
  ASSERT_EQ(OB_SUCCESS, instance->validate_object_store_url(candidate->ptr()));
  ASSERT_EQ(OB_SUCCESS, instance->get_logservice_access_point(*current));
  EXPECT_STREQ(old_url.c_str(), current->ptr());

  new_server.set_healthy(false);
  EXPECT_NE(OB_SUCCESS, instance->update_object_store_url(candidate->ptr()));
  ASSERT_EQ(OB_SUCCESS, instance->get_logservice_access_point(*current));
  EXPECT_STREQ(old_url.c_str(), current->ptr());

  new_server.set_healthy(true);
  // The next configuration reload retries the same target synchronously.
  ASSERT_EQ(OB_SUCCESS, instance->update_object_store_url(candidate->ptr()));
  ASSERT_EQ(OB_SUCCESS, instance->get_logservice_access_point(*current));
  EXPECT_NE(nullptr, strstr(current->ptr(), "host=http://localhost:"));
  EXPECT_NE(nullptr, strstr(current->ptr(), "addressing_model=path_style"));

  // An unchanged configuration does not need another successful OSS request.
  new_server.set_healthy(false);
  ASSERT_EQ(OB_SUCCESS, instance->update_object_store_url(candidate->ptr()));
  auto next = std::make_unique<Buffer>();
  ASSERT_EQ(OB_SUCCESS, encrypt_url(make_url(old_server.endpoint(true)), *next));
  ASSERT_EQ(OB_SUCCESS, instance->update_object_store_url(next->ptr()));
  ASSERT_EQ(OB_SUCCESS, instance->get_logservice_access_point(*current));
  EXPECT_NE(nullptr, strstr(current->ptr(), old_server.endpoint(true).c_str()));
  EXPECT_FALSE(old_server.invalid_request());
  EXPECT_FALSE(new_server.invalid_request());
  instance.reset();
  EXPECT_EQ(LibPalfErrno::Success, libpalf_close_env(ffi));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_libpalf_access_point.log", true);
  OB_LOGGER.set_log_level("INFO");
  oceanbase::common::ObClockGenerator::init();
  ::testing::InitGoogleTest(&argc, argv);
  const int result = RUN_ALL_TESTS();
  oceanbase::common::ObTimerService::get_instance().stop();
  oceanbase::common::ObTimerService::get_instance().wait();
  oceanbase::common::ObClockGenerator::destroy();
  return result;
}
