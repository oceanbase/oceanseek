/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_PL_OB_JAVA_SANDBOX_FRAME_H_
#define OCEANBASE_PL_OB_JAVA_SANDBOX_FRAME_H_

#include <stdint.h>
#include "lib/allocator/ob_allocator.h"

namespace oceanbase
{
namespace pl
{

enum ObJavaSandboxMsgType : uint8_t {
  JAVA_SANDBOX_EXECUTE_REQ         = 0x01,
  JAVA_SANDBOX_EXECUTE_RESP        = 0x02,
  JAVA_SANDBOX_LOAD_JAR            = 0x03,
  JAVA_SANDBOX_LOAD_JAR_RESP       = 0x04,
  JAVA_SANDBOX_CREATE_CHANNEL      = 0x0A,
  JAVA_SANDBOX_SHUTDOWN            = 0x0C,
  JAVA_SANDBOX_EVICT_STATEMENT     = 0x0D,
  JAVA_SANDBOX_EXECUTE_ADAPTER_REQ = 0x0E, // FunctionInfo; response is EXECUTE_RESP.
};

// Sandbox-internal error codes carried in the error payload of *_RESP frames
// (status byte = 1). These are NOT OB kernel error codes -- the proxy maps them:
//   SANDBOX_ERR_JAVA_TIMEOUT   -> OB_TIMEOUT
//   SANDBOX_ERR_JAVA_EXCEPTION -> OB_JNI_JAVA_EXCEPTION_ERROR
//   SANDBOX_ERR_JAVA_TIMEOUT_STUCK -> OB_TIMEOUT; no JVM rollover
// Values are positive and deliberately outside the OB kernel errno range (all
// OB errnos are <= 0) to avoid ambiguity. Must match the SDK's MsgType constants.
constexpr int32_t SANDBOX_ERR_JAVA_EXCEPTION = 1;
constexpr int32_t SANDBOX_ERR_JAVA_TIMEOUT   = 2;
constexpr int32_t SANDBOX_ERR_JAVA_TIMEOUT_STUCK = 3;

struct ObJavaSandboxFrameHeader {
  static constexpr uint16_t MAGIC = 0x4A55;  // "JU"
  static constexpr uint8_t VERSION = 0x01;
  static constexpr int64_t HEADER_SIZE = 12;
  static constexpr int64_t MAX_PAYLOAD_SIZE = 32 * 1024 * 1024;  // 32MB

  uint16_t magic_;
  uint8_t version_;
  uint32_t req_id_;
  uint8_t msg_type_;
  uint32_t payload_len_;

  ObJavaSandboxFrameHeader()
    : magic_(MAGIC), version_(VERSION), req_id_(0), msg_type_(0), payload_len_(0) {}

  void encode(char *buf) const;
  static int decode(const char *buf, int64_t len, ObJavaSandboxFrameHeader &header);
};

// Write one frame (header + payload) to fd, handling partial writes and EINTR.
int ob_java_sandbox_send_frame(int fd, uint32_t req_id, uint8_t msg_type,
                                const char *payload, int64_t payload_len);

// Read one frame from fd with timeout. Allocates payload buffer from alloc.
// payload is set to nullptr if payload_len is 0.
int ob_java_sandbox_recv_frame(int fd, int64_t timeout_us,
                                ObJavaSandboxFrameHeader &header,
                                common::ObIAllocator &alloc, char *&payload);

} // namespace pl
} // namespace oceanbase

#endif // OCEANBASE_PL_OB_JAVA_SANDBOX_FRAME_H_
