/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
}
}
namespace observer
{

struct ObDirectLoadStreamMeta;
class ObDirectLoadStreamBufferQueueManager;

struct ObDirectLoadStreamPrepareParam
{
  ObDirectLoadStreamPrepareParam();
  bool is_valid() const;
  TO_STRING_KV(K_(stream_id), K_(tenant_id), K_(user_id), K_(target_table_id),
               K_(owner_server), K_(session_id), K_(session_create_ts));

  int64_t stream_id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t target_table_id_;
  common::ObAddr owner_server_;
  uint64_t session_id_;
  int64_t session_create_ts_;
};

class ObDirectLoadStreamPrepareCtx
{
public:
  ObDirectLoadStreamPrepareCtx();
  ~ObDirectLoadStreamPrepareCtx();
  int claim(const ObDirectLoadStreamPrepareParam &param);
  // locked_schema_version 来自 table loader 持锁初始化后写入的
  // ddl_param_.schema_version_。
  int validate_after_claim(share::schema::ObSchemaGetterGuard &schema_guard,
                           const int64_t locked_schema_version,
                           ObDirectLoadStreamBufferQueueManager &queue_manager);
  void reset();
  int finish_error_if_claimed(const int result_code);
  bool is_claimed() const { return claimed_; }

private:
  int validate_meta_(const ObDirectLoadStreamMeta &meta) const;
  int validate_schema_(share::schema::ObSchemaGetterGuard &schema_guard,
                       const int64_t locked_schema_version,
                       const ObDirectLoadStreamMeta &meta) const;
  int validate_queue_(ObDirectLoadStreamBufferQueueManager &queue_manager,
                      const ObDirectLoadStreamMeta &meta) const;
  int64_t stream_id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t target_table_id_;
  common::ObAddr owner_server_;
  bool claimed_;
};

}  // namespace observer
}  // namespace oceanbase
