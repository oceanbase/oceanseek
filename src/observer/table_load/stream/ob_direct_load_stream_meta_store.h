/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#pragma once

#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "share/table/ob_direct_load_stream_queue_list.h"

namespace oceanbase
{
namespace observer
{

enum ObDirectLoadStreamStatus
{
  DIRECT_LOAD_STREAM_STATUS_OPEN = 0,
  DIRECT_LOAD_STREAM_STATUS_SEALED = 1,
  DIRECT_LOAD_STREAM_STATUS_FINISHED = 2,
  DIRECT_LOAD_STREAM_STATUS_MAX
};

enum ObDirectLoadStreamMetaCreateOutcome
{
  DIRECT_LOAD_STREAM_META_CREATED = 0,
  DIRECT_LOAD_STREAM_META_NOT_CREATED = 1,
  DIRECT_LOAD_STREAM_META_CREATE_UNKNOWN = 2,
  DIRECT_LOAD_STREAM_META_CREATE_OUTCOME_MAX
};

inline bool is_valid_stream_id(const int64_t stream_id)
{
  return stream_id > 0;
}

struct ObDirectLoadStreamCreateParam;
struct ObDirectLoadStreamClaimParam;
struct ObDirectLoadStreamScanParam;

int check_direct_load_stream_create_param(const ObDirectLoadStreamCreateParam &param);
int check_direct_load_stream_claim_param(const ObDirectLoadStreamClaimParam &param);
int check_direct_load_stream_scan_param(const ObDirectLoadStreamScanParam &param);

struct ObDirectLoadStreamCreateParam
{
public:
  ObDirectLoadStreamCreateParam()
    : tenant_id_(common::OB_INVALID_ID),
      user_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      target_schema_version_(0),
      expire_ts_(0),
      queue_entries_()
  {
  }
  TO_STRING_KV(K_(tenant_id), K_(user_id), K_(table_id), K_(target_schema_version),
               K_(expire_ts), K_(queue_entries));

  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t table_id_;
  int64_t target_schema_version_;
  int64_t expire_ts_;
  common::ObSEArray<table::ObDirectLoadStreamQueueEntry, 4> queue_entries_;
};

struct ObDirectLoadStreamClaimParam
{
public:
  ObDirectLoadStreamClaimParam()
    : stream_id_(common::OB_INVALID_ID),
      owner_server_(),
      session_id_(common::OB_INVALID_ID),
      session_create_ts_(0)
  {
  }
  TO_STRING_KV(K_(stream_id), K_(owner_server), K_(session_id), K_(session_create_ts));

  int64_t stream_id_;
  common::ObAddr owner_server_;
  uint64_t session_id_;
  int64_t session_create_ts_;
};

struct ObDirectLoadStreamScanParam
{
public:
  ObDirectLoadStreamScanParam()
    : status_(DIRECT_LOAD_STREAM_STATUS_MAX),
      upper_bound_ts_(0),
      start_after_stream_id_(0),
      limit_(0)
  {
  }
  TO_STRING_KV(K_(status), K_(upper_bound_ts), K_(start_after_stream_id), K_(limit));

  ObDirectLoadStreamStatus status_;
  // 时间扫描上界：OPEN/SEALED 与 expire_ts_ 比较，FINISHED 与 gmt_modified 比较。
  int64_t upper_bound_ts_;
  // 排他分页游标，仅返回 stream_id 大于该值的记录。
  int64_t start_after_stream_id_;
  int64_t limit_;
};

struct ObDirectLoadStreamMeta
{
public:
  ObDirectLoadStreamMeta()
    : stream_id_(common::OB_INVALID_ID),
      tenant_id_(common::OB_INVALID_ID),
      user_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      target_schema_version_(0),
      status_(DIRECT_LOAD_STREAM_STATUS_MAX),
      queue_entries_(),
      owner_server_(),
      session_id_(common::OB_INVALID_ID),
      session_create_ts_(0),
      result_code_(common::OB_SUCCESS),
      create_ts_(0),
      modified_ts_(0),
      expire_ts_(0)
  {
  }
  TO_STRING_KV(K_(stream_id), K_(tenant_id), K_(user_id), K_(table_id),
               K_(target_schema_version), K_(status), K_(queue_entries), K_(owner_server),
               K_(session_id), K_(session_create_ts), K_(result_code), K_(create_ts),
               K_(modified_ts), K_(expire_ts));

  int64_t stream_id_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t table_id_;
  int64_t target_schema_version_;
  ObDirectLoadStreamStatus status_;
  common::ObSEArray<table::ObDirectLoadStreamQueueEntry, 4> queue_entries_;
  common::ObAddr owner_server_;
  uint64_t session_id_;
  int64_t session_create_ts_;
  int result_code_;
  int64_t create_ts_;
  int64_t modified_ts_;
  int64_t expire_ts_;
};

class ObIDirectLoadStreamMetaStore
{
public:
  virtual ~ObIDirectLoadStreamMetaStore() {}
  virtual int create_stream(const int64_t stream_id,
                            const ObDirectLoadStreamCreateParam &param,
                            ObDirectLoadStreamMetaCreateOutcome &outcome) = 0;
  virtual int get_stream(const int64_t stream_id, ObDirectLoadStreamMeta &meta) = 0;
  virtual int scan_streams(const ObDirectLoadStreamScanParam &param,
                           common::ObIArray<ObDirectLoadStreamMeta> &metas) = 0;
  virtual int scan_gc_streams(const int64_t now,
                              common::ObIArray<ObDirectLoadStreamMeta> &metas) = 0;
  // stream 缺失时按 UPDATE 零行处理：返回 OB_SUCCESS，finished=false。
  virtual int finish_expired_unclaimed_stream(const int64_t stream_id,
                                              const int64_t now,
                                              bool &finished) = 0;
  virtual int seal_stream(const int64_t stream_id) = 0;
  virtual int claim_stream(const ObDirectLoadStreamClaimParam &param) = 0;
  virtual int finish_stream(const int64_t stream_id, const int result_code) = 0;
  // 正常 UPDATE 零行且 stream 缺失时返回 OB_STATE_NOT_MATCH，finished=false。
  virtual int finish_stream_if_open(const int64_t stream_id,
                                    const int result_code,
                                    bool &finished) = 0;
  virtual int delete_stream(const int64_t stream_id,
                            const int64_t max_modified_ts) = 0;
};

} // namespace observer
} // namespace oceanbase
