/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SSWRITER_MSG_
#define OCEANBASE_STORAGE_SSWRITER_MSG_

#include "storage/incremental/ob_inc_ss_define.h"

namespace oceanbase
{
namespace storage
{
struct ObSSWriterGrantArg
{
  ObSSWriterGrantArg()
    : group_id_(-1),
      ls_id_(),
      type_(ObSSWriterType::INVALID),
      epoch_(-1),
      target_addr_() {}
  bool is_valid() const;
  TO_STRING_KV(K_(group_id), K_(ls_id), K_(type), K_(epoch), K_(target_addr));
  int64_t group_id_;
  share::ObLSID ls_id_;
  ObSSWriterType type_;
  int64_t epoch_;
  common::ObAddr target_addr_;
};

struct ObSSWriterLeaseRequestArg
{
  ObSSWriterLeaseRequestArg()
    : group_id_(-1),
      ls_id_(),
      type_(ObSSWriterType::INVALID),
      epoch_(-1),
      lease_us_(-1),
      target_addr_(),
      sswriter_base_ts_(-1) {}
  bool is_valid() const;
  TO_STRING_KV(K_(group_id), K_(ls_id), K_(type), K_(epoch), K_(lease_us),
      K_(target_addr), K_(sswriter_base_ts));
  int64_t group_id_;
  share::ObLSID ls_id_;
  ObSSWriterType type_;
  int64_t epoch_;
  int64_t lease_us_;
  common::ObAddr target_addr_;
  int64_t sswriter_base_ts_;  // only for async logic
};

struct ObSSWriterLocationRequestArg
{
  ObSSWriterLocationRequestArg()
    : sswriter_key_(),
      request_start_ts_(-1) {}
  bool is_valid() const;
  TO_STRING_KV(K_(sswriter_key), K_(request_start_ts));
  ObSSWriterKey sswriter_key_;
  int64_t request_start_ts_;
};

struct ObSSWriterLocationResponseArg
{
  ObSSWriterLocationResponseArg()
    : sswriter_addr_(),
      epoch_(-1),
      remaining_lease_us_(-1) {}
  bool is_valid() const;
  TO_STRING_KV(K_(sswriter_addr), K_(epoch), K_(remaining_lease_us));
  common::ObAddr sswriter_addr_;
  int64_t epoch_;
  int64_t remaining_lease_us_;
};

class ObSSWriterGrantRequest
{
  OB_UNIS_VERSION(1);
public:
  ObSSWriterGrantRequest()
    : group_id_(-1),
      ls_id_(),
      type_(ObSSWriterType::INVALID),
      epoch_(-1),
      sender_() {}
  int init(const int64_t group_id,
           const share::ObLSID ls_id,
           const ObSSWriterType type,
           const int64_t epoch,
           const ObAddr &sender);
  bool is_valid() const;
public:
  int64_t get_group_id() const { return group_id_; }
  int64_t get_epoch() const { return epoch_; }
  const common::ObAddr &get_sender() const { return sender_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  ObSSWriterType get_sswriter_type() const { return type_; }
  TO_STRING_KV(K_(ls_id), K_(type), K_(epoch), K_(sender));
private:
  int64_t group_id_;
  share::ObLSID ls_id_;
  ObSSWriterType type_;
  int64_t epoch_;
  common::ObAddr sender_;
};

class ObSSWriterLeaseRequest
{
  OB_UNIS_VERSION(1);
public:
  ObSSWriterLeaseRequest()
    : group_id_(-1),
      ls_id_(),
      type_(ObSSWriterType::INVALID),
      epoch_(-1),
      lease_us_(-1),
      sender_(),
      sswriter_base_ts_(-1) {}
  int init(const ObSSWriterLeaseRequestArg &arg);
  bool is_valid() const;
  bool is_valid_for_async() const;
public:
  int64_t get_group_id() const { return group_id_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  ObSSWriterType get_sswriter_type() const { return type_; }
  int64_t get_epoch() const { return epoch_; }
  int64_t get_lease_us() const { return lease_us_; }
  const common::ObAddr &get_sender() const { return sender_; }
  int64_t get_sswriter_base_ts() const { return sswriter_base_ts_; }
  TO_STRING_KV(K_(group_id), K_(ls_id), K_(type), K_(epoch), K_(lease_us),
      K_(sender), K_(sswriter_base_ts));
private:
  int64_t group_id_;
  share::ObLSID ls_id_;
  ObSSWriterType type_;
  int64_t epoch_;
  int64_t lease_us_; // currently, use default value
  common::ObAddr sender_;
  int64_t sswriter_base_ts_; // only for async logic
};

// only use for async
class ObSSWriterLeaseResponse
{
  OB_UNIS_VERSION(1);
public:
  ObSSWriterLeaseResponse()
    : group_id_(-1),
      ls_id_(),
      type_(ObSSWriterType::INVALID),
      epoch_(-1),
      lease_us_(-1),
      sender_(),
      sswriter_base_ts_(-1) {}
  int init(const ObSSWriterLeaseRequest &req);
  bool is_valid() const;
public:
  int64_t get_group_id() const { return group_id_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  ObSSWriterType get_sswriter_type() const { return type_; }
  int64_t get_epoch() const { return epoch_; }
  int64_t get_lease_us() const { return lease_us_; }
  const common::ObAddr &get_sender() const { return sender_; }
  int64_t get_sswriter_base_ts() const { return sswriter_base_ts_; }
  TO_STRING_KV(K_(group_id), K_(ls_id), K_(type), K_(epoch), K_(lease_us),
      K_(sender), K_(sswriter_base_ts));
private:
  int64_t group_id_;
  share::ObLSID ls_id_;
  ObSSWriterType type_;
  int64_t epoch_;
  int64_t lease_us_; // currently, use default value
  common::ObAddr sender_;
  int64_t sswriter_base_ts_; // only for async logic
};

class ObSSWriterLocationRequest
{
  OB_UNIS_VERSION(1);
public:
  ObSSWriterLocationRequest()
    : sswriter_key_(),
      sender_(),
      request_start_ts_(-1) {}
  int init(const ObSSWriterLocationRequestArg &arg);
  bool is_valid() const;
public:
  const ObSSWriterKey &get_sswriter_key() const { return sswriter_key_; }
  const common::ObAddr &get_sender() const { return sender_; }
  int64_t get_request_start_ts() const { return request_start_ts_; }
  TO_STRING_KV(K_(sswriter_key), K_(sender), K_(request_start_ts));
private:
  ObSSWriterKey sswriter_key_;
  common::ObAddr sender_;
  int64_t request_start_ts_;
};

class ObSSWriterLocationResponse
{
  OB_UNIS_VERSION(1);
public:
  ObSSWriterLocationResponse()
    : sswriter_key_(),
      sender_(),
      sswriter_addr_(),
      epoch_(-1),
      remaining_lease_us_(-1),
      request_start_ts_(-1) {}
  int init(
      const ObSSWriterLocationRequest &req,
      const ObSSWriterLocationResponseArg &arg);
  bool is_valid() const;
public:
  const ObSSWriterKey &get_sswriter_key() const { return sswriter_key_; }
  const common::ObAddr &get_sender() const { return sender_; }
  const common::ObAddr &get_sswriter_addr() const { return sswriter_addr_; }
  int64_t get_epoch() const { return epoch_; }
  int64_t get_request_start_ts() const { return request_start_ts_; }
  int64_t get_remaining_lease_us() const { return remaining_lease_us_; }
  int64_t get_lease_expire_ts() const { return request_start_ts_ + remaining_lease_us_; }
  TO_STRING_KV(K_(sswriter_key), K_(sender), K_(sswriter_addr), K_(epoch),
      K_(remaining_lease_us), K_(request_start_ts));
private:
  ObSSWriterKey sswriter_key_;
  common::ObAddr sender_;
  common::ObAddr sswriter_addr_;
  int64_t epoch_;
  int64_t remaining_lease_us_;
  int64_t request_start_ts_;
};

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_SSWRITER_MSG_
