/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_INC_SS_
#define OCEANBASE_STORAGE_INC_SS_

#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"
#include "common/ob_simple_iterator.h"

namespace oceanbase
{
namespace storage
{
enum class ObSSWriterType: int8_t
{
  INVALID = -1,
  FREEZE = 0,
  COMPACTION = 1,
  META = 2,
  GC = 3,
};

static int sswriter_type_to_index(const ObSSWriterType &type)
{
  int type_index = -1;
  switch (type) {
  case ObSSWriterType::INVALID: {
    type_index = -1;
    break;
  }
  case ObSSWriterType::FREEZE: {
    type_index = 0;
    break;
  }
  case ObSSWriterType::COMPACTION: {
    type_index = 1;
    break;
  }
  case ObSSWriterType::META: {
    type_index = 2;
    break;
  }
  case ObSSWriterType::GC: {
    type_index = 3;
    break;
  }
  default: {
    // do nothing
  }
  }
  return type_index;
}

static const char *sswriter_type_to_str(const ObSSWriterType &type)
{
  const char *type_str = "INVALID";
  switch (type) {
  case ObSSWriterType::INVALID: {
    type_str = "INVALID";
    break;
  }
  case ObSSWriterType::FREEZE: {
    type_str = "FREEZE";
    break;
  }
  case ObSSWriterType::COMPACTION: {
    type_str = "COMPACTION";
    break;
  }
  case ObSSWriterType::META: {
    type_str = "META";
    break;
  }
  case ObSSWriterType::GC: {
    type_str = "GC";
    break;
  }
  default: {
    // do nothing
  }
  }
  return type_str;
}

enum class ObSSWriterGroupState : int8_t
{
  INVALID = -1,
  UNKNOWN = 0,
  PREPAREING = 1,
  EXPECTED = 2,
  EXISTING = 3,
};

static const char *sswriter_group_state_to_str(const ObSSWriterGroupState &state)
{
  const char *state_str = "INVALID";
  switch (state) {
  case ObSSWriterGroupState::INVALID: {
    state_str = "INVALID";
    break;
  }
  case ObSSWriterGroupState::UNKNOWN: {
    state_str = "UNKNOWN";
    break;
  }
  case ObSSWriterGroupState::PREPAREING: {
    state_str = "PREPAREING";
    break;
  }
  case ObSSWriterGroupState::EXPECTED: {
    state_str = "EXPECTED";
    break;
  }
  case ObSSWriterGroupState::EXISTING: {
    state_str = "EXISTING";
    break;
  }
  default: {
    // do nothing
  }
  }
  return state_str;
}

struct ObSSWriterAddr
{
  ObSSWriterAddr() : addr_(), epoch_(-1) {}
  ~ObSSWriterAddr() { reset(); }
  void reset();
  int is_valid() const;
  ObSSWriterAddr &operator=(const ObSSWriterAddr &sswriter_addr);
  TO_STRING_KV(K_(addr), K_(epoch));
  common::ObAddr addr_;
  int64_t epoch_;
};

class ObSSWriterKey
{
  friend class ObSSWriterService;
  OB_UNIS_VERSION(1);
public:
  ObSSWriterKey()
    : type_(ObSSWriterType::INVALID),
      ls_id_(),
      tablet_id_() {}
  ObSSWriterKey(const ObSSWriterKey &key)
  {
    type_ = key.type_;
    ls_id_ = key.ls_id_;
    tablet_id_ = key.tablet_id_;
  }
  ObSSWriterKey(
      const ObSSWriterType type);
  ObSSWriterKey(
      const ObSSWriterType type,
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id);
  ObSSWriterKey(
      const ObSSWriterType type,
      const share::ObLSID &ls_id);
  void reset();
  bool is_valid() const;
  ObSSWriterKey &operator=(const ObSSWriterKey &key);
  bool operator==(const ObSSWriterKey &key) const;
  bool operator!=(const ObSSWriterKey &key) const;
  uint64_t hash() const
  {
    // TODO, for performance
    uint64_t hash_value = 0;
    hash_value = common::murmurhash(&type_, sizeof(ObSSWriterType), 0);
    hash_value = common::murmurhash(&ls_id_, sizeof(share::ObLSID), hash_value);
    hash_value = common::murmurhash(&tablet_id_, sizeof(common::ObTabletID), hash_value);
    return hash_value;
  }
  int compare(const ObSSWriterKey &other) const;
public:
  ObSSWriterType get_type() const { return type_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  share::ObLSID get_mgr_ls_id() const;
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  TO_STRING_KV(K_(type), K_(tablet_id), K_(ls_id));
private:
  ObSSWriterType type_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
};

class ObLsSSWriterKey
{
public:
  ObLsSSWriterKey()
    : type_(ObSSWriterType::INVALID),
      ls_id_() {}
  ObLsSSWriterKey(
      const ObSSWriterType type,
      const share::ObLSID &ls_id);
  void reset();
  bool is_valid() const;
  uint64_t hash() const
  {
    uint64_t hash_value = 0;
    hash_value = common::murmurhash(&type_, sizeof(ObSSWriterType), 0);
    hash_value = common::murmurhash(&ls_id_, sizeof(share::ObLSID), hash_value);
    return hash_value;
  }
  ObLsSSWriterKey &operator=(const ObLsSSWriterKey &key);
  bool operator==(const ObLsSSWriterKey &key) const;
  bool operator!=(const ObLsSSWriterKey &key) const;
  int compare(const ObLsSSWriterKey &other) const;
public:
  ObSSWriterType get_type() const { return type_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  TO_STRING_KV(K_(type), K_(ls_id));
private:
  ObSSWriterType type_;
  share::ObLSID ls_id_;
};

static const int64_t SSWRITER_DEFAULT_LEASE_US = 5 * 1000 * 1000;  // 5 seconds
static const int64_t SSWRITER_LEASE_RENEW_THRESHOLD_US = 2 * 1000 * 1000;  // 2 seconds
static const int64_t SSWRITER_STATE_DURATION_US = 3 * 1000 * 1000;  // 3 seconds

static const int64_t SSWRITER_DEFAULT_GROUP_ID = 1;

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_INC_SS_
