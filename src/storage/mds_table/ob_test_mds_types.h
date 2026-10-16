/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_TEST_MDS_TYPES_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_TEST_MDS_TYPES_H

#include "lib/ob_errno.h"
#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/multi_data_source/mds_key_serialize_util.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "common/ob_tablet_id.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

/**
 * Single-row test MDS value.
 *
 * value_ is set from SQL parameter v. commit_version_, commit_scn_, and redo_scn_
 * are updated in on_commit / on_redo and can be read back via virtual tables to
 * verify callbacks.
 */
struct ObTestMdsSingleRowValue
{
  OB_UNIS_VERSION(1);

public:
  ObTestMdsSingleRowValue()
    : value_(0),
      commit_version_(0),
      commit_scn_(0),
      redo_scn_(0)
  {}

  explicit ObTestMdsSingleRowValue(const int64_t value)
    : value_(value),
      commit_version_(0),
      commit_scn_(0),
      redo_scn_(0)
  {}

  bool operator==(const ObTestMdsSingleRowValue &rhs) const {
    return value_ == rhs.value_;
  }

  void reset()
  {
    value_ = 0;
    commit_version_ = 0;
    commit_scn_ = 0;
    redo_scn_ = 0;
  }
  void destroy() { reset(); }

  TO_STRING_KV(K_(value),
               K_(commit_version),
               K_(commit_scn),
               K_(redo_scn));

  int assign(const ObTestMdsSingleRowValue &rhs)
  {
    value_ = rhs.value_;
    commit_version_ = rhs.commit_version_;
    commit_scn_ = rhs.commit_scn_;
    redo_scn_ = rhs.redo_scn_;
    return OB_SUCCESS;
  }

  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
  {
    commit_version_ = commit_version.get_val_for_tx(true /*ignore_invalid_scn*/);
    commit_scn_ = commit_scn.get_val_for_tx(true /*ignore_invalid_scn*/);
  }

  void on_redo(const share::SCN &redo_scn)
  {
    redo_scn_ = redo_scn.get_val_for_tx(true /*ignore_invalid_scn*/);
  }

  int64_t value_;
  int64_t commit_version_;
  int64_t commit_scn_;
  int64_t redo_scn_;
};

/** Multi-row test MDS key (int64). */
struct ObTestMdsMultiRowKey
{
  OB_UNIS_VERSION(1);

public:
  ObTestMdsMultiRowKey() : key_(0) {}
  explicit ObTestMdsMultiRowKey(const int64_t key) : key_(key) {}

  bool operator==(const ObTestMdsMultiRowKey &rhs) const {
    return key_ == rhs.key_;
  }

  bool operator<(const ObTestMdsMultiRowKey &rhs) const {
    return key_ < rhs.key_;
  }

  int compare(const ObTestMdsMultiRowKey &other, bool &equal) const
  {
    equal = (key_ == other.key_);
    if (key_ < other.key_) {
      return -1;
    } else if (key_ > other.key_) {
      return 1;
    }
    return 0;
  }

  void reset()
  {
    key_ = 0;
  }

  TO_STRING_KV(K_(key));

  int mds_serialize(char *buf, const int64_t buf_len, int64_t &pos) const
  {
    return ObMdsSerializeUtil::mds_key_serialize(key_, buf, buf_len, pos);
  }

  int mds_deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
  {
    int ret = OB_SUCCESS;
    int64_t tmp = 0;
    if (OB_FAIL(ObMdsSerializeUtil::mds_key_deserialize(buf, buf_len, pos, tmp))) {
    } else {
      key_ = tmp;
    }
    return ret;
  }

  int64_t mds_get_serialize_size() const
  {
    return ObMdsSerializeUtil::mds_key_get_serialize_size(key_);
  }

  int assign(const ObTestMdsMultiRowKey &rhs)
  {
    key_ = rhs.key_;
    return OB_SUCCESS;
  }

  int64_t key_;
};

/**
 * Multi-row test MDS value. Same callback fields as ObTestMdsSingleRowValue.
 */
struct ObTestMdsMultiRowValue
{
  OB_UNIS_VERSION(1);

public:
  ObTestMdsMultiRowValue()
    : value_(0),
      commit_version_(0),
      commit_scn_(0),
      redo_scn_(0)
  {}

  explicit ObTestMdsMultiRowValue(const int64_t value)
    : value_(value),
      commit_version_(0),
      commit_scn_(0),
      redo_scn_(0)
  {}

  bool operator==(const ObTestMdsMultiRowValue &rhs) const {
    return value_ == rhs.value_;
  }

  void reset()
  {
    value_ = 0;
    commit_version_ = 0;
    commit_scn_ = 0;
    redo_scn_ = 0;
  }
  void destroy() { reset(); }

  TO_STRING_KV(K_(value),
               K_(commit_version),
               K_(commit_scn),
               K_(redo_scn));

  int assign(const ObTestMdsMultiRowValue &rhs)
  {
    value_ = rhs.value_;
    commit_version_ = rhs.commit_version_;
    commit_scn_ = rhs.commit_scn_;
    redo_scn_ = rhs.redo_scn_;
    return OB_SUCCESS;
  }

  void on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
  {
    commit_version_ = commit_version.get_val_for_tx(true /*ignore_invalid_scn*/);
    commit_scn_ = commit_scn.get_val_for_tx(true /*ignore_invalid_scn*/);
  }

  void on_redo(const share::SCN &redo_scn)
  {
    redo_scn_ = redo_scn.get_val_for_tx(true /*ignore_invalid_scn*/);
  }

  int64_t value_;
  int64_t commit_version_;
  int64_t commit_scn_;
  int64_t redo_scn_;
};

/** Arguments for DBMS_TEST.REGISTER_MDS. */
struct ObTestMdsRegisterArg
{
  OB_UNIS_VERSION(1);

public:
  ObTestMdsRegisterArg()
    : ls_id_(),
      tablet_id_(),
      key_(0),
      value_(0),
      check_dup_(true)
  {}

  bool is_valid() const
  {
    return ls_id_.is_valid() && tablet_id_.is_valid();
  }

  TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(key), K_(value), K_(check_dup));

  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  int64_t key_;
  int64_t value_;
  // Only used by TEST_MULTI_ROW on_register (multi-key set supports check_dup).
  bool check_dup_;
};

/** Helper for single-row test MDS transaction registration. */
struct ObTestMdsSingleRowHelper
{
  static int on_register(const char *buf, const int64_t len, BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN &scn, BufferCtx &ctx);
};

/** Helper for multi-row test MDS transaction registration. */
struct ObTestMdsMultiRowHelper
{
  static int on_register(const char *buf, const int64_t len, BufferCtx &ctx);
  static int on_replay(const char *buf, const int64_t len, const share::SCN &scn, BufferCtx &ctx);
};

} // end of namespace mds
} // end of namespace storage
} // end of namespace oceanbase

#endif /* OCEANBASE_STORAGE_MDS_TABLE_OB_TEST_MDS_TYPES_H */
