/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MULTI_DATA_SOURCE_TEST_MDS_TEST_FRAMEWORK_HELPER_H
#define OCEANBASE_STORAGE_MULTI_DATA_SOURCE_TEST_MDS_TEST_FRAMEWORK_HELPER_H

#include "lib/utility/ob_unify_serialize.h"
#include "storage/multi_data_source/buffer_ctx.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/serialization.h"
#include "share/scn.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "deps/oblib/src/common/meta_programming/ob_type_traits.h"

namespace oceanbase {
namespace unittest {

/** Payload for TEST_FRAMEWORK multi-data-source registration. */
struct MDSTestFrameworkRegisterData {
  OB_UNIS_VERSION(1);
public:
  MDSTestFrameworkRegisterData() : value_(0) {}
  explicit MDSTestFrameworkRegisterData(const int64_t val) : value_(val) {}
  TO_STRING_KV(K_(value));
  int64_t value_;
};

OB_SERIALIZE_MEMBER_TEMP(inline, MDSTestFrameworkRegisterData, value_);

/** Merged framework test helper: check_can_do_tx_end + on_register/on_replay. */
struct MDSTestFrameworkHelper {
  static int on_register(const char *buf,
                         const int64_t len,
                         storage::mds::BufferCtx &ctx);

  static int on_replay(const char *buf,
                       const int64_t len,
                       const share::SCN &scn,
                       storage::mds::BufferCtx &ctx);

  static bool check_can_do_tx_end(const bool is_willing_to_commit,
                                  const bool for_replay,
                                  const share::SCN &log_scn,
                                  const char *buf,
                                  const int64_t buf_len,
                                  storage::mds::BufferCtx &ctx,
                                  const char *&can_not_do_reason);
};

struct MDSTestFrameworkCtx : public storage::mds::BufferCtx {
  MDSTestFrameworkCtx() : call_times_(0) {}
  virtual const storage::mds::MdsWriter get_writer() const override;
  virtual void on_redo(const share::SCN &redo_scn) override;
  virtual void before_prepare() override;
  virtual void on_prepare(const share::SCN &prepare_version) override;
  virtual void on_commit(const share::SCN &commit_version, const share::SCN &scn) override;
  virtual void on_abort(const share::SCN &scn) override;
  int assign(const MDSTestFrameworkCtx &rhs)
  {
    call_times_ = rhs.call_times_;
    return OB_SUCCESS;
  }
  int call_times_;
  virtual int64_t to_string(char *, const int64_t buf_len) const override
  {
    UNUSED(buf_len);
    return 0;
  }
  virtual int serialize(char *, const int64_t, int64_t &) const override { return OB_SUCCESS; }
  virtual int deserialize(const char *, const int64_t, int64_t &) override { return OB_SUCCESS; }
  virtual int64_t get_serialize_size(void) const override { return 0; }
};

#ifndef TEST_MDS_TRANSACTION

inline int MDSTestFrameworkHelper::on_register(const char *, const int64_t, storage::mds::BufferCtx &)
{
  return OB_SUCCESS;
}

inline int MDSTestFrameworkHelper::on_replay(
    const char *, const int64_t, const share::SCN &, storage::mds::BufferCtx &)
{
  return OB_SUCCESS;
}

inline bool MDSTestFrameworkHelper::check_can_do_tx_end(const bool is_willing_to_commit,
                                                            const bool for_replay,
                                                            const share::SCN &log_scn,
                                                            const char *buf,
                                                            const int64_t buf_len,
                                                            storage::mds::BufferCtx &ctx,
                                                            const char *&can_not_do_reason)
{
  static_assert(OB_TRAIT_HAS_CHECK_CAN_DO_TX_END(MDSTestFrameworkHelper), "static check failed");
  bool ret = true;
  UNUSED(is_willing_to_commit);
  UNUSED(for_replay);
  UNUSED(log_scn);
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(ctx);
  static int call_times = 0;
  if (call_times++ < 5) {
    ret = false;
    can_not_do_reason = "JUST FOR TEST";
  }
  return ret;
}

inline const storage::mds::MdsWriter MDSTestFrameworkCtx::get_writer() const
{
  return storage::mds::MdsWriter(storage::mds::WriterType::TRANSACTION,
                                 storage::mds::MdsWriter::DEFAULT_WRITER_ID);
}

inline void MDSTestFrameworkCtx::on_redo(const share::SCN &redo_scn)
{
  UNUSED(redo_scn);
  MDS_LOG(INFO, "[UNITTEST] call on_redo with ctx", K(++call_times_));
}

inline void MDSTestFrameworkCtx::before_prepare()
{
  MDS_LOG(INFO, "[UNITTEST] call before_prepare with ctx", K(++call_times_));
}

inline void MDSTestFrameworkCtx::on_prepare(const share::SCN &prepare_version)
{
  MDS_LOG(INFO, "[UNITTEST] call on_prepare with ctx", K(++call_times_), K(prepare_version));
}

inline void MDSTestFrameworkCtx::on_commit(const share::SCN &commit_version, const share::SCN &scn)
{
  MDS_LOG(INFO, "[UNITTEST] call on_commit with ctx", K(++call_times_), K(commit_version), K(scn));
}

inline void MDSTestFrameworkCtx::on_abort(const share::SCN &scn)
{
  MDS_LOG(INFO, "[UNITTEST] call on_abort with ctx", K(++call_times_), K(scn));
}
#endif

} // namespace unittest
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MULTI_DATA_SOURCE_TEST_MDS_TEST_FRAMEWORK_HELPER_H
