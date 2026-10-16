/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifdef TEST_MDS_TRANSACTION
#include "mds_test_framework_helper.h"
#include "deps/oblib/src/common/meta_programming/ob_type_traits.h"
#include "storage/tx/ob_trans_define.h"

namespace oceanbase {
namespace unittest {

using namespace storage;
using namespace mds;

int MDSTestFrameworkHelper::on_register(const char *buf,
                                      const int64_t len,
                                      storage::mds::BufferCtx &ctx)
{
  UNUSED(ctx);
  int ret = OB_SUCCESS;
  MDSTestFrameworkRegisterData data;
  int64_t pos = 0;
  if (OB_FAIL(data.deserialize(buf, len, pos))) {
    MDS_LOG(ERROR, "[UNITTEST] MDSTestFrameworkHelper fail to deserialize", KR(ret));
  } else {
    MDS_LOG(INFO, "[UNITTEST] MDSTestFrameworkHelper call on_register", K(data));
  }
  return ret;
}

int MDSTestFrameworkHelper::on_replay(const char *buf,
                                      const int64_t len,
                                      const share::SCN &scn,
                                      storage::mds::BufferCtx &ctx)
{
  UNUSED(scn);
  return on_register(buf, len, ctx);
}

bool MDSTestFrameworkHelper::check_can_do_tx_end(const bool is_willing_to_commit,
                                                 const bool for_replay,
                                                 const share::SCN &log_scn,
                                                 const char *buf,
                                                 const int64_t buf_len,
                                                 storage::mds::BufferCtx &ctx,
                                                 const char *&can_not_do_reason)
{
  static_assert(OB_TRAIT_HAS_CHECK_CAN_DO_TX_END(MDSTestFrameworkHelper), "static check failed");
  bool ok = true;
  UNUSED(is_willing_to_commit);
  UNUSED(for_replay);
  UNUSED(log_scn);
  UNUSED(buf);
  UNUSED(buf_len);
  UNUSED(ctx);
  static int call_times = 0;
  if (call_times++ < 5) {
    ok = false;
    can_not_do_reason = "JUST FOR TEST";
  }
  return ok;
}

const storage::mds::MdsWriter MDSTestFrameworkCtx::get_writer() const
{
  return storage::mds::MdsWriter(transaction::ObTransID(0));
}

void MDSTestFrameworkCtx::on_redo(const share::SCN &redo_scn)
{
  UNUSED(redo_scn);
  MDS_LOG(INFO, "[UNITTEST] call on_redo with ctx", K(++call_times_));
}

void MDSTestFrameworkCtx::before_prepare()
{
  MDS_LOG(INFO, "[UNITTEST] call before_prepare with ctx", K(++call_times_));
}

void MDSTestFrameworkCtx::on_prepare(const share::SCN &prepare_version)
{
  MDS_LOG(INFO, "[UNITTEST] call on_prepare with ctx", K(++call_times_), K(prepare_version));
}

void MDSTestFrameworkCtx::on_commit(const share::SCN &commit_version, const share::SCN &scn)
{
  MDS_LOG(INFO, "[UNITTEST] call on_commit with ctx", K(++call_times_), K(commit_version), K(scn));
}

void MDSTestFrameworkCtx::on_abort(const share::SCN &end_scn)
{
  MDS_LOG(INFO, "[UNITTEST] call on_abort with ctx", K(++call_times_), K(end_scn));
}

} // namespace unittest
} // namespace oceanbase

#endif // TEST_MDS_TRANSACTION
