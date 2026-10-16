/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX PL

#include "pl/sys_package/ob_dbms_test.h"
#include "storage/mds_table/ob_test_mds_types.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx/ob_multi_data_source.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
namespace pl
{
namespace
{
static int parse_mds_type_(const common::ObString &mds_type_name,
                           transaction::ObTxDataSourceType &type)
{
  int ret = OB_SUCCESS;
  static const char *const SINGLE_ROW = "TEST_SINGLE_ROW";
  static const char *const MULTI_ROW = "TEST_MULTI_ROW";
  if (mds_type_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mds type name is empty", KR(ret));
  } else if (0 == mds_type_name.case_compare(SINGLE_ROW)) {
    type = transaction::ObTxDataSourceType::TEST_SINGLE_ROW;
  } else if (0 == mds_type_name.case_compare(MULTI_ROW)) {
    type = transaction::ObTxDataSourceType::TEST_MULTI_ROW;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown mds type name", KR(ret), K(mds_type_name));
  }
  return ret;
}

static bool default_check_dup_for_mds_type_(const transaction::ObTxDataSourceType mds_type)
{
  return transaction::ObTxDataSourceType::TEST_MULTI_ROW == mds_type;
}
} // namespace

int ObDBMSTest::register_mds(sql::ObExecContext &ctx,
                             sql::ParamStore &params,
                             common::ObObj &result)
{
  int ret = OB_SUCCESS;
  int64_t ls_id_val = 0;
  int64_t tablet_id_val = 0;
  int64_t key_val = 0;
  int64_t value_val = 0;
  int64_t check_dup_val = 0;
  bool check_dup = true;
  common::ObString mds_type_name;
  transaction::ObTxDataSourceType mds_type = transaction::ObTxDataSourceType::UNKNOWN;
  sql::ObSQLSessionInfo *session = nullptr;
  transaction::ObTxDesc *tx_desc = nullptr;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  storage::mds::ObTestMdsRegisterArg arg;
  char buf[common::OB_MALLOC_NORMAL_BLOCK_SIZE];
  int64_t pos = 0;
  const bool has_check_dup = params.count() == 6 && !params.at(5).is_null();
  if (OB_ISNULL(session = ctx.get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (FALSE_IT(tenant_id = session->get_effective_tenant_id())) {
  } else if (OB_UNLIKELY(params.count() < 5 || params.count() > 6)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid params count", KR(ret), K(params.count()));
  } else if (OB_FAIL(params.at(0).get_int(ls_id_val))) {
    LOG_WARN("failed to get ls_id", KR(ret), K(params.at(0)));
  } else if (OB_FAIL(params.at(1).get_int(tablet_id_val))) {
    LOG_WARN("failed to get tablet_id", KR(ret), K(params.at(1)));
  } else if (OB_FAIL(params.at(2).get_varchar(mds_type_name))) {
    LOG_WARN("failed to get mds type name", KR(ret), K(params.at(2)));
  } else if (OB_FAIL(params.at(3).get_int(key_val))) {
    LOG_WARN("failed to get key", KR(ret), K(params.at(3)));
  } else if (OB_FAIL(params.at(4).get_int(value_val))) {
    LOG_WARN("failed to get value", KR(ret), K(params.at(4)));
  } else if (OB_FAIL(parse_mds_type_(mds_type_name, mds_type))) {
    LOG_WARN("failed to parse mds type", KR(ret), K(mds_type_name));
  } else if (has_check_dup && transaction::ObTxDataSourceType::TEST_SINGLE_ROW == mds_type) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("check_dup is not supported for TEST_SINGLE_ROW", KR(ret), K(mds_type_name));
  } else if (FALSE_IT(check_dup = default_check_dup_for_mds_type_(mds_type))) {
  } else if (has_check_dup && OB_FAIL(params.at(5).get_int(check_dup_val))) {
    LOG_WARN("failed to get check_dup", KR(ret), K(params.at(5)));
  } else if (has_check_dup && FALSE_IT(check_dup = (0 != check_dup_val))) {
  } else if (OB_ISNULL(tx_desc = session->get_tx_desc()) || !tx_desc->is_tx_active()) {
    ret = OB_TRANS_IS_EXITING;
    LOG_WARN("register mds must be called in active transaction", KR(ret), KP(tx_desc));
  } else if (FALSE_IT(arg.ls_id_ = share::ObLSID(ls_id_val))) {
  } else if (FALSE_IT(arg.tablet_id_ = common::ObTabletID(tablet_id_val))) {
  } else if (FALSE_IT(arg.key_ = key_val)) {
  } else if (FALSE_IT(arg.value_ = value_val)) {
  } else if (transaction::ObTxDataSourceType::TEST_MULTI_ROW == mds_type
             && FALSE_IT(arg.check_dup_ = check_dup)) {
  } else if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id or tablet_id", KR(ret), K(arg));
  } else if (OB_FAIL(arg.serialize(buf, sizeof(buf), pos))) {
    LOG_WARN("failed to serialize test mds register arg", KR(ret), K(arg));
  } else {
    MTL_SWITCH(tenant_id)
    {
      if (OB_FAIL(MTL(transaction::ObTransService *)
                      ->register_mds_into_tx(*tx_desc,
                                             arg.ls_id_,
                                             mds_type,
                                             buf,
                                             pos))) {
        LOG_WARN("failed to register test mds", KR(ret), K(arg), K(mds_type));
      }
    }
  }

  if (OB_SUCC(ret)) {
    result.set_int32(0);
  } else {
    result.set_int32(ret);
  }
  return ret;
}

} // end of pl
} // end of oceanbase
