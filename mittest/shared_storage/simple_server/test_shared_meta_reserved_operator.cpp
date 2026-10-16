// owner: muwei.ym
// owner group: storage

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */


#include <gmock/gmock.h>
#include <string>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/scn.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_define.h"
#include "src/storage/reserve_shared_meta/ob_shared_meta_reserved_operator.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
namespace storage
{
using namespace common;
using namespace share;
using namespace unittest;

constexpr int64_t TIMEOUT_US = 10L * 1000L * 1000L;

// Validation-path tests for ObSharedMetaReservedOperator. The cluster boot
// gives us a real ObISQLClient; we don't drive SQL through the operator here
// (the table is per user tenant and would require a created user tenant) —
// these cover init / not-init / argument-shape contracts.
class TestSharedMetaReservedOperator : public unittest::ObSimpleClusterTestBase
{
public:
  TestSharedMetaReservedOperator()
    : unittest::ObSimpleClusterTestBase("test_shared_meta_reserved_operator") {}

  ObMySQLProxy &sql_proxy() { return get_curr_simple_server().get_observer().get_mysql_proxy(); }

  static SCN scn(uint64_t v)
  {
    SCN s;
    (void)s.convert_for_tx(v);
    return s;
  }

  static ObSharedMetaReservedInfo make_info(
      const uint64_t tenant_id,
      const ObSharedMetaReservedTypeEnum type_enum,
      const SCN &meta_scn)
  {
    return ObSharedMetaReservedInfo(tenant_id, ObSharedMetaReservedType(type_enum), meta_scn);
  }
};

TEST_F(TestSharedMetaReservedOperator, init_rejects_non_positive_timeout)
{
  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.init(sql_proxy(), 0));
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.init(sql_proxy(), -1));
  ASSERT_FALSE(op.is_inited_);
}

TEST_F(TestSharedMetaReservedOperator, init_twice_rejected)
{
  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), TIMEOUT_US));
  ASSERT_EQ(OB_INIT_TWICE, op.init(sql_proxy(), TIMEOUT_US));
}

TEST_F(TestSharedMetaReservedOperator, reset_clears_state)
{
  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), TIMEOUT_US));
  ASSERT_TRUE(op.is_inited_);
  op.reset();
  ASSERT_FALSE(op.is_inited_);
  ASSERT_EQ(nullptr, op.sql_client_);
  ASSERT_EQ(0, op.sql_timeout_us_);
}

TEST_F(TestSharedMetaReservedOperator, reserved_type_key_mode_and_parse)
{
  ObSharedMetaReservedType gc_type(ObSharedMetaReservedTypeEnum::GC);
  ASSERT_TRUE(gc_type.is_valid());
  ASSERT_TRUE(gc_type.is_gc());
  ASSERT_EQ(ObSharedMetaReservedKeyMode::SCN_KEYED, gc_type.key_mode());
  ASSERT_TRUE(gc_type.is_scn_keyed());
  ASSERT_FALSE(gc_type.is_owner_keyed());
  ASSERT_STREQ("GC", gc_type.to_str());

  ObSharedMetaReservedType ls_ckpt_type(ObSharedMetaReservedTypeEnum::LS_CKPT);
  ASSERT_EQ(ObSharedMetaReservedKeyMode::SCN_KEYED, ls_ckpt_type.key_mode());
  ASSERT_TRUE(ls_ckpt_type.is_scn_keyed());
  ASSERT_FALSE(ls_ckpt_type.is_owner_keyed());
  ObSharedMetaReservedType backup_type(ObSharedMetaReservedTypeEnum::BACKUP);
  ASSERT_EQ(ObSharedMetaReservedKeyMode::OWNER_KEYED, backup_type.key_mode());
  ASSERT_FALSE(backup_type.is_scn_keyed());
  ASSERT_TRUE(backup_type.is_owner_keyed());
  ObSharedMetaReservedType snapshot_type(ObSharedMetaReservedTypeEnum::SNAPSHOT);
  ASSERT_EQ(ObSharedMetaReservedKeyMode::SCN_KEYED, snapshot_type.key_mode());
  ASSERT_TRUE(snapshot_type.is_scn_keyed());
  ASSERT_FALSE(snapshot_type.is_owner_keyed());
  ObSharedMetaReservedType max_reserved_type(ObSharedMetaReservedTypeEnum::MAX_RESERVED);
  ASSERT_EQ(ObSharedMetaReservedKeyMode::NONE, max_reserved_type.key_mode());
  ASSERT_FALSE(max_reserved_type.is_scn_keyed());
  ASSERT_FALSE(max_reserved_type.is_owner_keyed());
  ObSharedMetaReservedType fork_table_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
  ASSERT_TRUE(fork_table_type.is_fork_table());
  ASSERT_EQ(ObSharedMetaReservedKeyMode::OWNER_KEYED, fork_table_type.key_mode());
  ASSERT_TRUE(fork_table_type.is_owner_keyed());
  ObSharedMetaReservedType ddl_type(ObSharedMetaReservedTypeEnum::DDL);
  ASSERT_TRUE(ddl_type.is_ddl());
  ASSERT_EQ(ObSharedMetaReservedKeyMode::OWNER_KEYED, ddl_type.key_mode());
  ASSERT_TRUE(ddl_type.is_owner_keyed());
  ObSharedMetaReservedType data_check_type(ObSharedMetaReservedTypeEnum::DATA_CHECK);
  ASSERT_TRUE(data_check_type.is_data_check());
  ASSERT_EQ(ObSharedMetaReservedKeyMode::OWNER_KEYED, data_check_type.key_mode());
  ASSERT_TRUE(data_check_type.is_owner_keyed());

  ObSharedMetaReservedType parsed_type;
  ASSERT_EQ(OB_SUCCESS, ObSharedMetaReservedType::parse_from_str(
      ObString::make_string("GC"), parsed_type));
  ASSERT_EQ(gc_type, parsed_type);
  ASSERT_EQ(OB_SUCCESS, ObSharedMetaReservedType::parse_from_str(
      ObString::make_string("FORK_TABLE"), parsed_type));
  ASSERT_EQ(fork_table_type, parsed_type);
  ASSERT_EQ(OB_SUCCESS, ObSharedMetaReservedType::parse_from_str(
      ObString::make_string("DDL"), parsed_type));
  ASSERT_EQ(ddl_type, parsed_type);
  ASSERT_EQ(OB_SUCCESS, ObSharedMetaReservedType::parse_from_str(
      ObString::make_string("LS_CKPT"), parsed_type));
  ASSERT_EQ(ls_ckpt_type, parsed_type);
  ASSERT_EQ(OB_SUCCESS, ObSharedMetaReservedType::parse_from_str(
      ObString::make_string("DATA_CHECK"), parsed_type));
  ASSERT_EQ(data_check_type, parsed_type);
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObSharedMetaReservedType::parse_from_str(
      ObString::make_string("MAX"), parsed_type));
  // The name table is generated for every enum entry (INVALID .. MAX), so its
  // length is the MAX sentinel value plus one. This guards against adding an enum
  // value without a matching name (or vice versa).
  ASSERT_EQ(static_cast<int64_t>(ObSharedMetaReservedTypeEnum::MAX) + 1,
      static_cast<int64_t>(
          sizeof(OB_SHARED_META_RESERVED_TYPE_NAMES_) / sizeof(const char *)));
  ASSERT_EQ(static_cast<int64_t>(ObSharedMetaReservedTypeEnum::MAX) + 1,
      static_cast<int64_t>(
          sizeof(OB_SHARED_META_RESERVED_TYPE_KEY_MODES_)
          / sizeof(ObSharedMetaReservedKeyMode)));
}

TEST_F(TestSharedMetaReservedOperator, reserved_type_key_mode_table_matches_enum)
{
  const struct {
    ObSharedMetaReservedTypeEnum type_enum;
    ObSharedMetaReservedKeyMode expected_key_mode;
  } cases[] = {
    { ObSharedMetaReservedTypeEnum::INVALID, ObSharedMetaReservedKeyMode::NONE },
    { ObSharedMetaReservedTypeEnum::MAX_RESERVED, ObSharedMetaReservedKeyMode::NONE },
    { ObSharedMetaReservedTypeEnum::BACKUP, ObSharedMetaReservedKeyMode::OWNER_KEYED },
    { ObSharedMetaReservedTypeEnum::SNAPSHOT, ObSharedMetaReservedKeyMode::SCN_KEYED },
    { ObSharedMetaReservedTypeEnum::GC, ObSharedMetaReservedKeyMode::SCN_KEYED },
    { ObSharedMetaReservedTypeEnum::LS_CKPT, ObSharedMetaReservedKeyMode::SCN_KEYED },
    { ObSharedMetaReservedTypeEnum::FORK_TABLE, ObSharedMetaReservedKeyMode::OWNER_KEYED },
    { ObSharedMetaReservedTypeEnum::DDL, ObSharedMetaReservedKeyMode::OWNER_KEYED },
    { ObSharedMetaReservedTypeEnum::DATA_CHECK, ObSharedMetaReservedKeyMode::OWNER_KEYED },
    { ObSharedMetaReservedTypeEnum::MAX, ObSharedMetaReservedKeyMode::NONE },
  };
  for (const auto &c : cases) {
    const ObSharedMetaReservedType type(c.type_enum);
    ASSERT_EQ(c.expected_key_mode, shared_meta_reserved_type_key_mode(c.type_enum))
        << "enum key mode mismatch for " << type.to_str();
    ASSERT_EQ(c.expected_key_mode, type.key_mode())
        << "wrapper key mode mismatch for " << type.to_str();
  }
}

// Every public method should fail OB_NOT_INIT before init().
TEST_F(TestSharedMetaReservedOperator, not_init_rejected)
{
  ObSharedMetaReservedOperator op;
  ObSharedMetaReservedInfo info = make_info(1002, ObSharedMetaReservedTypeEnum::BACKUP, scn(1));
  ASSERT_EQ(OB_SUCCESS, info.set_owner(ObString::make_string("backup:1")));
  int64_t affected = 0;
  ASSERT_EQ(OB_NOT_INIT, op.insert_row(info, affected));
  ASSERT_EQ(OB_NOT_INIT, op.delete_row(info, affected));
  ASSERT_EQ(OB_NOT_INIT, op.delete_row(
      1002,
      ObSharedMetaReservedType(ObSharedMetaReservedTypeEnum::BACKUP),
      ObString::make_string("backup:1"),
      affected));
  ObArray<ObSharedMetaReservedInfo> rows;
  ObSharedMetaReservedType empty_type;
  ASSERT_EQ(OB_NOT_INIT, op.scan_rows(1002, empty_type, 0 /*limit_count*/, rows));
}

// insert/delete reject invalid info up-front (no SQL is sent).
TEST_F(TestSharedMetaReservedOperator, insert_delete_invalid_info)
{
  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), TIMEOUT_US));
  int64_t affected = 0;

  ObSharedMetaReservedInfo bad_tenant = make_info(
      OB_INVALID_TENANT_ID, ObSharedMetaReservedTypeEnum::BACKUP, scn(1));
  ASSERT_EQ(OB_SUCCESS, bad_tenant.set_owner(
      ObString::make_string("backup:bad_tenant")));
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.insert_row(bad_tenant, affected));
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.delete_row(bad_tenant, affected));

  ObSharedMetaReservedInfo bad_type = make_info(
      1002, ObSharedMetaReservedTypeEnum::INVALID, scn(1));
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.insert_row(bad_type, affected));
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.delete_row(bad_type, affected));

  const ObSharedMetaReservedType backup_type(ObSharedMetaReservedTypeEnum::BACKUP);
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.delete_row(
      1002, backup_type, ObString(), affected));
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.delete_row(
      1002, ObSharedMetaReservedType(), ObString::make_string("backup:1"), affected));
  const ObSharedMetaReservedType fork_table_type(ObSharedMetaReservedTypeEnum::FORK_TABLE);
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.delete_row(
      1002, fork_table_type, ObString(), affected));
  const ObSharedMetaReservedType ddl_type(ObSharedMetaReservedTypeEnum::DDL);
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.delete_row(
      1002, ddl_type, ObString(), affected));
}

TEST_F(TestSharedMetaReservedOperator, reserved_info_owner_validation)
{
  ObSharedMetaReservedInfo info = make_info(
      1002, ObSharedMetaReservedTypeEnum::BACKUP, scn(1));
  ASSERT_FALSE(info.has_owner());
  ASSERT_FALSE(info.is_valid());
  ASSERT_EQ(OB_SUCCESS, info.set_owner(ObString::make_string("backup:1")));
  ASSERT_TRUE(info.has_owner());
  ASSERT_TRUE(info.is_valid());
  ASSERT_EQ(ObString::make_string("backup:1"), info.get_owner());

  ObSharedMetaReservedInfo fork_table_info = make_info(
      1002, ObSharedMetaReservedTypeEnum::FORK_TABLE, scn(2));
  ASSERT_EQ(OB_SUCCESS, fork_table_info.set_owner(
      ObString::make_string("fork_table:1001")));
  ASSERT_TRUE(fork_table_info.type_.is_fork_table());
  ASSERT_TRUE(fork_table_info.type_.is_owner_keyed());

  ObSharedMetaReservedInfo ddl_info = make_info(
      1002, ObSharedMetaReservedTypeEnum::DDL, scn(3));
  ASSERT_EQ(OB_SUCCESS, ddl_info.set_owner(ObString::make_string("ddl:task:1")));
  ASSERT_TRUE(ddl_info.type_.is_ddl());
  ASSERT_TRUE(ddl_info.type_.is_owner_keyed());

  ObSharedMetaReservedInfo copied;
  ASSERT_EQ(OB_SUCCESS, copied.assign(fork_table_info));
  ASSERT_EQ(fork_table_info, copied);
  ASSERT_EQ(fork_table_info.hash(), copied.hash());
  ASSERT_EQ(OB_SUCCESS, copied.assign(info));
  ASSERT_EQ(info, copied);

  std::string max_owner(SHARED_META_RESERVED_OWNER_MAX_LENGTH, 'a');
  ASSERT_EQ(OB_SUCCESS, info.set_owner(
      ObString(max_owner.length(), max_owner.data())));
  std::string oversized_owner(SHARED_META_RESERVED_OWNER_MAX_LENGTH + 1, 'a');
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.set_owner(
      ObString(oversized_owner.length(), oversized_owner.data())));

  const char owner_with_null[] = {'a', '\0', 'b'};
  ASSERT_EQ(OB_INVALID_ARGUMENT, info.set_owner(
      ObString(sizeof(owner_with_null), owner_with_null)));
  ASSERT_EQ(OB_SUCCESS, info.set_owner(ObString()));
  ASSERT_FALSE(info.has_owner());
  ASSERT_FALSE(info.is_valid());
}

TEST_F(TestSharedMetaReservedOperator, reserved_info_is_valid_key_mode_owner)
{
  ObSharedMetaReservedInfo scn_info = make_info(
      1002, ObSharedMetaReservedTypeEnum::SNAPSHOT, scn(1));
  ASSERT_TRUE(scn_info.is_valid());
  ASSERT_EQ(OB_SUCCESS, scn_info.set_owner(ObString::make_string("snapshot:1")));
  ASSERT_FALSE(scn_info.is_valid());

  ObSharedMetaReservedInfo owner_info = make_info(
      1002, ObSharedMetaReservedTypeEnum::BACKUP, scn(2));
  ASSERT_FALSE(owner_info.is_valid());
  ASSERT_EQ(OB_SUCCESS, owner_info.set_owner(
      ObString::make_string("backup:1001")));
  ASSERT_TRUE(owner_info.is_valid());

  ObSharedMetaReservedInfo max_info = make_info(
      1002, ObSharedMetaReservedTypeEnum::MAX_RESERVED, scn(3));
  ASSERT_TRUE(max_info.is_valid());
}

TEST_F(TestSharedMetaReservedOperator, scan_rows_invalid_tenant)
{
  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), TIMEOUT_US));
  ObArray<ObSharedMetaReservedInfo> rows;
  ObSharedMetaReservedType empty_type;
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.scan_rows(OB_INVALID_TENANT_ID, empty_type, 0 /*limit_count*/, rows));
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.scan_rows(1002, empty_type, -1, rows));
}

TEST_F(TestSharedMetaReservedOperator, get_real_tenant_id_by_tenant_type)
{
  ObSharedMetaReservedOperator op;
  ASSERT_EQ(OB_SUCCESS, op.init(sql_proxy(), TIMEOUT_US));
  uint64_t real_tenant_id = OB_INVALID_TENANT_ID;
  ASSERT_EQ(OB_INVALID_ARGUMENT, op.get_real_tenant_id_(OB_INVALID_TENANT_ID, real_tenant_id));
  ASSERT_EQ(OB_SUCCESS, op.get_real_tenant_id_(OB_SYS_TENANT_ID, real_tenant_id));
  ASSERT_EQ(OB_SYS_TENANT_ID, real_tenant_id);
  ASSERT_EQ(OB_SUCCESS, op.get_real_tenant_id_(gen_meta_tenant_id(1002), real_tenant_id));
  ASSERT_EQ(OB_SYS_TENANT_ID, real_tenant_id);
  // 1002 is the smallest user tenant id; should map to its meta tenant.
  ASSERT_EQ(OB_SUCCESS, op.get_real_tenant_id_(1002, real_tenant_id));
  ASSERT_EQ(gen_meta_tenant_id(1002), real_tenant_id);
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
