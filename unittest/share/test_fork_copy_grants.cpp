/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#include <gtest/gtest.h>
#include "share/ob_rpc_struct.h"
#include "rootserver/fork_table/ob_fork_table_helper.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_cluster_version.h"
#include "share/schema/ob_priv_mgr.h"
#include "sql/resolver/ddl/ob_fork_table_resolver.h"
#include "sql/resolver/cmd/ob_merge_table_resolver.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/session/ob_sql_session_info.h"
#include "lib/utility/utility.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share::schema;
using rootserver::ObForkPrivilegeUtils;

// 固定旧版 v1 布局，验证普通 Fork 仍兼容；旧端会忽略 COPY GRANTS 尾字段，因此必须在 SQL 端设闸门。
struct LegacyForkTableArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  uint64_t tenant_id_ = 1002;
  ObString src_database_name_ = ObString::make_string("src");
  ObString src_table_name_ = ObString::make_string("t");
  ObString dst_database_name_ = ObString::make_string("dst");
  ObString dst_table_name_ = ObString::make_string("t");
  int64_t session_id_ = 12;
  bool need_lock_src_table_ = true;
};
OB_SERIALIZE_MEMBER((LegacyForkTableArg, ObDDLArg), tenant_id_, src_database_name_, src_table_name_,
                    dst_database_name_, dst_table_name_, session_id_, need_lock_src_table_);

struct LegacyForkDatabaseArg : ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  uint64_t tenant_id_ = 1002;
  ObString src_database_name_ = ObString::make_string("src");
  ObString dst_database_name_ = ObString::make_string("dst");
  int64_t session_id_ = 12;
};
OB_SERIALIZE_MEMBER((LegacyForkDatabaseArg, ObDDLArg), tenant_id_, src_database_name_, dst_database_name_, session_id_);

// 保持标准 v1 兼容规则，不再依赖 RPC 解码拒绝旧端；新节点往返仍应完整保留新增字段。
template <typename NewArg, typename OldArg>
void check_protocol(NewArg &arg)
{
  char buf[8192];
  int64_t size = 0;
  OldArg old_arg;
  NewArg decoded;
  ASSERT_EQ(OB_SUCCESS, old_arg.serialize(buf, sizeof(buf), size));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, size, pos));
  EXPECT_FALSE(decoded.copy_grants_);
  EXPECT_EQ(old_arg.tenant_id_, decoded.tenant_id_);
  EXPECT_EQ(old_arg.src_database_name_, decoded.src_database_name_);
  EXPECT_EQ(old_arg.dst_database_name_, decoded.dst_database_name_);

  for (const bool copy : {false, true}) {
    arg.copy_grants_ = copy;
    size = 0;
    ASSERT_EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), size));
    EXPECT_EQ(size, arg.get_serialize_size());
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, size, pos));
    EXPECT_EQ(copy, decoded.copy_grants_);
    EXPECT_EQ(size, pos);
    pos = 0;
    EXPECT_EQ(OB_SUCCESS, old_arg.deserialize(buf, size, pos));
    if (!copy) {
      EXPECT_EQ(arg.src_database_name_, old_arg.src_database_name_);
      EXPECT_EQ(arg.dst_database_name_, old_arg.dst_database_name_);
    }
  }

  // 标准 UNIS 仍拒绝截断和未知版本，不再测试自定义 v2 协议。
  pos = 0;
  EXPECT_NE(OB_SUCCESS, decoded.deserialize(buf, size - 1, pos));
  buf[0] = 3;
  pos = 0;
  EXPECT_EQ(OB_NOT_SUPPORTED, decoded.deserialize(buf, size, pos));
}

TEST(TestForkCopyGrants, table_protocol)
{
  ObForkTableArg arg;
  arg.tenant_id_ = 1002;
  arg.src_database_name_ = ObString::make_string("src");
  arg.src_table_name_ = ObString::make_string("t");
  arg.dst_database_name_ = ObString::make_string("dst");
  arg.dst_table_name_ = ObString::make_string("copy");
  arg.need_lock_src_table_ = true;
  check_protocol<ObForkTableArg, LegacyForkTableArg>(arg);

  char buf[8192];
  int64_t size = 0;
  arg.copy_grants_ = true;
  ASSERT_EQ(OB_SUCCESS, arg.serialize(buf, sizeof(buf), size));
  int64_t pos = 0;
  ObForkTableArg decoded;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf, size, pos));
  EXPECT_TRUE(decoded.copy_grants_);
  EXPECT_TRUE(decoded.is_valid()); // COPY GRANTS 不再要求 RPC 携带 Fork 执行者 ID。
  EXPECT_TRUE(decoded.need_lock_src_table_);
  ObForkTableArg assigned;
  ASSERT_EQ(OB_SUCCESS, assigned.assign(arg));
  EXPECT_TRUE(assigned.copy_grants_);
  EXPECT_TRUE(assigned.is_valid());
}

TEST(TestForkCopyGrants, database_protocol)
{
  ObForkDatabaseArg arg;
  arg.tenant_id_ = 1002;
  arg.src_database_name_ = ObString::make_string("src");
  arg.dst_database_name_ = ObString::make_string("dst");
  check_protocol<ObForkDatabaseArg, LegacyForkDatabaseArg>(arg);
}

// 直接调用两个 resolver 的入口。故意不提供 stmt_factory：越过版本闸门后应到达建 stmt 阶段，
// 返回 OB_ALLOCATE_MEMORY_FAILED；低版本的普通 Fork 和 COPY GRANTS 都必须更早返回 OB_NOT_SUPPORTED。
// 此测试只验证入口闸门，不把“到达后续阶段”误认为完整 SQL 执行成功。
template <typename Resolver>
int resolve_version_gate(const ObItemType type, const bool copy_grants,
                         const bool read_lock = false, const bool schema_only = false)
{
  ObArenaAllocator allocator;
  sql::ObSQLSessionInfo session;
  int ret = session.set_tenant(ObString::make_string("fork_test"), 1002);
  if (OB_SUCCESS == ret) {
    sql::ObResolverParams params;
    params.allocator_ = &allocator;
    params.session_info_ = &session;
    ParseNode option = {};
    ParseNode *children[Resolver::MAX_NODE] = {};
    children[Resolver::COPY_GRANTS_NODE] = copy_grants ? &option : nullptr;
    if (T_FORK_TABLE == type) {
      children[sql::ObForkTableResolver::READ_LOCK_NODE] = read_lock ? &option : nullptr;
    } else if (T_FORK_DATABASE == type) {
      children[sql::ObForkDatabaseResolver::SCHEMA_ONLY_NODE] = schema_only ? &option : nullptr;
    }
    ParseNode tree = {};
    tree.type_ = type;
    tree.num_child_ = Resolver::MAX_NODE;
    tree.children_ = children;
    Resolver resolver(params);
    ret = resolver.resolve(tree);
  }
  return ret;
}

template <typename Resolver>
void check_data_version_gate(const ObItemType type)
{
  lib::CompatModeGuard mode(lib::Worker::CompatMode::MYSQL);
  struct ResetMockVersion {
    ~ResetMockVersion() { ODV_MGR.set_mock_data_version(0); }
  } reset_mock;
  for (const uint64_t version : {DATA_VERSION_4_6_2_1, DATA_VERSION_4_6_2_2,
                                DATA_VERSION_4_6_3_0, DATA_VERSION_4_6_3_1}) {
    SCOPED_TRACE(version);
    ODV_MGR.set_mock_data_version(version);
    // 4.6.3.1 introduces SCHEMA ONLY; every Fork form must obey the same upgrade gate.
    const int expected = version < DATA_VERSION_4_6_3_1 ? OB_NOT_SUPPORTED : OB_ALLOCATE_MEMORY_FAILED;
    // 普通 Fork 与 COPY GRANTS 共用入口门禁，省略子句或使用 READ LOCK 都不能绕过版本检查。
    EXPECT_EQ(expected, resolve_version_gate<Resolver>(type, true));
    EXPECT_EQ(expected, resolve_version_gate<Resolver>(type, false));
    if (T_FORK_TABLE == type) {
      EXPECT_EQ(expected, resolve_version_gate<Resolver>(type, true, true));
      EXPECT_EQ(expected, resolve_version_gate<Resolver>(type, false, true));
    } else if (T_FORK_DATABASE == type) {
      EXPECT_EQ(expected, resolve_version_gate<Resolver>(type, true, false, true));
      EXPECT_EQ(expected, resolve_version_gate<Resolver>(type, false, false, true));
    }
  }
  // 未能取得 data version 时，两种 Fork 都应原样返回错误，不能把“不知道版本”当成允许执行。
  ODV_MGR.set_mock_data_version(0);
  uint64_t version = 0;
  const int version_ret = GET_MIN_DATA_VERSION(1002, version);
  ASSERT_NE(OB_SUCCESS, version_ret);
  EXPECT_EQ(version_ret, resolve_version_gate<Resolver>(type, true));
  EXPECT_EQ(version_ret, resolve_version_gate<Resolver>(type, false));
  if (T_FORK_TABLE == type) {
    EXPECT_EQ(version_ret, resolve_version_gate<Resolver>(type, true, true));
    EXPECT_EQ(version_ret, resolve_version_gate<Resolver>(type, false, true));
  } else if (T_FORK_DATABASE == type) {
    EXPECT_EQ(version_ret, resolve_version_gate<Resolver>(type, true, false, true));
    EXPECT_EQ(version_ret, resolve_version_gate<Resolver>(type, false, false, true));
  }
}

TEST(TestForkCopyGrants, table_data_version_gate)
{
  check_data_version_gate<sql::ObForkTableResolver>(T_FORK_TABLE);
}

TEST(TestForkCopyGrants, database_data_version_gate)
{
  check_data_version_gate<sql::ObForkDatabaseResolver>(T_FORK_DATABASE);
}

// MERGE introduces serialized expressions too: every strategy must pass the
// tenant data-version gate before allocating a statement or looking up tables.
TEST(TestForkCopyGrants, merge_table_data_version_gate)
{
  lib::CompatModeGuard mode(lib::Worker::CompatMode::MYSQL);
  struct ResetMockVersion {
    ~ResetMockVersion() { ODV_MGR.set_mock_data_version(0); }
  } reset_mock;
  ObArenaAllocator allocator;
  sql::ObSQLSessionInfo session;
  sql::ObSchemaChecker checker;
  ASSERT_EQ(OB_SUCCESS, session.set_tenant(ObString::make_string("merge_test"), 1002));
  sql::ObResolverParams params;
  params.allocator_ = &allocator;
  params.session_info_ = &session;
  params.schema_checker_ = &checker;
  ParseNode strategy = {};
  ParseNode *children[4] = {nullptr, nullptr, &strategy, nullptr};
  ParseNode tree = {};
  tree.type_ = T_MERGE_TABLE;
  tree.num_child_ = 4;
  tree.children_ = children;
  for (const uint64_t version : {DATA_VERSION_4_6_3_0, DATA_VERSION_4_6_3_1}) {
    SCOPED_TRACE(version);
    ODV_MGR.set_mock_data_version(version);
    for (int64_t i = 0; i < 3; ++i) {
      SCOPED_TRACE(i);
      strategy.value_ = i;
      sql::ObMergeTableResolver resolver(params);
      EXPECT_EQ(version < DATA_VERSION_4_6_3_1 ? OB_NOT_SUPPORTED : OB_ALLOCATE_MEMORY_FAILED,
                resolver.resolve(tree));
    }
  }
  ODV_MGR.set_mock_data_version(0);
  uint64_t version = 0;
  const int version_ret = GET_MIN_DATA_VERSION(1002, version);
  ASSERT_NE(OB_SUCCESS, version_ret);
  for (int64_t i = 0; i < 3; ++i) {
    strategy.value_ = i;
    sql::ObMergeTableResolver resolver(params);
    EXPECT_EQ(version_ret, resolver.resolve(tree));
  }
}

// 直接验证编码字节，而非只做往返校验，避免 encode/decode 同时出错却互相抵消。
// raw string 中的反斜杠就是 ACL 实际字节，不是 SQL 字符串转义。
TEST(TestForkCopyGrants, encode_database_pattern_literals)
{
  const struct {
    const char *name_;
    const char *pattern_;
  } cases[] = {
    {"", ""},
    {"abc", "abc"},
    {"a_b", R"(a\_b)"},
    {"a%b", R"(a\%b)"},
    {R"(a\b)", R"(a\\b)"},
    {R"(\)", R"(\\)"},
    {R"(a\)", R"(a\\)"},
    {"%_", R"(\%\_)"},
    {"%%__", R"(\%\%\_\_)"},
    {R"(%_\)", R"(\%\_\\)"},
    {R"(\%_)", R"(\\\%\_)"},
    {R"(a\_b)", R"(a\\\_b)"},
    {"订单_明细%", R"(订单\_明细\%)"},
  };
  for (const auto &item : cases) {
    SCOPED_TRACE(item.name_);
    ObSqlString pattern;
    ASSERT_EQ(OB_SUCCESS, pattern.append("old_output"));
    ASSERT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::encode_database_pattern(
        ObString::make_string(item.name_), pattern));
    EXPECT_EQ(ObString::make_string(item.pattern_), pattern.string());
  }
}

// 验证非通配符 ACL 的排序值和实际匹配，包括冗余转义、中文及尾部单个反斜杠。
// 与生产逻辑一致：先判断 sort，再用 wild_compare 匹配真实库名，不自行解码。
TEST(TestForkCopyGrants, database_pattern_exact_matching)
{
  const struct {
    const char *pattern_;
    const char *name_;
  } cases[] = {
    {"", ""},
    {"abc", "abc"},
    {R"(a\_b)", "a_b"},
    {R"(a\%b)", "a%b"},
    {R"(\%)", "%"},
    {R"(\_)", "_"},
    {R"(\\)", R"(\)"},
    {R"(a\\b)", R"(a\b)"},
    {R"(\)", R"(\)"},
    {R"(a\)", R"(a\)"},
    {R"(\\\)", R"(\\)"},
    {R"(\abc)", "abc"},
    {R"(\c\g\_dst)", "cg_dst"},
    {R"(\\\%)", R"(\%)"},
    {R"(\\\_)", R"(\_)"},
    {R"(订单\_明细\%)", "订单_明细%"},
  };
  ObString pattern;
  ObString name;
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    SCOPED_TRACE(cases[i].pattern_);
    pattern = ObString::make_string(cases[i].pattern_);
    name = ObString::make_string(cases[i].name_);
    EXPECT_EQ(pattern.empty() ? 0U : 128U, get_sort(pattern));
    EXPECT_EQ(0, wild_compare(name, pattern, false));
    if (!pattern.empty()) {
      EXPECT_NE(0, wild_compare(ObString::make_string("otherdb"), pattern, false));
    }
  }
  EXPECT_NE(0, wild_compare(ObString::make_string("ABC"), ObString::make_string("abc"), false));
}

// 只要仍有未转义 %/_ 就不能作为精确授权复制；连续反斜杠的奇偶性不能误判。
// wild_compare 本身允许通配匹配，因此仅凭匹配成功不能决定复制。
TEST(TestForkCopyGrants, database_pattern_wildcards_excluded)
{
  for (const char *pattern : {"%", "_", "src_db", "src%", "a_%", "订单_明细",
                             R"(a\_%)", R"(a\%_)", R"(\\%)", R"(\\_)",
                             R"(\\\%%)", R"(\\\_%)", R"(\%suffix_)"}) {
    SCOPED_TRACE(pattern);
    EXPECT_GT(get_sort(ObString::make_string(pattern)), 0U);
    EXPECT_LT(get_sort(ObString::make_string(pattern)), 128U);
  }
  EXPECT_EQ(0, wild_compare(ObString::make_string("src_db"), ObString::make_string("src_db"), false));
  EXPECT_NE(128U, get_sort(ObString::make_string("src_db")));
}

// 目标编码输出会被复用，不能残留上一次内容；每次编码后直接校验非通配符属性及匹配。
TEST(TestForkCopyGrants, database_pattern_reused_outputs)
{
  ObSqlString pattern;
  ASSERT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::encode_database_pattern(ObString::make_string("a_b"), pattern));
  EXPECT_EQ(ObString::make_string(R"(a\_b)"), pattern.string());
  EXPECT_EQ(128U, get_sort(pattern.string()));
  EXPECT_EQ(0, wild_compare(ObString::make_string("a_b"), pattern.string(), false));
  ASSERT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::encode_database_pattern(ObString::make_string("%"), pattern));
  EXPECT_EQ(ObString::make_string(R"(\%)"), pattern.string());
  EXPECT_EQ(128U, get_sort(pattern.string()));
  EXPECT_EQ(0, wild_compare(ObString::make_string("%"), pattern.string(), false));
  EXPECT_NE(0, wild_compare(ObString::make_string("a_b"), pattern.string(), false));
  ASSERT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::encode_database_pattern(ObString::make_string(""), pattern));
  EXPECT_TRUE(pattern.empty());
  EXPECT_EQ(0U, get_sort(pattern.string()));
}

// 穷举 0～4 字符的 a/%/_/反斜杠组合（共 341 个），检查编码永远得到精确模式且不会扩大匹配范围。
TEST(TestForkCopyGrants, database_pattern_short_strings_matching)
{
  const char alphabet[] = {'a', '%', '_', '\\'};
  ObSqlString name;
  ObSqlString pattern;
  for (int64_t length = 0, combinations = 1; length <= 4; ++length, combinations *= 4) {
    for (int64_t value = 0; value < combinations; ++value) {
      SCOPED_TRACE(::testing::Message() << "length=" << length << ", value=" << value);
      name.reset();
      int64_t digits = value;
      for (int64_t i = 0; i < length; ++i, digits /= 4) {
        ASSERT_EQ(OB_SUCCESS, name.append(alphabet + digits % 4, 1));
      }
      ASSERT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::encode_database_pattern(name.string(), pattern));
      EXPECT_EQ(name.empty() ? 0U : 128U, get_sort(pattern.string()));
      if (!name.empty()) {
        EXPECT_EQ(0, wild_compare(name.string(), pattern.string(), false));
        EXPECT_NE(0, wild_compare(ObString::make_string("otherdb"), pattern.string(), false));
      }
    }
  }
}

// 长串用于覆盖反复 append 后的缓冲区扩容，不依赖数据库名的 SQL 长度限制。
TEST(TestForkCopyGrants, database_pattern_long_string_matching)
{
  ObSqlString name;
  ObSqlString pattern;
  for (int64_t i = 0; i < 256; ++i) {
    ASSERT_EQ(OB_SUCCESS, name.append(R"(%_\abc)"));
  }
  ASSERT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::encode_database_pattern(name.string(), pattern));
  EXPECT_EQ(9 * 256, pattern.length());
  EXPECT_EQ(128U, get_sort(pattern.string()));
  EXPECT_EQ(0, wild_compare(name.string(), pattern.string(), false));
  EXPECT_NE(0, wild_compare(ObString::make_string("otherdb"), pattern.string(), false));
}

// 通配符位置超过 127 也只能得到 127，不能与非通配符模式的 128 混淆。
TEST(TestForkCopyGrants, database_pattern_sort_boundary)
{
  ObSqlString pattern;
  for (int64_t i = 0; i < 128; ++i) {
    ASSERT_EQ(OB_SUCCESS, pattern.append("a"));
  }
  EXPECT_EQ(128U, get_sort(pattern.string()));
  ASSERT_EQ(OB_SUCCESS, pattern.append("%"));
  EXPECT_EQ(127U, get_sort(pattern.string()));
  pattern.reset();
  EXPECT_EQ(0U, get_sort(pattern.string()));
  ASSERT_EQ(OB_SUCCESS, pattern.append("%"));
  EXPECT_EQ(1U, get_sort(pattern.string()));
  pattern.reset();
  ASSERT_EQ(OB_SUCCESS, pattern.append("cg_src"));
  EXPECT_EQ(3U, get_sort(pattern.string()));
}

// 不同原始 key 精确匹配同一库时，即使权限相同也拒绝；检查结果不依赖缓存顺序。
TEST(TestForkCopyGrants, reject_duplicate_database_source_acls)
{
  ObDBPriv normal;
  ObDBPriv alias;
  ObArray<const ObDBPriv *> privileges;
  ASSERT_EQ(OB_SUCCESS, normal.set_database_name("b"));
  ASSERT_EQ(OB_SUCCESS, alias.set_database_name(R"(\b)"));
  normal.set_user_id(100);
  alias.set_user_id(100);
  normal.set_priv_set(OB_PRIV_SELECT);
  alias.set_priv_set(OB_PRIV_SELECT);
  normal.set_sort(get_sort(normal.get_database_name_str()));
  alias.set_sort(get_sort(alias.get_database_name_str()));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&normal));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&alias));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_database_privileges(privileges, OB_ORIGIN_AND_SENSITIVE, "b", "dst"));
  privileges.reset();
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&alias));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&normal));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_database_privileges(privileges, OB_ORIGIN_AND_SENSITIVE, "b", "dst"));
  // 两个 grantee 各有一条精确授权，不是歧义；匿名用户自己的重复授权仍要拒绝。
  alias.set_user_id(101);
  privileges.reset();
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&normal));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&alias));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_database_privileges(privileges, OB_ORIGIN_AND_SENSITIVE, "b", "dst"));
  normal.set_user_id(OB_EMPTY_USER_ID);
  alias.set_user_id(OB_EMPTY_USER_ID);
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_database_privileges(privileges, OB_ORIGIN_AND_SENSITIVE, "b", "dst"));
}

TEST(TestForkCopyGrants, reject_duplicate_database_destination_acls)
{
  ObDBPriv source;
  ObDBPriv normal;
  ObDBPriv alias;
  ObArray<const ObDBPriv *> privileges;
  ASSERT_EQ(OB_SUCCESS, source.set_database_name("src"));
  ASSERT_EQ(OB_SUCCESS, normal.set_database_name(R"(cg\_dst)"));
  ASSERT_EQ(OB_SUCCESS, alias.set_database_name(R"(\cg\_dst)"));
  source.set_user_id(100);
  normal.set_user_id(100);
  alias.set_user_id(100);
  normal.set_priv_set(OB_PRIV_INSERT);
  alias.set_priv_set(OB_PRIV_SELECT);
  source.set_sort(get_sort(source.get_database_name_str()));
  normal.set_sort(get_sort(normal.get_database_name_str()));
  alias.set_sort(get_sort(alias.get_database_name_str()));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&source));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&normal));
  // 一条源授权加一条目标授权属于正常合并，不能错误地算成两条重复授权。
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_database_privileges(privileges, OB_ORIGIN_AND_SENSITIVE, "src", "cg_dst"));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&alias));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_database_privileges(privileges, OB_ORIGIN_AND_SENSITIVE, "src", "cg_dst"));
  ASSERT_EQ(OB_SUCCESS, alias.set_database_name("cg_dst")); // 未转义 _ 是通配 ACL，不参与计数。
  alias.set_sort(get_sort(alias.get_database_name_str()));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_database_privileges(privileges, OB_ORIGIN_AND_SENSITIVE, "src", "cg_dst"));
  ASSERT_EQ(OB_SUCCESS, alias.set_database_name(R"(\other)"));
  alias.set_sort(get_sort(alias.get_database_name_str()));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_database_privileges(privileges, OB_ORIGIN_AND_SENSITIVE, "src", "cg_dst"));
}

// 单条目标预授权 DstDb 在 mode 1/2 下不能因字节查找漏读而被 dstdb 的 REPLACE 覆盖。
TEST(TestForkCopyGrants, reject_differently_cased_database_destination_acl)
{
  ObDBPriv source;
  ObDBPriv destination;
  ObArray<const ObDBPriv *> privileges;
  ASSERT_EQ(OB_SUCCESS, source.set_database_name("aclemptysrc"));
  ASSERT_EQ(OB_SUCCESS, destination.set_database_name("DstDb"));
  source.set_user_id(100);
  destination.set_user_id(100);
  source.set_priv_set(OB_PRIV_SELECT);
  destination.set_priv_set(OB_PRIV_INSERT);
  source.set_sort(get_sort(source.get_database_name_str()));
  destination.set_sort(get_sort(destination.get_database_name_str()));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&source));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&destination));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_database_privileges(
      privileges, OB_ORIGIN_AND_SENSITIVE, "aclemptysrc", "dstdb"));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_database_privileges(
      privileges, OB_LOWERCASE_AND_INSENSITIVE, "aclemptysrc", "dstdb"));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_database_privileges(
      privileges, OB_ORIGIN_AND_INSENSITIVE, "aclemptysrc", "dstdb"));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_database_privileges(
      privileges, OB_ORIGIN_AND_INSENSITIVE, "aclemptysrc", "DstDb"));
  EXPECT_EQ(OB_INVALID_ARGUMENT, ObForkPrivilegeUtils::check_database_privileges(
      privileges, OB_NAME_CASE_INVALID, "aclemptysrc", "dstdb"));
}

// 名称等价检查复用字符集模式匹配，不自行解码；转义、尾部反斜杠及通配排除保持原语义。
TEST(TestForkCopyGrants, database_destination_case_guard_handles_acl_patterns)
{
  const struct {
    const char *pattern_;
    const char *destination_;
    int expected_ret_;
  } cases[] = {
    {R"(\DstDb)", "dstdb", OB_NOT_SUPPORTED},
    {R"(Dst\_Db)", "dst_db", OB_NOT_SUPPORTED},
    {R"(Dst\%Db)", "dst%db", OB_NOT_SUPPORTED},
    {R"(A\\B)", R"(a\b)", OB_NOT_SUPPORTED},
    {"Ab\\", "ab\\", OB_NOT_SUPPORTED},
    {R"(\cg\_dst)", "cg_dst", OB_SUCCESS},
    {R"(\%)", "%", OB_SUCCESS},
    {R"(a\)", R"(a\)", OB_SUCCESS},
    {"Dst%", "dstdb", OB_SUCCESS},
    {"Dst_Db", "dst_db", OB_SUCCESS},
    {"OtherDb", "dstdb", OB_SUCCESS},
  };
  ObDBPriv privilege;
  ObArray<const ObDBPriv *> privileges;
  privilege.set_user_id(100);
  privilege.set_priv_set(OB_PRIV_INSERT);
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&privilege));
  for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
    SCOPED_TRACE(cases[i].pattern_);
    ASSERT_EQ(OB_SUCCESS, privilege.set_database_name(cases[i].pattern_));
    privilege.set_sort(get_sort(privilege.get_database_name_str()));
    EXPECT_EQ(cases[i].expected_ret_, ObForkPrivilegeUtils::check_database_privileges(
        privileges, OB_ORIGIN_AND_INSENSITIVE, "source", cases[i].destination_));
  }
}

// 固定表名，只改变库名大小写，单独验证库名比较，不让表名差异掩盖结果。
TEST(TestForkCopyGrants, reject_duplicate_table_database_case_aliases)
{
  ObTablePriv normal;
  ObTablePriv alias;
  ObArray<const ObTablePriv *> privileges;
  ASSERT_EQ(OB_SUCCESS, normal.set_database_name("GrantDb"));
  ASSERT_EQ(OB_SUCCESS, normal.set_table_name("Target"));
  ASSERT_EQ(OB_SUCCESS, alias.set_database_name("grantdb"));
  ASSERT_EQ(OB_SUCCESS, alias.set_table_name("Target"));
  normal.set_user_id(100);
  alias.set_user_id(100);
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&normal));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&alias));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_table_privileges(
      privileges, OB_ORIGIN_AND_INSENSITIVE, "GrantDb", "Target", "Dst", "copy"));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_table_privileges(
      privileges, OB_ORIGIN_AND_INSENSITIVE, "grantdb", "Target", "Dst", "copy"));
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_table_privileges(
      privileges, OB_LOWERCASE_AND_INSENSITIVE, "GrantDb", "Target", "Dst", "copy"));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_table_privileges(
      privileges, OB_ORIGIN_AND_SENSITIVE, "GrantDb", "Target", "Dst", "copy"));
  // 目标大小写等价的预授权也拒绝；只存在不同用户的授权时允许。
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_table_privileges(
      privileges, OB_ORIGIN_AND_INSENSITIVE, "Src", "t", "GrantDb", "Target"));
  alias.set_user_id(101);
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_table_privileges(
      privileges, OB_ORIGIN_AND_INSENSITIVE, "GrantDb", "Target", "Dst", "copy"));
}

// 固定库名，只改变表名；源、目标各自验证两种拼写，mode 0 允许，mode 1/2 拒绝重复 ACL。
TEST(TestForkCopyGrants, reject_duplicate_table_name_case_aliases)
{
  ObTablePriv normal;
  ObTablePriv alias;
  ObArray<const ObTablePriv *> privileges;
  const ObNameCaseMode modes[] = {
      OB_ORIGIN_AND_SENSITIVE, OB_LOWERCASE_AND_INSENSITIVE, OB_ORIGIN_AND_INSENSITIVE};
  ASSERT_EQ(OB_SUCCESS, normal.set_database_name("GrantDb"));
  ASSERT_EQ(OB_SUCCESS, normal.set_table_name("Target"));
  ASSERT_EQ(OB_SUCCESS, alias.set_database_name("GrantDb"));
  ASSERT_EQ(OB_SUCCESS, alias.set_table_name("target"));
  normal.set_user_id(100);
  alias.set_user_id(100);
  normal.set_priv_set(OB_PRIV_INSERT);
  alias.set_priv_set(OB_PRIV_SELECT);
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&normal));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&alias));
  for (int64_t i = 0; i < ARRAYSIZEOF(modes); ++i) {
    const int expected_ret = OB_ORIGIN_AND_SENSITIVE == modes[i] ? OB_SUCCESS : OB_NOT_SUPPORTED;
    EXPECT_EQ(expected_ret, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "GrantDb", "Target", "Dst", "copy"));
    EXPECT_EQ(expected_ret, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "GrantDb", "target", "Dst", "copy"));
    EXPECT_EQ(expected_ret, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "Src", "t", "GrantDb", "Target"));
    EXPECT_EQ(expected_ret, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "Src", "t", "GrantDb", "target"));
  }
}

// 单条目标 ACL 也必须与 Fork 目标拼写一致；mode 0 的不同名称不是同一对象，不误拒绝。
TEST(TestForkCopyGrants, reject_single_destination_acl_case_alias)
{
  ObTablePriv privilege;
  ObArray<const ObTablePriv *> privileges;
  const ObNameCaseMode modes[] = {
      OB_ORIGIN_AND_SENSITIVE, OB_LOWERCASE_AND_INSENSITIVE, OB_ORIGIN_AND_INSENSITIVE};
  ASSERT_EQ(OB_SUCCESS, privilege.set_database_name("GrantDst"));
  ASSERT_EQ(OB_SUCCESS, privilege.set_table_name("Copy"));
  privilege.set_user_id(100);
  privilege.set_priv_set(OB_PRIV_INSERT);
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&privilege));
  for (int64_t i = 0; i < ARRAYSIZEOF(modes); ++i) {
    const int expected_ret = OB_ORIGIN_AND_SENSITIVE == modes[i] ? OB_SUCCESS : OB_NOT_SUPPORTED;
    // 分别只改变目标表名、目标库名，避免两个比较条件互相掩盖。
    EXPECT_EQ(expected_ret, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "Src", "t", "GrantDst", "copy"));
    EXPECT_EQ(expected_ret, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "Src", "t", "grantdst", "Copy"));
    EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "Src", "t", "GrantDst", "Copy"));
    // 相同差异出现在源端时不拒绝，复制循环负责按 mode 匹配源 ACL。
    EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "grantdst", "copy", "Dst", "new_copy"));
  }
}

// 目标计数已为 1 时，后续无关 ACL 不能被误判成目标的拼写差异；不同用户各一条也不算重复。
TEST(TestForkCopyGrants, unrelated_table_acls_do_not_trigger_destination_case_error)
{
  ObTablePriv target;
  ObTablePriv unrelated;
  ObTablePriv source;
  ObTablePriv other_user_target;
  ObArray<const ObTablePriv *> privileges;
  const ObNameCaseMode modes[] = {
      OB_ORIGIN_AND_SENSITIVE, OB_LOWERCASE_AND_INSENSITIVE, OB_ORIGIN_AND_INSENSITIVE};
  ASSERT_EQ(OB_SUCCESS, target.set_database_name("GrantDst"));
  ASSERT_EQ(OB_SUCCESS, target.set_table_name("Copy"));
  ASSERT_EQ(OB_SUCCESS, unrelated.set_database_name("GrantDst"));
  ASSERT_EQ(OB_SUCCESS, unrelated.set_table_name("Other"));
  ASSERT_EQ(OB_SUCCESS, source.set_database_name("Src"));
  ASSERT_EQ(OB_SUCCESS, source.set_table_name("t"));
  ASSERT_EQ(OB_SUCCESS, other_user_target.set_database_name("GrantDst"));
  ASSERT_EQ(OB_SUCCESS, other_user_target.set_table_name("Copy"));
  target.set_user_id(100);
  unrelated.set_user_id(100);
  source.set_user_id(100);
  other_user_target.set_user_id(101);
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&target));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&unrelated));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&source));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&other_user_target));
  for (int64_t i = 0; i < ARRAYSIZEOF(modes); ++i) {
    EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_table_privileges(
        privileges, modes[i], "Src", "t", "GrantDst", "Copy"));
  }
}

TEST(TestForkCopyGrants, table_acl_names_are_literals_and_null_records_fail)
{
  ObTablePriv literal;
  ObTablePriv other;
  ObArray<const ObTablePriv *> privileges;
  ASSERT_EQ(OB_SUCCESS, literal.set_database_name("db"));
  ASSERT_EQ(OB_SUCCESS, literal.set_table_name("t_%"));
  ASSERT_EQ(OB_SUCCESS, other.set_database_name("db"));
  ASSERT_EQ(OB_SUCCESS, other.set_table_name("table"));
  literal.set_user_id(100);
  other.set_user_id(100);
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&literal));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(&other));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_table_privileges(
      privileges, OB_ORIGIN_AND_INSENSITIVE, "db", "t_%", "dst", "copy"));
  ASSERT_EQ(OB_SUCCESS, privileges.push_back(nullptr));
  EXPECT_EQ(OB_ERR_UNEXPECTED, ObForkPrivilegeUtils::check_table_privileges(
      privileges, OB_ORIGIN_AND_INSENSITIVE, "db", "t_%", "dst", "copy"));
}

// 按用户交错插入缓存，验证现有枚举接口会把同一用户的 ACL 放在一起，计数不会漏掉重复或串到其他用户。
TEST(TestForkCopyGrants, cache_acl_lists_group_records_by_user)
{
  ObPrivMgr priv_mgr;
  ObDBPriv db_priv;
  ObTablePriv table_priv;
  ObArray<const ObDBPriv *> db_privileges;
  ObArray<const ObTablePriv *> table_privileges;
  ASSERT_EQ(OB_SUCCESS, priv_mgr.init());
  db_priv.set_tenant_id(1002);
  db_priv.set_user_id(100);
  db_priv.set_priv_set(OB_PRIV_SELECT);
  ASSERT_EQ(OB_SUCCESS, db_priv.set_database_name("b"));
  ASSERT_EQ(OB_SUCCESS, priv_mgr.add_db_priv(db_priv));
  db_priv.set_user_id(200);
  ASSERT_EQ(OB_SUCCESS, priv_mgr.add_db_priv(db_priv));
  ASSERT_EQ(OB_SUCCESS, priv_mgr.get_db_privs_in_tenant(1002, db_privileges));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_database_privileges(db_privileges, OB_ORIGIN_AND_SENSITIVE, "b", "dst"));
  db_priv.set_user_id(100);
  ASSERT_EQ(OB_SUCCESS, db_priv.set_database_name(R"(\b)"));
  ASSERT_EQ(OB_SUCCESS, priv_mgr.add_db_priv(db_priv));
  ASSERT_EQ(OB_SUCCESS, priv_mgr.get_db_privs_in_tenant(1002, db_privileges));
  ASSERT_EQ(3, db_privileges.count());
  EXPECT_EQ(100U, db_privileges.at(0)->get_user_id());
  EXPECT_EQ(100U, db_privileges.at(1)->get_user_id());
  EXPECT_EQ(200U, db_privileges.at(2)->get_user_id());
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_database_privileges(db_privileges, OB_ORIGIN_AND_SENSITIVE, "b", "dst"));

  table_priv.set_tenant_id(1002);
  table_priv.set_user_id(100);
  table_priv.set_priv_set(OB_PRIV_INSERT);
  ASSERT_EQ(OB_SUCCESS, table_priv.set_database_name("GrantDb"));
  ASSERT_EQ(OB_SUCCESS, table_priv.set_table_name("Target"));
  ASSERT_EQ(OB_SUCCESS, priv_mgr.add_table_priv(table_priv));
  table_priv.set_user_id(200);
  ASSERT_EQ(OB_SUCCESS, priv_mgr.add_table_priv(table_priv));
  ASSERT_EQ(OB_SUCCESS, priv_mgr.get_table_privs_in_tenant(1002, table_privileges));
  EXPECT_EQ(OB_SUCCESS, ObForkPrivilegeUtils::check_table_privileges(
      table_privileges, OB_ORIGIN_AND_INSENSITIVE, "GrantDb", "Target", "dst", "copy"));
  table_priv.set_user_id(100);
  ASSERT_EQ(OB_SUCCESS, table_priv.set_table_name("target"));
  ASSERT_EQ(OB_SUCCESS, priv_mgr.add_table_priv(table_priv));
  ASSERT_EQ(OB_SUCCESS, priv_mgr.get_table_privs_in_tenant(1002, table_privileges));
  ASSERT_EQ(3, table_privileges.count());
  EXPECT_EQ(100U, table_privileges.at(0)->get_user_id());
  EXPECT_EQ(100U, table_privileges.at(1)->get_user_id());
  EXPECT_EQ(200U, table_privileges.at(2)->get_user_id());
  EXPECT_EQ(OB_NOT_SUPPORTED, ObForkPrivilegeUtils::check_table_privileges(
      table_privileges, OB_ORIGIN_AND_INSENSITIVE, "GrantDb", "Target", "dst", "copy"));
}

} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
