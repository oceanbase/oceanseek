/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX RS

#include "rootserver/fork_table/ob_fork_table_helper.h"
#include "share/schema/ob_table_creator_privilege.h"

#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_sort.h"
#include "observer/ob_inner_sql_connection.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_autoincrement_service.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_server_struct.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_priv_sql_service.h"
#include "share/schema/ob_schema_service.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/tablet/ob_tablet_fork_mds_helper.h"
#include "storage/tx/ob_multi_data_source.h"

namespace oceanbase {
namespace rootserver {

using namespace common;
using namespace share;
using namespace share::schema;
using namespace transaction;

// 把真实库名编码为只匹配该库的 ACL 模式，例如 a_b -> a\_b、% -> \%。
// SQL 字符串转义和 ACL 模式转义是两层不同的事，SQL Service 只负责前者。
int ObForkPrivilegeUtils::encode_database_pattern(const ObString &name, ObSqlString &pattern)
{
  int ret = OB_SUCCESS;
  pattern.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < name.length(); ++i) {
    const char ch = name.ptr()[i];
    if (('%' == ch || '_' == ch || '\\' == ch) && OB_FAIL(pattern.append("\\"))) {
      LOG_WARN("[ForkTable] append escape character failed", KR(ret), K(ch));
    } else if (OB_FAIL(pattern.append(name.ptr() + i, 1))) {
      LOG_WARN("[ForkTable] append character failed", KR(ret), K(ch));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[ForkTable] encoded database pattern", K(name), K(pattern));
  }
  return ret;
}

// 只统计非通配 ACL；目标库按租户规则等价、但现有权限查询无法匹配的 ACL 也必须拒绝。
// get_db_privs_in_tenant 按 tenant_id、user_id 排序；同一用户分别计数源、目标，任一侧超过一条就拒绝。
int ObForkPrivilegeUtils::check_database_privileges(const ObIArray<const ObDBPriv *> &privileges,
                                                    const ObNameCaseMode name_case_mode,
                                                    const ObString &src_database_name,
                                                    const ObString &dst_database_name)
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  int64_t src_match_count = 0;
  int64_t dst_match_count = 0;
  ObCollationType name_collation = CS_TYPE_INVALID;
  if (src_database_name.empty() || dst_database_name.empty()
      || (OB_ORIGIN_AND_SENSITIVE != name_case_mode && OB_LOWERCASE_AND_INSENSITIVE != name_case_mode
          && OB_ORIGIN_AND_INSENSITIVE != name_case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid arguments", KR(ret), K(src_database_name), K(dst_database_name));
  } else {
    name_collation = ObSchema::get_cs_type_with_cmp_mode(name_case_mode);
    for (int64_t i = 0; OB_SUCC(ret) && i < privileges.count(); ++i) {
      const ObDBPriv *priv = privileges.at(i);
      if (OB_ISNULL(priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] database privilege schema is null", KR(ret), K(i));
      } else {
        if (user_id != priv->get_user_id()) {
          user_id = priv->get_user_id();
          src_match_count = 0;
          dst_match_count = 0;
        }
        if (NON_WILDCARD_PATTERN_SORT == priv->get_sort()) {
          src_match_count += (0 == wild_compare(src_database_name, priv->get_database_name_str(), false));
          dst_match_count += (0 == wild_compare(dst_database_name, priv->get_database_name_str(), false));
          // mode 2 的 DstDb/dstdb 在名称层等价，但 wild_compare 区分大小写，可能漏读原 INSERT。
          // 内表主键不区分大小写，漏读后 REPLACE 会覆盖旧行；先用现有字符集模式匹配识别并拒绝。
          // sort 已排除通配 ACL；同拼写的冗余转义仍可由 wild_compare 命中，不应误拒绝。
          if (0 != wild_compare(dst_database_name, priv->get_database_name_str(), false)
              && ObCharset::wildcmp(name_collation, dst_database_name, priv->get_database_name_str(), '\\', '_', '%')) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "COPY GRANTS with a differently cased destination database ACL is");
            LOG_WARN("[ForkTable] destination database ACL spelling differs from fork target", KR(ret), KPC(priv),
                     K(dst_database_name), K(name_case_mode));
          } else if (src_match_count > 1 || dst_match_count > 1) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "COPY GRANTS with multiple exact ACLs for the same grantee/object is");
            LOG_WARN("[ForkTable] ambiguous database ACL", KR(ret), KPC(priv),
                     K(src_database_name), K(dst_database_name), K(src_match_count), K(dst_match_count));
          }
        }
      }
    }
  }
  return ret;
}

// get_table_privs_in_tenant 按 tenant_id、user_id 排序；每个用户的源、目标匹配记录分别计数，超过一条就拒绝。
// 表级名称按租户规则比较；即使目标只有一条等价 ACL，拼写不同也拒绝，不沿用或改写旧 key。
int ObForkPrivilegeUtils::check_table_privileges(const ObIArray<const ObTablePriv *> &privileges,
                                                 const ObNameCaseMode name_case_mode,
                                                 const ObString &src_database_name, const ObString &src_table_name,
                                                 const ObString &dst_database_name, const ObString &dst_table_name)
{
  int ret = OB_SUCCESS;
  uint64_t user_id = OB_INVALID_ID;
  int64_t src_match_count = 0;
  int64_t dst_match_count = 0;
  ObCollationType name_collation = CS_TYPE_INVALID;
  if (src_database_name.empty() || src_table_name.empty() || dst_database_name.empty() || dst_table_name.empty()
      || (OB_ORIGIN_AND_SENSITIVE != name_case_mode && OB_LOWERCASE_AND_INSENSITIVE != name_case_mode
          && OB_ORIGIN_AND_INSENSITIVE != name_case_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid arguments", KR(ret), K(src_database_name), K(src_table_name), K(dst_database_name), K(dst_table_name), K(name_case_mode));
  } else {
    name_collation = ObSchema::get_cs_type_with_cmp_mode(name_case_mode);
    for (int64_t i = 0; OB_SUCC(ret) && i < privileges.count(); ++i) {
      const ObTablePriv *priv = privileges.at(i);
      if (OB_ISNULL(priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] table privilege schema is null", KR(ret), K(i));
      } else {
        if (user_id != priv->get_user_id()) {
          user_id = priv->get_user_id();
          src_match_count = 0;
          dst_match_count = 0;
        }
        // 当前 ACL 属于源表：只增加当前用户的源记录数。
        if (0 == ObCharset::strcmp(name_collation, src_database_name, priv->get_database_name_str())
            && 0 == ObCharset::strcmp(name_collation, src_table_name, priv->get_table_name_str())) {
          ++src_match_count;
        }
        // 当前 ACL 属于目标表：目标单独计数，不与源记录数混在一起。
        if (0 == ObCharset::strcmp(name_collation, dst_database_name, priv->get_database_name_str())
            && 0 == ObCharset::strcmp(name_collation, dst_table_name, priv->get_table_name_str())) {
          ++dst_match_count;
          // 防御 mode 2 已知权限 bug：缓存 key 区分大小写，内部权限表的名称主键却不区分。
          // 将 Copy 改写为 copy 可能覆盖旧权限并形成缓存别名；沿用 Copy 又可能使按 copy 访问时鉴权失败。
          // 因此目标已有等价 ACL 但拼写不同时直接拒绝
          if (dst_database_name != priv->get_database_name_str() || dst_table_name != priv->get_table_name_str()) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "COPY GRANTS with a differently cased destination ACL is");
            LOG_WARN("[ForkTable] destination ACL spelling differs from fork target", KR(ret), KPC(priv),
                     K(dst_database_name), K(dst_table_name));
          }
        }
        if (OB_SUCC(ret) && (src_match_count > 1 || dst_match_count > 1)) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "COPY GRANTS with multiple exact ACLs for the same grantee/object is");
          LOG_WARN("[ForkTable] ambiguous table ACL", KR(ret), KPC(priv), K(src_database_name),
                   K(src_table_name), K(dst_database_name), K(dst_table_name), K(src_match_count), K(dst_match_count));
        }
      }
    }
  }
  return ret;
}

// 初始化权限复制上下文，复用 Fork 的 schema guard 和事务完成权限读写。
ObForkPrivilegeCopier::ObForkPrivilegeCopier(
    const uint64_t tenant_id,
    ObMultiVersionSchemaService &schema_service,
    ObSchemaGetterGuard &schema_guard,
    ObMySQLTransaction &trans,
    const ObString *ddl_stmt_str)
  : tenant_id_(tenant_id),
    schema_service_(schema_service),
    schema_guard_(schema_guard),
    trans_(trans),
    ddl_stmt_str_(ddl_stmt_str)
{
}

// 将源库权限与目标库已有权限按位取并集，仅在目标权限发生变化时写入。
// 写入复用现有权限 SQL Service，并使用 Fork 当前事务和新的 schema version。
// 注意：此处 dst_database_name 已是转义后的精确 ACL 模式
int ObForkPrivilegeCopier::grant_database_privilege_(
    const uint64_t user_id,
    const ObString &dst_database_name,
    const ObPrivSet src_priv_set,
    const ObPrivSet dst_priv_set)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  const ObOriginalDBKey dst_priv_key(tenant_id_, user_id, dst_database_name);
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] schema sql service is null while copying database privilege", KR(ret), K(tenant_id_));
  } else if (OB_INVALID_ID == user_id || dst_database_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid destination database privilege key", KR(ret), K(tenant_id_), K(user_id),
             K(dst_database_name));
  } else if (OB_PRIV_SET_EMPTY == src_priv_set) {
  } else {
    const ObPrivSet merged_priv_set = src_priv_set | dst_priv_set;
    if (merged_priv_set != dst_priv_set) {
      int64_t new_schema_version = OB_INVALID_VERSION;
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id_, new_schema_version))) {
        LOG_WARN("[ForkTable] generate database privilege schema version failed", KR(ret), K(tenant_id_),
                 K(dst_priv_key));
      } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_database(
                     dst_priv_key, merged_priv_set, new_schema_version, ddl_stmt_str_, trans_))) {
        LOG_WARN("[ForkTable] write copied database privilege failed", KR(ret), K(dst_priv_key),
                 K(src_priv_set), K(dst_priv_set), K(merged_priv_set));
      }
    }
  }
  return ret;
}

// 将源表权限与目标表已有权限取并集：Fork 本次新增、扩权记录的 grantor/grantor_host 统一留空。
// 仅在权限变化时写入；现有 SQL Service 同时写 current/history，Oracle 对象权限参数保持为空。
int ObForkPrivilegeCopier::grant_table_privilege_(const uint64_t user_id,
    const ObString &dst_database_name,
    const ObString &dst_table_name,
    const ObPrivSet src_priv_set)
{
  int ret = OB_SUCCESS;
  ObSchemaService *schema_sql_service = schema_service_.get_schema_service();
  const ObTablePrivSortKey dst_priv_key(tenant_id_, user_id, dst_database_name, dst_table_name);
  ObPrivSet dst_priv_set = OB_PRIV_SET_EMPTY;
  if (OB_ISNULL(schema_sql_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] schema sql service is null while copying table privilege", KR(ret), K(tenant_id_));
  } else if (OB_INVALID_ID == user_id || dst_database_name.empty() || dst_table_name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid destination table privilege key", KR(ret), K(tenant_id_), K(user_id),
             K(dst_database_name), K(dst_table_name));
  } else if (OB_PRIV_SET_EMPTY == src_priv_set) {
  } else if (OB_FAIL(schema_guard_.get_table_priv_set(dst_priv_key, dst_priv_set))) {
    LOG_WARN("[ForkTable] get exact destination table privilege failed", KR(ret), K(dst_priv_key));
  } else {
    const ObPrivSet merged_priv_set = src_priv_set | dst_priv_set;
    if (merged_priv_set != dst_priv_set) {
      int64_t new_schema_version = OB_INVALID_VERSION;
      ObRawObjPrivArray empty_obj_privs;
      ObObjPrivSortKey empty_obj_priv_key;
      if (OB_FAIL(schema_service_.gen_new_schema_version(tenant_id_, new_schema_version))) {
        LOG_WARN("[ForkTable] generate table privilege schema version failed", KR(ret), K(tenant_id_),
                 K(dst_priv_key));
      } else if (OB_FAIL(schema_sql_service->get_priv_sql_service().grant_table(
                     dst_priv_key, merged_priv_set, new_schema_version, ddl_stmt_str_, trans_,
                     empty_obj_privs, 0 /* 授权选项 */, empty_obj_priv_key, OB_INVALID_VERSION,
                     true /* 执行授权 */, false /* 非 Oracle 全量撤权 */,
                     ObString() /* 授予者留空 */, ObString() /* 授予者 host 留空 */))) {
        LOG_WARN("[ForkTable] write copied table privilege failed", KR(ret), K(dst_priv_key),
                 K(src_priv_set), K(dst_priv_set), K(merged_priv_set));
      }
    }
  }
  return ret;
}

// 先拒绝重复精确 ACL，再收集去重 user_id，包含角色和匿名用户；不展开继承权限。
int ObForkPrivilegeCopier::get_db_priv_user_ids_(hash::ObHashSet<uint64_t> &db_priv_user_ids,
                                                 const ObString &src_database_name,
                                                 const ObString &dst_database_name)
{
  int ret = OB_SUCCESS;
  ObArray<const ObDBPriv *> tenant_db_privs;
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  if (OB_FAIL(schema_guard_.get_db_priv_with_tenant_id(tenant_id_, tenant_db_privs))) {
    LOG_WARN("[ForkTable] get tenant database privileges failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard_.get_tenant_name_case_mode(tenant_id_, name_case_mode))) {
    LOG_WARN("[ForkTable] get tenant name case mode failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObForkPrivilegeUtils::check_database_privileges(
                 tenant_db_privs, name_case_mode, src_database_name, dst_database_name))) {
    LOG_WARN("[ForkTable] ambiguous database privileges", KR(ret), K(tenant_id_),
             K(src_database_name), K(dst_database_name));
  } else if (OB_FAIL(db_priv_user_ids.create(
                 hash::cal_next_prime(tenant_db_privs.count() + 1), "ForkDbPriv", "ForkDbPriv", OB_SERVER_TENANT_ID))) {
    LOG_WARN("[ForkTable] create database privilege user set failed", KR(ret), K(tenant_id_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_db_privs.count(); ++i) {
      const ObDBPriv *db_priv = tenant_db_privs.at(i);
      if (OB_ISNULL(db_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] database privilege schema is null", KR(ret), K(tenant_id_), K(i));
      } else if (OB_FAIL(db_priv_user_ids.set_refactored(db_priv->get_user_id()))) {
        LOG_WARN("[ForkTable] save database privilege user failed", KR(ret), KPC(db_priv));
      }
    }
  }
  return ret;
}

// 租户库级 ACL 扫描用于拒绝重复精确授权并枚举 user_id，不累加源端权限。
// 源端复用鉴权接口选中一条 ACL，仅复制属于该用户的非通配授权；匿名兜底不转授给普通用户。
// 同一用户对源库或目标库有多条精确匹配 ACL 时直接报错，不依赖不稳定的首条匹配顺序。
// 目标只有一条精确授权时沿用原始 key 追加；所有歧义检查都在写权限前完成。
// 目标没有精确授权时，以真实库名生成标准精确 key，不修改通配或匿名兜底记录。
int ObForkPrivilegeCopier::copy_database_privileges(
    const ObString &fork_src_database_name,
    const ObString &fork_dst_database_name)
{
  int ret = OB_SUCCESS;
  int64_t copied_count = 0;
  hash::ObHashSet<uint64_t> db_priv_user_ids;
  ObSqlString fork_dst_database_acl_pattern;
  if (OB_INVALID_TENANT_ID == tenant_id_ || fork_src_database_name.empty() || fork_dst_database_name.empty()
      || !trans_.is_started()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid argument for copying database privileges", KR(ret), K(tenant_id_),
             K(fork_src_database_name), K(fork_dst_database_name), "trans_started", trans_.is_started());
  } else if (OB_FAIL(ObForkPrivilegeUtils::encode_database_pattern(
                 fork_dst_database_name, fork_dst_database_acl_pattern))) {
    LOG_WARN("[ForkTable] escape destination database privilege pattern failed", KR(ret), K(fork_dst_database_name));
  } else if (OB_FAIL(get_db_priv_user_ids_(db_priv_user_ids, fork_src_database_name, fork_dst_database_name))) {
    LOG_WARN("[ForkTable] get database privilege user ids failed", KR(ret), K(tenant_id_));
  } else {
    ObOriginalDBKey src_key(tenant_id_, OB_INVALID_ID, fork_src_database_name);
    ObOriginalDBKey dst_key(tenant_id_, OB_INVALID_ID, fork_dst_database_name);
    ObString dst_acl_pattern;
    hash::ObHashSet<uint64_t>::iterator it = db_priv_user_ids.begin();
    // 对每个候选用户，按真实库名查询鉴权选中的一条 ACL，不合并其他匹配记录。
    // src_key/dst_key 使用真实库名，接口内部按真实库名查询；缓存中的 ACL 仍按模式匹配。
    // 源 ACL 只复制当前用户的非通配授权，普通用户命中的匿名兜底记录会被跳过。
    // 目标有当前用户的非通配授权时，沿用选中记录的原始 key 并合并权限；否则使用编码后的目标精确 key。
    for (; OB_SUCC(ret) && it != db_priv_user_ids.end(); ++it) {
      const uint64_t user_id = it->first;
      src_key.user_id_ = user_id;
      dst_key.user_id_ = user_id;
      dst_acl_pattern.reset();
      const ObDBPriv *src_priv = nullptr;
      const ObDBPriv *dst_priv = nullptr;
      if (OB_FAIL(schema_guard_.get_db_priv(src_key, src_priv))) {
        LOG_WARN("[ForkTable] get matched source database privilege failed", KR(ret), K(src_key));
      } else if (nullptr == src_priv || user_id != src_priv->get_user_id()
              || ObForkPrivilegeUtils::NON_WILDCARD_PATTERN_SORT != src_priv->get_sort()
              || OB_PRIV_SET_EMPTY == src_priv->get_priv_set()) {
        // 源端仅复制选中的直接、非通配授权，不把其他匹配记录或匿名兜底权限合入。
      } else if (OB_FAIL(schema_guard_.get_db_priv(dst_key, dst_priv))) {
        LOG_WARN("[ForkTable] get matched destination database privilege failed", KR(ret), K(dst_key));
      } else {
        dst_acl_pattern = fork_dst_database_acl_pattern.string();
        ObPrivSet dst_priv_set = OB_PRIV_SET_EMPTY;
        if (nullptr != dst_priv && user_id == dst_priv->get_user_id()
            && ObForkPrivilegeUtils::NON_WILDCARD_PATTERN_SORT == dst_priv->get_sort()) {
          dst_acl_pattern = dst_priv->get_database_name_str();
          dst_priv_set = dst_priv->get_priv_set();
        }
        if (OB_FAIL(grant_database_privilege_(user_id, dst_acl_pattern, src_priv->get_priv_set(), dst_priv_set))) {
          LOG_WARN("[ForkTable] copy database privilege failed", KR(ret), K(user_id),
                   K(fork_dst_database_name), K(dst_acl_pattern));
        } else {
          ++copied_count;
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[ForkTable] copied explicit database privileges", K(tenant_id_), K(fork_src_database_name),
             K(fork_dst_database_name), K(copied_count));
  }
  return ret;
}

// 一次获取租户内全部表级 ACL，只复制库名、表名与源表精确相等的记录。
// 扫描直接覆盖普通用户、角色和匿名用户，不展开它们的有效权限。
int ObForkPrivilegeCopier::copy_table_privileges(
    const ObString &src_database_name,
    const ObString &src_table_name,
    const ObString &dst_database_name,
    const ObString &dst_table_name,
    const uint64_t creator_user_id)
{
  int ret = OB_SUCCESS;
  int64_t copied_count = 0;
  bool creator_copied = false;
  ObArray<const ObTablePriv *> tenant_table_privs;
  ObNameCaseMode name_case_mode = OB_NAME_CASE_INVALID;
  ObCollationType name_collation = CS_TYPE_INVALID;
  if (OB_INVALID_TENANT_ID == tenant_id_ || src_database_name.empty() || src_table_name.empty()
      || dst_database_name.empty() || dst_table_name.empty() || !trans_.is_started()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid argument for copying table privileges", KR(ret), K(tenant_id_),
             K(src_database_name), K(src_table_name), K(dst_database_name), K(dst_table_name),
             "trans_started", trans_.is_started());
  } else if (OB_FAIL(schema_guard_.get_table_priv_with_tenant_id(tenant_id_, tenant_table_privs))) {
    LOG_WARN("[ForkTable] get tenant table privileges failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard_.get_tenant_name_case_mode(tenant_id_, name_case_mode))) {
    LOG_WARN("[ForkTable] get tenant name case mode failed", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(ObForkPrivilegeUtils::check_table_privileges(tenant_table_privs, name_case_mode, src_database_name,
                                                                 src_table_name, dst_database_name, dst_table_name))) {
    LOG_WARN("[ForkTable] ambiguous table privileges", KR(ret), K(tenant_id_));
  } else {
    name_collation = ObSchema::get_cs_type_with_cmp_mode(name_case_mode);
    // 源端与预检使用同一比较规则，避免 mode 2 下单条 ACL 因拼写不同漏复制。
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_table_privs.count(); ++i) {
      const ObTablePriv *table_priv = tenant_table_privs.at(i);
      if (OB_ISNULL(table_priv)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] table privilege schema is null", KR(ret), K(tenant_id_), K(i));
      } else if (0 != ObCharset::strcmp(name_collation, src_database_name, table_priv->get_database_name_str())
              || 0 != ObCharset::strcmp(name_collation, src_table_name, table_priv->get_table_name_str())) {
      } else if (OB_FAIL(grant_table_privilege_(table_priv->get_user_id(), dst_database_name,
                     dst_table_name, table_priv->get_priv_set()
                         | (table_priv->get_user_id() == creator_user_id
                            ? ObTableCreatorPrivilege::PRIVILEGES : OB_PRIV_SET_EMPTY)))) {
        LOG_WARN("[ForkTable] copy table privilege failed", KR(ret), KPC(table_priv),
                 K(src_database_name), K(src_table_name), K(dst_database_name), K(dst_table_name));
      } else {
        creator_copied |= table_priv->get_user_id() == creator_user_id;
        ++copied_count;
      }
    }
  }
  // A second grant using the same schema guard could overwrite the creator's copied privileges.
  if (OB_SUCC(ret) && OB_INVALID_ID != creator_user_id && !creator_copied
      && OB_FAIL(grant_table_privilege_(creator_user_id, dst_database_name, dst_table_name,
                                       ObTableCreatorPrivilege::PRIVILEGES))) {
    LOG_WARN("[ForkTable] grant creator privileges failed", KR(ret), K(creator_user_id));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("[ForkTable] copied explicit table privileges", K(tenant_id_), K(src_database_name),
             K(src_table_name), K(dst_database_name), K(dst_table_name), K(copied_count));
  }
  return ret;
}

int ObForkTableGlobalStatusCopier::build_stat_table_column_list_(
    const char *table_name,
    ObSqlString &column_list)
{
  int ret = OB_SUCCESS;
  uint64_t stat_table_id = OB_INVALID_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *stat_table_schema = nullptr;

  if (OB_ISNULL(table_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid statistics table name", KR(ret), KP(table_name));
  } else if (0 == STRCMP(table_name, OB_ALL_TABLE_STAT_TNAME)) {
    stat_table_id = OB_ALL_TABLE_STAT_TID;
  } else if (0 == STRCMP(table_name, OB_ALL_COLUMN_STAT_TNAME)) {
    stat_table_id = OB_ALL_COLUMN_STAT_TID;
  } else if (0 == STRCMP(table_name, OB_ALL_HISTOGRAM_STAT_TNAME)) {
    stat_table_id = OB_ALL_HISTOGRAM_STAT_TID;
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] unsupported statistics table", KR(ret), K(table_name));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_service_.get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("[ForkTable] failed to get tenant schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, stat_table_id, stat_table_schema))) {
    LOG_WARN("[ForkTable] failed to get statistics table schema",
             KR(ret), K(tenant_id_), K(stat_table_id), K(table_name));
  } else if (OB_ISNULL(stat_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("[ForkTable] statistics table schema is null",
             KR(ret), K(tenant_id_), K(stat_table_id), K(table_name));
  } else if (OB_FAIL(build_stat_table_column_list_(*stat_table_schema, column_list))) {
    LOG_WARN("[ForkTable] failed to build statistics table column list",
             KR(ret), K(stat_table_id), K(table_name));
  }
  return ret;
}

int ObForkTableGlobalStatusCopier::build_stat_table_column_list_(
    const ObTableSchema &stat_table_schema,
    ObSqlString &column_list)
{
  int ret = OB_SUCCESS;
  bool has_table_id = false;
  bool has_partition_id = false;
  bool has_copyable_column = false;
  column_list.reset();

  for (ObTableSchema::const_column_iterator iter = stat_table_schema.column_begin();
       OB_SUCC(ret) && iter != stat_table_schema.column_end(); ++iter) {
    const ObColumnSchemaV2 *column_schema = *iter;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] statistics column schema is null",
               KR(ret), "stat_table_id", stat_table_schema.get_table_id());
    } else {
      const ObString &column_name = column_schema->get_column_name_str();
      if (0 == column_name.compare("table_id")) {
        has_table_id = true;
      } else if (0 == column_name.compare("partition_id")) {
        has_partition_id = true;
      } else if (0 == column_name.compare("gmt_create") || 0 == column_name.compare("gmt_modified")) {
        // Let the destination statistics row use system-maintained timestamps.
      } else if (column_schema->is_shadow_column() || column_schema->is_generated_column()) {
        // Internal, transitional, and generated columns are not stable copy payload.
      } else if (OB_FAIL(column_list.append_fmt("%s%.*s", has_copyable_column ? ", " : "",
                     column_name.length(), column_name.ptr()))) {
        LOG_WARN("[ForkTable] failed to append statistics column", KR(ret), K(column_name));
      } else {
        has_copyable_column = true;
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!has_table_id || !has_partition_id || !has_copyable_column)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] unexpected statistics table schema",
             KR(ret), K(has_table_id), K(has_partition_id), K(has_copyable_column),
             "stat_table_id", stat_table_schema.get_table_id());
  }
  return ret;
}

int ObForkTableGlobalStatusCopier::copy_stat_info_(const char *table_name,
                                               const int64_t src_part_id,
                                               const int64_t dst_part_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const uint64_t src_table_id = src_table_schema_.get_table_id();
  const uint64_t dst_table_id = dst_table_schema_.get_table_id();
  // get_extract_tenant_id(exec_tenant_id, tenant_id): the first argument is the tenant
  // executing the inner SQL, and the second is the logical owner of the statistics.
  // Both are tenant_id_ here, so tenant-local inner-table rowkeys store tenant_id as 0.
  const uint64_t stat_inner_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_);
  ObSqlString table_schema;
  ObSqlString sql_string;

  if (OB_FAIL(build_stat_table_column_list_(table_name, table_schema))) {
    LOG_WARN("[ForkTable] failed to build statistics table column list", KR(ret), KP(table_name));
  } else if (OB_FAIL(sql_string.assign_fmt(
                 "REPLACE INTO %s (table_id, partition_id, %s) "
                 "SELECT %lu, %ld, %s FROM %s "
                 "WHERE tenant_id = %lu AND table_id = %lu AND partition_id = %ld",
                 table_name, table_schema.ptr(), dst_table_id, dst_part_id, table_schema.ptr(),
                 table_name, stat_inner_tenant_id, src_table_id, src_part_id))) {
    LOG_WARN("[ForkTable] failed to assign sql string", KR(ret), K(table_name),
             K(src_table_id), K(src_part_id), K(dst_table_id), K(dst_part_id));
  } else if (OB_FAIL(trans_.write(tenant_id_, sql_string.ptr(), affected_rows))) {
    LOG_WARN("[ForkTable] failed to copy statistics", KR(ret), K(sql_string));
  } else if (OB_UNLIKELY(affected_rows < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] unexpected affected_rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObForkTableGlobalStatusCopier::copy_partition_stat_info_(const char *table_name)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const uint64_t src_table_id = src_table_schema_.get_table_id();
  const uint64_t dst_table_id = dst_table_schema_.get_table_id();
  const bool is_subpartition = PARTITION_LEVEL_TWO == src_table_schema_.get_part_level();
  const uint64_t stat_inner_tenant_id = ObSchemaUtils::get_extract_tenant_id(tenant_id_, tenant_id_);
  ObSqlString table_schema;
  ObSqlString sql_string;

  if (OB_FAIL(build_stat_table_column_list_(table_name, table_schema))) {
    LOG_WARN("[ForkTable] failed to build statistics table column list",
             KR(ret), KP(table_name));
  } else if (is_subpartition && OB_FAIL(sql_string.assign_fmt(
                 // The mapping contains global, partition, and subpartition entries,
                 // so all rows in one statistics table are copied by a single statement.
                 "REPLACE INTO %s (table_id, partition_id, %s)\n"
                 "SELECT %lu, mapping.dst_part_id, %s\n"
                 "FROM %s\n"
                 "JOIN (\n"
                 "  SELECT %ld AS src_part_id, %ld AS dst_part_id\n"  // global stat info
                 "  UNION ALL\n"
                 "  SELECT src_part.part_id, dst_part.part_id\n" // first level partition stat info
                 "  FROM %s src_part\n"
                 "  JOIN %s dst_part ON dst_part.tenant_id = %lu AND dst_part.table_id = %lu AND dst_part.part_name = src_part.part_name\n"
                 "  WHERE src_part.tenant_id = %lu AND src_part.table_id = %lu\n"
                 "  UNION ALL\n"
                 "  SELECT src_subpart.sub_part_id, dst_subpart.sub_part_id\n" // second level subpartition stat info
                 "  FROM %s src_subpart\n"
                 "  JOIN %s src_part ON src_part.tenant_id = src_subpart.tenant_id AND src_part.table_id = src_subpart.table_id AND src_part.part_id = src_subpart.part_id\n"
                 "  JOIN %s dst_part ON dst_part.tenant_id = %lu AND dst_part.table_id = %lu AND dst_part.part_name = src_part.part_name\n"
                 "  JOIN %s dst_subpart ON dst_subpart.tenant_id = dst_part.tenant_id AND dst_subpart.table_id = dst_part.table_id AND dst_subpart.part_id = dst_part.part_id AND dst_subpart.sub_part_name = src_subpart.sub_part_name\n"
                 "  WHERE src_subpart.tenant_id = %lu AND src_subpart.table_id = %lu\n"
                 ") mapping ON mapping.src_part_id = %s.partition_id\n"
                 "WHERE %s.tenant_id = %lu AND %s.table_id = %lu",
                 table_name, table_schema.ptr(),
                 dst_table_id, table_schema.ptr(), table_name,
                 GLOBAL_STAT_PARTITION_ID, GLOBAL_STAT_PARTITION_ID,
                 OB_ALL_PART_TNAME, OB_ALL_PART_TNAME,
                 stat_inner_tenant_id, dst_table_id,
                 stat_inner_tenant_id, src_table_id,
                 OB_ALL_SUB_PART_TNAME, OB_ALL_PART_TNAME,
                 OB_ALL_PART_TNAME, stat_inner_tenant_id, dst_table_id,
                 OB_ALL_SUB_PART_TNAME,
                 stat_inner_tenant_id, src_table_id,
                 table_name,
                 table_name, stat_inner_tenant_id,
                 table_name, src_table_id))) {
    LOG_WARN("[ForkTable] failed to build subpartition stat sql", KR(ret), K(table_name));
  } else if (!is_subpartition && OB_FAIL(sql_string.assign_fmt(
                 // The mapping contains global and partition entries,
                 // so all rows in one statistics table are copied by a single statement.
                 "REPLACE INTO %s (table_id, partition_id, %s)\n"
                 "SELECT %lu, mapping.dst_part_id, %s\n"
                 "FROM %s\n"
                 "JOIN (\n"
                 "  SELECT %ld AS src_part_id, %ld AS dst_part_id\n" // global stat info
                 "  UNION ALL\n"
                 "  SELECT src_part.part_id, dst_part.part_id\n" // first level partition stat info
                 "  FROM %s src_part\n"
                 "  JOIN %s dst_part ON dst_part.tenant_id = %lu AND dst_part.table_id = %lu AND dst_part.part_name = src_part.part_name\n"
                 "  WHERE src_part.tenant_id = %lu AND src_part.table_id = %lu\n"
                 ") mapping ON mapping.src_part_id = %s.partition_id\n"
                 "WHERE %s.tenant_id = %lu AND %s.table_id = %lu",
                 table_name, table_schema.ptr(),
                 dst_table_id, table_schema.ptr(), table_name,
                 GLOBAL_STAT_PARTITION_ID, GLOBAL_STAT_PARTITION_ID,
                 OB_ALL_PART_TNAME, OB_ALL_PART_TNAME,
                 stat_inner_tenant_id, dst_table_id,
                 stat_inner_tenant_id, src_table_id,
                 table_name,
                 table_name, stat_inner_tenant_id,
                 table_name, src_table_id))) {
    LOG_WARN("[ForkTable] failed to build partition stat sql", KR(ret), K(table_name));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(trans_.write(tenant_id_, sql_string.ptr(), affected_rows))) {
      LOG_WARN("[ForkTable] failed to copy partition statistics", KR(ret), K(sql_string), K(is_subpartition));
    } else if (OB_UNLIKELY(affected_rows < 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[ForkTable] unexpected affected_rows", KR(ret), K(affected_rows));
    } else {
      LOG_INFO("[ForkTable] copied partition statistics", K(table_name), K(affected_rows),
               K(is_subpartition), K(src_table_id), K(dst_table_id));
    }
  }
  return ret;
}

int ObForkTableGlobalStatusCopier::copy_table_autoinc()
{
  int ret = OB_SUCCESS;
  const uint64_t src_table_id = src_table_schema_.get_table_id();
  const uint64_t dst_table_id = dst_table_schema_.get_table_id();
  const uint64_t src_autoinc_column_id = src_table_schema_.get_autoinc_column_id();
  const uint64_t dst_autoinc_column_id = dst_table_schema_.get_autoinc_column_id();

  if (OB_INVALID_ID == src_autoinc_column_id || 0 == src_autoinc_column_id) {
    LOG_INFO("[ForkTable] src table has no autoinc column, skip", K(src_table_id));
  } else if (OB_INVALID_ID == dst_autoinc_column_id || 0 == dst_autoinc_column_id) {
    LOG_INFO("[ForkTable] dst table has no autoinc column, skip", K(dst_table_id));
  } else if (OB_UNLIKELY(src_autoinc_column_id != dst_autoinc_column_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] autoinc column id mismatch", KR(ret),
             K(src_autoinc_column_id), K(dst_autoinc_column_id));
  } else {
    const bool is_order_mode = src_table_schema_.is_order_auto_increment_mode();
    const int64_t autoinc_version = src_table_schema_.get_truncate_version();
    uint64_t src_sequence_value = 0;
    share::ObAutoincrementService &autoinc_service =
        share::ObAutoincrementService::get_instance();

    if (OB_FAIL(autoinc_service.get_sequence_value(
            tenant_id_, src_table_id, src_autoinc_column_id, is_order_mode,
            autoinc_version, src_sequence_value))) {
      LOG_WARN("[ForkTable] failed to get auto increment sequence value",
                 KR(ret), K(src_table_id), K(src_autoinc_column_id), K(is_order_mode));
    } else if (src_sequence_value > 0) {
      ObDDLOperator ddl_operator(schema_service_, sql_proxy_);
      const uint64_t sync_value = src_sequence_value - 1;
      if (OB_FAIL(ddl_operator.set_target_auto_inc_sync_value(
              tenant_id_, dst_table_id, dst_autoinc_column_id,
              src_sequence_value, sync_value, trans_))) {
        LOG_WARN("[ForkTable] failed to set auto increment to dst table",
                 KR(ret), K(dst_table_id), K(dst_autoinc_column_id),
                 K(src_sequence_value), K(sync_value));
      } else {
        LOG_INFO("[ForkTable] copied table autoinc info",
                 K(src_table_id), K(dst_table_id),
                 K(src_autoinc_column_id), K(src_sequence_value));
      }
    }
  }
  return ret;
}

int ObForkTableGlobalStatusCopier::execute()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(copy_table_autoinc())) {
    LOG_WARN("[ForkTable] failed to copy table autoinc", KR(ret), K(tenant_id_), "src_table_id",
              src_table_schema_.get_table_id(), "dst_table_id", dst_table_schema_.get_table_id());
  } else if (OB_FAIL(copy_table_statistics())) {
    LOG_WARN("[ForkTable] failed to copy table statistics", KR(ret), K(tenant_id_), "src_table_id",
              src_table_schema_.get_table_id(), "dst_table_id", dst_table_schema_.get_table_id());
  } else {
    LOG_INFO("[ForkTable] copied global table status", K(tenant_id_), "src_table_id",
              src_table_schema_.get_table_id(), "dst_table_id", dst_table_schema_.get_table_id());
  }
  return ret;
}

int ObForkTableGlobalStatusCopier::copy_table_statistics()
{
  int ret = OB_SUCCESS;
  const uint64_t src_table_id = src_table_schema_.get_table_id();
  const uint64_t dst_table_id = dst_table_schema_.get_table_id();
  const bool is_non_part_table = PARTITION_LEVEL_ZERO == src_table_schema_.get_part_level();
  const int64_t part_cnt = is_non_part_table ? 0 : src_table_schema_.get_partition_num();
  const int64_t subpart_cnt = PARTITION_LEVEL_TWO == src_table_schema_.get_part_level() ? src_table_schema_.get_all_part_num() : 0;
  static const char *STAT_TABLE_NAMES[] = {
      OB_ALL_TABLE_STAT_TNAME,
      OB_ALL_COLUMN_STAT_TNAME,
      OB_ALL_HISTOGRAM_STAT_TNAME
  };
  static const int64_t STAT_TABLE_CNT = ARRAYSIZEOF(STAT_TABLE_NAMES);
  for (int64_t j = 0; OB_SUCC(ret) && j < STAT_TABLE_CNT; ++j) {
    if (is_non_part_table) {
      const int64_t src_part_id = src_table_schema_.get_object_id(); // non-part table stat info use table_id (object_id) as partition_id
      const int64_t dst_part_id = dst_table_schema_.get_object_id();
      if (OB_UNLIKELY(src_part_id <= 0 || dst_part_id <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] invalid partition id for non-partitioned table", KR(ret),
                 K(src_part_id), K(dst_part_id), K(src_table_id), K(dst_table_id));
      } else if (OB_FAIL(copy_stat_info_(STAT_TABLE_NAMES[j], src_part_id, dst_part_id))) {
        LOG_WARN("[ForkTable] failed to copy stat info", KR(ret), K(j),
                 K(src_table_id), K(src_part_id), K(dst_table_id), K(dst_part_id));
      }
    } else if (OB_FAIL(copy_partition_stat_info_(STAT_TABLE_NAMES[j]))) {
      LOG_WARN("[ForkTable] failed to copy partition stat info", KR(ret), K(j),
               K(src_table_id), K(dst_table_id));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("[ForkTable] copied statistics", K(src_table_id), K(dst_table_id), K(part_cnt), K(subpart_cnt));
  }
  return ret;
}

int ObForkTableMdsWriter::register_fork_status_mds(
    const uint64_t tenant_id,
    const ObTxDataSourceType type,
    const ObIArray<ObTabletID> &tablet_ids,
    const ObIArray<ObTabletID> *fork_dst_tablet_ids,
    const storage::ObTabletMdsUserDataType data_type,
    const int64_t fork_snapshot_version,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<std::pair<ObLSID, ObTabletID>> ls_tablets;
  const bool need_fork_dst_tablet_ids =
      storage::ObTabletMdsUserDataType::START_FORK_SRC == data_type
      || storage::ObTabletMdsUserDataType::FINISH_FORK_SRC == data_type
      || storage::ObTabletMdsUserDataType::ABORT_FORK_SRC == data_type;
  hash::ObHashMap<ObTabletID, ObTabletID> fork_dst_tablet_map;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || tablet_ids.empty()
                  || (need_fork_dst_tablet_ids
                      && (OB_ISNULL(fork_dst_tablet_ids)
                          || tablet_ids.count() != fork_dst_tablet_ids->count())))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid arg", KR(ret), K(tenant_id), "tablet_cnt", tablet_ids.count(),
        KP(fork_dst_tablet_ids));
  } else if (OB_UNLIKELY(ObTxDataSourceType::FORK_TABLE_SRC_MDS != type
                         && ObTxDataSourceType::FORK_TABLE_FINISH_MDS != type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid fork status mds type", KR(ret), "type", static_cast<int64_t>(type));
  } else if (OB_ISNULL(trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] invalid connection", KR(ret));
  } else if (OB_FAIL(ObTabletBindingMdsHelper::get_sorted_ls_tablets(tenant_id, tablet_ids, ls_tablets, trans))) {
    LOG_WARN("[ForkTable] get sorted ls tablets failed", KR(ret), K(tenant_id));
  } else if (need_fork_dst_tablet_ids
             && OB_FAIL(fork_dst_tablet_map.create(
                    hash::cal_next_prime(tablet_ids.count() + 1), "ForkDstMap"))) {
    LOG_WARN("[ForkTable] create fork dst tablet map failed", KR(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && need_fork_dst_tablet_ids && i < tablet_ids.count(); ++i) {
    if (OB_FAIL(fork_dst_tablet_map.set_refactored(
            tablet_ids.at(i), fork_dst_tablet_ids->at(i)))) {
      LOG_WARN("[ForkTable] set fork dst tablet map failed", KR(ret), K(i),
          "src_tablet_id", tablet_ids.at(i), "dst_tablet_id", fork_dst_tablet_ids->at(i));
    }
  }
  if (OB_SUCC(ret)) {
    storage::ObTabletForkStatusMdsArg arg;
    ObSEArray<ObTabletID, BATCH_TABLET_CNT> batch_tablet_ids;
    ObSEArray<ObTabletID, BATCH_TABLET_CNT> batch_fork_dst_tablet_ids;
    ObTabletID fork_dst_tablet_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablets.count(); ++i) {
      const ObLSID &ls_id = ls_tablets.at(i).first;
      const ObTabletID &tablet_id = ls_tablets.at(i).second;
      const bool is_last_or_next_ls_id_changed = i == ls_tablets.count() - 1
          || ls_id != ls_tablets.at(i + 1).first;
      fork_dst_tablet_id.reset();
      if (need_fork_dst_tablet_ids
          && OB_FAIL(fork_dst_tablet_map.get_refactored(tablet_id, fork_dst_tablet_id))) {
        LOG_WARN("[ForkTable] get fork dst tablet id failed", KR(ret), K(tablet_id));
      } else if (OB_FAIL(batch_tablet_ids.push_back(tablet_id))) {
        LOG_WARN("[ForkTable] push back tablet failed", KR(ret), K(i), K(tablet_id));
      } else if (need_fork_dst_tablet_ids
                 && OB_FAIL(batch_fork_dst_tablet_ids.push_back(fork_dst_tablet_id))) {
        LOG_WARN("[ForkTable] push back fork dst tablet failed",
            KR(ret), K(i), K(fork_dst_tablet_id));
      } else if (is_last_or_next_ls_id_changed || batch_tablet_ids.count() >= BATCH_TABLET_CNT) {
        if (OB_FAIL(arg.init(tenant_id, ls_id, batch_tablet_ids, batch_fork_dst_tablet_ids,
                storage::ObTabletStatus(storage::ObTabletStatus::NORMAL), data_type))) {
          LOG_WARN("[ForkTable] init fork status mds arg failed", KR(ret), K(ls_id), K(data_type));
        } else if (storage::ObTabletMdsUserDataType::START_FORK_SRC == data_type
                   && OB_FAIL(arg.set_fork_src_mds_data(fork_snapshot_version))) {
          LOG_WARN("[ForkTable] set fork src mds data failed", KR(ret), K(fork_snapshot_version));
        } else if (OB_FAIL(storage::ObTabletForkStatusMdsHelper::register_mds(arg, trans, type))) {
          LOG_WARN("[ForkTable] register fork status mds failed", KR(ret), K(arg), "type", static_cast<int64_t>(type));
        } else {
          LOG_INFO("[ForkTable] register fork status mds succ", K(arg), "type", static_cast<int64_t>(type));
        }
        batch_tablet_ids.reuse();
        batch_fork_dst_tablet_ids.reuse();
      }
    }
  }
  return ret;
}

int ObForkTableMdsWriter::register_fork_dest_autoinc_mds(
    const uint64_t tenant_id,
    const uint64_t data_version,
    const common::hash::ObHashMap<ObLSID, obrpc::ObBatchSetTabletAutoincSeqArg> &autoinc_seq_args,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid tenant", KR(ret), K(tenant_id));
  } else if (!autoinc_seq_args.created() || autoinc_seq_args.empty()) {
    // fetch 阶段无 autoinc_seq > 1 的 tablet，跳过 DEST MDS 注册。
  } else if (OB_ISNULL(trans.get_connection())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] invalid connection", KR(ret));
  } else {
    for (common::hash::ObHashMap<ObLSID, obrpc::ObBatchSetTabletAutoincSeqArg>::const_iterator it = autoinc_seq_args.begin();
         OB_SUCC(ret) && it != autoinc_seq_args.end(); ++it) {
      const obrpc::ObBatchSetTabletAutoincSeqArg &seq_arg = it->second;
      if (seq_arg.is_valid()
          && OB_FAIL(storage::ObTabletForkDestMdsHelper::register_autoinc_mds(seq_arg, data_version, trans))) {
        LOG_WARN("[ForkTable] register fork dest autoinc mds failed", KR(ret), K(seq_arg), K(data_version));
      }
    }
  }
  return ret;
}

int ObForkTableMdsWriter::fetch_tablet_autoinc_seq_args(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &src_data_tablets,
    const ObIArray<ObTabletID> &dst_data_tablets,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    ObMySQLTransaction &trans,
    common::hash::ObHashMap<ObLSID, obrpc::ObBatchSetTabletAutoincSeqArg> &autoinc_seq_args)
{
  int ret = OB_SUCCESS;
  // 1. 查出每个 src tablet 所在的 LS，构建 (ls_id, index) 对并按 LS 排序。
  ObArray<ObLSID> src_ls_ids;
  ObArray<std::pair<ObLSID, int64_t>> sorted_items; // (ls_id, original_index)
  if (OB_UNLIKELY(src_data_tablets.count() != dst_data_tablets.count() || src_data_tablets.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid tablet arrays", KR(ret),
             "src_cnt", src_data_tablets.count(), "dst_cnt", dst_data_tablets.count());
  } else if (OB_FAIL(share::ObTabletToLSTableOperator::batch_get_ls(
                 trans, tenant_id, src_data_tablets, src_ls_ids))) {
    LOG_WARN("[ForkTable] batch get src tablet ls failed", KR(ret));
  } else if (OB_UNLIKELY(src_data_tablets.count() != src_ls_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[ForkTable] src tablet/ls count mismatch", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < src_ls_ids.count(); ++i) {
    if (OB_FAIL(sorted_items.push_back(std::make_pair(src_ls_ids.at(i), i)))) {
      LOG_WARN("[ForkTable] push sorted item failed", KR(ret));
    }
  }
  if (OB_SUCC(ret) && sorted_items.count() > 1) {
    lib::ob_sort(sorted_items.begin(), sorted_items.end());
  }

  // 2. 线性扫描，按 LS 攒批发 RPC。
  ObLocationService *location_service = GCTX.location_service_;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(location_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("[ForkTable] location_service is null", KR(ret));
  }
  ObArray<ObTabletID> batch_src;
  ObArray<ObTabletID> batch_dst;
  for (int64_t i = 0; OB_SUCC(ret) && i < sorted_items.count(); ++i) {
    const ObLSID &ls_id = sorted_items.at(i).first;
    const int64_t idx = sorted_items.at(i).second;
    const bool is_batch_end = (i == sorted_items.count() - 1) || ls_id != sorted_items.at(i + 1).first;
    ObAddr leader_addr;
    if (OB_FAIL(batch_src.push_back(src_data_tablets.at(idx)))) {
      LOG_WARN("[ForkTable] push src tablet failed", KR(ret));
    } else if (OB_FAIL(batch_dst.push_back(dst_data_tablets.at(idx)))) {
      LOG_WARN("[ForkTable] push dst tablet failed", KR(ret));
    } else if (!is_batch_end) {
    } else if (OB_FAIL(location_service->get_leader(GCONF.cluster_id, tenant_id, ls_id, true /*force_renew*/, leader_addr))) {
      LOG_WARN("[ForkTable] get leader failed", KR(ret), K(tenant_id), K(ls_id));
    } else {
      const int64_t timeout_us = THIS_WORKER.is_timeout_ts_valid() ? THIS_WORKER.get_timeout_remain() : GCONF.rpc_timeout;
      ObBatchGetTabletAutoincSeqProxy proxy(rpc_proxy, &obrpc::ObSrvRpcProxy::batch_get_tablet_autoinc_seq);
      obrpc::ObBatchGetTabletAutoincSeqArg get_arg;
      get_arg.tenant_id_ = tenant_id;
      get_arg.ls_id_ = ls_id;
      if (OB_FAIL(get_arg.src_tablet_ids_.assign(batch_src))
          || OB_FAIL(get_arg.dest_tablet_ids_.assign(batch_dst))) {
        LOG_WARN("[ForkTable] assign tablet ids failed", KR(ret));
      } else if (OB_FAIL(proxy.call(leader_addr, timeout_us, tenant_id, get_arg))) {
        LOG_WARN("[ForkTable] batch_get rpc failed", KR(ret), K(leader_addr), K(ls_id));
      }
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = proxy.wait())) {
        LOG_WARN("[ForkTable] rpc proxy wait failed", K(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_SUCC(ret)) {
        const auto &results = proxy.get_results();
        if (OB_UNLIKELY(1 != results.count()) || OB_ISNULL(results.at(0))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("[ForkTable] result count not match", KR(ret));
        } else {
          // 同一 LS 的结果直接构建一个 set_arg。
          obrpc::ObBatchSetTabletAutoincSeqArg set_arg;
          set_arg.tenant_id_ = tenant_id;
          set_arg.ls_id_ = ls_id;
          set_arg.is_tablet_creating_ = true;
          const auto *res = results.at(0);
          for (int64_t j = 0; OB_SUCC(ret) && j < res->autoinc_params_.count(); ++j) {
            const share::ObMigrateTabletAutoincSeqParam &p = res->autoinc_params_.at(j);
            if (OB_FAIL(p.ret_code_)) {
              LOG_WARN("[ForkTable] get autoinc failed", KR(ret), K(j));
            } else if (p.autoinc_seq_ > 1) {
              share::ObMigrateTabletAutoincSeqParam param;
              param.src_tablet_id_ = batch_src.at(j);
              param.dest_tablet_id_ = batch_dst.at(j);
              param.autoinc_seq_ = p.autoinc_seq_;
              if (OB_FAIL(set_arg.autoinc_params_.push_back(param))) {
                LOG_WARN("[ForkTable] push param failed", KR(ret));
              }
            }
          }
          if (OB_SUCC(ret) && set_arg.is_valid()) {
            if (OB_FAIL(autoinc_seq_args.set_refactored(ls_id, set_arg))) {
              LOG_WARN("[ForkTable] set autoinc_seq_arg failed", KR(ret), K(ls_id));
            }
          }
        }
      }
    }
    if (is_batch_end) {
      batch_src.reuse();
      batch_dst.reuse();
    }
  }
  return ret;
}

int ObForkDatabaseTaskOperator::lock_database_and_get_recyclebin_flag(
    common::ObMySQLTransaction &trans,
    const uint64_t tenant_id,
    const uint64_t database_id,
    bool &database_exists,
    int64_t &in_recyclebin)
{
  int ret = OB_SUCCESS;
  database_exists = false;
  in_recyclebin = 0;
  const uint64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
  const uint64_t extract_tenant_id = ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id);
  const uint64_t extract_database_id = ObSchemaUtils::get_extract_schema_id(exec_tenant_id, database_id);
  if (OB_UNLIKELY(!trans.is_started() || OB_INVALID_TENANT_ID == tenant_id || OB_INVALID_ID == database_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid database lock argument", KR(ret), K(tenant_id), K(database_id),
             "trans_started", trans.is_started());
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT in_recyclebin FROM %s WHERE tenant_id = %lu AND database_id = %lu FOR UPDATE",
                                 OB_ALL_DATABASE_TNAME, extract_tenant_id, extract_database_id))) {
        LOG_WARN("[ForkTable] build destination state query failed", KR(ret));
      } else if (OB_FAIL(trans.read(res, exec_tenant_id, sql.ptr()))) {
        LOG_WARN("[ForkTable] lock destination database row failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] destination database query result is null", KR(ret), KP(result));
      } else if (OB_FAIL(result->next())) {
        // 数据库行可能已被补偿事务删除，由调用方结合父任务和 child 一并判定。
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[ForkTable] get destination database row failed", KR(ret), K(database_id));
        }
      } else {
        database_exists = true;
        EXTRACT_INT_FIELD_MYSQL(*result, "in_recyclebin", in_recyclebin, int64_t);
      }
    }
  }
  return ret;
}

int ObForkDatabaseTaskOperator::lock_parent_task_and_get_identity(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    bool &task_exists,
    share::ObDDLTaskStatus &task_status,
    share::ObDDLType &ddl_type,
    uint64_t &target_object_id)
{
  int ret = OB_SUCCESS;
  task_exists = false;
  task_status = share::ObDDLTaskStatus::FAIL;
  ddl_type = share::ObDDLType::DDL_INVALID;
  target_object_id = OB_INVALID_ID;
  if (OB_ISNULL(sql_client.get_pool()) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid parent task lock argument", KR(ret), K(tenant_id), K(parent_task_id),
             KP(sql_client.get_pool()));
  } else {
    ObSqlString sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.assign_fmt("SELECT status, ddl_type, target_object_id FROM %s WHERE task_id = %ld FOR UPDATE",
                                 OB_ALL_DDL_TASK_STATUS_TNAME, parent_task_id))) {
        LOG_WARN("[ForkTable] build parent state query failed", KR(ret));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("[ForkTable] lock parent task row failed", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] parent task query result is null", KR(ret), KP(result));
      } else if (OB_FAIL(result->next())) {
        // 父任务行可能已被完整清理，由调用方结合数据库行是否存在一起判定。
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[ForkTable] get parent task row failed", KR(ret), K(parent_task_id));
        }
      } else {
        task_exists = true;
        EXTRACT_INT_FIELD_MYSQL(*result, "status", task_status, share::ObDDLTaskStatus);
        EXTRACT_INT_FIELD_MYSQL(*result, "ddl_type", ddl_type, share::ObDDLType);
        EXTRACT_INT_FIELD_MYSQL(*result, "target_object_id", target_object_id, uint64_t);
      }
    }
  }
  return ret;
}

int ObForkDatabaseTaskOperator::get_cleanup_children(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    common::ObIArray<ForkDatabaseCleanupChild> &children)
{
  int ret = OB_SUCCESS;
  children.reset();
  if (OB_ISNULL(sql_client.get_pool()) || OB_UNLIKELY(tenant_id <= 0 || parent_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid cleanup child query argument", KR(ret), K(tenant_id), K(parent_task_id));
  } else {
    ObSqlString query_sql;
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = nullptr;
      if (OB_FAIL(query_sql.assign_fmt("SELECT task_id, object_id, target_object_id, status FROM %s "
                                       "WHERE parent_task_id = %ld AND ddl_type = %d",
                                       OB_ALL_DDL_TASK_STATUS_TNAME, parent_task_id,
                                       static_cast<int>(DDL_FORK_TABLE)))) {
        LOG_WARN("[ForkTable] build cleanup child query failed", KR(ret), K(parent_task_id));
      } else if (OB_FAIL(sql_client.read(res, tenant_id, query_sql.ptr()))) {
        LOG_WARN("[ForkTable] query cleanup children failed", KR(ret), K(query_sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("[ForkTable] cleanup child query result is null", KR(ret));
      } else {
        while (OB_SUCC(ret)) {
          ForkDatabaseCleanupChild child;
          if (OB_FAIL(result->next())) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            }
            break;
          }
          EXTRACT_INT_FIELD_MYSQL(*result, "task_id", child.task_id_, int64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "object_id", child.src_table_id_, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "target_object_id", child.dst_table_id_, uint64_t);
          EXTRACT_INT_FIELD_MYSQL(*result, "status", child.status_, share::ObDDLTaskStatus);
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(child.task_id_ <= 0 || 0 == child.src_table_id_
                                 || OB_INVALID_ID == child.src_table_id_ || 0 == child.dst_table_id_
                                 || OB_INVALID_ID == child.dst_table_id_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("[ForkTable] invalid cleanup child identity", KR(ret), K(child));
          } else if (OB_FAIL(children.push_back(child))) {
            LOG_WARN("[ForkTable] append cleanup child failed", KR(ret), K(child));
          }
        }
      }
    }
  }
  return ret;
}

int ObForkDatabaseTaskOperator::update_cleanup_children_status(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    const share::ObDDLTaskStatus old_status,
    const share::ObDDLTaskStatus new_status,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString update_sql;
  affected_rows = 0;
  const bool is_valid_transition =
      (ObDDLTaskStatus::FORK_TABLE_WRITE_REORG == old_status
       && ObDDLTaskStatus::FORK_TABLE_CLEANUP_MDS_FINISHED == new_status)
      || (ObDDLTaskStatus::FORK_TABLE_CLEANUP_MDS_FINISHED == old_status
          && ObDDLTaskStatus::FORK_TABLE_CLEANUP_RESOURCES_RELEASED == new_status);
  if (OB_ISNULL(sql_client.get_pool()) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid cleanup child status update argument", KR(ret), K(tenant_id), K(parent_task_id),
             K(old_status), K(new_status), KP(sql_client.get_pool()));
  } else if (OB_UNLIKELY(!is_valid_transition)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid cleanup child status transition", KR(ret), K(parent_task_id), K(old_status),
             K(new_status));
  } else if (OB_FAIL(update_sql.assign_fmt(
          "UPDATE %s SET status = %ld WHERE parent_task_id = %ld AND ddl_type = %ld AND status = %ld",
          OB_ALL_DDL_TASK_STATUS_TNAME, static_cast<int64_t>(new_status), parent_task_id,
          static_cast<int64_t>(DDL_FORK_TABLE), static_cast<int64_t>(old_status)))) {
    LOG_WARN("[ForkTable] build cleanup child status update SQL failed", KR(ret), K(parent_task_id), K(old_status),
             K(new_status));
  } else if (OB_FAIL(sql_client.write(tenant_id, update_sql.ptr(), affected_rows))) {
    LOG_WARN("[ForkTable] update cleanup child status failed", KR(ret), K(update_sql), K(old_status), K(new_status));
  }
  return ret;
}

int ObForkDatabaseTaskOperator::delete_cleanup_children(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_sql;
  affected_rows = 0;
  if (OB_ISNULL(sql_client.get_pool()) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid child task cleanup argument", KR(ret), K(tenant_id), K(parent_task_id),
             KP(sql_client.get_pool()));
  } else if (OB_FAIL(delete_sql.assign_fmt(
          "DELETE FROM %s WHERE parent_task_id = %ld AND ddl_type = %ld",
          OB_ALL_DDL_TASK_STATUS_TNAME, parent_task_id, static_cast<int64_t>(DDL_FORK_TABLE)))) {
    LOG_WARN("[ForkTable] build child task cleanup SQL failed", KR(ret), K(parent_task_id));
  } else if (OB_FAIL(sql_client.write(tenant_id, delete_sql.ptr(), affected_rows))) {
    LOG_WARN("[ForkTable] delete child task rows failed", KR(ret), K(delete_sql));
  }
  return ret;
}

int ObForkDatabaseTaskOperator::delete_parent_task(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t parent_task_id,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString delete_sql;
  affected_rows = 0;
  if (OB_ISNULL(sql_client.get_pool()) || OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || parent_task_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[ForkTable] invalid parent task cleanup argument", KR(ret), K(tenant_id), K(parent_task_id),
             KP(sql_client.get_pool()));
  } else if (OB_FAIL(delete_sql.assign_fmt("DELETE FROM %s WHERE task_id = %ld",
                                           OB_ALL_DDL_TASK_STATUS_TNAME, parent_task_id))) {
    LOG_WARN("[ForkTable] build parent task cleanup SQL failed", KR(ret), K(parent_task_id));
  } else if (OB_FAIL(sql_client.write(tenant_id, delete_sql.ptr(), affected_rows))) {
    LOG_WARN("[ForkTable] delete parent task row failed", KR(ret), K(delete_sql));
  }
  return ret;
}

} // namespace rootserver
} // namespace oceanbase
