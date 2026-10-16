/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/dcl/ob_grant_proxy_resolver.h"

#include "sql/resolver/cmd/ob_mock_stmt.h"
#include "sql/session/ob_sql_session_info.h"
using namespace oceanbase::sql;
using namespace oceanbase::common;
using oceanbase::share::schema::ObUserInfo;

ObGrantProxyResolver::ObGrantProxyResolver(ObResolverParams &params)
    : ObDCLResolver(params)
{
}

ObGrantProxyResolver::~ObGrantProxyResolver()
{
}

// Extract user_name / host_name from a T_CREATE_USER_SPEC node.
// In MySQL mode the node has 5 children: [0]=user(or CURRENT_USER), [3]=host, [4]=auth_plugin.
int ObGrantProxyResolver::resolve_user_node(const ParseNode *user_node,
                                            ObString &user_name,
                                            ObString &host_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(user_node) || OB_UNLIKELY(user_node->type_ != T_CREATE_USER_SPEC)
      || OB_UNLIKELY(user_node->num_child_ < 4) || OB_ISNULL(user_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected user node", K(ret));
  } else if (OB_ISNULL(params_.session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else {
    if (user_node->children_[0]->type_ == T_FUN_SYS_CURRENT_USER) {
      user_name = params_.session_info_->get_user_name();
      host_name = params_.session_info_->get_host_name();
    } else {
      user_name.assign_ptr(const_cast<char *>(user_node->children_[0]->str_value_),
                           static_cast<int32_t>(user_node->children_[0]->str_len_));
      if (NULL != user_node->children_[3]) {
        host_name.assign_ptr(const_cast<char *>(user_node->children_[3]->str_value_),
                             static_cast<int32_t>(user_node->children_[3]->str_len_));
      } else {
        host_name = ObString(OB_DEFAULT_HOST_NAME);
      }
    }
  }
  return ret;
}

// Resolve MySQL-mode `GRANT/REVOKE PROXY` and reuse the ALTER USER PROXY DDL path.
int ObGrantProxyResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode *client_user = NULL;
  ParseNode *proxy_user_list = NULL;
  ParseNode *grant_option = NULL;
  ObAlterUserProxyStmt *stmt = NULL;
  ObMockStmt *mock_grant_option_stmt = NULL;
  const bool is_grant = (parse_tree.type_ == T_GRANT_PROXY);
  if (OB_ISNULL(params_.session_info_) || OB_ISNULL(params_.schema_checker_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(parse_tree.type_ != T_GRANT_PROXY && parse_tree.type_ != T_REVOKE_PROXY)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt type", K(ret), K(parse_tree.type_));
  } else if (OB_UNLIKELY(parse_tree.num_child_ != 3)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected stmt children", K(ret), K(parse_tree.num_child_));
  } else if (!lib::is_mysql_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("grant/revoke proxy is only for mysql mode", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "grant/revoke proxy in oracle mode");
  } else if (is_grant
             && (OB_ISNULL(parse_tree.children_[0])
                 || OB_UNLIKELY(parse_tree.children_[0]->type_ != T_LINK_NODE)
                 || OB_UNLIKELY(parse_tree.children_[0]->num_child_ != 2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected grant proxy privilege node", K(ret));
  } else if (is_grant
             && OB_NOT_NULL(grant_option = parse_tree.children_[0]->children_[1])) {
    if (OB_UNLIKELY(grant_option->type_ != T_PRIV_TYPE || grant_option->value_ != OB_PRIV_GRANT)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected grant option", K(ret), K(grant_option->type_), K(grant_option->value_));
    } else if (OB_ISNULL(mock_grant_option_stmt = create_stmt<ObMockStmt>())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to create mock stmt", K(ret));
    } else {
      // Keep the old MySQL mock behavior: success with "not supported" warning.
      mock_grant_option_stmt->set_stmt_type(stmt::T_GRANT_PROXY);
    }
  } else if (OB_ISNULL(client_user = parse_tree.children_[1])
             || OB_ISNULL(proxy_user_list = parse_tree.children_[2])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null child", K(ret));
  } else if (OB_UNLIKELY(proxy_user_list->type_ != T_USERS || proxy_user_list->num_child_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected proxy user list", K(ret));
  } else if (OB_ISNULL(stmt = create_stmt<ObAlterUserProxyStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create stmt", K(ret));
  } else {
    const uint64_t tenant_id = params_.session_info_->get_effective_tenant_id();
    stmt->set_tenant_id(tenant_id);
    stmt->set_is_grant(is_grant);
    stmt->set_flags(PROXY_USER_ACTIVATE_ALL_ROLES);

    // resolve client user
    ObString client_name;
    ObString client_host;
    const ObUserInfo *client_info = NULL;
    if (OB_FAIL(resolve_user_node(client_user, client_name, client_host))) {
      LOG_WARN("resolve client user failed", K(ret));
    } else if (OB_FAIL(params_.schema_checker_->get_user_info(tenant_id, client_name, client_host, client_info))) {
      LOG_WARN("get client user info failed", K(ret), K(client_name), K(client_host));
    } else if (OB_ISNULL(client_info)) {
      ret = OB_ERR_USER_NOT_EXIST;
      LOG_WARN("client user not exist", K(ret), K(client_name));
    } else if (OB_UNLIKELY(client_info->is_role())) {
      ret = ER_NO_SUCH_USER;
      LOG_WARN("client is a role, not a user", K(ret));
    } else if (OB_FAIL(stmt->add_client_user_id(client_info->get_user_id()))) {
      LOG_WARN("add client user id failed", K(ret));
    } else if (OB_FAIL(stmt->get_ddl_arg().based_schema_object_infos_.push_back(
                   ObBasedSchemaObjectInfo(client_info->get_user_id(), USER_SCHEMA,
                                           client_info->get_schema_version())))) {
      LOG_WARN("push back based schema object info failed", K(ret));
    }

    // resolve proxy users
    for (int64_t i = 0; OB_SUCC(ret) && i < proxy_user_list->num_child_; i++) {
      ObString proxy_name;
      ObString proxy_host;
      const ObUserInfo *proxy_info = NULL;
      if (OB_FAIL(resolve_user_node(proxy_user_list->children_[i], proxy_name, proxy_host))) {
        LOG_WARN("resolve proxy user failed", K(ret), K(i));
      } else if (OB_FAIL(params_.schema_checker_->get_user_info(tenant_id, proxy_name, proxy_host, proxy_info))) {
        LOG_WARN("get proxy user info failed", K(ret), K(proxy_name), K(proxy_host));
      } else if (OB_ISNULL(proxy_info)) {
        ret = OB_ERR_USER_NOT_EXIST;
        LOG_WARN("proxy user not exist", K(ret), K(proxy_name));
      } else if (OB_UNLIKELY(proxy_info->is_role())) {
        ret = ER_NO_SUCH_USER;
        LOG_WARN("proxy is a role, not a user", K(ret));
      } else if (OB_UNLIKELY(proxy_info->get_user_id() == client_info->get_user_id())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("proxy user act as itself is not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "proxy user act as the client itself");
      } else if (OB_FAIL(stmt->add_proxy_user_id(proxy_info->get_user_id()))) {
        LOG_WARN("add proxy user id failed", K(ret));
      } else if (OB_FAIL(stmt->get_ddl_arg().based_schema_object_infos_.push_back(
                     ObBasedSchemaObjectInfo(proxy_info->get_user_id(), USER_SCHEMA,
                                             proxy_info->get_schema_version())))) {
        LOG_WARN("push back based schema object info failed", K(ret));
      }
    }

    // duplicate proxy user check
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt->get_proxy_user_id().count(); i++) {
      for (int64_t j = i + 1; OB_SUCC(ret) && j < stmt->get_proxy_user_id().count(); j++) {
        if (stmt->get_proxy_user_id().at(i) == stmt->get_proxy_user_id().at(j)) {
          ret = OB_ERR_DUPLICATE_USERNAME_IN_LIST;
          LOG_WARN("duplicate proxy user in list", K(ret));
        }
      }
    }
  }
  return ret;
}
