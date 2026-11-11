/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX RS

#include "src/rootserver/ob_tenant_parallel_create_executor.h"

#include "rootserver/ob_root_service.h"
#include "rootserver/ob_tenant_ddl_service.h"
#include "rootserver/ob_ddl_service.h"
#include "share/ls/ob_ls_life_manager.h"
#include "share/ls/ob_ls_creator.h"
#include "share/backup/ob_backup_config.h"
#include "share/location_cache/ob_location_service.h"
#include "share/location_cache/ob_tablet_location_broadcast.h"
#include "rootserver/restore/ob_tenant_clone_util.h"
#include "share/ob_primary_zone_util.h"
#include "rootserver/ob_tenant_thread_helper.h"
#include "share/ls/ob_ls_operator.h"
#include "share/ls/ob_ls_status_operator.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_user_table_schema_applier.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/reserve_shared_meta/ob_shared_meta_reserved_mgr.h"
#endif
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_schema_service.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_get_compat_mode.h"
#include "share/ob_tenant_info_proxy.h"
#include "share/ob_max_id_fetcher.h"
#include "share/ob_define.h"
#include "lib/profile/ob_trace_id.h"
#include "lib/worker.h"
#include "sql/session/ob_sql_session_info.h"
#include "observer/ob_server.h"

namespace oceanbase
{
using namespace obrpc;
using namespace share;
namespace rootserver
{

namespace
{
bool is_balance_user_ls_for_tablet_alloc(
    const uint64_t tenant_id,
    const share::ObLSAttr &ls_attr)
{
  return ls_attr.ls_is_normal()
      && ls_attr.get_ls_id().is_user_ls()
      && !ls_attr.get_ls_flag().is_block_tablet_in()
      && !ls_attr.get_ls_flag().is_duplicate_ls();
}

// is_user_session_terminated() (declared in ob_load_sys_package_task.h) checks
// the original user session state so that the wait loops in this file fail
// fast once the client disconnects, instead of polling until the whole DDL
// budget is exhausted. For callers without any user session (e.g.
// restore/clone scheduler threads), THIS_WORKER.get_session() is NULL and the
// check is skipped.

template <typename Executor>
int retry_operation(const common::ObTimeoutCtx &ctx, const char *operation_name, Executor *executor,
    int (Executor::*operation)())
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int last_ret = OB_SUCCESS;
  int64_t retry_count = 0;
  const int64_t retry_interval_us = 200L * 1000L; // 200ms
  while (OB_SUCC(ret)) {
    if (ctx.is_timeouted()) {
      ret = OB_TIMEOUT;
      LOG_WARN("retry operation timeout", KR(ret), KR(last_ret), K(operation_name), K(retry_count), K(ctx));
    } else if (is_user_session_terminated(ret)) {
      // is_user_session_terminated has logged the interrupt reason; stop retrying.
      LOG_WARN("user session is terminated, stop operation retry",
          KR(ret), K(operation_name), K(retry_count));
    } else if (OB_TMP_FAIL((executor->*operation)())) {
      if (OB_ERR_SESSION_INTERRUPTED == tmp_ret || OB_ERR_QUERY_INTERRUPTED == tmp_ret) {
        // A terminated user session never recovers; stop retrying immediately
        // instead of masking the interrupt cause until the ctx timeout.
        ret = tmp_ret;
        LOG_WARN("user session is terminated, stop operation retry",
            KR(ret), K(operation_name), K(retry_count));
      } else {
        last_ret = tmp_ret;
        ++retry_count;
        LOG_WARN("failed to execute operation, retry",
            KR(tmp_ret), K(operation_name), K(retry_count), K(ctx));
        ob_usleep(retry_interval_us);
      }
    } else {
      break;
    }
  }
  return ret;
}

} // namespace

/* The reconstruction of zone_list follows the following rules:
 * 1. When creating tenant, the zone_list specified by the user will be ignored. Use the zone list of resource_pool.
 *  We still retain the zone_list column in the all_tenant table and the zone_list field in the tenantSchema.
 * 2. When creating table, the zone_list specified by the user will be ignored.
 *  If the create table does not specify locality, the locality of the table is not filled in,
 *  and the zone_list is not filled in.
 *  Both locality and zone_list are inherited from the tenant to which the table belongs.
 * 3. When creating table, the user specified zone_list and locality, the user-specified zone_list will be ignored;
 *  a zone list will be calculated based on the user-specified locality and the zone list of the resource pool,
 *  and the zone_list will be filled into the zone_list column of __all_table and in the zone_list field of TableSchema.
 * 4. When modifying locality, we are not allowed to modify F{3}@region_hz to F{2}@region_hz;
 *  because the semantics of this modification is not clear enough,
 *  it is impossible to clearly derive the change of zone
 */
int ObParallelCreateTenantExecutor::execute(obrpc::UInt64 &tenant_id)
{ 
  const int64_t start_time = ObTimeUtility::fast_current_time();
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  TIMEGUARD_INIT(create_tenant, 15_s);
  FLOG_INFO("[CREATE_TENANT] STEP 1. start create tenant", K(create_tenant_arg_));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (OB_FAIL(check_user_session_interrupted_())) {
    LOG_WARN("failed to check user session interrupted", KR(ret));
  } else if (GCONF.in_upgrade_mode()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("create tenant is not allowed in upgrade mode", KR(ret), K(GCONF.in_upgrade_mode()));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "in upgrade mode, CREATE TENANT is");
  } else if (CLICK_FAIL(common_rpc_->to_rs(*rs_mgr_).timeout(ctx_.get_timeout())
        .create_tenant(create_tenant_arg_, create_tenant_schema_result_))) {
    LOG_WARN("failed to create tenant schema", KR(ret), K(create_tenant_arg_), K(ctx_));
  } else if (create_tenant_schema_result_.tenant_exist_) {
    if (!create_tenant_arg_.if_not_exist_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant_exist without if_not_exist_, the rpc create_tenant_schema should fail",
          KR(ret), K(create_tenant_arg_), K(create_tenant_schema_result_));
    } else {
      LOG_INFO("tenant exists, not need to create tenant", KR(ret));
    }
  } else if (OB_FAIL(init_after_create_tenant_schema_())) {
    LOG_WARN("failed to init after create tenant schema", KR(ret));
  } else if (OB_FAIL(save_data_version_in_palf_kv_())) {
    LOG_WARN("failed to store data version in palf kv", KR(ret));
  } else {
    ObParallelCreateNormalTenantProxy proxy(*common_rpc_, &ObCommonRpcProxy::parallel_create_normal_tenant);
    bool use_shared_storage = false;
#ifdef OB_BUILD_SHARED_STORAGE
    use_shared_storage = GCTX.is_shared_storage_mode();
#endif
    if (use_shared_storage) {
#ifdef OB_BUILD_SHARED_STORAGE
      if (OB_FAIL(create_sys_ls_and_normal_tenant_for_ss_(tenant_id, proxy))) {
        LOG_WARN("failed to create sys ls and normal tenant for ss", KR(ret));
      }
#endif
    } else {
      if (OB_FAIL(create_sys_ls_and_normal_tenant_for_sn_(tenant_id, proxy))) {
        LOG_WARN("failed to create sys ls and normal tenant for sn", KR(ret));
      }
    }
    if (is_valid_tenant_id(tenant_id)) {
      // STEP 4-5: create user ls
      if (OB_SUCC(ret)) {
        // SS mode: meta tenant user LS was already created as STEP 3.2 inside
        // create_sys_ls_and_normal_tenant_for_ss_.
        if (!use_shared_storage) {
          FLOG_INFO("[CREATE_TENANT] STEP 4. start create meta tenant user ls");
          if (CLICK_FAIL(create_meta_tenant_user_ls_(proxy))) {
            if (OB_CANCELED == ret) {
              LOG_WARN("previous error occured, create meta tenant user ls is canceled", KR(ret));
            } else {
              LOG_WARN("failed to create meta tenant user ls", KR(ret));
            }
          } else {
            FLOG_INFO("[CREATE_TENANT] STEP 4. finish create meta tenant user ls", KR(ret));
          }
        }
        if (OB_SUCC(ret)) {
          FLOG_INFO("[CREATE_TENANT] STEP 5. start create user tenant user ls");
          if (CLICK_FAIL(create_user_ls_(proxy))) {
            if (OB_CANCELED == ret) {
              LOG_WARN("previous error occured, create user ls is canceled", KR(ret));
            } else {
              LOG_WARN("failed to wait and init user tenant", KR(ret));
            }
          } else {
            FLOG_INFO("[CREATE_TENANT] STEP 5. finish create user tenant user ls", KR(ret));
          }
        }
      }
      // STEP 6: wait parallel create normal tenant
      FLOG_INFO("[CREATE_TENANT] STEP 6. start wait parallel create normal tenant", KR(ret));
      if (CLICK_TMP_FAIL(wait_all_(proxy, ret))) {
        FLOG_INFO("[CREATE_TENANT] STEP 6. finish wait parallel create normal tenant", KR(ret));
        LOG_WARN("failed to wait all", KR(ret), KR(tmp_ret));
        ret = (OB_SUCC(ret) || ret == OB_CANCELED) ? tmp_ret : ret;
      } else {
        FLOG_INFO("[CREATE_TENANT] STEP 6. finish wait parallel create normal tenant", KR(ret));
      }
      // STEP 7: create user tables for meta tenant
      // SS mode: already done as STEP 3.3 inside create_sys_ls_and_normal_tenant_for_ss_.
      if (OB_SUCC(ret)) {
        if (!use_shared_storage) {
          FLOG_INFO("[CREATE_TENANT] STEP 7. start create user tables for meta tenant", K(tenant_id));
          if (CLICK_TMP_FAIL(create_user_table_for_meta_tenant_(tenant_id))) {
            LOG_WARN("failed to create user tables for meta tenant", KR(ret), KR(tmp_ret), K(tenant_id));
            ret = tmp_ret;
          } else {
            FLOG_INFO("[CREATE_TENANT] STEP 7. finish create user tables for meta tenant", KR(ret), K(tenant_id));
          }
        }
      }
      // Primary user tenants create their own LOB Control auxiliary table. Standby
      // tenants receive this schema through primary-cluster schema replay.
      if (OB_SUCC(ret) && create_tenant_arg_.get_tenant_role().is_primary()) {
        FLOG_INFO("[CREATE_TENANT] STEP 7.1. start create builtin user tenant tables", K(tenant_id));
        if (CLICK_TMP_FAIL(create_user_table_for_user_tenant_(tenant_id))) {
          LOG_WARN("failed to create builtin user tenant tables", KR(ret), KR(tmp_ret), K(tenant_id));
          ret = tmp_ret;
        } else {
          FLOG_INFO("[CREATE_TENANT] STEP 7.1. finish create builtin user tenant tables",
              KR(ret), K(tenant_id));
        }
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 8. start finish create tenant", KR(ret));
  if (CLICK_TMP_FAIL(finish_create_tenant_(ret))) {
    LOG_WARN("failed to finish create tenant", KR(tmp_ret), KR(ret));
    ret = OB_SUCC(ret) ? tmp_ret : ret;
  }
  FLOG_INFO("[CREATE_TENANT] STEP 8. finish create tenant", KR(ret), K(tenant_id), K(create_tenant_arg_),
      "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObParallelCreateTenantExecutor::create_sys_ls_and_normal_tenant_for_ss_(
    obrpc::UInt64 &tenant_id,
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  FLOG_INFO("[CREATE_TENANT] STEP 2. start create meta tenant sys ls in shared storage mode");
  if (OB_FAIL(create_meta_tenant_sys_ls_())) {
    LOG_WARN("failed to create meta tenant sys ls", KR(ret));
  } else {
    FLOG_INFO("[CREATE_TENANT] STEP 2. finish create meta tenant sys ls", KR(ret));
  }
#ifdef OB_BUILD_TDE_SECURITY
  if (OB_SUCC(ret) && OB_FAIL(upload_root_key_())) {
    LOG_WARN("failed to upload root key", KR(ret));
  }
#endif
  if (OB_SUCC(ret)) {
    tenant_id = user_tenant_schema_.get_tenant_id();
    FLOG_INFO("[CREATE_TENANT] STEP 3.1. start sync create meta normal tenant");
    if (OB_FAIL(call_create_normal_tenant_(meta_tenant_schema_, false/*async_call*/, proxy))) {
      LOG_WARN("failed to sync create meta normal tenant", KR(ret));
    } else {
      FLOG_INFO("[CREATE_TENANT] STEP 3.1. finish sync create meta normal tenant", KR(ret));
      // STEP 3.2 and 3.3 must precede STEP 3.4: creating the user tenant sys LS
      // triggers SS meta service ops that access __all_shared_meta_control (a
      // meta-tenant user table created in STEP 3.3). Both the meta tenant user
      // LS and its user tables must therefore exist before STEP 3.4.
      FLOG_INFO("[CREATE_TENANT] STEP 3.2. start create meta tenant user ls");
      if (OB_FAIL(create_meta_tenant_user_ls_(proxy))) {
        LOG_WARN("failed to create meta tenant user ls", KR(ret));
      } else {
        FLOG_INFO("[CREATE_TENANT] STEP 3.2. finish create meta tenant user ls", KR(ret));
        FLOG_INFO("[CREATE_TENANT] STEP 3.3. start create user tables for meta tenant", K(tenant_id));
        if (OB_FAIL(create_user_table_for_meta_tenant_(tenant_id))) {
          LOG_WARN("failed to create user tables for meta tenant", KR(ret), K(tenant_id));
        } else if (OB_FAIL(retry_operation(ctx_, "wait schema ready for user sys ls", this,
                       &ObParallelCreateTenantExecutor::try_wait_schema_ready_for_user_sys_ls_))) {
          LOG_WARN("failed to wait schema ready before create user sys ls", KR(ret), K(tenant_id));
        } else if (OB_FAIL(retry_operation(ctx_, "broadcast shared meta tables location", this,
                       &ObParallelCreateTenantExecutor::broadcast_shared_meta_tables_location_))) {
          LOG_WARN("failed to broadcast shared meta tables location before create user sys ls",
              KR(ret), K(tenant_id));
        } else {
          FLOG_INFO("[CREATE_TENANT] STEP 3.3. finish create user tables for meta tenant", KR(ret), K(tenant_id));
          FLOG_INFO("[CREATE_TENANT] STEP 3.4. start create user tenant sys ls");
          if (OB_FAIL(create_user_tenant_sys_ls_())) {
            LOG_WARN("failed to create user tenant sys ls", KR(ret));
          } else {
            FLOG_INFO("[CREATE_TENANT] STEP 3.4. finish create user tenant sys ls", KR(ret));
            FLOG_INFO("[CREATE_TENANT] STEP 3.5. start parallel create user normal tenant");
            if (OB_FAIL(call_create_normal_tenant_(user_tenant_schema_,
                    GCONF._enable_parallel_tenant_creation, proxy))) {
              LOG_WARN("failed to call create user normal tenant", KR(ret));
            } else {
              FLOG_INFO("[CREATE_TENANT] STEP 3.5. finish dispatch parallel create user normal tenant", KR(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}
#endif

int ObParallelCreateTenantExecutor::create_sys_ls_and_normal_tenant_for_sn_(
    obrpc::UInt64 &tenant_id,
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  tenant_id = OB_INVALID_TENANT_ID;
  // STEP 2.1 is logged inside create_tenant_sys_ls_
  FLOG_INFO("[CREATE_TENANT] STEP 2. start create tenant sys ls in shared nothing mode");
  if (OB_FAIL(create_tenant_sys_ls_())) {
    LOG_WARN("failed to create tenant sys ls", KR(ret));
  } else {
    FLOG_INFO("[CREATE_TENANT] STEP 2. finish create tenant sys ls", KR(ret));
  }
#ifdef OB_BUILD_TDE_SECURITY
#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_SUCC(ret) && OB_FAIL(upload_root_key_())) {
    LOG_WARN("failed to upload root key", KR(ret));
  }
#endif
#endif
  if (OB_SUCC(ret)) {
    tenant_id = user_tenant_schema_.get_tenant_id();
    FLOG_INFO("[CREATE_TENANT] STEP 3. start parallel create normal tenant");
    if (OB_FAIL(call_create_normal_tenant_(proxy))) {
      LOG_WARN("faled to async call create normal tenant", KR(ret));
      // STEP 2.2-2.6 are logged inside each parallel_create_normal_tenant RPC
    } else {
      FLOG_INFO("[CREATE_TENANT] STEP 3. finish dispatch parallel create normal tenant", KR(ret));
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::save_data_version_in_palf_kv_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  // tenant data_version has publish in create_tenant_scheama(include backup and recovery tenant)
  // no need check data_version.
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  uint64_t user_data_version = (create_tenant_arg_.is_restore_tenant() || create_tenant_arg_.is_clone_tenant())
                               ? create_tenant_arg_.compatible_version_ : DATA_CURRENT_VERSION;
  if (!GCTX.is_shared_storage_mode()) {
  } else if (OB_FAIL(ObServerZoneOpService::store_data_version_in_palf_kv(user_tenant_id, user_data_version))) {
    LOG_WARN("fail to store data version in palf kv", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(ObServerZoneOpService::store_data_version_in_palf_kv(meta_tenant_id, DATA_CURRENT_VERSION))) {
    LOG_WARN("fail to store data version in palf kv", KR(ret), K(meta_tenant_id));
  }
  FLOG_INFO("[CREATE_TENANT] store data version in palf kv", KR(ret), K(user_tenant_id), KDV(user_data_version), KDV(DATA_CURRENT_VERSION));
#endif
  return ret;
}

bool ObParallelCreateTenantExecutor::async_rpc_has_error(ObParallelCreateNormalTenantProxy &proxy)
{
  return proxy.check_has_error_result();
}

int ObParallelCreateTenantExecutor::wait_all_(
    ObParallelCreateNormalTenantProxy &proxy,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  ObArray<int> return_ret_array;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  DEBUG_SYNC(BEFORE_CHECK_CREATE_NORMAL_TENANT_RESULT);
  // NOTE: never skip proxy.wait_all() on user-session interruption here:
  // outstanding async RPC callbacks must be reaped before the proxy is
  // destroyed, otherwise in-flight responses would touch freed memory.
  // Session interruption is detected after wait_all via wait_ls_leader_() /
  // wait_sys_package_ready().
  // Residual window: if the remote create_normal_tenant RPCs are still in
  // flight when the client disconnects, wait_all() blocks until they return,
  // bounded by the RPC timeout (the create-tenant DDL budget passed to
  // proxy.call()). ObAsyncRpcProxy::wait_all() only terminates outstanding
  // RPCs early for proxies with detect_session_killed enabled (DAS / table
  // lock), which does not cover this proxy, so the residual window equals the
  // remaining RPC timeout; the interruption is still perceived afterwards.
  if (OB_FAIL(proxy.wait_all(return_ret_array))) {
    LOG_WARN("failed to wait all create normal tenant rpc", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(proxy.check_return_cnt(return_ret_array.count()))) {
    LOG_WARN("return cnt not match", KR(ret));
  } else {
    for (int64_t i = 0; i < return_ret_array.count(); i++) {
      int return_ret = return_ret_array.at(i);
      const ObAddr &addr = proxy.get_dests().at(i);
      if (OB_SUCCESS != return_ret) {
        ret = OB_FAIL(ret) ? ret :return_ret;
        LOG_WARN("rpc return error", KR(return_ret), K(addr));
      }
    } // end for
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ret_code)) {
    } else if (OB_FAIL(wait_ls_leader_(user_tenant_id, SYS_LS, true/*force_renew*/))) {
      LOG_WARN("failed to wait user sys ls exists in meta table", KR(ret), K(user_tenant_id));
      // To avoid load_sys_package and tenant DDL operations competing for DDL threads
      // wait for all load_sys_package tasks to complete before returning to the user.
      // ObCompatibilityMode::OCEANBASE_MODE means wait both mysql and oracle sys package
    } else if (!GCONF._enable_async_load_sys_package &&
        OB_FAIL(ObLoadSysPackageTask::wait_sys_package_ready(*sql_proxy_, ctx_, ObCompatibilityMode::OCEANBASE_MODE))) {
      LOG_WARN("failed to wait sys package ready", KR(ret), K(ctx_));
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::construct_tenant_info_(const uint64_t tenant_id,
    ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  const SCN recovery_until_scn = get_recovery_until_scn_();
  const ObTenantRole tenant_role = create_tenant_arg_.get_tenant_role();
  if (is_user_tenant(tenant_id) && OB_FAIL(tenant_info.init(tenant_id, tenant_role,
            NORMAL_SWITCHOVER_STATUS, 0, SCN::base_scn(), SCN::base_scn(), SCN::base_scn(),
            recovery_until_scn))) {
    LOG_WARN("failed to init tenant_info", KR(ret), K(tenant_id), K(tenant_role), K(recovery_until_scn));
  } else if (is_meta_tenant(tenant_id) && OB_FAIL(tenant_info.init(tenant_id, PRIMARY_TENANT_ROLE))) {
    LOG_WARN("failed to init tenant_info", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::init_after_create_tenant_schema_()
{
  // create_tenant_schema_result_ should be set
  int ret = OB_SUCCESS;
  if (!create_tenant_schema_result_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create tenant schema result is invalid", KR(ret), K(create_tenant_schema_result_));
  } else {
    if (OB_FAIL(get_tenant_schema_from_inner_table_())) {
      LOG_WARN("failed to get tenant_schema", KR(ret));
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::create_tenant_sys_ls_()
{
  int ret = OB_SUCCESS;
  palf::PalfBaseInfo meta_palf_base_info;
  ObAllTenantInfo tenant_info;
  ObArray<share::ObResourcePoolName> pools;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  if (OB_FAIL(construct_tenant_info_(meta_tenant_id, tenant_info))) {
    LOG_WARN("failed to get tenant_info", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(ObTenantDDLService::get_pools(create_tenant_arg_.pool_list_, pools))) {
    LOG_WARN("failed to init pools", KR(ret), K(create_tenant_arg_.pool_list_));
  } else if (OB_FAIL(create_tenant_sys_ls_(meta_tenant_schema_, pools, false /*create_ls_with_palf*/,
          meta_palf_base_info, create_tenant_arg_.source_tenant_id_, tenant_info))) {
    LOG_WARN("failed to create meta tenant sys ls", KR(ret), K(meta_tenant_schema_),
        K(pools), K(meta_palf_base_info), K(create_tenant_arg_));
  } else if (OB_FAIL(construct_tenant_info_(user_tenant_id, tenant_info))) {
    LOG_WARN("failed to get tenant_info", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(create_tenant_sys_ls_(user_tenant_schema_, pools, get_create_ls_with_palf_(),
            get_palf_base_info_(), create_tenant_arg_.source_tenant_id_, tenant_info))) {
    LOG_WARN("failed to create user tenant sys ls", KR(ret), K(user_tenant_schema_), K(pools),
        K(create_tenant_arg_));
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObParallelCreateTenantExecutor::create_meta_tenant_sys_ls_()
{
  int ret = OB_SUCCESS;
  palf::PalfBaseInfo meta_palf_base_info;
  ObAllTenantInfo tenant_info;
  ObArray<share::ObResourcePoolName> pools;
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  if (OB_FAIL(construct_tenant_info_(meta_tenant_id, tenant_info))) {
    LOG_WARN("failed to get tenant_info", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(ObTenantDDLService::get_pools(create_tenant_arg_.pool_list_, pools))) {
    LOG_WARN("failed to init pools", KR(ret), K(create_tenant_arg_.pool_list_));
  } else if (OB_FAIL(create_tenant_sys_ls_(meta_tenant_schema_, pools, false /*create_ls_with_palf*/,
          meta_palf_base_info, create_tenant_arg_.source_tenant_id_, tenant_info))) {
    LOG_WARN("failed to create meta tenant sys ls", KR(ret), K(meta_tenant_schema_),
        K(pools), K(meta_palf_base_info), K(create_tenant_arg_));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::create_user_tenant_sys_ls_()
{
  int ret = OB_SUCCESS;
  ObAllTenantInfo tenant_info;
  ObArray<share::ObResourcePoolName> pools;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  if (OB_FAIL(construct_tenant_info_(user_tenant_id, tenant_info))) {
    LOG_WARN("failed to get tenant_info", KR(ret), K(user_tenant_id));
  } else if (OB_FAIL(ObTenantDDLService::get_pools(create_tenant_arg_.pool_list_, pools))) {
    LOG_WARN("failed to init pools", KR(ret), K(create_tenant_arg_.pool_list_));
  } else if (OB_FAIL(create_tenant_sys_ls_(user_tenant_schema_, pools, get_create_ls_with_palf_(),
            get_palf_base_info_(), create_tenant_arg_.source_tenant_id_, tenant_info))) {
    LOG_WARN("failed to create user tenant sys ls", KR(ret), K(user_tenant_schema_), K(pools),
        K(create_tenant_arg_));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::collect_meta_schema_sync_target_servers_(
    const uint64_t user_tenant_id, common::ObIArray<common::ObAddr> &addrs)
{
  int ret = OB_SUCCESS;
  addrs.reset();
  ObUnitTableOperator unit_operator;
  if (OB_UNLIKELY(!is_user_tenant(user_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(user_tenant_id));
  } else if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_proxy_ is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(unit_operator.init(*sql_proxy_))) {
    LOG_WARN("failed to init unit table operator", KR(ret));
  } else if (OB_FAIL(unit_operator.get_alive_servers_by_tenant(user_tenant_id, addrs))) {
    LOG_WARN("failed to get alive servers by tenant", KR(ret), K(user_tenant_id));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::publish_schema_via_rs_(
    const uint64_t tenant_id, const common::ObIArray<common::ObAddr> &addrs, int64_t &refreshed_schema_version)
{
  int ret = OB_SUCCESS;
  obrpc::ObPublishSchemaAndGetSchemaVersionArg arg;
  obrpc::Int64 result;
  const int64_t timeout = ctx_.is_timeout_set() ? ctx_.get_timeout() : GCONF.rpc_timeout;
  refreshed_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(common_rpc_) || OB_ISNULL(rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_rpc_ or rs_mgr_ is null", KR(ret), KP(common_rpc_), KP(rs_mgr_));
  } else {
    arg.tenant_id_ = tenant_id;
    if (OB_FAIL(arg.addrs_.assign(addrs))) {
      LOG_WARN("failed to assign addrs", KR(ret), K(tenant_id));
    } else if (OB_FAIL(common_rpc_->to_rs(*rs_mgr_).timeout(timeout)
        .publish_schema_and_get_schema_version(arg, result))) {
      LOG_WARN("rpc publish schema and get schema version failed", KR(ret), K(arg));
    } else {
      refreshed_schema_version = result;
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::try_wait_schema_ready_for_user_sys_ls_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  ObArray<ObAddr> empty_addrs;
  ObArray<ObAddr> target_servers;
  int64_t refreshed_sys_schema_version = OB_INVALID_VERSION;
  int64_t refreshed_meta_schema_version = OB_INVALID_VERSION;
  if (OB_ISNULL(common_rpc_) || OB_ISNULL(rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("common_rpc_ or rs_mgr_ is null", KR(ret), KP(common_rpc_), KP(rs_mgr_));
  } else if (OB_FAIL(collect_meta_schema_sync_target_servers_(user_tenant_id, target_servers))) {
    LOG_WARN("failed to collect meta schema sync target servers", KR(ret), K(user_tenant_id));
  } else if (target_servers.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta schema sync target servers is empty", KR(ret), K(user_tenant_id));
  } else {
    FLOG_INFO("[CREATE_TENANT] start wait schema ready for user sys ls",
        K(user_tenant_id), K(meta_tenant_id), K(target_servers));
    if (OB_FAIL(publish_schema_via_rs_(OB_SYS_TENANT_ID, empty_addrs, refreshed_sys_schema_version))) {
      LOG_WARN("failed to refresh sys tenant schema on rs before create user sys ls",
          KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(publish_schema_via_rs_(meta_tenant_id, target_servers, refreshed_meta_schema_version))) {
      LOG_WARN("failed to force refresh meta tenant schema before create user sys ls",
          KR(ret), K(meta_tenant_id), K(target_servers));
    }
  }
  FLOG_INFO("[CREATE_TENANT] finish wait schema ready for user sys ls",
      KR(ret), K(user_tenant_id), K(meta_tenant_id),
      K(refreshed_sys_schema_version), K(refreshed_meta_schema_version),
      "meta_schema_sync_server_count", target_servers.count(),
      "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObParallelCreateTenantExecutor::collect_shared_meta_tablet_ids_(
    const uint64_t meta_tenant_id,
    ObIArray<ObLSID> &warm_up_ls_ids,
    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  static const char *SHARED_META_TABLE_NAMES[] = {
      OB_ALL_SHARED_META_TNAME,
      OB_ALL_SHARED_META_CONTROL_TNAME,
      OB_ALL_SHARED_TIERED_META_TNAME};
  ObSchemaGetterGuard schema_guard;
  ObLSStatusInfoArray meta_ls_status_infos;
  ObLSStatusOperator ls_status_operator;

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ls_status_operator.get_all_ls_status_by_order(
                 meta_tenant_id, meta_ls_status_infos, *sql_proxy_))) {
    LOG_WARN("failed to get meta tenant ls status for tablet location cache warm-up",
        KR(ret), K(meta_tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < meta_ls_status_infos.count(); ++i) {
      const ObLSStatusInfo &ls_status_info = meta_ls_status_infos.at(i);
      if (OB_UNLIKELY(!ls_status_info.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid meta tenant ls status for tablet location cache warm-up",
            KR(ret), K(meta_tenant_id), K(ls_status_info));
      } else if (OB_FAIL(warm_up_ls_ids.push_back(ls_status_info.get_ls_id()))) {
        LOG_WARN("failed to collect meta tenant ls id for tablet location cache warm-up",
            KR(ret), K(meta_tenant_id), K(ls_status_info));
      }
    }
    if (OB_SUCC(ret) && warm_up_ls_ids.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("meta tenant ls ids for tablet location cache warm-up are empty",
          KR(ret), K(meta_tenant_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(meta_tenant_id, schema_guard))) {
    LOG_WARN("failed to get meta tenant schema guard", KR(ret), K(meta_tenant_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ARRAYSIZEOF(SHARED_META_TABLE_NAMES); ++i) {
      const char *table_name = SHARED_META_TABLE_NAMES[i];
      const ObTableSchema *table_schema = nullptr;
      ObArray<ObTabletID> table_tablet_ids;
      if (OB_FAIL(schema_guard.get_table_schema(meta_tenant_id, ObString::make_string("oceanbase"),
              ObString::make_string(table_name), false /*is_index*/, table_schema))) {
        LOG_WARN("failed to get shared meta table schema", KR(ret), K(meta_tenant_id), K(table_name));
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("shared meta table schema does not exist", KR(ret), K(meta_tenant_id), K(table_name));
      } else if (OB_UNLIKELY(PARTITION_LEVEL_ONE != table_schema->get_part_level()
                 || !table_schema->is_hash_part())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("shared meta table has unexpected partition schema",
            KR(ret), K(meta_tenant_id), K(table_name), KPC(table_schema));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < warm_up_ls_ids.count(); ++j) {
          ObObj ls_id_obj;
          ObObjectID part_id = OB_INVALID_ID;
          ObTabletID tablet_id;
          ls_id_obj.set_int(warm_up_ls_ids.at(j).id());
          ObNewRow partition_row(&ls_id_obj, 1);
          if (OB_FAIL(ObPartitionUtils::get_tablet_and_part_id(
                  *table_schema, partition_row, tablet_id, part_id))) {
            LOG_WARN("failed to get shared meta tablet id by ls id",
                KR(ret), K(meta_tenant_id), K(table_name), "ls_id", warm_up_ls_ids.at(j));
          } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("invalid shared meta tablet id calculated by ls id",
                KR(ret), K(meta_tenant_id), K(table_name), "ls_id", warm_up_ls_ids.at(j), K(part_id));
          } else if (has_exist_in_array(tablet_ids, tablet_id)) {
          } else if (OB_FAIL(table_tablet_ids.push_back(tablet_id))) {
            LOG_WARN("failed to collect shared meta tablet id",
                KR(ret), K(meta_tenant_id), K(table_name), "ls_id", warm_up_ls_ids.at(j), K(tablet_id));
          } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
            LOG_WARN("failed to collect tablet id for location cache warm-up",
                KR(ret), K(meta_tenant_id), K(table_name), K(tablet_id));
          }
        }
        if (OB_SUCC(ret) && table_tablet_ids.empty()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("shared meta tablet ids are empty",
              KR(ret), K(meta_tenant_id), K(table_name), K(warm_up_ls_ids));
        }
      }
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::build_tablet_location_broadcast_tasks_(
    const uint64_t meta_tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    ObIArray<ObLSID> &grouped_ls_ids,
    ObIArray<ObTabletLocationBroadcastTask> &broadcast_tasks)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletToLSInfo> tablet_to_ls_infos;

  if (OB_ISNULL(sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret), KP(sql_proxy_));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get(
                 *sql_proxy_, meta_tenant_id, tablet_ids, tablet_to_ls_infos))) {
    LOG_WARN("failed to get shared meta tables tablet to ls mappings", KR(ret), K(meta_tenant_id), K(tablet_ids));
  } else if (tablet_ids.count() != tablet_to_ls_infos.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("shared meta tables tablet to ls mapping count mismatch",
        KR(ret), K(meta_tenant_id), K(tablet_ids), K(tablet_to_ls_infos));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_to_ls_infos.count(); ++i) {
      const ObLSID ls_id = tablet_to_ls_infos.at(i).get_ls_id();
      if (has_exist_in_array(grouped_ls_ids, ls_id)) {
        continue;
      }
      ObArray<ObTransferTabletInfo> tablet_infos;
      ObTabletLocationBroadcastTask broadcast_task;
      const ObTransferTaskID warm_up_task_id(ObTabletLocationBroadcastTask::CACHE_WARM_UP_TASK_ID);
      for (int64_t j = i; OB_SUCC(ret) && j < tablet_to_ls_infos.count(); ++j) {
        const ObTabletToLSInfo &tablet_to_ls = tablet_to_ls_infos.at(j);
        if (ls_id == tablet_to_ls.get_ls_id()) {
          ObTransferTabletInfo tablet_info;
          if (OB_FAIL(tablet_info.init(tablet_to_ls.get_tablet_id(), tablet_to_ls.get_transfer_seq()))) {
            LOG_WARN("failed to init tablet location info", KR(ret), K(tablet_to_ls));
          } else if (OB_FAIL(tablet_infos.push_back(tablet_info))) {
            LOG_WARN("failed to push tablet location info", KR(ret), K(tablet_info));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(broadcast_task.init(meta_tenant_id, warm_up_task_id, ls_id, tablet_infos))) {
        LOG_WARN("failed to init tablet location cache warm-up task",
            KR(ret), K(meta_tenant_id), K(ls_id), K(tablet_infos));
      } else if (OB_FAIL(broadcast_tasks.push_back(broadcast_task))) {
        LOG_WARN("failed to push tablet location cache warm-up task", KR(ret), K(broadcast_task));
      } else if (OB_FAIL(grouped_ls_ids.push_back(ls_id))) {
        LOG_WARN("failed to record grouped ls id", KR(ret), K(ls_id));
      }
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::send_tablet_location_broadcast_rpc_(
    const ObIArray<ObTabletLocationBroadcastTask> &broadcast_tasks,
    const ObIArray<ObAddr> &target_servers)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (OB_ISNULL(rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc proxy is null", KR(ret), KP(rpc_proxy_));
  } else {
    ObTabletLocationSendArg rpc_arg;
    ObTabletLocationSendProxy rpc_proxy(*rpc_proxy_, &ObSrvRpcProxy::tablet_location_send);
    const int64_t rpc_timeout = ctx_.is_timeout_set() ? ctx_.get_timeout() : GCONF.rpc_timeout;
    if (OB_FAIL(rpc_arg.set(broadcast_tasks))) {
      LOG_WARN("failed to set tablet location cache warm-up rpc arg", KR(ret), K(broadcast_tasks));
    } else {
      FOREACH_CNT(addr, target_servers) {
        if (OB_TMP_FAIL(rpc_proxy.call(*addr, rpc_timeout, rpc_arg))) {
          LOG_WARN("failed to send tablet location cache warm-up rpc", KR(tmp_ret), KPC(addr), K(rpc_arg));
          ret = OB_SUCC(ret) ? tmp_ret : ret;
        }
      }
      ObArray<int> return_codes;
      if (OB_TMP_FAIL(rpc_proxy.wait_all(return_codes))) {
        LOG_WARN("failed to wait tablet location cache warm-up rpc", KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      } else if (OB_SUCC(ret) && (return_codes.count() != rpc_proxy.get_results().count()
                 || return_codes.count() != rpc_proxy.get_dests().count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet location cache warm-up rpc result count mismatch", KR(ret), K(return_codes),
            "result_count", rpc_proxy.get_results().count(), "dest_count", rpc_proxy.get_dests().count());
      } else if (OB_SUCC(ret)) {
        for (int64_t i = 0; OB_SUCC(ret) && i < return_codes.count(); ++i) {
          const ObTabletLocationSendResult *result = rpc_proxy.get_results().at(i);
          if (OB_SUCCESS != return_codes.at(i)) {
            ret = return_codes.at(i);
            LOG_WARN("tablet location cache warm-up rpc failed", KR(ret), "dest", rpc_proxy.get_dests().at(i));
          } else if (OB_ISNULL(result)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("tablet location cache warm-up rpc result is null", KR(ret), "dest", rpc_proxy.get_dests().at(i));
          } else if (OB_SUCCESS != result->get_ret()) {
            ret = result->get_ret();
            LOG_WARN("tablet location cache warm-up failed on observer", KR(ret), "dest", rpc_proxy.get_dests().at(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::broadcast_shared_meta_tables_location_()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  ObArray<ObLSID> warm_up_ls_ids;
  ObArray<ObTabletID> tablet_ids;
  ObArray<ObLSID> grouped_ls_ids;
  ObArray<ObTabletLocationBroadcastTask> broadcast_tasks;
  ObArray<ObAddr> target_servers;

  if (OB_FAIL(collect_meta_schema_sync_target_servers_(user_tenant_id, target_servers))) {
    LOG_WARN("failed to collect location broadcast target servers", KR(ret), K(user_tenant_id));
  } else if (target_servers.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet location cache warm-up target server list is empty",
        KR(ret), K(user_tenant_id), K(meta_tenant_id));
  } else if (OB_FAIL(collect_shared_meta_tablet_ids_(
                 meta_tenant_id, warm_up_ls_ids, tablet_ids))) {
    LOG_WARN("failed to collect shared meta tablet ids", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(build_tablet_location_broadcast_tasks_(
                 meta_tenant_id, tablet_ids, grouped_ls_ids, broadcast_tasks))) {
    LOG_WARN("failed to build tablet location broadcast tasks", KR(ret), K(meta_tenant_id), K(tablet_ids));
  } else if (OB_FAIL(send_tablet_location_broadcast_rpc_(broadcast_tasks, target_servers))) {
    LOG_WARN("failed to send tablet location broadcast rpc", KR(ret), K(meta_tenant_id), K(target_servers));
  }
  FLOG_INFO("[CREATE_TENANT] finish broadcast shared meta tables location", KR(ret), K(user_tenant_id),
      K(meta_tenant_id), K(warm_up_ls_ids), "tablet_count", tablet_ids.count(),
      "tablet_host_ls_count", grouped_ls_ids.count(),
      "server_count", target_servers.count(), "cost", ObTimeUtility::current_time() - start_ts);
  return ret;
}

#endif

int ObParallelCreateTenantExecutor::reserve_meta_tenant_user_ls_id_for_ss_(
    const uint64_t meta_tenant_id)
{
  int ret = OB_SUCCESS;
  UNUSED(meta_tenant_id);
  return ret;
}

int ObParallelCreateTenantExecutor::create_meta_tenant_user_ls_(ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  FLOG_INFO("[CREATE_TENANT] STEP 4.1. start check can create meta tenant user ls", K(meta_tenant_id));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(check_can_create_user_ls_(meta_tenant_id, proxy))) {
    LOG_WARN("failed to check meta tenant __all_ls writable", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(reserve_meta_tenant_user_ls_id_for_ss_(meta_tenant_id))) {
    LOG_WARN("failed to reserve meta tenant user ls id for ss", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(notify_create_tenant_user_ls_rpc_(meta_tenant_id, proxy))) {
    LOG_WARN("failed to create meta tenant user ls", KR(ret), K(meta_tenant_id));
  }
  FLOG_INFO("[CREATE_TENANT] STEP 4.2. finish notify create meta tenant user ls", KR(ret), K(meta_tenant_id));
  return ret;
}

int ObParallelCreateTenantExecutor::create_user_ls_(ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  DEBUG_SYNC(BEFORE_CREATE_TENANT_FILL_USER_LS_INFO);
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  FLOG_INFO("[CREATE_TENANT] STEP 5.1. start check can create user tenant user ls", K(user_tenant_id));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (create_tenant_arg_.get_tenant_role().is_primary()) {
    // create primary tenant
    if (OB_FAIL(check_can_create_user_ls_(user_tenant_id, proxy))) {
      LOG_WARN("failed to check user tenant table writable", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(notify_create_tenant_user_ls_rpc_(user_tenant_id, proxy))) {
      LOG_WARN("failed to create tenant user ls", KR(ret), K(user_tenant_id));
    }
  }
  FLOG_INFO("[CREATE_TENANT] STEP 5.2. finish notify create user tenant user ls", KR(ret), K(user_tenant_id));
  return ret;
}

int ObParallelCreateTenantExecutor::notify_create_tenant_user_ls_rpc_(
    const uint64_t tenant_id,
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("variable is not init", KR(ret));
  } else {
    ObAddr leader;
    int64_t tmp_ret = OB_SUCCESS;
    const int64_t retry_interval_us = 200l * 1000l; // 200ms
    while (OB_SUCC(ret)) {
      const int64_t timeout = ctx_.get_timeout();
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("create tenant user ls timeout", KR(ret), K(tenant_id));
      } else if (is_user_session_terminated(ret)) {
        LOG_WARN("create tenant user ls interrupted by terminated user session",
            KR(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(GCTX.location_service_->get_leader(GCONF.cluster_id, tenant_id,
              SYS_LS, FALSE, leader))) {
        LOG_WARN("failed to get tenant sys ls leader", KR(ret), KR(tmp_ret), K(tenant_id), K(leader));
      } else if (OB_TMP_FAIL(rpc_proxy_->to(leader).timeout(timeout)
            .notify_create_tenant_user_ls(tenant_id))) {
        LOG_WARN("failed to notify create tenant user ls", KR(ret), KR(tmp_ret),
            K(tenant_id), K(leader), K(ctx_));
      } else {
        break;
      }
      if (OB_FAIL(ret)) {
        // previous async rpc create_normal_tenant may have error, this function should fail fast
      } else if (async_rpc_has_error(proxy)) {
        ret = OB_CANCELED;
        LOG_WARN("create normal tenant RPC are returned, create user ls is canceled",
            KR(ret), K(tenant_id));
      } else {
        ob_usleep(retry_interval_us);
      }
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::call_create_normal_tenant_(
    const ObTenantSchema &tenant_schema,
    const bool async_call,
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (OB_FAIL(async_call_create_normal_tenant_(tenant_schema, proxy))) {
    LOG_WARN("failed to call create normal tenant", KR(ret), K(tenant_schema));
  } else if (!async_call) {
    FLOG_INFO("[CREATE_TENANT] tenants will be created in sync mode", K(tenant_id), K(async_call));
    if (OB_FAIL(proxy.wait())) {
      LOG_WARN("failed to create tenant", KR(ret), K(tenant_schema));
    } else {
      proxy.reuse();
    }
  } else {
    FLOG_INFO("[CREATE_TENANT] tenants will be created in async mode", K(tenant_id), K(async_call));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::call_create_normal_tenant_(
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  const bool async_call = GCONF._enable_parallel_tenant_creation;
  if (OB_FAIL(call_create_normal_tenant_(meta_tenant_schema_, async_call, proxy))) {
    LOG_WARN("failed to call create normal tenant for meta tenant", KR(ret), K(meta_tenant_schema_),
        K(async_call));
  } else if (OB_FAIL(call_create_normal_tenant_(user_tenant_schema_, async_call, proxy))) {
    LOG_WARN("failed to call create normal tenant for user tenant", KR(ret), K(user_tenant_schema_),
        K(async_call));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::async_call_create_normal_tenant_(
    const ObTenantSchema &tenant_schema,
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  ObAddr addr;
  ObParallelCreateNormalTenantArg arg;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  if (is_user_tenant(tenant_id)) {
    DEBUG_SYNC(BEFORE_CREATE_USER_NORMAL_TENANT);
  } else if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(BEFORE_CREATE_META_NORMAL_TENANT);
  }
  if (OB_ISNULL(GCTX.rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pointer is NULL", KR(ret), KP(GCTX.rs_mgr_));
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check_inner_stat_", KR(ret));
  } else if (!tenant_schema.is_creating()) {
    // non primary tenant do not need to create normal tenant
  } else if (OB_FAIL(wait_ls_leader_(tenant_id, SYS_LS))) {
    LOG_WARN("failed to wait ls leader", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GCTX.rs_mgr_->get_master_root_server(addr))) {
    LOG_WARN("failed to get rs addr", KR(ret));
  } else if (OB_FAIL(arg.init(create_tenant_arg_, tenant_id))) {
    LOG_WARN("failed to init ObParallelCreateNormalTenantArg", KR(ret), K(create_tenant_arg_),
        K(tenant_id));
  } else if (OB_FAIL(proxy.call(addr, ctx_.get_timeout(), OB_SYS_TENANT_ID, arg))) {
    LOG_WARN("failed to call parallel create normal tenant", KR(ret), K(addr), K(ctx_), K(arg));
  }
  LOG_INFO("call create_normal_tenant", KR(ret), K(arg));
  return ret;
}

int ObParallelCreateTenantExecutor::create_tenant_sys_ls_(
    const ObTenantSchema &tenant_schema,
    const ObIArray<share::ObResourcePoolName> &pool_list,
    const bool create_ls_with_palf,
    const palf::PalfBaseInfo &palf_base_info,
    const uint64_t source_tenant_id,
    const ObAllTenantInfo &tenant_info,
    const ObLSID &ls_id)
{
  const int64_t start_time = ObTimeUtility::fast_current_time();
  FLOG_INFO("[CREATE_TENANT] STEP 2.1. start create sys log stream", K(tenant_schema), K(source_tenant_id), K(ls_id));
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_schema.get_tenant_id();
  // meta tenant do not have to reference source tenant id
  const uint64_t source_tenant_id_to_use = is_user_tenant(tenant_id) ? source_tenant_id : OB_INVALID_TENANT_ID;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("variable is not init", KR(ret));
  } else if (is_sys_tenant(tenant_id) || !ls_id.is_valid_with_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_id is invalid", KR(ret), K(tenant_id), K(ls_id));
  } else {
    uint64_t data_version = 0;
    if (is_meta_tenant(tenant_id)) {
      data_version = DATA_CURRENT_VERSION;
    } else {
      data_version = (create_tenant_arg_.is_restore_tenant() || create_tenant_arg_.is_clone_tenant())
                                   ? create_tenant_arg_.compatible_version_ : DATA_CURRENT_VERSION;
    }
    ObArray<share::ObZoneReplicaAttrSet> locality;
    ObZone primary_zone;
    ObSqlString zone_priority;
    share::schema::ObSchemaGetterGuard schema_guard; // not used
    int64_t paxos_replica_num = OB_INVALID_ID;
    ObLSCreator ls_creator(*rpc_proxy_, tenant_id, ls_id, data_version, sql_proxy_);
    if (OB_FAIL(tenant_schema.get_zone_replica_attr_array(locality))) {
      LOG_WARN("fail to get tenant's locality", KR(ret), K(locality));
    } else if (OB_FAIL(tenant_schema.get_paxos_replica_num(schema_guard, paxos_replica_num))) {
      LOG_WARN("failed to get paxos replica num", KR(ret));
    } else if (OB_FAIL(ObTenantDDLService::get_tenant_zone_priority(tenant_schema, primary_zone,
            zone_priority))) {
      LOG_WARN("failed to get tenant zone priority", KR(ret), K(tenant_schema));
    } else if (OB_FAIL(ls_creator.create_tenant_sys_ls(
               primary_zone, locality, pool_list, paxos_replica_num,
               tenant_schema.get_compatibility_mode(), zone_priority.string(),
               create_ls_with_palf, palf_base_info, source_tenant_id_to_use, tenant_info))) {
      LOG_WARN("fail to create tenant sys ls", KR(ret), K(pool_list), K(palf_base_info),
               K(locality), K(paxos_replica_num), K(tenant_schema), K(zone_priority), K(source_tenant_id_to_use));
    }
  }
  if (!ls_id.is_sys_ls()) {
  } else if (is_meta_tenant(tenant_id)) {
    DEBUG_SYNC(AFTER_CREATE_META_TENANT_SYS_LOGSTREAM);
  } else {
    DEBUG_SYNC(AFTER_CREATE_USER_TENANT_SYS_LOGSTREAM);
  }
  FLOG_INFO("[CREATE_TENANT] STEP 2.1. finish create sys log stream", KR(ret), K(tenant_schema), K(ls_id),
           "cost", ObTimeUtility::fast_current_time() - start_time);
  return ret;
}

int ObParallelCreateTenantExecutor::wait_ls_leader_(const uint64_t tenant_id, const ObLSID &ls_id, const bool force_renew)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t wait_leader = 0;
  // ignore LOG_USER_ERROR when trying to read from __all_ls_meta_table 
  ObWarningBufferIgnoreScope ignore_errors_in_warning_buffer;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else {
    int64_t wait_leader_start = ObTimeUtility::current_time();
    bool is_cache_hit = false;
    ObLSLocation location;
    ObAddr leader;
    const uint64_t expire_renew_time = force_renew ? INT64_MAX : 0;
    const int64_t retry_interval_us = 200l * 1000l; // 200ms
    bool first_round = true;
    while (OB_SUCC(ret)) {
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait ls leader elect timeout", KR(ret), K(tenant_id), K(ctx_));
      } else if (is_user_session_terminated(ret)) {
        LOG_WARN("wait ls leader interrupted by terminated user session",
            KR(ret), K(tenant_id), K(ls_id));
      } else if (!first_round && OB_TMP_FAIL(location_service_->renew_all_ls_locations_by_rpc())) {
        LOG_WARN("failed to renew all ls locations by rpc", KR(tmp_ret));
      } else if (OB_TMP_FAIL(location_service_->get(GCONF.cluster_id, tenant_id,
        ls_id, expire_renew_time, is_cache_hit, location))) {
        LOG_WARN("failed to get ls location", KR(tmp_ret), K(tenant_id));
      } else if (location.is_valid()) {
        if (OB_TMP_FAIL(location.get_leader(leader))) {
          LOG_WARN("failed to get leader from location cache", KR(ret), K(location));
        } else if (!leader.is_valid()) {
        } else {
          break;
        }
      }
      if (OB_SUCC(ret)) {
        ob_usleep(retry_interval_us);
      }
      first_round = false;
    }
    int64_t wait_leader_end = ObTimeUtility::current_time();
    wait_leader = wait_leader_end - wait_leader_start;
    LOG_INFO("wait tenant ls elect", KR(ret), K(tenant_id), K(ls_id), K(leader), "cost", wait_leader);
  }
  return ret;
}

SCN ObParallelCreateTenantExecutor::get_recovery_until_scn_()
{
  SCN scn = SCN::max_scn();
  if (create_tenant_arg_.is_restore_tenant() || create_tenant_arg_.is_clone_tenant()) {
    scn = create_tenant_arg_.recovery_until_scn_;
  }
  return scn;
}

palf::PalfBaseInfo ObParallelCreateTenantExecutor::get_palf_base_info_()
{
  palf::PalfBaseInfo palf_base_info;
  if (create_tenant_arg_.is_restore_tenant() || create_tenant_arg_.is_clone_tenant()) {
    palf_base_info = create_tenant_arg_.palf_base_info_;
  }
  return palf_base_info;
}

bool ObParallelCreateTenantExecutor::get_create_ls_with_palf_()
{
  return create_tenant_arg_.is_restore_tenant() || create_tenant_arg_.is_clone_tenant();
}

int ObParallelCreateTenantExecutor::do_check_can_create_user_ls_(
    const uint64_t tenant_id,
    bool &can_create)
{
  int ret = OB_SUCCESS;
  can_create = false;
  ObLSAttrArray array;
  ObAllTenantInfo tenant_info;
  ObLSAttrOperator ls_operator(tenant_id, sql_proxy_);
  ObTimeoutCtx timeout_ctx;
  const int64_t default_timeout = GCONF.internal_sql_execute_timeout;
  if (OB_FAIL(timeout_ctx.set_timeout(default_timeout))) {
    /*
    Hear, we are checking whether it is possible to create a user LS, which is a separate thread from create_normal_tenant().
    At the end of each while loop, async_rpc_has_error() will be used to check if create_normal_tenant() returns an error code,
    so that when the create_normal_tenant() fails, the while loop can be exited.
    But if the while loop gets stuck(such as due to schema related ...) in the middle step and cannot detect the create_normal_tenant() error code,
    it will need to wait until timeout to exit, subsequent step drop tenant force will ultimately failed due to timeout.
    For example:
      Injecting ERRSIM_BROADCAST_SCHEMA into z2 (z1 is normal) when creating tenant and broadcast schema,
      due to schema related modifications in the __all_sslog_table, it is assumed that version 1 of the schema is also readable,
      however, if a inner sql request reaches z2, it will cause z2's machine to repeatedly refresh the schema,
      which has not yet been written into the inner table, the refresh in z2 will not be successful and will be stuck until timeout occurs.
    Here we manually set the ctx timeout for each round to ensure that it will not be hung.
    Note that the ctx timeout needs to be restored later.
    */
    LOG_WARN("failed to set default timeout ctx", KR(ret), K(default_timeout));
  } else if (OB_FAIL(ls_operator.get_all_ls_by_order(array))) {
    LOG_WARN("failed to get all ls", KR(ret), K(tenant_id));
  } else if (0 == array.count()) {
    // STEP 2.4 set_sys_ls_ inserts SYS LS into __all_ls for both meta and user tenant.
    LOG_INFO("sys ls is not in __all_ls, need wait", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObAllTenantInfoProxy::load_tenant_info(
      tenant_id, sql_proxy_, false /*for_update*/, tenant_info))) {
    LOG_WARN("failed to get tenant info, need wait", KR(ret), K(tenant_id));
  } else {
    // Meta tenant row comes from init_meta_tenant_info_ (STEP 2.4.3).
    // Meta tenant is always primary, no need to check role/switchover status.
    can_create = true;
    FLOG_INFO("[CREATE_TENANT] tenant __all_tenant_info is ready", K(tenant_id), K(tenant_info));
  }
  return ret;
}

// wait tenant sys ls __all_ls readable/writable
int ObParallelCreateTenantExecutor::check_can_create_user_ls_(
    const uint64_t tenant_id,
    ObParallelCreateNormalTenantProxy &proxy)
{
  int ret = OB_SUCCESS;
  FLOG_INFO("[CREATE_TENANT] begin check can create user ls", K(tenant_id));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else {
    const int64_t retry_interval_us = 200l * 1000l; // 200ms
    // ignore LOG_USER_ERROR when trying to read from __all_ls
    ObWarningBufferIgnoreScope ignore_errors_in_warning_buffer;
    while (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      bool can_create = false;
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait tenant __all_ls writable failed", KR(ret), K(tenant_id));
      } else if (is_user_session_terminated(ret)) {
        LOG_WARN("check can create user ls interrupted by terminated user session",
            KR(ret), K(tenant_id));
      } else if (OB_TMP_FAIL(do_check_can_create_user_ls_(tenant_id, can_create))) {
        LOG_WARN("failed to check can create user ls", KR(tmp_ret), K(tenant_id));
      } else if (can_create) {
        FLOG_INFO("[CREATE_TENANT] tenant is ready for create user ls",
            K(tenant_id), "is_meta", is_meta_tenant(tenant_id));
        break;
      }
      if (OB_FAIL(ret)) {
        // previous async rpc create_normal_tenant may have error, this function should fail fast
      } else if (async_rpc_has_error(proxy)) {
        ret = OB_CANCELED;
        LOG_WARN("create normal tenant RPC are returned, create tenant user ls is canceled",
            KR(ret), K(tenant_id));
      } else {
        ob_usleep(retry_interval_us);
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] finish check create user ls", KR(ret), K(tenant_id),
      "timeout", ctx_.get_abs_timeout());
  return ret;
}

ERRSIM_POINT_DEF(ERRSIM_USER_CREATE_TENANT_END_FAIL);
ERRSIM_POINT_DEF(ERRSIM_META_CREATE_TENANT_END_FAIL);

int ObParallelCreateTenantExecutor::finish_create_tenant_(const int ret_code)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  const ObTenantRole tenant_role = create_tenant_arg_.get_tenant_role();
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check_inner_stat", KR(ret));
  } else if (!create_tenant_schema_result_.is_valid() || create_tenant_schema_result_.tenant_exist_) {
    // no need to finish
  } else {
    if (OB_SUCC(ret_code)) {
      ObCreateTenantEndArg create_tenant_end_arg;
      create_tenant_end_arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
      // finish create tenant
      if (OB_FAIL(create_tenant_end_arg.init(meta_tenant_id))) {
        LOG_WARN("failed to init create tenant end arg", KR(ret), K(meta_tenant_id));
      } else if (OB_FAIL(common_rpc_->to_rs(*rs_mgr_).timeout(ctx_.get_timeout())
            .create_tenant_end(create_tenant_end_arg))) {
        LOG_WARN("failed to create tenant end", KR(ret), K(create_tenant_end_arg), K(ctx_));
      } else if (OB_FAIL(ERRSIM_META_CREATE_TENANT_END_FAIL)) {
        LOG_WARN("ERRSIM_META_CREATE_TENANT_END_FAIL", KR(ret));
      } else if (OB_FAIL(create_tenant_end_arg.init(user_tenant_id))) {
        LOG_WARN("failed to init create tenant end arg", KR(ret), K(user_tenant_id));
      } else if (tenant_role.is_primary() && OB_FAIL(common_rpc_->to_rs(*rs_mgr_)
            .timeout(ctx_.get_timeout()).create_tenant_end(create_tenant_end_arg))) {
        LOG_WARN("failed to create tenant end", KR(ret), K(create_tenant_end_arg), K(ctx_));
      } else if (OB_FAIL(ERRSIM_USER_CREATE_TENANT_END_FAIL)) {
        LOG_WARN("ERRSIM_USER_CREATE_TENANT_END_FAIL", KR(ret));
      } else {
        LOG_INFO("successfully create tenant", KR(ret), K(user_tenant_id));
      }
    }
    if ((OB_FAIL(ret) || OB_FAIL(ret_code))
        && (tenant_role.is_primary() || tenant_role.is_restore())) {
      // Roll back failed creation of primary and restore tenants. Standby tenant IDs
      // must stay aligned with the primary; clone jobs manage their own cleanup.
      const ObString tenant_name = create_tenant_arg_.tenant_schema_.get_tenant_name();
      obrpc::ObDropTenantArg drop_tenant_arg;
      // A restore tenant may only be dropped while it is still in RESTORE status.
      drop_tenant_arg.drop_only_in_restore_ = tenant_role.is_restore();
      // ObDropTenantArg::is_valid() requires force_drop_ for this status check.
      drop_tenant_arg.force_drop_ = tenant_role.is_restore();
      ObSqlString ddl_stmt;
      // Creation may have exhausted its deadline. Give rollback a fresh configured
      // DDL budget without changing the original creation context or its error.
      const int64_t cleanup_timeout_us = GCONF._ob_ddl_timeout;
      ObTimeoutCtx cleanup_ctx;
      cleanup_ctx.reset_timeout_us();
      if (OB_TMP_FAIL(cleanup_ctx.set_timeout(cleanup_timeout_us))) {
        LOG_WARN("failed to set tenant cleanup timeout", KR(tmp_ret), K(user_tenant_id));
      } else if (OB_TMP_FAIL(ObTenantDDLService::generate_drop_tenant_arg(user_tenant_id,
              tenant_name, ddl_stmt, drop_tenant_arg))) {
        LOG_WARN("failed to generate drop tenant arg", KR(tmp_ret), K(user_tenant_id), K(tenant_name));
      } else {
        lib::WorkerTimeoutGuard timeout_guard(cleanup_ctx.get_abs_timeout());
        if (OB_TMP_FAIL(common_rpc_->to_rs(*rs_mgr_).timeout(cleanup_ctx.get_timeout())
              .drop_tenant(drop_tenant_arg))) {
          LOG_WARN("failed to force drop tenant after create failure", KR(tmp_ret),
              K(drop_tenant_arg), K(ctx_), K(cleanup_ctx));
        } else {
          LOG_INFO("failed to create tenant, drop tenant force", KR(tmp_ret), K(user_tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::get_tenant_schema_from_inner_table_()
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = create_tenant_schema_result_.user_tenant_id_;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  user_tenant_schema_.set_tenant_id(user_tenant_id);
  meta_tenant_schema_.set_tenant_id(meta_tenant_id);
  int64_t version_in_inner_table = OB_INVALID_VERSION;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = OB_SYS_TENANT_ID;
  ObSchemaGetterGuard schema_guard;
  const ObTenantSchema *tenant_schema = NULL;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
          OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", KR(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_info(user_tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(user_tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), KP(tenant_schema));
  } else if (OB_FAIL(user_tenant_schema_.assign(*tenant_schema))) {
    LOG_WARN("failed to assign user tenant schema", KR(ret), K(*tenant_schema));
  } else if (OB_FAIL(schema_guard.get_tenant_info(meta_tenant_id, tenant_schema))) {
    LOG_WARN("failed to get tenant schema", KR(ret), K(meta_tenant_id));
  } else if (OB_ISNULL(tenant_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), KP(tenant_schema));
  } else if (OB_FAIL(meta_tenant_schema_.assign(*tenant_schema))) {
    LOG_WARN("failed to assign meta tenant schema", KR(ret), K(*tenant_schema));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::init(
      const obrpc::ObCreateTenantArg &create_tenant_arg,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      obrpc::ObCommonRpcProxy *common_rpc,
      common::ObMySQLProxy *sql_proxy,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::ObLSTableOperator *lst_operator,
      share::ObLocationService *location_service)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_tenant_arg_.assign(create_tenant_arg))) {
    LOG_WARN("failed to assign create tenant arg", KR(ret), K(create_tenant_arg));
  } else if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx_, GCONF._ob_ddl_timeout))) {
    LOG_WARN("fail to set default timeout", KR(ret));
  } else {
    rpc_proxy_ = rpc_proxy;
    common_rpc_ = common_rpc;
    sql_proxy_ = sql_proxy;
    schema_service_ = schema_service;
    lst_operator_ = lst_operator;
    location_service_ = location_service;
    rs_mgr_ = GCTX.rs_mgr_;
  }
  return ret;
}
int ObParallelCreateTenantExecutor::apply_user_table_schema_defs_(
    const uint64_t tenant_id,
    ObMultiVersionSchemaService &schema_service,
    ObMySQLProxy &sql_proxy,
    const ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  ObUserTableSchemaApplier applier;
  if (OB_FAIL(applier.init(schema_service, sql_proxy, timeout_ctx))) {
    LOG_WARN("init applier failed", KR(ret));
  } else if (OB_FAIL(applier.apply_builtin_user_table_schemas(tenant_id))) {
    LOG_WARN("apply builtin user table schemas failed", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::check_has_available_ls_for_tablet_alloc_(
    const uint64_t tenant_id,
    bool &ready)
{
  int ret = OB_SUCCESS;
  ready = false;
  ObLSAttrOperator ls_operator(tenant_id, sql_proxy_);
  ObLSAttrArray ls_attr_array;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(ls_operator.get_all_ls_by_order(ls_attr_array))) {
    LOG_WARN("failed to get all ls attr", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("location service is null", KR(ret));
  } else {
    int64_t eligible_cnt = 0;
    int64_t available_cnt = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_attr_array.count(); ++i) {
      const ObLSAttr &ls_attr = ls_attr_array.at(i);
      if (is_balance_user_ls_for_tablet_alloc(tenant_id, ls_attr)) {
        eligible_cnt++;
        common::ObAddr leader;
        int tmp_ret = location_service_->get_leader(
            GCONF.cluster_id, tenant_id, ls_attr.get_ls_id(), false, leader);
        if (OB_SUCCESS != tmp_ret || !leader.is_valid()) {
          tmp_ret = location_service_->get_leader(
              GCONF.cluster_id, tenant_id, ls_attr.get_ls_id(), true, leader);
        }
        if (OB_SUCCESS == tmp_ret && leader.is_valid()) {
          available_cnt++;
        }
      }
    }
    // Create-tenant builtin tables only need one balance user ls with leader.
    if (OB_SUCC(ret) && eligible_cnt > 0 && available_cnt > 0) {
      ready = true;
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::wait_tenant_user_ls_leader_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  const int64_t retry_interval_us = 200l * 1000l; // 200ms
  ObWarningBufferIgnoreScope ignore_errors_in_warning_buffer;
  FLOG_INFO("[CREATE_TENANT] wait tenant user ls leader start", K(tenant_id));
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (!is_meta_tenant(tenant_id) && !is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    while (OB_SUCC(ret)) {
      if (ctx_.is_timeouted()) {
        ret = OB_TIMEOUT;
        LOG_WARN("wait tenant user ls leader timeout", KR(ret), K(tenant_id));
      } else if (is_user_session_terminated(ret)) {
        LOG_WARN("wait tenant user ls leader interrupted by terminated user session",
            KR(ret), K(tenant_id));
      } else {
        bool ready = false;
        if (OB_FAIL(check_has_available_ls_for_tablet_alloc_(tenant_id, ready))) {
          LOG_WARN("failed to check tenant available ls for ddl", KR(ret), K(tenant_id));
        } else if (ready) {
          break;
        } else {
          ObLSAttrOperator ls_operator(tenant_id, sql_proxy_);
          ObLSAttrArray ls_attr_array;
          if (OB_FAIL(ls_operator.get_all_ls_by_order(ls_attr_array))) {
            LOG_WARN("failed to get all ls attr for tenant", KR(ret), K(tenant_id));
          } else if (OB_ISNULL(location_service_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("location service is null", KR(ret));
          } else {
            bool has_eligible_ls = false;
            bool waited_one_ls = false;
            for (int64_t i = 0; OB_SUCC(ret) && i < ls_attr_array.count(); ++i) {
              const ObLSAttr &ls_attr = ls_attr_array.at(i);
              if (!is_balance_user_ls_for_tablet_alloc(tenant_id, ls_attr)) {
                // skip
              } else {
                has_eligible_ls = true;
                common::ObAddr leader;
                int tmp_ret = location_service_->get_leader(
                    GCONF.cluster_id, tenant_id, ls_attr.get_ls_id(), false, leader);
                if (OB_SUCCESS != tmp_ret || !leader.is_valid()) {
                  tmp_ret = location_service_->get_leader(
                      GCONF.cluster_id, tenant_id, ls_attr.get_ls_id(), true, leader);
                }
                if (OB_SUCCESS == tmp_ret && leader.is_valid()) {
                  // At least one ls already has leader; re-check in next round.
                } else if (!waited_one_ls) {
                  FLOG_INFO("[CREATE_TENANT] wait one balance user ls leader",
                      K(tenant_id), "ls_id", ls_attr.get_ls_id());
                  if (OB_FAIL(wait_ls_leader_(tenant_id, ls_attr.get_ls_id(), true/*force_renew*/))) {
                    LOG_WARN("failed to wait balance user ls leader", KR(ret),
                        K(tenant_id), "ls_id", ls_attr.get_ls_id());
                  } else {
                    waited_one_ls = true;
                  }
                }
              }
            }
            if (OB_SUCC(ret) && !has_eligible_ls) {
              FLOG_INFO("[CREATE_TENANT] tenant has no NORMAL balance user ls yet, retry",
                  K(tenant_id));
            }
            if (OB_SUCC(ret)) {
              ob_usleep(retry_interval_us);
            }
          }
        }
      }
    }
  }
  FLOG_INFO("[CREATE_TENANT] wait tenant user ls leader finish", KR(ret), K(tenant_id));
  return ret;
}

int ObParallelCreateTenantExecutor::create_meta_tenant_ss_meta_tablegroup_(
    const uint64_t meta_tenant_id)
{
  int ret = OB_SUCCESS;
  bool is_exist = false;
  uint64_t tablegroup_id = OB_INVALID_ID;
  const ObString tablegroup_name(OB_SHARED_META_TABLEGROUP_NAME);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (!is_meta_tenant(meta_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta tenant id", KR(ret), K(meta_tenant_id));
  } else if (OB_ISNULL(schema_service_) || OB_ISNULL(rs_mgr_) || OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr or rs rpc proxy is null", KR(ret), KP(schema_service_), KP(rs_mgr_),
        KP(GCTX.rs_rpc_proxy_));
  } else if (OB_FAIL(schema_service_->check_tablegroup_exist(
          meta_tenant_id, tablegroup_name, tablegroup_id, is_exist))) {
    LOG_WARN("failed to check ss_meta tablegroup exist", KR(ret), K(meta_tenant_id));
  } else if (is_exist) {
    FLOG_INFO("[CREATE_TENANT] ss_meta tablegroup already exists",
        K(meta_tenant_id), K(tablegroup_id));
  } else {
    obrpc::ObCreateTablegroupArg arg;
    obrpc::UInt64 tg_id = 0;
    common::ObAddr rs_addr;
    const int64_t timeout = GCONF.rpc_timeout;
    arg.exec_tenant_id_ = meta_tenant_id;
    arg.if_not_exist_ = false;
    ObTablegroupSchema &tablegroup_schema = arg.tablegroup_schema_;
    tablegroup_schema.set_tenant_id(meta_tenant_id);
    if (OB_FAIL(tablegroup_schema.set_tablegroup_name(tablegroup_name))) {
      LOG_WARN("failed to set ss_meta tablegroup name", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(tablegroup_schema.set_sharding(OB_PARTITION_SHARDING_ADAPTIVE))) {
      LOG_WARN("failed to set ss_meta tablegroup sharding", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
      LOG_WARN("failed to get rs leader", KR(ret), K(meta_tenant_id));
    } else if (rs_addr == GCONF.self_addr_) {
      if (OB_ISNULL(GCTX.root_service_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("root service is null", KR(ret), K(meta_tenant_id), K(rs_addr));
      } else if (OB_FAIL(GCTX.root_service_->create_tablegroup(arg, tg_id))) {
        LOG_WARN("failed to create ss_meta tablegroup", KR(ret), K(meta_tenant_id));
      }
    } else if (OB_FAIL(GCTX.rs_rpc_proxy_->to(rs_addr).timeout(timeout).create_tablegroup(arg, tg_id))) {
      LOG_WARN("rpc create ss_meta tablegroup failed", KR(ret), K(meta_tenant_id), K(rs_addr));
    }
    if (OB_SUCC(ret)) {
      FLOG_INFO("[CREATE_TENANT] create ss_meta tablegroup success",
          K(meta_tenant_id), K(tg_id), K(rs_addr));
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::create_user_table_for_meta_tenant_(const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (OB_FAIL(wait_tenant_user_ls_leader_(meta_tenant_id))) {
    LOG_WARN("failed to wait meta tenant user ls leader", KR(ret), K(meta_tenant_id));
  } else if (OB_ISNULL(rs_mgr_) || OB_ISNULL(GCTX.rs_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs mgr or rs rpc proxy is null", KR(ret), KP(rs_mgr_), KP(GCTX.rs_rpc_proxy_));
  } else {
    share::schema::ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(create_meta_tenant_ss_meta_tablegroup_(meta_tenant_id))) {
      LOG_WARN("failed to create ss_meta tablegroup", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(ObDDLService::get_tenant_schema_guard_with_version_in_inner_table(
            meta_tenant_id, schema_guard))) {
      LOG_WARN("failed to refresh meta tenant schema before apply user tables",
               KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(apply_user_table_schema_defs_(
                   meta_tenant_id, *schema_service_, *sql_proxy_, ctx_))) {
      LOG_WARN("failed to apply user table schema defs", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(insert_shared_meta_reserved_info_(user_tenant_id))) {
      LOG_WARN("failed to insert shared meta reserved info", KR(ret), K(user_tenant_id));
    }
  }
  return ret;
}

int ObParallelCreateTenantExecutor::create_user_table_for_user_tenant_(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("failed to check inner stat", KR(ret));
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid user tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(wait_tenant_user_ls_leader_(tenant_id))) {
    LOG_WARN("failed to wait user tenant user ls leader", KR(ret), K(tenant_id));
  } else if (OB_FAIL(apply_user_table_schema_defs_(
                 tenant_id, *schema_service_, *sql_proxy_, ctx_))) {
    LOG_WARN("failed to apply user tenant table schema defs", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::insert_shared_meta_reserved_info_(const uint64_t user_tenant_id)
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
  if (GCTX.is_shared_storage_mode()) {
    int tmp_ret = OB_SUCCESS;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(user_tenant_id);
    ObMySQLTransaction trans;
    storage::ObSharedMetaReservedMgr mgr;
    if (OB_FAIL(mgr.init(user_tenant_id))) {
      LOG_WARN("fail to init shared meta reserved mgr", KR(ret), K(user_tenant_id));
    } else if (OB_FAIL(trans.start(sql_proxy_, meta_tenant_id))) {
      LOG_WARN("failed to start trans", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(mgr.create_meta_row(trans))) {
      LOG_WARN("fail to insert shared meta reserved row", KR(ret), K(user_tenant_id));
    }
    if (trans.is_started()) {
      bool commit = OB_SUCC(ret);
      if (OB_TMP_FAIL(trans.end(commit))) {
        LOG_WARN("trans end failed", K(commit), KR(tmp_ret));
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
#endif
  return ret;
}

int ObParallelCreateTenantExecutor::check_inner_stat_()
{
  int ret = OB_SUCCESS;
  if (!create_tenant_arg_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create tenant arg is invalid", KR(ret), K(create_tenant_arg_));
  } else if (OB_ISNULL(rpc_proxy_) || OB_ISNULL(common_rpc_) || OB_ISNULL(sql_proxy_) 
      || OB_ISNULL(schema_service_) || OB_ISNULL(lst_operator_) || OB_ISNULL(location_service_)
      || OB_ISNULL(rs_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pointer", KR(ret), K(rpc_proxy_), K(common_rpc_), K(sql_proxy_), K(schema_service_),
        K(lst_operator_), K(location_service_), K(rs_mgr_));
  }
  return ret;
}

int ObParallelCreateTenantExecutor::check_user_session_interrupted_()
{
  int ret = OB_SUCCESS;
  // no extra log here: is_user_session_terminated has logged the interrupt
  // reason (including session id) when it detects a terminated session.
  is_user_session_terminated(ret);
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
#ifdef OB_BUILD_SHARED_STORAGE
int ObParallelCreateTenantExecutor::upload_root_key_()
{
  int ret = OB_SUCCESS;
  const uint64_t user_tenant_id = user_tenant_schema_.get_tenant_id();
  const uint64_t meta_tenant_id = meta_tenant_schema_.get_tenant_id();
  const int64_t timeout = ctx_.get_timeout();
  ObAddr leader;
  
  if (!GCTX.is_shared_storage_mode()) {
  } else if (GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_4_4_1_0) {
  } else if (OB_FAIL(check_inner_stat_())) {
    LOG_WARN("variable is not init", KR(ret));
  } else {
    FLOG_INFO("[CREATE_TENANT] start upload root key", K(user_tenant_id));
    // Any replica can upload root key; wait for meta tenant SYS_LS leader before RPC.
    if (OB_FAIL(wait_ls_leader_(meta_tenant_id, SYS_LS, true/*force_renew*/))) {
      LOG_WARN("failed to wait meta tenant sys ls leader", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(location_service_->get_leader(GCONF.cluster_id,
                                            meta_tenant_id,
                                            SYS_LS,
                                            FALSE,
                                            leader))) {
      LOG_WARN("failed to get meta tenant sys ls leader", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(rpc_proxy_->to(leader).timeout(timeout)
          .by(user_tenant_id).notify_upload_root_key(user_tenant_id))) {
      LOG_WARN("failed to upload root key", KR(ret), K(user_tenant_id), K(leader));
    }

    FLOG_INFO("[CREATE_TENANT] finish upload root key", KR(ret), K(user_tenant_id), K(leader));
  }

  return ret;
}
#endif
#endif
}
}
