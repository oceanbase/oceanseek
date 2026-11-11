/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_ROOTSERVER_OB_TENANT_PARALLEL_CREATE_EXECUTOR_H_
#define _OCEANBASE_ROOTSERVER_OB_TENANT_PARALLEL_CREATE_EXECUTOR_H_ 1

#include "share/ob_rpc_struct.h"
#include "src/share/ls/ob_ls_table_operator.h"
#include "rootserver/ob_rs_async_rpc_proxy.h"

namespace oceanbase 
{
namespace rootserver 
{
class ObDDLService;
class ObParallelCreateTenantExecutor
{
public:
  ObParallelCreateTenantExecutor() : create_tenant_arg_(), rpc_proxy_(NULL), common_rpc_(NULL),
    sql_proxy_(NULL), schema_service_(NULL), lst_operator_(NULL), location_service_(NULL), 
    ctx_(), create_tenant_schema_result_(), user_tenant_schema_(), meta_tenant_schema_() {}
  int execute(obrpc::UInt64 &tenant_id);
  int init(
      const obrpc::ObCreateTenantArg &create_tenant_arg,
      obrpc::ObSrvRpcProxy *rpc_proxy,
      obrpc::ObCommonRpcProxy *common_rpc,
      common::ObMySQLProxy *sql_proxy,
      share::schema::ObMultiVersionSchemaService *schema_service,
      share::ObLSTableOperator *lst_operator,
      share::ObLocationService *location_service);
  TO_STRING_KV(K_(create_tenant_arg), KP_(rpc_proxy), KP_(common_rpc), KP_(sql_proxy),
      KP_(schema_service), KP_(lst_operator), KP_(location_service), K_(ctx), 
      K_(create_tenant_schema_result), K(user_tenant_schema_), K(meta_tenant_schema_));
private:
  int save_data_version_in_palf_kv_();
#ifdef OB_BUILD_SHARED_STORAGE
  int create_sys_ls_and_normal_tenant_for_ss_(
      obrpc::UInt64 &tenant_id,
      ObParallelCreateNormalTenantProxy &proxy);
#endif
  int create_sys_ls_and_normal_tenant_for_sn_(
      obrpc::UInt64 &tenant_id,
      ObParallelCreateNormalTenantProxy &proxy);
  int create_meta_tenant_user_ls_(ObParallelCreateNormalTenantProxy &proxy);
  // SS mode: legacy no-op kept for create-tenant flow compatibility.
  int reserve_meta_tenant_user_ls_id_for_ss_(const uint64_t meta_tenant_id);
  int create_user_ls_(ObParallelCreateNormalTenantProxy &proxy);

  int wait_all_(ObParallelCreateNormalTenantProxy &proxy, const int ret_code);

  int init_after_create_tenant_schema_();

  int async_call_create_normal_tenant_(
      const ObTenantSchema &tenant_schema,
      ObParallelCreateNormalTenantProxy &proxy);

  int call_create_normal_tenant_(
      const ObTenantSchema &tenant_schema,
      const bool async_call,
      ObParallelCreateNormalTenantProxy &proxy);

  int call_create_normal_tenant_(ObParallelCreateNormalTenantProxy &proxy);

  int wait_ls_leader_(const uint64_t tenant_id, const share::ObLSID &ls_id, const bool force_renew = false);
  int wait_tenant_user_ls_leader_(const uint64_t tenant_id);
  // Same filter as ObNewTableTabletAllocator::get_available_ls; ready when at least one eligible ls has leader.
  int check_has_available_ls_for_tablet_alloc_(const uint64_t tenant_id, bool &ready);
  int do_check_can_create_user_ls_(const uint64_t tenant_id, bool &can_create);
  int check_can_create_user_ls_(const uint64_t tenant_id,
      ObParallelCreateNormalTenantProxy &proxy);
  int notify_create_tenant_user_ls_rpc_(const uint64_t tenant_id,
      ObParallelCreateNormalTenantProxy &proxy);

  int finish_create_tenant_(const int ret_code);
  int create_user_table_for_meta_tenant_(const uint64_t tenant_id);
  int create_user_table_for_user_tenant_(const uint64_t tenant_id);
  int create_meta_tenant_ss_meta_tablegroup_(const uint64_t meta_tenant_id);
  int insert_shared_meta_reserved_info_(const uint64_t user_tenant_id);
  static int apply_user_table_schema_defs_(const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService &schema_service,
      common::ObMySQLProxy &sql_proxy,
      const common::ObTimeoutCtx &timeout_ctx);
  int check_inner_stat_();
  // CREATE TENANT runs in the user session's worker thread; this checks the
  // original user session state (skipped when no user session is attached,
  // e.g. restore/clone scheduler threads) so that a disconnected client leads
  // to a fast rollback instead of waiting until the DDL timeout.
  int check_user_session_interrupted_();
  int get_tenant_schema_from_inner_table_();

  int create_tenant_sys_ls_();
#ifdef OB_BUILD_SHARED_STORAGE
  int create_meta_tenant_sys_ls_();
  int create_user_tenant_sys_ls_();
  // Wait until meta tenant schema is refreshed on target observers before creating
  // user tenant SYS LS (STEP 3.4). Meta schema target observers are user tenant unit
  // servers, because SS meta service writes __all_shared_* tables there.
  // SYS tenant schema refresh is delegated to RS directly without specifying addrs.
  int try_wait_schema_ready_for_user_sys_ls_();
  // Warm up shared-meta tablet/LS location cache on user-tenant unit servers
  // before creating user SYS LS. This issues synchronous tablet_location_send RPCs.
  int broadcast_shared_meta_tables_location_();
  int collect_shared_meta_tablet_ids_(const uint64_t meta_tenant_id,
      common::ObIArray<share::ObLSID> &warm_up_ls_ids,
      common::ObIArray<common::ObTabletID> &tablet_ids);
  int build_tablet_location_broadcast_tasks_(const uint64_t meta_tenant_id,
      const common::ObIArray<common::ObTabletID> &tablet_ids,
      common::ObIArray<share::ObLSID> &grouped_ls_ids,
      common::ObIArray<share::ObTabletLocationBroadcastTask> &broadcast_tasks);
  int send_tablet_location_broadcast_rpc_(
      const common::ObIArray<share::ObTabletLocationBroadcastTask> &broadcast_tasks,
      const common::ObIArray<common::ObAddr> &target_servers);
  int publish_schema_via_rs_(const uint64_t tenant_id, const common::ObIArray<common::ObAddr> &addrs,
      int64_t &refreshed_schema_version);
  int collect_meta_schema_sync_target_servers_(const uint64_t user_tenant_id, common::ObIArray<common::ObAddr> &addrs);
#endif

  int create_tenant_sys_ls_(const ObTenantSchema &tenant_schema,
      const ObIArray<share::ObResourcePoolName> &pool_list,
      const bool create_ls_with_palf,
      const palf::PalfBaseInfo &palf_base_info,
      const uint64_t source_tenant_id,
      const share::ObAllTenantInfo &tenant_info,
      const share::ObLSID &ls_id = share::SYS_LS);

  int construct_tenant_info_(const uint64_t tenant_id, share::ObAllTenantInfo &tenant_info);
  share::SCN get_recovery_until_scn_();
  palf::PalfBaseInfo get_palf_base_info_();
  bool get_create_ls_with_palf_();

  bool async_rpc_has_error(ObParallelCreateNormalTenantProxy &proxy);
  
#ifdef OB_BUILD_TDE_SECURITY
#ifdef OB_BUILD_SHARED_STORAGE
  int upload_root_key_();
#endif
#endif
private:
  // set by outside
  obrpc::ObCreateTenantArg create_tenant_arg_;
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  obrpc::ObCommonRpcProxy *common_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObLSTableOperator *lst_operator_;
  share::ObLocationService *location_service_;
  share::ObRsMgr *rs_mgr_;
private:
  // inited in init function
  ObTimeoutCtx ctx_;
  // inited after first rpc
  obrpc::ObCreateTenantSchemaResult create_tenant_schema_result_;
  ObTenantSchema user_tenant_schema_;
  ObTenantSchema meta_tenant_schema_;
};
}
}

#endif // _OCEANBASE_ROOTSERVER_OB_TENANT_PARALLEL_CREATE_EXECUTOR_H_
