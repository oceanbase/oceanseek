/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX BOOTSTRAP

#include "rootserver/ob_bootstrap.h"

#include "lib/restore/ob_storage.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ls/ob_ls_creator.h"//ObLSCreator
#include "share/ls/ob_ls_life_manager.h"//ObLSLifeAgentManager
#include "rootserver/ob_table_creator.h"
#include "rootserver/ob_heartbeat_service.h"
#include "rootserver/ob_root_service.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "close_modules/tde_security/share/ob_master_key_getter.h"
#endif
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/object_storage/ob_device_connectivity.h"
#include "storage/shared_storage/ob_ss_format_util.h"
#include "storage/shared_storage/ob_ss_cluster_info.h"
#include "share/ob_rpc_struct.h"//ObCreateLSArg
#include "storage/tx_storage/ob_ls_service.h"//ObLSService
#include "storage/ls/ob_ls.h"//ObLS
#include "storage/tablet/ob_tablet_iterator.h"//ObLSTabletIterator
#include "storage/tablet/ob_tablet.h"//ObTablet
#include "storage/incremental/ob_shared_meta_service.h"//ObSSMetaService
#include "storage/incremental/ob_sswriter_service.h"//ObSSWriterService
#endif
#include "share/inner_table/ob_load_inner_table_schema.h"
#include "rootserver/ob_load_inner_table_schema_executor.h"
#include "rootserver/ob_disaster_recovery_replace_tenant.h"
#include "rootserver/ob_user_table_schema_applier.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "share/ob_share_util.h"
#include "common/ob_timeout_ctx.h"

namespace oceanbase
{

using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
using namespace storage;
namespace rootserver
{

ObBaseBootstrap::ObBaseBootstrap(ObSrvRpcProxy &rpc_proxy,
                                 const ObServerInfoList &rs_list,
                                 common::ObServerConfig &config)
    : step_id_(0),
      rpc_proxy_(rpc_proxy),
      rs_list_(rs_list),
      config_(config)
{
  lib::ob_sort(rs_list_.begin(), rs_list_.end());
}

int ObBaseBootstrap::gen_sys_unit_ids(const ObIArray<ObZone> &zones,
                                      ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> sorted_zones;
  unit_ids.reuse();
  if (zones.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("zones is empty", K(zones), K(ret));
  } else if (OB_FAIL(sorted_zones.assign(zones))) {
    LOG_WARN("assign failed", K(ret));
  } else {
    lib::ob_sort(sorted_zones.begin(), sorted_zones.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < zones.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < sorted_zones.count(); ++j) {
        if (sorted_zones.at(j) == zones.at(i)) {
          if (OB_FAIL(unit_ids.push_back(OB_SYS_UNIT_ID + static_cast<uint64_t>(j)))) {
            LOG_WARN("push_back failed", K(ret));
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::check_inner_stat() const
{
  int ret = OB_SUCCESS;
  if (rs_list_.count() <= 0) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("rs_list is empty", K_(rs_list), K(ret));
  }
  return ret;
}

int ObBaseBootstrap::check_multiple_zone_deployment_rslist(
    const ObServerInfoList &rs_list)
{
  int ret = OB_SUCCESS;
  //In the multi zone deployment mode,
  //each server must come from a different zone,
  //and it will throw exception if there are duplicate zones
  for (int64_t i = 0; OB_SUCC(ret) && i < rs_list.count(); ++i) {
    const ObZone &zone = rs_list[i].zone_;
    for (int64_t j = 0; OB_SUCC(ret) && j < rs_list.count(); ++j) {
      if (i != j) {
        if (zone == rs_list[j].zone_) {
          ret = OB_PARTITION_ZONE_DUPLICATED;
          ObCStringHelper helper;
          LOG_WARN("should not choose two rs in same zone",
              "server1", helper.convert(rs_list[i].server_),
              "server2", helper.convert(rs_list[j].server_), K(zone), K(ret));
        }
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::check_bootstrap_rs_list(
    const ObServerInfoList &rs_list)
{
  int ret = OB_SUCCESS;
  if (rs_list.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs_list size must larger than 0", K(ret));
#ifdef OB_ENABLE_STANDALONE_LAUNCH
  } else if (rs_list.count() != 1) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("in standalone mode, bootstrap with more than server is not allowed", KR(ret), K(rs_list));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "bootstrap more than one server in standalone mode");
#endif
  } else if (OB_FAIL(check_multiple_zone_deployment_rslist(rs_list))) {
    LOG_WARN("fail to check multiple zone deployment rslist", K(ret));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()) {
    // In shared_storage_mode, for now, we need the cluster to have only one single REGION.
    // This constraint will be canceled in later version.
    const ObRegion &the_only_region = rs_list[0].region_;
    for (int64_t i = 1; i < rs_list_.count(); ++i) {
      if (the_only_region != rs_list_[i].region_) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("In shared storage mode, more than one REGION is not supported yet.", KR(ret), K(rs_list));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "more than one region in shared-storage mode");
      }
    }
#endif
  }
  //BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBaseBootstrap::gen_sys_unit_ids(ObIArray<uint64_t> &unit_ids)
{
  int ret = OB_SUCCESS;
  unit_ids.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(unit_ids.push_back(OB_SYS_UNIT_ID + static_cast<uint64_t>(i)))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::gen_sys_zone_list(ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  zone_list.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs list count unexpected", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(zone_list.push_back(rs_list_.at(i).zone_))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

int ObBaseBootstrap::gen_sys_units(ObIArray<share::ObUnit> &units)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> unit_ids;
  units.reuse();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(gen_sys_unit_ids(unit_ids))) {
    LOG_WARN("gen_sys_unit_ids failed", K(ret));
  } else {
    ObUnit unit;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      unit.reset();
      unit.unit_id_ = unit_ids.at(i);
      unit.resource_pool_id_ = OB_SYS_RESOURCE_POOL_ID;
      unit.unit_group_id_ = OB_SYS_UNIT_GROUP_ID;
      unit.zone_ = rs_list_.at(i).zone_;
      unit.server_ = rs_list_.at(i).server_;
      unit.status_ = ObUnit::UNIT_STATUS_ACTIVE;
      if (OB_FAIL(units.push_back(unit))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
  }
  return ret;
}

ObPreBootstrap::ObPreBootstrap(ObSrvRpcProxy &rpc_proxy,
                               const ObServerInfoList &rs_list,
                               ObLSTableOperator &lst_operator,
                               common::ObServerConfig &config,
                               const ObBootstrapArg &arg,
                               obrpc::ObCommonRpcProxy &rs_rpc_proxy)
  : ObBaseBootstrap(rpc_proxy, rs_list, config),
    stop_(false),
    ls_leader_waiter_(lst_operator, stop_),
    begin_ts_(0),
    arg_(arg),
    common_proxy_(rs_rpc_proxy)
{
}

int ObPreBootstrap::prepare_bootstrap(ObAddr &master_rs)
{
  int ret = OB_SUCCESS;
  bool is_empty = false;
  bool match = false;
  LOG_DBA_INFO_V2(OB_BOOTSTRAP_PREPARE_BEGIN, "bootstrap prepare begin.");
  common::ObArray<share::ObUnit> unit_array;
  begin_ts_ = ObTimeUtility::current_time();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(check_bootstrap_rs_list(rs_list_))) {
    LOG_WARN("failed to check_bootstrap_rs_list", KR(ret), K_(rs_list));
  } else if (OB_FAIL(check_all_server_bootstrap_mode_match(match))) {
    LOG_WARN("fail to check all server bootstrap mode match", KR(ret));
  } else if (!match) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("cannot do bootstrap with different bootstrap mode on servers", KR(ret));
    LOG_USER_ERROR(OB_OP_NOT_ALLOW, "startup mode not match, bootstrap");
    // TODO(cangming.zl): add case
  } else if (OB_FAIL(check_is_all_server_empty(is_empty))) {
    LOG_WARN("failed to check bootstrap stat", KR(ret));
  } else if (!is_empty) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot do bootstrap on not empty server", KR(ret));
  } else if (OB_FAIL(notify_sys_tenant_root_key())) {
    LOG_WARN("fail to notify sys tenant root key", KR(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  } else if (OB_FAIL(check_and_notify_logservice_access_point())) {
    LOG_WARN("fail to notify logservice access point", KR(ret));
#endif
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (OB_FAIL(check_and_notify_shared_storage_info())) {
    LOG_WARN("fail to notify shared-storage info", KR(ret));
#endif
  } else if (OB_FAIL(notify_sys_tenant_server_unit_resource())) {
    LOG_WARN("fail to notify sys tenant server unit resource", KR(ret));
  } else if (OB_FAIL(notify_sys_tenant_config_())) {
    LOG_WARN("fail to notify sys tenant config", KR(ret));
  } else if (OB_FAIL(gen_sys_units(unit_array))) {
    LOG_WARN("fail to gen sys unit array", KR(ret));
  } else if (OB_FAIL(create_sys_ls(SYS_LS, unit_array))) {
    LOG_WARN("failed to sys ls", KR(ret));
  } else if (OB_FAIL(wait_elect_ls(SYS_LS, master_rs))) {
    LOG_WARN("failed to wait elect leader", KR(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  if (OB_FAIL(ret)) {
    LOG_DBA_ERROR_V2(OB_BOOTSTRAP_PREPARE_FAIL, ret, "bootstrap prepare fail. "
                     "you may find solutions in previous error logs or seek help from official technicians.");
  } else {
    LOG_DBA_INFO_V2(OB_BOOTSTRAP_PREPARE_SUCCESS, "bootstrap prepare success.");
  }
  return ret;
}


#ifdef OB_BUILD_SHARED_LOG_SERVICE
int ObPreBootstrap::check_and_notify_logservice_access_point()
{
  int ret = OB_SUCCESS;
  const ObString &logservice_access_point = arg_.logservice_access_point_;
  ObStorageType storage_type = OB_STORAGE_MAX_TYPE;
  if (!GCONF.enable_logservice) { // do nothing
  } else if (logservice_access_point.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("logservice_access_point is empty with enable_logservice", KR(ret), K(arg_));
  } else if (OB_FAIL(get_storage_type_from_path(logservice_access_point, storage_type))) {
    LOG_WARN("failed to get storage type from logservice access point", KR(ret));
  } else if (OB_STORAGE_S3 != storage_type && OB_STORAGE_AZBLOB != storage_type) {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("unsupported storage type for logservice access point", KR(ret), K(storage_type));
  } else {
    // Notify rs_list nodes with logservice access point
    ObNotifyLogServiceAccessPointArg rpc_arg;
    ObNotifyLogServiceAccessPointResult rpc_result;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
      if (OB_FAIL(rpc_arg.init(logservice_access_point))) {
        LOG_WARN("fail to init rpc_arg", KR(ret));
      } else if (OB_FAIL(rpc_proxy_.to(rs_list_[i].server_)
                                    .notify_logservice_access_point(rpc_arg, rpc_result))) {
        LOG_WARN("fail to send rpc notify_logservice_access_point", KR(ret), K(rs_list_[i].server_));
      } else if (OB_FAIL(rpc_result.get_ret())) {
        LOG_WARN("notify_logservice_access_point via rpc failed", KR(ret), K(rs_list_[i].server_));
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}
#endif

#ifdef OB_BUILD_SHARED_STORAGE
/*
 * Parse and check the validity of shared_storage info.
 * Check connectivity and whether ss-format file exist.
 * Then write the ss-format file and notify rs-list nodes with shared_storage info.
 */
int ObPreBootstrap::check_and_notify_shared_storage_info()
{
  int ret = OB_SUCCESS;
  if (!GCTX.is_shared_storage_mode()) {
    // do nothing in shared_nothing mode
  } else {
    const ObString &shared_storage_info_str = arg_.shared_storage_info_;
    ObAdminStorageArg shared_storage_infos;
    ObBackupDest storage_dest;
    ObDeviceConnectivityCheckManager device_conn_check_mgr;
    if (OB_UNLIKELY(shared_storage_info_str.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shared_storage_infos is empty in SS mode", KR(ret), K(arg_));
    } else if (OB_FAIL(ObStorageDestCheck::parse_shared_storage_info(shared_storage_info_str,
                  shared_storage_infos, storage_dest))) {
      LOG_WARN("failed to parse shared_storage_infos", KR(ret), K(shared_storage_infos));
    } else if (OB_UNLIKELY(!shared_storage_infos.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shared_storage_infos is invalid", KR(ret), K(shared_storage_infos));
    }
    // Check storage-dest connectivity
    if (FAILEDx(device_conn_check_mgr.check_device_connectivity(storage_dest))) {
      LOG_WARN("fail to check device connectivity", KR(ret), K(storage_dest));
    } else if (OB_FAIL(wirte_ss_format_and_cluster_info_(storage_dest))) {
      LOG_WARN("fail to write ss_format and cluster_info", KR(ret), K(storage_dest));
    }

    // Notify rs_list nodes with shared-storage info
    if (OB_SUCC(ret)) {
      ObNotifySharedStorageInfoArg rpc_arg;
      ObNotifySharedStorageInfoResult rpc_result;
      // NOTICE: For now, shared-storage info in bootstrap only include one storage-dest and used for all zones.
      //         Its zone and region are not specified and need to be assigned from rs_list.
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
        shared_storage_infos.zone_ = rs_list_[i].zone_;
        shared_storage_infos.region_ = rs_list_[i].region_;
        if (OB_FAIL(rpc_arg.init(shared_storage_infos))) {
          LOG_WARN("fail to init rpc_arg", KR(ret), K(shared_storage_infos));
        } else if (OB_FAIL(rpc_proxy_.to(rs_list_[i].server_)
                                      .notify_shared_storage_info(rpc_arg, rpc_result))) {
          LOG_WARN("fail to send rpc notify_shared_stoarge_info", KR(ret), K(rpc_arg), K(rs_list_[i].server_));
        } else if (OB_FAIL(rpc_result.get_ret())) {
          LOG_WARN("notfiy_shared_storage_info via rpc failed", KR(ret), K(rpc_arg), K(rs_list_[i].server_));
        }
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::wirte_ss_format_and_cluster_info_(const ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  // Check and write the ss-format file to prevent from write conflict with another cluster
  // in same shared-storage directory
  bool is_ss_format_exist = false;
  bool is_ss_cluster_info_exist = false;
  ObSSFormat ss_format;
  ObSSClusterInfo ss_cluster_info;
  uint64_t logservice_cluster_id = OB_INVALID_ID;
  if (FAILEDx(ObSSFormatUtil::is_exist_ss_format(storage_dest, is_ss_format_exist))) {
    LOG_WARN("fail to judge ss_format exist", KR(ret), K(storage_dest), K(is_ss_format_exist));
  } else if (OB_UNLIKELY(is_ss_format_exist)) {
    ret = OB_FILE_ALREADY_EXIST;
    LOG_ERROR("ss_format file already exist, shared storage must use new path"
              "as the object storage path", KR(ret), K(is_ss_format_exist), K(storage_dest));
    LOG_USER_ERROR(OB_ENTRY_EXIST, "ss_format file already exist,"
                  "shared storage must use new path as the object storage path");
  } else if (OB_FAIL(ss_format.init(CLUSTER_CURRENT_VERSION, ObTimeUtility::fast_current_time()))) {
    LOG_WARN("fail to init ss_format", KR(ret), K(CLUSTER_CURRENT_VERSION));
  } else if (OB_FAIL(ObSSFormatUtil::write_ss_format(storage_dest, ss_format))) {
    LOG_ERROR("fail to write ss_format file", KR(ret), K(storage_dest), K(ss_format));
  } else if (OB_FAIL(ObSSClusterInfoUtil::is_exist_ss_cluster_info(storage_dest, is_ss_cluster_info_exist))) {
    LOG_WARN("fail to judge ss_cluster_info exist", KR(ret), K(storage_dest));
  } else if (OB_UNLIKELY(is_ss_cluster_info_exist)) {
    ret = OB_FILE_ALREADY_EXIST;
    LOG_ERROR("ss_cluster_info file already exist, shared storage must use new path"
              "as the object storage path", KR(ret), K(is_ss_cluster_info_exist), K(storage_dest));
    LOG_USER_ERROR(OB_ENTRY_EXIST, "ss_cluster_info file already exist,"
                  "shared storage must use new path as the object storage path");
  } else if (rs_list_.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rs_list size must larger than 0", K(ret));
  } else if (OB_FAIL(ObDRReplaceTenant::get_logservice_cluster_id(logservice_cluster_id))) {
    LOG_WARN("fail to get logservice cluster id", KR(ret));
  } else if (OB_FAIL(ss_cluster_info.init(logservice_cluster_id, rs_list_.at(0).region_))) {
    LOG_WARN("fail to init ss_cluster_info", KR(ret), K(logservice_cluster_id), K(CLUSTER_CURRENT_VERSION));
  } else if (OB_FAIL(ObSSClusterInfoUtil::write_ss_cluster_info(storage_dest, ss_cluster_info))) {
    LOG_ERROR("fail to write ss_cluster_info file", KR(ret), K(storage_dest), K(ss_cluster_info));
  }
  FLOG_INFO("write ss_cluster_info", KR(ret), K(ss_cluster_info), K(ss_format));
  return ret;
}
#endif

int ObPreBootstrap::notify_sys_tenant_root_key()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_TDE_SECURITY
  ObArray<ObAddr> addrs;
  obrpc::ObRootKeyArg arg;
  arg.tenant_id_ = OB_SYS_TENANT_ID;
  arg.is_set_ = false;
  obrpc::ObRootKeyResult result;
  if (OB_FAIL(addrs.reserve(rs_list_.count()))) {
    LOG_WARN("fail to reserve array", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
    if (OB_FAIL(addrs.push_back(rs_list_[i].server_))) {
      LOG_WARN("fail to push back server", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(OB_SYS_TENANT_ID,
                                                                result.key_type_,
                                                                result.root_key_))) {
  } else if (obrpc::RootKeyType::INVALID != result.key_type_ || !result.root_key_.empty()) {
    LOG_INFO("root key existed in local");
  } else if (OB_FAIL(ObTenantDDLService::notify_root_key(rpc_proxy_, arg, addrs, result, false))) {
    LOG_WARN("fail to notify root key", K(ret));
  } else if (obrpc::RootKeyType::INVALID != result.key_type_ || !result.root_key_.empty()) {
    LOG_INFO("root key existed in remote");
  } else if (OB_FAIL(ObTenantDDLService::create_root_key(rpc_proxy_, OB_SYS_TENANT_ID, addrs))) {
    LOG_WARN("fail to create sys tenant root key", KR(ret), K(addrs));
  }
  BOOTSTRAP_CHECK_SUCCESS();
#endif
  return ret;
}

int ObPreBootstrap::notify_sys_tenant_server_unit_resource()
{
  int ret = OB_SUCCESS;
  ObUnitConfig unit_config;
  common::ObArray<uint64_t> sys_unit_id_array;
  const bool is_hidden_sys = false;

#ifdef OB_BUILD_TDE_SECURITY
  obrpc::ObRootKeyResult root_key_result;
#endif

  if (OB_FAIL(unit_config.gen_sys_tenant_unit_config(is_hidden_sys))) {
    LOG_WARN("gen sys tenant unit config fail", KR(ret), K(is_hidden_sys));
  } else if (OB_FAIL(gen_sys_unit_ids(sys_unit_id_array))) {
    LOG_WARN("fail to gen sys unit ids", KR(ret));
  } else if (OB_UNLIKELY(sys_unit_id_array.count() != rs_list_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sys unit id array and rs list count not match", KR(ret),
             "unit_id_array_cnt", sys_unit_id_array.count(), "rs_list_cnt", rs_list_.count());
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(ObMasterKeyGetter::instance().get_root_key(OB_SYS_TENANT_ID,
                                                                root_key_result.key_type_,
                                                                root_key_result.root_key_))) {
    LOG_WARN("failed to get sys tenant root key", K(ret));
#endif
  } else {
    ObNotifyTenantServerResourceProxy notify_proxy(
                                      rpc_proxy_,
                                      &ObSrvRpcProxy::notify_tenant_server_unit_resource);
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      int64_t rpc_timeout = NOTIFY_RESOURCE_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      obrpc::TenantServerUnitConfig tenant_unit_server_config;
      if (OB_FAIL(tenant_unit_server_config.init(
              OB_SYS_TENANT_ID,
              sys_unit_id_array.at(i),
              lib::Worker::CompatMode::MYSQL,
              unit_config,
              ObReplicaType::REPLICA_TYPE_FULL,
              false/*if not grant*/,
              false/*is_delete*/
#ifdef OB_BUILD_TDE_SECURITY
              , root_key_result
#endif
              ))) {
        LOG_WARN("fail to init tenant unit server config", KR(ret));
      } else if (OB_FAIL(notify_proxy.call(
              rs_list_[i].server_, rpc_timeout, tenant_unit_server_config))) {
        LOG_WARN("fail to call notify resource to server",
                 K(ret), "dst", rs_list_[i].server_, K(rpc_timeout));
      }
    }
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(notify_proxy.wait())) {
      LOG_WARN("fail to wait notify resource", K(ret), K(tmp_ret));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    } else if (OB_SUCC(ret)) {
      // can use arg/dest/result here.
    }
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::notify_sys_tenant_config_()
{
  int ret = OB_SUCCESS;
  common::ObConfigPairs config;
  common::ObSEArray<common::ObConfigPairs, 1> init_configs;
  ObArray<ObAddr> addrs;
  if (OB_FAIL(ObTenantDDLService::gen_tenant_init_config(
      OB_SYS_TENANT_ID, DATA_CURRENT_VERSION, config))) {
  } else if (OB_FAIL(init_configs.push_back(config))) {
    LOG_WARN("fail to push back config", KR(ret), K(config));
  } else if (OB_FAIL(addrs.reserve(rs_list_.count()))) {
    LOG_WARN("fail to reserve array", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
    if (OB_FAIL(addrs.push_back(rs_list_[i].server_))) {
      LOG_WARN("fail to push back server", KR(ret));
    }
  } // end for
  if (FAILEDx(ObTenantDDLService::notify_init_tenant_config(
              rpc_proxy_, init_configs, addrs))) {
    LOG_WARN("fail to notify init tenant config", KR(ret), K(init_configs), K(addrs));
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::create_sys_ls(
    const ObLSID &ls_id,
    const common::ObArray<share::ObUnit> &unit_array)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start create sys tenant LS", K(ls_id));
  uint64_t data_version = 0;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", KR(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(OB_SYS_TENANT_ID))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(OB_SYS_TENANT_ID, data_version))) {
    LOG_WARN("failed to get min data version", K(ret), K(OB_SYS_TENANT_ID));
  } else {
    ObLSCreator ls_creator(rpc_proxy_, OB_SYS_TENANT_ID, ls_id, data_version);
    if (OB_FAIL(ls_creator.create_sys_tenant_ls(
            rs_list_, unit_array))) {
      LOG_WARN("fail to create sys log stream", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("succeed to create ls", K(ls_id));
  } else {
    LOG_WARN("create ls failed", KR(ret), K(ls_id));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::wait_elect_ls(
    const ObLSID &ls_id,
    common::ObAddr &master_rs)
{
  int ret = OB_SUCCESS;
  LOG_INFO("start LS election", K(ls_id));
  const uint64_t tenant_id = OB_SYS_TENANT_ID;

  int64_t timeout = WAIT_ELECT_SYS_LEADER_TIMEOUT_US;
  if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
    timeout = max(timeout, THIS_WORKER.get_timeout_remain());
  }

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id));
#ifdef OB_ENABLE_STANDALONE_LAUNCH
  } else if (!ls_id.is_sys_ls()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("standalone deployment enabled while sys tenant wait non sys ls", KR(ret), K(ls_id));
  } else if (FALSE_IT(master_rs = GCTX.self_addr())) {
#else
  } else if (OB_FAIL(ls_leader_waiter_.wait(
      tenant_id, ls_id, timeout, master_rs))) {
    LOG_WARN("leader_waiter_ wait failed", KR(ret), K(tenant_id), K(ls_id), K(timeout));
#endif
  }
  if (OB_SUCC(ret)) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("succeed to wait elect log stream", K(ls_id));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObPreBootstrap::check_all_server_bootstrap_mode_match(
    bool &match)
{
  int ret = OB_SUCCESS;
  match = true;
  Bool is_match(false);

  ObCheckDeploymentModeArg arg;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("fail to check inner stat", K(ret));
  } else if (OB_FAIL(arg.init(GCTX.startup_mode_, GCONF.enable_logservice))) {
    LOG_WARN("fail to init arg", KR(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && match && i < rs_list_.count(); ++i) {
      if (OB_FAIL(rpc_proxy_.to(rs_list_[i].server_).check_deployment_mode_match(
              arg, is_match))) {
        LOG_WARN("fail to check deployment mode match", K(ret));
      } else if (!is_match) {
        LOG_WARN("server deployment mode not match", "server", rs_list_[i].server_);
        match = false;
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();

  return ret;
}

int ObPreBootstrap::check_is_all_server_empty(bool &is_empty)
{
  int ret = OB_SUCCESS;
  is_empty = true;
  Bool is_server_empty;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ObCheckServerEmptyArg arg;
    const ObCheckServerEmptyArg::Mode mode = ObCheckServerEmptyArg::BOOTSTRAP;
    const uint64_t data_version = DATA_CURRENT_VERSION;
    for (int64_t i = 0; OB_SUCC(ret) && is_empty && i < rs_list_.count(); ++i) {
      int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
      uint64_t server_id = OB_INIT_SERVER_ID + i;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      // arg.server_id_ will be set if server is empty and mode is BOOTSTRAP.
      if (OB_FAIL(arg.init(mode, data_version, server_id))) {
        LOG_WARN("failed to init ObCheckServerEmptyArg", KR(ret), K(mode), K(data_version),
            K(server_id));
      } else if (OB_FAIL(rpc_proxy_.to(rs_list_[i].server_)
                            .timeout(rpc_timeout)
                            .check_server_empty(arg, is_server_empty))) {
        LOG_WARN("failed to check if server is empty",
            "server", rs_list_[i].server_, K(rpc_timeout), K(arg), K(ret));
      } else if (!is_server_empty) {
        // don't need to set ret
        LOG_WARN("server is not empty", "server", rs_list_[i].server_);
        is_empty = false;
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();

  return ret;
}

bool ObBootstrap::TableIdCompare::operator() (const ObSimpleTableSchemaV2* left, const ObSimpleTableSchemaV2* right)
{
  bool bret = false;

  if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret_ = OB_INVALID_ARGUMENT;
    LOG_WARN_RET(ret_, "invalid argument", K_(ret), KP(left), KP(right));
  } else {
    bool left_is_sys_index = left->is_index_table() && is_sys_table(left->get_table_id());
    bool right_is_sys_index = right->is_index_table() && is_sys_table(right->get_table_id());
    uint64_t left_table_id = left->get_table_id();
    uint64_t left_data_table_id = left->get_data_table_id();
    uint64_t right_table_id = right->get_table_id();
    uint64_t right_data_table_id = right->get_data_table_id();
    if (!left_is_sys_index && !right_is_sys_index) {
      bret = left_table_id < right_table_id;
    } else if (left_is_sys_index && right_is_sys_index) {
      bret = left_data_table_id < right_data_table_id;
    } else if (left_is_sys_index) {
      if (left_data_table_id == right_table_id) {
        bret = true;
      } else {
        bret = left_data_table_id < right_table_id;
      }
    } else {
      if (left_table_id == right_data_table_id) {
        bret = false;
      } else {
        bret = left_table_id < right_data_table_id;
      }
    }
  }
  return bret;
}


ObBootstrap::ObBootstrap(
    ObSrvRpcProxy &rpc_proxy,
    share::ObLSTableOperator &lst_operator,
    ObDDLService &ddl_service,
    ObTenantDDLService &tenant_ddl_service,
    ObUnitManager &unit_mgr,
    ObServerConfig &config,
    const obrpc::ObBootstrapArg &arg,
    obrpc::ObCommonRpcProxy &rs_rpc_proxy)
  : ObBaseBootstrap(rpc_proxy, arg.server_list_, config),
    lst_operator_(lst_operator),
    ddl_service_(ddl_service),
    tenant_ddl_service_(tenant_ddl_service),
    unit_mgr_(unit_mgr),
    arg_(arg),
    common_proxy_(rs_rpc_proxy),
    begin_ts_(0)
{
}

int ObBootstrap::execute_bootstrap(rootserver::ObServerZoneOpService &server_zone_op_service)
{
  int ret = OB_SUCCESS;
  bool already_bootstrap = true;
  ObSArray<ObTableSchema> table_schemas;
  ObArenaAllocator arena_allocator("InnerTableSchem", OB_MALLOC_MIDDLE_BLOCK_SIZE);
  begin_ts_ = ObTimeUtility::current_time();

  BOOTSTRAP_LOG(INFO, "start do execute_bootstrap");

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(check_is_already_bootstrap(already_bootstrap))) {
    LOG_WARN("failed to check_is_already_bootstrap", K(ret));
  } else if (already_bootstrap) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ob system is already bootstrap, cannot bootstrap again", K(ret));
  } else if (OB_FAIL(check_bootstrap_rs_list(rs_list_))) {
    LOG_WARN("failed to check_bootstrap_rs_list", K_(rs_list), K(ret));
  } else if (OB_FAIL(create_all_core_table_partition())) {
    LOG_WARN("fail to create all core_table partition", KR(ret));
  } else if (OB_FAIL(set_in_bootstrap())) {
    LOG_WARN("failed to set in bootstrap", K(ret));
  } else if (OB_FAIL(init_global_stat())) {
    LOG_WARN("failed to init_global_stat", K(ret));
  } else if (OB_FAIL(broadcast_sys_schema())) {
    LOG_WARN("broadcast_sys_schemas failed", K(table_schemas), K(ret));
  } else if (OB_FAIL(construct_all_schema(table_schemas, arena_allocator))) {
    LOG_WARN("construct all schema fail", K(ret));
  } else if (OB_FAIL(create_all_partitions())) {
    LOG_WARN("create all partitions fail", K(ret));
  } else if (GCONF._enable_parallel_tenant_creation &&
      OB_FAIL(load_all_schema(ddl_service_, table_schemas))) {
    LOG_WARN("load_all_schema failed",  K(table_schemas), K(ret));
  } else if (!GCONF._enable_parallel_tenant_creation &&
      OB_FAIL(create_all_schema(ddl_service_, table_schemas))) {
    LOG_WARN("create_all_schema failed",  K(table_schemas), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS_V2("create_all_schema");
  ObMultiVersionSchemaService &schema_service = ddl_service_.get_schema_service();

  if (FAILEDx(init_system_data())) {
    LOG_WARN("failed to init system data", KR(ret));
  } else {
    LOG_DBA_INFO_V2(OB_BOOTSTRAP_REFRESH_ALL_SCHEMA_BEGIN,
                    DBA_STEP_INC_INFO(bootstrap),
                    "bootstrap refresh all schema begin.");
  }
  // Create SS Meta user tables on all modes (SS and non-SS) so tests and
  // non-SS deployments can also access these tables.
  if (FAILEDx(create_sys_tenant_shared_meta_user_tables_())) {
    LOG_WARN("failed to create sys tenant shared meta user tables", KR(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS_V2("create_sys_tenant_shared_meta_user_tables");
#ifdef OB_BUILD_SHARED_STORAGE
  // parallel_create_table + publish_schema skips RS local cache refresh; heartbeat
  // refresh is blocked while in_bootstrap_. Sync refresh here so init_sys_tenant_ss_meta_
  // can SQL into __all_shared_meta / __all_shared_meta_control immediately after create.
  if (OB_SUCC(ret) && GCTX.is_shared_storage_mode()) {
    ObArray<uint64_t> tenant_ids;
    if (OB_FAIL(tenant_ids.push_back(OB_SYS_TENANT_ID))) {
      LOG_WARN("failed to push back tenant id", KR(ret));
    } else if (OB_FAIL(schema_service.refresh_and_add_schema(tenant_ids, false/*check_bootstrap*/))) {
      LOG_WARN("failed to refresh sys tenant schema after creating shared meta user tables", KR(ret));
    } else {
      LOG_INFO("refresh sys tenant schema after creating shared meta user tables success");
    }
  }
  // Persist local sys-tenant LS / non-inner tablet to V2 SS meta table
  // (__all_shared_meta) before refresh_schema, which is the last point where
  // GCTX.in_bootstrap_ is still true.
  if (FAILEDx(init_sys_tenant_ss_meta_())) {
    LOG_WARN("failed to init sys tenant ss meta", KR(ret));
  } else if (OB_FAIL(do_sys_tenant_ls_checkpoint_())) {
    LOG_WARN("failed to do sys tenant ls checkpoint", KR(ret));
  }
#endif
  if (FAILEDx(ddl_service_.refresh_schema(OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to refresh_schema", K(ret));
    LOG_DBA_ERROR_V2(OB_BOOTSTRAP_REFRESH_ALL_SCHEMA_FAIL, ret,
                     DBA_STEP_INC_INFO(bootstrap),
                     "bootstrap refresh all schema fail. [suggestion] you can: "
                     "1. Search previous error logs that may indicate the cause of this failure. "
                     "2. Check if other nodes are accessible via ssh. "
                     "2. Check whether other nodes can establish connections through sql client. "
                     "3. Check the alert.log on others node to see if there are other error logs.");
  } else {
    LOG_DBA_INFO_V2(OB_BOOTSTRAP_REFRESH_ALL_SCHEMA_SUCCESS,
                    DBA_STEP_INC_INFO(bootstrap),
                    "bootstrap refresh all schema success.");
  }
  BOOTSTRAP_CHECK_SUCCESS_V2("refresh_schema");

#ifdef OB_BUILD_SHARED_STORAGE
  if (FAILEDx(write_shared_storage_args())) {
    LOG_WARN("failed to init write shared storage args", KR(ret));
  } else {}
#endif

  if (FAILEDx(add_servers_in_rs_list(server_zone_op_service))) {
    LOG_WARN("fail to add servers in rs_list_", KR(ret));
  }
  ROOTSERVICE_EVENT_ADD("bootstrap", "bootstrap_succeed");
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::load_all_schema(
    ObDDLService &ddl_service,
    common::ObIArray<share::schema::ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  LOG_INFO("start load all schemas", "table count", table_schemas.count());
  LOG_DBA_INFO_V2(OB_BOOTSTRAP_CREATE_ALL_SCHEMA_BEGIN,
                  DBA_STEP_INC_INFO(bootstrap),
                  "bootstrap create all schema begin.");
  ObLoadInnerTableSchemaExecutor executor;
  if (OB_FAIL(executor.init(table_schemas, OB_SYS_TENANT_ID,
      GCONF.get_sys_tenant_default_max_cpu(), &rpc_proxy_))) {
    LOG_WARN("failed to init executor", KR(ret));
  } else if (OB_FAIL(executor.execute())) {
    LOG_WARN("failed to execute load all schema", KR(ret));
  } else if (OB_FAIL(ObLoadInnerTableSchemaExecutor::load_schema_version(
          OB_SYS_TENANT_ID, ddl_service.get_sql_proxy()))) {
    LOG_WARN("failed to load core schema version", KR(ret));
  }
  LOG_INFO("finish load all schemas", KR(ret), "cost", ObTimeUtility::current_time() - begin_time);
  if (OB_FAIL(ret)) {
    LOG_DBA_ERROR_V2(OB_BOOTSTRAP_CREATE_ALL_SCHEMA_FAIL, ret,
                     DBA_STEP_INC_INFO(bootstrap),
                     "bootstrap create all schema fail. "
                     "you may find solutions in previous error logs or seek help from official technicians.");
  } else {
    LOG_DBA_INFO_V2(OB_BOOTSTRAP_CREATE_ALL_SCHEMA_SUCCESS,
                    DBA_STEP_INC_INFO(bootstrap),
                    "bootstrap create all schema success.");
  }
  return ret;
}

int ObBootstrap::sort_schema(const ObIArray<ObTableSchema> &table_schemas,
                             ObIArray<ObTableSchema> &sorted_table_schemas)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObTableSchema*> ptr_table_schemas;
  if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "count", table_schemas.count());
  } else {
    for (int64_t i = 0; i < table_schemas.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(ptr_table_schemas.push_back(&table_schemas.at(i)))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      TableIdCompare compare;
      lib::ob_sort(ptr_table_schemas.begin(), ptr_table_schemas.end(), compare);
      if (OB_FAIL(compare.get_ret())) {
        LOG_WARN("fail to sort schema", KR(ret));
      } else {
        for (int64_t i = 0 ; i < ptr_table_schemas.count() && OB_SUCC(ret); i++) {
          if (OB_FAIL(sorted_table_schemas.push_back(*ptr_table_schemas.at(i)))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObBootstrap::generate_table_schema_array_for_create_partition(
    const share::schema::ObTableSchema &tschema,
    common::ObIArray<share::schema::ObTableSchema> &table_schema_array)
{
  int ret = OB_SUCCESS;
  table_schema_array.reset();
  const uint64_t table_id = tschema.get_table_id();
  int64_t tschema_idx = table_schema_array.count();

  if (OB_FAIL(table_schema_array.push_back(tschema))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(add_sys_table_lob_aux_table(table_id, table_schema_array))) {
    LOG_WARN("fail to add lob table to sys table", KR(ret), K(table_id));
  } else if (OB_FAIL(ObSysTableChecker::append_sys_table_index_schemas(
             OB_SYS_TENANT_ID, table_id, table_schema_array))) {
    LOG_WARN("fail to append sys table index schemas", KR(ret), K(table_id));
  }
  return ret;
}

int ObBootstrap::prepare_create_partition(
    ObTableCreator &creator,
    const share::schema_create_func func)
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> units;
  const bool set_primary_zone = false;
  ObTableSchema tschema;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (NULL == func) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("func is null", KR(ret));
  } else if (OB_FAIL(func(tschema))) {
    LOG_WARN("failed to create table schema", KR(ret));
  } else if (tschema.has_partition()) {
    common::ObArray<share::schema::ObTableSchema> table_schema_array;
    common::ObArray<const share::schema::ObTableSchema*> table_schema_ptrs;
    common::ObArray<share::ObLSID> ls_id_array;
    common::ObArray<bool> need_create_empty_majors;
    if (OB_FAIL(generate_table_schema_array_for_create_partition(tschema, table_schema_array))) {
      LOG_WARN("fail to generate table schema array", KR(ret));
    } else if (OB_UNLIKELY(table_schema_array.count() < 1)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generate table schema count is unexpected", KR(ret));
    } else if (OB_FAIL(table_schema_ptrs.reserve(table_schema_array.count()))
      || OB_FAIL(need_create_empty_majors.reserve(table_schema_array.count()))) {
      LOG_WARN("Fail to reserve rowkey column array", KR(ret));
    } else {
      for (int i = 0; i < table_schema_array.count() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(table_schema_ptrs.push_back(&table_schema_array.at(i)))
          || OB_FAIL(need_create_empty_majors.push_back(true))) {
          LOG_WARN("fail to push back", KR(ret), K(table_schema_array));
        }
      }
      for (int i = 0; i < tschema.get_all_part_num() && OB_SUCC(ret); ++i) {
        if (OB_FAIL(ls_id_array.push_back(share::ObLSID(SYS_LS)))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(creator.add_create_tablets_of_tables_arg(
            table_schema_ptrs,
            ls_id_array,
            DATA_CURRENT_VERSION,
            need_create_empty_majors/*need_create_empty_major_sstable*/))) {
      LOG_WARN("fail to add create tablet arg", KR(ret));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("succeed prepare create table partition",
             "table_id", tschema.get_table_id(),
             "table_name", tschema.get_table_name(),
             "cluster_role", cluster_role_to_str(arg_.cluster_role_));
  }

  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_all_core_table_partition()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ObMySQLTransaction trans;
    ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
    ObTableCreator table_creator(OB_SYS_TENANT_ID,
                                 SCN::base_scn(),
                                 trans);
    if (OB_FAIL(trans.start(&sql_proxy, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(table_creator.init(false/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init tablet creator", KR(ret));
    } else {
      // create all core table partition
      for (int64_t i = 0; OB_SUCC(ret) && NULL != all_core_table_schema_creator[i]; ++i) {
        if (OB_FAIL(prepare_create_partition(
            table_creator, all_core_table_schema_creator[i]))) {
          LOG_WARN("prepare create partition fail", K(ret));
        }
      }
      // execute creating tablet
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_creator.execute())) {
          LOG_WARN("execute create partition failed", K(ret));
        }
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }
  }

  LOG_INFO("finish creating all core table", K(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_all_partitions()
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> sys_table_ids;
  ObArray<int64_t> partition_nums;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    ObMySQLTransaction trans;
    ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
    ObTableCreator table_creator(OB_SYS_TENANT_ID,
                                 SCN::base_scn(),
                                 trans);
    if (OB_FAIL(trans.start(&sql_proxy, OB_SYS_TENANT_ID))) {
      LOG_WARN("fail to start trans", KR(ret));
    } else if (OB_FAIL(table_creator.init(false/*need_tablet_cnt_check*/))) {
      LOG_WARN("fail to init tablet creator", KR(ret));
    } else {
      // create core table partition
      for (int64_t i = 0; OB_SUCC(ret) && NULL != core_table_schema_creators[i]; ++i) {
        if (OB_FAIL(prepare_create_partition(
            table_creator, core_table_schema_creators[i]))) {
          LOG_WARN("prepare create partition fail", K(ret));
        }
      }
      // create sys table partition
      for (int64_t i = 0; OB_SUCC(ret) && NULL != sys_table_schema_creators[i]; ++i) {
        if (OB_FAIL(prepare_create_partition(
            table_creator, sys_table_schema_creators[i]))) {
          LOG_WARN("prepare create partition fail", K(ret));
        }
      }
      // execute creating tablet
      if (OB_SUCC(ret)) {
        if (OB_FAIL(table_creator.execute())) {
          LOG_WARN("execute create partition failed", K(ret));
        }
      }
    }
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      bool commit = OB_SUCC(ret);
      if (OB_SUCCESS != (temp_ret = trans.end(commit))) {
        ret = (OB_SUCC(ret)) ? temp_ret : ret;
        LOG_WARN("trans end failed", K(commit), K(temp_ret));
      }
    }
  }

  LOG_INFO("finish creating system tables", K(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::add_sys_table_lob_aux_table(
    uint64_t data_table_id,
    ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  if (is_system_table(data_table_id)) {
    HEAP_VARS_2((ObTableSchema, lob_meta_schema), (ObTableSchema, lob_piece_schema)) {
      if (is_hardcode_schema_table(data_table_id)) {
        // do nothing
      } else if (OB_FAIL(get_sys_table_lob_aux_schema(data_table_id, lob_meta_schema, lob_piece_schema))) {
        LOG_WARN("fail to get sys table lob aux schema", KR(ret), K(data_table_id));
      } else if (OB_FAIL(table_schemas.push_back(lob_meta_schema))) {
        LOG_WARN("fail to push lob meta into schemas", KR(ret), K(data_table_id));
      } else if (OB_FAIL(table_schemas.push_back(lob_piece_schema))) {
        LOG_WARN("fail to push lob piece into schemas", KR(ret), K(data_table_id));
      }
    }
  }
  return ret;
}

int ObBootstrap::construct_all_schema(ObSArray<ObTableSchema> &table_schemas, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> table_ids_to_construct; // empty means construct all 
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(ObSchemaUtils::construct_inner_table_schemas(OB_SYS_TENANT_ID,
          table_ids_to_construct, true/*include_index_and_lob_aux_schemas*/, allocator, table_schemas))) {
    LOG_WARN("failed to construct inner table schemas", KR(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::broadcast_sys_schema()
{
  int ret = OB_SUCCESS;
  obrpc::ObBatchBroadcastSchemaArg arg;
  obrpc::ObBatchBroadcastSchemaResult result;
  ObArray<ObTableSchema> table_schemas;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(arg.init(OB_SYS_TENANT_ID,
                              OB_CORE_SCHEMA_VERSION,
                              table_schemas,
                              true/*generate_schema*/))) {
    LOG_WARN("fail to init arg", KR(ret));
  } else {
    ObBatchBroadcastSchemaProxy proxy(rpc_proxy_,
                                      &ObSrvRpcProxy::batch_broadcast_schema);
    FOREACH_CNT_X(rs, rs_list_, OB_SUCC(ret)) {
      bool is_active = false;
      int64_t rpc_timeout = obrpc::ObRpcProxy::MAX_RPC_TIMEOUT;
      if (INT64_MAX != THIS_WORKER.get_timeout_ts()) {
        rpc_timeout = max(rpc_timeout, THIS_WORKER.get_timeout_remain());
      }
      if (OB_FAIL(proxy.call(rs->server_, rpc_timeout, arg))) {
        LOG_WARN("broadcast_sys_schema failed", KR(ret), K(rpc_timeout),
                 "server", rs->server_);
      }
    } // end foreach

    ObArray<int> return_code_array;
    int tmp_ret = OB_SUCCESS; // always wait all
    if (OB_TMP_FAIL(proxy.wait_all(return_code_array))) {
      LOG_WARN("wait batch result failed", KR(tmp_ret), KR(ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    } else if (OB_FAIL(ret)) {
    } else if (OB_FAIL(proxy.check_return_cnt(return_code_array.count()))) {
      LOG_WARN("return cnt not match", KR(ret), "return_cnt", return_code_array.count());
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < return_code_array.count(); i++) {
        int res_ret = return_code_array.at(i);
        const ObAddr &addr = proxy.get_dests().at(i);
        if (OB_SUCCESS != res_ret) {
          ret = res_ret;
          LOG_WARN("broadcast schema failed", KR(ret), K(addr));
        }
      } // end for
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_all_schema(ObDDLService &ddl_service,
                                   ObIArray<ObTableSchema> &table_schemas)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  LOG_INFO("start create all schemas", "table count", table_schemas.count());
  LOG_DBA_INFO_V2(OB_BOOTSTRAP_CREATE_ALL_SCHEMA_BEGIN,
                  DBA_STEP_INC_INFO(bootstrap),
                  "bootstrap create all schema begin.");
  if (table_schemas.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("table_schemas is empty", K(table_schemas), K(ret));
  } else {
    // persist __all_core_table's schema in inner table, which is only used for sys views.
    HEAP_VAR(ObTableSchema, core_table) {
       ObArray<ObTableSchema> tmp_tables;
      if (OB_FAIL(ObInnerTableSchema::all_core_table_schema(core_table))) {
        LOG_WARN("fail to construct __all_core_table's schema", KR(ret), K(core_table));
      } else if (OB_FAIL(tmp_tables.push_back(core_table))) {
        LOG_WARN("fail to push back __all_core_table's schema", KR(ret), K(core_table));
      } else if (OB_FAIL(batch_create_schema(ddl_service, tmp_tables, 0, 1))) {
        LOG_WARN("fail to create __all_core_table's schema", KR(ret), K(core_table));
      }
    }

    int64_t begin = 0;
    int64_t batch_count = BATCH_INSERT_SCHEMA_CNT;
    const int64_t MAX_RETRY_TIMES = 3;
    for (int64_t i = 0; OB_SUCC(ret) && i < table_schemas.count(); ++i) {
      if (table_schemas.count() == (i + 1) || (i + 1 - begin) >= batch_count) {
        int64_t retry_times = 1;
        while (OB_SUCC(ret)) {
          if (OB_FAIL(batch_create_schema(ddl_service, table_schemas, begin, i + 1))) {
            LOG_WARN("batch create schema failed", K(ret), "table count", i + 1 - begin);
            // bugfix:
            if ((OB_SCHEMA_EAGAIN == ret
                 || OB_ERR_WAIT_REMOTE_SCHEMA_REFRESH == ret)
                && retry_times <= MAX_RETRY_TIMES) {
              retry_times++;
              ret = OB_SUCCESS;
              LOG_INFO("schema error while create table, need retry", KR(ret), K(retry_times));
              ob_usleep(1 * 1000 * 1000L); // 1s
            }
          } else {
            break;
          }
        }
        if (OB_SUCC(ret)) {
          begin = i + 1;
        }
      }
    }
  }
  LOG_INFO("end create all schemas", K(ret), "table count", table_schemas.count(),
           "time_used", ObTimeUtility::current_time() - begin_time);
  if (OB_FAIL(ret)) {
    LOG_DBA_ERROR_V2(OB_BOOTSTRAP_CREATE_ALL_SCHEMA_FAIL, ret,
                     DBA_STEP_INC_INFO(bootstrap),
                     "bootstrap create all schema fail. "
                     "you may find solutions in previous error logs or seek help from official technicians.");
  } else {
    LOG_DBA_INFO_V2(OB_BOOTSTRAP_CREATE_ALL_SCHEMA_SUCCESS,
                    DBA_STEP_INC_INFO(bootstrap),
                    "bootstrap create all schema success.");
  }
  return ret;
}

int ObBootstrap::batch_create_schema(ObDDLService &ddl_service,
                                     ObIArray<ObTableSchema> &table_schemas,
                                     const int64_t begin, const int64_t end)
{
  int ret = OB_SUCCESS;
  const int64_t begin_time = ObTimeUtility::current_time();
  ObDDLSQLTransaction trans(&(ddl_service.get_schema_service()), true, true, false, false);
  if (begin < 0 || begin >= end || end > table_schemas.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(begin), K(end),
        "table count", table_schemas.count());
  } else {
    ObDDLOperator ddl_operator(ddl_service.get_schema_service(),
        ddl_service.get_sql_proxy());
    int64_t refreshed_schema_version = 0;
    if (OB_FAIL(trans.start(&ddl_service.get_sql_proxy(),
                            OB_SYS_TENANT_ID,
                            refreshed_schema_version))) {
      LOG_WARN("start transaction failed", KR(ret));
    } else {
      bool is_truncate_table = false;
      for (int64_t i = begin; OB_SUCC(ret) && i < end; ++i) {
        ObTableSchema &table = table_schemas.at(i);
        const ObString *ddl_stmt = NULL;
        bool need_sync_schema_version = !(ObSysTableChecker::is_sys_table_index_tid(table.get_table_id()) ||
                                          is_sys_lob_table(table.get_table_id()));
        int64_t start_time = ObTimeUtility::current_time();
        if (FAILEDx(ddl_operator.create_table(table, trans, ddl_stmt,
                                              need_sync_schema_version,
                                              is_truncate_table))) {
          LOG_WARN("add table schema failed", K(ret),
              "table_id", table.get_table_id(),
              "table_name", table.get_table_name());
        } else {
          int64_t end_time = ObTimeUtility::current_time();
          LOG_INFO("add table schema succeed", K(i),
              "table_id", table.get_table_id(),
              "table_name", table.get_table_name(), "core_table", is_core_table(table.get_table_id()), "cost", end_time-start_time);
        }
      }
    }
  }

  const int64_t begin_commit_time = ObTimeUtility::current_time();
  if (trans.is_started()) {
    const bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("end trans failed", K(tmp_ret), K(is_commit));
      ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    }
  }
  const int64_t now = ObTimeUtility::current_time();
  LOG_INFO("batch create schema finish", K(ret), "table count", end - begin,
      "total_time_used", now - begin_time,
      "end_transaction_time_used", now - begin_commit_time);
  //BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::construct_schema(
    const share::schema_create_func func, ObTableSchema &tschema)
{
  int ret = OB_SUCCESS;
  BOOTSTRAP_CHECK_SUCCESS_V2("before construct schema");
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (NULL == func) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("func is null", K(ret));
  } else if (OB_FAIL(func(tschema))) {
    LOG_WARN("failed to create table schema", K(ret));
  } else {} // no more to do
  return ret;
}

int ObBootstrap::add_servers_in_rs_list(rootserver::ObServerZoneOpService &server_zone_op_service) {
  int ret = OB_SUCCESS;
  ObArray<ObAddr> servers;
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.root_service_ is null", KR(ret), KP(GCTX.root_service_));
  } else {
    if (!ObHeartbeatService::is_service_enabled()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
        const ObAddr &server = rs_list_.at(i).server_;
        const ObZone &zone = rs_list_.at(i).zone_;
        if (OB_FAIL(GCTX.root_service_->add_server_for_bootstrap_in_version_smaller_than_4_2_0(server, zone))) {
          LOG_WARN("fail to add server in version < 4.2", KR(ret), K(server), K(zone));
        }
        FLOG_INFO("add servers in rs_list_ in version < 4.2", KR(ret), K(server), K(zone));
      }
    } else {
      // Attention: DO keep the order of this rs_list_ the same with that of ObPrepareBootstrap,
      //   otherwise server_ids allocated here will mismatch those notified during prepare-bootstrap stage.
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
        servers.reuse();
        const ObAddr &server = rs_list_.at(i).server_;
        const ObZone &zone = rs_list_.at(i).zone_;
        if (OB_FAIL(servers.push_back(server))) {
          LOG_WARN("fail to push an element into servers", KR(ret), K(server));
        } else if (OB_FAIL(server_zone_op_service.add_servers(servers, zone, true /* is_bootstrap */))) {
          LOG_WARN("fail to add servers", KR(ret), K(servers), K(zone));
        }
        FLOG_INFO("add servers in rs_list_ in version >= 4.2", KR(ret), K(servers), K(zone));
      }
      if (FAILEDx(GCTX.root_service_->load_server_manager())) {
        LOG_WARN("fail to load server manager", KR(ret), KP(GCTX.root_service_));
      }
    }
  }
  return ret;
}

int ObBootstrap::check_is_already_bootstrap(bool &is_bootstrap)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  is_bootstrap = true;
  ObMultiVersionSchemaService &schema_service = ddl_service_.get_schema_service();
  ObSchemaGetterGuard guard;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("get_schema_manager failed", K(ret));
  } else if (OB_FAIL(guard.get_schema_version(OB_SYS_TENANT_ID, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret));
  } else if (OB_CORE_SCHEMA_VERSION == schema_version) {
    is_bootstrap = false;
  } else {
    // don't need to set ret
    //LOG_WARN("observer is bootstrap already", "schema_table_count", guard->get_table_count());
    LOG_WARN("observer is already bootstrap");
    //const bool is_verbose = false;
    //guard->print_info(is_verbose);
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_global_stat()
{
  int ret = OB_SUCCESS;
  ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else {
    const int64_t baseline_schema_version = OB_INVALID_VERSION; // OB_INVALID_VERSION == -1
    const int64_t rootservice_epoch = 0;
    const SCN snapshot_gc_scn = SCN::min_scn();
    const int64_t snapshot_gc_timestamp = 0;
    const int64_t ddl_epoch = 0;
    ObGlobalStatProxy global_stat_proxy(trans, OB_SYS_TENANT_ID);
    ObSchemaStatusProxy *schema_status_proxy = GCTX.schema_status_proxy_;
    if (OB_FAIL(trans.start(&sql_proxy, OB_SYS_TENANT_ID))) {
      LOG_WARN("trans start failed", KR(ret));
    } else if (OB_ISNULL(schema_status_proxy)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_status_proxy is null", KR(ret));
    } else if (OB_FAIL(global_stat_proxy.set_init_value(
               OB_CORE_SCHEMA_VERSION, OB_CORE_SCHEMA_VERSION, baseline_schema_version,
               rootservice_epoch, snapshot_gc_scn, snapshot_gc_timestamp, ddl_epoch,
               DATA_CURRENT_VERSION, DATA_CURRENT_VERSION, DATA_CURRENT_VERSION))) {
      LOG_WARN("set_init_value failed", KR(ret), "schema_version", OB_CORE_SCHEMA_VERSION,
               K(baseline_schema_version), K(rootservice_epoch), K(ddl_epoch), "data_version", DATA_CURRENT_VERSION);
    }

    int temp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (temp_ret = trans.end(OB_SUCCESS == ret))) {
      LOG_WARN("trans end failed", "commit", OB_SUCCESS == ret, KR(temp_ret));
      ret = (OB_SUCCESS == ret) ? temp_ret : ret;
    }

    // Initializes a new state of refresh schema
    if (OB_SUCC(ret)) {
      ObRefreshSchemaStatus tenant_status(OB_SYS_TENANT_ID, OB_INVALID_TIMESTAMP,
          OB_INVALID_VERSION);
      if (OB_FAIL(schema_status_proxy->set_tenant_schema_status(tenant_status))) {
        LOG_WARN("fail to init create partition status", KR(ret), K(tenant_status));
      } else {}
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::gen_multiple_zone_deployment_sys_tenant_locality_str(
    share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone list count unexpected", K(ret));
  } else {
    const int64_t BUFF_SIZE = 32 * MAX_ZONE_LENGTH; // 4096
    char locality_str[BUFF_SIZE] = "";
    bool first = true;
    int64_t pos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (OB_FAIL(databuff_printf(
              locality_str, BUFF_SIZE, pos, "%sF{1}@%s", first ? "": ", ", rs_list_.at(i).zone_.ptr()))) {
        LOG_WARN("fail to do databuff printf", K(ret));
      } else {
        first = false;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(tenant_schema.set_locality(locality_str))) {
        LOG_WARN("fail to set locality", K(ret));
      }
    }
  }
  return ret;
}

int ObBootstrap::gen_sys_tenant_locality_str(
    share::schema::ObTenantSchema &tenant_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    if (OB_FAIL(gen_multiple_zone_deployment_sys_tenant_locality_str(tenant_schema))) {
      LOG_WARN("fail to gen multiple zone deployment sys tenant locality str", K(ret));
    }
  }
  return ret;
}

int ObBootstrap::create_sys_tenant()
{
  // insert zero system stat value for create system tenant.
  int ret= OB_SUCCESS;
  ObTenantSchema tenant;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else {
    obrpc::ObCreateTenantArg arg;
    arg.name_case_mode_ = OB_ORIGIN_AND_INSENSITIVE;
    tenant.set_tenant_id(OB_SYS_TENANT_ID);
    tenant.set_schema_version(OB_CORE_SCHEMA_VERSION);

    share::schema::ObSchemaGetterGuard dummy_schema_guard;
    ObArray<common::ObZone> zone_list;
    ObArray<share::schema::ObZoneRegion> zone_region_list;
    ObArenaAllocator allocator; // allocator for locality str
    int64_t pos = 0;
    ObLocalityDistribution locality_dist;
    common::ObArray<share::ObZoneReplicaAttrSet> zone_replica_num_array;
    char *locality_str = nullptr;
    if (OB_FAIL(gen_sys_tenant_locality_str(tenant))) {
      LOG_WARN("fail to gen sys tenant locality str", K(ret));
    } else if (OB_FAIL(gen_sys_zone_list(zone_list))) {
      LOG_WARN("fail to gen sys zone list", K(ret));
    } else if (OB_FAIL(build_zone_region_list(zone_region_list))) {
      LOG_WARN("fail to build zone region list", K(ret));
    } else if (OB_FAIL(locality_dist.init())) {
      LOG_WARN("fail to init locality distribution", K(ret));
    } else if (OB_FAIL(locality_dist.parse_locality(
            tenant.get_locality_str(), zone_list, &zone_region_list))) {
      LOG_WARN("fail to parse tenant schema locality", K(ret));
    } else if (OB_ISNULL(locality_str = (char *)allocator.alloc(MAX_LOCALITY_LENGTH + 1))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret));
    } else if (OB_FAIL(locality_dist.output_normalized_locality(
            locality_str, MAX_LOCALITY_LENGTH, pos))) {
      LOG_WARN("fail to output normalized locality", K(ret));
    } else if (OB_FAIL(tenant.set_locality(locality_str))) {
      LOG_WARN("fail to set locality", K(ret));
    } else if (OB_FAIL(locality_dist.get_zone_replica_attr_array(zone_replica_num_array))) {
      LOG_WARN("fail to get zone region replica num array", K(ret));
    } else if (OB_FAIL(tenant.set_zone_replica_attr_array(zone_replica_num_array))) {
      LOG_WARN("fail to set zone replica_num array", K(ret));
    } else if (OB_FAIL(tenant.set_tenant_name(OB_SYS_TENANT_NAME))) {
      LOG_WARN("set_tenant_name failed", "tenant_name", OB_SYS_TENANT_NAME, K(ret));
    } else if (OB_FAIL(tenant.set_comment("system tenant"))) {
      LOG_WARN("set_comment failed", "comment", "system tenant", K(ret));
    } else if (OB_FAIL(set_replica_options(tenant))) {
      LOG_WARN("failed to set replica options", KR(ret));
    } else if (OB_FAIL(tenant_ddl_service_.check_primary_zone_locality_condition(
            tenant, zone_list, zone_region_list, dummy_schema_guard))) {
      LOG_WARN("fail to check primary zone region condition", K(ret));
    } else if (OB_FAIL(tenant_ddl_service_.create_sys_tenant(arg, tenant))) {
      LOG_WARN("create tenant failed", K(ret), K(tenant));
    } else if (OB_FAIL(insert_sys_ls_(tenant, zone_list))) {
      LOG_WARN("failed to insert sys ls", KR(ret), K(zone_list));
    } else {} // no more to do
  }

  LOG_INFO("create tenant", K(ret), K(tenant));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::insert_sys_ls_(const share::schema::ObTenantSchema &tenant_schema,
    const ObIArray<ObZone> &zone_list)
{
  int ret = OB_SUCCESS;
  ObZone primary_zone;
  ObSqlString primary_zone_str;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_UNLIKELY(rs_list_.count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("rs_list_ count unexpected", K(ret), "rs list count", rs_list_.count());
    LOG_USER_ERROR(OB_ERR_UNEXPECTED, "rootserver list is empty");
  } else {
    bool found = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      if (rs_list_.at(i).server_ == GCTX.self_addr()) {
        if (found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("self addr is duplicate in rs list", KR(ret), K(rs_list_));
        } else {
          found = true;
          primary_zone = rs_list_.at(i).zone_;
        }
      }
      if (FAILEDx(primary_zone_str.append_fmt("%s", rs_list_.at(i).zone_.ptr()))) {
        LOG_WARN("failed to append fmt", KR(ret), K(i), K(rs_list_));
      } else if (rs_list_.count() - 1 != i) {
        if (OB_FAIL(primary_zone_str.append(","))) {
          LOG_WARN("failed to append fmt", KR(ret), K(i), K(rs_list_));
        }
      }
    }
    if (!found && OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can find self in rs list", KR(ret), K(rs_list_), "self", GCTX.self_addr());
    }
  }

  if (OB_SUCC(ret)) {
    ObLSLifeAgentManager life_agent(ddl_service_.get_sql_proxy());
    const uint64_t unit_group_id = 0;
    const uint64_t ls_group_id = 0;
    share::ObLSStatusInfo sys_status_info;
    share::ObLSFlag sys_flag(share::ObLSFlag::NORMAL_FLAG);
    ObUnitIDList unit_list;
    if (OB_FAIL(sys_status_info.init(OB_SYS_TENANT_ID, SYS_LS, ls_group_id,
            share::OB_LS_NORMAL, unit_group_id, primary_zone, sys_flag, unit_list))) {
      LOG_WARN("failed to init ls info", KR(ret), K(primary_zone), K(sys_flag));
    } else if (OB_FAIL(life_agent.create_new_ls(sys_status_info, SCN::base_scn(), primary_zone_str.string(),
                                                ObAllTenantInfo::INITIAL_SWITCHOVER_EPOCH))) {
      LOG_WARN("failed to create new sys ls", KR(ret), K(sys_status_info), K(primary_zone_str));
    }
  }
  return ret;
}

int ObBootstrap::init_system_data()
{
  int ret = OB_SUCCESS;
  LOG_DBA_INFO_V2(OB_BOOTSTRAP_CREATE_SYS_TENANT_BEGIN,
                  DBA_STEP_INC_INFO(bootstrap),
                  "bootstrap create sys tenant begin.");
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(unit_mgr_.load())) {
    LOG_WARN("unit_mgr load failed", KR(ret));
  } else if (OB_FAIL(create_sys_unit_config())) {
    LOG_WARN("create_sys_unit_config failed", KR(ret));
  } else if (OB_FAIL(create_sys_resource_pool())) {
    LOG_WARN("create sys resource pool failed", KR(ret));
  } else if (OB_FAIL(create_sys_tenant())) {
    LOG_WARN("create system tenant failed", KR(ret));
  } else if (OB_FAIL(init_all_zone_table())) {
    LOG_WARN("failed to init all zone table", KR(ret));
  } else if (GCTX.is_shared_storage_mode() && OB_FAIL(init_palf_kv_system_data_())) {
    LOG_WARN("failed to init palf kv system data", KR(ret));
  }
  if (OB_FAIL(ret)) {
    LOG_DBA_ERROR_V2(OB_BOOTSTRAP_CREATE_SYS_TENANT_FAIL, ret,
                     DBA_STEP_INC_INFO(bootstrap),
                     "bootstrap create sys tenant fail. maybe some resources are not enough. "
                     "you may find solutions in previous error logs or seek help from official technicians.");
  } else {
    LOG_DBA_INFO_V2(OB_BOOTSTRAP_CREATE_SYS_TENANT_SUCCESS,
                    DBA_STEP_INC_INFO(bootstrap),
                    "bootstrap create sys tenant success.");
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_sys_tenant_shared_meta_user_tables_()
{
  int ret = OB_SUCCESS;
  const int64_t table_cnt = share::get_user_table_builtin_schema_count();
  LOG_INFO("create sys tenant shared meta user tables begin", K(table_cnt));
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", KR(ret));
  } else {
    ObTimeoutCtx ctx;
    ObUserTableSchemaApplier applier;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(ctx, GCONF._ob_ddl_timeout))) {
      LOG_WARN("failed to set default timeout ctx", KR(ret));
    } else if (OB_FAIL(applier.init(ddl_service_.get_schema_service(), *GCTX.sql_proxy_, ctx))) {
      LOG_WARN("failed to init user table schema applier", KR(ret));
    } else if (OB_FAIL(applier.apply_builtin_user_table_schemas(OB_SYS_TENANT_ID, true/*force_local*/))) {
      LOG_WARN("failed to apply sys tenant shared meta user tables", KR(ret));
    }
  }
  LOG_INFO("create sys tenant shared meta user tables finish", KR(ret), K(table_cnt));
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObBootstrap::build_sys_create_ls_arg_(const share::ObLSID &ls_id,
                                          obrpc::ObCreateLSArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();
  share::ObAllTenantInfo tenant_info;
  palf::PalfBaseInfo palf_base_info;
  storage::ObMajorMVMergeInfo major_mv_merge_info;
  const common::ObReplicaProperty replica_property;
  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls_id", KR(ret), K(ls_id));
  } else if (OB_FAIL(tenant_info.init(OB_SYS_TENANT_ID, share::PRIMARY_TENANT_ROLE))) {
    LOG_WARN("tenant_info init failed", KR(ret));
  } else if (OB_FAIL(arg.init(OB_SYS_TENANT_ID,
                              ls_id,
                              REPLICA_TYPE_FULL,
                              replica_property,
                              tenant_info,
                              share::SCN::base_scn(),
                              lib::Worker::CompatMode::MYSQL,
                              false /*create_with_palf*/,
                              palf_base_info,
                              major_mv_merge_info,
                              DATA_CURRENT_VERSION))) {
    LOG_WARN("arg init failed", KR(ret), K(ls_id));
  }
  return ret;
}

int ObBootstrap::init_sys_tenant_ss_meta_()
{
  int ret = OB_SUCCESS;
  struct SSTabletTriple
  {
    share::ObLSID ls_id_;
    common::ObTabletID tablet_id_;
    share::SCN reorgan_scn_;
    SSTabletTriple() : ls_id_(), tablet_id_(), reorgan_scn_() {}
    SSTabletTriple(const share::ObLSID &ls_id,
                   const common::ObTabletID &tablet_id,
                   const share::SCN &reorgan_scn)
      : ls_id_(ls_id), tablet_id_(tablet_id), reorgan_scn_(reorgan_scn) {}
    TO_STRING_KV(K_(ls_id), K_(tablet_id), K_(reorgan_scn));
  };
  if (!GCTX.is_shared_storage_mode()) {
    // shared-nothing mode: skip
  } else {
    common::ObSEArray<share::ObLSID, 4> ls_ids;
    common::ObSEArray<SSTabletTriple, 64> tablet_triples;

    // ---- Phase 1: snapshot local sys-tenant LS / non-inner tablet ----
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      storage::ObLSService *ls_svr = MTL(storage::ObLSService *);
      common::ObSharedGuard<storage::ObLSIterator> ls_iter_guard;
      storage::ObLS *ls = nullptr;
      if (OB_ISNULL(ls_svr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls service is null", KR(ret));
      } else if (OB_FAIL(ls_svr->get_ls_iter(ls_iter_guard, storage::ObLSGetMod::RS_MOD))) {
        LOG_WARN("get_ls_iter failed", KR(ret));
      } else {
        while (OB_SUCC(ret)) {
          if (OB_FAIL(ls_iter_guard->get_next(ls))) {
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
              break;
            }
            LOG_WARN("ls iter get_next failed", KR(ret));
          } else if (OB_ISNULL(ls)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("ls is null", KR(ret));
          } else {
            const share::ObLSID ls_id = ls->get_ls_id();
            storage::ObLSTabletIterator tablet_iter(
                storage::ObMDSGetTabletMode::READ_WITHOUT_CHECK);
            if (OB_FAIL(ls_ids.push_back(ls_id))) {
              LOG_WARN("push back ls_id failed", KR(ret), K(ls_id));
            } else if (OB_ISNULL(ls->get_tablet_svr())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("tablet svr is null", KR(ret), K(ls_id));
            } else if (OB_FAIL(ls->get_tablet_svr()->build_tablet_iter(
                           tablet_iter, true /*except_ls_inner_tablet*/))) {
              LOG_WARN("build_tablet_iter failed", KR(ret), K(ls_id));
            } else {
              storage::ObTabletHandle tablet_hdl;
              while (OB_SUCC(ret)) {
                tablet_hdl.reset();
                if (OB_FAIL(tablet_iter.get_next_tablet(tablet_hdl))) {
                  if (OB_ITER_END == ret) {
                    ret = OB_SUCCESS;
                    break;
                  }
                  LOG_WARN("get_next_tablet failed", KR(ret), K(ls_id));
                } else {
                  storage::ObTablet *tablet = tablet_hdl.get_obj();
                  if (OB_ISNULL(tablet)) {
                    ret = OB_ERR_UNEXPECTED;
                    LOG_WARN("tablet is null", KR(ret), K(ls_id));
                  } else {
                    const common::ObTabletID tablet_id =
                        tablet->get_tablet_meta().tablet_id_;
                    const share::SCN reorgan_scn = tablet->get_reorganization_scn();
                    if (OB_FAIL(tablet_triples.push_back(
                            SSTabletTriple(ls_id, tablet_id, reorgan_scn)))) {
                      LOG_WARN("push back triple failed", KR(ret),
                               K(ls_id), K(tablet_id), K(reorgan_scn));
                    }
                  }
                }
              }
            }
          }
        }
      }
    } else {
      LOG_WARN("switch to sys tenant failed", KR(ret));
    }

    // ---- Phase 2: write LS / tablet meta into __all_ss_meta via V2 ----
    if (OB_SUCC(ret)) {
      MTL_SWITCH(OB_SYS_TENANT_ID) {
        storage::ObSSMetaService *meta_svr = MTL(storage::ObSSMetaService *);
        if (OB_ISNULL(meta_svr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ss meta service is null", KR(ret));
        } else {
          // 2.a write LS rows (and their inner tablet rows together)
          for (int64_t i = 0; OB_SUCC(ret) && i < ls_ids.count(); ++i) {
            const share::ObLSID &ls_id = ls_ids.at(i);
            obrpc::ObCreateLSArg arg;
            if (OB_FAIL(build_sys_create_ls_arg_(ls_id, arg))) {
              LOG_WARN("build_sys_create_ls_arg_ failed", KR(ret), K(ls_id));
            } else if (OB_FAIL(meta_svr->create_ls(arg, ObSSLSCreateMode::BOOTSTRAP_CREATE))) {
              LOG_WARN("ss meta service create_ls failed", KR(ret), K(ls_id));
            } else {
              LOG_INFO("ss meta service create_ls done", K(ls_id));
            }
          }
          // 2.b write non-inner tablet rows
          for (int64_t i = 0; OB_SUCC(ret) && i < tablet_triples.count(); ++i) {
            const SSTabletTriple &triple = tablet_triples.at(i);
            if (OB_FAIL(meta_svr->create_tablet(triple.ls_id_, triple.tablet_id_,
                                                triple.reorgan_scn_, false/*check_lease*/))) {
              LOG_WARN("ss meta service create_tablet failed", KR(ret), K(triple));
            } else {
              LOG_INFO("ss meta service create_tablet done", K(triple));
            }
          }
        }
      } else {
        LOG_WARN("switch to sys tenant failed", KR(ret));
      }
    }

    LOG_INFO("init_sys_tenant_ss_meta_ finish", KR(ret), "ls_cnt", ls_ids.count(),
             "tablet_cnt", tablet_triples.count());
  }
  return ret;
}

int ObBootstrap::do_sys_tenant_ls_checkpoint_()
{
  int ret = OB_SUCCESS;
  if (!GCTX.is_shared_storage_mode()) {
    // shared-nothing mode: skip
  } else if (!GCONF.enable_logservice) {
    // not support logservice, skip
    LOG_INFO("skip write ls ckpt when logservice disabled");
  } else {
    MTL_SWITCH(OB_SYS_TENANT_ID) {
      storage::ObSSWriterService *sswriter_svc = MTL(storage::ObSSWriterService*);
      if (OB_ISNULL(sswriter_svc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sswriter service is null", KR(ret));
      } else if (OB_FAIL(sswriter_svc->do_ls_checkpoint(storage::ObSSLsCkptMode::BOOTSTRAP))) {
        LOG_WARN("do ls checkpoint failed during bootstrap", KR(ret));
      }
    }
    LOG_INFO("do_sys_tenant_ls_checkpoint_ finish", KR(ret));
  }
  return ret;
}
#endif

int ObBootstrap::init_palf_kv_system_data_()
{
  int ret = OB_SUCCESS;
#ifdef OB_BUILD_SHARED_STORAGE
   if (!GCTX.is_shared_storage_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported", KR(ret));
   } else {
    common::ObSEArray<uint64_t, 128> init_server_ids;
    if (OB_FAIL(ObServerZoneOpService::store_data_version_in_palf_kv(OB_SYS_TENANT_ID, DATA_CURRENT_VERSION))) {
      LOG_WARN("fail to store data version in palf kv", KR(ret), K(DATA_CURRENT_VERSION));
    } else if (OB_FAIL(ObServerZoneOpService::store_server_ids_in_palf_kv(init_server_ids/*empty*/, OB_INIT_SERVER_ID - 1))) { // OB_INIT_SERVER_ID = 1
      LOG_WARN("fail to store server_ids in palf kv", KR(ret));
    } else if (OB_FAIL(ObServerZoneOpService::store_max_unit_id_in_palf_kv(OB_USER_UNIT_ID))) { // OB_USER_UNIT_ID = 1000
      LOG_WARN("fail to store unit_id in palf kv", KR(ret));
#ifdef OB_BUILD_TDE_SECURITY
    } else if (OB_FAIL(ObMasterKeyUtil::store_sys_root_key_in_palf_kv())) {
      LOG_WARN("fail to store sys root key in palf kv", KR(ret));
#endif
    } else {
      common::ObSEArray<ObZone, DEFAULT_ZONE_COUNT> zone_names;
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
        if (OB_FAIL(zone_names.push_back(rs_list_[i].zone_))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
      if (FAILEDx(ObServerZoneOpService::store_all_zone_in_palf_kv(zone_names))) {
        LOG_WARN("fail to store all zone in palf kv", KR(ret), K(zone_names));
      }
    }
  }
#endif
  return ret;
}

int ObBootstrap::init_sys_unit_config(share::ObUnitConfig &unit_config)
{
  int ret = OB_SUCCESS;
  const bool is_hidden_sys = false;

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(unit_config.gen_sys_tenant_unit_config(is_hidden_sys))) {
    LOG_WARN("gen sys tenant unit config fail", KR(ret), K(is_hidden_sys));
  } else {
    LOG_INFO("init sys tenant unit config succ", K(unit_config));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::create_sys_unit_config()
{
  int ret = OB_SUCCESS;
  ObUnitConfig unit_config;
  const bool if_not_exist = true;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(init_sys_unit_config(unit_config))) {
    LOG_WARN("init default sys unit config failed", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_unit_config(unit_config, if_not_exist))) {
    LOG_WARN("create_unit_config failed", K(unit_config), K(if_not_exist), K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::gen_sys_resource_pool(
    share::ObResourcePool &pool)
{
  int ret = OB_SUCCESS;
  pool.resource_pool_id_ = OB_SYS_RESOURCE_POOL_ID;
  pool.name_ = share::ObResourcePool::SYS_RESOURCE_POOL_NAME;
  pool.unit_count_ = 1;
  pool.unit_config_id_ = ObUnitConfig::SYS_UNIT_CONFIG_ID;
  pool.tenant_id_ = OB_SYS_TENANT_ID;
  if (OB_FAIL(gen_sys_zone_list(pool.zone_list_))) {
    LOG_WARN("fail to gen sys zone list", K(ret));
  }
  return ret;
}

int ObBootstrap::create_sys_resource_pool()
{
  int ret = OB_SUCCESS;
  ObArray<ObUnit> sys_units;
  share::ObResourcePool pool;
  const bool if_not_exist = false;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (OB_FAIL(gen_sys_resource_pool(pool))) {
    LOG_WARN("gen sys resource pool", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_resource_pool(
      pool, ObUnitConfig::SYS_UNIT_CONFIG_NAME, if_not_exist))) {
    LOG_WARN("create sys resource pool failed", K(pool),
             "unit_config", ObUnitConfig::SYS_UNIT_CONFIG_NAME, K(ret));
  } else if (OB_FAIL(gen_sys_units(sys_units))) {
    LOG_WARN("gen_sys_units failed", K(ret));
  } else if (OB_FAIL(unit_mgr_.create_sys_units(sys_units))) {
    LOG_WARN("create_sys_units failed", K(sys_units), K(ret));
  } else if (OB_FAIL(unit_mgr_.load())) {
    LOG_WARN("unit_mgr reload failed", K(ret));
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::init_multiple_zone_deployment_table(
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  HEAP_VAR(ObZoneInfo, zone_info) {
    for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); ++i) {
      zone_info.reset();
      zone_info.zone_ = rs_list_[i].zone_;
      // for compatibility with ob1.2, which has no region specified
      if (rs_list_[i].region_.is_empty()) {
        if (OB_FAIL(zone_info.region_.info_.assign(DEFAULT_REGION_NAME))) {
          LOG_WARN("fail assign default region info", K(ret), K(zone_info));
        }
      } else {
        if (OB_FAIL(zone_info.region_.info_.assign(
                ObString(rs_list_[i].region_.size(), rs_list_[i].region_.ptr())))) {
          LOG_WARN("fail assign region info", K(ret), K(zone_info));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(zone_info.status_.info_.assign(
                ObString(ObZoneStatus::get_status_str(ObZoneStatus::ACTIVE))))) {
          LOG_WARN("fail to assign zone status str", KR(ret));
        } else {
          zone_info.status_.value_ = ObZoneStatus::ACTIVE;
        }
      }

      if (OB_SUCC(ret)) {
        ObZoneInfo::StorageType storage_type = GCTX.is_shared_storage_mode() ?
                                               ObZoneInfo::STORAGE_TYPE_SHARED_STORAGE :
                                               ObZoneInfo::STORAGE_TYPE_LOCAL;
        if (OB_FAIL(zone_info.storage_type_.info_.assign(
                ObString(ObZoneInfo::get_storage_type_str(storage_type))))) {
          LOG_WARN("fail to assign zone storage_type str", KR(ret));
        } else {
          zone_info.storage_type_.value_ = storage_type;
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObZoneTableOperation::insert_zone_info(sql_client, zone_info))) {
          LOG_WARN("insert zone info failed", K(ret), K(zone_info));
        }
      }
    }
  }
  return ret;
}

int ObBootstrap::init_all_zone_table()
{
  int ret = OB_SUCCESS;
  common::ObMySQLTransaction trans;
  ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", KR(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy, OB_SYS_TENANT_ID))) {
    LOG_WARN("failed to start trans", KR(ret));
  } else {
    HEAP_VAR(ObGlobalInfo, global_info) {
      if (OB_FAIL(ObZoneTableOperation::insert_global_info(trans, global_info))) {
        LOG_WARN("insert global info failed", K(ret));
      } else{
        if (OB_FAIL(init_multiple_zone_deployment_table(trans))) {
          LOG_WARN("fail to init multiple zone deployment table", K(ret));
        }
      }

      int tmp_ret = trans.end(OB_SUCC(ret));
      if (OB_SUCCESS != tmp_ret) {
        LOG_WARN("end transaction failed", KR(tmp_ret), KR(ret));
        ret = OB_SUCCESS == ret ? tmp_ret : ret;
      }
    }
  }

  LOG_INFO("init all zone table", KR(ret));
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObBootstrap::write_shared_storage_args()
{
  int ret = OB_SUCCESS;
  ObAdminStorageArg shared_storage_args;
  if (GCTX.is_shared_storage_mode()) {
    const ObString &shared_storage_info = arg_.shared_storage_info_;
    if (shared_storage_info.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shared_storage_info is empty", KR(ret), K(arg_));
    } else if (OB_FAIL(ObStorageDestCheck::parse_shared_storage_info(shared_storage_info,
            shared_storage_args))) {
      LOG_WARN("failed to parse shared_storage_info", KR(ret), K(shared_storage_args));
    } else if (OB_UNLIKELY(!shared_storage_args.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("shared_storage_args is invalid", KR(ret), K(shared_storage_args));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rs_list_.count(); i++) {
        const ObZone &zone = rs_list_.at(i).zone_;
        const ObRegion &region = rs_list_.at(i).region_;
        if (OB_FAIL(write_shared_storage_args_for_zone(zone, region, shared_storage_args))) {
          LOG_WARN("failed to write shared storage args for zone", KR(ret), K(zone), K(shared_storage_args));
        } else {}
      }
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::write_shared_storage_args_for_zone(const ObZone &zone, const ObRegion &region,
    const obrpc::ObAdminStorageArg &storage_args)
{
  int ret = OB_SUCCESS;
  obrpc::ObAdminStorageArg full_args;
  ObMySQLProxy &sql_proxy = ddl_service_.get_sql_proxy();
  if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root_service_ is NULL", KR(ret), KP(GCTX.root_service_));
  } else if (OB_UNLIKELY(zone.is_empty() || region.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty zone or region", KR(ret), K(zone), K(region));
  } else if (OB_UNLIKELY(!storage_args.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage_args is invalid", KR(ret), K(storage_args));
  } else if (OB_FAIL(full_args.assign(storage_args))) {
    LOG_WARN("failed to assign full_args", KR(ret), K(zone), K(region), K(storage_args));
  } else {
    full_args.zone_ = zone;
    full_args.region_ = region;
    if (OB_FAIL(GCTX.root_service_->add_storage(full_args))) {
      LOG_WARN("failed to add storage", KR(ret), K(full_args));
    } else {
      FLOG_INFO("add storage succeed", KR(ret), K(zone), K(storage_args));
    }
  }
  return ret;
}
#endif

template<typename SCHEMA>
int ObBootstrap::set_replica_options(SCHEMA &schema)
{
  int ret = OB_SUCCESS;
  BOOTSTRAP_CHECK_SUCCESS_V2("before set replica options");
  ObArray<ObZone> zone_list;
  ObArray<ObString> zone_str_list;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check_inner_stat failed", K(ret));
  } else if (!schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(schema), K(ret));
  } else if (OB_FAIL(gen_sys_zone_list(zone_list))) {
    LOG_WARN("gen_zone_list failed", K(ret));
  } else if (zone_list.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("zone_list is empty", K(zone_list), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < zone_list.count(); ++i) {
      if (OB_FAIL(zone_str_list.push_back(ObString::make_string(zone_list.at(i).ptr())))) {
        LOG_WARN("push_back failed", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (zone_str_list.count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("zone_str_list is empty", K(zone_str_list), K(ret));
    } else if (OB_FAIL(schema.set_zone_list(zone_str_list))) {
      LOG_WARN("set_zone_list failed", K(zone_str_list), K(ret));
    } else if (OB_FAIL(schema.set_primary_zone(OB_RANDOM_PRIMARY_ZONE))) {
      LOG_WARN("failed to set primary zone", KR(ret), K(schema));
    }
  }
  BOOTSTRAP_CHECK_SUCCESS();
  return ret;
}

int ObBootstrap::build_zone_region_list(
    ObIArray<share::schema::ObZoneRegion> &zone_region_list)
{
  int ret = OB_SUCCESS;
  zone_region_list.reset();
  for (int64_t i = 0; i < rs_list_.count() && OB_SUCC(ret); ++i) {
    const common::ObZone &zone = rs_list_.at(i).zone_;
    const common::ObRegion &region = rs_list_.at(i).region_;
    if (OB_FAIL(zone_region_list.push_back(
            ObZoneRegion(zone, region, ObZoneRegion::CZY_NO_NEED_TO_CHECK)))) {
      LOG_WARN("fail to push back", K(ret), K(zone), K(region));
    } else {} // no more to do
  }
  return ret;
}

int ObBootstrap::set_in_bootstrap()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("failed to check inner stat error", K(ret));
  } else {
    ObMultiVersionSchemaService &multi_schema_service = ddl_service_.get_schema_service();
    ObSchemaService *schema_service = multi_schema_service.get_schema_service();
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema_service is null", K(ret));
    } else {
      schema_service->set_cluster_schema_status(
          ObClusterSchemaStatus::BOOTSTRAP_STATUS);
    }
  }
  return ret;
}


} // end namespace rootserver
} // end namespace oceanbase
