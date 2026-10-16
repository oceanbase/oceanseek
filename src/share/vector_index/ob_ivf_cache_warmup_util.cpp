/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/vector_index/ob_ivf_cache_warmup_util.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_multi_tenant.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/datum/ob_datum_funcs.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_vector_index_ivf_cache_mgr.h"
#include "sql/engine/expr/ob_expr_func_part_hash.h"
#include "sql/das/ob_das_define.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;

namespace share
{

int ObIvfCacheWarmupUtil::resolve_centroid_for_collection(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema &logic_data_table_schema,
    uint64_t &centroid_table_id,
    const ObTableSchema *&centroid_table_schema,
    ObVectorIndexParam &vec_param,
    bool &should_skip)
{
  int ret = OB_SUCCESS;
  centroid_table_id = OB_INVALID_ID;
  centroid_table_schema = nullptr;
  should_skip = false;

  // Find flat IVF centroid index
  const ObIArray<ObAuxTableMetaInfo> &index_infos =
      logic_data_table_schema.get_simple_index_infos();
  for (int64_t i = 0; i < index_infos.count(); ++i) {
    if (is_local_vec_ivf_centroid_index(index_infos.at(i).index_type_)) {
      centroid_table_id = index_infos.at(i).table_id_;
      break;
    }
  }
  if (OB_INVALID_ID == centroid_table_id) {
    should_skip = true;
  }

  // Get centroid table schema and vec_param
  if (OB_SUCC(ret) && !should_skip) {
    bool param_filled = false;
    if (OB_FAIL(schema_guard.get_table_schema(
            tenant_id, centroid_table_id, centroid_table_schema))) {
      LOG_WARN("fail to get centroid table schema", KR(ret), K(centroid_table_id));
    } else if (OB_ISNULL(centroid_table_schema)) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("centroid table not found", KR(ret), K(centroid_table_id));
    } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_param(
                   schema_guard, *centroid_table_schema, vec_param, param_filled))) {
      LOG_WARN("fail to get vector index param", KR(ret), K(centroid_table_id));
    } else if (!param_filled) {
      should_skip = true;
    } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(
                   *centroid_table_schema, logic_data_table_schema, vec_param.dim_))) {
      LOG_WARN("fail to get vector index column dim", KR(ret), K(centroid_table_id));
    }
  }
  return ret;
}

int ObIvfCacheWarmupUtil::calc_centroid_tablet_for_namespace(
    const ObTableSchema &centroid_schema,
    const uint64_t namespace_id,
    ObTabletID &centroid_tablet_id)
{
  int ret = OB_SUCCESS;
  if (PARTITION_LEVEL_ZERO == centroid_schema.get_part_level()) {
    ObObjectID part_id = OB_INVALID_ID;
    if (OB_FAIL(centroid_schema.get_tablet_and_object_id(centroid_tablet_id, part_id))) {
      LOG_WARN("get_tablet_and_object_id failed for LEVEL_ZERO table",
               KR(ret), "table_id", centroid_schema.get_table_id());
    }
  } else if (centroid_schema.is_hash_like_part()) {
    // Compute partition function value
    ObObj part_key_obj;
    ObObj part_func_obj;
    part_key_obj.set_uint64(namespace_id);
    if (centroid_schema.is_hash_part()) {
      if (OB_FAIL(sql::ObExprFuncPartHash::calc_value_for_mysql(
              part_key_obj, part_func_obj, part_key_obj.get_type()))) {
        LOG_WARN("calc hash part func value failed", KR(ret), K(namespace_id));
      }
    } else if (centroid_schema.is_key_part()) {
      ObDatum part_key_datum;
      char part_key_datum_buf[sizeof(uint64_t)];
      sql::ObExprBasicFuncs *basic_funcs =
          ObDatumFuncs::get_basic_func(part_key_obj.get_type(), part_key_obj.get_collation_type());
      uint64_t hash_value = 0;
      if (OB_ISNULL(basic_funcs) || OB_ISNULL(basic_funcs->murmur_hash_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null basic funcs for key partition", KR(ret), K(namespace_id),
                 K(part_key_obj));
      } else {
        part_key_datum.ptr_ = part_key_datum_buf;
        if (OB_FAIL(part_key_datum.from_obj(part_key_obj))) {
          LOG_WARN("fail to convert namespace id to datum", KR(ret), K(namespace_id));
        } else if (OB_FAIL(basic_funcs->murmur_hash_(part_key_datum, 0, hash_value))) {
          LOG_WARN("fail to calc key partition hash", KR(ret), K(namespace_id));
        } else {
          int64_t result_num = static_cast<int64_t>(hash_value);
          result_num = result_num < 0 ? -result_num : result_num;
          part_func_obj.set_int(result_num);
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("unsupported partition type", KR(ret));
    }
    if (OB_SUCC(ret)) {
      sql::ObDASTabletMapper tablet_mapper;
      tablet_mapper.set_table_schema(&centroid_schema);
      ObSEArray<ObTabletID, 1> tablet_ids;
      ObSEArray<ObObjectID, 1> part_ids;
      if (OB_FAIL(tablet_mapper.get_tablet_and_object_id(
              centroid_schema.get_part_level(), OB_INVALID_ID, part_func_obj,
              tablet_ids, part_ids))) {
        LOG_WARN("fail to calc tablet by hash-like part func obj", KR(ret),
                 K(namespace_id), K(part_func_obj));
      } else if (tablet_ids.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty tablet ids from hash-like routing", KR(ret), K(namespace_id));
      } else {
        centroid_tablet_id = tablet_ids.at(0);
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported partition scheme for centroid tablet calc", KR(ret),
             "table_id", centroid_schema.get_table_id());
  }
  return ret;
}

int ObIvfCacheWarmupUtil::resolve_vec_param_from_centroid_table_id(
    const uint64_t tenant_id,
    ObSchemaGetterGuard &schema_guard,
    const uint64_t centroid_table_id,
    ObVectorIndexParam &vec_param,
    bool &should_skip)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *centroid_schema = nullptr;
  const ObTableSchema *kv_schema = nullptr;
  bool param_filled = false;
  should_skip = false;

  if (OB_FAIL(schema_guard.get_table_schema(
          tenant_id, centroid_table_id, centroid_schema))) {
    LOG_WARN("fail to get centroid schema", KR(ret), K(centroid_table_id));
  } else if (OB_ISNULL(centroid_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("centroid schema not found", KR(ret), K(centroid_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id, centroid_schema->get_data_table_id(), kv_schema))) {
    LOG_WARN("fail to get kv schema", KR(ret));
  } else if (OB_ISNULL(kv_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("kv schema not found", KR(ret));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_param(
                 schema_guard, *centroid_schema, vec_param, param_filled))) {
    LOG_WARN("fail to get vec_param", KR(ret));
  } else if (!param_filled) {
    should_skip = true;
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(
                 *centroid_schema, *kv_schema, vec_param.dim_))) {
    LOG_WARN("fail to get vec dim", KR(ret));
  }
  return ret;
}

int ObIvfCacheWarmupUtil::warmup_ivf_cache_best_effort(
    const obrpc::ObPrewarmLogicTableArg &arg,
    const int64_t expected_role_epoch)
{
  int ret = OB_SUCCESS;
  if (!arg.has_ivf_prewarm_) {
    // nothing to do
  } else {
    ObSchemaGetterGuard schema_guard;
    ObVectorIndexParam vec_param;
    bool should_skip = false;
    ObPluginVectorIndexService *vec_svc = MTL(ObPluginVectorIndexService *);
    if (OB_ISNULL(vec_svc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[PREWARM_RPC] vec_svc is null, skip ivf warmup", KR(ret));
    } else if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[PREWARM_RPC] schema_service is null, skip ivf warmup", KR(ret));
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                   arg.tenant_id_, schema_guard))) {
      LOG_WARN("[PREWARM_RPC] fail to get schema guard for ivf warmup", KR(ret));
    } else if (OB_FAIL(resolve_vec_param_from_centroid_table_id(
                   arg.tenant_id_, schema_guard, arg.ivf_center_table_id_,
                   vec_param, should_skip))) {
      LOG_WARN("[PREWARM_RPC] fail to resolve vec_param for ivf warmup", KR(ret),
               K(arg.ivf_center_table_id_));
    } else if (should_skip) {
      LOG_INFO("[PREWARM_RPC] vec param not filled, skip ivf warmup",
               K(arg.ivf_center_table_id_));
    } else {
      ObIvfPrewarmParam ivf_param;
      ivf_param.namespace_id_     = static_cast<int64_t>(arg.namespace_id_);
      ivf_param.center_table_id_  = arg.ivf_center_table_id_;
      ivf_param.center_tablet_id_ = arg.ivf_center_tablet_id_;
      if (!ivf_param.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("[PREWARM_RPC] invalid ivf_param from rpc arg, skip ivf warmup",
                 KR(ret), K(ivf_param));
      } else if (OB_FAIL(ivf_param.ltable_ids_.assign(arg.ivf_ltable_ids_))) {
        LOG_WARN("[PREWARM_RPC] fail to assign ltable_ids for ivf warmup", KR(ret));
      } else if (expected_role_epoch > 0
                 && OB_FAIL(vec_svc->prewarm_ivf_cache_by_ns_ids(
                     arg.ls_id_, ivf_param, vec_param, expected_role_epoch))) {
        LOG_WARN("[PREWARM_RPC] generation-fenced ivf warmup failed",
                 KR(ret), K(ivf_param), K(expected_role_epoch));
      } else if (expected_role_epoch <= 0
                 && OB_FAIL(vec_svc->warmup_ivf_cache_by_ns_ids(
                     arg.ls_id_, ivf_param, vec_param))) {
        LOG_WARN("[PREWARM_RPC] ivf warmup failed, non-fatal", KR(ret), K(ivf_param));
      } else {
        LOG_INFO("[PREWARM_RPC] ivf warmup succeeded", K(arg.ivf_center_tablet_id_),
                 K(arg.namespace_id_), K(arg.ivf_ltable_ids_));
      }
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase
