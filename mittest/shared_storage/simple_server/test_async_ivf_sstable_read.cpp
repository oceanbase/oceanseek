/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <algorithm>
#include <array>
#include <cmath>
#include <cstdlib>
#include <cstdio>
#include <set>
#include <string>
#include <unistd.h>
#include <vector>

#include <gtest/gtest.h>

#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/string/ob_sql_string.h"
#include "lib/utility/ob_defer.h"
#include "lib/utility/ob_test_util.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_disaster_recovery_replace_tenant.h"
#include "rootserver/ob_server_zone_op_service.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "share/ob_async_index_refresh_util.h"
#include "share/ob_master_key_getter.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/object_storage/ob_device_connectivity.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "storage/access/ob_store_row_iterator.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_read_info.h"
#include "storage/async_index/ob_async_index_deps.h"
#include "storage/async_index/ob_async_index_service.h"
#include "storage/blocksstable/ob_datum_range.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_fht_kac_rotator.h"
#include "share/vector_index/quantization/rabitq_quantization/ob_rabitq_quantizer.h"
#include "storage/ob_storage_struct.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_table_store_iterator.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
const char *shared_storage_info = nullptr;

namespace rootserver
{
// Palf-KV belongs to the external control plane and is unavailable in mittest.
int ObDRReplaceTenant::get_logservice_cluster_id(uint64_t &logservice_cluster_id)
{
  logservice_cluster_id = 1;
  return OB_SUCCESS;
}

int ObServerZoneOpService::store_data_version_in_palf_kv(
    const uint64_t, const uint64_t)
{
  return OB_SUCCESS;
}

int ObServerZoneOpService::get_data_version_in_palf_kv(
    const uint64_t, uint64_t &data_version)
{
  data_version = DATA_CURRENT_VERSION;
  return OB_SUCCESS;
}

int ObServerZoneOpService::store_server_ids_in_palf_kv(
    ServerIDArray &, const uint64_t)
{
  return OB_SUCCESS;
}

int ObServerZoneOpService::generate_new_server_id_from_palf_kv(
    uint64_t &new_server_id)
{
  new_server_id = 1;
  return OB_SUCCESS;
}

int ObServerZoneOpService::store_max_unit_id_in_palf_kv(const uint64_t)
{
  return OB_SUCCESS;
}

int ObServerZoneOpService::store_all_zone_in_palf_kv(
    const ZoneNameArray &)
{
  return OB_SUCCESS;
}

int ObServerZoneOpService::insert_zone_in_palf_kv(const ObZone &)
{
  return OB_SUCCESS;
}
} // namespace rootserver

namespace share
{
int ObServiceEpochProxy::check_and_update_server_zone_op_service_epoch(
    ObMySQLTransaction &trans)
{
  UNUSED(trans);
  return OB_SUCCESS;
}

int ObDeviceConnectivityCheckManager::check_device_connectivity(
    const ObBackupDest &storage_dest)
{
  return storage_dest.is_valid() ? OB_SUCCESS : OB_INVALID_ARGUMENT;
}

int ObMasterKeyUtil::store_sys_root_key_in_palf_kv()
{
  return OB_SUCCESS;
}

int ObMasterKeyUtil::ss_dump_root_key_if_need_and_not_exist(const uint64_t)
{
  return OB_SUCCESS;
}
} // namespace share

namespace unittest
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace storage;
using namespace blocksstable;

namespace
{
constexpr int64_t VECTOR_DIM = 4;
constexpr int64_t VECTOR_PAYLOAD_SIZE =
    VECTOR_DIM * static_cast<int64_t>(sizeof(float));
constexpr int64_t RABITQ_CODE_SIZE = 16;
constexpr int64_t VECTOR_COUNT = 3;
constexpr int64_t TEST_TIMEOUT_US = 60L * 1000L * 1000L;
const char *const TENANT_NAME = "tt1";
const char *const DATABASE_NAME = "test";
const char *const DATA_TABLE_NAME = "async_ivf_sstable_read";
char test_run_name[128] = {0};

const std::array<std::array<float, VECTOR_DIM>, VECTOR_COUNT> EXPECTED_VECTORS = {{
    {{1.0F, 2.0F, 3.0F, 4.0F}},
    {{5.0F, 6.0F, 7.0F, 8.0F}},
    {{9.0F, 10.0F, 11.0F, 12.0F}},
}};

class TestAsyncIvfSSTableRead : public ObSimpleClusterTestBase
{
public:
  TestAsyncIvfSSTableRead()
      : ObSimpleClusterTestBase(test_run_name, "16G", "16G", "10G"),
        tenant_id_(OB_INVALID_TENANT_ID)
  {
    enable_env_warn_log_ = true;
    GCONF.ob_startup_mode.set_value("shared_storage");
    GCONF.enable_logservice.set_value("false");
  }

  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    ASSERT_EQ(OB_SUCCESS,
              create_tenant(TENANT_NAME, "4G", "4G", false, 1, "2G"));
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id_, TENANT_NAME));
    ASSERT_NE(OB_INVALID_TENANT_ID, tenant_id_);
    {
      ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
      ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
      ObLSService *ls_service = MTL(ObLSService *);
      ASSERT_NE(nullptr, ls_service);
      ObLSHandle ls_handle;
      ASSERT_EQ(OB_SUCCESS,
                ls_service->get_ls(ObLSID(ObLSID::SYS_LS_ID), ls_handle,
                                   ObLSGetMod::STORAGE_MOD));
      ObLS *ls = ls_handle.get_ls();
      ASSERT_NE(nullptr, ls);
      bool elected = false;
      for (int64_t i = 0; i < 1000; ++i) {
        ObRole role;
        int64_t proposal_id = 0;
        ASSERT_EQ(OB_SUCCESS,
                  ls->get_log_handler()->get_role(role, proposal_id));
        if (ObRole::LEADER == role) {
          elected = true;
          break;
        }
        ob_usleep(10L * 1000L);
      }
      ASSERT_TRUE(elected)
          << "user tenant SYS LS did not elect a leader within 10 s";
    }
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, exec_write_sql_sys(
        "ALTER SYSTEM SET _enable_async_index = true TENANT = 'tt1'",
        affected_rows));
    for (int64_t i = 0;
         i < 100 && !ObAsyncIndexRefreshUtil::is_async_index_enabled(tenant_id_);
         ++i) {
      ob_usleep(100L * 1000L);
    }
    ASSERT_TRUE(ObAsyncIndexRefreshUtil::is_async_index_enabled(tenant_id_));
    ASSERT_TRUE(ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(
                              TENANT_NAME, DATABASE_NAME));
  }

protected:
  int execute_sql(const char *sql)
  {
    int64_t affected_rows = 0;
    return get_curr_simple_server().get_sql_proxy2().write(sql, affected_rows);
  }

  int get_table_schemas(ObSchemaGetterGuard &schema_guard,
                        const ObTableSchema *&data_schema,
                        const ObTableSchema *&index_schema)
  {
    int ret = OB_SUCCESS;
    ObSEArray<uint64_t, 2> index_table_ids;
    data_schema = nullptr;
    index_schema = nullptr;
    if (OB_ISNULL(GCTX.schema_service_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(
                           tenant_id_, schema_guard))) {
    } else if (OB_FAIL(schema_guard.get_table_schema(
                           tenant_id_, ObString::make_string(DATABASE_NAME),
                           ObString::make_string(DATA_TABLE_NAME), false,
                           data_schema))) {
    } else if (OB_ISNULL(data_schema)) {
      ret = OB_TABLE_NOT_EXIST;
    } else if (OB_FAIL(ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
                           tenant_id_, data_schema->get_table_id(),
                           index_table_ids))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < index_table_ids.count(); ++i) {
      const ObTableSchema *candidate = nullptr;
      if (OB_FAIL(schema_guard.get_table_schema(
              tenant_id_, index_table_ids.at(i), candidate))) {
      } else if (OB_NOT_NULL(candidate) && candidate->is_async_ivf_index()) {
        if (OB_NOT_NULL(index_schema)) {
          ret = OB_ERR_UNEXPECTED;
        } else {
          index_schema = candidate;
        }
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(index_schema)) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  int get_latest_non_empty_ivf_sstable(
      const ObTableSchema &index_schema,
      ObLSHandle &ls_handle,
      ObTabletHandle &tablet_handle,
      ObTableStoreIterator &table_iter,
      ObIvfSSTable *&ivf_sstable)
  {
    int ret = OB_SUCCESS;
    ObSEArray<ObTabletID, 1> index_tablet_ids;
    ObLSID ls_id;
    ObLSService *ls_service = MTL(ObLSService *);
    ObITable *table = nullptr;
    int64_t max_snapshot_version = -1;
    ivf_sstable = nullptr;
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(index_schema.get_tablet_ids(index_tablet_ids))) {
    } else if (1 != index_tablet_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(SimpleServerHelper::select_table_loc(
                           tenant_id_, DATA_TABLE_NAME, ls_id))) {
    } else if (OB_FAIL(ls_service->get_ls(
                           ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    } else if (OB_ISNULL(ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(
                           index_tablet_ids.at(0), tablet_handle,
                           0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    } else if (OB_ISNULL(tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_sstables(
                           table_iter))) {
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_iter.get_next(table))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (table->is_inc_ivf_sstable()) {
        ObIvfSSTable *candidate = static_cast<ObIvfSSTable *>(table);
        if (!candidate->get_ivf_meta().is_empty()
            && candidate->get_snapshot_version() > max_snapshot_version) {
          max_snapshot_version = candidate->get_snapshot_version();
          ivf_sstable = candidate;
        }
      }
    }
    if (OB_SUCC(ret) && OB_ISNULL(ivf_sstable)) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  int scan_and_check_sstable(const ObTableSchema &index_schema,
                             ObLSHandle &ls_handle,
                             ObIvfSSTable &ivf_sstable,
                             const std::vector<std::array<float, VECTOR_DIM>>
                                 &expected_vectors)
  {
    int ret = OB_SUCCESS;
    ObArenaAllocator allocator(ObMemAttr(tenant_id_, "DiskBBQRead"));
    ObSEArray<ObColDesc, 4> column_descs;
    ObTableReadInfo read_info;
    ObTableIterParam iter_param;
    ObStoreCtx store_ctx;
    ObTableAccessContext access_context;
    ObVersionRange version_range;
    ObDatumRange whole_range;
    ObStoreRowIterator *row_iter = nullptr;
    const ObDatumRow *row = nullptr;
    ObQueryFlag query_flag(ObQueryFlag::Forward, false, false, false,
                           false, false, false);
    std::set<uint64_t> posting_vids;
    std::set<uint64_t> full_precision_vector_vids;
    std::set<int64_t> centroid_cids;
    std::vector<std::vector<float>> actual_vectors;
    int64_t centroid_count = 0;
    uint64_t centroid_posting_count = 0;
    int64_t posting_count = 0;
    int64_t full_precision_vector_count = 0;
    int64_t expected_code_size = 0;
    quantization::ObQuantizationParams quantization_params;
    const ObIvfSSTableMeta &ivf_meta = ivf_sstable.get_ivf_meta();
    const ObTabletID index_tablet_id = ivf_sstable.get_key().tablet_id_;

    query_flag.disable_cache();
    version_range.base_version_ = 0;
    version_range.multi_version_start_ = 0;
    version_range.snapshot_version_ = MERGE_READ_SNAPSHOT_VERSION;
    whole_range.set_whole_range();
    quantization_params.dim_ = ivf_meta.dim_;
    quantization_params.bits_ = static_cast<int8_t>(ivf_meta.nbits_);

    if (OB_UNLIKELY(ivf_meta.is_empty()
        || VECTOR_DIM != ivf_meta.dim_
        || IVF_SSTABLE_FORMAT_VERSION != ivf_meta.format_version_
        || 1 != ivf_meta.nbits_
        || ivf_meta.is_hierarchical()
        || ivf_meta.centroid_count_ <= 0
        || ivf_meta.centroid_count_ > ivf_meta.vector_count_
        || ivf_meta.vector_count_ != static_cast<int64_t>(expected_vectors.size())
        || quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION
               != ivf_meta.fht_algo_version_)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(quantization::ObRaBitQQuantizer::calc_code_size(
                           quantization_params, expected_code_size))) {
    } else if (OB_UNLIKELY(RABITQ_CODE_SIZE != expected_code_size)) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(index_schema.get_column_ids(column_descs, false))) {
    } else if (4 != column_descs.count()) {
      ret = OB_ERR_UNEXPECTED;
    } else if (OB_FAIL(read_info.init(
                           allocator, index_schema.get_column_count(),
                           index_schema.get_rowkey_column_num(), false,
                           column_descs, nullptr))) {
    } else if (FALSE_IT(iter_param.table_id_ = index_schema.get_table_id())) {
    } else if (FALSE_IT(iter_param.tablet_id_ = index_tablet_id)) {
    } else if (FALSE_IT(iter_param.read_info_ = &read_info)) {
    } else if (OB_FAIL(store_ctx.init_for_read(
                           ls_handle, ObTimeUtility::current_time() + TEST_TIMEOUT_US,
                           TEST_TIMEOUT_US, SCN::max_scn()))) {
    } else if (FALSE_IT(store_ctx.tablet_id_ = index_tablet_id)) {
    } else if (OB_FAIL(access_context.init(
                           query_flag, store_ctx, allocator, version_range))) {
    } else if (OB_FAIL(ivf_sstable.scan(
                           iter_param, access_context, whole_range, row_iter))) {
    } else if (OB_ISNULL(row_iter)) {
      ret = OB_ERR_UNEXPECTED;
    }

    while (OB_SUCC(ret)) {
      if (OB_FAIL(row_iter->get_next_row(row))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        break;
      } else if (OB_ISNULL(row) || 4 != row->count_) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        const int64_t row_type = row->storage_datums_[0].get_int();
        const int64_t cid = row->storage_datums_[1].get_int();
        const uint64_t vid = row->storage_datums_[2].get_uint();
        const ObString payload = row->storage_datums_[3].get_string();
        if (IVF_ROW_TYPE_CENTROID == row_type) {
          const float *center = nullptr;
          uint32_t children = 0;
          // Refresh emits FLAT centers: group CID is zero, self VID is 1..K.
          if (0 != cid || 0 == vid || vid > static_cast<uint64_t>(ivf_meta.centroid_count_)
              || !centroid_cids.insert(static_cast<int64_t>(vid)).second) {
            ret = OB_ERR_UNEXPECTED;
          } else if (OB_FAIL(ObIvfCentroidPayload::decode(payload, ivf_meta.dim_, center, children))) {
          } else {
            for (int64_t i = 0; OB_SUCC(ret)
                 && i < ivf_meta.dim_; ++i) {
              float value = 0;
              MEMCPY(&value, reinterpret_cast<const char *>(center) + i * sizeof(float), sizeof(float));
              if (!std::isfinite(value)) {
                ret = OB_ERR_UNEXPECTED;
              }
            }
            ++centroid_count;
            centroid_posting_count += children;
          }
        } else if (IVF_ROW_TYPE_LOW_PRECISION_VECTOR == row_type) {
          if (cid < 1 || cid > ivf_meta.centroid_count_
              || 0 == vid || expected_code_size != payload.length()
              || !posting_vids.insert(vid).second) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            ++posting_count;
          }
        } else if (IVF_ROW_TYPE_RAW_VECTOR == row_type) {
          if (IVF_RAW_VECTOR_CID != cid || 0 == vid
              || VECTOR_PAYLOAD_SIZE != payload.length()
              || !full_precision_vector_vids.insert(vid).second) {
            ret = OB_ERR_UNEXPECTED;
          } else {
            std::vector<float> vector(ivf_meta.dim_);
            MEMCPY(vector.data(), payload.ptr(), payload.length());
            actual_vectors.push_back(vector);
            ++full_precision_vector_count;
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
        }
      }
    }

    if (OB_SUCC(ret)) {
      quantization::ObFhtKacRotator rotator;
      std::vector<std::vector<float>> rotated_expected_vectors;
      rotated_expected_vectors.reserve(expected_vectors.size());
      if (OB_FAIL(quantization::ObFhtKacRotator::generate(
              allocator, ivf_meta.dim_, rotator,
              ivf_meta.fht_algo_version_))) {
      }
      for (int64_t i = 0; OB_SUCC(ret)
           && i < static_cast<int64_t>(expected_vectors.size()); ++i) {
        std::vector<float> rotated_vector(ivf_meta.dim_);
        MEMCPY(rotated_vector.data(), expected_vectors.at(i).data(),
               VECTOR_PAYLOAD_SIZE);
        if (OB_FAIL(rotator.rotate(ivf_meta.dim_, rotated_vector.data()))) {
        } else {
          rotated_expected_vectors.push_back(rotated_vector);
        }
      }
      if (OB_SUCC(ret)) {
        std::sort(rotated_expected_vectors.begin(),
                  rotated_expected_vectors.end());
        std::sort(actual_vectors.begin(), actual_vectors.end());
      }
      if (OB_SUCC(ret) && (ivf_meta.centroid_count_ != centroid_count
          || centroid_posting_count != static_cast<uint64_t>(posting_count)
          || static_cast<int64_t>(expected_vectors.size()) != posting_count
          || static_cast<int64_t>(expected_vectors.size())
                 != full_precision_vector_count
          || posting_vids != full_precision_vector_vids
          || rotated_expected_vectors != actual_vectors)) {
        ret = OB_ERR_UNEXPECTED;
      }
    }
    return ret;
  }

protected:
  uint64_t tenant_id_;
};

TEST_F(TestAsyncIvfSSTableRead, refresh_rows_are_readable_from_sstable)
{
  ASSERT_EQ(OB_SUCCESS, execute_sql(
      "CREATE TABLE async_ivf_sstable_read ("
      "id BIGINT, vec VECTOR(4), "
      "ASYNC VECTOR INDEX idx_async_ivf_sstable_read(vec) "
      "WITH (TYPE=DISK_BBQ, DISTANCE=L2, LIB=OB, CLUSTER_SIZE=64)) "
      "ORGANIZATION=HEAP"));
  ASSERT_EQ(OB_SUCCESS, execute_sql(
      "INSERT INTO async_ivf_sstable_read VALUES "
      "(1, '[1,2,3,4]'), "
      "(2, '[5,6,7,8]'), "
      "(3, '[9,10,11,12]')"));

  ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            get_table_schemas(schema_guard, data_schema, index_schema));
  ASSERT_NE(nullptr, data_schema);
  ASSERT_NE(nullptr, index_schema);
  ASSERT_EQ(OB_SUCCESS, execute_sql(
      "CALL dbms_async_index.refresh('async_ivf_sstable_read')"));

  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTableStoreIterator table_iter;
  ObIvfSSTable *ivf_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_latest_non_empty_ivf_sstable(
                            *index_schema, ls_handle, tablet_handle,
                            table_iter, ivf_sstable));
  ASSERT_NE(nullptr, ivf_sstable);
  const std::vector<std::array<float, VECTOR_DIM>> initial_expected_vectors(
      EXPECTED_VECTORS.begin(), EXPECTED_VECTORS.end());
  ASSERT_EQ(OB_SUCCESS,
            scan_and_check_sstable(
                *index_schema, ls_handle, *ivf_sstable,
                initial_expected_vectors));

  // Exercise the synchronous direct interface, which pins refresh parallelism
  // to the minimum value to avoid contention with periodic refresh tasks.
  ObAsyncIndexService *async_index_service = MTL(ObAsyncIndexService *);
  ASSERT_NE(nullptr, async_index_service);
  ObAsyncIndexRefreshWorker &refresh_worker =
      async_index_service->get_refresher().get_refresh_worker();
  ASSERT_EQ(OB_SUCCESS, execute_sql(
      "ALTER SYSTEM SET _enable_async_index_refresh = false"));
  DEFER(EXPECT_EQ(OB_SUCCESS, execute_sql(
      "ALTER SYSTEM SET _enable_async_index_refresh = true")));
  for (int64_t i = 0; i < 100
       && (ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_)
           || refresh_worker.get_in_flight_count() > 0); ++i) {
    ob_usleep(100L * 1000L);
  }
  ASSERT_FALSE(
      ObAsyncIndexRefreshUtil::is_async_index_refresh_enabled(tenant_id_));
  ASSERT_EQ(0, refresh_worker.get_in_flight_count());

  const int64_t multi_range_vector_count = 64;
  std::vector<std::array<float, VECTOR_DIM>> multi_range_expected_vectors;
  multi_range_expected_vectors.reserve(multi_range_vector_count);
  ObSqlString insert_sql;
  ASSERT_EQ(OB_SUCCESS, insert_sql.assign_fmt(
      "INSERT INTO async_ivf_sstable_read VALUES "));
  for (int64_t i = 0; i < multi_range_vector_count; ++i) {
    const float base = static_cast<float>(100 + i * VECTOR_DIM);
    const std::array<float, VECTOR_DIM> vector = {{
        base, base + 1.0F, base + 2.0F, base + 3.0F}};
    multi_range_expected_vectors.push_back(vector);
    ASSERT_EQ(OB_SUCCESS, insert_sql.append_fmt(
        "%s(%ld, '[%.1f,%.1f,%.1f,%.1f]')",
        0 == i ? "" : ", ", 1000 + i,
        static_cast<double>(vector.at(0)),
        static_cast<double>(vector.at(1)),
        static_cast<double>(vector.at(2)),
        static_cast<double>(vector.at(3))));
  }
  ASSERT_EQ(OB_SUCCESS, execute_sql(insert_sql.ptr()));

  ASSERT_NE(nullptr, ls_handle.get_ls());
  ObRole role;
  int64_t leader_proposal_id = -1;
  ASSERT_EQ(OB_SUCCESS,
            ls_handle.get_ls()->get_log_handler()->get_role(role, leader_proposal_id));
  ASSERT_EQ(ObRole::LEADER, role);
  SCN requested_target;
  transaction::MonotonicTs receive_gts_ts;
  ASSERT_EQ(OB_SUCCESS,
            OB_TS_MGR.get_gts_sync(tenant_id_, transaction::MonotonicTs::current_time(),
                10L * 1000L * 1000L, requested_target, receive_gts_ts));
  ObSEArray<ObTabletID, 1> data_tablet_ids;
  ObSEArray<ObTabletID, 1> index_tablet_ids;
  ASSERT_EQ(OB_SUCCESS, data_schema->get_tablet_ids(data_tablet_ids));
  ASSERT_EQ(OB_SUCCESS, index_schema->get_tablet_ids(index_tablet_ids));
  ASSERT_EQ(1, data_tablet_ids.count());
  ASSERT_EQ(1, index_tablet_ids.count());
  ASSERT_NE(nullptr, ls_handle.get_ls());

  ObTabletAsyncIndexRefreshInfo refresh_info;
  ASSERT_EQ(OB_SUCCESS, maintain_async_index_refresh_info(
      tenant_id_, ls_handle.get_ls(), leader_proposal_id, data_tablet_ids.at(0), requested_target,
      SCN::base_scn(), &async_index_service->get_refresher().get_dep_provider(),
      refresh_info, OB_ASYNC_INDEX_DDL_LOCK_TIMEOUT_US,
      true /* skip_incr_check */));
  ASSERT_FALSE(refresh_info.get_refresh_snapshots().empty());
  const int64_t target_snapshot = refresh_info.get_refresh_snapshots().at(
      refresh_info.get_refresh_snapshots().count() - 1);
  const SCN direct_target = async_index_snapshot_to_scn(target_snapshot);
  ASSERT_TRUE(direct_target.is_valid());

  ObAsyncIndexDirectRefreshParam direct_param;
  direct_param.tenant_id_ = tenant_id_;
  direct_param.ls_ = ls_handle.get_ls();
  direct_param.leader_proposal_id_ = leader_proposal_id;
  direct_param.data_table_id_ = data_schema->get_table_id();
  direct_param.index_table_id_ = index_schema->get_table_id();
  direct_param.data_tablet_id_ = data_tablet_ids.at(0);
  direct_param.index_tablet_id_ = index_tablet_ids.at(0);
  direct_param.target_refresh_scn_ = direct_target;
  direct_param.op_ = DIRECT_REFRESH_OP_INSERT_NEW;
  direct_param.refresh_type_ = ASYNC_REFRESH_TYPE_MANUAL;
  ASSERT_TRUE(direct_param.is_valid());
  const int64_t direct_begin_us = ObTimeUtility::current_time();
  ASSERT_EQ(OB_SUCCESS,
            async_index_service->trigger_incr_refresh(direct_param));

  ObSEArray<ObRefreshTaskStat, 16> finished_stats;
  ASSERT_EQ(OB_SUCCESS,
            refresh_worker.get_recent_finished_stats(finished_stats));
  const ObRefreshTaskStat *direct_stat = nullptr;
  for (int64_t i = finished_stats.count() - 1;
       i >= 0 && nullptr == direct_stat; --i) {
    const ObRefreshTaskStat &stat = finished_stats.at(i);
    if (tenant_id_ == stat.tenant_id_
        && data_schema->get_table_id() == stat.data_table_id_
        && ls_handle.get_ls()->get_ls_id() == stat.ls_id_
        && data_tablet_ids.at(0) == stat.data_tablet_id_
        && index_tablet_ids.at(0) == stat.tablet_id_
        && direct_target == stat.target_refresh_scn_
        && ASYNC_REFRESH_TYPE_MANUAL == stat.refresh_type_
        && OB_ASYNC_INDEX_MIN_PARALLELISM == stat.refresh_parallelism_
        && stat.end_time_ >= direct_begin_us) {
      direct_stat = &stat;
    }
  }
  ASSERT_NE(nullptr, direct_stat);
  EXPECT_EQ(OB_SUCCESS, direct_stat->result_);
  EXPECT_GT(direct_stat->refresh_task_id_, 0);
  EXPECT_FALSE(direct_stat->is_partition_local_);
  EXPECT_FALSE(direct_stat->is_complete_refresh_);
  EXPECT_LE(direct_stat->queue_time_, direct_stat->start_time_);
  EXPECT_LE(direct_stat->start_time_, direct_stat->end_time_);
  EXPECT_EQ(multi_range_vector_count, direct_stat->affected_rows_);

  ObLSHandle multi_range_ls_handle;
  ObTabletHandle multi_range_tablet_handle;
  ObTableStoreIterator multi_range_table_iter;
  ObIvfSSTable *multi_range_ivf_sstable = nullptr;
  ASSERT_EQ(OB_SUCCESS, get_latest_non_empty_ivf_sstable(
                            *index_schema, multi_range_ls_handle,
                            multi_range_tablet_handle, multi_range_table_iter,
                            multi_range_ivf_sstable));
  ASSERT_NE(nullptr, multi_range_ivf_sstable);
  ASSERT_NE(nullptr, multi_range_tablet_handle.get_obj());
  ASSERT_GE(multi_range_ivf_sstable->get_snapshot_version(), target_snapshot);
  int64_t max_published_version = 0;
  for (int64_t i = 0; i < 100
       && max_published_version < target_snapshot; ++i) {
    ObTabletHandle latest_tablet_handle;
    ASSERT_EQ(OB_SUCCESS, multi_range_ls_handle.get_ls()->get_tablet(
                              index_tablet_ids.at(0), latest_tablet_handle,
                              0, ObMDSGetTabletMode::READ_WITHOUT_CHECK));
    ASSERT_NE(nullptr, latest_tablet_handle.get_obj());
    ASSERT_EQ(OB_SUCCESS,
              latest_tablet_handle.get_obj()
                  ->get_async_index_max_published_version(
                      max_published_version));
    if (max_published_version < target_snapshot) {
      ob_usleep(100L * 1000L);
    }
  }
  EXPECT_GE(max_published_version, target_snapshot);
  ASSERT_EQ(OB_SUCCESS, scan_and_check_sstable(
                            *index_schema, multi_range_ls_handle,
                            *multi_range_ivf_sstable,
                            multi_range_expected_vectors));
}

TEST_F(TestAsyncIvfSSTableRead, post_zero_window_fallback_regression)
{
  OB_LOGGER.set_log_level("INFO");
  ASSERT_EQ(OB_SUCCESS, execute_sql("ALTER SYSTEM SET _disk_bbq_max_scan_vectors = 0"));
  ASSERT_EQ(OB_SUCCESS, execute_sql(
      "CREATE TABLE async_ivf_sstable_read ("
      "id INT NOT NULL, record_id VARCHAR(128) NOT NULL, "
      "knowledge_base_id VARCHAR(256) NOT NULL, content_vector VECTOR(4), "
      "ASYNC VECTOR INDEX vec_idx1(content_vector) "
      "WITH (TYPE=DISK_BBQ,DISTANCE=COSINE,LIB=OB,CLUSTER_SIZE=64)) "
      "ORGANIZATION=HEAP"));
  // Distribution makes an independence estimate nonzero although the AND is impossible.
  for (int base = 0; base < 100000; base += 500) {
    ObSqlString insert;
    ASSERT_EQ(OB_SUCCESS, insert.append("INSERT INTO async_ivf_sstable_read VALUES "));
    for (int i = base; i < base + 500; ++i) {
      const double x = 0.1 + ((i * 17) % 997) / 997.0;
      const double y = 0.1 + ((i * 31) % 991) / 991.0;
      const double z = 0.1 + ((i * 47) % 983) / 983.0;
      const double w = 0.1 + ((i * 71) % 977) / 977.0;
      ASSERT_EQ(OB_SUCCESS, insert.append_fmt(
          "%s(%d,'r%d','%s','[%.7f,%.7f,%.7f,%.7f]')",
          i == base ? "" : ",", i + 1, i + 1,
          i % 4 == 0 ? "175" : "198", x, y, z, w));
    }
    ASSERT_EQ(OB_SUCCESS, execute_sql(insert.ptr()));
  }
  ASSERT_EQ(OB_SUCCESS, execute_sql(
      "CALL DBMS_STATS.GATHER_TABLE_STATS('test','async_ivf_sstable_read')"));

  {
    ObTenantSwitchGuard tenant_guard = _make_tenant_switch_guard();
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *data_schema = nullptr;
    const ObTableSchema *index_schema = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_table_schemas(schema_guard, data_schema, index_schema));
    ObSEArray<uint64_t, 4> index_ids;
    ASSERT_EQ(OB_SUCCESS, ObAsyncIndexRefreshUtil::collect_async_index_table_ids(
        tenant_id_, data_schema->get_table_id(), index_ids));
    SCN target;
    transaction::MonotonicTs received(0);
    ASSERT_EQ(OB_SUCCESS, OB_TS_MGR.get_gts_sync(tenant_id_,
        transaction::MonotonicTs::current_time(), 10 * 1000000L, target, received));
    ASSERT_EQ(OB_SUCCESS, ObAsyncIndexRefreshUtil::trigger_refresh_for_table(
        tenant_id_, data_schema->get_table_id(), target.get_val_for_tx(), index_ids,
        ASYNC_REFRESH_TYPE_MANUAL));
    ObLSHandle ls_handle;
    ObTabletHandle tablet_handle;
    ObTableStoreIterator table_iter;
    ObIvfSSTable *sstable = nullptr;
    ASSERT_EQ(OB_SUCCESS, get_latest_non_empty_ivf_sstable(
        *index_schema, ls_handle, tablet_handle, table_iter, sstable));
    ASSERT_NE(nullptr, sstable);
  }

  auto run_query = [&](const char *label, int64_t nprobe, const char *filter,
                       int64_t limit, int64_t expected_rows, int64_t expected_id,
                       const char *path = "POST_FILTER", const char *strategy = "RECALL_FIRST",
                       int64_t refine_k = -1) {
    ObSqlString query;
    ASSERT_EQ(OB_SUCCESS, query.append_fmt(
        "SELECT /*+ QUERY_TIMEOUT(600000000) VECTOR_INDEX(ak vec_idx1 %s) */ "
        "ak.id,COSINE_DISTANCE(content_vector,'[1,2,3,4]') AS SCORE "
        "FROM async_ivf_sstable_read ak WHERE %s "
        "ORDER BY SCORE APPROXIMATE LIMIT %ld "
        "PARAMETERS(nprobes=%ld,strategy=%s", path, filter, limit, nprobe, strategy));
    if (refine_k > 0) {
      ASSERT_EQ(OB_SUCCESS, query.append_fmt(",refine_k=%ld", refine_k));
    }
    ASSERT_EQ(OB_SUCCESS, query.append(")"));
    const int64_t begin = ObTimeUtility::current_time();
    printf("POST_ZERO begin label=%s time_us=%ld nprobe=%ld\n", label, begin, nprobe);
    fflush(stdout);
    ObMySQLProxy::MySQLResult holder;
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().read(holder, query.ptr())) << label;
    auto *result = holder.get_result();
    ASSERT_NE(nullptr, result);
    int ret = OB_SUCCESS;
    int64_t count = 0;
    while (OB_SUCCESS == (ret = result->next())) {
      int64_t id = 0;
      ASSERT_EQ(OB_SUCCESS, result->get_int(static_cast<int64_t>(0), id));
      if (expected_id > 0) { EXPECT_EQ(expected_id, id) << label; }
      ++count;
    }
    ASSERT_EQ(OB_ITER_END, ret) << label;
    EXPECT_EQ(expected_rows, count) << label;
    printf("POST_ZERO end label=%s rows=%ld elapsed_us=%ld time_us=%ld\n",
           label, count, ObTimeUtility::current_time() - begin, ObTimeUtility::current_time());
    fflush(stdout);
  };
  const char *contradiction =
      "ak.knowledge_base_id IN ('198','176') AND ak.knowledge_base_id='175' AND ak.id>=256";
  run_query("fixed_zero", 8, contradiction, 1000, 0, -1);
  ASSERT_FALSE(HasFatalFailure());
  run_query("auto_zero", 0, contradiction, 1000, 0, -1);
  ASSERT_FALSE(HasFatalFailure());
  run_query("direct_pre_empty", 8, contradiction, 1000, 0, -1, "PRE_FILTER");
  ASSERT_FALSE(HasFatalFailure());

  // A zero POST window does not prove the table has no matches. Select the
  // vector furthest from the query, then verify the complete bitmap finds it.
  double min_cosine = 2.0;
  int64_t farthest_id = -1;
  for (int i = 0; i < 100000; ++i) {
    const double x = 0.1 + ((i * 17) % 997) / 997.0;
    const double y = 0.1 + ((i * 31) % 991) / 991.0;
    const double z = 0.1 + ((i * 47) % 983) / 983.0;
    const double w = 0.1 + ((i * 71) % 977) / 977.0;
    const double cosine = (x + 2*y + 3*z + 4*w) / std::sqrt(30.0 * (x*x+y*y+z*z+w*w));
    if (cosine < min_cosine) { min_cosine = cosine; farthest_id = i + 1; }
  }
  ASSERT_GT(farthest_id, 0);
  ObSqlString rare_filter;
  ASSERT_EQ(OB_SUCCESS, rare_filter.append_fmt("ak.id=%ld", farthest_id));
  run_query("fixed_zero_then_nonempty_pre", 1, rare_filter.ptr(), 1, 1, farthest_id);
  ASSERT_FALSE(HasFatalFailure());
  run_query("auto_zero_then_nonempty_pre", 0, rare_filter.ptr(), 1, 1, farthest_id);
  ASSERT_FALSE(HasFatalFailure());

  int64_t first_window_id = -1;
  {
    ObMySQLProxy::MySQLResult holder;
    const char *seed_query =
        "SELECT ak.id,COSINE_DISTANCE(content_vector,'[1,2,3,4]') AS SCORE "
        "FROM async_ivf_sstable_read ak ORDER BY SCORE APPROXIMATE LIMIT 1 "
        "PARAMETERS(nprobes=1,strategy=RECALL_FIRST)";
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy2().read(holder, seed_query));
    auto *result = holder.get_result();
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->next());
    ASSERT_EQ(OB_SUCCESS, result->get_int(static_cast<int64_t>(0), first_window_id));
    ASSERT_EQ(OB_ITER_END, result->next());
  }
  ObSqlString stalled_filter;
  ASSERT_EQ(OB_SUCCESS, stalled_filter.append_fmt("ak.id=%ld", first_window_id));
  run_query("fixed_three_stalled_windows", 1, stalled_filter.ptr(), 2, 1, first_window_id);
  ASSERT_FALSE(HasFatalFailure());
  // An explicit refinement multiplier retains the existing small automatic
  // budget, leaving multiple windows available after the first matching one.
  run_query("auto_three_stalled_windows", 0, stalled_filter.ptr(), 2, 1, first_window_id,
            "POST_FILTER", "RECALL_FIRST", 1);
  ASSERT_FALSE(HasFatalFailure());

  // The accumulated count is nonzero: preserve all refill rounds instead of
  // switching merely because a later round is insufficient for LIMIT.
  run_query("fixed_nonzero_refill", 8, "ak.knowledge_base_id='198'", 100000, 75000, -1);
  ASSERT_FALSE(HasFatalFailure());
  run_query("auto_nonzero", 0, "ak.knowledge_base_id='198'", 100000, 75000, -1);
  ASSERT_FALSE(HasFatalFailure());

  ASSERT_EQ(OB_SUCCESS, execute_sql("ALTER SYSTEM SET _vector_pre_filtering_timeout='10ms'"));
  // POST fallback keeps the existing PRE budget and return policy.
  run_query("latency_zero_budgeted_fallback", 8, contradiction, 1000, 0, -1,
            "POST_FILTER", "LATENCY_FIRST");
  ASSERT_FALSE(HasFatalFailure());
  run_query("latency_stalled_keeps_post_candidates", 1, stalled_filter.ptr(), 2, 1, first_window_id,
            "POST_FILTER", "LATENCY_FIRST");
  ASSERT_FALSE(HasFatalFailure());
  EXPECT_EQ(OB_SUCCESS, execute_sql("ALTER SYSTEM SET _vector_pre_filtering_timeout='50ms'"));
  ASSERT_EQ(OB_SUCCESS, execute_sql(
      "CREATE INDEX kb_idx ON async_ivf_sstable_read(knowledge_base_id)"));
  run_query("fixed_zero_with_scalar_index", 8, contradiction, 1000, 0, -1);
  ASSERT_FALSE(HasFatalFailure());
  run_query("auto_zero_with_scalar_index", 0, contradiction, 1000, 0, -1);
  ASSERT_FALSE(HasFatalFailure());
}

} // namespace
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  char storage_info[oceanbase::OB_MAX_BACKUP_DEST_LENGTH] = {0};
  const char *s3_bucket = std::getenv("OB_MITTEST_S3_BUCKET");
  const char *s3_endpoint = std::getenv("OB_MITTEST_S3_ENDPOINT");
  const char *s3_access_id = std::getenv("OB_MITTEST_S3_ACCESS_ID");
  const char *s3_access_key = std::getenv("OB_MITTEST_S3_ACCESS_KEY");
  const char *s3_region = std::getenv("OB_MITTEST_S3_REGION");
  s3_bucket = nullptr == s3_bucket ? oceanbase::unittest::S3_BUCKET : s3_bucket;
  s3_endpoint = nullptr == s3_endpoint ? oceanbase::unittest::S3_ENDPOINT : s3_endpoint;
  s3_access_id = nullptr == s3_access_id ? oceanbase::unittest::S3_AK : s3_access_id;
  s3_access_key = nullptr == s3_access_key ? oceanbase::unittest::S3_SK : s3_access_key;
  s3_region = nullptr == s3_region ? oceanbase::unittest::S3_REGION : s3_region;
  snprintf(oceanbase::unittest::test_run_name,
           sizeof(oceanbase::unittest::test_run_name),
           "test_async_ivf_sstable_read_%d", static_cast<int>(getpid()));
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  const int length = snprintf(
      storage_info, sizeof(storage_info),
      "%s/%lld?host=%s&access_id=%s&access_key=%s&s3_region=%s"
      "&max_iops=10000&max_bandwidth=200000000B&scope=region",
      s3_bucket,
      static_cast<long long>(oceanbase::common::ObTimeUtility::current_time_ns()),
      s3_endpoint, s3_access_id, s3_access_key, s3_region);
  if (length < 0 || length >= static_cast<int>(sizeof(storage_info))) {
    return 1;
  }
  oceanbase::shared_storage_info = storage_info;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
