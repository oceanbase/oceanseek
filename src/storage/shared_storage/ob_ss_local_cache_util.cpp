/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "storage/shared_storage/ob_ss_local_cache_util.h"
#include "share/io/ob_io_manager.h"
#include "share/ob_io_device_helper.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/ob_tablet_id.h"
#include "storage/blocksstable/ob_storage_object_handle.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "lib/alloc/alloc_func.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

int ObSSLocalCacheUtil::create_schema_guard_if_need(
    const uint64_t tenant_id,
    ObMultiVersionSchemaService *schema_service,
    ObSchemaGetterGuard &schema_guard)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service) || OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(schema_service));
  } else if (schema_guard.is_inited()) {
    // schema guard is already inited
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get tenant schema guard", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObSSLocalCacheUtil::get_table_schema(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObMultiVersionSchemaService *schema_service,
    ObSchemaGetterGuard &schema_guard,
    const ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(create_schema_guard_if_need(tenant_id, schema_service, schema_guard))) {
    LOG_WARN("fail to create schema_guard if need", KR(ret), K(tenant_id), KP(schema_service));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("table schema is null, need retry", KR(ret), K(tenant_id), K(table_id));
  }
  return ret;
}

int ObSSLocalCacheUtil::get_table_schema(
    const uint64_t tenant_id, 
    const ObString &db_name,
    const ObString &table_name,
    ObMultiVersionSchemaService *schema_service,
    ObSchemaGetterGuard &schema_guard, 
    const ObTableSchema *&table_schema,
    const bool is_index)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(db_name.empty() || table_name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(db_name), K(table_name), KP(schema_service));
  } else if (OB_FAIL(create_schema_guard_if_need(tenant_id, schema_service, schema_guard))) {
    LOG_WARN("fail to create schema_guard if need", KR(ret), K(tenant_id), KP(schema_service));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, db_name, table_name, is_index, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(db_name), K(table_name), K(is_index));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_EAGAIN;
    LOG_WARN("table schema is null, need retry", KR(ret), K(tenant_id), K(db_name), K(table_name));
  }
  return ret;
}

int ObSSLocalCacheUtil::get_table_index_array(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObMultiVersionSchemaService *schema_service,
    ObSchemaGetterGuard &schema_guard, 
    uint64_t *index_tid_array,
    int64_t &index_arr_cnt,
    const bool with_mv)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_FAIL(get_table_schema(tenant_id, table_id, schema_service, schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (OB_FAIL(schema_guard.get_can_read_index_array(tenant_id, table_id, index_tid_array,
             index_arr_cnt, with_mv))) {
    LOG_WARN("fail to get can read index array", KR(ret), K(tenant_id), K(table_id), K(with_mv));
  } else if (index_arr_cnt > OB_MAX_INDEX_PER_TABLE) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index count is bigger than OB_MAX_INDEX_PER_TABLE", KR(ret), K(tenant_id), K(table_id), K(index_arr_cnt));
  } else {
    if (table_schema->get_aux_lob_meta_tid() != OB_INVALID_ID) {
      index_tid_array[index_arr_cnt] = table_schema->get_aux_lob_meta_tid();
      ++index_arr_cnt;
    }
    if (table_schema->get_aux_lob_piece_tid() != OB_INVALID_ID) {
      index_tid_array[index_arr_cnt] = table_schema->get_aux_lob_piece_tid();
      ++index_arr_cnt;
    }
  }
  return ret;
}

int ObSSLocalCacheUtil::get_table_tablet_ids(
    const uint64_t tenant_id,
    const ObTableSchema *table_schema,
    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)) || OB_ISNULL(table_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), KP(table_schema));
  } else {
    ObSEArray<ObTabletID, 16> cur_tablet_ids;
    cur_tablet_ids.set_attr(ObMemAttr(tenant_id, "SSTabletIDs"));
    if (OB_FAIL(table_schema->get_tablet_ids(cur_tablet_ids))) {
      LOG_WARN("fail to get table tablet ids", KR(ret), KPC(table_schema));
    } else if (OB_FAIL(append(tablet_ids, cur_tablet_ids))) {
      LOG_WARN("fail to append array", KR(ret), K(tablet_ids.count()), K(cur_tablet_ids.count()),
        K(cur_tablet_ids), K(tablet_ids));
    }
  }
  return ret;
}

int ObSSLocalCacheUtil::get_table_id_by_tablet_id(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t schema_version,
    ObMultiVersionSchemaService *schema_service,
    uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_service) || OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_id), KP(schema_service));
  } else {
    ObSEArray<ObTabletID, 1> tablet_ids;
    tablet_ids.set_attr(ObMemAttr(tenant_id, "SSTabletID"));
    ObSEArray<uint64_t, 1> table_ids;
    table_ids.set_attr(ObMemAttr(tenant_id, "SSTableID"));
    if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
      LOG_WARN("fail to push back", KR(ret), K(tablet_id));
    } else if (OB_FAIL(schema_service->get_tablet_to_table_history(tenant_id, tablet_ids, schema_version, table_ids))) {
      LOG_WARN("fail to get table id according to tablet id", KR(ret), K(tablet_id), K(schema_version));
    } else if (OB_UNLIKELY(table_ids.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected empty table id", KR(ret), K(table_ids));
    } else if (table_ids.at(0) == OB_INVALID_ID) {
      ret = OB_TABLE_IS_DELETED;
      LOG_WARN("table is deleted", KR(ret), K(tablet_id), K(schema_version));
    } else {
      table_id = table_ids.at(0);
    }
  }
  return ret;
}

int ObSSLocalCacheUtil::get_tablet_part_idx(
    const ObTableSchema *table_schema,
    const ObTabletID &tablet_id,
    int64_t &part_idx,
    int64_t &sub_part_idx)
{
  int ret = OB_SUCCESS;
  part_idx = OB_INVALID_INDEX;
  sub_part_idx = OB_INVALID_INDEX;
  if (OB_ISNULL(table_schema) || OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tablet_id), KP(table_schema));
  } else if (OB_FAIL(table_schema->get_part_idx_by_tablet(tablet_id, part_idx, sub_part_idx))) {
    LOG_WARN("fail to get part idx by tablet", K(ret), K(tablet_id));
  }
  return ret;
}

int ObSSLocalCacheUtil::get_local_index_table_tablet_by_part_idx(
    const uint64_t tenant_id,
    const uint64_t table_id,
    ObMultiVersionSchemaService *schema_service,
    ObSchemaGetterGuard &schema_guard, 
    const int64_t part_idx,
    const int64_t sub_part_idx,
    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = nullptr;
  if (OB_ISNULL(schema_service) || OB_UNLIKELY(OB_INVALID_INDEX == part_idx && OB_INVALID_INDEX == sub_part_idx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_idx), K(sub_part_idx), KP(schema_service));
  } else if (OB_FAIL(get_table_schema(tenant_id, table_id, schema_service, schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  } else if (table_schema->is_index_local_storage() || table_schema->is_aux_lob_table()) {
    ObObjectID index_part_id = OB_INVALID_ID;
    ObObjectID index_first_level_part_id = OB_INVALID_ID;
    ObTabletID cur_tablet_id;
    
    if (OB_FAIL(table_schema->get_part_id_and_tablet_id_by_idx(part_idx, sub_part_idx, 
                index_part_id, index_first_level_part_id, cur_tablet_id))) {
      LOG_WARN("fail to get tablet id by idx", KR(ret), K(table_id), K(part_idx), K(sub_part_idx));
    } else if (OB_FAIL(tablet_ids.push_back(cur_tablet_id))) {
      LOG_WARN("fail to push back", K(ret), K(cur_tablet_id));
    }
  }
  return ret;
}

// get all tablets from each index table
int ObSSLocalCacheUtil::get_tablets_from_all_index_tables(
    const uint64_t tenant_id,
    const bool need_local_index,
    const bool need_global_index,
    const ObTableSchema *table_schema,
    ObMultiVersionSchemaService *schema_service,
    ObSchemaGetterGuard &schema_guard,
    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema) || OB_ISNULL(schema_service) || 
      OB_UNLIKELY(!need_local_index && !need_global_index)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(table_schema), KP(schema_service),
      K(need_local_index), K(need_global_index));
  } else {
    const uint64_t data_tid = table_schema->get_table_id();
    const int64_t MAX_INDEX_TABLE_CNT = OB_MAX_INDEX_PER_TABLE + OB_AUX_LOB_TABLE_CNT;
    uint64_t index_tid_array[MAX_INDEX_TABLE_CNT];
    int64_t index_cnt = 0;
    if (OB_FAIL(get_table_index_array(tenant_id, data_tid, schema_service, schema_guard,
        index_tid_array, index_cnt, false/*with_mv*/))) {
      LOG_WARN("fail to get table index array", KR(ret), K(tenant_id), K(data_tid), K(need_local_index),
        K(need_global_index));
    } else if (index_cnt > 0) {
      for (int64_t i = 0; i < index_cnt; ++i) {
        const uint64_t idx_tid = index_tid_array[i];
        // if failed, continue to handle next index table
        const ObTableSchema *table_schema = nullptr;
        if (OB_TMP_FAIL(get_table_schema(tenant_id, idx_tid, schema_service, schema_guard, table_schema))) {
          LOG_WARN("fail to get table schema", KR(tmp_ret), K(tenant_id), K(idx_tid), KP(schema_service));
        } else if ((table_schema->is_index_local_storage() || table_schema->is_aux_lob_table()) && !need_local_index) {
          // skip
        } else if (table_schema->is_global_index_table() && !need_global_index) {
          // skip
        } else if (OB_TMP_FAIL(get_table_tablet_ids(tenant_id, table_schema, tablet_ids))) {
          LOG_WARN("fail to get table tablet_ids", KR(tmp_ret), K(tenant_id), KPC(table_schema));
        }
        tmp_ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

// get one relative tablet from each local index table
int ObSSLocalCacheUtil::get_tablet_from_all_local_index_tables(
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t schema_version,
    ObMultiVersionSchemaService *schema_service,
    ObSchemaGetterGuard &schema_guard, 
    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const share::schema::ObTableSchema *data_table_schema = nullptr;
  uint64_t data_tid = 0;
  if (OB_FAIL(get_table_id_by_tablet_id(tenant_id, tablet_id, schema_version, schema_service, data_tid))) {
    LOG_WARN("fail to get table_id by tablet_id", KR(ret), K(tenant_id), K(schema_version), K(tablet_id));
  } else if (OB_FAIL(get_table_schema(tenant_id, data_tid, schema_service, schema_guard, data_table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(data_tid), K(tablet_id));
  } else {
    int64_t part_idx = OB_INVALID_INDEX;
    int64_t sub_part_idx = OB_INVALID_INDEX;

    if (OB_FAIL(data_table_schema->get_part_idx_by_tablet(tablet_id, part_idx, sub_part_idx))) {
      LOG_WARN("fail to get part idx by tablet", K(ret), K(tablet_id));
    } else {
      const int64_t MAX_INDEX_TABLE_CNT = OB_MAX_INDEX_PER_TABLE + OB_AUX_LOB_TABLE_CNT;
      uint64_t index_tid_array[MAX_INDEX_TABLE_CNT];
      int64_t index_cnt = 0;
      if (OB_FAIL(get_table_index_array(tenant_id, data_tid, schema_service, schema_guard,
          index_tid_array, index_cnt, false/*with_mv*/))) {
        LOG_WARN("fail to get table index array", KR(ret), K(tenant_id), K(data_tid));
      } else if (index_cnt > 0) {
        for (int64_t i = 0; i < index_cnt; ++i) {
          const uint64_t index_tid = index_tid_array[i];
          // if failed, continue to handle next index table
          if (OB_TMP_FAIL(get_local_index_table_tablet_by_part_idx(tenant_id, index_tid, schema_service, schema_guard,
              part_idx, sub_part_idx, tablet_ids))) {
            LOG_WARN("fail to get local index table tablet by part_idx", KR(tmp_ret), K(tenant_id), K(i), K(index_tid),
              K(part_idx), K(sub_part_idx));
            tmp_ret = OB_SUCCESS;
          }
        }
      }
    }
  }
  return ret;
}

int ObSSLocalCacheUtil::handle_hit_cache_in_memory(
    const char *io_buf,
    const int64_t data_size,
    ObIOInfo &io_info,
    ObStorageObjectHandle &object_handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(io_buf) || OB_UNLIKELY(data_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(io_buf), K(data_size));
  }
  // HIT_MEMORY directly copy data from memory of cache, and bypass IOManager.
  // if there exists io_callback, directly execute io_callback here in user thread.
  else if (OB_NOT_NULL(io_info.callback_)) {
    // issue: 58012372
    if (ObIOCallbackType::ASYNC_SINGLE_MICRO_BLOCK_CALLBACK == io_info.callback_->get_type()) {
      ObAsyncSingleMicroBlockIOCallback *callback = static_cast<ObAsyncSingleMicroBlockIOCallback *>(io_info.callback_);
      if (OB_FAIL(callback->process_without_tl_reader(io_buf, data_size))) {
        LOG_WARN("callback process without tl reader failed", KR(ret), K(data_size), K(io_info));
      }
    } else if (ObIOCallbackType::MULTI_DATA_BLOCK_CALLBACK == io_info.callback_->get_type()) {
      ObMultiDataBlockIOCallback *callback = static_cast<ObMultiDataBlockIOCallback *>(io_info.callback_);
      if (OB_FAIL(callback->process_without_tl_reader(io_buf, data_size))) {
        LOG_WARN("callback process without tl reader failed", KR(ret), K(data_size), K(io_info));
      }
    } else if (OB_FAIL(io_info.callback_->process(io_buf, data_size))) {
      LOG_WARN("callback process failed", KR(ret), K(data_size), K(io_info));
    }
  } else {
    if (OB_ISNULL(io_info.user_data_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user data buf is null", KR(ret), K(io_info));
    } else {
      MEMCPY(io_info.user_data_buf_, io_buf, data_size);
    }
  }
  
  if (OB_SUCC(ret)) {
    // need simulate ObIOResult. user will call ObObjectStorageHandle::get_buffer/get_data_size,
    // which get data from ObIOResult::user_data_buf_
    int64_t aligned_size = 0;
    int64_t simulate_complete_size = 0;
    io_info.fd_.device_handle_ = &LOCAL_DEVICE_INSTANCE;
    ObIOResult *io_result = nullptr;
    ObRefHolder<ObTenantIOManager> tenant_holder;
    if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(io_info.tenant_id_, tenant_holder))) {
      LOG_WARN("fail to get tenant io manager", KR(ret), "tenant_id", io_info.tenant_id_);
    } else if (OB_ISNULL(tenant_holder.get_ptr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tenant holder ptr is null", KR(ret));
    } else if (OB_FAIL(tenant_holder.get_ptr()->alloc_and_init_result(io_info, io_result))) {
      LOG_WARN("fail to alloc and init result", KR(ret), K(io_info));
    } else if (OB_ISNULL(io_result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io result is null", KR(ret));
    } else {
      aligned_size = io_info.fd_.device_handle_->get_io_aligned_size();
      simulate_complete_size = upper_align(io_info.offset_ + data_size, aligned_size);
      io_result->set_complete_size(simulate_complete_size);
      object_handle.get_io_handle().set_result(*io_result);
      io_result->finish_without_accumulate(ret);
    }
  }
  return ret;
}

int ObSSLocalCacheUtil::get_tenant_memory_info(const uint64_t tenant_id, int64_t &total_mem, int64_t &used_mem)
{
  int ret = OB_SUCCESS;
  total_mem = 0;
  used_mem = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else {
    total_mem = lib::get_tenant_memory_limit(tenant_id);
    used_mem = lib::get_tenant_memory_hold(tenant_id);
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
