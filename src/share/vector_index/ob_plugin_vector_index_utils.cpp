/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#define USING_LOG_PREFIX SERVER
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/allocator/ob_shared_memory_allocator_mgr.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/ob_vec_index_builder_util.h"
#include "share/diagnosis/ob_runtime_profile.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/async_index/ob_async_index_service.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/roaringbitmap/ob_roaringbitmap.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"

namespace oceanbase 
{
namespace share 
{

namespace
{
/**
 * Compare the persisted identity of two vector-index segments.
 * Runtime segment handles are excluded because a reload may recreate them.
 *
 * @param left First segment metadata.
 * @param right Second segment metadata.
 * @return True when both segments identify the same persisted data.
 */
bool is_same_persisted_segment(
    const ObVectorIndexSegmentMeta &left,
    const ObVectorIndexSegmentMeta &right)
{
  return 0 == left.start_key_.compare(right.start_key_)
      && 0 == left.end_key_.compare(right.end_key_);
}

/**
 * Compare an ordered list of persisted vector-index segments.
 *
 * @param left First segment list.
 * @param right Second segment list.
 * @return True when both lists contain the same persisted segments.
 */
bool are_same_persisted_segments(
    const common::ObIArray<ObVectorIndexSegmentMeta> &left,
    const common::ObIArray<ObVectorIndexSegmentMeta> &right)
{
  bool is_same = left.count() == right.count();
  for (int64_t i = 0; is_same && i < left.count(); ++i) {
    is_same = is_same_persisted_segment(left.at(i), right.at(i));
  }
  return is_same;
}

/**
 * Check whether a newly loaded snapshot contains a new Freeze result.
 *
 * @param old_segments Segments already known by the previous adaptor.
 * @param new_segments Segments loaded by the new adaptor.
 * @return True when new_segments contains a new FREEZE_PERSIST range.
 */
bool has_new_freeze_persist_segment(
    const common::ObIArray<ObVectorIndexSegmentMeta> &old_segments,
    const common::ObIArray<ObVectorIndexSegmentMeta> &new_segments)
{
  bool has_new_segment = false;
  for (int64_t i = 0; !has_new_segment && i < new_segments.count(); ++i) {
    const ObVectorIndexSegmentMeta &new_segment = new_segments.at(i);
    if (ObVectorIndexSegmentType::FREEZE_PERSIST == new_segment.seg_type_) {
      bool found = false;
      for (int64_t j = 0; !found && j < old_segments.count(); ++j) {
        found = is_same_persisted_segment(old_segments.at(j), new_segment);
      }
      has_new_segment = !found;
    }
  }
  return has_new_segment;
}
} // namespace

ERRSIM_POINT_DEF(ERRSIM_VEC_REFRESH_MEMDATA,
    "Inject error before vector index refresh memdata.");

int ObPluginVectorIndexUtils::get_task_read_snapshot(ObLSID &ls_id, SCN &read_version)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  // ObLSWRSHandler::get_ls_weak_read_ts
  storage::ObLSService *ls_svr = MTL(storage::ObLSService*);
  if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null ls", K(ret), K(ls_id));
  } else {
    read_version = ls->get_ls_wrs_handler()->get_ls_weak_read_ts();
  }

  return ret;
}

int ObPluginVectorIndexUtils::add_key_ranges(uint64_t table_id, ObRowkey& rowkey, storage::ObTableScanParam &scan_param)
{
  INIT_SUCC(ret);
  ObNewRange new_range;
  if (OB_NOT_NULL(scan_param.scan_allocator_)) {
    // LOG_INFO("scan_allocator_before_reuse", K(scan_param.scan_allocator_->used()), K(scan_param.scan_allocator_->total()));
    scan_param.scan_allocator_->reuse();
    // LOG_INFO("scan_allocator_after_reuse", K(scan_param.scan_allocator_->used()), K(scan_param.scan_allocator_->total()));
  }
  if (OB_FAIL(new_range.build_range(table_id, rowkey))) {
    LOG_WARN("failed to build range.", K(ret), K(table_id), K(rowkey));
  } else if (FALSE_IT(scan_param.key_ranges_.reuse())) {
  } else if (OB_FAIL(scan_param.key_ranges_.push_back(new_range))) {
    LOG_WARN("failed to build key ranges.", K(ret), K(table_id), K(rowkey));
  }

  return ret;
}

int ObPluginVectorIndexUtils::add_key_ranges(uint64_t table_id, ObRowkey& start_key, ObRowkey& end_key, storage::ObTableScanParam &scan_param)
{
  INIT_SUCC(ret);
  ObNewRange new_range;
  if (OB_NOT_NULL(scan_param.scan_allocator_)) {
    // LOG_INFO("scan_allocator_before_reuse", K(scan_param.scan_allocator_->used()), K(scan_param.scan_allocator_->total()));
    scan_param.scan_allocator_->reuse();
    // LOG_INFO("scan_allocator_after_reuse", K(scan_param.scan_allocator_->used()), K(scan_param.scan_allocator_->total()));
  }
  new_range.table_id_ = table_id;
  new_range.start_key_ = start_key;
  new_range.end_key_ = end_key;
  new_range.border_flag_.set_inclusive_start();
  new_range.border_flag_.set_inclusive_end();
  new_range.flag_ = 0;
  scan_param.key_ranges_.reuse();
  if (OB_FAIL(scan_param.key_ranges_.push_back(new_range))) {
    LOG_WARN("failed to build key ranges.", K(ret), K(table_id), K(start_key), K(end_key));
  }

  return ret;
}

int ObPluginVectorIndexUtils::iter_table_rescan(storage::ObTableScanParam &scan_param, common::ObNewRowIterator *iter)
{
  INIT_SUCC(ret);
  ObAccessService *tsc_service = MTL(ObAccessService *);

  if (OB_FAIL(tsc_service->reuse_scan_iter(false, iter))) {
    LOG_WARN("failed to reuse scan iter.", K(ret));
  } else if (OB_FAIL(tsc_service->table_rescan(scan_param, iter))) {
    LOG_WARN("failed to rescan iter.", K(ret));
  }

  return ret;
}

int ObPluginVectorIndexUtils::get_extra_info_objs(storage::ObTableScanParam &scan_param,
                                                  ObIAllocator &allocator, 
                                                  int64_t extra_column_count,
                                                  blocksstable::ObDatumRow *datum_row,
                                                  ObVecExtraInfoObj *out_extra_info_objs,
                                                  int offset)
{
  INIT_SUCC(ret);
  if (extra_column_count == 0) {
  } else if (OB_ISNULL(out_extra_info_objs)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), K(extra_column_count));
  } else {
    const ObIArray<share::schema::ObColumnParam *> *out_col_param =
        scan_param.table_param_->get_read_info().get_columns();
    const ObIArray<int32_t> &out_idxs = scan_param.table_param_->get_output_projector();
    if (OB_ISNULL(out_col_param) || out_idxs.count() < extra_column_count + offset) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column count not equal.", K(ret), KP(out_col_param), K(out_idxs), K(extra_column_count));
    }
    for (int i = 0; OB_SUCC(ret) && i < extra_column_count; ++i) {
      if (out_idxs.at(i + offset) >= out_col_param->count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column count not equal.", K(ret), KP(out_col_param), K(out_idxs), K(i));
      } else {
        ObObjMeta meta_type = out_col_param->at(out_idxs.at(i + offset))->get_meta_type();
        const ObDatum &extra_datum = datum_row->storage_datums_[i + offset];
        if (OB_FALSE_IT(out_extra_info_objs[i].reset())) {
        } else if (OB_FAIL(out_extra_info_objs[i].from_datum(extra_datum, meta_type, &allocator))) {
          LOG_WARN("failed to from obj.", K(ret), K(extra_datum), K(meta_type), K(i));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::read_object_from_data_table_iter(ObObj *&input_obj, 
                                                               int32_t data_table_rowkey_count,
                                                               uint64_t table_id, 
                                                               storage::ObTableScanParam &scan_param, 
                                                               common::ObNewRowIterator *iter,
                                                               schema::ObIndexType type,
                                                               ObIAllocator &allocator, 
                                                               ObObj &output_vec_obj,
                                                               int64_t extra_column_count,
                                                               ObVecExtraInfoObj *output_extra_info_objs,
                                                               bool &get_data)
{
  INIT_SUCC(ret);
  ObRowkey rowkey(input_obj, data_table_rowkey_count);
  
  ObString vector;
  if (OB_FAIL(add_key_ranges(table_id, rowkey, scan_param))) {
    LOG_WARN("failed to set vid id key", K(ret));
  } else if (OB_FAIL(iter_table_rescan(scan_param, iter))) {
    LOG_WARN("failed to recan vid id scan param.", K(ret));
  } else {
    blocksstable::ObDatumRow *datum_row = nullptr;
    storage::ObTableScanIterator *scan_iter = dynamic_cast<storage::ObTableScanIterator *>(iter);
    
    if (OB_ISNULL(scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast to vid iter.", K(ret));
    } else if (OB_FAIL(scan_iter->get_next_row(datum_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from next table.", K(ret));
      } else {
        output_vec_obj.reset();
        ret = OB_SUCCESS;
      }
    } else {
      if (datum_row->get_column_count() != 1 + extra_column_count &&  // at least vector col
          datum_row->get_column_count() != 2) {  // vector col and pk_increrment
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(extra_column_count), K(datum_row->get_column_count()));
      } else {
        if (extra_column_count > 0 && OB_FAIL(get_extra_info_objs(scan_param, allocator, extra_column_count, datum_row, output_extra_info_objs))) {
          LOG_WARN("failed to get extra info.", K(ret), K(extra_column_count), K(datum_row->storage_datums_[extra_column_count]));
        } else {
          char *copy_str = nullptr;
          ObString vector = datum_row->storage_datums_[0].get_string();
          if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
              &allocator, ObLongTextType, CS_TYPE_BINARY, vector))) {
            LOG_WARN("failed to read vector lob data.", K(ret), K(vector.length()));
          }
          const char *vec_ptr = vector.ptr();
          int64_t size = vector.length();
          if (OB_FAIL(ret)) {
          } else if (size == 0) {
            output_vec_obj.reset();
          } else if (OB_ISNULL(copy_str = static_cast<char *>(allocator.alloc(sizeof(char) * size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocator.", K(ret));
          } else {
            memcpy(copy_str, vec_ptr, size);
            output_vec_obj.reset();
            output_vec_obj.set_string(ObVarcharType, copy_str, size);
            get_data = true;
          }
        }
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexUtils::read_object_from_vid_rowkey_table_iter(ObObj *input_obj, 
                                                                     uint64_t table_id, 
                                                                     storage::ObTableScanParam &scan_param, 
                                                                     common::ObNewRowIterator *iter,
                                                                     schema::ObIndexType type,
                                                                     ObIAllocator &allocator, 
                                                                     ObObj *&output_obj,
                                                                     int32_t data_table_rowkey_count)
{
  INIT_SUCC(ret);
  ObRowkey rowkey(input_obj, 1); // vid_rowkey table only has one rowkey column
  
  if (OB_FAIL(add_key_ranges(table_id, rowkey, scan_param))) {
    LOG_WARN("failed to set vid id key", K(ret));
  } else if (OB_FAIL(iter_table_rescan(scan_param, iter))) {
    LOG_WARN("failed to recan vid id scan param.", K(ret));
  } else {
    blocksstable::ObDatumRow *datum_row = nullptr;
    storage::ObTableScanIterator *scan_iter = dynamic_cast<storage::ObTableScanIterator *>(iter);
    
    if (OB_ISNULL(scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast to vid iter.", K(ret));
    } else if (OB_FAIL(scan_iter->get_next_row(datum_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from next table.", K(ret));
      } else {
        // do nothing
        LOG_INFO("vid is removed", K(ret), K(rowkey));
      }
    } else {
      const ObIArray<share::schema::ObColumnParam *> *out_col_param 
        = scan_param.table_param_->get_read_info().get_columns();

      if (datum_row->get_column_count() != data_table_rowkey_count + 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < data_table_rowkey_count; ++i) {
          ObObj tmp_obj;
          output_obj[i].reset();
          ObObjMeta meta_type = out_col_param->at(i + 1)->get_meta_type();
          if (OB_FAIL(datum_row->storage_datums_[i + 1].to_obj(tmp_obj, meta_type))) {
            LOG_WARN("failed to convert datum to obj.", K(ret), K(i), K(datum_row->storage_datums_[i + 1]));
          } else if (OB_FAIL(ob_write_obj(allocator, tmp_obj, output_obj[i]))) {
            LOG_WARN("failed to write obj.", K(ret), K(i), K(tmp_obj));
          }
        }
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexUtils::read_object_from_embedded_table_iter(ObObj *&input_obj, 
                                                                   int32_t data_table_rowkey_count,
                                                                   uint64_t table_id, 
                                                                   storage::ObTableScanParam &scan_param, 
                                                                   common::ObNewRowIterator *iter,
                                                                   ObIAllocator &allocator, 
                                                                   ObObj &output_vec_obj,
                                                                   int64_t extra_column_count,
                                                                   ObVecExtraInfoObj *output_extra_info_objs,
                                                                   bool &get_data)
{
  INIT_SUCC(ret);
  ObRowkey rowkey(input_obj, data_table_rowkey_count + 1);
  
  if (OB_FAIL(add_key_ranges(table_id, rowkey, scan_param))) {
    LOG_WARN("failed to set vid id key", K(ret));
  } else if (OB_FAIL(iter_table_rescan(scan_param, iter))) {
    LOG_WARN("failed to recan vid id scan param.", K(ret));
  } else {
    blocksstable::ObDatumRow *datum_row = nullptr;
    storage::ObTableScanIterator *scan_iter = dynamic_cast<storage::ObTableScanIterator *>(iter);
    
    if (OB_ISNULL(scan_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to cast to vid iter.", K(ret));
    } else if (OB_FAIL(scan_iter->get_next_row(datum_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next row from next table.", K(ret));
      } else {
        output_vec_obj.reset();
        ret = OB_SUCCESS;
      }
    } else if (datum_row->get_column_count() < EMBEDDED_TABLE_BASE_COLUMN_CNT + data_table_rowkey_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get row column cnt invalid.", K(ret), K(extra_column_count), K(datum_row->get_column_count()));
    } else {
      if (extra_column_count > 0 && OB_FAIL(get_extra_info_objs(scan_param, allocator, extra_column_count, datum_row, output_extra_info_objs, 0))) {
        LOG_WARN("failed to get extra info.", K(ret), K(extra_column_count), K(datum_row->storage_datums_[extra_column_count]));
      } else {
        char *copy_str = nullptr;
        ObString vector = datum_row->storage_datums_[datum_row->get_column_count() - 1].get_string();
        if (vector.length() == 0) {
          output_vec_obj.reset();
        } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(
                &allocator, ObLongTextType, CS_TYPE_BINARY, vector))) {
          LOG_WARN("failed to strip lob header for embedded vector", K(ret), K(vector));
        } else {
          int64_t size = vector.length();
          if (size == 0) {
            output_vec_obj.reset();
          } else if (OB_ISNULL(copy_str = static_cast<char *>(allocator.alloc(sizeof(char) * size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocator.", K(ret));
          } else {
            memcpy(copy_str, vector.ptr(), size);
            output_vec_obj.reset();
            output_vec_obj.set_string(ObVarcharType, copy_str, size);
            get_data = true;
          }
        }
      }
    }
  }

  return ret;
}

int64_t ObPluginVectorIndexUtils::get_extra_column_count(const schema::ObTableParam &table_param,
                                                         schema::ObIndexType type)
{
  int64_t extra_column_count = 0;
  // Note. if DELTA_BUFFER table change table definition, must change this!
  if (type == INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL) {
    extra_column_count = table_param.get_read_info().get_schema_column_count() -
                         ObVecIndexBuilderUtil::OB_VEC_DELTA_BUFFER_TABLE_INDEX_PK_COL_CNT -
                         1;  // index_rowkey_column_cnt + common_col_cnt. The non-primary key columns of
                             // delta_buffer_table and index_id_table are 1.
  } else if (type == INDEX_TYPE_VEC_INDEX_ID_LOCAL) {
    extra_column_count = table_param.get_read_info().get_schema_column_count() -
                         ObVecIndexBuilderUtil::OB_VEC_INDEX_ID_TABLE_INDEX_PK_COL_CNT - 1;
  } else if (type == INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL) {
    extra_column_count =
        table_param.get_read_info().get_schema_column_count() -
        ObVecIndexBuilderUtil::OB_VEC_INDEX_SNAPSHOT_DATA_TABLE_INDEX_PK_COL_CNT -
        3;  // index_rowkey_column_cnt + common_col_cnt ,  The non-primary key columns of snapshot_data is 3
  } else if (type == INDEX_TYPE_HYBRID_INDEX_LOG_LOCAL) {
    extra_column_count =
        table_param.get_read_info().get_schema_column_count() -
        ObVecIndexBuilderUtil::OB_HYBRID_VEC_LOG_TABLE_INDEX_PK_COL_CNT -
        1;  // index_rowkey_column_cnt + common_col_cnt ,  The non-primary key columns of log table is 1
  } else if (type == INDEX_TYPE_HYBRID_INDEX_EMBEDDED_LOCAL) {
    extra_column_count =
        table_param.get_read_info().get_schema_column_count() -
        ObVecIndexBuilderUtil::OB_HYBRID_VEC_EMBEDDED_TABLE_INDEX_PK_COL_CNT -
        1;  // index_rowkey_column_cnt + common_col_cnt ,  The non-primary key columns of embedded table is 1
  } else if (type == INDEX_TYPE_IS_NOT) {
    extra_column_count = table_param.get_read_info().get_schema_rowkey_count();
  }
  return extra_column_count;
}

int ObPluginVectorIndexUtils::get_extra_column_count(
  ObPluginVectorIndexAdaptor &adapter,
  int &column_count)
{
  INIT_SUCC(ret);
  ObSchemaGetterGuard schema_guard;
  uint64_t tenant_id = adapter.get_tenant_id();
  const ObTableSchema *delta_buffer_schema = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService *schema_service = MTL(schema::ObTenantSchemaService*)->get_schema_service();
  int64_t extra_info_actual_size = 0;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema manager", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, adapter.get_inc_table_id(), delta_buffer_schema))) {
    LOG_WARN("failed to get table schema by index id.", K(ret), K(tenant_id), K(adapter.get_inc_table_id()));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, adapter.get_data_table_id(), table_schema))) {
    LOG_WARN("failed to get data table scheam.", K(ret), K(adapter.get_data_table_id()));
  } else if (OB_ISNULL(delta_buffer_schema) || OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid index table schema.", K(ret), KP(delta_buffer_schema), KP(table_schema));
  } else if (OB_FAIL(adapter.get_extra_info_actual_size(extra_info_actual_size))) {
    LOG_WARN("failed to get extra info actual size.", K(ret));
  } else if (extra_info_actual_size > 0) {
    ObSEArray<uint64_t, 4> extra_column_ids;
    if (OB_FAIL(ObVectorIndexUtil::get_extra_info_column_id(*table_schema, *delta_buffer_schema, extra_column_ids))) {
      LOG_WARN("failed to get extra info column id.", K(ret));
    } else {
      column_count = extra_column_ids.count();
    }
  }

  return ret;
}

int ObPluginVectorIndexUtils::get_data_table_out_column_id(
  ObSEArray<uint64_t, 4> &vector_column_ids,
  uint64_t incr_index_table_id,
  uint64_t data_table_id,
  uint64_t tenant_id,
  ObPluginVectorIndexAdaptor *adapter)
{
  INIT_SUCC(ret);
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *delta_buffer_schema = nullptr;
  const ObTableSchema *table_schema = nullptr;
  ObMultiVersionSchemaService *schema_service = MTL(schema::ObTenantSchemaService*)->get_schema_service();
  int64_t extra_info_actual_size = 0;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), KP(schema_service));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("failed to get schema manager", K(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, incr_index_table_id, delta_buffer_schema))) {
    LOG_WARN("failed to get table schema by index id.", K(ret), K(tenant_id), K(incr_index_table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, table_schema))) {
    LOG_WARN("failed to get data table scheam.", K(ret), K(data_table_id));
  } else if (OB_ISNULL(delta_buffer_schema) || OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid index table schema.", K(ret), KP(delta_buffer_schema), KP(table_schema));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_id(*table_schema, *delta_buffer_schema, vector_column_ids))) {
    LOG_WARN("failed to get vector index column id.", K(ret));
  } else if (vector_column_ids.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get vector column id count invalid.", K(ret), K(vector_column_ids.count()));
  } else if (OB_FAIL(adapter->get_extra_info_actual_size(extra_info_actual_size))) {
    LOG_WARN("failed to get extra info actual size.", K(ret));
  } else if (!adapter->get_is_need_vid()) {
    if (extra_info_actual_size > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("extra info actual size is not 0 for table without pk.", K(ret), K(extra_info_actual_size));
    } else if (OB_FAIL(table_schema->get_rowkey_column_ids(vector_column_ids))){
      LOG_WARN("failed to get pk increment column id.", K(ret));
    }
  } else if (extra_info_actual_size > 0) {
    adapter->set_extra_column_count(0);
    ObSEArray<uint64_t, 4> extra_column_ids;
    if (OB_FAIL(ObVectorIndexUtil::get_extra_info_column_id(*table_schema, *delta_buffer_schema, extra_column_ids))) {
      LOG_WARN("failed to get extra info column id.", K(ret));
    } else if (extra_column_ids.count() > 0) {
      adapter->set_extra_column_count(extra_column_ids.count());
      for (int i = 0; OB_SUCC(ret) && i < extra_column_ids.count(); ++i) {
        if (OB_FAIL(vector_column_ids.push_back(extra_column_ids.at(i)))) {
          LOG_WARN("failed to push back extra column id.", K(ret), K(extra_column_ids.at(i)));
        }
      }
    }
  } 

  return ret;
}

int ObPluginVectorIndexUtils::read_vector_info(ObPluginVectorIndexAdaptor *adapter,
                                               ObIAllocator &allocator,
                                               ObLSID &ls_id,
                                               SCN target_scn,
                                               ObVectorQueryAdaptorResultContext &ada_ctx,
                                               ObVecIndexAsyncTaskCtx *task_ctx,
                                               const common::ObTabletID &mem_sync_tablet_id)
{
  INIT_SUCC(ret);
  ObPluginVectorIndexMgr *ls_index_mgr = nullptr;
  const bool is_hybrid_vector = adapter->is_hybrid_index();
  uint64_t vid_id_table_table_id = adapter->get_vid_rowkey_table_id();
  uint64_t data_table_table_id = is_hybrid_vector? adapter->get_embedded_table_id(): adapter->get_data_table_id();
  schema::ObTableParam vid_table_param(allocator);
  schema::ObTableParam data_table_param(allocator);
  common::ObNewRowIterator *vid_id_iter = nullptr;
  common::ObNewRowIterator *data_iter = nullptr;
  schema::ObIndexType type = INDEX_TYPE_VEC_VID_ROWKEY_LOCAL;
  schema::ObIndexType data_table_type = is_hybrid_vector? INDEX_TYPE_HYBRID_INDEX_EMBEDDED_LOCAL: INDEX_TYPE_IS_NOT;
  ObObj *output_vec_obj = nullptr;
  ObVecExtraInfoObj* output_extra_info_obj = nullptr;
  int extra_column_count = 0;
  int64_t loop_cnt = 0;
  ObAccessService *tsc_service = MTL(ObAccessService *);

  SMART_VARS_2((storage::ObTableScanParam, vid_id_scan_param), 
               (storage::ObTableScanParam, data_scan_param)) {
    ObArenaAllocator vid_id_scan_allocator("VecIdxTaskSC1", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObArenaAllocator data_scan_allocator("VecIdxTaskSC2", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObArenaAllocator batch_temp_allocator("VecIdxTaskSC3", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    uint32_t alloc_size = (ada_ctx.get_count() > ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) 
                          ? ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE
                          : ada_ctx.get_count(); 
    if (ada_ctx.get_count() == 0) {
      // do noting
    } else if (OB_ISNULL(output_vec_obj = static_cast<ObObj *>(allocator.alloc(sizeof(ObObj) * alloc_size)))) { // use lots of memory
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc mem.", K(ret));
    } else if (adapter->get_is_need_vid() && OB_FAIL(read_local_tablet(ls_id,
                                        adapter, 
                                        target_scn, 
                                        type,
                                        allocator,
                                        vid_id_scan_allocator,
                                        vid_id_scan_param,
                                        vid_table_param,
                                        vid_id_iter))) {
      LOG_WARN("failed to read vid id table local tablet.", K(ret));
    } else if (OB_FAIL(read_local_tablet(ls_id, 
                                        adapter,
                                        target_scn, 
                                        data_table_type,
                                        allocator,
                                        data_scan_allocator,
                                        data_scan_param,
                                        data_table_param,
                                        data_iter,
                                        nullptr,
                                        is_hybrid_vector))) {
      LOG_WARN("failed to read data table local tablet.", K(ret));
    } else if (OB_FAIL(get_extra_column_count(*adapter, extra_column_count))) {
      LOG_WARN("failed to get extra column count", K(ret), KPC(adapter));
    } else if (extra_column_count > 0) {
      if (OB_ISNULL(output_extra_info_obj = static_cast<ObVecExtraInfoObj *>(allocator.alloc(sizeof(ObVecExtraInfoObj) * extra_column_count * alloc_size)))) { // use lots of memory
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc mem.", K(ret));
      }
    } 

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(try_get_ls_index_mgr(ls_id, ls_index_mgr))) {
      LOG_WARN("fail to try get ls index mgr", K(ret), K(ls_id));
    } else if (OB_ISNULL(ls_index_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls index mgr is null when reading vector info", K(ret), K(ls_id));
    } else if (adapter->get_is_need_vid()) {
      bool get_data = false;
      void *buf = nullptr;
      ObObj *obj_ptr =  nullptr;
      int32_t data_table_rowkey_count = vid_table_param.get_output_projector().count() - 1;
      LOG_INFO("data_table_rowkey_count", K(data_table_rowkey_count));
      if (data_table_rowkey_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get data table rowkey count invalid.", K(ret), K(data_table_rowkey_count));
      } else {
        if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * (data_table_rowkey_count + 1)))) { // use lots of memory
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc mem.", K(ret), K(data_table_rowkey_count));
        } else {
          obj_ptr = new (buf) ObObj[data_table_rowkey_count + 1];
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < ada_ctx.get_count() && !adapter->is_need_cancel_task() && !ls_index_mgr->is_ls_destroying(); j += ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) {
        batch_temp_allocator.reuse();
        int64_t vec_cnt = ada_ctx.get_vec_cnt();
        for (int64_t i = 0; OB_SUCC(ret) && i < vec_cnt; i++) {
          vid_id_scan_param.key_ranges_.pop_back();
          data_scan_param.key_ranges_.pop_back();
          obj_ptr[data_table_rowkey_count].set_int(ada_ctx.get_vids()[i+j].get_int());
          if (OB_FAIL(read_object_from_vid_rowkey_table_iter(&(ada_ctx.get_vids()[i+j]), 
                                                  vid_id_table_table_id, 
                                                  vid_id_scan_param, 
                                                  vid_id_iter, 
                                                  type, 
                                                  batch_temp_allocator, 
                                                  obj_ptr,
                                                  data_table_rowkey_count))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to read obj from 2nd table.", K(ret));
            } else {
              ret = OB_SUCCESS; // read next vid
              output_vec_obj[i].reset();
              if (OB_NOT_NULL(output_extra_info_obj)) {
                for (int64_t extra_idx = 0; OB_SUCC(ret) && extra_idx < extra_column_count; ++extra_idx) {
                  output_extra_info_obj[i * extra_column_count + extra_idx].reset();
                }
              }
            }
          } else if (is_hybrid_vector && OB_FAIL(read_object_from_embedded_table_iter(obj_ptr,
                                                                                       data_table_rowkey_count,
                                                                                       data_table_table_id, 
                                                                                       data_scan_param, 
                                                                                       data_iter, 
                                                                                       batch_temp_allocator,
                                                                                       output_vec_obj[i],
                                                                                       extra_column_count,
                                                                                       OB_NOT_NULL(output_extra_info_obj) ? &(output_extra_info_obj[i * extra_column_count]) : nullptr,
                                                                                       get_data))) {
            LOG_WARN("failed to read obj from embedded table.", K(ret));
          } else if (!is_hybrid_vector && OB_FAIL(read_object_from_data_table_iter(obj_ptr,
                                                              data_table_rowkey_count,
                                                              data_table_table_id,
                                                              data_scan_param,
                                                              data_iter,
                                                              INDEX_TYPE_IS_NOT,
                                                              batch_temp_allocator,
                                                              output_vec_obj[i],
                                                              extra_column_count,
                                                              OB_NOT_NULL(output_extra_info_obj) ? &(output_extra_info_obj[i * extra_column_count]) : nullptr,
                                                              get_data))) {
            LOG_WARN("failed to read obj from data table.", K(ret));
          }
          DEBUG_SYNC(BEFORE_VEC_UTILS_GET_DATA_CANCELLED);
          CHECK_TASK_CANCELLED_IN_PROCESS_WITH_TABLET(ret, loop_cnt, 20, task_ctx, mem_sync_tablet_id);
        }

        if (OB_FAIL(ret) && ret != OB_ITER_END) {
        } else if (adapter->is_need_cancel_task()
                   || ls_index_mgr->is_ls_destroying()) {
          // if it's the error code from iter_end and the task needs to be canceled at this moment, then override it.
          ret = OB_CANCELED;
          LOG_INFO("task is canceled",
                   K(ls_id),
                   "adapter_cancel", adapter->is_need_cancel_task(),
                   "mgr_ls_destroying", ls_index_mgr->is_ls_destroying(),
                   KPC(task_ctx));
        } else {
          ret = OB_SUCCESS;
        }

        if (OB_SUCC(ret)) {
          ada_ctx.set_vectors(output_vec_obj);
          ada_ctx.set_extra_infos(output_extra_info_obj);
          if (OB_FAIL(adapter->complete_delta_buffer_table_data(&ada_ctx))) {
            LOG_WARN("failed to complete delta buffer", KR(ret));
          } else {
            // do nothing, ada_ctx.do_next_batch already called in complete_delta_buffer_table_data
          }
        }
      }
    } else if (!adapter->get_is_need_vid()) {
      bool get_data = false;
      int32_t data_table_rowkey_count = 1;
      for (int64_t j = 0; OB_SUCC(ret) && j < ada_ctx.get_count() && !adapter->is_need_cancel_task() && !ls_index_mgr->is_ls_destroying(); j += ObVectorParamData::VI_PARAM_DATA_BATCH_SIZE) {
        batch_temp_allocator.reuse();
        int64_t vec_cnt = ada_ctx.get_vec_cnt();
        for (int64_t i = 0; OB_SUCC(ret) && i < vec_cnt; i++) {
          data_scan_param.key_ranges_.pop_back();
          ObObj *input_obj = &(ada_ctx.get_vids()[i+j]);
          input_obj->meta_.set_uint64();
          if (!is_hybrid_vector && OB_FAIL(read_object_from_data_table_iter(input_obj,
                                                       data_table_rowkey_count,
                                                       data_table_table_id,
                                                       data_scan_param,
                                                       data_iter,
                                                       INDEX_TYPE_IS_NOT,
                                                       batch_temp_allocator,
                                                       output_vec_obj[i],
                                                       extra_column_count,
                                                       nullptr,
                                                       get_data))) {
            LOG_WARN("failed to read obj from data table.", K(ret));
          } else if (is_hybrid_vector && OB_FAIL(read_object_from_embedded_table_iter(input_obj,
                                                                                       0,
                                                                                       data_table_table_id, 
                                                                                       data_scan_param, 
                                                                                       data_iter, 
                                                                                       batch_temp_allocator,
                                                                                       output_vec_obj[i],
                                                                                       extra_column_count,
                                                                                       OB_NOT_NULL(output_extra_info_obj) ? &(output_extra_info_obj[i * extra_column_count]) : nullptr,
                                                                                       get_data))) {
            LOG_WARN("failed to read obj from embedded table.", K(ret));
          }
          DEBUG_SYNC(BEFORE_VEC_UTILS_GET_DATA_WITHOUT_VID_CANCELLED);
          CHECK_TASK_CANCELLED_IN_PROCESS_WITH_TABLET(ret, loop_cnt, 20, task_ctx, mem_sync_tablet_id);
        }

        if (OB_FAIL(ret) && ret != OB_ITER_END) {
        } else if (adapter->is_need_cancel_task()
                   || ls_index_mgr->is_ls_destroying()) {
          // if it's the error code from iter_end and the task needs to be canceled at this moment, then override it.
          ret = OB_CANCELED;
          LOG_INFO("task is canceled",
                   K(ls_id),
                   "adapter_cancel", adapter->is_need_cancel_task(),
                   "mgr_ls_destroying", ls_index_mgr->is_ls_destroying(),
                   KPC(task_ctx));
        } else {
          ret = OB_SUCCESS;
        }

        if (OB_SUCC(ret)) {
          ada_ctx.set_vectors(output_vec_obj);
          if (OB_FAIL(adapter->complete_delta_buffer_table_data(&ada_ctx))) {
            LOG_WARN("failed to complete delta buffer", KR(ret));
          }
        }
      }
    }
    LOG_INFO("memdata sync scan_allocator_usage", 
      K(vid_id_scan_allocator.used()), K(vid_id_scan_allocator.total()),
      K(data_scan_allocator.used()), K(data_scan_allocator.total()),
      K(batch_temp_allocator.used()), K(batch_temp_allocator.total()));
    if (OB_SUCC(ret)) {
      LOG_INFO("refresh memdata vector done",
        K(ls_id), K(target_scn), K(ada_ctx.get_count()), K(ada_ctx.get_curr_idx()),
        K(adapter->get_incr_vsag_mem_used()), K(adapter->get_snap_vsag_mem_used()));
    }
  }

  if (OB_NOT_NULL(tsc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(vid_id_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(vid_id_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    vid_id_iter = nullptr;
    if (OB_NOT_NULL(data_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(data_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert data_iter failed", K(ret));
      }
    }
    vid_id_iter = nullptr;
  }

  return ret;
}

// debug interface, remove later
int ObPluginVectorIndexUtils::test_read_local_data(ObLSID &ls_id,
                                                   ObPluginVectorIndexAdaptor *adapter,
                                                   ObIndexType index_type,
                                                   SCN target_scn,
                                                   ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  storage::ObTableScanParam scan_param;
  schema::ObTableParam table_param(allocator);
  common::ObNewRowIterator *table_iter = nullptr;
  ObAccessService *tsc_service = MTL(ObAccessService *);

  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid adapter", K(ret), KPC(adapter));
  } else if (OB_FAIL(read_local_tablet(ls_id,
                                       adapter,
                                       target_scn,
                                       index_type,
                                       allocator,
                                       allocator,
                                       scan_param,
                                       table_param,
                                       table_iter))) {
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(index_type), KPC(adapter));
  } else {
    ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(table_iter);
    bool read_finish = false;
    int row_cnt = 0;
    while(OB_SUCC(ret) && !read_finish) {
      blocksstable::ObDatumRow* datum_row = nullptr;
      if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
        if (OB_ITER_END == ret) {
          LOG_INFO("dump local read finished", K(row_cnt), K(index_type));
          read_finish = true;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("dump local read fail to get next row", KR(ret), K(row_cnt), K(index_type));
        }
      } else if (FALSE_IT(row_cnt++)) { 
      } else if (OB_ISNULL(datum_row)) {
        ret = OB_ERR_NULL_VALUE;
        LOG_WARN("dump local read row is null.", K(ret), K(index_type));
      } else {
        // print for debug
        LOG_INFO("dump local read row", K(row_cnt), K(index_type), KPC(datum_row));
      }
    }
  }
  if (OB_NOT_NULL(table_iter) && OB_NOT_NULL(tsc_service)) {
    int tmp_ret = tsc_service->revert_scan_iter(table_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert test table_iter failed", K(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::try_sync_vbitmap_memdata(ObLSID &ls_id,
                                                        ObPluginVectorIndexAdaptor *adapter,
                                                        SCN &target_scn,
                                                        ObIAllocator &allocator,
                                                        ObVectorQueryAdaptorResultContext &ada_ctx,
                                                        ObVecIndexAsyncTaskCtx *task_ctx,
                                                        const common::ObTabletID &mem_sync_tablet_id)
{
  int ret = OB_SUCCESS;
  schema::ObIndexType index_type = INDEX_TYPE_VEC_INDEX_ID_LOCAL;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  common::ObNewRowIterator *index_id_iter = nullptr;
  storage::ObTableScanParam vbitmap_scan_param;
  schema::ObTableParam vbitmap_table_param(allocator);

  if (OB_FAIL(read_local_tablet(ls_id,
                                adapter,
                                target_scn,
                                index_type,
                                allocator,
                                allocator,
                                vbitmap_scan_param,
                                vbitmap_table_param,
                                index_id_iter))) { // read_local_tablet 4rd aux index get rowkey, backword
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(index_type), KPC(adapter));
  } else if (OB_FAIL(adapter->check_index_id_table_readnext_status(
                 &ada_ctx, index_id_iter, target_scn, task_ctx, mem_sync_tablet_id))) {
    LOG_WARN("fail to check and sync vbitmap.", KR(ret));
  } // ToDo: may also need to sync vector to incr memdata

  if (OB_NOT_NULL(index_id_iter) && OB_NOT_NULL(tsc_service)) {
    int tmp_ret = tsc_service->revert_scan_iter(index_id_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert index_id_iter failed", K(ret));
    }
    index_id_iter = nullptr;
  }

  return ret;
}

int ObPluginVectorIndexUtils::get_snap_index_visible_row_iter(ObAccessService *tsc_service,
                                                              ObLSID &ls_id,
                                                              ObPluginVectorIndexAdaptor *adapter,
                                                              SCN &target_scn,
                                                              ObIAllocator &allocator,
                                                              storage::ObTableScanParam &snapshot_scan_param,
                                                              schema::ObTableParam &snapshot_table_param,
                                                              common::ObNewRowIterator *&snapshot_idx_iter,
                                                              blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool need_rescan = false;
  if (!ls_id.is_valid() || OB_ISNULL(tsc_service) || OB_ISNULL(adapter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), KP(tsc_service), KP(adapter));
  } else if (OB_FAIL(read_local_tablet(ls_id, adapter, target_scn,
                                INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                allocator,
                                allocator,
                                snapshot_scan_param,
                                snapshot_table_param,
                                snapshot_idx_iter))) { // read_local_tablet 5th aux index get rowkey
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id));
  } else if (OB_FAIL(check_snapshot_iter_need_rescan(snapshot_idx_iter, need_rescan, row))) {
    LOG_WARN("fail to check snapshot iter need rescan", K(ret));
  } else if (need_rescan) {
    row = nullptr;
    snapshot_table_param.reset();
    if (OB_NOT_NULL(snapshot_idx_iter) && OB_NOT_NULL(tsc_service)) {
      int tmp_ret = tsc_service->revert_scan_iter(snapshot_idx_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert snapshot_idx_iter failed", K(ret));
      }
      snapshot_idx_iter = nullptr;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (need_rescan) {
    if (OB_FAIL(read_local_tablet(ls_id, adapter, target_scn,
                                INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                allocator,
                                allocator,
                                snapshot_scan_param,
                                snapshot_table_param,
                                snapshot_idx_iter,
                                nullptr, /*out_column_ids*/
                                false, /*need_all_column*/
                                true /*is_reverse_scan*/))) { // read_local_tablet 5th aux index get rowkey
      LOG_WARN("fail to read local tablet", KR(ret), K(ls_id));
    } else if (OB_FAIL(check_snapshot_iter_need_rescan(snapshot_idx_iter, need_rescan, row))) {
      LOG_WARN("fail to check snapshot iter need rescan", K(ret));
    } else if (need_rescan) {
      row = nullptr;
      LOG_INFO("revert scan table 5, but still no visible row, return row will be null", K(ret));
    }
  }
  return ret;
}

// deep copy
int ObPluginVectorIndexUtils::get_snap_index_visible_row_key(ObLSID &ls_id,
                                                             ObPluginVectorIndexAdaptor *adapter,
                                                             share::SCN &target_scn,
                                                             ObIAllocator &allocator,
                                                             ObString &row_key,
                                                             bool &is_meta_data,
                                                             int64_t &meta_scn)
{
  int ret = OB_SUCCESS;
  
  ObAccessService *tsc_service = MTL(ObAccessService *);
  ObTableScanIterator *table_scan_iter = nullptr;
  common::ObNewRowIterator *snapshot_idx_iter = nullptr;
  storage::ObTableScanParam snapshot_scan_param;
  schema::ObTableParam snapshot_table_param(allocator);
  blocksstable::ObDatumRow *row = nullptr;

  if (OB_ISNULL(tsc_service) || OB_ISNULL(adapter) || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexpected nullptr", K(ret), KP(tsc_service), KP(adapter), K(ls_id));
  } else if (OB_FAIL(ObPluginVectorIndexUtils::get_snap_index_visible_row_iter(tsc_service, ls_id, 
                                                                adapter, 
                                                                target_scn, 
                                                                allocator, 
                                                                snapshot_scan_param, 
                                                                snapshot_table_param,
                                                                snapshot_idx_iter, 
                                                                row))) {
    LOG_WARN("fail to get snap inde visible row iter", K(ret));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(snapshot_idx_iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_scan_iter is null", K(ret));
  } else if (OB_NOT_NULL(row) && row->storage_datums_[0].get_string().suffix_match("_meta_data")) {
    ObString meta_data = row->storage_datums_[1].get_string();
    is_meta_data = true;
    if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&allocator, ObLongTextType, meta_data, nullptr))) {
      LOG_WARN("read meta data fail", K(ret));
    } else if (OB_FAIL(ObVectorIndexMeta::get_meta_scn(meta_data, meta_scn))) {
      LOG_WARN("get meta scn fail", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator, meta_data, row_key))) {
      LOG_WARN("copy string fail", K(ret), K(meta_data.length()));
    }      
  } else if (OB_NOT_NULL(row) && OB_FAIL(ob_write_string(allocator, row->storage_datums_[0].get_string(), row_key))) {
    LOG_WARN("failed to write string", K(ret), K(row->storage_datums_[0].get_string()));
  } else {
    LOG_DEBUG("get snap index visible row key prefix", K(ret), K(row_key));
  }

  if (OB_NOT_NULL(tsc_service)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(snapshot_idx_iter)) {
      tmp_ret = tsc_service->revert_scan_iter(snapshot_idx_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert vid_id_iter failed", K(ret));
      }
    }
    snapshot_idx_iter = nullptr;
  }

  return ret;
}

static int try_reuse_single_base_seg_from_old_adapter(
    ObPluginVectorIndexAdaptor *new_adapter,
    ObPluginVectorIndexAdaptor *old_adapter,
    const ObString &row_key)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_adapter) || OB_ISNULL(old_adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_adapter or old_adapter is null", K(ret), KP(new_adapter), KP(old_adapter));
  } else if (row_key.empty()) {
    LOG_INFO("row key is empty, so can not reuse", K(row_key));
  } else {
    ObVecIdxSnapshotDataHandle &new_snap = new_adapter->get_snap_data();
    const ObVecIdxSnapshotDataHandle &old_snap = old_adapter->get_snap_data();
    if (!old_snap.is_valid()) {
      LOG_INFO("old snap data may be not init, so can not reuse", KPC(old_adapter));
    } else if (new_snap->meta_.bases_.count() > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new snap data already has base seg, so can not reuse", K(ret), KPC(new_adapter));
    } else {
      TCRLockGuard snap_mem_lock_guard(old_snap->mem_data_rwlock_);
      ObVectorIndexMeta &new_meta = new_snap->meta_;
      const ObVectorIndexMeta &old_meta = old_snap->meta_;
      bool found = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.bases_.count() && !found; ++i) {
        const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.bases_.at(i);
        if (old_seg_meta.segment_handle_.is_valid() && (0 == old_seg_meta.start_key_.compare(row_key))) {
          if (OB_FAIL(new_meta.bases_.push_back(old_seg_meta))) {
            LOG_WARN("failed to add base seg meta", K(ret), K(old_seg_meta));
          } else {
            found = true;
            LOG_INFO("reuse single base seg from old adapter", K(row_key), K(old_seg_meta));
          }
        } else {
          LOG_INFO("current segment is not matched", K(row_key), K(old_seg_meta));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::create_refresh_adaptor(
    ObLSID &ls_id,
    ObPluginVectorIndexService *vector_index_service,
    ObPluginVectorIndexAdaptor *old_adapter,
    ObPluginVectorIndexAdaptor *&new_adapter,
    void *&adpt_buff,
    const ObVectorIndexParam *schema_index_param,
    const ObString &schema_param_str)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *vec_idx_mgr = nullptr;
  if (OB_ISNULL(vector_index_service) || OB_ISNULL(old_adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(vector_index_service), KPC(old_adapter));
  } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(
                 ls_id, vec_idx_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector index ls mgr not exist", K(ret), K(ls_id));
    } else {
      LOG_WARN("fail to get vector index ls mgr", KR(ret));
    }
  } else if (OB_ISNULL(vec_idx_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret));
  } else if (OB_ISNULL(adpt_buff = vector_index_service->get_adaptor_allocator().alloc(
                 sizeof(ObPluginVectorIndexAdaptor)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for vector index adapter", KR(ret));
  } else {
    new_adapter = new(adpt_buff)ObPluginVectorIndexAdaptor(
        &vector_index_service->get_adaptor_allocator(),
        vec_idx_mgr->get_memory_context(), old_adapter->get_tenant_id());
    if (OB_FAIL(new_adapter->copy_meta_info(*old_adapter))) {
      LOG_WARN("failed to copy meta info", K(ret));
    } else if (OB_NOT_NULL(schema_index_param)
               && OB_FAIL(new_adapter->update_index_param(*schema_index_param, schema_param_str))) {
      LOG_WARN("failed to update refresh adaptor params", K(ret), K(schema_param_str));
    } else if (OB_FAIL(new_adapter->init(
                   vec_idx_mgr->get_memory_context(), vec_idx_mgr->get_all_vsag_use_mem()))) {
      LOG_WARN("failed to init adpt.", K(ret));
    } else {
      LOG_INFO("[VECTOR INDEX ADAPTOR] clone adaptor success for refresh",
          KP(new_adapter), KP(old_adapter), KPC(schema_index_param), K(lbt()));
    }
  }
  return ret;
}

/**
 * Recalculate the earliest async-query snapshot supported by persisted vector
 * visibility. Tables without async indexes clear the value.
 *
 * @param ls_id Log stream containing the vector-index tablet.
 * @param adapter Adaptor whose async-query barrier will be updated.
 * @param has_new_vector_visibility Whether vBitmap visibility advanced in memory.
 * @return OB_SUCCESS on success; otherwise an error code.
 */
int ObPluginVectorIndexUtils::refresh_async_query_scn(
    const ObLSID &ls_id,
    ObPluginVectorIndexAdaptor *adapter,
    const bool has_new_vector_visibility)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *data_schema = nullptr;
  ObSchemaGetterGuard schema_guard;
  bool has_async_target = false;
  SCN published_scn;
  ObLSHandle ls_handle;
  storage::ObLSService *ls_service = MTL(storage::ObLSService *);
  storage::ObAsyncIndexService *async_index_service =
      MTL(storage::ObAsyncIndexService *);
  if (OB_ISNULL(adapter) || OB_ISNULL(ls_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(adapter), KP(ls_service));
  } else if (!adapter->get_snap_data().is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snapshot data is invalid", K(ret), KPC(adapter));
  } else if (adapter->get_snap_data()->meta_.bases_.count() > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected BASE count", K(ret), KPC(adapter));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance()
                         .get_tenant_schema_guard(adapter->get_tenant_id(), schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), KPC(adapter));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 adapter->get_tenant_id(), adapter->get_data_table_id(), data_schema))) {
    LOG_WARN("failed to get data table schema", K(ret), KPC(adapter));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data table schema is null", K(ret), KPC(adapter));
  } else if (!data_schema->has_async_index()) {
    adapter->reset_async_query_scn();
  } else if (!has_new_vector_visibility
             && adapter->get_snap_data()->meta_.bases_.empty()
             && adapter->get_snap_data()->meta_.incrs_.empty()) {
    if (OB_FAIL(adapter->advance_async_query_scn(SCN::min_scn()))) {
      LOG_WARN("failed to advance async query scn", K(ret), KPC(adapter));
    }
  } else if (OB_ISNULL(async_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("async index service is null", K(ret), KPC(adapter));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id));
  } else {
    storage::ObAsyncIndexDepProvider &dep_provider =
        async_index_service->get_refresher().get_dep_provider();
    if (OB_FAIL(dep_provider.get_partition_min_async_published_scn(
            adapter->get_tenant_id(), ls_handle.get_ls(),
            adapter->get_data_table_id(), adapter->get_data_tablet_id(),
            has_async_target, published_scn))) {
      LOG_WARN("failed to get async index published scn", K(ret), KPC(adapter));
    } else if (!has_async_target) {
      ret = OB_EAGAIN;
      LOG_WARN("async index targets are incomplete", K(ret), KPC(adapter));
    } else {
      // The barrier is runtime-only. After reload, the current published snapshot
      // conservatively covers visibility stored in all persisted segments.
      ret = adapter->advance_async_query_scn(published_scn);
    }
  }
  return ret;
}

// schema_param_str borrows schema memory; the caller must retain schema_guard
// until the cloned adaptor has copied the parameters.
int ObPluginVectorIndexUtils::get_schema_index_params(
    ObPluginVectorIndexAdaptor &adapter,
    ObSchemaGetterGuard &schema_guard,
    ObVectorIndexParam &schema_param,
    ObString &schema_param_str,
    bool &has_schema_param)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = adapter.get_tenant_id();
  const ObTableSchema *inc_schema = nullptr;
  ObVectorIndexParam *old_param = nullptr;
  int64_t dim = 0;
  has_schema_param = false;
  schema_param_str.reset();
  if (OB_FAIL(adapter.get_hnsw_param(old_param))) {
    LOG_WARN("failed to get old adaptor params", K(ret), K(adapter));
  } else if (OB_ISNULL(old_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old adaptor params are null", K(ret), K(adapter));
  } else if (!ObVectorIndexUtil::is_hnsw_index_type(old_param->type_)) {
    // Only HNSW build params need schema alignment during refresh.
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(
                 tenant_id, schema_guard))) {
    LOG_WARN("failed to get tenant schema guard", K(ret), K(adapter));
  } else if (OB_FAIL(schema_guard.get_table_schema(
                 tenant_id, adapter.get_inc_table_id(), inc_schema))) {
    LOG_WARN("failed to get inc table schema", K(ret), K(adapter));
  } else if (OB_ISNULL(inc_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("inc table schema is null", K(ret), K(adapter));
  } else if (OB_FALSE_IT(schema_param_str = inc_schema->get_index_params())) {
  } else if (schema_param_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema index params are empty", K(ret), KPC(inc_schema));
  } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
                 schema_param_str, ObVectorIndexType::VIT_HNSW_INDEX, schema_param))) {
    LOG_WARN("failed to parse schema index params", K(ret), K(schema_param_str));
  } else if (OB_FAIL(ObVectorIndexUtil::get_vector_index_column_dim(*inc_schema, dim))) {
    LOG_WARN("failed to get vector dimension", K(ret), KPC(inc_schema));
  } else if (dim != old_param->dim_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector dimension changed during refresh", K(ret), K(dim), K(old_param));
  } else {
    has_schema_param = true;
  }
  return ret;
}

int ObPluginVectorIndexUtils::prepare_schema_index_params(
    ObPluginVectorIndexAdaptor &old_adapter,
    ObSchemaGetterGuard &schema_guard,
    ObVectorIndexParam &schema_param,
    ObString &schema_param_str,
    bool &has_schema_param,
    bool &index_params_changed,
    bool &can_reuse_incr_data)
{
  int ret = OB_SUCCESS;
  ObVectorIndexParam *old_param = nullptr;
  bool build_params_equal = true;
  bool index_params_equal = true;
  has_schema_param = false;
  index_params_changed = false;
  can_reuse_incr_data = true;

  const int tmp_ret = get_schema_index_params(
      old_adapter, schema_guard, schema_param,
      schema_param_str, has_schema_param);
  if (OB_SUCCESS != tmp_ret) {
    // Schema alignment is best-effort for compatibility.
    has_schema_param = false;
    schema_param_str.reset();
    LOG_INFO("failed to get schema index params, fallback to old adaptor params",
        K(tmp_ret), K(old_adapter));
  } else if (!has_schema_param) {
    // Non-HNSW indexes retain the original clone behavior.
  } else if (OB_FAIL(old_adapter.get_hnsw_param(old_param))) {
    LOG_WARN("failed to get old adaptor params", K(ret), K(old_adapter));
  } else if (OB_ISNULL(old_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old adaptor params are null", K(ret), K(old_adapter));
  } else if (OB_FAIL(ObVectorIndexUtil::check_build_params_equal(
                 *old_param, schema_param, build_params_equal))) {
    LOG_WARN("failed to compare schema build params", K(ret), KPC(old_param), K(schema_param));
  } else if (OB_FAIL(ObVectorIndexUtil::check_index_params_equal(
                 *old_param, schema_param, index_params_equal))) {
    LOG_WARN("failed to compare schema index params", K(ret), KPC(old_param), K(schema_param));
  } else {
    index_params_changed = !index_params_equal;
    can_reuse_incr_data = build_params_equal;
  }
  return ret;
}

int ObPluginVectorIndexUtils::try_sync_snapshot_memdata(ObLSID &ls_id,
                                                        ObPluginVectorIndexAdaptor *&adapter,
                                                        const bool create_new_adp,
                                                        SCN &target_scn,
                                                        ObIAllocator &allocator,
                                                        ObVecIndexAsyncTaskCtx *ctx,
                                                        const common::ObTabletID &mem_sync_tablet_id)
{
  int ret = OB_SUCCESS;
  common::ScopedTimer timer(common::ObMetricId::HS_VEC_HNSW_SYNC_SNAP_MEM_TIME);
  ObCostGuard sync_snapshot_cost_guard(adapter, "try_sync_snapshot_memdata", 0, 0, ObCostGuard::KNN_THRESHOLD_500MS);
  schema::ObIndexType index_type = INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vec_idx_mgr = nullptr;
  common::ObNewRowIterator *snapshot_idx_iter = nullptr;
  storage::ObTableScanParam snapshot_scan_param;
  schema::ObTableParam snapshot_table_param(allocator);
  ObPluginVectorIndexAdaptor *old_adapter = adapter;
  const bool old_async_query_scn_valid = OB_NOT_NULL(old_adapter)
      && old_adapter->get_async_query_scn().is_valid();
  ObPluginVectorIndexAdaptor *new_adapter = nullptr;
  bool is_meta_data = false;
  bool is_empty_snapshot_table = false;
  int64_t meta_scn = 0;
  ObString row_key;
  int64_t index_count = 0;
  void *adpt_buff = nullptr;
  ObSchemaGetterGuard schema_guard;
  ObVectorIndexParam schema_param;
  ObString schema_param_str;
  const ObVectorIndexParam *schema_index_param = nullptr;
  bool has_schema_param = false;
  bool can_reuse_incr_data = true;
  bool index_params_changed = false;

  if (OB_ISNULL(vector_index_service) || OB_ISNULL(old_adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, vec_idx_mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector index ls mgr not exist", KR(ret), K(ls_id));
    } else {
      LOG_WARN("fail to get vector index ls mgr", KR(ret));
    }
  } else if (OB_ISNULL(vec_idx_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid vector index ls mgr", KR(ret));
  }

  // Keep the schema guard alive until the cloned adaptor copies schema_param_str.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_schema_index_params(
                 *old_adapter, schema_guard, schema_param, schema_param_str,
                 has_schema_param, index_params_changed, can_reuse_incr_data))) {
    LOG_WARN("failed to prepare schema index params", K(ret), K(ls_id));
  } else if (has_schema_param) {
    schema_index_param = &schema_param;
    if (index_params_changed && !create_new_adp) {
      ret = OB_EAGAIN;
      LOG_WARN("index params changed but new adaptor creation is disabled", K(ret), K(ls_id));
    } else if (index_params_changed) {
      LOG_INFO("refresh adaptor params differ from schema",
          K(ls_id), KPC(old_adapter), K(schema_param),
          K(can_reuse_incr_data), K(target_scn));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(read_local_tablet(ls_id,
                                      adapter,
                                      target_scn,
                                      index_type,
                                      allocator,
                                      allocator,
                                      snapshot_scan_param,
                                      snapshot_table_param,
                                      snapshot_idx_iter))) { // read_local_tablet 5th aux index get rowkey
    LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(index_type), KPC(new_adapter));
  } else {
    blocksstable::ObDatumRow *row = nullptr;
    ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(snapshot_idx_iter);
    if (OB_FAIL(table_scan_iter->get_next_row(row))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        is_empty_snapshot_table = true;
        int64_t current_snapshot_count = 0;
        // Reuse an empty adaptor only when all index params still match schema.
        if (OB_ISNULL(old_adapter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null old adapter", K(ret), K(ls_id));
        } else if (OB_FAIL(old_adapter->get_snap_index_row_cnt_safe(current_snapshot_count))) {
          LOG_WARN("fail to get snap index number", K(ret), K(ls_id), KPC(old_adapter));
        } else if (0 == current_snapshot_count && !index_params_changed) {
          old_adapter->set_snap_data_has_complete();
          LOG_INFO("[VECTOR INDEX ADAPTOR] skip clone empty adaptor, old snap already empty",
              KP(old_adapter), K(ls_id), K(current_snapshot_count), K(lbt()));
        } else if (!create_new_adp) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("empty snapshot table requires new adapter", K(ret), K(ls_id),
              K(current_snapshot_count), KPC(adapter));
        } else if (OB_FAIL(create_refresh_adaptor(
                       ls_id, vector_index_service, adapter, new_adapter, adpt_buff,
              schema_index_param, schema_param_str))) {
          LOG_WARN("failed to create empty refresh adaptor", K(ret), K(ls_id), KPC(adapter));
        } else {
          new_adapter->set_snap_data_has_complete();
          adapter = new_adapter;
          if (OB_NOT_NULL(ctx)) {
            ctx->set_adaptor(new_adapter);
          }
          LOG_INFO("[VECTOR INDEX ADAPTOR] clone empty adaptor success for refresh",
              KP(new_adapter), KP(old_adapter), K(current_snapshot_count), K(lbt()));
        }
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_ISNULL(row) || row->get_column_count() < 2) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid row", K(ret), K(row));
    } else if (OB_FAIL(get_snap_index_visible_row_key(ls_id, adapter, target_scn, allocator, row_key, is_meta_data, meta_scn))) {
      LOG_WARN("fail to get snap index visible row iter", K(ret));
    } else if (!row_key.empty()) {
      ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(snapshot_idx_iter);
      if (index_params_changed ||
          (is_meta_data && (meta_scn > adapter->get_snapshot_scn())) ||
          (!is_meta_data && (adapter->get_snapshot_key_prefix().empty() || !row_key.prefix_match(adapter->get_snapshot_key_prefix())))) {
        if (OB_ISNULL(vector_index_service)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected nullptr", K(ret));
        } else if (create_new_adp) {
          if (OB_FAIL(create_refresh_adaptor(
              ls_id, vector_index_service, adapter, new_adapter, adpt_buff,
              schema_index_param, schema_param_str))) {
            LOG_WARN("failed to create refresh adaptor", K(ret), K(ls_id), KPC(adapter));
          } else {
            adapter = new_adapter;
          }
        } else {
          new_adapter = adapter;
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(ctx)) {
          ctx->set_adaptor(new_adapter);
        }
        CHECK_TASK_CANCELLED(ret, ctx);
        CHECK_MEM_SYNC_TABLET_CANCELLED(ret, ctx, mem_sync_tablet_id);
        if (OB_FAIL(ret)) {
        } else if (is_meta_data) {
          ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
          if (OB_FAIL(new_adapter->deserialize_snap_meta(row_key))) {
            LOG_WARN("deserialize snap meta fail", K(ret));
          } else if (OB_FAIL(try_reuse_segments_from_old_adapter(new_adapter, old_adapter))) {
            LOG_WARN("try reuse segments from old adapter fail", K(ret));
          } else if (OB_FAIL(new_adapter->get_snap_data()->load_persist_segments(new_adapter, tmp_allocator, ls_id, target_scn, ctx))) {
            LOG_WARN("load persist segments fail", K(ret));
          } else if (OB_FALSE_IT(new_adapter->get_snap_data()->set_inited())) {
          } else if (OB_FALSE_IT(new_adapter->set_snap_data_has_complete())) {
          } else if (can_reuse_incr_data
                     && new_adapter->get_snapshot_scn() == old_adapter->get_snapshot_scn()
                     && OB_FAIL(new_adapter->merge_incr_data(old_adapter))) {
            LOG_WARN("merge_incr_data fail", K(ret), KPC(old_adapter), KPC(new_adapter));
          } else if (OB_FAIL(new_adapter->get_snap_index_row_cnt(index_count))) {
            LOG_WARN("fail to get snap index row cnt", K(ret), KP(new_adapter), KP(adapter));
          }
        } else if (OB_FAIL(iter_table_rescan(snapshot_scan_param, table_scan_iter))) {
          LOG_WARN("failed to rescan", K(ret));
        } else {
          ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
          ObHNSWDeserializeCallback::CbParam param;
          param.iter_ = snapshot_idx_iter;
          param.allocator_ = &tmp_allocator;
          param.mgr_ = vec_idx_mgr;
          param.task_ctx_ = ctx;
          // ToDo: concurrency with weakread
          // name rule: TabletID_SCN_xxx_data_partn, we need TabletID_SCN
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(try_reuse_single_base_seg_from_old_adapter(
                         new_adapter, old_adapter, row_key))) {
            LOG_WARN("fail to reuse single base seg from old adapter", K(ret));
          } else if (OB_FAIL(new_adapter->deserialize_snap_data(param, row_key))) {
            LOG_WARN("deseriale snapshot data fail", K(ret));
          } else if (OB_FAIL(new_adapter->get_snap_index_row_cnt(index_count))) {
            LOG_WARN("fail to get snap index row cnt", K(ret), KP(new_adapter), KP(adapter));
          } else {
            LOG_INFO("deserialize snapshot data success", K(ret), K(row_key), KPC(new_adapter), KPC(adapter));
          }
        }
      } else {
        LOG_INFO("no need refresh snapshot memdata", K(ret), K(ls_id), K(meta_scn), K(row_key), KPC(adapter));
        adapter->set_snap_data_has_complete();
      }
    }
  }
  // check if need to refresh async_query_scn for async index query
  // async index checking process is in the ObPluginVectorIndexUtils::refresh_async_query_scn
  bool need_to_refresh = false;
  if (OB_SUCC(ret) && adapter != old_adapter) {
    const ObVecIdxSnapshotDataHandle &old_snap = old_adapter->get_snap_data();
    const ObVecIdxSnapshotDataHandle &new_snap = adapter->get_snap_data();
    if (!old_snap.is_valid() || !new_snap.is_valid()) {
      need_to_refresh = true;
    } else {
      // check if the base segments are the same
      need_to_refresh = !are_same_persisted_segments(
          old_snap->meta_.bases_, new_snap->meta_.bases_);
      if (!need_to_refresh) {
        // check if the incr segments are the same
        need_to_refresh = has_new_freeze_persist_segment(
          old_snap->meta_.incrs_, new_snap->meta_.incrs_);
      }
    }
    if (!need_to_refresh) {
      // if no base or incr segments are changed, check if the old async query scn is valid
      need_to_refresh = !old_async_query_scn_valid;
    }
  }
  // INCR_MERGE only reorganizes existing visibility and inherits the old barrier.
  // A new BASE or FREEZE_PERSIST segment introduces a new visibility boundary.
  if (OB_SUCC(ret) && OB_NOT_NULL(adapter) && adapter->get_snap_has_complete()
      && need_to_refresh
      && OB_FAIL(refresh_async_query_scn(ls_id, adapter))) {
    LOG_WARN("failed to refresh async query scn", K(ret), K(ls_id), KPC(adapter));
  }
  // free adapter memory when failed
  if ((OB_FAIL(ret) || (index_count == 0 && !is_meta_data && !is_empty_snapshot_table))
      && OB_NOT_NULL(new_adapter) && create_new_adp) {
    LOG_INFO("release new adapter memory in failure",
        K(ret), K(index_count), KP(new_adapter), K(create_new_adp), K(is_meta_data),
        K(is_empty_snapshot_table));
    if (OB_NOT_NULL(ctx)) {
      ctx->reset_adaptor();
    }
    new_adapter->~ObPluginVectorIndexAdaptor();
    vector_index_service->get_adaptor_allocator().free(adpt_buff);
    adpt_buff = nullptr;
    new_adapter = nullptr;
    adapter = nullptr;
  }
  if (OB_NOT_NULL(snapshot_idx_iter) && OB_NOT_NULL(tsc_service)) {
    int tmp_ret = tsc_service->revert_scan_iter(snapshot_idx_iter);
    if (tmp_ret != OB_SUCCESS) {
      LOG_WARN("revert snapshot_idx_iter failed", K(ret));
    }
    snapshot_idx_iter = nullptr;
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("refresh memdata snap done", KP(new_adapter), KP(adapter),
      K(ls_id), K(target_scn), K(index_count), K(is_meta_data),
      K(is_empty_snapshot_table), K(meta_scn));
  }
  return ret;
}

int ObPluginVectorIndexUtils::find_segment_in_old_meta(
    ObVectorIndexSegmentMeta &new_seg_meta,
    const ObVectorIndexMeta &old_meta)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.incrs_.count() && !found; ++i) {
    const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.incrs_.at(i);
    if (old_seg_meta.segment_handle_.is_valid() && 0 == new_seg_meta.start_key_.compare(old_seg_meta.start_key_)) {
      new_seg_meta.segment_handle_ = old_seg_meta.segment_handle_;
      new_seg_meta.blocks_cnt_ = old_seg_meta.blocks_cnt_;
      found = true;
      LOG_INFO("find segment in old adapter incrs", K(new_seg_meta), K(old_seg_meta));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < old_meta.bases_.count() && !found; ++i) {
    const ObVectorIndexSegmentMeta &old_seg_meta = old_meta.bases_.at(i);
    if (old_seg_meta.segment_handle_.is_valid() && 0 == new_seg_meta.start_key_.compare(old_seg_meta.start_key_)) {
      new_seg_meta.segment_handle_ = old_seg_meta.segment_handle_;
      new_seg_meta.blocks_cnt_ = old_seg_meta.blocks_cnt_;
      found = true;
      LOG_INFO("find segment in old adapter bases", K(new_seg_meta), K(old_seg_meta));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::try_reuse_segments_from_old_adapter(
    ObPluginVectorIndexAdaptor *new_adapter,
    ObPluginVectorIndexAdaptor *old_adapter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(new_adapter) || OB_ISNULL(old_adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new_adapter or old_adapter is null", K(ret), KP(new_adapter), KP(old_adapter));
  } else {
    ObVecIdxSnapshotDataHandle &new_snap = new_adapter->get_snap_data();
    const ObVecIdxSnapshotDataHandle &old_snap = old_adapter->get_snap_data();
    if (!new_snap.is_valid() || !old_snap.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("snap data is invalid", K(ret), K(new_snap), K(old_snap));
    } else {
      TCRLockGuard snap_mem_lock_guard(old_snap->mem_data_rwlock_);
      ObVectorIndexMeta &new_meta = new_snap->meta_;
      const ObVectorIndexMeta &old_meta = old_snap->meta_;
      for (int64_t i = 0; OB_SUCC(ret) && i < new_meta.incrs_.count(); ++i) {
        ObVectorIndexSegmentMeta &new_seg_meta = new_meta.incrs_.at(i);
        if (!new_seg_meta.segment_handle_.is_valid()) {
          if (OB_FAIL(find_segment_in_old_meta(new_seg_meta, old_meta))) {
            LOG_WARN("find segment in old adapter fail", K(ret), K(i), K(new_seg_meta.start_key_));
          }
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < new_meta.bases_.count(); ++i) {
        ObVectorIndexSegmentMeta &new_seg_meta = new_meta.bases_.at(i);
        if (!new_seg_meta.segment_handle_.is_valid()) {
          if (OB_FAIL(find_segment_in_old_meta(new_seg_meta, old_meta))) {
            LOG_WARN("find segment in old adapter fail", K(ret), K(i), K(new_seg_meta.start_key_));
          }
        }
      }
      LOG_INFO("try reuse segments from old adapter finished", K(ret), K(new_meta), K(old_meta));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::check_task_is_cancel(
    ObPluginVectorIndexTaskCtx *task_ctx, bool &is_cancel)
{
  int ret = OB_SUCCESS;
  is_cancel = false;
  if (OB_NOT_NULL(task_ctx)
      && task_ctx->task_status_ == ObVectorIndexTaskStatus::OB_TTL_TASK_CANCEL) {
    is_cancel = true;
  }
  return ret;
}

int ObPluginVectorIndexUtils::refresh_adp_from_table(
    ObLSID &ls_id,
    ObPluginVectorIndexAdaptor *&adapter,
    const bool create_new_adapter,
    SCN target_scn,
    ObIAllocator &allocator,
    ObVecIndexAsyncTaskCtx *ctx,
    const common::ObTabletID &mem_sync_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid adapter", K(ret), KPC(adapter));
  } else if (!adapter->is_ready_complete()) {
    // skip not complete adapter.
    LOG_INFO("adaptor not ready complete, skip", K(ls_id), KPC(adapter));
  } else {
    common::ObNewRowIterator *delta_buf_iter = nullptr; 
    ObAccessService *tsc_service = MTL(ObAccessService *);
    storage::ObTableScanParam inc_scan_param;
    schema::ObTableParam inc_table_param(allocator);
    int64_t extra_info_actual_size = 0;
    schema::ObIndexType delta_type = adapter->is_hybrid_index()? INDEX_TYPE_HYBRID_INDEX_LOG_LOCAL: INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL;
    DEBUG_SYNC(REFRESH_MEMDATA_BEFORE_SYNC_SNAPSHOT);

    if (OB_UNLIKELY(OB_SUCCESS != (ret = EVENT_CALL(
            ERRSIM_VEC_REFRESH_MEMDATA,
            mem_sync_tablet_id.is_valid() ? mem_sync_tablet_id.id() : 0)))) {
      LOG_WARN("ERRSIM_VEC_REFRESH_MEMDATA injected before sync snapshot",
               KR(ret), K(ls_id), K(mem_sync_tablet_id), K(target_scn));
    } else if (OB_FAIL(try_sync_snapshot_memdata(
            ls_id, adapter, create_new_adapter, target_scn, allocator, ctx, mem_sync_tablet_id))) {
      LOG_WARN("failed to refresh mem snapshots without refresh incr", KR(ret));
    }
    CHECK_TASK_CANCELLED(ret, ctx);
    CHECK_MEM_SYNC_TABLET_CANCELLED(ret, ctx, mem_sync_tablet_id);
    DEBUG_SYNC(REFRESH_MEMDATA_AFTER_SYNC_SNAPSHOT);

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(read_local_tablet(ls_id,
                                  adapter,
                                  target_scn,
                                  delta_type,
                                  allocator,
                                  allocator,
                                  inc_scan_param,
                                  inc_table_param,
                                  delta_buf_iter))) {
      LOG_WARN("fail to read local tablet", KR(ret), K(ls_id), K(delta_type), KP(adapter));
    } else if (OB_FAIL(adapter->get_extra_info_actual_size(extra_info_actual_size))) {
      LOG_WARN("fail to get extra info actual size", K(ret), KPC(adapter));
    } else {
      int64_t extra_info_column_count =
          extra_info_actual_size > 0
              ? ObPluginVectorIndexUtils::get_extra_column_count(inc_table_param, delta_type)
              : 0;
      ObArenaAllocator tmp_allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, adapter->get_tenant_id());
      ObVectorQueryAdaptorResultContext ada_ctx(adapter->get_tenant_id(), ls_id, extra_info_column_count, &allocator, &tmp_allocator);
      SCN vbitmap_scn_before_sync;
      ada_ctx.set_scn(target_scn);
      ada_ctx.set_is_refresh_adaptor(true);
      bool ls_leader = true;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObPluginVectorIndexUtils::get_ls_leader_flag(ls_id, ls_leader))) {
        LOG_WARN("fail to get ls leader flag", K(ret), K(ls_id));
      } else if (FALSE_IT(ada_ctx.set_ls_leader(ls_leader))) {
      } else if (OB_FAIL(adapter->check_delta_buffer_table_readnext_status(
                     &ada_ctx, delta_buf_iter, target_scn, ctx, mem_sync_tablet_id))) {
        LOG_WARN("fail to check_delta_buffer_table_readnext_status.", K(ret));
      }
      CHECK_TASK_CANCELLED(ret, ctx);
      CHECK_MEM_SYNC_TABLET_CANCELLED(ret, ctx, mem_sync_tablet_id);
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (OB_FAIL(adapter->get_vbitmap_start_scn(vbitmap_scn_before_sync))) {
        LOG_WARN("failed to get vbitmap scn before memdata sync", K(ret), KPC(adapter));
      } else if (OB_FAIL(try_sync_vbitmap_memdata(
                     ls_id, adapter, target_scn, allocator, ada_ctx, ctx, mem_sync_tablet_id))) {
        LOG_WARN("failed to sync vbitmap", KR(ret));
      } else {
        LOG_INFO("refresh memdata delta ready",
          K(ls_id), K(target_scn), K(ada_ctx.get_status()),
          K(ada_ctx.get_count()), K(ada_ctx.get_vec_cnt()));
      }
      if (OB_SUCC(ret)) {
        SCN vbitmap_scn_after_sync;
        if (OB_FAIL(adapter->get_vbitmap_start_scn(vbitmap_scn_after_sync))) {
          LOG_WARN("failed to get vbitmap scn after memdata sync", K(ret), KPC(adapter));
        } else if (vbitmap_scn_after_sync > vbitmap_scn_before_sync
                   && OB_FAIL(refresh_async_query_scn(ls_id, adapter, true))) {
          LOG_WARN("failed to advance async query scn after vbitmap sync",
                   K(ret), K(vbitmap_scn_before_sync), K(vbitmap_scn_after_sync),
                   KPC(adapter));
        }
      }
      if (OB_SUCC(ret) && ada_ctx.get_status() == PVQ_COM_DATA) {
        CHECK_TASK_CANCELLED(ret, ctx);
        CHECK_MEM_SYNC_TABLET_CANCELLED(ret, ctx, mem_sync_tablet_id);
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(read_vector_info(
                       adapter, allocator, ls_id, target_scn, ada_ctx, ctx, mem_sync_tablet_id))) {
          LOG_WARN("failed to read vector_info", KR(ret));
        }
      }

      if (OB_SUCC(ret) && ! ls_leader) {
        // check again if ls is follower
        bool is_leader_now = false;
        if (OB_FAIL(ObPluginVectorIndexUtils::get_ls_leader_flag(ls_id, is_leader_now))) {
          LOG_WARN("fail to get ls leader flag", K(ret), K(ls_id));
        } else if (is_leader_now) {
          adapter->reset_complete();
          LOG_INFO("ls change from follower to leader, reset complete", K(ls_id), KPC(adapter));
        }
      }
    }
    if (OB_NOT_NULL(delta_buf_iter) && OB_NOT_NULL(tsc_service)) {
      int tmp_ret = tsc_service->revert_scan_iter(delta_buf_iter);
      if (tmp_ret != OB_SUCCESS) {
        LOG_WARN("revert delta_buf_iter failed", K(tmp_ret));
      }
      delta_buf_iter = nullptr;
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_read_scn(bool is_leader, ObLSID &ls_id, SCN &target_scn)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  // ObLSWRSHandler::get_ls_weak_read_ts
  storage::ObLSService *ls_svr = MTL(storage::ObLSService*);
  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(ls_svr), K(ls_id));
  } else if (OB_FAIL(ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (is_leader && OB_FAIL(get_current_read_scn(target_scn))) {
    LOG_WARN("failed to get current read scn", K(ret), K(ls_id));
  } else if (!is_leader && FALSE_IT(target_scn = ls_handle.get_ls()->get_ls_wrs_handler()->get_ls_weak_read_ts())) {
  }
  return ret;
}

int ObPluginVectorIndexUtils::query_need_refresh_memdata(ObPluginVectorIndexAdaptor *adapter, ObLSID &ls_id, bool is_leader)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("VectorAdaptor", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  SCN target_scn;
  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(adapter), K(ls_id));
  } else {
    bool need_retry = false;
    ObVecIdxSchemaRetryReason retry_reason = ObVecIdxSchemaRetryReason::VEC_SCHEMA_RETRY_UNKNOWN;
    common::ObSpinLockGuard ctx_guard(adapter->get_reload_lock());
    if (adapter->get_reload_finish()) {
      need_retry = true;
      retry_reason = ObVecIdxSchemaRetryReason::VEC_SCHEMA_RETRY_RELOAD_ALREADY_FINISHED;
    } else if (OB_FAIL(get_read_scn(is_leader, ls_id, target_scn))) {
      LOG_WARN("failed to get weak read scn", K(ret), K(is_leader));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::refresh_memdata(ls_id, adapter, target_scn, allocator))) {
      LOG_WARN("fail to refresh adapter", K(ret));
    } else if (OB_FALSE_IT(adapter->set_reload_finish(true))) {
    } else {
      need_retry = true;
      retry_reason = ObVecIdxSchemaRetryReason::VEC_SCHEMA_RETRY_MEMDATA_REFRESHED;
    }
    if (OB_SUCC(ret) && need_retry) {
      SET_METRIC_VAL(common::ObMetricId::VEC_RETRY_REASON, static_cast<int64_t>(retry_reason));
      ret = OB_SCHEMA_EAGAIN; // sql retry
      LOG_WARN("sql retry ret_code to process_adapter_state", K(ret));
    }
  }
  return ret;
}

// Point read helper for bounded refresh; root-referenced rows are visible by write protocol.
static int read_snap_table_blob(ObPluginVectorIndexAdaptor *adaptor,
                        const ObLSID &ls_id,
                        const share::SCN &target_scn,
                        const ObString &key,
                        ObIAllocator &allocator,
                        bool &found,
                        ObString &actual_key,
                        ObString &blob_out)
{
  int ret = OB_SUCCESS;
  found = false;
  actual_key.reset();
  blob_out.reset();
  common::ObNewRowIterator *iter = nullptr;
  storage::ObTableScanParam scan_param;
  schema::ObTableParam table_param(allocator);
  ObTableScanIterator *table_scan_iter = nullptr;
  blocksstable::ObDatumRow *row = nullptr;
  if (OB_FAIL(ObPluginVectorIndexUtils::open_segment_data_iter(
      adaptor, allocator, ls_id, adaptor->get_snap_tablet_id(),
      key, key, target_scn, scan_param, table_param, iter))) {
    LOG_WARN("open snap-table point read iter fail", K(ret), K(key));
  } else if (OB_ISNULL(table_scan_iter = static_cast<ObTableScanIterator *>(iter))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snap-table scan iter is null", K(ret), KP(iter));
  } else if (OB_FAIL(table_scan_iter->get_next_row(row))) {
    if (OB_ITER_END == ret) {
      found = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get_next_row fail on snap-table point read", K(ret), K(key));
    }
  } else if (OB_ISNULL(row) || row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid snap-table row", K(ret), KPC(row));
  } else {
    ObString raw_blob = row->storage_datums_[1].get_string();
    const sql::ObDatumMeta blob_meta(ObLongTextType, CS_TYPE_BINARY, -1);
    if (OB_FAIL(ob_write_string(allocator, row->storage_datums_[0].get_string(), actual_key))) {
      LOG_WARN("copy actual_key fail on snap-table point read", K(ret), K(key));
    } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data_with_copy(
        allocator, row->storage_datums_[1], blob_meta, raw_blob))) {
      LOG_WARN("read lob fail on snap-table point read", K(ret), K(key));
    } else {
      found = true;
      blob_out = raw_blob;
    }
  }
  if (OB_NOT_NULL(iter)) {
    ObAccessService *oas = MTL(ObAccessService *);
    int tmp_ret = OB_SUCCESS;
    if (OB_NOT_NULL(oas) && OB_SUCCESS != (tmp_ret = oas->revert_scan_iter(iter))) {
      LOG_WARN("revert snap-table point read iter fail", K(tmp_ret), K(key));
    }
    iter = nullptr;
  }
  return ret;
}

struct BoundedRefreshBuildContext {
  BoundedRefreshBuildContext(ObLSID &ls_id,
                             ObPluginVectorIndexAdaptor *adapter,
                             SCN target_scn,
                             ObIAllocator &outer_allocator)
    : ls_id_(ls_id),
      adapter_(adapter),
      target_scn_(target_scn),
      outer_allocator_(outer_allocator),
      tmp_allocator_("BoundedRead", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()) {}

  ObLSID &ls_id_;
  ObPluginVectorIndexAdaptor *adapter_;
  SCN target_scn_;
  ObIAllocator &outer_allocator_;
  ObArenaAllocator tmp_allocator_;
};

static int deserialize_segment_bitmap(const ObString &blob,
                               ObIAllocator &allocator,
                               ObVectorIndexSegmentHandle &out_segment)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_FAIL(ObVectorIndexSegment::create_bitmap_only(out_segment, MTL_ID(), allocator))) {
    LOG_WARN("create bitmap-only segment fail", K(ret));
  } else if (OB_FAIL(out_segment->deserialize_meta(blob.ptr(), blob.length(), pos))) {
    LOG_WARN("deserialize segment meta fail", K(ret));
  }
  return ret;
}

static int deserialize_snap_meta_bridge(const ObString &blob,
                                 ObIAllocator &allocator,
                                 ObVectorIndexSegmentMeta &out_meta,
                                 ObVectorIndexSegmentHandle &out_segment)
{
  return ObVectorIndexSegmentMeta::deserialize_snap_meta_bridge(
      blob, MTL_ID(), allocator, out_meta, out_segment);
}

static int read_current_snap_meta(BoundedRefreshBuildContext &ctx,
                                  ObVectorIndexMeta &out_meta,
                                  int64_t &out_meta_scn,
                                  bool &out_found)
{
  int ret = OB_SUCCESS;
  out_found = false;
  out_meta_scn = 0;
  ObString meta_key;
  ObString actual_key;
  ObString blob;
  bool found = false;
  if (OB_FAIL(ObVectorIndexSegmentMeta::get_vector_index_meta_key(
      ctx.tmp_allocator_, ctx.adapter_->get_snap_tablet_id(), meta_key))) {
    LOG_WARN("build meta key fail", K(ret));
  } else if (OB_FAIL(read_snap_table_blob(ctx.adapter_, ctx.ls_id_, ctx.target_scn_,
      meta_key, ctx.tmp_allocator_, found, actual_key, blob))) {
    LOG_WARN("read meta blob fail", K(ret));
  } else if (!found) {
  } else if (!actual_key.suffix_match("_meta_data")) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expected meta_data key", K(ret), K(actual_key));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(ObVectorIndexMeta::get_meta_scn(blob, out_meta_scn))) {
      LOG_WARN("get current meta scn fail", K(ret));
    } else if (OB_FAIL(out_meta.deserialize(blob.ptr(), blob.length(), pos))) {
      LOG_WARN("deserialize current_meta fail", K(ret));
    }
    if (OB_SUCC(ret)) {
      out_found = true;
    }
  }
  return ret;
}

static int read_current_snap_meta_bridge(BoundedRefreshBuildContext &ctx,
                                         ObVectorIndexSegmentMeta &out_bridge_meta,
                                         ObVectorIndexSegmentHandle &out_bridge_segment,
                                         bool &coverage_ok)
{
  int ret = OB_SUCCESS;
  bool found = false;
  ObString snap_meta_key;
  ObString actual_key;
  ObString blob;
  coverage_ok = false;
  if (OB_FAIL(ObVectorIndexSegmentMeta::get_vector_index_snap_meta_key(
      ctx.tmp_allocator_, ctx.adapter_->get_snap_tablet_id(), snap_meta_key))) {
    LOG_WARN("build snap_meta child key fail", K(ret));
  } else if (OB_FAIL(read_snap_table_blob(ctx.adapter_, ctx.ls_id_, ctx.target_scn_,
      snap_meta_key, ctx.tmp_allocator_, found, actual_key, blob))) {
    // Storage/transaction read failures are not coverage failures and must propagate.
    LOG_WARN("read snap_meta child blob fail", K(ret), K(snap_meta_key));
  } else if (!found) {
    LOG_INFO("[VEC_INDEX][BOUNDED_READ] coverage not provable",
        "reason", "flagged snap_meta child row not found", K(snap_meta_key));
  } else if (0 != actual_key.compare(snap_meta_key)) {
    LOG_INFO("[VEC_INDEX][BOUNDED_READ] coverage not provable",
        "reason", "unexpected snap_meta child key", K(snap_meta_key), K(actual_key));
  } else {
    const int deserialize_ret = deserialize_snap_meta_bridge(
        blob, ctx.tmp_allocator_, out_bridge_meta, out_bridge_segment);
    if (OB_ALLOCATE_MEMORY_FAILED == deserialize_ret) {
      ret = deserialize_ret;
      LOG_WARN("allocate memory while deserializing snap_meta child fail", K(ret), K(blob.length()));
    } else if (OB_SUCCESS != deserialize_ret) {
      // The child is optional coverage proof. A malformed payload falls back to a full refresh.
      LOG_INFO("[VEC_INDEX][BOUNDED_READ] coverage not provable",
          "reason", "invalid snap_meta child payload", K(deserialize_ret), K(blob.length()));
    } else {
      coverage_ok = true;
    }
  }
  return ret;
}

static int fold_incr_segment_bitmaps(BoundedRefreshBuildContext &ctx,
                              const ObVectorIndexMeta &current_meta,
                              ObVectorIndexRoaringBitMap &target_bitmap,
                              bool &coverage_ok)
{
  int ret = OB_SUCCESS;
  coverage_ok = true;
  for (int64_t i = 0; OB_SUCC(ret) && coverage_ok && i < current_meta.incr_segment_count(); ++i) {
    const ObVectorIndexSegmentMeta &seg = current_meta.incr_seg_meta(i);
    if (!seg.has_segment_meta_row_) {
      coverage_ok = false;
      LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] coverage not provable",
          "reason", "legacy segment without meta row", K(i), K(seg.start_key_));
    } else {
      bool found = false;
      ObString actual_key;
      ObString seg_blob;
      if (OB_FAIL(read_snap_table_blob(ctx.adapter_, ctx.ls_id_, ctx.target_scn_,
          seg.start_key_, ctx.tmp_allocator_, found, actual_key, seg_blob))) {
        LOG_WARN("read segment blob fail", K(ret));
      } else if (!found) {
        coverage_ok = false;
        LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] coverage not provable",
            "reason", "segment meta row not found", K(seg.start_key_));
      } else if (0 != actual_key.compare(seg.start_key_)) {
        coverage_ok = false;
        LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] coverage not provable",
            "reason", "unexpected segment meta key", K(seg.start_key_), K(actual_key));
      } else {
        ObVectorIndexSegmentHandle bitmap_segment;
        const int deserialize_ret = deserialize_segment_bitmap(
            seg_blob, ctx.tmp_allocator_, bitmap_segment);
        if (OB_ALLOCATE_MEMORY_FAILED == deserialize_ret) {
          ret = deserialize_ret;
          LOG_WARN("allocate memory while deserializing persisted segment bitmap fail",
              K(ret), K(seg.start_key_));
        } else if (OB_SUCCESS != deserialize_ret) {
          // Persisted bitmap data is coverage proof, not required base data. Corruption
          // invalidates bounded-read coverage and lets the caller perform a full refresh.
          coverage_ok = false;
          LOG_INFO("[VEC_INDEX][BOUNDED_READ] coverage not provable",
              "reason", "invalid persisted segment bitmap", K(deserialize_ret), K(seg.start_key_));
        } else {
          const ObVectorIndexRoaringBitMap *seg_bitmap = bitmap_segment->get_vbitmap();
          if (OB_ISNULL(seg_bitmap)) {
            // No segment vbitmap: coverage is not provable.
            coverage_ok = false;
            LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] coverage not provable",
                "reason", "segment meta has no visibility bitmap", K(seg.start_key_));
          } else {
            lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(target_bitmap.tenant_id_, "VIBitmap"));
            if (OB_NOT_NULL(seg_bitmap->insert_bitmap_) && OB_NOT_NULL(target_bitmap.insert_bitmap_)) {
              ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_or_inplace(target_bitmap.insert_bitmap_, seg_bitmap->insert_bitmap_));
            }
            if (OB_SUCC(ret) && OB_NOT_NULL(seg_bitmap->delete_bitmap_) && OB_NOT_NULL(target_bitmap.delete_bitmap_)) {
              ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_or_inplace(target_bitmap.delete_bitmap_, seg_bitmap->delete_bitmap_));
            }
          }
        }
      }
    }
  }
  return ret;
}

static int build_bounded_overlay(BoundedRefreshBuildContext &ctx,
                                 const ObVectorIndexMeta &current_meta,
                                 int64_t loaded_base,
                                 ObVectorIndexRoaringBitMap &target_bitmap,
                                 bool &coverage_ok)
{
  int ret = OB_SUCCESS;
  coverage_ok = false;
  const int64_t cur_base_scn = current_meta.max_base_scn();

  // A matching base needs only current incremental visibility. If the base has
  // advanced, add bridge visibility before folding the current increments.
  if (loaded_base == cur_base_scn) {
    if (OB_FAIL(fold_incr_segment_bitmaps(
        ctx, current_meta, target_bitmap, coverage_ok))) {
      LOG_WARN("fold incr segment bitmaps fail", K(ret));
    }
  } else if (!current_meta.has_snap_meta_ref()) {
    LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] coverage not provable",
        "reason", "current meta has no snap_meta ref", K(loaded_base), K(cur_base_scn));
  } else {
    ObVectorIndexSegmentMeta bridge_meta;
    ObVectorIndexSegmentHandle bridge_segment;
    bool bridge_coverage_ok = false;
    if (OB_FAIL(read_current_snap_meta_bridge(
        ctx, bridge_meta, bridge_segment, bridge_coverage_ok))) {
      LOG_WARN("read current snap_meta bridge fail", K(ret));
    } else if (!bridge_coverage_ok) {
      coverage_ok = false;
    } else {
      const ObVectorIndexRoaringBitMap *bridge_bitmap =
          bridge_segment.is_valid() ? bridge_segment->get_vbitmap() : nullptr;
      ObVecIdxVBitmapDataHandle vbitmap_handle = ctx.adapter_->get_vbitmap_data();
      int64_t vbitmap_scn_val = 0;
      if (vbitmap_handle.is_valid()) {
        TCRLockGuard lock_guard(vbitmap_handle->bitmap_rwlock_);
        vbitmap_scn_val = vbitmap_handle->scn_.get_val_for_sql();
      }
      if (OB_ISNULL(bridge_bitmap)) {
        coverage_ok = false;
        LOG_INFO("[VEC_INDEX][BOUNDED_READ] coverage not provable",
            "reason", "snap_meta bridge has no vbitmap", K(loaded_base), K(cur_base_scn));
      } else if (OB_ISNULL(bridge_bitmap->insert_bitmap_)
          || OB_ISNULL(bridge_bitmap->delete_bitmap_)) {
        coverage_ok = false;
        LOG_INFO("[VEC_INDEX][BOUNDED_READ] coverage not provable",
            "reason", "snap_meta bridge has incomplete vbitmap",
            KP(bridge_bitmap->insert_bitmap_), KP(bridge_bitmap->delete_bitmap_));
      } else if (bridge_meta.scn_ != loaded_base) {
        LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] coverage not provable",
            "reason", "from_base mismatch",
            "from_base_scn", bridge_meta.scn_, K(loaded_base), K(cur_base_scn));
      } else if (vbitmap_scn_val < bridge_meta.scn_) {
        // vbitmap_data_->scn_ is the SCN of the latest actual row read from the
        // No.4 table, not a read-coverage watermark. If S0 is the last row SCN,
        // a base can advance to B0 while (S0, B0] contains no No.4 rows. After a
        // later BASE0 -> BASE1 merge writes a bridge whose from_base is B0, an
        // adaptor still on BASE0 can legitimately observe S0 < B0. Without a
        // separate covered_scn, keep coverage unproven so the caller returns
        // NOT_COVERED and refreshes the adaptor instead of reporting corruption.
        coverage_ok = false;
      } else {
        lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(target_bitmap.tenant_id_, "VIBitmap"));
        if (OB_NOT_NULL(target_bitmap.insert_bitmap_)) {
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_or_inplace(target_bitmap.insert_bitmap_, bridge_bitmap->insert_bitmap_));
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(target_bitmap.delete_bitmap_)) {
          ROARING_TRY_CATCH(roaring::api::roaring64_bitmap_or_inplace(target_bitmap.delete_bitmap_, bridge_bitmap->delete_bitmap_));
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(fold_incr_segment_bitmaps(
              ctx, current_meta, target_bitmap, coverage_ok))) {
            LOG_WARN("fold incr segment bitmaps fail", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// Allocate and pin a candidate overlay for bounded refresh.
static int alloc_candidate_overlay(int64_t meta_scn,
                            int64_t query_target_scn_val,
                            int64_t entry_generation,
                            ObVectorBoundedReadOverlayHandle &candidate_handle,
                            ObVectorBoundedReadOverlay *&candidate_overlay)
{
  int ret = OB_SUCCESS;
  candidate_overlay = nullptr;
  void *overlay_buf = nullptr;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null vector_index_service", K(ret));
    // Use the adaptor-owned allocator so a published overlay outlives this query.
  } else if (OB_ISNULL(overlay_buf = vector_index_service->get_adaptor_allocator().alloc(
      sizeof(ObVectorBoundedReadOverlay)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc candidate overlay fail", K(ret));
  } else {
    candidate_overlay = new (overlay_buf) ObVectorBoundedReadOverlay();
    candidate_overlay->visibility_bitmap_.allocator_ =
        &vector_index_service->get_adaptor_allocator();
    candidate_overlay->visibility_bitmap_.tenant_id_ = MTL_ID();
    if (OB_FAIL(candidate_handle.set_memdata(candidate_overlay,
        &vector_index_service->get_adaptor_allocator(), MTL_ID()))) {
      LOG_WARN("pin candidate handle fail", K(ret));
      candidate_overlay->~ObVectorBoundedReadOverlay();
      vector_index_service->get_adaptor_allocator().free(overlay_buf);
      candidate_overlay = nullptr;
    } else {
      candidate_overlay->meta_scn_ = meta_scn;
      candidate_overlay->build_target_scn_ = query_target_scn_val;
      candidate_overlay->build_generation_ = entry_generation;
    }
  }
  if (OB_SUCC(ret)) {
    {
      lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
      ROARING_TRY_CATCH(candidate_overlay->visibility_bitmap_.insert_bitmap_ = roaring::api::roaring64_bitmap_create());
    }
    if (OB_SUCC(ret)) {
      lib::ObMallocHookAttrGuard guard(lib::ObMemAttr(MTL_ID(), "VIBitmap"));
      ROARING_TRY_CATCH(candidate_overlay->visibility_bitmap_.delete_bitmap_ = roaring::api::roaring64_bitmap_create());
    }
    if (OB_SUCC(ret)
        && (OB_ISNULL(candidate_overlay->visibility_bitmap_.insert_bitmap_)
            || OB_ISNULL(candidate_overlay->visibility_bitmap_.delete_bitmap_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("create roaring bitmap fail", K(ret));
    }
  }
  return ret;
}


int ObPluginVectorIndexUtils::try_online_bounded_refresh(
    ObLSID &ls_id,
    ObPluginVectorIndexAdaptor *adapter,
    SCN target_scn,
    bool is_leader,
    ObIAllocator &allocator,
    ObVectorBoundedRefreshResult &result)
{
  int ret = OB_SUCCESS;
  result = ObVectorBoundedRefreshResult::NOT_COVERED;

  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(adapter));
  } else if (!ObVectorIndexSegmentMeta::is_snap_meta_supported(MTL_ID())) {
    LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] snap_meta not supported on tenant, skip bounded-read",
        "tenant_id", MTL_ID(), K(target_scn), K(is_leader));
  } else {
    const int64_t query_target_scn_val = target_scn.get_val_for_sql();
    int64_t loaded_base = 0;
    int64_t entry_generation = 0;
    ObVectorBoundedReadOverlayHandle existing_overlay;
    if (OB_FAIL(adapter->get_bounded_overlay_snapshot(existing_overlay, loaded_base, entry_generation))) {
      LOG_WARN("get bounded overlay snapshot fail", K(ret));
    }
    const bool existing_active = existing_overlay.is_valid()
        && existing_overlay->is_valid_overlay();
    if (OB_FAIL(ret)) {
    } else if (existing_active && query_target_scn_val < existing_overlay->build_target_scn_) {
      ret = OB_SCHEMA_EAGAIN;  // EAGAIN retry #1 (query too old)
      LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] query scn too old for existing overlay, eagain",
          K(query_target_scn_val), "build_target_scn", existing_overlay->build_target_scn_);
    } else if (loaded_base <= 0) {
      LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] coverage not provable",
          "reason", "loaded_base <= 0", K(loaded_base));
    } else {
      BoundedRefreshBuildContext ctx(ls_id, adapter, target_scn, allocator);
      ObVectorIndexMeta current_meta;
      int64_t meta_scn = 0;
      bool meta_found = false;
      if (OB_FAIL(read_current_snap_meta(ctx, current_meta, meta_scn, meta_found))) {
        LOG_WARN("read current snap meta fail", K(ret));
      } else if (!meta_found) {
        LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] coverage not provable",
            "reason", "current meta row absent");
      } else {
        const int64_t cur_base_scn = current_meta.max_base_scn();
        if (existing_active && existing_overlay->matches_identity(meta_scn, entry_generation)) {
          result = ObVectorBoundedRefreshResult::ALREADY_COVERED;
          LOG_DEBUG("[VEC_INDEX][BOUNDED_READ] overlay already covered",
              K(meta_scn), K(loaded_base), K(cur_base_scn), K(query_target_scn_val));
        } else {
          ObVectorBoundedReadOverlayHandle candidate_handle;
          ObVectorBoundedReadOverlay *candidate_overlay = nullptr;
          bool coverage_ok = false;
          if (OB_FAIL(alloc_candidate_overlay(meta_scn, query_target_scn_val,
              entry_generation, candidate_handle, candidate_overlay))) {
            LOG_WARN("alloc candidate overlay fail", K(ret));
          } else if (OB_FAIL(build_bounded_overlay(
              ctx, current_meta, loaded_base,
              candidate_overlay->visibility_bitmap_, coverage_ok))) {
            LOG_WARN("build bounded overlay fail", K(ret));
          } else if (!coverage_ok) {
            result = ObVectorBoundedRefreshResult::NOT_COVERED;
          } else {
            candidate_overlay->set_inited();
            if (OB_FAIL(adapter->publish_bounded_overlay(candidate_handle, candidate_overlay, result))) {
              if (OB_SCHEMA_EAGAIN != ret) {
                LOG_WARN("publish bounded overlay fail", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::check_can_do_refresh_memdata(ObPluginVectorIndexAdaptor *adapter, SCN target_scn, bool &can_do_refresh_memdata)
{
  int ret = OB_SUCCESS;
  can_do_refresh_memdata = false;
  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    SCN cur_adapter_replace_scn = adapter->get_replace_scn();
    if (!cur_adapter_replace_scn.is_valid()) {
      can_do_refresh_memdata = true; 
    } else {
      can_do_refresh_memdata = target_scn > cur_adapter_replace_scn;
    }
    LOG_INFO("scn check result", K(target_scn), K(cur_adapter_replace_scn), K(can_do_refresh_memdata), K(*adapter));
  }
  return ret;
}

int ObPluginVectorIndexUtils::refresh_memdata(ObLSID &ls_id,
                                              ObPluginVectorIndexAdaptor *adapter,
                                              SCN target_scn,
                                              ObIAllocator &allocator,
                                              ObVecIndexAsyncTaskCtx *ctx,
                                              const common::ObTabletID &mem_sync_tablet_id)
{
  int ret = OB_SUCCESS;
  // ToDo: remove test interface later
#if 0
  if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_ROWKEY_VID_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_ROWKEY_VID_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_VID_ROWKEY_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_VID_ROWKEY_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_INDEX_ID_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_INDEX_ID_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL));
  } else if (OB_FAIL(test_read_local_data(ls_id, adapter, INDEX_TYPE_IS_NOT, target_scn, allocator))) {
    LOG_WARN("fail to test read local data.", K(ret), K(ls_id), K(INDEX_TYPE_IS_NOT));
  }
#endif 
  bool can_do_refresh_memdata = false;
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_SUCCESS != (ret = EVENT_CALL(
                 ERRSIM_VEC_REFRESH_MEMDATA,
                 mem_sync_tablet_id.is_valid() ? mem_sync_tablet_id.id() : 0)))) {
    LOG_WARN("ERRSIM_VEC_REFRESH_MEMDATA injected",
             KR(ret), K(ls_id), K(mem_sync_tablet_id), K(target_scn));
  } else if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid adapter", K(ret), KPC(adapter));
  } else if (OB_FAIL(check_can_do_refresh_memdata(adapter, target_scn, can_do_refresh_memdata))) {
    LOG_WARN("failed to check can do refresh memdata", K(ret));
  } else if (can_do_refresh_memdata) {
    MTL_SWITCH(adapter->get_tenant_id()) {
      ObPluginVectorIndexAdaptor *new_adapter = adapter;
      ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
      ObPluginVectorIndexMgr *vec_idx_mgr = nullptr;
      if (OB_ISNULL(vector_index_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, vec_idx_mgr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get vector index ls mgr", KR(ret));
        }
      } else if (OB_ISNULL(vec_idx_mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid vector index ls mgr", KR(ret));
      } else if (OB_FAIL(refresh_adp_from_table(
                     ls_id, new_adapter, true, target_scn, allocator, ctx, mem_sync_tablet_id))) {
        LOG_WARN("failed to refresh adapter from table", K(ret), KPC(adapter));
      }
      if (adapter != new_adapter && OB_NOT_NULL(new_adapter)) {
        bool has_replace = false;
        if (OB_SUCC(ret)) {
          // Set replace scn before replace_old_adapter
          DEBUG_SYNC(BEFORE_REPLACE_OLD_ADAPTOR);
          RWLock::WLockGuard lock_guard(vec_idx_mgr->get_adapter_map_lock());
          if (OB_FAIL(new_adapter->set_replace_scn(target_scn))) {
            LOG_WARN("failed to set replace scn", K(ret), K(target_scn));
          } else if (OB_FAIL(vec_idx_mgr->replace_old_adapter_with_scn_check(new_adapter, has_replace))) {
            LOG_WARN("failed to replace old adapter", K(ret));
          }
        }
        if (OB_FAIL(ret) || !has_replace) {
          LOG_INFO("release new vector index adapter on failed", K(ret), K(new_adapter), K(has_replace));
          if(OB_NOT_NULL(ctx)) {
            ctx->reset_adaptor();
          }
          new_adapter->~ObPluginVectorIndexAdaptor();
          void *adpt_buff  = reinterpret_cast<void*>(new_adapter);
          vector_index_service->get_adaptor_allocator().free(adpt_buff);
          new_adapter = nullptr;
        }
      }
    }
  }
  return ret;
}

static bool is_non_shared_vec_index_aux_table(schema::ObIndexType type)
{
  bool bret = false;
  bret = (is_vec_delta_buffer_type(type)
          || is_vec_index_id_type(type)
          || is_vec_index_snapshot_data_type(type));
  return bret;
}

int ObPluginVectorIndexUtils::read_local_tablet(ObLSID &ls_id,
                                                ObPluginVectorIndexAdaptor* adapter,
                                                SCN target_scn,
                                                schema::ObIndexType type,
                                                ObIAllocator &allocator,
                                                ObIAllocator &scan_allocator,
                                                ObTableScanParam &scan_param,
                                                ObTableParam &table_param,
                                                common::ObNewRowIterator *&scan_iter,
                                                ObIArray<uint64_t> *out_column_ids,
                                                const bool need_all_columns,
                                                const bool is_reverse_scan,
                                                const bool need_ora_scn)
{
  int ret = OB_SUCCESS;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  scan_iter = nullptr;

  // init scan param refer to ObLocalIndexLookupOp::init_scan_param()
  // assign ls_id, tablet_id, tx_snapshot
  // set need_scn_ = true if need ora_rowscn
  ObTabletID tablet_id;
  uint64_t table_id = OB_INVALID_ID;
  ObTabletHandle tablet_handle;
  ObLSHandle ls_handle;

  // INDEX_TYPE_IS_NOT means data tablet
  if (!ls_id.is_valid() || OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid ls id or adapter", KR(ret), K(ls_id), KPC(adapter));
  } else if (is_vec_delta_buffer_type(type) || is_hybrid_vec_index_log_type(type)) {
    tablet_id = adapter->get_inc_tablet_id();
    table_id = adapter->get_inc_table_id();
  } else if (is_vec_index_id_type(type)) {
    tablet_id = adapter->get_vbitmap_tablet_id();
    table_id = adapter->get_vbitmap_table_id();
  } else if (is_vec_index_snapshot_data_type(type)) {
    tablet_id = adapter->get_snap_tablet_id();
    table_id = adapter->get_snapshot_table_id();
  } else if (is_hybrid_vec_index_embedded_type(type)) {
    tablet_id = adapter->get_embedded_tablet_id();
    table_id = adapter->get_embedded_table_id();
  } else if (is_vec_rowkey_vid_type(type)) {
    tablet_id = adapter->get_rowkey_vid_tablet_id();
    table_id = adapter->get_rowkey_vid_table_id();
  } else if (is_vec_vid_rowkey_type(type)) {
    tablet_id = adapter->get_vid_rowkey_tablet_id();
    table_id = adapter->get_vid_rowkey_table_id();
  } else if (is_hybrid_vec_index_embedded_type(type)) {
    tablet_id = adapter->get_embedded_tablet_id();
    table_id = adapter->get_embedded_table_id();
  } else if (type == INDEX_TYPE_IS_NOT) {
    tablet_id = adapter->get_data_tablet_id();
    table_id = adapter->get_data_table_id();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", KR(ret), K(type));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("read table tablet", K(ls_id), K(tablet_id), K(table_id), K(type), K(target_scn));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(tablet_id,
                                                                 tablet_handle,
                                                                 0, // timeout
                                                                 ObMDSGetTabletMode::READ_READABLE_COMMITED,
                                                                 target_scn))) {
    LOG_WARN("fail to get tablet handle", KR(ret), K(tablet_id), K(type));
  } else {
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    scan_param.schema_version_ = tablet_handle.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
    if (OB_FAIL(init_common_scan_param(scan_param, adapter, target_scn, &allocator, &scan_allocator, type, table_id, is_reverse_scan))) {
      LOG_WARN("fail to init common scan param", KR(ret), KPC(adapter));
    } else if (OB_FAIL(init_table_param(&table_param,
                                        adapter->get_inc_table_id(),
                                        adapter->get_data_table_id(),
                                        table_id,
                                        type,
                                        adapter,
                                        out_column_ids,
                                        need_all_columns,
                                        need_ora_scn))) {
      LOG_WARN("fail to init table param", KR(ret), KPC(adapter));
    } else if (FALSE_IT(scan_param.table_param_ = &table_param)) {
    } else {
      common::ObNewRange range;
      void *buf = nullptr;
      uint32_t col_cnt = 0;
      if (is_non_shared_vec_index_aux_table(type)) {
        if (OB_FAIL(get_non_shared_index_aux_table_rowkey_colum_count(type, col_cnt))) {
          LOG_WARN("fail to get index aux table colum count", KR(ret), K(type));
        } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * col_cnt * 2))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc scan range obj failed.", K(ret));
        } else {
          ObObj *row_objs = reinterpret_cast<ObObj*>(buf);
          for (int i = 0; i < col_cnt; i++) {
            row_objs[i] = ObObj::make_min_obj();
          }
          ObRowkey min_row_key(row_objs, col_cnt);
          for (int j = col_cnt; j < col_cnt * 2; j++) {
            row_objs[j] = ObObj::make_max_obj();
          }
          ObRowkey max_row_key(row_objs + col_cnt, col_cnt);

          range.table_id_ = table_id;
          range.start_key_ = min_row_key;
          range.end_key_ = max_row_key;
          range.border_flag_.set_inclusive_start();
          range.border_flag_.set_inclusive_end();
        } 
      } else {
        // vid_rowkey table or data table, get rowkey while complete
        if (OB_FAIL(get_shared_table_rowkey_colum_count(type, adapter->get_tenant_id(), table_id, col_cnt))) {
          LOG_WARN("fail to get index aux table colum count", KR(ret), K(type));
        } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * col_cnt * 2))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("alloc scan range obj failed.", K(ret));
        } else {
          ObObj *row_objs = reinterpret_cast<ObObj*>(buf);
          for (int i = 0; i < col_cnt; i++) {
            row_objs[i] = ObObj::make_min_obj();
          }
          ObRowkey min_row_key(row_objs, col_cnt);
          for (int j = col_cnt; j < col_cnt * 2; j++) {
            row_objs[j] = ObObj::make_max_obj();
          }
          ObRowkey max_row_key(row_objs + col_cnt, col_cnt);

          range.table_id_ = table_id;
          range.start_key_ = min_row_key;
          range.end_key_ = max_row_key;
          range.border_flag_.set_inclusive_start();
          range.border_flag_.set_inclusive_end();
        }
      }

      // need lob helper for read aux table 5?
      scan_param.key_ranges_.reset();
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
        LOG_WARN("failed to push key range.", K(ret), K(scan_param), K(range));
      } else {
        ObAccessService *oas = MTL(ObAccessService*);
        if (OB_ISNULL(oas)) {
          ret = OB_ERR_INTERVAL_INVALID;
          LOG_WARN("get access service failed.", K(ret));
        } else if (OB_FAIL(oas->table_scan(scan_param, scan_iter))) {
          LOG_WARN("do table scan falied.", K(ret), K(scan_param));
          if (OB_NOT_NULL(scan_iter)) {
            int tmp_ret = oas->revert_scan_iter(scan_iter);
            if (OB_SUCCESS != tmp_ret) {
              LOG_WARN("revert scan_iter failed", K(tmp_ret));
            }
            scan_iter = nullptr;
          }
        }
      }

      if (OB_NOT_NULL(buf)) {
        allocator.free(buf);
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexUtils::open_segment_data_iter(
    ObPluginVectorIndexAdaptor *adaptor,
    ObIAllocator &allocator,
    const ObLSID& ls_id,
    const ObTabletID &tablet_id,
    const ObString &start_key,
    const ObString &end_key,
    const share::SCN &target_scn,
    ObTableScanParam &scan_param,
    ObTableParam &table_param,
    common::ObNewRowIterator *&iter)
{
  int ret = OB_SUCCESS;
  ObAccessService *oas = MTL(ObAccessService*);
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::SHARE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet_with_timeout(tablet_id,
                                                                 tablet_handle,
                                                                 0, // timeout
                                                                 ObMDSGetTabletMode::READ_READABLE_COMMITED,
                                                                 target_scn))) {
    LOG_WARN("fail to get tablet handle", KR(ret), K(tablet_id));
  } else {
    scan_param.ls_id_ = ls_id;
    scan_param.tablet_id_ = tablet_id;
    scan_param.schema_version_ = tablet_handle.get_obj()->get_tablet_meta().max_sync_storage_schema_version_;
    if (OB_FAIL(ObPluginVectorIndexUtils::init_common_scan_param(scan_param, adaptor, target_scn,
        &allocator, &allocator, INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL, adaptor->get_snapshot_table_id(), false))) {
      LOG_WARN("fail to init common scan param", KR(ret), KPC(adaptor));
    } else if (OB_FAIL(ObPluginVectorIndexUtils::init_table_param(&table_param,
                                        adaptor->get_inc_table_id(),
                                        adaptor->get_data_table_id(),
                                        adaptor->get_snapshot_table_id(),
                                        INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                        adaptor,
                                        nullptr,
                                        false,
                                        false))) {
      LOG_WARN("fail to init table param", KR(ret), KPC(adaptor));
    } else if (FALSE_IT(scan_param.table_param_ = &table_param)) {
    } else {
      const ObCollationType key_col_cs_type = table_param.get_read_info().get_columns()->at(0)->get_meta_type().get_collation_type();
      common::ObNewRange range;
      void *buf = nullptr;
      const uint32_t col_cnt = table_param.get_read_info().get_schema_rowkey_count();
      if (OB_ISNULL(buf = allocator.alloc(sizeof(ObObj) * col_cnt * 2))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alloc scan range obj failed.", K(ret));
      } else {
        ObObj *row_objs = reinterpret_cast<ObObj*>(buf);
        for (int i = 0; i < col_cnt; i++) {
          if (i == 0) {
            row_objs[i].set_varchar(start_key);
            row_objs[i].set_collation_type(key_col_cs_type);
          } else {
            row_objs[i] = ObObj::make_min_obj();
          }
        }
        ObRowkey min_row_key(row_objs, col_cnt);
        for (int j = col_cnt; j < col_cnt * 2; j++) {
          if (j == col_cnt) {
            row_objs[j].set_varchar(end_key);
            row_objs[j].set_collation_type(key_col_cs_type);
          } else {
            row_objs[j] = ObObj::make_max_obj();
          }
        }
        ObRowkey max_row_key(row_objs + col_cnt, col_cnt);

        range.table_id_ = adaptor->get_snapshot_table_id();
        range.start_key_ = min_row_key;
        range.end_key_ = max_row_key;
        range.border_flag_.set_inclusive_start();
        range.border_flag_.set_inclusive_end();
      }
      scan_param.key_ranges_.reset();
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(scan_param.key_ranges_.push_back(range))) {
        LOG_WARN("failed to push key range.", K(ret), K(scan_param), K(range));
      } else if (OB_FAIL(oas->table_scan(scan_param, iter))) {
        LOG_WARN("do table scan falied.", K(ret), K(scan_param));
      }
      if (OB_NOT_NULL(buf)) {
        allocator.free(buf);
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::init_common_scan_param(storage::ObTableScanParam& scan_param, 
                                                     ObPluginVectorIndexAdaptor *adapter,
                                                     SCN target_scn,
                                                     ObIAllocator *allocator,
                                                     ObIAllocator *scan_allocator,
                                                     ObIndexType type,
                                                     uint64_t table_id,
                                                     const bool is_reverse_scan)
{
  // fix validate adapter & allocator
  // refer to ObPersistentLobAdaptor::build_common_scan_param
  int ret = OB_SUCCESS;
  ObQueryFlag query_flag((is_vec_index_id_type(type) || is_reverse_scan) ? ObQueryFlag::Reverse : ObQueryFlag::Forward, // scan_order
                         false, // daily_merge
                         false, // optimize
                         false, // sys scan
                         true, // full_row
                         false, // index_back
                         false, // query_stat
                         ObQueryFlag::MysqlMode, // sql_mode
                         false // read_latest
                        );
  query_flag.disable_cache();
  query_flag.scan_order_ = (is_vec_index_snapshot_data_type(type) && !is_reverse_scan) ? ObQueryFlag::KeepOrder : ObQueryFlag::Forward;
  query_flag.scan_order_ = (is_vec_index_id_type(type) || is_reverse_scan)? ObQueryFlag::Reverse : ObQueryFlag::Forward;
  scan_param.scan_flag_.flag_ = query_flag.flag_;
  // set column ids
  scan_param.column_ids_.reset();
  uint32 col_cnt = 0;

  if (is_vec_index(type) || type == INDEX_TYPE_IS_NOT){
    if(OB_ISNULL(adapter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null adapter", KR(ret), K(type));
    } else if (OB_FAIL(get_special_index_aux_table_column_count(type, adapter->get_tenant_id(),
                                                                table_id, col_cnt, scan_param))) {
      LOG_WARN("fail to get special index aux table column count", KR(ret), K(type), K(table_id));
    }
  }

  if (OB_SUCC(ret)) {
    scan_param.reserved_cell_count_ = scan_param.column_ids_.count();
    // table param
    scan_param.index_id_ = 0;
    scan_param.is_get_ = false;
    // set timeout
    scan_param.timeout_ = INT64_MAX;
    // scan_param.virtual_column_exprs_
    scan_param.limit_param_.limit_ = -1;
    scan_param.limit_param_.offset_ = 0;
    // sessions

    scan_param.snapshot_.init_weak_read(target_scn); 

    // never read_latest
    // if(param.read_latest_) {
    //  scan_param.tx_id_ = param.snapshot_.core_.tx_id_;
    // }
    scan_param.sql_mode_ = SMO_DEFAULT;
    // common set
    scan_param.allocator_ = allocator;
    scan_param.for_update_ = false;
    scan_param.for_update_wait_timeout_ = scan_param.timeout_;
    scan_param.scan_allocator_ = scan_allocator;
    scan_param.frozen_version_ = -1;
    scan_param.force_refresh_lc_ = false;
    scan_param.output_exprs_ = nullptr;
    scan_param.aggregate_exprs_ = nullptr;
    scan_param.op_ = nullptr;
    scan_param.row2exprs_projector_ = nullptr;
    scan_param.need_scn_ = false;
    scan_param.pd_storage_flag_ = false;
    // not flashback
    // scan_param.fb_snapshot_ = param.fb_snapshot_;
  }
  return ret;
}

int ObPluginVectorIndexUtils::init_table_param(ObTableParam *table_param,
                                               uint64_t inc_table_id,
                                               uint64_t data_table_id, 
                                               uint64_t table_id,
                                               schema::ObIndexType type,
                                               ObPluginVectorIndexAdaptor *adapter,
                                               ObIArray<uint64_t> *out_column_ids,
                                               const bool need_all_columns,
                                               const bool need_ora_scn)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> column_ids;
  uint64_t tenant_id = 0;
  if (OB_ISNULL(adapter)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null adapter", KR(ret), K(inc_table_id), K(data_table_id), K(table_id), K(type));
  } else {
    tenant_id = adapter->get_tenant_id();
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), KR(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
    LOG_WARN("get null table schema", KR(ret), KR(table_id));
  } else if (is_vec_delta_buffer_type(type) || is_hybrid_vec_index_log_type(type)) {
    ObArray<uint64_t> tmp_column_ids;
    const ObTableSchema *data_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else if (tmp_column_ids.count() < 3) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", K(tmp_column_ids.count()));
    } else {
      // need [vid][type][vector]/[chunk] [ORA_ROWSCN]
      uint64_t vid_column_id = 0;
      uint64_t type_column_id = 0;
      uint64_t vector_column_id = 0;
      uint64_t chunk_column_id = 0;
      ObSEArray<uint64_t, 4> part_column_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_vid_column()) {
          vid_column_id = col_schema->get_column_id();
        } else if (col_schema->is_hidden_pk_column_id(col_schema->get_column_id())) {
          if (vid_column_id == 0) {  // vid_column_id != 0 means it has been assigned by vid col.
            vid_column_id = col_schema->get_column_id();
          }
        } else if (col_schema->is_vec_hnsw_type_column()) {
          type_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_vector_column()) {
          vector_column_id = col_schema->get_column_id();
        } else if (col_schema->is_hybrid_vec_index_chunk_column()) {
          chunk_column_id = col_schema->get_column_id();
        } else if (OB_FAIL(part_column_ids.push_back(col_schema->get_column_id()))) {
          LOG_WARN("failed to push back column id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (vid_column_id == 0 || type_column_id == 0 || (vector_column_id == 0 && chunk_column_id == 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get valid column id", K(ret), K(vid_column_id), K(type_column_id), K(vector_column_id));
      } else if (OB_FAIL(column_ids.push_back(vid_column_id))) {
        LOG_WARN("failed to push 2nd column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(type_column_id))) {
        LOG_WARN("failed to push 3rd column id.", K(ret));
      } else if (0 != vector_column_id && OB_FAIL(column_ids.push_back(vector_column_id))) {
        LOG_WARN("failed to push 4th column id.", K(ret));
      } else if (0 != chunk_column_id && OB_FAIL(column_ids.push_back(chunk_column_id))) {
        LOG_WARN("failed to push chunk column id.", K(ret));
      } else if (need_ora_scn && OB_FAIL(column_ids.push_back(OB_HIDDEN_TRANS_VERSION_COLUMN_ID))) {
        LOG_WARN("failed to push ORA_ROWSCN column id.", K(ret));
      }
      if (OB_SUCC(ret) && need_all_columns) {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_column_ids.count(); ++i) {
          if (OB_FAIL(column_ids.push_back(part_column_ids.at(i)))) {
            LOG_WARN("failed to push back column id.", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(out_column_ids)) {
        out_column_ids->assign(column_ids);
      }
      if (OB_SUCC(ret) && OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (is_vec_index_id_type(type)) {
    // different with other index, refer to ObTscCgService::extract_vec_ir_access_columns
    ObArray<uint64_t> tmp_column_ids;
    const ObTableSchema *data_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else if (tmp_column_ids.count() < 4) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected column count", K(tmp_column_ids.count()));
    } else {
      // need [scn][vid][type][vector]
      uint64_t scn_column_id = 0;
      uint64_t vid_column_id = 0;
      uint64_t type_column_id = 0;
      uint64_t vector_column_id = 0;
      ObSEArray<uint64_t, 4> part_column_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_scn_column()) {
          scn_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_vid_column()) {  
          vid_column_id = col_schema->get_column_id();
        } else if (col_schema->is_hidden_pk_column_id(col_schema->get_column_id())) {
          if (vid_column_id == 0) {  // vid_column_id != 0 means it has been assigned by vid col.
            vid_column_id = col_schema->get_column_id();
          }
        } else if (col_schema->is_vec_hnsw_type_column()) {
          type_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_vector_column()) {
          vector_column_id = col_schema->get_column_id();
        } else if (OB_FAIL(part_column_ids.push_back(col_schema->get_column_id()))) {
          LOG_WARN("failed to push back column id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (scn_column_id == 0 || vid_column_id == 0 || type_column_id == 0 || vector_column_id == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get valid column id", K(ret), K(scn_column_id), K(vid_column_id), K(type_column_id), K(vector_column_id));
      } else if (OB_FAIL(column_ids.push_back(scn_column_id))) {
        LOG_WARN("failed to push 1st column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(vid_column_id))) {
        LOG_WARN("failed to push 2nd column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(type_column_id))) {
        LOG_WARN("failed to push 3rd column id.", K(ret));
      } else if (OB_FAIL(column_ids.push_back(vector_column_id))) {
        LOG_WARN("failed to push 4th column id.", K(ret));
      }
      if (OB_SUCC(ret) && need_all_columns) {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_column_ids.count(); ++i) {
          if (OB_FAIL(column_ids.push_back(part_column_ids.at(i)))) {
            LOG_WARN("failed to push back column id.", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(out_column_ids)) {
        out_column_ids->assign(column_ids);
      }
      if (OB_SUCC(ret) && OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (is_vec_rowkey_vid_type(type)) {
    uint64_t vid_column_id = 0;
    ObSEArray<uint64_t, 4> tmp_column_ids;
    const ObTableSchema *data_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else {
      // make sure vid column is the last output column
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count() && vid_column_id == 0; ++i) {
        const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema_by_idx(i);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_vid_column()) {
          vid_column_id = col_schema->get_column_id();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (FALSE_IT(tmp_column_ids.reuse())) {
      } else if (OB_FAIL(data_table_schema->get_rowkey_column_ids(tmp_column_ids))) {
        LOG_WARN("failed to get data table rowkey column id", K(ret), KPC(data_table_schema));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        if (OB_FAIL(column_ids.push_back(tmp_column_ids[i]))) {
          LOG_WARN("failed to push column id.", K(ret), K(i), K(tmp_column_ids[i]), K(vid_column_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_ids.push_back(vid_column_id))) {
        LOG_WARN("failed to push 1st column id.", K(ret));
      } else if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (is_vec_vid_rowkey_type(type)) {
    uint64_t vid_column_id = 0;
    ObSEArray<uint64_t, 4> tmp_column_ids;
    const ObTableSchema *data_table_schema = NULL;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else {
      // make sure vid column is the first output column
      for (int64_t i = 0; OB_SUCC(ret) && i < table_schema->get_column_count() && vid_column_id == 0; ++i) {
        const ObColumnSchemaV2 *col_schema = table_schema->get_column_schema_by_idx(i);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_vid_column()) {
          vid_column_id = col_schema->get_column_id();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_ids.push_back(vid_column_id))) {
        LOG_WARN("failed to push 1st column id.", K(ret));
      } else if (FALSE_IT(tmp_column_ids.reuse())) {
      } else if (OB_FAIL(data_table_schema->get_rowkey_column_ids(tmp_column_ids))) {
        LOG_WARN("failed to get data table rowkey column id", K(ret), KPC(data_table_schema));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        if (OB_FAIL(column_ids.push_back(tmp_column_ids[i]))) {
          LOG_WARN("failed to push column id.", K(ret), K(i), K(tmp_column_ids[i]), K(vid_column_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (is_vec_index_snapshot_data_type(type)) {
    const ObTableSchema *data_table_schema = NULL;
    ObSEArray<uint64_t, 4> tmp_column_ids;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else {
      uint64_t key_column_id = 0;
      uint64_t lob_data_column_id = 0;
      uint64_t visible_column_id = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_key_column()) {
          key_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_data_column()) {
          lob_data_column_id = col_schema->get_column_id();
        } else if (col_schema->is_vec_hnsw_visible_column()) {
          visible_column_id = col_schema->get_column_id();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (key_column_id == 0 || lob_data_column_id == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected snapshot data column ids", K(key_column_id), K(lob_data_column_id));
      } else if (OB_FAIL(column_ids.push_back(key_column_id))) {
        LOG_WARN("failed to push column id.", K(ret), K(key_column_id));
      } else if (OB_FAIL(column_ids.push_back(lob_data_column_id))) {
        LOG_WARN("failed to push column id.", K(ret), K(lob_data_column_id));
      } else if (visible_column_id != 0 && OB_FAIL(column_ids.push_back(visible_column_id))) {
        LOG_WARN("failed to push column id.", K(ret), K(visible_column_id));
      } else {
        table_param->get_enable_lob_locator_v2() = true;
        if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
          LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
        }
      }
    }
  } else if (is_hybrid_vec_index_embedded_type(type)) {
    const ObTableSchema *data_table_schema = NULL;
    ObSEArray<uint64_t, 4> tmp_column_ids;
    if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_table_schema))) {
      LOG_WARN("fail to get schema", KR(ret), KR(data_table_id));
    } else if (OB_ISNULL(table_schema) || OB_ISNULL(data_table_schema)) {
      ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
      LOG_WARN("get null table schema", KR(ret), K(table_id), K(data_table_id));
    } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else {
      // need [rowkey][vid][vector]
      uint64_t vid_column_id = 0;
      uint64_t vector_column_id = 0;
      ObSEArray<uint64_t, 4> part_column_ids;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
        const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column schema ptr", K(ret));
        } else if (col_schema->is_vec_hnsw_vid_column() || col_schema->is_hidden_pk_column_id(col_schema->get_column_id())) {
          if (vid_column_id != 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("vid col and pk increment col both exist", K(ret), K(vid_column_id), K(col_schema->get_column_id()));
          } else {
            vid_column_id = col_schema->get_column_id();
          }
        } else if (col_schema->is_vec_hnsw_vector_column()) {
          vector_column_id = col_schema->get_column_id();
        } else if (OB_FAIL(part_column_ids.push_back(col_schema->get_column_id()))) {
          LOG_WARN("failed to push back column id", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (vid_column_id == 0 || vector_column_id == 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected embedded table data column ids", K(vid_column_id), K(vector_column_id));
      } else if (!need_all_columns) {
      } else if (FALSE_IT(tmp_column_ids.reuse())) {
      } else if (adapter->get_is_need_vid() &&  OB_FAIL(data_table_schema->get_rowkey_column_ids(tmp_column_ids))) {
        LOG_WARN("failed to get data table rowkey column id", K(ret), KPC(data_table_schema));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
          if (OB_FAIL(column_ids.push_back(tmp_column_ids[i]))) {
            LOG_WARN("failed to push column id.", K(ret), K(i), K(tmp_column_ids[i]), K(vid_column_id));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_ids.push_back(vid_column_id))) {
        LOG_WARN("failed to push column id.", K(ret), K(vid_column_id));
      } else if (!adapter->get_is_need_vid()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < part_column_ids.count(); ++i) {
          if (OB_FAIL(column_ids.push_back(part_column_ids[i]))) {
            LOG_WARN("failed to push column id.", K(ret), K(i), K(part_column_ids[i]), K(vid_column_id));
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(column_ids.push_back(vector_column_id))) {
        LOG_WARN("failed to push column id.", K(ret), K(vector_column_id));
      }
      if (OB_SUCC(ret) && OB_NOT_NULL(out_column_ids)) {
        out_column_ids->assign(column_ids);
      }
      if (OB_SUCC(ret) && OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (is_vec_index(type)) {
    if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
      LOG_ERROR("fail to get index table all column ids", K(table_schema), KPC(adapter));
    } else {
      if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_ERROR("fail to convert table param", KR(ret), K(table_schema), K(type));
      }
    }
  } else if (type == INDEX_TYPE_IS_NOT) {
    if (OB_FAIL(get_data_table_out_column_id(column_ids, inc_table_id, table_id, tenant_id, adapter))) {
      LOG_WARN("failed to get vec column id.", K(ret));
    } else {
      table_param->get_enable_lob_locator_v2() = true;
      if (OB_FAIL(table_param->convert(*table_schema, column_ids, sql::ObStoragePushdownFlag()))) {
        LOG_WARN("failed to convert table param.", K(ret));
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_non_shared_index_aux_table_colum_count(schema::ObIndexType type, uint32 &col_cnt)
{
  static const uint32 delta_buffer_tab_col_cnt = 3; // vid, type, vector, "ora_rowscn", 3 or 4 columns
  static const uint32 index_id_tab_col_cnt = 4; // scn ,vid, type, vector
  static const uint32 index_snapshot_tab_col_cnt = 2; // key, data

  int ret = OB_SUCCESS;
  col_cnt = 0;
  if (is_vec_delta_buffer_type(type)) {
    col_cnt = delta_buffer_tab_col_cnt;
  } else if (is_vec_index_id_type(type)) {
    col_cnt = index_id_tab_col_cnt;
  } else if (is_vec_index_snapshot_data_type(type)) {
    col_cnt = index_snapshot_tab_col_cnt;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", KR(ret), K(type));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_non_shared_index_aux_table_colum_count", K(type), K(col_cnt)); // remove after debug;
  }
  return ret;  
}

int ObPluginVectorIndexUtils::get_special_index_aux_table_column_count(
  schema::ObIndexType type,
  uint64_t tenant_id,
  uint64_t table_id,
  uint32 &col_cnt,
  storage::ObTableScanParam& scan_param)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
    LOG_WARN("get null table schema", KR(ret), K(table_id));
  } else if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
    LOG_ERROR("fail to get index table all column ids", K(table_schema));
  } else if (OB_FAIL(scan_param.column_ids_.assign(column_ids))) {
    LOG_WARN("failed to assign column ids.", K(ret));
  } else {
    col_cnt = column_ids.count();
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_special_index_aux_table_column_count", K(type), K(col_cnt), K(column_ids), K(tenant_id)); // remove after debug;
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_non_shared_index_aux_table_rowkey_colum_count(schema::ObIndexType type, uint32 &col_cnt)
{
  // only need to do range scan for aux index table 3, 4, 5
  // other tables only needs multi get
  static const uint32 delta_buffer_tab_col_cnt = 2; // rowkey:vid, type, other:vector, "ora_rowscn"
  static const uint32 index_id_tab_col_cnt = 3; // rowkey:scn ,vid, type, other:vector
  static const uint32 index_snapshot_tab_col_cnt = 1; // rowkey:key, other:data

  int ret = OB_SUCCESS;
  col_cnt = 0;
  if (is_vec_delta_buffer_type(type)) {
    col_cnt = delta_buffer_tab_col_cnt;
  } else if (is_vec_index_id_type(type)) {
    col_cnt = index_id_tab_col_cnt;
  } else if (is_vec_index_snapshot_data_type(type)) {
    col_cnt = index_snapshot_tab_col_cnt;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected index type", KR(ret), K(type));
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_non_shared_index_aux_table_rowkey_colum_count", K(type), K(col_cnt)); // remove after debug;
  }
  return ret;  
}

int ObPluginVectorIndexUtils::get_shared_table_rowkey_colum_count(schema::ObIndexType type,
                                                                  uint64_t tenant_id,
                                                                  uint64_t table_id,
                                                                  uint32 &col_cnt)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObSEArray<uint64_t, 4> column_ids;
  if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), KR(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
    LOG_WARN("get null table schema", KR(ret), KR(table_id));
  } else {
    const ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    if (OB_FAIL(rowkey_info.get_column_ids(column_ids))) {
      LOG_WARN("get rowkey_info from  table schema faild", KR(ret), KR(table_id), KPC(table_schema));
    } else {
      col_cnt = column_ids.count();
    }
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("get_non_shared_index_aux_table_rowkey_colum_count", K(type), K(col_cnt), K(column_ids)); // remove after debug;
  }
  return ret;
}

int ObPluginVectorIndexUtils::release_vector_index_adapter(ObPluginVectorIndexAdaptor* &adapter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(adapter)) {
    // do nothing
  } else {
    if (adapter->dec_ref_and_check_release()) {
      ObIAllocator *allocator = adapter->get_allocator();
      if (OB_ISNULL(allocator)) {
        const int ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "release vector index adapter failed", KPC(adapter));
      } else {
        // OB_LOG(DEBUG, "adatper released", KPC(adapter));
        adapter->~ObPluginVectorIndexAdaptor();
        allocator->free(adapter);
      }
      adapter = nullptr;
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::release_vector_index_build_helper(ObIvfBuildHelper* &helper, bool *fully_released)
{
  int ret = OB_SUCCESS;
  if (nullptr != fully_released) {
    *fully_released = false;
  }
  if (OB_ISNULL(helper)) {
    // do nothing
  } else {
    if (helper->dec_ref_and_check_release()) {
      ObIAllocator *allocator = helper->get_allocator();
      if (OB_ISNULL(allocator)) {
        const int ret = OB_ERR_UNEXPECTED;
        OB_LOG(WARN, "release ivf vector index build helper failed", KPC(helper));
      } else {
        helper->~ObIvfBuildHelper();
        allocator->free(helper);
      }
      helper = nullptr;
      if (nullptr != fully_released) {
        *fully_released = true;
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::release_ivf_cache_mgr(ObIvfCacheMgr* &mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mgr)) {
    // do nothing
  } else if (mgr->dec_ref_and_check_release()) {
    ObIAllocator &allocator = mgr->get_self_allocator();
    mgr->~ObIvfCacheMgr();
    allocator.free(mgr);
    mgr = nullptr;
  }
  return ret;
}

ObVectorIndexRecordType ObPluginVectorIndexUtils::index_type_to_record_type(schema::ObIndexType type)
{
  ObVectorIndexRecordType record_type = VIRT_MAX;
  if (schema::is_vec_delta_buffer_type(type) || schema::is_hybrid_vec_index_log_type(type)) {
    record_type =  VIRT_INC;
  } else if (schema::is_vec_index_id_type(type)) {
    record_type = VIRT_BITMAP;
  } else if (schema::is_vec_index_snapshot_data_type(type)) {
    record_type = VIRT_SNAP;
  } else if (schema::is_hybrid_vec_index_embedded_type(type)) {
    record_type = VIRT_EMBEDDED;
  }
  return record_type;
}

static int assign_vector_index_prefix(const ObString &index_name,
                                      const char *suffix,
                                      ObString &prefix)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(suffix)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("suffix is null", K(ret));
  } else if (!index_name.suffix_match(suffix)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index name does not match expected suffix", K(ret), K(index_name), K(suffix));
  } else {
    const int64_t suffix_len = STRLEN(suffix);
    prefix.assign_ptr(index_name.ptr(), index_name.length() - suffix_len);
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_vector_index_prefix_inner(const ObSimpleTableSchemaV2 &index_schema,
                                                      const ObString index_name, 
                                                      ObString &prefix)
{
  int ret = OB_SUCCESS;
  prefix.reset();
  if (!index_schema.is_vec_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected, not vector index table", K(ret), K(index_schema)); 
  } else if (index_schema.is_vec_rowkey_vid_type() || index_schema.is_vec_vid_rowkey_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index type, only support get none share table prefix", 
      K(ret), K(index_schema));
  } else {
    const char* delta_buffer_table = ObVecIndexBuilderUtil::DELTA_BUFFER_TABLE_NAME_SUFFIX;
    const char* index_id_table = ObVecIndexBuilderUtil::INDEX_ID_TABLE_NAME_SUFFIX;
    const char* index_snapshot_data_table = ObVecIndexBuilderUtil::SNAPSHOT_DATA_TABLE_NAME_SUFFIX;
    const char* hybrid_log_table = ObVecIndexBuilderUtil::HYBRID_LOG_TABLE_NAME_SUFFIX;
    const char* hybrid_embedded_table = ObVecIndexBuilderUtil::HYBRID_EMBEDDED_VEC_TABLE_NAME_SUFFIX;
    // ivf
    const char *ivf_centroid_table = ObVecIndexBuilderUtil::IVF_CENTROID_TABLE_NAME_SUFFIX;
    const char* ivf_rowkey_cid_table = ObVecIndexBuilderUtil::IVF_ROWKEY_CID_TABLE_NAME_SUFFIX;
    const char* ivf_cid_vector_table = ObVecIndexBuilderUtil::IVF_CID_VECTOR_TABLE_NAME_SUFFIX;
    const char* ivf_sq_meta_table = ObVecIndexBuilderUtil::IVF_SQ_META_TABLE_NAME_SUFFIX;
    const char* ivf_pq_centroid_table = ObVecIndexBuilderUtil::IVF_PQ_CENTROID_TABLE_NAME_SUFFIX;
    const char* ivf_pq_code_table = ObVecIndexBuilderUtil::IVF_PQ_CODE_TABLE_NAME_SUFFIX;
    const char* ivf_pq_rowkey_cid_table = ObVecIndexBuilderUtil::IVF_PQ_ROWKEY_CID_TABLE_NAME_SUFFIX;
    
    const char *suffix = nullptr;

    if (index_schema.is_vec_delta_buffer_type()) {
      suffix = delta_buffer_table;
    } else if (index_schema.is_vec_index_id_type()) {
      suffix = index_id_table;
    } else if (index_schema.is_vec_index_snapshot_data_type()) {
      suffix = index_snapshot_data_table;
    } else if (index_schema.is_hybrid_vec_index_log_type()) {
      suffix = hybrid_log_table;
    } else if (index_schema.is_hybrid_vec_index_embedded_type()) {
      suffix = hybrid_embedded_table;
    } else if (index_schema.is_vec_ivf_centroid_index()) {
      suffix = ivf_centroid_table;
    } else if (index_schema.is_vec_ivfflat_rowkey_cid_index() ||
               index_schema.is_vec_ivfsq8_rowkey_cid_index()) {
      suffix = ivf_rowkey_cid_table;
    } else if (index_schema.is_vec_ivfflat_cid_vector_index() ||
               index_schema.is_vec_ivfsq8_cid_vector_index()) {
      suffix = ivf_cid_vector_table;
    } else if (index_schema.is_vec_ivfsq8_meta_index()) {
      suffix = ivf_sq_meta_table;
    } else if (index_schema.is_vec_ivfpq_pq_centroid_index()) {
      suffix = ivf_pq_centroid_table;
    } else if (index_schema.is_vec_ivfpq_code_index()) {
      suffix = ivf_pq_code_table;
    } else if (index_schema.is_vec_ivfpq_rowkey_cid_index()) {
      suffix = ivf_pq_rowkey_cid_table;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector index type", K(ret), K(index_schema));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(assign_vector_index_prefix(index_name, suffix, prefix))) {
        LOG_WARN("failed to assign vector index prefix", K(ret), K(index_name), K(suffix), K(index_schema.get_table_id()));
      } else {
        LOG_INFO("get_index_prefix", K(prefix), K(index_name), K(index_schema.get_table_id()));
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_vector_index_prefix(const ObSimpleTableSchemaV2 &index_schema,
                                                      ObString &prefix)
{
  int ret = OB_SUCCESS;
  ObString tmp_table_name = index_schema.get_table_name();
  if (OB_FAIL(get_vector_index_prefix_inner(index_schema, tmp_table_name, prefix))) {
    LOG_WARN("failed to get_vector_index_prefix_inner", K(ret), K(tmp_table_name));
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_vector_index_prefix(const schema::ObTableSchemaParam &index_schema,
                                                      ObString &prefix)
{
  int ret = OB_SUCCESS;
  prefix.reset();
  ObString index_name;
  if (!index_schema.is_vector_index()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected, not vector index table", K(ret), K(index_schema));
  } else if (schema::is_vec_rowkey_vid_type(index_schema.get_index_type())
             || schema::is_vec_vid_rowkey_type(index_schema.get_index_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected vector index type, only support get none share table prefix",
      K(ret), K(index_schema));
  } else if (OB_FAIL(index_schema.get_index_name(index_name))) {
    LOG_WARN("failed to get index name", K(ret), K(index_schema));
  } else {
    const char* delta_buffer_table = ObVecIndexBuilderUtil::DELTA_BUFFER_TABLE_NAME_SUFFIX;
    const char* index_id_table = ObVecIndexBuilderUtil::INDEX_ID_TABLE_NAME_SUFFIX;
    const char* index_snapshot_data_table = ObVecIndexBuilderUtil::SNAPSHOT_DATA_TABLE_NAME_SUFFIX;
    const char* hybrid_log_table = ObVecIndexBuilderUtil::HYBRID_LOG_TABLE_NAME_SUFFIX;
    const char* hybrid_embedded_table = ObVecIndexBuilderUtil::HYBRID_EMBEDDED_VEC_TABLE_NAME_SUFFIX;
    const char *suffix = nullptr;
    const schema::ObIndexType index_type = index_schema.get_index_type();
    if (schema::is_vec_delta_buffer_type(index_type)) {
      suffix = delta_buffer_table;
    } else if (schema::is_vec_index_id_type(index_type)) {
      suffix = index_id_table;
    } else if (schema::is_vec_index_snapshot_data_type(index_type)) {
      suffix = index_snapshot_data_table;
    } else if (schema::is_hybrid_vec_index_log_type(index_type)) {
      suffix = hybrid_log_table;
    } else if (schema::is_hybrid_vec_index_embedded_type(index_type)) {
      suffix = hybrid_embedded_table;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected vector index type", K(ret), K(index_schema));
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(assign_vector_index_prefix(index_name, suffix, prefix))) {
        LOG_WARN("failed to assign vector index prefix", K(ret), K(index_name), K(suffix), K(index_schema.get_table_id()));
      } else {
        LOG_INFO("get_index_prefix", K(prefix), K(index_name), K(index_schema.get_table_id()));
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_vector_index_name_prefix(const ObTableSchema &index_schema,
                                                      ObString &prefix)
{
  int ret = OB_SUCCESS;
  ObString index_name;
  if (OB_FAIL(index_schema.get_index_name(index_name))) {
    LOG_WARN("failed to get index name", K(ret), K(index_schema));
  } else if (OB_FAIL(get_vector_index_prefix_inner(index_schema, index_name, prefix))) {
    LOG_WARN("failed to get_vector_index_prefix_inner", K(ret), K(index_name));
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_domain_table_schema(share::schema::ObSchemaGetterGuard &schema_guard, const ObTableSchema &index_schema, const ObTableSchema *&domain_table_schema)
{
  int ret = OB_SUCCESS;
  ObString domain_table_name;
  if (OB_FAIL(get_vector_index_prefix(index_schema, domain_table_name))) {
    LOG_WARN("fail to get vector index prefix", K(ret), K(index_schema));
  } else if (OB_FAIL(schema_guard.get_table_schema(
      index_schema.get_tenant_id(),
      index_schema.get_database_id(),
      domain_table_name,
      true,
      domain_table_schema,
      index_schema.is_user_hidden_table(),
      false
  ))) {
    LOG_WARN("fail to get domain table schema", K(ret), K(domain_table_name));
  }
  return ret;
}

int ObPluginVectorIndexUtils::erase_ivf_build_helper(ObLSID ls_id, const ObIvfHelperKey &key, bool *fully_cleared)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(vec_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ObPluginVectorIndexService ptr", K(ret), K(MTL_ID()));
  } else if (OB_FAIL(vec_index_service->erase_ivf_build_helper(ls_id, key, fully_cleared))) {
    LOG_WARN("failed to erase ivf build helper", K(ret), K(ls_id), K(key));
  }
  if (ret == OB_HASH_NOT_EXIST) {
    LOG_WARN("erase ivf build helper, key not exist", K(ret), K(ls_id), K(key));
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_table_key_scn(const ObString &key_str, int64_t &scn)
{
  int ret = OB_SUCCESS;
  scn = 0;
  ObString tmp_key_str = key_str;
  ObArray<ObString> tmp_key_strs;
  char *end_ptr = nullptr;
  
  if (tmp_key_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tmp key str, is empty", K(ret));
  } else if (OB_FAIL(split_on(tmp_key_str, '_', tmp_key_strs))) {
    LOG_WARN("fail to split func expr", K(ret), K(tmp_key_str));
  } else if (tmp_key_strs.count() < 2) {  // at lease two str (tablet_scn)
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid vector index param count", K(tmp_key_strs.count()));
  } else {
    ObString tmp_scn_str = tmp_key_strs.at(1).trim();
    if (OB_FAIL(ob_strtoll(tmp_scn_str.ptr(), end_ptr, scn))) {
      LOG_WARN("fail to strtoll scn", K(ret), K(tmp_scn_str), K(key_str));
    } else if (scn <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected scn value", K(ret), K(scn), K(tmp_scn_str));
    } else {
      LOG_DEBUG("get key scn", K(ret), K(tmp_scn_str), K(tmp_key_strs), K(key_str));
    }
  }
  return ret;
}

void ObPluginVectorIndexUtils::get_key_prefix_scn(const ObString &key_prefix, int64_t &scn)
{
  int ret = OB_SUCCESS;
  scn = 0;
  ObString split_item("_");
  ObString scn_str;
  char *end_ptr = nullptr;
  if (OB_FAIL(get_suffix(key_prefix, split_item, scn_str))) {
    LOG_WARN("fail to split snapshot prefix", K(ret), K(key_prefix));
  } else if (scn_str.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected get empty scn of key_prefix", K(ret), K(key_prefix));
  } else if (OB_FAIL(ob_strtoll(scn_str.ptr(), end_ptr, scn))) {
    LOG_WARN("fail to strtoll scn", K(ret), K(key_prefix), K(scn_str));
  } else if (scn <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scn value", K(ret), K(scn), K(key_prefix));
  } else {
    LOG_DEBUG("get key scn", K(ret), K(key_prefix), K(scn_str));
  }
}

int ObPluginVectorIndexUtils::get_split_snapshot_prefix(
    const ObVectorIndexAlgorithmType index_type, const ObString &src, ObString &dst)
{
  int ret = OB_SUCCESS;

  if (index_type == VIAT_HGRAPH) {
    ObString split_item("_hgraph_");
    if (OB_FAIL(get_prefix(src, split_item, dst))) {
      LOG_WARN("fail to split snapshot prefix");
    }
  } else if (index_type == VIAT_HNSW || index_type == VIAT_HNSW_SQ || index_type == VIAT_HNSW_BQ) {
    ObString split_item("_hnsw_");
    if (OB_FAIL(get_prefix(src, split_item, dst))) {
      LOG_WARN("fail to split snapshot prefix");
    }
  } else if (index_type == VIAT_IPIVF || index_type == VIAT_IPIVF_SQ) {
    ObString split_item("_ipivf_");
    if (OB_FAIL(get_prefix(src, split_item, dst))) {
      LOG_WARN("fail to split ipivf snapshot prefix");
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid index type", K(ret), K(index_type));
  }
  return ret;
}


int ObPluginVectorIndexUtils::get_suffix(const ObString &src, const ObString &item, ObString &dst)
{
  int ret = OB_SUCCESS;
  dst.reset();
  if (src.empty() || item.empty() || (src.length() < item.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(src), K(item));
  } else {
    const char *str = strstr(src.ptr(), item.ptr());
    if (OB_ISNULL(str)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item is not include to src", K(ret), K(src), K(dst));
    } else {
      const int64_t prefix_len = str - src.ptr();
      const int64_t suffix_len = src.length() - prefix_len - item.length();
      dst.assign_ptr(str + item.length(), static_cast<int32_t>(suffix_len));
      LOG_DEBUG("get suffix", K(src), K(dst), K(prefix_len), K(suffix_len), K(item));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_prefix(const ObString &src, const ObString &item, ObString &dst)
{
  int ret = OB_SUCCESS;
  dst.reset();
  if (src.empty() || item.empty() || (src.length() < item.length())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(src), K(item));
  } else {
    const char *str = strstr(src.ptr(), item.ptr());
    if (OB_ISNULL(str)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item is not include to src", K(ret), K(src), K(dst));
    } else {
      const int64_t len = str - src.ptr();
      dst.assign_ptr(src.ptr(), static_cast<int32_t>(len));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::try_get_ls_index_mgr(const ObLSID &ls_id, ObPluginVectorIndexMgr *&mgr)
{
  int ret = OB_SUCCESS;
  mgr = nullptr;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  if (!ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls id", K(ret), K(ls_id));
  } else if (OB_ISNULL(vector_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service is null", K(ret));
  } else if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, mgr))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get vector index ls mgr", K(ret), K(ls_id));
    }
  }
  return ret;
}

void ObPluginVectorIndexUtils::set_ls_leader_flag(const ObLSID &ls_id, const bool is_leader)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  if (!ls_id.is_valid() || OB_ISNULL(vector_index_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), KP(vector_index_service));
  } else if (OB_FAIL(vector_index_service->acquire_vector_index_mgr(ls_id, index_ls_mgr))) {
    LOG_WARN("fail to acquire vector index mgr", K(ret), K(ls_id));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(index_ls_mgr));
  } else {
    index_ls_mgr->set_ls_leader(is_leader);
    if (is_leader) {
      index_ls_mgr->set_need_refresh_memdata(true);
    }
    LOG_INFO("success to set ls leader", K(ls_id), K(is_leader));
  }
}

int ObPluginVectorIndexUtils::get_ls_leader_flag(const ObLSID &ls_id, bool &is_leader)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  if (OB_FAIL(vector_index_service->get_ls_index_mgr_map().get_refactored(ls_id, index_ls_mgr))) {
    LOG_WARN("fail to get vector index ls mgr", KR(ret), K(ls_id));
  } else if (OB_ISNULL(index_ls_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret));
  } else {
    is_leader = index_ls_mgr->get_ls_leader();
    LOG_TRACE("success to set ls leader", K(ls_id), K(is_leader));
  }
  return ret;
}

int ObPluginVectorIndexUtils::fill_mem_context_detail_info(ObPluginVectorIndexService *service, ObIArray<ObLSTabletPair> &tablet_ids, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> adaptor_ptr_set;
  if (tablet_ids.count() > 0 && OB_FAIL(adaptor_ptr_set.create(tablet_ids.count()))) {
    LOG_WARN("fail to create tablet_id set", KR(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
    ObLSID ls_id = tablet_ids.at(i).ls_id_;
    ObTabletID tablet_id = tablet_ids.at(i).tablet_id_;
    ObPluginVectorIndexAdapterGuard adapter_guard;
    if (OB_FAIL(service->get_ls_local_adapter_inst_guard(ls_id, tablet_id, adapter_guard))) {
      if (OB_HASH_NOT_EXIST == ret) {
        // The adaptor may have been detached after collecting tablet IDs.
        ret = OB_SUCCESS;
        continue;
      } else {
        LOG_WARN("failed to get adapter inst guard", K(ls_id), K(tablet_id), KR(ret));
      }
    } else {
      ObPluginVectorIndexAdaptor *adaptor = adapter_guard.get_adatper();
      if (OB_ISNULL(adaptor)) {
        // do nothing
      } else if (OB_FAIL(adaptor_ptr_set.exist_refactored(reinterpret_cast<int64_t>(adaptor)))) {
        if (ret == OB_HASH_EXIST) {
          // do nothing
          ret = OB_SUCCESS;
        } else if (ret == OB_HASH_NOT_EXIST) {
          ret = OB_SUCCESS;
          if (adaptor != NULL) {
            if (OB_FAIL(adaptor->fill_mem_context_detail_info(buf, buf_len, pos))) {
              LOG_WARN("fill_mem_context_detail_info", K(ret), K(buf_len), K(pos));
            }
          }
          OZ(adaptor_ptr_set.set_refactored(reinterpret_cast<int64_t>(adaptor)));
        } else {
          LOG_WARN("fail to create tablet_id set", K(ret));
        } 
      }
    }
  }

  return ret;
}

int ObPluginVectorIndexUtils::get_mem_context_detail_info(ObPluginVectorIndexService *service,
                                                          ObIArray<ObLSTabletPair> &complete_tablet_ids,
                                                          ObIArray<ObLSTabletPair> &partial_tablet_ids,
                                                          ObIArray<ObLSCacheMgrKeyPair> &cache_keys,
                                                          char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_mem_context_detail_info(service, complete_tablet_ids, buf, buf_len, pos))) {
    LOG_WARN("failed to fill complete adaptor detail info", KR(ret));
  } else if (OB_FAIL(fill_mem_context_detail_info(service, partial_tablet_ids, buf, buf_len, pos))) {
    LOG_WARN("failed to fill partial adaptor detail info", KR(ret));
  } else if (OB_FAIL(fill_ivf_mem_context_detail_info(service, cache_keys, buf, buf_len, pos))) {
    LOG_WARN("failed to fill ivf mem_ctx detail info", KR(ret));
  }

  return ret;
}

int ObPluginVectorIndexUtils::fill_ivf_mem_context_detail_info(ObPluginVectorIndexService *service,
    ObIArray<ObLSCacheMgrKeyPair> &cache_keys, char *buf, int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  hash::ObHashSet<int64_t> mgr_ptr_set;
  if (cache_keys.count() > 0 && OB_FAIL(mgr_ptr_set.create(cache_keys.count()))) {
    LOG_WARN("fail to create mgr ptr set", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cache_keys.count(); ++i) {
    const ObLSCacheMgrKeyPair &pair = cache_keys.at(i);
    ObIvfCacheMgrGuard cache_mgr_guard;
    if (OB_FAIL(service->acquire_ivf_cache_mgr_guard(pair.ls_id_, pair.key_, cache_mgr_guard))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // cache mgr may have been evicted
        continue;
      }
      LOG_WARN("failed to acquire cache mgr guard", KR(ret), K(pair));
    } else {
      ObIvfCacheMgr *mgr = cache_mgr_guard.get_ivf_cache_mgr();
      if (OB_ISNULL(mgr)) {
        // skip null mgr
      } else {
        int tmp_ret = mgr_ptr_set.exist_refactored(reinterpret_cast<int64_t>(mgr));
        if (OB_HASH_EXIST == tmp_ret) {
          // already printed, skip
        } else if (OB_HASH_NOT_EXIST == tmp_ret) {
          if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                  ", \"ivf_cache_%lu\":%ld", mgr->get_table_id(), mgr->get_memory_hold()))) {
            LOG_WARN("failed to print ivf cache mem info", K(ret));
          } else if (OB_FAIL(mgr_ptr_set.set_refactored(reinterpret_cast<int64_t>(mgr)))) {
            LOG_WARN("failed to insert into mgr ptr set", KR(ret));
          }
        } else {
          ret = tmp_ret;
          LOG_WARN("fail to check mgr ptr set", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::release_row_scan_iter(common::ObNewRowIterator *&iter)
{
  int ret = OB_SUCCESS;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  if (OB_ISNULL(tsc_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr", K(ret), KP(tsc_service));
  } else if (OB_FAIL(tsc_service->revert_scan_iter(iter))) {
    LOG_WARN("failed to revert row scan iter", K(ret));
  } else {
    iter = nullptr;
  }
  return ret;
}

static int get_next_non_snap_meta_row(
    ObTableScanIterator *table_scan_iter,
    blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  bool found = false;
  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(table_scan_iter->get_next_row(row))) {
    } else if (OB_ISNULL(row) || row->get_column_count() < 1
        || !ObVectorIndexSegmentMeta::is_vector_index_snap_meta_key(
            row->storage_datums_[0].get_string())) {
      found = true;
    } else {
      LOG_INFO("[VEC_INDEX] skip snap_meta child row in visible row scan",
          "row_key", row->storage_datums_[0].get_string());
    }
  }
  return ret;
}

// use for select
int ObPluginVectorIndexUtils::check_snapshot_iter_need_rescan(common::ObNewRowIterator *snapshot_idx_iter, bool &need_rescan, blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  need_rescan = false;
  row = nullptr;

  ObTableScanIterator *table_scan_iter = static_cast<ObTableScanIterator *>(snapshot_idx_iter);
  if (OB_ISNULL(table_scan_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(snapshot_idx_iter));
  } else if (OB_FAIL(get_next_non_snap_meta_row(table_scan_iter, row))) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("failed to get next row", K(ret));
    }
  } else if (OB_ISNULL(row) || row->get_column_count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid row", K(ret), K(row));
  } else {
    INC_METRIC_VAL(common::ObMetricId::HS_VEC_HNSW_SNAPSHOT_TABLE_SCAN_ROWS, 1);
    if (row->storage_datums_[0].get_string().suffix_match("_meta_data")) {
      need_rescan = false;
    } else if (OB_NOT_NULL(row) && row->get_column_count() == 3 && !row->storage_datums_[2].get_bool()) { // if table 5 has visible row
      need_rescan = true;
    }
  }
  LOG_INFO("check snapshot iter need rescan", K(ret), K(need_rescan));
  return ret;
}

int ObPluginVectorIndexUtils::get_tenant_vector_index_ids(const uint64_t tenant_id, bool &has_ivf_index, common::ObIArray<uint64_t> &table_id_array)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  ObMultiVersionSchemaService &schema_service = ObMultiVersionSchemaService::get_instance();
  if (!schema_service.is_tenant_full_schema(tenant_id)) {
    ret = OB_EAGAIN;
    LOG_INFO("tenant does not has a full schema already, maybe server is restart, need retry!");
  } else if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_vector_info_index_ids_in_tenant(tenant_id, has_ivf_index, table_id_array))) {
    LOG_WARN("fail to get table ids in tenant", KR(ret), K(tenant_id));
  } else {
    LOG_INFO("get_tenant_vector_index_ids result", K(tenant_id), K(has_ivf_index),
             "table_count", table_id_array.count());
  }
  return ret;
}

int ObPluginVectorIndexUtils::check_tenant_has_vector_index(
    const uint64_t tenant_id, bool &has_vector_index)
{
  int ret = OB_SUCCESS;
  bool has_ivf_index = false;
  ObArray<uint64_t> table_id_array;
  has_vector_index = false;
  if (OB_FAIL(get_tenant_vector_index_ids(tenant_id, has_ivf_index, table_id_array))) {
    LOG_WARN("failed to get tenant vector index ids", KR(ret), K(tenant_id));
  } else {
    has_vector_index = (table_id_array.count() > 0);
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_ls_vector_mem_from_virtual_table_(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const common::ObAddr &src_addr,
    int64_t &ls_vector_mem)
{
  int ret = OB_SUCCESS;
  ls_vector_mem = 0;
  ObSqlString sql;
  char ip_buf[OB_IP_STR_BUFF] = {};
  int32_t svr_port = src_addr.get_port();

  if (OB_UNLIKELY(!src_addr.ip_to_string(ip_buf, sizeof(ip_buf)))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert src_addr ip to string", K(ret), K(src_addr));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("sql proxy is null", K(ret));
  } else if (OB_FAIL(sql.assign_fmt(
      "SELECT CAST(SUM(CAST(JSON_VALUE(statistics, '$.incr_mem_hold') AS SIGNED)"
      " + CAST(JSON_VALUE(statistics, '$.snap_mem_hold') AS SIGNED)) AS SIGNED) AS ls_vector_mem"
      " FROM %s"
      " WHERE tenant_id = %lu AND ls_id = %ld AND svr_ip = '%s' AND svr_port = %d",
      OB_ALL_VIRTUAL_VECTOR_INDEX_INFO_TNAME,
      tenant_id, ls_id.id(), ip_buf, svr_port))) {
    LOG_WARN("failed to construct sql to get ls vector mem", K(ret), K(tenant_id), K(ls_id));
  } else {
    HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
        LOG_WARN("failed to read ls vector mem from virtual table", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get sql result", K(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          ls_vector_mem = 0;
        } else {
          LOG_WARN("failed to get next row", K(ret), K(sql));
        }
      } else {
        EXTRACT_INT_FIELD_MYSQL_SKIP_RET(*result, "ls_vector_mem", ls_vector_mem, int64_t);
        if (OB_FAIL(ret)) {
          ret = OB_SUCCESS;
          ls_vector_mem = 0;
          LOG_WARN("ls_vector_mem column is null, treat as 0", K(sql));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::check_vector_memory_for_migrate(
    const uint64_t tenant_id,
    const ObLSID &ls_id,
    const common::ObAddr &src_addr)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t ls_vector_mem = 0;
  int64_t vector_memory_limit = 0;
  int64_t tenant_remain_mem = 0;
  int64_t all_vsag_use_mem = 0;
  int64_t tx_share_limit = 0;
  int64_t current_tx_share_hold = 0;
  ObPluginVectorIndexService *vec_idx_svc = MTL(ObPluginVectorIndexService *);

  if (OB_TMP_FAIL(get_ls_vector_mem_from_virtual_table_(
          tenant_id, ls_id, src_addr, ls_vector_mem))) {
    // non-fatal: virtual table may be unavailable during restore, skip check
    LOG_WARN("failed to get ls vector mem, skip vector memory check",
        KR(tmp_ret), K(ls_id), K(src_addr));
  } else if (ls_vector_mem <= 0) {
    // no vector memory on source LS, nothing to check
    LOG_INFO("no vector memory on source LS, nothing to check",
        K(tenant_id), K(ls_id), K(src_addr));
  } else if (OB_FAIL(ObPluginVectorIndexHelper::get_vector_memory_limit_size(
      tenant_id, vector_memory_limit))) {
    LOG_WARN("failed to get vector memory limit size", K(ret), K(tenant_id));
  } else {
    tenant_remain_mem = lib::get_tenant_memory_remain(tenant_id);
    if (OB_NOT_NULL(vec_idx_svc) && OB_NOT_NULL(vec_idx_svc->get_all_vsag_use_mem())) {
      all_vsag_use_mem = static_cast<int64_t>(
          ATOMIC_LOAD(vec_idx_svc->get_all_vsag_use_mem()));
    }
    ObSharedMemAllocMgr *shared_mem_mgr = MTL(ObSharedMemAllocMgr *);
    if (OB_NOT_NULL(shared_mem_mgr)) {
      tx_share_limit = shared_mem_mgr->share_resource_throttle_tool()
          .get_resource_limit<FakeAllocatorForTxShare>();
      current_tx_share_hold = shared_mem_mgr->memstore_allocator().hold()
          + shared_mem_mgr->tx_data_allocator().hold()
          + shared_mem_mgr->mds_memstore_allocator().hold()
          + shared_mem_mgr->vector_allocator().hold();
    }
    if (ls_vector_mem + all_vsag_use_mem > vector_memory_limit
        || ls_vector_mem > tenant_remain_mem
        || (tx_share_limit > 0
            && current_tx_share_hold + ls_vector_mem > tx_share_limit)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("vector memory check failed before migrate ls, not enough vector memory",
          K(ret), K(ls_vector_mem), K(all_vsag_use_mem),
          K(vector_memory_limit), K(tenant_remain_mem),
          K(tx_share_limit), K(current_tx_share_hold),
          K(tenant_id), K(ls_id), K(src_addr));
    } else {
      LOG_INFO("vector memory check passed before migrate ls",
          K(ls_vector_mem), K(all_vsag_use_mem),
          K(vector_memory_limit), K(tenant_remain_mem),
          K(tx_share_limit), K(current_tx_share_hold),
          K(tenant_id), K(ls_id), K(src_addr));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_current_read_scn(share::SCN &current_scn)
{
  int ret = OB_SUCCESS;
  const int64_t DEFAULT_TIMEOUT = GCONF.internal_sql_execute_timeout;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  if (OB_ISNULL(txs)) {
    ret = OB_ERR_SYS;
    LOG_WARN("trans service is null", KR(ret));
  } else {
    ObTimeoutCtx timeout_ctx;
    if (OB_FAIL(ObShareUtil::set_default_timeout_ctx(timeout_ctx,
                                                     DEFAULT_TIMEOUT))) {
      LOG_WARN("fail to set default timeout ctx", KR(ret));
    } else if (OB_FAIL(txs->get_read_snapshot_version(
                   timeout_ctx.get_abs_timeout(), current_scn))) {
      LOG_WARN("get read snapshot version", KR(ret));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_lob_tablet_id(const ObLSID &ls_id, const ObTabletID &data_tablet_id, ObTabletID &lob_meta_tablet_id, ObTabletID &lob_piece_tablet_id)
{
  int ret = OB_SUCCESS;
 // get lob tablet id
 HEAP_VARS_3((ObLSHandle, ls_handle), (ObTabletHandle, data_tablet_handle), (ObTabletBindingMdsUserData, ddl_data))
 {
   ObLSService *ls_service = nullptr;
   if (OB_ISNULL(ls_service = MTL(ObLSService *))) {
     ret = OB_ERR_UNEXPECTED;
     LOG_WARN("unexpected err", K(ret), K(MTL_ID()));
   } else if (OB_FAIL(ls_service->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
     LOG_WARN("failed to get log stream", K(ret), K(ls_id));
   } else if (OB_ISNULL(ls_handle.get_ls())) {
     ret = OB_ERR_UNEXPECTED;
     LOG_ERROR("ls should not be null", K(ret));
   } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(data_tablet_id, data_tablet_handle))) {
     LOG_WARN("fail to get tablet handle", K(ret), K(data_tablet_id));
   } else if (OB_FAIL(data_tablet_handle.get_obj()->get_ddl_data(ddl_data))) {
     LOG_WARN("failed to get ddl data from tablet", K(ret), K(data_tablet_handle));
   } else {
     lob_meta_tablet_id = ddl_data.lob_meta_tablet_id_;
     lob_piece_tablet_id = ddl_data.lob_piece_tablet_id_;
   }
 }
 return ret;
}
int ObPluginVectorIndexUtils::get_migration_adaptor_list(
    const ObLSID &ls_id,
    common::ObIArray<storage::ObMigrationVectorIndexAdaptorMeta> &adaptor_metas)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vi_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vi_mgr = nullptr;
  if (OB_ISNULL(vi_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service not available", K(ret), K(ls_id));
  } else if (OB_FAIL(vi_service->acquire_vector_index_mgr(ls_id, vi_mgr))) {
    LOG_WARN("failed to acquire vector index manager", K(ret), K(ls_id));
  } else if (OB_ISNULL(vi_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index manager is null", K(ret), K(ls_id));
  } else if (OB_FAIL(vi_mgr->get_migration_adaptor_list(adaptor_metas))) {
    LOG_WARN("failed to get migration adaptor list", K(ret), K(ls_id));
  }
  return ret;
}

int ObPluginVectorIndexUtils::batch_create_adaptor_shells(
    const ObLSID &ls_id,
    const common::ObIArray<storage::ObMigrationVectorIndexAdaptorMeta> &adaptor_metas)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vi_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vi_mgr = nullptr;
  if (OB_ISNULL(vi_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service not available", K(ret), K(ls_id));
  } else if (OB_FAIL(vi_service->acquire_vector_index_mgr(ls_id, vi_mgr))) {
    LOG_WARN("failed to acquire vector index manager", K(ret), K(ls_id));
  } else if (OB_ISNULL(vi_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index manager is null", K(ret), K(ls_id));
  } else if (OB_FAIL(vi_mgr->batch_create_adaptor_shells(
          vi_service->get_adaptor_allocator(), adaptor_metas))) {
    LOG_WARN("failed to batch create adaptor shells", K(ret), K(ls_id));
  }
  return ret;
}

static int erase_tenant_adaptor_shell_if_same_as_ls_shell_(
    const common::ObTabletID &inc_tablet_id,
    ObPluginVectorIndexAdapterGuard &local_guard,
    ObPluginVectorIndexAdapterGuard &tenant_guard,
    ObPluginVectorIndexService *vi_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(vi_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[MIG VEC] erase_tenant_adaptor_shell_if_same_as_ls_shell_: vi_service is null",
        K(ret), K(inc_tablet_id));
  // Only erase tenant_map if it still points to our migration shell
  // (same pointer as ls_map). If they differ, the scheduler has already
  // replaced it with a valid adaptor, so tenant_map must stay untouched.
  } else {
    ObPluginVectorIndexAdaptor *local_adaptor = local_guard.get_adatper();
    ObPluginVectorIndexAdaptor *tenant_adaptor = tenant_guard.get_adatper();
    const bool same_adaptor = OB_NOT_NULL(local_adaptor)
                              && OB_NOT_NULL(tenant_adaptor)
                              && local_adaptor == tenant_adaptor;
    if (same_adaptor) {
      bool erased = false;
      if (OB_FAIL(vi_service->erase_tenant_adapter_if_same(
              inc_tablet_id, tenant_adaptor, erased))) {
        LOG_WARN("[MIG VEC] erase_tenant_adaptor_shell_if_same_as_ls_shell_: failed to erase from tenant_map",
            K(ret), K(inc_tablet_id), KP(tenant_adaptor));
      } else if (!erased) {
        LOG_INFO("[MIG VEC] tenant adaptor changed before migration shell erase",
            K(inc_tablet_id), KP(tenant_adaptor));
      }
    } else {
      LOG_INFO("[MIG VEC] ls_map and tenant_map adaptor shell differ",
          K(inc_tablet_id), KP(local_adaptor), KP(tenant_adaptor),
          "local_ls_ref", OB_ISNULL(local_adaptor) ? -1 : local_adaptor->get_ls_ref(),
          "tenant_ls_ref", OB_ISNULL(tenant_adaptor) ? -1 : tenant_adaptor->get_ls_ref());
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::set_adaptor_mig_state(
    const ObLSID &ls_id,
    const ObTabletID &inc_tablet_id, ObAdaptorMigState state)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vi_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vi_mgr = nullptr;
  ObPluginVectorIndexAdapterGuard guard;
  if (OB_ISNULL(vi_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] vi_service not available", K(ret), K(ls_id), K(inc_tablet_id));
  } else if (OB_FAIL(vi_service->acquire_vector_index_mgr(ls_id, vi_mgr))) {
    LOG_WARN("[MIG VEC] failed to acquire vi_mgr", K(ret), K(ls_id), K(inc_tablet_id));
  } else if (OB_FAIL(vi_mgr->get_adapter_inst_guard(inc_tablet_id, guard))) {
    if (OB_HASH_NOT_EXIST == ret) {
      LOG_INFO("[MIG VEC] adaptor not exist, skip set mig state", K(ret), K(ls_id), K(inc_tablet_id), K(state));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("[MIG VEC] failed to get ls adaptor", K(ret), K(ls_id), K(inc_tablet_id), K(state));
    }
  } else if (OB_NOT_NULL(guard.get_adatper())) {
    guard.get_adatper()->set_mig_state(state);
    LOG_DEBUG("[MIG VEC] set adaptor mig_state", K(ls_id), K(inc_tablet_id), K(state),
        KPC(guard.get_adatper()));
  }
  return ret;
}

int ObPluginVectorIndexUtils::cleanup_adaptor_shells(
    const ObLSID &ls_id,
    const common::ObIArray<storage::ObMigrationVectorIndexAdaptorMeta> &adaptor_metas)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vi_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vi_mgr = nullptr;
  if (OB_ISNULL(vi_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] cleanup_adaptor_shells: vi_service not available", K(ret), K(ls_id));
  } else if (OB_FAIL(vi_service->acquire_vector_index_mgr(ls_id, vi_mgr))) {
    LOG_WARN("[MIG VEC] cleanup_adaptor_shells: failed to acquire vi_mgr",
        K(ret), K(ls_id));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < adaptor_metas.count(); ++i) {
      const ObTabletID &inc_tablet_id = adaptor_metas.at(i).inc_tablet_id_;
      ObPluginVectorIndexAdapterGuard local_guard;
      ObPluginVectorIndexAdapterGuard tenant_guard;
      // Fetch both pointers before any mutation so we can compare them later.
      if (OB_FAIL(vi_service->get_ls_local_adapter_inst_guard(ls_id, inc_tablet_id, local_guard))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[MIG VEC] cleanup_adaptor_shells: failed to get local adaptor",
              K(ret), K(ls_id), K(inc_tablet_id));
        }
      } else if (OB_FAIL(vi_service->get_tenant_adapter_inst_guard(inc_tablet_id, tenant_guard))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("[MIG VEC] cleanup_adaptor_shells: failed to get tenant adaptor",
              K(ret), K(inc_tablet_id));
        }
      }
      if (OB_SUCC(ret)) {
        bool detached = false;
        // always detach from ls_map, no matter whether the tenant_map is the same as the ls_map
        if (OB_FAIL(vi_mgr->detach_adapter(inc_tablet_id, detached))) {
          LOG_WARN("[MIG VEC] cleanup_adaptor_shells: failed to detach from ls_map",
              K(ret), K(ls_id), K(inc_tablet_id));
        // Only erase tenant_map if it still points to the same shell we saw in
        // ls_map. If the scheduler has already replaced tenant_map with a valid
        // adaptor, leave it untouched.
        } else if (OB_FAIL(erase_tenant_adaptor_shell_if_same_as_ls_shell_(
                       inc_tablet_id, local_guard, tenant_guard, vi_service))) {
          LOG_WARN("[MIG VEC] cleanup_adaptor_shells: failed to erase from tenant_map",
              K(ret), K(ls_id), K(inc_tablet_id));
        }
      }
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::cleanup_tenant_adaptor_shell_for_retry(
    const ObLSID &ls_id,
    const common::ObTabletID &inc_tablet_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vi_service = MTL(ObPluginVectorIndexService *);
  if (OB_ISNULL(vi_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[MIG VEC] cleanup_tenant_adaptor_shell_for_retry: vi_service not available",
        K(ret), K(ls_id), K(inc_tablet_id));
  } else {
    ObPluginVectorIndexAdapterGuard local_guard;
    ObPluginVectorIndexAdapterGuard tenant_guard;
    // Inner retry must only clean tenant_map for the current adaptor, but still
    // needs the same pointer check to avoid erasing a replacement installed by
    // the scheduler.
    if (OB_FAIL(vi_service->get_ls_local_adapter_inst_guard(ls_id, inc_tablet_id, local_guard))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[MIG VEC] cleanup_tenant_adaptor_shell_for_retry: failed to get local adaptor",
            K(ret), K(ls_id), K(inc_tablet_id));
      }
    } else if (OB_FAIL(vi_service->get_tenant_adapter_inst_guard(inc_tablet_id, tenant_guard))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("[MIG VEC] cleanup_tenant_adaptor_shell_for_retry: failed to get tenant adaptor",
            K(ret), K(inc_tablet_id));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(erase_tenant_adaptor_shell_if_same_as_ls_shell_(
            inc_tablet_id, local_guard, tenant_guard, vi_service))) {
      LOG_WARN("[MIG VEC] cleanup_tenant_adaptor_shell_for_retry: failed to erase from tenant_map",
          K(ret), K(ls_id), K(inc_tablet_id));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::try_reuse_adaptor_from_tenant_map(
    const ObLSID &ls_id,
    const common::ObTabletID &inc_tablet_id, bool &reused)
{
  int ret = OB_SUCCESS;
  reused = false;
  ObPluginVectorIndexService *vi_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vi_mgr = nullptr;
  if (OB_ISNULL(vi_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service not available", K(ret), K(ls_id));
  } else if (OB_FAIL(vi_service->acquire_vector_index_mgr(ls_id, vi_mgr))) {
    LOG_WARN("failed to acquire vector index manager", K(ret), K(ls_id), K(inc_tablet_id));
  } else if (OB_ISNULL(vi_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index manager is null", K(ret), K(ls_id), K(inc_tablet_id));
  } else {
    if (OB_FAIL(vi_mgr->try_reuse_adaptor_from_tenant_map(inc_tablet_id, reused))) {
      LOG_WARN("failed to reuse adaptor from tenant map", K(ret), K(inc_tablet_id));
    }
  }
  return ret;
}

int ObPluginVectorIndexUtils::enqueue_mem_sync_task(
    const ObLSID &ls_id,
    common::ObTabletID inc_tablet_id, const int64_t inc_table_id)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexService *vi_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexMgr *vi_mgr = nullptr;
  if (OB_ISNULL(vi_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index service not available", K(ret), K(ls_id));
  } else if (OB_FAIL(vi_service->acquire_vector_index_mgr(ls_id, vi_mgr))) {
    LOG_WARN("failed to acquire vector index manager", K(ret), K(ls_id));
  } else if (OB_ISNULL(vi_mgr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("vector index manager is null", K(ret), K(ls_id));
  } else if (OB_FAIL(vi_mgr->enqueue_mem_sync_task(inc_tablet_id, inc_table_id))) {
    LOG_WARN("failed to enqueue mem sync task", K(ret), K(ls_id), K(inc_tablet_id));
  }
  return ret;
}

int ObPluginVectorIndexUtils::get_lob_store_column_idx(
    const share::schema::ObTableDMLParam &table_dml_param,
    const uint64_t data_col_id,
    uint64_t &lob_store_column_idx)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchemaParam &table_param = table_dml_param.get_data_table();
  const storage::ObTableReadInfo &read_info = table_param.get_read_info();
  int32_t column_offset = OB_INVALID_INDEX;
  lob_store_column_idx = 0;
  if (OB_FAIL(table_param.get_col_map().get(data_col_id, column_offset))) {
    LOG_WARN("get param idx by column_id fail", K(ret), K(data_col_id));
  } else if (column_offset < 0 || column_offset >= read_info.get_columns_index().count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid param idx for lob column", K(ret), K(column_offset), K(data_col_id),
             K(read_info.get_request_count()));
  } else if (FALSE_IT(lob_store_column_idx = static_cast<uint64_t>(read_info.get_columns_index().at(column_offset)))) {
  } else if (OB_UNLIKELY(lob_store_column_idx == OB_INVALID_INDEX)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob store column idx", KR(ret), K(lob_store_column_idx));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
