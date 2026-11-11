/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX COMMON
#include "share/ob_virtual_table_iterator.h"

#include "lib/charset/ob_charset.h"
#include "share/external_table/ob_external_object_ctx.h"
#include "share/ob_i_tablet_scan.h"
#include "share/schema/ob_table_schema.h"
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/basic/ob_pushdown_filter.h"
#include "sql/engine/expr/ob_expr_column_conv.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/ob_sql_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;
using namespace oceanbase::share::schema;
namespace oceanbase
{
namespace common
{

void ObVirtualTableIterator::reset()
{
  output_column_ids_.reset();
  reserved_column_cnt_ = 0;
  schema_guard_ = NULL;
  table_schema_ = NULL;
  index_schema_ = NULL;
  //Since ObObj's destructor does not do meaningful operations, in order to save performance, ObObj's destructor call is omitted, and the cells_memory is directly released.
  if (OB_LIKELY(NULL != allocator_ && NULL != cur_row_.cells_)) {
    allocator_->free(cur_row_.cells_);
  }
  cur_row_.cells_ = NULL;
  cur_row_.count_ = 0;
  key_ranges_.reset();
  row_calc_buf_.reset();
  reset_convert_ctx();
  allocator_ = NULL;
  session_ = NULL;
  uc_catalog_ctx_.reset();
  sql_schema_guard_.reset();
  pd_cols_project_.reset();
  fill_column_ids_ = &output_column_ids_;
}

void ObVirtualTableIterator::reset_convert_ctx()
{
  if (OB_LIKELY(NULL != allocator_ && NULL != convert_row_.cells_)) {
    allocator_->free(convert_row_.cells_);
  }
  saved_key_ranges_.reset();
  cols_schema_.reset();
  convert_row_.cells_ = NULL;
  convert_row_.count_ = 0;
  need_convert_ = false;
  convert_alloc_.reset();
}

int ObVirtualTableIterator::free_convert_ctx()
{
  int ret = OB_SUCCESS;
  if (!need_convert_) {
  } else if (OB_UNLIKELY(convert_row_.count_ < 0 || NULL == convert_row_.cells_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("convert row is not init", K(ret), K(convert_row_));
  } else {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else {
      key_ranges_.reset();
      if (OB_FAIL(key_ranges_.assign(saved_key_ranges_))) {
        LOG_WARN("failed to assign key ranges", K(ret));
      }
      saved_key_ranges_.reset();
      //Since ObObj's destructor does not do meaningful operations, in order to save performance, ObObj's destructor call is omitted, and the cells_memory is directly released.
      allocator_->free(convert_row_.cells_);
      convert_row_.cells_ = NULL;
      convert_row_.count_ = 0;
      convert_alloc_.reset();
      cols_schema_.reset();
    }
  }
  return ret;
}

int ObVirtualTableIterator::convert_key(const ObRowkey &src, 
                                        ObRowkey &dst, 
                                        common::ObIArray<const ObColumnSchemaV2*> &key_cols, 
                                        bool is_start_key, 
                                        ObBorderFlag &border_flag)
{
  int ret = OB_SUCCESS;
  if (src.get_obj_cnt() > 0) {
    const ObObj *src_key_objs = src.get_obj_ptr();
    void *tmp_ptr = NULL;
    ObObj *new_key_obj = NULL;
    tmp_ptr = allocator_->alloc(src.get_obj_cnt() * sizeof(ObObj));
    if (OB_ISNULL(tmp_ptr)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new obj", K(ret));
    } else if (OB_ISNULL(new_key_obj = new (tmp_ptr) ObObj[src.get_obj_cnt()])) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc new obj", K(ret));
    } else if (src.get_obj_cnt() > key_cols.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("keys are not match with columns", K(ret));
    }
    lib::CompatModeGuard g(lib::Worker::CompatMode::MYSQL);
    const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session_);
    ObCastCtx cast_ctx(allocator_, &dtc_params, CM_NONE, ObCharset::get_system_collation());
    for (int64_t nth_obj = 0; OB_SUCC(ret) && nth_obj < src.get_obj_cnt(); ++nth_obj) {
      const ObObj &src_obj = src_key_objs[nth_obj];
      if (src_obj.is_min_value()) {
        new_key_obj[nth_obj].set_min_value();
      } else if (src_obj.is_max_value()) {
        new_key_obj[nth_obj].set_max_value();
      } else if (src_obj.is_null()) {
        new_key_obj[nth_obj].set_null();
      } else if (ObIntType == key_cols.at(nth_obj)->get_data_type() &&
                 src_key_objs[nth_obj].is_number()) {
        const number::ObNumber &nmb = src_key_objs[nth_obj].get_number();
        int64_t int_value = 0;
        int cmp = 0;
        if (nmb.is_valid_int64(int_value)) {
          new_key_obj[nth_obj].set_int(int_value);
        } else {
          if (OB_FAIL(sql::ObSQLUtils::convert_number_to_int_with_trunc(nmb, int_value, cmp))) {
            LOG_WARN("fail to convert number to int", K(ret), K(nmb));
          } else {
            new_key_obj[nth_obj].set_int(int_value);
            if (OB_FAIL(sql::ObSQLUtils::adjust_key_border_for_number_convert(cmp, is_start_key, 
                                                                               nth_obj == src.get_obj_cnt() - 1,
                                                                               nth_obj, 
                                                                               static_cast<int64_t>(src.get_obj_cnt()), 
                                                                               new_key_obj, border_flag))) {
              LOG_WARN("fail to adjust key border", K(ret), K(cmp), K(is_start_key));
            }
          }
        }
      } else {
        if (OB_FAIL(ObObjCaster::to_type(key_cols.at(nth_obj)->get_data_type(),
                                        cast_ctx,
                                        src_key_objs[nth_obj],
                                        new_key_obj[nth_obj]))) {
          LOG_WARN("fail to cast obj", K(ret), K(allocator_), K(key_cols.at(nth_obj)->get_data_type()), K(src_key_objs[nth_obj]));
        }
      }
    }//end for
    if (OB_SUCC(ret)) {
      dst.assign(new_key_obj, src.get_obj_cnt());
    }
  }
  return ret;
}

// get origin type of keys in mysql mode
// first find the column name that is same as origin virtual table in mysql mode
// then find column type by column name
int ObVirtualTableIterator::get_key_cols(common::ObIArray<const ObColumnSchemaV2*> &key_cols)
{
  int ret = OB_SUCCESS;
  common::ObArray<uint64_t> column_ids;
  key_cols.reset();
  if (need_convert_ && !key_ranges_.empty()) {
    if (index_schema_->get_rowkey_info().is_valid()
        && index_schema_->get_rowkey_info().get_column_ids(column_ids)) {
      LOG_WARN("get key column ids failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      common::ObArray<const ObString*> column_names;
      for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
        const ObColumnSchemaV2 * col_schema = table_schema_->get_column_schema(column_ids.at(i));
        if (OB_ISNULL(col_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column schema is null", K(ret));
        } else if (OB_FAIL(column_names.push_back(&col_schema->get_column_name_str()))) {
          LOG_WARN("fail to push back column name", K(ret));
        }
      }
      if (OB_SUCC(ret) && column_ids.count() != column_names.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column infos are not match ", K(ret));
      }
      // get origin key type by column name
      if (OB_SUCC(ret)) {
        const ObTableSchema *org_table_schema = NULL;
        uint64_t org_table_id = get_origin_tid_by_oracle_mapping_tid(table_schema_->get_table_id());
        if (OB_INVALID_ID == org_table_id) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get origin table id", K(ret), K(table_schema_->get_table_id()));
        } else if (OB_FAIL(schema_guard_->get_table_schema(OB_SYS_TENANT_ID, org_table_id, org_table_schema))) {
          LOG_WARN("get table schema failed", K(org_table_id), K(ret));
        } else if (NULL == org_table_schema) {
          ret = OB_TABLE_NOT_EXIST;
          LOG_WARN("get table schema failed", K(ret));
        } else {
          // switch mysql mode, find column schema by column name
          lib::CompatModeGuard g(lib::Worker::CompatMode::MYSQL);
          for (int64_t i = 0; OB_SUCC(ret) && i < column_names.count(); ++i) {
            const ObString *column_name = column_names.at(i);
            const ObColumnSchemaV2 *col_schema = org_table_schema->get_column_schema(*column_name);
            if (OB_ISNULL(col_schema)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("column schema is null", K(ret), K(*column_name));
            } else if (OB_FAIL(key_cols.push_back(col_schema))) {
              LOG_WARN("fail to push back column name", K(ret));
            }
          }
          if (OB_SUCC(ret) && key_cols.count() != column_names.count()) {
            LOG_WARN("column infos are not match ", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

// If key objects are in oracle mode, then need to convert to obj in mysql mode
// and it's find the origin type in mysql mode
// every virtual table in oracle mode must be match with one virtual table in mysql mode
int ObVirtualTableIterator::convert_key_ranges()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (!key_ranges_.empty()) {
    common::ObSEArray<common::ObNewRange, 16> tmp_range;
    common::ObArray<const ObColumnSchemaV2*> key_cols;
    if (OB_FAIL(get_key_cols(key_cols))) {
      LOG_WARN("failed to get key types", K(ret));
    } else if (key_cols.empty() && 1 == key_ranges_.count() && key_ranges_.at(0).is_whole_range()) {
      ObNewRange new_range;
      new_range.table_id_ = key_ranges_.at(0).table_id_;
      new_range.set_whole_range();
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < key_ranges_.count(); ++i) {
        ObNewRange new_range;
        new_range.table_id_ = key_ranges_.at(i).table_id_;
        new_range.border_flag_ = key_ranges_.at(i).border_flag_;
        if (OB_FAIL(convert_key(key_ranges_.at(i).start_key_, 
                                new_range.start_key_, 
                                key_cols,
                                true,
                                new_range.border_flag_))) {
          LOG_WARN("fail to convert start key", K(ret), K(allocator_));
        } else if (OB_FAIL(convert_key(key_ranges_.at(i).end_key_, 
                                       new_range.end_key_, 
                                       key_cols,
                                       false,
                                       new_range.border_flag_))) {
          LOG_WARN("fail to convert end key", K(ret), K(allocator_));
        } else if (OB_FAIL(tmp_range.push_back(new_range))) {
          LOG_WARN("fail to push back new range", K(ret), K(allocator_));
        }
      }//end for
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(saved_key_ranges_.assign(key_ranges_))) {
        LOG_WARN("fail to assign new range", K(ret), K(allocator_));
      } else {
        key_ranges_.reset();
        if (OB_FAIL(key_ranges_.assign(tmp_range))) {
          LOG_WARN("fail to assign new range", K(ret), K(allocator_));
        }
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::init_convert_ctx()
{
  int ret = OB_SUCCESS;
  row_calc_buf_.set_tenant_id(table_schema_->get_tenant_id());
  convert_alloc_.set_tenant_id(table_schema_->get_tenant_id());
  const ObDataTypeCastParams dtc_params = ObBasicSessionInfo::create_dtc_params(session_);
  ObCastCtx cast_ctx(&convert_alloc_, &dtc_params, CM_NONE, table_schema_->get_collation_type());
  cast_ctx_ = cast_ctx;

  if (need_convert_) {
    ObObj *cells = NULL;
    void *tmp_ptr = NULL;
    if (OB_UNLIKELY(NULL == allocator_ || NULL == table_schema_ || NULL == session_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("data member is not init", K(ret), K(allocator_));
    } else if (OB_ISNULL(tmp_ptr = allocator_->alloc(reserved_column_cnt_ <= 0 ? 1 * sizeof(ObObj): reserved_column_cnt_ * sizeof(ObObj)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      COMMON_LOG(ERROR, "fail to alloc cells", K(ret), K(reserved_column_cnt_));
    } else if (OB_ISNULL(cells = new (tmp_ptr) ObObj[reserved_column_cnt_ <= 0 ? 1 : reserved_column_cnt_])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to new cell array", K(ret), K(reserved_column_cnt_));
    } else {
      convert_row_.cells_ = cells;
      convert_row_.count_ = reserved_column_cnt_;
      if (OB_FAIL(convert_key_ranges())) {
        LOG_WARN("fail to convert key ranges", K(ret), K(key_ranges_), K(reserved_column_cnt_));
      }
    }
  }
  LOG_DEBUG("key ranges", K(ret), K(key_ranges_));
  return ret;
}

int ObVirtualTableIterator::open()
{
  int ret = OB_SUCCESS;
  void *tmp_ptr = NULL;
  ObObj *cells = NULL;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is not init", K(ret), K(allocator_));
  } else if (OB_ISNULL(scan_param_)
             || (NULL != scan_param_->output_exprs_ && NULL == scan_param_->op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(tmp_ptr = allocator_->alloc( (reserved_column_cnt_ > 0 ? reserved_column_cnt_ : 1) * sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(ERROR, "fail to alloc cells", K(ret), K(reserved_column_cnt_));
  } else if (OB_ISNULL(cells = new (tmp_ptr) ObObj[(reserved_column_cnt_ > 0 ? reserved_column_cnt_ : 1)])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to new cell array", K(ret), K(reserved_column_cnt_));
  } else if (OB_FAIL(init_sql_schema_guard_())) {
    LOG_WARN("failed to init SqlSchemaGuard", K(ret));
  } else {
    cur_row_.cells_ = cells;
    cur_row_.count_ = reserved_column_cnt_;
    if (OB_FAIL(init_convert_ctx())) {
      LOG_WARN("fail to init convert context", K(ret));
    } else if (OB_FAIL(inner_open())) {
      LOG_WARN("fail to inner open", K(ret));
    } else if (OB_FAIL(init_pd_cols_project())) {
      LOG_WARN("failed to init pushdown column ids", K(ret));
    } 
  }
  return ret;
}

int ObVirtualTableIterator::get_all_columns_schema()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
    const uint64_t column_id = output_column_ids_.at(i);
    const ObColumnSchemaV2 *col_schema = table_schema_->get_column_schema(column_id);
    if (OB_ISNULL(col_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("col_schema is NULL", K(ret), K(column_id));
    } else if (OB_FAIL(cols_schema_.push_back(col_schema))) {
      LOG_WARN("failed to push back column schema", K(ret));
    }
  }
  return ret;
}

int ObVirtualTableIterator::convert_output_row(ObNewRow *&cur_row, bool is_pd_filter_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current row is NULL", K(ret));
  } else if (!need_convert_) {
    // don't convert
  } else {
    convert_alloc_.reuse();
    if (cols_schema_.empty() && OB_FAIL(get_all_columns_schema())) {
      LOG_WARN("failed to get columns schema", K(ret));
    }
    const int64_t col_cnt = is_pd_filter_row ? pd_cols_project_.count() : output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const int64_t index = is_pd_filter_row ? pd_cols_project_.at(i) : i;
      const uint64_t column_id = output_column_ids_.at(index);
      const ObColumnSchemaV2 *col_schema = cols_schema_.at(index);
      if (cur_row->get_cell(i).is_null()
          || (cur_row->get_cell(i).is_string_type() && 0 == cur_row->get_cell(i).get_data_length())
          || ob_is_empty_lob(cur_row->get_cell(i))
          || (cur_row->get_cell(i).is_timestamp() && cur_row->get_cell(i).get_timestamp() <= 0)) {
        convert_row_.cells_[i].set_null();
      } else if (OB_FAIL(ObObjCaster::to_type(col_schema->get_data_type(),
                                              col_schema->get_collation_type(),
                                              cast_ctx_,
                                              cur_row->get_cell(i),
                                              convert_row_.cells_[i]))) {
        LOG_WARN("failed to cast obj in oracle mode", K(ret), K(column_id));
      }
    }
    cur_row = &convert_row_;
  }
  return ret;
}

int ObVirtualTableIterator::get_next_unfiltered_row(common::ObNewRow *&cur_row)
{
  int ret = OB_SUCCESS;
  cur_row = nullptr;
  if (!pd_cols_project_.empty()) {
    bool filtered = false;
    fill_column_ids_ = &scan_param_->vt_pd_colums_ids_;
    do {
      scan_param_->op_->clear_evaluated_flag();
      if (filtered) {
        // The previous row was discarded. Keep an accepted row alive through
        // fill_full_columns() and until the caller requests the next row.
        row_calc_buf_.reuse();
      }
      if (OB_FAIL(inner_get_next_row(cur_row))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next pushdown filter row", K(ret));
        }
      } else if (OB_ISNULL(cur_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("succ to get next pushdown filter row, but row is NULL", K(ret));
      } else if (OB_FAIL(convert_output_row(cur_row, true))) {
        LOG_WARN("failed to convert row", K(ret));
      } else if (OB_FAIL(check_type_and_convert_string(cur_row, true))) {
        LOG_WARN("failed to check type and convert string", K(ret));
      } else if (OB_FAIL(project_pd_filter_columns(cur_row))) {
        LOG_WARN("failed to project filter columns", K(ret));
      } else if (OB_FAIL(ObOperator::filter_row(scan_param_->op_->get_eval_ctx(),
                                                *scan_param_->op_filters_, filtered))) {
        LOG_WARN("failed to filter row", K(ret));
      } else if (filtered && OB_FAIL(try_check_status())) {
        LOG_WARN("iterate virtual table row failed", KR(ret));
      }
    } while (OB_SUCC(ret) && filtered);
    if (OB_SUCC(ret)) {
      fill_column_ids_ = &output_column_ids_;
      if (OB_FAIL(fill_full_columns(cur_row))) {
        LOG_WARN("failed to fill full columns", K(ret));
      }
    }
  } else {
    fill_column_ids_ = &output_column_ids_;
    if (OB_FAIL(inner_get_next_row(cur_row))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to inner get next row", K(ret), KPC(scan_param_));
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::get_next_row(ObNewRow *&row)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(scan_param_ != nullptr? scan_param_->index_id_ : 0);
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(tablet_id_, scan_param_ != nullptr? scan_param_->index_id_ : 0);
  int ret = OB_SUCCESS;
  ObNewRow *cur_row = NULL;
  row_calc_buf_.reuse();
  const int64_t abs_timeout_ts = get_scan_param()->timeout_;
  if (ObClockGenerator::getClock() > abs_timeout_ts) {
    ret = OB_TIMEOUT;
    LOG_WARN("iterate virtual table row timeout", KR(ret), KTIME(abs_timeout_ts));
  } else if (OB_FAIL(try_check_status())) {
    LOG_WARN("iterate virtual table row failed", KR(ret), KTIME(abs_timeout_ts));
  } else if (OB_FAIL(get_next_unfiltered_row(cur_row))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to inner get next row", K(ret), KPC(scan_param_));
    }
  } else if (OB_ISNULL(cur_row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("succ to inner get next row, but row is NULL", K(ret));
  } else if (OB_UNLIKELY(cur_row->count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("row count is less than output column count", K(ret),
              K(cur_row->count_), K(output_column_ids_.count()));
  } else if (cur_row->count_ > 0 &&
             OB_ISNULL(cur_row->cells_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("cur_row->cells_ is NULL", K(ret));
  } else if (OB_ISNULL(table_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("table schema is NULL", K(ret));
  } else if (OB_FAIL(convert_output_row(cur_row, false))) {
    LOG_WARN("failed to convert row", K(ret));
  } else if (OB_FAIL(check_type_and_convert_string(cur_row, false))) {
    LOG_WARN("failed to check type and convert string", K(ret));
  }

  if (OB_SUCC(ret)) {
    row = cur_row;
  }
  LOG_DEBUG("check result row", K(ret), KPC(row));
  return ret;
}

int ObVirtualTableIterator::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(capacity < 1)) {
  } else if (OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->op_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null arguments", K(ret));
  } else {
    ObEvalCtx::BatchInfoScopeGuard guard(scan_param_->op_->get_eval_ctx());
    guard.set_batch_size(1);
    guard.set_batch_idx(0);
    if (OB_FAIL(get_next_row())) {
      if (OB_ITER_END != ret) { LOG_WARN("get next row failed", K(ret)); }
    } else {
      count = 1;
    }
  }
  return ret;
}
int ObVirtualTableIterator::get_next_row()
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  common::ObASHTabletIdSetterGuard ash_tablet_id_guard(scan_param_ != nullptr? scan_param_->index_id_ : 0);
  ACTIVE_SESSION_RETRY_DIAG_INFO_SETTER(tablet_id_, scan_param_ != nullptr? scan_param_->index_id_ : 0);
  int ret = OB_SUCCESS;
  ObNewRow *row = NULL;
  if (OB_ISNULL(scan_param_)
      || OB_ISNULL(scan_param_->output_exprs_)
      || OB_ISNULL(scan_param_->op_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (VirtualSvrPair::EMPTY_VIRTUAL_TABLE_TABLET_ID == scan_param_->tablet_id_.id()) {
    row = NULL;
    ret = OB_ITER_END;
  } else if (OB_FAIL(get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(row)) {
    LOG_WARN("NULL row returned", K(ret));
  } else if (scan_param_->output_exprs_->count() > row->count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row count less than output exprs", K(ret), K(*row),
             "output_exprs_cnt", scan_param_->output_exprs_->count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < scan_param_->output_exprs_->count(); i++) {
      ObExpr *expr = scan_param_->output_exprs_->at(i);
      ObDatum &datum = expr->locate_datum_for_write(scan_param_->op_->get_eval_ctx());
      if (OB_FAIL(datum.from_obj(row->cells_[i], expr->obj_datum_map_))) {
        LOG_WARN("convert ObObj to ObDatum failed", K(ret));
      } else if (is_lob_storage(row->cells_[i].get_type()) &&
                 OB_FAIL(ob_adjust_lob_datum(row->cells_[i], expr->obj_meta_,
                                             expr->obj_datum_map_, *allocator_, datum))) {
        LOG_WARN("adjust lob datum failed", K(ret), K(i), K(row->cells_[i].get_meta()), K(expr->obj_meta_));
      } else {
        SANITY_CHECK_RANGE(datum.ptr_, datum.len_);
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::close()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(cur_row_.count_ > 0 && NULL == cur_row_.cells_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("cur_row is not init", K(ret), K(cur_row_));
  } else if (OB_FAIL(inner_close())) {
    LOG_WARN("fail to execute inner close", K(ret));
  } else if (OB_FAIL(free_convert_ctx())) {
    LOG_WARN("fail to free convert context", K(ret));
  } else {
    if (OB_ISNULL(allocator_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("allocator is NULL", K(ret));
    } else {
      //Since ObObj's destructor does not do meaningful operations, in order to save performance, ObObj's destructor call is omitted, and the cells_memory is directly released.
      if (cur_row_.cells_ != NULL) {
        allocator_->free(cur_row_.cells_);
      }
      cur_row_.cells_ = NULL;
      cur_row_.count_ = 0;
    }
  }
  row_calc_buf_.reset();
  return ret;
}

// level_str support: db_acc, table_acc
// reference: ob_expr_sys_privilege_check.cpp:calc_resultN
int ObVirtualTableIterator::check_priv(const ObString &level_str,
                                       const ObString &db_name,
                                       const ObString &table_name,
                                       int64_t tenant_id,
                                       bool &passed)
{
  int ret = OB_SUCCESS;
  share::schema::ObSessionPrivInfo session_priv;
  const common::ObIArray<uint64_t> &enable_role_id_array = session_->get_enable_role_array();
  CK (OB_NOT_NULL(session_) && OB_NOT_NULL(schema_guard_));
  OZ (session_->get_session_priv_info(session_priv));
  // bool allow_show = true;
  if (OB_SUCC(ret)) {
    //tenant_id in table is static casted to int64_t,
    //and use statis_cast<uint64_t> for retrieving(same with schema_service)
    // schema拆分后，普通租户schema表的tenant_id为0，此时鉴权取session_priv.tenant_id_
    if (session_priv.tenant_id_ != static_cast<uint64_t>(tenant_id)
        && OB_INVALID_TENANT_ID != tenant_id) {
      //not current tenant's row
    } else if (0 == level_str.case_compare("db_acc")) {
      if (OB_FAIL(schema_guard_->check_db_show(session_priv, enable_role_id_array, db_name, passed))) {
          LOG_WARN("Check db show failed", K(ret));
      }
    } else if (0 == level_str.case_compare("table_acc")) {
      //if (OB_FAIL(priv_mgr.check_table_show(session_priv,
      if (OB_FAIL(schema_guard_->check_table_show(session_priv, enable_role_id_array, db_name, table_name, passed))) {
        LOG_WARN("Check table show failed", K(ret));
      }
    } else {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("Check priv level error", K(ret));
    }
  }
  return ret;
}

void ObVirtualTableIterator::set_effective_tenant_id(const uint64_t tenant_id)
{
  effective_tenant_id_ = tenant_id;
}

int ObVirtualTableIterator::init_sql_schema_guard_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(schema_guard_) || OB_ISNULL(scan_param_) || OB_ISNULL(scan_param_->external_object_ctx_)) {
    // don't do anything
    // ignore ret
  } else if (OB_FALSE_IT(sql_schema_guard_.set_schema_guard(schema_guard_))) {
  } else if (OB_FAIL(sql_schema_guard_.recover_schema_from_external_objects(
                 scan_param_->external_object_ctx_->get_external_objects(),
                 session_,
                 uc_catalog_ctx_))) {
    LOG_WARN("recover external objects failed", K(ret));
  }
  return ret;
}

int ObVirtualTableIterator::init_pd_cols_project()
{
  int ret = OB_SUCCESS;
  const ObVTableScanParam *scan_param = get_scan_param();
  if (enable_pd_filter() && !scan_param->vt_pd_colums_ids_.empty()
    && nullptr != scan_param->op_filters_) {
    hash::ObHashMap<int64_t, int64_t, common::hash::NoPthreadDefendMode> col_index_map;
    if (OB_FAIL(pd_cols_project_.reserve(scan_param->vt_pd_colums_ids_.count()))) {
      LOG_WARN("failed to reserve", K(ret));
    } else if (OB_FAIL(col_index_map.create(common::hash::cal_next_prime(512), ObModIds::OB_SQL_HASH_SET))) {
      LOG_WARN("fail create hash map", K(ret));
    } else {
      for (int64_t index = 0; OB_SUCC(ret) && index < output_column_ids_.count(); ++index) {
        const uint64_t column_id = output_column_ids_.at(index);
        if (OB_FAIL(col_index_map.set_refactored(column_id, index))){
          LOG_WARN("set refactored failed", K(ret), K(column_id), K(index), K(ret));
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < scan_param->vt_pd_colums_ids_.count(); ++i) {
        int64_t index = -1;
        uint64_t column_id = scan_param->vt_pd_colums_ids_.at(i);
        if (OB_FAIL(col_index_map.get_refactored(column_id, index))) {
          LOG_WARN("failed to get refactored", K(column_id), K(ret));
        } else if (OB_FAIL(pd_cols_project_.push_back(index))) {
          LOG_WARN("failed to pushback", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::check_type_and_convert_string(ObNewRow *&cur_row, bool is_pd_filter_row)
{
  int ret = OB_SUCCESS;
  if (cols_schema_.empty() && OB_FAIL(get_all_columns_schema())) {
    LOG_WARN("failed to get columns schema", K(ret));
  } else {
    const int64_t col_cnt = is_pd_filter_row ? pd_cols_project_.count() : output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const int64_t index = is_pd_filter_row ? pd_cols_project_.at(i) : i;
      const uint64_t column_id = output_column_ids_.at(index);
      const ObColumnSchemaV2 *col_schema = cols_schema_.at(index);
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("col_schema is NULL", K(ret), K(column_id));
      } else if (OB_UNLIKELY(col_schema->get_data_type() != cur_row->cells_[i].get_type()
                            && ObNullType != cur_row->cells_[i].get_type())) {
        ret = OB_ERR_UNEXPECTED;
        if (GCONF.in_upgrade_mode()) {
          LOG_WARN("column type in this row is not expected type", K(ret), K(i),
                  "table_name", table_schema_->get_table_name_str(),
                  "column_name", col_schema->get_column_name_str(),
                  K(column_id), K(cur_row->cells_[i]),
                  K(col_schema->get_data_type()), K(output_column_ids_));
        } else {
          LOG_ERROR("column type in this row is not expected type", K(ret), K(i),
                    "table_name", table_schema_->get_table_name_str(),
                    "column_name", col_schema->get_column_name_str(),
                    K(column_id), K(cur_row->cells_[i]), K(cur_row->cells_[i].get_type()),
                    K(col_schema->get_data_type()), K(output_column_ids_));
        }
      }
      if (OB_SUCC(ret) && ob_is_string_tc(col_schema->get_data_type())
          && (col_schema->get_data_length() < cur_row->cells_[i].get_string_len()
              // do charset convert when obj meta is different from expr meta
              || (CS_TYPE_INVALID != cur_row->cells_[i].get_collation_type()
                  && col_schema->get_collation_type() != cur_row->cells_[i].get_collation_type()))) {
        //Check the column schema to ensure that it meets the schema definition;
        //But currently, only strings that exceed the length limit are processed to prevent occupying too many performance resources and causing too much interface delay
        ObObj output_obj;
        ObArray<ObString> *type_infos = NULL;
        const bool is_strict = false;
        ObRawExprResType res_type;
        res_type.set_accuracy(col_schema->get_accuracy());
        res_type.set_collation_type(col_schema->get_collation_type());
        res_type.set_type(col_schema->get_data_type());
        ObCastCtx cast_ctx = cast_ctx_;
        cast_ctx.cast_mode_ = cast_ctx_.cast_mode_ | CM_WARN_ON_FAIL;
        if (OB_FAIL(ObExprColumnConv::convert_skip_null_check(output_obj, cur_row->cells_[i],
                                                              res_type, is_strict, cast_ctx,
                                                              type_infos))) {
          LOG_WARN("fail to convert skip null check", KR(ret), "object", cur_row->cells_[i]);
        } else {
          cur_row->cells_[i] = output_obj;
          if (OB_SUCCESS != cast_ctx.warning_) {
            LOG_WARN("invalid row result, check schema", "warning_num", cast_ctx.warning_,
                    "object", cur_row->cells_[i], K(res_type));
          }
        }
      }
    }
  }
  return ret;
}

// 纯直接填充虚拟表（继承 ObVirtualTableIterator），用 pack_lob_cells帮助lob裸数据加头
int ObVirtualTableIterator::pack_lob_cells(ObNewRow *cur_row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row) || OB_ISNULL(cur_row->cells_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cur_row or cells is NULL", K(ret), KP(cur_row));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cur_row->count_; ++i) {
    ObObj &cell = cur_row->cells_[i];
    if (cell.is_lob_storage() && !cell.is_null() && !cell.is_nop_value()) {
      ObString data = cell.get_string();
      if (OB_FAIL(sql::ObTextStringHelper::pack_to_disk_inrow_lob(
            row_calc_buf_, data, cell.get_type(), cell))) {
        LOG_WARN("pack virtual table lob cell failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::project_pd_filter_columns(ObNewRow *&cur_row)
{
  int ret = OB_SUCCESS;
  if (scan_param_->output_exprs_->count() > cur_row->count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row count less than output exprs", K(ret), K(*cur_row),
             "output_exprs_cnt", scan_param_->output_exprs_->count());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < pd_cols_project_.count(); ++i) {
      const int64_t index = pd_cols_project_.at(i);
      ObExpr *expr = scan_param_->output_exprs_->at(index);
      ObDatum &datum = expr->locate_datum_for_write(scan_param_->op_->get_eval_ctx());
      if (OB_FAIL(datum.from_obj(cur_row->cells_[i], expr->obj_datum_map_))) {
        LOG_WARN("convert ObObj to ObDatum failed", K(ret));
      } else if (is_lob_storage(cur_row->cells_[i].get_type()) &&
                  OB_FAIL(ob_adjust_lob_datum(cur_row->cells_[i], expr->obj_meta_,
                                              expr->obj_datum_map_, *allocator_, datum))) {
        LOG_WARN("adjust lob datum failed", K(ret), K(i), K(cur_row->cells_[i].get_meta()), K(expr->obj_meta_));
      } else {
        SANITY_CHECK_RANGE(datum.ptr_, datum.len_);
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::get_tenant_id_filter(int &tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t captured = 0;
  bool found = false;
  if (OB_FAIL(get_int_filter_by_column_name("tenant_id", captured, found))) {
    LOG_WARN("failed to extract tenant_id filter", K(ret));
  } else if (found) {
    tenant_id = static_cast<int>(captured);
  } else if (is_sys_tenant(effective_tenant_id_)) {
    // sys tenant with no tenant_id predicate -> caller iterates all tenants
    ret = OB_ENTRY_NOT_EXIST;
  } else {
    tenant_id = static_cast<int>(effective_tenant_id_);
  }
  LOG_DEBUG("get tenant id filter", K(ret), K(tenant_id), K(effective_tenant_id_), K(found));
  return ret;
}

namespace
{
bool is_target_ref_column(const ObExpr *col_expr,
                          const ObVTableScanParam *param,
                          const uint64_t target_col_id)
{
  bool matched = false;
  if (OB_NOT_NULL(col_expr) && T_REF_COLUMN == col_expr->type_
      && OB_NOT_NULL(param) && OB_NOT_NULL(param->output_exprs_)) {
    const ExprFixedArray &access_exprs = *param->output_exprs_;
    for (int64_t i = 0; !matched && i < access_exprs.count() && i < param->column_ids_.count(); ++i) {
      if (access_exprs.at(i) == col_expr && param->column_ids_.at(i) == target_col_id) {
        matched = true;
      }
    }
  }
  return matched;
}

int extract_int_from_eq_expr(const ObExpr *expr,
                             const ObVTableScanParam *param,
                             const uint64_t target_col_id,
                             ObEvalCtx *eval_ctx,
                             int64_t &val,
                             bool &found,
                             bool &conflict)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr) || T_OP_EQ != expr->type_ || 2 != expr->arg_cnt_) {
    // do nothing
  } else {
    const ObExpr *col_expr = nullptr;
    const ObExpr *const_expr = nullptr;
    for (int64_t i = 0; i < 2; ++i) {
      const ObExpr *arg = expr->args_[i];
      if (OB_ISNULL(arg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("eq filter arg is null", K(ret), K(i));
      } else if (T_REF_COLUMN == arg->type_) {
        col_expr = arg;
      } else if (arg->is_const_expr()) {
        const_expr = arg;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(col_expr) || OB_ISNULL(const_expr)
               || !is_target_ref_column(col_expr, param, target_col_id)) {
      // not target_col = const
    } else if (OB_ISNULL(eval_ctx)) {
      LOG_WARN("eval ctx is null, skip int filter from op_filters", K(ret));
    } else {
      ObDatum *datum = nullptr;
      if (OB_FAIL(const_expr->eval(*eval_ctx, datum))) {
        LOG_WARN("failed to eval const expr in int filter", K(ret));
      } else if (OB_ISNULL(datum)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("const datum is null", K(ret));
      } else {
        ObObj obj;
        if (OB_FAIL(datum->to_obj(obj, const_expr->obj_meta_))) {
          LOG_WARN("failed to convert datum to obj", K(ret));
        } else if (obj.is_null()) {
          // skip
        } else {
          int64_t cur_val = 0;
          bool valid = false;
          if (obj.is_integer_type()) {
            cur_val = obj.get_int();
            valid = true;
          } else if (ObUInt64Type == obj.get_type()) {
            cur_val = static_cast<int64_t>(obj.get_uint64());
            valid = true;
          }
          if (valid) {
            if (found && val != cur_val) {
              conflict = true;
            } else {
              val = cur_val;
              found = true;
            }
          }
        }
      }
    }
  }
  return ret;
}

int extract_int_from_filter_expr(const ObExpr *expr,
                                 const ObVTableScanParam *param,
                                 const uint64_t target_col_id,
                                 ObEvalCtx *eval_ctx,
                                 int64_t &val,
                                 bool &found,
                                 bool &conflict)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("filter expr is null", K(ret));
  } else if (T_OP_AND == expr->type_) {
    for (int64_t i = 0; OB_SUCC(ret) && !conflict && i < expr->arg_cnt_; ++i) {
      if (OB_FAIL(extract_int_from_filter_expr(expr->args_[i], param, target_col_id,
                                               eval_ctx, val, found, conflict))) {
        LOG_WARN("failed to extract int from and child", K(ret), K(i));
      }
    }
  } else if (T_OP_EQ == expr->type_) {
    if (OB_FAIL(extract_int_from_eq_expr(expr, param, target_col_id,
                                         eval_ctx, val, found, conflict))) {
      LOG_WARN("failed to extract int from eq expr", K(ret));
    }
  }
  return ret;
}

} // namespace

int ObVirtualTableIterator::eval_string_filter_value_(const ObExpr *expr,
                                                      ObEvalCtx *eval_ctx,
                                                      ObString &value,
                                                      bool &is_null,
                                                      bool &is_valid)
{
  int ret = OB_SUCCESS;
  ObDatum *datum = nullptr;
  ObObj obj;
  value.reset();
  is_null = false;
  is_valid = false;
  if (OB_ISNULL(expr) || OB_ISNULL(eval_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(expr), KP(eval_ctx));
  } else if (!expr->is_const_expr()) {
    // 非常量表达式不能用于字符串过滤下推。
  } else if (OB_FAIL(expr->eval(*eval_ctx, datum))) {
    LOG_WARN("failed to eval const expr in string filter", K(ret));
  } else if (OB_ISNULL(datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("const datum is null", K(ret));
  } else if (OB_FAIL(datum->to_obj(obj, expr->obj_meta_))) {
    LOG_WARN("failed to convert datum to obj", K(ret));
  } else if (obj.is_null()) {
    is_null = true;
  } else if (obj.is_string_type()) {
    value = obj.get_string();
    is_valid = true;
  } else {
    // 非字符串常量不能部分下推，由上层完整过滤。
  }
  return ret;
}

int ObVirtualTableIterator::intersect_string_filter_values_(
    const ObIArray<ObString> &candidate_values,
    const ObCollationType collation_type,
    ObIArray<ObString> &values,
    bool &found)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 4> intersection;
  const ObString *value = nullptr;
  const ObString *candidate_value = nullptr;
  bool matched = false;
  bool is_pad = false;
  bool compare_end_space = false;
  if (OB_UNLIKELY(!ObCharset::is_valid_collation(collation_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid collation type", K(ret), K(collation_type));
  } else if (OB_FAIL(ObCharset::is_pad_charset(collation_type, is_pad))) {
    LOG_WARN("failed to check pad charset", K(ret), K(collation_type));
  } else if (candidate_values.empty()) {
    values.reset();
    found = true;
  } else if (!found) {
    if (OB_FAIL(values.assign(candidate_values))) {
      LOG_WARN("failed to assign string filter values", K(ret));
    } else {
      found = true;
    }
  } else {
    compare_end_space = !is_pad;
    for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
      value = &values.at(i);
      matched = false;
      for (int64_t j = 0; !matched && j < candidate_values.count(); ++j) {
        candidate_value = &candidate_values.at(j);
        matched = 0 == ObCharset::strcmpsp(
            collation_type,
            value->ptr(),
            value->length(),
            candidate_value->ptr(),
            candidate_value->length(),
            compare_end_space);
      }
      if (matched && OB_FAIL(intersection.push_back(*value))) {
        LOG_WARN("failed to push back intersected string filter", K(ret), KPC(value));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(values.assign(intersection))) {
      LOG_WARN("failed to assign intersected string filters", K(ret));
    }
  }
  return ret;
}

int ObVirtualTableIterator::extract_string_values_from_eq_expr_(
    const ObExpr *expr,
    const ObVTableScanParam *param,
    const uint64_t target_col_id,
    ObEvalCtx *eval_ctx,
    const ObCollationType collation_type,
    ObIArray<ObString> &values,
    bool &found)
{
  int ret = OB_SUCCESS;
  const ObExpr *col_expr = nullptr;
  const ObExpr *const_expr = nullptr;
  ObSEArray<ObString, 1> candidate_values;
  ObString value;
  bool is_null = false;
  bool is_valid = false;
  if (OB_ISNULL(expr) || T_OP_EQ != expr->type_ || 2 != expr->arg_cnt_) {
    // 非目标等值表达式，无需处理。
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
      const ObExpr *arg = expr->args_[i];
      if (OB_ISNULL(arg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("eq filter arg is null", K(ret), K(i));
      } else if (T_REF_COLUMN == arg->type_) {
        col_expr = arg;
      } else if (arg->is_const_expr()) {
        const_expr = arg;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(col_expr) || OB_ISNULL(const_expr)
               || !is_target_ref_column(col_expr, param, target_col_id)) {
      // 不是目标列与字符串常量的等值条件。
    } else if (OB_ISNULL(eval_ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("eval ctx is null, skip string filter from op_filters", K(ret));
    } else if (OB_FAIL(eval_string_filter_value_(
        const_expr, eval_ctx, value, is_null, is_valid))) {
      LOG_WARN("failed to get string value from eq expr", K(ret));
    } else if (is_null) {
      candidate_values.reset();
      if (OB_FAIL(intersect_string_filter_values_(
          candidate_values, collation_type, values, found))) {
        LOG_WARN("failed to intersect null string value from eq expr", K(ret));
      }
    } else if (!is_valid) {
      // 非字符串常量不参与字符串过滤提取。
    } else if (OB_FAIL(candidate_values.push_back(value))) {
      LOG_WARN("failed to push back candidate string value", K(ret), K(value));
    } else if (OB_FAIL(intersect_string_filter_values_(
        candidate_values, collation_type, values, found))) {
      LOG_WARN("failed to intersect string values from eq expr", K(ret));
    }
  }
  return ret;
}

int ObVirtualTableIterator::extract_string_values_from_in_expr_(
    const ObExpr *expr,
    const ObVTableScanParam *param,
    const uint64_t target_col_id,
    ObEvalCtx *eval_ctx,
    const ObCollationType collation_type,
    ObIArray<ObString> &values,
    bool &found)
{
  int ret = OB_SUCCESS;
  const ObExpr *col_expr = nullptr;
  const ObExpr *row_expr = nullptr;
  ObSEArray<ObString, 4> candidate_values;
  ObString value;
  bool is_null = false;
  bool is_valid = false;
  bool can_extract = true;
  if (OB_ISNULL(expr) || T_OP_IN != expr->type_ || 2 != expr->arg_cnt_) {
    // 非目标 IN 表达式，无需处理。
  } else if (OB_ISNULL(expr->args_[0]) || OB_ISNULL(expr->args_[1])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("in filter arg is null", K(ret));
  } else {
    col_expr = expr->args_[0];
    row_expr = expr->args_[1];
    if (!is_target_ref_column(col_expr, param, target_col_id)
        || T_OP_ROW != row_expr->type_ || row_expr->arg_cnt_ <= 0) {
      // 只处理目标列的标量 IN 常量列表。
    } else if (OB_ISNULL(eval_ctx)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("eval ctx is null, skip string filter from op_filters", K(ret));
    } else {
      for (int64_t i = 0;
           OB_SUCC(ret) && can_extract && i < row_expr->arg_cnt_;
           ++i) {
        const ObExpr *const_expr = row_expr->args_[i];
        value.reset();
        is_null = false;
        is_valid = false;
        if (OB_ISNULL(const_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("in filter value expr is null", K(ret), K(i));
        } else if (!const_expr->is_const_expr()) {
          can_extract = false;
        } else if (OB_FAIL(eval_string_filter_value_(
            const_expr, eval_ctx, value, is_null, is_valid))) {
          LOG_WARN("failed to get string value from in expr", K(ret), K(i));
        } else if (is_null) {
          // IN 中的 NULL 不会命中非 NULL 的目标列，可安全忽略。
        } else if (!is_valid) {
          can_extract = false;
        } else if (OB_FAIL(candidate_values.push_back(value))) {
          LOG_WARN("failed to push back candidate string value", K(ret), K(value));
        }
      }
      if (OB_SUCC(ret) && can_extract) {
        if (OB_FAIL(intersect_string_filter_values_(
            candidate_values, collation_type, values, found))) {
          LOG_WARN("failed to intersect string values from in expr", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::extract_string_values_from_filter_expr_(
    const ObExpr *expr,
    const ObVTableScanParam *param,
    const uint64_t target_col_id,
    ObEvalCtx *eval_ctx,
    const ObCollationType collation_type,
    ObIArray<ObString> &values,
    bool &found)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(expr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("filter expr is null", K(ret));
  } else if (T_OP_AND == expr->type_) {
    for (int64_t i = 0;
         OB_SUCC(ret) && !(found && values.empty()) && i < expr->arg_cnt_;
         ++i) {
      if (OB_FAIL(extract_string_values_from_filter_expr_(
          expr->args_[i], param, target_col_id, eval_ctx,
          collation_type, values, found))) {
        LOG_WARN("failed to extract string from and child", K(ret), K(i));
      }
    }
  } else if (T_OP_EQ == expr->type_) {
    if (OB_FAIL(extract_string_values_from_eq_expr_(
        expr, param, target_col_id, eval_ctx, collation_type, values, found))) {
      LOG_WARN("failed to extract string from eq expr", K(ret));
    }
  } else if (T_OP_IN == expr->type_) {
    if (OB_FAIL(extract_string_values_from_in_expr_(
        expr, param, target_col_id, eval_ctx, collation_type, values, found))) {
      LOG_WARN("failed to extract string from in expr", K(ret));
    }
  }
  return ret;
}

int ObVirtualTableIterator::get_int_filter_by_column_name(const char *col_name,
                                                          int64_t &val,
                                                          bool &found)
{
  int ret = OB_SUCCESS;
  found = false;
  const ObVTableScanParam *param = get_scan_param();
  if (OB_ISNULL(table_schema_) || OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_schema_), KP(col_name));
  } else {
    const ObColumnSchemaV2 *col = table_schema_->get_column_schema(col_name);
    if (OB_ISNULL(col)) {
      // column not in schema → nothing to extract
    } else if (OB_ISNULL(param) || OB_ISNULL(param->op_filters_) || param->op_filters_->empty()) {
      // no pushdown filters
    } else if (OB_ISNULL(param->op_)) {
      LOG_WARN("op is null, skip int filter from op_filters", K(ret));
    } else {
      const uint64_t target_col_id = col->get_column_id();
      ObEvalCtx &eval_ctx = param->op_->get_eval_ctx();
      bool conflict = false;
      for (int64_t i = 0; OB_SUCC(ret) && !conflict && i < param->op_filters_->count(); ++i) {
        if (OB_FAIL(extract_int_from_filter_expr(param->op_filters_->at(i),
                                                 param, target_col_id, &eval_ctx,
                                                 val, found, conflict))) {
          LOG_WARN("failed to extract int from filter expr", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret) && conflict) {
        found = false;
      }
    }
  }
  return ret;
}

int ObVirtualTableIterator::get_string_filter_values_by_column_name(
    const char *col_name, ObIArray<ObString> &values, bool &found)
{
  int ret = OB_SUCCESS;
  const ObVTableScanParam *param = get_scan_param();
  values.reset();
  found = false;
  if (OB_ISNULL(table_schema_) || OB_ISNULL(col_name)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(table_schema_), KP(col_name));
  } else {
    const ObColumnSchemaV2 *col = table_schema_->get_column_schema(col_name);
    if (OB_ISNULL(col)) {
      // schema 中不存在目标列，无需提取。
    } else if (OB_ISNULL(param) || OB_ISNULL(param->op_filters_) || param->op_filters_->empty()) {
      // 没有可用的下推过滤条件。
    } else if (OB_ISNULL(param->op_)) {
      LOG_WARN("op is null, skip string filter from op_filters", K(ret));
    } else if (!ObCharset::is_valid_collation(col->get_collation_type())) {
      LOG_WARN("invalid column collation, skip string filter from op_filters",
          K(ret), "collation_type", col->get_collation_type());
    } else {
      const uint64_t target_col_id = col->get_column_id();
      const ObCollationType collation_type = col->get_collation_type();
      ObEvalCtx &eval_ctx = param->op_->get_eval_ctx();
      for (int64_t i = 0;
           OB_SUCC(ret) && !(found && values.empty())
               && i < param->op_filters_->count();
           ++i) {
        if (OB_FAIL(extract_string_values_from_filter_expr_(
            param->op_filters_->at(i), param, target_col_id, &eval_ctx,
            collation_type, values, found))) {
          LOG_WARN("failed to extract string from filter expr", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

}// common
}// oceanbase
