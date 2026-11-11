/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_odps_table_writer.h"
#include "ob_odps_upload_session_mgr.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_arrow_basic.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "sql/engine/table/ob_odps_table_utils.h"
#include "sql/engine/expr/ob_expr_json_func_helper.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/udt/ob_collection_type.h"
#include "lib/wide_integer/ob_wide_integer_str_funcs.h"
#include "sql/ob_sql_utils.h"
#ifdef OB_BUILD_CPP_ODPS
#include "sql/engine/connector/ob_odps_cpp_upload_session_mgr.h"
#endif
#ifdef OB_BUILD_JNI_ODPS
#include "sql/engine/connector/ob_odps_jni_upload_session_mgr.h"
#include "sql/engine/connector/ob_odps_jni_writer.h"
#include "lib/jni_env/ob_java_env.h"
#include <arrow/api.h>
#include <arrow/c/bridge.h>
#include <arrow/array.h>
#endif

namespace oceanbase
{
using namespace common;
namespace sql
{
#define ARROW_FAIL(statement) (OB_UNLIKELY(!(statement).ok()))

static OB_INLINE int odps_get_cast_ret(const bool is_strict_mode, int ret)
{
  if (OB_SUCCESS != ret && !is_strict_mode) {
    ret = OB_SUCCESS;
  }
  return ret;
}

#define ODPS_CAST_FAIL(stmt) \
  (OB_UNLIKELY((OB_SUCCESS != (ret = odps_get_cast_ret((is_strict_mode), (stmt))))))

#ifdef OB_BUILD_CPP_ODPS
class ObODPSCppTableWriter final : public ObODPSTableWriter
{
public:
  ObODPSCppTableWriter(ObExecContext &ctx, ObEvalCtx &eval_ctx,
                      const ObODPSWriterOptions &options, ObODPSGeneralFormat &format)
      : ObODPSTableWriter(ctx, eval_ctx, options, format), upload_(), record_writer_()
  {}
  ~ObODPSCppTableWriter() override
  {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
    record_writer_.reset();
    upload_.reset();
  }
private:
  int open_impl() override;
  int write_row_impl() override;
  int write_batch_impl(const ObBatchRows &brs) override;
  int close_impl() override;
  int set_odps_column_value_mysql(apsara::odps::sdk::ODPSTableRecord &table_record,
                                  const ObDatum &datum,
                                  const ObDatumMeta &datum_meta,
                                  const ObObjMeta &obj_meta,
                                  uint32_t col_idx,
                                  const bool is_strict_mode,
                                  const ObDateSqlMode date_sql_mode);
  int set_odps_column_value_oracle(apsara::odps::sdk::ODPSTableRecord &table_record,
                                   const ObDatum &datum,
                                   const ObDatumMeta &datum_meta,
                                   const ObObjMeta &obj_meta,
                                   uint32_t col_idx);
  int recursive_fill_list_record(shared_ptr<apsara::odps::sdk::ODPSArray> odps_array,
                                 ObODPSArrayHelper *array_helper,
                                 ObIAllocator &alloc,
                                 const bool is_strict_mode,
                                 const ObDateSqlMode date_sql_mode);
  int decimal_or_number_to_int64(const ObDatum &datum, const ObDatumMeta &datum_meta, int64_t &res);
  int decimal_to_string(const ObDatum &datum,
                        const ObDatumMeta &datum_meta,
                        std::string &res,
                        ObIAllocator &allocator);
  apsara::odps::sdk::IUploadPtr upload_;
  apsara::odps::sdk::IRecordWriterPtr record_writer_;
};
#endif

#ifdef OB_BUILD_JNI_ODPS
class ObODPSJniTableWriter final : public ObODPSTableWriter
{
public:
  ObODPSJniTableWriter(ObExecContext &ctx, ObEvalCtx &eval_ctx,
                      const ObODPSWriterOptions &options, ObODPSGeneralFormat &format)
      : ObODPSTableWriter(ctx, eval_ctx, options, format), uploader_(),
        arrow_alloc_(), arrow_schema_()
  {}
  ~ObODPSJniTableWriter() override
  {
    if (OB_NOT_NULL(uploader_.writer_ptr.get())) {
      (void)uploader_.writer_ptr->do_close();
      uploader_.writer_ptr.reset();
    }
    arrow_schema_.reset();
  }
private:
  int open_impl() override;
  int write_row_impl() override
  {
    int ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS JNI row-at-a-time writes");
    return ret;
  }
  int write_batch_impl(const ObBatchRows &brs) override;
  int close_impl() override;
  int create_odps_schema();

  int into_odps_jni_batch_one_col(int64_t col_idx, ObOdpsJniConnector::OdpsType odps_type, arrow::Field &arrow_field,
      ObDatumMeta &meta, ObObjMeta &obj_meta, ObIVector &expr_vector, arrow::ArrayBuilder *builder,
      const ObBatchRows &brs, int &act_cnt, ObIAllocator &alloc, const bool is_strict_mode, const ObDateSqlMode date_sql_mode);

  int recursive_fill_list_builder(arrow::ListBuilder *list_builder,
                                  arrow::ArrayBuilder *child_builder,
                                  ObODPSArrayHelper *array_helper,
                                  ObIAllocator &alloc,
                                  const bool is_strict_mode,
                                  const ObDateSqlMode date_sql_mode);
  ObOdpsJniUploadSessionMgr::OdpsUploader uploader_;
  ObArrowMemPool arrow_alloc_;
  std::shared_ptr<arrow::Schema> arrow_schema_;
};
#endif

int ObODPSTableWriter::create(ObExecContext &ctx, ObEvalCtx &eval_ctx,
                             const ObODPSWriterOptions &options, ObODPSGeneralFormat &format,
                             ObODPSTableWriter *&writer)
{
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (format.use_odps_jni_connector_) {
#ifdef OB_BUILD_JNI_ODPS
    writer = OB_NEWx(ObODPSJniTableWriter, &ctx.get_allocator(), ctx, eval_ctx, options, format);
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS JNI writer in this build");
#endif
  } else {
#ifdef OB_BUILD_CPP_ODPS
    writer = OB_NEWx(ObODPSCppTableWriter, &ctx.get_allocator(), ctx, eval_ctx, options, format);
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ODPS C++ writer in this build");
#endif
  }
  if (OB_SUCC(ret) && OB_ISNULL(writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate ODPS writer", K(ret));
  }
  return ret;
}

ObODPSTableWriter::ObODPSTableWriter(ObExecContext &ctx, ObEvalCtx &eval_ctx,
                                    const ObODPSWriterOptions &options, ObODPSGeneralFormat &format)
    : ctx_(ctx), eval_ctx_(eval_ctx), options_(options), format_(format), block_id_(0),
      need_commit_(true), upload_mgr_(nullptr), array_helpers_(), closed_(false), close_ret_(OB_SUCCESS)
{}

ObODPSTableWriter::~ObODPSTableWriter()
{
  release_array_helpers();
}

int ObODPSTableWriter::open()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ctx_.get_sqc_handler())) {
    upload_mgr_ = ctx_.get_sqc_handler()->get_sqc_ctx().gi_pump_.get_odps_upload_session_mgr();
    const ObOdpsUploadSessionMgr::ConnectorType expected_type = format_.use_odps_jni_connector_
        ? ObOdpsUploadSessionMgr::ConnectorType::JNI : ObOdpsUploadSessionMgr::ConnectorType::CPP;
    if (OB_ISNULL(upload_mgr_) || upload_mgr_->get_connector_type() != expected_type) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("missing or mismatched ODPS upload session manager", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    ret = open_impl();
  }
  if (OB_FAIL(ret)) {
    set_failed();
  }
  return ret;
}

int ObODPSTableWriter::write_row()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_row_impl())) {
    set_failed();
  }
  return ret;
}

int ObODPSTableWriter::write_batch(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_batch_impl(brs))) {
    set_failed();
  }
  return ret;
}

int ObODPSTableWriter::close(bool execution_succeeded)
{
  int ret = OB_SUCCESS;
  if (closed_) {
    ret = close_ret_;
  } else {
    if (!execution_succeeded) {
      set_failed();
    }
    ret = close_impl();
    if (OB_NOT_NULL(upload_mgr_)) {
      // Even failed tasks arrive at the barrier, after finishing their own writer.
      const int finish_ret = upload_mgr_->finish_task(need_commit_ && OB_SUCCESS == ret);
      if (OB_SUCCESS != finish_ret) {
        LOG_WARN("failed to finalize ODPS upload task", K(ret), K(finish_ret));
        if (OB_SUCC(ret)) {
          ret = finish_ret;
        }
      }
    }
    close_ret_ = ret;
    closed_ = true;
    release_array_helpers();
  }
  return ret;
}

void ObODPSTableWriter::release_array_helpers()
{
  for (int64_t i = 0; i < array_helpers_.count(); ++i) {
    if (OB_NOT_NULL(array_helpers_.at(i))) {
      array_helpers_.at(i)->~ObODPSArrayHelper();
      ctx_.get_allocator().free(array_helpers_.at(i));
    }
  }
  array_helpers_.reset();
}

#ifdef OB_BUILD_CPP_ODPS
int ObODPSCppTableWriter::open_impl()
{
  int ret = OB_SUCCESS;
  bool is_in_px = (NULL != ctx_.get_sqc_handler());
  if (is_in_px) {
    ObOdpsCppUploadSessionMgr &odps_mgr = *static_cast<ObOdpsCppUploadSessionMgr *>(upload_mgr_);
    if (OB_FAIL(odps_mgr.get_odps_uploader(options_.task_id_, upload_, record_writer_))) {
      LOG_WARN("failed to get odps uploader", K(ret));
    }
  } else if (OB_FAIL(format_.decrypt())) {
    LOG_WARN("failed to decrypt odps format", K(ret));
  } else {
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
    try {
      if (OB_FAIL(ObOdpsCppUploadSessionMgr::create_upload_session(format_,
                                                                      options_.partition_,
                                                                      options_.overwrite_,
                                                                      upload_))) {
        LOG_WARN("failed to create upload session", K(ret));
      } else if (OB_UNLIKELY(!(record_writer_ = upload_->OpenWriter(block_id_, true)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      }
    } catch (apsara::odps::sdk::OdpsException& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when init odps tunnel", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    const ObIArray<ObExpr *> &select_exprs = options_.select_exprs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
      ObODPSArrayHelper *array_helper = nullptr;
      ObExpr *cur_expr = select_exprs.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null");
      } else if (ObCollectionSQLType == cur_expr->datum_meta_.get_type() &&
                 OB_FAIL(ObODPSTableUtils::create_array_helper(eval_ctx_.exec_ctx_, ctx_.get_allocator(),
                                                               *cur_expr, array_helper))) {
        LOG_WARN("failed to init array helper");
      } else if (OB_FAIL(array_helpers_.push_back(array_helper))) {
        LOG_WARN("failed to push back array_helper to array");
      }
    }
  }
  return ret;
}

int ObODPSCppTableWriter::write_row_impl()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = options_.select_exprs_;
  apsara::odps::sdk::ODPSTableRecordPtr table_record;
  ObDatum *datum = NULL;
  ObSQLMode sql_mode = eval_ctx_.exec_ctx_.get_my_session()->get_sql_mode();
  ObDateSqlMode date_sql_mode;
  date_sql_mode.init(sql_mode);
  bool is_strict_mode = common::is_strict_mode(sql_mode);
  try {
    if (OB_UNLIKELY(!upload_ || !record_writer_ || !(table_record = upload_->CreateBufferRecord())
                    || !(table_record->GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (table_record->GetSchema()->GetColumnCount() != select_exprs.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert into partial column in external table");
      LOG_WARN("column count of odps record is not equal to count of select exprs",
               K(table_record->GetSchema()->GetColumnCount()), K(select_exprs.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
        if (OB_ISNULL(select_exprs.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("select expr is unexpected null", K(ret));
        } else if (OB_FAIL(select_exprs.at(i)->eval(eval_ctx_, datum))) {
          LOG_WARN("eval expr failed", K(ret));
        } else if (OB_ISNULL(datum)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("datum is unexpected null", K(ret));
        } else if (lib::is_mysql_mode()
                   && OB_FAIL(set_odps_column_value_mysql(*table_record, *datum,
                                                          select_exprs.at(i)->datum_meta_,
                                                          select_exprs.at(i)->obj_meta_,
                                                          i, is_strict_mode, sql_mode))) {
          LOG_WARN("failed to set odps column value", K(ret));
        } else if (lib::is_oracle_mode()
                   && OB_FAIL(set_odps_column_value_oracle(*table_record, *datum,
                                                           select_exprs.at(i)->datum_meta_,
                                                           select_exprs.at(i)->obj_meta_,
                                                           i))) {
          LOG_WARN("failed to set odps column value", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        record_writer_->Write(*table_record);
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one row to odps", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one row to odps", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one row to odps", K(ret));
    }
  }
  return ret;
}

int ObODPSCppTableWriter::write_batch_impl(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr*> &select_exprs = options_.select_exprs_;
  ObArray<ObDatumVector> datum_vectors;
  ObDatum *datum = NULL;
  apsara::odps::sdk::ODPSTableRecordPtr table_record;
  ObSQLMode sql_mode = eval_ctx_.exec_ctx_.get_my_session()->get_sql_mode();
  ObDateSqlMode date_sql_mode;
  date_sql_mode.init(sql_mode);
  bool is_strict_mode = common::is_strict_mode(sql_mode);

  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    if (OB_ISNULL(select_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("select expr is unexpected null", K(ret));
    } else if (OB_FAIL(select_exprs.at(i)->eval_batch(eval_ctx_, *brs.skip_, brs.size_))) {
      LOG_WARN("failed to eval batch", K(ret), KPC(select_exprs.at(i)));
    } else if (OB_FAIL(datum_vectors.push_back(select_exprs.at(i)->locate_expr_datumvector(eval_ctx_)))) {
      LOG_WARN("failed to push back datum vector", K(ret));
    }
  }
  try {
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!upload_ || !record_writer_
                           || !(table_record = upload_->CreateBufferRecord())
                           || !(table_record->GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (table_record->GetSchema()->GetColumnCount() != select_exprs.count()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "insert into partial column in external table");
      LOG_WARN("column count of odps record is not equal to count of select exprs",
               K(table_record->GetSchema()->GetColumnCount()), K(select_exprs.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
        if (brs.skip_->contain(i)) {
          // do nothing
        } else {
          for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); ++col_idx) {
            if (OB_ISNULL(datum = datum_vectors.at(col_idx).at(i))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("datum is unexpected null", K(ret));
            } else if (lib::is_mysql_mode()
                       && OB_FAIL(set_odps_column_value_mysql(*table_record, *datum,
                                                              select_exprs.at(col_idx)->datum_meta_,
                                                              select_exprs.at(col_idx)->obj_meta_,
                                                              col_idx, is_strict_mode, date_sql_mode))) {
              LOG_WARN("failed to set odps column value", K(ret));
            } else if (lib::is_oracle_mode()
                       && OB_FAIL(set_odps_column_value_oracle(*table_record, *datum,
                                                               select_exprs.at(col_idx)->datum_meta_,
                                                               select_exprs.at(col_idx)->obj_meta_,
                                                               col_idx))) {
              LOG_WARN("failed to set odps column value", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            record_writer_->Write(*table_record);
          }
        }
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one batch to odps", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one batch to odps", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when write one batch to odps", K(ret));
    }
  }
  return ret;
}

int ObODPSCppTableWriter::set_odps_column_value_mysql(apsara::odps::sdk::ODPSTableRecord &table_record,
                                                const ObDatum &datum,
                                                const ObDatumMeta &datum_meta,
                                                const ObObjMeta &obj_meta,
                                                uint32_t col_idx,
                                                const bool is_strict_mode,
                                                const ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  apsara::odps::sdk::ODPSColumnType odps_type;
  uint32_t res_len = 0;
  char *buf = NULL;
  int64_t buf_size = 0;
  ObArenaAllocator allocator("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
  try {
    if (OB_UNLIKELY(!(table_record.GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (datum.is_null()) {
      table_record.SetNullValue(col_idx);
    } else {
      odps_type = table_record.GetSchema()->GetTableColumn(col_idx).GetType();
      switch (odps_type)
      {
        case apsara::odps::sdk::ODPS_BOOLEAN:
        {
          if (ObTinyIntType == ob_type) {
            table_record.SetBoolValue(col_idx, datum.get_tinyint() != 0);
          } else if (ObSmallIntType == ob_type) {
            table_record.SetBoolValue(col_idx, datum.get_smallint() != 0);
          } else if (ObMediumIntType == ob_type || ObInt32Type == ob_type) {
            table_record.SetBoolValue(col_idx, datum.get_int32() != 0);
          } else if (ObIntType == ob_type) {
            table_record.SetBoolValue(col_idx, datum.get_int() != 0);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_TINYINT:
        {
          table_record.SetTinyIntValue(col_idx, datum.get_tinyint());
          break;
        }
        case apsara::odps::sdk::ODPS_SMALLINT:
        {
          table_record.SetSmallIntValue(col_idx, datum.get_smallint());
          break;
        }
        case apsara::odps::sdk::ODPS_INTEGER:
        {
          table_record.SetIntegerValue(col_idx, datum.get_int32());
          break;
        }
        case apsara::odps::sdk::ODPS_BIGINT:
        {
          table_record.SetBigIntValue(col_idx, datum.get_int());
          break;
        }
        case apsara::odps::sdk::ODPS_FLOAT:
        {
          table_record.SetFloatValue(col_idx, datum.get_float());
          break;
        }
        case apsara::odps::sdk::ODPS_DOUBLE:
        {
          table_record.SetDoubleValue(col_idx, datum.get_double());
          break;
        }
        case apsara::odps::sdk::ODPS_DECIMAL:
        {
          std::string dec;
          if (OB_FAIL(decimal_to_string(datum, datum_meta, dec, allocator))) {
            LOG_WARN("failed to get string", K(ret));
          } else {
            table_record.SetDecimalValue(col_idx, dec);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_CHAR:
        case apsara::odps::sdk::ODPS_VARCHAR:
        {
          buf_size = datum.get_string().length() * ObCharset::MAX_MB_LEN;
          if (buf_size == 0 || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)) {
            res_len = static_cast<uint32_t>(datum.get_string().length());
            buf = const_cast<char *>(datum.get_string().ptr());
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_,
                                                        datum.get_string().ptr(),
                                                        datum.get_string().length(),
                                                        CS_TYPE_UTF8MB4_BIN,
                                                        buf,
                                                        buf_size,
                                                        res_len,
                                                        false,
                                                        false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if ((apsara::odps::sdk::ODPS_CHAR == odps_type && res_len > 255)
                     || (apsara::odps::sdk::ODPS_VARCHAR == odps_type && res_len > 65535)) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (OB_ISNULL(buf) && res_len == 0) {
            table_record.SetStringValue(col_idx, "", res_len, odps_type);
          } else {
            table_record.SetStringValue(col_idx, buf, res_len, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_STRING:
        case apsara::odps::sdk::ODPS_BINARY:
        {
          ObString lob_str;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                                datum,
                                                                datum_meta,
                                                                lob_str,
                                                                &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (lob_str.length() == 0 || apsara::odps::sdk::ODPS_BINARY == odps_type
                     || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)
                     || CS_TYPE_BINARY == datum_meta.cs_type_) {
            res_len = static_cast<uint32_t>(lob_str.length());
            buf = const_cast<char *>(lob_str.ptr());
          } else if (OB_FALSE_IT(buf_size = lob_str.length() * ObCharset::MAX_MB_LEN)) {
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_,
                                                        lob_str.ptr(),
                                                        lob_str.length(),
                                                        CS_TYPE_UTF8MB4_BIN,
                                                        buf,
                                                        buf_size,
                                                        res_len,
                                                        false,
                                                        false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (res_len > 8 * 1024 * 1024) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (OB_ISNULL(buf) && res_len == 0) {
            table_record.SetStringValue(col_idx, "", res_len, odps_type);
          } else {
            table_record.SetStringValue(col_idx, buf, res_len, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_JSON:
        {
          ObString json_str;
          ObIJsonBase *j_base = NULL;
          ObJsonBuffer jbuf(&allocator);
          ObJsonInType in_type = ObJsonInType::JSON_BIN;
          uint32_t parse_flag = lib::is_mysql_mode() ? 0 : ObJsonParser::JSN_RELAXED_FLAG;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                                datum,
                                                                datum_meta,
                                                                json_str,
                                                                &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, json_str, in_type,
                                                              in_type, j_base, parse_flag,
                                                              ObJsonExprHelper::get_json_max_depth_config()))) {
            COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
          } else if (OB_FAIL(j_base->print(jbuf, false))) { // json binary to string
            COMMON_LOG(WARN, "fail to convert json to string", K(ret));
          } else if (jbuf.length() > UINT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
          } else {
            LOG_DEBUG("set json value", K(datum_meta.cs_type_), K(ObString(jbuf.length(), jbuf.ptr())));
            table_record.SetJsonValue(col_idx, jbuf.ptr(), static_cast<uint32_t>(jbuf.length()));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_TIMESTAMP:
        case apsara::odps::sdk::ODPS_TIMESTAMP_NTZ:
        {
          int64_t datetime = datum.get_datetime();
          ObMySQLDateTime mdatetime = datetime;
          if (ob_is_mysql_datetime(ob_type)
              && ODPS_CAST_FAIL(ObTimeConverter::mdatetime_to_datetime(mdatetime, datetime, date_sql_mode))) {
            LOG_WARN("mdatetime_to_datetime", K(ret));
          }
          int64_t us = apsara::odps::sdk::ODPS_TIMESTAMP == odps_type
                       ? datum.get_timestamp()
                       : datetime;
          int64_t sec = us / 1000000;
          int32_t ns = (us % 1000000) * 1000;
          if (OB_FAIL(ret)) {
          } else if (us < ORACLE_DATETIME_MIN_VAL) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps timestamp min value is 0001-01-01 00:00:00", K(ret), K(us));
          } else {
            table_record.SetTimeValue(col_idx, sec, ns, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_DATE:
        {
          int32_t date = datum.get_date();
          ObMySQLDate mdate = date;
          if (ob_is_mysql_date_tc(ob_type)
              && ODPS_CAST_FAIL(ObTimeConverter::mdate_to_date(mdate, date, date_sql_mode))) {
            LOG_WARN("mdate_to_date fail", K(ret));
          } else if (date < ODPS_DATE_MIN_VAL) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps date min value is 0001-01-01", K(ret));
          } else {
            table_record.SetDateValue(col_idx, date);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_DATETIME:
        {
          int32_t tmp_offset = 0;
          int64_t datetime = datum.get_datetime();
          ObMySQLDateTime mdatetime = datetime;
          if (ob_is_mysql_datetime_tc(ob_type)
              && ODPS_CAST_FAIL(ObTimeConverter::mdatetime_to_datetime(mdatetime, datetime, date_sql_mode))) {
            LOG_WARN("mdatetime_to_datetime fail", K(ret));
          } else if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
            LOG_WARN("failed to get timezone offset", K(ret));
          } else if (datetime < ORACLE_DATETIME_MIN_VAL + SEC_TO_USEC(tmp_offset)) {
            ret = OB_DATETIME_FUNCTION_OVERFLOW;
            LOG_WARN("odps datetime min value is 0001-01-01 00:00:00", K(ret));
          } else {
            table_record.SetDatetimeValue(col_idx, (datetime - SEC_TO_USEC(tmp_offset)) / 1000);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_ARRAY:
        {
          ObODPSArrayHelper *array_helper = array_helpers_.at(col_idx);
          ObString array_bin_string;
          if (OB_ISNULL(array_helper)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(array_helper));
          } else {
            array_helper->array_->clear();
            shared_ptr<apsara::odps::sdk::ODPSArray> odps_array =
                        std::make_shared<apsara::odps::sdk::ODPSArray>(table_record.GetSchema()->GetTableColumn(col_idx).GetTypeInfo());
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator, datum, datum_meta,
                                                                  array_bin_string, &ctx_))) {
              LOG_WARN("failed to read string", K(ret));
            } else if (OB_FAIL(array_helper->array_->init(array_bin_string))) {
              LOG_WARN("failed to init array", K(ret));
            } else if (OB_FAIL(recursive_fill_list_record(odps_array, array_helper,
                                                          allocator, is_strict_mode, date_sql_mode))) {
              LOG_WARN("failed to recursice fill list builder");
            } else {
              table_record.SetArrayValue(col_idx, odps_array);
            }
          }
          break;
        }
        default:
        {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", K(ob_type), K(odps_type), K(ret));
        }
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value mysql", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value mysql", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value mysql", K(ret));
    }
  }
  return ret;
}

int ObODPSCppTableWriter::recursive_fill_list_record(shared_ptr<apsara::odps::sdk::ODPSArray> odps_array,
                                                ObODPSArrayHelper *array_helper,
                                                ObIAllocator &alloc,
                                                const bool is_strict_mode,
                                                const ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  ObIArrayType *child_array = nullptr;
  apsara::odps::sdk::ODPSColumnType child_odps_type = apsara::odps::sdk::ODPS_UNKNOWN;
  if (OB_ISNULL(odps_array) || OB_ISNULL(array_helper) ||
      OB_ISNULL(child_array = array_helper->array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child_array), K(array_helper));
  } else if (OB_FALSE_IT(child_odps_type = odps_array->GetElementType())) {
  } else if (apsara::odps::sdk::ODPS_ARRAY == child_odps_type &&
             ObCollectionSQLType == array_helper->element_type_) {
    ObArrayNested *nested_array = static_cast<ObArrayNested *>(array_helper->array_);
    ObIArrayType *child_array = nullptr;
    if (OB_ISNULL(array_helper->child_helper_) || OB_ISNULL(child_array = array_helper->child_helper_->array_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(array_helper->child_helper_), KP(child_array));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < nested_array->size(); ++i) {
        shared_ptr<apsara::odps::sdk::ODPSArray> child_odps_array =
                        std::make_shared<apsara::odps::sdk::ODPSArray>(odps_array->GetTypeInfo().mSubTypes.at(0));
        child_array->clear();
        if (nested_array->is_null(i)) {
          odps_array->AppendNull();
        } else if (OB_FAIL(nested_array->at(i, *child_array))) {
          LOG_WARN("failed to get elem", K(ret), K(i));
        } else if (OB_FAIL(SMART_CALL(recursive_fill_list_record(child_odps_array,
                                                                array_helper->child_helper_,
                                                                alloc, is_strict_mode, date_sql_mode)))) {
          LOG_WARN("failed to recursive fill list builder");
        } else {
          odps_array->AppendArrayValue(child_odps_array);
        }
      }
    }
  } else if (apsara::odps::sdk::ODPS_BIGINT == child_odps_type &&
             ObIntType == array_helper->element_type_) {
    if (!is_oracle_mode()) {
      ObArrayFixedSize<int64_t> *int_array = static_cast<ObArrayFixedSize<int64_t> *>(child_array);
      for (int64_t i = 0; OB_SUCC(ret) && i < int_array->size(); ++i) {
        if (int_array->is_null(i)) {
          odps_array->AppendNull();
        } else {
          odps_array->AppendBigIntValue((*int_array)[i]);
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_odps_type));
    }
  } else if (apsara::odps::sdk::ODPS_INTEGER == child_odps_type &&
             ObInt32Type == array_helper->element_type_) {
    if (!is_oracle_mode()) {
      ObArrayFixedSize<int32_t> *int_array = static_cast<ObArrayFixedSize<int32_t> *>(child_array);
      for (int64_t i = 0; OB_SUCC(ret) && i < int_array->size(); ++i) {
        if (int_array->is_null(i)) {
          odps_array->AppendNull();
        } else {
          odps_array->AppendIntegerValue((*int_array)[i]);
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_odps_type));
    }
  } else if (apsara::odps::sdk::ODPS_SMALLINT == child_odps_type &&
             ObSmallIntType == array_helper->element_type_) {
    if (!is_oracle_mode()) {
      ObArrayFixedSize<int16_t> *int_array = static_cast<ObArrayFixedSize<int16_t> *>(child_array);
      for (int64_t i = 0; OB_SUCC(ret) && i < int_array->size(); ++i) {
        if (int_array->is_null(i)) {
          odps_array->AppendNull();
        } else {
          odps_array->AppendSmallIntValue((*int_array)[i]);
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_odps_type));
    }
  } else if (apsara::odps::sdk::ODPS_TINYINT == child_odps_type &&
             ObTinyIntType == array_helper->element_type_) {
    if (!is_oracle_mode()) {
      ObArrayFixedSize<int8_t> *int_array = static_cast<ObArrayFixedSize<int8_t> *>(child_array);
      for (int64_t i = 0; OB_SUCC(ret) && i < int_array->size(); ++i) {
        if (int_array->is_null(i)) {
          odps_array->AppendNull();
        } else {
          odps_array->AppendTinyIntValue((*int_array)[i]);
        }
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_odps_type));
    }
  } else if (apsara::odps::sdk::ODPS_DOUBLE == child_odps_type &&
             ObDoubleType == array_helper->element_type_) {
    ObArrayFixedSize<double> *double_array = static_cast<ObArrayFixedSize<double> *>(child_array);
    for (int64_t i = 0; OB_SUCC(ret) && i < double_array->size(); ++i) {
      if (double_array->is_null(i)) {
        odps_array->AppendNull();
      } else {
        odps_array->AppendDoubleValue((*double_array)[i]);
      }
    }
  } else if (apsara::odps::sdk::ODPS_FLOAT == child_odps_type &&
             ObFloatType == array_helper->element_type_) {
    ObArrayFixedSize<float> *double_array = static_cast<ObArrayFixedSize<float> *>(child_array);
    for (int64_t i = 0; OB_SUCC(ret) && i < double_array->size(); ++i) {
      if (double_array->is_null(i)) {
        odps_array->AppendNull();
      } else {
        odps_array->AppendFloatValue((*double_array)[i]);
      }
    }
  } else if (apsara::odps::sdk::ODPS_BOOLEAN == child_odps_type) {
    if (!is_oracle_mode()) {
      if (ObTinyIntType == array_helper->element_type_) {
        ObArrayFixedSize<int8_t> *int_array = static_cast<ObArrayFixedSize<int8_t> *>(child_array);
        for (int64_t i = 0; OB_SUCC(ret) && i < int_array->size(); ++i) {
          if (int_array->is_null(i)) {
            odps_array->AppendNull();
          } else {
            odps_array->AppendBoolValue((*int_array)[i] != 0);
          }
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("arrow type not match with ob type", K(child_odps_type));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_odps_type));
    }
  } else if ((apsara::odps::sdk::ODPS_VARCHAR == child_odps_type ||
              apsara::odps::sdk::ODPS_STRING == child_odps_type) &&
             ObVarcharType == array_helper->element_type_) {
    ObArrayBinary * string_array = static_cast<ObArrayBinary *>(child_array);
    for (int64_t i = 0; OB_SUCC(ret) && i < string_array->size(); ++i) {
      if (string_array->is_null(i)) {
        odps_array->AppendNull();
      } else {
        ObString lob_str = (*string_array)[i];
        uint32_t res_len = 0;
        char *buf = NULL;
        int64_t buf_size = 0;
        if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(array_helper->element_collation_) ||
            CS_TYPE_BINARY == array_helper->element_collation_) {
          res_len = static_cast<uint32_t>(lob_str.length());
          buf = const_cast<char *>(lob_str.ptr());
        } else if (OB_FALSE_IT(buf_size = lob_str.length() * ObCharset::MAX_MB_LEN)) {
        } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else if (OB_FAIL(ObCharset::charset_convert(array_helper->element_collation_,
                                                      lob_str.ptr(),
                                                      lob_str.length(),
                                                      CS_TYPE_UTF8MB4_BIN,
                                                      buf,
                                                      buf_size,
                                                      res_len,
                                                      false,
                                                      false))) {
          LOG_WARN("failed to convert charset", K(ret));
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_ISNULL(buf) && res_len == 0) {
          odps_array->AppendNull();
        } else if (apsara::odps::sdk::ODPS_VARCHAR == child_odps_type) {
          odps_array->AppendVarcharValue(std::string(buf, res_len));
        } else {
          odps_array->AppendStringValue(std::string(buf, res_len));
        }
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("array not support", K(child_odps_type), K(array_helper->element_type_));
  }
  return ret;
}

int ObODPSCppTableWriter::set_odps_column_value_oracle(apsara::odps::sdk::ODPSTableRecord &table_record,
                                                 const ObDatum &datum,
                                                 const ObDatumMeta &datum_meta,
                                                 const ObObjMeta &obj_meta,
                                                 uint32_t col_idx)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  apsara::odps::sdk::ODPSColumnType odps_type;
  int64_t int_value = 0;
  uint32_t res_len = 0;
  char *buf = NULL;
  int64_t buf_size = 0;
  ObArenaAllocator allocator("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));
  try {
    if (OB_UNLIKELY(!(table_record.GetSchema()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (datum.is_null()) {
      table_record.SetNullValue(col_idx);
    } else {
      odps_type = table_record.GetSchema()->GetTableColumn(col_idx).GetType();
      switch (odps_type)
      {
        case apsara::odps::sdk::ODPS_BOOLEAN:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else {
            table_record.SetBoolValue(col_idx, int_value != 0);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_TINYINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT8_MIN || int_value > INT8_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            table_record.SetTinyIntValue(col_idx, static_cast<int8_t>(int_value));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_SMALLINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT16_MIN || int_value > INT16_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            table_record.SetSmallIntValue(col_idx, static_cast<int16_t>(int_value));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_INTEGER:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else if (int_value < INT32_MIN || int_value > INT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(ret));
          } else {
            table_record.SetIntegerValue(col_idx, static_cast<int32_t>(int_value));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_BIGINT:
        {
          if (OB_FAIL(decimal_or_number_to_int64(datum, datum_meta, int_value))) {
            LOG_WARN("failed to get int64", K(ret));
          } else {
            table_record.SetBigIntValue(col_idx, int_value);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_FLOAT:
        {
          table_record.SetFloatValue(col_idx, datum.get_float());
          break;
        }
        case apsara::odps::sdk::ODPS_DOUBLE:
        {
          table_record.SetDoubleValue(col_idx, datum.get_double());
          break;
        }
        case apsara::odps::sdk::ODPS_DECIMAL:
        {
          std::string dec;
          if (OB_FAIL(decimal_to_string(datum, datum_meta, dec, allocator))) {
            LOG_WARN("failed to get string", K(ret));
          } else {
            table_record.SetDecimalValue(col_idx, dec);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_CHAR:
        case apsara::odps::sdk::ODPS_VARCHAR:
        {
          buf_size = datum.get_string().length() * ObCharset::MAX_MB_LEN;
          if (buf_size == 0 || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)) {
            res_len = static_cast<uint32_t>(datum.get_string().length());
            buf = const_cast<char *>(datum.get_string().ptr());
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_,
                                                        datum.get_string().ptr(),
                                                        datum.get_string().length(),
                                                        CS_TYPE_UTF8MB4_BIN,
                                                        buf,
                                                        buf_size,
                                                        res_len,
                                                        false,
                                                        false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if ((apsara::odps::sdk::ODPS_CHAR == odps_type && res_len > 255)
                     || (apsara::odps::sdk::ODPS_VARCHAR == odps_type && res_len > 65535)) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (OB_ISNULL(buf) && res_len == 0) {
            table_record.SetStringValue(col_idx, "", res_len, odps_type);
          } else {
            table_record.SetStringValue(col_idx, buf, res_len, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_STRING:
        case apsara::odps::sdk::ODPS_BINARY:
        {
          ObString lob_str;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                                datum,
                                                                datum_meta,
                                                                lob_str,
                                                                &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (lob_str.length() == 0 || apsara::odps::sdk::ODPS_BINARY == odps_type
                     || CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(datum_meta.cs_type_)
                     || CS_TYPE_BINARY == datum_meta.cs_type_) {
            res_len = static_cast<uint32_t>(lob_str.length());
            buf = const_cast<char *>(lob_str.ptr());
          } else if (OB_FALSE_IT(buf_size = lob_str.length() * ObCharset::MAX_MB_LEN)) {
          } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(datum_meta.cs_type_,
                                                        lob_str.ptr(),
                                                        lob_str.length(),
                                                        CS_TYPE_UTF8MB4_BIN,
                                                        buf,
                                                        buf_size,
                                                        res_len,
                                                        false,
                                                        false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
          } else if (res_len > 8 * 1024 * 1024) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (OB_ISNULL(buf) && res_len == 0) {
            table_record.SetStringValue(col_idx, "", res_len, odps_type);
          } else {
            table_record.SetStringValue(col_idx, buf, res_len, odps_type);
          }
          break;
        }
        case apsara::odps::sdk::ODPS_JSON:
        {
          ObString json_str;
          ObIJsonBase *j_base = NULL;
          ObJsonBuffer jbuf(&allocator);
          ObJsonInType in_type = ObJsonInType::JSON_BIN;
          uint32_t parse_flag = lib::is_mysql_mode() ? 0 : ObJsonParser::JSN_RELAXED_FLAG;
          if (OB_FAIL(ObTextStringHelper::read_real_string_data(allocator,
                                                                datum,
                                                                datum_meta,
                                                                json_str,
                                                                &ctx_))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&allocator, json_str, in_type,
                                                              in_type, j_base, parse_flag,
                                                              ObJsonExprHelper::get_json_max_depth_config()))) {
            COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
          } else if (OB_FAIL(j_base->print(jbuf, false))) { // json binary to string
            COMMON_LOG(WARN, "fail to convert json to string", K(ret));
          } else if (jbuf.length() > UINT32_MAX) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
          } else {
            table_record.SetJsonValue(col_idx, jbuf.ptr(), static_cast<uint32_t>(jbuf.length()));
          }
          break;
        }
        case apsara::odps::sdk::ODPS_TIMESTAMP:
        case apsara::odps::sdk::ODPS_TIMESTAMP_NTZ:
        {
          ObOTimestampData timestamp = datum.get_otimestamp_tiny();
          table_record.SetTimeValue(col_idx,
                                    timestamp.time_us_ / 1000000,
                                    timestamp.time_ctx_.tail_nsec_ + (timestamp.time_us_ % 1000000) * 1000,
                                    odps_type);
          break;
        }
        case apsara::odps::sdk::ODPS_DATE:
        {
          table_record.SetDateValue(col_idx, datum.get_datetime() / 1000000 / 3600 / 24);
          break;
        }
        case apsara::odps::sdk::ODPS_DATETIME:
        {
          ObOTimestampData timestamp = datum.get_otimestamp_tiny();
          int32_t tmp_offset = 0;
          if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
            LOG_WARN("failed to get timezone offset", K(ret));
          } else {
            table_record.SetDatetimeValue(col_idx, (timestamp.time_us_ - SEC_TO_USEC(tmp_offset)) / 1000);
          }
          break;
        }
        default:
        {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", K(ob_type), K(odps_type), K(ret));
        }
      }
    }
  } catch (apsara::odps::sdk::OdpsException& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value oracle", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (const std::exception& ex) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value oracle", K(ret), K(ex.what()));
      LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
    }
  } catch (...) {
    if (OB_SUCC(ret)) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("caught exception when set odps column value oracle", K(ret));
    }
  }
  return ret;
}

int ObODPSCppTableWriter::decimal_to_string(const ObDatum &datum,
                                      const ObDatumMeta &datum_meta,
                                      std::string &res,
                                      ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  int64_t pos = 0;
  if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(OB_CAST_TO_VARCHAR_MAX_LENGTH)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret));
  } else if (ObDecimalIntType == datum_meta.get_type()) {
    if (OB_FAIL(wide::to_string(datum.get_decimal_int(), datum.get_int_bytes(), datum_meta.scale_,
                                buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, pos))) {
      LOG_WARN("failed to get string", K(ret));
    } else {
      res.assign(buf, pos);
    }
  } else if (ObNumberType == datum_meta.get_type()) {
    const number::ObNumber nmb(datum.get_number());
    if (OB_FAIL(nmb.format(buf, OB_CAST_TO_VARCHAR_MAX_LENGTH, pos, datum_meta.scale_))) {
      LOG_WARN("failed to format number", K(ret));
    } else {
      res.assign(buf, pos);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected decimal type", K(ret), K(datum_meta.get_type()));
  }
  return ret;
}

int ObODPSCppTableWriter::decimal_or_number_to_int64(const ObDatum &datum,
                                               const ObDatumMeta &datum_meta,
                                               int64_t &res)
{
  int ret = OB_SUCCESS;
  ObObjType ob_type = datum_meta.get_type();
  if (ObNumberType == ob_type) {
    const number::ObNumber nmb(datum.get_number());
    if (OB_FAIL(nmb.extract_valid_int64_with_trunc(res))) {
      LOG_WARN("failed to cast number to int64", K(ret));
    }
  } else if (ObDecimalIntType == ob_type) {
    int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
    bool is_valid;
    if (OB_FAIL(wide::check_range_valid_int64(datum.get_decimal_int(), int_bytes, is_valid, res))) {
      LOG_WARN("failed to check decimal int", K(int_bytes), K(ret));
    } else if (!is_valid) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("decimal int is not valid int64", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected type", K(ob_type), K(ret));
  }
  return ret;
}

int ObODPSCppTableWriter::close_impl()
{
  int ret = OB_SUCCESS;
  bool is_in_px = (NULL != ctx_.get_sqc_handler());
  if (is_in_px) {
    // The shared manager closes all C++ record writers after the last task.
  } else {
    std::vector<uint32_t> blocks;
    try {
      if (OB_UNLIKELY(!record_writer_ || !upload_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexcepted null ptr", K(ret));
      } else {
        record_writer_->Close();
        blocks.push_back(block_id_);
        if (need_commit_) {
          upload_->Commit(blocks);
        }
      }
    } catch (apsara::odps::sdk::OdpsException& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when commit", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (const std::exception& ex) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when commit", K(ret), K(ex.what()));
        LOG_USER_ERROR(OB_ODPS_ERROR, ex.what());
      }
    } catch (...) {
      if (OB_SUCC(ret)) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("caught exception when commit", K(ret));
      }
    }
  }
  return ret;
}
#endif

#ifdef OB_BUILD_JNI_ODPS
int ObODPSJniTableWriter::open_impl()
{
  int ret = OB_SUCCESS;
  bool is_in_px = (NULL != ctx_.get_sqc_handler());
  arrow_alloc_.init(MTL_ID());

  if (OB_FAIL(ObJniConnector::java_env_init())) {
    LOG_WARN("failed to env init", K(ret));
  } else if (is_in_px) {
    int task_id = options_.task_id_;
    block_id_ = task_id;
    ObOdpsJniUploadSessionMgr &odps_jni_mgr = *static_cast<ObOdpsJniUploadSessionMgr *>(upload_mgr_);
    if (OB_FAIL(odps_jni_mgr.get_odps_uploader_in_px(task_id, uploader_))) {
      LOG_WARN("failed to get odps writer from global config", K(ret));
    } else if (OB_FAIL(create_odps_schema())) {
      LOG_WARN("failed to create the odps schema", K(ret));
    }
  } else {
    common::hash::ObHashMap<ObString, ObString> writer_params;

    if (OB_FAIL(format_.decrypt())) {
      LOG_WARN("failed to decrypt odps format", K(ret));
    } else if (OB_FAIL(ObOdpsJniUploadSessionMgr::create_writer_params_map(ctx_.get_allocator(),
                                                                      format_,
                                                                      options_.partition_,
                                                                      options_.overwrite_,
                                                                      writer_params))) {
      LOG_WARN("failed to create params from odps format", K(ret));
    } else if (FALSE_IT(uploader_.writer_ptr = create_odps_jni_writer())) {
      /* do nothing */
    } else if (OB_FAIL(uploader_.writer_ptr->init_params(writer_params))) {
      LOG_WARN("failed to init writer params", K(ret));
    } else if (OB_FAIL(uploader_.writer_ptr->do_open())) {
      LOG_WARN("failed to do open writer", K(ret));
    } else if (OB_FAIL(uploader_.writer_ptr->do_open_record(block_id_))) {
      LOG_WARN("failed to open record writer", K(ret));
    } else if(OB_FAIL(create_odps_schema())) {
      LOG_WARN("failed to create the odps schema", K(ret));
    }

    if (writer_params.created()) {
      writer_params.destroy();
    }
  }
  return ret;
}

int ObODPSJniTableWriter::create_odps_schema()
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &select_exprs = options_.select_exprs_;
  const ObIArray<ObOdpsJniConnector::OdpsType> &col_type_from_odps = uploader_.writer_ptr->get_schema_from_odps();
  intptr_t export_schema_ptr = uploader_.writer_ptr->get_export_schema_ptr();

  if (export_schema_ptr == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("export schema ptr is null", K(ret));
  } else {
    arrow::Result<std::shared_ptr<arrow::DataType>> res = arrow::ImportType(reinterpret_cast<ArrowSchema*>(export_schema_ptr));
    if (!res.ok()) {
      ret = OB_ERR_INVALID_DATA_TYPE;
      LOG_WARN("failed to get remote arrow schema from odps jni", K(ret));
    } else if (select_exprs.count() != col_type_from_odps.count()) {
      ret = OB_ERR_TYPE_MISMATCH;
      LOG_WARN("select exprs count not equal to odps schema", K(ret));
    } else {
      std::shared_ptr<arrow::DataType> tableStructType = res.ValueOrDie();
      arrow_schema_ = arrow::schema(tableStructType->fields());
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
      ObODPSArrayHelper *array_helper = nullptr;
      ObExpr *cur_expr = select_exprs.at(i);
      if (OB_ISNULL(cur_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null");
      } else if (ObCollectionSQLType == cur_expr->datum_meta_.get_type() &&
                 OB_FAIL(ObODPSTableUtils::create_array_helper(eval_ctx_.exec_ctx_, ctx_.get_allocator(),
                                                               *cur_expr, array_helper))) {
        LOG_WARN("failed to init array helper");
      } else if (OB_FAIL(array_helpers_.push_back(array_helper))) {
        LOG_WARN("failed to push back array_helper to array");
      }
    }
  return ret;
}

template<typename T>
inline T arrow_get(ObIVector& expr_vector, int64_t idx)
{
  static_assert(sizeof(T) <= sizeof(int64_t), "invalid type");
  return *reinterpret_cast<const T *>(expr_vector.get_payload(idx));
}

using DaysChecker = std::function<bool(int32_t)>;
template <typename ArrowType, typename ObType>
int vectorize_fill_int_mysql(arrow::ArrayBuilder *builder, const ObBatchRows &brs,
                             ObIVector &expr_vector, ObDatumMeta &datum_meta, int &act_cnt,
                             const bool is_strict_mode, const ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::NumericBuilder<ArrowType> *builder_ref = dynamic_cast<arrow::NumericBuilder<ArrowType>*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (!is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          ObType value = arrow_get<ObType>(expr_vector, i);
          arrow::Status status = builder_ref->Append(value);
          if (OB_UNLIKELY(!status.ok())) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

template<typename ArrowType, typename ObType>
int vectorize_fill_int_oracle(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, ObDatumMeta &datum_meta, int &act_cnt) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::NumericBuilder<ArrowType> *builder_ref = dynamic_cast<arrow::NumericBuilder<ArrowType>*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (is_oracle_mode()){
    // oracle mode
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
        // do nothing
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          int64_t int_value = 0;

          if (ObNumberType == datum_meta.get_type()) {
            const number::ObNumber nmb(expr_vector.get_number(i));
            if (OB_FAIL(nmb.extract_valid_int64_with_trunc(int_value))) {
              LOG_WARN("failed to cast number to int64", K(ret));
            }
          } else if (ObDecimalIntType == datum_meta.get_type()) {
            int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
            bool is_valid;
            if (OB_FAIL(wide::check_range_valid_int64(expr_vector.get_decimal_int(i), int_bytes, is_valid, int_value))) {
              LOG_WARN("failed to check decimal int", K(int_bytes), K(ret));
            } else if (!is_valid) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("decimal int is not valid int64", K(ret));
            }
          } else {
            ret = OB_ERR_TYPE_MISMATCH;
            LOG_WARN("type mismatch in odps table and oracle mode table", K(ret), K(datum_meta));
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else if (OB_UNLIKELY(int_value > std::numeric_limits<ObType>::max() || int_value < std::numeric_limits<ObType>::min())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("int value is out of range", K(ret), K(int_value));
          } else {
            arrow::Status status = builder_ref->Append(int_value);
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

template<typename ObType>
int vectorize_fill_bool_mysql(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, int &act_cnt, ObDatumMeta &datum_meta) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::BooleanBuilder *builder_ref = dynamic_cast<arrow::BooleanBuilder*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (!is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          arrow::Status status = builder_ref->Append(arrow_get<ObType>(expr_vector, i) != 0);
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int vectorize_fill_bool_oracle(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, int &act_cnt, ObDatumMeta &datum_meta) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::BooleanBuilder *builder_ref = dynamic_cast<arrow::BooleanBuilder*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (is_oracle_mode()){
    // oracle mode
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
        // do nothing
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          int64_t int_value = 0;

          if (ObNumberType == datum_meta.get_type()) {
            const number::ObNumber nmb(expr_vector.get_number(i));
            if (OB_FAIL(nmb.extract_valid_int64_with_trunc(int_value))) {
              LOG_WARN("failed to cast number to int64", K(ret));
            }
          } else if (ObDecimalIntType == datum_meta.get_type()) {
            int32_t int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);
            bool is_valid;
            if (OB_FAIL(wide::check_range_valid_int64(expr_vector.get_decimal_int(i), int_bytes, is_valid, int_value))) {
              LOG_WARN("failed to check decimal int", K(int_bytes), K(ret));
            } else if (!is_valid) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("decimal int is not valid int64", K(ret));
            }
          } else {
            ret = OB_ERR_TYPE_MISMATCH;
            LOG_WARN("type mismatch in odps table and oracle mode table", K(ret), K(datum_meta));
          }

          if (OB_FAIL(ret)) {
            // do nothing
          } else {
            arrow::Status status = builder_ref->Append(int_value != 0);
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

template<typename ArrowType, typename ObType>
int vectorize_fill_double(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, int &act_cnt) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  arrow::NumericBuilder<ArrowType> *builder_ref = dynamic_cast<arrow::NumericBuilder<ArrowType>*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          arrow::Status status = builder_ref->Append(arrow_get<ObType>(expr_vector, i));
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

template<typename ArrowBuilderType>
int vectorize_fill_decimal(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, int &act_cnt, ObDatumMeta &datum_meta, ObIAllocator &alloc) {
  int ret = OB_SUCCESS;
  act_cnt = 0;
  ArrowBuilderType *builder_ref = dynamic_cast<ArrowBuilderType*>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else {
    int ob_precision = static_cast<int>(datum_meta.precision_);
    int ob_int_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(datum_meta.precision_);

    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          const ObDecimalInt *value = nullptr;
          ObDecimalInt *decint = nullptr;
          if (ObNumberType == datum_meta.get_type()) {
            const number::ObNumber nmb(expr_vector.get_number(i));
            if (OB_FAIL(wide::from_number_to_decimal_fixed_length(nmb, alloc, datum_meta.scale_,
                                                                  ob_int_bytes, decint))) {
              LOG_WARN("failed to convert number to decimal int", K(ret));
            } else {
              value = decint;
            }
          } else {
            value = expr_vector.get_decimal_int(i);
          }
          int64_t int_bytes = ArrowBuilderType::ValueType::kByteWidth;
          uint8_t buf[ArrowBuilderType::ValueType::kByteWidth];
          uint8_t* buffer = (uint8_t *)value;
          if (OB_FAIL(ret)) {
          } else if (ob_int_bytes > int_bytes) {
            ret = OB_ERR_TYPE_MISMATCH;
            LOG_WARN("failed to convert value to decimal", K(ob_int_bytes), K(int_bytes));
          } else {
            for (int i = 0; i < ob_int_bytes; ++i) {
              buf[ArrowBuilderType::ValueType::kByteWidth - i - 1] = buffer[i];
            }
            arrow::Result<typename ArrowBuilderType::ValueType> res = ArrowBuilderType::ValueType::FromBigEndian((const uint8_t *)buf, int_bytes);
            if (!res.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("failed to convert value to decimal");
            } else {
              arrow::Status status = builder_ref->Append(res.ValueOrDie());
              if (!status.ok()) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

template <typename ArrowBuilderType>
int vectorize_fill_string(arrow::ArrayBuilder *builder, const ObBatchRows &brs, ObIVector &expr_vector, ObDatumMeta &meta,
    const ObObjMeta &obj_meta, ObIAllocator &alloc, ObOdpsJniConnector::OdpsType odps_type, int& act_cnt)
{
  int ret = OB_SUCCESS;
  act_cnt = 0;
  ArrowBuilderType *builder_ref = dynamic_cast<ArrowBuilderType *>(builder);
  if (OB_ISNULL(builder_ref)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
      if (brs.skip_->contain(i)) {
      } else {
        ++act_cnt;
        if (expr_vector.is_null(i)) {
          arrow::Status status = builder_ref->AppendNull();
          if (!status.ok()) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else {
          ObString lob_str;
          uint32_t res_len = 0;
          char *buf = NULL;
          int64_t buf_size = 0;
          if (OB_FAIL(
                  ObTextStringHelper::read_real_string_data(alloc, &expr_vector, meta, lob_str, i))) {
            LOG_WARN("failed to read string", K(ret));
          } else if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(meta.cs_type_) ||
                     meta.cs_type_ == CS_TYPE_BINARY) {
            res_len = static_cast<uint32_t>(lob_str.length());
            buf = const_cast<char *>(lob_str.ptr());
          } else if (OB_FALSE_IT(buf_size = lob_str.length() * ObCharset::MAX_MB_LEN)) {
          } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc memory", K(ret));
          } else if (OB_FAIL(ObCharset::charset_convert(meta.cs_type_,
                         lob_str.ptr(),
                         lob_str.length(),
                         CS_TYPE_UTF8MB4_BIN,
                         buf,
                         buf_size,
                         res_len,
                         false,
                         false))) {
            LOG_WARN("failed to convert charset", K(ret));
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if ((ObOdpsJniConnector::OdpsType::CHAR == odps_type && res_len > 255) ||
                     (ObOdpsJniConnector::OdpsType::VARCHAR == odps_type && res_len > 65535) ||
                     (ObOdpsJniConnector::OdpsType::STRING == odps_type && res_len > 8 * 1024 * 1024) ||
                     (ObOdpsJniConnector::OdpsType::BINARY == odps_type && res_len > 8 * 1024 * 1024)) {
            ret = OB_DATA_OUT_OF_RANGE;
            LOG_WARN("string length out of range", K(res_len));
          } else if (OB_ISNULL(buf) && res_len == 0) {
            arrow::Status status = builder_ref->Append("", 0);
            if (!status.ok()) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
            }
          } else {
            arrow::Status status = builder_ref->Append(buf, res_len);
            if (!status.ok()) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to append null", K(ret), K(status.ToString().c_str()));
            }
          }
        }
      }
    }
  }
  return ret;
}


int ObODPSJniTableWriter::into_odps_jni_batch_one_col(int64_t col_idx, ObOdpsJniConnector::OdpsType odps_type,
    arrow::Field &arrow_field, ObDatumMeta &meta, ObObjMeta &obj_meta, ObIVector &expr_vector,
    arrow::ArrayBuilder *builder, const ObBatchRows &brs, int &act_cnt, ObIAllocator &alloc,
    const bool is_strict_mode, const ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  switch (odps_type) {
    case ObOdpsJniConnector::OdpsType::BIGINT:
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Int64Type, int64_t>(builder, brs, expr_vector, meta, act_cnt, is_strict_mode, date_sql_mode)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      } else {
        if (OB_FAIL((vectorize_fill_int_oracle<arrow::Int64Type, int64_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }
      break;
    case ObOdpsJniConnector::OdpsType::TINYINT:
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Int8Type, int8_t>(builder, brs, expr_vector, meta, act_cnt, is_strict_mode, date_sql_mode)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      } else {
        if (OB_FAIL((vectorize_fill_int_oracle<arrow::Int8Type, int8_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }

      break;
    case ObOdpsJniConnector::OdpsType::SMALLINT:
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Int16Type, int16_t>(builder, brs, expr_vector, meta, act_cnt, is_strict_mode, date_sql_mode)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      } else {
        if (OB_FAIL((vectorize_fill_int_oracle<arrow::Int16Type, int16_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }

      break;
    case ObOdpsJniConnector::OdpsType::INT:
      if (!is_oracle_mode()) {
        if (OB_FAIL((vectorize_fill_int_mysql<arrow::Int32Type, int32_t>(builder, brs, expr_vector, meta, act_cnt, is_strict_mode, date_sql_mode)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      } else {
        if (OB_FAIL((vectorize_fill_int_oracle<arrow::Int32Type, int32_t>(builder, brs, expr_vector, meta, act_cnt)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }
      break;
    case ObOdpsJniConnector::OdpsType::DOUBLE:
      if (OB_FAIL((vectorize_fill_double<arrow::DoubleType, double>(builder, brs, expr_vector, act_cnt)))) {
        LOG_WARN("fail to fill double", K(ret));
      }
      break;
    case ObOdpsJniConnector::OdpsType::FLOAT:
      if (OB_FAIL((vectorize_fill_double<arrow::FloatType, float>(builder, brs, expr_vector, act_cnt)))) {
        LOG_WARN("fail to fill float", K(ret));
      }
      break;
    case ObOdpsJniConnector::OdpsType::BOOLEAN:
      if (!is_oracle_mode()) {
        if (obj_meta.get_type() == ObTinyIntType) {
          if (OB_FAIL((vectorize_fill_bool_mysql<int8_t>(builder, brs, expr_vector, act_cnt, meta)))) {
            LOG_WARN("fail to fill int8", K(ret));
          }
        } else if (obj_meta.get_type() == ObSmallIntType) {
          if (OB_FAIL((vectorize_fill_bool_mysql<int16_t>(builder, brs, expr_vector, act_cnt, meta)))) {
            LOG_WARN("fail to fill int16", K(ret));
          }
        } else if (obj_meta.get_type() == ObMediumIntType || obj_meta.get_type() == ObInt32Type) {
          if (OB_FAIL((vectorize_fill_bool_mysql<int32_t>(builder, brs, expr_vector, act_cnt, meta)))) {
            LOG_WARN("fail to fill int32", K(ret));
          }
        } else if (obj_meta.get_type() == ObIntType) {
          if (OB_FAIL((vectorize_fill_bool_mysql<int64_t>(builder, brs, expr_vector, act_cnt, meta)))) {
            LOG_WARN("fail to fill int64", K(ret));
          }
        }
      } else {
        if (OB_FAIL((vectorize_fill_bool_oracle(builder, brs, expr_vector, act_cnt, meta)))) {
          LOG_WARN("fail to fill int64", K(ret));
        }
      }
      break;
    case ObOdpsJniConnector::OdpsType::VARCHAR:
    case ObOdpsJniConnector::OdpsType::CHAR:
    case ObOdpsJniConnector::OdpsType::STRING:
      if (OB_FAIL(vectorize_fill_string<arrow::StringBuilder>(builder, brs, expr_vector, meta, obj_meta, alloc, odps_type, act_cnt))) {
        LOG_WARN("failed to vectorize fill string");
      }
      break;
    case ObOdpsJniConnector::OdpsType::BINARY:
      if (OB_FAIL(vectorize_fill_string<arrow::BinaryBuilder>(builder, brs, expr_vector, meta, obj_meta, alloc, odps_type, act_cnt))) {
        LOG_WARN("failed to vectorize fill string");
      }
      break;
    case ObOdpsJniConnector::OdpsType::DECIMAL:
      if (arrow_field.type()->id() == arrow::Type::DECIMAL || arrow_field.type()->id() == arrow::Type::DECIMAL128) {
        if (OB_FAIL(vectorize_fill_decimal<arrow::Decimal128Builder>(builder, brs, expr_vector, act_cnt, meta, alloc))) {
          LOG_WARN("fail to vectorize decimal", K(ret));
        }
      } else if (arrow_field.type()->id() == arrow::Type::DECIMAL256) {
        if (OB_FAIL(vectorize_fill_decimal<arrow::Decimal256Builder>(builder, brs, expr_vector, act_cnt, meta, alloc))) {
          LOG_WARN("fail to vectorize decimal", K(ret));
        }
      }
      break;
    case ObOdpsJniConnector::OdpsType::DATE:  // arrowType = new ArrowType.Date(DateUnit.DAY);
    {
      if (!is_oracle_mode()) {
        arrow::NumericBuilder<arrow::Date32Type> *builder_ref =
            dynamic_cast<arrow::NumericBuilder<arrow::Date32Type> *>(builder);
        if (OB_ISNULL(builder_ref)) {
          ret = OB_ERR_TYPE_MISMATCH;
          LOG_WARN("builder is unexpected null", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
            if (brs.skip_->contain(i)) {
            } else {
              if (expr_vector.is_null(i)) {
                arrow::Status status = builder_ref->AppendNull();
                if (!status.ok()) {
                  ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                  LOG_WARN("fail to append null", K(ret));
                }
              } else {
                int32_t days = expr_vector.get_date(i);
                if (days >= ODPS_DATE_MIN_VAL) {
                  arrow::Status status = builder_ref->Append(days);
                  if (OB_UNLIKELY(!status.ok())) {
                    ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                    LOG_WARN("fail to append null", K(ret));
                  }
                } else {
                  ret = OB_DATETIME_FUNCTION_OVERFLOW;
                  LOG_WARN("data out of range", K(ret));
                }
              }
            }
          }
        }
      } else {
        arrow::NumericBuilder<arrow::Date32Type> *builder_ref =
            dynamic_cast<arrow::NumericBuilder<arrow::Date32Type> *>(builder);
        if (OB_ISNULL(builder_ref)) {
          ret = OB_ERR_TYPE_MISMATCH;
          LOG_WARN("builder is unexpected null", K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
            if (brs.skip_->contain(i)) {
            } else {
              if (expr_vector.is_null(i)) {
                arrow::Status status = builder_ref->AppendNull();
                if (!status.ok()) {
                  ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                  LOG_WARN("fail to append null", K(ret));
                }
              } else {
                int32_t days = expr_vector.get_datetime(i) / 1000000 / 3600 / 24;
                if (days >= ODPS_DATE_MIN_VAL) {
                  arrow::Status status = builder_ref->Append(days);
                  if (OB_UNLIKELY(!status.ok())) {
                    ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                    LOG_WARN("fail to append null", K(ret));
                  }
                } else {
                  ret = OB_DATETIME_FUNCTION_OVERFLOW;
                  LOG_WARN("data out of range", K(ret));
                }
              }
            }
          }
        }
      }
      break;
    }
    case ObOdpsJniConnector::OdpsType::DATETIME:  // arrowType = new ArrowType.Date(DateUnit.MILLISECOND);
    {
      arrow::NumericBuilder<arrow::Date64Type> *builder_ref =
          dynamic_cast<arrow::NumericBuilder<arrow::Date64Type> *>(builder);
      if (OB_ISNULL(builder_ref)) {
        ret = OB_ERR_TYPE_MISMATCH;
        LOG_WARN("builder is unexpected null", K(ret));
      } else {
        int32_t tmp_offset = 0;
        if (OB_ISNULL(ctx_.get_my_session()) || OB_ISNULL(ctx_.get_my_session()->get_timezone_info())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(ctx_.get_my_session()->get_timezone_info()->get_timezone_offset(0, tmp_offset))) {
          LOG_WARN("failed to get timezone offset", K(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
          if (brs.skip_->contain(i)) {
          } else if (expr_vector.is_null(i)) {
              arrow::Status status = builder_ref->AppendNull();
              if (!status.ok()) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
          } else {

            if (!is_oracle_mode()) {
              int64_t datetime = expr_vector.get_datetime(i);
              ObMySQLDateTime mdatetime = datetime;
              if (ob_is_mysql_datetime_tc(obj_meta.get_type())
                  && ODPS_CAST_FAIL(ObTimeConverter::mdatetime_to_datetime(mdatetime, datetime,
                                                                      date_sql_mode))) {
                LOG_WARN("mdatetime_to_datetime fail", K(ret));
              } else if (datetime < ORACLE_DATETIME_MIN_VAL + SEC_TO_USEC(tmp_offset)) {
                ret = OB_DATETIME_FUNCTION_OVERFLOW;
                LOG_WARN("odps datetime min value is 0001-01-01 00:00:00", K(ret));
              } else {
                arrow::Status status = builder_ref->Append((datetime - SEC_TO_USEC(tmp_offset)) / 1000);
                if (OB_UNLIKELY(!status.ok())) {
                  ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                  LOG_WARN("fail to append null", K(ret));
                }
              }
            } else {
              ObOTimestampTinyData timestamp = expr_vector.get_otimestamp_tiny(i);
              arrow::Status status = builder_ref->Append((timestamp.time_us_ - SEC_TO_USEC(tmp_offset)) / 1000);
              if (OB_UNLIKELY(!status.ok())) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            }
          }
        }
      }
      break;
    }
    case ObOdpsJniConnector::OdpsType::TIMESTAMP:
    case ObOdpsJniConnector::OdpsType::TIMESTAMP_NTZ:
      // arrowType = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
      // arrowType = new ArrowType.Timestamp(TimeUnit.NANOSECOND, null);
    {
      arrow::NumericBuilder<arrow::TimestampType> *builder_ref =
          dynamic_cast<arrow::NumericBuilder<arrow::TimestampType> *>(builder);

      if (OB_ISNULL(builder_ref)) {
        ret = OB_ERR_TYPE_MISMATCH;
        LOG_WARN("builder is unexpected null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
          if (brs.skip_->contain(i)) {
          } else if (expr_vector.is_null(i)) {
            arrow::Status status = builder_ref->AppendNull();
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          } else if (!is_oracle_mode()) {
            int64_t datetime = expr_vector.get_datetime(i);
            ObMySQLDateTime mdatetime = datetime;
            if (ob_is_mysql_datetime_tc(obj_meta.get_type())
                && ODPS_CAST_FAIL(
                  ObTimeConverter::mdatetime_to_datetime(mdatetime, datetime, date_sql_mode))) {
              LOG_WARN("mdatetime_to_datetime fail", K(ret));
            }
            int64_t us = ObOdpsJniWriter::OdpsType::TIMESTAMP == odps_type
                   ? expr_vector.get_timestamp(i)
                   : datetime;
            int64_t sec = us / 1000000;
            int32_t ns = (us % 1000000) * 1000;
            if (OB_FAIL(ret)) {
            } else if (us < ORACLE_DATETIME_MIN_VAL) {
              ret = OB_DATETIME_FUNCTION_OVERFLOW;
              LOG_WARN("odps timestamp min value is 0001-01-01 00:00:00", K(ret), K(us));
            } else {
              arrow::Status status = builder_ref->Append(us * 1000);
              if (!status.ok()) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            }
          } else {
            ObOTimestampData&& timestamp = const_cast<ObOTimestampTinyData*>(&expr_vector.get_otimestamp_tiny(i))->to_timestamp_data();
            arrow::Status status = builder_ref->Append(timestamp.time_us_ * 1000 + timestamp.time_ctx_.tail_nsec_);
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          }
        }
      }
      break;
    }
    case ObOdpsJniConnector::OdpsType::JSON:
    {
      arrow::StringBuilder *builder_ref =
          dynamic_cast<arrow::StringBuilder *>(builder);

      if (OB_ISNULL(builder_ref)) {
        ret = OB_ERR_TYPE_MISMATCH;
        LOG_WARN("builder is unexpected null", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
          if (brs.skip_->contain(i)) {
          } else if (expr_vector.is_null(i)) {
            arrow::Status status = builder_ref->AppendNull();
            if (!status.ok()) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          } else {
            ObString json_str;
            ObIJsonBase *j_base = NULL;
            ObJsonBuffer jbuf(&alloc);
            ObJsonInType in_type = ObJsonInType::JSON_BIN;
            uint32_t parse_flag = lib::is_mysql_mode() ? 0 : ObJsonParser::JSN_RELAXED_FLAG;
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(alloc,
                                                                  &expr_vector,
                                                                  meta,
                                                                  json_str,
                                                                  i))) {
              LOG_WARN("failed to read string", K(ret));
            } else if (OB_FAIL(ObJsonBaseFactory::get_json_base(&alloc, json_str, in_type,
                                                                in_type, j_base, parse_flag,
                                                                ObJsonExprHelper::get_json_max_depth_config()))) {
              COMMON_LOG(WARN, "fail to get json base", K(ret), K(in_type));
            } else if (OB_FAIL(j_base->print(jbuf, false))) { // json binary to string
              COMMON_LOG(WARN, "fail to convert json to string", K(ret));
            } else if (jbuf.length() > UINT32_MAX) {
              ret = OB_DATA_OUT_OF_RANGE;
              LOG_WARN("data out of range", K(odps_type), K(jbuf.length()), K(ret));
            } else {
              arrow::Status status = builder_ref->Append(jbuf.ptr(), jbuf.length());
              if (!status.ok()) {
                ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
                LOG_WARN("fail to append null", K(ret));
              }
            }
          }
        }
      }
      break;
    }
    case ObOdpsJniConnector::OdpsType::ARRAY:
    {
      arrow::ListBuilder *list_builder = dynamic_cast<arrow::ListBuilder *>(builder);
      arrow::ArrayBuilder *child_builder = nullptr;
      ObODPSArrayHelper *array_helper = array_helpers_.at(col_idx);
      if (OB_ISNULL(list_builder)) {
        ret = OB_ERR_TYPE_MISMATCH;
        LOG_WARN("builder is unexpected null", K(ret));
      } else if (OB_ISNULL(child_builder = list_builder->value_builder()) || OB_ISNULL(array_helper)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(child_builder), K(array_helper));
      } else {
        arrow::Type::type child_type_id = child_builder->type()->id();
        for (int64_t i = 0; OB_SUCC(ret) && i < brs.size_; ++i) {
          if (brs.skip_->contain(i)) {
          } else if (expr_vector.is_null(i)) {
            if (ARROW_FAIL(list_builder->AppendNull())) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("fail to append null", K(ret));
            }
          } else {
            ++act_cnt;
            array_helper->array_->clear();
            ObString array_bin_string;
            if (OB_FAIL(ObTextStringHelper::read_real_string_data(alloc, &expr_vector, meta,
                                                                  array_bin_string, i))) {
              LOG_WARN("failed to read string", K(ret));
            } else if (OB_FAIL(array_helper->array_->init(array_bin_string))) {
              LOG_WARN("failed to init array", K(ret));
            } else if (OB_FAIL(recursive_fill_list_builder(list_builder, child_builder, array_helper,
                                                           alloc, is_strict_mode, date_sql_mode))) {
              LOG_WARN("failed to recursice fill list builder");
            }
          }
        }
      }
      break;
    }
    case ObOdpsJniConnector::OdpsType::STRUCT:
    case ObOdpsJniConnector::OdpsType::INTERVAL_DAY_TIME:
    case ObOdpsJniConnector::OdpsType::INTERVAL_YEAR_MONTH:
    case ObOdpsJniConnector::OdpsType::MAP:
    default:
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(odps_type), K(ret));
      break;
  }
  return ret;
}

template <typename ArrowType, typename ObType>
int vectorize_fill_array_int_mysql(arrow::ArrayBuilder *child_builder,
                                   ObIArrayType *child_array,
                                   ObObjType &element_type,
                                   const bool is_strict_mode,
                                   const ObDateSqlMode date_sql_mode,
                                   std::shared_ptr<std::function<bool(ObType)>> filter = nullptr)
{
  int ret = OB_SUCCESS;
  arrow::NumericBuilder<ArrowType> *builder = dynamic_cast<arrow::NumericBuilder<ArrowType>*>(child_builder);
  ObArrayFixedSize<ObType> *int_array = static_cast<ObArrayFixedSize<ObType> *>(child_array);
  if (!is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < int_array->size(); ++i) {
      if (int_array->is_null(i)) {
        if (ARROW_FAIL(builder->AppendNull())) {
          ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
          LOG_WARN("fail to append null", K(ret));
        }
      } else {
        ObType value = (*int_array)[i];
        if (ob_is_mysql_date_tc(element_type)) {
          ObMySQLDate md_value(value);
          int32_t d_value = 0;
          if (ODPS_CAST_FAIL(ObTimeConverter::mdate_to_date(md_value, d_value, date_sql_mode))) {
            LOG_WARN("mdate_to_date fail", K(ret));
          } else {
            value = d_value;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_LIKELY(filter == nullptr) || ((*filter)(value))) {
          if (ARROW_FAIL(builder->Append(value))) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append int value", K(ret));
          }
        } else {
          ret = OB_DATETIME_FUNCTION_OVERFLOW;
          LOG_WARN("data out of range", K(ret));
        }
      }
    }
  }
  return ret;
}

template<typename ArrowType, typename ObType>
int vectorize_fill_array_double(arrow::ArrayBuilder *child_builder,
                                ObIArrayType *child_array)
{
  int ret = OB_SUCCESS;
  arrow::NumericBuilder<ArrowType> *builder = dynamic_cast<arrow::NumericBuilder<ArrowType>*>(child_builder);
  ObArrayFixedSize<ObType> *double_array = static_cast<ObArrayFixedSize<ObType> *>(child_array);
  if (OB_ISNULL(builder)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < double_array->size(); ++i) {
      if (double_array->is_null(i)) {
        if (ARROW_FAIL(builder->AppendNull())) {
          ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
          LOG_WARN("fail to append null", K(ret));
        }
      } else if (ARROW_FAIL(builder->Append((*double_array)[i]))) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        LOG_WARN("fail to append int value", K(ret));
      }
    }
  }
  return ret;
}

int vectorize_fill_array_bool(arrow::ArrayBuilder *child_builder,
                              ObIArrayType *child_array) {
  int ret = OB_SUCCESS;
  arrow::BooleanBuilder *builder = dynamic_cast<arrow::BooleanBuilder*>(child_builder);
  ObArrayFixedSize<int8_t> *int_array = static_cast<ObArrayFixedSize<int8_t> *>(child_array);
  if (OB_ISNULL(builder)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else if (!is_oracle_mode()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < int_array->size(); ++i) {
      if (int_array->is_null(i)) {
        if (ARROW_FAIL(builder->AppendNull())) {
          ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
          LOG_WARN("fail to append null", K(ret));
        }
      } else {
        bool v = ((*int_array)[i] != 0);
        if (ARROW_FAIL(builder->Append(v))) {
          ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
          LOG_WARN("fail to append int value", K(ret));
        }
      }
    }
  }
  return ret;
}

template <typename ArrowBuilderType>
int vectorize_fill_array_string(arrow::ArrayBuilder *child_builder,
                                ObIArrayType *child_array,
                                ObODPSArrayHelper *array_helper,
                                ObIAllocator &alloc)
                                // ObOdpsJniWriter::OdpsType odps_type)
{
  int ret = OB_SUCCESS;
  ArrowBuilderType *builder = dynamic_cast<ArrowBuilderType *>(child_builder);
  ObArrayBinary * string_array = static_cast<ObArrayBinary *>(child_array);
  if (OB_ISNULL(builder)) {
    ret = OB_ERR_TYPE_MISMATCH;
    LOG_WARN("builder is unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < string_array->size(); ++i) {
      if (string_array->is_null(i)) {
        if (ARROW_FAIL(builder->AppendNull())) {
          ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
          LOG_WARN("fail to append null", K(ret));
        }
      } else {
        ObString lob_str = (*string_array)[i];
        uint32_t res_len = 0;
        char *buf = NULL;
        int64_t buf_size = 0;

        if (CHARSET_UTF8MB4 == ObCharset::charset_type_by_coll(array_helper->element_collation_) ||
            CS_TYPE_BINARY == array_helper->element_collation_) {
          res_len = static_cast<uint32_t>(lob_str.length());
          buf = const_cast<char *>(lob_str.ptr());
        } else if (OB_FALSE_IT(buf_size = lob_str.length() * ObCharset::MAX_MB_LEN)) {
        } else if (OB_ISNULL(buf = static_cast<char *>(alloc.alloc(buf_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory", K(ret));
        } else if (OB_FAIL(ObCharset::charset_convert(array_helper->element_collation_,
                                                      lob_str.ptr(),
                                                      lob_str.length(),
                                                      CS_TYPE_UTF8MB4_BIN,
                                                      buf,
                                                      buf_size,
                                                      res_len,
                                                      false,
                                                      false))) {
          LOG_WARN("failed to convert charset", K(ret));
        }
        if (OB_FAIL(ret)) {
          // do nothing
        // } else if ((ObOdpsJniWriter::OdpsType::CHAR == odps_type && res_len > 255) ||
        //            (ObOdpsJniWriter::OdpsType::VARCHAR == odps_type && res_len > 65535) ||
        //            (ObOdpsJniWriter::OdpsType::STRING == odps_type && res_len > 8 * 1024 * 1024) ||
        //            (ObOdpsJniWriter::OdpsType::BINARY == odps_type && res_len > 8 * 1024 * 1024)) {
        //   ret = OB_DATA_OUT_OF_RANGE;
        //   LOG_WARN("string length out of range", K(res_len));
        } else if (OB_ISNULL(buf) && res_len == 0) {
          if (ARROW_FAIL(builder->Append("", 0))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to append empty string");
          }
        } else if (ARROW_FAIL(builder->Append(buf, res_len))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to append string");
        }
      }
    }
  }
  return ret;
}

int ObODPSJniTableWriter::recursive_fill_list_builder(arrow::ListBuilder *list_builder,
                                                arrow::ArrayBuilder *child_builder,
                                                ObODPSArrayHelper *array_helper,
                                                ObIAllocator &alloc,
                                                const bool is_strict_mode,
                                                const ObDateSqlMode date_sql_mode)
{
  int ret = OB_SUCCESS;
  arrow::Type::type child_type_id = child_builder->type()->id();
  ObIArrayType *child_array = nullptr;
  if (OB_ISNULL(list_builder) || OB_ISNULL(child_builder) ||
      OB_ISNULL(array_helper) || OB_ISNULL(child_array = array_helper->array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(list_builder), K(child_builder), K(array_helper), K(child_array));
  } else if (ARROW_FAIL(list_builder->Append())) {
    ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
    LOG_WARN("fail to append list vlaue", K(ret));
  } else if (arrow::Type::LIST == child_type_id) {
    if (ObCollectionSQLType == array_helper->element_type_) {
      arrow::ListBuilder *child_list_builder = dynamic_cast<arrow::ListBuilder*>(child_builder);
      ObArrayNested *nested_array = static_cast<ObArrayNested *>(array_helper->array_);
      ObIArrayType *child_array = nullptr;
      if (OB_ISNULL(array_helper->child_helper_) || OB_ISNULL(child_array = array_helper->child_helper_->array_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(array_helper->child_helper_), KP(child_array));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < nested_array->size(); ++i) {
        child_array->clear();
        if (nested_array->is_null(i)) {
          if (ARROW_FAIL(child_list_builder->AppendNull())) {
            ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
            LOG_WARN("fail to append null", K(ret));
          }
        } else if (OB_FAIL(nested_array->at(i, *child_array))) {
          LOG_WARN("failed to get elem", K(ret), K(i));
        } else if (OB_FAIL(SMART_CALL(recursive_fill_list_builder(child_list_builder,
                                                                  child_list_builder->value_builder(),
                                                                  array_helper->child_helper_,
                                                                  alloc, is_strict_mode, date_sql_mode)))) {
          LOG_WARN("failed to recursive fill list builder");
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("arrow type not match with ob type", K(array_helper->element_type_));
    }
  } else if (arrow::Type::INT64 == child_type_id && ObIntType == array_helper->element_type_) {
    if (!is_oracle_mode()) {
      if (OB_FAIL((vectorize_fill_array_int_mysql<arrow::Int64Type, int64_t>(child_builder, child_array,
                                                                             array_helper->element_type_,
                                                                             is_strict_mode, date_sql_mode)))) {
        LOG_WARN("fail to fill array int64", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_type_id), K(ret));
    }
  } else if (arrow::Type::INT32 == child_type_id && ObInt32Type == array_helper->element_type_) {
    if (!is_oracle_mode()) {
      if (OB_FAIL((vectorize_fill_array_int_mysql<arrow::Int32Type, int32_t>(child_builder, child_array,
                                                                             array_helper->element_type_,
                                                                             is_strict_mode, date_sql_mode)))) {
        LOG_WARN("fail to fill array int64", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_type_id), K(ret));
    }
  } else if (arrow::Type::INT16 == child_type_id && ObSmallIntType == array_helper->element_type_) {
    if (!is_oracle_mode()) {
      if (OB_FAIL((vectorize_fill_array_int_mysql<arrow::Int16Type, int16_t>(child_builder, child_array,
                                                                             array_helper->element_type_,
                                                                             is_strict_mode, date_sql_mode)))) {
        LOG_WARN("fail to fill array int64", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_type_id), K(ret));
    }
  } else if (arrow::Type::INT8 == child_type_id && ObTinyIntType == array_helper->element_type_) {
    if (!is_oracle_mode()) {
      if (OB_FAIL((vectorize_fill_array_int_mysql<arrow::Int8Type, int8_t>(child_builder, child_array,
                                                                           array_helper->element_type_,
                                                                           is_strict_mode, date_sql_mode)))) {
        LOG_WARN("fail to fill array int64", K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_type_id), K(ret));
    }
  } else if (arrow::Type::DOUBLE == child_type_id && ObDoubleType == array_helper->element_type_) {
    if (OB_FAIL((vectorize_fill_array_double<arrow::DoubleType, double>(child_builder, child_array)))) {
      LOG_WARN("fail to fill array double", K(ret));
    }
  } else if (arrow::Type::FLOAT == child_type_id && ObFloatType == array_helper->element_type_) {
    if (OB_FAIL((vectorize_fill_array_double<arrow::FloatType, float>(child_builder, child_array)))) {
      LOG_WARN("fail to fill array double", K(ret));
    }
  } else if (arrow::Type::BOOL == child_type_id) {
    if (!is_oracle_mode()) {
      if (ObTinyIntType == array_helper->element_type_) {
        if (OB_FAIL(vectorize_fill_array_bool(child_builder, child_array))) {
          LOG_WARN("fail to fill int8", K(ret));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("arrow type not match with ob type", K(child_type_id), K(ret));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("array(number) not support in oracle mode", K(child_type_id), K(ret));
    }
  } else if (arrow::Type::STRING == child_type_id) {
    if (OB_FAIL(vectorize_fill_array_string<arrow::StringBuilder>(child_builder, child_array, array_helper, alloc))) {
      LOG_WARN("failed to vectorize fill array string");
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("array not support", K(child_type_id), K(array_helper->element_type_));
  }
  return ret;
}

int ObODPSJniTableWriter::write_batch_impl(const ObBatchRows &brs)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObExpr *> &select_exprs = options_.select_exprs_;
  ObSQLMode sql_mode = eval_ctx_.exec_ctx_.get_my_session()->get_sql_mode();
  ObDateSqlMode date_sql_mode;
  date_sql_mode.init(sql_mode);
  bool is_strict_mode = common::is_strict_mode(sql_mode);
  ObArray<common::ObIVector *> expr_vectors;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); ++i) {
    ObExpr *cur_expr = select_exprs.at(i);
    if (OB_ISNULL(cur_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else if (OB_FAIL(cur_expr->eval_vector(eval_ctx_, brs))) {
      LOG_WARN("failed to eval vector", K(ret));
    } else if (cur_expr->is_nested_expr() && !is_uniform_format(cur_expr->get_format(eval_ctx_)) &&
               OB_FAIL(cur_expr->cast_to_uniform(brs.size_, eval_ctx_))) {
      LOG_WARN("failed to cast to uniform");
    } else if (OB_FAIL(expr_vectors.push_back(cur_expr->get_vector(eval_ctx_)))) {
      LOG_WARN("failed to push back vector", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to push back datum vector", K(ret));
  } else if (OB_ISNULL(uploader_.writer_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_FAIL(uploader_.writer_ptr->get_current_block_addr())) {
    LOG_WARN("failed to get bloock addr");
  } else {
    // TODO alloc
    // ObIAllocator& alloc = ctx_.get_allocator();
    ObArenaAllocator allocator("IntoOdps", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObMallocHookAttrGuard guard(ObMemAttr(MTL_ID(), "IntoOdps"));//???

    const ObIArray<ObOdpsJniConnector::OdpsType> &col_type_from_odps = uploader_.writer_ptr->get_schema_from_odps();
    const arrow::FieldVector &type_vec = arrow_schema_->fields();

    arrow::Result<std::shared_ptr<arrow::RecordBatchBuilder> > rbatchRes =
        arrow::RecordBatchBuilder::Make(arrow_schema_, &arrow_alloc_);
    if (!rbatchRes.ok()) {
      ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
      LOG_WARN("fail to make empty record batch", K(ret));
    } else {
      std::shared_ptr<arrow::RecordBatchBuilder> rbatch = rbatchRes.ValueOrDie();
      int act_cnt = 0;
      // 向量化
      for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < select_exprs.count(); ++col_idx) {
        ObDatumMeta &meta = select_exprs.at(col_idx)->datum_meta_;
        ObObjMeta &obj_meta = select_exprs.at(col_idx)->obj_meta_;
        ObIVector *vector = expr_vectors.at(col_idx);
        arrow::ArrayBuilder *array_builder = rbatch->GetField(col_idx);
        if (OB_ISNULL(vector) || OB_ISNULL(array_builder) || OB_ISNULL(type_vec.at(col_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("vector or array builder is unexpected null", K(ret));
        } else if (OB_FAIL(into_odps_jni_batch_one_col(col_idx,
                       col_type_from_odps.at(col_idx),
                       *type_vec.at(col_idx),
                       meta,
                       obj_meta,
                       *vector,
                       array_builder,
                       brs,
                       act_cnt,
                       allocator,
                       is_strict_mode,
                       date_sql_mode))) {
          LOG_WARN("failed to into odps jni batch one col", K(ret));
        }
      }

      arrow::Result<std::shared_ptr<arrow::RecordBatch>> res;
      std::shared_ptr<arrow::StructArray> array;
      if (OB_FAIL(ret)) {
        LOG_WARN("failed to build the batch", K(ret));
      } else if (FALSE_IT(res = rbatch->Flush())) {
        // do nothing
      } else if (!res.ok()) {
        ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
        std::ostringstream stream;
        stream << res.status();
        LOG_WARN("fail to export array", K(ret), K(stream.str().c_str()));
      } else {
        struct ArrowSchema *c_schema = reinterpret_cast<struct ArrowSchema *>(uploader_.writer_ptr->get_schema_ptr());
        struct ArrowArray *c_array = reinterpret_cast<struct ArrowArray *>(uploader_.writer_ptr->get_array_ptr());
        std::shared_ptr<arrow::RecordBatch> record_batch_ptr = res.ValueOrDie();
        if (OB_SUCC(ret)) {
          if (OB_ISNULL(c_array) || OB_ISNULL(c_schema)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("array ptr is unexpected null", K(ret));
          } else {
            arrow::Status c_array_status = arrow::ExportRecordBatch(*record_batch_ptr, c_array, c_schema);
            if (!c_array_status.ok()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to export scheam", K(ret));
            }
          }
        }
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (OB_FAIL(uploader_.writer_ptr->do_write_next_brs(0, act_cnt))) {
          LOG_WARN("failed to write data", K(ret));
        } else {
          bool is_in_px = (NULL != ctx_.get_sqc_handler());
          if (is_in_px) {
            ObOdpsJniUploadSessionMgr &odps_jni_mgr
                = *static_cast<ObOdpsJniUploadSessionMgr *>(upload_mgr_);
            if (OB_FAIL(odps_jni_mgr.append_block_id(block_id_))) {
              LOG_WARN("failed to append block id", K(ret));
            }
          } else {
            if (OB_FAIL(uploader_.writer_ptr->append_block_id(block_id_))) {
              LOG_WARN("failed to append block id", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObODPSJniTableWriter::close_impl()
{
  int ret = OB_SUCCESS;
  bool is_in_px = (NULL != ctx_.get_sqc_handler());
  if (is_in_px) {
    if (OB_ISNULL(uploader_.writer_ptr.get())) {
      ret = OB_ODPS_ERROR;
      LOG_WARN("failed to get the writer by sid", K(ret));
    } else if (OB_FAIL(uploader_.writer_ptr->finish_write())) {
      LOG_WARN("failed to finish write", K(ret));
    }
    // The base writer reports success/failure to the shared session manager.
  } else {
    if (OB_ISNULL(uploader_.writer_ptr.get())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexcepted null ptr", K(ret));
    } else if (OB_FAIL(uploader_.writer_ptr->finish_write())) {
      LOG_WARN("failed to finish write", K(ret));
    } else if (need_commit_) {
      if (OB_FAIL(uploader_.writer_ptr->commit_session(0))) {
        ret = OB_ODPS_ERROR;
        LOG_WARN("commmit failed in odps write");
      }
    }
  }
  return ret;
}
#endif

#undef ARROW_FAIL
#undef ODPS_CAST_FAIL
} // namespace sql
} // namespace oceanbase
