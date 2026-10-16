/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "ob_das_dml_ctx_define.h"
#include "sql/das/ob_das_utils.h"
#include "sql/das/ob_das_domain_utils.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "storage/blocksstable/ob_datum_row_utils.h"
#include "sql/das/ob_das_search_index_utils.h"
#include "storage/lob/ob_lob_manager.h"
#include "sql/engine/expr/ob_expr_vec_ivf_center_id.h"
#include "sql/das/ob_ivf_cid_util.h"
namespace oceanbase
{
namespace sql
{

const uint8_t ObIVFDMLCtDef::DISABLED_PROTOCOL_VERSION;
const uint8_t ObIVFDMLCtDef::ABANDONED_EXPR_PROTOCOL_VERSION;
const uint8_t ObIVFDMLCtDef::IVF_CTDEF_VERSION;
const uint16_t ObIVFDMLCtDef::INVALID_VECTOR_SUBSCHEMA_ID;

ObIVFDMLCtDef::ObIVFDMLCtDef()
{
  reset();
}

bool ObIVFDMLCtDef::is_valid() const
{
  return IVF_CTDEF_VERSION == protocol_version_
      && cid_store_idx_ >= 0
      && vector_store_idx_ >= 0
      && cid_store_idx_ != vector_store_idx_
      && INVALID_VECTOR_SUBSCHEMA_ID != vector_subschema_id_
      && common::OB_INVALID_ID != centroid_table_id_
      && distance_algorithm_ < static_cast<uint8_t>(share::VIDA_MAX);
}

bool ObIVFDMLCtDef::is_valid_for_row_count(const int64_t row_count) const
{
  return is_valid()
      && row_count > 0
      && cid_store_idx_ < row_count
      && vector_store_idx_ < row_count;
}

int ObIVFDMLCtDef::init_v2(
    const int64_t cid_store_idx,
    const int64_t vector_store_idx,
    const uint16_t vector_subschema_id,
    const uint64_t centroid_table_id,
    const uint8_t distance_algorithm,
    const int64_t row_count)
{
  int ret = OB_SUCCESS;
  reset();
  protocol_version_ = IVF_CTDEF_VERSION;
  cid_store_idx_ = cid_store_idx;
  vector_store_idx_ = vector_store_idx;
  vector_subschema_id_ = vector_subschema_id;
  centroid_table_id_ = centroid_table_id;
  distance_algorithm_ = distance_algorithm;
  if (!is_valid_for_row_count(row_count)) {
    ret = OB_INVALID_ARGUMENT;
    reset();
  }
  return ret;
}

void ObIVFDMLCtDef::reset()
{
  protocol_version_ = DISABLED_PROTOCOL_VERSION;
  cid_store_idx_ = common::OB_INVALID_INDEX;
  vector_store_idx_ = common::OB_INVALID_INDEX;
  vector_subschema_id_ = INVALID_VECTOR_SUBSCHEMA_ID;
  centroid_table_id_ = common::OB_INVALID_ID;
  distance_algorithm_ = static_cast<uint8_t>(share::VIDA_MAX);
}

OB_DEF_SERIALIZE(ObIVFDMLCtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, protocol_version_, cid_store_idx_, vector_store_idx_,
              vector_subschema_id_, centroid_table_id_, distance_algorithm_);
  return ret;
}

OB_DEF_DESERIALIZE(ObIVFDMLCtDef)
{
  int ret = OB_SUCCESS;
  // A legacy payload ends before this appended field and therefore remains
  // protocol v0. Any present nonzero unsupported version fails atomically.
  if (pos < data_len) {
    LST_DO_CODE(OB_UNIS_DECODE, protocol_version_, cid_store_idx_, vector_store_idx_,
                vector_subschema_id_, centroid_table_id_, distance_algorithm_);
  }
  if (OB_FAIL(ret)) {
    reset();
  } else if (ABANDONED_EXPR_PROTOCOL_VERSION == protocol_version_) {
    reset();
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("logical IVF CtDef expression-pointer protocol is abandoned", K(ret));
  } else if (DISABLED_PROTOCOL_VERSION != protocol_version_
      && IVF_CTDEF_VERSION != protocol_version_) {
    const uint8_t unsupported_version = protocol_version_;
    reset();
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unsupported logical IVF CtDef protocol version",
             K(ret), K(unsupported_version));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObIVFDMLCtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, protocol_version_, cid_store_idx_, vector_store_idx_,
              vector_subschema_id_, centroid_table_id_, distance_algorithm_);
  return len;
}

int resolve_ivf_centroid_tablet(
    const DASCtDefFixedArray &related_ctdefs,
    const ObIArray<ObTabletID> &related_tablet_ids,
    const uint64_t centroid_table_id,
    ObTabletID &centroid_tablet_id)
{
  int ret = OB_SUCCESS;
  bool found = false;
  centroid_tablet_id.reset();
  if (OB_INVALID_ID == centroid_table_id
      || related_ctdefs.count() != related_tablet_ids.count()) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs.count(); ++i) {
      const ObDASDMLBaseCtDef *candidate =
          static_cast<const ObDASDMLBaseCtDef *>(related_ctdefs.at(i));
      if (OB_ISNULL(candidate) || candidate->index_tid_ != centroid_table_id) {
      } else if (found
          || !related_tablet_ids.at(i).is_valid()
          || !share::schema::is_local_vec_ivf_centroid_index(
              candidate->table_param_.get_data_table().get_index_type())) {
        // Table id, tablet id, and centroid role must all match. Never fall
        // back to the task's primary tablet or a same-id non-centroid CtDef.
        ret = OB_ERR_UNEXPECTED;
      } else {
        found = true;
        centroid_tablet_id = related_tablet_ids.at(i);
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObDASDMLBaseCtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
    table_id_,
    index_tid_,
    rowkey_cnt_,
    spk_cnt_,
    schema_version_,
    column_ids_,
    column_types_,
    column_accuracys_,
    old_row_projector_,
    new_row_projector_,
    tz_info_,
    table_param_,
    encrypt_meta_,
    flags_,
    logical_new_row_exprs_,
    logical_old_row_exprs_,
    lt_namespace_id_,
    lt_logical_table_id_,
    lt_physical_data_table_id_,
    lt_physical_namespace_col_id_,
    lt_physical_key_col_id_,
    lt_physical_value_col_id_,
    lt_tablet_mappings_);
  if (OB_SUCC(ret)) {
    int64_t aux_type_val = static_cast<int64_t>(logical_table_aux_type_);
    OB_UNIS_ENCODE(aux_type_val);
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_ENCODE(logical_column_mapping_);
  }
  if (OB_SUCC(ret)) {
    bool has_phys_param = (nullptr != lt_physical_table_param_);
    OB_UNIS_ENCODE(has_phys_param);
    if (has_phys_param) {
      OB_UNIS_ENCODE(*lt_physical_table_param_);
    }
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, lt_ns_id_expr_, lt_id_expr_);
  }
  if (OB_SUCC(ret) && has_ivf_dml_ctdef_) {
    OB_UNIS_ENCODE(ivf_dml_ctdef_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASDMLBaseCtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
    table_id_,
    index_tid_,
    rowkey_cnt_,
    spk_cnt_,
    schema_version_,
    column_ids_,
    column_types_,
    column_accuracys_,
    old_row_projector_,
    new_row_projector_,
    tz_info_,
    table_param_,
    encrypt_meta_,
    flags_,
    logical_new_row_exprs_,
    logical_old_row_exprs_,
    lt_namespace_id_,
    lt_logical_table_id_,
    lt_physical_data_table_id_,
    lt_physical_namespace_col_id_,
    lt_physical_key_col_id_,
    lt_physical_value_col_id_,
    lt_tablet_mappings_);
  if (OB_SUCC(ret)) {
    int64_t aux_type_val = 0;
    OB_UNIS_DECODE(aux_type_val);
    if (OB_SUCC(ret)) {
      logical_table_aux_type_ = static_cast<share::ObLogicalTableAuxType>(aux_type_val);
    }
  }
  if (OB_SUCC(ret)) {
    OB_UNIS_DECODE(logical_column_mapping_);
  }
  if (OB_SUCC(ret)) {
    bool has_phys_param = false;
    OB_UNIS_DECODE(has_phys_param);
    if (OB_SUCC(ret) && has_phys_param) {
      // column_ids_ is initialized with the ctdef allocator during construction.
      common::ObIAllocator *alloc = column_ids_.get_allocator();
      if (OB_ISNULL(alloc)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("allocator of column ids is null", K(ret));
      } else {
        void *param_buf = alloc->alloc(sizeof(share::schema::ObTableDMLParam));
        if (OB_ISNULL(param_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate physical table dml param failed", K(ret));
        } else {
          share::schema::ObTableDMLParam *param =
              new(param_buf) share::schema::ObTableDMLParam(*alloc);
          OB_UNIS_DECODE(*param);
          if (OB_SUCC(ret)) {
            lt_physical_table_param_ = param;
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_DECODE, lt_ns_id_expr_, lt_id_expr_);
  }
  // Old CtDefs leave this flag clear. Do not infer the base-field boundary
  // from data_len because insert/update/delete CtDefs append derived fields.
  if (OB_SUCC(ret) && has_ivf_dml_ctdef_) {
    OB_UNIS_DECODE(ivf_dml_ctdef_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASDMLBaseCtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    table_id_,
    index_tid_,
    rowkey_cnt_,
    spk_cnt_,
    schema_version_,
    column_ids_,
    column_types_,
    column_accuracys_,
    old_row_projector_,
    new_row_projector_,
    tz_info_,
    table_param_,
    encrypt_meta_,
    flags_,
    logical_new_row_exprs_,
    logical_old_row_exprs_,
    lt_namespace_id_,
    lt_logical_table_id_,
    lt_physical_data_table_id_,
    lt_physical_namespace_col_id_,
    lt_physical_key_col_id_,
    lt_physical_value_col_id_,
    lt_tablet_mappings_);
  {
    int64_t aux_type_val = static_cast<int64_t>(logical_table_aux_type_);
    OB_UNIS_ADD_LEN(aux_type_val);
  }
  {
    OB_UNIS_ADD_LEN(logical_column_mapping_);
  }
  {
    bool has_phys_param = (nullptr != lt_physical_table_param_);
    OB_UNIS_ADD_LEN(has_phys_param);
    if (has_phys_param) {
      OB_UNIS_ADD_LEN(*lt_physical_table_param_);
    }
  }

  LST_DO_CODE(OB_UNIS_ADD_LEN, lt_ns_id_expr_, lt_id_expr_);
  if (has_ivf_dml_ctdef_) {
    OB_UNIS_ADD_LEN(ivf_dml_ctdef_);
  }
  return len;
}

OB_DEF_SERIALIZE(ObDASDMLBaseRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
    timeout_ts_,
    sql_mode_,
    prelock_,
    tenant_schema_version_,
    is_for_foreign_key_check_);
  if (OB_SUCC(ret)) {
    LST_DO_CODE(OB_UNIS_ENCODE, lt_rt_.namespace_id_, lt_rt_.logical_table_id_);
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASDMLBaseRtDef)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
    timeout_ts_,
    sql_mode_,
    prelock_,
    tenant_schema_version_,
    is_for_foreign_key_check_);
  // Tolerate senders that do not carry logical table ns/lt: OB_UNIS_DESERIALIZE clamps
  // data_len to the sender payload length, so an unconditional decode would report
  // OB_DESERIALIZE_ERROR when the peer runs an older version.
  if (OB_SUCC(ret) && pos < data_len) {
    LST_DO_CODE(OB_UNIS_DECODE, lt_rt_.namespace_id_, lt_rt_.logical_table_id_);
  }
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  if (OB_SUCC(ret) && lt_rt_.has_resolved_ns_lt()) {
    lt_rt_.enabled_ = true;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASDMLBaseRtDef)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    timeout_ts_,
    sql_mode_,
    prelock_,
    tenant_schema_version_,
    is_for_foreign_key_check_);
  LST_DO_CODE(OB_UNIS_ADD_LEN, lt_rt_.namespace_id_, lt_rt_.logical_table_id_);
  return len;
}

// add by dkz
OB_SERIALIZE_MEMBER((ObDASInsRtDef, ObDASDMLBaseRtDef),
                    need_fetch_conflict_,
                    direct_insert_task_id_,
                    use_put_,
                    ddl_task_id_);


OB_SERIALIZE_MEMBER((ObDASLockRtDef, ObDASDMLBaseRtDef),
                    for_upd_wait_time_);

OB_SERIALIZE_MEMBER((ObDASInsCtDef, ObDASDMLBaseCtDef),
                    table_rowkey_cids_,
                    table_rowkey_types_);

OB_SERIALIZE_MEMBER((ObDASUpdCtDef, ObDASDMLBaseCtDef),
                    updated_column_ids_);

OB_SERIALIZE_MEMBER((ObDASLockCtDef, ObDASDMLBaseCtDef),
                    lock_flag_);

ObDASDMLIterator::~ObDASDMLIterator()
{
  if (nullptr != domain_iter_) {
    domain_iter_->~ObDomainDMLIterator();
    domain_iter_ = nullptr;
  }
}

int ObDASDMLIterator::encode_to_physical_row_(const blocksstable::ObDatumRow &logical_row,
                                              blocksstable::ObDatumRow &out_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_logical_table_iter_())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("encode called when not in logical table mode", K(ret));
  } else if (OB_UNLIKELY(out_row.get_column_count()
      < (common::OB_INVALID_ID != das_ctdef_->lt_physical_namespace_col_id_ ? 3 : 2))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("physical row does not have enough cells",
             K(ret), K(out_row.get_column_count()), K(*das_ctdef_), K(*lt_rt_));
  } else {
    ObString out_key;
    ObString out_value;
    if (OB_FAIL(ObLogicalTableEncoder::encode_physical_row(
                                             lt_rt_->namespace_id_,
                                             lt_rt_->logical_table_id_,
                                             das_ctdef_->lt_physical_key_col_id_,
                                             das_ctdef_->lt_physical_value_col_id_,
                                             das_ctdef_->logical_table_aux_type_,
                                             das_ctdef_->logical_column_mapping_,
                                             logical_row,
                                             physical_row_allocator_,
                                             out_key,
                                             out_value))) {
      LOG_WARN("encode physical row failed", K(ret));
    } else if (common::OB_INVALID_ID != das_ctdef_->lt_physical_namespace_col_id_) {
      out_row.storage_datums_[0].set_uint(lt_rt_->namespace_id_);
      out_row.storage_datums_[1].set_string(out_key);
      ObString lob_value;
      if (OB_FAIL(storage::ObLobManager::fill_lob_header(
              physical_row_allocator_, out_value, lob_value))) {
        LOG_WARN("failed to fill lob header for kv_value", K(ret), K(out_value));
      } else {
        out_row.storage_datums_[2].set_string(lob_value);
        out_row.count_ = 3;
      }
    } else {
      out_row.storage_datums_[0].set_string(out_key);
      ObString lob_value;
      if (OB_FAIL(storage::ObLobManager::fill_lob_header(
              physical_row_allocator_, out_value, lob_value))) {
        LOG_WARN("failed to fill lob header for kv_value", K(ret), K(out_value));
      } else {
        out_row.storage_datums_[1].set_string(lob_value);
        out_row.count_ = 2;
      }
    }
  }
  return ret;
}

int ObDASDMLIterator::get_next_domain_index_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(domain_iter_)) {
    ObDomainDMLParam param(allocator_, row_projector_, write_iter_, das_ctdef_, main_ctdef_);
    if (das_ctdef_->table_param_.get_data_table().is_fts_index() && !das_ctdef_->old_row_projector_.empty()) {
      param.mode_ = !main_ctdef_->is_logical_table()
                        && das_ctdef_->table_param_.get_data_table().can_read_index()
                      ? ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN
                      : ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT;
      param.ft_doc_word_info_ = ft_doc_word_info_;
    }
    if (OB_FAIL(ObDomainDMLIterator::create_domain_dml_iterator(param, domain_iter_))) {
      LOG_WARN("fail to create domain index dml iterator", K(ret));
    } else {
    }
  }
  if (FAILEDx(domain_iter_->get_next_domain_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next domain_row", K(ret));
    }
  }
  return ret;
}

int ObDASDMLIterator::get_next_domain_index_rows(ObDatumRow *&rows, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(domain_iter_)) {
    ObDomainDMLParam param(allocator_, row_projector_, write_iter_, das_ctdef_, main_ctdef_);
    if (das_ctdef_->table_param_.get_data_table().is_fts_index() && !das_ctdef_->old_row_projector_.empty()) {
      param.mode_ = !main_ctdef_->is_logical_table()
                        && das_ctdef_->table_param_.get_data_table().can_read_index()
                      ? ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN
                      : ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT;
      param.ft_doc_word_info_ = ft_doc_word_info_;
    }
    if (OB_FAIL(ObDomainDMLIterator::create_domain_dml_iterator(param, domain_iter_))) {
      LOG_WARN("fail to create domain index dml iterator", K(ret));
    }
  }
  if (FAILEDx(domain_iter_->get_next_domain_rows(rows, row_count))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next domain rows", K(ret), KPC(domain_iter_));
    }
  }
  return ret;
}

int ObDASDMLIterator::get_next_row(blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(write_buffer_.ensure_ivf_materialized())) {
    LOG_WARN("failed to materialize IVF rows", K(ret));
  } else if (OB_ISNULL(cur_datum_row_)) {
    if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(allocator_, row_projector_->count(), cur_datum_row_))) {
      LOG_WARN("create current datum row failed", K(ret), K(row_projector_));
    } else if (OB_FAIL(write_buffer_.begin(write_iter_))) {
      LOG_WARN("begin write iterator failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (das_ctdef_->table_param_.get_data_table().is_domain_index()
        && !das_ctdef_->is_access_vidx_as_master_table_) {
      if (OB_FAIL(get_next_domain_index_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next domain index row", K(ret), K(das_ctdef_->table_param_.get_data_table()));
        }
      }
    } else {
      const ObChunkDatumStore::StoredRow *sr = nullptr;
      if (OB_FAIL(write_iter_.get_next_row(sr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from result iterator failed", K(ret));
        }
      } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                        *sr,
                                                        *row_projector_,
                                                        allocator_,
                                                        *cur_datum_row_))) {
        LOG_WARN("project storage row failed", K(ret));
      } else {
        datum_row = cur_datum_row_;
        LOG_TRACE("get next row from dml das iterator", KPC(sr), KPC(datum_row), K(das_ctdef_));
      }
    }
    // M6: logical-table encode at iterator output. Both domain and non-domain paths
    // funnel through here so the physical KV row is what storage receives.
    if (OB_SUCC(ret) && is_logical_table_iter_() && nullptr != datum_row) {
      physical_row_allocator_.reset();
      physical_row_ = nullptr;
      const int64_t physical_col_count =
          (common::OB_INVALID_ID != das_ctdef_->lt_physical_namespace_col_id_ ? 3 : 2);
      if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(
              physical_row_allocator_, physical_col_count, physical_row_))) {
        LOG_WARN("create physical row failed", K(ret));
        // fail loud: never hand back the un-encoded logical row on a write path
        datum_row = nullptr;
      } else if (OB_FAIL(encode_to_physical_row_(*datum_row, *physical_row_))) {
        LOG_WARN("encode datum row to physical row failed", K(ret), KPC(datum_row));
        datum_row = nullptr;
      } else {
        datum_row = physical_row_;
      }
    }
  }
  return ret;
}

int ObDASDMLIterator::get_next_rows(blocksstable::ObDatumRow *&rows, int64_t &row_count)
{
  row_count = 0;
  int ret = OB_SUCCESS;
  bool is_domain_index = false;
  if (OB_FAIL(write_buffer_.ensure_ivf_materialized())) {
    LOG_WARN("failed to materialize IVF rows", K(ret));
  } else if (FALSE_IT(is_domain_index = das_ctdef_->table_param_.get_data_table().is_domain_index())) {
  } else if (1 == batch_size_ && !is_domain_index) {
    if (OB_FAIL(get_next_row(rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Failed to get next row", K(ret), K_(batch_size), K(is_domain_index));
      }
    } else {
      row_count = 1;
    }
  } else {
    if (OB_ISNULL(cur_datum_rows_)) {
      if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_rows(allocator_, batch_size_, row_projector_->count(), cur_datum_rows_))) {
        LOG_WARN("Failed to create rows", K(ret), K_(row_projector));
      } else if (OB_FAIL(write_buffer_.begin(write_iter_))) {
        LOG_WARN("Failed to begin write iterator", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (is_domain_index && !das_ctdef_->is_access_vidx_as_master_table_) {
      if (OB_FAIL(get_next_domain_index_rows(rows, row_count))) {
        LOG_WARN("fail to get next domain index rows", K(ret));
      }
    } else {
      while (OB_SUCC(ret) && row_count < batch_size_) {
        const ObChunkDatumStore::StoredRow *sr = nullptr;
        if (OB_FAIL(write_iter_.get_next_row(sr))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("Failed to get next row from result iterator", K(ret));
          }
        } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                           *sr,
                                                           *row_projector_,
                                                           allocator_,
                                                           cur_datum_rows_[row_count]))) {
          LOG_WARN("Failed to project storage row", K(ret));
        } else {
          ++row_count;
          LOG_TRACE("Get next rows from dml das iterator", KPC(sr), K(cur_datum_rows_[row_count - 1]), K_(das_ctdef));
        }
      }
      if (OB_SUCC(ret) || OB_LIKELY(OB_ITER_END == ret)) {
        if (0 == row_count) {
          ret = OB_ITER_END;
        } else {
          rows = cur_datum_rows_;
          ret = OB_SUCCESS;
        }
      }
    }
    // M6: logical-table encode each logical row to physical KV row.
    if (OB_SUCC(ret) && row_count > 0 && is_logical_table_iter_() && nullptr != rows) {
      physical_row_allocator_.reset();
      physical_rows_ = nullptr;
      const int64_t physical_col_count =
          (common::OB_INVALID_ID != das_ctdef_->lt_physical_namespace_col_id_ ? 3 : 2);
      if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_rows(
              physical_row_allocator_, row_count, physical_col_count, physical_rows_))) {
        LOG_WARN("create physical rows failed", K(ret), K(row_count));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; ++i) {
        if (OB_FAIL(encode_to_physical_row_(rows[i], physical_rows_[i]))) {
          LOG_WARN("encode datum row to physical row failed", K(ret), K(i), K(rows[i]));
        } else {
          LOG_TRACE("encode datum row to physical row", K(das_ctdef_->table_id_),
                    K(i), K(rows[i]), K(physical_rows_[i]));
        }
      }
      if (OB_SUCC(ret)) {
        rows = physical_rows_;
      }
    }
  }
  return ret;
}

int ObDASDMLIterator::rewind(const ObDASDMLBaseCtDef *das_ctdef, const ObFTDocWordInfo *ft_doc_word_info)
{
  int ret = common::OB_SUCCESS;
  cur_datum_row_ = nullptr;
  ft_doc_word_info_ = ft_doc_word_info;
  if (nullptr != cur_datum_rows_) {
    allocator_.free(cur_datum_rows_);
    cur_datum_rows_ = nullptr;
  }
  // M6: drop encoded physical row caches; mapping/aux_type may change after rewind
  physical_row_allocator_.reset();
  physical_row_ = nullptr;
  physical_rows_ = nullptr;
  set_ctdef(das_ctdef);
  if (OB_NOT_NULL(domain_iter_)) {
    if (OB_FAIL(domain_iter_->rewind())) {
      LOG_WARN("fail to rewind for domain iterator", K(ret));
    }
  }
  return ret;
}

void ObDASDMLIterator::set_ctdef(const ObDASDMLBaseCtDef *das_ctdef)
{
  das_ctdef_ = das_ctdef;
  row_projector_ = !das_ctdef_->old_row_projector_.empty() ?
                   &das_ctdef_->old_row_projector_ :
                   &das_ctdef_->new_row_projector_;
  if (OB_NOT_NULL(domain_iter_)) {
    if (!das_ctdef->table_param_.get_data_table().is_domain_index()) {
      // This table isn't domain index, nothing to do.
    } else if (domain_iter_->is_same_domain_type(das_ctdef)) {
      // The das_ctdef and das_ctdef_ are either full-text search or multi-value index.
      ObDomainDMLMode mode = ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT;
      if (das_ctdef->table_param_.get_data_table().is_fts_index()) {
        static_cast<ObFTDMLIterator *>(domain_iter_)->set_ft_doc_word_info(ft_doc_word_info_);
        if (!das_ctdef_->old_row_projector_.empty()
            && !main_ctdef_->is_logical_table()
            && das_ctdef_->table_param_.get_data_table().can_read_index()) {
          mode = ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN;
        }
      }
      domain_iter_->set_ctdef(das_ctdef, row_projector_, mode);
    } else {
      // need to reset domain iter
      domain_iter_->~ObDomainDMLIterator();
      domain_iter_ = nullptr;
    }
  }
}

int ObDASMLogDMLIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_iter_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml iterator cannot be null", KR(ret), K_(row_iter));
  } else if (OB_FAIL(row_iter_->get_next_row(row))) {
    LOG_WARN("failed to get next row from dml iterator", KR(ret));
  } else if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row cannot be null", KR(ret), KP(row));
  } else {
    if (OB_FAIL(ObDASUtils::generate_mlog_row(ls_id_,
                                              tablet_id_,
                                              dml_param_,
                                              *row,
                                              op_type_,
                                              is_old_row_))) {
      LOG_WARN("failed to generate mlog rows", KR(ret));
    } else if (DAS_OP_TABLE_UPDATE == op_type_) {
      is_old_row_ = !is_old_row_;
    }
  }
  return ret;
}

int ObDASWriteBuffer::DmlShadowRow::init(ObIAllocator &allocator,
                                         int64_t datum_cnt,
                                         bool strip_lob_locator)
{
  int ret = DatumShadowStoredRow::init(allocator, datum_cnt);
  strip_lob_locator_ = strip_lob_locator;
  return ret;
}

int ObDASWriteBuffer::DmlShadowRow::init(ObIAllocator &allocator,
                                         const ObIArray<ObObjMeta> &col_types,
                                         bool strip_lob_locator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(init(allocator, col_types.count(), strip_lob_locator))) {
    LOG_WARN("init datum buffer failed", K(ret), K(col_types));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_types.count(); ++i) {
    ObObjDatumMapType obj_datum_map = ObDatum::get_obj_datum_map_type(
        col_types.at(i).get_type());
    if (OB_LIKELY(OBJ_DATUM_NULL != obj_datum_map)) {
      total_reserved_size_ += ObDatum::get_reserved_size(obj_datum_map);
    }
  }
  if (OB_SUCC(ret) && total_reserved_size_ > 0) {
    if (OB_ISNULL(reserved_buffer_ = static_cast<char *>(allocator.alloc(total_reserved_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", K(total_reserved_size_), K(ret));
    }
  }
  column_types_ = &col_types;
  reserve_datum_buf_ = true;
  if (OB_SUCC(ret)) {
    reset_datum_ptr();
  }
  return ret;
}

int ObDASWriteBuffer::DmlShadowRow::shadow_copy(const ObIArray<ObExpr*> &exprs, ObEvalCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_row_) || OB_UNLIKELY(store_row_->cnt_ != exprs.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL datums or count mismatch", K(ret), KPC(store_row_), K(exprs.count()));
  } else {
    ObDatum *datum = nullptr;
    ObDatum *cells = store_row_->cells();
    for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
      if (OB_FAIL(exprs.at(i)->eval(ctx, datum))) {
        LOG_WARN("failed to evaluate expr datum", K(ret), K(i));
      } else if (lib::is_oracle_mode() && !datum->is_null()
                 && exprs.at(i)->obj_meta_.is_lob_locator()
                 && strip_lob_locator_) {
        ObString payload;
        if (column_types_ != nullptr && !column_types_->at(i).is_lob()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column type", K(ret), K(column_types_->at(i)));
        } else if (OB_FAIL(datum->get_lob_locator().get_payload(payload))) {
          LOG_WARN("get lob payload failed", K(ret));
        } else {
          cells[i].set_string(payload);
        }
      } else {
        cells[i] = *datum;
      }
      if (OB_SUCC(ret)) {
        store_row_->row_size_ += cells[i].len_;
      }
    }
    saved_ = OB_SUCC(ret);
  }
  return ret;
}

int ObDASWriteBuffer::DmlShadowRow::shadow_copy(const blocksstable::ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(store_row_) || OB_UNLIKELY(store_row_->cnt_ != row.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL datums or count mismatch", K(ret), KPC(store_row_), K(row));
  } else {
    ObDatum *cells = store_row_->cells();
    ObObjDatumMapType map_type = OBJ_DATUM_MAPPING_MAX;
    for (int64_t i = 0; OB_SUCC(ret) && i < row.get_column_count(); ++i) {
      if (lib::is_oracle_mode() && column_types_ != nullptr && column_types_->at(i).is_lob_locator() && strip_lob_locator_) {
        ObString payload;
        if (!column_types_->at(i).is_lob()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column type", K(ret), K(column_types_->at(i)));
        } else if (OB_FAIL(row.storage_datums_[i].get_lob_locator().get_payload(payload))) {
          LOG_WARN("get lob payload failed", K(ret));
        } else {
          cells[i].set_string(payload);
        }
      } else if (FALSE_IT(map_type = ObDatum::get_obj_datum_map_type(column_types_->at(i).get_type()))) {
      } else if (OB_FAIL(cells[i].from_storage_datum(row.storage_datums_[i], map_type))) {
        LOG_WARN("shadow copy storage datum failed", K(ret), K(i), K(row));
      }
      if (OB_SUCC(ret)) {
        //add the data length of datum
        store_row_->row_size_ += cells[i].len_;
      }
    }
    saved_ = OB_SUCC(ret);
  }
  return ret;
}

ObDASWriteBuffer::ObDASWriteBuffer()
{
  memset(this, 0, sizeof(ObDASWriteBuffer));
}

ObDASWriteBuffer::~ObDASWriteBuffer()
{
  if (datum_store_ != nullptr) {
    datum_store_->~ObChunkDatumStore();
    datum_store_ = nullptr;
  }
}

int ObDASWriteBuffer::init(ObIAllocator &das_alloc,
                           uint32_t row_extend_size,
                           uint64_t tenant_id,
                           const char *label,
                           int64_t mem_ctx_id)
{
  int ret = OB_SUCCESS;
  mem_attr_.tenant_id_ = tenant_id;
  mem_attr_.label_ = label;
  mem_attr_.ctx_id_ = mem_ctx_id;
  das_alloc_ = &das_alloc;
  row_extend_size_ = row_extend_size;
  return ret;
}

ObDASWriteBuffer::IvfMaterializationGuard::IvfMaterializationGuard(
    ObDASWriteBuffer &buffer, void *ctx, Callback callback)
    : buffer_(buffer), ctx_(ctx), callback_(callback), attempted_(false), registered_(false),
      result_(OB_SUCCESS), placeholders_()
{
  if (nullptr != buffer_.ivf_materializer_) {
    result_ = OB_STATE_NOT_MATCH;
    LOG_ERROR_RET(result_, "overlapping IVF materialization guards", K(result_));
  } else {
    buffer_.ivf_materializer_ = this;
    registered_ = true;
  }
}

ObDASWriteBuffer::IvfMaterializationGuard::~IvfMaterializationGuard()
{
  if (registered_) {
    // The owning DAS op retains its non-spilling buffer until write_tablet /
    // insert_row_with_fetch returns. No row reset or buffer reuse occurs here.
    for (int64_t i = 0; i < placeholders_.count(); ++i) {
      int ret = ObExprVecIVFCenterID::restore_deferred_placeholder(*placeholders_.at(i));
      if (OB_SUCCESS != ret) {
        LOG_ERROR("failed to restore IVF placeholder after DAS write attempt", K(ret), K(i));
      }
    }
    buffer_.ivf_materializer_ = nullptr;
  }
}

int ObDASWriteBuffer::IvfMaterializationGuard::remember_placeholder(ObDatum &datum)
{
  return registered_ ? placeholders_.push_back(&datum) : OB_STATE_NOT_MATCH;
}

int ObDASWriteBuffer::IvfMaterializationGuard::ensure()
{
  if (registered_ && !attempted_) {
    attempted_ = true;
    // Reentrant consumption must fail rather than expose half-prepared rows.
    result_ = OB_ERR_UNEXPECTED;
    if (nullptr != callback_) {
      result_ = callback_(ctx_, buffer_);
    }
  }
  return result_;
}

int ObDASWriteBuffer::materialize_ivf_rows(
    const ObDASDMLBaseCtDef &ivf_ctdef,
    const ObTabletID &centroid_tablet_id,
    const ObLogicalTableRtDef &lt_rt,
    ObEvalCtx *eval_ctx)
{
  int ret = OB_SUCCESS;
  Iterator iter;
  const ObChunkDatumStore::StoredRow *const_row = nullptr;
  const ObIVFDMLCtDef &meta = ivf_ctdef.ivf_dml_ctdef_;
  ObSEArray<ObChunkDatumStore::StoredRow *, 16> rows_to_materialize;
  ObSEArray<ObCenterId, 16> assigned_center_ids;
  // A logical-table plan can be reused by different namespaces and logical
  // tables, so the CtDef identity may remain at its compile-time placeholder.
  // The current DAS task's validated RtDef is the authoritative row identity.
  if (OB_UNLIKELY(!ivf_ctdef.is_logical_table()
      || !meta.is_valid()
      || !centroid_tablet_id.is_valid()
      || !lt_rt.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid logical ivf dml ctdef", K(ret), K(ivf_ctdef), K(meta));
  } else if (0 == get_row_cnt()) {
    // An empty DAS task has no vector to decode or CID to assign. In
    // particular, INSERT ... SELECT may legitimately produce no StoredRow.
  } else if (OB_ISNULL(eval_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("eval context is required for leader ivf assignment", K(ret));
  } else if (OB_FAIL(begin(iter))) {
    LOG_WARN("begin das write buffer for ivf materialization failed", K(ret));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(iter.get_next_row(const_row))) {
      if (OB_ISNULL(const_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null stored row during ivf materialization", K(ret));
      } else if (OB_UNLIKELY(!meta.is_valid_for_row_count(const_row->cnt_))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ivf CtDef does not match DAS StoredRow",
                 K(ret), K(meta), K(const_row->cnt_));
      } else {
        ObChunkDatumStore::StoredRow *row =
            const_cast<ObChunkDatumStore::StoredRow *>(const_row);
        ObDatum &cid_datum = row->cells()[meta.cid_store_idx_];
        const ObDatum &vector_datum = row->cells()[meta.vector_store_idx_];
        ObCenterId center_id;
        if (OB_FAIL(validate_ivf_cid_transport(cid_datum, true /*allow_placeholder*/))) {
          LOG_WARN("StoredRow IVF CID slot violates the 16B transport contract",
                   K(ret), K(meta), K(cid_datum));
        } else if (!ObExprVecIVFCenterID::is_deferred_placeholder(
                       cid_datum.get_string())) {
          // A materialized CID can come from legacy/inner-session evaluation or
          // a repeated pass over the same write buffer. Preserve it to keep
          // assignment and centroid counters idempotent.
          LOG_DEBUG("skip leader IVF assignment for an already materialized CID",
                    K(meta), K(cid_datum));
        } else {
          if (vector_datum.is_null()) {
            // Match legacy contain_null semantics: skip distance evaluation for
            // a NULL vector but persist bootstrap center 0 for this tablet.
            center_id = ObCenterId(centroid_tablet_id.id(), 0);
            LOG_DEBUG("leader IVF assignment uses center 0 for null vector",
                      K(center_id), K(meta));
          } else if (OB_FAIL(ObIVFCidUtil::assign_spfresh_center_id(
              vector_datum,
              meta.vector_subschema_id_,
              meta.centroid_table_id_,
              centroid_tablet_id,
              static_cast<int64_t>(lt_rt.namespace_id_),
              lt_rt.logical_table_id_,
              static_cast<share::ObVectorIndexDistAlgorithm>(meta.distance_algorithm_),
              *eval_ctx,
              center_id))) {
            LOG_WARN("leader direct ivf assignment failed", K(ret), K(meta),
                     K(centroid_tablet_id), K(lt_rt));
          }
          if (OB_SUCC(ret) && OB_FAIL(rows_to_materialize.push_back(row))) {
            LOG_WARN("record logical IVF row to materialize failed", K(ret));
          } else if (OB_SUCC(ret) && OB_FAIL(assigned_center_ids.push_back(center_id))) {
            rows_to_materialize.pop_back();
            LOG_WARN("record leader-assigned center ID failed", K(ret), K(center_id));
          }
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    // Do not mutate the write buffer until every row has been assigned. A DAS
    // retry must never reuse a mixture of placeholders and materialized CIDs.
    for (int64_t i = 0; OB_SUCC(ret) && i < rows_to_materialize.count(); ++i) {
      ObDatum &cid_datum = rows_to_materialize.at(i)->cells()[meta.cid_store_idx_];
      ObString cid_string(share::OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH, 0,
                          const_cast<char *>(cid_datum.ptr_));
      if (nullptr != ivf_materializer_
          && OB_FAIL(ivf_materializer_->remember_placeholder(cid_datum))) {
        LOG_WARN("record IVF placeholder for attempt cleanup failed", K(ret), K(i));
      } else if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(
              assigned_center_ids.at(i), cid_string))) {
        LOG_WARN("encode leader-assigned center id failed",
                 K(ret), K(assigned_center_ids.at(i)), K(meta));
      } else if (OB_UNLIKELY(share::OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH
          != cid_string.length())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("leader direct IVF assignment did not produce 16B",
                 K(ret), K(meta), K(cid_string.length()));
      } else {
        cid_datum.len_ = share::OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH;
        if (OB_FAIL(validate_ivf_cid_transport(
                cid_datum, false /*allow_placeholder*/))) {
          LOG_WARN("leader-assigned CID violates the storage contract",
                   K(ret), K(meta), K(cid_datum));
        } else {
          ObCenterId decoded_center_id;
          if (OB_FAIL(ObVectorClusterHelper::get_center_id_from_string(
                  decoded_center_id, cid_datum.get_string()))) {
            LOG_WARN("decode leader-assigned CID failed", K(ret), K(meta));
          } else if (OB_UNLIKELY(decoded_center_id.tablet_id_
                        != assigned_center_ids.at(i).tablet_id_
                     || decoded_center_id.center_id_
                        != assigned_center_ids.at(i).center_id_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("leader-assigned CID changed during encoding",
                     K(ret), K(decoded_center_id),
                     "expected_center_id", assigned_center_ids.at(i), K(meta));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
      // Encoding is expected to be infallible for a validated 16-byte slot,
      // but restore every touched row defensively so the task remains retryable.
      for (int64_t i = 0; i < rows_to_materialize.count(); ++i) {
        ObDatum &cid_datum = rows_to_materialize.at(i)->cells()[meta.cid_store_idx_];
        cid_datum.len_ = share::OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH;
        int restore_ret = ObExprVecIVFCenterID::restore_deferred_placeholder(cid_datum);
        if (OB_SUCCESS != restore_ret) {
          LOG_ERROR("restore deferred IVF placeholder failed",
                    K(restore_ret), K(i), K(meta), K(cid_datum));
        }
      }
    }
  }
  return ret;
}

int ObDASWriteBuffer::validate_ivf_cid_transport(
    const ObDatum &cid_datum,
    const bool allow_placeholder)
{
  int ret = OB_SUCCESS;
  if (cid_datum.is_null()
      || OB_ISNULL(cid_datum.ptr_)
      || share::OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH
          != cid_datum.get_string().length()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (!allow_placeholder
      && ObExprVecIVFCenterID::is_deferred_placeholder(cid_datum.get_string())) {
    ret = OB_ERR_UNEXPECTED;
  }
  return ret;
}

int ObDASWriteBuffer::init_dml_shadow_row(int64_t column_cnt, bool strip_lob_locator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_shadow_row_)) {
    void *buff = das_alloc_->alloc(sizeof(DmlShadowRow));
    if (OB_ISNULL(buff)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate dml shadow row failed", K(ret));
    } else {
      dml_shadow_row_ = new(buff) DmlShadowRow();
      if (OB_FAIL(dml_shadow_row_->init(*das_alloc_, column_cnt, strip_lob_locator))) {
        LOG_WARN("init dml shadow row failed", K(ret));
      }
    }
  }
  return ret;
}

int ObDASWriteBuffer::add_row(const common::ObIArray<ObExpr*> &exprs,
                              ObEvalCtx *ctx,
                              DmlRow *&stored_row,
                              bool strip_lob_locator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_shadow_row_)) {
    if (OB_FAIL(init_dml_shadow_row(exprs.count(), strip_lob_locator))) {
      LOG_WARN("init dml shadow row failed", K(ret));
    }
  } else {
    dml_shadow_row_->reuse();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml_shadow_row_->shadow_copy(exprs, *ctx))) {
      LOG_WARN("shadow copy dml row failed", K(ret));
    } else if (OB_FAIL(add_row(*dml_shadow_row_, &stored_row))) {
      LOG_WARN("try add row with shadow row failed", KK(ret));
    } else if (OB_ISNULL(stored_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stored row is null", K(ret));
    } else {
      LOG_DEBUG("succ add dml_row", KPC(stored_row));
    }
  }
  return ret;
}
// 以后das的add_row接口只许成功，不许失败
int ObDASWriteBuffer::add_row(const DmlShadowRow &sr, DmlRow **stored_row)
{
  int ret = OB_SUCCESS;
  bool row_added = false;
  const DmlRow *lsr = sr.get_store_row();
  int64_t simulate_len = - EVENT_CALL(EventTable::EN_DAS_WRITE_ROW_LIST_LEN);
  int64_t final_row_list_len = simulate_len > 0 ? simulate_len : DAS_WRITE_ROW_LIST_LEN;

  if (OB_LIKELY(buffer_list_.size_ < final_row_list_len)) {
    //link write row buffer to dlist
    //avoid to create ObChunkDatumStore,
    //because it is too heavy for small dml queries
    ret = add_row_to_dlist(sr, row_added, stored_row);
  } else {
    ret = add_row_to_store(sr, stored_row);
  }
  return ret;
}

int ObDASWriteBuffer::add_row_to_store(const ObChunkDatumStore::ShadowStoredRow &sr,
                                       ObChunkDatumStore::StoredRow **stored_sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_store_)) {
    if (OB_FAIL(create_datum_store())) {
      LOG_WARN("create datum store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum_store_->add_row(sr, stored_sr))) {
      LOG_WARN("try add row to store failed", K(ret), K_(buffer_list_.mem_used));
    }
  }
  return ret;
}

int ObDASWriteBuffer::try_add_row(const ObIArray<ObExpr*> &exprs,
                                  ObEvalCtx *ctx,
                                  const int64_t memory_limit,
                                  DmlRow *&stored_row,
                                  bool &row_added,
                                  bool strip_lob_locator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dml_shadow_row_)) {
    if (OB_FAIL(init_dml_shadow_row(exprs.count(), strip_lob_locator))) {
      LOG_WARN("init dml shadow row failed", K(ret));
    }
  } else {
    dml_shadow_row_->reuse();
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(dml_shadow_row_->shadow_copy(exprs, *ctx))) {
      LOG_WARN("shadow copy dml row failed", K(ret));
    } else if (OB_FAIL(try_add_row(*dml_shadow_row_, memory_limit, row_added, &stored_row))) {
      LOG_WARN("try add row with shadow row failed", KK(ret));
    } else if (!row_added) {
      // buff已经满了，add row 失败
    } else if (OB_ISNULL(stored_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("stored row is null", K(ret));
    } else {
      LOG_DEBUG("succ add dml_row", KPC(stored_row));
    }

  }
  return ret;
}

int ObDASWriteBuffer::try_add_row(const DmlShadowRow &sr,
                                  const int64_t memory_limit,
                                  bool &row_added,
                                  DmlRow **stored_row)
{
  int ret = OB_SUCCESS;
  const DmlRow *lsr = sr.get_store_row();
  int64_t row_size = lsr->row_size_;
  int64_t simulate_len = - EVENT_CALL(EventTable::EN_DAS_WRITE_ROW_LIST_LEN);
  int64_t final_row_list_len = simulate_len > 0 ? simulate_len : DAS_WRITE_ROW_LIST_LEN;
  int64_t final_mem_limit = memory_limit;
  int64_t simulate_mem_limit = - EVENT_CALL(EventTable::EN_DAS_SIMULATE_DAS_TASK_SIZE);
  if (simulate_mem_limit != 0 && final_mem_limit > simulate_mem_limit) {
    LOG_TRACE("simulate_mem_limit", K(simulate_mem_limit));
    final_mem_limit = simulate_mem_limit;
  }
  if (OB_UNLIKELY(row_size + get_mem_used() > final_mem_limit && get_mem_used() > 0)) {
    //if the size of the first row exceeds memory_limit,
    //writing is also allowed,
    //ensuring that there is at least one row of data
    row_added = false;
  } else if (OB_LIKELY(buffer_list_.size_ < final_row_list_len)) {
    //link write row buffer to dlist
    //avoid to create ObChunkDatumStore,
    //because it is too heavy for small dml queries
    ret = add_row_to_dlist(sr, row_added, stored_row);
  } else {
    ret = add_row_to_store(sr, final_mem_limit, row_added, stored_row);
  }
  return ret;
}

OB_INLINE int ObDASWriteBuffer::create_link_buffer(int64_t row_size, DmlRow *&row_buffer)
{
  int ret = OB_SUCCESS;
  char *buf = NULL;
  DmlRow *dml_row = nullptr;
  int64_t NODE_HEADER_SIZE = sizeof(LinkNode);
  // Note that whether it is a deserialization process or a local add_row, the length of row_size here includes two parts
  // Store the length of ObDatum itself row_size + extended row_extend_size_length
  int64_t buffer_len = NODE_HEADER_SIZE + row_size;
  if (OB_ISNULL(buf = reinterpret_cast<char*>(das_alloc_->alloc(buffer_len, mem_attr_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc buf failed", K(ret), K(buffer_len));
  } else {
    reinterpret_cast<LinkNode*>(buf)->next_ = nullptr;
    dml_row = new(buf + NODE_HEADER_SIZE) DmlRow();
    if (buffer_list_.tailer_.next_ != nullptr) {
      char *row_ptr = reinterpret_cast<char *>(buffer_list_.tailer_.next_);
      LinkNode *last_node = reinterpret_cast<LinkNode*>(row_ptr - NODE_HEADER_SIZE);
      last_node->next_ = dml_row;
      buffer_list_.tailer_.next_ = dml_row;
    } else {
      buffer_list_.header_.next_ = dml_row;
      buffer_list_.tailer_.next_ = dml_row;
    }
    if (OB_SUCC(ret)) {
      buffer_list_.mem_used_ += buffer_len;
      buffer_list_.size_ += 1;
      row_buffer = dml_row;
    }
  }
  return ret;
}

OB_INLINE int ObDASWriteBuffer::add_row_to_dlist(const ObIArray<ObExpr*> &exprs,
                                                 ObEvalCtx *ctx,
                                                 int64_t row_size,
                                                 bool &row_added)
{
  int ret = OB_SUCCESS;
  DmlRow *dml_row = nullptr;
  if (OB_FAIL(create_link_buffer(row_size + row_extend_size_, dml_row))) {
    LOG_WARN("create link buffer failed", K(ret));
  } else if (OB_FAIL(DmlRow::build(dml_row, exprs, *ctx, (char *)dml_row, row_size))) {
    LOG_WARN("build stored row failed", K(ret));
  } else {
    row_added = true;
  }
  return ret;
}

OB_INLINE int ObDASWriteBuffer::add_row_to_dlist(const ObChunkDatumStore::ShadowStoredRow &sr,
                                                 bool &row_added,
                                                 DmlRow **stored_row)
{
  int ret = OB_SUCCESS;
  DmlRow *dml_row = nullptr;
  const DmlRow *lsr = sr.get_store_row();
  if (OB_FAIL(create_link_buffer(lsr->row_size_ + row_extend_size_, dml_row))) {
    LOG_WARN("create link buffer failed", K(ret));
  } else {
    char *buf = dml_row->payload_;
    int64_t buf_size = lsr->row_size_ + row_extend_size_ - ROW_HEAD_SIZE;
    if (OB_FAIL(dml_row->copy_shadow_datums(lsr->cells(), lsr->cnt_, buf, buf_size,
                                            lsr->row_size_ + row_extend_size_, row_extend_size_/*extra_size*/))) {
      LOG_WARN("failed to deep copy row", K(ret), K(lsr->row_size_), K(buf_size));
    } else {
      row_added = true;
      if (stored_row != nullptr) {
        *stored_row = dml_row;
      }
    }
  }
  return ret;
}

OB_NOINLINE int ObDASWriteBuffer::create_datum_store()
{
  int ret = OB_SUCCESS;
  void *buf = das_alloc_->alloc(sizeof(ObChunkDatumStore), mem_attr_);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate chunk datum store failed", K(ret));
  } else {
    datum_store_ = new(buf) ObChunkDatumStore(mem_attr_.label_);
    if (OB_FAIL(datum_store_->init(UINT64_MAX,
                                   mem_attr_.tenant_id_,
                                   mem_attr_.ctx_id_,
                                   mem_attr_.label_,
                                   false/*enable_dump*/,
                                   row_extend_size_))) {
      LOG_WARN("init datum store failed", K(ret), K(mem_attr_));
    }
  }
  return ret;
}

OB_NOINLINE int ObDASWriteBuffer::add_row_to_store(const ObIArray<ObExpr*> &exprs,
                                                   ObEvalCtx *ctx,
                                                   const int64_t memory_limit,
                                                   bool &row_added)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_store_)) {
    if (OB_FAIL(create_datum_store())) {
      LOG_WARN("create datum store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum_store_->try_add_row(exprs, ctx, memory_limit - buffer_list_.mem_used_, row_added))) {
      LOG_WARN("try add row to store failed", K(ret), K(memory_limit), K_(buffer_list_.mem_used));
    }
  }
  return ret;
}

OB_NOINLINE int ObDASWriteBuffer::add_row_to_store(const ObChunkDatumStore::ShadowStoredRow &sr,
                                                   const int64_t memory_limit,
                                                   bool &row_added,
                                                   ObChunkDatumStore::StoredRow **stored_sr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(datum_store_)) {
    if (OB_FAIL(create_datum_store())) {
      LOG_WARN("create datum store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(datum_store_->try_add_row(sr, memory_limit - buffer_list_.mem_used_, row_added, stored_sr))) {
      LOG_WARN("try add row to store failed", K(ret), K(memory_limit), K_(buffer_list_.mem_used));
    }
  }
  return ret;
}

int ObDASWriteBuffer::begin(Iterator &it)
{
  int ret = OB_SUCCESS;
  it.cur_row_ = buffer_list_.header_.next_;
  if (OB_UNLIKELY(datum_store_ != nullptr)) {
    void *buf = das_alloc_->alloc(sizeof(ObChunkDatumStore::Iterator));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc das allocator failed", K(ret));
    } else {
      it.datum_iter_ = new(buf) ObChunkDatumStore::Iterator();
      ret = datum_store_->begin(*it.datum_iter_);
    }
  }
  return ret;
}

int ObDASWriteBuffer::begin(NewRowIterator &it, const ObIArray<ObObjMeta> &col_types)
{
  int ret = OB_SUCCESS;
  it.cur_row_ = buffer_list_.header_.next_;
  if (OB_UNLIKELY(datum_store_ != nullptr)) {
    void *buf = das_alloc_->alloc(sizeof(ObChunkDatumStore::Iterator));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc das allocator failed", K(ret));
    } else {
      it.datum_iter_ = new(buf) ObChunkDatumStore::Iterator();
      ret = datum_store_->begin(*it.datum_iter_);
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(*das_alloc_, col_types.count(), it.cur_new_row_))) {
      LOG_WARN("create new row failed", K(ret));
    } else {
      it.col_types_ = &col_types;
    }
  }
  return ret;
}

int ObDASWriteBuffer::dump_data(const ObDASDMLBaseCtDef &das_base_ctdef) const
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow *new_row = NULL;
  blocksstable::ObDatumRow *old_row = NULL;
  const ObChunkDatumStore::StoredRow *store_row = NULL;
  int64_t rownum = 0;
  ObArenaAllocator tmp_alloc;
  ObDatum *trans_info_datum = nullptr;
  ObDASWriteBuffer::Iterator write_iter_tmp;
  ObString trans_info_str;

  if (OB_FAIL(const_cast<ObDASWriteBuffer*>(this)->begin(write_iter_tmp))) {
    LOG_WARN("get write iter failed", K(ret));
  }
  while (OB_SUCC(ret) && OB_SUCC(write_iter_tmp.get_next_row(store_row))) {
    trans_info_str.reset();
    if (OB_ISNULL(store_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (!das_base_ctdef.old_row_projector_.empty()) {
        // create old row
        if (OB_ISNULL(old_row)
            && OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(tmp_alloc, das_base_ctdef.old_row_projector_.count(), old_row))) {
          LOG_WARN("create old row buffer failed", K(ret), K(das_base_ctdef.old_row_projector_.count()));
        } else if (OB_FAIL(ObDASUtils::project_storage_row(das_base_ctdef,
                                                            *store_row,
                                                            das_base_ctdef.old_row_projector_,
                                                            tmp_alloc,
                                                            *old_row))) {
          LOG_WARN("project storage row failed", K(ret), K(das_base_ctdef.old_row_projector_));
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (!das_base_ctdef.new_row_projector_.empty()) {
        if (OB_ISNULL(new_row)
            && OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(tmp_alloc, das_base_ctdef.new_row_projector_.count(), new_row))) {
          LOG_WARN("create new row buffer failed", K(ret), K(das_base_ctdef.new_row_projector_.count()));
        } else if (OB_FAIL(ObDASUtils::project_storage_row(das_base_ctdef,
                                                           *store_row,
                                                           das_base_ctdef.new_row_projector_,
                                                           tmp_alloc,
                                                           *new_row))) {
          LOG_WARN("project storage row failed", K(ret), K(das_base_ctdef.new_row_projector_));
        }

      }
    }

    if (OB_SUCC(ret)) {
      if (row_extend_size_ > ObDASWriteBuffer::DAS_ROW_DEFAULT_EXTEND_SIZE) {
        // It means the payload contain trans_info string
        char *buf = static_cast<char *>(store_row->get_extra_payload());
        int32_t *str_len = reinterpret_cast<int32_t *>(store_row->get_extra_payload());
        int64_t pos = sizeof(int32_t);
        trans_info_str.assign(buf + pos, *str_len);
      }
    }

    if (OB_SUCC(ret)) {
      // do print
      LOG_INFO("DASWriteBuffer dump", K(rownum), "task_type", das_base_ctdef.op_type_,
          K(trans_info_str), KPC(new_row), KPC(old_row), KPC(store_row));
      rownum++;
    }
  } // end while

  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

ObDASWriteBuffer::DmlRow *ObDASWriteBuffer::get_next_dml_row(DmlRow *cur_row)
{
  int64_t NODE_HEADER_SIZE = sizeof(LinkNode);
  char *row_ptr = reinterpret_cast<char *>(cur_row);
  LinkNode *link_node = reinterpret_cast<LinkNode*>(row_ptr - NODE_HEADER_SIZE);
  return link_node->next_;
}

OB_DEF_SERIALIZE(ObDASWriteBuffer)
{
  int ret = OB_SUCCESS;
  bool has_more = (datum_store_ != nullptr);
  OZ(serialize_buffer_list(buf, buf_len, pos));
  OB_UNIS_ENCODE(has_more);
  if (has_more) {
    OB_UNIS_ENCODE(*datum_store_);
  }
  OB_UNIS_ENCODE(row_extend_size_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASWriteBuffer)
{
  int64_t len = 0;
  bool has_more = (datum_store_ != nullptr);
  len += get_buffer_list_serialize_size();
  OB_UNIS_ADD_LEN(has_more);
  if (has_more) {
    OB_UNIS_ADD_LEN(*datum_store_);
  }
  OB_UNIS_ADD_LEN(row_extend_size_);
  return len;
}

OB_DEF_DESERIALIZE(ObDASWriteBuffer)
{
  int ret = OB_SUCCESS;
  bool has_more = false;
  OZ(deserialize_buffer_list(buf, data_len, pos));
  OB_UNIS_DECODE(has_more);
  if (OB_SUCC(ret) && has_more) {
    void *buffer = das_alloc_->alloc(sizeof(ObChunkDatumStore), mem_attr_);
    if (OB_ISNULL(buffer)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate chunk row store failed", K(ret));
    } else {
      datum_store_ = new(buffer) ObChunkDatumStore(mem_attr_.label_);
      OB_UNIS_DECODE(*datum_store_);
    }
  }
  OB_UNIS_DECODE(row_extend_size_);
  return ret;
}

int ObDASWriteBuffer::serialize_buffer_list(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(buffer_list_.size_);
  if (OB_SUCC(ret)) {
    DmlRow *dml_row = buffer_list_.header_.next_;
    while (OB_SUCC(ret) && dml_row != nullptr) {
      if (OB_FAIL(serialization::encode(buf, buf_len, pos, dml_row->row_size_))) {
        LOG_WARN("serialize row size failed", K(ret), K(dml_row->row_size_));
      } else if (dml_row->row_size_ > buf_len - pos) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("serialize write buffer overflow", K(ret), K(buf_len), K(pos), K(dml_row->row_size_));
      } else {
        DmlRow *tmp_row = reinterpret_cast<DmlRow*>(buf + pos);
        MEMCPY(tmp_row, dml_row, dml_row->row_size_);
        DmlRow::unswizzling_datum(tmp_row->cells(), dml_row->cnt_, dml_row->payload_);
        pos += dml_row->row_size_;
        dml_row = ObDASWriteBuffer::get_next_dml_row(dml_row);
      }
    }
  }
  return ret;
}

int64_t ObDASWriteBuffer::get_buffer_list_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(buffer_list_.size_);
  DmlRow *dml_row = buffer_list_.header_.next_;
  while (dml_row != nullptr) {
    OB_UNIS_ADD_LEN(dml_row->row_size_);
    len += dml_row->row_size_;
    dml_row = ObDASWriteBuffer::get_next_dml_row(dml_row);
  }

  return len;
}

int ObDASWriteBuffer::deserialize_buffer_list(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t list_size = 0;
  OB_UNIS_DECODE(list_size);
  for (int64_t i = 0; OB_SUCC(ret) && i < list_size; ++i) {
    int64_t row_size = 0;
    DmlRow *row_buffer = nullptr;
    OB_UNIS_DECODE(row_size);
    OZ(create_link_buffer(row_size, row_buffer));
    if (OB_SUCC(ret)) {
      //deserialize row buffer data
      MEMCPY(row_buffer, buf + pos, row_size);
      row_buffer->swizzling(row_buffer->payload_);
      pos += row_buffer->row_size_;
    }
  }
  return ret;
}

int ObDASWriteBuffer::Iterator::get_next_row(const ObChunkDatumStore::StoredRow *&sr)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(cur_row_ != nullptr)) {
    //get next row from buffer list
    sr = cur_row_;
    cur_row_ = ObDASWriteBuffer::get_next_dml_row(cur_row_);
  } else if (OB_ISNULL(datum_iter_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(datum_iter_->get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next stored row failed", K(ret));
    }
  } else if (OB_ISNULL(sr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("returned stored row is NULL", K(ret));
  }
  return ret;
}

int ObDASWriteBuffer::Iterator::get_next_row_skip_const(ObEvalCtx &ctx, const ObIArray<ObExpr*> &exprs)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(cur_row_ != nullptr)) {
    //get next row from buffer list
    ret = cur_row_->to_expr(exprs, ctx);
    cur_row_ = ObDASWriteBuffer::get_next_dml_row(cur_row_);
  } else if (OB_ISNULL(datum_iter_)) {
    ret = OB_ITER_END;
  } else {
    ret = datum_iter_->get_next_row(ctx, exprs);
  }

  return ret;
}

int ObDASWriteBuffer::NewRowIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const DmlRow *sr = nullptr;
  row = nullptr;
  if (OB_LIKELY(cur_row_ != nullptr)) {
    //get next row from buffer list
    sr = cur_row_;
    cur_row_ = ObDASWriteBuffer::get_next_dml_row(cur_row_);
  } else if (OB_ISNULL(datum_iter_)) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(datum_iter_->get_next_row(sr))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from datum iter failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && sr != nullptr) {
    for (int64_t i = 0; OB_SUCC(ret) && i < sr->cnt_; ++i) {
      cur_new_row_->storage_datums_[i].shallow_copy_from_datum(sr->cells()[i]);
    }
    if (OB_SUCC(ret)) {
      row = cur_new_row_;
    }
  }
  return ret;
}

int ObDASWriteBuffer::get_stored_row_size(const ObIArray<ObExpr*> &exprs,
                                          ObEvalCtx &ctx,
                                          int64_t &size)
{
  return ObChunkDatumStore::Block::row_store_size(exprs, ctx, size);
}
}  // namespace sql
}  // namespace oceanbase
