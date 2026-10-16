/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/ob_das_dml_ivf_iter.h"
#include "sql/das/ob_das_utils.h"
#include "sql/engine/expr/ob_expr_vec_ivf_center_id.h"
#include "storage/blocksstable/ob_datum_row_utils.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

int ObIVFDMLIterator::get_cid_output_idx_(int64_t &cid_idx) const
{
  int ret = OB_SUCCESS;
  cid_idx = OB_INVALID_INDEX;
  int64_t cid_key_ordinal = OB_INVALID_INDEX;
  if (OB_ISNULL(das_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ivf dml ctdef is null", K(ret));
  } else if (OB_FAIL(share::get_ivf_cid_key_suffix_ordinal(
      das_ctdef_->logical_table_aux_type_, cid_key_ordinal))) {
    LOG_WARN("not an ivf user-row logical auxiliary table",
             K(ret), K(das_ctdef_->logical_table_aux_type_));
  }
  for (int64_t i = 0; OB_SUCC(ret) && cid_idx < 0
      && i < das_ctdef_->logical_column_mapping_.entries_.count(); ++i) {
    const ObLogicalColumnMappingEntry &entry =
        das_ctdef_->logical_column_mapping_.entries_.at(i);
    if (FROM_KEY_SUFFIX == entry.source_
        && cid_key_ordinal == entry.key_suffix_ordinal_) {
      // Mapping entries may omit partition columns and schema-rowkey extras,
      // so the entries_ array ordinal is not necessarily the projected-row
      // column ordinal.  source_idx_ records the auxiliary schema/output
      // column position consumed by collect_datums_from_datum_row_.
      cid_idx = entry.source_idx_;
    }
  }
  if (OB_SUCC(ret) && (cid_idx < 0
      || cid_idx >= das_ctdef_->column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cid output column is missing from logical mapping",
             K(ret), K(cid_idx), K(cid_key_ordinal),
             K(das_ctdef_->column_ids_.count()),
             K(das_ctdef_->logical_column_mapping_));
  }
  return ret;
}

int ObIVFDMLIterator::generate_domain_rows(
    const ObChunkDatumStore::StoredRow *store_row)
{
  int ret = OB_SUCCESS;
  blocksstable::ObDatumRow *row = nullptr;
  int64_t cid_idx = OB_INVALID_INDEX;
  const share::schema::ObTableSchemaParam &table_param =
      das_ctdef_->table_param_.get_data_table();
  const bool is_pq = share::schema::is_vec_ivfpq_code_index(
                         table_param.get_index_type())
      || share::schema::is_vec_ivfpq_rowkey_cid_index(
                         table_param.get_index_type());
  const bool is_sq8 = share::schema::is_vec_ivfsq8_index(
      table_param.get_index_type());
  if (OB_ISNULL(store_row) || OB_ISNULL(row_projector_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ivf dml iterator input",
             K(ret), KP(store_row), KP(row_projector_));
  } else if (is_pq || is_sq8) {
    // This release enables the deferred CtDef protocol for IVF_FLAT only.
    // Reject SQ8/PQ before producing any auxiliary row instead of relying on
    // encoder or storage errors.
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED,
                   is_pq ? "logical-table IVF PQ DML"
                         : "logical-table IVF SQ8 DML");
    LOG_WARN("logical-table non-FLAT IVF DML is not enabled",
             K(ret), K(is_pq), K(is_sq8), K(table_param));
  } else if (OB_FAIL(get_cid_output_idx_(cid_idx))) {
    LOG_WARN("get ivf cid output index failed", K(ret));
  } else if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(
      allocator_, row_projector_->count(), row))) {
    LOG_WARN("create ivf logical row failed", K(ret), K(row_projector_->count()));
  } else if (OB_FAIL(ObDASUtils::project_storage_row(
      *das_ctdef_, *store_row, *row_projector_, allocator_, *row))) {
    LOG_WARN("project ivf logical row failed", K(ret));
  } else if (OB_UNLIKELY(cid_idx >= row->get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ivf cid output index is out of range",
             K(ret), K(cid_idx), K(row->get_column_count()));
  } else {
    const blocksstable::ObStorageDatum &cid = row->storage_datums_[cid_idx];
    if (cid.is_null() || OB_ISNULL(cid.ptr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("materialized ivf cid has no payload",
               K(ret), K(cid_idx), K(cid.len_), KP(cid.ptr_));
    } else if (share::OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH != cid.len_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("materialized ivf cid has an invalid persisted width",
               K(ret), K(cid_idx), K(cid.len_),
               "expected_length", share::OB_LOGICAL_TABLE_IVF_CID_BYTE_LENGTH);
    } else if (ObExprVecIVFCenterID::is_deferred_placeholder(cid.get_string())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ivf projector observed an unmaterialized CtDef placeholder",
               K(ret), K(cid_idx));
    } else if (OB_FAIL(rows_.push_back(row))) {
      LOG_WARN("append ivf logical row failed", K(ret));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase
