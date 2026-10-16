/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_data_to_lob_table_channel.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_table_op.h"

namespace oceanbase
{
namespace observer
{
using namespace blocksstable;
using namespace common;
using namespace storage;

int ObTableLoadDataToLobTableChannel::fill_lob_aux_key(
  const ObStorageDatum &lob_datum, ObDirectLoadDatumRow &lob_aux_key_row, bool &is_outrow)
{
  int ret = OB_SUCCESS;
  is_outrow = false;
  if (lob_datum.is_null()) {
  } else if (OB_UNLIKELY(lob_datum.get_string().length() < sizeof(ObLobCommon))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob datum length", KR(ret), K(lob_datum.get_string().length()));
  } else if (OB_UNLIKELY(2 != lob_aux_key_row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid lob aux key row", KR(ret), K(lob_aux_key_row));
  } else {
    const ObLobCommon &lob_common = lob_datum.get_lob_data();
    if (OB_UNLIKELY(!lob_common.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid lob common header", KR(ret), K(lob_common));
    } else if (OB_UNLIKELY(!lob_common.in_row_ && lob_common.is_mem_loc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected mem lob locator", KR(ret), K(lob_common));
    } else if (!lob_common.in_row_) {
      if (OB_UNLIKELY(lob_datum.get_string().length() <
                      sizeof(ObLobCommon) + sizeof(ObLobData))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid outrow lob datum length", KR(ret), K(lob_datum.get_string().length()));
      } else {
        const ObString lob_id_string(sizeof(ObLobId), lob_common.buffer_);
        ObLobId lob_id;
        if (OB_FAIL(lob_id.from_string(lob_id_string))) {
          LOG_WARN("failed to parse lob id", KR(ret));
        } else if (OB_UNLIKELY(!lob_id.is_valid() || lob_id.get_column_idx() > INT64_MAX)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid lob id", KR(ret), K(lob_id));
        } else {
          lob_aux_key_row.storage_datums_[0].set_int(
            static_cast<int64_t>(lob_id.get_column_idx()));
          lob_aux_key_row.storage_datums_[1].set_string(lob_id_string);
          is_outrow = true;
        }
      }
    }
  }
  return ret;
}

int ObTableLoadDataToLobTableChannel::handle_insert_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_insert_row(const ObTabletID &tablet_id,
                                                        const ObDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_insert_batch(const ObTabletID &tablet_id,
                                                          const ObBatchDatumRows &datum_rows)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_delete_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataToLobTableChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected channel is closed", KR(ret));
  } else if (OB_FAIL(append_lob_aux_delete_rows(tablet_id, datum_row))) {
    LOG_WARN("fail to append lob aux delete rows", KR(ret), K(tablet_id));
  }
  return ret;
}

int ObTableLoadDataToLobTableChannel::handle_update_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_update_row(
  const ObTabletID &tablet_id, ObIArray<const ObDirectLoadExternalRow *> &rows,
  const ObDirectLoadExternalRow *result_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_update_row(
  ObArray<const ObDirectLoadMultipleDatumRow *> &rows,
  const ObDirectLoadMultipleDatumRow *result_row)
{
  return OB_SUCCESS;
}

int ObTableLoadDataToLobTableChannel::handle_update_row(const ObTabletID &tablet_id,
                                                        const ObDirectLoadDatumRow &old_row,
                                                        const ObDirectLoadDatumRow &new_row,
                                                        const ObDirectLoadDatumRow *result_row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTableLoadDataToLobTableChannel not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(is_closed_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected channel is closed", KR(ret));
  } else if (result_row == &new_row &&
             OB_FAIL(append_lob_aux_delete_rows(tablet_id, old_row))) {
    LOG_WARN("fail to append lob aux delete rows", KR(ret), K(tablet_id));
  }
  return ret;
}

int ObTableLoadDataToLobTableChannel::append_lob_aux_delete_rows(
  const ObTabletID &tablet_id, const ObDirectLoadDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  ObTableLoadTableBuilder *table_builder = nullptr;
  if (OB_FAIL(table_builder_mgr_.get_table_builder(table_builder))) {
    LOG_WARN("fail to get table builder", KR(ret));
  } else {
    ObDirectLoadDatumRow &delete_datum_row = table_builder->get_delete_datum_row();
    delete_datum_row.seq_no_ = datum_row.seq_no_;
    ObTabletID lob_tablet_id;
    if (OB_FAIL(
          (static_cast<ObTableLoadStoreLobTableCtx *>(down_table_op_->op_ctx_->store_table_ctx_))
            ->get_tablet_id(tablet_id, lob_tablet_id))) {
      LOG_WARN("fail to get tablet id", KR(ret));
    }
    common::ObArray<int64_t> &lob_column_idxs =
      up_table_op_->op_ctx_->store_table_ctx_->schema_->lob_column_idxs_;
    for (int64_t i = 0; OB_SUCC(ret) && i < lob_column_idxs.count(); i++) {
      const int64_t lob_idx = lob_column_idxs.at(i);
      bool is_outrow = false;
      if (OB_FAIL(fill_lob_aux_key(
            datum_row.storage_datums_[lob_idx], delete_datum_row, is_outrow))) {
        LOG_WARN("fail to fill lob aux key", KR(ret), K(lob_idx));
      } else if (is_outrow &&
                 OB_FAIL(table_builder->append_row(lob_tablet_id, delete_datum_row))) {
        LOG_WARN("fail to append row", KR(ret));
      }
    }
  }
  return ret;
}

int ObTableLoadDataToLobTableChannel::handle_insert_delete_conflict(const ObTabletID &tablet_id,
                                                                     const ObDirectLoadDatumRow &datum_row)
{
  return OB_SUCCESS;
}

} // namespace observer
} // namespace oceanbase
