/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_update_op.h"
#include "sql/das/ob_das_domain_utils.h"
#include "sql/das/ob_das_utils.h"
#include "src/sql/engine/px/ob_dfo.h"
#include "sql/engine/dml/ob_dml_service.h"
#include "storage/blocksstable/ob_datum_row_utils.h"
#include "sql/das/ob_das_dml_vec_iter.h"
#include "sql/das/ob_das_search_index_utils.h"
#include "storage/lob/ob_lob_manager.h"

namespace oceanbase
{
namespace common
{
namespace serialization
{
template <>
struct EnumEncoder<false, const sql::ObDASUpdCtDef *> : sql::DASCtEncoder<sql::ObDASUpdCtDef>
{
};

template <>
struct EnumEncoder<false, sql::ObDASUpdRtDef *> : sql::DASRtEncoder<sql::ObDASUpdRtDef>
{
};
} // end namespace serialization
} // end namespace common

using namespace common;
using namespace storage;
using namespace share;
namespace sql
{

class ObDASUpdIterator : public blocksstable::ObDatumRowIterator
{
public:
  ObDASUpdIterator(const ObDASUpdCtDef *das_ctdef,
                   ObDASWriteBuffer &write_buffer,
                   ObIAllocator &alloc)
    : das_ctdef_(das_ctdef),
      write_buffer_(write_buffer),
      old_row_(nullptr),
      new_row_(nullptr),
      old_rows_(nullptr),
      new_rows_(nullptr),
      got_row_count_(0),
      domain_iter_(nullptr),
      ft_doc_word_info_(nullptr),
      got_old_row_(false),
      iter_has_built_(false),
      allocator_(alloc),
      main_ctdef_(das_ctdef)
  {
    batch_size_ = MIN(write_buffer_.get_row_cnt(), DEFAULT_BATCH_SIZE);
  }
  virtual ~ObDASUpdIterator();

  virtual int get_next_row(blocksstable::ObDatumRow *&row) override;
  virtual int get_next_rows(blocksstable::ObDatumRow *&rows, int64_t &row_count) override;

  ObDASWriteBuffer &get_write_buffer() { return write_buffer_; }
  void set_lt_rt(const ObLogicalTableRtDef *) { /* no-op for update iter */ }
  int rewind(const ObDASDMLBaseCtDef *das_ctdef, const ObFTDocWordInfo *ft_doc_word_info)
  {
    int ret = common::OB_SUCCESS;
    old_row_ = nullptr;
    new_row_ = nullptr;
#define FREE_ROW_PTR(row_ptr)     \
  if (nullptr != row_ptr) {       \
    allocator_.free(row_ptr);     \
    row_ptr = nullptr;            \
  }
    FREE_ROW_PTR(old_rows_);
    FREE_ROW_PTR(new_rows_);
#undef FREE_ROW_PTR
    got_old_row_ = false;
    iter_has_built_ = false;
    ft_doc_word_info_ = ft_doc_word_info;
    das_ctdef_ = static_cast<const ObDASUpdCtDef*>(das_ctdef);
    if (OB_NOT_NULL(domain_iter_)) {
      if (!das_ctdef->table_param_.get_data_table().is_domain_index()) {
        // This table isn't domain index, nothing to do.
      } else if (domain_iter_->is_same_domain_type(das_ctdef)) {
        // The das_ctdef and das_ctdef_ are either full-text search or multi-value index.
        ObDomainDMLMode mode = ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT;
        if (das_ctdef->table_param_.get_data_table().is_fts_index()) {
          static_cast<ObFTDMLIterator *>(domain_iter_)->set_ft_doc_word_info(ft_doc_word_info_);
          if (!got_old_row_
              && !das_ctdef->is_logical_table()
              && das_ctdef->table_param_.get_data_table().can_read_index()) {
            mode = ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN;
          }
        }
        domain_iter_->set_ctdef(das_ctdef_,
                                &(got_old_row_ ? das_ctdef_->new_row_projector_
                                               : das_ctdef_->old_row_projector_),
                                mode);
        if (OB_FAIL(domain_iter_->rewind())) {
          LOG_WARN("fail to rewind for domain iterator", K(ret));
        }
      } else {
        // need to reset domain iter
        domain_iter_->~ObDomainDMLIterator();
        domain_iter_ = nullptr;
      }
    }
    return ret;
  }
private:
  // domain index
  int get_next_domain_index_row(blocksstable::ObDatumRow *&row);
  int get_next_domain_index_rows(ObDatumRow *&rows, int64_t &row_count);
private:
  static const int64_t DEFAULT_BATCH_SIZE = 256;
  const ObDASUpdCtDef *das_ctdef_;
  ObDASWriteBuffer &write_buffer_;
  blocksstable::ObDatumRow *old_row_;
  blocksstable::ObDatumRow *new_row_;
  blocksstable::ObDatumRow *old_rows_;
  blocksstable::ObDatumRow *new_rows_;
  int64_t got_row_count_;
  ObDASWriteBuffer::Iterator result_iter_;
  ObDomainDMLIterator *domain_iter_;
  const ObFTDocWordInfo *ft_doc_word_info_;
  bool got_old_row_;
  bool iter_has_built_;
  common::ObIAllocator &allocator_;
  int64_t batch_size_;
  const ObDASDMLBaseCtDef *main_ctdef_;
};

int ObDASUpdIterator::get_next_row(blocksstable::ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *sr = nullptr;

  if (OB_FAIL(write_buffer_.ensure_ivf_materialized())) {
    LOG_WARN("failed to materialize IVF rows", K(ret));
  } else if (OB_UNLIKELY(das_ctdef_->table_param_.get_data_table().is_domain_index())) {
    if (OB_FAIL(get_next_domain_index_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get next domain index row failed", K(ret), K(das_ctdef_->table_param_.get_data_table()));
      }
    }
  } else if (!got_old_row_) {
    got_old_row_ = true;
    if (OB_ISNULL(old_row_)) {
      if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(allocator_, das_ctdef_->old_row_projector_.count(), old_row_))) {
        LOG_WARN("create row buffer failed", K(ret), K(das_ctdef_->old_row_projector_.count()));
      } else if (OB_FAIL(write_buffer_.begin(result_iter_))) {
        LOG_WARN("begin datum result iterator failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_ISNULL(new_row_)) {
      if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(allocator_, das_ctdef_->new_row_projector_.count(), new_row_))) {
        LOG_WARN("create row buffer failed", K(ret), K(das_ctdef_->new_row_projector_.count()));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(result_iter_.get_next_row(sr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from result iterator failed", K(ret));
        }
      } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                         *sr,
                                                         das_ctdef_->old_row_projector_,
                                                         allocator_,
                                                         *old_row_))) {
        LOG_WARN("project old storage row failed", K(ret));
      } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                         *sr,
                                                         das_ctdef_->new_row_projector_,
                                                         allocator_,
                                                         *new_row_))) {
        LOG_WARN("project new storage row failed", K(ret));
      } else {
        row = old_row_;
        LOG_DEBUG("DAS update get old row",
                  K_(das_ctdef_->old_row_projector),
                  K_(das_ctdef_->new_row_projector),
                  "table_id", das_ctdef_->table_id_,
                  "index_tid", das_ctdef_->index_tid_, KPC_(old_row),
                  KPC_(new_row));
      }
    }
  } else {
    got_old_row_ = false;
    if (OB_ISNULL(new_row_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new row is null", K(ret));
    } else {
      row = new_row_;
      LOG_DEBUG("DAS update get new row",
                "table_id", das_ctdef_->table_id_,
                "index_tid", das_ctdef_->index_tid_, KPC_(new_row));
    }
  }
  return ret;
}

int ObDASUpdIterator::get_next_rows(blocksstable::ObDatumRow *&rows, int64_t &row_count)
{
  row_count = 0;
  int ret = OB_SUCCESS;
  const ObChunkDatumStore::StoredRow *sr = nullptr;
  bool is_domain_index = false;

  if (OB_FAIL(write_buffer_.ensure_ivf_materialized())) {
    LOG_WARN("failed to materialize IVF rows", K(ret));
  } else if (FALSE_IT(is_domain_index = das_ctdef_->table_param_.get_data_table().is_domain_index())) {
  } else if (1 == batch_size_) {
    if (OB_FAIL(get_next_row(rows))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("Failed to get next row", K(ret), K_(batch_size), K(is_domain_index));
      }
    } else {
      row_count = 1;
    }
  } else if (is_domain_index) {
    if (OB_FAIL(get_next_domain_index_rows(rows, row_count))) {
      LOG_WARN("fail to get next domain index rows", K(ret));
    }
  } else if (!got_old_row_) {
    got_old_row_ = true;
    if (OB_ISNULL(old_rows_)) {
      if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_rows(allocator_, batch_size_, das_ctdef_->old_row_projector_.count(), old_rows_))) {
        LOG_WARN("create old rows buffer failed", K(ret), K(das_ctdef_->old_row_projector_.count()));
      } else if (OB_FAIL(write_buffer_.begin(result_iter_))) {
        LOG_WARN("begin datum result iterator failed", K(ret));
      }
    }

    if (OB_SUCC(ret) && OB_ISNULL(new_rows_)) {
      if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_rows(allocator_, batch_size_, das_ctdef_->new_row_projector_.count(), new_rows_))) {
        LOG_WARN("create new rows buffer failed", K(ret), K(das_ctdef_->new_row_projector_.count()));
      }
    }
    while (OB_SUCC(ret) && row_count < batch_size_) {
      if (OB_FAIL(result_iter_.get_next_row(sr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row from result iterator failed", K(ret));
        }
      } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                         *sr,
                                                         das_ctdef_->old_row_projector_,
                                                         allocator_,
                                                         old_rows_[row_count]))) {
        LOG_WARN("project old storage row failed", K(ret));
      } else if (OB_FAIL(ObDASUtils::project_storage_row(*das_ctdef_,
                                                         *sr,
                                                         das_ctdef_->new_row_projector_,
                                                         allocator_,
                                                         new_rows_[row_count]))) {
        LOG_WARN("project new storage row failed", K(ret));
      } else {
        LOG_DEBUG("DAS update get row", K_(das_ctdef_->old_row_projector), K_(das_ctdef_->new_row_projector),
                  "table_id", das_ctdef_->table_id_, "index_tid", das_ctdef_->index_tid_,
                  K(old_rows_[row_count]), K(new_rows_[row_count]));
        ++row_count;
        got_row_count_ = row_count;
      }
    }
    if (OB_SUCC(ret) || OB_LIKELY(OB_ITER_END == ret)) {
      if (0 == row_count) {
        ret = OB_ITER_END;
      } else {
        rows = old_rows_;
        ret = OB_SUCCESS;
      }
    }
  } else {
    got_old_row_ = false;
    if (OB_ISNULL(new_rows_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("new rows is null", K(ret));
    } else {
      rows = new_rows_;
      row_count = got_row_count_;
      got_row_count_ = 0;
    }
  }
  return ret;
}

ObDASUpdIterator::~ObDASUpdIterator()
{
  if (nullptr != domain_iter_) {
    domain_iter_->~ObDomainDMLIterator();
    domain_iter_ = nullptr;
  }
}

int ObDASUpdIterator::get_next_domain_index_row(ObDatumRow *&row)
{
  int ret = OB_SUCCESS;
  if (!iter_has_built_) {
    if (OB_FAIL(write_buffer_.begin(result_iter_))) {
      LOG_WARN("begin write iterator failed", K(ret));
    } else {
      iter_has_built_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(domain_iter_)){
      bool is_search_index = das_ctdef_->table_param_.get_data_table().is_search_index();
      const IntFixedArray &cur_proj = got_old_row_ ? das_ctdef_->new_row_projector_ : das_ctdef_->old_row_projector_;
      ObDomainDMLParam param(allocator_, &cur_proj, result_iter_, das_ctdef_, is_search_index ? main_ctdef_ : nullptr);
      if (das_ctdef_->table_param_.get_data_table().is_fts_index() && !got_old_row_) {
        param.mode_ = !das_ctdef_->is_logical_table()
                          && das_ctdef_->table_param_.get_data_table().can_read_index()
                        ? ObDomainDMLMode::DOMAIN_DML_MODE_FT_SCAN
                        : ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT;
        param.ft_doc_word_info_ = ft_doc_word_info_;
      }
      if (OB_FAIL(ObDomainDMLIterator::create_domain_dml_iterator(param, domain_iter_))) {
        LOG_WARN("fail to create domain index dml iterator", K(ret));
      }
    }
    if (FAILEDx(domain_iter_->get_next_domain_row(row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next domain row", K(ret), KPC(domain_iter_));
      } else if (!got_old_row_) {
        // ret == OB_ITER_END, old row is finished, get next new row
        iter_has_built_ = false;
        got_old_row_ = true;
        domain_iter_->set_row_projector(&(das_ctdef_->new_row_projector_));
        sql::ObEmbeddedVecDMLIterator* tmp_iter = dynamic_cast<sql::ObEmbeddedVecDMLIterator *>(domain_iter_);
        if (OB_NOT_NULL(tmp_iter)) {
          tmp_iter->is_old_row_ = false;
        }
        if (OB_FAIL(domain_iter_->change_domain_dml_mode(ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT))) {
          LOG_WARN("fail to change domain dml mode", K(ret));
        } else {
          ret = OB_ITER_END;
        }
      }
    }
  }
  LOG_DEBUG("get next domain index row", K(ret), K(iter_has_built_), K(got_old_row_),
      KPC(domain_iter_), KPC(row));
  return ret;
}

int ObDASUpdIterator::get_next_domain_index_rows(ObDatumRow *&rows, int64_t &row_count)
{
  int ret = OB_SUCCESS;
  if (!iter_has_built_) {
    if (OB_FAIL(write_buffer_.begin(result_iter_))) {
      LOG_WARN("begin write iterator failed", K(ret));
    } else {
      iter_has_built_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(domain_iter_)) {
      bool is_search_index = das_ctdef_->table_param_.get_data_table().is_search_index();
      const IntFixedArray &cur_proj = got_old_row_ ? das_ctdef_->new_row_projector_ : das_ctdef_->old_row_projector_;
      ObDomainDMLParam param(allocator_, &cur_proj, result_iter_, das_ctdef_, is_search_index ? main_ctdef_ : nullptr);
      if (das_ctdef_->table_param_.get_data_table().is_fts_index() && !got_old_row_) {
        param.mode_ = !das_ctdef_->is_logical_table()
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
        LOG_WARN("fail to get next domain row", K(ret), KPC(domain_iter_));
      } else if (!got_old_row_) {
        // ret == OB_ITER_END, old row is finished, get next new row
        iter_has_built_ = false;
        got_old_row_ = true;
        domain_iter_->set_row_projector(&(das_ctdef_->new_row_projector_));
        sql::ObEmbeddedVecDMLIterator* tmp_iter = dynamic_cast<sql::ObEmbeddedVecDMLIterator *>(domain_iter_);
        if (OB_NOT_NULL(tmp_iter)) {
          tmp_iter->is_old_row_ = false;
        }
        if (OB_FAIL(domain_iter_->change_domain_dml_mode(ObDomainDMLMode::DOMAIN_DML_MODE_DEFAULT))) {
          LOG_WARN("fail to change domain dml mode", K(ret));
        } else {
          ret = OB_ITER_END;
        }
      }
    }
  }
  LOG_DEBUG("get next domain index rows", K(ret), K(iter_has_built_), K(got_old_row_),
      KPC(domain_iter_), KPC(rows), K(row_count));
  return ret;
}

// For logical table UPDATE: encode logical rows as physical KV rows.
// Normal indexes produce old/new row pairs; domain indexes produce all old rows
// before all new rows, so they must be consumed phase by phase.
class ObDASLogicalTableUpdPhaseIter : public blocksstable::ObDatumRowIterator
{
public:
  ObDASLogicalTableUpdPhaseIter()
    : inner_iter_(nullptr), lt_ctdef_(nullptr), lt_rt_(nullptr),
      aux_type_(share::LT_AUX_MAX_TYPE),
      mapping_(nullptr), in_delete_phase_(true),
      is_domain_index_(false), physical_row_(nullptr), new_row_idx_(0),
      delete_phase_allocator_(), insert_phase_allocator_() {}
  virtual ~ObDASLogicalTableUpdPhaseIter() = default;
  int init(blocksstable::ObDatumRowIterator *inner_iter,
           const ObDASDMLBaseCtDef *ctdef,
           const ObLogicalTableRtDef *lt_rt,
           share::ObLogicalTableAuxType aux_type,
           const ObLogicalTableColumnMapping *mapping,
           const bool is_domain_index)
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(inner_iter) || OB_ISNULL(ctdef) || OB_ISNULL(lt_rt) || OB_ISNULL(mapping)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(inner_iter), KP(ctdef), KP(lt_rt), KP(mapping));
    } else {
      inner_iter_ = inner_iter;
      lt_ctdef_ = ctdef;
      lt_rt_ = lt_rt;
      aux_type_ = aux_type;
      mapping_ = mapping;
      is_domain_index_ = is_domain_index;
    }
    return ret;
  }
  virtual int get_next_row(blocksstable::ObDatumRow *&datum_row) override
  {
    int ret = OB_SUCCESS;
    if (is_domain_index_) {
      blocksstable::ObDatumRow *domain_row = nullptr;
      if (OB_FAIL(inner_iter_->get_next_row(domain_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get domain index row failed", K(ret), K_(in_delete_phase));
        }
      } else if (OB_ISNULL(domain_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("domain index row is null", K(ret), K_(in_delete_phase));
      } else {
        common::ObArenaAllocator &phase_allocator =
            in_delete_phase_ ? delete_phase_allocator_ : insert_phase_allocator_;
        blocksstable::ObDatumRow *domain_phys = nullptr;
        phase_allocator.reset();
        if (OB_FAIL(encode_physical_row_(*domain_row, domain_phys, phase_allocator))) {
          LOG_WARN("encode domain physical row failed", K(ret), K_(in_delete_phase));
        } else {
          datum_row = domain_phys;
        }
      }
    } else if (in_delete_phase_) {
      blocksstable::ObDatumRow *old_row = nullptr;
      blocksstable::ObDatumRow *new_row = nullptr;
      if (OB_FAIL(inner_iter_->get_next_row(old_row))) {
        if (OB_ITER_END != ret) { LOG_WARN("get old row failed", K(ret)); }
      } else if (OB_ISNULL(old_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("old row is null", K(ret));
      } else {
        // Rows returned by the DML iterator may reference the shared expression
        // frame.  Fetching/evaluating the new row can overwrite datums in the
        // old row (notably a CID filled by the domain-id merge iterator), so the
        // old logical row must be encoded before advancing the inner iterator.
        delete_phase_allocator_.reset();
        physical_row_ = nullptr;
        if (OB_FAIL(encode_physical_row_(*old_row, physical_row_, delete_phase_allocator_))) {
          LOG_WARN("encode old physical row failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(inner_iter_->get_next_row(new_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("logical update stream ended after old row without paired new row", K(ret));
        } else {
          LOG_WARN("get new row failed", K(ret));
        }
      } else if (OB_SUCC(ret) && OB_ISNULL(new_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("new row is null", K(ret));
      } else if (OB_SUCC(ret)) {
        blocksstable::ObDatumRow *new_phys = nullptr;
        if (OB_FAIL(encode_physical_row_(*new_row, new_phys, insert_phase_allocator_))) {
          LOG_WARN("encode new physical row failed", K(ret));
        } else if (OB_FAIL(new_encoded_rows_.push_back(new_phys))) {
          LOG_WARN("push new encoded row failed", K(ret));
        } else {
          datum_row = physical_row_;
        }
      }
    } else {
      if (new_row_idx_ >= new_encoded_rows_.count()) {
        ret = OB_ITER_END;
      } else {
        datum_row = new_encoded_rows_.at(new_row_idx_++);
      }
    }
    return ret;
  }
  virtual void reset() override
  {
    physical_row_ = nullptr;
    new_encoded_rows_.reset();
    new_row_idx_ = 0;
    in_delete_phase_ = true;
    delete_phase_allocator_.reset();
    insert_phase_allocator_.reset();
  }
  void switch_to_insert_phase()
  {
    in_delete_phase_ = false;
    new_row_idx_ = 0;
    physical_row_ = nullptr;
    delete_phase_allocator_.reset();
  }
private:
  int encode_physical_row_(const blocksstable::ObDatumRow &logical_row,
                           blocksstable::ObDatumRow *&physical_row,
                           common::ObIAllocator &row_allocator)
  {
    int ret = OB_SUCCESS;
    const int64_t physical_col_count =
        (OB_INVALID_ID != lt_ctdef_->lt_physical_namespace_col_id_ ? 3 : 2);
    if (OB_ISNULL(physical_row)) {
      if (OB_FAIL(blocksstable::ObDatumRowUtils::ob_create_row(
              row_allocator, physical_col_count, physical_row))) {
        LOG_WARN("create physical row failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      ObString key;
      ObString value;
      if (OB_FAIL(ObLogicalTableEncoder::encode_physical_row(
              lt_rt_->namespace_id_, lt_rt_->logical_table_id_,
              lt_ctdef_->lt_physical_key_col_id_, lt_ctdef_->lt_physical_value_col_id_,
              aux_type_, *mapping_, logical_row, row_allocator, key, value))) {
        LOG_WARN("encode physical row failed", K(ret));
      } else {
        if (is_domain_index_) {
          static constexpr int64_t MAX_HEX_PREVIEW = 256;
          const int64_t key_preview_len =
              MIN(static_cast<int64_t>(key.length()), MAX_HEX_PREVIEW);
          const int64_t value_preview_len =
              MIN(static_cast<int64_t>(value.length()), MAX_HEX_PREVIEW);
          char key_hex_buf[2 * MAX_HEX_PREVIEW + 4];
          char value_hex_buf[2 * MAX_HEX_PREVIEW + 4];
          key_hex_buf[0] = '\0';
          value_hex_buf[0] = '\0';
          if (key_preview_len > 0
              && OB_SUCCESS != common::to_hex_cstr(
                  key.ptr(), key_preview_len, key_hex_buf, sizeof(key_hex_buf))) {
            key_hex_buf[0] = '\0';
          }
          if (value_preview_len > 0
              && OB_SUCCESS != common::to_hex_cstr(
                  value.ptr(), value_preview_len, value_hex_buf, sizeof(value_hex_buf))) {
            value_hex_buf[0] = '\0';
          }
          LOG_DEBUG("[LT hybrid DAS] logical table update encoded domain physical row",
                   K_(in_delete_phase), K_(is_domain_index), K_(aux_type),
                   "index_seq", mapping_->index_seq_,
                   "key_len", key.length(), "key_hex", key_hex_buf,
                   "value_len", value.length(), "value_hex", value_hex_buf,
                   K(logical_row));
        }
        if (OB_INVALID_ID != lt_ctdef_->lt_physical_namespace_col_id_) {
          physical_row->storage_datums_[0].set_uint(lt_rt_->namespace_id_);
          physical_row->storage_datums_[1].set_string(key);
          ObString lob_value;
          if (OB_FAIL(storage::ObLobManager::fill_lob_header(
                  row_allocator, value, lob_value))) {
            LOG_WARN("failed to fill lob header for kv_value", K(ret), K(value));
          } else {
            physical_row->storage_datums_[2].set_string(lob_value);
            physical_row->count_ = 3;
          }
        } else {
          physical_row->storage_datums_[0].set_string(key);
          ObString lob_value;
          if (OB_FAIL(storage::ObLobManager::fill_lob_header(
                  row_allocator, value, lob_value))) {
            LOG_WARN("failed to fill lob header for kv_value", K(ret), K(value));
          } else {
            physical_row->storage_datums_[1].set_string(lob_value);
            physical_row->count_ = 2;
          }
        }
      }
    }
    return ret;
  }

private:
  blocksstable::ObDatumRowIterator *inner_iter_;
  const ObDASDMLBaseCtDef *lt_ctdef_;
  const ObLogicalTableRtDef *lt_rt_;
  share::ObLogicalTableAuxType aux_type_;
  const ObLogicalTableColumnMapping *mapping_;
  bool in_delete_phase_;
  bool is_domain_index_;
  blocksstable::ObDatumRow *physical_row_;
  common::ObSEArray<blocksstable::ObDatumRow*, 8> new_encoded_rows_;
  int64_t new_row_idx_;
  common::ObArenaAllocator delete_phase_allocator_;
  common::ObArenaAllocator insert_phase_allocator_;
};

template <>
int ObDASIndexDMLAdaptor<DAS_OP_TABLE_UPDATE, ObDASUpdIterator>::write_rows(const ObLSID &ls_id,
                                                                            const ObTabletID &tablet_id,
                                                                            const CtDefType &ctdef,
                                                                            RtDefType &rtdef,
                                                                            ObDASUpdIterator &iter,
                                                                            int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObAccessService *as = MTL(ObAccessService *);
  // Logical table UPDATE is always delete-then-insert. The iterator encodes
  // physical KV rows; storage redirect is driven by dml_param_.ctx_.
  if (ctdef.is_logical_table()
      && rtdef.lt_rt_.enabled_
      && !ctdef.logical_column_mapping_.empty()
      && OB_NOT_NULL(ctdef.lt_physical_table_param_)) {
    ObDASLogicalTableUpdPhaseIter lt_upd_iter;
    common::ObSEArray<uint64_t, 3> physical_column_ids;
    // The phase iterator needs logical old/new rows and performs the physical
    // KV encoding itself. Disable the generic iterator's logical-table encoding
    // here, otherwise an already encoded physical row is encoded a second time.
    iter.set_lt_rt(nullptr);
    if (OB_INVALID_ID != ctdef.lt_physical_namespace_col_id_
        && OB_FAIL(physical_column_ids.push_back(ctdef.lt_physical_namespace_col_id_))) {
      LOG_WARN("push physical namespace column id failed", K(ret));
    } else if (OB_FAIL(physical_column_ids.push_back(ctdef.lt_physical_key_col_id_))) {
      LOG_WARN("push physical key column id failed", K(ret));
    } else if (OB_FAIL(physical_column_ids.push_back(ctdef.lt_physical_value_col_id_))) {
      LOG_WARN("push physical value column id failed", K(ret));
    } else if (OB_FAIL(lt_upd_iter.init(&iter, &ctdef, &rtdef.lt_rt_,
                                 ctdef.logical_table_aux_type_,
                                 &ctdef.logical_column_mapping_,
                                 ctdef.table_param_.get_data_table().is_domain_index()))) {
      LOG_WARN("init logical table upd iter failed", K(ret));
    } else if (OB_FAIL(as->delete_rows(ls_id, tablet_id, *tx_desc_, dml_param_,
                                       physical_column_ids, &lt_upd_iter, affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("delete old rows to access service failed (logical table update)", K(ret),
                 K(tablet_id));
      }
    } else {
      lt_upd_iter.switch_to_insert_phase();
      if (OB_FAIL(as->insert_rows(ls_id, tablet_id, *tx_desc_, dml_param_,
                                  physical_column_ids, &lt_upd_iter, affected_rows))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("insert new rows to access service failed (logical table update)", K(ret),
                   K(tablet_id));
        }
      }
    }
  } else if (OB_UNLIKELY((ctdef.table_param_.get_data_table().is_vector_delta_buffer() ||
                  ctdef.table_param_.get_data_table().is_hybrid_vector_index_log()) &&
                  !ctdef.is_access_mlog_as_master_table_)) {
    // for vector delta buffer/hybrid log table, only do insert when DML with main table
    if (OB_FAIL(as->insert_rows(ls_id, tablet_id, *tx_desc_, dml_param_,
                                ctdef.column_ids_, &iter, affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("insert rows to access service failed", K(ret), K(ls_id), K(tablet_id));
      }
    }
  } else if (OB_UNLIKELY(ctdef.table_param_.get_data_table().is_domain_index())) {
    if (OB_FAIL(as->delete_rows(ls_id, tablet_id, *tx_desc_, dml_param_,
                                ctdef.column_ids_, &iter, affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("delete rows to access service failed", K(ret), K(ls_id), K(tablet_id));
      }
    } else if (OB_FAIL(as->insert_rows(ls_id, tablet_id, *tx_desc_, dml_param_,
                                       ctdef.column_ids_, &iter, affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("insert rows to access service failed", K(ret), K(ls_id), K(tablet_id));
      }
    }
  } else if (ctdef.table_param_.get_data_table().is_mlog_table()
      && !ctdef.is_access_mlog_as_master_table_) {
    ObDASMLogDMLIterator mlog_iter(ls_id, tablet_id, dml_param_, &iter, DAS_OP_TABLE_UPDATE);
    if (OB_FAIL(as->insert_rows(ls_id,
                                tablet_id,
                                *tx_desc_,
                                dml_param_,
                                ctdef.column_ids_,
                                &mlog_iter,
                                affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("delete rows to access service failed", K(ret));
      }
    }
  } else if (OB_FAIL(as->update_rows(ls_id,
                                     tablet_id,
                                     *tx_desc_,
                                     dml_param_,
                                     ctdef.column_ids_,
                                     ctdef.updated_column_ids_,
                                     &iter,
                                     affected_rows))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
      LOG_WARN("update row to partition storage failed", K(ret));
    }
  } else if (!(ctdef.is_ignore_ || 
            ctdef.table_param_.get_data_table().is_domain_index())
      && 0 == affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected_rows after do update", K(affected_rows), K(ret));
  }
  return ret;
}

ObDASUpdateOp::ObDASUpdateOp(ObIAllocator &op_alloc)
  : ObIDASTaskOp(op_alloc),
    upd_ctdef_(nullptr),
    upd_rtdef_(nullptr),
    write_buffer_(),
    affected_rows_(0),
    outtable_lob_control_key_collector_()
{
}

int ObDASUpdateOp::open_op()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  common::ObSEArray<ObFTDocWordInfo, 4> doc_word_infos;
  doc_word_infos.set_attr(lib::ObMemAttr(MTL_ID(), "FTDocWInfo"));
  common::ObSEArray<share::ObVectorIndexAcquireCtx, OB_DAS_VEC_INDEX_ACQUIRE_CTX_INLINE_CNT> vec_index_ctxs;
  // R9/R12: UPDATE open_op 填主 lt_rt 并 propagate
  // 本地 DAS 路径：write_row 已用 engine eval_ctx 预解析 lt_rt（enabled_=true）
  // Remote DAS 路径：eval_ctx_ 在反序列化时注入，此处补充解析
  outtable_lob_control_key_collector_.reuse();
  if (OB_ISNULL(upd_ctdef_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update ctdef is null", K(ret));
  } else if (OB_FAIL(outtable_lob_control_key_collector_.prepare_if_needed(*upd_ctdef_))) {
    LOG_WARN("failed to prepare outtable lob control key array", K(ret));
  } else if (OB_NOT_NULL(upd_rtdef_) && upd_ctdef_->is_logical_table()) {
    ObLogicalTableRtDef &lt_rt = upd_rtdef_->lt_rt_;
    if (!lt_rt.enabled_ && OB_NOT_NULL(upd_rtdef_->eval_ctx_)) {
      if (OB_FAIL(ObDASUtils::resolve_lt_rt_from_dml_ctdef(
                     *upd_ctdef_,
                     *upd_rtdef_->eval_ctx_,
                     lt_rt,
                     OB_NOT_SUPPORTED,
                     "logical table DML must carry namespace_id and ltable_id WHERE predicates"))) {
        LOG_WARN("resolve lt_rt from update WHERE exprs failed", K(ret));
      }
    } else if (!lt_rt.enabled_) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "logical table DML must carry namespace_id and ltable_id WHERE predicates");
      LOG_WARN("lt_rt not resolved and eval_ctx not available", K(ret));
    }
    if (OB_SUCC(ret) && lt_rt.enabled_) {
      for (int64_t i = 0; OB_SUCC(ret) && i < related_ctdefs_.count(); ++i) {
        ObDASDMLBaseRtDef *related_rtdef = static_cast<ObDASDMLBaseRtDef *>(related_rtdefs_.at(i));
        if (OB_NOT_NULL(related_rtdef)) {
          ObDASUtils::copy_lt_rt_from_main(lt_rt, related_rtdef->lt_rt_);
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(ObLogicalTableRtDef::require_resolved_ns_lt(lt_rt, "DML open_op after propagate"))) {
          LOG_WARN("lt_rt not resolved after propagate", K(ret), K(lt_rt));
        }
      }
    }
  }
  ObDASUpdIterator upd_iter(upd_ctdef_, write_buffer_, op_alloc_);
  ObDASIndexDMLAdaptor<DAS_OP_TABLE_UPDATE, ObDASUpdIterator> upd_adaptor;
  upd_adaptor.tx_desc_ = trans_desc_;
  upd_adaptor.snapshot_ = snapshot_;
  upd_adaptor.write_branch_id_ = write_branch_id_;
  upd_adaptor.ctdef_ = upd_ctdef_;
  upd_adaptor.rtdef_ = upd_rtdef_;
  upd_adaptor.related_ctdefs_ = &related_ctdefs_;
  upd_adaptor.related_rtdefs_ = &related_rtdefs_;
  upd_adaptor.tablet_id_ = tablet_id_;
  upd_adaptor.ls_id_ = ls_id_;
  upd_adaptor.related_tablet_ids_ = &related_tablet_ids_;
  upd_adaptor.is_do_gts_opt_ = das_gts_opt_info_.use_specify_snapshot_;
  upd_adaptor.das_allocator_ = &op_alloc_;
  upd_adaptor.tablet_loc_ = tablet_loc_;
  upd_adaptor.ft_doc_word_infos_ = &doc_word_infos;
  upd_adaptor.set_outtable_lob_control_keys(outtable_lob_control_key_collector_.get_keys());
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDASDomainUtils::build_ft_doc_word_infos(ls_id_, trans_desc_, snapshot_, related_ctdefs_, related_tablet_ids_,
          upd_ctdef_->is_main_table_in_fts_ddl_, doc_word_infos))) {
    LOG_WARN("fail to build fulltext doc word infos", K(ret), K(ls_id_), KPC(snapshot_), K(related_ctdefs_),
        K(related_tablet_ids_));
  } else if (!upd_ctdef_->not_need_build_vec_index_tablet_infos_
      && OB_FAIL(ObDASDomainUtils::build_vec_index_tablet_infos(
          tablet_id_, upd_ctdef_->table_param_.get_data_table().get_table_id(),
          *upd_ctdef_,
          related_ctdefs_, related_tablet_ids_, vec_index_ctxs))) {
    LOG_WARN("fail to build vec index tablet infos", K(ret), K(tablet_id_), K(related_ctdefs_),
        K(related_tablet_ids_));
  } else {
    if (!vec_index_ctxs.empty()) {
      upd_adaptor.vec_index_acquire_ctxs_ = &vec_index_ctxs;
    }
    if (OB_FAIL(upd_adaptor.write_tablet(upd_iter, affected_rows))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("update row to partition storage failed", K(ret));
      }
    } else {
      affected_rows_ = affected_rows;
    }
  }
  return ret;
}

int ObDASUpdateOp::release_op()
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObDASUpdateOp::assign_task_result(ObIDASTaskOp *other)
{
  int ret = OB_SUCCESS;
  if (other->get_type() != get_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected task type", K(ret), KPC(other));
  } else {
    ObDASUpdateOp *upd_op = static_cast<ObDASUpdateOp *>(other);
    affected_rows_ = upd_op->get_affected_rows();
    const ObIArray<ObOuttableLobControlKey> *control_keys =
        upd_op->get_outtable_lob_control_keys();
    if (OB_FAIL(outtable_lob_control_key_collector_.assign(control_keys))) {
      LOG_WARN("failed to assign outtable lob control keys", K(ret));
    }
  }
  return ret;
}

int ObDASUpdateOp::record_task_result_to_rtdef()
{
  int ret = OB_SUCCESS;
  upd_rtdef_->affected_rows_ += affected_rows_;
  if (OB_FAIL(outtable_lob_control_key_collector_.append_to(
      upd_rtdef_->outtable_lob_control_keys_))) {
    LOG_WARN("failed to record outtable lob control keys", K(ret));
  }
  return ret;
}

int ObDASUpdateOp::decode_task_result(ObIDASTaskResult *task_result)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(*task_result) == typeid(ObDASUpdateResult));
  CK(task_id_ == task_result->get_task_id());
#endif
  if (OB_SUCC(ret)) {
    ObDASUpdateResult *update_result = static_cast<ObDASUpdateResult*>(task_result);
    const ObIArray<ObOuttableLobControlKey> *control_keys =
        update_result->get_outtable_lob_control_keys();
    if (OB_FAIL(outtable_lob_control_key_collector_.assign(control_keys))) {
      LOG_WARN("failed to decode outtable lob control keys", K(ret));
    }
    if (OB_SUCC(ret)) {
      affected_rows_ = update_result->get_affected_rows();
    }
  }
  return ret;
}

int ObDASUpdateOp::fill_task_result(ObIDASTaskResult &task_result, bool &has_more, int64_t &memory_limit)
{
  int ret = OB_SUCCESS;
#if !defined(NDEBUG)
  CK(typeid(task_result) == typeid(ObDASUpdateResult));
#endif
  if (OB_SUCC(ret)) {
    ObDASUpdateResult &update_result = static_cast<ObDASUpdateResult&>(task_result);
    const ObIArray<ObOuttableLobControlKey> *control_keys =
        outtable_lob_control_key_collector_.get_keys();
    if (OB_NOT_NULL(control_keys)
        && OB_FAIL(update_result.assign_outtable_lob_control_keys(*control_keys))) {
      LOG_WARN("failed to fill outtable lob control keys", K(ret));
    } else {
      memory_limit -= outtable_lob_control_key_collector_.get_serialize_size();
    }
    if (OB_SUCC(ret)) {
      update_result.set_affected_rows(affected_rows_);
      has_more = false;
    }
  }
  return ret;
}

int ObDASUpdateOp::init_task_info(uint32_t row_extend_size)
{
  int ret = OB_SUCCESS;
  if (!write_buffer_.is_inited()
      && OB_FAIL(write_buffer_.init(op_alloc_, row_extend_size, MTL_ID(), "DASUpdateBuffer"))) {
    LOG_WARN("init update buffer failed", K(ret));
  }
  return ret;
}

int ObDASUpdateOp::swizzling_remote_task(ObDASRemoteInfo *remote_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObIDASTaskOp::swizzling_remote_task(remote_info))) {
    LOG_WARN("fail to swizzling remote task", K(ret));
  } else if (remote_info != nullptr) {
    trans_desc_ = remote_info->trans_desc_;
  }
  return ret;
}

int ObDASUpdateOp::write_row(const ExprFixedArray &row,
                             ObEvalCtx &eval_ctx,
                             ObChunkDatumStore::StoredRow *&stored_row)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(upd_rtdef_)) {
    upd_rtdef_->eval_ctx_ = &eval_ctx;
  }
  if (!write_buffer_.is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer not inited", K(ret));
  } else if (OB_FAIL(write_buffer_.add_row(row, &eval_ctx, stored_row, true))) {
    LOG_WARN("add row to write buffer failed", K(ret), K(row), K(write_buffer_));
  } else if (OB_NOT_NULL(upd_ctdef_) && OB_NOT_NULL(upd_rtdef_)
             && upd_ctdef_->is_logical_table()) {
    if (OB_FAIL(ObDASUtils::resolve_lt_rt_from_dml_ctdef(
            *upd_ctdef_, eval_ctx, upd_rtdef_->lt_rt_, OB_SUCCESS, nullptr))) {
      LOG_WARN("resolve lt_rt from update WHERE exprs failed", K(ret));
    }
  }
  return ret;
}

OB_SERIALIZE_MEMBER((ObDASUpdateOp, ObIDASTaskOp),
                    upd_ctdef_,
                    upd_rtdef_,
                    write_buffer_);

ObDASUpdateResult::ObDASUpdateResult()
  : ObIDASTaskResult(),
    affected_rows_(0),
    outtable_lob_control_key_collector_()
{
}

ObDASUpdateResult::~ObDASUpdateResult()
{
}

int ObDASUpdateResult::init(const ObIDASTaskOp &op, common::ObIAllocator &alloc)
{
  UNUSED(op);
  UNUSED(alloc);
  outtable_lob_control_key_collector_.reset();
  return OB_SUCCESS;
}

int ObDASUpdateResult::reuse()
{
  int ret = OB_SUCCESS;
  affected_rows_ = 0;
  outtable_lob_control_key_collector_.reset();
  return ret;
}

int ObDASUpdateResult::assign_outtable_lob_control_keys(
    const ObIArray<ObOuttableLobControlKey> &control_keys)
{
  return outtable_lob_control_key_collector_.assign(&control_keys);
}

OB_SERIALIZE_MEMBER((ObDASUpdateResult, ObIDASTaskResult),
                    affected_rows_,
                    outtable_lob_control_key_collector_);
}  // namespace sql
}  // namespace oceanbase
