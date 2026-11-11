/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/ddl/ob_async_index_macro_writer.h"
#include "storage/blocksstable/vector_index/ob_ivf_tablet_context.h"
#include "storage/ddl/ob_ddl_redo_log_writer.h"
#include "storage/ddl/ob_ddl_inc_redo_log_writer.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/blocksstable/ob_batch_datum_rows.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_ddl_common.h"

namespace oceanbase
{
using namespace blocksstable;
using namespace share;
using namespace share::schema;
namespace storage
{

ObAsyncIndexMacroWriterAdapter::ObAsyncIndexMacroWriterAdapter()
  : is_inited_(false),
    allocator_(ObMemAttr(MTL_ID(), "AsyncIdxWrite")),
    nested_group_allocator_(ObMemAttr(MTL_ID(), "AsyncIdxGroup")),
    nested_key_list_buffer_(&nested_group_allocator_),
    nested_key_list_writer_(),
    pending_nested_row_(),
    inv_idx_row_desc_(),
    ivf_row_desc_(),
    data_desc_(),
    index_builder_(nullptr),
    object_cleaner_(nullptr),
    ddl_redo_callback_(nullptr),
    is_inc_redo_callback_(false),
    inv_writer_(),
    ivf_writer_(),
    is_disk_bbq_writer_(false),
    tablet_id_(),
    cg_idx_(0),
    parallel_idx_(0),
    row_count_(0),
    has_pending_nested_group_(false)
{
}

ObAsyncIndexMacroWriterAdapter::~ObAsyncIndexMacroWriterAdapter()
{
  reset();
}

void ObAsyncIndexMacroWriterAdapter::reset()
{
  inv_writer_.reset();
  ivf_writer_.reset();
  reuse_nested_group();
  if (nullptr != ddl_redo_callback_) {
    if (is_inc_redo_callback_) {
      static_cast<ObDDLIncRedoLogWriterCallback *>(ddl_redo_callback_)->~ObDDLIncRedoLogWriterCallback();
    } else {
      static_cast<ObDDLRedoLogWriterCallback *>(ddl_redo_callback_)->~ObDDLRedoLogWriterCallback();
    }
    allocator_.free(ddl_redo_callback_);
    ddl_redo_callback_ = nullptr;
    is_inc_redo_callback_ = false;
  }
  if (nullptr != index_builder_) {
    index_builder_->~ObSSTableIndexBuilder();
    allocator_.free(index_builder_);
    index_builder_ = nullptr;
  }
  object_cleaner_ = nullptr; // owned by index_builder_
  inv_idx_row_desc_.reset();
  ivf_row_desc_.reset();
  data_desc_.reset();
  tablet_id_.reset();
  cg_idx_ = 0;
  parallel_idx_ = 0;
  row_count_ = 0;
  is_disk_bbq_writer_ = false;
  has_pending_nested_group_ = false;
  is_inc_redo_callback_ = false;
  pending_nested_row_.reset();
  allocator_.reset();
  is_inited_ = false;
}

int ObAsyncIndexMacroWriterAdapter::build_inv_idx_row_desc(
    const ObWriteMacroParam &param,
    ObInvIdxRowDesc &inv_idx_row_desc)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *index_schema = nullptr;
  const ObTableSchema *data_schema = nullptr;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t index_table_id = param.ddl_table_schema_.table_id_;
  uint64_t data_table_id = OB_INVALID_ID;
  inv_idx_row_desc.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || OB_INVALID_ID == index_table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid schema id", K(ret), K(tenant_id), K(index_table_id), K(data_table_id), K(param));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("get tenant schema guard failed", K(ret), K(tenant_id), K(param));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
    LOG_WARN("get index table schema failed", K(ret), K(tenant_id), K(index_table_id), K(param));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("index table schema is null", K(ret), K(tenant_id), K(index_table_id), K(param));
  } else if (FALSE_IT(data_table_id = index_schema->get_data_table_id())) {
  } else if (OB_UNLIKELY(OB_INVALID_ID == data_table_id)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index table has invalid data table id", K(ret), K(tenant_id), K(index_table_id), K(data_table_id),
        KPC(index_schema), K(param));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, data_table_id, data_schema))) {
    LOG_WARN("get data table schema failed", K(ret), K(tenant_id), K(data_table_id), K(param));
  } else if (OB_ISNULL(data_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("data table schema is null", K(ret), K(tenant_id), K(data_table_id), K(param));
  } else if (OB_FAIL(inv_idx_row_desc.init(*index_schema, *data_schema))) {
    LOG_WARN("fail to init inverted index row desc", K(ret), KPC(index_schema), KPC(data_schema), K(param));
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::build_ivf_row_desc(
    const ObWriteMacroParam &param,
    ObIvfSSTableRowDesc &ivf_row_desc)
{
  int ret = OB_SUCCESS;
  ivf_row_desc.reset();
  if (OB_ISNULL(param.tablet_context_)
      || OB_ISNULL(param.tablet_context_->ivf_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Disk BBQ tablet context is null", K(ret), K(param));
  } else {
    const ObIvfSSTableMeta &ivf_meta =
        param.tablet_context_->ivf_ctx_->ivf_meta_;
    const int32_t format_version = ivf_meta.is_empty()
        ? IVF_SSTABLE_FORMAT_VERSION
        : ivf_meta.format_version_;
    if (OB_FAIL(ivf_row_desc.init(format_version))) {
      LOG_WARN("failed to initialize IVF SSTable row descriptor", K(ret),
               K(ivf_meta));
    }
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::build_search_inv_idx_row_desc(
    const bool has_nested_paths,
    const int64_t rowkey_column_cnt,
    const int64_t storage_column_cnt,
    ObInvIdxRowDesc &inv_idx_row_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(inv_idx_row_desc.init_search(
      has_nested_paths, rowkey_column_cnt, storage_column_cnt))) {
    LOG_WARN("fail to init search index row desc",
        K(ret), K(has_nested_paths), K(rowkey_column_cnt), K(storage_column_cnt));
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::prepare_data_store_desc(
    const ObWriteMacroParam &param,
    const ObInvIdxRowDesc &inv_idx_row_desc)
{
  int ret = OB_SUCCESS;
  const ObWriteTabletParam &tablet_param = param.tablet_param_;
  const ObStorageSchema *storage_schema = tablet_param.storage_schema_;
  compaction::ObExecMode exec_mode = GCTX.is_shared_storage_mode() ?
                                     compaction::ObExecMode::EXEC_MODE_OUTPUT :
                                     compaction::ObExecMode::EXEC_MODE_LOCAL;
  // inc-major (async index incremental refresh) must build the inc-major macro
  // block object type in the inverted index macro writer, which is driven by the
  // static desc's is_inc_major_ flag; keep it consistent with prepare_callback.
  const bool is_inc_major = nullptr != param.ddl_dag_ && param.ddl_dag_->is_inc_major_log();
  int64_t macro_block_op_id = 0;
  if (OB_ISNULL(storage_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage schema is null", K(ret), K(param));
  } else if (OB_FAIL(param.get_macro_block_op_id(is_inc_major, macro_block_op_id))) {
    LOG_WARN("failed to get macro block op id", KR(ret), K(is_inc_major), K(param));
  } else if (OB_FAIL(data_desc_.init(true/*is_ddl*/,
                                     *storage_schema,
                                     param.ls_id_,
                                     tablet_id_,
                                     compaction::ObMergeType::MAJOR_MERGE,
                                     param.snapshot_version_,
                                     param.tenant_data_version_,
                                     is_disk_bbq_writer_
                                         ? false /* ordinary row-store IVF data micro blocks */
                                         : tablet_param.is_micro_index_clustered_,
                                     tablet_param.tablet_transfer_seq_,
                                     0/*concurrent_cnt*/,
                                     tablet_param.reorganization_scn_,
                                     SCN::min_scn(),
                                     nullptr/*cg_schema*/,
                                     cg_idx_,
                                     exec_mode,
                                     true/*need_submit_io*/,
                                     is_inc_major,
                                     ObStorageObjectWriteStrategy::INVALID_WRITE_STRATEGY,
                                     macro_block_op_id))) {
    LOG_WARN("fail to init data store desc", K(ret), K(param));
  } else if (FALSE_IT(data_desc_.get_static_desc().schema_version_ = param.schema_version_)) {
  } else if (OB_ISNULL(index_builder_ = OB_NEWx(ObSSTableIndexBuilder, &allocator_, true/*use_buffer*/))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc sstable index builder", K(ret));
  } else if (OB_FAIL(index_builder_->init(data_desc_.get_desc(), ObSSTableIndexBuilder::DISABLE))) {
    LOG_WARN("fail to init sstable index builder", K(ret));
  } else {
    // inverted-index data macro block stores SSTableData typed micro blocks.
    data_desc_.get_desc().data_store_type_ = ObMacroBlockCommonHeader::SSTableData;
    data_desc_.get_desc().sstable_index_builder_ = index_builder_;
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::prepare_callback(const ObWriteMacroParam &param)
{
  int ret = OB_SUCCESS;
  const ObWriteTabletParam &tablet_param = param.tablet_param_;
  blocksstable::ObMacroMetaTempStore *macro_meta_store = nullptr;
  ObDDLWriteStat *ddl_write_stat = nullptr;
  ObITable::TableKey table_key;
  const bool is_inc_major = nullptr != param.ddl_dag_ && param.ddl_dag_->is_inc_major_log();
  const transaction::ObTxSEQ seq_no = transaction::ObTxSEQ::cast_from_int(param.tx_info_.seq_no_);
  table_key.tablet_id_ = tablet_id_;
  table_key.column_group_idx_ = cg_idx_;
  table_key.version_range_.snapshot_version_ = param.snapshot_version_;
  if (is_inc_major) {
    table_key.table_type_ = is_disk_bbq_writer_ ? ObITable::INC_IVF_INDEX_SSTABLE
                                           : ObITable::INC_INV_INDEX_SSTABLE;
    // version_range_ and scn_range_ share storage; keep end_scn/snapshot as param.snapshot_version_.
    table_key.scn_range_.start_scn_ = param.start_scn_;
  } else {
    table_key.table_type_ = is_disk_bbq_writer_ ? ObITable::IVF_INDEX_SSTABLE
                                           : ObITable::INV_INDEX_SSTABLE;
  }
#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_SUCC(ret) && GCTX.is_shared_storage_mode()) {
    ObMacroMetaStoreManager *macro_meta_store_mgr =
        const_cast<ObMacroMetaStoreManager *>(param.macro_meta_store_mgr_);
    if (OB_ISNULL(macro_meta_store_mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("macro meta store manager is null in shared storage mode", K(ret), K(param));
    } else if (OB_FAIL(macro_meta_store_mgr->add_macro_meta_store(
                   tablet_id_, cg_idx_, parallel_idx_, macro_meta_store))) {
      LOG_WARN("fail to add macro meta store", K(ret), K_(cg_idx), K_(parallel_idx));
    }
  }
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObDDLUtil::get_ddl_write_stat(
      param, ObDirectLoadTabletTarget::DATA, table_key, ddl_write_stat))) {
    LOG_WARN("fail to get ddl write stat", K(ret), K(table_key), K(param));
  } else {
    ObDDLRedoLogWriterCallbackInitParam init_param;
    init_param.ls_id_ = param.ls_id_;
    init_param.tablet_id_ = tablet_id_;
    init_param.direct_load_type_ = param.direct_load_type_;
    init_param.block_type_ = param.is_no_logging_ ? DDL_MB_SS_EMPTY_DATA_TYPE : DDL_MB_DATA_TYPE;
    init_param.table_key_ = table_key;
    init_param.async_index_snapshot_version_ = param.snapshot_version_;
    init_param.start_scn_ = param.start_scn_;
    init_param.task_id_ = param.task_id_;
    init_param.data_format_version_ = param.tenant_data_version_;
    init_param.parallel_cnt_ = param.get_logic_parallel_count();
    init_param.cg_cnt_ = tablet_param.storage_schema_->get_column_group_count();
    init_param.row_id_offset_ = param.row_offset_;
    init_param.need_delay_ = false;
    init_param.with_cs_replica_ = tablet_param.with_cs_replica_;
    init_param.need_submit_io_ = true;
    init_param.macro_meta_store_ = macro_meta_store;
    init_param.write_stat_ = ddl_write_stat;
    init_param.is_inc_major_log_ = param.ddl_dag_ ? param.ddl_dag_->is_inc_major_log() : false;
    if (is_inc_major) {
      ObDDLIncRedoLogWriterCallback *inc_redo_callback = nullptr;
      init_param.tx_desc_ = param.tx_info_.tx_desc_;
      init_param.trans_id_ = param.tx_info_.trans_id_;
      init_param.seq_no_ = seq_no;
      if (OB_ISNULL(inc_redo_callback = OB_NEWx(ObDDLIncRedoLogWriterCallback, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ddl inc redo callback", K(ret));
      } else if (FALSE_IT(ddl_redo_callback_ = inc_redo_callback)) {
      } else if (FALSE_IT(is_inc_redo_callback_ = true)) {
      } else if (OB_FAIL(inc_redo_callback->init(init_param))) {
        LOG_WARN("fail to init ddl inc redo callback", K(ret), K(init_param));
      }
    } else {
      ObDDLRedoLogWriterCallback *redo_callback = nullptr;
      if (OB_ISNULL(redo_callback = OB_NEWx(ObDDLRedoLogWriterCallback, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to new ddl redo callback", K(ret));
      } else if (FALSE_IT(ddl_redo_callback_ = redo_callback)) {
      } else if (FALSE_IT(is_inc_redo_callback_ = false)) {
      } else if (OB_FAIL(redo_callback->init(init_param))) {
        LOG_WARN("fail to init ddl redo callback", K(ret), K(init_param));
      }
    }
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::init(const ObWriteMacroParam &param)
{
  int ret = OB_SUCCESS;
  ObMacroDataSeq start_seq;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid write macro param", K(ret), K(param));
  } else if (FALSE_IT(tablet_id_ = param.tablet_id_)) {
  } else if (FALSE_IT(cg_idx_ = param.cg_idx_ >= 0 ? param.cg_idx_ : 0)) {
  } else if (FALSE_IT(parallel_idx_ = param.slice_idx_)) {
  } else if (OB_FAIL(ObDDLUtil::init_macro_block_seq(param.direct_load_type_,
                                                     param.tablet_id_,
                                                     param.slice_idx_,
                                                     start_seq))) {
    LOG_WARN("fail to init macro block seq", K(ret), K(param));
  } else if (FALSE_IT(is_disk_bbq_writer_ = is_async_ivf_index(
                 param.ddl_table_schema_.table_item_.index_type_))) {
  } else if (is_disk_bbq_writer_
             && OB_FAIL(build_ivf_row_desc(param, ivf_row_desc_))) {
    LOG_WARN("fail to build IVF SSTable row descriptor", K(ret), K(param));
  } else if (!is_disk_bbq_writer_
             && OB_FAIL(build_inv_idx_row_desc(param, inv_idx_row_desc_))) {
    LOG_WARN("fail to build inv idx row desc", K(ret), K(param));
  } else if (OB_FAIL(prepare_data_store_desc(param, inv_idx_row_desc_))) {
    LOG_WARN("fail to prepare data store desc", K(ret), K(param));
  } else if (OB_FAIL(prepare_callback(param))) {
    LOG_WARN("fail to prepare callback", K(ret), K(param));
  } else if (OB_FAIL(ObISSTableObjectCleaner::get_cleaner_from_data_store_desc(
                 data_desc_.get_desc(), object_cleaner_))) {
    LOG_WARN("fail to get cleaner from data store desc", K(ret));
  } else if (OB_ISNULL(object_cleaner_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("object cleaner is null", K(ret));
  } else {
    ObMacroSeqParam macro_seq_param;
    ObPreWarmerParam pre_warm_param;
    macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    macro_seq_param.start_ = start_seq.macro_data_seq_;
    if (OB_FAIL(pre_warm_param.init(param.ls_id_, tablet_id_))) {
      LOG_WARN("fail to init pre warm param", K(ret), K(param));
    } else if (is_disk_bbq_writer_
               && OB_FAIL(ivf_writer_.open(ivf_row_desc_, data_desc_.get_desc(), parallel_idx_,
                                            macro_seq_param, pre_warm_param,
                                            *object_cleaner_, ddl_redo_callback_))) {
      LOG_WARN("fail to open IVF macro block writer", K(ret), K(param));
    } else if (!is_disk_bbq_writer_
               && OB_FAIL(inv_writer_.open(inv_idx_row_desc_, data_desc_.get_desc(),
                                            parallel_idx_, macro_seq_param,
                                            pre_warm_param, *object_cleaner_,
                                            ddl_redo_callback_,
                                            !GCTX.is_shared_storage_mode()))) {
      LOG_WARN("fail to open inverted index macro block writer", K(ret), K(param));
    } else {
      is_inited_ = true;
      LOG_INFO("[ASYNC INDEX] open async index macro block writer",
               K(ret), K_(tablet_id), K_(cg_idx), K_(parallel_idx),
               K_(is_disk_bbq_writer), K_(inv_idx_row_desc));
    }
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::append_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_disk_bbq_writer_ && OB_FAIL(ivf_writer_.append_row(row))) {
    LOG_WARN("append IVF row failed", K(ret));
  } else if (!is_disk_bbq_writer_ && OB_FAIL(append_one_row(row))) {
    LOG_WARN("append row failed", K(ret));
  } else {
    if (is_disk_bbq_writer_) {
      row_count_ = ivf_writer_.get_appended_row_count();
    } else {
      ++row_count_;
    }
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::append_batch(const ObBatchDatumRows &batch_rows)
{
  int ret = OB_SUCCESS;
  ObDatumRow row;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_disk_bbq_writer_) {
    if (OB_FAIL(ivf_writer_.append_batch(batch_rows))) {
      LOG_WARN("append IVF batch failed", K(ret), K(batch_rows));
    }
    // append_batch() may fail after a prefix has already been written.
    row_count_ = ivf_writer_.get_appended_row_count();
  } else if (inv_idx_row_desc_.nested_key_col_idx_ < 0) {
    if (OB_FAIL(inv_writer_.append_batch(batch_rows))) {
      LOG_WARN("append batch failed", K(ret), K(batch_rows));
    } else {
      row_count_ += batch_rows.row_count_;
    }
  } else if (OB_UNLIKELY(batch_rows.row_count_ < 0
      || (batch_rows.row_count_ > 0 && batch_rows.get_column_count() <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nested search index batch", K(ret), K(batch_rows));
  } else if (0 == batch_rows.row_count_) {
  } else if (OB_FAIL(row.init(batch_rows.get_column_count()))) {
    LOG_WARN("fail to init nested search index batch row", K(ret), K(batch_rows));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_rows.row_count_; ++i) {
      if (OB_FAIL(batch_rows.to_datum_row(i, row))) {
        LOG_WARN("fail to convert nested search index batch row", K(ret), K(i), K(batch_rows));
      } else if (OB_FAIL(append_one_row(row))) {
        LOG_WARN("fail to append nested search index batch row", K(ret), K(i), K(row));
      } else {
        ++row_count_;
      }
    }
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::append_one_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (inv_idx_row_desc_.nested_key_col_idx_ >= 0) {
    if (OB_FAIL(append_nested_search_row(row))) {
      LOG_WARN("fail to fold nested search index row", K(ret), K(row));
    }
  } else if (OB_FAIL(inv_writer_.append_row(row))) {
    LOG_WARN("fail to append inverted index row", K(ret), K(row));
  }
  return ret;
}

bool ObAsyncIndexMacroWriterAdapter::is_same_posting(const ObDatumRow &row) const
{
  bool bool_ret = has_pending_nested_group_
      && row.get_column_count() > inv_idx_row_desc_.doc_id_col_idx_
      && pending_nested_row_.get_column_count() > inv_idx_row_desc_.doc_id_col_idx_;
  for (int64_t i = 0; bool_ret && i <= inv_idx_row_desc_.doc_id_col_idx_; ++i) {
    bool_ret = common::ObDatum::binary_equal(
        pending_nested_row_.storage_datums_[i], row.storage_datums_[i]);
  }
  return bool_ret;
}

int ObAsyncIndexMacroWriterAdapter::append_nested_key(const ObStorageDatum &nested_key)
{
  int ret = OB_SUCCESS;
  if (nested_key.is_null()) {
  } else if (!nested_key_list_writer_.is_inited()
      && OB_FAIL(nested_key_list_writer_.init(
          nested_key_list_buffer_, common::OB_MAX_VARCHAR_LENGTH))) {
    LOG_WARN("fail to initialize nested key list codec", K(ret));
  } else if (OB_FAIL(nested_key_list_writer_.append(nested_key))) {
    LOG_WARN("fail to append nested key through list codec", K(ret), K(nested_key));
  }
  return ret;
}

void ObAsyncIndexMacroWriterAdapter::reuse_nested_group()
{
  nested_key_list_writer_.reset();
  pending_nested_row_.reuse();
  nested_key_list_buffer_.reset();
  nested_group_allocator_.reset();
  nested_key_list_buffer_.set_allocator(&nested_group_allocator_);
  has_pending_nested_group_ = false;
}

int ObAsyncIndexMacroWriterAdapter::start_nested_group(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  const int64_t nested_key_storage_idx = inv_idx_row_desc_.nested_key_col_idx_;
  const int64_t storage_col_cnt = row.get_column_count();
  reuse_nested_group();
  if (OB_UNLIKELY(!row.is_valid()
      || nested_key_storage_idx < 0
      || nested_key_storage_idx >= storage_col_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid nested search index row", K(ret), K(row), K_(inv_idx_row_desc));
  } else if (!pending_nested_row_.is_valid()
      && OB_FAIL(pending_nested_row_.init(allocator_, storage_col_cnt))) {
    LOG_WARN("fail to init pending nested search index row", K(ret), K(storage_col_cnt));
  } else if (OB_UNLIKELY(pending_nested_row_.get_capacity() < storage_col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pending nested row capacity is insufficient", K(ret),
        K(pending_nested_row_.get_capacity()), K(storage_col_cnt));
  } else if (OB_FAIL(pending_nested_row_.copy_attributes_except_datums(row))) {
    LOG_WARN("fail to copy pending nested search index row attributes", K(ret), K(row));
  } else if (FALSE_IT(pending_nested_row_.count_ = storage_col_cnt)) {
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_col_cnt; ++i) {
      if (OB_FAIL(pending_nested_row_.storage_datums_[i].deep_copy(
          row.storage_datums_[i], nested_group_allocator_))) {
        LOG_WARN("fail to copy nested search index storage datum", K(ret), K(i), K(row));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(append_nested_key(
      row.storage_datums_[nested_key_storage_idx]))) {
    LOG_WARN("fail to append first nested key", K(ret), K(row));
  } else {
    has_pending_nested_group_ = true;
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::flush_nested_group()
{
  int ret = OB_SUCCESS;
  ObString nested_key_list;
  if (!has_pending_nested_group_) {
  } else if (OB_UNLIKELY(inv_idx_row_desc_.nested_key_col_idx_ < 0
      || inv_idx_row_desc_.nested_key_col_idx_ >= pending_nested_row_.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pending nested search index group", K(ret), K_(inv_idx_row_desc));
  } else {
    ObStorageDatum &nested_key_list_datum =
        pending_nested_row_.storage_datums_[inv_idx_row_desc_.nested_key_col_idx_];
    if (nested_key_list_datum.is_null()) {
      if (OB_UNLIKELY(nested_key_list_writer_.is_inited()
          || 0 != nested_key_list_writer_.get_count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected encoded keys for null nested key list", K(ret),
            "nested_key_count", nested_key_list_writer_.get_count());
      }
    } else if (OB_FAIL(nested_key_list_writer_.build(nested_key_list))) {
      LOG_WARN("fail to build nested key list through codec", K(ret),
          "nested_key_count", nested_key_list_writer_.get_count());
    } else {
      nested_key_list_datum.set_string(nested_key_list);
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(inv_writer_.append_row(pending_nested_row_))) {
      LOG_WARN("fail to append folded nested search index posting", K(ret),
          "nested_key_count", nested_key_list_writer_.get_count(), K_(pending_nested_row));
    } else {
      reuse_nested_group();
    }
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::append_nested_search_row(const ObDatumRow &row)
{
  int ret = OB_SUCCESS;
  if (!has_pending_nested_group_) {
    if (OB_FAIL(start_nested_group(row))) {
      LOG_WARN("fail to start nested search index posting group", K(ret), K(row));
    }
  } else if (is_same_posting(row)) {
    const int64_t nested_key_col_idx = inv_idx_row_desc_.nested_key_col_idx_;
    if (OB_UNLIKELY(nested_key_col_idx < 0
        || nested_key_col_idx >= row.get_column_count()
        || nested_key_col_idx >= pending_nested_row_.get_column_count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("nested key column is outside input row", K(ret), K(row), K_(inv_idx_row_desc));
    } else if (OB_UNLIKELY(
        pending_nested_row_.storage_datums_[nested_key_col_idx].is_null()
        != row.storage_datums_[nested_key_col_idx].is_null())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null and non-null nested keys share one posting", K(ret),
          K(nested_key_col_idx), K(row), K_(pending_nested_row));
    } else if (OB_FAIL(append_nested_key(
        row.storage_datums_[nested_key_col_idx]))) {
      LOG_WARN("fail to append nested key to posting group", K(ret), K(row));
    }
  } else if (OB_FAIL(flush_nested_group())) {
    LOG_WARN("fail to flush previous nested search index posting group", K(ret));
  } else if (OB_FAIL(start_nested_group(row))) {
    LOG_WARN("fail to start next nested search index posting group", K(ret), K(row));
  }
  return ret;
}

int ObAsyncIndexMacroWriterAdapter::close()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_disk_bbq_writer_ && OB_FAIL(ivf_writer_.close())) {
    LOG_WARN("close IVF macro block writer failed", K(ret));
  } else if (!is_disk_bbq_writer_ && OB_FAIL(flush_nested_group())) {
    LOG_WARN("flush pending nested search index posting failed", K(ret));
  } else if (!is_disk_bbq_writer_ && OB_FAIL(inv_writer_.close())) {
    LOG_WARN("close inverted index macro block writer failed", K(ret));
  } else if (0 == row_count_) {
    LOG_INFO("[ASYNC INDEX] close empty async index slice", K_(tablet_id), K_(parallel_idx));
  } else {
    LOG_INFO("[ASYNC INDEX] close async index macro writer",
             K(ret), K_(tablet_id), K_(cg_idx), K_(parallel_idx),
             K_(row_count));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
