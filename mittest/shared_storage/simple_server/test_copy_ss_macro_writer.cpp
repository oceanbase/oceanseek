/**
 * Copyright (c) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGETEST

#include <algorithm>
#include <gtest/gtest.h>
#ifdef ERRSIM
#include "lib/utility/ob_tracepoint.h"
#endif
#define protected public
#define private public
#include "mittest/shared_storage/simple_server/test_storage_cache_common_util.h"
#include "sensitive_test/object_storage/object_storage_authorization_info.h"
#include "storage/incremental/ob_copy_ss_macro_writer.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/shared_storage/ob_remote_io_adapter.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
}

// Required by the shared simple-server fixture.
const char *get_per_file_test_name()
{
  return "test_copy_ss_macro_writer_";
}

namespace oceanbase
{
namespace unittest
{
using namespace blocksstable;
using namespace common;
using namespace compaction;
using namespace share;
using namespace storage;

class TestCopySSMacroBlockWriter : public ObStorageCachePolicyPrewarmerTest
{
protected:
  struct Resources
  {
    Resources()
      : allocator_("CopySSSeqTest"), schema_(nullptr), source_desc_(nullptr),
        source_builder_(nullptr), source_writer_(nullptr), source_handle_(), copy_writer_(nullptr)
    {}
    ~Resources()
    {
      // Destroy writers and table handles before their arenas and the tenant guard.
      if (OB_NOT_NULL(copy_writer_)) {
        copy_writer_->~CopySSMacroBlockWriter();
      }
      source_handle_.reset();
      if (OB_NOT_NULL(source_writer_)) {
        source_writer_->~ObMacroBlockWriter();
      }
      if (OB_NOT_NULL(source_builder_)) {
        source_builder_->~ObSSTableIndexBuilder();
      }
      if (OB_NOT_NULL(source_desc_)) {
        source_desc_->~ObWholeDataStoreDesc();
      }
      if (OB_NOT_NULL(schema_)) {
        schema_->~ObStorageSchema();
      }
    }
    ObArenaAllocator allocator_;
    ObStorageSchema *schema_;
    ObWholeDataStoreDesc *source_desc_;
    ObSSTableIndexBuilder *source_builder_;
    ObMacroBlockWriter *source_writer_;
    ObTableHandleV2 source_handle_;
    CopySSMacroBlockWriter *copy_writer_;
  private:
    DISALLOW_COPY_AND_ASSIGN(Resources);
  };

#ifdef ERRSIM
  class TailWriteErrorGuard
  {
  public:
    TailWriteErrorGuard() : event_(nullptr), saved_event_() {}
    ~TailWriteErrorGuard() { reset(); }

    int init()
    {
      int ret = OB_SUCCESS;
      if (OB_NOT_NULL(event_)) {
        ret = OB_INIT_TWICE;
      } else {
        DLIST_FOREACH_NORET(item, EventTable::global_item_list()) {
          if (OB_NOT_NULL(item->item_.name_)
              && 0 == STRCMP(item->item_.name_, "EN_MACRO_BLOCK_CLOSE_WRITE_FAILED")) {
            event_ = &item->item_;
          }
        }
        if (OB_ISNULL(event_)) {
          ret = OB_ENTRY_NOT_EXIST;
        } else {
          EventItem error;
          saved_event_ = *event_;
          error.error_code_ = OB_IO_ERROR;
          error.occur_ = 1;
          error.cond_ = GETTID();
          set_event_(error);
        }
      }
      return ret;
    }

    int64_t get_trigger_count() const
    {
      return OB_ISNULL(event_) ? 0 : 1 - ATOMIC_LOAD(&event_->occur_);
    }

    void reset()
    {
      if (OB_NOT_NULL(event_)) {
        set_event_(saved_event_);
        event_ = nullptr;
      }
    }

  private:
    void set_event_(const EventItem &error)
    {
      // Stop other threads at the write fault until all settings are ready.
      ATOMIC_STORE(&event_->cond_, 0);
      ATOMIC_STORE(&event_->occur_, error.occur_);
      ATOMIC_STORE(&event_->trigger_freq_, error.trigger_freq_);
      ATOMIC_STORE(&event_->error_code_, error.error_code_);
      ATOMIC_STORE(&event_->cond_, error.cond_);
    }

    EventItem *event_;
    EventItem saved_event_;
    DISALLOW_COPY_AND_ASSIGN(TailWriteErrorGuard);
  };
#endif

  enum { DATA_BLOCK_COUNT = 40, ROWKEY_LENGTH = 2048 };

  void prepare_tablet(const char *table_name, ObLSHandle &ls_handle, ObTabletHandle &tablet_handle)
  {
    ObLSService *ls_service = MTL(ObLSService *);
    ObSqlString sql;
    ASSERT_NE(nullptr, table_name);
    ASSERT_NE(nullptr, ls_service);
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("create table %s "
        "(k varbinary(2048) primary key, v bigint) compression='none'", table_name));
    ASSERT_EQ(OB_SUCCESS, exe_sql(sql.ptr()));
    set_ls_and_tablet_id_for_run_ctx(table_name);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(OB_SUCCESS, ls_service->get_ls(run_ctx_.ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD));
    ASSERT_NE(nullptr, ls_handle.get_ls());
    ASSERT_EQ(OB_SUCCESS, ls_handle.get_ls()->get_tablet(run_ctx_.tablet_id_, tablet_handle));
    ASSERT_NE(nullptr, tablet_handle.get_obj());
    ASSERT_TRUE(tablet_handle.get_obj()->get_tablet_meta().micro_index_clustered_);
  }

  void prepare_source(ObTablet &tablet, Resources &resources, const SCN &end_scn)
  {
    int32_t transfer_epoch = 0;
    ObMacroSeqParam seq_param;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ASSERT_TRUE(end_scn.is_valid_and_not_min());
    ASSERT_EQ(OB_SUCCESS, tablet.load_storage_schema(resources.allocator_, resources.schema_));
    ASSERT_NE(nullptr, resources.schema_);
    // Keep both source and destination index rows large enough to spill the 32 KiB SS root.
    resources.schema_->row_store_type_ = FLAT_ROW_STORE;
    resources.schema_->set_minor_row_store_type(FLAT_ROW_STORE);
    resources.schema_->compressor_type_ = ObCompressorType::NONE_COMPRESSOR;
    ASSERT_EQ(OB_SUCCESS, tablet.get_private_transfer_epoch(transfer_epoch));
    resources.source_desc_ = OB_NEWx(ObWholeDataStoreDesc, &resources.allocator_);
    resources.source_builder_ = OB_NEWx(ObSSTableIndexBuilder, &resources.allocator_, false);
    resources.source_writer_ = OB_NEWx(ObMacroBlockWriter, &resources.allocator_, false);
    ASSERT_NE(nullptr, resources.source_desc_);
    ASSERT_NE(nullptr, resources.source_builder_);
    ASSERT_NE(nullptr, resources.source_writer_);
    ASSERT_EQ(OB_SUCCESS, resources.source_desc_->init(false, *resources.schema_,
        run_ctx_.ls_id_, tablet.get_tablet_id(), MINI_MERGE, end_scn.get_val_for_tx(),
        DATA_CURRENT_VERSION, false /* source indexes remain in data blocks */, transfer_epoch,
        0, tablet.get_reorganization_scn(), end_scn));
    ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.create_tablet_data_tablet_id_private_macro_third_id_dir(
        MTL_ID(), MTL_EPOCH_ID(), tablet.get_tablet_id().id(),
        tablet.get_reorganization_scn().get_val_for_tx()));
    ObDataStoreDesc &desc = resources.source_desc_->get_desc();
    ASSERT_EQ(OB_SUCCESS, resources.source_builder_->init(
        desc, ObSSTableIndexBuilder::DISABLE));
    ObISSTableObjectCleaner *cleaner = resources.source_builder_->get_object_cleaner();
    ASSERT_NE(nullptr, cleaner);
    desc.sstable_index_builder_ = resources.source_builder_;
    seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    seq_param.start_ = 0;
    seq_param.interval_ = 1;
    seq_param.step_ = 1;
    ASSERT_EQ(OB_SUCCESS, resources.source_writer_->open(desc, 0, seq_param, pre_warm_param,
        *cleaner, nullptr));
  }

  void write_source(Resources &resources, const SCN &end_scn)
  {
    ObDatumRow row;
    ObSSTableMergeRes res;
    char key[ROWKEY_LENGTH];
    ASSERT_NE(nullptr, resources.source_writer_);
    ASSERT_NE(nullptr, resources.source_builder_);
    ASSERT_NE(nullptr, resources.schema_);
    ASSERT_EQ(OB_SUCCESS, row.init(4));
    row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    row.mvcc_row_flag_.set_first_multi_version_row(true);
    row.mvcc_row_flag_.set_last_multi_version_row(true);
    row.storage_datums_[1].set_int(-end_scn.get_val_for_tx());
    row.storage_datums_[2].set_int(0);
    // One long, ordered key per real data macro block bounds the test at 80 MiB of source I/O.
    for (int64_t i = 0; i < DATA_BLOCK_COUNT; ++i) {
      MEMSET(key, 'k', sizeof(key));
      ASSERT_EQ(8, snprintf(key, 9, "%08ld", i));
      key[8] = 'k';
      row.storage_datums_[0].set_string(ObString(sizeof(key), key));
      row.storage_datums_[3].set_int(i);
      ASSERT_EQ(OB_SUCCESS, resources.source_writer_->append_row(row));
      ASSERT_EQ(OB_SUCCESS, resources.source_writer_->build_micro_block());
      ASSERT_EQ(OB_SUCCESS, resources.source_writer_->try_switch_macro_block());
    }
    ASSERT_EQ(OB_SUCCESS, resources.source_writer_->close());
    ASSERT_EQ(OB_SUCCESS, resources.source_builder_->close(res));
    ASSERT_EQ(DATA_BLOCK_COUNT, res.data_block_ids_.count());
    create_source_sstable(resources, res, end_scn);
    ASSERT_FALSE(HasFatalFailure());
  }

  void create_source_sstable(Resources &resources, const ObSSTableMergeRes &res, const SCN &end_scn)
  {
    ObTabletCreateSSTableParam param;
    ASSERT_NE(nullptr, resources.schema_);
    // Reuse schema defaults, then replace all empty-table fields with the real merge result.
    ASSERT_EQ(OB_SUCCESS, param.init_for_empty_major_sstable(run_ctx_.tablet_id_,
        *resources.schema_, end_scn.get_val_for_tx(), -1, false, false));
    param.table_key_.table_type_ = ObITable::MINI_SSTABLE;
    param.table_key_.scn_range_.start_scn_ = SCN::base_scn();
    param.table_key_.scn_range_.end_scn_ = end_scn;
    param.max_merged_trans_version_ = res.max_merged_trans_version_;
    param.min_merged_trans_version_ = res.min_merged_trans_version_;
    param.filled_tx_scn_ = end_scn;
    param.rec_scn_ = end_scn;
    param.nested_offset_ = res.nested_offset_;
    param.nested_size_ = res.nested_size_;
    param.column_cnt_ = res.data_column_cnt_;
    ASSERT_EQ(OB_SUCCESS, param.column_checksums_.assign(res.data_column_checksums_));
    ASSERT_EQ(OB_SUCCESS, param.inner_init_with_merge_res(res));
    ASSERT_TRUE(param.is_valid());
    // This private SSTable is never installed into the tablet, so background upload cannot see it.
    ASSERT_EQ(OB_SUCCESS, ObTabletCreateDeleteHelper::create_sstable(
        param, resources.allocator_, resources.source_handle_));
  }

  void copy_blocks(CopySSMacroBlockWriter &writer, const ObIArray<MacroBlockId> &ids,
      const int64_t begin, const int64_t end)
  {
    ObDataMacroBlockMeta meta;
    CopySSMacroBlockWriter::IOCtx *head = writer.busy_io_ctx_list_.get_header();
    CopySSMacroBlockWriter::IOCtx *ctx = nullptr;
    ASSERT_GE(begin, 0);
    ASSERT_GT(end, begin);
    ASSERT_LE(end, ids.count());
    for (int64_t i = begin; i < end; ++i) {
      // process() reads the real source block by ID and rebuilds its metadata from the bytes.
      meta.val_.macro_id_ = ids.at(i);
      ASSERT_EQ(OB_SUCCESS, writer.process(meta));
    }
    // Keep contexts in the busy list so process() resets them before reuse.
    ctx = head->get_next();
    while (ctx != head) {
      ASSERT_EQ(OB_SUCCESS, ctx->wait());
      ctx = ctx->get_next();
    }
  }

  void prepare_copy_writer(ObTablet &tablet, Resources &resources, const int64_t op_id)
  {
    ObSSTable *source = nullptr;
    ASSERT_EQ(nullptr, resources.copy_writer_);
    ASSERT_NE(nullptr, resources.schema_);
    ASSERT_EQ(OB_SUCCESS, resources.source_handle_.get_sstable(source));
    ASSERT_NE(nullptr, source);
    resources.copy_writer_ = OB_NEWx(CopySSMacroBlockWriter, &resources.allocator_, run_ctx_.ls_id_,
        source->get_key(), tablet, *source, *resources.schema_, 1, op_id, DATA_CURRENT_VERSION);
    ASSERT_NE(nullptr, resources.copy_writer_);
    ASSERT_EQ(OB_SUCCESS, resources.copy_writer_->init());
    ObSSTableIndexBuilder &builder = resources.copy_writer_->sstable_index_builder_;
    ASSERT_FALSE(builder.enable_dump_disk());
    ASSERT_FALSE(ObStoreFormat::is_row_store_type_with_encoding(
        builder.index_store_desc_.get_desc().get_row_store_type()));
    ASSERT_EQ(ObCompressorType::NONE_COMPRESSOR,
        builder.index_store_desc_.get_desc().get_compressor_type());
  }

  void prepare_copy_tail(CopySSMacroBlockWriter &writer, const ObIArray<MacroBlockId> &data_ids)
  {
    ASSERT_NE(nullptr, writer.index_block_rebuilder_.clustered_index_writer_);
    ObMacroBlockWriter *clustered_writer =
        writer.index_block_rebuilder_.clustered_index_writer_->macro_writer_;
    ASSERT_NE(nullptr, clustered_writer);
    copy_blocks(writer, data_ids, 0, DATA_BLOCK_COUNT / 2);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_TRUE(clustered_writer->is_dirty());
    // Write an intermediate block, leaving the real close to write the final block.
    const int64_t flushed_index = clustered_writer->current_index_;
    ASSERT_EQ(OB_SUCCESS, clustered_writer->try_switch_macro_block());
    ASSERT_EQ(OB_SUCCESS, clustered_writer->wait_io_finish(
        clustered_writer->macro_handles_[flushed_index],
        &clustered_writer->macro_blocks_[flushed_index]));
    const ObIArray<MacroBlockId> &clustered_ids =
        clustered_writer->get_macro_block_write_ctx().get_macro_block_list();
    ASSERT_GT(clustered_ids.count(), 0);
    check_remote_objects(clustered_ids);
    ASSERT_FALSE(HasFatalFailure());
    const int64_t flushed_count = clustered_ids.count();
    copy_blocks(writer, data_ids, DATA_BLOCK_COUNT / 2, DATA_BLOCK_COUNT);
    ASSERT_FALSE(HasFatalFailure());
    ASSERT_EQ(OB_SUCCESS, writer.wait());
    ASSERT_EQ(flushed_count, clustered_ids.count());
    ASSERT_TRUE(clustered_writer->macro_blocks_[clustered_writer->current_index_].is_dirty());
  }

  void check_remote_objects(const ObIArray<MacroBlockId> &ids)
  {
    ASSERT_GT(ids.count(), 0);
    for (int64_t i = 0; i < ids.count(); ++i) {
      bool exists = false;
      ASSERT_EQ(OB_SUCCESS, ObRemoteIoAdapter::is_exist(
          MTL_ID(), ids.at(i), GCONF.observer_id, exists));
      ASSERT_TRUE(exists) << "missing META object at index " << i;
    }
  }

  void check_result(CopySSMacroBlockWriter &writer, ObTablet &tablet,
      const int64_t expected_clustered_block_count, const int64_t op_id)
  {
    int ret = OB_SUCCESS;
    ObTableHandleV2 result_handle = writer.get_result_sstable_handle();
    ObSSTable *result = nullptr;
    ObSSTableMetaHandle meta_handle;
    ObMacroIdIterator iter;
    ObSEArray<MacroBlockId, 8> ids;
    MacroBlockId id;
    MacroBlockId expected_namespace;
    ObStorageObjectOpt opt;
    ObIndexTreeRootCtx *root_ctx = writer.index_block_rebuilder_.index_tree_root_ctx_;
    ASSERT_GT(expected_clustered_block_count, 0);
    ASSERT_NE(nullptr, root_ctx);
    ASSERT_NE(nullptr, root_ctx->clustered_index_write_ctx_);
    // close() moves clustered block IDs into the root context.
    const ObIArray<MacroBlockId> &clustered_ids =
        root_ctx->clustered_index_write_ctx_->get_macro_block_list();
    ASSERT_EQ(expected_clustered_block_count, clustered_ids.count());
    ASSERT_EQ(OB_SUCCESS, result_handle.get_sstable(result));
    ASSERT_NE(nullptr, result);
    ASSERT_EQ(OB_SUCCESS, result->get_meta(meta_handle));
    ASSERT_TRUE(meta_handle.is_valid());
    ASSERT_EQ(OB_SUCCESS,
        meta_handle.get_sstable_meta().get_macro_info().get_other_block_iter(iter));
    while (OB_SUCC(iter.get_next_macro_id(id))) {
      ASSERT_EQ(OB_SUCCESS, ids.push_back(id));
    }
    ASSERT_EQ(OB_ITER_END, ret);
    // The result includes clustered blocks and the subsequent public index/meta tree blocks.
    ASSERT_GT(ids.count(), clustered_ids.count());
    ASSERT_EQ(writer.sstable_index_builder_.res_.other_block_ids_.count(), ids.count());
    opt.set_ss_share_object_opt(ObStorageObjectType::SHARED_MINI_META_MACRO,
        tablet.get_tablet_id().is_ls_inner_tablet(), run_ctx_.ls_id_.id(),
        tablet.get_tablet_id().id(), op_id, 0, 0, tablet.get_reorganization_scn().get_val_for_tx());
    ASSERT_EQ(OB_SUCCESS, ObObjectManager::ss_get_object_id(opt, expected_namespace));
    for (int64_t i = 0; i < ids.count(); ++i) {
      id = ids.at(i);
      id.set_seq_id(0);
      ASSERT_EQ(expected_namespace, id) << "META objects must share all non-sequence ID fields";
    }
    std::sort(ids.begin(), ids.end());
    for (int64_t i = 0; i < ids.count(); ++i) {
      // Starting at zero and matching every ordinal rejects both holes and duplicate IDs.
      ASSERT_EQ(i, ids.at(i).seq_id());
      if (i < clustered_ids.count()) {
        ASSERT_EQ(clustered_ids.at(i), ids.at(i));
      }
    }
    ASSERT_EQ(clustered_ids.at(clustered_ids.count() - 1).seq_id() + 1,
        ids.at(clustered_ids.count()).seq_id());
    check_remote_objects(ids);
    ASSERT_FALSE(HasFatalFailure());
  }
};

TEST_F(TestCopySSMacroBlockWriter, clustered_close_hands_off_next_meta_sequence)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  Resources resources;
  SCN end_scn;
  const int64_t op_id = ObTimeUtility::current_time_ns();
  ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_tx(op_id));
  prepare_tablet("copy_ss_seq", ls_handle, tablet_handle);
  ASSERT_FALSE(HasFatalFailure());
  ObTablet &tablet = *tablet_handle.get_obj();
  prepare_source(tablet, resources, end_scn);
  ASSERT_FALSE(HasFatalFailure());
  write_source(resources, end_scn);
  ASSERT_FALSE(HasFatalFailure());
  prepare_copy_writer(tablet, resources, op_id);
  ASSERT_FALSE(HasFatalFailure());
  CopySSMacroBlockWriter &writer = *resources.copy_writer_;
  prepare_copy_tail(writer, resources.source_builder_->res_.data_block_ids_);
  ASSERT_FALSE(HasFatalFailure());
  ObMacroBlockWriter *clustered_writer =
      writer.index_block_rebuilder_.clustered_index_writer_->macro_writer_;
  const int64_t flushed_count =
      clustered_writer->get_macro_block_write_ctx().get_macro_block_list().count();
  // Only the real CopySS close may write the tail and pass its next sequence to the public builder.
  ASSERT_EQ(OB_SUCCESS, writer.close(resources.allocator_));
  check_result(writer, tablet, flushed_count + 1, op_id);
}

#ifdef ERRSIM
TEST_F(TestCopySSMacroBlockWriter, clustered_tail_write_failure_blocks_final_build)
{
  ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(run_ctx_.tenant_id_));
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  Resources resources;
  SCN end_scn;
  const int64_t op_id = ObTimeUtility::current_time_ns();
  ASSERT_EQ(OB_SUCCESS, end_scn.convert_for_tx(op_id));
  prepare_tablet("copy_ss_seq_tail_fail", ls_handle, tablet_handle);
  ASSERT_FALSE(HasFatalFailure());
  ObTablet &tablet = *tablet_handle.get_obj();
  prepare_source(tablet, resources, end_scn);
  ASSERT_FALSE(HasFatalFailure());
  write_source(resources, end_scn);
  ASSERT_FALSE(HasFatalFailure());
  prepare_copy_writer(tablet, resources, op_id);
  ASSERT_FALSE(HasFatalFailure());
  {
    CopySSMacroBlockWriter &writer = *resources.copy_writer_;
    prepare_copy_tail(writer, resources.source_builder_->res_.data_block_ids_);
    ASSERT_FALSE(HasFatalFailure());
    ObMacroBlockWriter *clustered_writer =
        writer.index_block_rebuilder_.clustered_index_writer_->macro_writer_;
    const ObIArray<MacroBlockId> &clustered_ids =
        clustered_writer->get_macro_block_write_ctx().get_macro_block_list();
    const int64_t flushed_count = clustered_ids.count();
    const int64_t next_meta_seq = clustered_writer->get_last_macro_seq();
    ASSERT_GT(next_meta_seq, 0);
    ASSERT_EQ(0, writer.meta_macro_seq_);
    ASSERT_FALSE(writer.sstable_index_builder_.is_closed());
    ASSERT_FALSE(writer.get_result_sstable_handle().is_valid());
    TailWriteErrorGuard error_guard;
    ASSERT_EQ(OB_SUCCESS, error_guard.init());
    ASSERT_EQ(OB_IO_ERROR, writer.close(resources.allocator_));
    ASSERT_EQ(1, error_guard.get_trigger_count());
    ASSERT_EQ(flushed_count, clustered_ids.count());
    ASSERT_EQ(next_meta_seq, clustered_writer->get_last_macro_seq());
    // Failure must stop the sequence handoff and leave no final tree or SSTable result.
    ASSERT_EQ(0, writer.meta_macro_seq_);
    ASSERT_FALSE(writer.sstable_index_builder_.is_closed());
    ASSERT_EQ(0, writer.sstable_index_builder_.res_.other_block_ids_.count());
    ASSERT_FALSE(writer.get_result_sstable_handle().is_valid());
    ASSERT_EQ(OB_SUCCESS, writer.wait());
    ASSERT_TRUE(writer.busy_io_ctx_list_.is_empty());
    for (int64_t i = 0; i < ARRAYSIZEOF(clustered_writer->macro_handles_); ++i) {
      ASSERT_TRUE(clustered_writer->macro_handles_[i].is_finished());
    }
    error_guard.reset();
  }
  // Destroy the failed writer before replaying the same input under a new object namespace.
  resources.copy_writer_->~CopySSMacroBlockWriter();
  resources.copy_writer_ = nullptr;
  const int64_t retry_op_id = op_id + 1;
  prepare_copy_writer(tablet, resources, retry_op_id);
  ASSERT_FALSE(HasFatalFailure());
  CopySSMacroBlockWriter &retry_writer = *resources.copy_writer_;
  prepare_copy_tail(retry_writer, resources.source_builder_->res_.data_block_ids_);
  ASSERT_FALSE(HasFatalFailure());
  ObMacroBlockWriter *retry_clustered_writer =
      retry_writer.index_block_rebuilder_.clustered_index_writer_->macro_writer_;
  const int64_t retry_flushed_count =
      retry_clustered_writer->get_macro_block_write_ctx().get_macro_block_list().count();
  ASSERT_EQ(OB_SUCCESS, retry_writer.close(resources.allocator_));
  check_result(retry_writer, tablet, retry_flushed_count + 1, retry_op_id);
}
#endif

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = oceanbase::common::OB_SUCCESS;
  char storage_info[1000] = {0};
  const int64_t cur_time_ns = oceanbase::common::ObTimeUtility::current_time_ns();
  if (OB_FAIL(oceanbase::common::databuff_printf(storage_info, sizeof(storage_info),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000"
      "&max_bandwidth=200000000B&scope=region", oceanbase::unittest::S3_BUCKET, cur_time_ns,
      oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK,
      oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION))) {
    // Do not start the server with a truncated object-storage configuration.
  } else {
    oceanbase::shared_storage_info = storage_info;
    oceanbase::unittest::init_log_and_gtest(argc, argv);
    OB_LOGGER.set_log_level("INFO");
    GCONF.ob_startup_mode.set_value("shared_storage");
    GCONF.datafile_size.set_value("20G");
    GCONF.memory_limit.set_value("10G");
    GCONF.system_memory.set_value("3G");
    ::testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}
