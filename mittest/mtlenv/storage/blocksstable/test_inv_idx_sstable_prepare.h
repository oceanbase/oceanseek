/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OB_TEST_INV_IDX_SSTABLE_PREPARE_H_
#define OB_TEST_INV_IDX_SSTABLE_PREPARE_H_

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "lib/allocator/page_arena.h"
#include "lib/random/ob_random.h"
#include "lib/worker.h"
#include "share/schema/ob_column_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "share/schema/ob_table_schema.h"
#include "mtlenv/mock_tenant_module_env.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_writer.h"
#include "storage/blocksstable/async_index/ob_inv_sstable.h"
#include "storage/blocksstable/ob_datum_rowkey.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/blocksstable/index_block/ob_index_block_builder.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/blocksstable/ob_sstable_private_object_cleaner.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/ob_storage_schema.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace blocksstable;
using namespace storage;
using namespace share;
using namespace share::schema;

namespace unittest
{

// =============================================================================
// Helper: build a full-text inverted-index table schema with FTS column flags.
//
// Column layout (order matters — matches storage_datums_ in ObDatumRow):
//   col 0: term (VARCHAR, rowkey #1)
//   col 1: doc_id (uint64, rowkey #2, GENERATED_DOC_ID_COLUMN_FLAG)
//   col 2: token_freq (uint64, GENERATED_FTS_WORD_COUNT_COLUMN_FLAG)
//   col 3: doc_length (uint64, GENERATED_FTS_DOC_LENGTH_COLUMN_FLAG)
//   col 4: pos_list (VARCHAR, GENERATED_FTS_POS_LIST_COLUMN_FLAG)  [only when has_pos_list]
//
// The schema rowkey is (term, doc_id): doc_id is the trailing rowkey column so the inverted
// index endkey (term, max_doc_id) is typed against the real doc_id column.
//
// When has_pos_list is false the pos_list column is omitted, producing an inverted index that
// carries no position list (phrase queries disabled).
// =============================================================================
static void prepare_inv_idx_schema(
    ObTableSchema &table_schema,
    const bool has_pos_list,
    const int64_t micro_block_size = 16 * 1024)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = 1;
  const uint64_t table_id = 7777;
  const int64_t rowkey_column_cnt = 2;   // term, doc_id
  const int64_t column_cnt = has_pos_list ? 5 : 4;

  table_schema.reset();
  ret = table_schema.set_table_name("test_inv_idx_sstable");
  ASSERT_EQ(OB_SUCCESS, ret);
  table_schema.set_tenant_id(tenant_id);
  table_schema.set_tablegroup_id(1);
  table_schema.set_database_id(1);
  table_schema.set_table_id(table_id);
  table_schema.set_rowkey_column_num(rowkey_column_cnt);
  table_schema.set_max_used_column_id(common::OB_APP_MIN_COLUMN_ID + column_cnt);
  table_schema.set_schema_version(10);
  table_schema.set_block_size(micro_block_size);
  table_schema.set_compress_func_name("none");
  table_schema.set_row_store_type(FLAT_ROW_STORE);
  table_schema.set_pctfree(10);

  struct ColMeta
  {
    int64_t rowkey_pos_;   // 0 = non-rowkey, >=1 = rowkey position
    ObObjType obj_type_;
    int64_t flags_;
  };
  const ColMeta col_metas[] = {
      {1, ObVarcharType, 0},                                    // term     (rowkey #1)
      {2, ObUInt64Type,  GENERATED_DOC_ID_COLUMN_FLAG},         // doc_id   (rowkey #2)
      {0, ObUInt64Type,  GENERATED_FTS_WORD_COUNT_COLUMN_FLAG}, // token_freq
      {0, ObUInt64Type,  GENERATED_FTS_DOC_LENGTH_COLUMN_FLAG}, // doc_length
      {0, ObVarcharType, GENERATED_FTS_POS_LIST_COLUMN_FLAG},   // pos_list
  };

  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));
  for (int64_t i = 0; i < column_cnt; ++i) {
    ObColumnSchemaV2 column;
    const int64_t column_id = common::OB_APP_MIN_COLUMN_ID + i;
    column.reset();
    column.set_table_id(table_id);
    column.set_column_id(column_id);
    sprintf(name, "test%020ld", i);
    ASSERT_EQ(OB_SUCCESS, column.set_column_name(name));
    column.set_data_type(col_metas[i].obj_type_);
    column.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
    column.set_data_length(64);
    column.set_rowkey_position(col_metas[i].rowkey_pos_);
    if (0 != col_metas[i].flags_) {
      column.set_column_flags(col_metas[i].flags_);
    }
    ObSkipIndexColumnAttr skip_idx_attr;
    if (!is_lob_storage(col_metas[i].obj_type_)) {
      skip_idx_attr.set_min_max();
      column.set_skip_index_attr(skip_idx_attr.get_packed_value());
    }
    ASSERT_EQ(OB_SUCCESS, table_schema.add_column(column));
  }
}

// Resolve each FTS value column index from the multi-version column layout.
static int prepare_inv_idx_row_desc(
    const ObTableSchema &table_schema,
    ObInvIdxRowDesc &row_desc)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<ObColDesc, 8> col_descs;
  row_desc.reset();
  if (OB_FAIL(table_schema.get_multi_version_column_descs(col_descs))) {
    STORAGE_LOG(WARN, "fail to get multi version column descs", K(ret));
  } else {
    row_desc.index_type_ = INV_IDX_TYPE_FTS;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
      const ObColDesc &col_desc = col_descs.at(i);
      const ObColumnSchemaV2 *col_schema = table_schema.get_column_schema(col_desc.col_id_);
      if (OB_ISNULL(col_schema)) {
        // skip multi-version extra columns
      } else {
        const uint64_t flags = col_schema->get_column_flags();
        if (ObSchemaUtils::is_doc_id_column(flags)) {
          row_desc.doc_id_col_idx_ = static_cast<int16_t>(i);
        } else if (ObSchemaUtils::is_word_count_column(flags)) {
          row_desc.token_freq_col_idx_ = static_cast<int16_t>(i);
        } else if (ObSchemaUtils::is_doc_length_column(flags)) {
          row_desc.doc_length_col_idx_ = static_cast<int16_t>(i);
        } else if (ObSchemaUtils::is_pos_list_column(flags)) {
          row_desc.pos_list_col_idx_ = static_cast<int16_t>(i);
        }
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!row_desc.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "inv idx row desc is invalid", K(ret), K(row_desc));
  }
  return ret;
}

// Derive the schema-driven inv-meta fields from the already-resolved row desc.
// Data-driven fields (min/max doc_id, total_doc_length_) are filled by the
// caller from the feeder. The shared index builder in this fixture does not
// receive the writer's segment meta automatically, so the test supplies the
// merge-result meta explicitly using the same RowDesc column resolution.
static void fill_inv_meta_from_schema(
    const ObTableSchema &table_schema,
    const ObInvIdxRowDesc &row_desc,
    ObInvIdxStatsMeta &inv_meta)
{
  common::ObSEArray<ObColDesc, 8> col_descs;
  ASSERT_EQ(OB_SUCCESS, table_schema.get_multi_version_column_descs(col_descs));
  ASSERT_GE(row_desc.doc_id_col_idx_, 0);
  ASSERT_LT(row_desc.doc_id_col_idx_, col_descs.count());

  inv_meta.reset();
  // FTS index is identified by its doc_id column; pos_list presence by the
  // resolved pos_list column index. nested_key has no schema flag yet.
  inv_meta.index_type_ = (row_desc.doc_id_col_idx_ >= 0) ? INV_IDX_TYPE_FTS : INV_IDX_TYPE_INVALID;
  inv_meta.has_relevance_params_ =
      (row_desc.token_freq_col_idx_ >= 0 && row_desc.doc_length_col_idx_ >= 0);
  inv_meta.has_position_list_ = (row_desc.pos_list_col_idx_ >= 0);
  inv_meta.has_nested_key_ = false;
  inv_meta.doc_id_obj_meta_ = col_descs.at(row_desc.doc_id_col_idx_).col_type_;
}

// Build a 1-datum rowkey holding the term string (the inverted-index query key).
static void build_term_rowkey(
    const char *term_str,
    const int32_t term_len,
    ObStorageDatum &datum,
    ObDatumRowkey &key)
{
  datum.set_string(term_str, term_len);
  key.assign(&datum, 1);
}

// Build an ObDatumRow in the multi-version column layout. The MV extra rowkey
// columns (trans_version, sql_sequence) sit between the schema rowkey (term)
// and the non-rowkey schema columns; they are left nop.
//
// pos_list is only written when the row desc carries a pos_list column; for a
// no-position-list index the pos_list datum is left nop.
static int make_inv_idx_row(
    ObDatumRow &row,
    const ObInvIdxRowDesc &row_desc,
    const int64_t mv_col_cnt,
    const ObString &term_str,
    const uint64_t doc_id,
    const uint64_t token_freq,
    const uint64_t doc_length,
    const ObString &pos_list_str,
    const ObString &nested_key_str = ObString())
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(mv_col_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid mv col cnt", K(ret), K(mv_col_cnt));
  } else if (OB_UNLIKELY(!row.is_valid() || row.get_column_count() != mv_col_cnt)) {
    row.reset();
    if (OB_FAIL(row.init(mv_col_cnt))) {
      STORAGE_LOG(WARN, "fail to init datum row", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    row.count_ = static_cast<uint16_t>(mv_col_cnt);
    for (int16_t i = 0; i < row.count_; ++i) {
      row.storage_datums_[i].set_nop();
    }
    row.storage_datums_[0].set_string(term_str);
    row.storage_datums_[row_desc.doc_id_col_idx_].set_uint(doc_id);
    if (row_desc.token_freq_col_idx_ >= 0) {
      row.storage_datums_[row_desc.token_freq_col_idx_].set_uint(token_freq);
    }
    if (row_desc.doc_length_col_idx_ >= 0) {
      row.storage_datums_[row_desc.doc_length_col_idx_].set_uint(doc_length);
    }
    if (row_desc.pos_list_col_idx_ >= 0) {
      row.storage_datums_[row_desc.pos_list_col_idx_].set_string(pos_list_str);
    }
    if (row_desc.nested_key_col_idx_ >= 0) {
      row.storage_datums_[row_desc.nested_key_col_idx_].set_string(nested_key_str);
    }
  }
  return ret;
}

// Configure ObInvIdxRowDesc for SEARCH (no BM25). Mittest borrows pos_list slot as nested_key.
static void apply_search_index_row_desc(ObInvIdxRowDesc &row_desc, const bool has_nested_key)
{
  const int16_t nested_key_col_idx = has_nested_key ? row_desc.pos_list_col_idx_ : -1;
  row_desc.index_type_ = INV_IDX_TYPE_SEARCH;
  row_desc.token_freq_col_idx_ = -1;
  row_desc.doc_length_col_idx_ = -1;
  row_desc.pos_list_col_idx_ = -1;
  row_desc.nested_key_col_idx_ = nested_key_col_idx;
}

// =============================================================================
// ObInvIdxRowFeeder — tool class that generates inverted-index rows in strictly
// increasing <term, doc_id> order and feeds them into the macro block writer.
//
//  - term is a zero-padded increasing string so its byte order is monotonic.
//  - doc_id increases globally (starts at 1 so max_doc_id_ > 0).
//  - a new term is started every deterministic 1..10000 rows (fixed seed ->
//    reproducible data across runs).
//  - tracks min/max doc_id and the sum of token_freq for the inv sstable
//    extended meta.
// =============================================================================
class ObInvIdxRowFeeder
{
public:
  static const uint64_t FEEDER_RAND_SEED = 42;

  // Legacy form (dumpsst / prewarm tests): optional fixed position_list payload.
  ObInvIdxRowFeeder(
      const ObInvIdxRowDesc &row_desc,
      const int64_t mv_col_cnt,
      const ObString &position_list)
    : ObInvIdxRowFeeder(row_desc, mv_col_cnt, false /*vary_posting_payload*/,
                        0 /*initial_term_seq*/, 0 /*initial_doc_id*/, position_list)
  {}

  // Default-payload convenience form.
  ObInvIdxRowFeeder(
      const ObInvIdxRowDesc &row_desc,
      const int64_t mv_col_cnt)
    : ObInvIdxRowFeeder(row_desc, mv_col_cnt, ObString())
  {}

  // Deterministic varying-payload form with initial term/doc offsets
  // (TestInvIdxSSTablePrepare::build_inv_sstable).
  ObInvIdxRowFeeder(
      const ObInvIdxRowDesc &row_desc,
      const int64_t mv_col_cnt,
      const bool vary_posting_payload,
      const int64_t initial_term_seq,
      const uint64_t initial_doc_id,
      const uint64_t doc_id_step = 1)
    : ObInvIdxRowFeeder(row_desc, mv_col_cnt, vary_posting_payload,
                        initial_term_seq, initial_doc_id, ObString(), doc_id_step)
  {}

private:
  ObInvIdxRowFeeder(
      const ObInvIdxRowDesc &row_desc,
      const int64_t mv_col_cnt,
      const bool vary_posting_payload,
      const int64_t initial_term_seq,
      const uint64_t initial_doc_id,
      const ObString &position_list,
      const uint64_t doc_id_step = 1)
    : row_desc_(row_desc),
      mv_col_cnt_(mv_col_cnt),
      position_list_(position_list),
      term_seq_(initial_term_seq),
      doc_id_(initial_doc_id),
      doc_id_step_(doc_id_step),
      rows_until_switch_(0),
      min_doc_id_(0),
      max_doc_id_(0),
      term_count_(0),
      total_token_freq_(0),
      vary_posting_payload_(vary_posting_payload)
  {
    MEMSET(pos_list_buf_, 'p', sizeof(pos_list_buf_));
    rng_.seed(FEEDER_RAND_SEED);
  }

public:

  uint64_t min_doc_id() const { return min_doc_id_; }
  uint64_t max_doc_id() const { return max_doc_id_; }
  int64_t term_count() const { return term_count_; }
  uint64_t total_token_freq() const { return total_token_freq_; }

  int feed_n(ObInvIdxMacroBlockWriter &writer, const int64_t row_cnt)
  {
    int ret = OB_SUCCESS;
    ObDatumRow row;
    if (OB_FAIL(row.init(mv_col_cnt_))) {
      STORAGE_LOG(WARN, "fail to init feeder row", K(ret), K_(mv_col_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < row_cnt; ++i) {
      next_keys();
      const uint64_t token_freq = vary_posting_payload_ ? 1 + i % 7 : 1;
      const uint64_t doc_length = vary_posting_payload_ ? 50 + i % 53 : 100;
      int32_t pos_list_len = 8;
      if (vary_posting_payload_) {
        // A larger deterministic, high-entropy payload guarantees multiple macro blocks with a
        // modest row count and makes corruption in the middle of the position list observable.
        uint64_t state = (static_cast<uint64_t>(i) + 1) * 0x9E3779B97F4A7C15ULL;
        pos_list_len = static_cast<int32_t>(sizeof(pos_list_buf_));
        for (int64_t j = 0; j < pos_list_len; ++j) {
          state ^= state << 13;
          state ^= state >> 7;
          state ^= state << 17;
          pos_list_buf_[j] = static_cast<char>(state & 0xFF);
        }
      }
      ObString term_str(static_cast<int32_t>(strlen(term_buf_)), term_buf_);
      const ObString pos_list = position_list_.empty()
          ? ObString(pos_list_len, pos_list_buf_)
          : position_list_;
      if (OB_FAIL(make_inv_idx_row(row, row_desc_, mv_col_cnt_, term_str, doc_id_,
          token_freq, doc_length, pos_list))) {
        STORAGE_LOG(WARN, "fail to make inv idx row", K(ret), K(i));
      } else if (OB_FAIL(writer.append_row(row))) {
        STORAGE_LOG(WARN, "fail to append inv idx row", K(ret), K(i), K_(doc_id), K(term_str));
      } else {
        // Track min/max doc_id and the sum of token_freq for the inv sstable extended meta.
        if (0 == min_doc_id_) {
          min_doc_id_ = doc_id_;
        }
        max_doc_id_ = doc_id_;
        total_token_freq_ += token_freq;
      }
    }
    return ret;
  }

private:
  // Advance to the next (term, doc_id) key. doc_id always increments; the term
  // is rotated to a strictly larger string every deterministic 1..10000 rows.
  void next_keys()
  {
    if (rows_until_switch_ <= 0) {
      ++term_seq_;
      ++term_count_;
      sprintf(term_buf_, "term%010ld", term_seq_);
      rows_until_switch_ = 1 + rng_.get(0, 9999);
    }
    doc_id_ += doc_id_step_;
    --rows_until_switch_;
  }

private:
  const ObInvIdxRowDesc &row_desc_;
  const int64_t mv_col_cnt_;
  const ObString position_list_;
  int64_t term_seq_;
  uint64_t doc_id_;
  uint64_t doc_id_step_;
  int64_t rows_until_switch_;
  uint64_t min_doc_id_;
  uint64_t max_doc_id_;
  int64_t term_count_;
  uint64_t total_token_freq_;
  bool vary_posting_payload_;
  common::ObRandom rng_;
  char term_buf_[32];
  char pos_list_buf_[128];
};

// =============================================================================
// TestInvIdxSSTablePrepare — full pipeline:
//   schema -> macro block writer -> sstable index builder -> merge res
//          -> ObTabletCreateSSTableParam (INV) -> ObInvSSTable
//
// build_inv_sstable() encapsulates the pipeline so that downstream tests can
// construct an inverted-index sstable with or without a position list and with
// a configurable row count.
// =============================================================================
class TestInvIdxSSTablePrepare : public ::testing::Test
{
public:
  static const int64_t SNAPSHOT_VERSION = 10000;
  static constexpr int64_t TEST_POS_LIST_LEN = 8;
  static const uint64_t LS_ID = 1001;
  static const uint64_t TABLET_ID = 200001;

  TestInvIdxSSTablePrepare()
    : index_builder_(nullptr), mv_col_cnt_(0)
  {}

  static void SetUpTestCase()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }
  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  virtual void SetUp() override
  {
    allocator_.reset();
    table_schema_.reset();
    whole_desc_.reset();
    inv_idx_row_desc_.reset();
    index_builder_ = nullptr;
    mv_col_cnt_ = 0;
  }
  virtual void TearDown() override
  {
    if (OB_NOT_NULL(index_builder_)) {
      index_builder_->~ObSSTableIndexBuilder();
      index_builder_ = nullptr;
    }
    whole_desc_.reset();
    table_schema_.reset();
    allocator_.reset();
  }

  void prepare_desc(const ObRowStoreType force_row_store_type = ObRowStoreType::MAX_ROW_STORE);

  // Build an ObInvSSTable from `row_cnt` generated inverted-index rows.
  //  - has_pos_list: whether the FTS schema carries a pos_list column.
  //  - row_cnt: number of rows to feed (configurable).
  //  - vary_posting_payload: generate deterministic non-uniform posting values.
  //  - micro_index_clustered: build the macro index as a clustered index tree, so
  //    whole-macro-block reuse must replay the clustered index micro block.
  //  - doc_id_step: increment between generated doc ids; values greater than one
  //    produce deterministic gaps for deletion-envelope fallback tests.
  // On success `inv_sstable` is initialized and asserted valid.
  void build_inv_sstable(const bool has_pos_list,
                         const int64_t row_cnt,
                         ObInvSSTable &inv_sstable,
                         const bool vary_posting_payload = false,
                         const int64_t initial_term_seq = 0,
                         const uint64_t initial_doc_id = 0,
                         const bool micro_index_clustered = false,
                         const uint64_t doc_id_step = 1,
                         int64_t *out_term_count = nullptr);

  // Construct an ObInvSSTable from an already-closed ObSSTableMergeRes.
  // res.inv_meta_ must be filled by the caller before invoking this helper.
  // Uses table_schema_ and the fixture constants (LS_ID, TABLET_ID, SNAPSHOT_VERSION)
  // to drive init_for_merge; the caller is responsible for keeping res alive until
  // after out.init() returns.
  void finalize_inv_sstable_from_res(ObSSTableMergeRes &res, ObInvSSTable &out);

protected:
  ObArenaAllocator allocator_;
  ObTableSchema table_schema_;
  ObWholeDataStoreDesc whole_desc_;
  ObInvIdxRowDesc inv_idx_row_desc_;
  ObSSTableIndexBuilder *index_builder_;
  ObMacroSeqParam seq_param_;
  int64_t mv_col_cnt_;
};

void TestInvIdxSSTablePrepare::prepare_desc(const ObRowStoreType force_row_store_type)
{
  SCN end_scn;
  end_scn.convert_for_tx(SNAPSHOT_VERSION);
  const SCN reorganization_scn(SCN::min_scn());
  OK(whole_desc_.init(false /*is_ddl*/, table_schema_, ObLSID(LS_ID), ObTabletID(TABLET_ID),
      MINI_MERGE, SNAPSHOT_VERSION, DATA_CURRENT_VERSION,
      table_schema_.get_micro_index_clustered(), 0 /*transfer_epoch*/, 0 /*concurrent_cnt*/,
      reorganization_scn, end_scn));
  ObDataStoreDesc &desc = whole_desc_.get_desc();
  desc.static_desc_->schema_version_ = 10;
  // inverted-index data macro block stores SSTableData typed micro blocks.
  desc.data_store_type_ = ObMacroBlockCommonHeader::SSTableData;

  void *builder_buf = allocator_.alloc(sizeof(ObSSTableIndexBuilder));
  ASSERT_NE(nullptr, builder_buf);
  index_builder_ = new (builder_buf) ObSSTableIndexBuilder(false /*not need writer buffer*/);
  OK(index_builder_->init(desc));
  desc.sstable_index_builder_ = index_builder_;

  // The n-1 index micro writer is built from the index builder's leaf store desc, so overriding just
  // that desc exercises a chosen index-micro-block format while the posting/data path stays flat.
  if (ObStoreFormat::is_row_store_type_with_pax_encoding(force_row_store_type)) {
    // PAX encoding is invalid on a minor-merge desc (is_store_type_valid rejects it). Give the leaf
    // desc a private static desc with a non-minor merge type so is_valid() accepts PAX via the
    // SSTableIndex branch, then set the row store type ObSSTableIndexBuilder::set_row_store_type would
    // produce (SELECTIVE_ENCODING -> ObMicroBlockEncoder).
    void *sd_buf = allocator_.alloc(sizeof(ObStaticDataStoreDesc));
    ASSERT_NE(nullptr, sd_buf);
    ObStaticDataStoreDesc *leaf_static = new (sd_buf) ObStaticDataStoreDesc();
    OK(leaf_static->assign(*index_builder_->leaf_store_desc_.static_desc_));
    leaf_static->merge_type_ = MAJOR_MERGE;
    index_builder_->leaf_store_desc_.static_desc_ = leaf_static;
    index_builder_->leaf_store_desc_.row_store_type_ = SELECTIVE_ENCODING_ROW_STORE;
    index_builder_->leaf_store_desc_.encoder_opt_.set_store_type(SELECTIVE_ENCODING_ROW_STORE);
  } else if (CS_ENCODING_ROW_STORE == force_row_store_type) {
    // CS encoding is valid on minor merges, so no static-desc swap is needed; just switch the leaf
    // desc format to exercise ObMicroBlockCSEncoder.
    index_builder_->leaf_store_desc_.row_store_type_ = CS_ENCODING_ROW_STORE;
    index_builder_->leaf_store_desc_.encoder_opt_.set_store_type(CS_ENCODING_ROW_STORE);
  }

  ASSERT_TRUE(desc.is_valid());
  mv_col_cnt_ = desc.get_row_column_count();
  ASSERT_GT(mv_col_cnt_, 0);

  seq_param_.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
  seq_param_.start_ = 0;
  seq_param_.interval_ = 1;
  seq_param_.step_ = 1;
}

void TestInvIdxSSTablePrepare::build_inv_sstable(
    const bool has_pos_list,
    const int64_t row_cnt,
    ObInvSSTable &inv_sstable,
    const bool vary_posting_payload,
    const int64_t initial_term_seq,
    const uint64_t initial_doc_id,
    const bool micro_index_clustered,
    const uint64_t doc_id_step,
    int64_t *out_term_count)
{
  ASSERT_GT(row_cnt, 0);
  ASSERT_GT(doc_id_step, 0);
  if (OB_NOT_NULL(out_term_count)) {
    *out_term_count = 0;
  }

  // 1. schema + row desc + data store desc + index builder
  prepare_inv_idx_schema(table_schema_, has_pos_list);
  table_schema_.set_micro_index_clustered(micro_index_clustered);
  OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
  ASSERT_TRUE(inv_idx_row_desc_.is_valid());
  ASSERT_EQ(has_pos_list, inv_idx_row_desc_.pos_list_col_idx_ >= 0);
  prepare_desc();

  // 2. open macro block writer
  ObInvIdxMacroBlockWriter writer;
  ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
  ObISSTableObjectCleaner *cleaner = index_builder_->get_object_cleaner();
  ASSERT_NE(nullptr, cleaner);
  OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
      seq_param_, pre_warm_param, *cleaner, nullptr /*callback*/));
  ASSERT_TRUE(writer.is_opened());

  // 3. feed rows in monotonically increasing <term, doc_id> order
  ObInvIdxRowFeeder feeder(
      inv_idx_row_desc_, mv_col_cnt_, vary_posting_payload,
      initial_term_seq, initial_doc_id, doc_id_step);
  OK(feeder.feed_n(writer, row_cnt));
  if (OB_NOT_NULL(out_term_count)) {
    *out_term_count = feeder.term_count();
  }
  STORAGE_LOG(INFO, "fed inverted index rows", K(row_cnt), K(has_pos_list), K(feeder.term_count()),
      K(feeder.min_doc_id()), K(feeder.max_doc_id()));

  // 4. close writer -> flushes index tree into the shared index builder
  OK(writer.close());
  ASSERT_FALSE(writer.is_opened());

  // 5. close index builder -> obtain the sstable merge result
  ObSSTableMergeRes res;
  OK(index_builder_->close(res));
  ASSERT_TRUE(res.root_desc_.is_valid());
  ASSERT_EQ(row_cnt, res.row_count_);

  // 6. build the ObTabletCreateSSTableParam via the production init_for_merge path.
  //    The inverted-index table-type signal lives on the merge ctx (is_inv_index_),
  //    not on the merge res; res carries the inv extended meta as payload. The
  //    schema-driven fields are derived from the resolved row desc, and the
  //    data-driven fields (doc id range, total_doc_length_) from the feeder.
  ObInvIdxStatsMeta inv_meta_stats;
  fill_inv_meta_from_schema(table_schema_, inv_idx_row_desc_, inv_meta_stats);
  inv_meta_stats.min_doc_id_ = feeder.min_doc_id();
  inv_meta_stats.max_doc_id_ = feeder.max_doc_id();
  // sum(token_freq)
  inv_meta_stats.total_doc_length_ = feeder.total_token_freq();
  ASSERT_EQ(OB_SUCCESS, res.inv_meta_.init(inv_meta_stats));
  ASSERT_TRUE(res.inv_meta_.is_valid());

  // Build a storage schema from the FTS table schema; init_for_merge reads the
  // rowkey/mode/index-type/column counts from it.
  ObStorageSchema *storage_schema = nullptr;
  {
    void *schema_buf = allocator_.alloc(sizeof(ObStorageSchema));
    ASSERT_NE(nullptr, schema_buf);
    storage_schema = new (schema_buf) ObStorageSchema();
    OK(storage_schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  }

  // Minimal merge ctx: no real tablet/LS is needed — init_for_merge only reads
  // static_param_ (schema, merge signal, version range) on the inverted path.
  ObTabletMergeDagParam dag_param;
  dag_param.merge_type_ = MINOR_MERGE;
  dag_param.ls_id_ = ObLSID(LS_ID);
  dag_param.tablet_id_ = ObTabletID(TABLET_ID);
  ObTabletMergeCtx merge_ctx(dag_param, allocator_);
  merge_ctx.static_param_.is_inv_index_ = true;   // emits INV_INDEX_SSTABLE
  merge_ctx.static_param_.schema_ = storage_schema;
  merge_ctx.static_param_.version_range_.snapshot_version_ = SNAPSHOT_VERSION;
  merge_ctx.static_param_.merge_scn_.convert_for_tx(SNAPSHOT_VERSION);

  ObTabletCreateSSTableParam param;
  OK(param.init_for_merge(merge_ctx, res, nullptr /*cg_schema*/, 0 /*cg_idx*/));
  ASSERT_TRUE(param.table_key_.is_inv_sstable());
  ASSERT_TRUE(param.inv_meta_.is_valid());
  ASSERT_TRUE(param.inv_meta_.get_stats().is_fts_index());
  ASSERT_EQ(has_pos_list, param.inv_meta_.get_stats().has_position_list_);
  ASSERT_FALSE(param.inv_meta_.get_stats().has_nested_key_);
  ASSERT_EQ(feeder.min_doc_id(), param.inv_meta_.get_stats().min_doc_id_);
  ASSERT_EQ(feeder.max_doc_id(), param.inv_meta_.get_stats().max_doc_id_);
  ASSERT_EQ(feeder.total_token_freq(), param.inv_meta_.get_stats().total_doc_length_);

  // 7. construct the ObInvSSTable
  inv_sstable.reset();
  OK(inv_sstable.init(param, &allocator_));

  // 8. assertions
  ASSERT_TRUE(inv_sstable.is_inv_sstable());
  ASSERT_TRUE(inv_sstable.is_valid());
  ASSERT_TRUE(inv_sstable.get_inv_meta().get_stats().is_valid());
  ASSERT_TRUE(inv_sstable.get_inv_meta().get_stats().is_fts_index());
  ASSERT_EQ(has_pos_list, inv_sstable.get_inv_meta().get_stats().has_position_list_);
  ASSERT_FALSE(inv_sstable.get_inv_meta().get_stats().has_nested_key_);
  ASSERT_EQ(feeder.min_doc_id(), inv_sstable.get_inv_meta().get_stats().min_doc_id_);
  ASSERT_EQ(feeder.max_doc_id(), inv_sstable.get_inv_meta().get_stats().max_doc_id_);
  ASSERT_EQ(feeder.total_token_freq(), inv_sstable.get_inv_meta().get_stats().total_doc_length_);
  ASSERT_GT(inv_sstable.get_data_macro_block_count(), 0);
  STORAGE_LOG(INFO, "built inv sstable", K(inv_sstable), K(param));
}

void TestInvIdxSSTablePrepare::finalize_inv_sstable_from_res(
    ObSSTableMergeRes &res,
    ObInvSSTable &out)
{
  ASSERT_TRUE(res.inv_meta_.is_valid());
  ASSERT_TRUE(res.root_desc_.is_valid());

  ObStorageSchema *storage_schema = nullptr;
  {
    void *schema_buf = allocator_.alloc(sizeof(ObStorageSchema));
    ASSERT_NE(nullptr, schema_buf);
    storage_schema = new (schema_buf) ObStorageSchema();
    OK(storage_schema->init(allocator_, table_schema_, lib::Worker::CompatMode::MYSQL));
  }

  // Minimal merge ctx — init_for_merge reads only static_param_ on the inv-index path.
  ObTabletMergeDagParam dag_param;
  dag_param.merge_type_ = MINOR_MERGE;
  dag_param.ls_id_ = ObLSID(LS_ID);
  dag_param.tablet_id_ = ObTabletID(TABLET_ID);
  ObTabletMergeCtx merge_ctx(dag_param, allocator_);
  merge_ctx.static_param_.is_inv_index_ = true;
  merge_ctx.static_param_.schema_ = storage_schema;
  merge_ctx.static_param_.version_range_.snapshot_version_ = SNAPSHOT_VERSION;
  merge_ctx.static_param_.merge_scn_.convert_for_tx(SNAPSHOT_VERSION);

  ObTabletCreateSSTableParam param;
  OK(param.init_for_merge(merge_ctx, res, nullptr /*cg_schema*/, 0 /*cg_idx*/));
  ASSERT_TRUE(param.table_key_.is_inv_sstable());
  ASSERT_TRUE(param.inv_meta_.is_valid());

  out.reset();
  OK(out.init(param, &allocator_));
  ASSERT_TRUE(out.is_valid());
}

} // namespace unittest
} // namespace oceanbase

#endif // OB_TEST_INV_IDX_SSTABLE_PREPARE_H_
