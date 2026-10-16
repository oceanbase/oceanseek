/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include <functional>
#include <string>
#include <unistd.h>

#define OK(ass) ASSERT_EQ(OB_SUCCESS, (ass))
#define private public
#define protected public

#include "test_inv_idx_sstable_prepare.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_fts_pos_list_codec.h"
#include "storage/blocksstable/ob_dag_macro_block_writer.h"
#include "storage/blocksstable/ob_macro_block_common_header.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_header.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "storage/blocksstable/ob_sstable_macro_block_header.h"
#include "storage/blocksstable/async_index/ob_inv_idx_macro_block_dumper.h"
#include "storage/blocksstable/async_index/ob_inv_idx_micro_block_reader.h"
#include "storage/blocksstable/async_index/ob_posting_block_struct.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
#include "tools/ob_admin/ob_admin_common_utils.h"

#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace blocksstable;

namespace
{
class MemStreamCapture final
{
public:
  MemStreamCapture() : file_(nullptr), buf_(nullptr), size_(0) {}
  ~MemStreamCapture()
  {
    if (nullptr != file_) {
      fclose(file_);
      file_ = nullptr;
    }
    if (nullptr != buf_) {
      free(buf_);
      buf_ = nullptr;
    }
  }
  FILE *open()
  {
    file_ = open_memstream(&buf_, &size_);
    return file_;
  }
  std::string str()
  {
    if (nullptr != file_) {
      fflush(file_);
    }
    return nullptr == buf_ ? std::string() : std::string(buf_, size_);
  }
private:
  FILE *file_;
  char *buf_;
  size_t size_;
};

std::string capture_stderr(const std::function<int()> &fn, int &ret)
{
  std::string output;
  FILE *tmp = tmpfile();
  EXPECT_NE(nullptr, tmp);
  if (nullptr != tmp) {
    fflush(stderr);
    const int saved_fd = dup(STDERR_FILENO);
    EXPECT_GE(saved_fd, 0);
    EXPECT_EQ(STDERR_FILENO, dup2(fileno(tmp), STDERR_FILENO));
    ret = fn();
    fflush(stderr);
    EXPECT_EQ(STDERR_FILENO, dup2(saved_fd, STDERR_FILENO));
    close(saved_fd);
    fseek(tmp, 0, SEEK_SET);
    char buf[4096];
    while (size_t n = fread(buf, 1, sizeof(buf), tmp)) {
      output.append(buf, n);
    }
    fclose(tmp);
  }
  return output;
}

int dump_single_macro_block_to_stderr(const char *buf, const int64_t size)
{
  tools::ObDumpMacroBlockContext ctx;
  tools::ObDumpMacroBlockParam param(ctx, false /*hex_print*/);
  return tools::ObAdminCommonUtils::dump_single_macro_block(param, buf, size);
}

void refresh_common_payload_checksum(char *buf, const int64_t size)
{
  ObMacroBlockCommonHeader common_header;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, common_header.deserialize(buf, size, pos));
  ASSERT_GE(size, common_header.get_header_size());
  const int64_t payload_size = size - common_header.get_header_size();
  common_header.set_payload_size(static_cast<int32_t>(payload_size));
  common_header.set_payload_checksum(static_cast<int32_t>(
      ob_crc64(buf + common_header.get_header_size(), payload_size)));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, common_header.serialize(buf, size, pos));
}

void refresh_micro_payload_checksum(char *micro_buf, const int64_t micro_size)
{
  ObMicroBlockHeader header;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.deserialize(micro_buf, micro_size, pos));
  ASSERT_GT(micro_size, static_cast<int64_t>(header.header_size_));
  header.data_checksum_ = ob_crc64_sse42(0, micro_buf + header.header_size_, header.data_zlength_);
  header.set_header_checksum();
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, header.serialize(micro_buf, micro_size, pos));
}

const char *find_bytes(
    const char *haystack,
    const int64_t haystack_size,
    const char *needle,
    const int64_t needle_size)
{
  const char *found = nullptr;
  if (nullptr != haystack && nullptr != needle && haystack_size >= needle_size && needle_size > 0) {
    for (int64_t i = 0; nullptr == found && i <= haystack_size - needle_size; ++i) {
      if (0 == MEMCMP(haystack + i, needle, needle_size)) {
        found = haystack + i;
      }
    }
  }
  return found;
}
} // namespace

class TestInvIdxSSTableDumpsst : public TestInvIdxSSTablePrepare
{
protected:
  class CaptureMacroBlockFlusher final : public ObIMacroBlockFlusher
  {
  public:
    explicit CaptureMacroBlockFlusher(ObIAllocator &allocator)
      : allocator_(allocator), buf_(nullptr), size_(0)
    {}
    virtual int write_disk(ObMacroBlock &macro_block, const bool /*is_close_flush*/) override
    {
      int ret = OB_SUCCESS;
      size_ = macro_block.get_data_size();
      if (OB_UNLIKELY(size_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(size_)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        MEMCPY(buf_, macro_block.get_data_buf(), size_);
      }
      return ret;
    }
    ObIAllocator &allocator_;
    char *buf_;
    int64_t size_;
  };

  class CaptureMacroBlockWriter final : public ObMacroBlockWriter
  {
  public:
    CaptureMacroBlockWriter() : ObMacroBlockWriter(true /*is_need_macro_buffer*/) {}
    virtual bool allow_use_external_flusher() const override { return true; }
  };

  static const int64_t NORMAL_COLUMN_CNT = 20;
  static const int64_t NORMAL_ROWKEY_COLUMN_CNT = 20;
  static const int64_t NORMAL_SNAPSHOT_VERSION = 2;

  class ValidatedInvDumperHarness final
  {
  public:
    int init(const char *buf, const int64_t size, FILE *fd = nullptr)
    {
      int ret = OB_SUCCESS;
      reader_.reset();
      dumper_.reset();
      ret = reader_.init(buf, size,
          tools::ObDumpMacroBlockContext::DEFUALT_DUMP_HEX_LENGTH,
          false /*hex_print*/, fd);
      if (OB_SUCC(ret)) {
        ret = dumper_.init_with_validated_reader(reader_);
      }
      return ret;
    }

    ObSSTableDataBlockReader reader_;
    ObInvIdxSSTableDataBlockDumper dumper_;
  };

  void prepare_normal_schema(ObTableSchema &schema)
  {
    ObColumnSchemaV2 column;
    schema.reset();
    OK(schema.set_table_name("test_dumpsst_normal_macro_block"));
    schema.set_tenant_id(1);
    schema.set_tablegroup_id(1);
    schema.set_database_id(1);
    schema.set_table_id(3001);
    schema.set_rowkey_column_num(NORMAL_ROWKEY_COLUMN_CNT);
    schema.set_max_used_column_id(NORMAL_COLUMN_CNT);
    schema.set_block_size(16 * 1024);
    schema.set_compress_func_name("none");
    schema.set_row_store_type(FLAT_ROW_STORE);
    schema.set_micro_index_clustered(true);

    ObObjType obj_types[2] = {ObInt32Type, ObFloatType};
    int64_t rowkey_pos = 1;
    char name[OB_MAX_FILE_NAME_LENGTH];
    for (int64_t i = 0; i < NORMAL_COLUMN_CNT; ++i) {
      column.reset();
      column.set_table_id(schema.get_table_id());
      column.set_column_id(i + OB_APP_MIN_COLUMN_ID);
      snprintf(name, sizeof(name), "test%020ld", i);
      OK(column.set_column_name(name));
      column.set_data_type(obj_types[i % 2]);
      column.set_collation_type(CS_TYPE_BINARY);
      column.set_data_length(1);
      column.set_rowkey_position(rowkey_pos++);
      column.set_order_in_rowkey(0 == i % 2 ? ObOrderType::ASC : ObOrderType::DESC);
      OK(schema.add_column(column));
    }
  }

  void convert_to_normal_multi_version_row(
      const ObDatumRow &src,
      ObDatumRow &dst)
  {
    for (int64_t i = 0; i < NORMAL_ROWKEY_COLUMN_CNT; ++i) {
      dst.storage_datums_[i] = src.storage_datums_[i];
    }
    dst.storage_datums_[NORMAL_ROWKEY_COLUMN_CNT].set_int(-NORMAL_SNAPSHOT_VERSION);
    dst.storage_datums_[NORMAL_ROWKEY_COLUMN_CNT + 1].set_int(0);
    for (int64_t i = NORMAL_ROWKEY_COLUMN_CNT; i < NORMAL_COLUMN_CNT; ++i) {
      dst.storage_datums_[i + 2] = src.storage_datums_[i];
    }
    dst.count_ = NORMAL_COLUMN_CNT + 2;
    dst.row_flag_ = DF_INSERT;
    dst.mvcc_row_flag_.set_last_multi_version_row(true);
  }

  void build_normal_sstable_macro_block(const char *&buf, int64_t &size)
  {
    ObTableSchema schema;
    prepare_normal_schema(schema);
    ObWholeDataStoreDesc data_desc;
    ObSSTableIndexBuilder sstable_builder(false /*not need writer buffer*/);
    const int64_t snapshot_version = ObTimeUtility::fast_current_time();
    SCN end_scn;
    end_scn.convert_for_tx(snapshot_version);
    OK(data_desc.init(false /*is_ddl*/, schema, ObLSID(1), ObTabletID(1), ObMergeType::MAJOR_MERGE,
        snapshot_version, DATA_CURRENT_VERSION, schema.get_micro_index_clustered(),
        0 /*transfer_epoch*/, 0 /*concurrent_cnt*/, end_scn, end_scn, nullptr,
        0 /*table_cg_idx*/, compaction::ObExecMode::EXEC_MODE_LOCAL, false /*need_submit_io*/));
    OK(sstable_builder.init(data_desc.get_desc()));
    data_desc.get_desc().sstable_index_builder_ = &sstable_builder;

    ObRowGenerate row_generate;
    OK(row_generate.init(schema, &allocator_));
    ObMacroDataSeq data_seq(0);
    data_seq.set_parallel_degree(0);
    ObMacroSeqParam seq_param;
    seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
    seq_param.start_ = data_seq.macro_data_seq_;
    ObSSTablePrivateObjectCleaner cleaner;
    const ObPreWarmerParam pre_warm_param(MEM_PRE_WARM);
    CaptureMacroBlockFlusher flusher(allocator_);
    CaptureMacroBlockWriter writer;
    OK(writer.open(data_desc.get_desc(), data_seq.get_parallel_idx(), seq_param,
        pre_warm_param, cleaner, nullptr, nullptr, nullptr));
    writer.custom_macro_flusher_ = &flusher;

    ObDatumRow src;
    ObDatumRow multi_row;
    ObArenaAllocator row_allocator;
    OK(src.init(row_allocator, NORMAL_COLUMN_CNT));
    OK(multi_row.init(allocator_, NORMAL_COLUMN_CNT + 2));
    for (int64_t i = 0; i < 1000; ++i) {
      OK(row_generate.get_next_row(i, src));
      convert_to_normal_multi_version_row(src, multi_row);
      OK(writer.append_row(multi_row));
    }
    OK(writer.close());
    size = flusher.size_;
    ASSERT_GT(size, 0);
    char *copy = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, copy);
    ASSERT_NE(nullptr, flusher.buf_);
    MEMCPY(copy, flusher.buf_, size);
    refresh_common_payload_checksum(copy, size);
    buf = copy;
  }

  void build_inv_sstable_macro_block(
      const bool has_pos_list,
      const int64_t row_cnt,
      char *&buf,
      int64_t &size)
  {
    ASSERT_GT(row_cnt, 0);

    prepare_inv_idx_schema(table_schema_, has_pos_list);
    OK(prepare_inv_idx_row_desc(table_schema_, inv_idx_row_desc_));
    ASSERT_TRUE(inv_idx_row_desc_.is_valid());
    ASSERT_EQ(has_pos_list, inv_idx_row_desc_.pos_list_col_idx_ >= 0);
    prepare_desc();

    ObInvIdxMacroBlockWriter writer;
    ObPreWarmerParam pre_warm_param(PRE_WARM_TYPE_NONE);
    ObSSTablePrivateObjectCleaner cleaner;
    OK(writer.open(inv_idx_row_desc_, whole_desc_.get_desc(), 0 /*parallel_idx*/,
        seq_param_, pre_warm_param, cleaner, nullptr /*callback*/));
    ASSERT_TRUE(writer.is_opened());

    ObString position_list;
    ObArenaAllocator position_list_allocator;
    common::ObSEArray<int64_t, 2> positions;
    char *position_list_buf = nullptr;
    int64_t position_list_size = 0;
    if (has_pos_list) {
      share::ObFTSPositionListStore position_list_store;
      OK(positions.push_back(10));
      OK(positions.push_back(13));
      OK(position_list_store.encode_and_serialize(
          position_list_allocator, positions, position_list_buf, position_list_size));
      ASSERT_NE(nullptr, position_list_buf);
      ASSERT_GT(position_list_size, 0);
      ASSERT_LE(position_list_size, INT32_MAX);
      position_list.assign_ptr(position_list_buf, static_cast<int32_t>(position_list_size));
    }

    ObInvIdxRowFeeder feeder(inv_idx_row_desc_, mv_col_cnt_, position_list);
    OK(feeder.feed_n(writer, row_cnt));
    OK(writer.close());
    ASSERT_FALSE(writer.is_opened());

    size = writer.macro_block_.get_data_size();
    ASSERT_GT(size, 0);
    char *copy = static_cast<char *>(allocator_.alloc(size));
    ASSERT_NE(nullptr, copy);
    MEMCPY(copy, writer.macro_block_.get_data_buf(), size);
    buf = copy;
  }

  void get_macro_header(char *buf, const int64_t size, ObSSTableMacroBlockHeader &macro_header)
  {
    ObMacroBlockCommonHeader common_header;
    int64_t pos = 0;
    OK(common_header.deserialize(buf, size, pos));
    OK(macro_header.deserialize(buf, size, pos));
  }

  void get_inv_micro_blocks(
      char *buf,
      const int64_t size,
      ValidatedInvDumperHarness &harness)
  {
    OK(harness.init(buf, size));
    ObInvIdxSSTableDataBlockDumper &dumper = harness.dumper_;
    bool is_inv_idx = false;
    OK(dumper.is_inv_idx_macro_block(is_inv_idx));
    ASSERT_TRUE(is_inv_idx);
    OK(dumper.collect_inv_idx_micro_blocks());
    ASSERT_GT(dumper.posting_blocks_.count(), 0);
  }

  void locate_first_agg_payload(char *buf, const int64_t size, char *&payload, int64_t &agg_size)
  {
    payload = nullptr;
    agg_size = 0;
    ValidatedInvDumperHarness harness;
    OK(harness.init(buf, size));
    ObInvIdxSSTableDataBlockDumper &dumper = harness.dumper_;
    ObMicroBlockData index_block;
    OK(dumper.decode_index_micro_block_(index_block));
    ObMicroBlockReaderHelper reader_helper;
    ObIMicroBlockReader *reader = nullptr;
    OK(reader_helper.init(allocator_));
    OK(reader_helper.get_reader(*index_block.get_micro_header(), reader));
    OK(reader->init(index_block, nullptr /*datum_utils*/));
    ObDatumRow row;
    OK(row.init(allocator_, index_block.get_micro_header()->column_count_));
    OK(reader->get_row(0, row));
    ObIndexBlockRowParser parser;
    const ObIndexBlockRowHeader *row_header = nullptr;
    const char *decoded_agg = nullptr;
    OK(parser.init(index_block.get_micro_header()->rowkey_column_count_, row));
    OK(parser.get_header(row_header));
    ASSERT_TRUE(row_header->is_inverted_index());
    OK(parser.get_agg_row(decoded_agg, agg_size));
    ASSERT_GE(agg_size, static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader)));
    int64_t idx_off = -1;
    int64_t idx_size = -1;
    OK(dumper.probe_locate_inv_idx_micro_block_(idx_off, idx_size));
    ASSERT_GT(idx_off, 0);
    ASSERT_GT(idx_size, 0);
    const char *found = find_bytes(buf + idx_off, idx_size, decoded_agg, agg_size);
    ASSERT_NE(nullptr, found);
    payload = const_cast<char *>(found);
  }

  void locate_macro_meta_locator(
      char *buf,
      const int64_t size,
      int64_t &locator_offset,
      int64_t &locator_size,
      const char *&locator_replacement,
      int64_t &meta_micro_offset,
      int64_t &meta_micro_size)
  {
    locator_offset = -1;
    locator_size = 0;
    locator_replacement = nullptr;
    meta_micro_offset = -1;
    meta_micro_size = -1;
    ObMacroBlockCommonHeader common_header;
    ObSSTableMacroBlockHeader macro_header;
    int64_t pos = 0;
    OK(common_header.deserialize(buf, size, pos));
    OK(macro_header.deserialize(buf, size, pos));
    meta_micro_offset = macro_header.fixed_header_.meta_block_offset_;
    meta_micro_size = macro_header.fixed_header_.meta_block_size_;
    ASSERT_GT(meta_micro_offset, 0);
    ASSERT_GT(meta_micro_size, 0);

    ObMicroBlockBareIterator micro_iter;
    ObDataMacroBlockMeta *macro_meta = nullptr;
    OK(micro_iter.open(buf, size, false /*need_check_data_integrity*/, true /*need_deserialize*/));
    OK(micro_iter.get_macro_meta(macro_meta, allocator_));
    ASSERT_NE(nullptr, macro_meta);
    const ObDataBlockMetaVal &meta_val = macro_meta->get_meta_val();

    ObMacroBlockReader macro_block_reader;
    ObMicroBlockData meta_block;
    bool is_compressed = false;
    OK(macro_block_reader.decrypt_and_decompress_data(macro_header,
        buf + meta_micro_offset,
        meta_micro_size,
        false /*need deep copy*/,
        meta_block,
        is_compressed));
    ASSERT_FALSE(is_compressed);
    ObMicroBlockReaderHelper reader_helper;
    ObIMicroBlockReader *reader = nullptr;
    OK(reader_helper.init(allocator_));
    OK(reader_helper.get_reader(*meta_block.get_micro_header(), reader));
    OK(reader->init(meta_block, nullptr /*datum_utils*/));
    ObDatumRow row;
    OK(row.init(allocator_, macro_header.fixed_header_.rowkey_column_count_ + 1));
    OK(reader->get_row(0, row));
    ObString meta_val_buf = row.storage_datums_[row.get_column_count() - 1].get_string();
    ASSERT_GE(meta_val_buf.ptr(), buf);
    ASSERT_LE(meta_val_buf.ptr() + meta_val_buf.length(), buf + size);

    const int64_t data_version = DATA_CURRENT_VERSION;
    const int64_t old_serialize_size = meta_val.get_serialize_size(data_version);
    ASSERT_EQ(old_serialize_size, meta_val_buf.length());
    char *old_serialized = static_cast<char *>(allocator_.alloc(old_serialize_size));
    char *new_serialized = static_cast<char *>(allocator_.alloc(old_serialize_size));
    ASSERT_NE(nullptr, old_serialized);
    ASSERT_NE(nullptr, new_serialized);
    int64_t serialize_pos = 0;
    OK(meta_val.serialize(old_serialized, old_serialize_size, serialize_pos, data_version));
    ASSERT_EQ(old_serialize_size, serialize_pos);
    ASSERT_EQ(0, MEMCMP(meta_val_buf.ptr(), old_serialized, old_serialize_size));

    ObDataBlockMetaVal new_meta_val(allocator_);
    OK(new_meta_val.assign(meta_val));
    new_meta_val.block_size_ += 1;
    ASSERT_EQ(old_serialize_size, new_meta_val.get_serialize_size(data_version));
    serialize_pos = 0;
    OK(new_meta_val.serialize(new_serialized, old_serialize_size, serialize_pos, data_version));
    ASSERT_EQ(old_serialize_size, serialize_pos);

    int64_t diff_begin = 0;
    while (diff_begin < old_serialize_size
        && old_serialized[diff_begin] == new_serialized[diff_begin]) {
      ++diff_begin;
    }
    int64_t diff_end = old_serialize_size;
    while (diff_end > diff_begin
        && old_serialized[diff_end - 1] == new_serialized[diff_end - 1]) {
      --diff_end;
    }
    ASSERT_LT(diff_begin, diff_end);
    locator_offset = meta_val_buf.ptr() + diff_begin - buf;
    locator_size = diff_end - diff_begin;
    locator_replacement = new_serialized + diff_begin;
  }

  void make_first_posting_micro_block_compressed_and_corrupt(char *buf, const int64_t size)
  {
    ValidatedInvDumperHarness harness;
    get_inv_micro_blocks(buf, size, harness);
    ObInvIdxSSTableDataBlockDumper &dumper = harness.dumper_;
    const ObInvIdxDumpMicroBlockInfo &posting = dumper.posting_blocks_.at(0);
    ObMicroBlockHeader *header = reinterpret_cast<ObMicroBlockHeader *>(buf + posting.offset_);
    const int64_t header_size = header->header_size_;
    const int64_t original_payload_size = header->data_zlength_;
    ASSERT_GT(original_payload_size, 0);
    ASSERT_GE(posting.size_, header_size + original_payload_size);

    // Present a checksum-valid but malformed LZ4 stream so the dump path reaches
    // materialization and reports the decompression failure deterministically.
    for (int64_t i = 0; i < original_payload_size; ++i) {
      buf[posting.offset_ + header_size + i] =
          static_cast<char>(0xA5 ^ static_cast<unsigned char>(i));
    }
    header->data_length_ = static_cast<int32_t>(original_payload_size + 1);
    header->data_zlength_ = static_cast<int32_t>(original_payload_size);
    header->compressor_type_ = static_cast<uint8_t>(common::ObCompressorType::LZ4_COMPRESSOR);
    header->data_checksum_ = ob_crc64_sse42(0, buf + posting.offset_ + header_size, original_payload_size);
    header->set_header_checksum();
    refresh_macro_checksums(buf, size);
  }

  void refresh_index_micro_checksum(char *buf, const int64_t size)
  {
    ObSSTableMacroBlockHeader macro_header;
    get_macro_header(buf, size, macro_header);
    refresh_micro_payload_checksum(
        buf + macro_header.fixed_header_.idx_block_offset_,
        macro_header.fixed_header_.idx_block_size_);
  }

  void refresh_macro_checksums(char *buf, const int64_t size)
  {
    refresh_common_payload_checksum(buf, size);
  }

  ObInvIdxDumpValidationResult dump_macro_block(
      const char *buf,
      const int64_t size,
      const bool expect_dump_success = true,
      std::string *output = nullptr,
      const bool check_inv_idx_signal = true)
  {
    MemStreamCapture capture;
    ValidatedInvDumperHarness harness;
    EXPECT_EQ(OB_SUCCESS, harness.init(buf, size, capture.open()));
    ObInvIdxSSTableDataBlockDumper &dumper = harness.dumper_;
    if (check_inv_idx_signal) {
      bool is_inv_idx = false;
      EXPECT_EQ(OB_SUCCESS, dumper.is_inv_idx_macro_block(is_inv_idx));
      EXPECT_TRUE(is_inv_idx);
    }
    const int dump_ret = dumper.dump();
    if (expect_dump_success) {
      EXPECT_EQ(OB_SUCCESS, dump_ret);
    } else {
      EXPECT_NE(OB_SUCCESS, dump_ret);
    }
    const std::string captured_output = capture.str();
    if (!expect_dump_success) {
      EXPECT_TRUE(captured_output.empty());
    }
    if (nullptr != output) {
      *output = captured_output;
    }
    return dumper.get_validation_result();
  }

  std::string dump_single_macro_block(char *buf, const int64_t size, int &ret)
  {
    return capture_stderr([&]() { return dump_single_macro_block_to_stderr(buf, size); }, ret);
  }

};

TEST_F(TestInvIdxSSTableDumpsst, dump_inv_sstable_with_position_list)
{
  char *buf = nullptr;
  int64_t size = 0;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size);

  ASSERT_TRUE(result.all_ok());
  ASSERT_TRUE(result.is_ok(CHECK_POSTING_VALUE_LOCATOR));
  ASSERT_TRUE(result.is_ok(CHECK_BLOCK_IDX_ALIGNMENT));
  ASSERT_TRUE(result.is_ok(CHECK_ROWKEY_MAX_DOC_ID));
  ASSERT_GT(result.posting_micro_block_count_, 0);
  ASSERT_GT(result.posting_value_micro_block_count_, 0);
}

TEST_F(TestInvIdxSSTableDumpsst, dump_inv_sstable_without_position_list)
{
  char *buf = nullptr;
  int64_t size = 0;
  build_inv_sstable_macro_block(false /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size);

  ASSERT_TRUE(result.all_ok());
  ASSERT_TRUE(result.is_ok(CHECK_POSTING_VALUE_LOCATOR));
  ASSERT_TRUE(result.is_ok(CHECK_ROWKEY_MAX_DOC_ID));
  ASSERT_EQ(0, result.posting_value_micro_block_count_);
  ASSERT_EQ(0, result.checks_[CHECK_POSTING_VALUE_LOCATOR].checked_count_);
  ASSERT_EQ(0, result.checks_[CHECK_POSTING_VALUE_LOCATOR].failure_count_);
  ASSERT_EQ(result.agg_entry_count_,
      result.checks_[CHECK_BLOCK_IDX_ALIGNMENT].checked_count_);
}

TEST_F(TestInvIdxSSTableDumpsst, probe_reuses_decoded_index_block)
{
  char *buf = nullptr;
  int64_t size = 0;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);

  ValidatedInvDumperHarness harness;
  OK(harness.init(buf, size));
  ObInvIdxSSTableDataBlockDumper &dumper = harness.dumper_;
  bool is_inv_idx = false;
  OK(dumper.is_inv_idx_macro_block(is_inv_idx));
  ASSERT_TRUE(is_inv_idx);
  ASSERT_NE(nullptr, dumper.cached_index_block_);

  const char *cached_buf = dumper.cached_index_block_->get_buf();
  ObMicroBlockData index_block;
  OK(dumper.decode_index_micro_block_(index_block));
  ASSERT_EQ(cached_buf, index_block.get_buf());
}

TEST_F(TestInvIdxSSTableDumpsst, dump_single_normal_macro_block_falls_back_to_legacy_path)
{
  const char *buf = nullptr;
  int64_t size = 0;
  build_normal_sstable_macro_block(buf, size);

  int ret = OB_SUCCESS;
  const std::string output = dump_single_macro_block(const_cast<char *>(buf), size, ret);

  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_FALSE(output.empty());
  ASSERT_NE(std::string::npos, output.find("Data Micro Block"));
  ASSERT_EQ(std::string::npos, output.find("Inverted Index Micro Block"));
}

TEST_F(TestInvIdxSSTableDumpsst, payload_header_collision_without_main_signal_uses_legacy_path)
{
  const char *raw = nullptr;
  int64_t size = 0;
  build_normal_sstable_macro_block(raw, size);
  char *buf = static_cast<char *>(allocator_.alloc(size));
  ASSERT_NE(nullptr, buf);
  MEMCPY(buf, raw, size);

  ObSSTableMacroBlockHeader macro_header;
  get_macro_header(buf, size, macro_header);
  char *first_micro = buf + macro_header.fixed_header_.micro_block_data_offset_;
  ObMicroBlockHeader *micro_header = reinterpret_cast<ObMicroBlockHeader *>(first_micro);
  ASSERT_GT(micro_header->data_zlength_, static_cast<int32_t>(sizeof(ObInvIdxMicroBlockPayloadHeader)));
  micro_header->rowkey_column_count_ = 1;
  ObInvIdxMicroBlockPayloadHeader payload_header;
  payload_header.block_count_ = 1;
  payload_header.blocks_offset_ = sizeof(ObInvIdxMicroBlockPayloadHeader);
  payload_header.block_offsets_offset_ = payload_header.blocks_offset_;
  payload_header.total_size_ = payload_header.block_offsets_offset_
      + 2 * ObInvIdxMicroBlockPayloadHeader::BLOCK_OFFSET_SIZE;
  ASSERT_TRUE(payload_header.is_valid());
  MEMCPY(first_micro + micro_header->header_size_, &payload_header, sizeof(payload_header));
  refresh_micro_payload_checksum(first_micro, micro_header->header_size_ + micro_header->data_zlength_);
  refresh_macro_checksums(buf, size);

  ValidatedInvDumperHarness harness;
  ObInvIdxSSTableDataBlockDumper &dumper = harness.dumper_;
  bool is_inv_idx = true;
  OK(harness.init(buf, size));
  OK(dumper.is_inv_idx_macro_block(is_inv_idx));
  ASSERT_FALSE(is_inv_idx);
}

TEST_F(TestInvIdxSSTableDumpsst, corrupted_posting_payload_checksum_reports_context)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  ValidatedInvDumperHarness harness;
  get_inv_micro_blocks(buf, size, harness);
  ObInvIdxSSTableDataBlockDumper &dumper = harness.dumper_;
  const int64_t posting_block_count = dumper.posting_blocks_.count();
  const ObInvIdxDumpMicroBlockInfo &posting = dumper.posting_blocks_.at(0);
  ObMicroBlockHeader *header = reinterpret_cast<ObMicroBlockHeader *>(buf + posting.offset_);
  buf[posting.offset_ + header->header_size_] ^= 0x01;
  refresh_macro_checksums(buf, size);

  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size, false);

  ASSERT_GT(result.checks_[CHECK_MICRO_BLOCK_CHECKSUM].failure_count_, 0);
  ASSERT_GT(result.checks_[CHECK_MICRO_BLOCK_MATERIALIZE].failure_count_, 0);
  ASSERT_LT(result.posting_micro_block_count_, posting_block_count);
}

TEST_F(TestInvIdxSSTableDumpsst, corrupted_agg_posting_value_locator_reports_failure)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  int64_t agg_size = 0;
  char *agg_buf = nullptr;
  locate_first_agg_payload(buf, size, agg_buf, agg_size);
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(agg_buf);
  header->posting_value_offset_ = static_cast<uint32_t>(size + 4096);
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size, false);

  ASSERT_GT(result.checks_[CHECK_POSTING_VALUE_LOCATOR].failure_count_, 0);
}

TEST_F(TestInvIdxSSTableDumpsst, corrupted_agg_block_idx_reports_block_count_failure)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  int64_t agg_size = 0;
  char *agg_buf = nullptr;
  locate_first_agg_payload(buf, size, agg_buf, agg_size);
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(agg_buf);
  ASSERT_GT(header->logical_block_count_, 0);
  ObInvIdxAggDataEntry *entries =
      reinterpret_cast<ObInvIdxAggDataEntry *>(agg_buf + sizeof(ObInvIdxAggDataHeader));
  entries[0].block_idx_ = UINT16_MAX;
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size, false);

  ASSERT_GT(result.checks_[CHECK_BLOCK_IDX_ALIGNMENT].failure_count_, 0);
}

TEST_F(TestInvIdxSSTableDumpsst, corrupted_posting_value_row_count_reports_mismatch)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  ValidatedInvDumperHarness harness;
  get_inv_micro_blocks(buf, size, harness);
  ObInvIdxSSTableDataBlockDumper &dumper = harness.dumper_;
  ASSERT_GT(dumper.value_blocks_.count(), 0);
  const ObInvIdxDumpMicroBlockInfo &value = dumper.value_blocks_.at(0);
  ObInvIdxMicroBlockReader reader;
  OK(reader.init_payload(value.payload_buf_, value.payload_size_));
  const char *logical_buf = nullptr;
  int64_t logical_size = 0;
  OK(reader.get_payload(0, logical_buf, logical_size));
  const int64_t logical_offset = value.offset_
      + reinterpret_cast<const ObMicroBlockHeader *>(buf + value.offset_)->header_size_
      + (logical_buf - value.payload_buf_);
  ASSERT_GE(logical_offset, value.offset_);
  ASSERT_LE(logical_offset + logical_size, value.offset_ + value.size_);
  ObPostingValueBlockHeader *value_header =
      reinterpret_cast<ObPostingValueBlockHeader *>(buf + logical_offset);
  value_header->row_count_ = value_header->row_count_ + 1;
  refresh_micro_payload_checksum(buf + value.offset_, value.size_);
  refresh_macro_checksums(buf, size);

  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size, false);

  ASSERT_GT(result.checks_[CHECK_POSTING_VALUE_ROW_OFFSET_COVERAGE].failure_count_, 0);
}

TEST_F(TestInvIdxSSTableDumpsst, corrupted_frontier_offset_reports_bounds_failure)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  int64_t agg_size = 0;
  char *agg_buf = nullptr;
  locate_first_agg_payload(buf, size, agg_buf, agg_size);
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(agg_buf);
  ASSERT_GT(header->logical_block_count_, 0);
  ObInvIdxAggDataEntry *entries =
      reinterpret_cast<ObInvIdxAggDataEntry *>(agg_buf + sizeof(ObInvIdxAggDataHeader));
  entries[0].frontier_count_ = 1;
  entries[0].frontier_offset_ = UINT16_MAX;
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size, false);

  ASSERT_GT(result.checks_[CHECK_FRONTIER_BOUNDS].failure_count_, 0);
  ASSERT_EQ(OB_INVALID_DATA, result.checks_[CHECK_FRONTIER_BOUNDS].first_ret_);
  ASSERT_EQ(OB_INVALID_DATA, result.checks_[CHECK_FRONTIER_POOL_COVERAGE].first_ret_);
  ASSERT_EQ(1, result.checks_[CHECK_PARSE_STATUS].checked_count_);
  ASSERT_EQ(1, result.checks_[CHECK_PARSE_STATUS].failure_count_);
}

TEST_F(TestInvIdxSSTableDumpsst, corrupted_agg_header_counts_single_parse_failure)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  int64_t agg_size = 0;
  char *agg_buf = nullptr;
  locate_first_agg_payload(buf, size, agg_buf, agg_size);
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(agg_buf);
  header->version_ = UINT8_MAX;
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  // Classification uses only the explicit index-row bit; the malformed agg is
  // decoded once by the formal dump and contributes one parse failure.
  MemStreamCapture capture;
  ValidatedInvDumperHarness harness;
  OK(harness.init(buf, size, capture.open()));
  ASSERT_EQ(OB_VERSION_NOT_MATCH, harness.dumper_.dump());
  ASSERT_TRUE(capture.str().empty());
  const ObInvIdxDumpValidationResult &result = harness.dumper_.get_validation_result();

  ASSERT_EQ(1, result.checks_[CHECK_PARSE_STATUS].checked_count_);
  ASSERT_EQ(1, result.checks_[CHECK_PARSE_STATUS].failure_count_);
}

TEST_F(TestInvIdxSSTableDumpsst, macro_meta_locator_mismatch_reports_failure)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  int64_t locator_offset = -1;
  int64_t locator_size = 0;
  const char *locator_replacement = nullptr;
  int64_t meta_micro_offset = -1;
  int64_t meta_micro_size = -1;
  locate_macro_meta_locator(
      buf, size, locator_offset, locator_size, locator_replacement, meta_micro_offset, meta_micro_size);
  ASSERT_GT(locator_offset, 0);
  ASSERT_GT(locator_size, 0);
  ASSERT_NE(nullptr, locator_replacement);
  MEMCPY(buf + locator_offset, locator_replacement, locator_size);
  refresh_micro_payload_checksum(buf + meta_micro_offset, meta_micro_size);
  refresh_macro_checksums(buf, size);

  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size, false);

  ASSERT_GT(result.checks_[CHECK_MACRO_META_LOCATOR].failure_count_, 0);
}

TEST_F(TestInvIdxSSTableDumpsst, compressed_payload_corruption_is_reported_without_crash)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 128 /*row_cnt*/, buf, size);
  make_first_posting_micro_block_compressed_and_corrupt(buf, size);

  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size, false);

  ASSERT_GT(result.checks_[CHECK_MICRO_BLOCK_MATERIALIZE].failure_count_, 0);
}

TEST_F(TestInvIdxSSTableDumpsst, logical_block_coverage_reports_isolated_block)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 5000 /*row_cnt*/, buf, size);
  int64_t agg_size = 0;
  char *agg_buf = nullptr;
  locate_first_agg_payload(buf, size, agg_buf, agg_size);
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(agg_buf);
  ASSERT_GE(header->logical_block_count_, 2);
  ObInvIdxAggDataEntry *entries =
      reinterpret_cast<ObInvIdxAggDataEntry *>(agg_buf + sizeof(ObInvIdxAggDataHeader));
  const int64_t original_entries_size =
      header->logical_block_count_ * sizeof(ObInvIdxAggDataEntry);
  char *original_frontier_pool =
      agg_buf + sizeof(ObInvIdxAggDataHeader) + original_entries_size;
  const int64_t frontier_pool_size =
      header->total_size_ - sizeof(ObInvIdxAggDataHeader) - original_entries_size;
  const ObInvIdxAggDataEntry &removed_entry = entries[header->logical_block_count_ - 1];
  const int64_t removed_frontier_size =
      removed_entry.frontier_count_ * sizeof(ObInvIdxBM25ParamPair);
  ASSERT_GE(frontier_pool_size, 0);
  ASSERT_EQ(frontier_pool_size,
      removed_entry.frontier_offset_ + removed_frontier_size);
  MEMMOVE(original_frontier_pool - sizeof(ObInvIdxAggDataEntry),
      original_frontier_pool, frontier_pool_size - removed_frontier_size);
  --header->logical_block_count_;
  header->total_size_ -= sizeof(ObInvIdxAggDataEntry) + removed_frontier_size;
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  const ObInvIdxDumpValidationResult result = dump_macro_block(buf, size, false);

  ASSERT_GT(result.checks_[CHECK_LOGICAL_BLOCK_COVERAGE].failure_count_, 0);
}

TEST_F(TestInvIdxSSTableDumpsst, frontier_positive_branch_and_failures_are_recorded)
{
  int64_t size = 0;
  char *buf = nullptr;
  build_inv_sstable_macro_block(true /*has_pos_list*/, 5000 /*row_cnt*/, buf, size);
  int64_t agg_size = 0;
  char *agg_buf = nullptr;
  locate_first_agg_payload(buf, size, agg_buf, agg_size);
  ObInvIdxAggDataHeader *header = reinterpret_cast<ObInvIdxAggDataHeader *>(agg_buf);
  ASSERT_GT(header->logical_block_count_, 0);
  ObInvIdxAggDataEntry *entries =
      reinterpret_cast<ObInvIdxAggDataEntry *>(agg_buf + sizeof(ObInvIdxAggDataHeader));
  const uint32_t entries_end = static_cast<uint32_t>(sizeof(ObInvIdxAggDataHeader)
      + header->logical_block_count_ * sizeof(ObInvIdxAggDataEntry));
  for (int64_t i = 0; i < header->logical_block_count_; ++i) {
    entries[i].frontier_count_ = 0;
    entries[i].frontier_offset_ = 0;
  }
  header->total_size_ = entries_end;
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  ObInvIdxDumpValidationResult result = dump_macro_block(buf, size);
  ASSERT_TRUE(result.is_ok(CHECK_FRONTIER_BOUNDS));
  ASSERT_TRUE(result.is_ok(CHECK_FRONTIER_POOL_COVERAGE));

  ASSERT_GE(agg_size, static_cast<int64_t>(entries_end + sizeof(ObInvIdxBM25ParamPair)));
  header->total_size_ = entries_end + sizeof(ObInvIdxBM25ParamPair);
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  result = dump_macro_block(buf, size, false);
  ASSERT_TRUE(result.is_ok(CHECK_FRONTIER_BOUNDS));
  ASSERT_GT(result.checks_[CHECK_FRONTIER_POOL_COVERAGE].failure_count_, 0);
  ASSERT_EQ(OB_INVALID_DATA, result.checks_[CHECK_FRONTIER_POOL_COVERAGE].first_ret_);
  ASSERT_EQ(1, result.checks_[CHECK_PARSE_STATUS].failure_count_);
  ASSERT_EQ(OB_INVALID_DATA, result.checks_[CHECK_PARSE_STATUS].first_ret_);

  entries[0].frontier_count_ = 1;
  entries[0].frontier_offset_ = 0;
  ObInvIdxBM25ParamPair *pair = reinterpret_cast<ObInvIdxBM25ParamPair *>(
      agg_buf + entries_end);
  pair->token_freq_ = 7;
  pair->doc_length_ = 11;
  header->total_size_ = entries_end + sizeof(ObInvIdxBM25ParamPair);
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  result = dump_macro_block(buf, size);
  ASSERT_TRUE(result.is_ok(CHECK_FRONTIER_BOUNDS));
  ASSERT_TRUE(result.is_ok(CHECK_FRONTIER_POOL_COVERAGE));

  ASSERT_GE(header->logical_block_count_, 2);
  entries[1].frontier_count_ = 1;
  entries[1].frontier_offset_ = 0;
  header->total_size_ = entries_end + sizeof(ObInvIdxBM25ParamPair);
  refresh_index_micro_checksum(buf, size);
  refresh_macro_checksums(buf, size);

  result = dump_macro_block(buf, size, false);
  ASSERT_GT(result.checks_[CHECK_FRONTIER_POOL_COVERAGE].failure_count_, 0);
  ASSERT_EQ(OB_INVALID_DATA, result.checks_[CHECK_FRONTIER_POOL_COVERAGE].first_ret_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_inv_idx_sstable_dumpsst.log*");
  OB_LOGGER.set_file_name("test_inv_idx_sstable_dumpsst.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::lib::set_memory_limit(40UL << 30);
  signal(49, SIG_IGN);
  return RUN_ALL_TESTS();
}
