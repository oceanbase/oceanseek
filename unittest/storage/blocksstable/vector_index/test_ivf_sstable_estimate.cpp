/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <signal.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <vector>

#include "test_disk_bbq_common.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/column_store/ob_column_store_util.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_reader.h"
#include "storage/blocksstable/ob_micro_block_cache.h"
#include "storage/blocksstable/ob_micro_block_row_scanner.h"
#include "storage/blocksstable/ob_micro_block_writer.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/blocksstable/vector_index/ob_ivf_sstable.h"
#include "storage/schema_utils.h"
#include "share/cache/ob_kv_storecache.h"
// The test fixture (TestIVFSSTableWriter) provides the storage environment, but main() itself
// needs the complete MockTenantModuleEnv definition below.
#include "mtlenv/mock_tenant_module_env.h"
#undef private
#undef protected

namespace oceanbase
{
namespace unittest
{

// Async IVF index sstable estimation unit tests.
//
// The class level entry points (ObIndexBlockScanEstimator::estimate_row_count /
// estimate_block_count) take the index read info from a real tablet, so this file covers the
// measurement core instead: the static ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count /
// estimate_ivf_sstable_result, and the ObMicroBlockRowScanner guards that keep FastScan encoded
// low precision vector data blocks (ObMicroBlockData::IVF_FASTSCAN_BLOCK, which carries no
// row-format micro header) from being opened. The SQL end to end coverage of the class level entry
// points lives in
// tools/deploy/mysql_test/test_suite/vector_index/t/async_ivf_direct_select_estimate.test.
class TestIvfSstableEstimate : public TestIVFSSTableWriter
{
public:
  // constexpr on purpose: gtest's ASSERT_EQ binds the constants by reference, which odr-uses
  // them, and a plain in-class static const integral would need an out-of-line definition. The
  // constexpr static data members are implicitly inline (C++17) and always have a definition.
  static constexpr int64_t TEST_DIM = 64;
  static constexpr int64_t VECTOR_CNT = 64;
  // One centroid row (nlist = 1) plus VECTOR_CNT low precision vector rows, no raw vector rows.
  static constexpr int64_t TOTAL_ROW_CNT = 1 + VECTOR_CNT;
  // FastScan encoded block size reported by the crash scene: the transformed low precision block
  // held in the block cache never has a row-format micro header.
  static constexpr int64_t FASTSCAN_BLOCK_SIZE = 14880;
  static constexpr int64_t NORMAL_BLOCK_ROW_CNT = 5;

  // Storage of the start key datums built by build_low_precision_tail_range(). A datum rowkey
  // does not own its datums, so the range keeps pointing at them, and the estimator compares that
  // range border against the index row keys well after the helper has returned. Declaring the
  // array inside the helper would leave the range pointing at reused stack memory, and the
  // comparison would then dereference whatever the caller frames left behind (a null datum
  // pointer with the null flag clear, i.e. a crash inside ObNullSafeDatumTCCmp). Keeping the
  // datums in the fixture also keeps their inline buffers (ObStorageDatum::buf_) at a stable
  // address for the whole test.
  ObStorageDatum tail_range_start_datums_[3];

  void SetUp() override
  {
    TestIVFSSTableWriter::SetUp();
  }

  void TearDown() override
  {
    TestIVFSSTableWriter::TearDown();
  }

  // Build a baseline async IVF sstable: 1 centroid row + VECTOR_CNT low precision vector rows.
  // Raw vector rows are skipped so the row count of the sstable is deterministic.
  int prepare_ivf_sstable(ObTableReadInfo &read_info)
  {
    int ret = OB_SUCCESS;
    std::vector<float> vectors(static_cast<int64_t>(VECTOR_CNT) * TEST_DIM, 0.5F);
    std::vector<int64_t> cids(VECTOR_CNT, 0);
    std::vector<int64_t> vids(VECTOR_CNT, 0);
    std::vector<float> centroid(TEST_DIM, 0.25F);
    char flip[4 * TEST_DIM / CHAR_BIT] = {0};
    for (int64_t i = 0; i < VECTOR_CNT; ++i) {
      vectors[i * TEST_DIM + static_cast<int64_t>(i % TEST_DIM)] = 1.0F;
      vids[i] = i;
    }
    if (OB_FAIL(prepare_write_env(0, CS_ENCODING_ROW_STORE))) {
      LOG_WARN("failed to prepare ivf write env", K(ret));
    } else if (OB_FAIL(write_disk_bbq_index(vectors.data(), cids.data(), vids.data(),
        VECTOR_CNT, centroid.data(), 1 /*nlist*/, TEST_DIM, flip, 1 /*nbits*/,
        nullptr /*extra_xipnorm_override*/, share::VIDA_L2, false /*write_raw_vectors*/))) {
      LOG_WARN("failed to write ivf index sstable", K(ret));
    } else if (OB_FAIL(object_cleaner_.mark_succeed())) {
      LOG_WARN("failed to retain ivf test macro blocks", K(ret));
    } else if (OB_FAIL(prepare_read_info(read_info))) {
      LOG_WARN("failed to prepare read info", K(ret));
    }
    return ret;
  }

  // Whole range: [MIN, MAX], both borders open, valid for the datum rowkey.
  void build_whole_range(ObDatumRange &range)
  {
    range.reset();
    range.set_whole_range();
  }

  // Boundary range that starts in the middle of the first low precision vector micro block:
  // [(LOW_PRECISION, 0, 1), MAX]. The first data block of the range is only partly covered, which
  // is the case that used to descend and open the data micro block.
  void build_low_precision_tail_range(ObDatumRange &range)
  {
    ObStorageDatum *start_datums = tail_range_start_datums_;
    ObDatumRowkey start_key;
    start_datums[0].set_int(IVF_ROW_TYPE_LOW_PRECISION_VECTOR);
    start_datums[1].set_int(0);
    start_datums[2].set_int(1);
    ASSERT_EQ(OB_SUCCESS, start_key.assign(start_datums, ARRAYSIZEOF(tail_range_start_datums_)));
    range.reset();
    range.set_start_key(start_key);
    range.set_end_key(ObDatumRowkey::MAX_ROWKEY);
    range.set_left_closed();
    range.set_right_closed();
    ASSERT_TRUE(range.is_valid());
  }

  // Locate the micro block index info of the (type, cid, vid) ivf row without reading any data
  // block payload. A single block is expected to contain the whole locator range.
  int locate_ivf_row_index_info(
      const ObTableReadInfo &read_info,
      const int64_t type,
      const int64_t cid,
      const int64_t vid,
      ObMicroIndexInfo &index_info)
  {
    int ret = OB_SUCCESS;
    ObStorageDatum datums[3];
    ObDatumRowkey start_key;
    ObDatumRowkey end_key;
    ObDatumRange range;
    ObMicroBlockIndexIterator *iter = nullptr;
    datums[0].set_int(type);
    datums[1].set_int(cid);
    datums[2].set_int(vid);
    if (OB_FAIL(start_key.assign(datums, ARRAYSIZEOF(datums)))) {
    } else if (OB_FAIL(end_key.assign(datums, ARRAYSIZEOF(datums)))) {
    } else {
      range.set_start_key(start_key);
      range.set_end_key(end_key);
      range.set_left_closed();
      range.set_right_closed();
      if (OB_FAIL(sstable_.scan_micro_block(range, read_info, allocator_, iter, false))) {
      } else if (OB_ISNULL(iter)) {
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(iter->get_next(index_info))) {
      } else {
        ObMicroIndexInfo unexpected;
        const int next_ret = iter->get_next(unexpected);
        if (OB_ITER_END != next_ret) {
          ret = OB_SUCCESS == next_ret ? OB_ERR_UNEXPECTED : next_ret;
        }
      }
    }
    if (nullptr != iter) {
      iter->~ObMicroBlockIndexIterator();
      allocator_.free(iter);
      iter = nullptr;
    }
    return ret;
  }

  // Build the FastScan encoded shape of a low precision vector data block: the block cache stores
  // it as ObMicroBlockData::IVF_FASTSCAN_BLOCK with a null micro header.
  int build_fastscan_block_data(ObMicroBlockData &block_data)
  {
    int ret = OB_SUCCESS;
    char *buf = static_cast<char *>(allocator_.alloc(FASTSCAN_BLOCK_SIZE));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate fastscan block buffer", K(ret));
    } else {
      MEMSET(buf, 0, FASTSCAN_BLOCK_SIZE);
      block_data.reset();
      block_data.buf_ = buf;
      block_data.size_ = FASTSCAN_BLOCK_SIZE;
      block_data.micro_header_ = nullptr;
      block_data.type_ = ObMicroBlockData::IVF_FASTSCAN_BLOCK;
    }
    return ret;
  }

  // Build a genuine row-format data micro block (flat, 3 schema rowkeys + 2 multi version extra
  // rowkeys, 6 columns) holding NORMAL_BLOCK_ROW_CNT low precision vector rows. The block is
  // serialized exactly like the micro block writer does it, so the estimation must accept it.
  int build_normal_data_block(ObMicroBlockData &block_data)
  {
    int ret = OB_SUCCESS;
    ObMicroBlockWriter<> writer;
    ObDatumRow row;
    ObMicroBlockDesc micro_desc;
    const int64_t micro_block_size = 16 * 1024;
    const int64_t mv_col_cnt =
        table_schema_.get_column_count() + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    // 3 schema rowkeys plus the 2 multi version extra rowkeys.
    const int64_t rowkey_col_cnt = table_schema_.get_rowkey_column_num()
        + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    char *buf = nullptr;
    int64_t pos = 0;
    int64_t size = 0;
    if (OB_FAIL(writer.init(micro_block_size, rowkey_col_cnt, mv_col_cnt))) {
      LOG_WARN("failed to init micro block writer", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < NORMAL_BLOCK_ROW_CNT; ++i) {
      char data[64];
      snprintf(data, sizeof(data), "t%ld_c%ld_v%ld",
          IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 0L, i);
      if (OB_FAIL(make_ivf_row(row, IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 0, i, data))) {
        LOG_WARN("failed to make ivf row", K(ret), K(i));
      } else if (OB_FAIL(writer.append_row(row))) {
        LOG_WARN("failed to append ivf row", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(writer.build_micro_block_desc_in_unittest(micro_desc))) {
      LOG_WARN("failed to build micro block desc", K(ret));
    } else if (OB_ISNULL(micro_desc.header_) || OB_UNLIKELY(!micro_desc.header_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid micro block header", K(ret), K(micro_desc));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(
        micro_desc.header_->header_size_ + micro_desc.buf_size_)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate normal block buffer", K(ret));
    } else if (FALSE_IT(size = micro_desc.header_->header_size_ + micro_desc.buf_size_)) {
    } else if (OB_FAIL(micro_desc.header_->serialize(buf, size, pos))) {
      LOG_WARN("failed to serialize micro header", K(ret));
    } else if (OB_UNLIKELY(micro_desc.header_->header_size_ != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected micro header size", K(ret), K(pos), KPC(micro_desc.header_));
    } else {
      MEMCPY(buf + pos, micro_desc.buf_, micro_desc.buf_size_);
      block_data.reset();
      if (OB_FAIL(block_data.init_with_prepare_micro_header(buf, size))) {
        LOG_WARN("failed to prepare micro header", K(ret));
      } else if (OB_ISNULL(block_data.get_micro_header())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("normal data block has no micro header", K(ret));
      }
    }
    return ret;
  }

  // An empty ivf sstable (valid meta, no data macro block at all).
  int build_empty_ivf_sstable()
  {
    int ret = OB_SUCCESS;
    ObStorageSchema storage_schema;
    ObTabletCreateSSTableParam param;
    ObIvfSSTableMeta ivf_meta;
    share::schema::ObTableSchema table_schema;
    TestSchemaUtils::prepare_data_schema(table_schema);
    ivf_meta.reset();
    ivf_meta.index_type_ = share::VIAT_DISK_BBQ;
    ivf_meta.dist_type_ = share::VIDA_L2;
    ivf_meta.dim_ = static_cast<int32_t>(TEST_DIM);
    ivf_meta.format_version_ = IVF_SSTABLE_FORMAT_VERSION;
    ivf_meta.nbits_ = 1;
    ivf_meta.fht_algo_version_ =
        quantization::ObFhtKacRotator::CURRENT_FHT_ALGO_VERSION;
    if (OB_FAIL(storage_schema.init(allocator_, table_schema,
        lib::Worker::CompatMode::MYSQL))) {
      LOG_WARN("failed to init storage schema", K(ret));
    } else if (OB_FAIL(param.init_for_empty_major_sstable(
        ObTabletID(IVF_TABLET_ID), storage_schema, IVF_SNAPSHOT_VERSION, -1, false, false))) {
      LOG_WARN("failed to init empty major sstable param", K(ret));
    } else {
      param.table_key_.table_type_ = ObITable::IVF_INDEX_SSTABLE;
      param.table_key_.tablet_id_ = ObTabletID(IVF_TABLET_ID);
      param.table_key_.version_range_.base_version_ = 0;
      param.table_key_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
      param.ivf_meta_ = ivf_meta;
      if (OB_FAIL(sstable_.init(param, &allocator_))) {
        LOG_WARN("failed to init empty ivf sstable", K(ret));
      }
    }
    return ret;
  }

  // Clone the prepared sstable as an incremental segment (INC_IVF_INDEX_SSTABLE).
  int clone_as_inc_ivf_sstable(ObIvfSSTable *&inc_sstable)
  {
    int ret = OB_SUCCESS;
    ObSSTable *copied_sstable = nullptr;
    inc_sstable = nullptr;
    if (OB_FAIL(sstable_.ObSSTable::deep_copy(allocator_, copied_sstable))) {
      LOG_WARN("failed to clone ivf test sstable", K(ret));
    } else if (OB_ISNULL(copied_sstable)
        || OB_UNLIKELY(!copied_sstable->is_ivf_sstable())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected cloned ivf test sstable", K(ret), KP(copied_sstable));
    } else {
      copied_sstable->key_.table_type_ = ObITable::INC_IVF_INDEX_SSTABLE;
      copied_sstable->key_.version_range_.base_version_ = 0;
      copied_sstable->key_.version_range_.snapshot_version_ = IVF_SNAPSHOT_VERSION;
      inc_sstable = static_cast<ObIvfSSTable *>(copied_sstable);
      if (OB_UNLIKELY(!inc_sstable->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid cloned inc ivf test sstable", K(ret), KPC(inc_sstable));
      }
    }
    return ret;
  }
};

// The whole range estimation must count every row of the ivf sstable by only reading the index
// tree: 1 centroid row plus VECTOR_CNT low precision vector rows.
TEST_F(TestIvfSstableEstimate, estimate_ivf_sstable_whole_range)
{
  ObTableReadInfo read_info;
  ObDatumRange range;
  int64_t row_count = 0;
  ObIvfEstimateResult result;
  ASSERT_EQ(OB_SUCCESS, prepare_ivf_sstable(read_info));
  ASSERT_TRUE(sstable_.is_valid());
  ASSERT_FALSE(sstable_.is_empty());
  build_whole_range(range);
  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      sstable_, read_info, range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(TOTAL_ROW_CNT, row_count);
  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_result(
      sstable_, read_info, range, nullptr /*tablet*/, result));
  ASSERT_EQ(TOTAL_ROW_CNT, result.row_count_);
  // The exact block counts depend on how the writer packs the rows, so only require sane values.
  ASSERT_GE(result.macro_block_cnt_, 1);
  ASSERT_LE(result.macro_block_cnt_, 3);
  ASSERT_GE(result.micro_block_cnt_, 1);
  ASSERT_LE(result.micro_block_cnt_, result.macro_block_cnt_ * 2);
  // Estimating twice must be stable and must not consume the index tree.
  int64_t second_row_count = 0;
  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      sstable_, read_info, range, nullptr /*tablet*/, second_row_count));
  ASSERT_EQ(row_count, second_row_count);
}

// A range whose first data block is only partly covered is estimated conservatively: the whole
// boundary block is counted, and the data micro block is never opened.
TEST_F(TestIvfSstableEstimate, estimate_ivf_sstable_boundary_range)
{
  ObTableReadInfo read_info;
  ObDatumRange range;
  int64_t row_count = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_ivf_sstable(read_info));
  build_low_precision_tail_range(range);
  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      sstable_, read_info, range, nullptr /*tablet*/, row_count));
  // VECTOR_CNT - 1 rows are covered by the range, the boundary block is counted as a whole.
  ASSERT_GT(row_count, 0);
  ASSERT_LE(row_count, 2 * TOTAL_ROW_CNT);
  // The boundary block is not fully covered, so the estimate can not be smaller than the rows
  // inside the range.
  ASSERT_GE(row_count, VECTOR_CNT - 1);
}

// Regression test of the crash scene: with the FastScan encoded low precision block already in the
// block cache (exactly what an ANN query leaves behind), the estimation must not open it. Before
// the fix the traverser descended into the boundary data block, hit this cache entry and
// dereferenced the null micro header of the FastScan block.
TEST_F(TestIvfSstableEstimate, estimate_ivf_sstable_with_fastscan_block_cached)
{
  ObTableReadInfo read_info;
  ObDatumRange range;
  ObMicroIndexInfo index_info;
  ObMicroBlockData fastscan_data;
  int64_t cold_row_count = 0;
  int64_t warm_row_count = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_ivf_sstable(read_info));
  build_low_precision_tail_range(range);
  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      sstable_, read_info, range, nullptr /*tablet*/, cold_row_count));
  ASSERT_GT(cold_row_count, 0);
  // Locate the low precision data block holding the boundary row of the range: the estimation
  // descends into it before the fix, so its cache entry is the exact crash scene.
  ASSERT_EQ(OB_SUCCESS, locate_ivf_row_index_info(read_info,
      IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 0 /*cid*/, 1 /*vid*/, index_info));
  ASSERT_TRUE(index_info.is_data_block());

  ObDataMicroBlockCache &cache = ObStorageCacheSuite::get_instance().get_block_cache();
  const ObMicroBlockCacheKey key(MTL_ID(), index_info);
  ObMicroBlockBufferHandle probe_handle;
  const int probe_ret = cache.get_cache_block(key, probe_handle);
  ASSERT_TRUE(OB_SUCCESS == probe_ret || OB_ENTRY_NOT_EXIST == probe_ret);
  if (OB_SUCCESS == probe_ret) {
    const int pre_erase_ret = cache.erase(key);
    ASSERT_TRUE(OB_SUCCESS == pre_erase_ret || OB_ENTRY_NOT_EXIST == pre_erase_ret);
  }
  probe_handle.reset();

  // Inject the FastScan encoded form of the block under the very key the data block read uses.
  ASSERT_EQ(OB_SUCCESS, build_fastscan_block_data(fastscan_data));
  ObMicroBlockDesMeta des_meta;
  ObMacroBlockReader macro_reader;
  const ObMicroBlockCacheValue *cached_value = nullptr;
  ObKVCacheHandle cache_handle;
  ASSERT_EQ(OB_SUCCESS, cache.put_cache_block(des_meta, ObMicroBlockData::IVF_FASTSCAN_BLOCK,
      fastscan_data.buf_, fastscan_data.size_, key, macro_reader, allocator_,
      cached_value, cache_handle));
  ASSERT_NE(nullptr, cached_value);
  ASSERT_EQ(ObMicroBlockData::IVF_FASTSCAN_BLOCK, cached_value->get_block_data().type_);
  ASSERT_EQ(FASTSCAN_BLOCK_SIZE, cached_value->get_block_data().size_);
  ASSERT_EQ(nullptr, cached_value->get_block_data().get_micro_header());

  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      sstable_, read_info, range, nullptr /*tablet*/, warm_row_count));
  ASSERT_EQ(cold_row_count, warm_row_count);

  cached_value = nullptr;
  cache_handle.reset();
  const int erase_ret = cache.erase(key);
  ASSERT_TRUE(OB_SUCCESS == erase_ret || OB_ENTRY_NOT_EXIST == erase_ret);
}

// An incremental segment (INC_IVF_INDEX_SSTABLE) shares the same data block layout and must be
// estimated the same way.
TEST_F(TestIvfSstableEstimate, estimate_inc_ivf_sstable_whole_range)
{
  ObTableReadInfo read_info;
  ObDatumRange range;
  ObIvfSSTable *inc_sstable = nullptr;
  int64_t row_count = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_ivf_sstable(read_info));
  ASSERT_EQ(OB_SUCCESS, clone_as_inc_ivf_sstable(inc_sstable));
  ASSERT_NE(nullptr, inc_sstable);
  ASSERT_TRUE(inc_sstable->is_ivf_sstable());
  build_whole_range(range);
  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      *inc_sstable, read_info, range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(TOTAL_ROW_CNT, row_count);
}

// An empty segment has no data macro block: no row and no data block may be reported.
TEST_F(TestIvfSstableEstimate, estimate_empty_ivf_sstable)
{
  ObTableReadInfo read_info;
  ObDatumRange range;
  int64_t row_count = -1;
  ObIvfEstimateResult result;
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  ASSERT_EQ(OB_SUCCESS, build_empty_ivf_sstable());
  ASSERT_TRUE(sstable_.is_valid());
  ASSERT_TRUE(sstable_.is_empty());
  build_whole_range(range);
  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      sstable_, read_info, range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(0, row_count);
  ASSERT_EQ(OB_SUCCESS, ObIndexBlockScanEstimator::estimate_ivf_sstable_result(
      sstable_, read_info, range, nullptr /*tablet*/, result));
  ASSERT_EQ(0, result.row_count_);
  ASSERT_EQ(0, result.macro_block_cnt_);
  ASSERT_EQ(0, result.micro_block_cnt_);
}

// Invalid arguments are rejected before any index tree traversal, and never leave a bogus row
// count behind.
TEST_F(TestIvfSstableEstimate, estimate_ivf_sstable_invalid_args)
{
  ObTableReadInfo read_info;
  ObTableReadInfo invalid_read_info;
  ObDatumRange range;
  ObDatumRange invalid_range;
  ObIvfSSTable invalid_sstable;
  int64_t row_count = 0;
  ASSERT_EQ(OB_SUCCESS, prepare_ivf_sstable(read_info));
  build_whole_range(range);
  ASSERT_TRUE(read_info.is_valid());
  ASSERT_FALSE(invalid_read_info.is_valid());
  ASSERT_FALSE(invalid_range.is_valid());

  row_count = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      sstable_, invalid_read_info, range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(0, row_count);
  row_count = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      sstable_, read_info, invalid_range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(0, row_count);
  // A never initialized sstable must not report a bogus row count either.
  row_count = -1;
  ASSERT_EQ(OB_INVALID_ARGUMENT, ObIndexBlockScanEstimator::estimate_ivf_sstable_row_count(
      invalid_sstable, read_info, range, nullptr /*tablet*/, row_count));
  ASSERT_EQ(0, row_count);
}

// Guard: a FastScan encoded block (or any block without a valid row-format micro header) must be
// rejected by ObMicroBlockRowScanner::estimate_row_count instead of dereferencing a null header.
TEST_F(TestIvfSstableEstimate, estimate_micro_block_rejects_block_without_header)
{
  ObTableReadInfo read_info;
  ObDatumRange range;
  ObDatumRange invalid_range;
  ObMicroBlockData fastscan_data;
  ObMicroBlockData data_block_without_header;
  ObPartitionEst est;
  ObMicroBlockRowScanner scanner(allocator_);
  // Use a fully prepared read info so that the estimation only fails on the block guard.
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  build_whole_range(range);
  ASSERT_EQ(OB_SUCCESS, build_fastscan_block_data(fastscan_data));
  ASSERT_TRUE(fastscan_data.is_valid());
  ASSERT_EQ(nullptr, fastscan_data.get_micro_header());
  // The very crash shape: a data block without micro header.
  data_block_without_header.reset();
  data_block_without_header.buf_ = fastscan_data.buf_;
  data_block_without_header.size_ = fastscan_data.size_;
  data_block_without_header.micro_header_ = nullptr;
  data_block_without_header.type_ = ObMicroBlockData::DATA_BLOCK;

  // Invalid arguments keep precedence over the new guard.
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.estimate_row_count(
      read_info, fastscan_data, invalid_range, false /*consider_multi_version*/, est));
  ObMicroBlockData invalid_block;
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.estimate_row_count(
      read_info, invalid_block, range, false /*consider_multi_version*/, est));
  // FastScan encoded block, null micro header.
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.estimate_row_count(
      read_info, fastscan_data, range, false /*consider_multi_version*/, est));
  // Data block whose micro header could not be parsed.
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.estimate_row_count(
      read_info, data_block_without_header, range, false /*consider_multi_version*/, est));
}

// Guard: a block whose type is not DATA_BLOCK is rejected even when it carries a valid micro
// header, because estimation may only open plain row-format data blocks.
TEST_F(TestIvfSstableEstimate, estimate_micro_block_rejects_non_data_block)
{
  ObTableReadInfo read_info;
  ObDatumRange range;
  ObMicroBlockData block_data;
  ObPartitionEst est;
  ObMicroBlockRowScanner scanner(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  build_whole_range(range);
  ASSERT_EQ(OB_SUCCESS, build_normal_data_block(block_data));
  ASSERT_NE(nullptr, block_data.get_micro_header());
  ASSERT_EQ(ObMicroBlockData::DATA_BLOCK, block_data.type_);
  block_data.type_ = ObMicroBlockData::INDEX_BLOCK;
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.estimate_row_count(
      read_info, block_data, range, false /*consider_multi_version*/, est));
  block_data.type_ = ObMicroBlockData::INV_IDX_POSTING_BLOCK;
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.estimate_row_count(
      read_info, block_data, range, false /*consider_multi_version*/, est));
  block_data.type_ = ObMicroBlockData::DDL_MERGE_INDEX_BLOCK;
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.estimate_row_count(
      read_info, block_data, range, false /*consider_multi_version*/, est));
}

// Guard: a genuine row-format data block is still estimated normally, i.e. the new checks do not
// regress the normal path.
TEST_F(TestIvfSstableEstimate, estimate_micro_block_accepts_normal_data_block)
{
  ObTableReadInfo read_info;
  ObDatumRange range;
  ObMicroBlockData block_data;
  ObPartitionEst est;
  ObMicroBlockRowScanner scanner(allocator_);
  ASSERT_EQ(OB_SUCCESS, prepare_write_env());
  ASSERT_EQ(OB_SUCCESS, prepare_read_info(read_info));
  build_whole_range(range);
  ASSERT_EQ(OB_SUCCESS, build_normal_data_block(block_data));
  ASSERT_EQ(ObMicroBlockData::DATA_BLOCK, block_data.type_);
  ASSERT_EQ(OB_SUCCESS, scanner.estimate_row_count(
      read_info, block_data, range, false /*consider_multi_version*/, est));
  ASSERT_EQ(NORMAL_BLOCK_ROW_CNT, est.physical_row_count_);
  ASSERT_EQ(NORMAL_BLOCK_ROW_CNT, est.logical_row_count_);
}

// Guard: opening a micro block without a valid row-format micro header is rejected before the
// reader is built, so the missing header is never dereferenced.
TEST_F(TestIvfSstableEstimate, open_micro_block_rejects_block_without_header)
{
  ObTableReadInfo read_info;
  ObTableIterParam iter_param;
  ObTableAccessContext context;
  ObStoreCtx store_ctx;
  MacroBlockId macro_id;
  MacroBlockId invalid_macro_id;
  uint64_t block_offset = 0;
  ObMicroBlockData fastscan_data;
  ObCSRange cs_range;
  ObMicroBlockRowScanner scanner(allocator_);
  invalid_macro_id.reset();
  ASSERT_EQ(OB_SUCCESS, prepare_ivf_sstable(read_info));
  ASSERT_EQ(OB_SUCCESS, init_scan_context(read_info, iter_param, context, store_ctx));
  ASSERT_EQ(OB_SUCCESS, scanner.init(iter_param, context, &sstable_));
  ASSERT_EQ(OB_SUCCESS, locate_ivf_row_micro_block(read_info,
      IVF_ROW_TYPE_LOW_PRECISION_VECTOR, 0 /*cid*/, 0 /*vid*/, macro_id, block_offset));
  ASSERT_TRUE(macro_id.is_valid());
  ASSERT_EQ(OB_SUCCESS, build_fastscan_block_data(fastscan_data));
  cs_range.set_whole_range();

  // Invalid argument keeps precedence over the new guard.
  ASSERT_EQ(OB_INVALID_ARGUMENT, scanner.open(invalid_macro_id, fastscan_data,
      true /*is_left_border*/, true /*is_right_border*/));
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.open(macro_id, fastscan_data,
      true /*is_left_border*/, true /*is_right_border*/));
  ASSERT_EQ(OB_ERR_UNEXPECTED, scanner.open_column_block(macro_id, fastscan_data, cs_range));
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  using namespace oceanbase;
  signal(49, SIG_IGN);
  system("rm -f test_ivf_sstable_estimate.log*");
  OB_LOGGER.set_file_name("test_ivf_sstable_estimate.log", true);
  OB_LOGGER.set_log_level("INFO");
  fflush(stdout);
  const int saved_stdout = dup(STDOUT_FILENO);
  if (saved_stdout >= 0) {
    freopen("/dev/null", "w", stdout);
    oceanbase::common::init_arches();
    fflush(stdout);
    dup2(saved_stdout, STDOUT_FILENO);
    close(saved_stdout);
  } else {
    oceanbase::common::init_arches();
  }
  testing::InitGoogleTest(&argc, argv);
  if (OB_SUCCESS != oceanbase::MockTenantModuleEnv::get_instance().init()) {
    return 1;
  }
  const int ret = RUN_ALL_TESTS();
  oceanbase::MockTenantModuleEnv::get_instance().destroy();
  std::_Exit(ret);
}
