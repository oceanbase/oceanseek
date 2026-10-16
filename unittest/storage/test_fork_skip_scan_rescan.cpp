/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#include <gtest/gtest.h>
#include "share/ob_cluster_version.h"

#define private public
#include "storage/access/ob_multiple_scan_merge.h"
#include "storage/access/ob_index_skip_scanner.h"
#include "storage/tablet/ob_tablet.h"
#undef private

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace share;
using namespace storage;
using namespace blocksstable;

class ForkSourceSSTable : public ObSSTable
{
public:
  ForkSourceSSTable()
  {
    key_.tablet_id_ = ObTabletID(200001);
    key_.table_type_ = ObITable::MAJOR_SSTABLE;
  }
};

// 简化 store context 建立、选表和磁盘 I/O；保留真实的 source access context 初始化、
// merge open、回源 context 选择及 skip scanner 创建。
// 本测试覆盖 ObTableScanIterator::rescan -> open_iter 中已有 scan_merge_->open 的分支，
// 不依赖优化器生成特定 SQL 计划，也不声称覆盖 SQL/NLJ 到 rescan 的完整调用链。
class ForkScanMergeForTest : public ObMultipleScanMerge
{
public:
  ForkScanMergeForTest() : bind_count_(0), construct_count_(0), scanner_(nullptr), scanner_ctx_(nullptr) {}
  ~ForkScanMergeForTest() { close_scanner(); }

  int bind(ObTableAccessParam &param, ObTableAccessContext &ctx, ObGetTableParam &get_param)
  {
    ++bind_count_;
    access_param_ = &param;
    access_ctx_ = &ctx;
    get_table_param_ = &get_param;
    long_life_allocator_ = ctx.stmt_allocator_;
    int ret = cur_row_.init(*long_life_allocator_, param.get_max_out_col_cnt());
    if (OB_SUCC(ret)) {
      // 本用例不读盘，无需通过 LS 服务构造事务快照；仅建立可独立释放的 source store ctx。
      // factory 的首次复制仍走真实 init_for_fork，不能由测试手工赋值替代。
      fork_source_ctx_.alloc_ = long_life_allocator_;
      fork_source_ctx_.store_ctx_ = OB_NEWx(ObStoreCtx, long_life_allocator_);
      fork_source_ctx_.access_ctx_ = OB_NEWx(ObTableAccessContext, long_life_allocator_);
      if (OB_ISNULL(fork_source_ctx_.store_ctx_) || OB_ISNULL(fork_source_ctx_.access_ctx_)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        fork_source_ctx_.store_ctx_->ls_id_ = ctx.ls_id_;
        fork_source_ctx_.store_ctx_->tablet_id_ = get_param.tablet_iter_.get_fork_info()->get_src_tablet_id();
        ret = fork_source_ctx_.access_ctx_->init_for_fork(
            ctx, fork_source_ctx_.store_ctx_, *get_param.tablet_iter_.get_fork_info());
      }
    }
    inited_ = OB_SUCC(ret);
    return ret;
  }

  ObTableAccessContext *source_ctx() const { return fork_source_ctx_.access_ctx_; }
  void close_scanner() { ObIndexSkipScanFactory::destroy_index_skip_scanner(scanner_); }
  int bind_count_;
  int construct_count_;
  ObISkipScanner *scanner_;
  ObTableAccessContext *scanner_ctx_;

protected:
  int prepare() override { return OB_SUCCESS; }
  int construct_iters() override
  {
    ++construct_count_;
    int ret = get_access_ctx_for_table(&source_table_, scanner_ctx_);
    if (OB_SUCC(ret) && access_param_->iter_param_.is_index_skip_scan()) {
      ret = ObIndexSkipScanFactory::build_index_skip_scanner(
          access_param_->iter_param_, *scanner_ctx_, range_, scanner_, false/*is_for_memtable*/);
    }
    return ret;
  }

private:
  ForkSourceSSTable source_table_;
};

class TestForkSkipScanRescan : public ::testing::Test
{
public:
  void SetUp() override
  {
    ObColDescArray columns;
    for (int64_t i = 0; i < 2; ++i) {
      share::schema::ObColDesc col;
      col.col_id_ = OB_APP_MIN_COLUMN_ID + i;
      col.col_type_.set_int();
      ASSERT_EQ(OB_SUCCESS, columns.push_back(col));
    }
    ASSERT_EQ(OB_SUCCESS, read_info_.init(allocator_, 2, 2, false/*is_oracle_mode*/,
        columns, nullptr/*storage_cols_index*/));
    param_.iter_param_.read_info_ = &read_info_;
    param_.iter_param_.tablet_id_ = ObTabletID(200002);

    ctx_.is_inited_ = true;
    ctx_.allocator_ = &allocator_;
    ctx_.stmt_allocator_ = &allocator_;
    ctx_.store_ctx_ = &store_ctx_;
    ctx_.tablet_id_ = ObTabletID(200002);
    ctx_.ls_id_ = ObLSID(1);
    ctx_.query_flag_.scan_order_ = ObQueryFlag::Forward;
    ctx_.trans_version_range_.snapshot_version_ = 200;
    store_ctx_.ls_id_ = ObLSID(1);
    store_ctx_.timeout_ = INT64_MAX;

    // 无需启动 tablet 服务：只为真实的 fork overlay 判断提供有效的 source handle。
    ASSERT_EQ(OB_SUCCESS, get_param_.tablet_iter_.build_fork_ctx_());
    auto &fork_ctx = *get_param_.tablet_iter_.fork_ctx_;
    fork_ctx.fork_info_.set_src_tablet_id(ObTabletID(200001));
    fork_ctx.fork_info_.set_fork_snapshot_version(100);
    fork_ctx.fork_info_.set_data_complete(false);
    void *tablet_buf = allocator_.alloc(sizeof(ObTablet));
    ASSERT_NE(nullptr, tablet_buf);
    fork_ctx.fork_tablet_handle_.set_obj(ObTabletHandle::ObTabletHdlType::STANDALONE,
        new (tablet_buf) ObTablet(true/*is_external_tablet*/), &allocator_, nullptr);
    ASSERT_NE(nullptr, get_param_.tablet_iter_.get_fork_info());

    scan_param_.scan_allocator_ = &allocator_;
    scan_param_.scan_flag_.scan_order_ = ObQueryFlag::Forward;
    ObNewRange range;
    range.set_whole_range();
    ASSERT_EQ(OB_SUCCESS, scan_param_.key_ranges_.push_back(range));
    ASSERT_EQ(OB_SUCCESS, scan_param_.ss_key_ranges_.push_back(range));
    scan_range_.set_whole_range();
  }

  void TearDown() override
  {
    merge_.close_scanner();
    merge_.reset();
    ctx_.reset();
    get_param_.tablet_iter_.reset();
  }

  void enable_skip_scan_on_rescan()
  {
    // 对应 rescan 的参数准备：只替换 suffix range，不重新初始化 merge 或回源 context。
    suffix_value_.set_int(7);
    ObNewRange &suffix = scan_param_.ss_key_ranges_.at(0);
    suffix.start_key_.assign(&suffix_value_, 1);
    suffix.end_key_.assign(&suffix_value_, 1);
    suffix.border_flag_.set_inclusive_start();
    suffix.border_flag_.set_inclusive_end();
    ASSERT_TRUE(scan_param_.use_index_skip_scan());
    ASSERT_EQ(OB_SUCCESS, ctx_.rescan_reuse(scan_param_));
    ASSERT_EQ(OB_SUCCESS, ctx_.alloc_skip_scan_factory());
    ASSERT_EQ(OB_SUCCESS, param_.check_skip_scan(scan_param_, param_.iter_param_));
    ASSERT_TRUE(param_.iter_param_.is_index_skip_scan());
    ASSERT_EQ(1, param_.iter_param_.get_ss_rowkey_prefix_cnt());
    ASSERT_EQ(OB_SUCCESS, suffix_range_.from_range(suffix, allocator_));
    param_.iter_param_.set_skip_scan_range(suffix_range_);
  }

  ObArenaAllocator allocator_;
  ObTableReadInfo read_info_;
  ObTableAccessParam param_;
  ObStoreCtx store_ctx_;
  ObTableAccessContext ctx_;
  ObGetTableParam get_param_;
  ObTableScanParam scan_param_;
  ObObj suffix_value_;
  ObDatumRange scan_range_;
  ObDatumRange suffix_range_;
  ForkScanMergeForTest merge_;
};

TEST_F(TestForkSkipScanRescan, normal_scan_then_reopen_with_skip_scan)
{
  ASSERT_FALSE(scan_param_.use_index_skip_scan());
  ASSERT_EQ(OB_SUCCESS, merge_.bind(param_, ctx_, get_param_));
  ObTableAccessContext *const original_source_ctx = merge_.source_ctx();
  ASSERT_NE(nullptr, original_source_ctx);
  ASSERT_EQ(nullptr, ctx_.get_skip_scan_factory());
  ASSERT_EQ(nullptr, original_source_ctx->get_skip_scan_factory());

  // 首轮必须执行 open 和 source 选路，不能用空范围让扫描在创建 merge 前被跳过。
  ASSERT_EQ(OB_SUCCESS, merge_.open(scan_range_));
  ASSERT_EQ(1, merge_.construct_count_);
  ASSERT_EQ(original_source_ctx, merge_.scanner_ctx_);
  ASSERT_EQ(nullptr, merge_.scanner_);

  enable_skip_scan_on_rescan();
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_NE(nullptr, ctx_.get_skip_scan_factory());
  ASSERT_EQ(nullptr, original_source_ctx->get_skip_scan_factory());

  // INIT_AND_OPEN_ITER 对已有 merge 直接调用 open，不调用 init/switch_param/advance_scan。
  // 去掉 open 内的同步后，这里必须因真实 build_index_skip_scanner 返回 -4016 而失败。
  ASSERT_EQ(OB_SUCCESS, merge_.open(scan_range_));
  EXPECT_EQ(1, merge_.bind_count_);
  EXPECT_EQ(2, merge_.construct_count_);
  EXPECT_EQ(original_source_ctx, merge_.source_ctx());
  EXPECT_EQ(original_source_ctx, merge_.scanner_ctx_);
  EXPECT_EQ(ctx_.get_skip_scan_factory(), original_source_ctx->get_skip_scan_factory());
  ASSERT_NE(nullptr, merge_.scanner_);
  EXPECT_TRUE(merge_.scanner_->is_index_skip_scanner());
}

TEST_F(TestForkSkipScanRescan, factory_exists_before_source_context_is_created)
{
  // 对照组：首次扫描已启用 skip scan，旧代码也应通过，不能拿它证明延迟创建路径。
  enable_skip_scan_on_rescan();
  ASSERT_FALSE(HasFatalFailure());
  ASSERT_EQ(OB_SUCCESS, merge_.bind(param_, ctx_, get_param_));
  ASSERT_EQ(ctx_.get_skip_scan_factory(), merge_.source_ctx()->get_skip_scan_factory());
  ASSERT_EQ(OB_SUCCESS, merge_.open(scan_range_));
  ASSERT_NE(nullptr, merge_.scanner_);
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  // 使用现有的单测版本接口，让真实 table read-info 无需依赖租户版本服务。
  oceanbase::common::ObClusterVersion::get_instance().update_cluster_version(CLUSTER_CURRENT_VERSION);
  oceanbase::common::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
