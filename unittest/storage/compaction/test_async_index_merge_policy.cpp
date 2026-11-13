/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include <gtest/gtest.h>
#include <cmath>
#define private public
#define protected public
#include "storage/compaction/ob_async_index_merge_policy.h"
#include "storage/compaction/ob_async_index_merge_ctx.h"
#include "storage/compaction/ob_tablet_merge_task.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;

namespace unittest
{

struct MergeInfoSpec
{
  int64_t start_scn_;
  int64_t end_scn_;
  int64_t seg_size_;
};

// LogMergePolicy 参考实现，仅用于单测对比。
class ObAsyncIndexLogMergePolicy : public ObAsyncIndexMergePolicyBase
{
public:
  static const int64_t DEFAULT_MERGE_FACTOR = 10;
  static constexpr float LEVEL_LOG_SPAN = 0.75f;
  static const int64_t DEFAULT_MIN_MERGE_SIZE = 15000;
  static constexpr int64_t DEFAULT_MAX_MERGE_SIZE = ObAsyncIndexMergePolicy::DEFAULT_MAX_MERGE_SIZE;

  static double calc_level(const int64_t seg_size, const int64_t merge_factor)
  {
    const double size = static_cast<double>(MAX(1, seg_size));
    const double merge_factor_d = static_cast<double>(MAX(2, merge_factor));
    return std::log(size) / std::log(merge_factor_d);
  }

  ObAsyncIndexLogMergePolicy()
    : merge_factor_(DEFAULT_MERGE_FACTOR),
      min_merge_size_(DEFAULT_MIN_MERGE_SIZE),
      max_merge_size_(DEFAULT_MAX_MERGE_SIZE)
  {
  }

  int init()
  {
    merge_factor_ = DEFAULT_MERGE_FACTOR;
    min_merge_size_ = DEFAULT_MIN_MERGE_SIZE;
    max_merge_size_ = DEFAULT_MAX_MERGE_SIZE;
    is_inited_ = true;
    return OB_SUCCESS;
  }

  virtual int find_merges(
      ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      ObNewVersionRange &merge_range) override
  {
    int ret = OB_SUCCESS;
    merge_range.reset();
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (merge_infos.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no merge infos", K(ret), K(merge_infos.count()));
    } else if (OB_FAIL(validate_merge_infos_(merge_infos))) {
    } else if (OB_FAIL(calc_and_fill_levels_(merge_infos))) {
    } else {
      const int32_t count = merge_infos.count();
      const double level_floor = calc_level_floor_();
      bool found = false;
      int32_t merge_start_idx = -1;
      int32_t merge_end_idx = -1;
      for (int32_t start = 0; OB_SUCC(ret) && !found && start < count; ) {
        float max_level = merge_infos.at(start).level_;
        for (int32_t idx = start + 1; idx < count; ++idx) {
          max_level = MAX(max_level, merge_infos.at(idx).level_);
        }
        const float max_level_f = max_level;
        const float level_bottom = (max_level_f > level_floor)
            ? (max_level_f - LEVEL_LOG_SPAN)
            : (max_level_f - 2 * LEVEL_LOG_SPAN);
        int32_t upto = count - 1;
        while (upto >= start && merge_infos.at(upto).level_ < level_bottom) {
          --upto;
        }
        if (upto >= start) {
          const int tmp_ret = pick_merge_at_level_(merge_infos,
                                                   start,
                                                   upto,
                                                   merge_start_idx,
                                                   merge_end_idx);
          if (OB_SUCCESS == tmp_ret) {
            found = true;
          } else if (OB_ENTRY_NOT_EXIST != tmp_ret) {
            ret = tmp_ret;
          }
        }
        start = upto + 1;
      }
      if (OB_SUCC(ret) && found) {
        merge_range.base_version_ = merge_infos.at(merge_start_idx).start_snapshot_version_;
        merge_range.snapshot_version_ = merge_infos.at(merge_end_idx).end_snapshot_version_;
      } else if (OB_SUCC(ret)) {
        ret = OB_NO_NEED_MERGE;
      }
    }
    return ret;
  }

private:
  int calc_and_fill_levels_(ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos) const
  {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < merge_infos.count(); ++i) {
      ObAsyncIndexSSTableMergeInfo &info = merge_infos.at(i);
      info.level_ = static_cast<float>(calc_level(info.seg_size_, merge_factor_));
    }
    return ret;
  }

  double calc_level_floor_() const
  {
    return calc_level(min_merge_size_, merge_factor_);
  }

  int pick_merge_at_level_(
      const ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      int32_t start,
      const int32_t upto,
      int32_t &merge_start_idx,
      int32_t &merge_end_idx) const
  {
    int ret = OB_SUCCESS;
    merge_start_idx = -1;
    merge_end_idx = -1;
    if (start < 0 || upto < start || upto >= merge_infos.count()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      const int32_t merge_factor = static_cast<int32_t>(merge_factor_);
      int32_t end = start + merge_factor;
      while (OB_SUCC(ret) && merge_start_idx < 0 && end <= upto + 1) {
        int64_t merge_size = 0;
        for (int32_t i = start; i < end; ++i) {
          const int64_t segment_size = merge_infos.at(i).seg_size_;
          if (merge_size + segment_size > max_merge_size_) {
            if (i == start) {
              end = i + 1;
            } else {
              end = i;
            }
            break;
          }
          merge_size += segment_size;
        }
        if (end - start >= merge_factor
            && min_merge_size_ < max_merge_size_
            && merge_size < min_merge_size_) {
          while (end <= upto) {
            const int64_t segment_size = merge_infos.at(end).seg_size_;
            if (merge_size + segment_size > min_merge_size_
                || merge_size + segment_size > max_merge_size_) {
              break;
            }
            merge_size += segment_size;
            ++end;
          }
        }
        if (end - start > 1) {
          merge_start_idx = start;
          merge_end_idx = end - 1;
        }
        start = end;
        end = start + merge_factor;
      }
      if (OB_SUCC(ret) && merge_start_idx < 0) {
        ret = OB_ENTRY_NOT_EXIST;
      }
    }
    return ret;
  }

  int64_t merge_factor_;
  int64_t min_merge_size_;
  int64_t max_merge_size_;
};

// TieredMergePolicy 参考实现，仅用于单测对比。
class ObAsyncIndexTieredMergePolicy : public ObAsyncIndexMergePolicyBase
{
public:
  static const int32_t DEFAULT_MAX_MERGE_AT_ONCE = 10;
  static constexpr double DEFAULT_SEGMENTS_PER_TIER = 8.0;
  static const int64_t DEFAULT_FLOOR_SEGMENT_SIZE = 15000;
  static const int64_t DEFAULT_MAX_MERGED_SEGMENT_SIZE = 15000000L;
  static const int32_t TMP_TARGET_SEARCH_CONCURRENCY = 1;
  static constexpr double TMP_MERGE_SIZE_EXPONENT = 0.05;
  static constexpr double TMP_MIN_GROWTH_RATIO = 1.5;

  struct ObAsyncIndexTieredMergeScore
  {
    double score_;
    bool has_cand_;

    ObAsyncIndexTieredMergeScore() : score_(0.0), has_cand_(false) {}
    void reset() { score_ = 0.0; has_cand_ = false; }
    bool is_better_than(const ObAsyncIndexTieredMergeScore &other) const
    {
      if (!other.has_cand_) {
        return has_cand_;
      } else if (!has_cand_) {
        return false;
      } else {
        return score_ < other.score_;
      }
    }
  };

  ObAsyncIndexTieredMergePolicy()
    : max_merge_at_once_(DEFAULT_MAX_MERGE_AT_ONCE),
      segments_per_tier_(DEFAULT_SEGMENTS_PER_TIER),
      floor_segment_size_(DEFAULT_FLOOR_SEGMENT_SIZE),
      max_merged_segment_size_(DEFAULT_MAX_MERGED_SEGMENT_SIZE)
  {
  }

  int init()
  {
    max_merge_at_once_ = DEFAULT_MAX_MERGE_AT_ONCE;
    segments_per_tier_ = DEFAULT_SEGMENTS_PER_TIER;
    floor_segment_size_ = DEFAULT_FLOOR_SEGMENT_SIZE;
    max_merged_segment_size_ = DEFAULT_MAX_MERGED_SEGMENT_SIZE;
    is_inited_ = true;
    return OB_SUCCESS;
  }

  virtual int find_merges(
      ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      ObNewVersionRange &merge_range) override
  {
    int ret = OB_SUCCESS;
    merge_range.reset();
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
    } else if (merge_infos.count() <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("no merge infos", K(ret), K(merge_infos.count()));
    } else if (OB_FAIL(validate_merge_infos_(merge_infos))) {
    } else {
      const int32_t count = merge_infos.count();
      ObSEArray<bool, 64> is_eligible;
      int32_t eligible_count = 0;
      int32_t too_big_count = 0;
      for (int32_t i = 0; OB_SUCC(ret) && i < count; ++i) {
        const bool too_big = is_too_big_segment_(merge_infos.at(i).seg_size_);
        if (OB_FAIL(is_eligible.push_back(!too_big))) {
        } else if (too_big) {
          ++too_big_count;
        } else {
          ++eligible_count;
        }
      }
      bool found = false;
      if (OB_SUCC(ret)) {
        if (eligible_count > 1) {
          const int32_t allowed_seg_count = calc_allowed_seg_count_(merge_infos,
                                                                    is_eligible,
                                                                    too_big_count);
          if (eligible_count > allowed_seg_count) {
            int32_t best_start_idx = -1;
            int32_t best_end_idx = -1;
            const int tmp_ret = pick_best_contiguous_merge_(merge_infos,
                                                            is_eligible,
                                                            best_start_idx,
                                                            best_end_idx);
            if (OB_SUCCESS == tmp_ret) {
              merge_range.base_version_ = merge_infos.at(best_start_idx).start_snapshot_version_;
              merge_range.snapshot_version_ = merge_infos.at(best_end_idx).end_snapshot_version_;
              found = true;
            } else if (OB_ENTRY_NOT_EXIST != tmp_ret) {
              ret = tmp_ret;
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_NO_NEED_MERGE;
        }
      }
    }
    return ret;
  }

private:
  int32_t calc_merge_factor_() const
  {
    const int32_t segs_per_tier = static_cast<int32_t>(segments_per_tier_);
    return MIN(max_merge_at_once_, MAX(2, segs_per_tier));
  }

  int64_t floor_size_(const int64_t seg_size) const
  {
    return MAX(floor_segment_size_, seg_size);
  }

  bool is_too_big_segment_(const int64_t seg_size) const
  {
    return seg_size > max_merged_segment_size_ / 2;
  }

  int calc_allowed_seg_count_(
      const ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      const ObIArray<bool> &is_eligible,
      const int32_t too_big_count) const
  {
    int ret = OB_SUCCESS;
    int32_t allowed_seg_count = 0;
    int64_t tot_size = 0;
    int64_t min_seg_size = INT64_MAX;
    const int64_t count = merge_infos.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (!is_eligible.at(i)) {
        continue;
      }
      const int64_t seg_size = merge_infos.at(i).seg_size_;
      tot_size += seg_size;
      min_seg_size = MIN(min_seg_size, seg_size);
    }
    if (OB_SUCC(ret)) {
      if (tot_size <= 0 || min_seg_size == INT64_MAX) {
        allowed_seg_count = 0;
      } else {
        const int32_t merge_factor = calc_merge_factor_();
        int64_t level_size = MAX(min_seg_size, floor_segment_size_);
        int64_t size_left = tot_size;
        double allowed = 0.0;
        while (true) {
          const double seg_count_level = static_cast<double>(size_left)
              / static_cast<double>(level_size);
          if (seg_count_level < segments_per_tier_
              || level_size >= max_merged_segment_size_) {
            allowed += std::ceil(seg_count_level);
            break;
          }
          allowed += segments_per_tier_;
          size_left -= static_cast<int64_t>(segments_per_tier_ * level_size);
          level_size = MIN(max_merged_segment_size_,
                           level_size * static_cast<int64_t>(merge_factor));
        }
        allowed = MAX(allowed, segments_per_tier_);
        allowed = MAX(allowed,
            static_cast<double>(TMP_TARGET_SEARCH_CONCURRENCY - too_big_count));
        allowed_seg_count = static_cast<int32_t>(allowed);
      }
    }
    return ret == OB_SUCCESS ? allowed_seg_count : 0;
  }

  double calc_merge_score_(
      const int64_t max_candidate_seg_size,
      const int64_t size_this_merge_floored,
      const int64_t size_this_merge,
      const bool hit_too_large,
      const int32_t merge_factor) const
  {
    double skew = 1.0;
    if (size_this_merge_floored > 0) {
      if (hit_too_large) {
        skew = 1.0 / static_cast<double>(MAX(2, merge_factor));
      } else {
        skew = static_cast<double>(floor_size_(max_candidate_seg_size))
            / static_cast<double>(size_this_merge_floored);
      }
    }
    double merge_score = skew;
    if (size_this_merge > 0) {
      merge_score *= std::pow(static_cast<double>(size_this_merge), TMP_MERGE_SIZE_EXPONENT);
    }
    return merge_score;
  }

  int pick_best_contiguous_merge_(
      const ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      const ObIArray<bool> &is_eligible,
      int32_t &best_start_idx,
      int32_t &best_end_idx) const
  {
    int ret = OB_SUCCESS;
    ObAsyncIndexTieredMergeScore best_score;
    best_score.reset();
    best_start_idx = -1;
    best_end_idx = -1;
    const int32_t count = merge_infos.count();
    const int32_t merge_factor = calc_merge_factor_();
    for (int32_t start_idx = 0; OB_SUCC(ret) && start_idx < count; ++start_idx) {
      if (!is_eligible.at(start_idx)) {
        continue;
      }
      int64_t size_this_merge = 0;
      int64_t size_this_merge_floored = 0;
      int64_t max_candidate_seg_size = 0;
      bool hit_too_large = false;
      for (int32_t idx = start_idx; OB_SUCC(ret) && idx < count; ++idx) {
        if (!is_eligible.at(idx)) {
          break;
        }
        const int64_t seg_size = merge_infos.at(idx).seg_size_;
        const int32_t candidate_cnt = idx - start_idx + 1;
        if (size_this_merge + seg_size > max_merged_segment_size_) {
          hit_too_large = true;
          if (candidate_cnt > 1) {
            break;
          }
          break;
        }
        size_this_merge += seg_size;
        size_this_merge_floored += floor_size_(seg_size);
        max_candidate_seg_size = MAX(max_candidate_seg_size, seg_size);
        if (candidate_cnt >= max_merge_at_once_) {
        } else if (candidate_cnt < merge_factor
                   && size_this_merge < floor_segment_size_) {
          continue;
        } else if (candidate_cnt < merge_factor) {
          continue;
        }
        if (candidate_cnt < 2) {
          continue;
        }
        if (!hit_too_large
            && size_this_merge
                   < static_cast<int64_t>(max_candidate_seg_size * TMP_MIN_GROWTH_RATIO)) {
          continue;
        }
        ObAsyncIndexTieredMergeScore score;
        score.has_cand_ = true;
        score.score_ = calc_merge_score_(max_candidate_seg_size,
                                         size_this_merge_floored,
                                         size_this_merge,
                                         hit_too_large,
                                         merge_factor);
        if (score.is_better_than(best_score)) {
          best_score = score;
          best_start_idx = start_idx;
          best_end_idx = idx;
        }
        if (candidate_cnt >= max_merge_at_once_) {
          break;
        }
      }
    }
    if (OB_SUCC(ret) && !best_score.has_cand_) {
      ret = OB_ENTRY_NOT_EXIST;
    }
    return ret;
  }

  int32_t max_merge_at_once_;
  double segments_per_tier_;
  int64_t floor_segment_size_;
  int64_t max_merged_segment_size_;
};

const char *async_index_merge_policy_type_name(const ObAsyncIndexMergePolicyBase &policy)
{
  if (nullptr != dynamic_cast<const ObAsyncIndexMergePolicy *>(&policy)) {
    return "ObAsyncIndexMergePolicy";
  } else if (nullptr != dynamic_cast<const ObAsyncIndexTieredMergePolicy *>(&policy)) {
    return "ObAsyncIndexTieredMergePolicy";
  } else if (nullptr != dynamic_cast<const ObAsyncIndexLogMergePolicy *>(&policy)) {
    return "ObAsyncIndexLogMergePolicy";
  } else {
    return "ObAsyncIndexMergePolicyBase";
  }
}

class TestAsyncIndexMergePolicy : public ::testing::Test
{
public:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, lmp_policy_.init());
    ASSERT_EQ(OB_SUCCESS, tmp_policy_.init());
    ASSERT_EQ(OB_SUCCESS, merge_policy_.init(10));
  }

  int build_merge_infos(
      const MergeInfoSpec *specs,
      const int64_t spec_cnt,
      ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos)
  {
    int ret = OB_SUCCESS;
    merge_infos.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < spec_cnt; ++i) {
      ObAsyncIndexSSTableMergeInfo info;
      info.start_snapshot_version_ = specs[i].start_scn_;
      info.end_snapshot_version_ = specs[i].end_scn_;
      info.seg_size_ = specs[i].seg_size_;
      info.level_ = 0.0f;
      if (OB_FAIL(merge_infos.push_back(info))) {
      }
    }
    return ret;
  }

  int build_base_plus_inc_segments(
      const int64_t base_size,
      const int64_t inc_cnt,
      const int64_t inc_size,
      ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos)
  {
    int ret = OB_SUCCESS;
    merge_infos.reset();
    ObAsyncIndexSSTableMergeInfo base;
    base.start_snapshot_version_ = 0;
    base.end_snapshot_version_ = 50;
    base.seg_size_ = base_size;
    base.level_ = 0.0f;
    if (OB_FAIL(merge_infos.push_back(base))) {
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < inc_cnt; ++i) {
      ObAsyncIndexSSTableMergeInfo inc;
      inc.start_snapshot_version_ = 50 + i * 10;
      inc.end_snapshot_version_ = 60 + i * 10;
      inc.seg_size_ = inc_size;
      inc.level_ = 0.0f;
      if (OB_FAIL(merge_infos.push_back(inc))) {
      }
    }
    return ret;
  }

  void expect_range(
      const ObNewVersionRange &range,
      const int64_t base_version,
      const int64_t snapshot_version)
  {
    ASSERT_TRUE(range.is_valid());
    EXPECT_EQ(base_version, range.base_version_);
    EXPECT_EQ(snapshot_version, range.snapshot_version_);
  }

  void expect_merge(
      ObAsyncIndexMergePolicyBase &policy,
      ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos,
      const int64_t base_version,
      const int64_t snapshot_version)
  {
    SCOPED_TRACE(::testing::Message()
                 << "policy type: " << async_index_merge_policy_type_name(policy));
    ObNewVersionRange merge_range;
    const int ret = policy.find_merges(merge_infos, merge_range);
    ASSERT_EQ(OB_SUCCESS, ret)
        << "policy=" << async_index_merge_policy_type_name(policy)
        << ", ret=" << ret;
    expect_range(merge_range, base_version, snapshot_version);
  }

  void expect_no_merge(
      ObAsyncIndexMergePolicyBase &policy,
      ObIArray<ObAsyncIndexSSTableMergeInfo> &merge_infos)
  {
    SCOPED_TRACE(::testing::Message()
                 << "policy type: " << async_index_merge_policy_type_name(policy));
    ObNewVersionRange merge_range;
    const int ret = policy.find_merges(merge_infos, merge_range);
    ASSERT_EQ(OB_NO_NEED_MERGE, ret)
        << "policy=" << async_index_merge_policy_type_name(policy)
        << ", ret=" << ret
        << ", merge_range.valid=" << merge_range.is_valid();
    ASSERT_FALSE(merge_range.is_valid())
        << "policy=" << async_index_merge_policy_type_name(policy);
  }

  ObAsyncIndexLogMergePolicy lmp_policy_;
  ObAsyncIndexTieredMergePolicy tmp_policy_;
  ObAsyncIndexMergePolicy merge_policy_;
};

TEST_F(TestAsyncIndexMergePolicy, not_init)
{
  ObAsyncIndexLogMergePolicy lmp;
  ObAsyncIndexTieredMergePolicy tmp;
  ObAsyncIndexMergePolicy merge_policy;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  ObNewVersionRange merge_range;
  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 10, 10000, merge_infos));
  ASSERT_EQ(OB_NOT_INIT, lmp.find_merges(merge_infos, merge_range));
  ASSERT_EQ(OB_NOT_INIT, tmp.find_merges(merge_infos, merge_range));
  ASSERT_EQ(OB_NOT_INIT, merge_policy.find_merges(merge_infos, merge_range));
}

TEST_F(TestAsyncIndexMergePolicy, empty_or_single_segment)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 4> merge_infos;
  ObNewVersionRange merge_range;

  ASSERT_EQ(OB_INVALID_ARGUMENT, lmp_policy_.find_merges(merge_infos, merge_range));
  ASSERT_FALSE(merge_range.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, tmp_policy_.find_merges(merge_infos, merge_range));
  ASSERT_FALSE(merge_range.is_valid());
  ASSERT_EQ(OB_INVALID_ARGUMENT, merge_policy_.find_merges(merge_infos, merge_range));
  ASSERT_FALSE(merge_range.is_valid());

  const MergeInfoSpec specs[] = {{0, 10, 100000}};
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 1, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_no_merge(merge_policy_, merge_infos);
}

TEST_F(TestAsyncIndexMergePolicy, invalid_order_or_gap)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 4> merge_infos;
  ObNewVersionRange merge_range;

  const MergeInfoSpec disorder_specs[] = {{10, 20, 100}, {0, 10, 100}};
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(disorder_specs, 2, merge_infos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, lmp_policy_.find_merges(merge_infos, merge_range));
  ASSERT_EQ(OB_INVALID_ARGUMENT, tmp_policy_.find_merges(merge_infos, merge_range));
  ASSERT_EQ(OB_INVALID_ARGUMENT, merge_policy_.find_merges(merge_infos, merge_range));

  const MergeInfoSpec gap_specs[] = {{0, 30, 100}, {20, 40, 100}};
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(gap_specs, 2, merge_infos));
  ASSERT_EQ(OB_INVALID_ARGUMENT, lmp_policy_.find_merges(merge_infos, merge_range));
  ASSERT_EQ(OB_INVALID_ARGUMENT, tmp_policy_.find_merges(merge_infos, merge_range));
  ASSERT_EQ(OB_INVALID_ARGUMENT, merge_policy_.find_merges(merge_infos, merge_range));
}

TEST_F(TestAsyncIndexMergePolicy, insufficient_segments_no_merge)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  // LMP: merge_factor=10，inc 不足 10 段
  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 5, 10000, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_no_merge(merge_policy_, merge_infos);

  // TMP / Hybrid: budget 认为结构 healthy，3 个 inc 不 merge
  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 3, 10000, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_no_merge(merge_policy_, merge_infos);
}

TEST_F(TestAsyncIndexMergePolicy, merge_similar_inc_without_base)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 10, 10000, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_merge(tmp_policy_, merge_infos, 50, 150);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

TEST_F(TestAsyncIndexMergePolicy, moderate_base_tmp_no_merge)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  // base 500 万 < too_big 阈值 7.5M，TMP 仍视为 eligible 并抬高 budget
  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(5000000, 10, 10000, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

TEST_F(TestAsyncIndexMergePolicy, prefer_balanced_window)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 8000000},
    {50, 60, 10000}, {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000},
    {90, 100, 10000}, {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000},
    {130, 140, 10000}, {140, 150, 10000}, {150, 160, 10000}, {160, 170, 10000},
    {170, 180, 80000}, {180, 190, 90000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 15, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_merge(tmp_policy_, merge_infos, 50, 150);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

TEST_F(TestAsyncIndexMergePolicy, lmp_merges_many_uniform_segments)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 32> merge_infos;
  const MergeInfoSpec specs[20] = {
    {0, 10, 10000}, {10, 20, 10000}, {20, 30, 10000}, {30, 40, 10000}, {40, 50, 10000},
    {50, 60, 10000}, {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000}, {140, 150, 10000},
    {150, 160, 10000}, {160, 170, 10000}, {170, 180, 10000}, {180, 190, 10000}, {190, 200, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 20, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 0, 100);
  expect_merge(tmp_policy_, merge_infos, 0, 100);
  expect_merge(merge_policy_, merge_infos, 0, 100);
}

TEST_F(TestAsyncIndexMergePolicy, huge_middle_segment)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 10, 10000},
    {10, 20, 8000000},
    {20, 30, 10000}, {30, 40, 10000}, {40, 50, 10000}, {50, 60, 10000},
    {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 12, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 20, 120);
  expect_merge(tmp_policy_, merge_infos, 20, 120);
  expect_merge(merge_policy_, merge_infos, 20, 120);
}

TEST_F(TestAsyncIndexMergePolicy, merge_below_min_merge_size)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 10, 100, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_merge(tmp_policy_, merge_infos, 50, 150);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

TEST_F(TestAsyncIndexMergePolicy, lmp_min_merge_size_expansion_cap)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 10, 1400}, {10, 20, 1400}, {20, 30, 1400}, {30, 40, 1400}, {40, 50, 1400},
    {50, 60, 1400}, {60, 70, 1400}, {70, 80, 1400}, {80, 90, 1400}, {90, 100, 1400},
    {100, 110, 1400},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 11, merge_infos));
  // 10 段合计 14000 < min_merge_size；扩展时第 11 段会使 merge_size 超限，停在 10 段
  expect_merge(lmp_policy_, merge_infos, 0, 100);
  expect_merge(tmp_policy_, merge_infos, 0, 100);
  expect_merge(merge_policy_, merge_infos, 0, 100);
}

TEST_F(TestAsyncIndexMergePolicy, lmp_max_merge_size_truncation)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const int64_t segment_size = ObAsyncIndexMergePolicy::DEFAULT_MAX_MERGE_SIZE / 8;
  const MergeInfoSpec specs[] = {
    {0, 10, segment_size}, {10, 20, segment_size}, {20, 30, segment_size}, {30, 40, segment_size},
    {40, 50, segment_size}, {50, 60, segment_size}, {60, 70, segment_size}, {70, 80, segment_size},
    {80, 90, segment_size}, {90, 100, segment_size},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 10, merge_infos));
  // 前 8 段恰好达到生产上限 5GiB，第 9 段超限；等于上限时仍可选入。
  // TMP 保留独立的 15MB 上限，这些段均过大，不参与合并。
  expect_merge(lmp_policy_, merge_infos, 0, 80);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 0, 80);
}

TEST_F(TestAsyncIndexMergePolicy, lmp_singleton_too_large_skipped)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 10, ObAsyncIndexMergePolicy::DEFAULT_MAX_MERGE_SIZE + 1},
    {10, 20, 10000}, {20, 30, 10000}, {30, 40, 10000}, {40, 50, 10000},
    {50, 60, 10000}, {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000},
    {90, 100, 10000}, {100, 110, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 11, merge_infos));
  // 首段为生产上限 5GiB + 1B，跳过超大单段后选择后面 10 个小段。
  expect_merge(lmp_policy_, merge_infos, 10, 110);
  expect_merge(tmp_policy_, merge_infos, 10, 110);
  expect_merge(merge_policy_, merge_infos, 10, 110);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_moderate_base_merges)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(5000000, 10, 10000, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_nine_inc_merges)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 9, 10000, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_merge(tmp_policy_, merge_infos, 50, 140);
  expect_no_merge(merge_policy_, merge_infos);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_large_inc_small_tail)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 10000000},
    {50, 60, 4000000},
    {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000},
    {140, 150, 10000}, {150, 160, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 12, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 60, 160);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 60, 160);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_avoids_large_small_mix_window)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 8000000},
    {50, 60, 5000000},
    {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000},
    {140, 150, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 11, merge_infos));
  
  expect_no_merge(lmp_policy_, merge_infos);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_no_merge(merge_policy_, merge_infos);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_segment_aware_budget_400w_inc)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 10000000},
    {50, 60, 4000000},
    {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000},
    {140, 150, 10000}, {150, 160, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 12, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 60, 160);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 60, 160);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_segment_aware_budget_480w_inc)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 10000000},
    {50, 60, 4800000},
    {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000},
    {140, 150, 10000}, {150, 160, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 12, merge_infos));
  
  expect_merge(lmp_policy_, merge_infos, 60, 160);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 60, 160);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_segment_aware_budget_uniform_small_inc)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 15, 10000, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_merge(tmp_policy_, merge_infos, 50, 150);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_segment_aware_budget_no_eligible)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 4> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 10000000},
    {50, 60, 8000000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 2, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_no_merge(merge_policy_, merge_infos);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_fifteen_uniform_inc_merges)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(5000000, 10, 10000, merge_infos));
  // base 500 万 eligible，10 小 inc + base → tier: 8 + 2 + 1
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_segment_aware_budget_few_segments_floor)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 3, 10000, merge_infos));
  // 3 个 eligible inc，不足 segments_per_tier，仍取 floor=8
  expect_no_merge(lmp_policy_, merge_infos);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_no_merge(merge_policy_, merge_infos);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_segment_aware_budget_ten_uniform_inc)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 10, 10000, merge_infos));
  // base too_big，10 个同级小 inc → 8 + 2
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_merge(tmp_policy_, merge_infos, 50, 150);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

TEST_F(TestAsyncIndexMergePolicy, hybrid_segment_aware_budget_mixed_sizes)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 8000000},
    {50, 60, 10000}, {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000},
    {90, 100, 100000}, {100, 110, 100000}, {110, 120, 100000}, {120, 130, 100000},
    {130, 140, 1000000}, {140, 150, 1000000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 11, merge_infos));
  // eligible: 4x10k + 4x100k + 2x1M；sorted tier: 4 + 4 + 2 = 10
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

// 例 6 / LMP 好 TMP 差：中等 base 500 万，三策略对比
TEST_F(TestAsyncIndexMergePolicy, scenario_moderate_base_three_policies)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(5000000, 10, 10000, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

// 均匀 refresh：10 个同级小 inc，三策略均 merge
TEST_F(TestAsyncIndexMergePolicy, scenario_uniform_inc_all_policies_merge)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 10, 10000, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_merge(tmp_policy_, merge_infos, 50, 150);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

// TMP 好 LMP 差：9 个 inc，TMP/Hybrid merge，LMP 等第 10 段
TEST_F(TestAsyncIndexMergePolicy, scenario_nine_inc_tmp_and_hybrid_merge)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 9, 10000, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_merge(tmp_policy_, merge_infos, 50, 140);
  expect_no_merge(merge_policy_, merge_infos);
}

// budget 边界：8 个 inc，LMP/TMP/生产策略均 NO_MERGE
TEST_F(TestAsyncIndexMergePolicy, scenario_eight_inc_budget_boundary_no_merge)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 8, 10000, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_no_merge(merge_policy_, merge_infos);
}

// 大 inc 抬高 TMP budget：base 1000 万 + 400 万 inc + 10 小 inc
TEST_F(TestAsyncIndexMergePolicy, scenario_large_inc_inflates_tmp_budget)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 10000000},
    {50, 60, 4000000},
    {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000},
    {140, 150, 10000}, {150, 160, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 12, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 60, 160);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 60, 160);
}

// 多轮 merge 后 inc 接近 base/2：480 万 inc + 10 小 inc
TEST_F(TestAsyncIndexMergePolicy, scenario_merged_inc_near_base_half)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 10000000},
    {50, 60, 4800000},
    {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000},
    {140, 150, 10000}, {150, 160, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 12, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 60, 160);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 60, 160);
}

// too_big 中间段后仅 9 个小 inc：LMP/生产策略不合，TMP 合尾部
TEST_F(TestAsyncIndexMergePolicy, scenario_nine_inc_tail_after_too_big_middle)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 8000000},
    {50, 60, 10000},
    {60, 70, 8000000},
    {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000}, {100, 110, 10000},
    {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000}, {140, 150, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 11, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_merge(tmp_policy_, merge_infos, 70, 150);
  expect_no_merge(merge_policy_, merge_infos);
}

// LMP level 隔离：前缀大 inc 不参与，只合 10 个小 inc
TEST_F(TestAsyncIndexMergePolicy, scenario_lmp_skips_large_inc_prefix)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 8000000},
    {50, 60, 1000000},
    {60, 70, 10000}, {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000},
    {100, 110, 10000}, {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000},
    {140, 150, 10000}, {150, 160, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 12, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 60, 160);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 60, 160);
}

// 段数过少：3 个 inc，TMP budget 认为 healthy
TEST_F(TestAsyncIndexMergePolicy, scenario_three_inc_tmp_healthy_no_merge)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 3, 10000, merge_infos));
  expect_no_merge(lmp_policy_, merge_infos);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_no_merge(merge_policy_, merge_infos);
}

// 800 万 base（too_big）+ 10 小 inc：TMP 正常路径
TEST_F(TestAsyncIndexMergePolicy, scenario_too_big_base_tmp_merge)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;

  ASSERT_EQ(OB_SUCCESS, build_base_plus_inc_segments(8000000, 10, 10000, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 50, 150);
  expect_merge(tmp_policy_, merge_infos, 50, 150);
  expect_merge(merge_policy_, merge_infos, 50, 150);
}

// 前缀 orphan：too_big 前仅 1 个小 inc，三策略均无法 merge 该 orphan
TEST_F(TestAsyncIndexMergePolicy, scenario_prefix_orphan_inc_no_merge)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const MergeInfoSpec specs[] = {
    {0, 50, 8000000},
    {50, 60, 10000},
    {60, 70, 8000000},
    {70, 80, 10000}, {80, 90, 10000}, {90, 100, 10000}, {100, 110, 10000},
    {110, 120, 10000}, {120, 130, 10000}, {130, 140, 10000}, {140, 150, 10000},
    {150, 160, 10000}, {160, 170, 10000}, {170, 180, 10000},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 14, merge_infos));
  expect_merge(lmp_policy_, merge_infos, 70, 170);
  expect_merge(tmp_policy_, merge_infos, 70, 170);
  expect_merge(merge_policy_, merge_infos, 70, 170);
}

// 超过生产 5GiB 上限时截断；TMP 保留独立的 15MB 上限，不合并这些超大段。
TEST_F(TestAsyncIndexMergePolicy, hit_too_large_merges_at_max_merged_cap)
{
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 16> merge_infos;
  const int64_t segment_size = ObAsyncIndexMergePolicy::DEFAULT_MAX_MERGE_SIZE / 8 + 1;
  const MergeInfoSpec specs[] = {
    {0, 10, segment_size}, {10, 20, segment_size}, {20, 30, segment_size}, {30, 40, segment_size},
    {40, 50, segment_size}, {50, 60, segment_size}, {60, 70, segment_size}, {70, 80, segment_size},
    {80, 90, segment_size}, {90, 100, segment_size},
  };

  ASSERT_EQ(OB_SUCCESS, build_merge_infos(specs, 10, merge_infos));
  // 每段比 5GiB / 8 多 1B，第 8 段使总量超限，只选择前 7 段。
  expect_merge(lmp_policy_, merge_infos, 0, 70);
  expect_no_merge(tmp_policy_, merge_infos);
  expect_merge(merge_policy_, merge_infos, 0, 70);
}

TEST_F(TestAsyncIndexMergePolicy, align_sparse_index_inputs_without_splitting_tables)
{
  ObSEArray<ObSEArray<ObAsyncIndexSSTableMergeInfo, 8>, 4> inputs;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> index;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> output;
  ObAsyncIndexSSTableMergeInfo info;
  info.start_snapshot_version_ = 0;
  info.end_snapshot_version_ = 100;
  info.seg_size_ = 10;
  ASSERT_EQ(OB_SUCCESS, index.push_back(info));
  info.start_snapshot_version_ = 200;
  info.end_snapshot_version_ = 300;
  info.seg_size_ = 20;
  ASSERT_EQ(OB_SUCCESS, index.push_back(info));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  index.reuse();
  info.start_snapshot_version_ = 0;
  info.end_snapshot_version_ = 250;
  info.seg_size_ = 40;
  ASSERT_EQ(OB_SUCCESS, index.push_back(info));
  info.start_snapshot_version_ = 400;
  info.end_snapshot_version_ = 500;
  info.seg_size_ = 80;
  ASSERT_EQ(OB_SUCCESS, index.push_back(info));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  index.reuse();
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  int64_t snapshot = 500;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::align_merge_infos(inputs, snapshot, output));
  ASSERT_EQ(2, output.count());
  EXPECT_EQ(0, output.at(0).start_snapshot_version_);
  EXPECT_EQ(300, output.at(0).end_snapshot_version_);
  EXPECT_EQ(70, output.at(0).seg_size_);
  EXPECT_EQ(400, output.at(1).start_snapshot_version_);
  EXPECT_EQ(500, output.at(1).end_snapshot_version_);
  EXPECT_EQ(80, output.at(1).seg_size_);
  ObAsyncIndexMergePolicy policy;
  ObNewVersionRange range;
  ASSERT_EQ(OB_SUCCESS, policy.init(2));
  ASSERT_EQ(OB_SUCCESS, policy.find_merges(output, range));
  EXPECT_EQ(0, range.base_version_);
  EXPECT_EQ(500, range.snapshot_version_);
}

TEST_F(TestAsyncIndexMergePolicy, align_empty_inputs_does_not_fabricate_segments)
{
  ObSEArray<ObSEArray<ObAsyncIndexSSTableMergeInfo, 8>, 4> inputs;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> empty;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> output;
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(empty));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(empty));
  int64_t snapshot = 100;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::align_merge_infos(inputs, snapshot, output));
  EXPECT_TRUE(output.empty());
}

TEST_F(TestAsyncIndexMergePolicy, align_many_incremental_inputs_into_two_groups)
{
  ObSEArray<ObSEArray<ObAsyncIndexSSTableMergeInfo, 8>, 4> inputs;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> index;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> output;
  MergeInfoSpec a[49] = {{0, 100, 1000000}};
  for (int64_t i = 0; i < 48; ++i) {
    a[i + 1] = {100 + i, 101 + i, 1};
  }
  const MergeInfoSpec b[] = {{0, 147, 1000000}, {147, 148, 1}};
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(a, ARRAYSIZEOF(a), index));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(b, ARRAYSIZEOF(b), index));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  ASSERT_EQ(49, inputs.at(0).count());
  ASSERT_EQ(2, inputs.at(1).count());

  // B's major joins A's major and 47 incremental inputs into one group.
  // The aligned group count does not represent the actual incremental input count.
  int64_t snapshot = 148;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::align_merge_infos(inputs, snapshot, output));
  EXPECT_EQ(148, snapshot);
  ASSERT_EQ(2, output.count());
  EXPECT_EQ(0, output.at(0).start_snapshot_version_);
  EXPECT_EQ(147, output.at(0).end_snapshot_version_);
  EXPECT_EQ(2000047, output.at(0).seg_size_);
  EXPECT_EQ(147, output.at(1).start_snapshot_version_);
  EXPECT_EQ(148, output.at(1).end_snapshot_version_);
  EXPECT_EQ(2, output.at(1).seg_size_);
}

TEST_F(TestAsyncIndexMergePolicy, align_many_incremental_inputs_into_single_group)
{
  ObSEArray<ObSEArray<ObAsyncIndexSSTableMergeInfo, 8>, 4> inputs;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> index;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> output;
  MergeInfoSpec a[49] = {{0, 100, 1000000}};
  for (int64_t i = 0; i < 48; ++i) {
    a[i + 1] = {100 + i, 101 + i, 1};
  }
  const MergeInfoSpec b[] = {{0, 148, 1000000}};
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(a, ARRAYSIZEOF(a), index));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(b, ARRAYSIZEOF(b), index));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  ASSERT_EQ(49, inputs.at(0).count());
  ASSERT_EQ(1, inputs.at(1).count());

  // One aligned group can contain all 48 incremental inputs from A.
  int64_t snapshot = 148;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::align_merge_infos(inputs, snapshot, output));
  EXPECT_EQ(148, snapshot);
  ASSERT_EQ(1, output.count());
  EXPECT_EQ(0, output.at(0).start_snapshot_version_);
  EXPECT_EQ(148, output.at(0).end_snapshot_version_);
  EXPECT_EQ(2000048, output.at(0).seg_size_);
}

TEST_F(TestAsyncIndexMergePolicy, align_sparse_adjacent_boundaries_remain_independent)
{
  ObSEArray<ObSEArray<ObAsyncIndexSSTableMergeInfo, 8>, 4> inputs;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> index;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> output;
  ObAsyncIndexSSTableMergeInfo info;
  for (int64_t i = 0; i < 3; ++i) {
    index.reuse();
    info.start_snapshot_version_ = i * 100;
    info.end_snapshot_version_ = (i + 1) * 100;
    info.seg_size_ = 10;
    ASSERT_EQ(OB_SUCCESS, index.push_back(info));
    ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  }
  int64_t snapshot = 300;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::align_merge_infos(inputs, snapshot, output));
  ASSERT_EQ(3, output.count());
  for (int64_t i = 0; i < 3; ++i) {
    EXPECT_EQ(i * 100, output.at(i).start_snapshot_version_);
    EXPECT_EQ((i + 1) * 100, output.at(i).end_snapshot_version_);
    EXPECT_EQ(10, output.at(i).seg_size_);
  }
}

TEST_F(TestAsyncIndexMergePolicy, align_crossing_inputs_keeps_mergeable_prefix)
{
  ObSEArray<ObSEArray<ObAsyncIndexSSTableMergeInfo, 8>, 4> inputs;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> index;
  ObSEArray<ObAsyncIndexSSTableMergeInfo, 8> output;
  const MergeInfoSpec a[] = {{0, 100, 10}, {100, 200, 10}, {200, 400, 20}};
  const MergeInfoSpec b[] = {{0, 100, 10}, {100, 200, 10}, {200, 300, 10}, {300, 400, 10}};
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(a, ARRAYSIZEOF(a), index));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(b, ARRAYSIZEOF(b), index));
  ASSERT_EQ(OB_SUCCESS, inputs.push_back(index));

  // Crossing, exact boundary, empty prefix, and a published empty tail.
  const int64_t limits[] = {300, 200, 150, 100, 50, 450};
  const int64_t expected_snapshots[] = {200, 200, 100, 100, 0, 450};
  const int64_t expected_counts[] = {2, 2, 1, 1, 0, 3};
  for (int64_t i = 0; i < ARRAYSIZEOF(limits); ++i) {
    int64_t snapshot = limits[i];
    ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::align_merge_infos(inputs, snapshot, output));
    EXPECT_EQ(expected_snapshots[i], snapshot);
    ASSERT_EQ(expected_counts[i], output.count());
    if (300 == limits[i]) {
      ObAsyncIndexMergePolicy policy;
      ObNewVersionRange range;
      ASSERT_EQ(OB_SUCCESS, policy.init(2));
      ASSERT_EQ(OB_SUCCESS, policy.find_merges(output, range));
      EXPECT_EQ(0, range.base_version_);
      EXPECT_EQ(200, range.snapshot_version_);
    }
  }

  // B bridges A's boundary at 200, so lowering the limit to 200 is still unsafe.
  const MergeInfoSpec bridging[] = {{0, 100, 10}, {100, 300, 20}, {300, 400, 10}};
  ASSERT_EQ(OB_SUCCESS, build_merge_infos(bridging, ARRAYSIZEOF(bridging), inputs.at(1)));
  int64_t snapshot = 300;
  ASSERT_EQ(OB_SUCCESS, ObAsyncIndexMergePolicyHelper::align_merge_infos(inputs, snapshot, output));
  EXPECT_EQ(100, snapshot);
  ASSERT_EQ(1, output.count());
  EXPECT_EQ(100, output.at(0).end_snapshot_version_);
}

TEST_F(TestAsyncIndexMergePolicy, s5_m07_inc_major_does_not_prepare_delete_bitmap)
{
  ObArenaAllocator allocator;
  ObTabletMergeDagParam param;
  param.merge_type_ = INC_MAJOR_MERGE;
  ObAsyncIndexMergeCtx ctx(param, allocator);
  ASSERT_TRUE(share::schema::is_async_inverted_storage_index(
      share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_INDEX_LOCAL));
  ASSERT_TRUE(share::schema::is_async_inverted_storage_index(
      share::schema::INDEX_TYPE_ASYNC_SEARCH_TEXT_MATCH_PHRASE_INDEX_LOCAL));
  // No LS/schema/tablet is installed: the IncMajor branch must return before
  // any shared Delete lookup. Calling the real context proves this routing.
  ASSERT_EQ(OB_SUCCESS, ctx.prepare_deletion_bitmap());
  ASSERT_EQ(OB_INVALID_ID, ctx.delete_index_table_id_);
  ASSERT_FALSE(ctx.delete_index_tablet_id_.is_valid());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
