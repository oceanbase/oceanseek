/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef UNITTEST_SQL_ENGINE_DAS_TEST_DAS_BMM_COMMON_H_
#define UNITTEST_SQL_ENGINE_DAS_TEST_DAS_BMM_COMMON_H_

#include <map>
#include <set>
#include <vector>

#include "share/rc/ob_tenant_base.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "sql/das/search/ob_das_doc_score_accumulator.h"
#include "sql/das/search/ob_i_das_search_op.h"

namespace oceanbase
{
namespace sql
{
namespace test
{

class BMMVectorizedMergeConfigGuard final
{
public:
  explicit BMMVectorizedMergeConfigGuard(const bool enabled = true)
    : tenant_config_(TENANT_CONF(MTL_ID())),
      old_value_(false),
      is_valid_(tenant_config_.is_valid())
  {
    if (is_valid_) {
      old_value_ = tenant_config_->_enable_bmm_vectorized_merge;
      tenant_config_->_enable_bmm_vectorized_merge = enabled;
    }
  }

  ~BMMVectorizedMergeConfigGuard()
  {
    if (is_valid_) {
      tenant_config_->_enable_bmm_vectorized_merge = old_value_;
    }
  }

  bool is_valid() const { return is_valid_; }

private:
  omt::ObTenantConfigGuard tenant_config_;
  bool old_value_;
  bool is_valid_;
};

struct ScriptedBMMSearchOpParam : public ObIDASSearchOpParam
{
  ScriptedBMMSearchOpParam() : ObIDASSearchOpParam(DAS_SEARCH_OP_DUMMY) {}
  int get_children_ops(common::ObIArray<ObIDASSearchOp *> &children) const override
  {
    UNUSED(children);
    return OB_SUCCESS;
  }
};

struct ScriptedBMMPosting
{
  ScriptedBMMPosting()
    : doc_id_(0), score_(0.0), block_end_id_(0), block_max_score_(0.0)
  {}

  ScriptedBMMPosting(
      const uint64_t doc_id,
      const double score,
      const uint64_t block_end_id,
      const double block_max_score)
    : doc_id_(doc_id),
      score_(score),
      block_end_id_(block_end_id),
      block_max_score_(block_max_score)
  {}

  uint64_t doc_id_;
  double score_;
  uint64_t block_end_id_;
  double block_max_score_;
};

using ScriptedBMMPostingList = std::vector<ScriptedBMMPosting>;

// Builds posting scripts without changing their order or values.
class ScriptedBMMPostingBuilder final
{
public:
  ScriptedBMMPostingBuilder &add(
      const uint64_t doc_id,
      const double score,
      const uint64_t block_end_id,
      const double block_max_score)
  {
    postings_.push_back(ScriptedBMMPosting(doc_id, score, block_end_id, block_max_score));
    return *this;
  }

  const ScriptedBMMPostingList &build() const { return postings_; }

private:
  ScriptedBMMPostingList postings_;
};

struct BMMReferenceResult
{
  BMMReferenceResult(const uint64_t doc_id, const double score)
    : doc_id_(doc_id), score_(score)
  {}

  uint64_t doc_id_;
  double score_;
};

// Computes expected BMM output only from test postings.
class BMMReferenceOracle final
{
public:
  static std::vector<BMMReferenceResult> evaluate(
      const std::vector<ScriptedBMMPostingList> &scoring_postings,
      const ScriptedBMMPostingList *filter_postings,
      const double threshold)
  {
    std::map<uint64_t, double> scores;
    std::set<uint64_t> filter_ids;
    std::vector<BMMReferenceResult> results;
    for (const ScriptedBMMPostingList &posting_list : scoring_postings) {
      for (const ScriptedBMMPosting &posting : posting_list) {
        scores[posting.doc_id_] += posting.score_;
      }
    }
    if (nullptr != filter_postings) {
      for (const ScriptedBMMPosting &posting : *filter_postings) {
        filter_ids.insert(posting.doc_id_);
      }
    }
    for (const std::pair<const uint64_t, double> &entry : scores) {
      if ((nullptr == filter_postings || filter_ids.count(entry.first) > 0)
          && entry.second > threshold) {
        results.push_back(BMMReferenceResult(entry.first, entry.second));
      }
    }
    return results;
  }
};

struct ScriptedBMMRangeMaxResult
{
  ScriptedBMMRangeMaxResult(
      const uint64_t start_id,
      const bool start_inclusive,
      const uint64_t end_id,
      const double max_score,
      const bool can_calc,
      const int ret)
    : start_id_(start_id),
      start_inclusive_(start_inclusive),
      end_id_(end_id),
      max_score_(max_score),
      can_calc_(can_calc),
      ret_(ret)
  {}

  uint64_t start_id_;
  bool start_inclusive_;
  uint64_t end_id_;
  double max_score_;
  bool can_calc_;
  int ret_;
};

enum class ScriptedBMMCallType
{
  NONE,
  OPEN,
  CLOSE,
  RESCAN,
  RESCAN_SEGMENT,
  NEXT,
  ADVANCE,
  SHALLOW,
  CALC_MAX_SCORE,
  RANGE_MAX,
  DRAIN,
  ACTIVE_DRAIN,
};

struct ScriptedBMMCallCounters
{
  ScriptedBMMCallCounters()
    : open_(0),
      close_(0),
      rescan_(0),
      rescan_segment_(0),
      next_(0),
      advance_(0),
      shallow_(0),
      calc_max_score_(0),
      range_max_(0),
      drain_(0),
      active_drain_(0)
  {}

  int64_t open_;
  int64_t close_;
  int64_t rescan_;
  int64_t rescan_segment_;
  int64_t next_;
  int64_t advance_;
  int64_t shallow_;
  int64_t calc_max_score_;
  int64_t range_max_;
  int64_t drain_;
  int64_t active_drain_;
};

class ScriptedBMMSearchOp : public ObIDASSearchOp
{
public:
  ScriptedBMMSearchOp(
      ObDASSearchCtx &search_ctx,
      const uint64_t posting_id,
      const uint64_t block_end_id,
      const double block_max_score,
      const double dim_max_score)
    : ObIDASSearchOp(search_ctx),
      single_posting_(posting_id, block_max_score, block_end_id, block_max_score),
      postings_(&single_posting_),
      posting_count_(1),
      dim_max_score_(dim_max_score),
      advance_to_id_count_(0),
      load_curr_score_count_(0),
      enable_id_only_(false),
      current_idx_(-1),
      range_max_score_(0.0),
      can_calc_range_max_(false),
      range_max_ret_(OB_SUCCESS),
      range_max_results_(),
      counters_(),
      use_batch_drain_(false),
      next_segment_postings_(nullptr),
      next_segment_posting_count_(0),
      has_next_segment_(false),
      fail_type_(ScriptedBMMCallType::NONE),
      fail_on_call_(0),
      fail_ret_(OB_SUCCESS)
  {}

  ScriptedBMMSearchOp(
      ObDASSearchCtx &search_ctx,
      const ScriptedBMMPosting *postings,
      const int64_t posting_count,
      const double dim_max_score)
    : ObIDASSearchOp(search_ctx),
      single_posting_(),
      postings_(postings),
      posting_count_(posting_count),
      dim_max_score_(dim_max_score),
      advance_to_id_count_(0),
      load_curr_score_count_(0),
      enable_id_only_(false),
      current_idx_(-1),
      range_max_score_(0.0),
      can_calc_range_max_(false),
      range_max_ret_(OB_SUCCESS),
      range_max_results_(),
      counters_(),
      use_batch_drain_(false),
      next_segment_postings_(nullptr),
      next_segment_posting_count_(0),
      has_next_segment_(false),
      fail_type_(ScriptedBMMCallType::NONE),
      fail_on_call_(0),
      fail_ret_(OB_SUCCESS)
  {}

  ScriptedBMMSearchOp(
      ObDASSearchCtx &search_ctx,
      const ScriptedBMMPostingList &postings,
      const double dim_max_score)
    : ScriptedBMMSearchOp(
        search_ctx, postings.empty() ? nullptr : postings.data(), postings.size(), dim_max_score)
  {}

  int64_t get_calc_max_score_count() const { return counters_.calc_max_score_; }
  int64_t get_advance_to_count() const { return counters_.advance_; }
  int64_t get_calc_range_max_score_count() const { return counters_.range_max_; }
  const ScriptedBMMCallCounters &get_call_counters() const { return counters_; }

  int64_t get_advance_to_id_count() const
  {
    return advance_to_id_count_;
  }

  int64_t get_load_curr_score_count() const
  {
    return load_curr_score_count_;
  }

  void enable_advance_to_id()
  {
    enable_id_only_ = true;
  }

  bool can_advance_to_id() const override
  {
    return enable_id_only_;
  }

  void set_range_max_score(const double range_max_score)
  {
    range_max_score_ = range_max_score;
    can_calc_range_max_ = true;
    range_max_ret_ = OB_SUCCESS;
  }

  void set_range_max_ret(const int range_max_ret)
  {
    can_calc_range_max_ = false;
    range_max_ret_ = range_max_ret;
  }

  void add_range_max_result(
      const uint64_t start_id,
      const bool start_inclusive,
      const uint64_t end_id,
      const double max_score,
      const bool can_calc,
      const int ret = OB_SUCCESS)
  {
    range_max_results_.push_back(ScriptedBMMRangeMaxResult(
        start_id, start_inclusive, end_id, max_score, can_calc, ret));
  }

  void use_batch_drain(const bool use_batch_drain) { use_batch_drain_ = use_batch_drain; }

  // Switch to this posting script on the next segment rescan.
  void set_next_segment(const ScriptedBMMPostingList &postings)
  {
    next_segment_postings_ = postings.empty() ? nullptr : postings.data();
    next_segment_posting_count_ = postings.size();
    has_next_segment_ = true;
  }

  void fail_on(
      const ScriptedBMMCallType type,
      const int64_t nth_call,
      const int ret)
  {
    fail_type_ = type;
    fail_on_call_ = nth_call;
    fail_ret_ = ret;
  }

  void clear_failure()
  {
    fail_type_ = ScriptedBMMCallType::NONE;
    fail_on_call_ = 0;
    fail_ret_ = OB_SUCCESS;
  }

protected:
  int do_open() override
  {
    const int ret = record_call(ScriptedBMMCallType::OPEN, counters_.open_);
    if (OB_SUCCESS == ret) {
      current_idx_ = -1;
    }
    return OB_SUCCESS == ret && !is_valid_posting_array() ? OB_INVALID_ARGUMENT : ret;
  }

  int do_close() override
  {
    return record_call(ScriptedBMMCallType::CLOSE, counters_.close_);
  }

  int do_rescan() override
  {
    const int ret = record_call(ScriptedBMMCallType::RESCAN, counters_.rescan_);
    if (OB_SUCCESS == ret) {
      current_idx_ = -1;
    }
    return OB_SUCCESS == ret && !is_valid_posting_array() ? OB_INVALID_ARGUMENT : ret;
  }

  int do_rescan_segment(const common::ObNewVersionRange &range) override
  {
    UNUSED(range);
    const int ret = record_call(ScriptedBMMCallType::RESCAN_SEGMENT, counters_.rescan_segment_);
    if (OB_SUCCESS == ret) {
      if (has_next_segment_) {
        postings_ = next_segment_postings_;
        posting_count_ = next_segment_posting_count_;
        has_next_segment_ = false;
      }
      current_idx_ = -1;
    }
    return ret;
  }

  int do_next_rowid(ObDASRowID &rowid, double &score) override
  {
    int ret = record_call(ScriptedBMMCallType::NEXT, counters_.next_);
    if (OB_SUCCESS != ret) {
    } else if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    } else if (current_idx_ + 1 >= posting_count_) {
      current_idx_ = posting_count_;
      rowid.set_max();
      ret = OB_ITER_END;
    } else {
      ++current_idx_;
      set_current_row(rowid, score);
    }
    return ret;
  }

  int do_advance_to(const ObDASRowID &target, ObDASRowID &rowid, double &score) override
  {
    int ret = record_call(ScriptedBMMCallType::ADVANCE, counters_.advance_);
    int64_t idx = current_idx_ < 0 ? 0 : current_idx_;
    if (OB_SUCCESS != ret) {
    } else if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (idx < posting_count_ && postings_[idx].doc_id_ < target.get_uint64()) {
        ++idx;
      }
      if (idx >= posting_count_) {
        current_idx_ = posting_count_;
        rowid.set_max();
        ret = OB_ITER_END;
      } else {
        current_idx_ = idx;
        set_current_row(rowid, score);
      }
    }
    return ret;
  }

  int do_advance_to_id(const ObDASRowID &target, ObDASRowID &rowid) override
  {
    int ret = OB_SUCCESS;
    int64_t idx = current_idx_ < 0 ? 0 : current_idx_;
    ++advance_to_id_count_;
    if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (idx < posting_count_ && postings_[idx].doc_id_ < target.get_uint64()) {
        ++idx;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (idx >= posting_count_) {
      current_idx_ = posting_count_;
      ret = OB_ITER_END;
    } else {
      current_idx_ = idx;
      rowid.set_uint64(postings_[idx].doc_id_);
    }
    return ret;
  }

  int do_advance_shallow(
      const ObDASRowID &target,
      const bool inclusive,
      const MaxScoreTuple *&max_score_tuple) override
  {
    int ret = record_call(ScriptedBMMCallType::SHALLOW, counters_.shallow_);
    int64_t idx = 0;
    max_score_tuple = nullptr;
    if (OB_SUCCESS != ret) {
    } else if (!is_valid_posting_array()) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      while (idx < posting_count_
          && (postings_[idx].block_end_id_ < target.get_uint64()
              || (!inclusive && postings_[idx].block_end_id_ == target.get_uint64()))) {
        ++idx;
      }
      if (idx >= posting_count_) {
        ret = OB_ITER_END;
      } else {
        ObDASRowID block_end_id;
        block_end_id.set_uint64(postings_[idx].block_end_id_);
        max_score_tuple_.set(target, block_end_id, postings_[idx].block_max_score_);
        max_score_tuple = &max_score_tuple_;
      }
    }
    return ret;
  }

  int do_load_curr_score(double &score) override
  {
    int ret = OB_SUCCESS;
    ++load_curr_score_count_;
    if (OB_UNLIKELY(!is_valid_posting_array() || current_idx_ < 0
        || current_idx_ >= posting_count_)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      score = postings_[current_idx_].score_;
    }
    return ret;
  }

  int do_calc_max_score(double &max_score) override
  {
    const int ret = record_call(ScriptedBMMCallType::CALC_MAX_SCORE, counters_.calc_max_score_);
    if (OB_SUCCESS == ret) {
      max_score = dim_max_score_;
    }
    return ret;
  }

  int do_calc_range_max_score(
      const ObDASRowID &start_id,
      const bool start_inclusive,
      const ObDASRowID &end_id,
      double &max_score,
      bool &can_calc) override
  {
    int ret = record_call(ScriptedBMMCallType::RANGE_MAX, counters_.range_max_);
    bool matched = false;
    if (OB_SUCCESS == ret) {
      for (const ScriptedBMMRangeMaxResult &result : range_max_results_) {
        if (result.start_id_ == start_id.get_uint64()
            && result.start_inclusive_ == start_inclusive
            && result.end_id_ == end_id.get_uint64()) {
          ret = result.ret_;
          can_calc = OB_SUCCESS == ret && result.can_calc_;
          if (can_calc) {
            max_score = result.max_score_;
          }
          matched = true;
          break;
        }
      }
      if (!matched) {
        ret = range_max_ret_;
        can_calc = OB_SUCCESS == ret && can_calc_range_max_;
        if (can_calc) {
          max_score = range_max_score_;
        }
      }
    }
    return ret;
  }

  int do_drain_until(
      const ObDASRowID &boundary,
      int64_t capacity,
      uint64_t *out_ids,
      double *out_scores,
      int64_t &count,
      ObDASRowID &next_id_after,
      double &next_score_after) override
  {
    int ret = record_call(ScriptedBMMCallType::DRAIN, counters_.drain_);
    if (OB_SUCCESS != ret) {
    } else if (!use_batch_drain_) {
      ret = ObIDASSearchOp::do_drain_until(
          boundary, capacity, out_ids, out_scores, count, next_id_after, next_score_after);
    } else {
      count = 0;
      int64_t idx = current_idx_ + 1;
      while (idx < posting_count_
          && postings_[idx].doc_id_ <= boundary.get_uint64()
          && count < capacity) {
        out_ids[count] = postings_[idx].doc_id_;
        if (nullptr != out_scores) {
          out_scores[count] = postings_[idx].score_;
        }
        current_idx_ = idx;
        ++idx;
        ++count;
      }
      if (count < capacity) {
        if (idx >= posting_count_) {
          current_idx_ = posting_count_;
          next_id_after.set_max();
        } else {
          current_idx_ = idx;
          set_current_row(next_id_after, next_score_after);
        }
      }
    }
    return ret;
  }

  int do_drain_until_active(
      const ObDASRowID &boundary,
      DocScoreAccumulator &accumulator,
      int64_t &count) override
  {
    int ret = record_call(ScriptedBMMCallType::ACTIVE_DRAIN, counters_.active_drain_);
    if (OB_SUCCESS == ret) {
      ret = ObIDASSearchOp::do_drain_until_active(boundary, accumulator, count);
    }
    return ret;
  }

private:
  int record_call(const ScriptedBMMCallType type, int64_t &counter)
  {
    ++counter;
    return type == fail_type_ && counter == fail_on_call_ ? fail_ret_ : OB_SUCCESS;
  }

  bool is_valid_posting_array() const
  {
    return posting_count_ >= 0 && (0 == posting_count_ || nullptr != postings_);
  }

  void set_current_row(ObDASRowID &rowid, double &score) const
  {
    rowid.set_uint64(postings_[current_idx_].doc_id_);
    score = postings_[current_idx_].score_;
  }

private:
  ScriptedBMMPosting single_posting_;
  const ScriptedBMMPosting *postings_;
  int64_t posting_count_;
  double dim_max_score_;
  int64_t advance_to_id_count_;
  int64_t load_curr_score_count_;
  bool enable_id_only_;
  int64_t current_idx_;
  double range_max_score_;
  bool can_calc_range_max_;
  int range_max_ret_;
  std::vector<ScriptedBMMRangeMaxResult> range_max_results_;
  ScriptedBMMCallCounters counters_;
  bool use_batch_drain_;
  const ScriptedBMMPosting *next_segment_postings_;
  int64_t next_segment_posting_count_;
  bool has_next_segment_;
  ScriptedBMMCallType fail_type_;
  int64_t fail_on_call_;
  int fail_ret_;
};

class EmptyScriptedBMMSearchOp : public ScriptedBMMSearchOp
{
public:
  explicit EmptyScriptedBMMSearchOp(ObDASSearchCtx &search_ctx)
    : ScriptedBMMSearchOp(search_ctx, nullptr, 0, 0.0)
  {}
};

class NonEnumerableScriptedBMMSearchOp : public ScriptedBMMSearchOp
{
public:
  NonEnumerableScriptedBMMSearchOp(
      ObDASSearchCtx &search_ctx,
      const ScriptedBMMPostingList &postings,
      const double dim_max_score)
    : ScriptedBMMSearchOp(search_ctx, postings, dim_max_score)
  {}

  bool can_enumerate_rowids() const override { return false; }
};

} // namespace test
} // namespace sql
} // namespace oceanbase

#endif // UNITTEST_SQL_ENGINE_DAS_TEST_DAS_BMM_COMMON_H_
