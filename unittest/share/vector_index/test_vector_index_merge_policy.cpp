/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include "lib/time/ob_time_utility.h"
#include "share/scn.h"
#include "share/vector_index/ob_vector_index_merge_policy.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "observer/omt/ob_tenant_config.h"
#define protected public
#include "share/vector_index/ob_vector_index_aux_table_handler.h"
#undef protected

namespace oceanbase
{
namespace share
{
namespace
{
constexpr int64_t DAY = 24L * 60 * 60 * 1000000;
constexpr int64_t HOUR = 60L * 60 * 1000000;
constexpr int64_t L = INT64_MAX;
using Strategy = ObVecMergePlan::MergeStrategy;
constexpr Strategy TARGET = ObVecMergePlan::TARGET_MERGE;
constexpr Strategy TIERED = ObVecMergePlan::TIERED_MERGE;
constexpr Strategy DELETE = ObVecMergePlan::DELETE_MERGE;
constexpr Strategy IDLE = ObVecMergePlan::RESIDUAL_MERGE;
constexpr Strategy MANUAL = ObVecMergePlan::MANUAL_MERGE;
constexpr Strategy FULL = ObVecMergePlan::FULL_MERGE;
constexpr auto AUTO = OB_VEC_TRIGGER_AUTO;
constexpr auto COMPACT = OB_VEC_TRIGGER_MANUAL;
constexpr auto ALTER = OB_VEC_TRIGGER_ALTER_INDEX_PARAM;

// All sizes are vector counts. Ages are microseconds relative to test setup time.
struct Segment
{
  Segment(int64_t count, int64_t deleted = 0, bool readable = true, int64_t age = 0)
      : count_(count), deleted_(deleted), readable_(readable), age_(age) {}
  int64_t count_;
  int64_t deleted_;
  bool readable_;
  int64_t age_;
};
struct Config
{
  int64_t c_ = 1000;
  int64_t f_ = 2;
  int64_t r_ = 20;
  int64_t t_ = DAY;
};
struct Expected
{
  int ret_;
  Strategy strategy_ = TIERED;
  const char *first_ = "";
  const char *last_ = ""; // Inclusive segment label, e.g. B1..I2.
  bool base_ = false;
};
Expected Merge(Strategy strategy, const char *first, const char *last, bool base)
{ return {OB_SUCCESS, strategy, first, last, base}; }
Expected NoMerge() { return {OB_NO_NEED_MERGE}; }
Expected Pending() { return {OB_EAGAIN}; }

struct Scenario
{
  const char *name_;
  Config config_;
  std::vector<Segment> bases_;
  std::vector<Segment> incrs_;
  Expected expected_;
  ObVecIndexAsyncTaskTriggerType trigger_ = AUTO;
  int64_t global_deleted_ = 0; // Explicit union count; never sum segment deletions.
  bool scheduling_ = false;
};
const char *strategy_name(Strategy strategy)
{
  switch (strategy) {
    case TARGET: return "TARGET";
    case TIERED: return "TIERED";
    case DELETE: return "DELETE";
    case IDLE: return "IDLE";
    case MANUAL: return "MANUAL";
    case FULL: return "FULL";
    case ObVecMergePlan::FREEZE_REBUILD: return "FREEZE_REBUILD";
    default: return "UNKNOWN";
  }
}
void PrintTo(const Scenario &s, std::ostream *out)
{
  *out << s.name_ << " C=" << s.config_.c_ << " F=" << s.config_.f_
       << " R=" << s.config_.r_ << "% T=" << s.config_.t_ << "us"
       << " trigger=" << (s.trigger_ == AUTO ? "AUTO" : s.trigger_ == COMPACT ? "MANUAL" : "ALTER")
       << " global_deleted=" << s.global_deleted_;
  const auto print = [&](const char *role, const std::vector<Segment> &segments) {
    *out << " " << role << "[";
    for (size_t i = 0; i < segments.size(); ++i) {
      const auto &seg = segments[i];
      const int64_t deleted = seg.deleted_;
      *out << (i == 0 ? "" : ", ") << role << i << ":" << seg.count_ << "-" << deleted
           << "=" << MAX(seg.count_ - deleted, 0) << " age=" << seg.age_ << "us"
           << (seg.readable_ ? "" : " UNREADABLE");
    }
    *out << "]";
  };
  print("B", s.bases_); print("I", s.incrs_);
  *out << " expected_ret=" << s.expected_.ret_;
  *out << (s.scheduling_ ? " SCHEDULING" : " EXECUTION");
  if (s.expected_.ret_ == OB_SUCCESS) {
    *out << " " << strategy_name(s.expected_.strategy_) << " " << s.expected_.first_
         << ".." << s.expected_.last_ << " -> " << (s.expected_.base_ ? "Base" : "Incr");
  }
}
std::string label(int64_t position, int64_t bases)
{
  return position < bases ? "B" + std::to_string(position) : "I" + std::to_string(position - bases);
}

// Literal expectations for vector-segment-merge-policy.md. Existing scenarios
// retain their inputs; TARGET/U changes update only the specified outcomes.
// {} config means M=1000/F=2/R=20/T=24h, with U=floor(1.2M).
std::vector<Scenario> design_scenarios()
{
  return {
    // 2.3.2: tier bands, capacity, readiness barriers; 2.4: output roles.
    {"DeletedBasesFit", {}, {{600,150},{600,150}}, {}, Merge(TIERED,"B0","B1",true)},
    {"InteriorSmallSegment", {}, {}, {800,600,300,600,100,100}, Merge(TIERED,"I1","I2",false)},
    {"TargetFillsExpandedCapacity", {1000,3}, {}, {400,400,400}, Merge(TARGET,"I0","I2",true)},
    {"FewerThanFactor", {1000,3}, {}, {400,400}, NoMerge()},
    {"BarrierBlocksManual", {}, {}, {200,{100,0,false},200}, Pending(), COMPACT},
    {"WindowAfterBarrier", {}, {}, {200,{100,0,false},200,200}, Merge(TIERED,"I2","I3",false)},
    {"FullTailBasePromotes", {}, {1000}, {100,100}, Merge(TIERED,"I0","I1",true)},
    {"TargetIncludesDeletedTailBase", {}, {{1000,1}}, {100,100}, Merge(TARGET,"B0","I1",true)},
    {"UnreadableTailBaseDoesNotPromote", {}, {{1000,0,false}}, {100,100}, Merge(TIERED,"I0","I1",false)},
    // 2.3.2: extend to capacity or the tier boundary, with no F/10000 cap.
    {"ExtendPastFormerMinimum", {100000,3}, {}, {2000,2000,2000,2000,2000,2000}, Merge(TIERED,"I0","I5",true)},
    {"ExtensionCapacityStopsNext", {9000,3}, {}, {2000,2000,2000,2000,2000,2000}, Merge(TARGET,"I0","I4",true)},
    {"TruncatedWindowDoesNotExtend", {5000,3}, {}, {2000,2000,2000,2000,2000,2000}, Merge(TARGET,"I0","I2",true)},
    {"CannotRestoreTrimmedTail", {100000,2}, {}, {2000,2000,100}, Merge(TIERED,"I0","I1",true)},
    {"CannotExtendAcrossBarrier", {100000,2}, {}, {2000,2000,{2000,0,false},2000}, Merge(TIERED,"I0","I1",true)},
    {"ExtensionUsesLiveCounts", {100000,3}, {}, {{4000,2000},{4000,2000},{4000,2000},{4000,2000}}, Merge(TIERED,"I0","I3",true)},
    {"SmallWindowStillRequiresFactor", {100000,3}, {}, {2000,2000}, NoMerge()},
    {"AllEmptySegments", {100000,3}, {}, {0,0,0,0,0}, Merge(TIERED,"I0","I4",true)},
    // Key configuration and boundary additions (not a Cartesian product).
    {"MinimumCapacity", {1,2}, {}, {1,1}, NoMerge()},
    {"ExtensionExactlyTarget", {9000,2}, {}, {3000,3000,3000,3000}, Merge(TARGET,"I0","I2",true)},
    {"ContinueAfterFormerMinimum", {100000,2}, {}, {5000,5000,5000}, Merge(TIERED,"I0","I2",true)},
    {"ContinueAboveFormerMinimum", {100000,2}, {}, {6000,6000,6000}, Merge(TIERED,"I0","I2",true)},
    {"MaximumFactorMissingOne", {50000000,20}, {}, std::vector<Segment>(19,100), NoMerge()},
    {"MaximumFactorCanExtend", {50000000,20}, {}, std::vector<Segment>(21,100), Merge(TIERED,"I0","I20",true)},
    {"ExtensionKeepsEveryInteriorSegment", {100000,2}, {}, {3000,3000,5000,1000,3000}, Merge(TIERED,"I0","I4",true)},
    {"FindWindowAfterOversizedSegment", {}, {}, {100,1200,200,200}, Merge(TIERED,"I2","I3",false)},
    // F=16: 16^0.75=8. Sixteen positions are necessary to test the band boundary.
    {"BelowBandBoundary", {100000,16}, {8000}, std::vector<Segment>(15,999), NoMerge()},
    {"ExactlyBandBoundary", {100000,16}, {8000}, std::vector<Segment>(15,1000), Merge(TIERED,"B0","I14",true)},
    {"AboveBandBoundary", {100000,16}, {8000}, std::vector<Segment>(15,1001), Merge(TIERED,"B0","I14",true)},
    // 2.3.1: deletion rebuild retains the input role and ignores C/F/T.
    {"DeleteFirstBase", {}, {{1000,700},1000,1000}, {}, Merge(DELETE,"B0","B0",true)},
    {"DeleteFirstIncr", {}, {}, {{1000,700},1000,1000}, Merge(DELETE,"I0","I0",false)},
    {"LargeDeletedGraph", {}, {{L,L/2}}, {}, Merge(DELETE,"B0","B0",true)},
    // 2.3.3: test expiry and latest-segment selection with ample wall-clock slack.
    {"IdleNotYetDue", {}, {}, {100,10}, NoMerge()},
    {"IdleDue", {}, {}, {{100,0,true,DAY+HOUR},{10,0,true,DAY+HOUR}}, Merge(IDLE,"I0","I1",true)},
    {"LatestSegmentDelaysIdle", {}, {}, {{100,0,true,DAY+HOUR},{10,0,true,DAY-HOUR}}, NoMerge()},
    {"IdleExtendsToRangeEnd", {}, {}, {{700,0,true,DAY+HOUR},{70,0,true,DAY+HOUR},{7,0,true,DAY+HOUR}}, Merge(IDLE,"I0","I2",true)},
    {"TargetPrecedesIdleDespiteFactor", {1000,4}, {}, {{600,0,true,DAY+HOUR},{600,0,true,DAY+HOUR},{300,0,true,DAY+HOUR}}, Merge(TARGET,"I0","I1",true)},
    {"IdleBarrierBlocksManual", {1000,4}, {}, {{600,0,true,DAY+HOUR},{600,0,false,DAY+HOUR},{300,0,true,DAY+HOUR}}, Pending(), COMPACT},
    {"IdleWindowAfterBarrier", {1000,4}, {}, {{600,0,true,DAY+HOUR},{600,0,false,DAY+HOUR},{300,0,true,DAY+HOUR},{30,0,true,DAY+HOUR}}, Merge(IDLE,"I2","I3",false)},
    {"IdleWithFewerThanFactor", {100000,3}, {}, {{2000,0,true,DAY+HOUR},{2000,0,true,DAY+HOUR}}, Merge(IDLE,"I0","I1",true)},
    // 2.3.4 and 2.4: manual strategy priority, fallback scope, ALTER all sources.
    {"ManualKeepsHistory", {}, {2000,2000}, {2000}, Merge(MANUAL,"B1","I0",true), COMPACT},
    {"ManualKeepsHistoryWithMultipleIncrs", {}, {2000,2000}, {2000,2000}, Merge(MANUAL,"B1","I1",true), COMPACT},
    {"ManualSingleBase", {}, {1000}, {}, NoMerge(), COMPACT},
    {"ManualFullBasesWithoutIncrs", {}, {1000,1000}, {}, NoMerge(), COMPACT},
    {"ManualSingleBaseBelowDeleteThreshold", {}, {{1000,199}}, {}, NoMerge(), COMPACT},
    {"ManualSingleBaseAtDeleteThreshold", {}, {{1000,200}}, {}, Merge(DELETE,"B0","B0",true), COMPACT},
    {"ManualSingleIncrAtDeleteThreshold", {}, {}, {{1000,200}}, Merge(DELETE,"I0","I0",false), COMPACT},
    {"ManualPrefersTiered", {}, {}, {{400,100,true,DAY+HOUR},{400,100,true,DAY+HOUR}}, Merge(TIERED,"I0","I1",true), COMPACT},
    {"ManualPrefersDelete", {}, {}, {{400,100,true,DAY+HOUR},{110,100,true,DAY+HOUR}}, Merge(DELETE,"I0","I0",false), COMPACT},
    {"ManualPrefersIdle", {}, {}, {{400,0,true,DAY+HOUR},{110,0,true,DAY+HOUR}}, Merge(IDLE,"I0","I1",true), COMPACT},
    {"ManualFallback", {}, {}, {400,110}, Merge(MANUAL,"I0","I1",true), COMPACT},
    {"ZeroCachedDeletesDoNotRebuildSingleIncr", {}, {}, {100}, NoMerge(), COMPACT},
    {"TieredWithZeroCachedDeletes", {}, {}, {100,100}, Merge(TIERED,"I0","I1",true), COMPACT},
    {"EmptyAuto", {}, {}, {}, NoMerge()},
    {"EmptyManual", {}, {}, {}, NoMerge(), COMPACT},
    {"EmptyAlter", {}, {}, {}, NoMerge(), ALTER},
    {"AlterIgnoresCapacityAndStatistics", {}, {2000,3000}, {10}, Merge(FULL,"B0","I0",true), ALTER},
    {"AlterWaitsForAllSources", {}, {2000,{3000,0,false}}, {10}, Pending(), ALTER},
    {"GlobalDeletesAreNotLocalDeletes", {}, {1000}, {}, NoMerge(), AUTO, 201},
  };
}

// 2.3.1: ceil(N*R/100), checked for both execution and scheduling.
std::vector<Scenario> threshold_scenarios()
{
  struct Row { const char *name; int64_t n; int64_t d; int64_t r; bool hit; };
  const Row rows[] = {
    {"Below20",100,19,20,false}, {"Equal20",100,20,20,true}, {"Above20",100,21,20,true},
    {"Below100",100,99,100,false}, {"Equal100",100,100,100,true},
    {"RoundingBelow20",101,20,20,false}, {"RoundingAbove20",101,21,20,true},
    {"HugeBelow50",L,L/2,50,false}, {"HugeAbove50",L,L/2+1,50,true}, {"HugeEqual100",L,L,100,true},
    {"MinimumRatioBelow",100,0,1,false}, {"MinimumRatioEqual",100,1,1,true},
    {"MinimumRatioRoundingBelow",101,1,1,false}, {"MinimumRatioRoundingAbove",101,2,1,true},
    {"EmptyTotal",0,1,20,false},
  };
  std::vector<Scenario> cases;
  for (const auto &row : rows) {
    Scenario s{row.name, {1000,2,row.r}, {{row.n,row.d}}, {},
        row.hit ? Merge(DELETE,"B0","B0",true) : NoMerge(), AUTO, row.d};
    cases.push_back(s);
    s.scheduling_ = true;
    s.expected_ = row.hit ? Merge(DELETE,"B0","B0",true) : NoMerge();
    cases.push_back(s);
  }
  return cases;
}
std::vector<Scenario> scheduling_scenarios()
{
  return {
    {"LocalDeleteBelowGlobalThreshold", {}, {{1000,300},1000}, {}, Merge(DELETE,"B0","B0",true), AUTO, 0, true},
    {"ExactGlobalDeleteDoesNotForceMerge", {}, {600}, {900}, NoMerge(), AUTO, 300},
    {"GlobalDeletesDoNotScheduleLargestIncr", {}, {600}, {900}, NoMerge(), AUTO, 300, true},
    {"SchedulingLiveCounts", {}, {{600,150}}, {{600,150}}, Merge(TIERED,"B0","I0",true), AUTO, 300, true},
    {"ExactLiveCounts", {}, {{600,150}}, {{600,150}}, Merge(TIERED,"B0","I0",true), AUTO, 300},
    {"SchedulingDeleteBeforeIdle", {}, {}, {{100,30,true,DAY+HOUR},{10,0,true,DAY+HOUR}}, Merge(DELETE,"I0","I0",false), AUTO, 30, true},
    {"ExactDeleteBeforeIdle", {}, {}, {{100,30,true,DAY+HOUR},{10,0,true,DAY+HOUR}}, Merge(DELETE,"I0","I0",false), AUTO, 30},
    {"TotalAtHalf", {1000,2,50}, {L,L}, {}, NoMerge(), AUTO, L, true},
    {"TotalAbove49", {1000,2,49}, {L,L}, {}, NoMerge(), AUTO, L, true},
    {"TotalUint64Max", {1000,2,50}, {L,L,1}, {}, NoMerge(), AUTO, L, true},
    {"TotalOverflow", {1000,2,50}, {L,L,2}, {}, {OB_SIZE_OVERFLOW}, AUTO, L, true},
    {"ZeroCachedDeletesDoNotSchedule", {}, {}, {100}, NoMerge(), AUTO, 0, true},
    {"TieredWithZeroCachedDeletes", {}, {}, {100,100}, Merge(TIERED,"I0","I1",true), AUTO, 0, true},
    {"CapacityWindowTrigger", {100000,3}, {}, {2000,2000,2000,2000,2000,2000}, Merge(TIERED,"I0","I5",true), AUTO, 0, true},
    {"LatestSegmentDelaysIdle", {}, {}, {{100,0,true,DAY+HOUR},{10,0,true,DAY-HOUR}}, NoMerge(), AUTO, 0, true},
    {"IdleDue", {}, {}, {{100,0,true,DAY+HOUR},{10,0,true,DAY+HOUR}}, Merge(IDLE,"I0","I1",true), AUTO, 0, true},
    {"IdleCannotExceedCapacity", {}, {}, {{1001,0,true,DAY+HOUR},{1001,0,true,DAY+HOUR}}, NoMerge(), AUTO, 0, true},
    {"IdleCannotPromoteSingleton", {}, {}, {{100,0,true,DAY+HOUR}}, NoMerge(), AUTO, 0, true},
  };
}
std::string scenario_name(const ::testing::TestParamInfo<Scenario> &info)
{
  return std::string(info.param.name_) + (info.param.scheduling_ ? "Scheduling" : "Execution");
}

// Literal expectations from the overview's common examples, in actual vector
// counts (C=1m). These exercise production selection, not a policy simulation.
std::vector<Scenario> capacity_window_scenarios()
{
  const Config config{1000000, 2};
  std::vector<Scenario> cases = {
    {"SixSegmentsFillCapacity", config, {}, std::vector<Segment>(6,200000), Merge(TARGET,"I0","I5",true)},
    {"EightSegmentsBelowCapacity", config, {}, std::vector<Segment>(8,50000), Merge(TIERED,"I0","I7",true)},
    {"TrimSmallTail", config, {}, {800000,10000}, NoMerge()},
    {"TargetIncludesTrimmedTail", config, {}, {800000,100000,100000}, Merge(TARGET,"I0","I2",true)},
    {"KeepSmallInterior", config, {}, {800000,10000,800000}, Merge(TIERED,"I0","I1",true)},
    {"AdjacentSegmentsExactlyCapacity", config, {}, {600000,600000}, Merge(TARGET,"I0","I1",true)},
    {"BaseAndTwoIncrs", config, {400000}, {300000,300000}, Merge(TARGET,"B0","I1",true)},
    {"TwoBasesAndIncrPrefix", config, {300000,300000}, {300000,300000}, Merge(TARGET,"B0","I1",true)},
    {"PromoteAfterFullBase", config, {1000000}, {200000,200000,200000}, Merge(TIERED,"I0","I2",true)},
    {"TargetIncludesPartialBase", config, {900000}, {200000,200000,200000}, Merge(TARGET,"B0","I0",true)},
    {"WindowAfterUnreadable", config, {}, {200000,{200000,0,false},200000,200000}, Merge(TIERED,"I2","I3",false)},
    {"FewerCandidatesThanFactor", {1000000,3}, {}, {400000,400000}, NoMerge()},
    {"TargetAtFactor", {1000000,3}, {}, {400000,400000,400000}, Merge(TARGET,"I0","I2",true)},
    {"DeletedBasesFit", config, {{600000,150000},{600000,150000}}, {}, Merge(TIERED,"B0","B1",true)},
    {"SingleBaseDeleteRebuild", config, {{1000000,300000}}, {}, Merge(DELETE,"B0","B0",true)},
    {"IdleExpandsPastFactor", config, {}, {{800000,0,true,DAY+HOUR},{100000,0,true,DAY+HOUR},
        {10000,0,true,DAY+HOUR}}, Merge(IDLE,"I0","I2",true)},
    // Additional boundaries: do not skip a full next segment, search globally
    // for a better fill, cross a barrier, or overflow the remaining capacity.
    {"CannotSkipNextToFillCapacity", config, {}, {400000,400000,300000,200000,400000}, Merge(TARGET,"I0","I2",true)},
    {"FirstWindowBeforeBetterFill", config, {}, {300000,300000,500000,500000}, Merge(TARGET,"I0","I2",true)},
    {"ExpansionStopsAtUnreadable", config, {}, {200000,200000,200000,{200000,0,false},200000}, Merge(TIERED,"I0","I2",true)},
    {"BeyondInlineArraySize", config, {}, std::vector<Segment>(21,40000), Merge(TIERED,"I0","I20",true)},
    {"CapacityAtMaximumConfig", {50000000,2}, {}, {20000000,20000000,20000000,20000000}, Merge(TARGET,"I0","I2",true)},
    {"TargetPrecedesTieredTailAndIdle", config, {}, {{800000,0,true,DAY+HOUR},{100000,0,true,DAY+HOUR},
        {10000,0,true,DAY+HOUR},{100000,0,true,DAY+HOUR}}, Merge(TARGET,"I0","I3",true)},
    // Each descending segment is below the previous tier's tail boundary.
    // TARGET can now span those tiers and include the last 20000 within U.
    {"TargetAcrossDescendingTiers", config, {}, {{600000,0,true,DAY+HOUR},{300000,0,true,DAY+HOUR},
        {90000,0,true,DAY+HOUR},{20000,0,true,DAY+HOUR}}, Merge(TARGET,"I0","I3",true)},
    {"IdleExpansionStopsAtUnreadable", {1000000,5}, {}, {{200000,0,true,DAY+HOUR},
        {200000,0,true,DAY+HOUR},{200000,0,true,DAY+HOUR},{200000,0,false,DAY+HOUR},
        {200000,0,true,DAY+HOUR}}, Merge(IDLE,"I0","I2",true)},
  };
  // Scheduling and execution share the same expectations, including deletions.
  const size_t exact_count = cases.size();
  for (size_t i = 0; i < exact_count; ++i) {
    if (cases[i].trigger_ == AUTO) {
      Scenario scheduling = cases[i];
      scheduling.scheduling_ = true;
      cases.push_back(scheduling);
    }
  }
  return cases;
}
std::vector<Scenario> unified_scenarios()
{
  std::vector<Segment> long_window_before_barrier(40,25);
  long_window_before_barrier.push_back(Segment(25,0,false));
  long_window_before_barrier.push_back(200);
  std::vector<Segment> zero_prefix(64,0);
  zero_prefix.push_back(800);
  zero_prefix.push_back(200);
  std::vector<Scenario> cases = {
    {"TargetCrossesTiers", {}, {}, {800,200}, Merge(TARGET,"I0","I1",true)},
    {"TargetCrossesTiersAtCapacity", {}, {}, {800,400}, Merge(TARGET,"I0","I1",true)},
    {"TargetExtendsToCapacity", {}, {}, {800,200,200}, Merge(TARGET,"I0","I2",true)},
    {"TargetStopsAtCapacity", {}, {}, {800,200,201}, Merge(TARGET,"I0","I1",true)},
    {"TargetKeepsTrailingZero", {}, {}, {800,200,0}, Merge(TARGET,"I0","I2",true)},
    {"TargetWinsSameStartAndIncludesTrimmedTail", {}, {}, {600,400,100}, Merge(TARGET,"I0","I2",true)},
    {"EarlierTieredProtectsPrefix", {}, {}, {600,300,800,200}, Merge(TIERED,"I0","I1",true)},
    {"AcceptedEarlierTieredLayout", {}, {}, {1000,200,300,400,500}, Merge(TIERED,"I1","I3",false)},
    {"EarlierTargetBeforeCheaperLaterWindow", {}, {}, {800,200,{1,0,false},400,300,300}, Merge(TARGET,"I0","I1",true)},
    {"EarlierTargetBeforeLargerLaterWindow", {}, {}, {800,200,800,200}, Merge(TARGET,"I0","I1",true)},
    {"TargetIndependentOfTieredFactor", {1000,16}, {}, {800,200}, Merge(TARGET,"I0","I1",true)},
    {"TargetExtendsPast32Inputs", {}, {}, std::vector<Segment>(33,32), Merge(TARGET,"I0","I32",true)},
    {"TargetReachesMWith33Inputs", {}, {}, std::vector<Segment>(33,31), Merge(TARGET,"I0","I32",true)},
    {"TargetUses128Inputs", {}, {}, std::vector<Segment>(128,8), Merge(TARGET,"I0","I127",true)},
    {"TargetLongWindowStopsAtCapacity", {}, {}, std::vector<Segment>(60,25), Merge(TARGET,"I0","I47",true)},
    {"TargetLongWindowStopsAtBarrier", {}, {}, long_window_before_barrier, Merge(TARGET,"I0","I39",true)},
    {"TargetIncludesLongZeroPrefix", {}, {}, zero_prefix, Merge(TARGET,"I0","I65",true)},
    {"ManyTinySegmentsBelowTarget", {1000000,2}, {}, std::vector<Segment>(4096,1), Merge(TIERED,"I0","I4095",true)},
    {"TieredCapacityTruncatesBelowFactor", {1000,4}, {}, {450,450,450,450}, Merge(TIERED,"I0","I1",true)},
    {"TieredHasNo32InputLimit", {}, {}, std::vector<Segment>(33,1), Merge(TIERED,"I0","I32",true)},
    {"TargetRejectsGrowthBelowCeil", {}, {}, {834,166}, NoMerge()},
    {"TargetAcceptsGrowthAtCeil", {}, {}, {834,167}, Merge(TARGET,"I0","I1",true)},
    {"TargetMaxConfigGrowthBelowCeil", {50000000,2}, {}, {49999999,9999999}, NoMerge()},
    {"TargetMaxConfigGrowthAtCeil", {50000000,2}, {}, {49999999,10000000}, Merge(TARGET,"I0","I1",true)},
    {"TargetRejectsTinyTopup", {}, {}, {990,10}, NoMerge()},
    {"TargetExcludesAlreadyAtM", {}, {}, {1000,200}, NoMerge()},
    {"TieredCanAbsorbIntoFullSegment", {}, {}, {1000,200,1200}, Merge(TIERED,"I0","I1",true)},
    {"AlreadyFullSegmentAbsorbsAfterIdle", {}, {}, {{1000,0,true,DAY+HOUR},{200,0,true,DAY+HOUR}}, Merge(IDLE,"I0","I1",true)},
    {"OneVectorAboveCapacity", {}, {}, {600,601}, NoMerge()},
    {"CapacityBlocksIdleToo", {}, {}, {{1000,0,true,DAY+HOUR},{900,0,true,DAY+HOUR},{500,0,true,DAY+HOUR}}, NoMerge()},
    {"TargetDoesNotCrossUnreadable", {}, {}, {800,{200,0,false}}, Pending()},
    {"TargetAfterUnreadable", {}, {}, {800,{200,0,false},800,200}, Merge(TARGET,"I2","I3",false)},
    {"TargetStopsBeforeUnreadable", {}, {}, {800,200,{100,0,false},200}, Merge(TARGET,"I0","I1",true)},
    {"EarlierTieredBeforeUnreadableAndTarget", {}, {}, {100,100,{100,0,false},800,200}, Merge(TIERED,"I0","I1",true)},
    {"TargetUsesLiveCountsBeforeDeleteCleanup", {}, {}, {{1000,200},200}, Merge(TARGET,"I0","I1",true)},
    {"TargetPromotesAfterFullBase", {}, {1000}, {800,200}, Merge(TARGET,"I0","I1",true)},
    {"TargetAfterUnreadableBaseKeepsIncr", {}, {{1000,0,false}}, {800,200}, Merge(TARGET,"I0","I1",false)},
    {"TargetWithZeroCachedDeletes", {}, {}, {800,200}, Merge(TARGET,"I0","I1",true), COMPACT},
    {"ManualUsesUnifiedOrder", {}, {}, {600,300,800,200}, Merge(TIERED,"I0","I1",true), COMPACT},
    {"AlterBypassesUnifiedOrder", {}, {}, {600,300,800,200}, Merge(FULL,"I0","I3",true), ALTER},
    {"TargetCapacityRoundsDown", {1001,2}, {}, {800,401}, Merge(TARGET,"I0","I1",true)},
    {"TargetCapacityOneAboveRounded", {1001,2}, {}, {800,402}, NoMerge()},
    {"TargetAtMaximumConfig", {50000000,2}, {}, {25000000,25000000}, Merge(TARGET,"I0","I1",true)},
  };
  const size_t count = cases.size();
  for (size_t i = 0; i < count; ++i) {
    if (cases[i].trigger_ == AUTO) {
      Scenario scheduling = cases[i];
      scheduling.scheduling_ = true;
      cases.push_back(scheduling);
    }
  }
  return cases;
}
} // namespace

class TestVectorIndexMergePolicy : public ::testing::TestWithParam<Scenario> {};
TEST_P(TestVectorIndexMergePolicy, MatchesDesign)
{
  const auto &s = GetParam();
  std::ostringstream description;
  PrintTo(s, &description);
  SCOPED_TRACE(description.str());
  ObVecMergeConfig config;
  config.max_vectors_ = s.config_.c_;
  config.merge_factor_ = s.config_.f_;
  config.delete_percentage_ = s.config_.r_;
  config.idle_us_ = s.config_.t_;
  ObVecMergePolicyInput input;
  input.base_count_ = s.bases_.size();
  input.deleted_vid_count_ = s.global_deleted_;
  const int64_t now = ObTimeUtility::current_time();
  for (const auto *segments : {&s.bases_, &s.incrs_}) {
    for (const auto &segment : *segments) {
      ObVecSegmentMergeInfo info;
      info.vector_count_ = segment.count_;
      // Scheduling and execution both consume per-segment counts.
      info.deleted_vector_count_ = segment.deleted_;
      info.readable_ = segment.readable_;
      SCN scn;
      ASSERT_EQ(OB_SUCCESS, scn.convert_for_tx((now - segment.age_) * 1000));
      info.scn_ = scn.get_val_for_inner_table_field();
      ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
    }
  }
  ObVectorIndexMergePolicy policy(config);
  ObVecMergePlan plan;
  // Scheduling wraps the same selection and converts the no-merge return code.
  plan.begin_ = plan.end_ = 123;
  int ret = OB_SUCCESS;
  if (s.scheduling_) {
    bool need_task = true;
    ret = policy.check_need_merge_task(input, need_task, plan);
    EXPECT_EQ(s.expected_.ret_ == OB_SUCCESS, need_task);
    if (ret == OB_SUCCESS && !need_task) { ret = OB_NO_NEED_MERGE; }
  } else {
    ret = policy.find_merge_segments(input, s.trigger_, plan);
  }
  ASSERT_EQ(s.expected_.ret_, ret);
  if (s.expected_.ret_ == OB_SUCCESS) {
    SCOPED_TRACE(std::string("actual: ") + strategy_name(plan.strategy_) + " "
        + label(plan.begin_, input.base_count_) + ".." + label(plan.end_ - 1, input.base_count_));
    ASSERT_GE(plan.begin_, 0);
    ASSERT_GT(plan.end_, plan.begin_);
    ASSERT_LE(plan.end_, input.segments_.count());
    EXPECT_EQ(s.expected_.strategy_, plan.strategy_);
    EXPECT_EQ(std::string(s.expected_.first_), label(plan.begin_, input.base_count_));
    EXPECT_EQ(std::string(s.expected_.last_), label(plan.end_ - 1, input.base_count_));
    EXPECT_EQ(s.expected_.base_, plan.output_base_);
    int64_t expected_count = 0;
    for (int64_t i = plan.begin_; i < plan.end_; ++i) {
      ASSERT_TRUE(input.segments_.at(i).readable_);
      const int64_t live = input.segments_.at(i).live_vector_count();
      ASSERT_LE(live, INT64_MAX - expected_count);
      expected_count += live;
    }
    int64_t actual_count = -1;
    ASSERT_EQ(OB_SUCCESS, plan.get_vector_count(input, actual_count));
    EXPECT_EQ(expected_count, actual_count);
    if (plan.strategy_ == TARGET || plan.strategy_ == TIERED || plan.strategy_ == IDLE) {
      EXPECT_GE(plan.end_ - plan.begin_, 2);
      EXPECT_LE(actual_count, config.merge_capacity());
      if (plan.strategy_ == TARGET) {
        EXPECT_GE(actual_count, config.max_vectors_);
        int64_t largest = 0;
        for (int64_t i = plan.begin_; i < plan.end_; ++i) {
          const int64_t size = input.segments_.at(i).live_vector_count();
          EXPECT_LT(size, config.max_vectors_);
          largest = MAX(largest, size);
        }
        EXPECT_GE(actual_count - largest, largest / 5 + (largest % 5 != 0));
      }
    }
  } else {
    EXPECT_EQ(-1, plan.begin_);
    EXPECT_EQ(-1, plan.end_);
  }
}
INSTANTIATE_TEST_CASE_P(Design, TestVectorIndexMergePolicy, ::testing::ValuesIn(design_scenarios()), scenario_name);
INSTANTIATE_TEST_CASE_P(Threshold, TestVectorIndexMergePolicy, ::testing::ValuesIn(threshold_scenarios()), scenario_name);
INSTANTIATE_TEST_CASE_P(Scheduling, TestVectorIndexMergePolicy, ::testing::ValuesIn(scheduling_scenarios()), scenario_name);
INSTANTIATE_TEST_CASE_P(CapacityWindow, TestVectorIndexMergePolicy,
    ::testing::ValuesIn(capacity_window_scenarios()), scenario_name);
INSTANTIATE_TEST_CASE_P(Unified, TestVectorIndexMergePolicy,
    ::testing::ValuesIn(unified_scenarios()), scenario_name);

TEST(TestVectorIndexMergeConfig, CapacityRoundingAndConfigLimit)
{
  ObVecMergeConfig config;
  // Exercise every remainder around the percentage conversion boundary.
  for (int64_t target = 1; target <= 1024; ++target) {
    config.max_vectors_ = target;
    EXPECT_EQ(target + target / 5, config.merge_capacity()) << target;
  }
  for (const auto &row : std::vector<std::pair<int64_t, int64_t>>{
      {0,0}, {-1,0}, {1,1}, {4,4}, {5,6}, {1001,1201},
      {49999999,59999998}, {50000000,60000000}}) {
    config.max_vectors_ = row.first;
    EXPECT_EQ(row.second, config.merge_capacity()) << row.first;
  }
}

TEST(TestVectorIndexMergeConfig, TenantParameterDefaultAndRange)
{
  std::unique_ptr<omt::ObTenantConfig> tenant_config(new omt::ObTenantConfig());
  auto &limit = tenant_config->vector_index_segment_max_vectors;
  EXPECT_EQ(20000000, limit.get_value());
  EXPECT_EQ(limit.get_value(), ObVecMergeConfig().max_vectors_);
  EXPECT_EQ(24000000, ObVecMergeConfig().merge_capacity());
  for (const char *value : {"100000", "20000000", "50000000"}) {
    ASSERT_TRUE(limit.set_value(value));
    EXPECT_TRUE(limit.check()) << value;
  }
  for (const char *value : {"-1", "0", "1", "99999", "50000001"}) {
    ASSERT_TRUE(limit.set_value(value));
    EXPECT_FALSE(limit.check()) << value;
  }
}

TEST(TestVectorIndexMergePolicy, OrdinaryAndIdleConvergence)
{
  struct Step { int64_t begin; int64_t end; Strategy strategy; bool aged; };
  struct Case {
    std::vector<int64_t> input;
    std::vector<Step> steps;
    std::vector<int64_t> final;
  };
  // Literal complete paths: the accepted three-segment outcome, prefix protection,
  // and the two ordinary-stage residuals that become mergeable after idle.
  const std::vector<Case> cases = {
    {{1000,200,300,400,500}, {{1,4,TIERED,false}}, {1000,900,500}},
    {{600,300,800,200}, {{0,2,TIERED,false},{1,3,TARGET,false}}, {900,1000}},
    {{400,50,200,600,200}, {{0,3,TIERED,false},{1,3,IDLE,true}}, {650,800}},
    {{850,150,50,850,100}, {{0,3,TARGET,false},{1,3,IDLE,true}}, {1050,950}},
  };
  for (const auto &c : cases) {
    ObVecMergeConfig config;
    config.max_vectors_ = 1000;
    ObVectorIndexMergePolicy policy(config);
    ObVecMergePolicyInput input;
    for (const int64_t size : c.input) {
      ObVecSegmentMergeInfo info;
      info.vector_count_ = size;
      info.readable_ = true;
      ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
    }
    const auto set_age = [&](bool aged) {
      SCN scn;
      EXPECT_EQ(OB_SUCCESS, scn.convert_for_tx(
          (ObTimeUtility::current_time() - (aged ? DAY+HOUR : 0)) * 1000));
      for (auto &info : input.segments_) { info.scn_ = scn.get_val_for_inner_table_field(); }
    };
    for (const auto &step : c.steps) {
      set_age(false);
      ObVecMergePlan plan;
      if (step.aged) {
        ASSERT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, AUTO, plan));
        // An old small segment alone does not expire the global idle timer.
        set_age(true);
        SCN fresh;
        ASSERT_EQ(OB_SUCCESS, fresh.convert_for_tx(ObTimeUtility::current_time() * 1000));
        input.segments_.at(input.segments_.count()-1).scn_ = fresh.get_val_for_inner_table_field();
        ASSERT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, AUTO, plan));
        set_age(true);
      }
      ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
      ASSERT_EQ(step.begin, plan.begin_);
      ASSERT_EQ(step.end, plan.end_);
      ASSERT_EQ(step.strategy, plan.strategy_);
      int64_t total = 0;
      ASSERT_EQ(OB_SUCCESS, plan.get_vector_count(input, total));
      ObSEArray<ObVecSegmentMergeInfo, 8> next;
      for (int64_t i = 0; i < input.segments_.count(); ++i) {
        if (i == plan.begin_) {
          ObVecSegmentMergeInfo output;
          output.vector_count_ = total;
          output.readable_ = true;
          ASSERT_EQ(OB_SUCCESS, next.push_back(output));
        } else if (i < plan.begin_ || i >= plan.end_) {
          ASSERT_EQ(OB_SUCCESS, next.push_back(input.segments_.at(i)));
        }
      }
      if (plan.begin_ < input.base_count_) {
        input.base_count_ -= MIN(plan.end_, input.base_count_) - plan.begin_ - 1;
      } else if (plan.output_base_) {
        ++input.base_count_;
      }
      ASSERT_EQ(OB_SUCCESS, input.segments_.assign(next));
    }
    ASSERT_EQ(c.final.size(), input.segments_.count());
    int64_t before = 0;
    int64_t after = 0;
    for (const int64_t size : c.input) { before += size; }
    for (size_t i = 0; i < c.final.size(); ++i) {
      EXPECT_EQ(c.final[i], input.segments_.at(i).vector_count_);
      after += input.segments_.at(i).vector_count_;
    }
    EXPECT_EQ(before, after);
    set_age(true);
    ObVecMergePlan plan;
    EXPECT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, AUTO, plan));
  }
}

namespace
{
struct PlanCountScenario
{
  const char *name_;
  std::vector<Segment> bases_;
  std::vector<Segment> incrs_;
  int64_t begin_;
  int64_t end_;
  int ret_;
  int64_t expected_count_;
};
void PrintTo(const PlanCountScenario &s, std::ostream *out)
{
  Scenario input{s.name_, {}, s.bases_, s.incrs_, {s.ret_}};
  PrintTo(input, out);
  *out << " plan=[" << s.begin_ << "," << s.end_ << ") expected_live_vector_count=" << s.expected_count_;
}
// Literal plan/count expectations, independent of the memory model implementation.
// Policy selection is checked separately by the Design/Threshold/Scheduling tables.
const PlanCountScenario plan_count_scenarios[] = {
  {"WindowExcludesLargeBase", {100000}, {100,100}, 1, 3, OB_SUCCESS, 200},
  {"DeleteFallbackSingleBase", {900}, {600}, 0, 1, OB_SUCCESS, 900},
  {"DeleteFallbackSingleIncr", {600}, {900}, 1, 2, OB_SUCCESS, 900},
  {"DeductDeletes", {{600,150}}, {{600,150}}, 0, 2, OB_SUCCESS, 900},
  {"ManualWindowKeepsHistory", {2000,3000}, {4000,5000}, 1, 4, OB_SUCCESS, 12000},
  {"WindowAfterUnreadable", {}, {{100,0,false},200,200}, 1, 3, OB_SUCCESS, 400},
  {"SelectedSourceUnreadable", {600}, {{600,0,false}}, 0, 2, OB_EAGAIN, 0},
  {"AllVectorsDeleted", {{600,600}}, {}, 0, 1, OB_SUCCESS, 0},
  {"DeletesExceedCount", {{600,700}}, {}, 0, 1, OB_SUCCESS, 0},
  {"DeletesAvoidRawCountOverflow", {{L,1}}, {1}, 0, 2, OB_SUCCESS, L},
  {"NegativeDeleteCount", {{100,-1}}, {}, 0, 1, OB_INVALID_ARGUMENT, 0},
  {"ZeroVectorPlan", {}, {0,0}, 0, 2, OB_SUCCESS, 0},
  {"MaximumCount", {L}, {}, 0, 1, OB_SUCCESS, L},
  {"CountOverflow", {L}, {1}, 0, 2, OB_SIZE_OVERFLOW, 0},
  {"NegativeCount", {}, {100,-1}, 0, 2, OB_INVALID_ARGUMENT, 0},
  {"MissingPlan", {100}, {}, -1, -1, OB_INVALID_ARGUMENT, 0},
  {"EmptyRange", {100}, {}, 0, 0, OB_INVALID_ARGUMENT, 0},
  {"ReversedRange", {100}, {100}, 2, 1, OB_INVALID_ARGUMENT, 0},
  {"RangePastEnd", {100}, {}, 0, 2, OB_INVALID_ARGUMENT, 0},
  {"EmptySources", {}, {}, 0, 1, OB_INVALID_ARGUMENT, 0},
};
std::string plan_count_scenario_name(const ::testing::TestParamInfo<PlanCountScenario> &info)
{ return info.param.name_; }
} // namespace

class TestVectorIndexMergePlanCount : public ::testing::TestWithParam<PlanCountScenario> {};
TEST_P(TestVectorIndexMergePlanCount, CountsOnlySelectedLiveVectors)
{
  const auto &s = GetParam();
  ObVecMergePolicyInput input;
  input.base_count_ = s.bases_.size();
  // Count only the supplied per-segment live vectors.
  for (const auto *segments : {&s.bases_, &s.incrs_}) {
    for (const auto &segment : *segments) {
      ObVecSegmentMergeInfo info;
      info.vector_count_ = segment.count_;
      info.deleted_vector_count_ = segment.deleted_;
      info.readable_ = segment.readable_;
      ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
    }
  }
  ObVecMergePlan plan;
  plan.begin_ = s.begin_;
  plan.end_ = s.end_;
  int64_t count = -1;
  EXPECT_EQ(s.ret_, plan.get_vector_count(input, count));
  EXPECT_EQ(s.expected_count_, count);
  EXPECT_EQ(s.begin_, plan.begin_);
  EXPECT_EQ(s.end_, plan.end_);
  if (s.ret_ != OB_SUCCESS || s.expected_count_ == 0) {
    // Validate the real estimator entry before schema access; no mocks or live tenant.
    ObVecIndexAsyncTaskCtx ctx;
    ctx.task_status_.task_type_ = OB_VECTOR_ASYNC_INDEX_MERGE;
    ctx.task_status_.tenant_id_ = 1001;
    ctx.task_status_.table_id_ = 20001;
    ctx.task_status_.tablet_id_ = ObTabletID(200001);
    int64_t memory = -1;
    EXPECT_EQ(s.ret_, ObVecIndexAsyncTaskUtil::estimate_merge_memory(&ctx, input, plan, memory));
    EXPECT_EQ(0, memory);
  }
}
INSTANTIATE_TEST_CASE_P(SelectedRange, TestVectorIndexMergePlanCount,
    ::testing::ValuesIn(plan_count_scenarios), plan_count_scenario_name);

TEST(TestVectorIndexMergeMemory, RejectNullTaskContext)
{
  ObVecMergePolicyInput input;
  ObVecMergePlan plan;
  int64_t memory = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObVecIndexAsyncTaskUtil::estimate_merge_memory(nullptr, input, plan, memory));
  EXPECT_EQ(0, memory);
}

TEST(TestVectorIndexMergeMemory, RejectWrongTaskTypeWithOtherwiseValidInput)
{
  // A valid zero-vector plan avoids schema access and isolates the task-type check.
  ObVecMergePolicyInput input;
  ObVecSegmentMergeInfo info;
  info.readable_ = true;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ObVecMergePlan plan;
  plan.begin_ = 0;
  plan.end_ = 1;
  ObVecIndexAsyncTaskCtx ctx;
  ctx.task_status_.tenant_id_ = 1001;
  ctx.task_status_.table_id_ = 20001;
  ctx.task_status_.tablet_id_ = ObTabletID(200001);
  ctx.task_status_.task_type_ = OB_VECTOR_ASYNC_INDEX_MERGE;
  int64_t memory = -1;
  ASSERT_EQ(OB_SUCCESS,
      ObVecIndexAsyncTaskUtil::estimate_merge_memory(&ctx, input, plan, memory));
  ASSERT_EQ(0, memory);
  ctx.task_status_.task_type_ = OB_VECTOR_ASYNC_MEM_SYNC_TASK;
  memory = -1;
  EXPECT_EQ(OB_INVALID_ARGUMENT,
      ObVecIndexAsyncTaskUtil::estimate_merge_memory(&ctx, input, plan, memory));
  EXPECT_EQ(0, memory);
}

TEST(TestVectorIndexMergePolicy, GlobalDeletesDoNotReplaceSegmentStatistics)
{
  ObVecMergeConfig config;
  config.max_vectors_ = 1000;
  ObVecMergePolicyInput input;
  input.deleted_vid_count_ = 300;
  input.base_count_ = 1;
  ObVecSegmentMergeInfo info;
  info.readable_ = true;
  info.vector_count_ = 900;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  info.vector_count_ = 600;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ObVectorIndexMergePolicy policy(config);
  ObVecMergePlan plan;
  EXPECT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(-1, plan.begin_);
  EXPECT_EQ(-1, plan.end_);
  bool need_task = false;
  ASSERT_EQ(OB_SUCCESS, policy.check_need_merge_task(input, need_task, plan));
  EXPECT_FALSE(need_task);
  EXPECT_EQ(-1, plan.begin_);
  EXPECT_EQ(-1, plan.end_);
}

TEST(TestVectorIndexMergePolicy, SelectedWindowLiveVectorCount)
{
  // M=1000/F=2: raw 600+600 selects TARGET; live 450+450 selects TIERED.
  // Check selection and counting together; this does not exercise memory admission.
  ObVecMergeConfig config;
  config.max_vectors_ = 1000;
  ObVecMergePolicyInput input;
  input.base_count_ = 1;
  input.deleted_vid_count_ = 300;
  ObVecSegmentMergeInfo info;
  info.vector_count_ = 600;
  info.deleted_vector_count_ = 150;
  info.readable_ = true;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ObVecMergePlan plan;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(TIERED, plan.strategy_);
  EXPECT_EQ(0, plan.begin_);
  EXPECT_EQ(2, plan.end_);
  int64_t count = -1;
  ASSERT_EQ(OB_SUCCESS, plan.get_vector_count(input, count));
  EXPECT_EQ(900, count);
}

TEST(TestVectorIndexMergePolicy, AsyncInsertReclamationAndNoDowngrade)
{
  using Type = ObVectorIndexSegmentType;
  ObVecMergeConfig config;
  config.max_vectors_ = 100;
  ObVectorIndexMergePolicy policy(config);
  ObVecMergePolicyInput input;
  input.has_async_index_ = true;
  input.async_published_scn_ = 100;
  ObVecSegmentMergeInfo info;
  info.vector_count_ = 30;
  info.scn_ = 100;
  info.readable_ = true;
  info.seg_type_ = Type::INCR_MERGE;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ObVecMergePlan plan;
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_); // Equality is safe.
  EXPECT_FALSE(plan.output_base_);
  input.segments_.at(1).scn_ = 101;
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(Type::INCR_MERGE, plan.output_type_);
  input.segments_.at(0).seg_type_ = Type::INCR_BASE;
  EXPECT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, AUTO, plan));
  input.segments_.at(1).scn_ = 99;
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_);
  input.segments_.at(0).seg_type_ = Type::BASE;
  input.base_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(Type::BASE, plan.output_type_); // Consuming a BASE preserves its role.
  EXPECT_TRUE(plan.output_base_);
  input.segments_.at(1).seg_type_ = Type::FREEZE_PERSIST;
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(ObVecMergePlan::FREEZE_REBUILD, plan.strategy_);
  EXPECT_EQ(1, plan.begin_);
  EXPECT_EQ(2, plan.end_);
  EXPECT_EQ(Type::INCR_MERGE, plan.output_type_);
  input.base_count_ = 0;
  input.segments_.at(0).seg_type_ = Type::INCR_BASE;
  info.seg_type_ = Type::FREEZE_PERSIST;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(1, plan.begin_); // An unsafe prefix must not starve later windows.
  EXPECT_EQ(3, plan.end_);
  EXPECT_EQ(Type::INCR_MERGE, plan.output_type_);
  input.async_published_scn_ = 0;
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(1, plan.begin_);
  EXPECT_EQ(Type::INCR_MERGE, plan.output_type_);
  input.segments_.at(1).seg_type_ = Type::INCR_MERGE;
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(Type::INCR_MERGE, plan.output_type_); // MERGE + FREEZE keeps visibility.
  ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, ALTER, plan));
  EXPECT_EQ(Type::BASE, plan.output_type_); // FULL remains a separate path.
}

TEST(TestVectorIndexMergePolicy, MergePreservesBaseRole)
{
  using Type = ObVectorIndexSegmentType;
  for (const Type tail_type : {Type::INCR_BASE, Type::INCR_MERGE}) {
    for (const bool has_async : {false, true}) {
      for (const int64_t size : {30, 60}) {
        ObVecMergeConfig config;
        config.max_vectors_ = 100;
        ObVecMergePolicyInput input;
        input.base_count_ = 1;
        input.has_async_index_ = has_async;
        input.async_published_scn_ = 100;
        ObVecSegmentMergeInfo info;
        info.seg_type_ = Type::BASE;
        info.vector_count_ = size;
        info.readable_ = true;
        info.scn_ = 100;
        ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
        info.seg_type_ = tail_type;
        ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
        ObVecMergePlan plan;
        ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan));
        EXPECT_EQ(size == 30 ? TIERED : TARGET, plan.strategy_);
        EXPECT_EQ(0, plan.begin_);
        EXPECT_EQ(2, plan.end_);
        EXPECT_EQ(Type::BASE, plan.output_type_);
        EXPECT_TRUE(plan.output_base_);
        input.async_published_scn_ = 99;
        EXPECT_EQ(has_async && tail_type == Type::INCR_MERGE ? OB_NO_NEED_MERGE : OB_SUCCESS,
            ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan));
      }
    }
  }
}

TEST(TestVectorIndexMergePolicy, AsyncFreezeRebuildMakesProgress)
{
  using Type = ObVectorIndexSegmentType;
  const std::vector<std::vector<Type>> layouts = {
    {Type::BASE, Type::FREEZE_PERSIST},
    {Type::INCR_BASE, Type::FREEZE_PERSIST},
    {Type::FREEZE_PERSIST, Type::INCR_BASE},
    {Type::INCR_BASE, Type::FREEZE_PERSIST, Type::INCR_BASE},
  };
  for (const auto &layout : layouts) {
    for (const auto trigger : {AUTO, COMPACT}) {
      ObVecMergeConfig config;
      config.max_vectors_ = 100;
      ObVectorIndexMergePolicy policy(config);
      ObVecMergePolicyInput input;
      input.base_count_ = layout.front() == Type::BASE ? 1 : 0;
      int64_t freeze = -1;
      for (const Type type : layout) {
        ObVecSegmentMergeInfo info;
        info.seg_type_ = type;
        info.readable_ = true;
        info.vector_count_ = 30;
        info.scn_ = type == Type::FREEZE_PERSIST ? 300 : 200;
        if (type == Type::FREEZE_PERSIST) { freeze = input.segments_.count(); }
        ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
      }
      ObVecMergePlan plan;
      bool need_task = false;
      input.has_async_index_ = true;
      input.async_published_scn_ = 100;
      // Scheduling sees the same async constraints and admits the FREEZE rebuild.
      ASSERT_EQ(OB_SUCCESS, policy.check_need_merge_task(input, need_task, plan));
      EXPECT_TRUE(need_task);
      EXPECT_EQ(ObVecMergePlan::FREEZE_REBUILD, plan.strategy_);
      EXPECT_EQ(freeze, plan.begin_);
      ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, trigger, plan));
      EXPECT_EQ(ObVecMergePlan::FREEZE_REBUILD, plan.strategy_);
      EXPECT_EQ(freeze, plan.begin_);
      EXPECT_EQ(freeze + 1, plan.end_);
      EXPECT_EQ(Type::INCR_MERGE, plan.output_type_);
      EXPECT_FALSE(plan.output_base_);
      EXPECT_EQ(Type::FREEZE_PERSIST, input.segments_.at(freeze).seg_type_);
      ASSERT_EQ(OB_SUCCESS, policy.check_need_merge_task(input, need_task, plan));
      EXPECT_TRUE(need_task);
      EXPECT_EQ(ObVecMergePlan::FREEZE_REBUILD, plan.strategy_);

      // Simulate snapshot rebuild at T > P; AUTO must wait without rebuilding again.
      input.segments_.at(freeze).seg_type_ = Type::INCR_MERGE;
      input.segments_.at(freeze).scn_ = 400;
      EXPECT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, AUTO, plan));
      input.async_published_scn_ = 400;
      ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
      EXPECT_EQ(0, plan.begin_);
      EXPECT_EQ(input.segments_.count(), plan.end_);
      EXPECT_EQ(input.base_count_ > 0 ? Type::BASE : Type::INCR_BASE, plan.output_type_);
      EXPECT_EQ(input.base_count_ > 0, plan.output_base_);
    }
  }
}

TEST(TestVectorIndexMergePolicy, AsyncFreezeRebuildPreservesWindowConstraints)
{
  using Type = ObVectorIndexSegmentType;
  struct Case {
    int64_t left_count_;
    int64_t right_count_;
    int64_t factor_;
    int64_t idle_us_;
    int64_t published_;
    bool readable_;
    int expected_;
  };
  const Case cases[] = {
    {60, 60, 3, INT64_MAX, 100, true, OB_SUCCESS}, // TARGET only.
    {30, 30, 2, INT64_MAX, 100, true, OB_SUCCESS}, // TIERED only.
    {80, 5, 3, 0, 100, true, OB_SUCCESS}, // Idle RESIDUAL only.
    {80, 5, 3, INT64_MAX, 100, true, OB_NO_NEED_MERGE}, // Not idle yet.
    {70, 70, 2, 0, 100, true, OB_NO_NEED_MERGE}, // Over capacity.
    {30, 30, 2, 0, 0, true, OB_NO_NEED_MERGE}, // No publication point.
    {30, 30, 2, 0, 99, true, OB_SUCCESS}, // Pruned source inherits visibility.
    {30, 30, 2, 0, 100, false, OB_EAGAIN}, // Unreadable FREEZE.
  };
  for (const auto &c : cases) {
    SCOPED_TRACE(::testing::Message() << "left=" << c.left_count_ << " right=" << c.right_count_
        << " factor=" << c.factor_ << " idle=" << c.idle_us_ << " P=" << c.published_
        << " readable=" << c.readable_);
    ObVecMergeConfig config;
    config.max_vectors_ = 100;
    config.merge_factor_ = c.factor_;
    config.idle_us_ = c.idle_us_;
    ObVecMergePolicyInput input;
    input.has_async_index_ = true;
    input.async_published_scn_ = c.published_;
    ObVecSegmentMergeInfo info;
    info.seg_type_ = Type::INCR_BASE;
    info.scn_ = 100;
    info.vector_count_ = c.left_count_;
    info.readable_ = true;
    ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
    info.seg_type_ = Type::FREEZE_PERSIST;
    info.scn_ = 300; // FREEZE's SCN is not a publication bound, even when above P.
    info.vector_count_ = c.right_count_;
    info.readable_ = c.readable_;
    ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
    ObVecMergePlan plan;
    ASSERT_EQ(c.expected_, ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan));
    if (c.expected_ == OB_SUCCESS) {
      EXPECT_EQ(ObVecMergePlan::FREEZE_REBUILD, plan.strategy_);
      EXPECT_EQ(1, plan.begin_);
      EXPECT_EQ(2, plan.end_);
      EXPECT_EQ(Type::INCR_MERGE, plan.output_type_);
    }
    input.segments_.reuse();
    ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
    EXPECT_EQ(c.readable_ ? OB_NO_NEED_MERGE : OB_EAGAIN,
        ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan)); // No blocked window.
  }
}

TEST(TestVectorIndexMergePolicy, AsyncPublicationBoundaries)
{
  using Type = ObVectorIndexSegmentType;
  struct Case
  {
    Type source_;
    int64_t scn_;
    int64_t published_;
    int ret_;
    Type output_;
  };
  const Case cases[] = {
    {Type::INCR_MERGE, 100, 0, OB_SUCCESS, Type::INCR_MERGE},
    {Type::INCR_MERGE, 100, 99, OB_SUCCESS, Type::INCR_MERGE},
    {Type::INCR_MERGE, 100, 100, OB_SUCCESS, Type::INCR_BASE},
    {Type::INCR_MERGE, 100, 101, OB_SUCCESS, Type::INCR_BASE},
    {Type::INCR_MERGE, 0, 100, OB_SUCCESS, Type::INCR_MERGE},
    {Type::FREEZE_PERSIST, 99, 100, OB_SUCCESS, Type::INCR_MERGE},
    {Type::INCR_BASE, 100, 0, OB_NO_NEED_MERGE, Type::INVALID},
    {Type::INCR_BASE, 100, 99, OB_SUCCESS, Type::INCR_BASE},
    {Type::INCR_BASE, 100, 100, OB_SUCCESS, Type::INCR_BASE},
    {Type::BASE, 100, 99, OB_SUCCESS, Type::BASE},
    {Type::BASE, 100, 100, OB_SUCCESS, Type::BASE},
    {Type::BASE, 100, 0, OB_NO_NEED_MERGE, Type::INVALID},
    {Type::BASE, 0, 100, OB_NO_NEED_MERGE, Type::INVALID},
    {Type::INCR_BASE, 0, 100, OB_NO_NEED_MERGE, Type::INVALID},
  };
  ObVecMergeConfig config;
  config.max_vectors_ = 100;
  for (const auto &c : cases) {
    for (const int64_t count : {30, 60}) {
      SCOPED_TRACE(::testing::Message() << "source=" << static_cast<int>(c.source_)
          << " scn=" << c.scn_ << " published=" << c.published_ << " count=" << count);
      ObVecMergePolicyInput input;
      input.has_async_index_ = true;
      input.async_published_scn_ = c.published_;
      input.base_count_ = c.source_ == Type::BASE ? 2 : 0;
      ObVecSegmentMergeInfo info;
      info.seg_type_ = c.source_;
      info.scn_ = c.scn_;
      info.vector_count_ = count;
      info.readable_ = true;
      ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
      ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
      ObVecMergePlan plan;
      ASSERT_EQ(c.ret_, ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan));
      EXPECT_EQ(c.output_, plan.output_type_);
      if (c.ret_ == OB_SUCCESS) {
        EXPECT_EQ(count == 30 ? TIERED : TARGET, plan.strategy_);
        EXPECT_EQ(0, plan.begin_);
        EXPECT_EQ(2, plan.end_);
        EXPECT_EQ(c.output_ == Type::BASE, plan.output_base_);
      }
    }
  }
}

TEST(TestVectorIndexMergePolicy, AsyncMixedWindowRetainsSafePrefix)
{
  using Type = ObVectorIndexSegmentType;
  for (const int64_t base_count : {0, 1, 2}) {
    for (const int64_t size : {30, 50}) {
      for (const Type tail : {Type::INCR_MERGE, Type::FREEZE_PERSIST}) {
        SCOPED_TRACE(::testing::Message() << "bases=" << base_count << " size=" << size
            << " tail=" << static_cast<int>(tail));
        ObVecMergeConfig config;
        config.max_vectors_ = 100;
        ObVectorIndexMergePolicy policy(config);
        ObVecMergePolicyInput input;
        input.base_count_ = base_count;
        input.has_async_index_ = true;
        input.async_published_scn_ = 100;
        ObVecSegmentMergeInfo info;
        info.readable_ = true;
        info.scn_ = 200;
        info.vector_count_ = size;
        for (int64_t i = 0; i < 2; ++i) {
          info.seg_type_ = i < base_count ? Type::BASE : Type::INCR_BASE;
          ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
        }
        info.seg_type_ = tail;
        info.scn_ = 100;
        info.vector_count_ = size == 30 ? 30 : 10;
        ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
        ObVecMergePlan plan;
        ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
        EXPECT_EQ(size == 30 ? TIERED : TARGET, plan.strategy_);
        EXPECT_EQ(0, plan.begin_);
        EXPECT_EQ(tail == Type::INCR_MERGE ? 3 : 2, plan.end_);
        EXPECT_EQ(base_count > 0 ? Type::BASE : Type::INCR_BASE, plan.output_type_);
        if (tail == Type::INCR_MERGE) {
          input.segments_.at(2).scn_ = 101;
          ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
          EXPECT_EQ(0, plan.begin_);
          EXPECT_EQ(2, plan.end_); // An unpublished tail must not discard the safe prefix.
          input.async_published_scn_ = 101;
          ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, plan));
          EXPECT_EQ(3, plan.end_); // No need to wait for the BASE build SCN of 200.
        }
      }
    }
  }
}

TEST(TestVectorIndexMergePolicy, AsyncPureBaseResidualAndManual)
{
  using Type = ObVectorIndexSegmentType;
  ObVecMergeConfig config;
  config.max_vectors_ = 100;
  config.merge_factor_ = 3;
  config.idle_us_ = INT64_MAX;
  ObVecMergePolicyInput input;
  input.has_async_index_ = true;
  input.async_published_scn_ = 100;
  ObVecSegmentMergeInfo info;
  info.readable_ = true;
  info.scn_ = 200;
  info.seg_type_ = Type::INCR_BASE;
  info.vector_count_ = 80;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  info.vector_count_ = 5;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ObVecMergePlan plan;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, COMPACT, plan));
  EXPECT_EQ(MANUAL, plan.strategy_);
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_);
  config.idle_us_ = 0;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(IDLE, plan.strategy_);
  EXPECT_EQ(0, plan.begin_);
  EXPECT_EQ(2, plan.end_);
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_);
  config.idle_us_ = INT64_MAX;
  input.segments_.at(0).vector_count_ = 150;
  input.segments_.at(1).vector_count_ = 150;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, COMPACT, plan));
  EXPECT_EQ(MANUAL, plan.strategy_);
  EXPECT_EQ(Type::BASE, plan.output_type_); // Preserve the existing oversized output rule.
}

TEST(TestVectorIndexMergePolicy, AsyncMixedWindowAdmissionAndExecution)
{
  using Type = ObVectorIndexSegmentType;
  for (const Type base_type : {Type::BASE, Type::INCR_BASE}) {
    for (const int64_t size : {30, 60}) {
      for (const int64_t published : {0, 100}) {
        for (const int64_t merge_scn : {0, 90, 100, 101}) {
          SCOPED_TRACE(::testing::Message() << "base=" << static_cast<int>(base_type)
              << " size=" << size << " P=" << published << " merge_scn=" << merge_scn);
          ObVecMergeConfig config;
          config.max_vectors_ = 100;
          ObVectorIndexMergePolicy policy(config);
          ObVecMergePolicyInput input;
          input.has_async_index_ = true;
          input.async_published_scn_ = published;
          input.base_count_ = base_type == Type::BASE ? 1 : 0;
          ObVecSegmentMergeInfo info;
          info.readable_ = true;
          info.scn_ = 200;
          info.vector_count_ = size;
          info.seg_type_ = base_type;
          ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
          info.seg_type_ = Type::INCR_MERGE;
          info.scn_ = merge_scn;
          ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
          const bool eligible = published > 0 && merge_scn > 0 && merge_scn <= published;
          for (int64_t round = 0; round < 3; ++round) {
            bool need_task = true;
            ObVecMergePlan scheduled;
            ASSERT_EQ(OB_SUCCESS, policy.check_need_merge_task(input, need_task, scheduled));
            EXPECT_EQ(eligible, need_task);
            ObVecMergePlan executed;
            ASSERT_EQ(eligible ? OB_SUCCESS : OB_NO_NEED_MERGE,
                policy.find_merge_segments(input, AUTO, executed));
            EXPECT_EQ(scheduled.strategy_, executed.strategy_);
            EXPECT_EQ(scheduled.begin_, executed.begin_);
            EXPECT_EQ(scheduled.end_, executed.end_);
            EXPECT_EQ(scheduled.output_type_, executed.output_type_);
            if (eligible) {
              EXPECT_EQ(size == 30 ? TIERED : TARGET, executed.strategy_);
              EXPECT_EQ(0, executed.begin_);
              EXPECT_EQ(2, executed.end_);
              EXPECT_EQ(base_type, executed.output_type_);
            } else {
              EXPECT_EQ(-1, scheduled.begin_);
              EXPECT_EQ(Type::INVALID, scheduled.output_type_);
            }
          }
          // Advancing publication admits the blocked window without waiting for BASE SCN=200.
          if (published == 100 && merge_scn == 101) {
            input.async_published_scn_ = 101;
            bool need_task = false;
            ObVecMergePlan scheduled;
            ObVecMergePlan executed;
            ASSERT_EQ(OB_SUCCESS, policy.check_need_merge_task(input, need_task, scheduled));
            EXPECT_TRUE(need_task);
            ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, AUTO, executed));
            EXPECT_EQ(scheduled.strategy_, executed.strategy_);
            EXPECT_EQ(0, executed.begin_);
            EXPECT_EQ(2, executed.end_);
            EXPECT_EQ(base_type, executed.output_type_);
          }
        }
      }
    }
  }
}

TEST(TestVectorIndexMergePolicy, AsyncDeleteAndFallbackReclamation)
{
  using Type = ObVectorIndexSegmentType;
  ObVecMergeConfig config;
  config.max_vectors_ = 100;
  config.merge_factor_ = 3;
  config.idle_us_ = INT64_MAX;
  ObVecMergePolicyInput input;
  input.has_async_index_ = true;
  input.async_published_scn_ = 100;
  ObVecSegmentMergeInfo info;
  info.seg_type_ = Type::INCR_BASE;
  info.scn_ = 200;
  info.vector_count_ = 100;
  info.deleted_vector_count_ = 100;
  info.readable_ = true;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ObVecMergePlan plan;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(DELETE, plan.strategy_);
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_); // Empty output retains its visibility role.
  input.async_published_scn_ = 99;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, COMPACT, plan));
  EXPECT_EQ(DELETE, plan.strategy_);
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_);
  input.deleted_vid_count_ = 100;
  bool need_task = true;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).check_need_merge_task(input, need_task, plan));
  EXPECT_TRUE(need_task); // Pure BASE rebuild inherits visibility despite a newer build SCN.
  input.async_published_scn_ = 100;
  input.segments_.at(0).deleted_vector_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).check_need_merge_task(input, need_task, plan));
  EXPECT_FALSE(need_task); // Global deletes cannot replace per-segment statistics.
  input.segments_.at(0).deleted_vector_count_ = 100;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).check_need_merge_task(input, need_task, plan));
  EXPECT_TRUE(need_task);
  EXPECT_EQ(DELETE, plan.strategy_);
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_);

  input.segments_.at(0).vector_count_ = 80;
  input.segments_.at(0).deleted_vector_count_ = 0;
  info.seg_type_ = Type::INCR_MERGE;
  info.scn_ = 100;
  info.vector_count_ = 5;
  info.deleted_vector_count_ = 0;
  ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, COMPACT, plan));
  EXPECT_EQ(MANUAL, plan.strategy_);
  EXPECT_EQ(0, plan.begin_);
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_);
  input.async_published_scn_ = 99;
  // Skipping the unsafe prefix leaves only one segment, so manual fallback must stop.
  ASSERT_EQ(OB_NO_NEED_MERGE, ObVectorIndexMergePolicy(config).find_merge_segments(input, COMPACT, plan));
  EXPECT_EQ(-1, plan.begin_);
  EXPECT_EQ(-1, plan.end_);
  input.has_async_index_ = false;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, COMPACT, plan));
  EXPECT_EQ(0, plan.begin_);
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_); // Dropping async indexes does not downgrade history.
  input.has_async_index_ = true;
  input.async_published_scn_ = 100;
  config.idle_us_ = 0;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, AUTO, plan));
  EXPECT_EQ(IDLE, plan.strategy_);
  EXPECT_EQ(Type::INCR_BASE, plan.output_type_);
  config.idle_us_ = INT64_MAX;
  input.segments_.at(0).vector_count_ = 150;
  input.segments_.at(1).vector_count_ = 150;
  ASSERT_EQ(OB_SUCCESS, ObVectorIndexMergePolicy(config).find_merge_segments(input, COMPACT, plan));
  EXPECT_EQ(MANUAL, plan.strategy_);
  EXPECT_EQ(Type::BASE, plan.output_type_); // Oversized manual output keeps multi-BASE publication.
  EXPECT_TRUE(plan.output_base_);
}

TEST(TestVectorIndexMergePolicy, AsyncOversizedManualMergePreservesWhitelist)
{
  using Type = ObVectorIndexSegmentType;
  ObVecMergeConfig config;
  config.max_vectors_ = 2;
  config.idle_us_ = INT64_MAX;
  ObVectorIndexMergePolicy policy(config);
  // Reject a single FREEZE after excluding an unsafe BASE; preserve multi-segment merging.
  for (const int64_t base_count : {0, 1}) {
    for (const int64_t incr_count : {1, 2}) {
      SCOPED_TRACE(::testing::Message() << "bases=" << base_count << " incrs=" << incr_count);
      ObVecMergePolicyInput input;
      input.base_count_ = base_count;
      input.has_async_index_ = true;
      input.async_published_scn_ = 100;
      ObVecSegmentMergeInfo info;
      info.readable_ = true;
      info.scn_ = 90;
      info.vector_count_ = 8;
      info.seg_type_ = Type::BASE;
      if (base_count > 0) {
        ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
      }
      info.vector_count_ = 3;
      info.seg_type_ = Type::FREEZE_PERSIST;
      for (int64_t i = 0; i < incr_count; ++i) {
        ASSERT_EQ(OB_SUCCESS, input.segments_.push_back(info));
      }
      ObVecMergePlan plan;
      EXPECT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, AUTO, plan));
      for (const int64_t published : {0, 100}) {
        input.async_published_scn_ = published;
        if (incr_count == 1) {
          ASSERT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, COMPACT, plan));
          EXPECT_EQ(-1, plan.begin_);
          EXPECT_EQ(-1, plan.end_);
          continue;
        }
        ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, COMPACT, plan));
        EXPECT_EQ(MANUAL, plan.strategy_);
        EXPECT_EQ(base_count, plan.begin_);
        EXPECT_EQ(base_count + incr_count, plan.end_);
        EXPECT_EQ(Type::INCR_MERGE, plan.output_type_);
        EXPECT_FALSE(plan.output_base_);
        // Memory admission must count the complete oversized output, not cap it at M.
        int64_t row_count = 0;
        ASSERT_EQ(OB_SUCCESS, plan.get_vector_count(input, row_count));
        EXPECT_EQ(3 * incr_count, row_count);
      }

      // A previously rebuilt incremental segment cannot merge with BASE until
      // publication reaches T=300. It must not be rewritten on its own either.
      input.segments_.at(base_count).seg_type_ = Type::INCR_MERGE;
      input.segments_.at(base_count).scn_ = 300;
      input.segments_.at(base_count).vector_count_ = 3 * incr_count;
      while (input.segments_.count() > base_count + 1) { input.segments_.pop_back(); }
      input.async_published_scn_ = 299;
      ASSERT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, COMPACT, plan));
      EXPECT_EQ(-1, plan.begin_);
      EXPECT_EQ(-1, plan.end_);

      input.async_published_scn_ = 300;
      EXPECT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, AUTO, plan));
      if (base_count == 0) {
        ASSERT_EQ(OB_NO_NEED_MERGE, policy.find_merge_segments(input, COMPACT, plan));
        EXPECT_EQ(-1, plan.begin_);
        EXPECT_EQ(-1, plan.end_);
        continue;
      }
      ASSERT_EQ(OB_SUCCESS, policy.find_merge_segments(input, COMPACT, plan));
      EXPECT_EQ(MANUAL, plan.strategy_);
      EXPECT_EQ(0, plan.begin_);
      EXPECT_EQ(base_count + 1, plan.end_);
      EXPECT_EQ(Type::BASE, plan.output_type_);
      EXPECT_TRUE(plan.output_base_);
    }
  }
}

TEST(TestVectorIndexMergePublication, replace_history_in_place_and_reject_changed_source)
{
  ObVectorIndexMeta old_meta;
  old_meta.header_.scn_ = 1;
  old_meta.is_persistent_ = true;
  for (const char *key : {"b0", "b1", "b2", "i0"}) {
    ObVectorIndexSegmentMeta meta;
    meta.seg_type_ = key[0] == 'b' ? ObVectorIndexSegmentType::BASE : ObVectorIndexSegmentType::INCR_MERGE;
    meta.index_type_ = VIAT_HNSW;
    meta.scn_ = 1;
    meta.blocks_cnt_ = 1;
    ASSERT_EQ(OB_SUCCESS, meta.deep_copy_seg_key(ObString::make_string(key), ObString::make_string(key)));
    ASSERT_EQ(OB_SUCCESS, (key[0] == 'b' ? old_meta.bases_ : old_meta.incrs_).push_back(meta));
  }
  ObSEArray<ObVectorIndexSegmentMeta, 4> outputs;
  ASSERT_EQ(OB_SUCCESS, outputs.push_back(old_meta.bases_.at(1)));
  auto &output = outputs.at(0);
  output.scn_ = 2;
  ASSERT_EQ(OB_SUCCESS, output.deep_copy_seg_key(ObString::make_string("new"), ObString::make_string("new")));
  ObVectorIndexSegmentMeta selected(old_meta.bases_.at(1));
  ObSEArray<const ObVectorIndexSegmentMeta *, 1> sources;
  ASSERT_EQ(OB_SUCCESS, sources.push_back(&selected));
  ObVecIdxSnapTableSegMergeOp op(1001);
  ASSERT_EQ(OB_SUCCESS, op.preprea_meta(outputs, sources));
  ObVectorIndexMeta result;
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 2));
  ASSERT_EQ(3, result.bases_.count()); ASSERT_EQ(1, result.incrs_.count());
  EXPECT_EQ(old_meta.bases_.at(0).start_key_, result.bases_.at(0).start_key_);
  EXPECT_EQ(output.start_key_, result.bases_.at(1).start_key_);
  EXPECT_EQ(old_meta.bases_.at(2).start_key_, result.bases_.at(2).start_key_);
  ++old_meta.bases_.at(1).scn_;
  EXPECT_EQ(OB_CANCELED, op.prepare_meta_for_update(result, old_meta, 3));

  sources.reuse();
  ASSERT_EQ(OB_SUCCESS, sources.push_back(&old_meta.incrs_.at(0)));
  output.seg_type_ = ObVectorIndexSegmentType::INCR_MERGE;
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 3));
  ASSERT_EQ(3, result.bases_.count()); ASSERT_EQ(1, result.incrs_.count());
  EXPECT_EQ(output.start_key_, result.incrs_.at(0).start_key_);
  EXPECT_EQ(old_meta.bases_.at(2).start_key_, result.bases_.at(2).start_key_);

  output.seg_type_ = ObVectorIndexSegmentType::BASE;
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 3));
  ASSERT_EQ(4, result.bases_.count()); EXPECT_EQ(0, result.incrs_.count());
  EXPECT_EQ(output.start_key_, result.bases_.at(3).start_key_);

  sources.reuse();
  ASSERT_EQ(OB_SUCCESS, sources.push_back(&old_meta.bases_.at(2)));
  ASSERT_EQ(OB_SUCCESS, sources.push_back(&old_meta.incrs_.at(0)));
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 3));
  ASSERT_EQ(3, result.bases_.count()); EXPECT_EQ(0, result.incrs_.count());
  EXPECT_EQ(output.start_key_, result.bases_.at(2).start_key_);
  sources.at(0) = &old_meta.bases_.at(0);
  EXPECT_EQ(OB_CANCELED, op.prepare_meta_for_update(result, old_meta, 3));

  sources.reuse();
  for (int64_t i = 0; i < old_meta.segment_count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sources.push_back(&old_meta.flat_seg_meta_at(i)));
  }
  output.seg_type_ = ObVectorIndexSegmentType::INCR_BASE;
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 3));
  EXPECT_EQ(0, result.bases_.count());
  ASSERT_EQ(1, result.incrs_.count());
  EXPECT_EQ(ObVectorIndexSegmentType::INCR_BASE, result.incrs_.at(0).seg_type_);
}
TEST(TestVectorIndexMergePublication, multi_results_replace_only_selected_sources)
{
  ObVectorIndexMeta old_meta, result;
  old_meta.header_.scn_ = 1;
  for (const char *key : {"b0", "b1", "i0", "i1"}) {
    ObVectorIndexSegmentMeta meta;
    meta.seg_type_ = key[0] == 'b' ? ObVectorIndexSegmentType::BASE : ObVectorIndexSegmentType::INCR_MERGE;
    meta.scn_ = 1; meta.blocks_cnt_ = 1; meta.index_type_ = VIAT_HNSW;
    ASSERT_EQ(OB_SUCCESS, meta.deep_copy_seg_key(ObString::make_string(key), ObString::make_string(key)));
    ASSERT_EQ(OB_SUCCESS, (key[0] == 'b' ? old_meta.bases_ : old_meta.incrs_).push_back(meta));
  }
  ObSEArray<ObVectorIndexSegmentMeta, 4> outputs;
  for (const char *key : {"n0", "n1", "n2"}) {
    ObVectorIndexSegmentMeta meta(old_meta.bases_.at(0));
    meta.scn_ = 2;
    ASSERT_EQ(OB_SUCCESS, meta.deep_copy_seg_key(ObString::make_string(key), ObString::make_string(key)));
    ASSERT_EQ(OB_SUCCESS, outputs.push_back(meta));
  }
  ObSEArray<const ObVectorIndexSegmentMeta *, 4> sources;
  ASSERT_EQ(OB_SUCCESS, sources.push_back(&old_meta.bases_.at(1)));
  ASSERT_EQ(OB_SUCCESS, sources.push_back(&old_meta.incrs_.at(0)));
  ObVecIdxSnapTableSegMergeOp op(1001);
  ASSERT_EQ(OB_SUCCESS, op.preprea_meta(outputs, sources));
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 3));
  ASSERT_EQ(4, result.bases_.count()); ASSERT_EQ(1, result.incrs_.count());
  EXPECT_EQ(old_meta.bases_.at(0).start_key_, result.bases_.at(0).start_key_);
  EXPECT_EQ(old_meta.incrs_.at(1).start_key_, result.incrs_.at(0).start_key_); // Concurrent Incr survives.
  for (int i = 0; i < 3; ++i) { EXPECT_EQ(outputs.at(i).start_key_, result.bases_.at(i + 1).start_key_); }
  sources.reuse();
  for (int64_t i = 0; i < old_meta.segment_count(); ++i) {
    ASSERT_EQ(OB_SUCCESS, sources.push_back(&old_meta.flat_seg_meta_at(i)));
  }
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 3));
  EXPECT_EQ(3, result.bases_.count()); EXPECT_EQ(0, result.incrs_.count()); // ALTER full replacement.
  sources.reuse();
  ASSERT_EQ(OB_SUCCESS, sources.push_back(&old_meta.incrs_.at(0)));
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 3));
  EXPECT_EQ(5, result.bases_.count()); EXPECT_EQ(1, result.incrs_.count());
  old_meta.bases_.reuse(); // No historical Base: promote an Incr prefix.
  ASSERT_EQ(OB_SUCCESS, op.prepare_meta_for_update(result, old_meta, 3));
  EXPECT_EQ(3, result.bases_.count()); EXPECT_EQ(1, result.incrs_.count());
  ASSERT_EQ(OB_SUCCESS, outputs.push_back(outputs.at(0)));
  EXPECT_EQ(OB_INVALID_ARGUMENT, op.prepare_meta_for_update(result, old_meta, 3));
  outputs.pop_back();
  outputs.at(1).seg_type_ = ObVectorIndexSegmentType::INCR_MERGE;
  EXPECT_EQ(OB_INVALID_ARGUMENT, op.prepare_meta_for_update(result, old_meta, 3));
}

} // namespace share
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_vector_index_merge_policy.log", true);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
