/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"
#include "share/vector/ob_discrete_vector.h"
#include "share/vector/ob_continuous_vector.h"
#include "share/vector/ob_uniform_vector.h"
#include "sql/engine/px/ob_slice_calc.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_vec_msg.h"
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_msg.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

class StringHashTest : public OpTestKit
{
protected:
  void SetUp() override
  {
    OpTestKit::SetUp();
    // TestSqlUtils may already initialize the dynamic collations.
    if (!ObCharset::is_valid_collation(CS_TYPE_UTF8MB4_0900_BIN)) {
      ASSERT_EQ(OB_SUCCESS, ObCharset::init_charset());
    }
  }

  void check_null_and_skip(const ObExprBasicFuncs &funcs, const ObDatum &value)
  {
    const uint64_t seeds[2] = {99194853094755497ULL, 0x123456789abcdef0ULL};
    uint64_t value_hashes[2] = {};
    ObDatum null_datum;
    null_datum.set_null();
    for (int i = 0; i < 2; ++i) {
      uint64_t hash = 0;
      ASSERT_EQ(OB_SUCCESS, funcs.murmur_hash_v2_(null_datum, seeds[i], hash));
      EXPECT_EQ(seeds[i], hash);
      ASSERT_EQ(OB_SUCCESS, funcs.murmur_hash_v2_(value, seeds[i], value_hashes[i]));
    }
    for (int null_row : {0, 1}) {
      ObDatum datums[2] = {value, value};
      datums[null_row].set_null();
      for (bool batch_datum : {false, true}) {
        for (bool batch_seed : {false, true}) {
          for (bool with_skip : {false, true}) {
            SCOPED_TRACE(::testing::Message() << null_row << ":" << batch_datum
                << ":" << batch_seed << ":" << with_skip);
            uint64_t skip_storage = 0;
            auto &skip = *reinterpret_cast<ObBitVector *>(&skip_storage);
            if (with_skip) { skip.set(1); }
            uint64_t hashes[2] = {UINT64_MAX, UINT64_MAX};
            ASSERT_EQ(OB_SUCCESS, funcs.murmur_hash_v2_batch_(
                hashes, datums, batch_datum, skip, 2, seeds, batch_seed));
            for (int i = 0; i < 2; ++i) {
              const int seed_idx = batch_seed ? i : 0;
              const bool is_null = datums[batch_datum ? i : 0].is_null();
              EXPECT_EQ(with_skip && i == 1 ? UINT64_MAX
                  : is_null ? seeds[seed_idx] : value_hashes[seed_idx], hashes[i]);
            }
          }
        }
      }
    }
  }
};

TEST_F(StringHashTest, trailing_spaces_across_formats)
{
  for (auto cs : {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_0900_BIN}) {
    for (const char *value : {"", " ", "   ", "abc", "abc ", "abc  "}) {
      const int32_t len = strlen(value);
      int32_t hash_len = len;
      if (cs == CS_TYPE_UTF8MB4_BIN) {
        while (hash_len > 0 && value[hash_len - 1] == ' ') { --hash_len; }
      }
      const uint64_t seeds[2] = {99194853094755497ULL, 0x123456789abcdef0ULL};
      int32_t old_hash_len = len;
      while (old_hash_len > 0 && value[old_hash_len - 1] == ' ') { --old_hash_len; }
      SCOPED_TRACE(::testing::Message() << cs << ":" << len);

      ObDatum datums[2];
      datums[0].set_string(value, len);
      datums[1].set_string(value, len);
      auto *funcs = ObDatumFuncs::get_basic_func(ObVarcharType, cs, 0, false, false);
      ASSERT_NE(nullptr, funcs);
      uint64_t skip_storage = 0;
      auto &skip = *reinterpret_cast<ObBitVector *>(&skip_storage);
      for (bool v2 : {false, true}) {
        const int32_t expected_len = v2 ? hash_len : old_hash_len;
        auto scalar_func = v2 ? funcs->murmur_hash_v2_ : funcs->murmur_hash_;
        auto batch_func = v2 ? funcs->murmur_hash_v2_batch_ : funcs->murmur_hash_batch_;
        uint64_t hash = 0;
        ASSERT_EQ(OB_SUCCESS, scalar_func(datums[0], seeds[0], hash));
        EXPECT_EQ(ObMurmurHash::hash(value, expected_len, seeds[0]), hash) << v2;
        for (bool batch_seed : {false, true}) {
          uint64_t hashes[2] = {};
          ASSERT_EQ(OB_SUCCESS, batch_func(hashes, datums, true, skip, 2, seeds, batch_seed));
          for (int i = 0; i < 2; ++i) {
            EXPECT_EQ(ObMurmurHash::hash(value, expected_len, seeds[batch_seed ? i : 0]), hashes[i]) << v2;
          }
        }
      }

      if (cs == CS_TYPE_UTF8MB4_0900_BIN) {
        check_null_and_skip(*funcs, datums[0]);
      }
      char payload[32] = {};
      memcpy(payload, value, len);
      memcpy(payload + len, value, len);
      char *ptrs[2] = {payload, payload + len};
      int32_t lengths[2] = {len, len};
      uint32_t offsets[3] = {0, static_cast<uint32_t>(len), static_cast<uint32_t>(2 * len)};
      uint64_t null_storage = 0;
      auto *nulls = reinterpret_cast<ObBitVector *>(&null_storage);
      ObEvalInfo info{};
      ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> discrete(lengths, ptrs, nulls);
      ObContinuousVector<VectorBasicOp<VEC_TC_STRING>> continuous(offsets, payload, nulls);
      ObUniformVector<false, VectorBasicOp<VEC_TC_STRING>> uniform(datums, &info);
      ObExpr expr;
      expr.obj_meta_.set_varchar();
      expr.obj_meta_.set_collation_type(cs);
      // HGB registers popular candidates through this scalar callback.
      auto candidate_hash_func = get_hashfunc_by_tc(VEC_TC_STRING, VEC_HASH_ALGO_MURMUR);
      ASSERT_NE(nullptr, candidate_hash_func);
      uint64_t candidate_hash = 0;
      ASSERT_EQ(OB_SUCCESS, candidate_hash_func(expr.obj_meta_, value, len, seeds[0], candidate_hash));
      EXPECT_EQ(ObMurmurHash::hash(value, hash_len, seeds[0]), candidate_hash);
      for (auto algo : {VEC_HASH_ALGO_CRC, VEC_HASH_ALGO_MURMUR}) {
        expr.set_vec_hash_algo(algo);
        const auto expected = [&](uint64_t seed) {
          return algo == VEC_HASH_ALGO_CRC ? ObCrcHash::hash(value, hash_len, seed)
                                          : ObMurmurHash::hash(value, hash_len, seed);
        };
        for (ObIVector *vec : {static_cast<ObIVector *>(&discrete),
                              static_cast<ObIVector *>(&continuous),
                              static_cast<ObIVector *>(&uniform)}) {
          for (bool has_null : {false, true}) {
            SCOPED_TRACE(::testing::Message() << cs << ":" << algo << ":" << len
                << ":" << vec->get_format() << ":" << has_null);
            datums[1].set_string(value, len);
            vec->unset_null(1);
            vec->reset_has_null();
            if (has_null) { vec->set_null(1); vec->set_has_null(); }
            for (bool batch_seed : {false, true}) {
              for (bool with_skip : {false, true}) {
                skip_storage = 0;
                if (with_skip) { skip.set(1); }
                uint64_t hashes[2] = {0, UINT64_MAX};
                ASSERT_EQ(OB_SUCCESS, vec->hash(expr, hashes, skip, EvalBound(2, !with_skip),
                                               seeds, batch_seed));
                EXPECT_EQ(expected(seeds[0]), hashes[0]);
                EXPECT_EQ(with_skip ? UINT64_MAX : has_null ? seeds[batch_seed ? 1 : 0]
                              : expected(seeds[batch_seed ? 1 : 0]), hashes[1]);
                uint64_t one_hash = 0;
                ASSERT_EQ(OB_SUCCESS, vec->hash_for_one_row(expr, one_hash, 0, 2, seeds[0]));
                EXPECT_EQ(expected(seeds[0]), one_hash);
              }
            }
          }
          vec->unset_null(1);
          vec->reset_has_null();
        }
      }
    }
  }
}

TEST_F(StringHashTest, lob_hash_versions)
{
  for (auto cs : {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_0900_BIN}) {
    auto *funcs = ObDatumFuncs::get_basic_func(ObLongTextType, cs, 0, false, true);
    ASSERT_NE(nullptr, funcs);
    for (const char *value : {"", " ", "abc", "abc ", "abc  "}) {
      const int32_t len = strlen(value);
      int32_t old_hash_len = len;
      while (old_hash_len > 0 && value[old_hash_len - 1] == ' ') { --old_hash_len; }
      const int32_t hash_len = cs == CS_TYPE_UTF8MB4_0900_BIN ? len : old_hash_len;
      SCOPED_TRACE(::testing::Message() << cs << ":" << len);
      std::vector<char> lob(sizeof(ObLobCommon) + len);
      new(lob.data()) ObLobCommon();
      memcpy(lob.data() + sizeof(ObLobCommon), value, len);
      ObObj obj;
      obj.set_lob_value(ObLongTextType, lob.data(), lob.size());
      obj.set_collation_type(cs);
      ObDatum datums[2];
      datums[0].set_string(lob.data(), lob.size());
      datums[1].set_string(lob.data(), lob.size());
      const uint64_t seeds[2] = {99194853094755497ULL, 0x123456789abcdef0ULL};
      uint64_t skip_storage = 0;
      auto &skip = *reinterpret_cast<ObBitVector *>(&skip_storage);
      for (bool v2 : {false, true}) {
        const int32_t expected_len = v2 ? hash_len : old_hash_len;
        auto scalar_func = v2 ? funcs->murmur_hash_v2_ : funcs->murmur_hash_;
        auto batch_func = v2 ? funcs->murmur_hash_v2_batch_ : funcs->murmur_hash_batch_;
        uint64_t hash = 0;
        ASSERT_EQ(OB_SUCCESS, scalar_func(datums[0], seeds[0], hash));
        EXPECT_EQ(ObMurmurHash::hash(value, expected_len, seeds[0]), hash) << v2;
        for (bool batch_seed : {false, true}) {
          uint64_t hashes[2] = {};
          ASSERT_EQ(OB_SUCCESS, batch_func(hashes, datums, true, skip, 2, seeds, batch_seed));
          for (int i = 0; i < 2; ++i) {
            EXPECT_EQ(ObMurmurHash::hash(value, expected_len, seeds[batch_seed ? i : 0]), hashes[i]) << v2;
          }
        }
      }
      ObEvalInfo info{};
      ObUniformVector<false, VectorBasicOp<VEC_TC_LOB>> uniform(datums, &info);
      ObExpr expr;
      expr.obj_meta_ = obj.get_meta();
      auto candidate_hash_func = get_hashfunc_by_tc(VEC_TC_LOB, VEC_HASH_ALGO_MURMUR);
      ASSERT_NE(nullptr, candidate_hash_func);
      uint64_t candidate_hash = 0;
      ASSERT_EQ(OB_SUCCESS, candidate_hash_func(expr.obj_meta_, lob.data(), lob.size(),
                                               seeds[0], candidate_hash));
      EXPECT_EQ(ObMurmurHash::hash(value, hash_len, seeds[0]), candidate_hash);
      for (auto algo : {VEC_HASH_ALGO_CRC, VEC_HASH_ALGO_MURMUR}) {
        expr.set_vec_hash_algo(algo);
        uint64_t hashes[2] = {};
        ASSERT_EQ(OB_SUCCESS, uniform.hash(expr, hashes, skip, EvalBound(2), seeds, true));
        for (int i = 0; i < 2; ++i) {
          const uint64_t expected = algo == VEC_HASH_ALGO_CRC
              ? ObCrcHash::hash(value, hash_len, seeds[i])
              : ObMurmurHash::hash(value, hash_len, seeds[i]);
          EXPECT_EQ(expected, hashes[i]);
          uint64_t one_hash = 0;
          ASSERT_EQ(OB_SUCCESS, uniform.hash_for_one_row(expr, one_hash, i, 2, seeds[i]));
          EXPECT_EQ(expected, one_hash);
        }
      }
      if (cs == CS_TYPE_UTF8MB4_0900_BIN) {
        check_null_and_skip(*funcs, datums[0]);
      }
    }
  }
}

TEST_F(StringHashTest, lob_memory_locator_hash)
{
  for (auto cs : {CS_TYPE_UTF8MB4_BIN, CS_TYPE_UTF8MB4_0900_BIN}) {
    for (const char *value : {"", " ", "abc ", "abc  "}) {
      const int32_t len = strlen(value);
      int32_t trimmed_len = len;
      while (trimmed_len > 0 && value[trimmed_len - 1] == ' ') { --trimmed_len; }
      SCOPED_TRACE(::testing::Message() << cs << ":" << len);
      // A memory locator takes the content-extraction path instead of the disk in-row fast path.
      std::vector<char> lob(sizeof(ObMemLobCommon) + sizeof(ObLobCommon) + len);
      auto *mem_lob = new(lob.data()) ObMemLobCommon(TEMP_FULL_LOB);
      mem_lob->disk_locator_size_ = sizeof(ObLobCommon) + len;
      new(mem_lob->buffer_) ObLobCommon();
      memcpy(mem_lob->buffer_ + sizeof(ObLobCommon), value, len);
      common::ObLobLocatorV2 locator(ObString(lob.size(), lob.data()));
      ASSERT_TRUE(locator.is_valid());
      ObObj obj;
      obj.set_lob_value(ObLongTextType, lob.data(), lob.size());
      obj.set_collation_type(cs);
      ObExpr expr;
      expr.obj_meta_ = obj.get_meta();
      ObDatum datums[2];
      for (auto &datum : datums) { datum.set_string(lob.data(), lob.size()); }
      ObEvalInfo info{};
      ObUniformVector<false, VectorBasicOp<VEC_TC_LOB>> vec(datums, &info);
      uint64_t skip_storage = 0;
      auto &skip = *reinterpret_cast<ObBitVector *>(&skip_storage);
      const uint64_t seeds[2] = {99194853094755497ULL, 0x123456789abcdef0ULL};
      uint64_t hashes[2] = {};
      for (auto algo : {VEC_HASH_ALGO_CRC, VEC_HASH_ALGO_MURMUR}) {
        expr.set_vec_hash_algo(algo);
        ASSERT_EQ(OB_SUCCESS, vec.hash(expr, hashes, skip, EvalBound(2), seeds, true));
        for (int i = 0; i < 2; ++i) {
          const int32_t hash_len = cs == CS_TYPE_UTF8MB4_0900_BIN ? len : trimmed_len;
          const uint64_t expected = algo == VEC_HASH_ALGO_CRC
              ? ObCrcHash::hash(value, hash_len, seeds[i])
              : ObMurmurHash::hash(value, hash_len, seeds[i]);
          EXPECT_EQ(expected, hashes[i]);
          uint64_t one_hash = 0;
          ASSERT_EQ(OB_SUCCESS, vec.hash_for_one_row(expr, one_hash, i, 2, seeds[i]));
          EXPECT_EQ(expected, one_hash);
        }
      }
      ASSERT_EQ(OB_SUCCESS, vec.murmur_hash(expr, hashes, skip, EvalBound(2), seeds, true));
      for (int i = 0; i < 2; ++i) {
        EXPECT_EQ(ObMurmurHash::hash(value, trimmed_len, seeds[i]), hashes[i]);
      }
    }
  }
}

TEST_F(StringHashTest, hybrid_popular_datum_hash_matches_vector)
{
  // Code generation hashes popular values as Datum v2 with seed 0; PX probes vectors.
  ObArenaAllocator alloc;
  ObEvalCtx ctx(engine_.get_exec_ctx());
  alignas(64) char frame_storage[2048] = {};
  char *frames[] = {frame_storage};
  ctx.frames_ = frames;
  ctx.set_batch_size(2);
  ObExpr expr;
  expr.frame_idx_ = 0;
  expr.vector_header_off_ = 640;
  expr.eval_info_off_ = 128;
  expr.batch_result_ = true;
  expr.batch_idx_mask_ = UINT64_MAX;
  expr.obj_meta_.set_varchar();
  expr.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_0900_BIN);
  expr.datum_meta_ = ObDatumMeta(ObVarcharType, CS_TYPE_UTF8MB4_0900_BIN, 0, 0);
  char hot[] = "abc ", cold[] = "abc";
  char *strings[] = {hot, cold};
  int32_t lengths[] = {4, 3};
  uint64_t null_storage = 0, skip_storage = 0;
  auto *nulls = reinterpret_cast<ObBitVector *>(&null_storage);
  auto &skip = *reinterpret_cast<ObBitVector *>(&skip_storage);
  auto &header = expr.get_vector_header(ctx);
  new (&header) VectorHeader();
  header.set_format(VEC_DISCRETE);
  new (header.vector_buf_) ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>>(lengths, strings, nulls);
  auto *funcs = ObDatumFuncs::get_basic_func(ObVarcharType, CS_TYPE_UTF8MB4_0900_BIN, 0, false, false);
  ASSERT_NE(nullptr, funcs);
  ObHashFunc hash_func;
  hash_func.hash_func_ = funcs->murmur_hash_v2_;
  hash_func.batch_hash_func_ = funcs->murmur_hash_v2_batch_;
  ObSEArray<ObExpr *, 1> exprs;
  ObSEArray<ObHashFunc, 1> hash_funcs;
  ASSERT_EQ(OB_SUCCESS, exprs.push_back(&expr));
  ASSERT_EQ(OB_SUCCESS, hash_funcs.push_back(hash_func));
  for (int popular_count : {1, 4}) {
    ObSEArray<uint64_t, 4> popular_hashes;
    for (const char *value : {"abc ", "other1 ", "other2 ", "other3 "}) {
      if (popular_hashes.count() == popular_count) { break; }
      ObObj obj;
      obj.set_varchar(value);
      obj.set_collation_type(CS_TYPE_UTF8MB4_0900_BIN);
      ObDatum datum;
      ASSERT_EQ(OB_SUCCESS, datum.from_obj(obj));
      uint64_t hash = 0;
      ASSERT_EQ(OB_SUCCESS, hash_func.hash_func_(datum, 0, hash));
      ASSERT_EQ(OB_SUCCESS, popular_hashes.push_back(hash));
    }
    ObHybridHashBroadcastSliceIdCalc calc(alloc, 4, ObNullDistributeMethod::NONE,
                                         &exprs, &hash_funcs, &popular_hashes, false);
    for (int row : {0, 1}) {
      SCOPED_TRACE(::testing::Message() << popular_count << ":" << row);
      ctx.set_batch_idx(row);
      ObSliceIdxCalc::SliceIdxArray indexes;
      ASSERT_EQ(OB_SUCCESS, calc.get_slice_indexes_inner<true>(exprs, ctx, indexes, &skip));
      ASSERT_EQ(row == 0 ? 4 : 1, indexes.count());
      if (row == 0) {
        for (int i = 0; i < 4; ++i) { EXPECT_EQ(i, indexes.at(i)); }
      }
    }
  }
}

TEST_F(StringHashTest, runtime_filter_string_lob_membership)
{
  ObArenaAllocator alloc;
  ObEvalCtx ctx(engine_.get_exec_ctx());
  ObSEArray<ObExpr *, 1> unused_exprs;
  ObHashFuncs unused_hash_funcs(alloc);
  for (const char *value : {"", " ", "abc", "abc ", "abc  "}) {
    const int32_t len = strlen(value);
    std::vector<char> lob(sizeof(ObLobCommon) + len);
    new (lob.data()) ObLobCommon();
    memcpy(lob.data() + sizeof(ObLobCommon), value, len);
    ObDatum datums[2];
    datums[0].set_string(lob.data(), lob.size());
    datums[1].set_null();
    ObEvalInfo info{};
    ObUniformVector<false, VectorBasicOp<VEC_TC_LOB>> lob_vec(datums, &info);
    lob_vec.set_has_null();
    char *strings[] = {const_cast<char *>(value), const_cast<char *>(value)};
    int32_t lengths[] = {len, len};
    uint64_t null_storage = 0, skip_storage = 0;
    auto *nulls = reinterpret_cast<ObBitVector *>(&null_storage);
    auto &skip = *reinterpret_cast<ObBitVector *>(&skip_storage);
    ObDiscreteVector<VectorBasicOp<VEC_TC_STRING>> str_vec(lengths, strings, nulls);
    ObExpr str_expr, lob_expr;
    str_expr.obj_meta_.set_varchar();
    str_expr.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_0900_BIN);
    ObObj lob_obj;
    lob_obj.set_lob_value(ObLongTextType, lob.data(), lob.size());
    lob_obj.set_collation_type(CS_TYPE_UTF8MB4_0900_BIN);
    lob_expr.obj_meta_ = lob_obj.get_meta();
    const uint64_t seed = ObExprJoinFilter::JOIN_FILTER_SEED;
    for (auto algo : {VEC_HASH_ALGO_CRC, VEC_HASH_ALGO_MURMUR}) {
      SCOPED_TRACE(::testing::Message() << len << ":" << algo);
      str_expr.set_vec_hash_algo(algo);
      lob_expr.set_vec_hash_algo(algo);
      uint64_t str_hashes[2] = {}, lob_hashes[2] = {};
      ASSERT_EQ(OB_SUCCESS, str_vec.hash(str_expr, str_hashes, skip, EvalBound(2), &seed, false));
      ASSERT_EQ(OB_SUCCESS, lob_vec.hash(lob_expr, lob_hashes, skip, EvalBound(2), &seed, false));
      ASSERT_EQ(str_hashes[0], lob_hashes[0]);
      EXPECT_EQ(seed, lob_hashes[1]);
      for (bool build_lob : {false, true}) {
        uint64_t *build_hashes = build_lob ? lob_hashes : str_hashes;
        const uint64_t probe_hash = build_lob ? str_hashes[0] : lob_hashes[0];
        ObBatchRows brs;
        brs.size_ = 1;
        brs.skip_ = &skip;
        brs.all_rows_active_ = true;
        ObRFInFilterVecMsg in_filter;
        in_filter.max_in_num_ = 16;
        in_filter.build_send_opt_ = true;
        ASSERT_EQ(OB_SUCCESS, in_filter.sm_hash_set_.init(16, MTL_ID()));
        ASSERT_EQ(OB_SUCCESS, in_filter.insert_by_row_vector_without_calc_hash_value(
            &brs, unused_exprs, unused_hash_funcs, ctx, build_hashes));
        EXPECT_TRUE(in_filter.sm_hash_set_.test_hash(probe_hash));
        ObRFBloomFilterMsg bloom_filter;
        ASSERT_EQ(OB_SUCCESS, bloom_filter.bloom_filter_.init(16, alloc, MTL_ID()));
        ASSERT_EQ(OB_SUCCESS, bloom_filter.insert_bloom_filter_with_hash_values(&brs, build_hashes));
        bool match = false;
        ASSERT_EQ(OB_SUCCESS, bloom_filter.bloom_filter_.might_contain(probe_hash, match));
        EXPECT_TRUE(match);
      }
    }
  }
}

TEST_F(StringHashTest, hash_join_string_and_lob_keys)
{
  // Include both build/probe directions, plus a NULL in only the build input.
  for (const char *left_type : {"varchar(32)", "text"}) {
    for (const char *right_type : {"varchar(32)", "text"}) {
      SCOPED_TRACE(::testing::Message() << left_type << ":" << right_type);
      const std::string left_columns = std::string("c1 ") + left_type + " collate utf8mb4_0900_bin, c2 int";
      const std::string right_columns = std::string("c1 ") + right_type + " collate utf8mb4_0900_bin, c2 int";
      const std::string suffix = std::string(1, left_type[0]) + right_type[0];
      const std::string left_table = "t1_" + suffix, right_table = "t2_" + suffix;
      const std::string select = left_table + ".c2, " + right_table + ".c2";
      const std::string condition = left_table + ".c1 = " + right_table + ".c1";
      auto result = hash_join_test()
          .left_table(left_table.c_str(), left_columns.c_str())
          .right_table(right_table.c_str(), right_columns.c_str())
          .select(select.c_str())
          .join_type(INNER_JOIN)
          .on(condition.c_str())
          .with_left_data({{"abc ", 1}, {"abc", 2}, {NULL_VAL, 3},
                           {" ", 4}, {"", 5}, {"abc  ", 6}})
          .with_right_data({{"abc ", 11}, {"abc", 12}, {" ", 13},
                            {"", 14}, {"abc  ", 15}})
          .with_batch_size(2)
          .with_rich_format(true)
          .run(engine_);
      ASSERT_EQ(OB_SUCCESS, result.get_ret_code());
      EXPECT_TRUE(result.verify_unordered({{1, 11}, {2, 12}, {4, 13}, {5, 14}, {6, 15}}));
    }
  }
}

TEST_F(StringHashTest, in_not_in_datum_build_vector_probe)
{
  // More than two constants select the hash path. Right-hand NULL also checks NOT IN semantics.
  for (const char *type : {"varchar(32)", "text"}) {
    SCOPED_TRACE(type);
    const std::string columns = std::string("c1 ") + type + " collate utf8mb4_0900_bin";
    const std::string table = std::string("t1_") + type[0];
    ExprTestSpec spec;
    spec.table(table.c_str(), columns.c_str());
    const std::string values = "'abc ' collate utf8mb4_0900_bin, "
        "' ' collate utf8mb4_0900_bin, '' collate utf8mb4_0900_bin";
    const std::string expressions = "c1 in (" + values + "), c1 not in (" + values
        + "), c1 in (" + values + ", null), c1 not in (" + values + ", null)";
    auto result = spec.with_expr(expressions.c_str())
        .cg_as_vector_2_0()
        .with_data({{"abc "}, {"abc"}, {"abc  "}, {" "}, {""}, {"missing"}, {NULL_VAL}})
        .with_batch_size(2)
        .run(engine_);
    ASSERT_EQ(OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_ordered({{1, 0, 1, 0}, {0, 1, NULL_VAL, NULL_VAL},
                                      {0, 1, NULL_VAL, NULL_VAL}, {1, 0, 1, 0}, {1, 0, 1, 0},
                                      {0, 1, NULL_VAL, NULL_VAL},
                                      {NULL_VAL, NULL_VAL, NULL_VAL, NULL_VAL}}));
    ASSERT_EQ(4, spec.prepared_out_exprs_vec_.size());
    for (const auto *expr : spec.prepared_out_exprs_vec_) {
      ASSERT_NE(nullptr, expr->eval_vector_func_);
      EXPECT_EQ(type[0] == 't' ? VEC_TC_LOB : VEC_TC_STRING, expr->args_[0]->get_vec_value_tc());
    }
  }
}

TEST_F(StringHashTest, group_by_across_null_batches)
{
  for (const char *type : {"varchar(32)", "text"}) {
    SCOPED_TRACE(type);
    const std::string columns = std::string("c1 ") + type + " collate utf8mb4_0900_bin";
    const std::string table = std::string("t1_") + type[0];
    auto result = hash_groupby_test()
        .table(table.c_str(), columns.c_str())
        .select("c1, count(*)")
        .group_by("c1")
        .with_data({{"abc "}, {"abc "}, {NULL_VAL}, {"abc "}, {"abc"}, {"abc  "},
                    {" "}, {""}, {" "}, {NULL_VAL}})
        .with_batch_size(2)
        .with_rich_format(true)
        .run(engine_);
    ASSERT_EQ(OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_unordered({{"abc ", 3}, {NULL_VAL, 2}, {"abc", 1},
                                        {"abc  ", 1}, {" ", 2}, {"", 1}}));
  }
}

TEST_F(StringHashTest, distinct_across_null_batches)
{
  for (const char *type : {"varchar(32)", "text"}) {
    SCOPED_TRACE(type);
    const std::string columns = std::string("c1 ") + type + " collate utf8mb4_0900_bin";
    const std::string table = std::string("t1_") + type[0];
    auto result = hash_distinct_test()
        .table(table.c_str(), columns.c_str())
        .select("c1")
        .with_data({{"abc "}, {"abc "}, {NULL_VAL}, {"abc "}, {"abc"}, {"abc  "},
                    {" "}, {""}, {" "}, {NULL_VAL}})
        .with_batch_size(2)
        .with_rich_format(true)
        .run(engine_);
    ASSERT_EQ(OB_SUCCESS, result.get_ret_code());
    EXPECT_TRUE(result.verify_unordered({{"abc "}, {NULL_VAL}, {"abc"}, {"abc  "}, {" "}, {""}}));
  }
}

int main(int argc, char **argv)
{
  ObLogger::get_logger().set_file_name("test_string_hash.log", true);
  ObLogger::get_logger().set_log_level("WARN");
  init_arches();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
