/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <string>
#include <thread>
#include "lib/allocator/page_arena.h"
#include "lib/json_type/ob_json_parse.h"
using namespace oceanbase::common;
using namespace oceanbase::lib;

struct MyPageAllocator: public ObIAllocator
{
  void* alloc(const int64_t sz, const ObMemAttr &attr)
  {
    UNUSED(attr);
    alloc_count_++;
    return ob_malloc(sz, ObNewModIds::TEST);
  }
  void *alloc(const int64_t sz)
  {
    alloc_count_++;
    return ob_malloc(sz, ObNewModIds::TEST);
  }
  void free(void *p)
  {
    free_count_++;
    ob_free(p);
  }
  void freed(const int64_t sz) { UNUSED(sz); }
  void set_label(const oceanbase::lib::ObLabel &label) { UNUSED(label); }
  void set_tenant_id(uint64_t tenant_id) { UNUSED(tenant_id); }
  oceanbase::lib::ObLabel get_label() const { return "test"; }

  static int64_t alloc_count_;
  static int64_t free_count_;
};

typedef PageArena<char, MyPageAllocator> MyModuleArena;
int64_t MyPageAllocator::alloc_count_;
int64_t MyPageAllocator::free_count_;

#define CHECK(expect_ac, expect_fc)                     \
  do {                                                  \
    int64_t &ac = MyPageAllocator::alloc_count_;        \
    int64_t &fc = MyPageAllocator::free_count_;         \
    EXPECT_EQ(expect_ac, ac);                           \
    EXPECT_EQ(expect_fc, fc);                           \
  } while (0)

#define RESET()                                         \
  do {                                                  \
    int64_t &ac = MyPageAllocator::alloc_count_;        \
    int64_t &fc = MyPageAllocator::free_count_;         \
    EXPECT_EQ(ac, fc);                                  \
    ac = fc = 0;                                        \
  } while (0)

TEST(TestPageArena, Basic)
{
  {
    CHECK(0, 0);
    MyModuleArena ma;
    ma.alloc(OB_MALLOC_NORMAL_BLOCK_SIZE-32-31);
    CHECK(1, 0);
    ma.free();
    CHECK(1, 1);
    RESET();

    // If we allocate memory with size 8K-32-32, it can be hold in the
    // first normal page.
    ma.alloc(OB_MALLOC_NORMAL_BLOCK_SIZE-32-32);
    CHECK(1, 0);
  }
  CHECK(1, 1);
  RESET();
}

TEST(TestPageArena, Tracer1)
{
  MyModuleArena ma;
  CHECK(0, 0);
  ma.alloc(10);
  CHECK(1, 0);

  EXPECT_TRUE(ma.set_tracer());
  ma.alloc(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ma.alloc(OB_MALLOC_NORMAL_BLOCK_SIZE);
  ma.alloc(OB_MALLOC_NORMAL_BLOCK_SIZE);
  CHECK(4, 0);

  constexpr auto N = OB_MALLOC_NORMAL_BLOCK_SIZE-32-32;
  for (int i = 0; i < N; i++) {
    ma.alloc(1);
  }
  CHECK(5, 0);

  EXPECT_TRUE(ma.revert_tracer());
  CHECK(5, 4);
  ma.free();
  RESET();
}

TEST(TestPageArena, Tracer2)
{
  MyModuleArena ma;
  CHECK(0, 0);

  for (int i = 0; i < 2; i++) {
    EXPECT_TRUE(ma.set_tracer());
    CHECK(3*i, 3*i);

    // some small allocates
    constexpr auto N = OB_MALLOC_NORMAL_BLOCK_SIZE-32-32;
    for (int i = 0; i < N; i++) {
      ma.alloc(1);
    }
    CHECK(1+3*i, 3*i);

    // some big allocates
    ma.alloc(OB_MALLOC_NORMAL_BLOCK_SIZE);
    ma.alloc(OB_MALLOC_NORMAL_BLOCK_SIZE);

    EXPECT_TRUE(ma.revert_tracer());
    CHECK(3+3*i, 3+3*i);
  }
  ma.free();
  RESET();
}

TEST(TestPageArena, aligned_alloc_bf)
{
  {
    ObAlignedArenaAllocator alloc(64);
    for (int i = 0; i < 1000; ++i) {
      const int64_t sz = ObRandom::rand(1000, 4000000);
      void *p = alloc.alloc(sz);
      ASSERT_EQ(0, (uint64_t)p % 32);
    }
  }
  {
    ObAlignedArenaAllocator alloc(64);
    for (int i = 0; i < 1000; ++i) {
      const int64_t sz = ObRandom::rand(100, 4000000);
      void *p = alloc.alloc(sz);
      ASSERT_EQ(0, (uint64_t)p % 64);
    }
  }
}

TEST(TestSafeArena, realloc_preserves_data)
{
  ObArenaAllocator arena;
  ObSafeArenaAllocator safe_arena(arena);
  ObIAllocator &allocator = safe_arena;
  char *buf = static_cast<char *>(allocator.alloc(256));
  ASSERT_NE(nullptr, buf);
  MEMSET(buf, 'x', 256);
  char *grown = static_cast<char *>(allocator.realloc(buf, 256, 384));
  ASSERT_NE(nullptr, grown);
  EXPECT_EQ(std::string(256, 'x'), std::string(grown, 256));
  MEMSET(grown, 'y', 384);

  // An intervening allocation prevents growing the buffer in place.
  char *sentinel = static_cast<char *>(allocator.alloc(64));
  ASSERT_NE(nullptr, sentinel);
  MEMSET(sentinel, 's', 64);
  char *moved = static_cast<char *>(allocator.realloc(grown, 384, 1024));
  ASSERT_NE(nullptr, moved);
  EXPECT_NE(grown, moved);
  EXPECT_EQ(std::string(384, 'y'), std::string(moved, 384));
  EXPECT_EQ(std::string(64, 's'), std::string(sentinel, 64));
}

TEST(TestSafeArena, concurrent_realloc)
{
  ObArenaAllocator arena;
  ObSafeArenaAllocator safe_arena(arena);
  ObIAllocator &allocator = safe_arena;
  std::thread workers[4];
  for (int i = 0; i < 4; ++i) {
    workers[i] = std::thread([&allocator, i]() {
      // Each thread owns its buffer; only the allocator is shared.
      for (int round = 0; round < 100; ++round) {
        int64_t size = 256;
        char *buf = static_cast<char *>(allocator.alloc(size));
        ASSERT_NE(nullptr, buf);
        MEMSET(buf, 'a' + i, size);
        for (int growth = 0; growth < 4; ++growth) {
          char *grown = static_cast<char *>(allocator.realloc(buf, size, size * 2));
          ASSERT_NE(nullptr, grown);
          EXPECT_EQ(std::string(size, 'a' + i), std::string(grown, size));
          MEMSET(grown + size, 'a' + i, size);
          buf = grown;
          size *= 2;
        }
      }
    });
  }
  for (auto &worker : workers) {
    worker.join();
  }
}

TEST(TestSafeArena, reset_releases_pages)
{
  ObArenaAllocator arena;
  ObSafeArenaAllocator safe_arena(arena);
  ObIAllocator &allocator = safe_arena;
  ASSERT_NE(nullptr, allocator.alloc(256));
  ASSERT_GT(allocator.used(), 0);
  ASSERT_GT(allocator.total(), 0);

  allocator.reset();
  EXPECT_EQ(0, allocator.used());
  EXPECT_EQ(0, allocator.total());
  ASSERT_NE(nullptr, allocator.alloc(512));
  allocator.reset();
  EXPECT_EQ(0, allocator.total());
}

TEST(TestSafeArena, set_attr_through_base_interface)
{
  ObArenaAllocator arena;
  ObSafeArenaAllocator safe_arena(arena);
  ObIAllocator &allocator = safe_arena;
  const ObMemAttr attr(1001, "SafeArenaTest", 1);
  allocator.set_attr(attr);

  const ObMemAttr &actual = arena.get_arena().get_page_allocator().get_attr();
  EXPECT_EQ(attr.tenant_id_, actual.tenant_id_);
  EXPECT_EQ(attr.label_, actual.label_);
  EXPECT_EQ(attr.ctx_id_, actual.ctx_id_);
}

TEST(TestSafeArena, strict_json_string_growth)
{
  // Strict parsing uses RapidJSON's 256-byte decoding stack, including the NUL.
  // Exercise both the first growth and subsequent growth of that stack.
  for (int64_t length : {255, 256, 257, 1024}) {
    ObArenaAllocator arena;
    ObSafeArenaAllocator safe_arena(arena);
    const std::string value(length, 'z');
    const std::string json = "\"" + value + "\"";
    ObJsonNode *node = nullptr;
    ASSERT_EQ(OB_SUCCESS, ObJsonParser::get_tree(&safe_arena, json.c_str(), json.size(),
                                               node, ObJsonParser::JSN_STRICT_FLAG));
    ASSERT_NE(nullptr, node);
    ASSERT_EQ(ObJsonNodeType::J_STRING, node->json_type());
    const ObString &parsed = static_cast<ObJsonString *>(node)->value();
    EXPECT_EQ(value, std::string(parsed.ptr(), parsed.length()));
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
