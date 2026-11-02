/**
 * Copyright (C) <2026> OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#include <string>
#define private public
#define protected public

#include "lib/ob_errno.h"
#include "lib/allocator/page_arena.h"
#include "share/ob_rpc_struct.h"
#include "storage/incremental/share/ob_shared_tablet_extra_info.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/incremental/ssmeta/ob_ss_tablet_meta_info_iterator.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;

// Scope: this unit test covers the private grow-buffer primitives of
// ObSSTabletMetaInfoIterator (append_cstr_grow_ / append_obj_to_string_grow_ /
// obj_to_string_grow_), SIMPLE tablet-meta SCN output, the FULL-mode row compose
// shape, and the get_next not-init contract. The full init() -> get_next() integration flow (real
// __all_shared_meta read, OB_ITER_END on missing tablet, tiered sub-meta read)
// needs an observer/MTL environment and is covered by the obtest cases
// tools/obtest/basic/1003_c1_dump_ss_tablet_meta_info.* and 1005_*.

// A stand-in for an OB object whose to_string() wraps its text in braces, like
// the real TO_STRING_KV output. Crucially it models the REAL OB to_string()
// return contract: the value is clamped to at most cap-1 (databuff_vprintf sets
// pos = buf_len-1 on overflow), so a full-buffer result is indistinguishable from
// a truncated one. The grow loop must therefore treat n == cap-1 as "grow".
struct FakeObj
{
  const char *text_;  // already brace-wrapped, e.g. "{tablet}"
  int64_t to_string(char *buf, const int64_t cap) const
  {
    if (cap <= 0) {
      return 0;
    }
    const int64_t intended = static_cast<int64_t>(strlen(text_));
    const int64_t copy = intended < cap ? intended : cap - 1;  // leave room for '\0'
    if (copy > 0) {
      MEMCPY(buf, text_, copy);
    }
    return copy;
  }
};

class TestSSTabletMetaInfoIteratorGrow : public ::testing::Test
{
public:
  TestSSTabletMetaInfoIteratorGrow() = default;
  ~TestSSTabletMetaInfoIteratorGrow() = default;
};

// ------------------------------ append_cstr_grow_ ------------------------------

// A short literal fits in the seed buffer with no growth: buf pointer unchanged,
// len updated, null-terminated.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_cstr_fits_no_growth)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[64] = {0};
  char *buf = seed;
  int64_t cap = 64;
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, "hello", buf, cap, len));
  ASSERT_EQ(5, len);
  ASSERT_EQ(0, MEMCMP(buf, "hello", 5));
  ASSERT_EQ('\0', buf[5]);
  ASSERT_EQ(seed, buf);  // no growth → buf still the seed
  ASSERT_EQ(64, cap);
}

// A literal longer than avail triggers growth: buf may be reassigned to an arena
// block, the whole string lands in the buffer, cap doubles past the need.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_cstr_triggers_growth)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  const char *s = "01234567890123456789";  // 20 chars
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, s, buf, cap, len));
  ASSERT_EQ(20, len);
  ASSERT_EQ(0, MEMCMP(buf, s, 20));
  ASSERT_EQ('\0', buf[20]);
  ASSERT_EQ(32, cap);  // 8 -> 16 -> 32
}

// Growth must preserve the existing prefix: append a short prefix that fits, then a
// long literal that forces a grow — the prefix must survive the realloc.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_cstr_growth_preserves_prefix)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, "AAAA", buf, cap, len));
  ASSERT_EQ(4, len);
  ASSERT_EQ(8, cap);  // still fits
  const char *tail = "bcdefghijklmnopqrst";  // 19 chars
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, tail, buf, cap, len));
  ASSERT_EQ(4 + 19, len);
  ASSERT_EQ(0, MEMCMP(buf, "AAAA", 4));
  ASSERT_EQ(0, MEMCMP(buf + 4, tail, 19));
  ASSERT_EQ('\0', buf[4 + 19]);
  ASSERT_EQ(32, cap);  // 8 -> 16 -> 32
}

// Several doublings from a tiny cap. Start cap 4, append 100 chars: the loop must
// walk 4->8->16->32->64->128 and land the full string.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_cstr_multi_doubling)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[4] = {0};
  char *buf = seed;
  int64_t cap = 4;
  int64_t len = 0;
  char fill_str[101];
  for (int i = 0; i < 100; ++i) {
    fill_str[i] = static_cast<char>('a' + (i % 26));
  }
  fill_str[100] = '\0';
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, fill_str, buf, cap, len));
  ASSERT_EQ(100, len);
  ASSERT_EQ(0, MEMCMP(buf, fill_str, 100));
  ASSERT_EQ('\0', buf[100]);
  ASSERT_EQ(128, cap);  // 4->8->16->32->64->128
}

// Empty literal is a no-op write but must still be accepted (len stays).
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_cstr_empty)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, "", buf, cap, len));
  ASSERT_EQ(0, len);
  ASSERT_EQ('\0', buf[0]);
  ASSERT_EQ(8, cap);
}

// Boundary: slen == avail. The fit check is strict `slen < avail` (room for '\0'),
// so slen == avail must trigger a grow. After grow avail doubles and the string fits.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_cstr_boundary_exact_avail_grows)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  const char *s = "01234567";  // 8 chars == avail (cap - len = 8)
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, s, buf, cap, len));
  ASSERT_EQ(8, len);
  ASSERT_EQ(0, MEMCMP(buf, s, 8));
  ASSERT_EQ('\0', buf[8]);
  ASSERT_EQ(16, cap);  // 8 -> 16 (slen == avail grows)
}

// Invalid args: null buf / non-positive cap / negative len / null s all return
// OB_INVALID_ARGUMENT and reset len to 0.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_cstr_invalid_args)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  {
    char *buf = nullptr;
    int64_t cap = 8;
    int64_t len = 0;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.append_cstr_grow_(alloc, "x", buf, cap, len));
    ASSERT_EQ(0, len);
  }
  {
    char *buf = seed;
    int64_t cap = 0;
    int64_t len = 0;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.append_cstr_grow_(alloc, "x", buf, cap, len));
    ASSERT_EQ(0, len);
    ASSERT_EQ('\0', buf[0]);
  }
  {
    char *buf = seed;
    int64_t cap = 8;
    int64_t len = -1;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.append_cstr_grow_(alloc, "x", buf, cap, len));
    ASSERT_EQ(0, len);
    ASSERT_EQ('\0', buf[0]);
  }
  {
    char *buf = seed;
    int64_t cap = 8;
    int64_t len = 0;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.append_cstr_grow_(alloc, nullptr, buf, cap, len));
    ASSERT_EQ(0, len);
    ASSERT_EQ('\0', buf[0]);
  }
}

// ---------------------------- append_obj_to_string_grow_ -----------------------------

// Short object fits with no growth.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_obj_fits_no_growth)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[64] = {0};
  char *buf = seed;
  int64_t cap = 64;
  int64_t len = 0;
  FakeObj obj{"{abc}"};
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, obj, buf, cap, len));
  ASSERT_EQ(5, len);
  ASSERT_EQ(0, MEMCMP(buf, "{abc}", 5));
  ASSERT_EQ('\0', buf[5]);
  ASSERT_EQ(seed, buf);
  ASSERT_EQ(64, cap);
}

// Object longer than avail triggers growth; full to_string lands in the buffer.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_obj_triggers_growth)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  FakeObj obj{"{01234567890123456789}"};  // 22 chars
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, obj, buf, cap, len));
  ASSERT_EQ(22, len);
  ASSERT_EQ(0, MEMCMP(buf, obj.text_, 22));
  ASSERT_EQ('\0', buf[22]);
  ASSERT_EQ(32, cap);  // 8 -> 16 -> 32
}

// Boundary: to_string() returns exactly `avail` (intended length == free space).
// The grow loop treats n < avail as "fits"; n == avail must NOT be treated as fit
// (there's no room for the null terminator) — it must grow. Verifies the truncation
// check is strict-less-than, not less-or-equal.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_obj_boundary_exact_avail_grows)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  FakeObj obj{"01234567"};  // 8 chars == avail (cap - len = 8)
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, obj, buf, cap, len));
  // n == avail (8) is not < avail → grow happened; after grow avail=16, n=8<16 fits.
  ASSERT_EQ(8, len);
  ASSERT_EQ(0, MEMCMP(buf, "01234567", 8));
  ASSERT_EQ('\0', buf[8]);
  ASSERT_EQ(16, cap);  // 8 -> 16
}

// Boundary: to_string() returns exactly avail-1. Because OB to_string() clamps its
// return to at most avail-1, this value is ambiguous — a genuine exact-fit and a
// truncation both report avail-1 — so the loop must conservatively grow. After the
// grow the content fits with room to spare. This costs one extra grow for the rare
// exact-fill case, which is the safe trade to never silently truncate.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_obj_boundary_one_less_grows)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  FakeObj obj{"0123456"};  // 7 chars == avail-1 → ambiguous, must grow
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, obj, buf, cap, len));
  ASSERT_EQ(7, len);
  ASSERT_EQ(0, MEMCMP(buf, "0123456", 7));
  ASSERT_EQ('\0', buf[7]);
  ASSERT_EQ(16, cap);  // 8 -> 16 (grew because avail-1 is ambiguous)
}

// Growth preserves an existing prefix: append a short obj, then a long obj that
// forces a grow — the first object's bytes must survive the realloc.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_obj_growth_preserves_prefix)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[16] = {0};
  char *buf = seed;
  int64_t cap = 16;
  int64_t len = 0;
  FakeObj a{"AAAA"};
  FakeObj b{"{01234567890123456789}"};  // 22 chars > 16-4
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, a, buf, cap, len));
  ASSERT_EQ(4, len);
  ASSERT_EQ(16, cap);
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, b, buf, cap, len));
  ASSERT_EQ(4 + 22, len);
  ASSERT_EQ(0, MEMCMP(buf, "AAAA", 4));
  ASSERT_EQ(0, MEMCMP(buf + 4, b.text_, 22));
  ASSERT_EQ('\0', buf[4 + 22]);
  ASSERT_EQ(32, cap);  // 16 -> 32
}

// Invalid args for the obj variant.
TEST_F(TestSSTabletMetaInfoIteratorGrow, append_obj_invalid_args)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  FakeObj obj{"x"};
  {
    char *buf = nullptr;
    int64_t cap = 8;
    int64_t len = 0;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.append_obj_to_string_grow_(alloc, obj, buf, cap, len));
    ASSERT_EQ(0, len);
  }
  {
    char *buf = seed;
    int64_t cap = 0;
    int64_t len = 0;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.append_obj_to_string_grow_(alloc, obj, buf, cap, len));
    ASSERT_EQ(0, len);
  }
  {
    char *buf = seed;
    int64_t cap = 8;
    int64_t len = -1;
    ASSERT_EQ(OB_INVALID_ARGUMENT, iter.append_obj_to_string_grow_(alloc, obj, buf, cap, len));
    ASSERT_EQ(0, len);
  }
}

// ----------------------------- obj_to_string_grow_ -----------------------------
// obj_to_string_grow_ is the non-append variant used by format_and_add_row_ for every
// DETAIL/FULL sstable/cg/table-store/schema row. It resets len=0 and calls alloc.reuse()
// on each grow (no prefix to preserve), unlike append_obj_to_string_grow_ which preserves
// a prefix. These tests lock its grow + reuse behavior.

// Short object fits with no growth; len reset to the object length.
TEST_F(TestSSTabletMetaInfoIteratorGrow, obj_to_string_fits_no_growth)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[64] = {0};
  char *buf = seed;
  int64_t cap = 64;
  int64_t len = 99;  // ignored: obj_to_string_grow_ resets len=0
  FakeObj obj{"{abc}"};
  ASSERT_EQ(OB_SUCCESS, iter.obj_to_string_grow_(alloc, obj, buf, cap, len));
  ASSERT_EQ(5, len);
  ASSERT_EQ(0, MEMCMP(buf, "{abc}", 5));
  ASSERT_EQ('\0', buf[5]);
  ASSERT_EQ(seed, buf);
  ASSERT_EQ(64, cap);
}

// Object longer than cap triggers growth via reuse+realloc; full to_string lands in buf.
TEST_F(TestSSTabletMetaInfoIteratorGrow, obj_to_string_triggers_growth)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  FakeObj obj{"{01234567890123456789}"};  // 22 chars
  ASSERT_EQ(OB_SUCCESS, iter.obj_to_string_grow_(alloc, obj, buf, cap, len));
  ASSERT_EQ(22, len);
  ASSERT_EQ(0, MEMCMP(buf, obj.text_, 22));
  ASSERT_EQ('\0', buf[22]);
  ASSERT_EQ(32, cap);  // 8 -> 16 -> 32
}

// reuse-on-grow does NOT preserve a prior prefix: a second call after a grow overwrites
// from pos 0 (len reset to 0), so only the second object's text remains. This is the
// key behavioral difference from append_obj_to_string_grow_.
TEST_F(TestSSTabletMetaInfoIteratorGrow, obj_to_string_reuse_does_not_preserve_prefix)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[8] = {0};
  char *buf = seed;
  int64_t cap = 8;
  int64_t len = 0;
  FakeObj long_obj{"{01234567890123456789}"};  // forces a grow + reuse
  FakeObj short_obj{"{xy}"};
  ASSERT_EQ(OB_SUCCESS, iter.obj_to_string_grow_(alloc, long_obj, buf, cap, len));
  ASSERT_EQ(22, len);
  // second call: len reset to 0, reuse frees the prior grown block, short obj fits
  ASSERT_EQ(OB_SUCCESS, iter.obj_to_string_grow_(alloc, short_obj, buf, cap, len));
  ASSERT_EQ(4, len);
  ASSERT_EQ(0, MEMCMP(buf, "{xy}", 4));
  ASSERT_EQ('\0', buf[4]);
}

// ------------------------------- get_next contract -------------------------------

// A default-constructed (un-init'd) iterator must reject get_next with OB_NOT_INIT.
// (init() requires an observer/MTL environment; the success + OB_ITER_END paths are
// covered by obtest 1003/1005.)
TEST_F(TestSSTabletMetaInfoIteratorGrow, get_next_not_init)
{
  ObSSTabletMetaInfoIterator iter;
  ObSSTabletMetaInfoRow row;
  ASSERT_EQ(OB_NOT_INIT, iter.get_next(row));
}

TEST_F(TestSSTabletMetaInfoIteratorGrow, simple_tablet_meta_contains_mds_start_scn)
{
  ObTablet tablet;
  ObTabletMeta &meta = tablet.tablet_meta_;
  // Distinct values above 32 bits detect a wrong SCN source or integer truncation.
  ASSERT_EQ(OB_SUCCESS, meta.clog_checkpoint_scn_.convert_for_tx(1000000000001));
  ASSERT_EQ(OB_SUCCESS, meta.ddl_checkpoint_scn_.convert_for_tx(1000000000002));
  ASSERT_EQ(OB_SUCCESS, meta.mds_start_scn_.convert_for_tx(1000000000003));

  ObSSTabletMetaInfoIterator iter;
  iter.mode_ = obrpc::ObDumpSSTabletMetaInfoMode::SIMPLE;
  iter.meta_value_buf_ = static_cast<char *>(iter.allocator_.alloc(ObSSTabletMetaInfoIterator::META_VALUE_BUF_LEN));
  ASSERT_NE(nullptr, iter.meta_value_buf_);
  ObSSMetaHeadValueInfo head_info;
  ObSSUpdateTabletLog update_log;
  ASSERT_EQ(OB_SUCCESS, iter.emit_tablet_meta_row_(tablet, head_info, 0 /*data_version*/, update_log));

  ASSERT_EQ(1, iter.rows_.count());
  const ObSSTabletMetaInfoRow &row = iter.rows_.at(0);
  ASSERT_EQ(1, row.level_);
  ASSERT_EQ(0, row.type_.case_compare("tablet meta"));
  ASSERT_FALSE(row.meta_value_.empty());
  const std::string summary(row.meta_value_.ptr(), row.meta_value_.length());
  EXPECT_NE(std::string::npos, summary.find(",clog_checkpoint_scn=1000000000001,")) << summary;
  EXPECT_NE(std::string::npos, summary.find(",ddl_checkpoint_scn=1000000000002,")) << summary;
  EXPECT_NE(std::string::npos, summary.find(",mds_start_scn=1000000000003,")) << summary;
}

// ----------------------- combined weave (the real FULL-mode shape) -----------------------

// Reproduce the exact append sequence used in collect_and_fill_sub_meta_ FULL branch
// (open "{" + obj + "," + obj + "," + obj + close "}") with small seed cap so growth +
// prefix-preservation are exercised. Each FakeObj already wraps in braces, so the result
// must be {{<tablet>},{<head>},{<log>}} — i.e. one outer {} around comma-separated braced
// objects. This also guards against the separator bug ("," vs "},{") regressing.
TEST_F(TestSSTabletMetaInfoIteratorGrow, weave_compose_full_row_with_log)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[16] = {0};  // small → forces growth mid-compose
  char *buf = seed;
  int64_t cap = 16;
  int64_t len = 0;
  FakeObj tablet{"{tablet}"};
  FakeObj head{"{head}"};
  FakeObj log{"{log}"};
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, "{", buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, tablet, buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, ",", buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, head, buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, ",", buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, log, buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, "}", buf, cap, len));
  const char *expected = "{{tablet},{head},{log}}";
  ASSERT_EQ(static_cast<int64_t>(strlen(expected)), len);
  ASSERT_EQ(0, MEMCMP(buf, expected, len));
  ASSERT_EQ('\0', buf[len]);
}

// Same weave but skipping the update_log (the "old row" degradation path), so the
// result is {{<tablet>},{<head>}} with no trailing comma.
TEST_F(TestSSTabletMetaInfoIteratorGrow, weave_compose_full_row_without_log)
{
  ObSSTabletMetaInfoIterator iter;
  ObArenaAllocator alloc;
  char seed[16] = {0};
  char *buf = seed;
  int64_t cap = 16;
  int64_t len = 0;
  FakeObj tablet{"{tablet}"};
  FakeObj head{"{head}"};
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, "{", buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, tablet, buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, ",", buf, cap, len));
  ASSERT_EQ(OB_SUCCESS, iter.append_obj_to_string_grow_(alloc, head, buf, cap, len));
  // no update_log → skip the second "," + obj
  ASSERT_EQ(OB_SUCCESS, iter.append_cstr_grow_(alloc, "}", buf, cap, len));
  const char *expected = "{{tablet},{head}}";
  ASSERT_EQ(static_cast<int64_t>(strlen(expected)), len);
  ASSERT_EQ(0, MEMCMP(buf, expected, len));
  ASSERT_EQ('\0', buf[len]);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_tablet_meta_info_iterator_grow.log*");
  OB_LOGGER.set_file_name("test_ss_tablet_meta_info_iterator_grow.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
