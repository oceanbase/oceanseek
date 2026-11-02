/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef  OCEANBASE_UNITTEST_MEMTABLE_ROWKEY_BUILDER_H_
#define  OCEANBASE_UNITTEST_MEMTABLE_ROWKEY_BUILDER_H_

#include "lib/allocator/page_arena.h"
#include "common/object/ob_object.h"
#include "common/rowkey/ob_rowkey.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;

class ObRowkeyWrapper
{
public:
  template <class... Args>
  ObRowkeyWrapper(const Args&... args) : obj_cnt_(0)
  {
    fill(args...);
  }
  ObRowkeyWrapper(const uint8_t obj_cnt) : obj_cnt_(obj_cnt) {}
  ObRowkeyWrapper(const int32_t obj_cnt) : obj_cnt_(obj_cnt) {}
  ~ObRowkeyWrapper() {}
public:
  CharArena &get_allocator() { return allocator_; }
  ObObj &get_cur_obj() { return objs_[obj_cnt_]; }
  int64_t get_obj_cnt() const { return obj_cnt_; }
  void add_obj() { obj_cnt_++; }
  ObRowkey &get_rowkey() { return rowkey_ = ObRowkey(objs_, obj_cnt_); }
  const ObRowkey &get_rowkey() const { return rowkey_ = ObRowkey(objs_, obj_cnt_); }
private:
  template <class T, class... Args>
  void fill(const T &head, const Args&... args)
  {
    head.build(*this);
    fill(args...);
  }
  void fill() { /*for recursion exit*/ }
private:
  CharArena allocator_;
  mutable ObObj objs_[OB_MAX_ROWKEY_COLUMN_NUMBER];
  int64_t obj_cnt_;
  mutable ObRowkey rowkey_;
};

////////////////////////////////////////////////////////////////////////////////////////////////////

class Varchar
{
public:
  Varchar(const char *str, const int64_t len) : str_(str), len_(len) {}
  ~Varchar() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    ObString obstr;
    obstr.assign_ptr(const_cast<char*>(str_), static_cast<int32_t>(len_));
    rowkey_wrapper.get_cur_obj().set_varchar(obstr);
    rowkey_wrapper.add_obj();
  }
private:
  const char *str_;
  const int64_t len_;
};

class Int
{
public:
  Int(const int64_t v) : v_(v) {}
  ~Int() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_int(v_);
    rowkey_wrapper.add_obj();
  }
private:
  const int64_t v_;
};

class Number
{
public:
  Number(const char *str) : str_(str) {}
  ~Number() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    number::ObNumber obnmb;
    obnmb.from(str_, rowkey_wrapper.get_allocator());
    rowkey_wrapper.get_cur_obj().set_number(obnmb);
    rowkey_wrapper.add_obj();
  }
private:
  const char *str_;
};

class Bool
{
public:
  Bool(const bool b) : b_(b) {}
  ~Bool() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_bool(b_);
    rowkey_wrapper.add_obj();
  }
private:
  const bool b_;
};

class PreciseDateTime
{
public:
  PreciseDateTime(const int64_t precise_datetime) : precise_datetime_(precise_datetime) {}
  ~PreciseDateTime() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_precise_datetime(precise_datetime_);
    rowkey_wrapper.add_obj();
  }
private:
  const int64_t precise_datetime_;
};

class Null
{
public:
  Null() {}
  ~Null() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_null();
    rowkey_wrapper.add_obj();
  }
};

class Min
{
public:
  Min() {}
  ~Min() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_ext(ObObj::MIN_OBJECT_VALUE);
    rowkey_wrapper.add_obj();
  }
};

class Max
{
public:
  Max() {}
  ~Max() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_ext(ObObj::MAX_OBJECT_VALUE);
    rowkey_wrapper.add_obj();
  }
};

class Ext
{
public:
  Ext(const int ext) : ext_(ext) {}
  ~Ext() {}
public:
  void build(ObRowkeyWrapper &rowkey_wrapper) const
  {
    rowkey_wrapper.get_cur_obj().set_ext(ext_);
    rowkey_wrapper.add_obj();
  }
private:
  const int ext_;
};

typedef Varchar         V;
typedef Int             I;
typedef Number          N;
typedef Bool            B;
typedef PreciseDateTime T;
typedef Null            U;
typedef Min             MIN;
typedef Max             MAX;
typedef Ext             E;
typedef ObRowkeyWrapper RK;

#define DEF_MTK(allocator, mtk, table_id, ...) \
  ObMemtableKey mtk; \
  { \
    RK _rkb_(__VA_ARGS__); \
    ret = mtk.deep_copy(table_id, _rkb_.get_rowkey(), allocator); \
    EXPECT_EQ(OB_SUCCESS, ret); \
  }

#define INIT_MTK(allocator, mtk, table_id, ...) \
  { \
    RK _rkb_(__VA_ARGS__); \
    ret = mtk.deep_copy(table_id, _rkb_.get_rowkey(), allocator); \
    EXPECT_EQ(OB_SUCCESS, ret); \
  }

}
}

#endif //OCEANBASE_UNITTEST_MEMTABLE_ROWKEY_BUILDER_H_
