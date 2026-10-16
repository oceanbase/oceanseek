/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_I_ASYNC_INDEX_ITER_H_
#define OCEANBASE_STORAGE_OB_I_ASYNC_INDEX_ITER_H_

#include <cstdint>

namespace oceanbase
{
namespace storage
{

enum AsyncIndexIterType : uint8_t
{
  ASYNC_INDEX_ITER_INVALID = 0,
  // ASYNC_INDEX_ITER_EXAMPLE,
  ASYNC_INDEX_ITER_MAX
};

class ObIAsyncIndexIter
{
public:
  explicit ObIAsyncIndexIter(const AsyncIndexIterType type) : type_(type) {}
  virtual ~ObIAsyncIndexIter() {}
  AsyncIndexIterType type() const { return type_; }
  virtual void reset() = 0;
  virtual void reuse() = 0;

protected:
  AsyncIndexIterType type_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_I_ASYNC_INDEX_ITER_H_
