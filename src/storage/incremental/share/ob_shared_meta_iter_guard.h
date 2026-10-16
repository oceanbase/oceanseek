/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_SHARED_META_ITER_GUARD_
#define OCEANBASE_STORAGE_OB_SHARED_META_ITER_GUARD_
#include "storage/incremental/ob_shared_meta_service.h"

namespace oceanbase
{
namespace storage
{
template<typename T>
class ObSSMetaIterGuard
{
public:
  ObSSMetaIterGuard() : iter_(nullptr) {}
  ~ObSSMetaIterGuard() { reset(); }
  void set_iter(T *iter)
  {
    reset();
    iter_ = iter;
  }
  int get_iter(T *&iter);
  void reset();
private:
  T *iter_;

  DISALLOW_COPY_AND_ASSIGN(ObSSMetaIterGuard);
};

template<typename T>
void ObSSMetaIterGuard<T>::reset()
{
  if (OB_NOT_NULL(iter_)) {
    MTL(ObSSMetaService *)->free_iterator(iter_);
    iter_ = nullptr;
  }
}

template<typename T>
int ObSSMetaIterGuard<T>::get_iter(T *&iter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(iter_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "iter_ is null", K(ret));
  } else {
    iter = iter_;
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
#endif // OCEANBASE_STORAGE_OB_SHARED_META_ITER_GUARD_
