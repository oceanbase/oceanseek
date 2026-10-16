/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_LOGIC_TABLE_UTIL_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_LOGIC_TABLE_UTIL_H_

#include "lib/hash/ob_hashmap.h"
#include "lib/hash/ob_hashset.h"
#include "lib/allocator/ob_allocator.h"
#include "common/ob_tablet_id.h"
#include "lib/ob_errno.h"
#include "share/ob_errno.h"

namespace oceanbase
{
namespace share
{

struct ObIvfHotNsIds {
  explicit ObIvfHotNsIds(ObIAllocator &allocator)
    : self_allocator_(allocator), is_inited_(false)
  {}
  ~ObIvfHotNsIds()
  {
    if (is_inited_) {
      ns_id_set_.destroy();
    }
  }
  int init(int64_t bucket_num, uint64_t tenant_id)
  {
    int ret = ns_id_set_.create(bucket_num,
                                ObMemAttr(tenant_id, "IvfHotNsSet"),
                                ObMemAttr(tenant_id, "IvfHotNsSet"));
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
    return ret;
  }
  ObIAllocator &get_self_allocator() { return self_allocator_; }
  common::hash::ObHashSet<int64_t> ns_id_set_;
  ObIAllocator &self_allocator_;
  bool is_inited_;
};
typedef common::hash::ObHashMap<common::ObTabletID, ObIvfHotNsIds*> HotNsIdsMap;

// Functor for read_atomic callback: check if ns_id exists in ObIvfHotNsIds
class ObCheckHotNsIdFunc
{
public:
  ObCheckHotNsIdFunc(int64_t ns_id, bool &is_hot) : ns_id_(ns_id), is_hot_(is_hot) {}
  void operator()(const common::hash::HashMapPair<common::ObTabletID, ObIvfHotNsIds*> &entry)
  {
    if (OB_NOT_NULL(entry.second)) {
      int tmp_ret = entry.second->ns_id_set_.exist_refactored(ns_id_);
      if (OB_HASH_EXIST == tmp_ret) {
        is_hot_ = true;
      }
    }
  }
private:
  int64_t ns_id_;
  bool &is_hot_;
};

// Functor for atomic_refactored callback: add ns_ids into existing ObIvfHotNsIds
class ObAddNsIdsFunc
{
public:
  ObAddNsIdsFunc(const common::ObIArray<int64_t> &ns_ids, int &ret)
    : ns_ids_(ns_ids), ret_(ret) {}
  void operator()(common::hash::HashMapPair<common::ObTabletID, ObIvfHotNsIds*> &entry)
  {
    int &ret = ret_; // LOG_WARN macro requires 'ret' in scope
    if (OB_NOT_NULL(entry.second)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < ns_ids_.count(); ++i) {
        if (OB_FAIL(entry.second->ns_id_set_.set_refactored(ns_ids_.at(i)))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            SHARE_LOG(WARN, "fail to insert ns_id into hot set", KR(ret), K(i), K(ns_ids_.at(i)));
          }
        }
      }
    }
  }
private:
  const common::ObIArray<int64_t> &ns_ids_;
  int &ret_;
};

// Functor for read_atomic callback: collect all ns_ids from ObIvfHotNsIds into an array
class ObCollectNsIdsFunc
{
public:
  ObCollectNsIdsFunc(common::ObIArray<int64_t> &ns_ids, int &ret)
    : ns_ids_(ns_ids), ret_(ret) {}
  void operator()(const common::hash::HashMapPair<common::ObTabletID, ObIvfHotNsIds*> &entry)
  {
    int &ret = ret_; // LOG_WARN macro requires 'ret' in scope
    if (OB_NOT_NULL(entry.second)) {
      ObCollectNsIdSetFunc collect_fn(ns_ids_);
      if (OB_FAIL(entry.second->ns_id_set_.foreach_refactored(collect_fn))) {
        SHARE_LOG(WARN, "fail to iterate hot ns id set", KR(ret));
      }
    }
  }
private:
  // Inner functor for HashSet::foreach_refactored
  class ObCollectNsIdSetFunc
  {
  public:
    explicit ObCollectNsIdSetFunc(common::ObIArray<int64_t> &ns_ids) : ns_ids_(ns_ids) {}
    int operator()(const common::hash::HashSetTypes<int64_t>::pair_type &pair)
    {
      int ret = common::OB_SUCCESS;
      if (OB_FAIL(ns_ids_.push_back(pair.first))) {
        SHARE_LOG(WARN, "fail to push back hot ns id", KR(ret));
      }
      return ret;
    }
  private:
    common::ObIArray<int64_t> &ns_ids_;
  };

  common::ObIArray<int64_t> &ns_ids_;
  int &ret_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_OB_VEC_LOGIC_TABLE_UTIL_H_
