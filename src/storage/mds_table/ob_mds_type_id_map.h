/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TYPE_ID_MAP_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TYPE_ID_MAP_H

#include <type_traits>
#include "lib/container/ob_tuple.h"

// mds_register.h entries reference concrete key/value types.
// Enable the define block so those types are visible while building the type-id map.
#define NEED_MDS_REGISTER_DEFINE
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef NEED_MDS_REGISTER_DEFINE

namespace oceanbase
{
namespace storage
{
namespace mds_type_id_detail
{

template <typename K, typename V>
struct TypeTag
{};

template <typename FIRST, typename ...Args>
struct DropFirstElemtTuple
{
  typedef common::ObTuple<Args...> type;
};

typedef DropFirstElemtTuple<char
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
  ,TypeTag<KEY_TYPE, VALUE_TYPE>
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE
>::type RegisteredMdsKvTuple;

template <typename K, typename V>
struct TypeIdOf
{
  static constexpr uint16_t value = static_cast<uint16_t>(
      RegisteredMdsKvTuple::template get_element_index<
          TypeTag<typename std::decay<K>::type, typename std::decay<V>::type>>());
};

template <typename K, typename V>
constexpr uint16_t get_mds_type_id()
{
  return TypeIdOf<K, V>::value;
}

} // namespace mds_type_id_detail
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TYPE_ID_MAP_H
