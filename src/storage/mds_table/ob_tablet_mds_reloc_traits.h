/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_RELOC_TRAITS_H_
#define OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_RELOC_TRAITS_H_

#include <type_traits>

#include "storage/multi_data_source/compile_utility/mds_dummy_key.h"

namespace oceanbase
{
namespace storage
{

// This trait is applied only after an MDS unit has been selected by the reloc
// registry flags.  Within that set, key shape is the structural source of truth
// for single-row versus multi-row semantics: a multi-key unit is relocated by
// operation in V2, while a DummyKey unit is carried as a materialized row.
template <typename K>
struct ObIsRelocOpType
  : std::integral_constant<bool, !std::is_same<K, mds::DummyKey>::value> {};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_TABLET_MDS_RELOC_TRAITS_H_
