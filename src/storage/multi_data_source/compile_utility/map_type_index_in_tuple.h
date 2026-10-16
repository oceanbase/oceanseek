/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
 
#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MAP_TYPE_INDEX_IN_TUPLE_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MAP_TYPE_INDEX_IN_TUPLE_H

#define NEED_MDS_REGISTER_DEFINE
#include "mds_register.h"
#undef NEED_MDS_REGISTER_DEFINE
#include "lib/container/ob_tuple.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

template <typename K, typename V, bool MULTI_VERSION>
struct TypeHelper {};

template <typename FIRST, typename ...Args>
struct DropFirstElemtTuple { typedef common::ObTuple<Args...> type; };

#define NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
#define _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME) \
,BUFFER_CTX_TYPE
typedef DropFirstElemtTuple<char
#include "mds_register.h"
>::type BufferCtxTupleHelper;
#undef _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_
#undef NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION

}
}
}
#endif