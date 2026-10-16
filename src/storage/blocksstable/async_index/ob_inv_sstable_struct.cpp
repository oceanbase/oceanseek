/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/blocksstable/async_index/ob_inv_sstable_struct.h"
#include "lib/utility/serialization.h"

namespace oceanbase
{
namespace blocksstable
{
using namespace common;

OB_SERIALIZE_MEMBER(ObInvIdxStatsMeta,
    index_type_,
    has_relevance_params_,
    has_position_list_,
    has_nested_key_,
    doc_id_obj_meta_,
    min_doc_id_,
    max_doc_id_,
    total_doc_length_);

} // namespace blocksstable
} // namespace oceanbase
