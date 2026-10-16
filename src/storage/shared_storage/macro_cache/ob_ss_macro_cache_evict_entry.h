/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_EVICT_ENTRY_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_EVICT_ENTRY_H_

#include "lib/queue/ob_link.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"


namespace oceanbase
{
namespace storage
{

struct ObSSMacroCacheEvictEntry : public common::ObLink
{
public:
  ObSSMacroCacheEvictEntry()
    : macro_id_(), cache_type_(ObSSMacroCacheType::MAX_TYPE), is_write_cache_(false)
  {}
  ObSSMacroCacheEvictEntry(const blocksstable::MacroBlockId macro_id,
                           const ObSSMacroCacheType cache_type,
                           const bool is_write_cache)
    : macro_id_(macro_id), cache_type_(cache_type), is_write_cache_(is_write_cache)
  {}
  virtual ~ObSSMacroCacheEvictEntry() {}
  OB_INLINE bool is_valid() const
  {
    return macro_id_.is_valid() && (ObSSMacroCacheType::MAX_TYPE != cache_type_);
  }
  TO_STRING_KV(K_(macro_id), K_SS_MACRO_CACHE_TYPE(cache_type_), K_(is_write_cache));

public:
  blocksstable::MacroBlockId macro_id_;
  ObSSMacroCacheType cache_type_;
  bool is_write_cache_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_OB_SS_MACRO_CACHE_EVICT_ENTRY_H_ */
