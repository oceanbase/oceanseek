/**
 * Copyright (C) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_GLOBAL_MACRO_BLOCK_KVCACHE_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_SS_GLOBAL_MACRO_BLOCK_KVCACHE_H_

#include "share/cache/ob_kv_storecache.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

class ObSSMacroBlockCacheKey final : public common::ObIKVCacheKey
{
public:
  ObSSMacroBlockCacheKey();
  ObSSMacroBlockCacheKey(const uint64_t tenant_id,
                         const blocksstable::MacroBlockId &macro_id);
  virtual ~ObSSMacroBlockCacheKey() = default;

  virtual bool operator==(const common::ObIKVCacheKey &other) const override;
  virtual uint64_t get_tenant_id() const override { return tenant_id_; }
  virtual uint64_t hash() const override;
  virtual int64_t size() const override { return sizeof(*this); }
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        common::ObIKVCacheKey *&key) const override;
  bool is_valid() const;
  TO_STRING_KV(K_(tenant_id), K_(macro_id));

private:
  uint64_t tenant_id_;
  blocksstable::MacroBlockId macro_id_;
};

class ObSSMacroBlockCacheValue final : public common::ObIKVCacheValue
{
public:
  ObSSMacroBlockCacheValue();
  ObSSMacroBlockCacheValue(const uint64_t effective_tablet_id,
                           const char *data,
                           const int64_t data_size,
                           const uint32_t crc);
  virtual ~ObSSMacroBlockCacheValue() = default;

  virtual int64_t size() const override;
  virtual int deep_copy(char *buf,
                        const int64_t buf_len,
                        common::ObIKVCacheValue *&value) const override;
  bool is_valid() const;
  int check_crc() const;
  TO_STRING_KV(K_(effective_tablet_id), K_(data_size), K_(crc), KP_(data));

public:
  uint64_t effective_tablet_id_;
  int32_t data_size_;
  uint32_t crc_;
  const char *data_;
};

class ObSSGlobalMacroBlockKVCache final
  : public common::ObKVCache<ObSSMacroBlockCacheKey,
                             ObSSMacroBlockCacheValue>
{
public:
  typedef common::ObKVCache<ObSSMacroBlockCacheKey,
                            ObSSMacroBlockCacheValue> BaseKVCache;
  // 通用 KVCACHE 要求 priority > 0；1 表示最小合法淘汰权重。
  static const int64_t DEFAULT_PRIORITY = 1;

  static ObSSGlobalMacroBlockKVCache &get_instance();
  static const char *get_cache_name() { return "ss_macro_block_kvcache"; }

  int init();
  void destroy();
  bool is_inited() const { return is_inited_; }

  int erase_tenant(const uint64_t tenant_id);

private:
  ObSSGlobalMacroBlockKVCache();
  virtual ~ObSSGlobalMacroBlockKVCache();
  DISALLOW_COPY_AND_ASSIGN(ObSSGlobalMacroBlockKVCache);

private:
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase

#define OB_GLOBAL_MACRO_BLOCK_KVCACHE \
  (oceanbase::storage::ObSSGlobalMacroBlockKVCache::get_instance())

#endif // OCEANBASE_STORAGE_SHARED_STORAGE_MACRO_CACHE_MACRO_BLOCK_KVCACHE_OB_GLOBAL_MACRO_BLOCK_KVCACHE_H_
