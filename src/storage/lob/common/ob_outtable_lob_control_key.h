/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_KEY_H_
#define OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_KEY_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "storage/blocksstable/ob_macro_block_id.h"

namespace oceanbase
{
namespace storage
{

struct ObOuttableLobControlKey final
{
  ObOuttableLobControlKey() : macro_id_(), hash_(0) {}

  void reset()
  {
    macro_id_.reset();
    hash_ = 0;
  }

  bool is_valid() const { return macro_id_.is_valid(); }

  TO_STRING_KV(K_(macro_id), K_(hash));

  blocksstable::MacroBlockId macro_id_;
  uint64_t hash_;
  OB_UNIS_VERSION(1);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_LOB_OB_OUTTABLE_LOB_CONTROL_KEY_H_
