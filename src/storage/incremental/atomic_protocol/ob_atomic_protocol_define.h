/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ATOMIC_PROTOCOL_DEFINE_
#define OCEANBASE_ATOMIC_PROTOCOL_DEFINE_

#include <string.h>
#include "lib/container/ob_se_array.h"
#include "lib/string/ob_string_holder.h"
#include "lib/utility/ob_macro_utils.h"                 // DISALLOW_COPY_AND_ASSIGN
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/incremental/ss_meta_ctrl/ob_ss_meta_ctrl_define.h"


namespace oceanbase
{
namespace storage
{

#define REGISTER_ATOMIC_PROTOCOL(TYPE_NAME, ID, TYPE_LEVEL, SSWRITER_TYPE, ...)
#define NEED_REGISTER_ATOMIC_PROTOCOL
  #include "storage/incremental/atomic_protocol/compile_utility/ob_atomic_protocol_register.h"
#undef NEED_REGISTER_ATOMIC_PROTOCOL

#undef REGISTER_ATOMIC_PROTOCOL

enum class ObAtomicProtocolTypeEnum : int32_t
{
  INVALID_TYPE = 0,
#define REGISTER_ATOMIC_PROTOCOL(TYPE_NAME, ID, ...) \
  TYPE_NAME = ID,

#define NEED_REGISTER_ATOMIC_PROTOCOL
  #include "storage/incremental/atomic_protocol/compile_utility/ob_atomic_protocol_register.h"
#undef NEED_REGISTER_ATOMIC_PROTOCOL

#undef REGISTER_ATOMIC_PROTOCOL
  MAX_TYPE
};

// Protocol-semantic level. Mirrors the TYPE_LEVEL column in
// compile_utility/ob_atomic_protocol_register.h:
//   LS      -- LS-scoped (e.g. LS_META, CHECKPOINT)
//   TABLET  -- tablet-scoped, not an sstable (e.g. TABLET_META)
//   SSTABLE -- tablet-scoped sstable variants (MINI/MINOR/MAJOR)
//
// The enumerator names MUST match the TYPE_LEVEL token used in the register
// file verbatim, because get_level() uses macro concatenation
// (ObAtomicProtocolLevel::TYPE_LEVEL) to map enum -> level. Adding a new
// TYPE_LEVEL value in the register file therefore requires adding a matching
// enumerator here.
enum class ObAtomicProtocolLevel : int32_t
{
  INVALID = 0,
  LS = 1,
  TABLET = 2,
  SSTABLE = 3,
};

struct ObAtomicProtocolType final
{
public:
  ObAtomicProtocolType();
  ~ObAtomicProtocolType() = default;
  void reset();
  bool is_valid() const;
  int get_ss_meta_ctrl_type(ObSSMetaCtrlType &ctrl_type);
  // Classify the protocol type into LS / TABLET / SSTABLE. The mapping is
  // generated at compile time from the TYPE_LEVEL column in
  // ob_atomic_protocol_register.h, so adding / changing a registered entry
  // automatically keeps this in sync.
  int get_level(ObAtomicProtocolLevel &level) const;
  bool is_ls_level() const;
  bool is_tablet_level() const;
  bool is_sstable_level() const;
  TO_STRING_KV(K_(type));

  ObAtomicProtocolTypeEnum type_;
};

} // storage
} // oceanbase

#endif
