/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_SSLOG_DEFINE
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_SSLOG_DEFINE

#include <cstdint>
#include "share/ob_ls_id.h"
#include "deps/oblib/src/common/ob_tablet_id.h"
#include "share/scn.h"
#include "deps/oblib/src/lib/oblog/ob_log_module.h"
#include "share/ob_occam_time_guard.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace sslog
{

#define SSLOG_TG(timeout) TIMEGUARD_INIT(SSLOG, timeout)

// IMPORTANT: When adding a new meta type, you MUST add it here with the should_backup flag.
// X(name, value, should_backup)
//   - name: the enum name of the meta type
//   - value: the numeric value (must be unique and sequential)
//   - should_backup: bool flag indicating whether this meta type should be backed up
#define OB_SSLOG_META_TYPE_DEF(X)                            \
  X(SSLOG_INVALID_META,            0,  false)                \
  X(SSLOG_LS_META,                 1,  true)                 \
  X(SSLOG_TABLET_META,             2,  true)                 \
  X(SSLOG_MINI_SSTABLE,            3,  true)                 \
  X(SSLOG_MINOR_SSTABLE,           4,  true)                 \
  X(SSLOG_FLUSH_MINI_SSTABLE,      5,  true) /*FARM COMPAT WHITELIST*/  \
  X(SSLOG_SPLIT_MINOR_SSTABLE,     6,  true)                 \
  X(SSLOG_MAJOR_SSTABLE,           7,  true)                 \
  X(SSLOG_TABLET_TABLESTORE,       8,  false)                \
  X(SSLOG_TABLET_STORAGE_SCHEMA,   9,  false)                \
  X(SSLOG_TABLET_SSTABLE,          10, false)                \
  X(SSLOG_TABLET_SSTABLE_FOR_DDL,  11, false)                \
  X(SSLOG_TENANT_ID_SET,           12, false)                 \
  X(SSLOG_UPLOAD_MINOR_SSTABLE,    13, true)                 \
  X(SSLOG_MAX_META,                14, false)

#define GENERATE_SSLOG_META_TYPE_ENUM_(name, value, can_backup) name = value,
enum class ObSSLogMetaType : int64_t //FARM COMPAT WHITELIST
{
  OB_SSLOG_META_TYPE_DEF(GENERATE_SSLOG_META_TYPE_ENUM_)
};
#undef GENERATE_SSLOG_META_TYPE_ENUM_

#define GENERATE_TO_STRING_ENTRY_(name, value, can_backup) #name,
constexpr const char *OB_SSLOG_META_TYPE_NAMES_[] = {
  OB_SSLOG_META_TYPE_DEF(GENERATE_TO_STRING_ENTRY_)
};
#undef GENERATE_TO_STRING_ENTRY_

inline const char *obj_to_string(ObSSLogMetaType meta_type) {
  const char *ret = "UNKNOWN";
  const int64_t idx = static_cast<int64_t>(meta_type);
  if (idx >= 0 && idx < static_cast<int64_t>(sizeof(OB_SSLOG_META_TYPE_NAMES_) / sizeof(const char *))) {
    ret = OB_SSLOG_META_TYPE_NAMES_[idx];
  }
  return ret;
}


struct ObSSLogWriteParam
{
public:
  ObSSLogWriteParam(const bool only_update_extra_info,
                    const bool check_extra_info,
                    const ObString &extra_info);

  TO_STRING_KV(K_(check_extra_info), K_(check_extra_info), K_(extra_info));
  bool only_update_extra_info_;
  bool check_extra_info_;
  const ObString extra_info_;
};

inline bool is_valid_sslog_meta_type(const ObSSLogMetaType meta_type)
{
  return meta_type > ObSSLogMetaType::SSLOG_INVALID_META
    && meta_type < ObSSLogMetaType::SSLOG_MAX_META;
}

// reuse buf means try to reuse output_str's buf
// when buf not enough would free old buf and alloc new buf
int convert_cstr_to_hex(const ObString &input_str,
                        const bool reuse_buf,
                        ObString &output_str);
// reuse buf means try to reuse output_str's buf
// when buf not enough would free old buf and alloc new buf
int convert_hex_to_cstr(const ObString &input_str,
                        const bool reuse_buf,
                        ObString &output_str);

OB_INLINE uint64_t gen_sslog_tenant_id(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  uint64_t new_tenant_id = OB_INVALID_TENANT_ID;
  if (is_virtual_tenant_id(tenant_id) || !ls_id.is_valid()) {
    new_tenant_id = OB_INVALID_TENANT_ID; //invalid
  } else {
    new_tenant_id = gen_meta_tenant_id(tenant_id);
  }
  return new_tenant_id;
}

} // namespace sslog
} // namespace oceanbase

#endif // OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SSLOG_SSLOG_DEFINE
