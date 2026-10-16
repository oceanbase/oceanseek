/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_DEFINE
#define OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_DEFINE

#include <cstdint>
#include "lib/string/ob_fixed_length_string.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{

static constexpr int64_t SHARED_META_RESERVED_OWNER_MAX_LENGTH = 1024;
using ObSharedMetaReservedOwner =
    common::ObFixedLengthString<SHARED_META_RESERVED_OWNER_MAX_LENGTH + 1>;

// X(name, value, key_mode)
//   - name:     the enum name of the reserved type
//   - value:    the numeric value (must be unique and sequential)
//   - key_mode: how callers address the reservation row
//
// IMPORTANT: When adding a new reserved type, add it here. The string form
// stored in the `type` column of __all_shared_meta_reserved_scn is the enum
// name produced by shared_meta_reserved_type_to_string(); ObSharedMetaReservedType
// uses this list as the single source of truth for its name table and for
// parse_from_str().
//
// MAX_RESERVED is a per-tenant control row holding the high water mark of all
// reserved scns. It is monotonically non-decreasing: only create_reserved_scn()
// pushes it up (and skips when the new scn is not larger). remove_reserved_scn()
// never touches it, so the high water mark never retreats.
enum class ObSharedMetaReservedKeyMode : int64_t
{
  NONE = 0,
  SCN_KEYED = 1,
  OWNER_KEYED = 2,
};

#define OB_SHARED_META_RESERVED_TYPE_DEF(X)                              \
  X(INVALID, 0, ObSharedMetaReservedKeyMode::NONE)                       \
  X(MAX_RESERVED, 1, ObSharedMetaReservedKeyMode::NONE)                \
  X(BACKUP,  2, ObSharedMetaReservedKeyMode::OWNER_KEYED)              \
  X(SNAPSHOT,3, ObSharedMetaReservedKeyMode::SCN_KEYED)                \
  X(GC,      4, ObSharedMetaReservedKeyMode::SCN_KEYED)                \
  X(LS_CKPT, 5, ObSharedMetaReservedKeyMode::SCN_KEYED)               \
  X(FORK_TABLE, 6, ObSharedMetaReservedKeyMode::OWNER_KEYED)           \
  X(DDL,     7, ObSharedMetaReservedKeyMode::OWNER_KEYED)              \
  X(DATA_CHECK, 8, ObSharedMetaReservedKeyMode::OWNER_KEYED)           \
  X(MAX,     9, ObSharedMetaReservedKeyMode::NONE)

#define GENERATE_SHARED_META_RESERVED_ENUM_(name, value, key_mode) name = value,
enum class ObSharedMetaReservedTypeEnum : int64_t
{
  OB_SHARED_META_RESERVED_TYPE_DEF(GENERATE_SHARED_META_RESERVED_ENUM_)
};
#undef GENERATE_SHARED_META_RESERVED_ENUM_

#define GENERATE_SHARED_META_RESERVED_NAME_(name, value, key_mode) #name,
constexpr const char *OB_SHARED_META_RESERVED_TYPE_NAMES_[] = {
  OB_SHARED_META_RESERVED_TYPE_DEF(GENERATE_SHARED_META_RESERVED_NAME_)
};
#undef GENERATE_SHARED_META_RESERVED_NAME_

#define GENERATE_SHARED_META_RESERVED_KEY_MODE_(name, value, key_mode) key_mode,
constexpr ObSharedMetaReservedKeyMode OB_SHARED_META_RESERVED_TYPE_KEY_MODES_[] = {
  OB_SHARED_META_RESERVED_TYPE_DEF(GENERATE_SHARED_META_RESERVED_KEY_MODE_)
};
#undef GENERATE_SHARED_META_RESERVED_KEY_MODE_

inline ObSharedMetaReservedKeyMode shared_meta_reserved_type_key_mode(
    const ObSharedMetaReservedTypeEnum type)
{
  ObSharedMetaReservedKeyMode key_mode = ObSharedMetaReservedKeyMode::NONE;
  const int64_t idx = static_cast<int64_t>(type);
  if (idx >= 0
      && idx < static_cast<int64_t>(sizeof(OB_SHARED_META_RESERVED_TYPE_KEY_MODES_)
                                    / sizeof(ObSharedMetaReservedKeyMode))) {
    key_mode = OB_SHARED_META_RESERVED_TYPE_KEY_MODES_[idx];
  }
  return key_mode;
}

inline const char *shared_meta_reserved_type_to_string(ObSharedMetaReservedTypeEnum type)
{
  const char *ret = "UNKNOWN";
  const int64_t idx = static_cast<int64_t>(type);
  if (idx >= 0
      && idx < static_cast<int64_t>(sizeof(OB_SHARED_META_RESERVED_TYPE_NAMES_)
                                    / sizeof(const char *))) {
    ret = OB_SHARED_META_RESERVED_TYPE_NAMES_[idx];
  }
  return ret;
}

// Type column of __all_shared_meta_reserved_scn. Wraps the underlying enum so
// callers get is_valid()/hash()/to_string() with the same shape as the rest
// of the shared-storage type wrappers.
struct ObSharedMetaReservedType final
{
public:
  ObSharedMetaReservedType();
  explicit ObSharedMetaReservedType(const ObSharedMetaReservedTypeEnum type);
  ~ObSharedMetaReservedType() = default;

  void reset();
  bool is_valid() const;
  int64_t to_string(
      char *buf,
      const int64_t buf_len) const;

  bool is_max_reserved() const { return ObSharedMetaReservedTypeEnum::MAX_RESERVED == type_; }
  bool is_backup() const   { return ObSharedMetaReservedTypeEnum::BACKUP == type_; }
  bool is_snapshot() const { return ObSharedMetaReservedTypeEnum::SNAPSHOT == type_; }
  bool is_gc() const       { return ObSharedMetaReservedTypeEnum::GC == type_; }
  bool is_ls_ckpt() const  { return ObSharedMetaReservedTypeEnum::LS_CKPT == type_; }
  bool is_fork_table() const { return ObSharedMetaReservedTypeEnum::FORK_TABLE == type_; }
  bool is_ddl() const { return ObSharedMetaReservedTypeEnum::DDL == type_; }
  bool is_data_check() const { return ObSharedMetaReservedTypeEnum::DATA_CHECK == type_; }
  ObSharedMetaReservedKeyMode key_mode() const
  {
    return shared_meta_reserved_type_key_mode(type_);
  }
  bool is_scn_keyed() const
  {
    return ObSharedMetaReservedKeyMode::SCN_KEYED == key_mode();
  }
  bool is_owner_keyed() const
  {
    return ObSharedMetaReservedKeyMode::OWNER_KEYED == key_mode();
  }

  // Const-string form of `type_`, suitable for direct use in SQL (the value
  // stored in the VARCHAR(128) `type` column of __all_shared_meta_reserved_scn).
  // Returns "UNKNOWN" if `type_` is out of range.
  const char *to_str() const { return shared_meta_reserved_type_to_string(type_); }
  // Inverse of to_str(); resolves a VARCHAR(128) column value back to the
  // corresponding type. Returns OB_INVALID_ARGUMENT when `str` does not name
  // any ObSharedMetaReservedTypeEnum value, or names INVALID / MAX.
  static int parse_from_str(
      const common::ObString &str,
      ObSharedMetaReservedType &type);

  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;
  bool operator==(const ObSharedMetaReservedType &other) const { return type_ == other.type_; }
  bool operator!=(const ObSharedMetaReservedType &other) const { return !(*this == other); }

  ObSharedMetaReservedTypeEnum type_;
};

// One row of __all_shared_meta_reserved_scn (table_id = 602).
//
// Each row is a *placeholder* asserting that the SCN `meta_scn_` for tenant
// `tenant_id_` and reserved kind `type_` must NOT be GC'd. Multiple rows may
// share the same (tenant_id_, type_); the effective reserved SCN for that
// (tenant, type) is the MIN of all `meta_scn_` rows. A reservation is
// released by deleting its row.
//
// Column mapping:
//   tenant_id  (int,         rowkey 1) <- tenant_id_
//   type       (varchar 128, rowkey 2) <- shared_meta_reserved_type_to_string(type_.type_)
//   meta_scn   (int,         rowkey 3) <- meta_scn_.get_val_for_tx()
//   owner      (varbinary 1024)         <- owner_.str(); empty means SQL NULL
struct ObSharedMetaReservedInfo final
{
public:
  ObSharedMetaReservedInfo();
  ObSharedMetaReservedInfo(
      const uint64_t tenant_id,
      const ObSharedMetaReservedType &type,
      const share::SCN &meta_scn);
  ~ObSharedMetaReservedInfo() = default;

  void reset();
  bool is_valid() const;
  int set_owner(const common::ObString &owner);
  bool has_owner() const { return !owner_.is_empty(); }
  common::ObString get_owner() const { return owner_.str(); }
  int assign(const ObSharedMetaReservedInfo &other);
  bool operator==(const ObSharedMetaReservedInfo &other) const;
  bool operator!=(const ObSharedMetaReservedInfo &other) const;

  int hash(uint64_t &hash_val) const;
  uint64_t hash() const;

  TO_STRING_KV(K_(tenant_id), K_(type), K_(meta_scn), K_(owner));

  uint64_t tenant_id_;
  ObSharedMetaReservedType type_;
  share::SCN meta_scn_;
  ObSharedMetaReservedOwner owner_;
};

// Shared helpers for the __all_shared_meta_reserved_scn table.
class ObSharedMetaReservedUtil final
{
public:
  // Map `tenant_id` to the tenant that physically stores
  // __all_shared_meta_reserved_scn (where its trans runs and its table lock is
  // taken):
  //   user tenant -> its meta tenant
  //   meta tenant -> SYS tenant
  //   sys  tenant -> itself
  // Returns OB_INVALID_ARGUMENT for an invalid tenant id, and OB_ERR_UNEXPECTED
  // for any other (e.g. virtual) tenant type. All callers of this table must
  // route through here so the trans tenant and the lock tenant always agree.
  static int get_real_tenant_id(
      const uint64_t tenant_id,
      uint64_t &real_tenant_id);
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_RESERVE_SHARED_META_OB_SHARED_META_RESERVED_DEFINE
