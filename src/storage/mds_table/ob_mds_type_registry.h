/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TYPE_REGISTRY_H
#define OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TYPE_REGISTRY_H

#include <type_traits>
#include <utility>
#include "lib/ob_errno.h"
#include "share/scn.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}
namespace storage
{

template <typename V, typename = void>
struct ObMdsUserDataHasAssignAllocFirst : std::false_type
{};

/// `int assign(ObIAllocator &alloc, const V &other);` — allocator is the first parameter.
template <typename V>
struct ObMdsUserDataHasAssignAllocFirst<
    V,
    std::void_t<decltype(std::declval<V &>().assign(std::declval<common::ObIAllocator &>(),
                                                    std::declval<const V &>()))>> : std::true_type
{};

template <typename V, typename = void>
struct ObMdsUserDataHasAssignAllocSecond : std::false_type
{};

/// `int assign(const V &other, ObIAllocator &alloc);` — allocator is the second parameter.
template <typename V>
struct ObMdsUserDataHasAssignAllocSecond<
    V,
    std::void_t<decltype(std::declval<V &>().assign(std::declval<const V &>(),
                                                    std::declval<common::ObIAllocator &>()))>> : std::true_type
{};

template <typename V, typename = void>
struct ObMdsUserDataHasAssign : std::false_type
{};

template <typename V>
struct ObMdsUserDataHasAssign<V, std::void_t<decltype(std::declval<V &>().assign(std::declval<const V &>()))>>
  : std::true_type
{};

template <typename V, typename = void>
struct ObMdsUserDataHasOnCommit : std::false_type
{};

template <typename V>
struct ObMdsUserDataHasOnCommit<
    V,
    std::void_t<decltype(std::declval<V &>().on_commit(std::declval<const share::SCN &>(),
                                                       std::declval<const share::SCN &>()))>> : std::true_type
{};

template <typename V, typename = void>
struct ObMdsUserDataHasOnRedo : std::false_type
{};

template <typename V>
struct ObMdsUserDataHasOnRedo<
    V,
    std::void_t<decltype(std::declval<V &>().on_redo(std::declval<const share::SCN &>()))>> : std::true_type
{};

template <typename V, typename = void>
struct ObMdsUserDataDeserializeNeedsAlloc : std::false_type
{};

template <typename V>
struct ObMdsUserDataDeserializeNeedsAlloc<
    V,
    std::void_t<decltype(std::declval<V &>().deserialize(std::declval<common::ObIAllocator &>(),
                                                         std::declval<const char *>(),
                                                         std::declval<int64_t>(),
                                                         std::declval<int64_t &>()))>> : std::true_type
{};

// Traits class that maps user-data type V (and key K for key_to_string) to the
// operations the MDS path needs. All pointers are bare V*; memtable layout is
// outside the registry.
template <typename K, typename V>
struct ObMdsTypeTraits
{
  static int64_t value_size()
  {
    return static_cast<int64_t>(sizeof(V));
  }

  static void construct(void *user_data_ptr)
  {
    ::new (user_data_ptr) V();
  }

  static void destruct(void *user_data_ptr)
  {
    static_cast<V *>(user_data_ptr)->~V();
  }

  static int serialize(const void *user_data_ptr, char *buf, const int64_t buf_len, int64_t &pos)
  {
    return static_cast<const V *>(user_data_ptr)->serialize(buf, buf_len, pos);
  }

  static int64_t get_serialize_size(const void *user_data_ptr)
  {
    return static_cast<const V *>(user_data_ptr)->get_serialize_size();
  }

  static int deserialize(void *user_data_ptr,
                         const char *buf,
                         const int64_t data_len,
                         int64_t &pos,
                         common::ObIAllocator *alloc)
  {
    V *const value = static_cast<V *>(user_data_ptr);
    if constexpr (ObMdsUserDataDeserializeNeedsAlloc<V>::value) {
      if (OB_UNLIKELY(nullptr == alloc)) {
        return OB_INVALID_ARGUMENT;
      }
      return value->deserialize(*alloc, buf, data_len, pos);
    } else {
      return value->deserialize(buf, data_len, pos);
    }
  }

  static void on_redo(void *user_data_ptr, const share::SCN &redo_scn)
  {
    if constexpr (ObMdsUserDataHasOnRedo<V>::value) {
      static_cast<V *>(user_data_ptr)->on_redo(redo_scn);
    }
  }

  static void on_commit(void *user_data_ptr,
                        const share::SCN &commit_version,
                        const share::SCN &commit_scn)
  {
    if constexpr (ObMdsUserDataHasOnCommit<V>::value) {
      static_cast<V *>(user_data_ptr)->on_commit(commit_version, commit_scn);
    }
  }

  // Copy src V into an already-constructed dst V. Called by build_mds_tx_node_
  // right after construct so the live memtable object carries the write payload.
  static int copy_from(void *dst_user_data_ptr, const void *src, common::ObIAllocator *alloc)
  {
    int ret = OB_SUCCESS;
    V *const dst = static_cast<V *>(dst_user_data_ptr);
    const V *const from = static_cast<const V *>(src);
    if (OB_UNLIKELY(nullptr == dst_user_data_ptr || nullptr == src)) {
      ret = OB_INVALID_ARGUMENT;
      MDS_LOG(ERROR, "mds copy_from with null argument", K(ret), KP(dst_user_data_ptr), KP(src));
    } else if constexpr (ObMdsUserDataHasAssignAllocFirst<V>::value) {
      if (OB_UNLIKELY(nullptr == alloc)) {
        ret = OB_INVALID_ARGUMENT;
        MDS_LOG(ERROR, "mds copy_from needs allocator for this V", K(ret), KP(alloc));
      } else if (OB_FAIL(dst->assign(*alloc, *from))) {
        MDS_LOG(WARN, "mds copy_from assign(allocator first) failed", K(ret));
      }
    } else if constexpr (ObMdsUserDataHasAssignAllocSecond<V>::value) {
      if (OB_UNLIKELY(nullptr == alloc)) {
        ret = OB_INVALID_ARGUMENT;
        MDS_LOG(ERROR, "mds copy_from needs allocator for this V", K(ret), KP(alloc));
      } else if (OB_FAIL(dst->assign(*from, *alloc))) {
        MDS_LOG(WARN, "mds copy_from assign(allocator second) failed", K(ret));
      }
    } else if constexpr (ObMdsUserDataHasAssign<V>::value) {
      if (OB_FAIL(dst->assign(*from))) {
        MDS_LOG(WARN, "mds copy_from assign failed", K(ret));
      }
    } else if constexpr (std::is_copy_assignable<V>::value) {
      *dst = *from;
    } else {
      dst->~V();
      ::new (static_cast<void *>(dst)) V(*from);
    }
    return ret;
  }

  static int64_t key_to_string(const common::ObString &key_buf, char *str_buf, const int64_t str_buf_len)
  {
    K key;
    int64_t pos = 0;
    if (OB_SUCCESS != key.mds_deserialize(key_buf.ptr(), key_buf.length(), pos)) {
      return 0;
    }
    return key.to_string(str_buf, str_buf_len);
  }

  static int64_t value_to_string(const void *user_data_ptr, char *buf, const int64_t buf_len)
  {
    return static_cast<const V *>(user_data_ptr)->to_string(buf, buf_len);
  }
};

// ObMdsTypeRegistry is a type-erased dispatch table keyed by mds_type
// (LsInnerMdsTable unit id generated from ObTuple / TupleTypeIdx). All MDS user_data types
// register their operations at observer startup via ObMdsTypeRegistry::init_all_mds_types(),
// which expands LsInnerMdsTable tuple units from mds_register.h and calls register_type<K, V>
// per (KEY_TYPE, VALUE_TYPE). Value ops always take bare V*.
//
// Lifecycle: singleton, populated once during ObServer::init before any MDS
// write is accepted. Registration is expected to be single-threaded at
// startup; lookups after that are lock-free reads.
class ObMdsTypeRegistry
{
public:
  // Function pointers describing bare V lifecycle. A slot whose value_size == 0
  // is unregistered.
  struct TypeOps
  {
    int64_t value_size;
    void    (*construct)(void *user_data_ptr);
    void    (*destruct)(void *user_data_ptr);
    int     (*serialize)(const void *user_data_ptr, char *buf, const int64_t buf_len, int64_t &pos);
    int64_t (*get_serialize_size)(const void *user_data_ptr);
    int     (*deserialize)(void *user_data_ptr,
                           const char *buf,
                           const int64_t data_len,
                           int64_t &pos,
                           common::ObIAllocator *alloc);
    // CONSTRAINT on V::on_redo / V::on_commit implementations:
    // they are invoked as IN-PLACE patches on the live V object embedded in a
    // memtable trans node, inside ObMdsValueMeta's patch-seqlock write section
    // (ObMvccRowCallback::invoke_mds_on_redo / invoke_mds_on_commit), while
    // lock-free readers may be serializing the same object concurrently
    // (ObMdsTableDatumRowConverter::convert_memtable_row). A reader detects a
    // torn read only AFTER it finishes, via seq validation, so the patch must
    // be safe to observe mid-flight:
    //   - only in-place scalar field stores (SCN / int64 etc.);
    //   - no memory (re)allocation, no pointer / buffer swaps (a swung
    //     pointer could crash a reader before validation runs);
    //   - no failure path (the write section must always reach
    //     end_value_patch).
    void    (*on_redo)(void *user_data_ptr, const share::SCN &redo_scn);
    void    (*on_commit)(void *user_data_ptr, const share::SCN &commit_version, const share::SCN &commit_scn);
    // Set at register_type time from ObMdsUserDataHasOnCommit / HasOnRedo on V.
    bool has_on_commit;
    bool has_on_redo;
    // Copy src V into an already-constructed dst V.
    int     (*copy_from)(void *dst_user_data_ptr, const void *src, common::ObIAllocator *alloc);
    int64_t (*key_to_string)(const common::ObString &key_buf, char *str_buf, const int64_t str_buf_len);
    int64_t (*value_to_string)(const void *user_data_ptr, char *buf, const int64_t buf_len);

    TypeOps()
      : value_size(0),
        construct(nullptr),
        destruct(nullptr),
        serialize(nullptr),
        get_serialize_size(nullptr),
        deserialize(nullptr),
        on_redo(nullptr),
        on_commit(nullptr),
        has_on_commit(false),
        has_on_redo(false),
        copy_from(nullptr),
        key_to_string(nullptr),
        value_to_string(nullptr)
    {}

    bool is_valid() const { return value_size > 0 && construct != nullptr; }
  };

  // Max mds_type value. LsInnerMdsTable unit ids are currently uint8-like and
  // compact; keep a larger bound for future extension without resize.
  static constexpr int64_t MAX_MDS_TYPE = 256;

  static ObMdsTypeRegistry &get_instance();

  // Register default ops for <K,V> at given mds_type.
  template <typename K, typename V>
  int register_type(const uint16_t mds_type)
  {
    TypeOps ops;
    ops.value_size          = ObMdsTypeTraits<K, V>::value_size();
    ops.construct           = &ObMdsTypeTraits<K, V>::construct;
    ops.destruct            = &ObMdsTypeTraits<K, V>::destruct;
    ops.serialize           = &ObMdsTypeTraits<K, V>::serialize;
    ops.get_serialize_size  = &ObMdsTypeTraits<K, V>::get_serialize_size;
    ops.deserialize         = &ObMdsTypeTraits<K, V>::deserialize;
    ops.on_redo             = &ObMdsTypeTraits<K, V>::on_redo;
    ops.on_commit           = &ObMdsTypeTraits<K, V>::on_commit;
    ops.has_on_commit       = ObMdsUserDataHasOnCommit<V>::value;
    ops.has_on_redo         = ObMdsUserDataHasOnRedo<V>::value;
    ops.copy_from           = &ObMdsTypeTraits<K, V>::copy_from;
    ops.key_to_string       = &ObMdsTypeTraits<K, V>::key_to_string;
    ops.value_to_string     = &ObMdsTypeTraits<K, V>::value_to_string;
    return register_ops_(mds_type, ops);
  }

  // Allow callers to install a custom op-set when V does not satisfy the
  // default traits. Rarely needed; most types go through register_type<K, V>.
  int register_ops(const uint16_t mds_type, const TypeOps &ops)
  {
    return register_ops_(mds_type, ops);
  }

  // Register all tablet-bound MDS value types at observer startup.
  // Must be called once, before any MDS write is accepted.
  static int init_all_mds_types();

  // Look up the dispatch table. Returns nullptr for unregistered mds_type.
  const TypeOps *get_ops(const uint16_t mds_type) const;

  // Whether the registered V type defines on_commit / on_redo. False for unregistered mds_type.
  bool has_on_commit(const uint16_t mds_type) const;
  bool has_on_redo(const uint16_t mds_type) const;

  // Convenience dispatchers over get_ops. These log and return OB_ERR_UNEXPECTED
  // on unregistered mds_type; they never abort, so callers must check ret.
  void invoke_destruct(const uint16_t mds_type, void *buf) const;
  int  invoke_on_redo(const uint16_t mds_type,
                        void *buf,
                        const share::SCN &redo_scn) const;
  int  invoke_on_commit(const uint16_t mds_type,
                        void *buf,
                        const share::SCN &commit_version,
                        const share::SCN &commit_scn) const;
  int  invoke_serialize(const uint16_t mds_type,
                        const void *buf,
                        char *out_buf,
                        const int64_t out_buf_len,
                        int64_t &pos) const;
  int  invoke_get_serialize_size(const uint16_t mds_type,
                                 const void *buf,
                                 int64_t &size) const;
  int  invoke_deserialize(const uint16_t mds_type,
                          void *buf,
                          const char *in_buf,
                          const int64_t in_buf_len,
                          int64_t &pos,
                          common::ObIAllocator *alloc = nullptr) const;
  int  invoke_construct(const uint16_t mds_type, void *buf) const;

private:
  ObMdsTypeRegistry();
  ~ObMdsTypeRegistry() = default;
  ObMdsTypeRegistry(const ObMdsTypeRegistry &) = delete;
  ObMdsTypeRegistry &operator=(const ObMdsTypeRegistry &) = delete;

  int register_ops_(const uint16_t mds_type, const TypeOps &ops);

  TypeOps ops_table_[MAX_MDS_TYPE];
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_MDS_TABLE_OB_MDS_TYPE_REGISTRY_H
