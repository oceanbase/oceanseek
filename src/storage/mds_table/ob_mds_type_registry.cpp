/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_mds_type_registry.h"
#include "storage/mds_table/ob_mds_type_id_map.h"
#include "storage/tablet/ob_tablet_binding_mds_user_data.h"
#include "storage/tablet/ob_tablet_split_mds_user_data.h"
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"

namespace oceanbase
{
namespace storage
{

ObMdsTypeRegistry::ObMdsTypeRegistry()
{
  // ops_table_ entries are value-initialized to the default TypeOps{},
  // so value_size == 0 marks unregistered slots.
}

ObMdsTypeRegistry &ObMdsTypeRegistry::get_instance()
{
  static ObMdsTypeRegistry instance;
  return instance;
}

int ObMdsTypeRegistry::register_ops_(const uint16_t mds_type, const TypeOps &ops)
{
  int ret = OB_SUCCESS;
  if (mds_type >= MAX_MDS_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("mds_type out of range", K(ret), K(mds_type), LITERAL_K(MAX_MDS_TYPE));
  } else if (!ops.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("ops is not valid", K(ret), K(mds_type), K(ops.value_size));
    ob_abort();
  } else {
    TypeOps &slot = ops_table_[mds_type];
    if (slot.is_valid()) {
      ret = OB_ENTRY_EXIST;
      LOG_ERROR("mds_type already registered with a different V", K(ret), K(mds_type));
      ob_abort();
    } else {
      slot = ops;
    }
  }
  return ret;
}

const ObMdsTypeRegistry::TypeOps *ObMdsTypeRegistry::get_ops(const uint16_t mds_type) const
{
  const TypeOps *result = nullptr;
  if (mds_type < MAX_MDS_TYPE && ops_table_[mds_type].is_valid()) {
    result = &ops_table_[mds_type];
  }
  return result;
}

bool ObMdsTypeRegistry::has_on_commit(const uint16_t mds_type) const
{
  const TypeOps *ops = get_ops(mds_type);
  return nullptr != ops && ops->has_on_commit;
}

bool ObMdsTypeRegistry::has_on_redo(const uint16_t mds_type) const
{
  const TypeOps *ops = get_ops(mds_type);
  return nullptr != ops && ops->has_on_redo;
}

void ObMdsTypeRegistry::invoke_destruct(const uint16_t mds_type, void *buf) const
{
  const TypeOps *ops = get_ops(mds_type);
  if (OB_ISNULL(ops)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "destruct unregistered mds_type", K(mds_type));
  } else if (OB_ISNULL(buf)) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "destruct null buf", K(mds_type));
  } else {
    ops->destruct(buf);
  }
}

int ObMdsTypeRegistry::invoke_on_redo(const uint16_t mds_type,
                                        void *buf,
                                        const share::SCN &redo_scn) const
{
  int ret = OB_SUCCESS;
  const TypeOps *ops = get_ops(mds_type);
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("on_redo for unregistered mds_type", K(ret), K(mds_type));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("on_redo with null buf", K(ret), K(mds_type));
  } else {
    ops->on_redo(buf, redo_scn);
  }
  LOG_INFO("invoke_on_redo done", K(ret), K(mds_type), KP(buf), K(redo_scn));
  return ret;
}

int ObMdsTypeRegistry::invoke_on_commit(const uint16_t mds_type,
                                        void *buf,
                                        const share::SCN &commit_version,
                                        const share::SCN &commit_scn) const
{
  int ret = OB_SUCCESS;
  const TypeOps *ops = get_ops(mds_type);
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("on_commit for unregistered mds_type", K(ret), K(mds_type));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("on_commit with null buf", K(ret), K(mds_type));
  } else {
    ops->on_commit(buf, commit_version, commit_scn);
  }
  // FIXME: @yunxing.cyx change to trace level
  LOG_INFO("invoke_on_commit done", K(ret), K(mds_type), KP(buf), K(commit_version), K(commit_scn));
  return ret;
}

int ObMdsTypeRegistry::invoke_serialize(const uint16_t mds_type,
                                        const void *buf,
                                        char *out_buf,
                                        const int64_t out_buf_len,
                                        int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const TypeOps *ops = get_ops(mds_type);
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("serialize for unregistered mds_type", K(ret), K(mds_type));
  } else if (OB_ISNULL(buf) || OB_ISNULL(out_buf) || out_buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("serialize invalid args", K(ret), K(mds_type), KP(buf), KP(out_buf), K(out_buf_len));
  } else {
    ret = ops->serialize(buf, out_buf, out_buf_len, pos);
  }
  return ret;
}

int ObMdsTypeRegistry::invoke_get_serialize_size(const uint16_t mds_type,
                                                 const void *buf,
                                                 int64_t &size) const
{
  int ret = OB_SUCCESS;
  const TypeOps *ops = get_ops(mds_type);
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get_serialize_size for unregistered mds_type", K(ret), K(mds_type));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("get_serialize_size with null buf", K(ret), K(mds_type));
  } else {
    size = ops->get_serialize_size(buf);
  }
  return ret;
}

int ObMdsTypeRegistry::invoke_deserialize(const uint16_t mds_type,
                                          void *buf,
                                          const char *in_buf,
                                          const int64_t in_buf_len,
                                          int64_t &pos,
                                          common::ObIAllocator *alloc) const
{
  int ret = OB_SUCCESS;
  const TypeOps *ops = get_ops(mds_type);
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("deserialize for unregistered mds_type", K(ret), K(mds_type));
  } else if (OB_ISNULL(buf) || OB_ISNULL(in_buf) || in_buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("deserialize invalid args", K(ret), K(mds_type), KP(buf), KP(in_buf), K(in_buf_len));
  } else {
    ret = ops->deserialize(buf, in_buf, in_buf_len, pos, alloc);
  }
  return ret;
}

int ObMdsTypeRegistry::invoke_construct(const uint16_t mds_type, void *buf) const
{
  int ret = OB_SUCCESS;
  const TypeOps *ops = get_ops(mds_type);
  if (OB_ISNULL(ops)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("construct for unregistered mds_type", K(ret), K(mds_type));
  } else if (OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("construct with null buf", K(ret), K(mds_type));
  } else {
    ops->construct(buf);
  }
  return ret;
}

int ObMdsTypeRegistry::init_all_mds_types()
{
  int ret = OB_SUCCESS;
  ObMdsTypeRegistry &reg = ObMdsTypeRegistry::get_instance();

  // Keep registry ids aligned with LsInnerMdsTable unit ids generated by ObTuple.
  // This gives a compile-time type<->id mapping via TupleTypeIdx and table tuple.
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION, TRANSFER_RELOC, SPLIT_RELOC) \
  if (OB_SUCC(ret)) {                                                                        \
    constexpr uint16_t cur_type = mds_type_id_detail::get_mds_type_id<KEY_TYPE, VALUE_TYPE>(); \
    ret = reg.register_type<KEY_TYPE, VALUE_TYPE>(cur_type);                                \
    if (OB_FAIL(ret)) {                                                                     \
      LOG_ERROR("failed to register MDS type by tuple id", K(ret), K(cur_type),             \
                "K", #KEY_TYPE, "V", #VALUE_TYPE, "MV", #NEED_MULTI_VERSION);               \
    }                                                                                        \
  }
#include "storage/multi_data_source/compile_utility/mds_register.h"
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE
  return ret;
}

} // namespace storage
} // namespace oceanbase
