/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_tiered_metadata_store.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_server_struct.h"
#include "storage/shared_storage/ob_dir_manager.h"
#include "storage/blocksstable/ob_ss_obj_util.h"
#include "share/inner_table/user_table/ob_user_table_schema.h"
#include "storage/incremental/ssmeta/ob_ss_meta_table_iterator.h"

namespace oceanbase {
namespace storage {
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;

int ObTieredMetadataStore::init(const uint64_t tenant_id, ObISQLClient *sql_client)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(sql_client)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), KP(sql_client));
  } else if (OB_FAIL(operator_.init(*sql_client, OB_ALL_SHARED_TIERED_META_TNAME))) {
    LOG_WARN("fail to init operator", K(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    sql_client_ = sql_client;
    is_inited_ = true;
  }
  return ret;
}

int ObTieredMetadataStore::pre_check_()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTieredMetadataStore not inited", K(ret));
  } else if (!is_valid_tenant_id(tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret), K_(tenant_id));
  } else if (OB_ISNULL(sql_client_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql_client_ is null", K(ret), K_(tenant_id));
  }
  return ret;
}

bool ObTieredMetadataStore::is_valid_macro_block_id_(const MacroBlockId &macro_block_id) const
{
  return macro_block_id.is_shared_tablet_sub_meta_in_table();
}

int ObTieredMetadataStore::macro_block_id_to_path_(
    const MacroBlockId &macro_block_id,
    char *buf,
    const int64_t buf_size,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_block_id.is_valid() || !SSObjUtil::is_valid(macro_block_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
  } else if (OB_FAIL(SSObjUtil::to_relative_remote_path_format(
      macro_block_id, buf, buf_size, pos))) {
    LOG_WARN("fail to convert macro block id to relative remote path", K(ret), K(macro_block_id));
  }
  return ret;
}

int ObTieredMetadataStore::macro_block_id_to_ss_meta_key_(
    const MacroBlockId &macro_block_id,
    share::ObLSID &ls_id,
    ObSSTieredMetaType &meta_type,
    char *key_buf,
    const int64_t key_buf_size,
    int64_t &key_len)
{
  int ret = OB_SUCCESS;
  key_len = 0;
  // sub_meta的宏块id中不包含 ls_id 和 meta_type。ls_id 取默认值 0；
  // meta_type 置为无效值，read/delete/iterate 仅按 meta_key 定位，不按 meta_type 过滤。
  ls_id = share::ObLSID(0);
  meta_type.reset();

  if (OB_FAIL(macro_block_id_to_path_(macro_block_id, key_buf, key_buf_size, key_len))) {
    LOG_WARN("fail to convert macro block id to path", K(ret), K(macro_block_id));
  }
  return ret;
}

int ObTieredMetadataStore::read_meta(
    const MacroBlockId &macro_block_id,
    char *buf,
    const int64_t buf_size,
    int64_t &actual_size)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTieredMetadataStore::read_meta", 100_ms);
  actual_size = 0;

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else if (!is_valid_macro_block_id_(macro_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
  } else if (OB_ISNULL(buf) || 0 >= buf_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buf", K(ret), K(macro_block_id), KP(buf), K(buf_size));
  } else {
    share::ObLSID ls_id;
    ObSSTieredMetaType meta_type;
    SMART_VAR(char[MAX_PATH_SIZE], key_buf) {
      int64_t key_len = 0;
      if (OB_FAIL(macro_block_id_to_ss_meta_key_(macro_block_id, ls_id, meta_type, key_buf, MAX_PATH_SIZE, key_len))) {
        LOG_WARN("fail to get v2 key", K(ret), K(macro_block_id));
      } else {
        ObString meta_key(key_len, key_buf);
        ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2"));
        ObString meta_value;
        if (OB_FAIL(operator_.read_row(tenant_id_, ls_id, meta_type, meta_key, allocator, meta_value))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_OBJECT_NOT_EXIST;
            LOG_WARN("meta not exist", K(ret), K(macro_block_id));
          } else {
            LOG_WARN("fail to read row", K(ret), K(macro_block_id));
          }
        } else if (meta_value.length() > buf_size) {
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("buf is not enough", K(ret), K(macro_block_id), K(meta_value.length()), K(buf_size));
        } else {
          MEMCPY(buf, meta_value.ptr(), meta_value.length());
          actual_size = meta_value.length();
        }
      }
    }
  }
  return ret;
}

int ObTieredMetadataStore::read_meta_row_data(
    const MacroBlockId &macro_block_id,
    ObIAllocator &allocator,
    ObTieredMetaTableRowData &row_data)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTieredMetadataStore::read_meta_row_data", 100_ms);
  row_data.reset();

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else if (!is_valid_macro_block_id_(macro_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
  } else {
    share::ObLSID expected_ls_id;
    ObSSTieredMetaType meta_type_filter;
    SMART_VAR(char[MAX_PATH_SIZE], key_buf) {
      int64_t key_len = 0;
      if (OB_FAIL(macro_block_id_to_ss_meta_key_(macro_block_id, expected_ls_id, meta_type_filter, key_buf,
                                                 MAX_PATH_SIZE, key_len))) {
        LOG_WARN("fail to get tiered meta key", K(ret), K(macro_block_id));
      } else {
        const ObString expected_meta_key(key_len, key_buf);
        ObSSMetaVersionFilter version_filter;
        // meta_type_filter 已被置为无效；迭代器仅按 meta_key 定位，不过滤 meta_type，
        // ORDER BY 也不包含 meta_type。直接透传 actual_meta_type 依赖以下不变量：
        // meta_key 由 macro_block_id 推导，并与 meta_type 一一对应。
        ObSSTieredMetaTableIterator iter;
        if (OB_FAIL(iter.init(*sql_client_, tenant_id_, expected_ls_id, meta_type_filter, expected_meta_key,
                              expected_meta_key, version_filter))) {
          LOG_WARN("fail to init exact tiered meta iterator", K(ret), K(macro_block_id), K(expected_meta_key));
        } else {
          uint64_t actual_tenant_id = OB_INVALID_TENANT_ID;
          share::ObLSID actual_ls_id;
          ObSSTieredMetaType actual_meta_type;
          ObString actual_meta_key;
          ObString actual_meta_value;
          if (OB_FAIL(iter.get_next(allocator, actual_tenant_id, actual_ls_id, actual_meta_type,
                                    actual_meta_key, actual_meta_value))) {
            if (OB_ITER_END == ret) {
              ret = OB_OBJECT_NOT_EXIST;
              LOG_WARN("exact tiered meta row data does not exist", K(ret), K(macro_block_id),
                       K(expected_meta_key));
            } else {
              LOG_WARN("fail to get exact tiered meta row data", K(ret), K(macro_block_id), K(expected_meta_key));
            }
          } else {
            if (OB_UNLIKELY(actual_meta_key != expected_meta_key)) {
              ret = OB_OBJECT_NOT_EXIST;
              LOG_WARN("exact tiered meta row data does not exist", K(ret), K(macro_block_id), K(expected_meta_key),
                       K(actual_meta_key));
            } else if (OB_UNLIKELY(actual_tenant_id != tenant_id_ || actual_ls_id != expected_ls_id
                            || actual_meta_value.empty())) {
              ret = OB_INVALID_DATA;
              LOG_WARN("invalid exact tiered meta row data", K(ret), K(macro_block_id), K(actual_tenant_id),
                       K(actual_ls_id), K(actual_meta_type), K(actual_meta_key), K(actual_meta_value.length()));
            } else {
              row_data.meta_type_ = actual_meta_type.get_type_val();
              row_data.meta_value_ = actual_meta_value;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObTieredMetadataStore::write_meta(
    const MacroBlockId &macro_block_id,
    const int64_t meta_type,
    const ObString &meta_value)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTimeGuard time_guard("ObTieredMetadataStore::write_meta", 500_ms);

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else if (!is_valid_macro_block_id_(macro_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
  } else if (meta_value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta_value is empty", K(ret), K(macro_block_id), K(meta_value));
  } else {
    share::ObLSID ls_id;
    ObSSTieredMetaType tiered_meta_type;
    SMART_VAR(char[MAX_PATH_SIZE], key_buf) {
      int64_t key_len = 0;
      if (OB_FAIL(macro_block_id_to_ss_meta_key_(macro_block_id, ls_id, tiered_meta_type,
                                                 key_buf, MAX_PATH_SIZE, key_len))) {
        LOG_WARN("fail to get v2 key", K(ret), K(macro_block_id));
      } else {
        // meta_type不能从macro_block_id获取；请使用上层传递的实际值。
        tiered_meta_type.type_val_ = meta_type;
        ObString meta_key(key_len, key_buf);
        if (OB_FAIL(operator_.insert_row(tenant_id_, ls_id, tiered_meta_type,
                                         meta_key, meta_value, affected_rows))) {
          if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
            if (OB_FAIL(compare_with_existing_value_when_write_conflict_(macro_block_id, meta_value))) {
              LOG_WARN("failed to write meta", K(ret), K(macro_block_id));
            } else {
              LOG_INFO("write conflict resolved, existing value matches", K(macro_block_id));
            }
          } else {
            LOG_WARN("fail to insert row", K(ret), K(macro_block_id));
          }
        } else {
          LOG_TRACE("succeed write meta v2", K(macro_block_id), K(meta_value), K(affected_rows));
        }
      }
    }
  }
  return ret;
}

// 批次内不同逻辑对象之间不保证原子性。单个逻辑对象的所有分片由同一条 SQL 语句插入，
// 批次操作具有幂等性，部分失败后可以安全重试。
int ObTieredMetadataStore::batch_write_meta(
    const ObIArray<MacroBlockId> &macro_block_ids,
    const ObIArray<int64_t> &meta_types,
    const ObIArray<ObString> &meta_values)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTieredMetadataStore::batch_write_meta", 1_s);

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else if (macro_block_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro_block_ids is empty", K(ret), K_(tenant_id));
  } else if (macro_block_ids.count() != meta_values.count()
             || macro_block_ids.count() != meta_types.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("array count is not match", K(ret), K_(tenant_id),
             "macro_block_ids.count", macro_block_ids.count(),
             "meta_types.count", meta_types.count(),
             "meta_values.count", meta_values.count());
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); i++) {
    const MacroBlockId &macro_block_id = macro_block_ids.at(i);
    const ObString &meta_value = meta_values.at(i);
    int64_t affected_rows = 0;
    if (!is_valid_macro_block_id_(macro_block_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
    } else if (meta_value.empty()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("meta_value is empty", K(ret), K(macro_block_id), K(meta_value));
    } else {
      share::ObLSID ls_id;
      ObSSTieredMetaType tiered_meta_type;
      SMART_VAR(char[MAX_PATH_SIZE], key_buf) {
        int64_t key_len = 0;
        if (OB_FAIL(macro_block_id_to_ss_meta_key_(macro_block_id, ls_id, tiered_meta_type,
                                                   key_buf, MAX_PATH_SIZE, key_len))) {
          LOG_WARN("fail to get v2 key", K(ret), K(i), K(macro_block_id));
        } else {
          // meta_type不能从macro_block_id获取；请使用上层传递的实际值。
          tiered_meta_type.type_val_ = meta_types.at(i);
          ObString meta_key(key_len, key_buf);
          if (OB_FAIL(operator_.insert_row(tenant_id_, ls_id, tiered_meta_type,
                                           meta_key, meta_value, affected_rows))) {
            if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
              if (OB_FAIL(compare_with_existing_value_when_write_conflict_(macro_block_id, meta_value))) {
                LOG_WARN("failed to write meta on duplicate key", K(ret), K(i), K(macro_block_id));
              } else {
                LOG_INFO("write conflict resolved, existing value matches", K(i), K(macro_block_id));
              }
            } else {
              LOG_WARN("fail to insert row", K(ret), K(i), K(macro_block_id));
            }
          }
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed batch write meta v2", K(macro_block_ids.count()));
  }
  return ret;
}

int ObTieredMetadataStore::del_meta(const MacroBlockId &macro_block_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObTimeGuard time_guard("ObTieredMetadataStore::del_meta", 500_ms);

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else if (!is_valid_macro_block_id_(macro_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
  } else {
    share::ObLSID ls_id;
    ObSSTieredMetaType meta_type;
    SMART_VAR(char[MAX_PATH_SIZE], key_buf) {
      int64_t key_len = 0;
      if (OB_FAIL(macro_block_id_to_ss_meta_key_(macro_block_id, ls_id, meta_type, key_buf, MAX_PATH_SIZE, key_len))) {
        LOG_WARN("fail to get v2 key", K(ret), K(macro_block_id));
      } else {
        ObString meta_key(key_len, key_buf);
        if (OB_FAIL(operator_.delete_row(tenant_id_, ls_id, meta_type, meta_key, false/*prefix_delete*/, affected_rows))) {
          LOG_WARN("fail to delete row", K(ret), K(macro_block_id));
        } else if (0 == affected_rows) {
          LOG_INFO("delete meta but macro block not exist in v2", K(macro_block_id));
        } else {
          LOG_TRACE("succeed delete meta v2", K(macro_block_id));
        }
      }
    }
  }
  return ret;
}

// 批次内不同逻辑对象之间不保证原子性。单个逻辑对象的所有分片由同一条 SQL 语句删除，
// 批次操作具有幂等性，部分失败后可以安全重试。
int ObTieredMetadataStore::batch_del_meta(const ObIArray<MacroBlockId> &macro_block_ids)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTieredMetadataStore::batch_del_meta", 1_s);
  static constexpr int64_t MAX_BATCH_DELETE_COUNT = 128;
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMDelKeys"));
  ObSEArray<ObString, MAX_BATCH_DELETE_COUNT> meta_keys;
  share::ObLSID ls_id;
  ObSSTieredMetaType meta_type;

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else if (macro_block_ids.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("macro_block_ids is empty", K(ret), K_(tenant_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < macro_block_ids.count(); i++) {
    const MacroBlockId &macro_block_id = macro_block_ids.at(i);
    if (!is_valid_macro_block_id_(macro_block_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
    } else {
      SMART_VAR(char[MAX_PATH_SIZE], key_buf) {
        int64_t key_len = 0;
        ObString meta_key;
        if (OB_FAIL(macro_block_id_to_ss_meta_key_(macro_block_id, ls_id, meta_type, key_buf, MAX_PATH_SIZE, key_len))) {
          LOG_WARN("fail to get v2 key", K(ret), K(i), K(macro_block_id));
        } else if (OB_FAIL(ob_write_string(allocator, ObString(key_len, key_buf), meta_key))) {
          LOG_WARN("fail to copy meta key", K(ret), K(i));
        } else if (OB_FAIL(meta_keys.push_back(meta_key))) {
          LOG_WARN("fail to add meta key", K(ret), K(i));
        }
      }
    }
    if (OB_SUCC(ret)
        && (MAX_BATCH_DELETE_COUNT == meta_keys.count() || i + 1 == macro_block_ids.count())) {
      // All sub-meta keys use ls_id=0 and no meta_type filter; omit part to delete every fragment.
      if (OB_FAIL(operator_.batch_delete_rows(tenant_id_, ls_id, meta_type, meta_keys))) {
        LOG_WARN("fail to batch delete meta rows", K(ret), K(i), K(meta_keys.count()));
      }
      meta_keys.reuse();
      allocator.reuse();
    }
  }

  if (OB_SUCC(ret)) {
    LOG_TRACE("succeed batch del meta v2", K(macro_block_ids.count()));
  }
  return ret;
}

int ObTieredMetadataStore::is_meta_exist(
    const MacroBlockId &macro_block_id,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTieredMetadataStore::is_meta_exist", 500_ms);
  is_exist = false;

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else if (!is_valid_macro_block_id_(macro_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(macro_block_id));
  } else {
    share::ObLSID ls_id;
    ObSSTieredMetaType meta_type;
    SMART_VAR(char[MAX_PATH_SIZE], key_buf) {
      int64_t key_len = 0;
      if (OB_FAIL(macro_block_id_to_ss_meta_key_(macro_block_id, ls_id, meta_type, key_buf, MAX_PATH_SIZE, key_len))) {
        LOG_WARN("fail to get v2 key", K(ret), K(macro_block_id));
      } else {
        ObString meta_key(key_len, key_buf);
        ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2"));
        ObString meta_value;
        if (OB_FAIL(operator_.read_row(tenant_id_, ls_id, meta_type, meta_key, allocator, meta_value))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            is_exist = false;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to read row", K(ret), K(macro_block_id));
          }
        } else {
          is_exist = true;
        }
      }
    }
  }
  return ret;
}

int ObTieredMetadataStore::list_meta_macro_files(
    const bool is_ls_inner_tablet,
    const int64_t ls_id,
    const int64_t tablet_id,
    const int64_t reorganization_scn,
    ObIArray<MacroBlockId> &macro_ids)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTieredMetadataStore::list_meta_macro_files", 1_s);

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else {
    SMART_VAR(char[MAX_PATH_SIZE], dir_path_str) {
      if (OB_FAIL(OB_DIR_MGR.get_shared_tablet_meta_relative_dir(dir_path_str,
                                                                 MAX_PATH_SIZE,
                                                                 is_ls_inner_tablet,
                                                                 ls_id,
                                                                 tablet_id,
                                                                 reorganization_scn))) {
        LOG_WARN("get shared tablet meta relative dir failed", K(ret),
                 K(is_ls_inner_tablet), K(ls_id), K(tablet_id), K(reorganization_scn));
      } else {
        ObString prefix(dir_path_str);
        share::ObLSID iter_ls_id(0);
        // meta_type 置为无效值，仅按 meta_key(path) 迭代，不按 meta_type 过滤，
        // 否则会漏掉 sstable/co_sstable/storage_schema 等非 TABLE_STORE 的行。
        ObSSTieredMetaType meta_type;
        meta_type.reset();
        ObSSMetaVersionFilter version_filter;
        ObSSTieredMetaTableIterator iter;

        if (OB_FAIL(iter.init(*sql_client_, tenant_id_, iter_ls_id, meta_type, prefix, version_filter))) {
          LOG_WARN("fail to init tiered meta iterator", K(ret), K(prefix));
        } else {
          ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2"));
          uint64_t out_tenant_id = 0;
          share::ObLSID out_ls_id;
          ObSSMetaBaseType out_meta_type;
          ObString meta_key;
          ObString meta_value;
          MacroBlockId macro_id;

          while (OB_SUCC(ret)) {
            allocator.reuse();
            if (OB_FAIL(iter.get_next(allocator, out_tenant_id, out_ls_id, out_meta_type, meta_key, meta_value))) {
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
                break;
              } else {
                LOG_WARN("fail to get next", K(ret));
              }
            } else {
              SMART_VAR(char[MAX_PATH_SIZE], path_buf) {
                if (OB_FAIL(databuff_printf(path_buf, MAX_PATH_SIZE, "%.*s",
                                            meta_key.length(), meta_key.ptr()))) {
                  LOG_WARN("fail to format path", K(ret), K(meta_key));
                } else if (OB_FAIL(SSObjUtil::relative_remote_path_to_macro_id(
                               ObStorageObjectType::SHARED_TABLET_SUB_META,
                               path_buf, macro_id))) {
                  LOG_WARN("fail to convert path to macro id", K(ret), K(meta_key));
                } else if (OB_FAIL(macro_ids.push_back(macro_id))) {
                  LOG_WARN("fail to push back", K(ret), K(macro_id));
                }
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          LOG_TRACE("list meta macro files v2", K(macro_ids.count()));
        }
      }
    }
  }
  return ret;
}

int ObTieredMetadataStore::get_tiered_metadata_iter(
    const MacroBlockId &start_macro_block_id,
    const MacroBlockId &end_macro_block_id,
    ObSSTieredMetaTableIterator &iter)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObTieredMetadataStore::get_tiered_metadata_iter", 500_ms);

  if (OB_FAIL(pre_check_())) {
    LOG_WARN("pre check failed", K(ret));
  } else if (!is_valid_macro_block_id_(start_macro_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(start_macro_block_id));
  } else if (!is_valid_macro_block_id_(end_macro_block_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid macro block id", K(ret), K(end_macro_block_id));
  } else {
    int64_t start_pos = 0;
    int64_t end_pos = 0;
    SMART_VARS_2((char[MAX_PATH_SIZE], start_path),
                 (char[MAX_PATH_SIZE], end_path)) {
      if (OB_FAIL(macro_block_id_to_path_(start_macro_block_id, start_path, MAX_PATH_SIZE, start_pos))) {
        LOG_WARN("fail to format start path", K(ret), K(start_macro_block_id));
      } else if (OB_FAIL(macro_block_id_to_path_(end_macro_block_id, end_path, MAX_PATH_SIZE, end_pos))) {
        LOG_WARN("fail to format end path", K(ret), K(end_macro_block_id));
      } else {
        ObString start_key(start_pos, start_path);
        ObString end_key(end_pos, end_path);
        share::ObLSID ls_id(0);
        // meta_type 置为无效值，仅按 meta_key(path) 范围迭代，不按 meta_type 过滤，
        // 否则会漏掉 sstable/co_sstable/storage_schema 等非 TABLE_STORE 的行。
        ObSSTieredMetaType meta_type;
        meta_type.reset();
        ObSSMetaVersionFilter version_filter;

        if (OB_FAIL(iter.init(*sql_client_, tenant_id_, ls_id, meta_type, start_key, end_key, version_filter))) {
          LOG_WARN("fail to init tiered meta iterator", K(ret), K(start_key), K(end_key));
        }
      }
    }
  }
  return ret;
}

int ObTieredMetadataStore::compare_with_existing_value_when_write_conflict_(
    const MacroBlockId &macro_block_id,
    const ObString &meta_value)
{
  int ret = OB_SUCCESS;
  char *read_buf = nullptr;
  int64_t read_size = 0;
  const int64_t buf_size = meta_value.length();
  ObArenaAllocator allocator(ObMemAttr(tenant_id_, "TMStoreV2"));

  if (OB_ISNULL(read_buf = static_cast<char *>(allocator.alloc(buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", K(ret), K(buf_size));
  } else if (OB_FAIL(read_meta(macro_block_id, read_buf, buf_size, read_size))) {
    if (OB_SIZE_OVERFLOW == ret) {
      ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
      LOG_WARN("value size mismatch on write conflict", K(ret), K(macro_block_id), K(buf_size));
    } else {
      LOG_WARN("fail to read meta", K(ret), K(macro_block_id), K(buf_size));
    }
  } else if (read_size != buf_size) {
    ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
    LOG_WARN("value size mismatch on write conflict", K(ret), K(macro_block_id), K(buf_size), K(read_size));
  } else if (0 != MEMCMP(read_buf, meta_value.ptr(), buf_size)) {
    ret = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
    LOG_WARN("value content mismatch on write conflict", K(ret), K(macro_block_id), K(buf_size), K(read_size));
  }
  return ret;
}
}
}
