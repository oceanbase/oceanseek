/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#define USING_LOG_PREFIX SHARE

#include "share/ob_fork_table_util.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_table_schema.h"
#include "share/ob_domain_index_builder_util.h"
#include "share/ob_fts_index_builder_util.h"
#include "share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace share
{

OB_SERIALIZE_MEMBER(ObForkTableInfo, fork_src_table_id_, fork_dest_table_id_, fork_snapshot_version_);

OB_SERIALIZE_MEMBER(ObForkTabletInfo,
                    fork_snapshot_version_,
                    src_tablet_id_,
                    data_complete_,
                    all_replicas_land_complete_);

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

bool ObForkTableUtil::is_domain_or_aux_index(const ObTableSchema &index_schema)
{
  const ObIndexType index_type = index_schema.get_index_type();
  return index_schema.is_rowkey_doc_id()
         || index_schema.is_doc_id_rowkey()
         || index_schema.is_vec_rowkey_vid_type()
         || index_schema.is_vec_vid_rowkey_type()
         || index_schema.is_fts_index_aux()
         || index_schema.is_multivalue_index_aux()
         || index_schema.is_vec_domain_index()
         || share::schema::is_fts_doc_word_aux(index_type)
         || share::schema::is_vec_index_id_type(index_type)
         || share::schema::is_vec_index_snapshot_data_type(index_type)
         || share::schema::is_built_in_vec_ivf_index(index_type)
         || share::schema::is_hybrid_vec_index_embedded_type(index_type);
}

int ObForkTableUtil::collect_complete_domain_index_schemas(
    ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const ObTableSchema &table_schema,
    hash::ObHashMap<uint64_t, ObTableSchema> &complete_index_schema_map)
{
  int ret = OB_SUCCESS;
  ObArray<ObAuxTableMetaInfo> simple_index_infos;
  ObSArray<ObTableSchema> shared_schema_array;
  ObSArray<ObTableSchema> domain_schema_array;
  ObSArray<ObTableSchema> aux_schema_array;
  ObArenaAllocator allocator(lib::ObLabel("ForkTableIdx"));
  ObSArray<ObTableSchema> complete_index_schemas;

  if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
    LOG_WARN("fail to get simple index infos", K(ret), K(table_schema));
  } else {
    if (OB_FAIL(complete_index_schema_map.create(simple_index_infos.count() * 2 + 8, lib::ObLabel("ForkTableIdxMap")))) {
      LOG_WARN("fail to create complete index schema map", K(ret), K(simple_index_infos.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
      const ObTableSchema *index_schema = nullptr;
      const uint64_t index_table_id = simple_index_infos.at(i).table_id_;
      if (OB_INVALID_ID == index_table_id) {
        continue;
      } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
        LOG_WARN("get index table schema failed", K(ret), K(tenant_id), K(index_table_id));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_TABLE_NOT_EXIST;
        LOG_WARN("index table schema is null", K(ret), K(tenant_id), K(index_table_id));
      } else if (index_schema->is_in_recyclebin() || INDEX_STATUS_AVAILABLE != index_schema->get_index_status()) {
        continue;
      } else if (!is_domain_or_aux_index(*index_schema)) {
        continue;
      } else {
        const ObIndexType index_type = index_schema->get_index_type();
        if (index_schema->is_rowkey_doc_id() || index_schema->is_doc_id_rowkey()
            || index_schema->is_vec_rowkey_vid_type() || index_schema->is_vec_vid_rowkey_type()) {
          if (OB_FAIL(shared_schema_array.push_back(*index_schema))) {
            LOG_WARN("fail to push back shared schema", K(ret), K(index_table_id));
          }
        } else if (index_schema->is_fts_index_aux() || index_schema->is_multivalue_index_aux()
                   || index_schema->is_vec_domain_index()) {
          if (OB_FAIL(domain_schema_array.push_back(*index_schema))) {
            LOG_WARN("fail to push back domain schema", K(ret), K(index_table_id));
          }
        } else if (share::schema::is_fts_doc_word_aux(index_type) || share::schema::is_vec_index_id_type(index_type)
                   || share::schema::is_vec_index_snapshot_data_type(index_type)
                   || share::schema::is_built_in_vec_ivf_index(index_type)
                   || share::schema::is_hybrid_vec_index_embedded_type(index_type)) {
          if (OB_FAIL(aux_schema_array.push_back(*index_schema))) {
            LOG_WARN("fail to push back aux schema", K(ret), K(index_table_id));
          }
        }
      }
    }
  }

  if (OB_SUCC(ret) && !domain_schema_array.empty()) {
    bool need_doc_id = false;
    bool need_vid = false;
    if (OB_FAIL(ObFtsIndexBuilderUtil::check_need_doc_id(table_schema, need_doc_id))) {
      LOG_WARN("fail to check need doc id", K(ret));
    } else if (OB_FAIL(ObVectorIndexUtil::check_need_vid(table_schema, need_vid))) {
      LOG_WARN("fail to check need vid", K(ret));
    } else if (OB_FAIL(ObDomainIndexBuilderUtil::retrieve_complete_domain_index(
                 shared_schema_array,
                 domain_schema_array,
                 aux_schema_array,
                 allocator,
                 table_schema.get_table_id(),
                 complete_index_schemas,
                 need_doc_id,
                 need_vid))) {
      LOG_WARN("fail to retrieve complete domain index", K(ret), K(table_schema.get_table_id()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < complete_index_schemas.count(); ++i) {
        const ObTableSchema &schema = complete_index_schemas.at(i);
        if (OB_FAIL(complete_index_schema_map.set_refactored(schema.get_table_id(), schema))) {
          if (OB_HASH_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to set complete index schema", K(ret), K(schema.get_table_id()));
          }
        }
      }
    }
  }

  return ret;
}

int ObForkTableUtil::find_fork_src_table_id(
    const ObIArray<ObForkTableInfo> &fork_table_infos,
    const uint64_t fork_dest_table_id,
    uint64_t &fork_src_table_id)
{
  int ret = OB_SUCCESS;
  fork_src_table_id = OB_INVALID_ID;
  for (int64_t i = 0; OB_INVALID_ID == fork_src_table_id && i < fork_table_infos.count(); ++i) {
    if (fork_table_infos.at(i).get_fork_dest_table_id() == fork_dest_table_id) {
      fork_src_table_id = fork_table_infos.at(i).get_fork_src_table_id();
    }
  }
  if (OB_INVALID_ID == fork_src_table_id) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObForkTableUtil::collect_table_ids_from_table(
    share::schema::ObSchemaGetterGuard &schema_guard,
    const uint64_t tenant_id,
    const share::schema::ObTableSchema &table_schema,
    common::ObIArray<uint64_t> &table_ids)
{
  int ret = OB_SUCCESS;
  table_ids.reset();
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
  }

  // 1. 主表
  const uint64_t main_table_id = table_schema.get_table_id();
  if (OB_SUCC(ret) && OB_FAIL(table_ids.push_back(main_table_id))) {
    LOG_WARN("fail to push back main table id", KR(ret), K(main_table_id));
  }

  // 2. 索引：普通 row-store index 直接收；domain/aux 索引只收完整组成员，残缺组跳过。
  if (OB_SUCC(ret)) {
    ObArray<ObAuxTableMetaInfo> simple_index_infos;
    if (OB_FAIL(table_schema.get_simple_index_infos(simple_index_infos))) {
      LOG_WARN("fail to get simple index infos", KR(ret), K(table_schema));
    } else {
      hash::ObHashMap<uint64_t, ObTableSchema> complete_index_schema_map;
      if (OB_FAIL(collect_complete_domain_index_schemas(
              schema_guard, tenant_id, table_schema, complete_index_schema_map))) {
        LOG_WARN("fail to collect complete domain index schemas", KR(ret), K(main_table_id));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < simple_index_infos.count(); ++i) {
          const ObTableSchema *index_schema = nullptr;
          const uint64_t index_table_id = simple_index_infos.at(i).table_id_;
          if (OB_INVALID_ID == index_table_id) {
            continue;
          } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_schema))) {
            LOG_WARN("get index table schema failed", KR(ret), K(tenant_id), K(index_table_id));
          } else if (OB_ISNULL(index_schema)) {
            ret = OB_TABLE_NOT_EXIST;
            LOG_WARN("index table schema is null", KR(ret), K(tenant_id), K(index_table_id));
          } else if (index_schema->is_in_recyclebin() || INDEX_STATUS_AVAILABLE != index_schema->get_index_status()) {
            continue;
          } else if (is_domain_or_aux_index(*index_schema)) {
            if (OB_ISNULL(complete_index_schema_map.get(index_table_id))) {
              // 残缺 domain/aux 组，跳过。
            } else if (OB_FAIL(table_ids.push_back(index_table_id))) {
              LOG_WARN("fail to push back domain index table id", KR(ret), K(index_table_id));
            }
          } else if (OB_FAIL(table_ids.push_back(index_table_id))) {
            LOG_WARN("fail to push back index table id", KR(ret), K(index_table_id));
          } else if (index_schema->is_search_def_index()) {
            ObSEArray<uint64_t, 3> search_aux_table_ids;
            const ObTableSchema *search_data_schema = nullptr;
            // Data/Match/Phrase 物理子表紧跟 Search Def 收集，保证源、目标表按数组位置正确配对。
            if (OB_FAIL(index_schema->get_search_aux_index_tids(search_aux_table_ids))) {
              LOG_WARN("fail to get search auxiliary index ids", KR(ret), K(index_table_id));
            }
            for (int64_t j = 0; OB_SUCC(ret) && j < search_aux_table_ids.count(); ++j) {
              const uint64_t search_data_table_id = search_aux_table_ids.at(j);
              if (OB_FAIL(schema_guard.get_table_schema(tenant_id, search_data_table_id, search_data_schema))) {
                LOG_WARN("fail to get search auxiliary index schema", KR(ret), K(tenant_id), K(search_data_table_id));
              } else if (OB_ISNULL(search_data_schema)) {
                ret = OB_SCHEMA_EAGAIN;
                LOG_WARN("search auxiliary index schema is null", KR(ret), K(tenant_id), K(search_data_table_id));
              } else if (OB_UNLIKELY(!search_data_schema->is_search_aux_index()
                                     || search_data_schema->get_data_table_id() != index_table_id
                                     || search_data_schema->is_in_recyclebin()
                                     || INDEX_STATUS_AVAILABLE != search_data_schema->get_index_status())) {
                ret = OB_SCHEMA_EAGAIN;
                LOG_WARN("search index schema topology is not ready", KR(ret), KPC(index_schema), KPC(search_data_schema));
              } else if (OB_FAIL(table_ids.push_back(search_data_table_id))) {
                LOG_WARN("fail to push back search auxiliary index table id", KR(ret), K(search_data_table_id));
              }
            }
          }
        }
      }
    }
  }

  // 3. lob meta / piece 辅助表
  if (OB_SUCC(ret)) {
    const uint64_t lob_meta_table_id = table_schema.get_aux_lob_meta_tid();
    const uint64_t lob_piece_table_id = table_schema.get_aux_lob_piece_tid();
    if (OB_INVALID_ID != lob_meta_table_id && OB_FAIL(table_ids.push_back(lob_meta_table_id))) {
      LOG_WARN("fail to push back LOB meta table id", KR(ret), K(lob_meta_table_id));
    } else if (OB_INVALID_ID != lob_piece_table_id && OB_FAIL(table_ids.push_back(lob_piece_table_id))) {
      LOG_WARN("fail to push back LOB piece table id", KR(ret), K(lob_piece_table_id));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
