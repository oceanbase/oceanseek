/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_COMMON_INFO_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_COMMON_INFO_H_

#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace storage
{

const bool OB_SS_ENABLE_LOCAL_CACHE_SYSSTAT = true;

static constexpr char OB_SS_LOCAL_CACHE_NAME[] = "ss_local_cache";
static constexpr char OB_SS_MACRO_CACHE_NAME[] = "ss_macro_cache"; // include mem/disk mode 'tmpfile & shared_macro & private_macro & ext_table'
static constexpr char OB_SS_LOCAL_CACHE_TMPFILE_NAME[] = "ss_local_cache_tmpfile";
static constexpr char OB_SS_LOCAL_CACHE_SHARED_MACRO_NAME[] = "ss_local_cache_shared_macro";
static constexpr char OB_SS_LOCAL_CACHE_PRIVATE_MACRO_NAME[] = "ss_local_cache_private_macro";
static constexpr char OB_SS_LOCAL_CACHE_EXTERNAL_TABLE_NAME[] = "ss_local_cache_external_table";
static constexpr int OB_SS_LOCAL_CACHE_PRIORITY = 0;
static constexpr int OB_SS_MACRO_CACHE_PRIORITY = 0;
static constexpr int OB_LOCAL_CACHE_TMPFILE_PRIORITY = 0;
static constexpr int OB_LOCAL_CACHE_SHARED_MACRO_PRIORITY = 0;
static constexpr int OB_LOCAL_CACHE_PRIVATE_MACRO_PRIORITY = 0;
static constexpr int OB_LOCAL_CACHE_EXTERNAL_TABLE_PRIORITY = 0;

static constexpr int OB_SS_DIAGNOSE_MOD_NAME_LENGTH = 64;

int set_ss_stats(const int64_t tenant_id, common::ObDiagnoseTenantInfo &diag_info);

// SS_FOREACH_MAP_NODE_BASE - Traverse buckets/nodes with optional bucket condition and per-bucket hook
// Parameters:
//   map                 - HashMap object (e.g. meta_map_)
//   map_type            - HashMap type (e.g. SSMacroCacheMetaMap, must not contain comma, use typedef/using)
//   node_iter_name      - node iterator variable name (available in node_body)
//   bucket_cond         - bucket loop condition expression (e.g. true / !is_stopped_)
//   node_body           - node loop body code block (wrap with braces)
//   bucket_body         - code block executed after each bucket (wrap with braces, can be {})
//
#define SS_FOREACH_MAP_NODE_BASE(map, map_type, node_iter_name, bucket_cond, node_body, bucket_body) \
  do { \
    map_type::bucket_iterator bucket_iter__ = map.bucket_begin(); \
    while (OB_SUCC(ret) && (bucket_cond) && (bucket_iter__ != map.bucket_end())) { \
      { \
        map_type::hashtable::bucket_lock_cond blk(*bucket_iter__); \
        map_type::hashtable::readlocker locker(blk.lock()); \
        map_type::hashtable::hashbucket::const_iterator node_iter_name = bucket_iter__->node_begin(); \
        while (OB_SUCC(ret) && (node_iter_name != bucket_iter__->node_end())) { \
          node_body \
          node_iter_name++; \
        } \
      } \
      bucket_body \
      ++bucket_iter__; \
    } \
  } while (0)

#define SS_FOREACH_MAP_NODE(map, map_type, node_iter_name, node_body) \
  SS_FOREACH_MAP_NODE_BASE(map, map_type, node_iter_name, true, node_body, {})

#define SS_FOREACH_MAP_NODE_WITH_COND(map, map_type, node_iter_name, extra_bucket_cond, node_body) \
  SS_FOREACH_MAP_NODE_BASE(map, map_type, node_iter_name, (extra_bucket_cond), node_body, {})

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_COMMON_INFO_H_ */
