/**
 * Copyright (C) <2026> OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_INFO_UTIL_H_
#define OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_INFO_UTIL_H_

#include "common/ob_tablet_id.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/incremental/ssmeta/ob_ss_meta_define.h"
#include "storage/meta_mem/ob_meta_obj_struct.h"

namespace oceanbase
{
namespace storage
{

class ObSSMetaInfoUtil
{
public:
  // 构造 ls_ckpt 的 palf_kv key / reserved_scn owner，各调用方统一走这里，避免格式漂移
  static int build_ls_ckpt_key(const share::ObLSID &ls_id,
                               char *buf,
                               const int64_t buf_len,
                               int64_t &key_len);

  static const char *find_marker(const common::ObString &meta_key,
                                 const char *marker = SSMETA_SCN_MARKER);
  static int extract_prefix(const common::ObString &meta_key,
                            common::ObString &logical_prefix,
                            const char *marker = SSMETA_SCN_MARKER);
  // Format: A/B/C/scn{scn_val}.Txx, "/scn"为marker，解析出scn_val
  static int extract_scn_value(const common::ObString &meta_key,
                               uint64_t &scn_val,
                               const char *marker = SSMETA_SCN_MARKER);
  // meta_key format: tablet/{tablet_id}/{reorgan_scn}/meta/scn{meta_scn}.T{type}
  static int parse_tablet_meta_key(const common::ObString &meta_key,
                                   common::ObTabletID &tablet_id,
                                   share::SCN &reorgan_scn,
                                   share::SCN &meta_scn);

  // 反向解析 tablet meta_key，兼容 inner_tablet 与普通 tablet 两种格式：
  //   inner_tablet: ls/{ls_id}/{tablet_name}/meta/scn{meta_scn}.T{type}
  //   普通 tablet:  tablet/{tablet_id}/{reorgan_scn}/meta/scn{meta_scn}.T{type}
  // inner_tablet 解析出 ls_id，普通 tablet 解析出 reorgan_scn。
  static int parse_tablet_meta_key(const common::ObString &meta_key,
                                   share::ObLSID &ls_id,
                                   common::ObTabletID &tablet_id,
                                   share::SCN &reorgan_scn,
                                   share::SCN &meta_scn);

  // meta_key format: ls/{ls_id}/meta/scn{meta_scn}.T{type}
  static int parse_ls_meta_key(const common::ObString &meta_key,
                               share::ObLSID &ls_id,
                               share::SCN &meta_scn);

  static int gen_meta_key_prefix(const ObSSMetaReadParamV2 &read_param,
                                 common::ObSqlString &prefix);

  // 生成 tablet meta_key 的 logical prefix，统一处理 inner_tablet 与普通 tablet：
  //   inner_tablet:                  ls/{ls_id}/{tablet_name}/meta
  //   普通 tablet(reorgan_scn 合法): tablet/{tablet_id}/{reorgan_scn}/meta
  //   普通 tablet(reorgan_scn 非法): tablet/{tablet_id}/
  // inner_tablet 不依赖 reorgan_scn，reorgan_scn 非法时仍生成完整前缀。
  static int gen_tablet_meta_key_prefix(const share::ObLSID &ls_id,
                                        const common::ObTabletID &tablet_id,
                                        const share::SCN &reorgan_scn,
                                        common::ObSqlString &prefix);

  static int gen_tablet_addr(const share::ObLSID &ls_id,
                             const common::ObTabletID &tablet_id,
                             const share::SCN &reorganization_scn,
                             const uint64_t op_id,
                             const int64_t tablet_buffer_size,
                             ObMetaDiskAddr &tablet_addr);

  static int build_tablet_handle(const share::ObLSID &ls_id,
                                 const common::ObTabletID &tablet_id,
                                 const share::SCN &reorganization_scn,
                                 const common::ObString &meta_value,
                                 common::ObArenaAllocator &allocator,
                                 ObTabletHandle &tablet_handle);
  static int build_tablet_handle(const share::ObLSID &ls_id,
                                 const common::ObTabletID &tablet_id,
                                 const share::SCN &reorganization_scn,
                                 const common::ObString &meta_value,
                                 common::ObArenaAllocator &allocator,
                                 ObTabletHandle &tablet_handle,
                                 ObSSUpdateTabletLog &update_log);
  static int get_ssmeta_trans_timeout_us(const uint64_t tenant_id,
                                         int64_t &timeout_us);
  static int get_ls_ckpt_timeout_us(const uint64_t tenant_id,
                                    int64_t &timeout_us);

private:
  DISALLOW_COPY_AND_ASSIGN(ObSSMetaInfoUtil);
};

} // end namespace storage
} // end namespace oceanbase
#endif /* OCEANBASE_SHARED_STORAGE_STORAGE_INCREMENTAL_SS_META_INFO_UTIL_H_ */
