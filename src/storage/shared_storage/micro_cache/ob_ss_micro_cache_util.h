/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_UTIL_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_UTIL_H_

#include <stdint.h>
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_common_meta.h"
#include "storage/shared_storage/micro_cache/ob_ss_physical_block_info.h"

namespace oceanbase
{
namespace storage
{
class ObLSHandle;

class ObSSMicroCacheUtil
{
public:
  static int parse_micro_block_indexs(const char *buf, const int64_t buf_len, 
      common::ObIArray<ObSSMicroBlockIndex> &micro_indexs);
  static int parse_phy_block_common_header(const char *buf, const int64_t buf_len, 
      ObSSPhyBlockCommonHeader &common_header);
  static int parse_micro_data_block_header(const char *buf, const int64_t buf_len, 
      ObSSMicroDataBlockHeader &header);
  static int dump_phy_block(char *buf, const int64_t buf_len, const char *file_path, 
      const bool only_index = false, const bool is_micro_meta = false);
  static int parse_micro_data_block(char *dest_buf, const int64_t dest_len, const char *src_buf, 
      const int64_t src_len, int64_t &pos, const bool only_index);
  static int parse_ss_super_block(char *dest_buf, const int64_t dest_len, const char *src_buf, 
      const int64_t src_len, int64_t &pos);
  static int parse_checkpoint_block(char *dest_buf, const int64_t dest_len, char *src_buf, 
      const int64_t src_len, int64_t &pos, const bool is_micro_meta);
  static int calc_ls_tablet_cache_info(const uint64_t tenant_id, const ObSSMCTabletInfoMap &tablet_info_map,
      common::ObIArray<ObSSMicroCacheTabletInfo> &tablet_info_list,
      common::ObIArray<ObSSMicroCacheLSInfo> &ls_info_list);
  static int get_ls_handles(common::ObIArray<ObLSHandle> &ls_handles);
  static void get_ls_id_by_tablet_id(const uint64_t tenant_id, const common::ObTabletID &tablet_id, 
      const common::ObIArray<ObLSHandle> &ls_handles, share::ObLSID &ls_id);
  static int64_t calc_max_micro_blk_size(const uint64_t tenant_id);
  static int64_t calc_micro_cache_mem_limit_size(const uint64_t tenant_id);
  static int64_t calc_micro_cache_expiration_time(const uint64_t tenant_id);
  static int64_t calc_micro_cache_arc_limit_pct(const uint64_t tenant_id);
  static uint32_t calc_micro_ckpt_split_cnt(const bool is_mini_cache, const int64_t meta_blk_min_cnt);
  static uint32_t calc_init_range_cnt(const int64_t micro_cnt_limit);
  static int64_t calc_micro_cnt_limit(const uint64_t tenant_id, const int64_t mem_limit);
  static int parse_range_micro_metas(const uint64_t tenant_id, const char *buf, const int64_t buf_size, 
      const int32_t blk_size, common::ObIArray<ObSSMicroBlockMetaInfo> &parsed_micro_infos);
  static bool contain_micro_key(const common::ObIArray<ObSSMicroBlockMetaInfo> &micro_infos,
                                const ObSSMicroBlockCacheKey &micro_key,
                                int64_t &micro_info_idx);
  static int get_micro_ckpt_split_cnt(uint32_t &cur_ckpt_split_cnt);
  static bool check_mini_cache(const int64_t cache_file_size);
  static bool should_delete_tablet_record(const int64_t recorded_ckpt_op_cnt,
                                          const int64_t cur_micro_ckpt_op_cnt,
                                          const int64_t meta_ckpt_split_cnt);
  static int check_data_crc(const char *data, const int64_t size, const uint32_t crc);
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_MICRO_CACHE_UTIL_H_ */
