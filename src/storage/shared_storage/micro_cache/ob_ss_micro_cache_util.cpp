/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "share/ob_io_device_helper.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_util.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_struct.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObSSMicroCacheUtil::parse_micro_block_indexs(
    const char *buf, 
    const int64_t buf_len, 
    ObIArray<ObSSMicroBlockIndex> &micro_indexs)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else {
    int64_t deserialized_size = 0;
    while (OB_SUCC(ret) && (deserialized_size < buf_len)) {
      ObSSMicroBlockIndex micro_index;
      int64_t pos = 0;
      if (OB_FAIL(micro_index.deserialize(buf + deserialized_size, buf_len - deserialized_size, pos))) {
        LOG_WARN("fail to deserialize micro_index", KR(ret), K(deserialized_size), K(buf_len), K(pos));
      } else if (OB_FAIL(micro_indexs.push_back(micro_index))) {
        LOG_WARN("fail to push back micro_index", KR(ret), K(micro_index));
      } else {
        deserialized_size += pos;
      }
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_phy_block_common_header(
    const char *buf, 
    const int64_t buf_len, 
    ObSSPhyBlockCommonHeader &common_header)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < common_header.header_size_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(common_header));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(common_header.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize common header", KR(ret), KP(buf), K(buf_len), K(common_header));
    } else if (OB_UNLIKELY(!common_header.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block common header is invalid", KR(ret), K(common_header));
    } else if (OB_FAIL(common_header.check_payload_checksum(buf + common_header.header_size_,
               common_header.payload_size_))) {
      LOG_WARN("fail to check payload checksum", KR(ret), K(common_header), KP(buf));
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_micro_data_block_header(
    const char *buf, 
    const int64_t buf_len, 
    ObSSMicroDataBlockHeader &header)
{
  int ret = OB_SUCCESS;
  const int64_t header_size = header.get_serialize_size();
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < header_size)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(header_size));
  } else {
    int64_t pos = 0;
    if (OB_FAIL(header.deserialize(buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize phy_block header", KR(ret), KP(buf), K(buf_len));
    } else if (OB_UNLIKELY(!header.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_block_header is invalid", KR(ret), K(header));
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::dump_phy_block(
    char *buf, 
    const int64_t buf_len, 
    const char *file_path, 
    const bool only_index,
    const bool is_micro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len <= 0) || OB_ISNULL(file_path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len), K(file_path));
  } else {
    ObArenaAllocator allocator;
    int64_t pos = 0;
    char *dest_buf = nullptr;
    const int64_t dest_len = buf_len * 12; // reserve enough space for parsing phy_block.
    ObSSPhyBlockCommonHeader common_hdr;
    if (OB_ISNULL(dest_buf = static_cast<char *>(allocator.alloc(dest_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc memory", KR(ret), K(dest_len));
    } else if (OB_FAIL(ObSSMicroCacheUtil::parse_phy_block_common_header(buf, buf_len, common_hdr))) {
      LOG_WARN("fail to parse phy_blk common header", KR(ret), KP(buf), K(buf_len));
    } else if (OB_UNLIKELY(!common_hdr.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("phy_blk common header is invalid", KR(ret), K(common_hdr));
    } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, "ObSSPhyBlockCommonHeader: ", 
               common_hdr, "\n"))) {
      LOG_WARN("fail to write phy_blk common header", KR(ret), KP(dest_buf), K(dest_len), K(pos), K(common_hdr));
    } else {
      if (common_hdr.is_super_blk()) {
        if (OB_FAIL(parse_ss_super_block(dest_buf, dest_len, buf, buf_len, pos))) {
          LOG_WARN("fail to parse ss_super_block", KR(ret), KP(dest_buf), K(dest_len), KP(buf), K(buf_len), K(pos));
        }
      } else if (common_hdr.is_micro_data_blk()) {
        if (OB_FAIL(parse_micro_data_block(dest_buf, dest_len, buf, buf_len, pos, only_index))) {
          LOG_WARN("fail to parse normal_block", KR(ret), KP(dest_buf), K(dest_len), KP(buf), K(buf_len), K(pos));
        }
      } else if (common_hdr.is_ckpt_blk()) {
        if (OB_FAIL(parse_checkpoint_block(dest_buf, dest_len, buf, buf_len, pos, is_micro_meta))) {
          LOG_WARN("fail to parse checkpoint_block", KR(ret), KP(dest_buf), K(dest_len), KP(buf), K(buf_len), K(pos), 
            K(is_micro_meta));
        }
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("unknow phy_block type", KR(ret), K(common_hdr));
      }
    }

    ObIOFd fd;
    int64_t written_size = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObIODeviceLocalFileOp::open(file_path, O_RDWR | O_CREAT | O_TRUNC, S_IWUSR | S_IRUSR, fd))) {
      LOG_WARN("fail to open file", KR(ret), K(file_path));
    } else if (OB_FAIL(ObIODeviceLocalFileOp::write(fd, dest_buf, pos, written_size))) {
      LOG_WARN("fail to write phy_block content", KR(ret), K(fd), KP(dest_buf), K(pos), K(written_size));
    } else if (OB_UNLIKELY(written_size != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("write_size is wrong", KR(ret), K(written_size), K(pos));
    }
    int tmp_ret = OB_SUCCESS;
    if (fd.is_valid() && OB_TMP_FAIL(ObIODeviceLocalFileOp::close(fd))) {
      ret = (OB_SUCC(ret)) ? tmp_ret : ret;
      LOG_WARN("fail to close file", KR(ret), KR(tmp_ret), K(fd));
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_checkpoint_block(
    char *dest_buf, 
    const int64_t dest_len, 
    char *src_buf,
    const int64_t src_len, 
    int64_t &pos,
    const bool is_micro_meta)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_buf) || OB_ISNULL(src_buf) || OB_UNLIKELY((dest_len <= 0) || (src_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dest_buf), KP(src_buf), K(dest_len), K(src_len));
  } else {
    int64_t src_buf_pos = 0;
    ObSSPhyBlockCommonHeader common_header;
    ObSSCkptBlockHeader ckpt_header;
    const int64_t common_hdr_size = common_header.get_serialize_size();
    const int64_t ckpt_hdr_size = ckpt_header.get_serialize_size();
    if (OB_FAIL(ckpt_header.deserialize(src_buf + common_hdr_size, src_len - common_hdr_size, src_buf_pos))) {
      LOG_WARN("fail to deserialize ObSSCkptBlockHeader", KR(ret), KP(src_buf), K(src_len), K(src_buf_pos));
    } else if (OB_UNLIKELY(!ckpt_header.is_valid() || ckpt_hdr_size != src_buf_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ckpt_header is invalid or deserialized pos unexpected", KR(ret), K(ckpt_header), K(src_buf_pos),
        K(ckpt_hdr_size));
    } else {
      ObArenaAllocator allocator;
      const int64_t def_blk_size = 2 * 1024 * 1024L;
      const int64_t item_cnt = ckpt_header.item_count_;
      const int64_t payload_size = ckpt_header.get_payload_size();
      const int64_t payload_offset = common_hdr_size + ckpt_hdr_size;
      char *data_buf = src_buf + payload_offset;
      ObSSCkptPhyBlockItemReader item_reader;
      ObSSMicroBlockMeta *micro_meta = nullptr;
      if (OB_FAIL(item_reader.init(data_buf, payload_size, def_blk_size))) {
        LOG_WARN("fail to init item_reader", KR(ret), KP(data_buf), K(payload_size), K(def_blk_size));
      } else if (OB_ISNULL(micro_meta = static_cast<ObSSMicroBlockMeta *>(allocator.alloc(sizeof(ObSSMicroBlockMeta))))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", KR(ret), K(sizeof(ObSSMicroBlockMeta)));
      }
      for (int64_t i = 0; OB_SUCC(ret) && (i < item_cnt); ++i) {
        char *item_buf = nullptr;
        int64_t item_buf_len = 0;
        int64_t item_buf_pos = 0;
        if (OB_FAIL(item_reader.get_next_item(item_buf, item_buf_len))) {
          LOG_WARN("fail to get next item", KR(ret), K(i), K(item_cnt));
        } else if (is_micro_meta) {
          micro_meta->reset();
          if (OB_FAIL(micro_meta->deserialize(item_buf, item_buf_len, item_buf_pos))) {
            LOG_WARN("fail to deserialize micro_meta", KR(ret), K(item_buf_len), KP(item_buf));
          } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, *micro_meta, "\n"))) {
            LOG_WARN("fail to write micro_meta", KR(ret), KP(dest_buf), K(dest_len), K(pos), K(*micro_meta));
          }
        } else {
          ObSSPhyBlockInfoCkptItem blk_ckpt_item;
          if (OB_FAIL(blk_ckpt_item.deserialize(item_buf, item_buf_len, item_buf_pos))) {
            LOG_WARN("fail to deserialize phy_block ckpt_item", KR(ret), K(item_buf_len), KP(item_buf));
          } else if (OB_UNLIKELY(!blk_ckpt_item.is_valid())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("phy_block ckpt_item should be valid", KR(ret), K(blk_ckpt_item));
          } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, blk_ckpt_item, "\n"))) {
            LOG_WARN("fail to write phy_block ckpt item", KR(ret), KP(dest_buf), K(dest_len), K(pos), K(blk_ckpt_item));
          }
        }
      }
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_ss_super_block(
    char *dest_buf, 
    const int64_t dest_len, 
    const char *src_buf, 
    const int64_t src_len, 
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_buf) || OB_ISNULL(src_buf) || OB_UNLIKELY((dest_len <= 0) || (src_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dest_buf), KP(src_buf), K(dest_len), K(src_len));
  } else {
    ObSSPhyBlockCommonHeader common_header;
    ObSSMicroCacheSuperBlk ss_super_blk;
    int64_t src_buf_pos = 0;
    const int64_t common_hdr_size = common_header.get_serialize_size();
    if (OB_FAIL(ss_super_blk.deserialize(src_buf + common_hdr_size, src_len - common_hdr_size, src_buf_pos))) {
      LOG_WARN("fail to deserialize ss_super_blk", KR(ret), KP(src_buf), K(src_len), K(src_buf_pos));
    } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, ss_super_blk, "\n"))) {
      LOG_WARN("fail to write ss_super_blk", KR(ret), KP(dest_buf), K(dest_len), K(pos), K(ss_super_blk));
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::parse_micro_data_block(
    char *dest_buf, 
    const int64_t dest_len, 
    const char *src_buf, 
    const int64_t src_len, 
    int64_t &pos,
    const bool only_index)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(dest_buf) || OB_ISNULL(src_buf) || OB_UNLIKELY(dest_len <= 0) || OB_UNLIKELY(src_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(dest_buf), KP(src_buf), K(dest_len), K(src_len));
  } else {
    ObSSPhyBlockCommonHeader common_header;
    ObSSMicroDataBlockHeader data_blk_header;
    const int64_t common_hdr_size = common_header.get_serialize_size();
    const int64_t blk_hdr_size = data_blk_header.get_serialize_size();
    ObSSPhyBlockHandle phy_blk_handle;
    ObSEArray<ObSSMicroBlockIndex, 64> micro_indexs;
    if (OB_FAIL(ObSSMicroCacheUtil::parse_micro_data_block_header(src_buf + common_hdr_size, blk_hdr_size, 
        data_blk_header))) {
      LOG_WARN("fail to parse micro_data phy_block header", KR(ret), KP(src_buf), K(src_len));
    } else if (OB_UNLIKELY(!data_blk_header.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data_blk_header is invalid", KR(ret), K(data_blk_header));
    } else if (OB_FAIL(ObSSMicroCacheUtil::parse_micro_block_indexs(src_buf + data_blk_header.micro_index_offset_,
               data_blk_header.micro_index_size_, micro_indexs))) {
      LOG_WARN("fail to parse micro_block indexs", KR(ret), K_(data_blk_header.micro_index_size), K(data_blk_header));
    } else if (OB_UNLIKELY(data_blk_header.micro_count_ != micro_indexs.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("micro_block count mismatch", KR(ret), K_(data_blk_header.micro_count), "micro_idx_cnt", 
        micro_indexs.count());
    } else {
      int32_t micro_data_offset = data_blk_header.payload_offset_;
      if (OB_FAIL(databuff_printf(dest_buf, dest_len, pos, "micro_blk_cnt=%ld\n", micro_indexs.count()))) {
        LOG_WARN("fail to write mciro_blk_cnt", KR(ret), K(dest_len), K(pos));
      }
      for (int64_t i = 0; OB_SUCC(ret) && (i < micro_indexs.count()); ++i) {
        const ObSSMicroBlockIndex &micro_index = micro_indexs.at(i);
        if (OB_UNLIKELY(!micro_index.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("phy_block micro index should be valid", KR(ret), K(micro_index));
        } else if (OB_FAIL(databuff_print_multi_objs(dest_buf, dest_len, pos, "\nmicro_key=", micro_index.get_micro_key(),
                   ", size=", micro_index.get_size(), "\n"))) {
          LOG_WARN("fail to write micro index", KR(ret), K(dest_len), K(pos), K(micro_index));
        } else if (!only_index && OB_FAIL(databuff_memcpy(dest_buf, dest_len, pos, micro_index.get_size(), 
                   src_buf + micro_data_offset))) {
          LOG_WARN("fail to write micro data", KR(ret), K(dest_len), K(pos), K(micro_index.get_size()));
        }
        micro_data_offset += micro_index.get_size();
      }
    }
  }
  return ret;
}

int ObSSMicroCacheUtil::calc_ls_tablet_cache_info(
    const uint64_t tenant_id,
    const ObSSMCTabletInfoMap &tablet_info_map,
    ObIArray<ObSSMicroCacheTabletInfo> &tablet_info_list,
    ObIArray<ObSSMicroCacheLSInfo> &ls_info_list)
{
  int ret = OB_SUCCESS;
  ObStorageCachePolicyService *storage_cache_policy_service = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(storage_cache_policy_service = MTL(ObStorageCachePolicyService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("storage cache policy service is null", KR(ret));
  }

  ObSSMCTabletInfoMap::const_iterator iter = tablet_info_map.begin();
  int64_t tablet_cnt = 0;
  bool is_hot_tablet = false;
  for (; OB_SUCC(ret) && (iter != tablet_info_map.end()); ++iter) {
    ++tablet_cnt;
    is_hot_tablet = false;
    const ObTabletID &tablet_id = iter->first;
    if (tablet_id.is_valid()) {
      if (OB_FAIL(tablet_info_list.push_back(iter->second))) {
        LOG_WARN("fail to push back", KR(ret), K(iter->first), K(iter->second), K(tablet_cnt));
      } else if (OB_FAIL(storage_cache_policy_service->is_hot_tablet(tablet_id.id(), is_hot_tablet))) {
        LOG_WARN("fail to check is hot tablet", KR(ret), K(tablet_id));
      }
    }

    const ObLSID &ls_id = iter->second.ls_id_;
    if (OB_SUCC(ret) && ls_id.is_valid()) {
      int64_t ls_idx = -1;
      const int64_t ls_cnt = ls_info_list.count();
      for (int64_t i = 0; OB_SUCC(ret) && (i < ls_cnt) && (-1 == ls_idx); ++i) {
        if (ls_id == ls_info_list.at(i).ls_id_) {
          ls_idx = i;
        }
      }

      if (-1 == ls_idx) {
        ObSSMicroCacheLSInfo ls_cache_info;
        ls_cache_info.ls_id_ = ls_id;
        ls_cache_info.t1_size_ = iter->second.t1_size_;
        ls_cache_info.t2_size_ = iter->second.t2_size_;
        ls_cache_info.tablet_cnt_ = 1;
        ls_cache_info.t1_hot_size_ = (is_hot_tablet ? iter->second.t1_size_ : 0);
        ls_cache_info.t2_hot_size_ = (is_hot_tablet ? iter->second.t2_size_ : 0);
        if (OB_FAIL(ls_info_list.push_back(ls_cache_info))) {
          LOG_WARN("fail to push back", KR(ret), K(ls_cache_info), "tablet_cache_info", iter->second);
        }
      } else {
        ObSSMicroCacheLSInfo &ls_cache_info = ls_info_list.at(ls_idx);
        ls_cache_info.t1_size_ += iter->second.t1_size_;
        ls_cache_info.t2_size_ += iter->second.t2_size_;
        ls_cache_info.tablet_cnt_ += 1;
        if (is_hot_tablet) {
          ls_cache_info.t1_hot_size_ += iter->second.t1_size_;
          ls_cache_info.t2_hot_size_ += iter->second.t2_size_;
        }
      }
    }
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSSMicroCacheUtil::get_ls_handles(ObIArray<ObLSHandle> &ls_handles)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService *);
  ObSEArray<ObLSID, 16> ls_ids;
  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls_service should not be null", KR(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls_ids(ls_ids))) {
    LOG_WARN("fail to get ls ids", KR(ret));
  } else {
    ObLSHandle ls_handle;
    for (int64_t i = 0; OB_SUCC(ret) && (i < ls_ids.count()); ++i) {
      ls_handle.reset();
      if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_ids.at(i), ls_handle, ObLSGetMod::SS_MICRO_CACHE_MOD))) {
        LOG_WARN("fail to get ls", KR(ret), K(i), K(ls_ids.at(i).id()));
      } else if (OB_FAIL(ls_handles.push_back(ls_handle))) {
        LOG_WARN("fail to push back", KR(ret), K(i));
      }
    }
  }
  return ret;
}

void ObSSMicroCacheUtil::get_ls_id_by_tablet_id(
    const uint64_t tenant_id, 
    const ObTabletID &tablet_id,
    const ObIArray<ObLSHandle> &ls_handles,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id = SYS_LS;
  const int64_t ls_handle_cnt = ls_handles.count();
  // skip reserved tablet
  if (tablet_id.is_valid_with_tenant(tenant_id) && !tablet_id.is_reserved_tablet() && (ls_handle_cnt > 0)) {
    bool is_exist = false;
    ObLSTabletService *tablet_svr = nullptr;
    for (int64_t i = 0; (i < ls_handle_cnt) && !is_exist; ++i) {
      const ObLSHandle &ls_handle = ls_handles.at(i);
      ObLSID cur_ls_id;
      if (OB_ISNULL(ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", KR(ret), K(i), K(ls_handle_cnt), K(ls_handle));
      } else if (FALSE_IT(cur_ls_id = ls_handle.get_ls()->get_ls_id())) {
      } else if (OB_ISNULL(tablet_svr = ls_handle.get_ls()->get_tablet_svr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get ls tablet svr", KR(ret), K(i), K(cur_ls_id));
      } else if (OB_FAIL(tablet_svr->is_tablet_exist(tablet_id, is_exist))) {
        LOG_WARN("fail to judge is tablet exist", KR(ret), K(tablet_id), K(cur_ls_id));
      } else if (is_exist) {
        ls_id = cur_ls_id;
      } else {
        ls_id.reset();
      }
      ret = OB_SUCCESS; // continue to check next
    }
  }
}

int64_t ObSSMicroCacheUtil::calc_max_micro_blk_size(const uint64_t tenant_id)
{
  UNUSED(tenant_id);
  return SS_MICRO_CACHE_MAX_BLOCK_SIZE;
}

int64_t ObSSMicroCacheUtil::calc_micro_cache_mem_limit_size(const uint64_t tenant_id)
{
  const int64_t DEFAULT_CACHE_MEM_PCT = 20;
  int64_t micro_meta_mem_pct = DEFAULT_CACHE_MEM_PCT;
  micro_meta_mem_pct = is_meta_tenant(tenant_id) ? 
                       (micro_meta_mem_pct / 2) :
                       micro_meta_mem_pct;
  const int64_t tnt_mem_size = MTL_MEM_SIZE();
  return tnt_mem_size * (micro_meta_mem_pct / 100.0);
}

int64_t ObSSMicroCacheUtil::calc_micro_cache_expiration_time(const uint64_t tenant_id)
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t expiration_time_s = tenant_config.is_valid() 
                              ? (tenant_config->_local_cache_expiration_time / 1000000L)
                              : 0L;
  if (0L == expiration_time_s) {
    expiration_time_s = SS_DEF_CACHE_EXPIRATION_TIME_S;
  }
  return expiration_time_s;
}

int64_t ObSSMicroCacheUtil::calc_micro_cache_arc_limit_pct(const uint64_t tenant_id)
{
  UNUSED(tenant_id);
  return SS_ARC_LIMIT_PCT;
}

uint32_t ObSSMicroCacheUtil::calc_micro_ckpt_split_cnt(
    const bool is_mini_cache,
    const int64_t meta_blk_min_cnt)
{
  const int64_t meta_ckpt_blk_cnt = meta_blk_min_cnt / 2;
  uint32_t micro_ckpt_split_cnt = 0;
  if (is_mini_cache) {
    micro_ckpt_split_cnt = MIN(SS_PERSIST_MICRO_CKPT_SPLIT_SMALL_CNT, meta_ckpt_blk_cnt);
  } else {
    micro_ckpt_split_cnt = MIN(SS_PERSIST_MICRO_CKPT_SPLIT_CNT, meta_ckpt_blk_cnt);
  }
  return micro_ckpt_split_cnt;
}

uint32_t ObSSMicroCacheUtil::calc_init_range_cnt(const int64_t micro_cnt_limit)
{
  uint32_t init_rng_cnt = SS_INIT_RNG_MIN_CNT;
  if (micro_cnt_limit > 0 && SS_INIT_RNG_DEF_META_CNT > 0) {
    const int64_t delta = ((micro_cnt_limit % SS_INIT_RNG_DEF_META_CNT > 0) ? 1 : 0);
    init_rng_cnt = MIN(MAX(init_rng_cnt, (micro_cnt_limit / SS_INIT_RNG_DEF_META_CNT + delta)), SS_INIT_RNG_MAX_CNT);
  }
  return init_rng_cnt;
}

int64_t ObSSMicroCacheUtil::calc_micro_cnt_limit(const uint64_t tenant_id, const int64_t mem_limit)
{
  int ret = OB_SUCCESS;
  int64_t micro_cnt_limit = 0;
  int64_t micro_cnt_per_block = 0;
  ObSmallAllocator *tmp_allocator = nullptr;
  void *mem_buffer = nullptr;
  ObArray<void *> slice_arr;
  const int64_t micro_meta_size = sizeof(ObSSMicroBlockMeta);
  const int64_t MAX_MICRO_CNT =  SS_META_ALLOCATOR_BLOCK_SIZE / micro_meta_size;

  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || mem_limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id),K(mem_limit));
  } else if (OB_ISNULL(mem_buffer = ob_malloc(sizeof(ObSmallAllocator), ObMemAttr(tenant_id, "CalMicroLimit")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(sizeof(ObSmallAllocator)));
  } else if (OB_FALSE_IT(tmp_allocator = new (mem_buffer) ObSmallAllocator())) {
  } else if (OB_FAIL(tmp_allocator->init(micro_meta_size, "CalMicroLimit", tenant_id, SS_META_ALLOCATOR_BLOCK_SIZE))) {
    LOG_WARN("fail to init ObSmallAllocator", KR(ret), K(micro_meta_size), K(tenant_id));
  } else {
    tmp_allocator->set_total_limit(SS_META_ALLOCATOR_BLOCK_SIZE);
    int64_t micro_cnt = 0;
    while (OB_SUCC(ret) && micro_cnt <= MAX_MICRO_CNT) {
      void *ptr = tmp_allocator->alloc();
      if (OB_ISNULL(ptr)) {
        break;
      } else if (OB_FAIL(slice_arr.push_back(ptr))) {
        LOG_WARN("fail to push", KR(ret), KP(ptr));
      } else {
        micro_cnt++;
      }
    }
    
    if (OB_SUCC(ret)) {
      if (micro_cnt > 0 && micro_cnt <= MAX_MICRO_CNT) {
        micro_cnt_per_block = micro_cnt;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("fail to test block capacity", KR(ret), K(tenant_id), K(micro_cnt), K(MAX_MICRO_CNT));
      }
    }
  }

  if (OB_NOT_NULL(tmp_allocator)) {
    for (int64_t i = 0; i < slice_arr.count(); ++i) {
      void *ptr = slice_arr[i];
      if (OB_NOT_NULL(ptr)) {
        tmp_allocator->free(ptr);
      }
    }
    tmp_allocator->destroy();
    ob_free(tmp_allocator);
  }

  if (SS_META_ALLOCATOR_BLOCK_SIZE > 0) {
    micro_cnt_per_block = (micro_cnt_per_block > 0) ? micro_cnt_per_block : SS_DEF_MICRO_META_CNT_PER_8KB;
    micro_cnt_limit = (mem_limit / SS_META_ALLOCATOR_BLOCK_SIZE) * micro_cnt_per_block;
    LOG_INFO("finish calculate micro_cnt_limit", KR(ret), K(tenant_id), K(micro_cnt_per_block), K(micro_cnt_limit));
  }

  return micro_cnt_limit;
}

int ObSSMicroCacheUtil::parse_range_micro_metas(
    const uint64_t tenant_id,
    const char *buf, 
    const int64_t buf_size,
    const int32_t blk_size,
    common::ObIArray<ObSSMicroBlockMetaInfo> &parsed_micro_infos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || blk_size <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(buf_size), K(blk_size), KP(buf));
  } else {
    int64_t pos = 0;
    ObSSCkptPhyBlockSegmentHeader seg_header;
    if (OB_FAIL(seg_header.deserialize(buf, buf_size, pos))) {
      LOG_WARN("fail to deserialize segment header", KR(ret), KP(buf), K(buf_size));
    } else if (OB_UNLIKELY(pos < 0 || seg_header.header_size_ != pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deserialized pos unexpected", KR(ret), K(seg_header), K(pos), K(buf_size), KP(buf));
    } else if (OB_UNLIKELY(!seg_header.is_valid() || pos >= buf_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("segment header should be valid", KR(ret), K(pos), KP(buf), K(buf_size), K(seg_header));
    } else {
      DefaultPageAllocator allocator("RngMicroBuf", tenant_id);

      const char *data_buf = buf + pos;
      const int64_t ori_payload_size = seg_header.get_payload_size();
      const int64_t real_payload_size = seg_header.get_real_payload_size();
      if (OB_UNLIKELY((ori_payload_size >= blk_size) || (real_payload_size >= blk_size) || 
          (real_payload_size > (buf_size - pos)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected seg_header", KR(ret), KP(buf), K(blk_size), K(buf_size), K(pos), K(ori_payload_size),
          K(real_payload_size), K(seg_header));
      } else if (seg_header.need_compress()) {
        const ObCompressorType cur_type = static_cast<ObCompressorType>(seg_header.get_compressor_type());
        char *decomp_buf = nullptr;
        ObCompressor *compressor = nullptr;
        int64_t decompress_size = 0;
        if (OB_ISNULL(decomp_buf = static_cast<char *>(allocator.alloc(ori_payload_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret), K(ori_payload_size));
        } else if (OB_FAIL(ObCompressorPool::get_instance().get_compressor(cur_type, compressor))) {
          LOG_WARN("fail to get compressor", KR(ret), K(cur_type));
        } else if (OB_ISNULL(compressor)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("compressor should not be null", KR(ret), KP(compressor), K(cur_type));
        } else if (OB_FAIL(compressor->decompress(buf + pos, real_payload_size, decomp_buf, ori_payload_size, 
                   decompress_size))) {
          LOG_WARN("fail to decompress", KR(ret), K(cur_type), K(pos), KP(compressor), KP(buf), KP(decomp_buf),
            K(seg_header));
        } else if (OB_UNLIKELY(decompress_size != ori_payload_size)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected decompress size", KR(ret), K(decompress_size), K(ori_payload_size), K(seg_header));
        } else {
          data_buf = decomp_buf;
        }
      }

      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(data_buf)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("data_buf should not be null", KR(ret), KP(data_buf));
      } else {
        ObSSCkptPhyBlockItemHeader item_header;
        ObSSMicroBlockMetaInfo micro_info;
        int64_t item_pos = 0;
        int64_t prev_item_pos = 0;
        while (OB_SUCC(ret) && (item_pos < ori_payload_size)) {
          item_header.reset();
          micro_info.reset();
          if (OB_FAIL(item_header.deserialize(data_buf, ori_payload_size, item_pos))) {
            LOG_WARN("fail to deserialize item header", KR(ret), K(ori_payload_size), K(item_pos), KP(data_buf),
              K(parsed_micro_infos.count()), KP(buf), K(buf_size), K(seg_header));
          } else if (FALSE_IT(prev_item_pos = item_pos)) {
          } else if (OB_FAIL(micro_info.deserialize(data_buf, ori_payload_size, item_pos))) {
            LOG_WARN("fail to deserialize micro_info", KR(ret), K(ori_payload_size), K(item_pos), KP(data_buf),
              K(parsed_micro_infos.count()), KP(buf), K(buf_size), K(seg_header));
          } else if (OB_UNLIKELY(item_pos - prev_item_pos != item_header.payload_size_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("item header payload size is unexpected", KR(ret), K(item_pos), K(prev_item_pos), K(item_header),
              K(micro_info), K(parsed_micro_infos.count()));
          } else if (OB_FAIL(parsed_micro_infos.push_back(micro_info))) {
            LOG_WARN("fail to push back", KR(ret), K(micro_info), K(parsed_micro_infos.count()));
          }
        }
      }
    }
  }
  return ret;
}

bool ObSSMicroCacheUtil::contain_micro_key(
     const ObIArray<ObSSMicroBlockMetaInfo> &micro_infos,
     const ObSSMicroBlockCacheKey &micro_key,
     int64_t &micro_info_idx)
{
  bool is_contained = false;
  int ret = OB_SUCCESS;
  micro_info_idx = -1;
  const int64_t micro_info_cnt = micro_infos.count();
  if (OB_LIKELY(micro_key.is_valid() && (micro_info_cnt > 0))) {
    for (int64_t i = 0; (!is_contained) && (i < micro_info_cnt); ++i) {
      is_contained = (micro_key == micro_infos.at(i).get_micro_key());
      if (is_contained) {
        micro_info_idx = i;
      }
    }
  }
  return is_contained;
}

int ObSSMicroCacheUtil::get_micro_ckpt_split_cnt(uint32_t &cur_ckpt_split_cnt)
{
  int ret = OB_SUCCESS;
  ObSSMicroCache *micro_cache = nullptr;
  if (OB_ISNULL(micro_cache = MTL(ObSSMicroCache *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("micro_cache is null", KR(ret), KP(micro_cache));
  } else {
    cur_ckpt_split_cnt = micro_cache->get_micro_ckpt_split_cnt();
  }
  return ret;
}

bool ObSSMicroCacheUtil::should_delete_tablet_record(
     const int64_t recorded_ckpt_op_cnt,
     const int64_t cur_micro_ckpt_op_cnt,
     const int64_t meta_ckpt_split_cnt)
{
  // Check if recorded_ckpt_op_cnt <= cur_micro_ckpt_op_cnt - meta_ckpt_split_cnt - 1
  // This means one full round of meta ckpt and an extra meta ckpt has been completed after the deletion request
  return (recorded_ckpt_op_cnt <= cur_micro_ckpt_op_cnt - meta_ckpt_split_cnt - 1);
}

int ObSSMicroCacheUtil::check_data_crc(const char *data, const int64_t size, const uint32_t crc)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(data) || OB_UNLIKELY(size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(data), K(size));
  } else {
    const uint32_t real_crc = static_cast<uint32_t>(ob_crc64(data, size));
    if (OB_UNLIKELY(real_crc != crc)) {
      ret = OB_CHECKSUM_ERROR;
      LOG_ERROR("data checksum error", KR(ret), K(real_crc), K(crc), K(size), KP(data));
    }
  }
  return ret;
}

bool ObSSMicroCacheUtil::check_mini_cache(const int64_t cache_file_size)
{
  return (cache_file_size <= SS_MINI_MODE_CACHE_FILE_SIZE);
}

} // namespace storage
} // namespace oceanbase
