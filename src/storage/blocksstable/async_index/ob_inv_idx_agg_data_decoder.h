/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGG_DATA_DECODER_H_
#define OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGG_DATA_DECODER_H_

#include "ob_inv_idx_agg_data_encoder.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase
{
namespace blocksstable
{

// 读取由 ObInvIdxAggDataEncoder 生成的单条（微块 M, term T）索引行聚合数据。
//
// frontier_pairs_ 逐对物化到 meta 内（每块至多 MAX_FRONTIER_PAIR_COUNT 对）：
// 查询侧 block-max 剪枝与 compaction 逻辑块复用路径（源端 frontier 携带进输出
// meta）都依赖它；解码后的 metas 可安全传给使用
// ObInvIdxLogicalBlockMeta::is_valid() 的路径。
class ObInvIdxAggDataDecoder final
{
public:
  // 将聚合数据 [buf, buf + size) 解码为 metas（先清空，按 block_idx 升序），
  // 同时输出 M 的 posting value 微块定位信息。
  static int decode(const char *buf,
                    const int64_t size,
                    common::ObIArray<ObInvIdxLogicalBlockMeta> &metas,
                    int64_t &posting_value_offset,
                    int64_t &posting_value_size);

private:
  static constexpr int64_t HEADER_SIZE = static_cast<int64_t>(sizeof(ObInvIdxAggDataHeader));
  static constexpr int64_t ENTRY_SIZE = static_cast<int64_t>(sizeof(ObInvIdxAggDataEntry));
  static constexpr int64_t PAIR_SIZE = static_cast<int64_t>(sizeof(ObInvIdxBM25ParamPair));
  DISALLOW_COPY_AND_ASSIGN(ObInvIdxAggDataDecoder);
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_ASYNC_INDEX_OB_INV_IDX_AGG_DATA_DECODER_H_
