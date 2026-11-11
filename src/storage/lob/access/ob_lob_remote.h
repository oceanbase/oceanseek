/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_REMOTE_H_
#define OCEANBASE_STORAGE_OB_LOB_REMOTE_H_

#include "data_buffer.h"
#include "lib/allocator/page_arena.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "storage/ob_storage_rpc.h"
#include "storage/lob/access/ob_lob_rpc_struct.h"
#include "storage/lob/common/ob_lob_context.h"

namespace oceanbase
{
namespace storage
{

// @brief 
// ObLobRemoteQueryHandler 是 lob remote query 的 handler，用户通过 ObLobRemoteUtil 的 query 接口获取本次的查询句柄，进而获取查询结果。目前查询类型有：
//  - 读取：读取 lob 数据
//  - 获取字节长度：获取 lob 数据的字节长度
//  - 获取字符长度：获取 lob 数据的字符长度
//  - 获取 LOB Summary 信息：获取 lob meta 表中 seq_id=0 的 Summary 行
class ObLobRemoteQueryHandler
{
public:
  ObLobRemoteQueryHandler();
  ~ObLobRemoteQueryHandler() { reset(); };
  int init(const ObLobReadContext &read_ctx, const ObLobQueryArg::QueryType &qtype);
  void reset();
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> &get_sshandle() { return handle_; }
  ObDataBuffer &get_rpc_buffer() { return rpc_buffer_; }
  ObLobQueryArg &get_query_arg() { return query_arg_; }

public:
  int get_next_block(ObString &data);
  int get_byte_length(uint64_t &byte_length);
  int get_char_length(uint64_t &char_length); 
  int get_lob_summary_info(ObLobMetaInfo &lob_info);
private:
  int fetch_rpc_buffer_();
private:
  bool is_inited_;
  ObIAllocator *allocator_;
  // Owns only rpc_buffer_ and buffer_; its lifetime follows this handler.
  common::ObArenaAllocator rpc_allocator_;
  obrpc::ObStorageRpcProxy::SSHandle<obrpc::OB_LOB_QUERY> handle_;
  common::ObDataBuffer rpc_buffer_;
  ObLobQueryArg query_arg_;
  // rpc_buffer_pos_ indicated the content has been readed in rpc_buffer_, not the current length of rpc_buffer_
  int64_t rpc_buffer_pos_;
  ObString buffer_;
};

class ObLobRemoteUtil
{
public:
  static int query(const ObLobReadContext &read_ctx,
                   const ObLobQueryArg::QueryType qtype,
                   const ObAddr &dst_addr,
                   ObLobRemoteQueryHandler &handler);
};

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_REMOTE_H_
