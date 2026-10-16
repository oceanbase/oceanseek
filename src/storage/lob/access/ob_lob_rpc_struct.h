/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OB_LOB_RPC_STRUCT_
#define OCEANBASE_OB_LOB_RPC_STRUCT_

#include "common/object/ob_object.h"
#include "share/ob_define.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/ob_rpc_struct.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{

namespace obrpc
{

struct ObLobQueryBlock
{
  OB_UNIS_VERSION(1);
public:
  ObLobQueryBlock();
  virtual ~ObLobQueryBlock() {}
  void reset();
  bool is_valid() const;

  TO_STRING_KV(K_(size));
  uint64_t size_;
};

class ObLobQueryArg final
{
  OB_UNIS_VERSION(1);
public:
  enum QueryType {
    READ = 0,
    GET_BYTE_LENGTH,
    GET_CHAR_LENGTH,
    GET_LOB_SUMMARY_INFO
  };
  ObLobQueryArg();
  ~ObLobQueryArg();
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(offset), K_(len), K_(cs_type), K_(qtype), K_(scan_backward), K_(lob_locator),
      K_(enable_remote_retry));
public:
  static const int64_t OB_LOB_QUERY_BUFFER_LEN = 256 * 1024L;
  static const int64_t OB_LOB_QUERY_OLD_LEN_REFACTOR = 8;
  uint64_t tenant_id_;
  uint64_t offset_; // char offset
  uint64_t len_; // char len
  common::ObCollationType cs_type_;
  bool scan_backward_;
  QueryType qtype_;
  ObLobLocatorV2 lob_locator_;
  bool enable_remote_retry_;
  DISALLOW_COPY_AND_ASSIGN(ObLobQueryArg);
};

} // obrpc

} // oceanbase

#endif // OCEANBASE_OB_LOB_RPC_STRUCT_
