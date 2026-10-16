/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBSE_LOG_SHARED_LOG_UPLOAD_HANDLER_
 #define OCEANBSE_LOG_SHARED_LOG_UPLOAD_HANDLER_

#include "lib/hash/ob_link_hashmap.h"
#include "share/ob_ls_id.h"
#include "logservice/palf/log_define.h"
namespace oceanbase
{
namespace logservice
{
struct LogUploadCtx
{
public:
  LogUploadCtx() { reset(); }
  ~LogUploadCtx() { reset(); }
public:
  void reset();
  bool is_valid() const;
  bool operator==(const LogUploadCtx &ctx) const;
  int update(const LogUploadCtx &other);
  int get_next_block_to_upload(palf::block_id_t &to_upload_block_id) const;
  int after_upload_file(const palf::block_id_t uploaded_block_id);
  bool need_locate_upload_range() const;

  TO_STRING_KV(K(has_file_on_ss_),
               K(max_block_id_on_ss_),
               K(start_block_id_),
               K(end_block_id_));
public:
  bool has_file_on_ss_;
  palf::block_id_t max_block_id_on_ss_;
  palf::block_id_t start_block_id_;
  palf::block_id_t end_block_id_;
};

struct ObSharedLogUploadHandler : public common::LinkHashValue<palf::LSKey>
{
public:
  ObSharedLogUploadHandler();
  ~ObSharedLogUploadHandler();
public:
  void reset();
  int init(const share::ObLSID &id);
  share::ObLSID get_ls_id() const { return ls_id_; }
  int get_log_upload_ctx(LogUploadCtx &ctx) const;
  int update_log_upload_ctx(const LogUploadCtx &ctx);
  void reset_upload_ctx() {log_upload_ctx_.reset();}
  int after_upload_file(const palf::block_id_t uploaded_block_id);
  TO_STRING_KV(K(is_inited_), K(ls_id_), K(log_upload_ctx_))
public:
  bool is_inited_;
  share::ObLSID ls_id_;
  LogUploadCtx log_upload_ctx_;
};

}//end of namespace logservice
}//end of namespace oceanbase
#endif
