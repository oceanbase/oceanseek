/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TENANT_SERVICE_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TENANT_SERVICE_H

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/allocator/ob_vslice_alloc.h"
#include "share/ob_ls_id.h"
#include "lib/function/ob_function.h"
#include "share/allocator/ob_mds_allocator.h"

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace storage
{
class ObLS;
namespace mds
{
class ObTenantMdsService;

class ObTenantMdsService
{
public:
  ObTenantMdsService() : is_inited_(false) {}
  ~ObTenantMdsService() {
    MDS_LOG_RET(INFO, OB_SUCCESS, "ObTenantMdsService destructed");
  }
  static int mtl_init(ObTenantMdsService* &);
  static int mtl_start(ObTenantMdsService* &);
  static void mtl_stop(ObTenantMdsService* &);
  static void mtl_wait(ObTenantMdsService* &);
  void destroy() { this->~ObTenantMdsService(); }
  static int for_each_ls_in_tenant(const common::ObFunction<int(ObLS &)> &op);
  share::ObTenantBufferCtxAllocator &get_buffer_ctx_allocator() { return buffer_ctx_allocator_; }
  TO_STRING_KV(KP(this), K_(is_inited))
private:
  bool is_inited_;
  share::ObTenantBufferCtxAllocator buffer_ctx_allocator_;
};

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase


#endif