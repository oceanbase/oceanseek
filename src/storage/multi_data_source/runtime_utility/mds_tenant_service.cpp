/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "mds_tenant_service.h"
#include "observer/virtual_table/ob_mds_event_buffer.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tablet/ob_tablet_iterator.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
namespace mds
{
int ObTenantMdsService::mtl_init(ObTenantMdsService *&mds_service)
{
  int ret = OB_SUCCESS;
  MDS_TG(10_ms);
  if (mds_service->is_inited_) {
    ret = OB_INIT_TWICE;
    MDS_LOG(ERROR, "init mds tenant service twice!", KR(ret), KPC(mds_service));
  } else {
    mds_service->is_inited_ = true;
  }
  return ret;
}

int ObTenantMdsService::mtl_start(ObTenantMdsService *&mds_service)
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObTenantMdsService::mtl_stop(ObTenantMdsService *&mds_service)
{
}

void ObTenantMdsService::mtl_wait(ObTenantMdsService *&mds_service)
{
}

int ObTenantMdsService::for_each_ls_in_tenant(const ObFunction<int(ObLS &)> &op)
{
  #define PRINT_WRAPPER KR(ret)
  int ret = OB_SUCCESS;
  MDS_TG(3_s);
  ObSharedGuard<ObLSIterator> iter;
  ObLS *ls = nullptr;
  int64_t succ_num = 0;
  if (!op.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_NONE(WARN, "invalid op");
  } else if (MDS_FAIL(MTL(ObLSService*)->get_ls_iter(iter, ObLSGetMod::MDS_TABLE_MOD))) {
    MDS_LOG_NONE(WARN, "fail to get ls iterator");
  } else {
    do {
      if (MDS_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END != ret) {
          MDS_LOG_NONE(WARN, "get next iter failed");
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (MDS_FAIL(op(*ls))) {
        MDS_LOG_NONE(WARN, "fail to for each ls", K(succ_num));
      } else {
        MDS_LOG_NONE(DEBUG, "succeed to operate one ls", K(ret), "ls_id", ls->get_ls_id());
      }
    } while (++succ_num && OB_SUCC(ret));
  }
  MDS_LOG_NONE(INFO, "for each ls", K(succ_num));
  return ret;
  #undef PRINT_WRAPPER
}

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase
