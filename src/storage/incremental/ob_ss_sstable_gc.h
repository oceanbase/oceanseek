// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OCEANBASE_STORAGE_OB_SS_SSTABLE_GC_H_
#define OCEANBASE_STORAGE_OB_SS_SSTABLE_GC_H_
#include "storage/ls/ob_ls.h"
#include "share/ob_ls_id.h"
#include "share/ob_shared_meta_common.h"

namespace oceanbase {
namespace storage {
class ObSSSSTableGCProcessor : public common::ObTimerTask {
public:
    ObSSSSTableGCProcessor();
    ~ObSSSSTableGCProcessor();
    int start();
    int stop();
    void wait();
    void destroy();
    void runTimerTask() override;
private:
    int get_all_ls_ids_(ObIArray<ObLSID> &ls_ids);
    int loop_all_tablets_(ObLS &ls);
    int process_one_tablet_(ObLS &ls,
                            const ObTablet &tablet,
                            const ObSSTabletTableStoreMetaInfo &table_store_meta_info);
private:
    static const int64_t SS_SSTABLE_GC_INTERVAL = 300 * 1000 * 1000L; // 5mins
    int ss_sstable_gc_tg_id_;
    bool stopped_;
    bool processing_;
};

}
}
#endif