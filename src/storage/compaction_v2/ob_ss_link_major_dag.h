// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_STORAGE_COMPACTION_SS_LINK_MAJOR_DAG_H_
#define OB_STORAGE_COMPACTION_SS_LINK_MAJOR_DAG_H_

#include "storage/compaction/ob_tablet_merge_task.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"

namespace oceanbase
{
namespace compaction
{

class ObLinkSSMajorSSTableDag : public share::ObIDag
{
public:
  ObLinkSSMajorSSTableDag()
    : ObIDag(share::ObDagType::ObDagTypeEnum::DAG_TYPE_LINK_MAJOR_SSTABLE),
      is_inited_(false),
      param_()
  {}
  virtual ~ObLinkSSMajorSSTableDag() = default;

  bool operator == (const ObIDag &other) const override;
  uint64_t hash() const override;
  int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int init_by_param(const share::ObIDagInitParam *param) override;
  int create_first_task() override;
  int fill_dag_key(char *buf, const int64_t buf_len) const override;
  lib::Worker::CompatMode get_compat_mode() const override { return compat_mode_; }
  uint64_t get_consumer_group_id() const override { return consumer_group_id_; }
  const share::ObLSID &get_ls_id() const { return param_.ls_id_; }
  INHERIT_TO_STRING_KV("ObIDag", ObIDag, K_(param), K_(is_inited));

private:
  bool is_inited_;
  ObTabletMergeDagParam param_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObLinkSSMajorSSTableDag);
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SS_LINK_MAJOR_DAG_H_

