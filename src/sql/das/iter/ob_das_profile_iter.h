/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OBDEV_SRC_SQL_DAS_ITER_OB_DAS_PROFILE_ITER_H_
#define OBDEV_SRC_SQL_DAS_ITER_OB_DAS_PROFILE_ITER_H_

#include "common/ob_tablet_id.h"
#include "sql/das/iter/ob_das_iter.h"
#include "share/diagnosis/ob_runtime_profile.h"

namespace oceanbase
{
namespace sql
{

struct ObDASProfileIterParam : public ObDASIterParam
{
public:
  ObDASProfileIterParam(ObDASIter *child,
                        bool enable_profile,
                        common::ObTabletID tablet_id,
                        common::ObProfileId root_profile_id)
    : ObDASIterParam(ObDASIterType::DAS_ITER_PROFILE),
      enable_profile_(enable_profile),
      tablet_id_(tablet_id),
      root_profile_id_(root_profile_id)
  {
    if (OB_NOT_NULL(child)) {
      max_size_ = child->get_max_size();
      eval_ctx_ = child->get_eval_ctx();
      exec_ctx_ = child->get_exec_ctx();
      output_ = child->get_output();
    }
  }
  virtual ~ObDASProfileIterParam() {}

  virtual bool is_valid() const override
  {
    return ObDASIterParam::is_valid() && tablet_id_.is_valid();
  }
  bool is_enable_profile() const { return enable_profile_; }
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  common::ObProfileId get_root_profile_id() const { return root_profile_id_; }
  
private:
  bool enable_profile_;
  common::ObTabletID tablet_id_;
  common::ObProfileId root_profile_id_;
};

class ObDASProfileIter : public ObDASIter
{
public:
  ObDASProfileIter()
    : ObDASIter(ObDASIterType::DAS_ITER_PROFILE),
      enable_profile_(false),
      tablet_id_(),
      root_profile_id_(common::ObProfileId::PHY_INVALID),
      partition_profile_id_(common::ObProfileId::PHY_INVALID),
      tablet_metric_id_(common::ObMetricId::MONITOR_STATNAME_BEGIN),
      table_scan_profile_(nullptr),
      partition_profile_(nullptr),
      execution_profile_(nullptr)
  {}
  virtual ~ObDASProfileIter() {}

public:
  virtual int do_table_scan() override;
  virtual int rescan() override;
  virtual void clear_evaluated_flag() override;
  void set_tablet_id(const common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }

  static int init_runtime_profile(
      common::ObProfileId profile_id,
      common::ObOpProfile<common::ObMetric> *&profile_out);

protected:
  virtual int inner_init(ObDASIterParam &param) override;
  virtual int inner_reuse() override;
  virtual int inner_release() override;
  virtual int inner_get_next_row() override;
  virtual int inner_get_next_rows(int64_t &count, int64_t capacity) override;

private:
  int create_partition_profile();
  bool enable_profile_;
  common::ObTabletID tablet_id_;
  common::ObProfileId root_profile_id_;
  common::ObProfileId partition_profile_id_;
  common::ObMetricId tablet_metric_id_;
  common::ObOpProfile<common::ObMetric> *table_scan_profile_;
  common::ObOpProfile<common::ObMetric> *partition_profile_;
  common::ObOpProfile<common::ObMetric> *execution_profile_;
};

}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_ITER_OB_DAS_PROFILE_ITER_H_ */
