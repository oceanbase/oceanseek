/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_RESTORE_OB_STORAGE_CONDITION_PUT_PROBE_H_
#define OCEANBASE_LIB_RESTORE_OB_STORAGE_CONDITION_PUT_PROBE_H_

#include "lib/restore/ob_storage_info.h"

namespace oceanbase
{
namespace common
{

class ObStorageObDalReader;
class ObStorageObDalWriter;

class ObStorageConditionPutProbe final
{
public:
  ObStorageConditionPutProbe();
  ~ObStorageConditionPutProbe();
  void reset();

  // A missing object or mismatched readback means unsupported. Other I/O errors propagate.
  // The output is valid only on success.
  int detect(const char *pathname, const ObObjectStorageInfo &storage_info, bool &is_supported);

private:
  int build_detect_path_(const char *pathname, char *detect_path, const int64_t path_len);
  int check_condition_put_(const char *detect_path, ObStorageObDalWriter &writer,
                           ObStorageObDalReader &reader, bool &is_supported);
  int check_readback_(const char *detect_path, ObStorageObDalReader &reader);
  int cleanup_(const char *detect_path, ObStorageObDalWriter &writer, ObStorageObDalReader &reader);

private:
  ObObjectStorageInfo storage_info_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageConditionPutProbe);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_LIB_RESTORE_OB_STORAGE_CONDITION_PUT_PROBE_H_
