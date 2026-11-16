/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/restore/ob_storage_condition_put_probe.h"
#include "lib/random/ob_random.h"
#include "lib/restore/ob_storage.h"
#include "lib/restore/ob_storage_obdal_base.h"

namespace oceanbase
{
namespace common
{
constexpr char IF_MATCH_PROBE_DATA[] = "detect_content_v1";

ObStorageConditionPutProbe::ObStorageConditionPutProbe()
{
  reset();
}

ObStorageConditionPutProbe::~ObStorageConditionPutProbe()
{
  reset();
}

void ObStorageConditionPutProbe::reset()
{
  storage_info_.reset();
}

int ObStorageConditionPutProbe::detect(
    const char *pathname,
    const ObObjectStorageInfo &storage_info,
    bool &is_supported)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  char detect_path[OB_MAX_URI_LENGTH] = {0};
  ObStorageObDalWriter writer;
  ObStorageObDalReader reader;
  is_supported = false;
  reset();
  if (OB_ISNULL(pathname) || !storage_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid condition put probe input", KR(ret), KP(pathname));
  } else if (OB_FAIL(storage_info_.assign(storage_info))) {
    OB_LOG(WARN, "failed to copy condition put probe storage info", KR(ret));
  } else if (OB_FAIL(build_detect_path_(pathname, detect_path, sizeof(detect_path)))) {
    OB_LOG(WARN, "failed to build detect path", KR(ret), KCSTRING(pathname));
  } else if (OB_FAIL(check_condition_put_(detect_path, writer, reader, is_supported))) {
    OB_LOG(WARN, "failed to check condition put", KR(ret), KCSTRING(detect_path));
  }
  if (OB_NOT_SUPPORTED == ret) {
    OB_LOG(INFO, "backend does not support conditional write", KR(ret), K(detect_path));
    ret = OB_SUCCESS;
  }
  if (OB_TMP_FAIL(cleanup_(detect_path, writer, reader))) {
    ret = COVER_SUCC(tmp_ret);
  }
  OB_LOG(INFO, "condition put probe finished", KR(ret), K(is_supported), K(detect_path));
  return ret;
}

int ObStorageConditionPutProbe::build_detect_path_(
    const char *pathname,
    char *detect_path,
    const int64_t path_len)
{
  int ret = OB_SUCCESS;
  const char *scheme = STRSTR(pathname, "://");
  const char *last_slash = STRRCHR(pathname, '/');
  if (OB_UNLIKELY(nullptr == scheme || nullptr == last_slash || last_slash <= scheme + 2)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid condition put probe object path", KR(ret), KCSTRING(pathname));
  } else if (OB_FAIL(databuff_printf(detect_path, path_len, "%.*s/.ob_if_match_detect_%ld_%ld",
      static_cast<int32_t>(last_slash - pathname), pathname, ObTimeUtility::current_time_us(),
      ObRandom::rand(0, INT64_MAX)))) {
    OB_LOG(WARN, "failed to build condition put probe path", KR(ret));
  }
  return ret;
}

int ObStorageConditionPutProbe::check_condition_put_(
    const char *detect_path,
    ObStorageObDalWriter &writer,
    ObStorageObDalReader &reader,
    bool &is_supported)
{
  int ret = OB_SUCCESS;
  const int64_t data_size = sizeof(IF_MATCH_PROBE_DATA) - 1;
  const char second_data[] = "detect_content_v2";
  if (OB_FAIL(writer.open(detect_path, &storage_info_))) {
    OB_LOG(WARN, "failed to open condition put probe writer", KR(ret), K(detect_path));
  } else if (OB_FAIL(writer.write(IF_MATCH_PROBE_DATA, data_size,
                                  true/*if_match*/, true/*is_detect*/))) {
    OB_LOG(WARN, "first conditional write failed", KR(ret), K(detect_path));
  } else if (OB_FAIL(reader.open(detect_path, &storage_info_, false/*head_meta*/))) {
    OB_LOG(WARN, "failed to open object after conditional write", KR(ret), K(detect_path));
  } else if (OB_FAIL(check_readback_(detect_path, reader))) {
    OB_LOG(WARN, "failed to verify first conditional write", KR(ret), K(detect_path));
  } else if (OB_FAIL(writer.write(IF_MATCH_PROBE_DATA, data_size,
                                  true/*if_match*/, true/*is_detect*/))) {
    OB_LOG(WARN, "conditional write with identical data failed", KR(ret), K(detect_path));
  } else if (OB_FAIL(writer.write(second_data, sizeof(second_data) - 1,
                                  true/*if_match*/, true/*is_detect*/))) {
    if (OB_OBJECT_STORAGE_CONDITION_NOT_MATCH != ret) {
      OB_LOG(WARN, "conditional write with different data failed unexpectedly", KR(ret), K(detect_path));
    } else {
      ret = OB_SUCCESS;
    }

    if (FAILEDx(check_readback_(detect_path, reader))) {
      OB_LOG(WARN, "failed to verify original data after rejected conditional write", KR(ret), K(detect_path));
    } else {
      is_supported = true;
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "conditional write unexpectedly accepted different data", KR(ret), K(detect_path));
  }
  return ret;
}

int ObStorageConditionPutProbe::check_readback_(
    const char *detect_path,
    ObStorageObDalReader &reader)
{
  int ret = OB_SUCCESS;
  char read_buf[sizeof(IF_MATCH_PROBE_DATA)] = {0};
  const int64_t expected_size = sizeof(IF_MATCH_PROBE_DATA) - 1;
  int64_t read_size = 0;
  if (OB_FAIL(reader.pread(read_buf, sizeof(read_buf), 0, read_size))) {
    OB_LOG(WARN, "failed to read object after conditional write", KR(ret), K(detect_path));
    // Some backends acknowledge a conditional write without creating the object.
    if (OB_OBJECT_NOT_EXIST == ret) {
      ret = OB_NOT_SUPPORTED;
    }
  } else if (read_size != expected_size ||
             0 != MEMCMP(read_buf, IF_MATCH_PROBE_DATA, expected_size)) {
    ret = OB_NOT_SUPPORTED;
    OB_LOG(WARN, "conditional write readback differs", KR(ret), K(read_size), K(expected_size), K(detect_path));
  }
  return ret;
}

int ObStorageConditionPutProbe::cleanup_(
    const char *detect_path,
    ObStorageObDalWriter &writer,
    ObStorageObDalReader &reader)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const bool is_writer_opened = writer.is_opened();
  if (reader.is_opened() && OB_TMP_FAIL(reader.close())) {
    ret = COVER_SUCC(tmp_ret);
    OB_LOG(WARN, "failed to close condition put probe reader", KR(ret), K(tmp_ret), K(detect_path));
  }
  if (is_writer_opened && OB_TMP_FAIL(writer.close())) {
    ret = COVER_SUCC(tmp_ret);
    OB_LOG(WARN, "failed to close condition put probe writer", KR(ret), K(tmp_ret), K(detect_path));
  }
  if (is_writer_opened && !storage_info_.is_enable_worm()) {
    ObStorageUtil util;
    if (OB_TMP_FAIL(util.open(&storage_info_))) {
      ret = COVER_SUCC(tmp_ret);
      OB_LOG(WARN, "failed to open condition put probe cleanup util", KR(ret), K(tmp_ret), K(detect_path));
    } else if (OB_TMP_FAIL(util.del_file(ObString::make_string(detect_path)))) {
      ret = COVER_SUCC(tmp_ret);
      OB_LOG(WARN, "failed to delete condition put probe object", KR(ret), K(tmp_ret), K(detect_path));
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
