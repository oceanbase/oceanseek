/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_COMMON_DEFINE_
#define OCEANBASE_LOGSERVICE_LIBPALF_COMMON_DEFINE_

namespace oceanbase
{
namespace ipalf
{
enum class AccessMode;
}
namespace palf
{
class PalfBaseInfo;
}
namespace libpalf
{
enum class LibPalfAccessMode;
class LibPalfBaseInfo;

#define LIBPALF_ERRNO_CAST(libpalf_errno) (static_cast<int32_t>(libpalf_errno))
#define GET_MEMBERSHIP_MAX_RETRY_TIME 128
#define LIBPALF_EVENT(info_string, palf_id, args...) FLOG_INFO("[LIBPALF_EVENT] "info_string, "palf_id", palf_id, args)
#define LIBPALF_REPORT_EXTRA_INFO_KV(args...) \
const int64_t MAX_INFO_LENGTH = 1024; \
char EXTRA_INFOS[MAX_INFO_LENGTH]; \
int64_t pos = 0; \
::oceanbase::common::databuff_print_kv(EXTRA_INFOS, MAX_INFO_LENGTH, pos, ##args); \


constexpr int LIBPALF_INTEGER_PARAM_DIGIT_LEN = 32;
extern int transfer_access_mode(const ipalf::AccessMode &access_mode,
                                LibPalfAccessMode& libpalf_access_mode);

extern int transfer_libpalf_access_mode_to_access_mode(const LibPalfAccessMode &libpalf_access_mode,
                                                        ipalf::AccessMode &access_mode);

extern int transfer_palf_base_info_to_libpalf_base_info(const palf::PalfBaseInfo &palf_base_info,
                                                        LibPalfBaseInfo &libpalf_base_info);

extern int transfer_libpalf_base_info_to_palf_base_info(const LibPalfBaseInfo &libpalf_base_info,
                                                        palf::PalfBaseInfo &palf_base_info);

} // end namespace libpalf
} // end namespace oceanbase

#endif