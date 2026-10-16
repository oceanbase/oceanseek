/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_BASE_FORMAT_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_BASE_FORMAT_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_ss_common_header.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace storage
{

struct ObSSFormatBody;
struct ObSSClusterInfoBody;

const int64_t SS_FORMAT_META_BUFF_SIZE = 4096;

/**
 * Base class for SS format structures
 * Provides common functionality for all SS formats
 */
template<typename BodyType>
class ObSSBaseFormat
{
public:
  ObSSBaseFormat() = default;
  virtual ~ObSSBaseFormat() = default;

  ObSSBaseFormat &operator==(const ObSSBaseFormat &other) = delete;
  ObSSBaseFormat &operator!=(const ObSSBaseFormat &other) = delete;
public:
  void reset();
  bool is_valid() const;
  const ObSSCommonHeader &get_header() const { return header_; }
  const BodyType &get_body() const { return body_; }
  TO_STRING_KV(K_(header), K_(body));
  OB_UNIS_VERSION(1);
protected:
  ObSSCommonHeader header_;
  BodyType body_;
};

/**
 * Base utility class for SS format operations
 * Provides common file operations for all SS formats
 */
template<typename FormatType>
class ObSSBaseUtil
{
public:
  static int write_ss_format(const share::ObBackupDest &storage_dest,
                             const FormatType &ss_format,
                             const char *file_name);
  static int read_ss_format(const share::ObBackupDest &storage_dest,
                            FormatType &ss_format,
                            const char *file_name);
  static int is_exist_ss_format(const share::ObBackupDest &storage_dest,
                                bool &is_exist,
                                const char *file_name);
  static int get_ss_format_path(const share::ObBackupDest &storage_dest,
                                char *path, const int64_t length,
                                const char *file_name);
};

} // storage
} // oceanbase

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_BASE_FORMAT_H_ */

