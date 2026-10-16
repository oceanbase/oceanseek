/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_APPEND_CALLBACK_H_
#define OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_APPEND_CALLBACK_H_

#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/shared_storage/ob_file_manager.h"

namespace oceanbase
{
namespace storage
{

class ObSSTmpFileAppendCallback
{
public:
  ObSSTmpFileAppendCallback() {}
  virtual ~ObSSTmpFileAppendCallback() {}
  virtual int process_on_appending(TmpFileMeta &meta) = 0;
  virtual int process_on_non_appending(TmpFileMeta &meta) = 0;
  VIRTUAL_TO_STRING_KV("", "");
};

class ObSSTmpFileEvictCallback : public ObSSTmpFileAppendCallback
{
public:
  explicit ObSSTmpFileEvictCallback(const blocksstable::MacroBlockId macro_id) : macro_id_(macro_id)
  {}
  virtual ~ObSSTmpFileEvictCallback() {}
  virtual int process_on_appending(TmpFileMeta &meta) override;
  virtual int process_on_non_appending(TmpFileMeta &meta) override;
  INHERIT_TO_STRING_KV("ObSSTmpFileAppendCallback", ObSSTmpFileAppendCallback, K_(macro_id));

private:
  blocksstable::MacroBlockId macro_id_;
};

} /* namespace storage */
} /* namespace oceanbase */

#endif /* OCEANBASE_STORAGE_SHARED_STORAGE_OB_SS_TMP_FILE_APPEND_CALLBACK_H_ */
