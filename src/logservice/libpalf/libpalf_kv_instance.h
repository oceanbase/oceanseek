/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOGSERVICE_LIBPALF_KV_INSTANCE_
#define OCEANBASE_LOGSERVICE_LIBPALF_KV_INSTANCE_

#include "lib/string/ob_string.h"
#include "lib/string/ob_string_buffer.h"
#include "libpalf_env_ffi_instance.h"
#include "palf_ffi.h"

namespace oceanbase {
namespace libpalf {


class LibPalfKVIteraotr {
public:
  LibPalfKVIteraotr(ObIAllocator &allocator, common::ObString &dir, common::ObString &sub_dir, common::ObString &prefix)
    : allocator_(allocator),
      dir_(dir),
      sub_dir_(sub_dir),
      prefix_(prefix),
      libpalf_env_ffi_(nullptr),
      libpalf_kv_pairs_(nullptr),
      is_inited_(false) {}
  ~LibPalfKVIteraotr() {
    reset();
  }
  int init();
  bool has_next() const;
  int next(common::ObStringBuffer &key, common::ObStringBuffer &value);
private:
  void reset();

private:
  ObIAllocator &allocator_;
  common::ObString &dir_;
  common::ObString &sub_dir_;
  common::ObString &prefix_;
  const LibPalfEnvFFI *libpalf_env_ffi_;
  LibPalfKVPairs *libpalf_kv_pairs_;
  bool is_inited_;
};

class LibPalfKVInstance {
public:
    static LibPalfKVInstance &get_instance();
    LibPalfKVInstance();
    int get(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, common::ObStringBuffer &value);
    int insert(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &value);
    int update(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &value);
    int cas(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &old_value, const common::ObString &new_value, bool *expected);
    int watch_cas(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &watch_key, const common::ObString &old_value, const common::ObString &new_value, bool *expected);
    int del(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key);
    int insert_encrypted(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, const common::ObString &value);
    int get_decrypted(const common::ObString &dir, const common::ObString &sub_dir, const common::ObString &key, common::ObStringBuffer &value);
    int seek(LibPalfKVIteraotr &iterator);
};

}
}

#endif