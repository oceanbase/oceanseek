/**
 * Copyright (C) 2026 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_SHARE_SHARED_STORAGE_OB_local_cache_control_MODE_H_
#define OCEANBASE_SHARE_SHARED_STORAGE_OB_local_cache_control_MODE_H_

#include "share/config/ob_config.h"

namespace oceanbase
{
namespace common
{

struct ObSSLocalCacheControlMode : public ObIConfigMode
{
public:
  ObSSLocalCacheControlMode(): value_(0) {}
  ObSSLocalCacheControlMode(const uint8_t *values);
  void assign(const ObSSLocalCacheControlMode &other);
  virtual int set_value(const ObConfigModeItem &mode_item) override;
  bool check_mode_valid(const uint16_t mode) const { return mode > 2 ? false : true; }
  bool is_macro_read_cache_enable() const { return ((macro_read_cache_mode_ == MODE_OFF) ? false : true); }
  bool is_macro_write_cache_enable() const { return ((macro_write_cache_mode_ == MODE_OFF) ? false : true); }
  bool is_mem_macro_cache_enable() const { return ((mem_macro_cache_mode_ == MODE_OFF) ? false : true); }
  void set_macro_read_cache_mode(const uint16_t mode);
  void set_macro_write_cache_mode(const uint16_t mode);
  void set_mem_macro_cache_mode(const uint16_t mode);
  uint16_t get_value() const { return value_; }

public:
  static const int16_t MODE_DEFAULT = 0b00;
  static const int16_t MODE_ON = 0b01;
  static const int16_t MODE_OFF = 0b10;
private:
  union {
    uint16_t value_;
    struct { // FARM COMPAT WHITELIST: removed micro_cache_mode_
      uint16_t macro_read_cache_mode_ : 2;
      uint16_t macro_write_cache_mode_ : 2;
      uint16_t mem_macro_cache_mode_ : 2;
      uint16_t reserved_ : 10;
    };
  };
private:
  DISALLOW_COPY_AND_ASSIGN(ObSSLocalCacheControlMode);
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_SHARED_STORAGE_OB_local_cache_control_MODE_H_
