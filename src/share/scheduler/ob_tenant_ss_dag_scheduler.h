/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

public:
  void ss_reload_config(const bool force_reload = false);
private:
  static const int64_t ADAPT_WORK_THREAD_INTERVAL = 10 * 1000L * 1000L; // 10s
  void adapt_ss_work_thread();
  int64_t get_ss_total_thread_num_() const;
