/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */

#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_REQUEST_CANCEL_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_REQUEST_CANCEL_H_

#include <atomic>
#include <exception>
#include <functional>
#include <mutex>
#include <utility>

namespace oceanbase
{
namespace observer
{

// The request owner must retire this object before releasing its query guard.
// Cancellation and retirement share a gate so a captured cancellation handle
// cannot interrupt a later query on the same SQL session. Callbacks must not
// reenter cancel()/retire(); SQL execution itself never holds this gate.
class ObFlightSqlRequestCancellation
{
public:
  explicit ObFlightSqlRequestCancellation(std::function<void()> cancel_rpc = {})
      : cancel_rpc_(std::move(cancel_rpc))
  {
  }

  bool is_cancelled() const
  {
    return cancelled_.load(std::memory_order_acquire);
  }

  // True means this request is still active, including an already accepted
  // cancellation. Transport cancellation and SQL interruption run at most once.
  // Both are attempted even if one callback throws; retirement remains usable.
  template <typename InterruptSql>
  bool cancel(InterruptSql &&interrupt_sql)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!active_) {
      return false;
    }
    if (!cancelled_.load(std::memory_order_relaxed)) {
      cancelled_.store(true, std::memory_order_release);
      std::exception_ptr failure;
      try {
        if (cancel_rpc_) {
          cancel_rpc_();
        }
      } catch (...) {
        failure = std::current_exception();
      }
      try {
        interrupt_sql();
      } catch (...) {
        if (!failure) {
          failure = std::current_exception();
        }
      }
      if (failure) {
        std::rethrow_exception(failure);
      }
    }
    return true;
  }

  bool retire()
  {
    return retire([] {});
  }

  // The optional cleanup runs under the same gate, after any accepted cancel
  // finishes. The owner can restore a query-killed session state here while it
  // still owns the query guard. Retirement is idempotent. The returned flag
  // lets a successful response be decided atomically with retirement, so a
  // cancellation accepted before retirement cannot be reported as success.
  template <typename Cleanup>
  bool retire(Cleanup &&cleanup)
  {
    std::lock_guard<std::mutex> guard(mutex_);
    if (active_) {
      active_ = false;
      cancel_rpc_ = {};
      cleanup();
    }
    return cancelled_.load(std::memory_order_acquire);
  }

private:
  std::atomic<bool> cancelled_{false};
  std::mutex mutex_;
  bool active_{true};
  std::function<void()> cancel_rpc_;

  ObFlightSqlRequestCancellation(const ObFlightSqlRequestCancellation &) = delete;
  ObFlightSqlRequestCancellation &operator=(const ObFlightSqlRequestCancellation &) = delete;
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_REQUEST_CANCEL_H_
