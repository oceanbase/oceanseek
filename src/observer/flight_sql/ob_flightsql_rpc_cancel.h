/**
 * Copyright (c) 2024 OceanBase
 * 本文件包含机密及专有信息，未经授权不得使用或披露。
 */
#ifndef OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_RPC_CANCEL_H_
#define OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_RPC_CANCEL_H_

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <utility>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_interceptor.h>

namespace oceanbase {
namespace observer {

// Added by the server interceptor, never trusted from a client. This correlates
// Arrow's ServerCallContext with the exact native call without transport casts.
static constexpr const char *kFlightSqlRpcCancelIdHeader = "x-ob-flight-internal-rpc-id";

class ObFlightSqlRpcCancelHandle
{
public:
  explicit ObFlightSqlRpcCancelHandle(grpc::ServerContextBase *context)
      : context_(context) {}

  bool cancel()
  {
    // PRE_SEND_CANCEL re-enters our interceptor synchronously. That hook must
    // only Proceed(), so holding this lock also fences context destruction.
    std::lock_guard<std::mutex> guard(mutex_);
    if (context_ == nullptr) { return false; }
    if (!cancelled_.exchange(true, std::memory_order_acq_rel)) {
      context_->TryCancel();
    }
    return true;
  }

  bool is_cancelled() const { return cancelled_.load(std::memory_order_acquire); }

  void detach()
  {
    std::lock_guard<std::mutex> guard(mutex_);
    context_ = nullptr;
  }

private:
  std::mutex mutex_;
  grpc::ServerContextBase *context_;
  std::atomic<bool> cancelled_{false};
};

class ObFlightSqlRpcCancelRegistry
{
private:
  struct Entry {
    std::shared_ptr<ObFlightSqlRpcCancelHandle> handle;
    const char *header_data;
  };

  struct State {
    std::mutex mutex;
    std::atomic<uint64_t> next_id{1};
    std::unordered_map<std::string, Entry> calls;
  };

  class Interceptor : public grpc::experimental::Interceptor {
  public:
    Interceptor(std::shared_ptr<State> state, grpc::ServerContextBase *context)
        : state_(std::move(state)),
          id_(std::to_string(state_->next_id.fetch_add(1, std::memory_order_relaxed))),
          handle_(std::make_shared<ObFlightSqlRpcCancelHandle>(context)) {}

    ~Interceptor() override
    {
      try { cleanup(); } catch (...) {}
    }

    void Intercept(grpc::experimental::InterceptorBatchMethods *methods) override
    {
      using Hook = grpc::experimental::InterceptionHookPoints;
      // TryCancel() invokes this hook on the cancelling thread. Never acquire
      // the handle or registry mutex here, including via cleanup().
      if (methods->QueryInterceptionHookPoint(Hook::PRE_SEND_CANCEL)) {
        methods->Proceed();
        return;
      }
      try {
        if (methods->QueryInterceptionHookPoint(Hook::POST_RECV_INITIAL_METADATA)) {
          auto *headers = methods->GetRecvInitialMetadata();
          if (headers != nullptr) {
            // Strip every client-supplied value before inserting our own ID.
            headers->erase(grpc::string_ref(kFlightSqlRpcCancelIdHeader));
            {
              std::lock_guard<std::mutex> guard(state_->mutex);
              state_->calls.emplace(id_, Entry{handle_, id_.data()});
            }
            // The interceptor owns id_ for the lifetime of the native call.
            headers->emplace(grpc::string_ref(kFlightSqlRpcCancelIdHeader),
                             grpc::string_ref(id_));
          }
        }
        if (methods->QueryInterceptionHookPoint(Hook::PRE_SEND_STATUS)) {
          // Fence cancellation before gRPC can destroy ServerContext. Waiting
          // until the interceptor destructor would be too late.
          cleanup();
          if (handle_->is_cancelled() && methods->GetSendStatus().ok()) {
            methods->ModifySendStatus(grpc::Status(
                grpc::StatusCode::CANCELLED, "Flight SQL request was cancelled"));
          }
        }
      } catch (...) {
        // Allocation failures must not escape through the gRPC callback. A
        // missing ID is detected by the Flight handler before admitting work.
        try { cleanup(); } catch (...) {}
      }
      methods->Proceed();
    }

  private:
    void cleanup()
    {
      handle_->detach();
      std::lock_guard<std::mutex> guard(state_->mutex);
      state_->calls.erase(id_);
    }

    std::shared_ptr<State> state_;
    const std::string id_;
    std::shared_ptr<ObFlightSqlRpcCancelHandle> handle_;
  };

  class Factory : public grpc::experimental::ServerInterceptorFactoryInterface {
  public:
    explicit Factory(std::shared_ptr<State> state) : state_(std::move(state)) {}

    grpc::experimental::Interceptor *CreateServerInterceptor(
        grpc::experimental::ServerRpcInfo *info) override
    {
      try {
        const std::string method(info->method());
        if (method == "/arrow.flight.protocol.FlightService/DoPut"
            || method == "/arrow.flight.protocol.FlightService/DoGet") {
          return new Interceptor(state_, info->server_context());
        }
      } catch (...) {}
      return nullptr;
    }

  private:
    std::shared_ptr<State> state_;
  };

public:
  ObFlightSqlRpcCancelRegistry() : state_(std::make_shared<State>()) {}

  std::shared_ptr<ObFlightSqlRpcCancelHandle> get(
      const std::string &id, const char *header_data) const
  {
    std::lock_guard<std::mutex> guard(state_->mutex);
    const auto it = state_->calls.find(id);
    // Arrow's gRPC context preserves metadata values as string_views. Require
    // the exact interceptor-owned backing storage as well as the ID. Thus a
    // forged header cannot bind another RPC even if interceptor allocation
    // failed and gRPC continued that call without our interceptor.
    return it == state_->calls.end() || it->second.header_data != header_data
        ? nullptr : it->second.handle;
  }

  std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface> make_factory()
  {
    return std::unique_ptr<grpc::experimental::ServerInterceptorFactoryInterface>(
        new Factory(state_));
  }

private:
  std::shared_ptr<State> state_;
};

} // namespace observer
} // namespace oceanbase

#endif // OCEANBASE_OBSERVER_FLIGHT_SQL_OB_FLIGHTSQL_RPC_CANCEL_H_
