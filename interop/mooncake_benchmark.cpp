// Wire-compatible no-state Mooncake Master RPC benchmark peer.

#include <algorithm>
#include <atomic>
#include <barrier>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <limits>
#include <mutex>
#include <optional>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <async_simple/coro/Collect.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>
#include <ylt/struct_pack.hpp>
#include <ylt/util/tl/expected.hpp>

namespace mooncake {

inline constexpr std::string_view kMooncakeStoreVersion = "2.0.0";

enum class ErrorCode : std::int32_t {
  OK = 0,
  NO_AVAILABLE_HANDLE = -200,
  INVALID_PARAMS = -600,
  OBJECT_NOT_FOUND = -704,
  OBJECT_ALREADY_EXISTS = -705,
};

enum class ObjectDataType : std::uint8_t {
  UNKNOWN = 0,
  KVCACHE = 1,
};

enum class ReplicaType {
  MEMORY = 0,
  DISK = 1,
  LOCAL_DISK = 2,
  NOF_SSD = 3,
  ALL = 4,
};

enum class ReplicaStatus {
  UNDEFINED = 0,
  INITIALIZED = 1,
  PROCESSING = 2,
  COMPLETE = 3,
  REMOVED = 4,
  FAILED = 5,
};

enum class SoftPinAction : std::uint8_t {
  PRESERVE = 0,
  ENABLE = 1,
  DISABLE = 2,
};

using UUID = std::pair<std::uint64_t, std::uint64_t>;

struct BufferDescriptor {
  std::uint64_t size;
  std::uintptr_t buffer_address;
  std::string protocol;
  std::string transport_endpoint;
};
YLT_REFL(BufferDescriptor, size, buffer_address, protocol, transport_endpoint);

struct MemoryDescriptor {
  BufferDescriptor buffer_descriptor;
};
YLT_REFL(MemoryDescriptor, buffer_descriptor);

struct NoFDescriptor {
  BufferDescriptor buffer_descriptor;
};
YLT_REFL(NoFDescriptor, buffer_descriptor);

struct DiskDescriptor {
  std::string file_path;
  std::uint64_t object_size;
};
YLT_REFL(DiskDescriptor, file_path, object_size);

struct LocalDiskDescriptor {
  UUID client_id;
  std::uint64_t object_size;
  std::string transport_endpoint;
};
YLT_REFL(LocalDiskDescriptor, client_id, object_size, transport_endpoint);

using DescriptorVariant = std::variant<MemoryDescriptor, NoFDescriptor,
                                       DiskDescriptor, LocalDiskDescriptor>;

struct ReplicaDescriptor {
  std::uint64_t id;
  DescriptorVariant descriptor_variant;
  ReplicaStatus status;
};
YLT_REFL(ReplicaDescriptor, id, descriptor_variant, status);

struct GetReplicaListResponse {
  std::vector<ReplicaDescriptor> replicas;
  std::uint64_t lease_ttl_ms;
  std::optional<std::uint64_t> object_checksum;
};
YLT_REFL(GetReplicaListResponse, replicas, lease_ttl_ms, object_checksum);

struct GetStorageConfigResponse {
  std::string fsdir;
  bool enable_disk_eviction;
  std::uint64_t quota_bytes;
};
YLT_REFL(GetStorageConfigResponse, fsdir, enable_disk_eviction, quota_bytes);

struct ObjectMeta {
  std::string key;
  std::optional<std::uint64_t> object_checksum;
};
YLT_REFL(ObjectMeta, key, object_checksum);

struct ReplicateConfig {
  std::size_t replica_num{1};
  std::size_t nof_replica_num{0};
  SoftPinAction soft_pin_action{SoftPinAction::PRESERVE};
  std::optional<std::uint64_t> soft_pin_ttl_ms;
  bool with_hard_pin{false};
  std::vector<std::string> preferred_segments;
  std::string preferred_segment;
  std::vector<std::string> preferred_nof_segments;
  bool prefer_alloc_in_same_node{false};
  ObjectDataType data_type{ObjectDataType::UNKNOWN};
  std::string host_id;
  std::optional<std::vector<std::string>> group_ids;
};
YLT_REFL(ReplicateConfig, replica_num, nof_replica_num, soft_pin_action,
         soft_pin_ttl_ms, with_hard_pin, preferred_segments,
         preferred_segment, preferred_nof_segments, prefer_alloc_in_same_node,
         data_type, host_id, group_ids);

using ExpectedBool = tl::expected<bool, ErrorCode>;
using ExpectedGetReplicaListResponse =
    tl::expected<GetReplicaListResponse, ErrorCode>;
using ExpectedGetStorageConfigResponse =
    tl::expected<GetStorageConfigResponse, ErrorCode>;
using ExpectedReplicaDescriptors =
    tl::expected<std::vector<ReplicaDescriptor>, ErrorCode>;
using ExpectedString = tl::expected<std::string, ErrorCode>;
using ExpectedVoid = tl::expected<void, ErrorCode>;

ReplicaDescriptor memory_replica() {
  return ReplicaDescriptor{1,
                           DescriptorVariant{MemoryDescriptor{BufferDescriptor{
                               4096, 0x1000, "tcp", "127.0.0.1:12345"}}},
                           ReplicaStatus::COMPLETE};
}

class WrappedMasterService {
public:
  ExpectedGetStorageConfigResponse GetStorageConfig() {
    return GetStorageConfigResponse{"", false, 0};
  }

  ExpectedString ServiceReady() { return std::string{kMooncakeStoreVersion}; }

  ExpectedBool ExistKey(const std::string &, const std::string &) {
    return false;
  }

  ExpectedGetReplicaListResponse GetReplicaList(const std::string &,
                                                 const std::string &) {
    return GetReplicaListResponse{
        std::vector<ReplicaDescriptor>{memory_replica()}, 1000, std::nullopt};
  }

  std::vector<ExpectedBool> BatchExistKey(const std::vector<std::string> &keys,
                                          const std::string &) {
    return std::vector<ExpectedBool>(keys.size(), ExpectedBool{false});
  }

  std::vector<ExpectedGetReplicaListResponse>
  BatchGetReplicaList(const std::vector<std::string> &keys,
                      const std::string &) {
    std::vector<ExpectedGetReplicaListResponse> response;
    response.reserve(keys.size());
    for (std::size_t index = 0; index < keys.size(); ++index) {
      response.emplace_back(GetReplicaListResponse{
          std::vector<ReplicaDescriptor>{memory_replica()}, 1000,
          std::nullopt});
    }
    return response;
  }

  std::vector<ExpectedReplicaDescriptors>
  BatchPutStart(const UUID &, const std::vector<std::string> &keys,
                const std::vector<std::uint64_t> &, const ReplicateConfig &,
                const std::string &) {
    std::vector<ExpectedReplicaDescriptors> response;
    response.reserve(keys.size());
    for (std::size_t index = 0; index < keys.size(); ++index) {
      response.emplace_back(std::vector<ReplicaDescriptor>{memory_replica()});
    }
    return response;
  }

  std::vector<ExpectedVoid>
  BatchPutEnd(const UUID &, const std::vector<ObjectMeta> &object_metas,
              ReplicaType, const std::string &) {
    return successful_void_results(object_metas.size());
  }

  std::vector<ExpectedVoid> BatchPutRevoke(const UUID &,
                                           const std::vector<std::string> &keys,
                                           ReplicaType, const std::string &) {
    return successful_void_results(keys.size());
  }

private:
  static std::vector<ExpectedVoid> successful_void_results(std::size_t count) {
    std::vector<ExpectedVoid> response;
    response.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      response.emplace_back();
    }
    return response;
  }
};

} // namespace mooncake

template <typename Range>
void print_hex(std::string_view label, const Range &bytes) {
  std::cout << label << '=';
  for (std::size_t index = 0; index < bytes.size(); ++index) {
    std::cout << std::hex << std::setfill('0') << std::setw(2)
              << static_cast<unsigned>(
                     static_cast<std::uint8_t>(bytes.data()[index]));
  }
  std::cout << std::dec << '\n';
}

template <typename T> void print_type(std::string_view label) {
  constexpr auto literal = struct_pack::get_type_literal<T>();
  print_hex(label, literal);
  std::cout << label << "_hash=" << struct_pack::get_type_code<T>() << '\n';
}

template <typename T> void print_value(std::string_view label, const T &value) {
  print_hex(label, struct_pack::serialize(value));
}

template <typename... Args>
void print_values(std::string_view label, const Args &...args) {
  print_hex(label, struct_pack::serialize(args...));
}

void print_metadata() {
  using Service = mooncake::WrappedMasterService;
  using SingleKeyRequest = std::tuple<std::string, std::string>;
  using BatchKeyRequest = std::tuple<std::vector<std::string>, std::string>;
  using BatchPutStartRequest =
      std::tuple<mooncake::UUID, std::vector<std::string>,
                 std::vector<std::uint64_t>, mooncake::ReplicateConfig,
                 std::string>;
  using BatchPutEndRequest =
      std::tuple<mooncake::UUID, std::vector<mooncake::ObjectMeta>,
                 mooncake::ReplicaType, std::string>;
  using BatchPutRevokeRequest =
      std::tuple<mooncake::UUID, std::vector<std::string>,
                 mooncake::ReplicaType, std::string>;

  print_type<SingleKeyRequest>("single_key_request");
  print_type<mooncake::ExpectedBool>("single_exists_response");
  print_type<mooncake::ExpectedGetReplicaListResponse>("single_get_response");
  print_type<BatchKeyRequest>("batch_key_request");
  print_type<std::vector<mooncake::ExpectedBool>>("batch_exists_response");
  print_type<std::vector<mooncake::ExpectedGetReplicaListResponse>>(
      "batch_get_response");
  print_type<BatchPutStartRequest>("batch_put_start_request");
  print_type<std::vector<mooncake::ExpectedReplicaDescriptors>>(
      "batch_put_start_response");
  print_type<BatchPutEndRequest>("batch_put_end_request");
  print_type<BatchPutRevokeRequest>("batch_put_revoke_request");
  print_type<std::vector<mooncake::ExpectedVoid>>("batch_void_response");
  print_type<mooncake::GetStorageConfigResponse>("storage_config_value");
  print_type<mooncake::ExpectedGetStorageConfigResponse>(
      "storage_config_response");
  print_type<mooncake::ExpectedString>("service_ready_response");
  print_value(
      "storage_config_response_sample",
      mooncake::ExpectedGetStorageConfigResponse{
          mooncake::GetStorageConfigResponse{"", false, 0}});
  print_value("service_ready_response_sample",
              mooncake::ExpectedString{
                  std::string{mooncake::kMooncakeStoreVersion}});

  const mooncake::UUID sample_client_id{1, 2};
  const std::vector<mooncake::ObjectMeta> sample_object_metas{
      {"benchmark-key-00000000", std::nullopt}};
  const std::string sample_tenant = "default";
  print_value("batch_put_end_tuple_sample",
              BatchPutEndRequest{sample_client_id, sample_object_metas,
                                 mooncake::ReplicaType::ALL, sample_tenant});
  print_values("batch_put_end_args_sample", sample_client_id,
               sample_object_metas, mooncake::ReplicaType::ALL, sample_tenant);

  mooncake::ReplicateConfig sample_replicate_config;
  sample_replicate_config.soft_pin_action = mooncake::SoftPinAction::ENABLE;
  sample_replicate_config.soft_pin_ttl_ms = 1234;
  sample_replicate_config.data_type = mooncake::ObjectDataType::KVCACHE;
  print_value(
      "batch_put_start_tuple_sample",
      BatchPutStartRequest{sample_client_id,
                           std::vector<std::string>{"benchmark-key-00000000"},
                           std::vector<std::uint64_t>{4096},
                           sample_replicate_config, sample_tenant});

  std::cout << "single_exists_route="
            << coro_rpc::func_id<&Service::ExistKey>() << '\n';
  std::cout << "single_get_route="
            << coro_rpc::func_id<&Service::GetReplicaList>() << '\n';

  std::cout << "batch_exists_route="
            << coro_rpc::func_id<&Service::BatchExistKey>() << '\n';
  std::cout << "batch_get_route="
            << coro_rpc::func_id<&Service::BatchGetReplicaList>() << '\n';
  std::cout << "batch_put_start_route="
            << coro_rpc::func_id<&Service::BatchPutStart>() << '\n';
  std::cout << "batch_put_end_route="
            << coro_rpc::func_id<&Service::BatchPutEnd>() << '\n';
  std::cout << "batch_put_revoke_route="
            << coro_rpc::func_id<&Service::BatchPutRevoke>() << '\n';
  std::cout << "get_storage_config_route="
            << coro_rpc::func_id<&Service::GetStorageConfig>() << '\n';
  std::cout << "service_ready_route="
            << coro_rpc::func_id<&Service::ServiceReady>() << '\n';
}

template <typename Call, typename Send, typename Validate>
async_simple::coro::Lazy<bool>
run_calls(coro_rpc::coro_rpc_client &client, std::uint64_t iterations,
          std::size_t pipeline, Call call, Send send, Validate validate) {
  if (pipeline == 1) {
    for (std::uint64_t index = 0; index < iterations; ++index) {
      auto result = co_await call(client);
      if (!result) {
        std::cerr << "rpc error code=" << result.error().code.val()
                  << " message=" << result.error().msg << '\n';
        co_return false;
      }
      if (!validate(result.value())) {
        std::cerr << "response validation failed\n";
        co_return false;
      }
    }
    co_return true;
  }

  std::uint64_t issued = 0;
  while (issued < iterations) {
    const auto batch_size = static_cast<std::size_t>(
        std::min<std::uint64_t>(iterations - issued, pipeline));
    auto first = co_await send(client);
    std::vector<decltype(first)> pending;
    pending.reserve(batch_size);
    pending.push_back(std::move(first));
    for (std::size_t offset = 1; offset < batch_size; ++offset) {
      pending.push_back(co_await send(client));
    }
    auto completed =
        co_await async_simple::coro::collectAll(std::move(pending));
    for (auto &item : completed) {
      auto &result = item.value();
      if (!result) {
        std::cerr << "rpc error code=" << result.error().code.val()
                  << " message=" << result.error().msg << '\n';
        co_return false;
      }
      if (!validate(result->result())) {
        std::cerr << "response validation failed\n";
        co_return false;
      }
    }
    issued += batch_size;
  }
  co_return true;
}

std::vector<std::string> benchmark_keys(std::size_t batch_size) {
  std::vector<std::string> keys;
  keys.reserve(batch_size);
  for (std::size_t index = 0; index < batch_size; ++index) {
    std::string suffix = std::to_string(index);
    keys.push_back("benchmark-key-" + std::string(8 - suffix.size(), '0') +
                   suffix);
  }
  return keys;
}

async_simple::coro::Lazy<bool> run_operation(coro_rpc::coro_rpc_client &client,
                                             std::string_view operation,
                                             std::size_t batch_size,
                                             std::uint64_t iterations,
                                             std::size_t pipeline) {
  using Service = mooncake::WrappedMasterService;
  auto keys = benchmark_keys(batch_size);
  const std::string tenant_id = "default";

  if (operation == "single-exists") {
    auto call = [&](auto &rpc) {
      return rpc.template call<&Service::ExistKey>(keys.front(), tenant_id);
    };
    auto send = [&](auto &rpc) {
      return rpc.template send_request<&Service::ExistKey>(keys.front(),
                                                           tenant_id);
    };
    auto validate = [](const auto &response) {
      return response && !response.value();
    };
    co_return co_await run_calls(client, iterations, pipeline, call, send,
                                 validate);
  }

  if (operation == "single-get") {
    auto call = [&](auto &rpc) {
      return rpc.template call<&Service::GetReplicaList>(keys.front(),
                                                         tenant_id);
    };
    auto send = [&](auto &rpc) {
      return rpc.template send_request<&Service::GetReplicaList>(keys.front(),
                                                                  tenant_id);
    };
    auto validate = [](const auto &response) {
      return response && response->replicas.size() == 1;
    };
    co_return co_await run_calls(client, iterations, pipeline, call, send,
                                 validate);
  }

  if (operation == "exists") {
    auto call = [&](auto &rpc) {
      return rpc.template call<&Service::BatchExistKey>(keys, tenant_id);
    };
    auto send = [&](auto &rpc) {
      return rpc.template send_request<&Service::BatchExistKey>(keys,
                                                                tenant_id);
    };
    auto validate = [batch_size](const auto &response) {
      return response.size() == batch_size &&
             std::all_of(
                 response.begin(), response.end(),
                 [](const auto &item) { return item && !item.value(); });
    };
    co_return co_await run_calls(client, iterations, pipeline, call, send,
                                 validate);
  }

  if (operation == "get") {
    auto call = [&](auto &rpc) {
      return rpc.template call<&Service::BatchGetReplicaList>(keys, tenant_id);
    };
    auto send = [&](auto &rpc) {
      return rpc.template send_request<&Service::BatchGetReplicaList>(
          keys, tenant_id);
    };
    auto validate = [batch_size](const auto &response) {
      return response.size() == batch_size &&
             std::all_of(response.begin(), response.end(),
                         [](const auto &item) {
                           return item && item->replicas.size() == 1;
                         });
    };
    co_return co_await run_calls(client, iterations, pipeline, call, send,
                                 validate);
  }

  if (operation == "put-start") {
    const mooncake::UUID client_id{1, 2};
    const std::vector<std::uint64_t> slice_lengths(batch_size, 4096);
    mooncake::ReplicateConfig config;
    config.data_type = mooncake::ObjectDataType::KVCACHE;
    auto call = [&](auto &rpc) {
      return rpc.template call<&Service::BatchPutStart>(
          client_id, keys, slice_lengths, config, tenant_id);
    };
    auto send = [&](auto &rpc) {
      return rpc.template send_request<&Service::BatchPutStart>(
          client_id, keys, slice_lengths, config, tenant_id);
    };
    auto validate = [batch_size](const auto &response) {
      return response.size() == batch_size &&
             std::all_of(
                 response.begin(), response.end(),
                 [](const auto &item) { return item && item->size() == 1; });
    };
    co_return co_await run_calls(client, iterations, pipeline, call, send,
                                 validate);
  }

  if (operation == "put-end") {
    const mooncake::UUID client_id{1, 2};
    const auto replica_type = mooncake::ReplicaType::ALL;
    std::vector<mooncake::ObjectMeta> object_metas;
    object_metas.reserve(batch_size);
    for (const auto &key : keys) {
      object_metas.push_back({key, std::nullopt});
    }
    auto call = [&](auto &rpc) {
      return rpc.template call<&Service::BatchPutEnd>(client_id, object_metas,
                                                      replica_type, tenant_id);
    };
    auto send = [&](auto &rpc) {
      return rpc.template send_request<&Service::BatchPutEnd>(
          client_id, object_metas, replica_type, tenant_id);
    };
    auto validate = [batch_size](const auto &response) {
      return response.size() == batch_size &&
             std::all_of(response.begin(), response.end(),
                         [](const auto &item) { return item.has_value(); });
    };
    co_return co_await run_calls(client, iterations, pipeline, call, send,
                                 validate);
  }

  if (operation == "put-revoke") {
    const mooncake::UUID client_id{1, 2};
    const auto replica_type = mooncake::ReplicaType::ALL;
    auto call = [&](auto &rpc) {
      return rpc.template call<&Service::BatchPutRevoke>(
          client_id, keys, replica_type, tenant_id);
    };
    auto send = [&](auto &rpc) {
      return rpc.template send_request<&Service::BatchPutRevoke>(
          client_id, keys, replica_type, tenant_id);
    };
    auto validate = [batch_size](const auto &response) {
      return response.size() == batch_size &&
             std::all_of(response.begin(), response.end(),
                         [](const auto &item) { return item.has_value(); });
    };
    co_return co_await run_calls(client, iterations, pipeline, call, send,
                                 validate);
  }

  co_return false;
}

async_simple::coro::Lazy<int>
run_client(std::string host, std::string port, std::string operation,
           std::size_t batch_size, std::uint64_t iterations,
           std::size_t pipeline, std::uint64_t warmup) {
  coro_rpc::coro_rpc_client client;
  auto error = co_await client.connect(std::move(host), std::move(port));
  if (error) {
    std::cerr << "connect failed: " << error.message() << '\n';
    co_return 2;
  }
  if (!(co_await run_operation(client, operation, batch_size, warmup,
                               pipeline))) {
    std::cerr << "warmup call failed\n";
    co_return 3;
  }

  const auto started = std::chrono::steady_clock::now();
  if (!(co_await run_operation(client, operation, batch_size, iterations,
                               pipeline))) {
    std::cerr << "measured call failed\n";
    co_return 4;
  }
  const auto elapsed = std::chrono::steady_clock::now() - started;
  const auto elapsed_seconds = std::chrono::duration<double>(elapsed).count();
  const auto qps = static_cast<double>(iterations) / elapsed_seconds;
  const auto items_per_call =
      operation == "single-exists" || operation == "single-get" ? 1
                                                                  : batch_size;
  const auto item_qps = qps * static_cast<double>(items_per_call);
  const auto completion_us = elapsed_seconds * 1'000'000.0 / iterations;
  std::cout << std::fixed << std::setprecision(6)
            << "client=cpp operation=" << operation
            << " batch_size=" << batch_size << " iterations=" << iterations
            << " pipeline=" << pipeline << " elapsed_s=" << elapsed_seconds
            << std::setprecision(0) << " qps=" << qps
            << " item_qps=" << item_qps << std::setprecision(3)
            << " us_per_completion=" << completion_us << '\n';
  co_return 0;
}

struct MixedStats {
  std::vector<std::uint64_t> put_latencies_ns;
  std::vector<std::uint64_t> get_latencies_ns;
  std::vector<std::uint64_t> exists_latencies_ns;
  std::uint64_t put_batches{0};
  std::uint64_t get_batches{0};
  std::uint64_t exists_batches{0};
  std::uint64_t put_items{0};
  std::uint64_t put_start_success{0};
  std::uint64_t put_success{0};
  std::uint64_t put_no_handle{0};
  std::uint64_t put_other_failure{0};
  std::uint64_t get_hits{0};
  std::uint64_t get_misses{0};
  std::uint64_t get_errors{0};
  std::uint64_t exists_hits{0};
  std::uint64_t exists_misses{0};
  std::uint64_t exists_errors{0};
  std::uint64_t scheduled_late_requests{0};
  std::uint64_t maximum_scheduler_lag_ns{0};
  std::uint64_t elapsed_ns{0};
  bool rpc_failed{false};
};

struct MixedArguments {
  std::size_t batch_size{333};
  std::uint64_t operation_qps{150};
  std::uint64_t duration_seconds{30};
  std::uint64_t warmup_seconds{10};
  std::uint64_t prefill_objects{1'000'000};
  std::uint64_t hot_objects{500'000};
  std::uint64_t object_bytes{1'024};
};

std::uint64_t elapsed_ns(std::chrono::steady_clock::time_point begin) {
  return static_cast<std::uint64_t>(
      std::chrono::duration_cast<std::chrono::nanoseconds>(
          std::chrono::steady_clock::now() - begin)
          .count());
}

async_simple::coro::Lazy<bool>
mixed_batch_put(coro_rpc::coro_rpc_client &client,
                const mooncake::UUID &client_id,
                const std::vector<std::string> &keys,
                std::uint64_t object_bytes, MixedStats &stats,
                bool record_latency) {
  using Service = mooncake::WrappedMasterService;
  const std::string tenant_id = "default";
  std::vector<std::uint64_t> lengths(keys.size(), object_bytes);
  mooncake::ReplicateConfig config;
  config.data_type = mooncake::ObjectDataType::KVCACHE;

  const auto begin = std::chrono::steady_clock::now();
  auto started = co_await client.call<&Service::BatchPutStart>(
      client_id, keys, lengths, config, tenant_id);
  stats.put_batches++;
  stats.put_items += keys.size();
  if (!started) {
    std::cerr << "BatchPutStart transport error code="
              << started.error().code.val()
              << " message=" << started.error().msg << '\n';
    stats.rpc_failed = true;
    co_return false;
  }
  if (started->size() != keys.size()) {
    std::cerr << "BatchPutStart response size mismatch\n";
    stats.rpc_failed = true;
    co_return false;
  }

  std::vector<mooncake::ObjectMeta> metas;
  metas.reserve(keys.size());
  for (std::size_t index = 0; index < keys.size(); ++index) {
    const auto &item = started.value()[index];
    if (item) {
      stats.put_start_success++;
      metas.push_back({keys[index], std::nullopt});
    } else if (item.error() == mooncake::ErrorCode::NO_AVAILABLE_HANDLE) {
      stats.put_no_handle++;
    } else {
      stats.put_other_failure++;
    }
  }

  if (!metas.empty()) {
    auto finished = co_await client.call<&Service::BatchPutEnd>(
        client_id, metas, mooncake::ReplicaType::ALL, tenant_id);
    if (!finished) {
      std::cerr << "BatchPutEnd transport error code="
                << finished.error().code.val()
                << " message=" << finished.error().msg << '\n';
      stats.rpc_failed = true;
      co_return false;
    }
    if (finished->size() != metas.size()) {
      std::cerr << "BatchPutEnd response size mismatch\n";
      stats.rpc_failed = true;
      co_return false;
    }
    for (const auto &item : finished.value()) {
      if (item) {
        stats.put_success++;
      } else {
        stats.put_other_failure++;
      }
    }
  }
  if (record_latency) {
    stats.put_latencies_ns.push_back(elapsed_ns(begin));
  }
  co_return true;
}

async_simple::coro::Lazy<bool>
mixed_batch_get(coro_rpc::coro_rpc_client &client,
                const std::vector<std::string> &keys, MixedStats &stats,
                bool record_latency) {
  using Service = mooncake::WrappedMasterService;
  const auto begin = std::chrono::steady_clock::now();
  auto response = co_await client.call<&Service::BatchGetReplicaList>(
      keys, std::string{"default"});
  stats.get_batches++;
  if (!response) {
    std::cerr << "BatchGetReplicaList transport error code="
              << response.error().code.val()
              << " message=" << response.error().msg << '\n';
    stats.rpc_failed = true;
    co_return false;
  }
  if (response->size() != keys.size()) {
    std::cerr << "BatchGetReplicaList response size mismatch\n";
    stats.rpc_failed = true;
    co_return false;
  }
  for (const auto &item : response.value()) {
    if (item) {
      stats.get_hits++;
    } else if (item.error() == mooncake::ErrorCode::OBJECT_NOT_FOUND) {
      stats.get_misses++;
    } else {
      stats.get_errors++;
    }
  }
  if (record_latency) {
    stats.get_latencies_ns.push_back(elapsed_ns(begin));
  }
  co_return true;
}

async_simple::coro::Lazy<bool>
mixed_batch_exists(coro_rpc::coro_rpc_client &client,
                   const std::vector<std::string> &keys, MixedStats &stats,
                   bool record_latency) {
  using Service = mooncake::WrappedMasterService;
  const auto begin = std::chrono::steady_clock::now();
  auto response = co_await client.call<&Service::BatchExistKey>(
      keys, std::string{"default"});
  stats.exists_batches++;
  if (!response) {
    std::cerr << "BatchExistKey transport error code="
              << response.error().code.val()
              << " message=" << response.error().msg << '\n';
    stats.rpc_failed = true;
    co_return false;
  }
  if (response->size() != keys.size()) {
    std::cerr << "BatchExistKey response size mismatch\n";
    stats.rpc_failed = true;
    co_return false;
  }
  for (const auto &item : response.value()) {
    if (!item) {
      stats.exists_errors++;
    } else if (item.value()) {
      stats.exists_hits++;
    } else {
      stats.exists_misses++;
    }
  }
  if (record_latency) {
    stats.exists_latencies_ns.push_back(elapsed_ns(begin));
  }
  co_return true;
}

std::vector<std::string>
mixed_write_keys(std::size_t worker, std::uint64_t sequence,
                 std::size_t batch_size) {
  std::vector<std::string> keys;
  keys.reserve(batch_size);
  for (std::size_t index = 0; index < batch_size; ++index) {
    keys.push_back("mixed-write-w" + std::to_string(worker) + "-s" +
                   std::to_string(sequence) + "-i" + std::to_string(index));
  }
  return keys;
}

std::vector<std::string>
mixed_hot_batch(const std::vector<std::string> &hot_keys, std::size_t worker,
                std::uint64_t sequence, std::size_t batch_size) {
  std::vector<std::string> keys;
  keys.reserve(batch_size);
  const auto worker_offset = (hot_keys.size() / 3) * worker;
  const auto start =
      (sequence * batch_size + worker_offset) % hot_keys.size();
  for (std::size_t index = 0; index < batch_size; ++index) {
    keys.push_back(hot_keys[(start + index) % hot_keys.size()]);
  }
  return keys;
}

enum class MixedOperation { PUT = 0, GET = 1, EXISTS = 2 };

async_simple::coro::Lazy<bool>
run_rate_limited_phase(coro_rpc::coro_rpc_client &client,
                       MixedOperation operation,
                       const mooncake::UUID &client_id,
                       const std::vector<std::string> &hot_keys,
                       const MixedArguments &arguments,
                       std::uint64_t request_count,
                       std::uint64_t sequence_base, MixedStats &stats,
                       bool record_latency) {
  const auto phase_start = std::chrono::steady_clock::now();
  const auto interval_ns = 1'000'000'000ULL / arguments.operation_qps;
  for (std::uint64_t request = 0; request < request_count; ++request) {
    const auto deadline = phase_start +
                          std::chrono::nanoseconds(interval_ns * request);
    const auto now = std::chrono::steady_clock::now();
    if (now < deadline) {
      std::this_thread::sleep_until(deadline);
    } else if (request != 0) {
      const auto lag = static_cast<std::uint64_t>(
          std::chrono::duration_cast<std::chrono::nanoseconds>(now - deadline)
              .count());
      stats.scheduled_late_requests++;
      stats.maximum_scheduler_lag_ns =
          std::max(stats.maximum_scheduler_lag_ns, lag);
    }

    const auto sequence = sequence_base + request;
    bool ok = false;
    if (operation == MixedOperation::PUT) {
      auto keys = mixed_write_keys(0, sequence, arguments.batch_size);
      ok = co_await mixed_batch_put(client, client_id, keys,
                                    arguments.object_bytes, stats,
                                    record_latency);
    } else {
      const auto reader = operation == MixedOperation::GET ? 1 : 2;
      auto keys = mixed_hot_batch(hot_keys, reader, sequence,
                                  arguments.batch_size);
      if (operation == MixedOperation::GET) {
        ok = co_await mixed_batch_get(client, keys, stats, record_latency);
      } else {
        ok =
            co_await mixed_batch_exists(client, keys, stats, record_latency);
      }
    }
    if (!ok) {
      co_return false;
    }
  }
  co_return true;
}

async_simple::coro::Lazy<MixedStats>
run_rate_limited_worker(std::string host, std::string port,
                        MixedOperation operation,
                        const std::vector<std::string> &hot_keys,
                        MixedArguments arguments,
                        std::barrier<> &phase_barrier,
                        std::atomic<bool> &failed) {
  MixedStats stats;
  const auto warmup_requests =
      arguments.operation_qps * arguments.warmup_seconds;
  const auto measured_requests =
      arguments.operation_qps * arguments.duration_seconds;

  coro_rpc::coro_rpc_client client;
  auto error = co_await client.connect(std::move(host), std::move(port));
  if (error) {
    std::cerr << "mixed worker connect failed: " << error.message() << '\n';
    stats.rpc_failed = true;
    failed.store(true, std::memory_order_release);
    phase_barrier.arrive_and_drop();
    co_return stats;
  }
  const mooncake::UUID client_id{0xCAFE,
                                 static_cast<std::uint64_t>(operation) + 1};

  phase_barrier.arrive_and_wait();
  if (!(co_await run_rate_limited_phase(
          client, operation, client_id, hot_keys, arguments, warmup_requests,
          0, stats, false))) {
    failed.store(true, std::memory_order_release);
  }

  // All three streams finish warmup before the measured steady-state window.
  phase_barrier.arrive_and_wait();
  if (failed.load(std::memory_order_acquire)) {
    stats.rpc_failed = true;
    co_return stats;
  }
  stats = MixedStats{};
  if (operation == MixedOperation::PUT) {
    stats.put_latencies_ns.reserve(measured_requests);
  } else if (operation == MixedOperation::GET) {
    stats.get_latencies_ns.reserve(measured_requests);
  } else {
    stats.exists_latencies_ns.reserve(measured_requests);
  }
  const auto begin = std::chrono::steady_clock::now();
  if (!(co_await run_rate_limited_phase(
          client, operation, client_id, hot_keys, arguments,
          measured_requests, warmup_requests, stats, true))) {
    failed.store(true, std::memory_order_release);
  }
  stats.elapsed_ns = elapsed_ns(begin);
  co_return stats;
}

async_simple::coro::Lazy<MixedStats>
prefill_mixed_workload(std::string host, std::string port,
                       const MixedArguments &arguments,
                       const std::vector<std::string> &hot_keys) {
  MixedStats stats;
  coro_rpc::coro_rpc_client client;
  auto error = co_await client.connect(std::move(host), std::move(port));
  if (error) {
    std::cerr << "prefill connect failed: " << error.message() << '\n';
    stats.rpc_failed = true;
    co_return stats;
  }
  const mooncake::UUID client_id{0xBEEF, 1};
  std::uint64_t inserted = 0;
  while (inserted < arguments.prefill_objects) {
    const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(
        arguments.batch_size, arguments.prefill_objects - inserted));
    std::vector<std::string> keys;
    keys.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
      keys.push_back("mixed-prefill-" + std::to_string(inserted + index));
    }
    const auto successes_before = stats.put_success;
    if (!(co_await mixed_batch_put(client, client_id, keys,
                                   arguments.object_bytes, stats, false))) {
      co_return stats;
    }
    const auto successes = stats.put_success - successes_before;
    if (successes != count) {
      std::cerr << "prefill stopped after " << stats.put_success
                << " successful objects; requested "
                << arguments.prefill_objects << '\n';
      co_return stats;
    }
    inserted += count;
  }

  // Touch the hot tail after cold prefill so BatchGet and BatchExists start
  // from a stable hit set and grant the same leases on both implementations.
  for (std::size_t offset = 0; offset < hot_keys.size();
       offset += arguments.batch_size) {
    const auto count = std::min(arguments.batch_size, hot_keys.size() - offset);
    std::vector<std::string> keys(hot_keys.begin() + offset,
                                  hot_keys.begin() + offset + count);
    if (!(co_await mixed_batch_get(client, keys, stats, false)) ||
        !(co_await mixed_batch_exists(client, keys, stats, false))) {
      co_return stats;
    }
  }
  co_return stats;
}

void merge_mixed_stats(MixedStats &target, MixedStats source) {
  target.put_latencies_ns.insert(target.put_latencies_ns.end(),
                                 source.put_latencies_ns.begin(),
                                 source.put_latencies_ns.end());
  target.get_latencies_ns.insert(target.get_latencies_ns.end(),
                                 source.get_latencies_ns.begin(),
                                 source.get_latencies_ns.end());
  target.exists_latencies_ns.insert(target.exists_latencies_ns.end(),
                                    source.exists_latencies_ns.begin(),
                                    source.exists_latencies_ns.end());
  target.put_batches += source.put_batches;
  target.get_batches += source.get_batches;
  target.exists_batches += source.exists_batches;
  target.put_items += source.put_items;
  target.put_start_success += source.put_start_success;
  target.put_success += source.put_success;
  target.put_no_handle += source.put_no_handle;
  target.put_other_failure += source.put_other_failure;
  target.get_hits += source.get_hits;
  target.get_misses += source.get_misses;
  target.get_errors += source.get_errors;
  target.exists_hits += source.exists_hits;
  target.exists_misses += source.exists_misses;
  target.exists_errors += source.exists_errors;
  target.scheduled_late_requests += source.scheduled_late_requests;
  target.maximum_scheduler_lag_ns =
      std::max(target.maximum_scheduler_lag_ns,
               source.maximum_scheduler_lag_ns);
  target.elapsed_ns = std::max(target.elapsed_ns, source.elapsed_ns);
  target.rpc_failed = target.rpc_failed || source.rpc_failed;
}

std::uint64_t percentile_ns(std::vector<std::uint64_t> &samples,
                            std::uint64_t numerator,
                            std::uint64_t denominator) {
  if (samples.empty()) {
    return 0;
  }
  std::sort(samples.begin(), samples.end());
  const auto rank = (samples.size() * numerator + denominator - 1) /
                    denominator;
  return samples[std::min(samples.size() - 1, std::max<std::size_t>(1, rank) -
                                                  1)];
}

int run_mixed_client(const std::string &host, const std::string &port,
                     MixedArguments arguments) {
  if (arguments.batch_size < 2 || arguments.operation_qps == 0 ||
      arguments.duration_seconds == 0 || arguments.prefill_objects == 0 ||
      arguments.hot_objects == 0 ||
      arguments.hot_objects > arguments.prefill_objects ||
      arguments.object_bytes == 0) {
    std::cerr << "mixed workload requires batch_size>=2, qps/duration/"
                 "prefill/hot/object_bytes>0, and hot<=prefill\n";
    return 64;
  }

  std::vector<std::string> hot_keys;
  hot_keys.reserve(arguments.hot_objects);
  const auto hot_begin = arguments.prefill_objects - arguments.hot_objects;
  for (std::uint64_t index = hot_begin; index < arguments.prefill_objects;
       ++index) {
    hot_keys.push_back("mixed-prefill-" + std::to_string(index));
  }

  auto prefill = async_simple::coro::syncAwait(
      prefill_mixed_workload(host, port, arguments, hot_keys));
  std::cout << "mixed_prefill requested=" << arguments.prefill_objects
            << " successful=" << prefill.put_success
            << " get_hits=" << prefill.get_hits
            << " get_misses=" << prefill.get_misses
            << " exists_hits=" << prefill.exists_hits
            << " exists_misses=" << prefill.exists_misses << '\n';
  if (prefill.rpc_failed || prefill.put_success != arguments.prefill_objects ||
      prefill.get_misses != 0 || prefill.exists_misses != 0) {
    return 3;
  }

  constexpr std::size_t kOperationStreams = 3;
  std::barrier phase_barrier(
      static_cast<std::ptrdiff_t>(kOperationStreams));
  std::atomic<bool> failed{false};
  std::vector<MixedStats> worker_stats(kOperationStreams);
  std::vector<std::thread> workers;
  workers.reserve(kOperationStreams);
  for (std::size_t worker = 0; worker < kOperationStreams; ++worker) {
    workers.emplace_back([&, worker] {
      worker_stats[worker] = async_simple::coro::syncAwait(
          run_rate_limited_worker(host, port,
                                  static_cast<MixedOperation>(worker),
                                  hot_keys, arguments, phase_barrier, failed));
    });
  }
  for (auto &worker : workers) {
    worker.join();
  }

  MixedStats total;
  for (auto &stats : worker_stats) {
    merge_mixed_stats(total, std::move(stats));
  }
  if (total.rpc_failed) {
    return 4;
  }
  const auto elapsed_seconds = total.elapsed_ns / 1'000'000'000.0;
  const auto logical_batches =
      total.put_batches + total.get_batches + total.exists_batches;
  const auto logical_batch_qps = logical_batches / elapsed_seconds;
  const auto item_ops_per_second =
      logical_batch_qps * static_cast<double>(arguments.batch_size);
  const auto put_p50 = percentile_ns(total.put_latencies_ns, 50, 100) / 1000.0;
  const auto put_p99 = percentile_ns(total.put_latencies_ns, 99, 100) / 1000.0;
  const auto put_p999 =
      percentile_ns(total.put_latencies_ns, 999, 1000) / 1000.0;
  const auto get_p50 = percentile_ns(total.get_latencies_ns, 50, 100) / 1000.0;
  const auto get_p99 = percentile_ns(total.get_latencies_ns, 99, 100) / 1000.0;
  const auto get_p999 =
      percentile_ns(total.get_latencies_ns, 999, 1000) / 1000.0;
  const auto exists_p50 =
      percentile_ns(total.exists_latencies_ns, 50, 100) / 1000.0;
  const auto exists_p99 =
      percentile_ns(total.exists_latencies_ns, 99, 100) / 1000.0;
  const auto exists_p999 =
      percentile_ns(total.exists_latencies_ns, 999, 1000) / 1000.0;

  std::cout << std::fixed << std::setprecision(6)
            << "client=cpp operation=mixed-1:1:1"
            << " batch_size=" << arguments.batch_size
            << " target_qps_per_operation=" << arguments.operation_qps
            << " duration_seconds=" << arguments.duration_seconds
            << " warmup_seconds=" << arguments.warmup_seconds
            << " prefill_objects=" << arguments.prefill_objects
            << " hot_objects=" << arguments.hot_objects
            << " object_bytes=" << arguments.object_bytes
            << " elapsed_s=" << elapsed_seconds << std::setprecision(0)
            << " logical_batch_qps=" << logical_batch_qps
            << " item_ops_per_s=" << item_ops_per_second
            << " put_batches=" << total.put_batches
            << " get_batches=" << total.get_batches
            << " exists_batches=" << total.exists_batches
            << " put_items=" << total.put_items
            << " put_start_success=" << total.put_start_success
            << " put_success=" << total.put_success
            << " put_no_handle=" << total.put_no_handle
            << " put_other_failure=" << total.put_other_failure
            << " get_hits=" << total.get_hits
            << " get_misses=" << total.get_misses
            << " get_errors=" << total.get_errors
            << " exists_hits=" << total.exists_hits
            << " exists_misses=" << total.exists_misses
            << " exists_errors=" << total.exists_errors
            << " scheduled_late_requests="
            << total.scheduled_late_requests
            << " maximum_scheduler_lag_us="
            << total.maximum_scheduler_lag_ns / 1000.0
            << std::setprecision(3) << " put_p50_us=" << put_p50
            << " put_p99_us=" << put_p99 << " put_p999_us=" << put_p999
            << " get_p50_us=" << get_p50 << " get_p99_us=" << get_p99
            << " get_p999_us=" << get_p999
            << " exists_p50_us=" << exists_p50
            << " exists_p99_us=" << exists_p99
            << " exists_p999_us=" << exists_p999 << '\n';
  return 0;
}

int main(int argc, char **argv) {
  if (argc == 2 && std::string_view(argv[1]) == "metadata") {
    print_metadata();
    return 0;
  }
  if (argc >= 3 && std::string_view(argv[1]) == "server") {
    const auto port = static_cast<std::uint16_t>(std::stoul(argv[2]));
    const auto threads = argc >= 4 ? std::stoul(argv[3]) : 1;
    mooncake::WrappedMasterService service;
    coro_rpc::coro_rpc_server server(threads, port);
    server
        .register_handler<&mooncake::WrappedMasterService::ExistKey,
                          &mooncake::WrappedMasterService::GetReplicaList,
                          &mooncake::WrappedMasterService::BatchExistKey,
                          &mooncake::WrappedMasterService::BatchGetReplicaList,
                          &mooncake::WrappedMasterService::BatchPutStart,
                          &mooncake::WrappedMasterService::BatchPutEnd,
                          &mooncake::WrappedMasterService::BatchPutRevoke,
                          &mooncake::WrappedMasterService::GetStorageConfig,
                          &mooncake::WrappedMasterService::ServiceReady>(
            &service);
    std::cout << "cpp_mooncake_server_ready=127.0.0.1:" << port << std::endl;
    return !server.start();
  }
  if (argc >= 5 && std::string_view(argv[1]) == "client") {
    const std::string operation = argv[4];
    const auto batch_size = argc >= 6 ? std::max(1UL, std::stoul(argv[5])) : 16;
    const auto iterations = argc >= 7 ? std::stoull(argv[6]) : 100'000;
    const auto pipeline = argc >= 8 ? std::max(1UL, std::stoul(argv[7])) : 1;
    const auto warmup = argc >= 9 ? std::stoull(argv[8]) : 10'000;
    return async_simple::coro::syncAwait(run_client(
        argv[2], argv[3], operation, batch_size, iterations, pipeline, warmup));
  }
  if (argc >= 4 && std::string_view(argv[1]) == "mixed-client") {
    MixedArguments arguments;
    if (argc >= 5) {
      arguments.batch_size = std::max(2UL, std::stoul(argv[4]));
    }
    if (argc >= 6) {
      arguments.operation_qps = std::stoull(argv[5]);
    }
    if (argc >= 7) {
      arguments.duration_seconds = std::stoull(argv[6]);
    }
    if (argc >= 8) {
      arguments.warmup_seconds = std::stoull(argv[7]);
    }
    if (argc >= 9) {
      arguments.prefill_objects = std::stoull(argv[8]);
    }
    if (argc >= 10) {
      arguments.hot_objects = std::stoull(argv[9]);
    }
    if (argc >= 11) {
      arguments.object_bytes = std::stoull(argv[10]);
    }
    return run_mixed_client(argv[2], argv[3], arguments);
  }

  std::cerr
      << "usage: mooncake_benchmark server <port> [threads] | client <host> "
         "<port> <single-exists|single-get|exists|get|put-start|put-end|put-revoke> [batch-size] "
         "[iterations] [pipeline] [warmup] | mixed-client <host> <port> "
         "[batch-size] [qps-per-operation] [duration-seconds] "
         "[warmup-seconds] "
         "[prefill-objects] [hot-objects] [object-bytes]\n";
  return 64;
}
