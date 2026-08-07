// Wire-compatible no-state Mooncake Master RPC benchmark peer.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <optional>
#include <string>
#include <string_view>
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

enum class ErrorCode : std::int32_t {
  OK = 0,
  INVALID_PARAMS = -600,
  OBJECT_NOT_FOUND = -704,
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

struct ObjectMeta {
  std::string key;
  std::optional<std::uint64_t> object_checksum;
};
YLT_REFL(ObjectMeta, key, object_checksum);

struct ReplicateConfig {
  std::size_t replica_num{1};
  std::size_t nof_replica_num{0};
  bool with_soft_pin{false};
  bool with_hard_pin{false};
  std::vector<std::string> preferred_segments;
  std::string preferred_segment;
  std::vector<std::string> preferred_nof_segments;
  bool prefer_alloc_in_same_node{false};
  ObjectDataType data_type{ObjectDataType::UNKNOWN};
  std::string host_id;
  std::optional<std::vector<std::string>> group_ids;
};
YLT_REFL(ReplicateConfig, replica_num, nof_replica_num, with_soft_pin,
         with_hard_pin, preferred_segments, preferred_segment,
         preferred_nof_segments, prefer_alloc_in_same_node, data_type, host_id,
         group_ids);

using ExpectedBool = tl::expected<bool, ErrorCode>;
using ExpectedGetReplicaListResponse =
    tl::expected<GetReplicaListResponse, ErrorCode>;
using ExpectedReplicaDescriptors =
    tl::expected<std::vector<ReplicaDescriptor>, ErrorCode>;
using ExpectedVoid = tl::expected<void, ErrorCode>;

ReplicaDescriptor memory_replica() {
  return ReplicaDescriptor{1,
                           DescriptorVariant{MemoryDescriptor{BufferDescriptor{
                               4096, 0x1000, "tcp", "127.0.0.1:12345"}}},
                           ReplicaStatus::COMPLETE};
}

class WrappedMasterService {
public:
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

  const mooncake::UUID sample_client_id{1, 2};
  const std::vector<mooncake::ObjectMeta> sample_object_metas{
      {"benchmark-key-00000000", std::nullopt}};
  const std::string sample_tenant = "default";
  print_value("batch_put_end_tuple_sample",
              BatchPutEndRequest{sample_client_id, sample_object_metas,
                                 mooncake::ReplicaType::ALL, sample_tenant});
  print_values("batch_put_end_args_sample", sample_client_id,
               sample_object_metas, mooncake::ReplicaType::ALL, sample_tenant);

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
  const auto item_qps = qps * static_cast<double>(batch_size);
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
        .register_handler<&mooncake::WrappedMasterService::BatchExistKey,
                          &mooncake::WrappedMasterService::BatchGetReplicaList,
                          &mooncake::WrappedMasterService::BatchPutStart,
                          &mooncake::WrappedMasterService::BatchPutEnd,
                          &mooncake::WrappedMasterService::BatchPutRevoke>(
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

  std::cerr
      << "usage: mooncake_benchmark server <port> [threads] | client <host> "
         "<port> <exists|get|put-start|put-end|put-revoke> [batch-size] "
         "[iterations] [pipeline] [warmup]\n";
  return 64;
}
