// Minimal same-wire benchmark for comparing this crate with yalantinglibs.
//
// Build against the pinned yalantinglibs checkout:
//   g++ -std=c++20 -O3 -DNDEBUG \
//     -I /path/to/yalantinglibs/include \
//     -I /path/to/yalantinglibs/include/ylt/thirdparty \
//     interop/upstream_benchmark.cpp -pthread -o /tmp/upstream_benchmark

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include <async_simple/coro/Collect.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

std::int32_t add(std::int32_t left, std::int32_t right) {
  return left + right;
}

std::int32_t value_for(std::uint64_t index) {
  return static_cast<std::int32_t>(index % 1'000'000);
}

async_simple::coro::Lazy<bool> run_calls(
    coro_rpc::coro_rpc_client &client, std::uint64_t iterations,
    std::size_t pipeline) {
  if (pipeline == 1) {
    for (std::uint64_t index = 0; index < iterations; ++index) {
      const auto value = value_for(index);
      auto result = co_await client.call<add>(value, 1);
      if (!result || result.value() != value + 1) {
        co_return false;
      }
    }
    co_return true;
  }

  std::uint64_t issued = 0;
  while (issued < iterations) {
    const auto batch_size = static_cast<std::size_t>(
        std::min<std::uint64_t>(iterations - issued, pipeline));
    std::vector<async_simple::coro::Lazy<coro_rpc::async_rpc_result<int>>>
        pending;
    pending.reserve(batch_size);
    for (std::size_t offset = 0; offset < batch_size; ++offset) {
      const auto value = value_for(issued + offset);
      pending.push_back(co_await client.send_request<add>(value, 1));
    }
    auto results = co_await async_simple::coro::collectAll(std::move(pending));
    for (std::size_t offset = 0; offset < batch_size; ++offset) {
      const auto value = value_for(issued + offset);
      auto &result = results[offset].value();
      if (!result || result->result() != value + 1) {
        co_return false;
      }
    }
    issued += batch_size;
  }
  co_return true;
}

async_simple::coro::Lazy<int> run_client(
    std::string host, std::string port, std::uint64_t iterations,
    std::size_t pipeline, std::uint64_t warmup) {
  coro_rpc::coro_rpc_client client;
  auto error = co_await client.connect(std::move(host), std::move(port));
  if (error) {
    std::cerr << "connect failed: " << error.message() << '\n';
    co_return 2;
  }
  if (!(co_await run_calls(client, warmup, pipeline))) {
    std::cerr << "warmup call failed\n";
    co_return 3;
  }

  const auto started = std::chrono::steady_clock::now();
  if (!(co_await run_calls(client, iterations, pipeline))) {
    std::cerr << "measured call failed\n";
    co_return 4;
  }
  const auto elapsed = std::chrono::steady_clock::now() - started;
  const auto elapsed_seconds = std::chrono::duration<double>(elapsed).count();
  const auto qps = static_cast<double>(iterations) / elapsed_seconds;
  const auto completion_us = elapsed_seconds * 1'000'000.0 / iterations;
  std::cout << std::fixed << std::setprecision(6)
            << "client=cpp iterations=" << iterations
            << " pipeline=" << pipeline << " elapsed_s=" << elapsed_seconds
            << std::setprecision(0) << " qps=" << qps
            << std::setprecision(3)
            << " us_per_completion=" << completion_us << '\n';
  co_return 0;
}

int main(int argc, char **argv) {
  if (argc >= 3 && std::string_view(argv[1]) == "server") {
    const auto port = static_cast<std::uint16_t>(std::stoul(argv[2]));
    const auto threads = argc >= 4 ? std::stoul(argv[3]) : 1;
    coro_rpc::coro_rpc_server server(threads, port);
    server.register_handler<add>();
    std::cout << "cpp_server_ready=127.0.0.1:" << port << std::endl;
    return !server.start();
  }
  if (argc >= 4 && std::string_view(argv[1]) == "client") {
    const auto iterations = argc >= 5 ? std::stoull(argv[4]) : 200'000;
    const auto pipeline = argc >= 6 ? std::max(1UL, std::stoul(argv[5])) : 1;
    const auto warmup = argc >= 7 ? std::stoull(argv[6]) : 10'000;
    return async_simple::coro::syncAwait(
        run_client(argv[2], argv[3], iterations, pipeline, warmup));
  }

  std::cerr << "usage: upstream_benchmark server <port> [threads] | "
               "client <host> <port> [iterations] [pipeline] [warmup]\n";
  return 64;
}
