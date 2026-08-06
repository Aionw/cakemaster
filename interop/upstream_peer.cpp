// A small yalantinglibs peer used for manual bidirectional interoperability
// checks. It can run as either the C++ server or the C++ client.

#include <cstdint>
#include <iostream>
#include <string>
#include <string_view>

#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>
#include <ylt/coro_rpc/coro_rpc_context.hpp>
#include <ylt/coro_rpc/coro_rpc_server.hpp>

std::string echo(std::string value) { return value; }
std::int32_t add(std::int32_t lhs, std::int32_t rhs) { return lhs + rhs; }
std::string ping() { return "pong"; }
void fail(coro_rpc::context<void> context) {
  context.response_error(coro_rpc::err_code{1001}, "expected interop error");
}
void attachment_echo() {
  auto *context = coro_rpc::get_context();
  context->set_response_attachment(context->get_request_attachment());
}

async_simple::coro::Lazy<int> run_client(std::string host, std::string port) {
  coro_rpc::coro_rpc_client client;
  auto ec = co_await client.connect(std::move(host), std::move(port));
  if (ec) {
    std::cerr << "connect failed: " << ec.message() << '\n';
    co_return 2;
  }

  auto echo_result = co_await client.call<echo>("hello from C++");
  if (!echo_result || echo_result.value() != "hello from C++") {
    std::cerr << "echo call failed\n";
    co_return 3;
  }

  auto add_result = co_await client.call<add>(20, 22);
  if (!add_result || add_result.value() != 42) {
    std::cerr << "add call failed\n";
    co_return 4;
  }

  auto ping_result = co_await client.call<ping>();
  if (!ping_result || ping_result.value() != "pong") {
    std::cerr << "ping call failed\n";
    co_return 5;
  }

  auto fail_result = co_await client.call<fail>();
  if (fail_result || fail_result.error().code.val() != 1001 ||
      fail_result.error().msg != "expected interop error") {
    std::cerr << "extended error call failed\n";
    co_return 6;
  }

  client.set_req_attachment("C++ attachment");
  auto attachment_result = co_await client.call<attachment_echo>();
  if (!attachment_result || client.get_resp_attachment() != "C++ attachment") {
    std::cerr << "attachment call failed\n";
    co_return 7;
  }

  std::cout << "C++ client -> Rust server: OK\n";
  co_return 0;
}

int main(int argc, char **argv) {
  if (argc != 3 && argc != 4) {
    std::cerr << "usage: upstream_peer server <port> | client <host> <port>\n";
    return 64;
  }

  const std::string_view mode = argv[1];
  if (mode == "server" && argc == 3) {
    const auto port = static_cast<std::uint16_t>(std::stoi(argv[2]));
    coro_rpc::coro_rpc_server server(1, port);
    server.register_handler<echo, add, ping, fail, attachment_echo>();
    std::cout << "C++ coro_rpc server listening on " << port << std::endl;
    return !server.start();
  }
  if (mode == "client" && argc == 4) {
    return async_simple::coro::syncAwait(run_client(argv[2], argv[3]));
  }

  std::cerr << "invalid arguments\n";
  return 64;
}
