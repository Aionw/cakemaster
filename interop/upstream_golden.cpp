// Generates the golden byte sequences used by the Rust compatibility tests.
//
// Build against a yalantinglibs checkout, for example:
//   g++ -std=c++20 -O2 -DNDEBUG \
//     -I /path/to/yalantinglibs/include \
//     -I /path/to/yalantinglibs/include/ylt/thirdparty \
//     interop/upstream_golden.cpp -o /tmp/upstream_golden

#include <array>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <string>
#include <tuple>
#include <vector>

#include <ylt/coro_rpc/impl/protocol/coro_rpc_protocol.hpp>
#include <ylt/struct_pack.hpp>
#include <ylt/util/utils.hpp>

struct person {
  std::int32_t id;
  std::string name;
};
YLT_REFL(person, id, name);

std::string echo(std::string value) { return value; }
std::int32_t add(std::int32_t lhs, std::int32_t rhs) { return lhs + rhs; }

template <typename Range>
void print_hex(std::string_view label, const Range &bytes) {
  std::cout << label << '=';
  for (std::size_t i = 0; i < bytes.size(); ++i) {
    const auto byte = bytes.data()[i];
    std::cout << std::hex << std::setfill('0') << std::setw(2)
              << static_cast<unsigned>(static_cast<std::uint8_t>(byte));
  }
  std::cout << std::dec << '\n';
}

template <typename T> void print_value(std::string_view label, const T &value) {
  const auto bytes = struct_pack::serialize(value);
  print_hex(label, bytes);
}

template <typename T> void print_type(std::string_view label) {
  constexpr auto literal = struct_pack::get_type_literal<T>();
  print_hex(label, literal);
  std::cout << label << "_hash=" << std::hex << struct_pack::get_type_code<T>()
            << std::dec << '\n';
}

int main() {
  using coro_rpc::protocol::coro_rpc_protocol;

  print_type<std::int32_t>("i32_type");
  print_type<std::string>("string_type");
  print_type<std::tuple<std::int32_t, std::string>>("tuple_type");
  print_type<person>("person_type");

  print_value("i32", std::int32_t{-42});
  print_value("string", std::string{"hello"});
  print_value("tuple", std::tuple{std::int32_t{7}, std::string{"cake"}});
  print_value("person", person{42, "Betty"});
  print_value("optional_some", std::optional<std::string>{"yes"});
  print_value("optional_none", std::optional<std::string>{});
  print_value("unit", std::monostate{});
  print_value("vector", std::vector<std::int32_t>{1, -2, 3});
  print_value("bool", true);
  print_value("u64", std::uint64_t{0x0102030405060708});
  print_value("double", 3.5);
  print_value("char32", U'\U0001F370');
  print_value("array", std::array<std::int16_t, 3>{1, -2, 3});
  print_value("map", std::map<std::string, std::int32_t>{{"a", 1}, {"b", 2}});
  print_value("set", std::set<std::int32_t>{-2, 1, 3});
  print_value("extended_error", std::pair<std::uint16_t, std::string>{
                                    1001, "expected interop error"});
  print_value("long_string", std::string(300, 'x'));

  coro_rpc_protocol::req_header request{};
  request.magic = coro_rpc_protocol::magic_number;
  request.version = coro_rpc_protocol::VERSION_NUMBER;
  request.serialize_type = 0;
  request.msg_type = 0;
  request.seq_num = 0x01020304;
  request.function_id = coro_rpc::func_id<echo>();
  request.length = 9;
  request.attach_length = 3;
  std::string request_bytes;
  struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
      request_bytes, request);
  print_hex("request_header", request_bytes);

  coro_rpc_protocol::resp_header response{};
  response.magic = coro_rpc_protocol::magic_number;
  response.version = coro_rpc_protocol::VERSION_NUMBER;
  response.err_code = 0;
  response.msg_type = 0;
  response.seq_num = 0x01020304;
  response.length = 9;
  response.attach_length = 3;
  std::string response_bytes;
  struct_pack::serialize_to<struct_pack::sp_config::DISABLE_ALL_META_INFO>(
      response_bytes, response);
  print_hex("response_header", response_bytes);

  std::cout << "echo_id=" << std::hex << coro_rpc::func_id<echo>() << '\n';
  std::cout << "add_id=" << std::hex << coro_rpc::func_id<add>() << '\n';
}
