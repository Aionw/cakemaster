namespace rs api

typedef i64 UserId

struct User {
  1: required UserId id
  2: required string name
  3: optional list<string> tags
  4: optional map<string, i32> counters
  5: optional set<i16> levels
}

service DemoService {
  string echo(1: required string value)
  i32 add(1: required i32 left, 2: required i32 right)
  string ping()
  void fail()
  void attachment_echo() (coro_rpc.attachment = "true")
}
