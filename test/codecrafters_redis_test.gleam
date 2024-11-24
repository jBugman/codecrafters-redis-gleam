import gleam/bit_array
import gleam/bytes_tree
import gleam/result
import gleeunit
import gleeunit/should
import snag

import resp2

pub fn main() {
  gleeunit.main()
}

fn encode(resp: resp2.RESP) -> Result(String, Nil) {
  resp2.encode(resp) |> bytes_tree.to_bit_array |> bit_array.to_string
}

pub fn marshal_simple_string_test() {
  resp2.SimpleString("OK")
  |> encode
  |> should.be_ok
  |> should.equal("+OK\r\n")
}

pub fn marshal_simple_error_test() {
  resp2.SimpleError("Unknown command")
  |> encode
  |> should.be_ok
  |> should.equal("-Unknown command\r\n")
}

pub fn marshal_integer_test() {
  resp2.Integer(123)
  |> encode
  |> should.be_ok
  |> should.equal(":123\r\n")
}

pub fn marshal_bulk_string_test() {
  resp2.BulkString("Hello, world!")
  |> encode
  |> should.be_ok
  |> should.equal("$13\r\nHello, world!\r\n")
}

pub fn marshal_array_test() {
  resp2.Array([resp2.BulkString("ECHO"), resp2.BulkString("hey")])
  |> encode
  |> should.be_ok
  |> should.equal("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n")
}

fn parse(s: String) -> snag.Result(resp2.RESP) {
  s
  |> bit_array.from_string
  |> resp2.parse
  |> result.map(fn(state) { state.data })
}

pub fn parse_simple_string_test() {
  parse("+OK\r\n")
  |> should.be_ok
  |> should.equal(resp2.SimpleString("OK"))
}

pub fn parse_integer_test() {
  parse(":123\r\n")
  |> should.be_ok
  |> should.equal(resp2.Integer(123))
}

pub fn parse_signed_integer_test() {
  parse(":-123\r\n")
  |> should.be_ok
  |> should.equal(resp2.Integer(-123))
}

pub fn parse_bad_integer_test() {
  parse(":1x3\r\n")
  |> should.be_error
}

pub fn parse_bulk_string_test() {
  parse("$13\r\nHello, world!\r\n")
  |> should.be_ok
  |> should.equal(resp2.BulkString("Hello, world!"))
}

pub fn parse_bulk_utf8_string_test() {
  parse("$4\r\n" <> "cão" <> "\r\n")
  |> should.be_ok
  |> should.equal(resp2.BulkString("cão"))
}

pub fn parse_bad_bulk_string_test() {
  parse("$4\r\n" <> "cão" <> "\n")
  |> should.be_error
}

pub fn parse_array_test() {
  parse("*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n")
  |> should.be_ok
  |> should.equal(
    resp2.Array([resp2.BulkString("ECHO"), resp2.BulkString("hey")]),
  )
}

pub fn parse_empty_array_test() {
  parse("*0\r\n")
  |> should.be_ok
  |> should.equal(resp2.Array([]))
}

pub fn parse_bad_array_test() {
  parse("*2\r\n$4\r\nECHO\r\n")
  |> should.be_error
}

pub fn parse_nested_array_test() {
  parse("*2\r\n*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n$3\r\nfoo\r\n")
  |> should.be_ok
  |> should.equal(
    resp2.Array([
      resp2.Array([resp2.BulkString("ECHO"), resp2.BulkString("hey")]),
      resp2.BulkString("foo"),
    ]),
  )
}

pub fn parse_heterogenous_array_test() {
  parse("*3\r\n+PONG\r\n$4\r\nECHO\r\n:123\r\n")
  |> should.be_ok
  |> should.equal(
    resp2.Array([
      resp2.SimpleString("PONG"),
      resp2.BulkString("ECHO"),
      resp2.Integer(123),
    ]),
  )
}
