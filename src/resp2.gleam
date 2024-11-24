//// RESP (REdis Serialization Protocol) implementation version 2, see https://redis.io/docs/latest/develop/reference/protocol-spec/

import gleam/bit_array
import gleam/bytes_tree.{type BytesTree}
import gleam/int
import gleam/list
import gleam/result
import gleam/string
import snag

pub type RESP {
  SimpleString(String)
  SimpleError(String)
  Integer(Int)
  BulkString(String)
  Array(List(RESP))
}

const crlf = "\r\n"

pub fn encode(resp: RESP) -> BytesTree {
  case resp {
    SimpleString(s) ->
      bytes_tree.from_string("+")
      |> bytes_tree.append_string(s)
      |> bytes_tree.append_string(crlf)

    SimpleError(s) ->
      bytes_tree.from_string(s)
      |> bytes_tree.prepend_string("-")
      |> bytes_tree.append_string(crlf)

    Integer(i) ->
      bytes_tree.from_string(":")
      |> bytes_tree.append_string(int.to_string(i))
      |> bytes_tree.append_string(crlf)

    BulkString(s) -> {
      let len = string.length(s) |> int.to_string

      bytes_tree.from_string("$")
      |> bytes_tree.append_string(len)
      |> bytes_tree.append_string(crlf)
      |> bytes_tree.append_string(s)
      |> bytes_tree.append_string(crlf)
    }

    Array(xs) -> {
      let len = list.length(xs) |> int.to_string

      let payload =
        bytes_tree.from_string("*")
        |> bytes_tree.append_string(len)
        |> bytes_tree.append_string(crlf)

      list.map(xs, encode)
      |> bytes_tree.concat
      |> bytes_tree.prepend_tree(payload)
    }
  }
}

fn with_context(res: Result(a, err), context: String) -> snag.Result(a) {
  result.replace_error(res, snag.new(context))
}

pub type State {
  State(data: RESP, remaining: BitArray)
}

pub fn parse(input: BitArray) -> snag.Result(State) {
  case input {
    <<"+":utf8, input:bits>> -> parse_simple_string(input, <<>>)
    <<":":utf8, input:bits>> -> parse_integer(input, <<>>)
    <<"$":utf8, input:bits>> -> parse_bulk_string(input)
    <<"*":utf8, input:bits>> -> parse_array(input)
    _ -> snag.error("invalid resp: unexpected first byte")
  }
}

fn parse_simple_string(input: BitArray, acc: BitArray) -> snag.Result(State) {
  case input {
    <<>> -> snag.error("invalid resp: not enough input")

    <<"\r\n":utf8, rest:bits>> -> {
      use s <- result.try(
        acc
        |> bit_array.to_string
        |> with_context("invalid resp: failed to decode bytes"),
      )
      Ok(State(SimpleString(s), rest))
    }

    <<char, rest:bits>> -> parse_simple_string(rest, <<acc:bits, char>>)

    _ -> snag.error("invalid resp: unexpected input")
  }
}

fn parse_integer(input: BitArray, acc: BitArray) -> snag.Result(State) {
  use #(i, rest) <- result.try(parse_int(input, acc))

  Ok(State(Integer(i), rest))
}

fn parse_int(input: BitArray, acc: BitArray) -> snag.Result(#(Int, BitArray)) {
  case input {
    <<>> -> snag.error("invalid resp: not enough input")

    <<"\r\n":utf8, rest:bits>> -> {
      use s <- result.try(
        acc
        |> bit_array.to_string
        |> with_context("invalid resp: failed to decode bytes"),
      )
      use i <- result.try(
        s
        |> int.parse
        |> with_context("invalid resp: failed to parse integer"),
      )
      Ok(#(i, rest))
    }

    <<num:signed, rest:bits>> -> parse_int(rest, <<acc:bits, num>>)

    _ -> snag.error("invalid resp: unexpected input")
  }
}

fn parse_bulk_string(input: BitArray) -> snag.Result(State) {
  use #(len, input) <- result.try(parse_int(input, <<>>))

  let total_len = bit_array.byte_size(input)
  use data <- result.try(
    bit_array.slice(input, 0, len)
    |> with_context("invalid resp: failed to slice bulk string"),
  )
  use rest <- result.try(
    bit_array.slice(input, len, total_len - len)
    |> with_context("invalid resp: failed to slice bulk string"),
  )

  case rest {
    <<"\r\n":utf8, rest:bits>> -> {
      use s <- result.try(
        data
        |> bit_array.to_string
        |> with_context("invalid resp: failed to decode bytes"),
      )
      Ok(State(BulkString(s), rest))
    }

    _ -> snag.error("invalid resp: unexpected input")
  }
}

fn parse_array(input: BitArray) -> snag.Result(State) {
  use #(len, input) <- result.try(parse_int(input, <<>>))
  use #(elements, rest) <- result.try(parse_element(input, [], len))
  elements |> list.reverse |> Array |> State(rest) |> Ok
}

fn parse_element(
  input: BitArray,
  acc: List(RESP),
  count: Int,
) -> snag.Result(#(List(RESP), BitArray)) {
  case count {
    0 -> Ok(#(acc, input))
    _ -> {
      use State(data, rest) <- result.try(parse(input))
      parse_element(rest, [data, ..acc], count - 1)
    }
  }
}
