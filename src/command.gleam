import gleam/erlang/process.{type Subject}
import gleam/io
import gleam/option.{None, Some}
import gleam/otp/actor
import gleam/string

import glisten

import resp2.{type RESP, BulkString, NullString, SimpleString}
import store

pub fn execute(
  cmd: String,
  args: List(resp2.RESP),
  store_pid: Subject(store.Action),
  conn: glisten.Connection(_),
) -> actor.Next(a, Subject(store.Action)) {
  let cmd = string.uppercase(cmd)

  let respond_ok = fn(payload: RESP) {
    let msg = payload |> resp2.encode
    let assert Ok(_) = glisten.send(conn, msg)
    Nil
  }
  let respond_error = respond_error(_, conn)

  case cmd {
    "PING" -> {
      io.println("PING")
      SimpleString("PONG") |> respond_ok
      actor.continue(store_pid)
    }

    "ECHO" -> {
      case args {
        [BulkString(s)] -> {
          io.println("ECHO " <> s)
          BulkString(s) |> respond_ok
          actor.continue(store_pid)
        }
        _ -> {
          respond_error("expected 1 argument")
          actor.continue(store_pid)
        }
      }
    }

    "SET" -> {
      case args {
        [BulkString(key), BulkString(value)] -> {
          io.println("SET " <> key <> " " <> value)
          process.send(store_pid, store.Set(key, value))
          SimpleString("OK") |> respond_ok
          actor.continue(store_pid)
        }

        _ -> {
          respond_error("expected 2 arguments")
          actor.continue(store_pid)
        }
      }
    }

    "GET" -> {
      case args {
        [BulkString(key)] -> {
          io.println("GET " <> key)

          let value = process.call(store_pid, store.Get(key, _), 10)

          case value {
            Some(s) -> BulkString(s) |> respond_ok

            None -> NullString |> respond_ok
          }
          actor.continue(store_pid)
        }

        _ -> {
          respond_error("expected 1 argument")
          actor.continue(store_pid)
        }
      }
    }

    cmd -> {
      let msg = "unknown command: " <> cmd
      io.println_error(msg)
      respond_error(msg)
      actor.continue(store_pid)
    }
  }
}

pub fn respond_error(err: String, conn: glisten.Connection(_)) {
  let msg = err |> resp2.SimpleError |> resp2.encode
  let assert Ok(_) = glisten.send(conn, msg)
  Nil
}
