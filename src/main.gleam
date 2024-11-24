import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import gleam/string
import glisten
import snag

import resp2.{type RESP, BulkString, SimpleString}

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_) { #(Nil, None) }, loop)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn loop(msg: glisten.Message(_), state: Nil, conn: glisten.Connection(_)) {
  // There was no Selector, so we shouldn't expect User messages
  let assert glisten.Packet(msg) = msg

  let respond_error = respond_error(_, conn)

  case resp2.parse(msg) {
    Ok(resp2.State(resp, _)) -> {
      case resp {
        resp2.Array([BulkString(cmd), ..args]) ->
          execute(cmd, args, state, conn)

        _ -> {
          respond_error("unexpected command format")
          actor.continue(state)
        }
      }
    }

    Error(err) -> {
      err |> snag.pretty_print |> respond_error
      actor.continue(state)
    }
  }
}

fn respond_error(err: String, conn: glisten.Connection(_)) {
  let msg = err |> resp2.SimpleError |> resp2.encode
  let assert Ok(_) = glisten.send(conn, msg)
  Nil
}

fn execute(
  cmd: String,
  args: List(RESP),
  state: state,
  conn: glisten.Connection(_),
) -> actor.Next(a, state) {
  let cmd = string.uppercase(cmd)

  let respond_ok = fn(payload: RESP) {
    let msg = payload |> resp2.encode
    let assert Ok(_) = glisten.send(conn, msg)
    Nil
  }
  let respond_error = respond_error(_, conn)

  case cmd {
    "PING" -> {
      io.debug("PING")
      SimpleString("PONG") |> respond_ok
      actor.continue(state)
    }

    "ECHO" -> {
      case args {
        [BulkString(s)] -> {
          io.debug("ECHO " <> s)
          BulkString(s) |> respond_ok
          actor.continue(state)
        }
        _ -> {
          respond_error("expected 1 argument")
          actor.continue(state)
        }
      }
    }

    cmd -> {
      let msg = "unknown command: " <> cmd
      io.debug(msg)
      respond_error(msg)
      actor.continue(state)
    }
  }
}
