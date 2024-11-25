import gleam/erlang/process.{type Subject}
import gleam/option.{None}
import gleam/otp/actor

import glisten
import snag

import command
import resp2
import store

pub fn main() {
  let assert Ok(state_pid) = store.start()

  let assert Ok(_) =
    glisten.handler(fn(_) { #(state_pid, None) }, loop)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn loop(
  msg: glisten.Message(_),
  state: Subject(store.Action),
  conn: glisten.Connection(_),
) {
  // There was no Selector, so we shouldn't expect User messages
  let assert glisten.Packet(msg) = msg

  let respond_error = command.respond_error(_, conn)

  case resp2.parse(msg) {
    Ok(resp2.State(resp, _)) -> {
      case resp {
        resp2.Array([resp2.BulkString(cmd), ..args]) ->
          command.execute(cmd, args, state, conn)

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
