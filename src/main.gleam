import gleam/bytes_builder
import gleam/erlang/process
import gleam/io
import gleam/option.{None}
import gleam/otp/actor
import glisten

pub fn main() {
  let assert Ok(_) =
    glisten.handler(fn(_conn) { #(Nil, None) }, loop)
    |> glisten.serve(6379)

  process.sleep_forever()
}

fn loop(_msg: glisten.Message(_), state: Nil, conn: glisten.Connection(_)) {
  // PING
  io.println("Received PING")
  let pong_resp = "+PONG\r\n"
  let assert Ok(_) = glisten.send(conn, bytes_builder.from_string(pong_resp))
  actor.continue(state)
}
