import gleam/dict
import gleam/erlang/process.{type Subject}
import gleam/option.{type Option}
import gleam/otp/actor

pub type Key =
  String

pub type Value =
  String

type Store {
  Store(kv: dict.Dict(String, Value))
}

pub type Action {
  Set(Key, Value)
  Get(Key, Subject(Option(Value)))
}

pub fn start() {
  actor.start(Store(dict.new()), handle_message)
}

fn handle_message(msg: Action, store: Store) -> actor.Next(Action, Store) {
  case msg {
    Set(key, value) -> {
      store.kv |> dict.insert(key, value) |> Store |> actor.continue
    }

    Get(key, client) -> {
      let value = store.kv |> dict.get(key) |> option.from_result
      process.send(client, value)
      actor.continue(store)
    }
  }
}
