# Circe

ODBC Proxy in Rust.

## Build

Use Rust stable.

```
cargo build --release
```

## Install

Just copy the release binary onto the target computer.

## Config

There's no .env, so everything goes through the actual env.

- `DSN`: full ODBC DSN, like the Node ODBC.
- `RUST_LOG`: see [env\_logger](http://doc.rust-lang.org/log/env_logger), controls logging, defaults to `info` just for this crate.
- `PORT`: port to listen on, defaults to 3000.
