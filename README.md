# bilirelay

[Download](https://github.com/lemos1235/bilirelay/releases) → configure [config.toml](config.example.toml) → run `./bilirelay`.

Monitor a running relay in the terminal with `./bilirelay monitor` (reads `server.bind` and `server.admin_token` from `config.toml`; press `?` for keys).

Edits to `config.toml` apply without a restart (or send `SIGHUP`). With `server.admin_token` set, `GET /metrics` serves Prometheus metrics for the relay, each client and each upstream.
