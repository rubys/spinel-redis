# redis (spinel-redis)

A pure spinel-Ruby Redis client speaking RESP2 over spinel's `sp_net`
sockets. The require string is `redis` and the command surface mirrors
[redis-rb](https://github.com/redis/redis-rb)'s contracts, so code written
against redis-rb (or transpiled from it) resolves here unchanged.

```ruby
require "redis"

r = Redis.new("127.0.0.1", 6379)
r.set("k", "v")
r.get("k")          # => "v"
r.get("missing")    # => nil
r.exists?("k")      # => true
r.hgetall("h")      # => {"f" => "1", ...}
```

Pub/sub mirrors redis-rb's block DSL. A subscribed connection is
dedicated (RESP2), so pubsub gets its own; the loop blocks until the
subscription count drains to zero, and handlers drive unsubscription:

```ruby
ps = Redis.pubsub("127.0.0.1", 6379)
ps.subscribe("news") do |on|
  on.subscribe   { |ch, count| }
  on.message     { |ch, msg| ps.unsubscribe_all if msg == "quit" }
  on.unsubscribe { |ch, count| }
end
# psubscribe / on.pmessage for patterns; subscribe_many for several channels
```

The blocking subscribe read parks scheduler-aware under `SP_THREADS`, so
a subscribe loop in one fiber coexists with other work.

No C, no FFI beyond the four `sp_net` externs the compiler links into
every binary: protocol encode/decode is pure Ruby (compiled to native
code by spinel), the same architecture as redis-rb's own default driver.
Bulk strings are binary-safe end to end (`write_bytes` with explicit
lengths on the send side, `:binstr` reads on the receive side — embedded
NUL and CRLF bytes round-trip through a real server; the live test
proves it).

## Architecture

Three layers, each testable one level down:

| file | what | tested how |
|---|---|---|
| `redis/resp.rb` | RESP2 encoding (pure functions) + incremental reply parser | dual-runtime: same test runs compiled and under CRuby |
| `redis/client.rb` | typed per-command methods over an injected transport duck | dual-runtime, against a scripted transport |
| `redis/pubsub.rb` | SUBSCRIBE-mode dispatch loop + block-DSL listener | dual-runtime (scripted) + compiled-only live test |
| `redis/sock.rb` + `redis/connection.rb` | the real `sp_net` transport | compiled-only live test against a real redis-server (committed snapshot) |

The parser is a `try_next`/`reply` state machine rather than a
value-or-nil API: booleans and typed ivars keep every call site
monomorphic. Commands are individual typed methods (`incrby(key, n)`,
`lrange(key, start, stop)`) built on a typed `Array<String>` plumbing
layer — there is deliberately no public `call(*args)` funnel.

`sp_net` recv parks scheduler-aware under `SP_THREADS`, so blocking-style
calls are already fiber-friendly.

## Examples

`examples/` ports redis-rb v5.4.1's examples; each has a gated copy in
`test/` running against a test-owned server, so `spin publish` gates on
the ported behavior:

- `basic.rb`, `incr-decr.rb`, `list.rb`, `sets.rb` — verbatim flows
  (these forced `[]`/`[]=`, `ltrim`, `sinter` into the client surface).
- `pubsub.rb` — reshaped from interactive (redis-cli in a second
  terminal) to self-driving: a second connection publishes from inside
  the subscriber's callbacks. `trap(:INT)` and the rescue/retry
  reconnect wrapper are not ported (ledger).
- `prefork.rb` — the `unicorn/` example's lesson translated to
  `sp_net_fork` discipline: close before fork, connect per worker after.
- Not ported: `dist_redis.rb` / `sentinel*` (v0.1 exclusions below) and
  `consistency.rb` (a random-driven soak tool; the API it exercises is
  covered by the lanes above).

## Tests

```sh
spin test          # resp + client + pubsub lanes also run under CRuby and must match
```

The live lane (`test/live_smoke_test.rb`) starts its own `redis-server`
on port 16391 (no persistence, shut down after) and carries a committed
`.expected`; it needs `redis-server`/`redis-cli` on PATH.

## v0.1 exclusion ledger

Deliberately not in this release, recorded rather than implied:

- **RESP3** — RESP2 only; no client-side caching push. A dedicated
  connection per subscriber is the model, as in redis-rb's default.
- **Non-blocking subscribe drain** — the subscribe loop is blocking
  (scheduler-parked); poll-loop embedders that need "dispatch only
  what's buffered" get that hook when the tep integration lands.
- **TLS** — `sp_net` has no TLS yet (matz/spinel#1054); TLS-only managed
  Redis endpoints (ElastiCache in-transit encryption etc.) need a local
  stunnel/nginx hop for now.
- **Sentinel / Cluster / `dist_redis`** — single connection to a single
  server. Mastodon supports Sentinel via env, so this is a future ledger
  entry, not a never.
- **AUTH/ACL, SCAN family, transactions, pipelining, unix sockets** —
  straightforward additions when a consumer needs them.
- Variadic command forms are narrowed to typed arities (`del(key)`,
  `sadd(key, member)`); `sadd` returns the Integer added-count, redis-rb's
  legacy Boolean mode is not mirrored.
- `Redis.new(host, port)` is positional for now (redis-rb uses kwargs/URL).

## Spinel notes

Two compiler defects found while building this, both with minimal repros
filed and workarounds in-tree:

- [matz/spinel#1773](https://github.com/matz/spinel/issues/1773) — `&&`
  evaluates a later operand's receiver before an earlier operand's side
  effect. Workaround: sequential statements instead of
  `x && obj.method == y` chains where the receiver depends on earlier
  effects.
- [matz/spinel#1775](https://github.com/matz/spinel/issues/1775) —
  `return <expr>` inside `begin` pops the rescue frame before evaluating
  the expression. Workaround: assign, then return the local.

(Also [#1774](https://github.com/matz/spinel/issues/1774): don't name a
class `Val`.)
