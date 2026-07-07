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

Embedders with their own event loop skip the blocking loop entirely:
`subscribe_start` sends the command, `fd` exposes the socket, and
`drain(listener)` does one read plus dispatch of every complete push —
park on the fd however you like between drains. This is the surface
`Tep::RedisFeed` (roundhouse) rides to feed tep's in-process broadcast
registry from cross-process Redis pub/sub:

```ruby
l = RedisListener.new
l.message { |ch, msg| Broadcast.publish(ch, msg) }
ps.subscribe_start("timeline:1")
loop { ps.drain(l) if fd_readable?(ps.fd) }   # io_wait / poll between
```

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
  (these forced `ltrim` and `sinter` into the client surface). One
  correction to upstream: `basic.rb`'s `r['foo'] = 'bar'` sugar was
  removed from redis-rb in 5.0 — as written upstream it falls through
  `method_missing` and sends a literal `[]=` command to the server. The
  oracle harness caught this; the port (and this client) uses set/get.
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
sh oracle/run.sh   # replay every snapshot flow through the REAL redis-rb gem
```

The live lanes start their own `redis-server` on private ports (no
persistence, shut down after) and carry committed `.expected` snapshots;
they need `redis-server`/`redis-cli` on PATH.

`oracle/run.sh` is the second conformance axis: each `oracle/*.rb` is a
twin of a snapshot-gated flow, run under CRuby against the *real*
redis-rb gem (no `-I`, so `require "redis"` resolves to the gem), and
its output is diffed against the same committed snapshots the compiled
client froze. Zero hand-authored expectations; a diff on either side is
a contract divergence. Snapshot-determinism rules the oracle enforced:
set replies are sorted, and unsubscribes are explicit per-channel (the
no-arg form's confirmation order is server-internal and flaps between
runs).

## v0.1 exclusion ledger

Deliberately not in this release, recorded rather than implied:

- **RESP3** — RESP2 only; no client-side caching push. A dedicated
  connection per subscriber is the model, as in redis-rb's default.
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

Three compiler defects were found while building this, filed with
minimal repros, and **all three were fixed upstream the same day**
(spinel a7e42e90):

- [matz/spinel#1773](https://github.com/matz/spinel/issues/1773) — `&&`
  evaluated a later operand's receiver before an earlier operand's side
  effect. Fixed (38923ee9).
- [matz/spinel#1775](https://github.com/matz/spinel/issues/1775) —
  `return <expr>` inside `begin` popped the rescue frame before
  evaluating. Fixed (5c56c94c).
- [matz/spinel#1774](https://github.com/matz/spinel/issues/1774) — user
  class named `Val` collided with a runtime typedef. Fixed (3e7173e9 —
  user classes now get a distinct C stem).

The in-tree code kept the workaround-era shapes (sequential statements,
assign-then-return) — they're valid Ruby either way and keep the
package compiling on pre-fix spinel builds.
