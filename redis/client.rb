# Command surface over an injected transport. The transport duck owns the
# socket: write(bytes) -> Integer, read_some(max) -> String ("" = closed),
# close -> Integer. Injection is what makes the client logic dual-runtime
# testable (scripted transport under CRuby parity) while the real sp_net
# transport stays spinel-only.
#
# Commands are individual typed methods built on a typed Array<String>
# plumbing layer — deliberately no public call(*args) funnel.
require "redis/resp"
require "redis/pipeline"

class RedisClientCore
  def initialize(transport)
    @t = transport
    @parser = RespParser.new
  end

  def close
    @t.close
  end

  # -- plumbing ---------------------------------------------------------

  def call_argv(argv)
    send_bytes(Resp.encode_command(argv))
    read_reply
  end

  def send_bytes(data)
    if @t.write(data) < 0
      raise Redis::ConnectionError.new("redis: connection lost")
    end
    data.bytesize
  end

  # The next reply; an error reply raises as redis-rb's CommandError
  # family (Redis.command_error picks the subclass from its prefix).
  def read_reply
    r = read_value
    if r.kind == RespValue::ERROR
      raise Redis.command_error(r.str)
    end
    r
  end

  # The next whole reply, error replies included.
  def read_value
    while true
      if @parser.try_next
        return @parser.reply
      end
      chunk = @t.read_some(65536)
      if chunk.bytesize == 0
        raise Redis::ConnectionError.new("redis: connection lost")
      end
      @parser.feed(chunk)
    end
  end

  # redis-rb's pipelined: queue commands in the block, send them in one
  # write, read every reply, then raise the first error reply if there
  # was one. Returns the replies, coerced as the client's own methods
  # coerce them.
  def pipelined
    p = RedisPipeline.new
    yield p
    run_pipeline(p)
  end

  def run_pipeline(p)
    out = []
    n = p.argvs.length
    if n == 0
      return out
    end
    data = ""
    p.argvs.each do |argv|
      data = data + Resp.encode_command(argv)
    end
    send_bytes(data)
    failed = false
    first_error = ""
    i = 0
    while i < n
      r = read_value
      if r.kind == RespValue::ERROR && !failed
        failed = true
        first_error = r.str
      end
      RedisClientCore.push_coerced(out, r, p.kinds[i])
      i = i + 1
    end
    if failed
      raise Redis.command_error(first_error)
    end
    out
  end

  # redis-rb's multi: the block queues commands as pipelined's does, and
  # they go out wrapped in MULTI ... EXEC in one write, so the server
  # runs them as one transaction. Returns EXEC's replies, coerced as the
  # client's own methods coerce them.
  #
  # Errors come back as redis-rb raises them. A command the server
  # refuses to queue (an unknown command, a wrong arity) aborts the
  # whole transaction, and that refusal is raised, not EXECABORT. A
  # command that fails while EXEC runs (WRONGTYPE) doesn't stop the
  # others, as in Redis; the first such error is raised after every
  # reply is read. Either way the connection stays in step. An empty
  # block sends nothing and answers [].
  def multi
    p = RedisPipeline.new
    yield p
    run_multi(p)
  end

  def run_multi(p)
    out = []
    n = p.argvs.length
    if n == 0
      return out
    end
    data = Resp.encode_command(["MULTI"])
    p.argvs.each do |argv|
      data = data + Resp.encode_command(argv)
    end
    data = data + Resp.encode_command(["EXEC"])
    send_bytes(data)
    # MULTI's +OK, then one +QUEUED (or a refusal) per command
    queue_failed = false
    queue_error = ""
    i = 0
    while i <= n
      r = read_value
      if r.kind == RespValue::ERROR && !queue_failed
        queue_failed = true
        queue_error = r.str
      end
      i = i + 1
    end
    ex = read_value
    if queue_failed
      raise Redis.command_error(queue_error)
    end
    if ex.kind == RespValue::ERROR
      raise Redis.command_error(ex.str)
    end
    if ex.kind != RespValue::ARRAY
      raise Redis::BaseError, "redis: EXEC answered neither an Array nor an error"
    end
    failed = false
    first_error = ""
    i = 0
    ex.items.each do |r|
      if r.kind == RespValue::ERROR && !failed
        failed = true
        first_error = r.str
      end
      RedisClientCore.push_coerced(out, r, p.kinds[i])
      i = i + 1
    end
    if failed
      raise Redis.command_error(first_error)
    end
    out
  end

  # One queued command's reply, coerced by its recorded kind and pushed
  # onto out. Pushing in each branch, rather than returning the value,
  # keeps the coercion free of a union return type. An error reply is
  # pushed as its text; the caller raises the first one.
  def self.push_coerced(out, r, kind)
    if r.kind == RespValue::ERROR
      out.push(r.str)
    elsif kind == RedisPipeline::BOOL
      out.push(r.int == 1)
    elsif kind == RedisPipeline::INT
      out.push(r.int)
    elsif kind == RedisPipeline::OPT
      if r.nil_reply?
        out.push(nil)
      else
        out.push(r.str)
      end
    elsif kind == RedisPipeline::SET_NX
      out.push(!r.nil_reply?)
    else
      out.push(r.str)
    end
    nil
  end

  # A blocking command (BRPOP) waits up to its own timeout on the
  # server, so the read deadline is that timeout plus the configured
  # read_timeout, as redis-rb sets it. A timeout of 0 blocks on the
  # server until a value arrives, so the read waits as long too. The
  # transport's own deadline comes back afterwards.
  def blocking_reply(argv, timeout)
    base = @t.read_timeout
    if timeout <= 0.0
      @t.set_read_timeout(-1.0)
    elsif base >= 0.0
      @t.set_read_timeout(base + timeout)
    end
    begin
      r = call_argv(argv)
    ensure
      @t.set_read_timeout(base)
    end
    r
  end

  # LPUSH/RPUSH/SADD with one value or an Array of them, as redis-rb
  # takes either.
  def self.push_argv(cmd, key, value)
    argv = [cmd, key.to_s]
    if value.is_a?(Array)
      value.each do |v|
        argv.push(v.to_s)
      end
    else
      argv.push(value.to_s)
    end
    argv
  end

  # A command over one key or an Array of keys (UNLINK).
  def self.keys_argv(cmd, keys)
    argv = [cmd]
    if keys.is_a?(Array)
      keys.each do |k|
        argv.push(k.to_s)
      end
    else
      argv.push(keys.to_s)
    end
    argv
  end

  # SET key value, with redis-rb's ex: (seconds to live) and nx: (only
  # when the key is missing). ex: 0, the default, sets no TTL.
  def self.set_argv(key, value, ex, nx)
    argv = ["SET", key.to_s, value.to_s]
    if ex > 0
      argv.push("EX")
      argv.push(ex.to_s)
    end
    if nx
      argv.push("NX")
    end
    argv
  end

  # HSET key field value, or HSET key with every pair of a Hash, as
  # redis-rb's hset(key, field, value) and hset(key, hash) both write it.
  # A field with no value goes out short, for the server to refuse.
  def self.hset_argv(key, field, value)
    argv = ["HSET", key.to_s]
    if field.is_a?(Hash)
      field.each do |f, v|
        argv.push(f.to_s)
        argv.push(v.to_s)
      end
    else
      argv.push(field.to_s)
      if !value.nil?
        argv.push(value.to_s)
      end
    end
    argv
  end

  # Reply coercions. Status (+OK) and bulk both land in str.
  def str_reply(argv)
    call_argv(argv).str
  end

  def int_reply(argv)
    call_argv(argv).int
  end

  def bool_reply(argv)
    int_reply(argv) == 1
  end

  # Bulk-or-nil: redis-rb returns nil for a missing key.
  #
  # One nullable local rather than an early `return nil`: in a large
  # program, Spinel 6577e8393 typed the early-return form as String and
  # a nil reply reached the caller as stale text (a GET of a missing key
  # answered the GET command's own bytes).
  def opt_reply(argv)
    r = call_argv(argv)
    out = nil
    out = r.str unless r.nil_reply?
    out
  end

  def strings_reply(argv)
    out = []
    call_argv(argv).items.each do |x|
      out.push(x.str)
    end
    out
  end

  # -- connection / server ---------------------------------------------

  def ping
    str_reply(["PING"])
  end

  # AUTH password, or AUTH username password (Redis 6 ACLs), as
  # redis-rb's auth(*args) takes them.
  def auth(user_or_password, password = nil)
    if password.nil?
      return str_reply(["AUTH", user_or_password.to_s])
    end
    str_reply(["AUTH", user_or_password.to_s, password.to_s])
  end

  def echo(s)
    str_reply(["ECHO", s.to_s])
  end

  def select(db)
    str_reply(["SELECT", db.to_s])
  end

  def flushdb
    str_reply(["FLUSHDB"])
  end

  # -- strings -----------------------------------------------------------

  # "OK", or with nx: true, whether the key was set (redis-rb answers
  # true or false there, not "OK" or nil). ex: is seconds to live.
  def set(key, value, ex: 0, nx: false)
    r = call_argv(RedisClientCore.set_argv(key, value, ex, nx))
    if nx
      return !r.nil_reply?
    end
    r.str
  end

  def get(key)
    opt_reply(["GET", key.to_s])
  end

  def del(key)
    int_reply(["DEL", key.to_s])
  end

  # One key or an Array of them; how many were removed. The memory is
  # freed in the background, unlike DEL.
  def unlink(keys)
    int_reply(RedisClientCore.keys_argv("UNLINK", keys))
  end

  def exists?(key)
    bool_reply(["EXISTS", key.to_s])
  end

  # How many of the keys exist, as redis-rb's exists answers it (exists?
  # is the Boolean form).
  def exists(key)
    int_reply(["EXISTS", key.to_s])
  end

  def incr(key)
    int_reply(["INCR", key.to_s])
  end

  def decr(key)
    int_reply(["DECR", key.to_s])
  end

  def incrby(key, n)
    int_reply(["INCRBY", key.to_s, n.to_s])
  end

  def decrby(key, n)
    int_reply(["DECRBY", key.to_s, n.to_s])
  end

  def append(key, value)
    int_reply(["APPEND", key.to_s, value.to_s])
  end

  def strlen(key)
    int_reply(["STRLEN", key.to_s])
  end

  # SET with TTL in one round trip (Mastodon streaming's presence keys).
  def setex(key, seconds, value)
    str_reply(["SETEX", key.to_s, seconds.to_s, value.to_s])
  end

  def expire(key, seconds)
    bool_reply(["EXPIRE", key.to_s, seconds.to_s])
  end

  def ttl(key)
    int_reply(["TTL", key.to_s])
  end

  def keys(pattern)
    strings_reply(["KEYS", pattern.to_s])
  end

  # -- lists -------------------------------------------------------------

  # One value, or an Array of values pushed in order (so the last ends
  # up first); the list length.
  def lpush(key, value)
    int_reply(RedisClientCore.push_argv("LPUSH", key, value))
  end

  # One value, or an Array of values pushed in order; the list length.
  def rpush(key, value)
    int_reply(RedisClientCore.push_argv("RPUSH", key, value))
  end

  def lpop(key)
    opt_reply(["LPOP", key.to_s])
  end

  def rpop(key)
    opt_reply(["RPOP", key.to_s])
  end

  # Pops from the first of keys that has an element, waiting up to
  # timeout: seconds (0, the default, waits forever). [key, value], or []
  # when the timeout passes: redis-rb returns nil there, and [] keeps one
  # return type.
  def brpop(keys, timeout: 0)
    argv = ["BRPOP"]
    keys.each do |k|
      argv.push(k.to_s)
    end
    argv.push(timeout.to_s)
    r = blocking_reply(argv, timeout.to_f)
    out = []
    if r.kind == RespValue::NILV
      return out
    end
    r.items.each do |x|
      out.push(x.str)
    end
    out
  end

  def llen(key)
    int_reply(["LLEN", key.to_s])
  end

  def lrange(key, start, stop)
    strings_reply(["LRANGE", key.to_s, start.to_s, stop.to_s])
  end

  def ltrim(key, start, stop)
    str_reply(["LTRIM", key.to_s, start.to_s, stop.to_s])
  end

  # -- sets ----------------------------------------------------------------

  # One member, or an Array of members; how many were new.
  def sadd(key, member)
    int_reply(RedisClientCore.push_argv("SADD", key, member))
  end

  def srem(key, member)
    int_reply(["SREM", key.to_s, member.to_s])
  end

  def sismember(key, member)
    bool_reply(["SISMEMBER", key.to_s, member.to_s])
  end

  def smembers(key)
    strings_reply(["SMEMBERS", key.to_s])
  end

  def scard(key)
    int_reply(["SCARD", key.to_s])
  end

  # Yields every member of the set, walking SSCAN's cursor until it
  # comes back to 0, as redis-rb's sscan_each does with a block. match:
  # is SSCAN's MATCH glob and count: its COUNT hint. A member can be
  # yielded twice if the set changes during the walk, as with SSCAN
  # itself. The block form only: redis-rb's Enumerator without a block
  # isn't mirrored (ledger).
  def sscan_each(key, match: nil, count: nil)
    cursor = "0"
    while true
      argv = ["SSCAN", key.to_s, cursor]
      if !match.nil?
        argv.push("MATCH")
        argv.push(match.to_s)
      end
      if !count.nil?
        argv.push("COUNT")
        argv.push(count.to_s)
      end
      r = call_argv(argv)
      cursor = r.items[0].str
      r.items[1].items.each do |m|
        yield m.str
      end
      if cursor == "0"
        break
      end
    end
    nil
  end

  # Two-key intersection (examples/sets.rb); redis-rb's splat form is
  # narrowed to a typed arity.
  def sinter(key, other)
    strings_reply(["SINTER", key.to_s, other.to_s])
  end

  # -- hashes ----------------------------------------------------------------

  # hset(key, field, value), or hset(key, hash) for several fields in
  # one command; how many fields were new.
  def hset(key, field, value = nil)
    int_reply(RedisClientCore.hset_argv(key, field, value))
  end

  def hget(key, field)
    opt_reply(["HGET", key.to_s, field.to_s])
  end

  def hdel(key, field)
    int_reply(["HDEL", key.to_s, field.to_s])
  end

  def hgetall(key)
    flat = strings_reply(["HGETALL", key.to_s])
    out = {}
    i = 0
    while i < flat.length
      out[flat[i]] = flat[i + 1]
      i = i + 2
    end
    out
  end

  # -- sorted sets -----------------------------------------------------------

  # One score/member pair, as redis-rb's single-pair form: true when the
  # member is new. The score is written with to_s, the spelling redis-rb
  # sends (Float#to_s for a Float).
  def zadd(key, score, member)
    bool_reply(["ZADD", key.to_s, score.to_s, member.to_s])
  end

  # One member; true when it was there.
  def zrem(key, member)
    bool_reply(["ZREM", key.to_s, member.to_s])
  end

  def zcard(key)
    int_reply(["ZCARD", key.to_s])
  end

  # The member's score as a Float, or nil when it isn't in the set.
  def zscore(key, member)
    s = opt_reply(["ZSCORE", key.to_s, member.to_s])
    if s.nil?
      return nil
    end
    RedisClientCore.float_of(s)
  end

  # Members by rank. redis-rb's with_scores: form isn't mirrored (ledger).
  def zrange(key, start, stop)
    strings_reply(["ZRANGE", key.to_s, start.to_s, stop.to_s])
  end

  # Removes members scored min..max; the count removed. min and max are
  # written with to_s, so "-inf", "+inf" and "(5" go through as Redis
  # reads them.
  def zremrangebyscore(key, min, max)
    int_reply(["ZREMRANGEBYSCORE", key.to_s, min.to_s, max.to_s])
  end

  # Removes members ranked start..stop (negative counts from the
  # highest); the count removed.
  def zremrangebyrank(key, start, stop)
    int_reply(["ZREMRANGEBYRANK", key.to_s, start.to_s, stop.to_s])
  end

  # A score reply as redis-rb's Floatify reads it.
  def self.float_of(s)
    if s == "inf"
      return Float::INFINITY
    end
    if s == "-inf"
      return -Float::INFINITY
    end
    s.to_f
  end

  # -- scripting ---------------------------------------------------------------

  # redis-rb's script(:load, source), answering the script's SHA1
  # digest, and script(:flush), answering "OK". Other subcommands
  # (`:exists`, `:kill`) aren't mirrored and raise ArgumentError.
  def script(subcommand, source = "")
    sub = subcommand.to_s
    if sub == "load"
      return str_reply(["SCRIPT", "LOAD", source.to_s])
    end
    if sub == "flush"
      return str_reply(["SCRIPT", "FLUSH"])
    end
    raise ArgumentError, "script(:" + sub + ") isn't supported; only :load and :flush are"
  end

  # EVALSHA sha numkeys key... arg...: the script's bulk reply, or nil
  # when it returns nothing, as the one-member pops a Sidekiq-style
  # scheduler runs answer. keys and argv are Arrays of Strings, in
  # redis-rb's positional evalsha(sha, keys, argv) form. Integer and
  # Array replies aren't mirrored (ledger): they raise Redis::BaseError
  # rather than reach the caller as "", which redis-rb would never
  # answer. A script the server doesn't have raises
  # Redis::NoScriptError, so the caller can load it again.
  def evalsha(sha, keys, argv)
    cmd = ["EVALSHA", sha.to_s, keys.length.to_s]
    keys.each do |k|
      cmd.push(k.to_s)
    end
    argv.each do |a|
      cmd.push(a.to_s)
    end
    r = call_argv(cmd)
    if r.kind == RespValue::INT
      raise Redis::BaseError, "redis: EVALSHA answered an Integer; only a bulk string or nil reply is mirrored"
    end
    if r.kind == RespValue::ARRAY
      raise Redis::BaseError, "redis: EVALSHA answered an Array; only a bulk string or nil reply is mirrored"
    end
    out = nil
    out = r.str unless r.nil_reply?
    out
  end

  # -- pub/sub (publish side only; SUBSCRIBE mode is the next milestone) --

  def publish(channel, message)
    int_reply(["PUBLISH", channel.to_s, message.to_s])
  end
end

# redis-rb error classes; RedisClientCore has to exist first.
require "redis/errors"
