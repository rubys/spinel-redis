# Command surface over an injected transport. The transport duck owns the
# socket: write(bytes) -> Integer, read_some(max) -> String ("" = closed),
# close -> Integer. Injection is what makes the client logic dual-runtime
# testable (scripted transport under CRuby parity) while the real sp_net
# transport stays spinel-only.
#
# Commands are individual typed methods built on a typed Array<String>
# plumbing layer — deliberately no public call(*args) funnel.
require "redis/resp"

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
    @t.write(Resp.encode_command(argv))
    read_reply
  end

  def read_reply
    while true
      if @parser.try_next
        r = @parser.reply
        if r.kind == RespValue::ERROR
          raise "redis: " + r.str
        end
        return r
      end
      chunk = @t.read_some(65536)
      if chunk.bytesize == 0
        raise "redis: connection lost"
      end
      @parser.feed(chunk)
    end
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
  def opt_reply(argv)
    r = call_argv(argv)
    if r.kind == RespValue::NILV
      return nil
    end
    r.str
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

  def echo(s)
    str_reply(["ECHO", s])
  end

  def select(db)
    str_reply(["SELECT", db.to_s])
  end

  def flushdb
    str_reply(["FLUSHDB"])
  end

  # -- strings -----------------------------------------------------------

  def set(key, value)
    str_reply(["SET", key, value])
  end

  def get(key)
    opt_reply(["GET", key])
  end

  def del(key)
    int_reply(["DEL", key])
  end

  def exists?(key)
    bool_reply(["EXISTS", key])
  end

  def incr(key)
    int_reply(["INCR", key])
  end

  def decr(key)
    int_reply(["DECR", key])
  end

  def incrby(key, n)
    int_reply(["INCRBY", key, n.to_s])
  end

  def decrby(key, n)
    int_reply(["DECRBY", key, n.to_s])
  end

  def append(key, value)
    int_reply(["APPEND", key, value])
  end

  def strlen(key)
    int_reply(["STRLEN", key])
  end

  def expire(key, seconds)
    bool_reply(["EXPIRE", key, seconds.to_s])
  end

  def ttl(key)
    int_reply(["TTL", key])
  end

  def keys(pattern)
    strings_reply(["KEYS", pattern])
  end

  # -- lists -------------------------------------------------------------

  def lpush(key, value)
    int_reply(["LPUSH", key, value])
  end

  def rpush(key, value)
    int_reply(["RPUSH", key, value])
  end

  def lpop(key)
    opt_reply(["LPOP", key])
  end

  def rpop(key)
    opt_reply(["RPOP", key])
  end

  def llen(key)
    int_reply(["LLEN", key])
  end

  def lrange(key, start, stop)
    strings_reply(["LRANGE", key, start.to_s, stop.to_s])
  end

  # -- sets ----------------------------------------------------------------

  def sadd(key, member)
    int_reply(["SADD", key, member])
  end

  def srem(key, member)
    int_reply(["SREM", key, member])
  end

  def sismember(key, member)
    bool_reply(["SISMEMBER", key, member])
  end

  def smembers(key)
    strings_reply(["SMEMBERS", key])
  end

  def scard(key)
    int_reply(["SCARD", key])
  end

  # -- hashes ----------------------------------------------------------------

  def hset(key, field, value)
    int_reply(["HSET", key, field, value])
  end

  def hget(key, field)
    opt_reply(["HGET", key, field])
  end

  def hdel(key, field)
    int_reply(["HDEL", key, field])
  end

  def hgetall(key)
    flat = strings_reply(["HGETALL", key])
    out = {}
    i = 0
    while i < flat.length
      out[flat[i]] = flat[i + 1]
      i = i + 2
    end
    out
  end

  # -- pub/sub (publish side only; SUBSCRIBE mode is the next milestone) --

  def publish(channel, message)
    int_reply(["PUBLISH", channel, message])
  end
end
