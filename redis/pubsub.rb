# SUBSCRIBE-mode client, mirroring redis-rb's shape:
#
#   ps = Redis.pubsub("127.0.0.1", 6379)
#   ps.subscribe("news") do |on|
#     on.subscribe   { |ch, count| ... }
#     on.message     { |ch, msg| ps.unsubscribe_all if done }
#     on.unsubscribe { |ch, count| ... }
#   end                                  # blocks until the count hits 0
#
# A subscribed connection is a dedicated connection (RESP2 model: once in
# subscribe mode only (p)subscribe/(p)unsubscribe traffic is legal), so
# this class owns its own transport rather than sharing Redis's.
#
# The blocking read parks scheduler-aware under SP_THREADS (sp_net_wait_io
# routes to sp_sched_wait_io), so a subscribe loop in one fiber coexists
# with other work.
#
# Handler blocks live in ivars initialized to no-op lambdas — never nil,
# so dispatch stays monomorphic (no poly Proc|nil receivers).
require "redis/resp"

class RedisListener
  def initialize
    @on_subscribe    = ->(channel, count) { }
    @on_message      = ->(channel, message) { }
    @on_unsubscribe  = ->(channel, count) { }
    @on_psubscribe   = ->(pattern, count) { }
    @on_pmessage     = ->(pattern, channel, message) { }
    @on_punsubscribe = ->(pattern, count) { }
  end

  # registration (the `on.` surface inside the subscribe block)

  def subscribe(&blk)
    @on_subscribe = blk
  end

  def message(&blk)
    @on_message = blk
  end

  def unsubscribe(&blk)
    @on_unsubscribe = blk
  end

  def psubscribe(&blk)
    @on_psubscribe = blk
  end

  def pmessage(&blk)
    @on_pmessage = blk
  end

  def punsubscribe(&blk)
    @on_punsubscribe = blk
  end

  # dispatch (called by RedisPubSub's loop)

  def fire_subscribe(channel, count)
    @on_subscribe.call(channel, count)
  end

  def fire_message(channel, message)
    @on_message.call(channel, message)
  end

  def fire_unsubscribe(channel, count)
    @on_unsubscribe.call(channel, count)
  end

  def fire_psubscribe(pattern, count)
    @on_psubscribe.call(pattern, count)
  end

  def fire_pmessage(pattern, channel, message)
    @on_pmessage.call(pattern, channel, message)
  end

  def fire_punsubscribe(pattern, count)
    @on_punsubscribe.call(pattern, count)
  end
end

class RedisPubSub
  def initialize(transport)
    @t = transport
    @parser = RespParser.new
    @count = 0
    @started = false
  end

  # Active (p)subscription count as last reported by the server.
  def subscription_count
    @count
  end

  # Subscribe to one channel and run the dispatch loop until the
  # subscription count returns to zero. Multi-channel form below;
  # redis-rb's splat is narrowed to typed arities.
  def subscribe(channel, &blk)
    listener = RedisListener.new
    blk.call(listener)
    @t.write(Resp.encode_command(["SUBSCRIBE", channel.to_s]))
    run(listener)
  end

  def subscribe_many(channels, &blk)
    listener = RedisListener.new
    blk.call(listener)
    argv = ["SUBSCRIBE"]
    channels.each do |c|
      argv.push(c.to_s)   # keeps argv a typed Array<String> even when the
    end                   # caller's array arrives as a poly array
    @t.write(Resp.encode_command(argv))
    run(listener)
  end

  def psubscribe(pattern, &blk)
    listener = RedisListener.new
    blk.call(listener)
    @t.write(Resp.encode_command(["PSUBSCRIBE", pattern.to_s]))
    run(listener)
  end

  # -- event-loop embedding surface -------------------------------------
  #
  # The blocking subscribe() above owns the read loop. Embedders with
  # their own loop (a poll round, Tep::Scheduler.io_wait) instead:
  #
  #   l = RedisListener.new
  #   l.message { |ch, m| ... }
  #   ps.subscribe_start("chan")
  #   loop: wait until ps.fd is readable, then ps.drain(l)
  #
  # drain() performs ONE transport read (prompt when the caller waited
  # for readability) and dispatches every complete reply buffered.

  # The subscribed connection's socket fd (requires a transport that
  # exposes fd, as RedisTransport does).
  def fd
    @t.fd
  end

  # Send (P)SUBSCRIBE without entering a read loop.
  def subscribe_start(channel)
    @t.write(Resp.encode_command(["SUBSCRIBE", channel.to_s]))
  end

  def psubscribe_start(pattern)
    @t.write(Resp.encode_command(["PSUBSCRIBE", pattern.to_s]))
  end

  # One read + dispatch of everything complete. Returns the number of
  # replies dispatched (0 = a partial reply is still assembling).
  # Raises when the connection is gone, same as the blocking loop.
  def drain(listener)
    chunk = @t.read_some(65536)
    if chunk.bytesize == 0
      raise "redis: connection lost (subscribe mode)"
    end
    @parser.feed(chunk)
    n = 0
    while @parser.try_next
      dispatch(@parser.reply, listener)
      n = n + 1
    end
    n
  end

  # Callable from inside handler blocks: the commands go out on the same
  # connection; the confirmations come back through the running loop.
  def unsubscribe(channel)
    @t.write(Resp.encode_command(["UNSUBSCRIBE", channel.to_s]))
  end

  def unsubscribe_all
    @t.write(Resp.encode_command(["UNSUBSCRIBE"]))
  end

  def punsubscribe(pattern)
    @t.write(Resp.encode_command(["PUNSUBSCRIBE", pattern.to_s]))
  end

  def punsubscribe_all
    @t.write(Resp.encode_command(["PUNSUBSCRIBE"]))
  end

  def close
    @t.close
  end

  def run(listener)
    while true
      if @parser.try_next
        dispatch(@parser.reply, listener)
        if @started
          if @count == 0
            return
          end
        end
      else
        chunk = @t.read_some(65536)
        if chunk.bytesize == 0
          raise "redis: connection lost (subscribe mode)"
        end
        @parser.feed(chunk)
      end
    end
  end

  # One push array from the server. Confirmations carry the new
  # subscription count as their integer third element; message payloads
  # are bulk (binary-safe).
  def dispatch(r, listener)
    if r.kind == RespValue::ERROR
      raise "redis: " + r.str
    end
    if r.kind != RespValue::ARRAY
      return                       # e.g. +PONG if a ping was sent; ignore
    end
    items = r.items
    kind = items[0].str
    if kind == "message"
      listener.fire_message(items[1].str, items[2].str)
    elsif kind == "pmessage"
      listener.fire_pmessage(items[1].str, items[2].str, items[3].str)
    elsif kind == "subscribe"
      @count = items[2].int
      @started = true
      listener.fire_subscribe(items[1].str, items[2].int)
    elsif kind == "unsubscribe"
      @count = items[2].int
      listener.fire_unsubscribe(items[1].str, items[2].int)
    elsif kind == "psubscribe"
      @count = items[2].int
      @started = true
      listener.fire_psubscribe(items[1].str, items[2].int)
    elsif kind == "punsubscribe"
      @count = items[2].int
      listener.fire_punsubscribe(items[1].str, items[2].int)
    end
  end
end
