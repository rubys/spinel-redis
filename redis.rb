# redis — a pure spinel-Ruby Redis client (RESP2) over sp_net sockets.
#
# The require string mirrors redis-rb so transpiled `require "redis"`
# resolves here; the command surface mirrors redis-rb's contracts
# (nil for missing keys, booleans for EXISTS/EXPIRE/SISMEMBER, Hash for
# HGETALL). See README for the v0.1 exclusion ledger (Sentinel, cluster,
# TLS-only endpoints, RESP3).
require "redis/resp"
require "redis/client"
require "redis/pubsub"
require "redis/sock"
require "redis/connection"

class Redis < RedisClientCore
  def initialize(host = "127.0.0.1", port = 6379)
    super(RedisTransport.new(host, port))
  end

  # A subscribed connection is a dedicated connection (RESP2), so pubsub
  # gets its own transport rather than sharing this client's.
  def self.pubsub(host = "127.0.0.1", port = 6379)
    RedisPubSub.new(RedisTransport.new(host, port))
  end
end
