# Ported from redis-rb v5.4.1 examples/pubsub.rb, reshaped from an
# interactive demo (`redis-cli publish ...` from a second terminal) into
# a self-driving one: a second connection publishes from inside the
# subscriber's own callbacks, so the flow — and the output — is
# deterministic.
#
# Mapping from upstream:
# - redis-rb subscribes on the client object; here a subscribed
#   connection is a dedicated RedisPubSub connection (Redis.pubsub).
# - `redis.subscribe(:one, :two)` -> subscribe_many(["one", "two"]).
# - `redis.unsubscribe` (no args = all) -> unsubscribe_all.
# Not ported (v0.1 ledger): trap(:INT), and the
# rescue-BaseConnectionError/sleep/retry reconnect wrapper — RedisPubSub
# raises on a lost connection and reconnect policy belongs to the caller.
#
# Gated copy: test/example_pubsub_test.rb
require "redis"

redis = Redis.pubsub
driver = Redis.new

redis.subscribe_many(["one", "two"]) do |on|
  on.subscribe do |channel, subscriptions|
    puts "Subscribed to ##{channel} (#{subscriptions} subscriptions)"
    if subscriptions == 2
      driver.publish("one", "hello")
    end
  end

  on.message do |channel, message|
    puts "##{channel}: #{message}"
    if message == "hello"
      driver.publish("two", "exit")
    end
    if message == "exit"
      redis.unsubscribe_all
    end
  end

  on.unsubscribe do |channel, subscriptions|
    puts "Unsubscribed from ##{channel} (#{subscriptions} subscriptions)"
  end
end
