# Oracle twin of test/example_pubsub_test.rb (real redis-rb gem).
# redis-rb subscribes on the client object and unsubscribes with the
# no-arg form; the flow and output are otherwise identical.
require "redis"

port = Integer(ARGV[0] || "6379")
redis = Redis.new(host: "127.0.0.1", port: port)
driver = Redis.new(host: "127.0.0.1", port: port)

redis.subscribe("one", "two") do |on|
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
      # explicit per-channel order — no-arg UNSUBSCRIBE confirmations are
      # server-internal order (nondeterministic run to run)
      redis.unsubscribe("one")
      redis.unsubscribe("two")
    end
  end

  on.unsubscribe do |channel, subscriptions|
    puts "Unsubscribed from ##{channel} (#{subscriptions} subscriptions)"
  end
end
