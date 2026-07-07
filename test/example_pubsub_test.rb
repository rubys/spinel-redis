# Gated copy of examples/pubsub.rb against a test-owned server.
require "redis"

module ExShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

def ex_connect(port)
  attempts = 0
  while true
    begin
      c = Redis.new("127.0.0.1", port)   # assign-then-return: matz/spinel#1775
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "example test: redis-server did not come up on " + port.to_s
      end
      ExShell.sp_net_shell_capture("sleep 0.1", 16)
    end
  end
end

ExShell.sp_net_shell_capture("redis-server --port 16405 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-ex-pubsub.pid --logfile /tmp/spinel-redis-ex-pubsub.log", 256)
driver = ex_connect(16405)
redis = Redis.pubsub("127.0.0.1", 16405)

# --- examples/pubsub.rb flow ---

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

# --- end flow ---

redis.close
driver.close
ExShell.sp_net_shell_capture("redis-cli -p 16405 shutdown nosave 2>/dev/null", 64)
