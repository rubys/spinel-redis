# End-to-end SUBSCRIBE against a real redis-server, spinel-only
# (committed .expected; the ffi graph can't load under CRuby). The test
# owns its server on a private port. Everything is single-threaded and
# handler-driven, so ordering is deterministic: the publisher connection
# only publishes from inside the subscriber's own callbacks.
require "redis"

module PubSubShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

def live_connect(port)
  attempts = 0
  while true
    begin
      # assign-then-return: matz/spinel#1775
      c = Redis.new("127.0.0.1", port)
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "pubsub_live: redis-server did not come up on port " + port.to_s
      end
      PubSubShell.sp_net_shell_capture("sleep 0.1", 16)
    end
  end
end

PubSubShell.sp_net_shell_capture("redis-server --port 16392 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-pubsub.pid --logfile /tmp/spinel-redis-pubsub.log", 256)

publisher = live_connect(16392)
ps = Redis.pubsub("127.0.0.1", 16392)

# --- subscribe leg: two messages (one binary), then unsubscribe ------------

bin = [112, 0, 13, 10, 113].pack("C*")   # p NUL CR LF q
ps.subscribe("live:chan") do |on|
  on.subscribe do |ch, n|
    puts "subscribed   " + ch + " count=" + n.to_s
    receivers = publisher.publish("live:chan", "m1")
    puts "publish_m1   receivers=" + receivers.to_s
  end
  on.message do |ch, m|
    if m == "m1"
      puts "got_m1       " + ch
      publisher.publish("live:chan", bin)
    else
      ok = m.bytesize == 5
      ok = ok && m.getbyte(1) == 0
      ok = ok && m.getbyte(2) == 13
      puts "got_binary   " + ok.to_s
      ps.unsubscribe_all
    end
  end
  on.unsubscribe do |ch, n|
    puts "unsubscribed " + ch + " count=" + n.to_s
  end
end
puts "sub_leg_done count=" + ps.subscription_count.to_s

# --- psubscribe leg on the SAME connection: RESP2 returns a drained
# --- subscribe-mode connection to normal, so it must be reusable ----------

ps.psubscribe("live:p:*") do |on|
  on.psubscribe do |pat, n|
    puts "psubscribed  " + pat + " count=" + n.to_s
    publisher.publish("live:p:alpha", "pm1")
  end
  on.pmessage do |pat, ch, m|
    puts "got_pmessage " + pat + " " + ch + " " + m
    ps.punsubscribe_all
  end
  on.punsubscribe do |pat, n|
    puts "punsubscribd " + pat + " count=" + n.to_s
  end
end
puts "psub_leg_done count=" + ps.subscription_count.to_s

# publisher still works after all of it
puts "publisher_ok " + publisher.ping

ps.close
publisher.close
PubSubShell.sp_net_shell_capture("redis-cli -p 16392 shutdown nosave 2>/dev/null", 64)
puts "done"
