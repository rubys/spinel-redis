# Live proof of the event-loop embedding surface: subscribe_start +
# poll-until-readable + drain against a real server. The poll shape here
# is exactly Tep::Scheduler.io_wait's no-fiber fallback (poll_reset /
# poll_add / poll_run / poll_ready), so this is the tep integration
# contract minus the fiber. Spinel-only, committed snapshot.
require "redis"

module DrainShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
  ffi_func :sp_net_poll_reset,    [],           :int
  ffi_func :sp_net_poll_add,      [:int, :int], :int
  ffi_func :sp_net_poll_run,      [:int],       :int
  ffi_func :sp_net_poll_ready,    [:int],       :int
end

def drain_connect(port)
  attempts = 0
  while true
    begin
      c = Redis.new("127.0.0.1", port)   # assign-then-return: matz/spinel#1775
      return c
    rescue
      attempts = attempts + 1
      if attempts > 50
        raise "drain test: redis-server did not come up on " + port.to_s
      end
      DrainShell.sp_net_shell_capture("sleep 0.1", 16)
    end
  end
end

# Single-shot io_wait, the same sequence Tep::Scheduler.io_wait runs
# outside a fiber. Returns ready bits (1 = readable).
def wait_readable(fd, timeout_ms)
  DrainShell.sp_net_poll_reset
  slot = DrainShell.sp_net_poll_add(fd, 1)
  DrainShell.sp_net_poll_run(timeout_ms)
  DrainShell.sp_net_poll_ready(slot)
end

# Poll+drain until `listener` has dispatched `want` replies in total.
def drain_until(ps, listener, seen, want)
  rounds = 0
  while seen.length < want
    rounds = rounds + 1
    if rounds > 100
      raise "drain test: expected " + want.to_s + " events, saw " + seen.length.to_s
    end
    ready = wait_readable(ps.fd, 1000)
    if ready > 0
      ps.drain(listener)
    end
  end
  0
end

DrainShell.sp_net_shell_capture("redis-server --port 16407 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-drain.pid --logfile /tmp/spinel-redis-drain.log", 256)

publisher = drain_connect(16407)
ps = Redis.pubsub("127.0.0.1", 16407)

events = []
l = RedisListener.new
l.subscribe do |ch, n|
  events.push("sub:" + ch + ":" + n.to_s)
end
l.message do |ch, m|
  events.push("msg:" + ch + ":" + m)
end
l.unsubscribe do |ch, n|
  events.push("unsub:" + ch + ":" + n.to_s)
end

puts "fd_valid     " + (ps.fd >= 0).to_s

# not yet readable: nothing sent
puts "quiet_fd     " + (wait_readable(ps.fd, 50) == 0).to_s

ps.subscribe_start("drain:chan")
drain_until(ps, l, events, 1)          # subscribe confirmation
puts "after_start  " + events.join(",")

publisher.publish("drain:chan", "d1")
publisher.publish("drain:chan", "d2")
drain_until(ps, l, events, 3)          # both messages (however they chunk)
puts "after_pub    " + events.join(",")

ps.unsubscribe("drain:chan")
drain_until(ps, l, events, 4)
puts "after_unsub  " + events.join(",")
puts "count_zero   " + (ps.subscription_count == 0).to_s

ps.close
publisher.close
DrainShell.sp_net_shell_capture("redis-cli -p 16407 shutdown nosave 2>/dev/null", 64)
puts "done"
