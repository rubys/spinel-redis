# Gated copy of examples/list.rb against a test-owned server.
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

ExShell.sp_net_shell_capture("redis-server --port 16403 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-ex-list.pid --logfile /tmp/spinel-redis-ex-list.log", 256)
r = ex_connect(16403)
r.flushdb

# --- examples/list.rb flow ---

r.del("logs")

puts

p "pushing log messages into a LIST"
r.rpush("logs", "some log message")
r.rpush("logs", "another log message")
r.rpush("logs", "yet another log message")
r.rpush("logs", "also another log message")

puts
p "contents of logs LIST"

p r.lrange("logs", 0, -1)

puts
p "Trim logs LIST to last 2 elements(easy circular buffer)"

r.ltrim("logs", -2, -1)

p r.lrange("logs", 0, -1)

# --- end flow ---

r.close
ExShell.sp_net_shell_capture("redis-cli -p 16403 shutdown nosave 2>/dev/null", 64)
