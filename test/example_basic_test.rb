# Gated copy of examples/basic.rb against a test-owned server.
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

ExShell.sp_net_shell_capture("redis-server --port 16401 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-ex-basic.pid --logfile /tmp/spinel-redis-ex-basic.log", 256)
r = ex_connect(16401)
r.flushdb

# --- examples/basic.rb flow ---

r.del("foo")

puts

p 'set foo to "bar"'
r["foo"] = "bar"

puts

p "value of foo"
p r["foo"]

# --- end flow ---

r.close
ExShell.sp_net_shell_capture("redis-cli -p 16401 shutdown nosave 2>/dev/null", 64)
