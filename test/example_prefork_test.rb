# Gated copy of examples/prefork.rb against a test-owned server.
require "redis"

module ExShell
  ffi_func :sp_net_shell_capture, [:str, :int], :binstr
end

module PreforkSock
  ffi_func :sp_net_fork,     [],     :int
  ffi_func :sp_net_exit,     [:int], :int
  ffi_func :sp_net_wait_any, [],     :int
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

ExShell.sp_net_shell_capture("redis-server --port 16406 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-ex-prefork.pid --logfile /tmp/spinel-redis-ex-prefork.log", 256)

# --- examples/prefork.rb flow ---

setup = ex_connect(16406)
setup.flushdb
setup.del("prefork:counter")
setup.del("prefork:done")
setup.close                     # nothing shared across the fork

i = 0
while i < 3
  pid = PreforkSock.sp_net_fork
  if pid == 0
    # worker: its OWN connection, opened after the fork
    w = Redis.new("127.0.0.1", 16406)
    w.incr("prefork:counter")
    w.rpush("prefork:done", "worker-" + i.to_s)
    w.close
    PreforkSock.sp_net_exit(0)
  end
  i = i + 1
end

reaped = 0
while reaped < 3
  PreforkSock.sp_net_wait_any
  reaped = reaped + 1
end

r = Redis.new("127.0.0.1", 16406)
p "counter after 3 workers"
p r.get("prefork:counter")
p "workers that reported in"
p r.lrange("prefork:done", 0, -1).sort

# --- end flow ---

r.close
ExShell.sp_net_shell_capture("redis-cli -p 16406 shutdown nosave 2>/dev/null", 64)
