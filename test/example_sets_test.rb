# Gated copy of examples/sets.rb against a test-owned server. Set replies
# are sorted here: server-side set ordering is unspecified, and this copy
# diffs a committed snapshot.
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

ExShell.sp_net_shell_capture("redis-server --port 16404 --save '' --appendonly no --daemonize yes --pidfile /tmp/spinel-redis-ex-sets.pid --logfile /tmp/spinel-redis-ex-sets.log", 256)
r = ex_connect(16404)
r.flushdb

# --- examples/sets.rb flow (set replies sorted) ---

r.del("foo-tags")
r.del("bar-tags")

puts
p "create a set of tags on foo-tags"

r.sadd("foo-tags", "one")
r.sadd("foo-tags", "two")
r.sadd("foo-tags", "three")

puts
p "create a set of tags on bar-tags"

r.sadd("bar-tags", "three")
r.sadd("bar-tags", "four")
r.sadd("bar-tags", "five")

puts
p "foo-tags"

p r.smembers("foo-tags").sort

puts
p "bar-tags"

p r.smembers("bar-tags").sort

puts
p "intersection of foo-tags and bar-tags"

p r.sinter("foo-tags", "bar-tags").sort

# --- end flow ---

r.close
ExShell.sp_net_shell_capture("redis-cli -p 16404 shutdown nosave 2>/dev/null", 64)
