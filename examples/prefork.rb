# Ported from redis-rb v5.4.1 examples/unicorn/ — the lesson translates
# verbatim to sp_net prefork discipline: a Redis connection opened
# BEFORE forking is a socket fd shared by every worker, and concurrent
# replies interleave into protocol garbage. So: close before fork, and
# each worker opens its own connection after the fork (upstream's
# `after_fork { redis.disconnect! }`).
#
# Workers stay silent (their stdout would interleave); results flow back
# through Redis itself and the parent prints them, sorted.
#
# Gated copy: test/example_prefork_test.rb
require "redis"

module PreforkSock
  ffi_func :sp_net_fork,     [],     :int
  ffi_func :sp_net_exit,     [:int], :int
  ffi_func :sp_net_wait_any, [],     :int
end

WORKERS = 3

setup = Redis.new
setup.del("prefork:counter")
setup.del("prefork:done")
setup.close                     # nothing shared across the fork

i = 0
while i < WORKERS
  pid = PreforkSock.sp_net_fork
  if pid == 0
    # worker: its OWN connection, opened after the fork
    w = Redis.new
    w.incr("prefork:counter")
    w.rpush("prefork:done", "worker-" + i.to_s)
    w.close
    PreforkSock.sp_net_exit(0)
  end
  i = i + 1
end

reaped = 0
while reaped < WORKERS
  PreforkSock.sp_net_wait_any
  reaped = reaped + 1
end

r = Redis.new
p "counter after " + WORKERS.to_s + " workers"
p r.get("prefork:counter")
p "workers that reported in"
p r.lrange("prefork:done", 0, -1).sort
r.del("prefork:counter")
r.del("prefork:done")
