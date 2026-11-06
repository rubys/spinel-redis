# Client-core conformance against a scripted transport — dual-runtime (no
# snapshot): the command bytes written and the reply coercions must match
# CRuby exactly. Wire bytes come straight from the RESP2 spec; the reply
# values mirror redis-rb's contracts (nil for missing keys, bool for
# EXISTS/EXPIRE/SISMEMBER, Hash for HGETALL).
require "redis/resp"
require "redis/client"
require "redis/url"

# Serves canned reply bytes chunk-by-chunk and records every write, and
# the read deadline each read ran under.
class ScriptedTransport
  def initialize(chunks)
    @chunks = chunks
    @i = 0
    @written = ""
    @read_timeout = -1.0
    @deadlines = ""
  end

  def read_timeout
    @read_timeout
  end

  def set_read_timeout(seconds)
    @read_timeout = seconds
    nil
  end

  def deadlines
    @deadlines
  end

  def write(data)
    @written = @written + data
    data.bytesize
  end

  def read_some(max)
    @deadlines = @deadlines + @read_timeout.to_s + " "
    if @i >= @chunks.length
      return ""
    end
    c = @chunks[@i]
    @i = @i + 1
    c
  end

  def close
    0
  end

  def written
    @written
  end
end

# GET hit: command encoding + bulk coercion
t = ScriptedTransport.new(["$3\r\nbar\r\n"])
c = RedisClientCore.new(t)
v = c.get("foo")
puts "get_wire     " + (t.written == "*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").to_s
puts "get_value    " + (v == "bar").to_s

# GET miss: $-1 -> nil
t = ScriptedTransport.new(["$-1\r\n"])
c = RedisClientCore.new(t)
v = c.get("absent")
puts "get_nil      " + v.nil?.to_s

# SET: status reply
t = ScriptedTransport.new(["+OK\r\n"])
c = RedisClientCore.new(t)
puts "set_ok       " + (c.set("k", "v") == "OK").to_s

# INCR: integer reply; INCRBY stringifies its argument
t = ScriptedTransport.new([":6\r\n", ":16\r\n"])
c = RedisClientCore.new(t)
a = c.incr("n")
b = c.incrby("n", 10)
w = t.written
puts "incr_values  " + (a == 6 && b == 16).to_s
puts "incrby_wire  " + w.include?("$6\r\nINCRBY\r\n$1\r\nn\r\n$2\r\n10\r\n").to_s

# SETEX wire shape
t = ScriptedTransport.new(["+OK\r\n"])
c = RedisClientCore.new(t)
ok = c.setex("pres", 60, "1") == "OK"
w = t.written
puts "setex        " + (ok && w.include?("$5\r\nSETEX\r\n$4\r\npres\r\n$2\r\n60\r\n$1\r\n1\r\n")).to_s

# EXISTS -> bool via :1/:0
t = ScriptedTransport.new([":1\r\n", ":0\r\n"])
c = RedisClientCore.new(t)
a = c.exists?("yes")
b = c.exists?("no")
puts "exists_bool  " + (a == true && b == false).to_s

# KEYS -> Array<String>
t = ScriptedTransport.new(["*2\r\n$1\r\na\r\n$1\r\nb\r\n"])
c = RedisClientCore.new(t)
ks = c.keys("*")
puts "keys_array   " + (ks.length == 2 && ks[0] == "a" && ks[1] == "b").to_s

# LRANGE with numeric args on the wire
t = ScriptedTransport.new(["*2\r\n$1\r\nx\r\n$1\r\ny\r\n"])
c = RedisClientCore.new(t)
l = c.lrange("list", 0, -1)
w = t.written
puts "lrange_wire  " + w.include?("$2\r\n-1\r\n").to_s
puts "lrange_vals  " + (l[0] == "x" && l[1] == "y").to_s

# HGETALL flat pairs -> Hash
t = ScriptedTransport.new(["*4\r\n$1\r\nf\r\n$1\r\n1\r\n$1\r\ng\r\n$1\r\n2\r\n"])
c = RedisClientCore.new(t)
h = c.hgetall("h")
puts "hgetall_hash " + (h.length == 2 && h["f"] == "1" && h["g"] == "2").to_s

# a reply dribbling in across many reads still coerces whole
t = ScriptedTransport.new(["$1", "1\r", "\nhello world", "\r\n"])
c = RedisClientCore.new(t)
v = c.get("k")
puts "chunked_read " + (v == "hello world").to_s

# binary-safe round trip: value with NUL and CRLF inside
bin = "a" + [0].pack("C*") + "\r\nb"
t = ScriptedTransport.new(["$" + bin.bytesize.to_s + "\r\n" + bin + "\r\n"])
c = RedisClientCore.new(t)
v = c.get("bin")
got = false
if !v.nil?
  got = v.bytesize == 5
  got = got && v.getbyte(1) == 0
end
puts "binary_value " + got.to_s

# -ERR reply raises
t = ScriptedTransport.new(["-ERR unknown command 'FROB'\r\n"])
c = RedisClientCore.new(t)
raised = false
begin
  c.ping
rescue => e
  raised = e.message.include?("unknown command")
end
puts "error_raises " + raised.to_s

# closed connection mid-reply raises
t = ScriptedTransport.new(["$10\r\npart"])
c = RedisClientCore.new(t)
raised = false
begin
  c.get("k")
rescue => e
  raised = e.message.include?("connection lost")
end
puts "closed_raise " + raised.to_s

# -- error classes: redis-rb names ------------------------------------------

# an error reply is a CommandError, a lost connection a ConnectionError,
# and neither is mistaken for the other
t = ScriptedTransport.new(["-ERR unknown command 'FROB'\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.ping
rescue Redis::BaseConnectionError
  kind = "connection"
rescue Redis::CommandError => e
  kind = "command " + e.message
end
puts "err_class    " + kind

t = ScriptedTransport.new(["-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.incr("s")
rescue Redis::WrongTypeError => e
  kind = "wrongtype " + e.is_a?(Redis::CommandError).to_s + " " + e.is_a?(Redis::BaseError).to_s
end
puts "err_wrongty  " + kind

t = ScriptedTransport.new(["-NOPERM this user has no permissions\r\n", "-OOM command not allowed\r\n"])
c = RedisClientCore.new(t)
kinds = ""
begin
  c.ping
rescue Redis::PermissionError
  kinds = kinds + "noperm "
end
begin
  c.ping
rescue Redis::OutOfMemoryError
  kinds = kinds + "oom"
end
puts "err_prefixes " + kinds

t = ScriptedTransport.new(["$10\r\npart"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.get("k")
rescue Redis::CommandError
  kind = "command"
rescue Redis::ConnectionError => e
  kind = "connection " + e.is_a?(Redis::BaseConnectionError).to_s + " " + e.message
end
puts "err_lost     " + kind

# -- AUTH -------------------------------------------------------------------

t = ScriptedTransport.new(["+OK\r\n", "+OK\r\n"])
c = RedisClientCore.new(t)
a = c.auth("s3cret")
b = c.auth("app", "pw")
w = t.written
puts "auth_wire    " + (w == "*2\r\n$4\r\nAUTH\r\n$6\r\ns3cret\r\n*3\r\n$4\r\nAUTH\r\n$3\r\napp\r\n$2\r\npw\r\n").to_s
puts "auth_reply   " + (a == "OK" && b == "OK").to_s

# -- redis:// URLs ----------------------------------------------------------

def url_line(url)
  u = RedisURL.parse(url)
  u.host + "|" + u.port.to_s + "|" + u.username + "|" + u.password + "|" + u.db.to_s
end

puts "url_bare     " + url_line("redis://localhost")
puts "url_full     " + url_line("redis://:s3cret@cache.internal:6380/2")
puts "url_user     " + url_line("redis://app:p%40ss+word@10.0.0.5/7")
puts "url_useronly " + url_line("redis://onlysecret@host")
puts "url_empty    " + url_line("redis://")
puts "url_query    " + url_line("redis://host:6390?db=4")
puts "url_ipv6     " + url_line("redis://[::1]:6381/1")

def url_error(url)
  begin
    RedisURL.parse(url)
    "parsed"
  rescue ArgumentError => e
    e.message
  end
end

puts "url_tls      " + url_error("rediss://:pw@host:6380").include?("rediss:// (TLS) is not supported").to_s
puts "url_unix     " + url_error("unix:///tmp/redis.sock").include?("unix sockets").to_s
puts "url_scheme   " + url_error("http://:pw@host")
puts "url_badport  " + url_error("redis://host:63x9")
puts "redact       " + RedisURL.redact("redis://app:hunter2@host:6379/0")
puts "redact_pw    " + RedisURL.redact("redis://:hunter2@host")
puts "redact_none  " + RedisURL.redact("redis://host:6379/0")

# -- sorted sets ----------------------------------------------------------------

# ZADD writes the score with to_s (Float and Integer) and reads :1/:0 as
# a Boolean, as redis-rb's single-pair form does
t = ScriptedTransport.new([":1\r\n", ":0\r\n"])
c = RedisClientCore.new(t)
a = c.zadd("schedule", 1791599858.8335917, "job-a")
b = c.zadd("schedule", 5, "job-b")
w = t.written
puts "zadd_wire    " + (w == "*4\r\n$4\r\nZADD\r\n$8\r\nschedule\r\n$18\r\n1791599858.8335917\r\n$5\r\njob-a\r\n*4\r\n$4\r\nZADD\r\n$8\r\nschedule\r\n$1\r\n5\r\n$5\r\njob-b\r\n").to_s
puts "zadd_bool    " + (a == true && b == false).to_s

# ZSCORE: bulk -> Float, $-1 -> nil, inf spelled as Redis does
t = ScriptedTransport.new(["$18\r\n1791599858.8335917\r\n", "$-1\r\n", "$3\r\ninf\r\n", "$4\r\n-inf\r\n"])
c = RedisClientCore.new(t)
s1 = c.zscore("schedule", "job-a")
s2 = c.zscore("schedule", "nope")
s3 = c.zscore("schedule", "far")
s4 = c.zscore("schedule", "past")
puts "zscore_float " + (s1 == 1791599858.8335917).to_s
puts "zscore_nil   " + s2.nil?.to_s
puts "zscore_inf   " + (s3 == Float::INFINITY && s4 == -Float::INFINITY).to_s

# ZCARD, ZREM, ZRANGE
t = ScriptedTransport.new([":2\r\n", ":1\r\n", "*1\r\n$5\r\njob-b\r\n"])
c = RedisClientCore.new(t)
n = c.zcard("schedule")
r = c.zrem("schedule", "job-a")
m = c.zrange("schedule", 0, -1)
puts "zcard_zrem   " + (n == 2 && r == true).to_s
puts "zrange       " + (m.length == 1 && m[0] == "job-b").to_s

# ZREMRANGEBYSCORE takes -inf/+inf and exclusive bounds as Strings,
# ZREMRANGEBYRANK negative ranks; both answer the count removed
t = ScriptedTransport.new([":3\r\n", ":1\r\n", ":2\r\n"])
c = RedisClientCore.new(t)
a = c.zremrangebyscore("dead", "-inf", 1791599558.5)
b = c.zremrangebyscore("dead", "(5", "+inf")
d = c.zremrangebyrank("dead", 0, -10001)
w = t.written
puts "zrembyscore  " + (w.include?("*4\r\n$16\r\nZREMRANGEBYSCORE\r\n$4\r\ndead\r\n$4\r\n-inf\r\n$12\r\n1791599558.5\r\n") && w.include?("$2\r\n(5\r\n$4\r\n+inf\r\n")).to_s
puts "zrembyrank   " + w.include?("*4\r\n$15\r\nZREMRANGEBYRANK\r\n$4\r\ndead\r\n$1\r\n0\r\n$6\r\n-10001\r\n").to_s
puts "zrem_counts  " + (a == 3 && b == 1 && d == 2).to_s

# -- lists: multi-value RPUSH and BRPOP ----------------------------------------

# RPUSH with one value, then with an Array: one command, values in order
t = ScriptedTransport.new([":1\r\n", ":4\r\n"])
c = RedisClientCore.new(t)
a = c.rpush("queue:default", "j1")
b = c.rpush("queue:default", ["j2", "j3", "j4"])
w = t.written
puts "rpush_wire   " + (w == "*3\r\n$5\r\nRPUSH\r\n$13\r\nqueue:default\r\n$2\r\nj1\r\n*5\r\n$5\r\nRPUSH\r\n$13\r\nqueue:default\r\n$2\r\nj2\r\n$2\r\nj3\r\n$2\r\nj4\r\n").to_s
puts "rpush_len    " + (a == 1 && b == 4).to_s

# BRPOP: keys in order, then the timeout; [key, value] back
t = ScriptedTransport.new(["*2\r\n$14\r\nqueue:critical\r\n$2\r\nj1\r\n"])
c = RedisClientCore.new(t)
v = c.brpop(["queue:critical", "queue:default"], timeout: 2)
puts "brpop_wire   " + (t.written == "*4\r\n$5\r\nBRPOP\r\n$14\r\nqueue:critical\r\n$13\r\nqueue:default\r\n$1\r\n2\r\n").to_s
puts "brpop_pair   " + (v.length == 2 && v[0] == "queue:critical" && v[1] == "j1").to_s

# the timeout passing is a nil multi-bulk: [] (redis-rb: nil)
t = ScriptedTransport.new(["*-1\r\n", "+PONG\r\n"])
c = RedisClientCore.new(t)
v = c.brpop(["queue:default"], timeout: 1)
puts "brpop_nil    " + (v.length == 0).to_s
puts "brpop_insync " + c.ping

# the read deadline: timeout + read_timeout while BRPOP waits, the
# configured one again after; 0 (block forever) waits without a deadline;
# no read_timeout stays none
t = ScriptedTransport.new(["*-1\r\n", "*-1\r\n", "+PONG\r\n"])
t.set_read_timeout(0.5)
c = RedisClientCore.new(t)
c.brpop(["q"], timeout: 2)
c.brpop(["q"], timeout: 0)
c.ping
puts "brpop_dl     " + t.deadlines
t = ScriptedTransport.new(["*-1\r\n"])
c = RedisClientCore.new(t)
c.brpop(["q"], timeout: 2)
puts "brpop_dl_off " + t.deadlines

# an error reply still puts the deadline back
t = ScriptedTransport.new(["-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"])
t.set_read_timeout(0.5)
c = RedisClientCore.new(t)
kind = "none"
begin
  c.brpop(["s"], timeout: 1)
rescue Redis::WrongTypeError
  kind = "wrongtype"
end
puts "brpop_error  " + kind + " " + t.read_timeout.to_s

# -- pipelined ------------------------------------------------------------------

# one write for every queued command, every reply read, replies coerced
# as the client coerces them (Integer, Boolean, String)
t = ScriptedTransport.new([":1\r\n:3\r\n", ":1\r\n+OK\r\n"])
c = RedisClientCore.new(t)
res = c.pipelined do |p|
  p.sadd("queues", "default")
  p.lpush("queue:default", "{}")
  p.zadd("schedule", 1.5, "j")
  p.set("k", "v")
end
w = t.written
puts "pipe_wire    " + (w == "*3\r\n$4\r\nSADD\r\n$6\r\nqueues\r\n$7\r\ndefault\r\n*3\r\n$5\r\nLPUSH\r\n$13\r\nqueue:default\r\n$2\r\n{}\r\n*4\r\n$4\r\nZADD\r\n$8\r\nschedule\r\n$3\r\n1.5\r\n$1\r\nj\r\n*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").to_s
puts "pipe_count   " + res.length.to_s
puts "pipe_values  " + (res[0] == 1 && res[1] == 3 && res[2] == true && res[3] == "OK").to_s

# an empty block sends nothing
t = ScriptedTransport.new([])
c = RedisClientCore.new(t)
res = c.pipelined do |p|
end
puts "pipe_empty   " + (res.length == 0 && t.written == "").to_s

# an error in the middle: the rest are still read, so the next command
# gets its own reply, and the first error is raised
t = ScriptedTransport.new([":1\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n:2\r\n", "+PONG\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.pipelined do |p|
    p.sadd("queues", "default")
    p.lpush("queues", "x")
    p.rpush("list", "y")
  end
rescue Redis::WrongTypeError => e
  kind = "wrongtype"
end
puts "pipe_error   " + kind
puts "pipe_insync  " + c.ping

# replies split across reads
t = ScriptedTransport.new([":", "1\r", "\n:", "2\r\n"])
c = RedisClientCore.new(t)
res = c.pipelined do |p|
  p.incrby("a", 1)
  p.incrby("b", 2)
end
puts "pipe_chunked " + (res[0] == 1 && res[1] == 2).to_s

# the connection lost mid-pipeline is a ConnectionError
t = ScriptedTransport.new([":1\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.pipelined do |p|
    p.sadd("queues", "default")
    p.lpush("queue:default", "{}")
  end
rescue Redis::ConnectionError
  kind = "connection"
end
puts "pipe_lost    " + kind

# the worker commands queue like the rest: multi-value RPUSH and both
# ZREMRANGEs, Integer replies
t = ScriptedTransport.new([":2\r\n:1\r\n:0\r\n"])
c = RedisClientCore.new(t)
res = c.pipelined do |p|
  p.rpush("queue:default", ["a", "b"])
  p.zremrangebyscore("dead", "-inf", 100)
  p.zremrangebyrank("dead", 0, -10001)
end
w = t.written
puts "pipe_worker  " + (w == "*4\r\n$5\r\nRPUSH\r\n$13\r\nqueue:default\r\n$1\r\na\r\n$1\r\nb\r\n*4\r\n$16\r\nZREMRANGEBYSCORE\r\n$4\r\ndead\r\n$4\r\n-inf\r\n$3\r\n100\r\n*4\r\n$15\r\nZREMRANGEBYRANK\r\n$4\r\ndead\r\n$1\r\n0\r\n$6\r\n-10001\r\n").to_s
puts "pipe_wvalues " + (res[0] == 2 && res[1] == 1 && res[2] == 0).to_s

# -- scripting: SCRIPT LOAD and EVALSHA ------------------------------------------

# SCRIPT LOAD answers the SHA1; EVALSHA sends numkeys, then the keys and
# the arguments, and answers the bulk reply or nil
t = ScriptedTransport.new(["$40\r\ne0e1f9fabfc9d4800c877a703b823ac0578ff8db\r\n", "$5\r\njob-a\r\n", "$-1\r\n"])
c = RedisClientCore.new(t)
sha = c.script(:load, "return 1")
a = c.evalsha(sha, ["retry"], ["1791599558.5"])
b = c.evalsha(sha, ["schedule"], ["1791599558.5"])
w = t.written
puts "script_load  " + (sha == "e0e1f9fabfc9d4800c877a703b823ac0578ff8db" && w.include?("*3\r\n$6\r\nSCRIPT\r\n$4\r\nLOAD\r\n$8\r\nreturn 1\r\n")).to_s
puts "evalsha_wire " + w.include?("*5\r\n$7\r\nEVALSHA\r\n$40\r\ne0e1f9fabfc9d4800c877a703b823ac0578ff8db\r\n$1\r\n1\r\n$5\r\nretry\r\n$12\r\n1791599558.5\r\n").to_s
puts "evalsha_vals " + (a == "job-a" && b.nil?).to_s

# NOSCRIPT is a NoScriptError, a CommandError, and the connection stays
# in step for the reload
t = ScriptedTransport.new(["-NOSCRIPT No matching script. Please use EVAL.\r\n", "$40\r\ne0e1f9fabfc9d4800c877a703b823ac0578ff8db\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.evalsha("e0e1f9fabfc9d4800c877a703b823ac0578ff8db", ["retry"], ["1"])
rescue Redis::NoScriptError => e
  kind = "noscript " + e.is_a?(Redis::CommandError).to_s + " " + e.message.include?("NOSCRIPT").to_s
end
puts "err_noscript " + kind
puts "after_nosc   " + (c.script(:load, "return 1").length == 40).to_s

# SCRIPT FLUSH answers OK; other subcommands aren't mirrored
t = ScriptedTransport.new(["+OK\r\n"])
c = RedisClientCore.new(t)
flushed = c.script(:flush)
puts "script_flush " + (flushed == "OK" && t.written == "*2\r\n$6\r\nSCRIPT\r\n$5\r\nFLUSH\r\n").to_s
other = "none"
begin
  c.script(:exists, "abc")
rescue ArgumentError => e
  other = e.message
end
puts "script_other " + other

# An Integer or Array script reply isn't mirrored: it raises
# Redis::BaseError (not a CommandError, since the server didn't refuse
# anything) rather than answer "", and the reply is read in full, so the
# next command still lines up
t = ScriptedTransport.new([":1\r\n", "*2\r\n$1\r\na\r\n$1\r\nb\r\n", "$5\r\njob-a\r\n"])
c = RedisClientCore.new(t)
kinds = []
2.times do
  k = "none"
  begin
    c.evalsha("e0e1f9fabfc9d4800c877a703b823ac0578ff8db", ["retry"], ["1"])
  rescue Redis::CommandError
    k = "command"
  rescue Redis::BaseError => e
    k = e.message.include?("Integer") ? "base_int" : (e.message.include?("Array") ? "base_array" : "base")
  end
  kinds.push(k)
end
puts "evalsha_kind " + kinds.join(",")
puts "after_kind   " + (c.evalsha("e0e1f9fabfc9d4800c877a703b823ac0578ff8db", ["retry"], ["1"]) == "job-a").to_s

# -- multi ----------------------------------------------------------------------

# Sidekiq's dead-set write: MULTI, the three commands and EXEC go out in
# one write; the replies are MULTI's +OK, a +QUEUED each, then EXEC's
# Array, coerced as the client coerces them
t = ScriptedTransport.new(["+OK\r\n+QUEUED\r\n+QUEUED\r\n", "+QUEUED\r\n*3\r\n:1\r\n:0\r\n:2\r\n"])
c = RedisClientCore.new(t)
res = c.multi do |tx|
  tx.zadd("dead", 1.5, "j")
  tx.zremrangebyscore("dead", "-inf", 0)
  tx.zremrangebyrank("dead", 0, -10000)
end
w = t.written
puts "multi_wire   " + (w == "*1\r\n$5\r\nMULTI\r\n*4\r\n$4\r\nZADD\r\n$4\r\ndead\r\n$3\r\n1.5\r\n$1\r\nj\r\n*4\r\n$16\r\nZREMRANGEBYSCORE\r\n$4\r\ndead\r\n$4\r\n-inf\r\n$1\r\n0\r\n*4\r\n$15\r\nZREMRANGEBYRANK\r\n$4\r\ndead\r\n$1\r\n0\r\n$6\r\n-10000\r\n*1\r\n$4\r\nEXEC\r\n").to_s
puts "multi_values " + (res.length == 3 && res[0] == true && res[1] == 0 && res[2] == 2).to_s

# the heartbeat's shape: variadic SADD, EXISTS, multi-field HSET, EXPIRE
# and an RPOP that answers nil
t = ScriptedTransport.new(["+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*5\r\n:1\r\n:0\r\n:2\r\n:1\r\n$-1\r\n"])
c = RedisClientCore.new(t)
res = c.multi do |tx|
  tx.sadd("processes", ["host:1:abc"])
  tx.exists("host:1:abc")
  tx.hset("host:1:abc", {"busy" => 0, "beat" => 1.5})
  tx.expire("host:1:abc", 60)
  tx.rpop("host:1:abc-signals")
end
puts "beat_wire    " + t.written.include?("*6\r\n$4\r\nHSET\r\n$10\r\nhost:1:abc\r\n$4\r\nbusy\r\n$1\r\n0\r\n$4\r\nbeat\r\n$3\r\n1.5\r\n").to_s
puts "beat_values  " + (res[0] == 1 && res[1] == 0 && res[2] == 2 && res[3] == true && res[4].nil?).to_s

# SET NX inside a transaction: true when set, false on a nil reply;
# UNLINK and variadic LPUSH answer Integers
t = ScriptedTransport.new(["+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*4\r\n:2\r\n+OK\r\n$-1\r\n:2\r\n"])
c = RedisClientCore.new(t)
res = c.multi do |tx|
  tx.unlink(["a", "b"])
  tx.set("lock", "1", nx: true, ex: 10)
  tx.set("lock", "2", nx: true)
  tx.lpush("l", ["x", "y"])
end
w = t.written
puts "tx_wire      " + (w.include?("*3\r\n$6\r\nUNLINK\r\n$1\r\na\r\n$1\r\nb\r\n") && w.include?("*6\r\n$3\r\nSET\r\n$4\r\nlock\r\n$1\r\n1\r\n$2\r\nEX\r\n$2\r\n10\r\n$2\r\nNX\r\n") && w.include?("*4\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\nx\r\n$1\r\ny\r\n")).to_s
puts "tx_values    " + (res[0] == 2 && res[1] == true && res[2] == false && res[3] == 2).to_s

# an empty block sends nothing
t = ScriptedTransport.new([])
c = RedisClientCore.new(t)
res = c.multi do |tx|
end
puts "multi_empty  " + (res.length == 0 && t.written == "").to_s

# a command failing while EXEC runs doesn't stop the rest; the first
# error is raised after EXEC's whole reply is read
t = ScriptedTransport.new(["+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n+OK\r\n", "+PONG\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.multi do |tx|
    tx.set("s", "x")
    tx.lpush("s", "y")
    tx.set("after", "1")
  end
rescue Redis::WrongTypeError
  kind = "wrongtype"
end
puts "exec_error   " + kind
puts "exec_insync  " + c.ping

# a command the server won't queue aborts the transaction: that refusal
# is raised (not EXECABORT), after EXEC's reply is read
t = ScriptedTransport.new(["+OK\r\n+QUEUED\r\n-ERR wrong number of arguments for 'hset' command\r\n-EXECABORT Transaction discarded because of previous errors.\r\n", "+PONG\r\n"])
c = RedisClientCore.new(t)
msg = "none"
begin
  c.multi do |tx|
    tx.set("q", "1")
    tx.hset("h", {})
  end
rescue Redis::CommandError => e
  msg = e.message
end
puts "queue_error  " + msg.start_with?("redis: ERR wrong number").to_s
puts "queue_insync " + c.ping

# the connection lost before EXEC answers is a ConnectionError
t = ScriptedTransport.new(["+OK\r\n+QUEUED\r\n"])
c = RedisClientCore.new(t)
kind = "none"
begin
  c.multi do |tx|
    tx.set("q", "1")
  end
rescue Redis::ConnectionError
  kind = "connection"
end
puts "multi_lost   " + kind

# -- Phase 5 commands outside a transaction --------------------------------------

# SET with ex: and nx:
t = ScriptedTransport.new(["+OK\r\n", "+OK\r\n", "$-1\r\n", "+OK\r\n"])
c = RedisClientCore.new(t)
a = c.set("k", "v", ex: 5)
puts "set_ex       " + (a == "OK" && t.written == "*5\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n$2\r\nEX\r\n$1\r\n5\r\n").to_s
b = c.set("n", "1", nx: true)
d = c.set("n", "1", nx: true)
puts "set_nx       " + (b == true && d == false).to_s
puts "set_plain    " + (c.set("k", "v") == "OK").to_s

# UNLINK one key or several; EXISTS as an Integer
t = ScriptedTransport.new([":1\r\n", ":2\r\n", ":0\r\n"])
c = RedisClientCore.new(t)
a = c.unlink("a")
b = c.unlink(["a", "b"])
puts "unlink       " + (a == 1 && b == 2 && t.written == "*2\r\n$6\r\nUNLINK\r\n$1\r\na\r\n*3\r\n$6\r\nUNLINK\r\n$1\r\na\r\n$1\r\nb\r\n").to_s
puts "exists_int   " + (c.exists("a") == 0).to_s

# multi-field HSET from a Hash; the three-argument form unchanged
t = ScriptedTransport.new([":2\r\n", ":0\r\n"])
c = RedisClientCore.new(t)
a = c.hset("h", {"a" => "1", "b" => 2})
b = c.hset("h", "a", "1")
puts "hset_hash    " + (a == 2 && b == 0 && t.written == "*6\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\na\r\n$1\r\n1\r\n$1\r\nb\r\n$1\r\n2\r\n*4\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\na\r\n$1\r\n1\r\n").to_s

# a field with no value goes out short for the server to refuse, as
# redis-rb sends it; it doesn't store an empty value
t = ScriptedTransport.new(["-ERR wrong number of arguments for 'hset' command\r\n"])
c = RedisClientCore.new(t)
msg = "none"
begin
  c.hset("h", "f")
rescue Redis::CommandError => e
  msg = e.message
end
puts "hset_short   " + (msg.start_with?("redis: ERR wrong number") && t.written == "*3\r\n$4\r\nHSET\r\n$1\r\nh\r\n$1\r\nf\r\n").to_s

# variadic LPUSH and SADD
t = ScriptedTransport.new([":2\r\n", ":3\r\n"])
c = RedisClientCore.new(t)
a = c.lpush("l", ["x", "y"])
b = c.sadd("s", ["a", "b", "c"])
puts "variadic     " + (a == 2 && b == 3 && t.written == "*4\r\n$5\r\nLPUSH\r\n$1\r\nl\r\n$1\r\nx\r\n$1\r\ny\r\n*5\r\n$4\r\nSADD\r\n$1\r\ns\r\n$1\r\na\r\n$1\r\nb\r\n$1\r\nc\r\n").to_s

# SSCAN walks the cursor until 0, yielding each member, with MATCH and
# COUNT passed through
t = ScriptedTransport.new(["*2\r\n$2\r\n17\r\n*2\r\n$1\r\na\r\n$1\r\nb\r\n", "*2\r\n$1\r\n0\r\n*1\r\n$1\r\nc\r\n"])
c = RedisClientCore.new(t)
got = []
c.sscan_each("s", match: "*", count: 2) do |m|
  got.push(m)
end
puts "sscan_each   " + (got.join(",") == "a,b,c" && t.written == "*7\r\n$5\r\nSSCAN\r\n$1\r\ns\r\n$1\r\n0\r\n$5\r\nMATCH\r\n$1\r\n*\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n*7\r\n$5\r\nSSCAN\r\n$1\r\ns\r\n$2\r\n17\r\n$5\r\nMATCH\r\n$1\r\n*\r\n$5\r\nCOUNT\r\n$1\r\n2\r\n").to_s
t = ScriptedTransport.new(["*2\r\n$1\r\n0\r\n*0\r\n"])
c = RedisClientCore.new(t)
n = 0
c.sscan_each("empty") do |m|
  n = n + 1
end
puts "sscan_empty  " + (n == 0 && t.written == "*3\r\n$5\r\nSSCAN\r\n$5\r\nempty\r\n$1\r\n0\r\n").to_s
