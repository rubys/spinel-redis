# Client-core conformance against a scripted transport — dual-runtime (no
# snapshot): the command bytes written and the reply coercions must match
# CRuby exactly. Wire bytes come straight from the RESP2 spec; the reply
# values mirror redis-rb's contracts (nil for missing keys, bool for
# EXISTS/EXPIRE/SISMEMBER, Hash for HGETALL).
require "redis/resp"
require "redis/client"

# Serves canned reply bytes chunk-by-chunk and records every write.
class ScriptedTransport
  def initialize(chunks)
    @chunks = chunks
    @i = 0
    @written = ""
  end

  def write(data)
    @written = @written + data
    data.bytesize
  end

  def read_some(max)
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
