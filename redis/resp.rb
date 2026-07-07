# RESP2 protocol layer: encoding as pure functions, decoding as an
# incremental parser fed by whatever transport owns the socket.
#
# Byte-oriented throughout (getbyte / byteslice / bytesize): RESP bulk
# strings are binary-safe, and character-based String ops diverge from
# byte counts on non-ASCII payloads.

# One decoded RESP value. A tagged value: `kind` says which of the other
# fields is meaningful. Kept monomorphic — every field is present and
# typed — rather than a per-kind class hierarchy, so reply handling in
# the client is flat comparisons on `kind`.
class RespValue
  SIMPLE = 0   # +OK           -> str
  ERROR  = 1   # -ERR ...      -> str
  INT    = 2   # :42           -> int
  BULK   = 3   # $3 abc        -> str (binary-safe)
  ARRAY  = 4   # *N            -> items
  NILV   = 5   # $-1 / *-1

  def initialize(kind, str, int, items)
    @kind = kind
    @str = str
    @int = int
    @items = items
  end

  def kind
    @kind
  end

  def str
    @str
  end

  def int
    @int
  end

  def items
    @items
  end

  def self.simple(s)
    RespValue.new(SIMPLE, s, 0, [])
  end

  def self.error(s)
    RespValue.new(ERROR, s, 0, [])
  end

  def self.of_int(i)
    RespValue.new(INT, "", i, [])
  end

  def self.bulk(s)
    RespValue.new(BULK, s, 0, [])
  end

  def self.of_array(items)
    RespValue.new(ARRAY, "", 0, items)
  end

  def self.nil_value
    RespValue.new(NILV, "", 0, [])
  end
end

module Resp
  # A client command is always a flat array of bulk strings:
  # *<n>CRLF then $<bytelen>CRLF<bytes>CRLF per argument.
  def self.encode_command(argv)
    out = "*" + argv.length.to_s + "\r\n"
    argv.each do |a|
      out = out + "$" + a.bytesize.to_s + "\r\n" + a + "\r\n"
    end
    out
  end
end

# Incremental RESP2 reply parser. feed() appends raw socket bytes;
# try_next() returns true when a complete reply was decoded (read it via
# reply()) and false when the buffer holds only a partial reply — feed
# more and retry; the read position rewinds, so a reply is only ever
# consumed whole.
#
# The try/ivar shape (rather than returning value-or-nil) keeps every
# local monomorphic: a String|nil or RespValue|nil return would force
# poly dispatch on the hot path.
class RespParser
  def initialize
    @buf = ""
    @pos = 0
    @line = ""
    @val = RespValue.nil_value
  end

  def feed(data)
    @buf = @buf + data
  end

  def buffered_bytes
    @buf.bytesize - @pos
  end

  # The last reply decoded by a true-returning try_next.
  def reply
    @val
  end

  def try_next
    saved = @pos
    if !parse_value
      @pos = saved
      return false
    end
    # Drop consumed bytes once they accumulate; byteslice keeps it binary-safe.
    if @pos >= 4096
      @buf = @buf.byteslice(@pos, @buf.bytesize - @pos)
      @pos = 0
    end
    true
  end

  # A CRLF-terminated header line starting at @pos: on true, @line holds
  # it without the CRLF and @pos is past it. False = not fully arrived.
  def read_line
    i = @pos
    last = @buf.bytesize - 1
    while i < last
      if @buf.getbyte(i) == 13 && @buf.getbyte(i + 1) == 10
        @line = @buf.byteslice(@pos, i - @pos)
        @pos = i + 2
        return true
      end
      i = i + 1
    end
    false
  end

  # On true, @val holds the decoded value. False = incomplete input
  # (caller rewinds @pos; partial nested progress is discarded).
  def parse_value
    if !read_line
      return false
    end
    t = @line.getbyte(0)
    rest = @line.byteslice(1, @line.bytesize - 1)
    if t == 43                       # '+' simple string
      @val = RespValue.simple(rest)
      true
    elsif t == 45                    # '-' error
      @val = RespValue.error(rest)
      true
    elsif t == 58                    # ':' integer
      @val = RespValue.of_int(rest.to_i)
      true
    elsif t == 36                    # '$' bulk string
      n = rest.to_i
      if n < 0
        @val = RespValue.nil_value
        return true
      end
      if @buf.bytesize - @pos < n + 2
        return false                 # payload (+ CRLF) not all here yet
      end
      @val = RespValue.bulk(@buf.byteslice(@pos, n))
      @pos = @pos + n + 2
      true
    elsif t == 42                    # '*' array
      n = rest.to_i
      if n < 0
        @val = RespValue.nil_value
        return true
      end
      items = []
      k = 0
      while k < n
        if !parse_value
          return false               # nested incomplete: whole reply rewinds
        end
        items.push(@val)
        k = k + 1
      end
      @val = RespValue.of_array(items)
      true
    else
      @val = RespValue.error("protocol error: unexpected type byte " + t.to_s)
      true
    end
  end
end
