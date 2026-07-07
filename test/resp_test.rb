# RESP2 encode/decode conformance. Dual-runtime by design: no snapshot is
# committed, so `spin test` diffs the compiled run against CRuby directly.
require "redis/resp"

# --- encoding ---------------------------------------------------------------

puts "enc_set      " + (Resp.encode_command(["SET", "k", "v"]) == "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n").to_s
puts "enc_empty    " + (Resp.encode_command(["GET", ""]) == "*2\r\n$3\r\nGET\r\n$0\r\n\r\n").to_s
bin = [97, 0, 98].pack("C*")   # "a\0b" — bulk strings are binary-safe
puts "enc_binary   " + (Resp.encode_command(["SET", "b", bin]) == "*3\r\n$3\r\nSET\r\n$1\r\nb\r\n$3\r\n" + bin + "\r\n").to_s
puts "enc_utf8_len " + (Resp.encode_command(["ECHO", "héllo"]).include?("$6\r\n")).to_s

# --- decoding: one complete reply per kind ----------------------------------

def parse_one(bytes)
  p = RespParser.new
  p.feed(bytes)
  if !p.try_next
    return RespValue.error("test: incomplete")
  end
  p.reply
end

v = parse_one("+OK\r\n")
puts "dec_simple   " + (v.kind == RespValue::SIMPLE && v.str == "OK").to_s

v = parse_one("-ERR unknown command\r\n")
puts "dec_error    " + (v.kind == RespValue::ERROR && v.str == "ERR unknown command").to_s

v = parse_one(":1234\r\n")
puts "dec_int      " + (v.kind == RespValue::INT && v.int == 1234).to_s

v = parse_one(":-7\r\n")
puts "dec_int_neg  " + (v.kind == RespValue::INT && v.int == -7).to_s

v = parse_one("$5\r\nhello\r\n")
puts "dec_bulk     " + (v.kind == RespValue::BULK && v.str == "hello").to_s

v = parse_one("$-1\r\n")
puts "dec_bulk_nil " + (v.kind == RespValue::NILV).to_s

v = parse_one("$0\r\n\r\n")
puts "dec_bulk_mt  " + (v.kind == RespValue::BULK && v.str == "").to_s

# bulk payload containing CRLF and a NUL: the length-prefixed read must not
# stop at either ("a\r\nb\0c" = 6 bytes, NUL at index 4)
raw = "$6\r\n" + "a\r\nb" + [0].pack("C*") + "c" + "\r\n"
v = parse_one(raw)
puts "dec_bulk_bin " + (v.kind == RespValue::BULK && v.str.bytesize == 6 && v.str.getbyte(4) == 0).to_s

v = parse_one("*3\r\n$3\r\nfoo\r\n:9\r\n$-1\r\n")
ok = v.kind == RespValue::ARRAY && v.items.length == 3
ok = ok && v.items[0].str == "foo" && v.items[1].int == 9 && v.items[2].kind == RespValue::NILV
puts "dec_array    " + ok.to_s

v = parse_one("*-1\r\n")
puts "dec_arr_nil  " + (v.kind == RespValue::NILV).to_s

v = parse_one("*0\r\n")
puts "dec_arr_mt   " + (v.kind == RespValue::ARRAY && v.items.length == 0).to_s

# nested arrays (e.g. EXEC / pubsub message shapes)
v = parse_one("*2\r\n*2\r\n:1\r\n:2\r\n$2\r\nok\r\n")
ok = v.kind == RespValue::ARRAY && v.items[0].kind == RespValue::ARRAY
ok = ok && v.items[0].items[1].int == 2 && v.items[1].str == "ok"
puts "dec_nested   " + ok.to_s

# --- decoding: incremental feeding ------------------------------------------

# byte-at-a-time: every prefix must come up incomplete, the final byte
# completes the reply
full = "*2\r\n$3\r\nabc\r\n:42\r\n"
p = RespParser.new
premature = 0
done = false
final_ok = false
i = 0
while i < full.bytesize
  p.feed(full.byteslice(i, 1))
  done = p.try_next
  if i < full.bytesize - 1
    if done
      premature = premature + 1
    end
  else
    if done
      got = p.reply
      final_ok = got.kind == RespValue::ARRAY && got.items[0].str == "abc" && got.items[1].int == 42
    end
  end
  i = i + 1
end
puts "dec_dribble  " + (premature == 0 && final_ok).to_s

# rewind correctness: an incomplete parse must not consume; the retry after
# more bytes arrive sees the whole reply.
# (Sequential statements, not `x && p.try_next && p.reply...` chains:
# matz/spinel#1773 evaluates a later &&-operand's receiver early.)
p = RespParser.new
p.feed("$5\r\nhel")
was_incomplete = !p.try_next
p.feed("lo\r\n+NEXT\r\n")
got1 = p.try_next
s1 = p.reply.str
got2 = p.try_next
s2 = p.reply.str
puts "dec_rewind   " + (was_incomplete && got1 && s1 == "hello" && got2 && s2 == "NEXT").to_s

# two pipelined replies in one buffer come out one at a time
p = RespParser.new
p.feed(":1\r\n:2\r\n")
got1 = p.try_next
i1 = p.reply.int
got2 = p.try_next
i2 = p.reply.int
got3 = p.try_next
puts "dec_pipeline " + (got1 && i1 == 1 && got2 && i2 == 2 && !got3).to_s

# buffer compaction across the 4096 threshold keeps replies intact
p = RespParser.new
big = "x" * 3000
p.feed("$3000\r\n" + big + "\r\n$4\r\ntail\r\n")
got1 = p.try_next
n1 = p.reply.str.bytesize
got2 = p.try_next
s2 = p.reply.str
puts "dec_compact  " + (got1 && n1 == 3000 && got2 && s2 == "tail").to_s
