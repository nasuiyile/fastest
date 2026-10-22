"""Key-command semantics, byte-for-byte against memcached 1.6.x; uses only the Python stdlib.

Covers every SingleKeyCommand / MultiKeyCommand: set add replace append prepend cas
get gets gat gats delete touch incr decr. Each case is a scripted session of
(request, expected reply) steps; the same script runs against fastest and, with
--reference, against a real memcached to prove the expectations themselves.

python3 tests/python/test_command_semantics.py --server build/fastest
python3 tests/python/test_command_semantics.py --server build/fastest --reference /usr/bin/memcached

Normalization (the only allowed differences):
- cas unique values are replaced by <cas> (each server numbers versions itself);
- runs of VALUE blocks in one reply are sorted: fastest answers a multi-key
  request shard by shard (doc/design_detail.md), memcached in request order.
"""
import argparse
import contextlib
import os
import re
import socket
import subprocess
import sys
import time
from pathlib import Path

SENTINEL_KEY = b"__semantics_sentinel__"
SENTINEL_REPLY = b"VALUE " + SENTINEL_KEY + b" 0 4\r\nDONE\r\nEND\r\n"
VALUE_LINE = re.compile(rb"VALUE (\S+) (\d+) (\d+)(?: (\d+|<cas>))?\r\n")
PLACEHOLDER = re.compile(rb"\{(cas:)?(\w+)\}")
LONG_KEY = "k" * 251


def lines(*parts):
    """Wire text: each part is one CRLF-terminated line; bytes parts are raw."""
    out = bytearray()
    for part in parts:
        out += part if isinstance(part, bytes) else part.encode() + b"\r\n"
    return bytes(out)


Q = lines  # request
R = lines  # reply
NOTHING = b""


def case(name, *steps):
    return name, steps


BIG = bytes(range(32, 127)) * 106  # 10070 bytes: larger than the parser's direct-read threshold
HALF = b"0123456789" * 600

CASES = [
    # ---------------- set ----------------
    case("set stores flags and data",
         (Q("set {k} 123 0 3", "abc", "get {k}"), R("STORED", "VALUE {k} 123 3", "abc", "END"))),
    case("set noreply",
         (Q("set {k} 0 0 1 noreply", "x", "get {k}"), R("VALUE {k} 0 1", "x", "END"))),
    case("set ignores an unknown last option",
         (Q("set {k} 0 0 1 garbage", "x"), R("STORED"))),
    case("set with noreply in the length slot is a silent format error",
         (Q("set {k} 0 0 noreply", "get {k}"), R("END"))),
    case("set with a negative exptime expires immediately",
         (Q("set {k} 0 -1 1", "x", "get {k}"), R("STORED", "END"))),
    case("set exptime is a truncated int32",
         (Q("set {a} 0 4294967295 1", "x", "get {a}"), R("STORED", "END")),
         (Q("set {b} 0 2147483648 1", "x", "get {b}"), R("STORED", "END")),
         (Q("set {c} 0 -2147483649 1", "x", "get {c}"), R("STORED", "VALUE {c} 0 1", "x", "END")),
         (Q("set {d} 0 -9223372036854775808 1", "x", "get {d}"), R("STORED", "VALUE {d} 0 1", "x", "END")),
         (Q("set {e} 0 +5 1", "x", "get {e}"), R("STORED", "VALUE {e} 0 1", "x", "END")),
         (Q("set {f} 0 9223372036854775808 1", "x"), R("CLIENT_ERROR bad command line format", "ERROR"))),
    case("set with an absolute exptime in the past expires immediately",
         (Q("set {k} 0 1000000000 1", "x", "get {k}"), R("STORED", "END"))),
    case("large set and append",
         (b"set {k} 0 0 10070\r\n" + BIG + b"\r\n", R("STORED")),
         (b"append {k} 0 0 6000\r\n" + HALF + b"\r\nget {k}\r\n",
          R("STORED", "VALUE {k} 0 16070") + BIG + HALF + b"\r\nEND\r\n")),

    # ---------------- add ----------------
    case("add stores only a missing key",
         (Q("add {k} 1 0 1", "a", "add {k} 2 0 1", "b", "get {k}"), R("STORED", "NOT_STORED", "VALUE {k} 1 1", "a", "END")),
         (Q("add {k} 2 0 1 noreply", "b", "get {k}"), R("VALUE {k} 1 1", "a", "END"))),
    case("add treats an expired key as missing",
         (Q("set {k} 0 -1 1", "a", "add {k} 7 0 1", "b", "get {k}"), R("STORED", "STORED", "VALUE {k} 7 1", "b", "END"))),
    case("add does not touch the existing item's cas",
         (Q("set {k} 0 0 1", "a", "gets {k}"), R("STORED", "VALUE {k} 0 1 <cas>", "a", "END")),
         (Q("add {k} 0 0 1", "b", "cas {k} 0 0 1 {cas:k}", "c", "get {k}"), R("NOT_STORED", "STORED", "VALUE {k} 0 1", "c", "END"))),

    # ---------------- replace ----------------
    case("replace needs an existing key and takes the new flags",
         (Q("replace {k} 0 0 1", "a", "get {k}"), R("NOT_STORED", "END")),
         (Q("set {k} 1 0 1", "a", "replace {k} 9 0 1", "b", "get {k}"), R("STORED", "STORED", "VALUE {k} 9 1", "b", "END")),
         (Q("replace {k} 3 0 1 noreply", "c", "get {k}"), R("VALUE {k} 3 1", "c", "END"))),
    case("replace treats an expired key as missing",
         (Q("set {k} 0 -1 1", "a", "replace {k} 0 0 1", "b", "get {k}"), R("STORED", "NOT_STORED", "END"))),
    case("replace takes the new exptime",
         (Q("set {k} 0 0 1", "a", "replace {k} 0 -1 1", "b", "get {k}"), R("STORED", "STORED", "END"))),

    # ---------------- append / prepend ----------------
    case("append and prepend need an existing key",
         (Q("append {k} 0 0 1", "x", "prepend {k} 0 0 1", "x", "get {k}"), R("NOT_STORED", "NOT_STORED", "END"))),
    case("append and prepend keep the old flags and exptime",
         (Q("set {k} 5 0 2", "mm", "append {k} 9 -1 2", ">>", "prepend {k} 7 -1 2", "<<", "get {k}"),
          R("STORED", "STORED", "STORED", "VALUE {k} 5 6", "<<mm>>", "END"))),
    case("append and prepend noreply",
         (Q("set {k} 0 0 1", "m", "append {k} 0 0 1 noreply", "a", "prepend {k} 0 0 1 noreply", "p", "get {k}"),
          R("STORED", "VALUE {k} 0 3", "pma", "END"))),
    case("append of an empty block",
         (Q("set {k} 0 0 1", "m", "append {k} 0 0 0", "", "prepend {k} 0 0 0", "", "get {k}"),
          R("STORED", "STORED", "STORED", "VALUE {k} 0 1", "m", "END"))),
    case("append treats an expired key as missing",
         (Q("set {k} 0 -1 1", "m", "append {k} 0 0 1", "a", "prepend {k} 0 0 1", "p", "get {k}"),
          R("STORED", "NOT_STORED", "NOT_STORED", "END"))),
    case("append and prepend change the cas",
         (Q("set {k} 0 0 1", "m", "gets {k}"), R("STORED", "VALUE {k} 0 1 <cas>", "m", "END")),
         (Q("append {k} 0 0 1", "a", "cas {k} 0 0 1 {cas:k}", "x"), R("STORED", "EXISTS")),
         (Q("gets {k}"), R("VALUE {k} 0 2 <cas>", "ma", "END")),
         (Q("prepend {k} 0 0 1", "p", "cas {k} 0 0 1 {cas:k}", "x"), R("STORED", "EXISTS"))),

    # ---------------- cas ----------------
    case("cas on a missing key",
         (Q("cas {k} 0 0 1 1", "x", "get {k}"), R("NOT_FOUND", "END"))),
    case("cas matches the gets version",
         (Q("set {k} 0 0 1", "a", "gets {k}"), R("STORED", "VALUE {k} 0 1 <cas>", "a", "END")),
         (Q("cas {k} 4 0 1 {cas:k}", "b", "cas {k} 0 0 1 {cas:k}", "c", "get {k}"),
          R("STORED", "EXISTS", "VALUE {k} 4 1", "b", "END"))),
    case("cas unique parsing",
         (Q("set {k} 0 0 1", "a", "cas {k} 0 0 1 0", "b", "cas {k} 0 0 1 +1", "b"), R("STORED", "EXISTS", "EXISTS")),
         (Q("cas {k} 0 0 1 -1", "b"), R("CLIENT_ERROR bad command line format", "ERROR")),
         (Q("cas {k} 0 0 1 1 noreply", "b", "cas {k} 0 0 1 1 garbage", "b"), R("EXISTS")),
         (Q("cas {k} 0 0 1 noreply", "get {k}"), R("VALUE {k} 0 1", "a", "END"))),
    case("cas treats an expired key as missing",
         (Q("set {k} 0 -1 1", "a", "cas {k} 0 0 1 1", "b"), R("STORED", "NOT_FOUND"))),

    # ---------------- get / gets ----------------
    case("get and gets, single and multi",
         (Q("get {a}", "gets {a}"), R("END", "END")),
         (Q("set {a} 1 0 1", "A", "set {b} 2 0 2", "BB", "get {a}"), R("STORED", "STORED", "VALUE {a} 1 1", "A", "END")),
         (Q("get {a} {missing} {b} {a}"), R("VALUE {a} 1 1", "A", "VALUE {b} 2 2", "BB", "VALUE {a} 1 1", "A", "END")),
         (Q("gets {b} {a}"), R("VALUE {b} 2 2 <cas>", "BB", "VALUE {a} 1 1 <cas>", "A", "END"))),
    case("get skips expired keys",
         (Q("set {a} 0 -1 1", "A", "set {b} 0 0 1", "B", "get {a} {b}", "gets {a}"),
          R("STORED", "STORED", "VALUE {b} 0 1", "B", "END", "END"))),

    # ---------------- gat / gats ----------------
    case("gat and gats return hits",
         (Q("set {a} 3 0 1", "A", "gat 100 {a}", "gat 100 {missing}"), R("STORED", "VALUE {a} 3 1", "A", "END", "END")),
         (Q("set {b} 0 0 1", "B", "gats 100 {a} {missing} {b} {a}"),
          R("STORED", "VALUE {a} 3 1 <cas>", "A", "VALUE {b} 0 1 <cas>", "B", "VALUE {a} 3 1 <cas>", "A", "END"))),
    case("gat does not change the cas",
         (Q("set {k} 0 0 1", "a", "gets {k}"), R("STORED", "VALUE {k} 0 1 <cas>", "a", "END")),
         (Q("gat 100 {k}", "touch {k} 100", "cas {k} 0 0 1 {cas:k}", "b"), R("VALUE {k} 0 1", "a", "END", "TOUCHED", "STORED"))),
    case("gat with a negative exptime returns the item once",
         (Q("set {k} 0 0 1", "a", "gat -1 {k}", "get {k}"), R("STORED", "VALUE {k} 0 1", "a", "END", "END")),
         (Q("set {k} 0 0 1", "a", "gat -1 {k} {k}"), R("STORED", "VALUE {k} 0 1", "a", "END")),
         (Q("set {a} 0 0 1", "a", "set {b} 0 0 1", "b", "gats -1 {a} {b}", "get {a} {b}"),
          R("STORED", "STORED", "VALUE {a} 0 1 <cas>", "a", "VALUE {b} 0 1 <cas>", "b", "END", "END"))),
    case("gat argument errors",
         (Q("gat", "gats", "gat 100", "gats 100"), R("ERROR", "ERROR", "END", "END")),
         (Q("gat abc {k}", "gat abc", "gats 0x10 {k}", "gat 1.5 {k}", "gat +-1 {k}"),
          R(*["CLIENT_ERROR invalid exptime argument"] * 5)),
         (Q("gat 99999999999999999999 {k}", "gat noreply {k}"), R(*["CLIENT_ERROR invalid exptime argument"] * 2)),
         (Q("gat 0 " + LONG_KEY), R("CLIENT_ERROR bad command line format"))),
    case("gat on an expired key misses",
         (Q("set {k} 0 -1 1", "a", "gat 100 {k}", "touch {k} 100"), R("STORED", "END", "NOT_FOUND"))),

    # ---------------- touch ----------------
    case("touch",
         (Q("touch {k} 10", "set {k} 0 0 1", "a", "touch {k} 10", "touch {k} +5"), R("NOT_FOUND", "STORED", "TOUCHED", "TOUCHED")),
         (Q("touch {k} 10 noreply", "touch {k} 10 garbage", "touch {k} -1", "get {k}", "touch {k} 10"),
          R("TOUCHED", "TOUCHED", "END", "NOT_FOUND"))),
    case("touch argument errors",
         (Q("touch", "touch {k}", "touch {k} 10 noreply extra"), R("ERROR", "ERROR", "ERROR")),
         (Q("touch {k} abc", "touch {k} abc noreply", "touch {k} noreply", "touch {k} 1.5"),
          R("CLIENT_ERROR invalid exptime argument", "CLIENT_ERROR invalid exptime argument")),
         (Q("touch " + LONG_KEY + " 10", "touch " + LONG_KEY + " 10 noreply"), R("CLIENT_ERROR bad command line format"))),

    # ---------------- incr / decr ----------------
    case("incr and decr",
         (Q("incr {k} 1", "decr {k} 1", "set {k} 5 0 1", "1", "incr {k} 41", "decr {k} 2", "get {k}"),
          R("NOT_FOUND", "NOT_FOUND", "STORED", "42", "40", "VALUE {k} 5 2", "40", "END")),
         (Q("decr {k} 100", "incr {k} 0", "incr {k} 18446744073709551615", "incr {k} 18446744073709551615"),
          R("0", "0", "18446744073709551615", "18446744073709551614"))),
    case("incr and decr noreply",
         (Q("set {k} 0 0 1", "5", "incr {k} 1 noreply", "decr {k} 2 noreply", "get {k}"), R("STORED", "VALUE {k} 0 1", "4", "END")),
         (Q("incr {missing} 1 noreply", "incr {k} 1 garbage"), R("5"))),
    case("decrementing to a shorter number keeps the length padded with spaces",
         (Q("set {k} 0 0 3", "100", "decr {k} 1", "get {k}"), R("STORED", "99", "VALUE {k} 0 3", "99 ", "END")),
         (Q("incr {k} 1", "get {k}", "incr {k} 1", "get {k}"), R("100", "VALUE {k} 0 3", "100", "END", "101", "VALUE {k} 0 3", "101", "END")),
         (Q("set {k} 0 0 4", "1000", "decr {k} 999", "get {k}", "incr {k} 8", "get {k}"),
          R("STORED", "1", "VALUE {k} 0 4", "1   ", "END", "9", "VALUE {k} 0 4", "9   ", "END"))),
    case("padding does not depend on a pending reply that still references the old value",
         (b"set {k} 3 0 5001\r\n1" + b" " * 5000 + b"\r\nget {k}\r\ndecr {k} 1\r\nget {k}\r\n",
          R("STORED", "VALUE {k} 3 5001") + b"1" + b" " * 5000 + R("", "END", "0", "VALUE {k} 3 5001") + b"0" + b" " * 5000
          + R("", "END"))),
    case("incrementing to a longer number grows the value",
         (Q("set {k} 0 0 1", "9", "incr {k} 1", "get {k}", "incr {k} 18446744073709551000", "get {k}"),
          R("STORED", "10", "VALUE {k} 0 2", "10", "END", "18446744073709551010", "VALUE {k} 0 20", "18446744073709551010", "END"))),
    case("counter values parse like strtoull",
         (Q("set {a} 0 0 2", " 5", "incr {a} 1", "get {a}"), R("STORED", "6", "VALUE {a} 0 2", "6 ", "END")),
         (Q("set {b} 0 0 5", "5 abc", "incr {b} 1", "get {b}"), R("STORED", "6", "VALUE {b} 0 5", "6    ", "END")),
         (Q("set {c} 0 0 2", "+5", "incr {c} 1", "set {d} 0 0 2", "-0", "incr {d} 1"), R("STORED", "6", "STORED", "1")),
         (Q("set {e} 0 0 3", b"5\t\t\r\n", "incr {e} 1", "set {f} 0 0 3", "007", "incr {f} 1", "get {f}"),
          R("STORED", "6", "STORED", "8", "VALUE {f} 0 3", "8  ", "END")),
         (Q("set {g} 0 0 20", "18446744073709551615", "incr {g} 1", "get {g}"),
          R("STORED", "0", "VALUE {g} 0 20", "0                   ", "END")),
         (Q("set {h} 0 0 2", b"7\x00\r\n", "incr {h} 1"), R("STORED", "8"))),
    case("non-numeric counter values",
         (Q("set {k} 0 0 0", "", "incr {k} 1"), R("STORED", "CLIENT_ERROR cannot increment or decrement non-numeric value")),
         (Q("set {k} 0 0 3", "abc", "decr {k} 1", "set {k} 0 0 2", "5x", "incr {k} 1"),
          R("STORED", "CLIENT_ERROR cannot increment or decrement non-numeric value",
            "STORED", "CLIENT_ERROR cannot increment or decrement non-numeric value")),
         (Q("set {k} 0 0 2", "-5", "incr {k} 1", "set {k} 0 0 2", "0x", "decr {k} 0", "set {k} 0 0 1", " ", "incr {k} 1"),
          R("STORED", "CLIENT_ERROR cannot increment or decrement non-numeric value",
            "STORED", "CLIENT_ERROR cannot increment or decrement non-numeric value",
            "STORED", "CLIENT_ERROR cannot increment or decrement non-numeric value")),
         (Q("set {k} 0 0 20", "18446744073709551616", "incr {k} 1", "incr {k} 1 noreply", "get {k}"),
          R("STORED", "CLIENT_ERROR cannot increment or decrement non-numeric value", "VALUE {k} 0 20", "18446744073709551616", "END"))),
    case("incr keeps flags and changes the cas",
         (Q("set {k} 7 0 1", "9", "gets {k}"), R("STORED", "VALUE {k} 7 1 <cas>", "9", "END")),
         (Q("incr {k} 1", "cas {k} 0 0 1 {cas:k}", "x", "gets {k}"), R("10", "EXISTS", "VALUE {k} 7 2 <cas>", "10", "END")),
         (Q("incr {k} 90", "cas {k} 0 0 1 {cas:k}", "x", "gets {k}"), R("100", "EXISTS", "VALUE {k} 7 3 <cas>", "100", "END"))),
    case("incr treats an expired key as missing",
         (Q("set {k} 0 -1 1", "1", "incr {k} 1", "decr {k} 1"), R("STORED", "NOT_FOUND", "NOT_FOUND"))),
    case("incr argument errors",
         (Q("incr", "incr {k}", "decr {k} 1 noreply extra"), R("ERROR", "ERROR", "ERROR")),
         (Q("incr {k} abc", "incr {k} -1", "decr {k} 18446744073709551616", "incr {k} 1.5", "incr {k} 0x1"),
          R(*["CLIENT_ERROR invalid numeric delta argument"] * 5)),
         (Q("incr {k} abc noreply", "incr {k} noreply", "set {k} 0 0 1", "5", "incr {k} +2", "incr {k} -0"),
          R("STORED", "7", "7")),
         (Q("incr " + LONG_KEY + " 1", "decr " + LONG_KEY + " 1 noreply"), R("CLIENT_ERROR bad command line format"))),

    # ---------------- delete ----------------
    case("delete",
         (Q("delete {k}", "set {k} 0 0 1", "a", "delete {k}", "get {k}"), R("NOT_FOUND", "STORED", "DELETED", "END")),
         (Q("set {k} 0 0 1", "a", "delete {k} noreply", "delete {k} 0", "set {k} 0 0 1", "a", "delete {k} 0 noreply", "get {k}"),
          R("STORED", "NOT_FOUND", "STORED", "END"))),
    case("delete treats an expired key as missing",
         (Q("set {k} 0 -1 1", "a", "delete {k}"), R("STORED", "NOT_FOUND"))),
    case("delete argument errors",
         (Q("delete {k} 1", "delete {k} 0 0", "delete {k} x noreply"),
          R(*["CLIENT_ERROR bad command line format.  Usage: delete <key> [noreply]"] * 2)),
         (Q("delete", "delete {k} 0 noreply x"), R("ERROR", "ERROR")),
         (Q("delete " + LONG_KEY, "delete " + LONG_KEY + " noreply"), R("CLIENT_ERROR bad command line format"))),

    # ---------------- expiry over time ----------------
    case("relative exptime, touch, gat and derived values over time",
         (Q("set {plain} 0 1 1", "a", "set {touched} 0 1 1", "a", "set {gat0} 0 1 1", "a", "set {gat1} 0 0 1", "a",
            "set {touch1} 0 0 1", "a", "set {appended} 0 1 1", "a", "set {counter} 0 1 1", "1", "set {cleared} 0 1 1", "a"),
          R(*["STORED"] * 8)),
         (Q("touch {touched} 100", "gat 0 {gat0}", "gat 1 {gat1}", "touch {touch1} 1", "append {appended} 0 100 1", "b",
            "incr {counter} 1", "touch {cleared} 0"),
          R("TOUCHED", "VALUE {gat0} 0 1", "a", "END", "VALUE {gat1} 0 1", "a", "END", "TOUCHED", "STORED", "2", "TOUCHED")),
         2.2,
         (Q("get {plain} {touched} {gat0} {gat1} {touch1} {appended} {counter} {cleared}"),
          R("VALUE {touched} 0 1", "a", "VALUE {gat0} 0 1", "a", "VALUE {cleared} 0 1", "a", "END"))),
]


def pipeline_case():
    """Many keys across all shards in one write: replies must stay in request order."""
    request, reply = bytearray(), bytearray()
    for i in range(64):
        k = "{p%d}" % i
        request += Q(f"set {k} {i} 0 2", "10", f"append {k} 0 0 1", "0", f"incr {k} {i}", f"prepend {k} 0 0 1", "x",
                     f"touch {k} 100", f"gat 100 {k}", f"add {k} 0 0 1", "y", f"replace {k} 1 0 1", "z", f"decr {k} 1",
                     f"get {k}")
        reply += R("STORED", "STORED", str(100 + i), "STORED", "TOUCHED", f"VALUE {k} {i} 4", f"x{100 + i}", "END",
                   "NOT_STORED", "STORED", "CLIENT_ERROR cannot increment or decrement non-numeric value",
                   f"VALUE {k} 1 1", "z", "END")
    request += Q("gets " + " ".join("{p%d}" % i for i in range(64)))
    reply += b"".join(R("VALUE {p%d} 1 1 <cas>" % i, "z") for i in range(64)) + R("END")
    return case("pipelined commands across shards", (bytes(request), bytes(reply)))


CASES.append(pipeline_case())


class Session:
    def __init__(self, port, prefix):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.sock.settimeout(5)
        self.prefix = prefix.encode()
        self.cas = {}

    def close(self):
        self.sock.close()

    def expand(self, data):
        def repl(match):
            name = self.prefix + match.group(2)
            if match.group(1):
                return self.cas[name]
            return name

        return PLACEHOLDER.sub(repl, data)

    def request(self, data):
        self.sock.sendall(data + b"get " + SENTINEL_KEY + b"\r\n")
        received = bytearray()
        while not received.endswith(SENTINEL_REPLY):
            part = self.sock.recv(1 << 16)
            if not part:
                raise AssertionError(f"early EOF after {bytes(received)!r}")
            received += part
        reply = bytes(received[: -len(SENTINEL_REPLY)])
        for match in VALUE_LINE.finditer(reply):
            if match.group(4) is not None:
                self.cas[match.group(1)] = match.group(4)
        return reply


def normalize(reply):
    """Replace cas values with <cas> and sort each run of VALUE blocks."""
    units, pos = [], 0
    while pos < len(reply):
        match = VALUE_LINE.match(reply, pos)
        if match:
            end = match.end() + int(match.group(3)) + 2
            head = match.group(0) if match.group(4) is None else match.group(0)[: match.start(4) - match.start()] + b"<cas>\r\n"
            units.append((True, head + reply[match.end():end]))
            pos = end
            continue
        end = reply.find(b"\r\n", pos)
        end = len(reply) if end < 0 else end + 2
        units.append((False, reply[pos:end]))
        pos = end
    out, run = bytearray(), []
    for is_value, unit in units + [(False, b"")]:
        if is_value:
            run.append(unit)
            continue
        out += b"".join(sorted(run)) + unit
        run = []
    return bytes(out)


def run_cases(port, label):
    failures = 0
    run_id = f"{int(time.time() * 1000) % 100000}"
    for index, (name, steps) in enumerate(CASES):
        session = Session(port, f"s{run_id}c{index}_")
        try:
            for step in steps:
                if isinstance(step, float):
                    time.sleep(step)
                    continue
                request, expected = step
                actual = session.request(session.expand(request))
                want = session.expand(expected)
                if normalize(actual) != normalize(want):
                    failures += 1
                    print(f"FAIL [{label}] {name}\n  request:  {session.expand(request)!r}\n"
                          f"  expected: {normalize(want)!r}\n  actual:   {normalize(actual)!r}")
                    break
            else:
                print(f"PASS [{label}] {name}")
        finally:
            session.close()
    return failures


@contextlib.contextmanager
def running(command, port):
    with socket.socket() as probe:  # never attach to somebody else's server
        probe.bind(("127.0.0.1", port))
    process = subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.monotonic() + 5
        while True:
            if process.poll() is not None:
                raise RuntimeError(f"server exited: {command}")
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                    break
            except OSError:
                if time.monotonic() >= deadline:
                    raise TimeoutError("server did not start")
                time.sleep(0.02)
        with socket.create_connection(("127.0.0.1", port), timeout=5) as sock:
            sock.sendall(b"set " + SENTINEL_KEY + b" 0 0 4\r\nDONE\r\n")
            assert sock.recv(8) == b"STORED\r\n"
        yield port
    finally:
        process.terminate()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--reference", type=Path, help="memcached binary; checks the expectations against it")
    args = parser.parse_args()
    failed = 0
    if args.reference:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            ref_port = probe.getsockname()[1]
        command = [str(args.reference.resolve()), "-l", "127.0.0.1", "-p", str(ref_port), "-U", "0", "-t", "1", "-I", "16m"]
        if os.geteuid() == 0:
            command += ["-u", "root"]  # memcached refuses to run as root otherwise
        with running(command, ref_port) as port:
            failed += run_cases(port, "memcached")
    with running([str(args.server.resolve())], 2333) as port:
        failed += run_cases(port, "fastest")
    print(f"{failed} failure(s)")
    sys.exit(1 if failed else 0)
