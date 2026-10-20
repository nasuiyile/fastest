"""Wire regression tests; starts/stops its own server, uses only Python stdlib.

python3 tests/python/test_protocol_errors.py --server cmake-build-debug/fastest
Optional: --reference /path/to/memcached (compares the same cases with 1.6.x).
"""
import argparse
import contextlib
from pathlib import Path
import socket
import select
import subprocess
import threading
import time


@contextlib.contextmanager
def running(command, port):
    # fastest has a fixed port. Never attach to or terminate somebody else's server.
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", port))
    with open("/tmp/fastest-protocol-test.log", "ab") as log:
        process = subprocess.Popen(command, stdout=log, stderr=log)
        try:
            deadline = time.monotonic() + 5
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"server exited: {command}; see /tmp/fastest-protocol-test.log")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise TimeoutError("server did not start")
                    time.sleep(0.02)
            yield port
        finally:
            process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=3)


def connect(port):
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    sock.settimeout(5)
    return sock


def expect(sock, expected):
    received = bytearray()
    while len(received) < len(expected):
        part = sock.recv(len(expected) - len(received))
        if not part:
            raise AssertionError(f"early EOF: {bytes(received)!r}; expected {expected!r}")
        received.extend(part)
    assert bytes(received) == expected, (bytes(received), expected)


def wire_cases(port):
    cases = [
        ("unknown after valid prefix", b"set k 0 0 1\r\nx\r\nunknown\r\nget k\r\n",
         b"STORED\r\nERROR\r\nVALUE k 0 1\r\nx\r\nEND\r\n"),
        ("malformed header preserves body boundary", b"set k invalid 0 1\r\nx\r\nget missing\r\n",
         b"CLIENT_ERROR bad command line format\r\nERROR\r\nEND\r\n"),
        ("wrong argument count", b"set k 0 0\r\nget\r\nget missing\r\n", b"ERROR\r\nERROR\r\nEND\r\n"),
        ("bad data suffix", b"set k 0 0 1\r\nx!!get missing\r\n", b"CLIENT_ERROR bad data chunk\r\nEND\r\n"),
        ("noreply bad header", b"set k invalid 0 1 noreply\r\nx\r\nget missing\r\n", b"ERROR\r\nEND\r\n"),
        ("noreply bad suffix", b"set k 0 0 1 noreply\r\nx!!get missing\r\n", b"END\r\n"),
        ("LF command lines", b"set k 0 0 1\nx\r\n\nget k\n", b"STORED\r\nERROR\r\nVALUE k 0 1\r\nx\r\nEND\r\n"),
        ("invalid length does not swallow", b"set k 0 0 2147483646\r\nget missing\r\n",
         b"CLIENT_ERROR bad command line format\r\nEND\r\n"),
        ("long key", b"get " + b"k" * 251 + b"\r\nget missing\r\n",
         b"CLIENT_ERROR bad command line format\r\nEND\r\n"),
    ]
    for name, request, response in cases:
        with connect(port) as sock:
            sock.sendall(request)
            expect(sock, response)
            sock.sendall(b"get connection_still_usable\r\n")
            expect(sock, b"END\r\n")
        print("PASS", name)

    # Force many parser batches and mailbox round trips; each value has a
    # distinct payload so reusing the input buffer cannot silently pass.
    with connect(port) as sock:
        request = bytearray()
        expected = bytearray()
        for i in range(384):
            key = f"pipeline{i}".encode()
            value = (f"payload-{i}-".encode() + b"\0") * 16
            request.extend(b"set " + key + b" 7 0 " + str(len(value)).encode() + b"\r\n" + value + b"\r\nunknown\r\nget " + key + b"\r\n")
            expected.extend(b"STORED\r\nERROR\r\nVALUE " + key + b" 7 " + str(len(value)).encode() + b"\r\n" + value + b"\r\nEND\r\n")
        sock.sendall(request)
        expect(sock, expected)
    print("PASS 1,152 pipelined commands with binary payloads")

    # A refused SET removes its old value. Command-looking bytes in the
    # discarded body must never execute; the error follows the complete body.
    for noreply in (False, True):
        with connect(port) as sock:
            sock.sendall(b"set rejected 0 0 3\r\nold\r\n")
            expect(sock, b"STORED\r\n")
            size = 16 * 1024 * 1024 + 1
            sock.sendall(b"set rejected 0 0 " + str(size).encode() + (b" noreply" if noreply else b"") + b"\r\n")
            assert not select.select([sock], [], [], 0.05)[0], "reply arrived before data was swallowed"
            chunk = (b"get rejected\r\n" * 8192)[:65536]
            remaining = size
            while remaining:
                part = chunk[:min(remaining, len(chunk))]
                sock.sendall(part)
                remaining -= len(part)
            sock.sendall(b"\r")
            sock.sendall(b"\nget rejected\r\nget after_swallow\r\n")
            expected = b"END\r\nEND\r\n"
            if not noreply:
                expected = b"SERVER_ERROR object too large for cache\r\n" + expected
            expect(sock, expected)
        print("PASS oversized SET streams, invalidates old value, noreply=", noreply)


def fatal_case(port):
    with connect(port) as sock:
        sock.sendall(b"set before_fatal 0 0 1\r\nx\r\nget before_fatal\r\n" + b"z" * 8193)
        expect(sock, b"STORED\r\nVALUE before_fatal 0 1\r\nx\r\nEND\r\nCLIENT_ERROR command line too long\r\n")
        assert sock.recv(1) == b"", "fatal error did not close the connection"
    print("PASS configured fatal line limit drains prefix before EOF")


def pipeline_cases(port):
    # 读写互不等待：写由回调驱动，mailbox 不等 socket。只发不读的慢连接不能拖住同分片的其他连接。
    big = b"x" * (1 << 20)
    with connect(port) as sock:
        for i in range(8):
            sock.sendall(b"set big%d 0 0 %d\r\n" % (i, len(big)) + big + b"\r\n")
            expect(sock, b"STORED\r\n")
            sock.sendall(b"set small%d 0 0 1\r\nv\r\n" % i)
            expect(sock, b"STORED\r\n")
    slow = []
    try:
        for _ in range(4):
            sock = connect(port)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
            sock.sendall((b"get " + b" ".join(b"big%d" % i for i in range(8)) + b"\r\n") * 20)
            slow.append(sock)
        time.sleep(0.5)
        for _ in range(8):
            with connect(port) as sock:
                sock.settimeout(2)
                for i in range(8):
                    sock.sendall(b"get small%d\r\n" % i)
                    expect(sock, b"VALUE small%d 0 1\r\nv\r\nEND\r\n" % i)
    finally:
        for sock in slow:
            sock.close()
    print("PASS slow readers do not stall other connections")

    # 大小响应混合、跨分片和本分片混合，并且客户端先不读：响应字节流不能交错。
    values = {b"mix%d" % i: (bytes([65 + i]) * (512 * 1024 if i % 2 == 0 else 16)) for i in range(8)}
    with connect(port) as sock:
        sock.settimeout(20)
        for key, value in values.items():
            sock.sendall(b"set " + key + b" 0 0 %d\r\n" % len(value) + value + b"\r\n")
            expect(sock, b"STORED\r\n")
        order = [b"mix%d" % (i % 8) for i in range(600)]

        def sender():
            sock.sendall(b"".join(b"get " + key + b"\r\n" for key in order[:200]))
            for key in order[200:]:
                sock.sendall(b"get " + key + b"\r\n")
                time.sleep(0.002)

        thread = threading.Thread(target=sender)
        thread.start()
        time.sleep(1)
        for key in order:
            expect(sock, b"VALUE " + key + b" 0 %d\r\n" % len(values[key]) + values[key] + b"\r\nEND\r\n")
        thread.join()
    print("PASS mixed large/small pipelined responses stay byte-exact")

    # 背压：客户端只发不读时，积压超过高水位后服务端暂停读取；客户端开始读之后全部响应按序到达。
    with connect(port) as sock:
        sock.sendall(b"set bp 0 0 1\r\nv\r\n")
        expect(sock, b"STORED\r\n")
        count = 4_000_000
        request = b"get bp\r\n" * count
        sock.setblocking(False)
        sent = 0

        def push(seconds):
            nonlocal sent
            deadline = time.monotonic() + seconds
            while time.monotonic() < deadline and sent < len(request):
                try:
                    sent += sock.send(request[sent:sent + (1 << 20)])
                except BlockingIOError:
                    time.sleep(0.01)

        # 等到某个 0.5s 窗口内一个字节都发不出去：说明服务端已停止读取（Debug 构建较慢，所以多给几轮）。
        stalled = False
        for _ in range(20):
            before = sent
            push(0.5)
            if sent == before:
                stalled = True
                break
        assert stalled and sent < len(request), f"server kept reading ({sent} of {len(request)} bytes) without the client reading"
        reply = b"VALUE bp 0 1\r\nv\r\nEND\r\n"
        received, total = 0, len(reply) * count
        while received < total:
            readable, writable, _ = select.select([sock], [sock] if sent < len(request) else [], [], 5)
            assert readable or writable, f"stalled after {received} of {total} bytes"
            if writable:
                try:
                    sent += sock.send(request[sent:sent + (1 << 20)])
                except BlockingIOError:
                    pass
            if readable:
                chunk = sock.recv(1 << 20)
                assert chunk, "early EOF"
                start = received % len(reply)
                assert chunk == (reply * (len(chunk) // len(reply) + 2))[start:start + len(chunk)], "corrupted reply stream"
                received += len(chunk)
    print("PASS backpressure pauses reading until the client reads")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--server", type=Path, required=True)
    parser.add_argument("--reference", type=Path)
    args = parser.parse_args()
    with running([str(args.server.resolve())], 2333) as port:
        wire_cases(port)
        fatal_case(port)
        pipeline_cases(port)
    if args.reference:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            ref_port = probe.getsockname()[1]
        with running([str(args.reference.resolve()), "-l", "127.0.0.1", "-p", str(ref_port), "-U", "0", "-t", "1", "-I", "16m"], ref_port) as port:
            wire_cases(port)
