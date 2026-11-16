#!/usr/bin/env python3
"""Exercise the embedded echo reader without DPDK: python3 -m unittest discover -s switch-bench/local."""
import ast
from pathlib import Path
import unittest


script = Path(__file__).with_name("junction-smoke.sh").read_text()
source = script.split("python3 - <<'PY'\n", 1)[1].rsplit("\nPY", 1)[0]
reader = next(node for node in ast.parse(source).body
              if isinstance(node, ast.FunctionDef) and node.name == "recv_echo")
namespace = {}
exec(compile(ast.Module(body=[reader], type_ignores=[]), "junction-smoke.sh", "exec"), namespace)
recv_echo = namespace["recv_echo"]


class Peer:
    def __init__(self, chunks):
        self.chunks = iter(chunks)

    def recv(self, size):
        try:
            chunk = next(self.chunks)
        except StopIteration:
            raise AssertionError("echo reader retried after EOF or completion") from None
        assert len(chunk) <= size
        return chunk


class EchoReaderTests(unittest.TestCase):
    def test_tcp_echo_can_arrive_in_parts(self):
        self.assertEqual(recv_echo(Peer([b"ju", b"nction"]), 8), b"junction")

    def test_tcp_eof_before_complete_echo_fails_immediately(self):
        for chunks, received in [([b""], 0), ([b"jun", b""], 3)]:
            with self.subTest(received=received):
                with self.assertRaisesRegex(EOFError, f"closed after {received}/8 bytes"):
                    recv_echo(Peer(chunks), 8)


if __name__ == "__main__":
    unittest.main()
