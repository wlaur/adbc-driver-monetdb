# /// script
# requires-python = ">=3.13"
# dependencies = ["pymonetdb>=1.9"]
# ///
import argparse
import socket
import struct

import pymonetdb


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reset a connection during binary export on a disposable MonetDB server"
    )
    parser.add_argument("uri")
    args = parser.parse_args()
    connection = pymonetdb.connect(args.uri, autocommit=True)
    mapi = connection.mapi
    mapi.cmd("Xreply_size 1")
    reply = mapi.cmd("sSELECT value, value * 2, value * 3 FROM sys.generate_series(1, 20000001) AS g(value);\n")
    result_id = reply.split()[1]
    mapi._putblock(f"Xexportbin {result_id} 0 20000000")
    stream = mapi.sock
    assert stream is not None
    stream.settimeout(10)
    prefix = b""
    while len(prefix) < 80:
        chunk = stream.recv(4096)
        if not chunk:
            raise RuntimeError("server closed before the binary export started")
        prefix += chunk
    assert b"&6 " in prefix[:80], prefix[:80]
    stream.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
    stream.close()
    mapi.sock = None
    mapi.raw_sock = None
    print(f"Reset after receiving {len(prefix)} bytes of binary export")


if __name__ == "__main__":
    main()
