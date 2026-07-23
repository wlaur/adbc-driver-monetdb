import socket
import sys
from queue import Queue
from threading import Event, Thread
from time import monotonic
from typing import Literal

import adbc_driver_manager
import pyarrow as pa
import pytest

from adbc_driver_monetdb import DatabaseOptions, StatementOptions, dbapi


def _read_exact(stream: socket.socket, size: int) -> bytes:
    result = bytearray()
    while len(result) < size:
        chunk = stream.recv(size - len(result))
        if not chunk:
            raise ConnectionError("client disconnected")
        result.extend(chunk)
    return bytes(result)


def _read_message(stream: socket.socket) -> bytes:
    result = bytearray()
    while True:
        header = int.from_bytes(_read_exact(stream, 2), "little")
        result.extend(_read_exact(stream, header >> 1))
        if header & 1:
            return bytes(result)


def _write_message(stream: socket.socket, message: bytes) -> None:
    stream.sendall(((len(message) << 1) | 1).to_bytes(2, "little") + message)


def _text_result(result_id: int, columns: tuple[str, ...], values: tuple[str, ...]) -> bytes:
    delimiter = ",\t"
    metadata = (
        f"&1 {result_id} 1 {len(columns)} 1\n"
        f"% {delimiter.join('sys.environment' for _ in columns)} # table_name\n"
        f"% {delimiter.join(columns)} # name\n"
        f"% {delimiter.join('varchar' for _ in columns)} # type\n"
        f"% {delimiter.join('1024' for _ in columns)} # length\n"
        f"% {delimiter.join('0 0' for _ in columns)} # typesizes\n"
    )
    row = f"[ {delimiter.join(f'"{value}"' for value in values)}\t]\n"
    return (metadata + row).encode()


class _BlackHoleServer:
    def __init__(
        self,
        *,
        initialize: bool,
        behavior: Literal["blackhole", "slow_drip", "write_stall", "connect_only"] = "blackhole",
        endian: Literal["LIT", "BIG"] = "LIT",
        advertise_binary: bool = True,
        version: str = "11.55.3",
    ) -> None:
        self._initialize = initialize
        self._behavior = behavior
        self._endian = endian
        self._advertise_binary = advertise_binary
        self._version = version
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self._listener.settimeout(5)
        self._thread = Thread(target=self._serve, daemon=True)
        self.accepted = Event()
        self.query_received = Event()
        self._release = Event()
        self.errors: Queue[BaseException] = Queue()

    @property
    def uri(self) -> str:
        return f"monetdb://127.0.0.1:{self._listener.getsockname()[1]}/test"

    def __enter__(self) -> "_BlackHoleServer":
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._release.set()
        self._listener.close()
        self._thread.join(timeout=5)
        assert not self._thread.is_alive(), "fake MAPI server did not stop"
        if not self.errors.empty():
            raise self.errors.get_nowait()

    def _serve(self) -> None:
        try:
            with self._listener.accept()[0] as stream:
                stream.settimeout(5)
                self.accepted.set()
                if not self._initialize:
                    while stream.recv(1):
                        pass
                    return
                assert _read_exact(stream, 8) == bytes(8)
                binary = "BINARY=1:" if self._advertise_binary else ""
                challenge = f"salt:mserver:9:SHA512:{self._endian}:SHA512:sql=9:{binary}".encode()
                _write_message(stream, challenge)
                _read_message(stream)
                _write_message(stream, b"=OK")
                if self._endian != "LIT" or not self._advertise_binary:
                    while stream.recv(1):
                        pass
                    return
                assert b"sys.environment" in _read_message(stream)
                _write_message(
                    stream,
                    _text_result(1, ("name", "value"), ("monet_version", self._version)),
                )
                if tuple(map(int, self._version.split("."))) < (11, 55, 0):
                    while stream.recv(1):
                        pass
                    return
                assert b"current_schema" in _read_message(stream)
                _write_message(stream, _text_result(2, ("__adbc_current_schema",), ("sys",)))
                if self._behavior == "connect_only":
                    while stream.recv(1):
                        pass
                    return
                query = _read_message(stream)
                self.query_received.set()
                if self._behavior == "blackhole":
                    while stream.recv(1):
                        pass
                    return
                if self._behavior == "slow_drip":
                    stream.sendall(((64 << 1) | 1).to_bytes(2, "little"))
                    for _ in range(64):
                        try:
                            stream.sendall(b"x")
                        except OSError:
                            return
                        if self._release.wait(0.2):
                            return
                    return
                while b"COPY" not in query:
                    response = b"&4 f\n" if b"Xauto_commit 0" in query else b"&2 0\n"
                    _write_message(stream, response)
                    query = _read_message(stream)
                _write_message(stream, b"\x01\x03\nrb c0\n")
                self._release.wait(timeout=10)
        except BaseException as error:
            if not self._release.is_set():
                self.errors.put(error)


class _FailureServer:
    def __init__(self, mode: str) -> None:
        self._mode = mode
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen(1)
        self._listener.settimeout(5)
        self._thread = Thread(target=self._serve, daemon=True)
        self.export_received = Event()
        self.errors: Queue[BaseException] = Queue()

    @property
    def uri(self) -> str:
        return f"monetdb://127.0.0.1:{self._listener.getsockname()[1]}/test"

    def __enter__(self) -> "_FailureServer":
        self._thread.start()
        return self

    def __exit__(self, *_exc: object) -> None:
        self._listener.close()
        self._thread.join(timeout=5)
        assert not self._thread.is_alive(), "fake MAPI server did not stop"
        if not self.errors.empty():
            raise self.errors.get_nowait()

    def _serve(self) -> None:
        try:
            with self._listener.accept()[0] as stream:
                stream.settimeout(5)
                assert _read_exact(stream, 8) == bytes(8)
                _write_message(stream, b"salt:mserver:9:SHA512:LIT:SHA512:sql=9:BINARY=1:")
                _read_message(stream)
                _write_message(stream, b"=OK")
                assert b"sys.environment" in _read_message(stream)
                _write_message(
                    stream,
                    _text_result(1, ("name", "value"), ("monet_version", "11.55.3")),
                )
                assert b"current_schema" in _read_message(stream)
                _write_message(stream, _text_result(2, ("__adbc_current_schema",), ("sys",)))
                assert b"SELECT value" in _read_message(stream)
                if self._mode == "disconnect":
                    return
                _write_message(
                    stream,
                    b"&1 42 2 1 1\n"
                    b"% t # table_name\n"
                    b"% value # name\n"
                    b"% int # type\n"
                    b"% 32 # length\n"
                    b"% 0 0 # typesizes\n"
                    b"[ 1\t]\n",
                )
                assert _read_message(stream).startswith(b"Xexportbin 42 0 ")
                self.export_received.set()
                if self._mode == "stream_stall":
                    while stream.recv(1):
                        pass
                    return
                _write_message(stream, b"!42000!mid-stream failure\n!second diagnostic")
        except BaseException as error:
            self.errors.put(error)


@pytest.mark.parametrize("option_channel", ["uri", "kwargs"])
def test_connect_deadline_covers_silent_login(option_channel: str) -> None:
    with _BlackHoleServer(initialize=False) as server:
        uri = f"{server.uri}?connect_timeout=1" if option_channel == "uri" else server.uri
        db_kwargs: dict[str, str] | None = None
        if option_channel == "kwargs":
            db_kwargs = {DatabaseOptions.CONNECT_TIMEOUT: "1"}
        started = monotonic()
        with pytest.raises(adbc_driver_manager.OperationalError) as caught:
            dbapi.connect(uri, db_kwargs=db_kwargs, autocommit=True)
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.TIMEOUT
        assert monotonic() - started < 5
        assert server.accepted.is_set()


def test_statement_read_timeout_closes_black_holed_connection() -> None:
    with (
        _BlackHoleServer(initialize=True) as server,
        dbapi.connect(server.uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={StatementOptions.READ_TIMEOUT: 1, StatementOptions.OPERATION_TIMEOUT: 0}
        ) as cursor,
    ):
        started = monotonic()
        with pytest.raises(adbc_driver_manager.OperationalError) as caught:
            cursor.execute("SELECT 1")
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.TIMEOUT
        assert monotonic() - started < 5
        assert server.query_received.is_set()
        with pytest.raises(adbc_driver_manager.ProgrammingError):
            cursor.execute("SELECT 2")


def test_statement_operation_timeout_bounds_slow_drip_response() -> None:
    with (
        _BlackHoleServer(initialize=True, behavior="slow_drip") as server,
        dbapi.connect(server.uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={StatementOptions.READ_TIMEOUT: 0, StatementOptions.OPERATION_TIMEOUT: 1}
        ) as cursor,
    ):
        started = monotonic()
        with pytest.raises(adbc_driver_manager.OperationalError) as caught:
            cursor.execute("SELECT 1")
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.TIMEOUT
        assert monotonic() - started < 5
        assert server.query_received.is_set()


@pytest.mark.skipif(
    sys.platform == "win32",
    reason="Windows loopback accepts a complete 16 MiB upload fragment before the peer stops reading",
)
def test_statement_write_timeout_bounds_stalled_copy_upload() -> None:
    rows = 8 * 1024 * 1024
    batch = pa.record_batch({"value": pa.nulls(rows, type=pa.int64())})
    with (
        _BlackHoleServer(initialize=True, behavior="write_stall") as server,
        dbapi.connect(server.uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.READ_TIMEOUT: 30,
                StatementOptions.WRITE_TIMEOUT: 1,
                StatementOptions.OPERATION_TIMEOUT: 0,
            }
        ) as cursor,
    ):
        started = monotonic()
        with pytest.raises(adbc_driver_manager.OperationalError) as caught:
            cursor.adbc_ingest("write_timeout", batch, mode="create")
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.TIMEOUT
        assert monotonic() - started < 5
        assert server.query_received.is_set()


@pytest.mark.parametrize(
    ("server_options", "message"),
    [
        ({"endian": "BIG"}, "only little-endian"),
        ({"advertise_binary": False}, "does not advertise BINARY=1"),
        ({"version": "11.54.99"}, "11.54.99 is unsupported"),
    ],
)
def test_connect_rejects_unsupported_server_capabilities(
    server_options: dict[str, object],
    message: str,
) -> None:
    with _BlackHoleServer(initialize=True, behavior="connect_only", **server_options) as server:  # pyright: ignore[reportArgumentType]
        with pytest.raises(adbc_driver_manager.NotSupportedError, match=message) as caught:
            dbapi.connect(server.uri, autocommit=True)
        assert caught.value.status_code == adbc_driver_manager.AdbcStatusCode.NOT_IMPLEMENTED


def test_connect_accepts_next_major_server_version() -> None:
    with (
        _BlackHoleServer(initialize=True, behavior="connect_only", version="12.0.0") as server,
        dbapi.connect(server.uri, autocommit=True),
    ):
        pass


def test_statement_cancel_interrupts_black_holed_query() -> None:
    with (
        _BlackHoleServer(initialize=True) as server,
        dbapi.connect(server.uri, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        outcome: Queue[BaseException | None] = Queue()

        def execute() -> None:
            try:
                cursor.execute("SELECT 1")
            except BaseException as error:
                outcome.put(error)
            else:
                outcome.put(None)

        worker = Thread(target=execute, daemon=True)
        worker.start()
        assert server.query_received.wait(timeout=5), "query did not reach the fake server"
        cursor.adbc_cancel()
        worker.join(timeout=5)
        assert not worker.is_alive(), "cancelled query did not return"
        result = outcome.get_nowait()
        assert isinstance(result, adbc_driver_manager.OperationalError)
        assert result.status_code == adbc_driver_manager.AdbcStatusCode.CANCELLED
        assert "connection closed by cancel" in str(result)
        with pytest.raises(adbc_driver_manager.ProgrammingError):
            cursor.execute("SELECT 2")


def test_query_disconnect_is_io_and_closes_connection() -> None:
    with (
        _FailureServer("disconnect") as server,
        dbapi.connect(server.uri, autocommit=True) as connection,
        connection.cursor() as cursor,
    ):
        with pytest.raises(adbc_driver_manager.OperationalError) as disconnected:
            cursor.execute("SELECT value")
        assert disconnected.value.status_code == adbc_driver_manager.AdbcStatusCode.IO
        with pytest.raises(adbc_driver_manager.ProgrammingError):
            cursor.execute("SELECT value")


def test_midstream_error_preserves_sqlstate_and_all_diagnostics() -> None:
    with (
        _FailureServer("stream_error") as server,
        dbapi.connect(server.uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.READ_BATCH_ROWS: 1,
                StatementOptions.READ_PREFETCH: "true",
            }
        ) as cursor,
    ):
        cursor.execute("SELECT value")
        with pytest.raises(ValueError, match="42000!mid-stream failure") as failed:
            cursor.fetchall()
        assert "second diagnostic" in str(failed.value)
        with pytest.raises(adbc_driver_manager.OperationalError) as disconnected:
            cursor.execute("SELECT value")
        assert disconnected.value.status_code == adbc_driver_manager.AdbcStatusCode.IO
        with pytest.raises(adbc_driver_manager.ProgrammingError):
            cursor.execute("SELECT value")


def test_closing_prefetched_reader_cancels_a_stalled_fetch() -> None:
    with (
        _FailureServer("stream_stall") as server,
        dbapi.connect(server.uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.READ_BATCH_ROWS: 1,
                StatementOptions.READ_PREFETCH: "true",
            }
        ) as cursor,
    ):
        cursor.execute("SELECT value")
        reader = cursor.fetch_record_batch()
        assert server.export_received.wait(timeout=5), "prefetch did not reach the fake server"

        started = monotonic()
        reader.close()
        assert monotonic() - started < 5
        with pytest.raises(adbc_driver_manager.ProgrammingError):
            cursor.execute("SELECT value")


def test_cancel_interrupts_a_stalled_prefetch() -> None:
    with (
        _FailureServer("stream_stall") as server,
        dbapi.connect(server.uri, autocommit=True) as connection,
        connection.cursor(
            adbc_stmt_kwargs={
                StatementOptions.READ_BATCH_ROWS: 1,
                StatementOptions.READ_PREFETCH: "true",
            }
        ) as cursor,
    ):
        cursor.execute("SELECT value")
        reader = cursor.fetch_record_batch()
        assert server.export_received.wait(timeout=5), "prefetch did not reach the fake server"

        cursor.adbc_cancel()
        reader.close()
        with pytest.raises(adbc_driver_manager.ProgrammingError):
            cursor.execute("SELECT value")
