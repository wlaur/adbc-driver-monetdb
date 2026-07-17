import hashlib
import re
import socket
import ssl
import subprocess
from contextlib import suppress
from pathlib import Path
from threading import Event, Thread
from types import TracebackType
from urllib.parse import quote, urlencode, urlsplit

import adbc_driver_manager
import pytest

from adbc_driver_monetdb import dbapi


class _TlsProxy:
    def __init__(self, context: ssl.SSLContext, upstream_host: str, upstream_port: int) -> None:
        self._context = context
        self._upstream = (upstream_host, upstream_port)
        self._listener = socket.socket()
        self._listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._listener.bind(("127.0.0.1", 0))
        self._listener.listen()
        self._listener.settimeout(0.1)
        self._stop = Event()
        self._thread = Thread(target=self._serve)
        self._workers: list[Thread] = []

    @property
    def port(self) -> int:
        return self._listener.getsockname()[1]

    def __enter__(self) -> "_TlsProxy":
        self._thread.start()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._stop.set()
        self._listener.close()
        self._thread.join()
        for worker in self._workers:
            worker.join()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                client, _ = self._listener.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            worker = Thread(target=self._handle, args=(client,))
            self._workers.append(worker)
            worker.start()

    def _handle(self, raw_client: socket.socket) -> None:
        try:
            with self._context.wrap_socket(raw_client, server_side=True) as client:
                if client.selected_alpn_protocol() != "mapi/9":
                    return
                with socket.create_connection(self._upstream) as server:
                    upstream = Thread(target=self._pump, args=(client, server))
                    downstream = Thread(target=self._pump, args=(server, client))
                    upstream.start()
                    downstream.start()
                    upstream.join()
                    downstream.join()
        except (OSError, ssl.SSLError):
            raw_client.close()

    @staticmethod
    def _pump(source: socket.socket, destination: socket.socket) -> None:
        try:
            while data := source.recv(65_536):
                destination.sendall(data)
        except OSError:
            pass
        with suppress(OSError):
            destination.shutdown(socket.SHUT_WR)


def _openssl(directory: Path, *arguments: str) -> None:
    subprocess.run(
        ["openssl", *arguments],
        cwd=directory,
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
    )


def _certificates(directory: Path) -> tuple[Path, Path, Path, Path, str]:
    try:
        version = subprocess.run(
            ["openssl", "version"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout
    except (FileNotFoundError, subprocess.CalledProcessError):
        pytest.skip("OpenSSL 3 CLI is required for TLS certificate tests")
    match = re.match(r"OpenSSL\s+(\d+)", version)
    if match is None or int(match.group(1)) < 3:
        pytest.skip("OpenSSL 3 CLI is required for -copy_extensions")

    ca = directory / "ca.crt"
    server = directory / "server.crt"
    client = directory / "client.crt"
    _openssl(
        directory,
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-keyout",
        "ca.key",
        "-out",
        ca.name,
        "-subj",
        "/CN=ADBC Test CA",
        "-addext",
        "basicConstraints=critical,CA:TRUE",
        "-addext",
        "keyUsage=critical,keyCertSign,cRLSign",
        "-days",
        "1",
    )
    for name, common_name, usage in [
        ("server", "localhost", "serverAuth"),
        ("client", "ADBC Client", "clientAuth"),
    ]:
        arguments = [
            "req",
            "-new",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-keyout",
            f"{name}.key",
            "-out",
            f"{name}.csr",
            "-subj",
            f"/CN={common_name}",
            "-addext",
            "basicConstraints=critical,CA:FALSE",
            "-addext",
            "keyUsage=critical,digitalSignature,keyEncipherment",
            "-addext",
            f"extendedKeyUsage={usage}",
        ]
        if name == "server":
            arguments.extend(["-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1"])
        _openssl(directory, *arguments)
        _openssl(
            directory,
            "x509",
            "-req",
            "-in",
            f"{name}.csr",
            "-CA",
            ca.name,
            "-CAkey",
            "ca.key",
            "-CAcreateserial",
            "-out",
            f"{name}.crt",
            "-days",
            "1",
            "-copy_extensions",
            "copy",
        )
    der = ssl.PEM_cert_to_DER_cert(server.read_text())
    fingerprint = hashlib.sha256(der).hexdigest()
    return ca, server, directory / "server.key", client, fingerprint


def _server_context(server: Path, server_key: Path, ca: Path | None = None) -> ssl.SSLContext:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_3
    context.load_cert_chain(server, server_key)
    context.set_alpn_protocols(["mapi/9"])
    if ca is not None:
        context.load_verify_locations(ca)
        context.verify_mode = ssl.CERT_REQUIRED
    return context


def _tls_uri(monetdb_uri: str, port: int, **parameters: str) -> str:
    parsed = urlsplit(monetdb_uri)
    user = quote(parsed.username or "", safe="")
    password = quote(parsed.password or "", safe="")
    credentials = f"{user}:{password}@" if user or password else ""
    query = f"?{urlencode(parameters)}" if parameters else ""
    return f"monetdbs://{credentials}localhost:{port}{parsed.path}{query}"


def _select_42(uri: str) -> tuple[object, ...] | None:
    with dbapi.connect(uri) as connection, connection.cursor() as cursor:
        cursor.execute("SELECT 42")
        return cursor.fetchone()


@pytest.mark.integration
def test_tls_system_roots_custom_ca_and_client_certificates(monetdb_uri: str, tmp_path: Path) -> None:
    parsed = urlsplit(monetdb_uri)
    assert parsed.hostname is not None
    ca, server, server_key, client, fingerprint = _certificates(tmp_path)

    with _TlsProxy(_server_context(server, server_key), parsed.hostname, parsed.port or 50000) as proxy:
        assert _select_42(_tls_uri(monetdb_uri, proxy.port, certhash=f"sha256:{fingerprint[:17]}")) == (42,)
        assert _select_42(_tls_uri(monetdb_uri, proxy.port, cert=str(ca))) == (42,)
        with pytest.raises(adbc_driver_manager.OperationalError):
            dbapi.connect(_tls_uri(monetdb_uri, proxy.port, certhash="sha256:deadbeef"))
        with pytest.raises(adbc_driver_manager.OperationalError):
            dbapi.connect(_tls_uri(monetdb_uri, proxy.port))

    with _TlsProxy(_server_context(server, server_key, ca), parsed.hostname, parsed.port or 50000) as proxy:
        with pytest.raises(adbc_driver_manager.OperationalError):
            dbapi.connect(_tls_uri(monetdb_uri, proxy.port, cert=str(ca)))
        assert _select_42(
            _tls_uri(
                monetdb_uri,
                proxy.port,
                cert=str(ca),
                clientcert=str(client),
                clientkey=str(tmp_path / "client.key"),
            )
        ) == (42,)
