import ipaddress
import socket
import ssl
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from threading import Event, Thread
from types import TracebackType
from urllib.parse import quote, urlencode, urlsplit

import adbc_driver_manager
import polars as pl
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

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


def _certificates(directory: Path) -> tuple[Path, Path, Path, Path, str]:
    ca = directory / "ca.crt"
    server = directory / "server.crt"
    client = directory / "client.crt"
    now = datetime.now(UTC)
    ca_key = rsa.generate_private_key(public_exponent=65_537, key_size=2048)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "ADBC Test CA")])
    ca_cert = (
        x509.CertificateBuilder()
        .subject_name(ca_name)
        .issuer_name(ca_name)
        .public_key(ca_key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .add_extension(
            x509.KeyUsage(
                digital_signature=False,
                content_commitment=False,
                key_encipherment=False,
                data_encipherment=False,
                key_agreement=False,
                key_cert_sign=True,
                crl_sign=True,
                encipher_only=False,
                decipher_only=False,
            ),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    ca.write_bytes(ca_cert.public_bytes(serialization.Encoding.PEM))
    (directory / "ca.key").write_bytes(
        ca_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )

    leaf_certificates: dict[str, x509.Certificate] = {}
    for name, common_name, usage in [
        ("server", "localhost", ExtendedKeyUsageOID.SERVER_AUTH),
        ("client", "ADBC Client", ExtendedKeyUsageOID.CLIENT_AUTH),
    ]:
        key = rsa.generate_private_key(public_exponent=65_537, key_size=2048)
        subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, common_name)])
        builder = (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_name)
            .public_key(key.public_key())
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(days=1))
            .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True)
            .add_extension(
                x509.KeyUsage(
                    digital_signature=True,
                    content_commitment=False,
                    key_encipherment=True,
                    data_encipherment=False,
                    key_agreement=False,
                    key_cert_sign=False,
                    crl_sign=False,
                    encipher_only=False,
                    decipher_only=False,
                ),
                critical=True,
            )
            .add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
        )
        if name == "server":
            builder = builder.add_extension(
                x509.SubjectAlternativeName(
                    [x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
                ),
                critical=False,
            )
        certificate = builder.sign(ca_key, hashes.SHA256())
        leaf_certificates[name] = certificate
        (directory / f"{name}.crt").write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
        (directory / f"{name}.key").write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.NoEncryption(),
            )
        )
    fingerprint = leaf_certificates["server"].fingerprint(hashes.SHA256()).hex()
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
        hash_uri = _tls_uri(monetdb_uri, proxy.port, certhash=f"sha256:{fingerprint[:17]}")
        ca_uri = _tls_uri(monetdb_uri, proxy.port, cert=str(ca))
        assert _select_42(hash_uri) == (42,)
        assert _select_42(ca_uri) == (42,)
        assert pl.read_database_uri("SELECT 42 AS value", hash_uri, engine="adbc").item() == 42
        assert pl.read_database_uri("SELECT 42 AS value", ca_uri, engine="adbc").item() == 42
        with pytest.raises(adbc_driver_manager.OperationalError):
            dbapi.connect(_tls_uri(monetdb_uri, proxy.port, certhash="sha256:deadbeefdeadbeef"))
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
