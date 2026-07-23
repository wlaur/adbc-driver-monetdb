import struct
import subprocess
import sys
import tarfile
import tomllib
from pathlib import Path

import pytest

ROOT = Path(__file__).parents[1]
BUILDER = ROOT / "packaging" / "dbc" / "build_package.py"
PLATFORMS = {
    "linux_amd64": ("libadbc_driver_monetdb.so", ".so", "elf", 62),
    "linux_arm64": ("libadbc_driver_monetdb.so", ".so", "elf", 183),
    "macos_arm64": ("libadbc_driver_monetdb.dylib", ".dylib", "macho", 0x0100000C),
    "windows_amd64": ("adbc_driver_monetdb.dll", ".dll", "pe", 0x8664),
}


def test_installation_docs_only_advertise_published_channels() -> None:
    readme = (ROOT / "README.md").read_text()
    template = (ROOT / "docs" / "monetdb.md").read_text()

    for documentation in (readme, template):
        assert "uv add adbc-driver-monetdb" in documentation
        assert "dbc install --no-verify" in documentation
        assert "dbc install monetdb" not in documentation


def _binary(kind: str, machine: int) -> bytes:
    binary = bytearray(128)
    if kind == "elf":
        binary[:6] = b"\x7fELF\x02\x01"
        struct.pack_into("<H", binary, 18, machine)
    elif kind == "macho":
        binary[:4] = b"\xcf\xfa\xed\xfe"
        struct.pack_into("<I", binary, 4, machine)
    else:
        binary[:2] = b"MZ"
        struct.pack_into("<I", binary, 0x3C, 64)
        binary[64:68] = b"PE\0\0"
        struct.pack_into("<H", binary, 68, machine)
    return bytes(binary)


def _run_builder(tmp_path: Path, platform: str, suffix: str, binary: bytes) -> subprocess.CompletedProcess[str]:
    library = tmp_path / f"driver{suffix}"
    library.write_bytes(binary)
    license_path = tmp_path / "THIRD_PARTY_LICENSES"
    license_path.write_text("dependency licenses")
    source_root = tmp_path / "source"
    (source_root / "monetdb-rust").mkdir(parents=True)
    version = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["version"]
    (source_root / "pyproject.toml").write_text(f'[project]\nversion = "{version}"\n')
    (source_root / "LICENSE").write_text("MIT")
    (source_root / "NOTICE").write_text("notice")
    (source_root / "monetdb-rust" / "LICENSE").write_text("MPL")
    return subprocess.run(
        [
            sys.executable,
            str(BUILDER),
            "--library",
            str(library),
            "--platform",
            platform,
            "--out-dir",
            str(tmp_path / "out"),
            "--license",
            str(license_path),
            "--source-root",
            str(source_root),
        ],
        check=False,
        capture_output=True,
        text=True,
    )


@pytest.mark.parametrize(
    ("platform", "driver_name", "suffix", "kind", "machine"),
    [(platform, *values) for platform, values in PLATFORMS.items()],
)
def test_builds_flat_dbc_archive(
    tmp_path: Path,
    platform: str,
    driver_name: str,
    suffix: str,
    kind: str,
    machine: int,
) -> None:
    completed = _run_builder(tmp_path, platform, suffix, _binary(kind, machine))
    assert completed.returncode == 0, completed.stderr

    packages = list((tmp_path / "out").glob("*.tar.gz"))
    assert len(packages) == 1
    with tarfile.open(packages[0]) as archive:
        assert archive.getnames() == [
            "MANIFEST",
            driver_name,
            "THIRD_PARTY_LICENSES",
            "LICENSE",
            "NOTICE",
            "LICENSE.monetdb-rust",
        ]
        manifest_file = archive.extractfile("MANIFEST")
        assert manifest_file is not None
        manifest = tomllib.loads(manifest_file.read().decode())
        driver_file = archive.extractfile(driver_name)
        assert driver_file is not None
        assert driver_file.read() == _binary(kind, machine)
        dependency_licenses = archive.extractfile("THIRD_PARTY_LICENSES")
        assert dependency_licenses is not None
        assert dependency_licenses.read() == b"dependency licenses"
        for name, content in {"LICENSE": b"MIT", "NOTICE": b"notice", "LICENSE.monetdb-rust": b"MPL"}.items():
            license_file = archive.extractfile(name)
            assert license_file is not None
            assert license_file.read() == content
    assert manifest["description"] == "ADBC driver for MonetDB: Arrow-native reads and writes"
    assert manifest["url"] == "https://github.com/wlaur/adbc-driver-monetdb"
    assert manifest["Driver"]["entrypoint"] == "AdbcDriverMonetdbInit"
    assert manifest["Files"]["driver"] == driver_name


@pytest.mark.parametrize(
    ("platform", "driver_name", "suffix", "kind", "machine"),
    [(platform, *values) for platform, values in PLATFORMS.items()],
)
def test_rejects_wrong_architecture_or_format(
    tmp_path: Path,
    platform: str,
    driver_name: str,
    suffix: str,
    kind: str,
    machine: int,
) -> None:
    del driver_name
    wrong_arch = _run_builder(tmp_path, platform, suffix, _binary(kind, machine ^ 1))
    assert wrong_arch.returncode != 0
    assert "does not match" in wrong_arch.stderr

    invalid_dir = tmp_path / "invalid"
    invalid_dir.mkdir()
    invalid = _run_builder(invalid_dir, platform, suffix, b"not a shared library")
    assert invalid.returncode != 0
    assert "standalone library is not" in invalid.stderr
