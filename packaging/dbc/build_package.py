from __future__ import annotations

import argparse
import io
import struct
import tarfile
import tomllib
from pathlib import Path

PLATFORMS = {
    "linux_amd64": ("MANIFEST.linux.toml", ".so", "elf", 62),
    "linux_arm64": ("MANIFEST.linux.toml", ".so", "elf", 183),
    "macos_arm64": ("MANIFEST.macos.toml", ".dylib", "macho", 0x0100000C),
    "windows_amd64": ("MANIFEST.windows.toml", ".dll", "pe", 0x8664),
}
LICENSE_FILES = {
    "LICENSE": "LICENSE",
    "NOTICE": "NOTICE",
    "monetdb-rust/LICENSE": "LICENSE.monetdb-rust",
}


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a dbc-installable MonetDB ADBC driver archive")
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--platform", choices=PLATFORMS, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--license", type=Path, required=True)
    return parser.parse_args()


def _add_bytes(archive: tarfile.TarFile, name: str, content: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(content)
    info.mode = 0o644
    archive.addfile(info, io.BytesIO(content))


def _machine(binary: bytes, kind: str) -> int:
    if kind == "elf":
        if len(binary) < 20 or binary[:6] != b"\x7fELF\x02\x01":
            raise ValueError("standalone library is not a little-endian 64-bit ELF binary")
        return struct.unpack_from("<H", binary, 18)[0]
    if kind == "macho":
        if len(binary) < 8 or binary[:4] != b"\xcf\xfa\xed\xfe":
            raise ValueError("standalone library is not a little-endian 64-bit Mach-O binary")
        return struct.unpack_from("<I", binary, 4)[0]
    if len(binary) < 64 or binary[:2] != b"MZ":
        raise ValueError("standalone library is not a PE binary")
    header = struct.unpack_from("<I", binary, 0x3C)[0]
    if header + 6 > len(binary) or binary[header : header + 4] != b"PE\0\0":
        raise ValueError("standalone library has an invalid PE header")
    return struct.unpack_from("<H", binary, header + 4)[0]


def build_package(library: Path, platform: str, out_dir: Path, license_path: Path) -> Path:
    template_name, native_suffix, binary_kind, expected_machine = PLATFORMS[platform]
    if library.suffix.lower() != native_suffix:
        raise ValueError(f"{platform} requires a {native_suffix} standalone library, got {library.name}")
    library_bytes = library.read_bytes()
    machine = _machine(library_bytes, binary_kind)
    if machine != expected_machine:
        raise ValueError(f"standalone library machine {machine:#x} does not match {platform} ({expected_machine:#x})")

    template_path = Path(__file__).with_name(template_name)
    manifest_bytes = template_path.read_bytes()
    manifest = tomllib.loads(manifest_bytes.decode())
    version = manifest["version"]
    driver_name = manifest["Files"]["driver"]
    root = Path(__file__).parents[2]
    project_version = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    if version != project_version:
        raise ValueError(f"manifest version {version} does not match project version {project_version}")

    files = {
        driver_name: library_bytes,
        "THIRD_PARTY_LICENSES": license_path.read_bytes(),
        **{archive_name: (root / source).read_bytes() for source, archive_name in LICENSE_FILES.items()},
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    output = out_dir / f"monetdb_{platform}_v{version}.tar.gz"
    with tarfile.open(output, "w:gz") as archive:
        _add_bytes(archive, "MANIFEST", manifest_bytes)
        for name, content in files.items():
            _add_bytes(archive, name, content)
    return output


def main() -> None:
    args = _arguments()
    print(build_package(args.library, args.platform, args.out_dir, args.license))


if __name__ == "__main__":
    main()
