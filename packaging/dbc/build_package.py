from __future__ import annotations

import argparse
import io
import tarfile
import tomllib
from pathlib import Path
from zipfile import ZipFile

PLATFORMS = {
    "linux_amd64": ("MANIFEST.linux.toml", ".so"),
    "linux_arm64": ("MANIFEST.linux.toml", ".so"),
    "macos_arm64": ("MANIFEST.macos.toml", ".so"),
    "windows_amd64": ("MANIFEST.windows.toml", ".pyd"),
}
LICENSE_MEMBERS = {
    "/licenses/LICENSE": "LICENSE",
    "/licenses/NOTICE": "NOTICE",
    "/licenses/monetdb-rust/LICENSE": "LICENSE.monetdb-rust",
}


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build a dbc-installable MonetDB ADBC driver archive")
    parser.add_argument("--wheel", type=Path, required=True)
    parser.add_argument("--platform", choices=PLATFORMS, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--license", type=Path, required=True)
    return parser.parse_args()


def _add_bytes(archive: tarfile.TarFile, name: str, content: bytes) -> None:
    info = tarfile.TarInfo(name)
    info.size = len(content)
    info.mode = 0o644
    archive.addfile(info, io.BytesIO(content))


def build_package(wheel: Path, platform: str, out_dir: Path, license_path: Path) -> Path:
    template_name, native_suffix = PLATFORMS[platform]
    template_path = Path(__file__).with_name(template_name)
    manifest_bytes = template_path.read_bytes()
    manifest = tomllib.loads(manifest_bytes.decode())
    version = manifest["version"]
    driver_name = manifest["Files"]["driver"]
    project_path = Path(__file__).parents[2] / "pyproject.toml"
    project_version = tomllib.loads(project_path.read_text())["project"]["version"]
    if version != project_version:
        raise ValueError(f"manifest version {version} does not match project version {project_version}")

    with ZipFile(wheel) as wheel_archive:
        native_members = [
            name
            for name in wheel_archive.namelist()
            if name.startswith("adbc_driver_monetdb/_native") and name.endswith(native_suffix)
        ]
        if len(native_members) != 1:
            raise ValueError(f"expected one native driver in {wheel}, found {native_members}")
        files = {
            driver_name: wheel_archive.read(native_members[0]),
            "THIRD_PARTY_LICENSES": license_path.read_bytes(),
        }
        for suffix, archive_name in LICENSE_MEMBERS.items():
            members = [name for name in wheel_archive.namelist() if name.endswith(suffix)]
            if len(members) != 1:
                raise ValueError(f"expected one {suffix} in {wheel}, found {members}")
            files[archive_name] = wheel_archive.read(members[0])

    out_dir.mkdir(parents=True, exist_ok=True)
    output = out_dir / f"monetdb_{platform}_v{version}.tar.gz"
    with tarfile.open(output, "w:gz") as archive:
        _add_bytes(archive, "MANIFEST", manifest_bytes)
        for name, content in files.items():
            _add_bytes(archive, name, content)
    return output


def main() -> None:
    args = _arguments()
    print(build_package(args.wheel, args.platform, args.out_dir, args.license))


if __name__ == "__main__":
    main()
