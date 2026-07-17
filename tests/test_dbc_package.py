import subprocess
import sys
import tarfile
import tomllib
from pathlib import Path
from zipfile import ZipFile

import pytest

ROOT = Path(__file__).parents[1]
BUILDER = ROOT / "packaging" / "dbc" / "build_package.py"
PLATFORMS = {
    "linux_amd64": ("libadbc_driver_monetdb.so", ".so"),
    "linux_arm64": ("libadbc_driver_monetdb.so", ".so"),
    "macos_arm64": ("libadbc_driver_monetdb.dylib", ".so"),
    "windows_amd64": ("adbc_driver_monetdb.dll", ".pyd"),
}


@pytest.mark.parametrize(
    ("platform", "driver_name", "wheel_suffix"), [(platform, *values) for platform, values in PLATFORMS.items()]
)
def test_builds_flat_dbc_archive(
    tmp_path: Path,
    platform: str,
    driver_name: str,
    wheel_suffix: str,
) -> None:
    wheel = tmp_path / "driver.whl"
    license_path = tmp_path / "THIRD_PARTY_LICENSES"
    license_path.write_text("dependency licenses")
    with ZipFile(wheel, "w") as archive:
        archive.writestr(f"adbc_driver_monetdb/_native.abi3{wheel_suffix}", b"native")
        archive.writestr("driver.dist-info/licenses/LICENSE", b"MIT")
        archive.writestr("driver.dist-info/licenses/NOTICE", b"notice")
        archive.writestr("driver.dist-info/licenses/monetdb-rust/LICENSE", b"MPL")

    subprocess.run(
        [
            sys.executable,
            str(BUILDER),
            "--wheel",
            str(wheel),
            "--platform",
            platform,
            "--out-dir",
            str(tmp_path / "out"),
            "--license",
            str(license_path),
        ],
        check=True,
    )

    packages = list((tmp_path / "out").glob("*.tar.gz"))
    assert len(packages) == 1
    with tarfile.open(packages[0]) as archive:
        assert archive.getnames() == [
            "MANIFEST",
            driver_name,
            "LICENSE",
            "NOTICE",
            "LICENSE.monetdb-rust",
        ]
        manifest_file = archive.extractfile("MANIFEST")
        assert manifest_file is not None
        manifest = tomllib.loads(manifest_file.read().decode())
        license_file = archive.extractfile("LICENSE")
        assert license_file is not None
        assert license_file.read() == b"dependency licenses"
    assert manifest["description"] == "ADBC driver for MonetDB: Arrow-native reads and writes"
    assert manifest["url"] == "https://github.com/wlaur/adbc-driver-monetdb"
    assert manifest["Driver"]["entrypoint"] == "AdbcDriverMonetdbInit"
    assert manifest["Files"]["driver"] == driver_name
