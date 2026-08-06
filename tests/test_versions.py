import tomllib
from importlib.metadata import version
from pathlib import Path
from typing import cast

import pytest

ROOT = Path(__file__).parents[1]


def _toml(path: Path) -> dict[str, object]:
    return tomllib.loads(path.read_text())


WORKSPACE_CRATES = ("adbc-monetdb", "monetdb-arrow")


def _locked_version(path: Path, name: str) -> str | None:
    packages = cast(list[dict[str, object]], _toml(path).get("package", []))
    for package in packages:
        if package.get("name") == name:
            return cast(str, package["version"])
    return None


def test_release_versions_stay_in_sync() -> None:
    project = cast(dict[str, object], _toml(ROOT / "pyproject.toml")["project"])
    expected = project["version"]
    assert isinstance(expected, str)
    workspace = cast(dict[str, object], _toml(ROOT / "Cargo.toml")["workspace"])
    package = cast(dict[str, object], workspace["package"])
    assert package["version"] == expected
    for manifest in (ROOT / "packaging" / "dbc").glob("MANIFEST.*.toml"):
        assert _toml(manifest)["version"] == expected
    assert version("adbc-driver-monetdb") == expected

    assert _locked_version(ROOT / "uv.lock", "adbc-driver-monetdb") == expected
    for crate in WORKSPACE_CRATES:
        assert _locked_version(ROOT / "Cargo.lock", crate) == expected


def test_dependency_license_roll_up_is_regenerated_for_the_current_version() -> None:
    # The roll-up embeds the workspace crate versions, so a version bump that does not
    # rerun packaging/generate_licenses.py fails CI several minutes in. Catch it here.
    project = cast(dict[str, object], _toml(ROOT / "pyproject.toml")["project"])
    expected = project["version"]
    licenses = (ROOT / "THIRD_PARTY_LICENSES").read_text()
    for crate in WORKSPACE_CRATES:
        assert f"- {crate} {expected} " in licenses, (
            f"THIRD_PARTY_LICENSES lists no {crate} {expected}; run packaging/generate_licenses.py"
        )


@pytest.mark.integration
def test_reported_driver_version_matches_package(monetdb_uri: str) -> None:
    from adbc_driver_monetdb import dbapi

    with dbapi.connect(monetdb_uri) as connection:
        assert connection.adbc_get_info()["driver_version"] == version("adbc-driver-monetdb")
