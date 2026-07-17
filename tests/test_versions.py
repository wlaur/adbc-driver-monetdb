import tomllib
from importlib.metadata import version
from pathlib import Path
from typing import cast

import pytest

ROOT = Path(__file__).parents[1]


def _toml(path: Path) -> dict[str, object]:
    return tomllib.loads(path.read_text())


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


@pytest.mark.integration
def test_reported_driver_version_matches_package(monetdb_uri: str) -> None:
    from adbc_driver_monetdb import dbapi

    with dbapi.connect(monetdb_uri) as connection:
        assert connection.adbc_get_info()["driver_version"] == version("adbc-driver-monetdb")
