"""ADBC driver for MonetDB."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("adbc-driver-monetdb")
except PackageNotFoundError:  # pragma: no cover - source checkout without an installed wheel
    __version__ = "0.0.0"

ENTRYPOINT = "AdbcDriverMonetdbInit"


def driver_path() -> str:
    """Filesystem path of the compiled ADBC driver shared library.

    The driver is shipped as the extension module ``adbc_driver_monetdb._native``;
    the ADBC driver manager loads that file by path.
    """
    from adbc_driver_monetdb import _native

    file = _native.__file__
    assert file is not None
    return file


__all__ = ["ENTRYPOINT", "__version__", "driver_path"]
