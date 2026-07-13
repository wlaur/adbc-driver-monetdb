"""ADBC driver for MonetDB."""

from importlib.metadata import PackageNotFoundError, version

from adbc_driver_monetdb import _native

try:
    __version__ = version("adbc-driver-monetdb")
except PackageNotFoundError:  # pragma: no cover - source checkout without an installed wheel
    __version__ = "0.0.0"

ENTRYPOINT = _native.__adbc_entrypoint__


def driver_path() -> str:
    """Filesystem path of the compiled ADBC driver shared library.

    The driver is shipped as the extension module ``adbc_driver_monetdb._native``;
    the ADBC driver manager loads that file by path.
    """
    file = getattr(_native, "__file__", None)
    if not isinstance(file, str):
        raise RuntimeError("compiled driver module has no filesystem path")
    return file


__all__ = ["ENTRYPOINT", "__version__", "driver_path"]
