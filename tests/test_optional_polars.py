import subprocess
import sys
from textwrap import dedent


def test_base_package_imports_without_polars_or_pyarrow() -> None:
    script = dedent(
        """
        import builtins

        original_import = builtins.__import__

        def import_without_polars(name, *args, **kwargs):
            if name.partition(".")[0] in {"polars", "pyarrow"}:
                raise ModuleNotFoundError(name=name)
            return original_import(name, *args, **kwargs)

        builtins.__import__ = import_without_polars

        import adbc_driver_monetdb

        assert adbc_driver_monetdb.driver_path()
        try:
            adbc_driver_monetdb.PolarsArrowStream(None)
        except ModuleNotFoundError as exc:
            assert "adbc-driver-monetdb[polars]" in str(exc)
        else:
            raise AssertionError("constructing PolarsArrowStream should require the optional extra")
        """
    )

    subprocess.run([sys.executable, "-c", script], check=True)
