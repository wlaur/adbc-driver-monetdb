"""PEP 249 (DB-API 2.0) interface to MonetDB, backed by adbc_driver_manager."""

import adbc_driver_manager.dbapi

from adbc_driver_monetdb import ENTRYPOINT, driver_path


def connect(
    uri: str,
    /,
    *,
    autocommit: bool = False,
    db_kwargs: dict[str, str] | None = None,
    conn_kwargs: dict[str, str] | None = None,
) -> adbc_driver_manager.dbapi.Connection:
    """Connect to MonetDB.

    ``uri`` is a ``monetdb://`` or ``monetdbs://`` URL, e.g.
    ``monetdb://user:password@localhost:50000/database``.
    """
    return adbc_driver_manager.dbapi.connect(
        driver=driver_path(),
        entrypoint=ENTRYPOINT,
        db_kwargs={**(db_kwargs or {}), "uri": uri},
        conn_kwargs=conn_kwargs,
        autocommit=autocommit,
    )


__all__ = ["connect"]
