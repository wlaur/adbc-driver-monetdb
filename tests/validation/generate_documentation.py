import argparse
from pathlib import Path

from adbc_drivers_validation import generate_documentation  # pyright: ignore[reportMissingTypeStubs]

from .monetdb import MonetdbQuirks, get_quirks


def _get_quirks(version: str, *, vendor: str) -> MonetdbQuirks:
    del version, vendor
    return get_quirks()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--report", action="append", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).parents[2]
    generate_documentation.generate(
        "monetdb",
        _get_quirks,
        [("monetdb", "MonetDB")],
        [report.resolve() for report in args.report],
        (root / "docs" / "monetdb.md").resolve(),
        args.output.resolve(),
    )


if __name__ == "__main__":
    main()
