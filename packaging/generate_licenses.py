from __future__ import annotations

import subprocess
from pathlib import Path

TARGETS = (
    "aarch64-apple-darwin",
    "aarch64-unknown-linux-gnu",
    "x86_64-pc-windows-msvc",
    "x86_64-unknown-linux-gnu",
)


def main() -> None:
    command = ["cargo", "about", "generate", "--all-features", "--fail", "--locked"]
    for target in TARGETS:
        command.extend(["--target", target])
    command.append("license.tpl")
    generated = subprocess.run(command, check=True, stdout=subprocess.PIPE, text=True).stdout
    normalized = "\n".join(line.rstrip() for line in generated.splitlines()).rstrip() + "\n"
    Path("THIRD_PARTY_LICENSES").write_text(normalized)


if __name__ == "__main__":
    main()
