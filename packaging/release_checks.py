from __future__ import annotations

import argparse
import subprocess
import tomllib
from pathlib import Path


def _git(root: Path, *arguments: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=check,
        capture_output=True,
        text=True,
    )


def release_versions(root: Path) -> dict[str, str]:
    project = tomllib.loads((root / "pyproject.toml").read_text())["project"]["version"]
    cargo = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["package"]["version"]
    manifests = {
        path.name: tomllib.loads(path.read_text())["version"]
        for path in (root / "packaging" / "dbc").glob("MANIFEST.*.toml")
    }
    return {"pyproject.toml": project, "Cargo.toml": cargo, **manifests}


def verify_provenance(root: Path, tag: str, commit: str, main_ref: str) -> None:
    versions = release_versions(root)
    project = versions["pyproject.toml"]
    mismatches = {name: value for name, value in versions.items() if value != project}
    expected_tag = f"v{project}"
    if mismatches or tag != expected_tag:
        raise ValueError(f"release versions/tag disagree: versions={versions}, tag={tag!r}, expected={expected_tag!r}")
    tag_commit = _git(root, "rev-parse", f"{tag}^{{commit}}").stdout.strip()
    workflow_commit = _git(root, "rev-parse", f"{commit}^{{commit}}").stdout.strip()
    if tag_commit != workflow_commit:
        raise ValueError(f"tag {tag} peels to {tag_commit}, not workflow commit {workflow_commit}")
    ancestor = _git(root, "merge-base", "--is-ancestor", tag_commit, main_ref, check=False)
    if ancestor.returncode != 0:
        raise ValueError(f"release commit {tag_commit} is not reachable from protected {main_ref}")


def verify_publish_artifacts(dist: Path) -> None:
    wheels = list(dist.glob("adbc_driver_monetdb-*.whl"))
    sdists = list(dist.glob("adbc_driver_monetdb-*.tar.gz"))
    expected_platforms = ["x86_64.whl", "aarch64.whl", "arm64.whl", "win_amd64.whl"]
    missing = [
        platform for platform in expected_platforms if not any(wheel.name.endswith(platform) for wheel in wheels)
    ]
    if len(wheels) != 4 or missing:
        raise ValueError(f"expected four platform wheels, found {[path.name for path in wheels]}; missing={missing}")
    if len(sdists) != 1:
        raise ValueError(f"expected one sdist, found {[path.name for path in sdists]}")


def verify_github_artifacts(release: Path) -> None:
    verify_publish_artifacts(release / "python")
    dbc = list((release / "dbc").glob("monetdb_*_v*.tar.gz"))
    documentation = list((release / "docs").glob("monetdb.md"))
    if len(dbc) != 4:
        raise ValueError(f"expected four dbc archives, found {[path.name for path in dbc]}")
    if len(documentation) != 1:
        raise ValueError("expected exactly one generated MonetDB validation document")


def main() -> None:
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest="command", required=True)
    provenance = subparsers.add_parser("provenance")
    provenance.add_argument("--root", type=Path, default=Path.cwd())
    provenance.add_argument("--tag", required=True)
    provenance.add_argument("--commit", required=True)
    provenance.add_argument("--main-ref", default="origin/main")
    publish = subparsers.add_parser("publish-artifacts")
    publish.add_argument("path", type=Path)
    github = subparsers.add_parser("github-artifacts")
    github.add_argument("path", type=Path)
    args = parser.parse_args()
    if args.command == "provenance":
        verify_provenance(args.root, args.tag, args.commit, args.main_ref)
    elif args.command == "publish-artifacts":
        verify_publish_artifacts(args.path)
    else:
        verify_github_artifacts(args.path)


if __name__ == "__main__":
    main()
