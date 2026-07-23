from __future__ import annotations

import argparse
import hashlib
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
    locked = [
        package["version"]
        for package in tomllib.loads((root / "uv.lock").read_text())["package"]
        if package["name"] == "adbc-driver-monetdb"
    ]
    if len(locked) != 1:
        raise ValueError(f"expected one adbc-driver-monetdb package in uv.lock, found {locked}")
    manifest_paths = list((root / "packaging" / "dbc").glob("MANIFEST.*.toml"))
    expected_manifests = {f"MANIFEST.{platform}.toml" for platform in ("linux", "macos", "windows")}
    found_manifests = {path.name for path in manifest_paths}
    if found_manifests != expected_manifests:
        raise ValueError(
            f"dbc manifest set mismatch: found={sorted(found_manifests)}, expected={sorted(expected_manifests)}"
        )
    manifests = {path.name: tomllib.loads(path.read_text())["version"] for path in manifest_paths}
    return {"pyproject.toml": project, "Cargo.toml": cargo, "uv.lock": locked[0], **manifests}


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


def _wheel_platform(tag: str) -> str:
    if tag.startswith("manylinux") and tag.endswith("_x86_64"):
        return "linux_x86_64"
    if tag.startswith("manylinux") and tag.endswith("_aarch64"):
        return "linux_aarch64"
    if tag == "macosx_11_0_arm64":
        return "macos_arm64"
    if tag == "win_amd64":
        return "windows_amd64"
    return f"unexpected:{tag}"


def verify_publish_artifacts(dist: Path, version: str) -> None:
    distribution = f"adbc_driver_monetdb-{version}"
    wheel_prefix = f"{distribution}-cp313-abi3-"
    artifacts = list(dist.iterdir())
    wheels = [
        path.name
        for path in artifacts
        if path.is_file() and path.name.startswith(wheel_prefix) and path.suffix == ".whl"
    ]
    platform_tags = [wheel.removeprefix(wheel_prefix).removesuffix(".whl") for wheel in wheels]
    platform_set = {_wheel_platform(tag) for tag in platform_tags}
    expected_platforms = {"linux_x86_64", "linux_aarch64", "macos_arm64", "windows_amd64"}
    expected_names = set(wheels) | {f"{distribution}.tar.gz"}
    found_names = {path.name for path in artifacts}
    if (
        any(not path.is_file() for path in artifacts)
        or len(wheels) != 4
        or platform_set != expected_platforms
        or found_names != expected_names
    ):
        raise ValueError(
            "expected four platform wheels and one sdist; "
            f"found={sorted(found_names)}, platforms={sorted(platform_set)}, "
            f"missing_platforms={sorted(expected_platforms - platform_set)}, "
            f"unexpected={sorted(found_names - expected_names)}"
        )


def github_artifact_paths(release: Path) -> list[Path]:
    return [
        path for directory in ("python", "dbc", "docs") for path in (release / directory).iterdir() if path.is_file()
    ]


def write_github_checksums(release: Path) -> None:
    artifacts = github_artifact_paths(release)
    names = [path.name for path in artifacts]
    if len(names) != len(set(names)):
        raise ValueError(f"GitHub release asset basenames are not unique: {names}")
    lines = [f"{hashlib.sha256(path.read_bytes()).hexdigest()}  {path.name}\n" for path in artifacts]
    (release / "SHA256SUMS").write_text("".join(sorted(lines)))


def verify_github_artifacts(release: Path, version: str) -> None:
    verify_publish_artifacts(release / "python", version)
    expected_dbc = {
        f"monetdb_{platform}_v{version}.tar.gz"
        for platform in ("linux_amd64", "linux_arm64", "macos_arm64", "windows_amd64")
    }
    dbc_artifacts = list((release / "dbc").iterdir())
    found_dbc = {path.name for path in dbc_artifacts}
    if any(not path.is_file() for path in dbc_artifacts) or found_dbc != expected_dbc:
        raise ValueError(f"dbc artifact set mismatch: found={sorted(found_dbc)}, expected={sorted(expected_dbc)}")
    documentation_artifacts = list((release / "docs").iterdir())
    documentation = {path.name for path in documentation_artifacts}
    if any(not path.is_file() for path in documentation_artifacts) or documentation != {"monetdb.md"}:
        raise ValueError(f"documentation artifact set mismatch: found={sorted(documentation)}")
    artifacts = github_artifact_paths(release)
    expected = {path.name: path for path in artifacts}
    if len(expected) != len(artifacts):
        raise ValueError("GitHub release asset basenames are not unique")
    checksums = release / "SHA256SUMS"
    if not checksums.is_file():
        raise ValueError("release is missing SHA256SUMS")
    found: set[str] = set()
    for line in checksums.read_text().splitlines():
        digest, separator, relative = line.partition("  ")
        if separator != "  " or len(digest) != 64 or any(character not in "0123456789abcdef" for character in digest):
            raise ValueError(f"invalid SHA256SUMS line: {line!r}")
        if relative not in expected or relative in found:
            raise ValueError(f"unexpected or duplicate SHA256SUMS asset: {relative!r}")
        found.add(relative)
        actual = hashlib.sha256(expected[relative].read_bytes()).hexdigest()
        if actual != digest:
            raise ValueError(f"checksum mismatch for {relative}")
    expected_names = set(expected)
    if found != expected_names:
        raise ValueError(f"SHA256SUMS does not cover the release artifacts: missing={sorted(expected_names - found)}")


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
    publish.add_argument("--version", required=True)
    github = subparsers.add_parser("github-artifacts")
    github.add_argument("path", type=Path)
    github.add_argument("--version", required=True)
    checksums = subparsers.add_parser("github-checksums")
    checksums.add_argument("path", type=Path)
    args = parser.parse_args()
    if args.command == "provenance":
        verify_provenance(args.root, args.tag, args.commit, args.main_ref)
    elif args.command == "publish-artifacts":
        verify_publish_artifacts(args.path, args.version)
    elif args.command == "github-checksums":
        write_github_checksums(args.path)
    else:
        verify_github_artifacts(args.path, args.version)


if __name__ == "__main__":
    main()
