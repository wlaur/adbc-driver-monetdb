import subprocess
import sys
from pathlib import Path

RELEASE_CHECKS = Path(__file__).parents[1] / "packaging" / "release_checks.py"


def _git(root: Path, *arguments: str) -> str:
    return subprocess.run(
        ["git", *arguments],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def _repository(tmp_path: Path, version: str = "1.2.3") -> tuple[Path, str]:
    root = tmp_path / "repository"
    (root / "packaging" / "dbc").mkdir(parents=True)
    (root / "pyproject.toml").write_text(f'[project]\nversion = "{version}"\n')
    (root / "Cargo.toml").write_text(f'[workspace.package]\nversion = "{version}"\n')
    (root / "uv.lock").write_text(f'[[package]]\nname = "adbc-driver-monetdb"\nversion = "{version}"\n')
    for platform in ["linux", "macos", "windows"]:
        (root / "packaging" / "dbc" / f"MANIFEST.{platform}.toml").write_text(f'version = "{version}"\n')
    _git(root, "init", "-b", "main")
    _git(root, "config", "user.email", "release-tests@example.invalid")
    _git(root, "config", "user.name", "Release Tests")
    _git(root, "add", ".")
    _git(root, "commit", "-m", "release")
    return root, _git(root, "rev-parse", "HEAD")


def _verify(root: Path, tag: str, commit: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(RELEASE_CHECKS),
            "provenance",
            "--root",
            str(root),
            "--tag",
            tag,
            "--commit",
            commit,
            "--main-ref",
            "main",
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _publish_artifacts(path: Path, version: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(RELEASE_CHECKS),
            "publish-artifacts",
            str(path),
            "--version",
            version,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _write_python_artifacts(path: Path, version: str) -> None:
    path.mkdir(parents=True)
    for platform in [
        "manylinux_2_28_x86_64",
        "manylinux_2_28_aarch64",
        "macosx_11_0_arm64",
        "win_amd64",
    ]:
        (path / f"adbc_driver_monetdb-{version}-cp313-abi3-{platform}.whl").touch()
    (path / f"adbc_driver_monetdb-{version}.tar.gz").touch()


def _github_artifacts(path: Path, version: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            str(RELEASE_CHECKS),
            "github-artifacts",
            str(path),
            "--version",
            version,
        ],
        check=False,
        capture_output=True,
        text=True,
    )


def _write_github_artifacts(path: Path, version: str) -> None:
    _write_python_artifacts(path / "python", version)
    (path / "dbc").mkdir()
    for platform in ["linux_amd64", "linux_arm64", "macos_arm64", "windows_amd64"]:
        (path / "dbc" / f"monetdb_{platform}_v{version}.tar.gz").write_bytes(platform.encode())
    (path / "docs").mkdir()
    (path / "docs" / "monetdb.md").write_text("validation")


def test_accepts_valid_merged_annotated_tag(tmp_path: Path) -> None:
    root, commit = _repository(tmp_path)
    _git(root, "tag", "-a", "v1.2.3", "-m", "release")
    assert _verify(root, "v1.2.3", commit).returncode == 0


def test_rejects_unmerged_release_commit(tmp_path: Path) -> None:
    root, _ = _repository(tmp_path)
    _git(root, "switch", "-c", "unmerged")
    (root / "unmerged").write_text("unmerged")
    _git(root, "add", "unmerged")
    _git(root, "commit", "-m", "unmerged")
    commit = _git(root, "rev-parse", "HEAD")
    _git(root, "tag", "v1.2.3")
    rejected = _verify(root, "v1.2.3", commit)
    assert rejected.returncode != 0
    assert "not reachable" in rejected.stderr


def test_rejects_mismatched_version_or_tag(tmp_path: Path) -> None:
    root, commit = _repository(tmp_path)
    _git(root, "tag", "v1.2.3")
    manifest = root / "packaging" / "dbc" / "MANIFEST.linux.toml"
    manifest.write_text('version = "9.9.9"\n')
    rejected = _verify(root, "v1.2.3", commit)
    assert rejected.returncode != 0
    assert "versions/tag disagree" in rejected.stderr


def test_rejects_mismatched_lockfile_version(tmp_path: Path) -> None:
    root, commit = _repository(tmp_path)
    _git(root, "tag", "v1.2.3")
    (root / "uv.lock").write_text('[[package]]\nname = "adbc-driver-monetdb"\nversion = "9.9.9"\n')
    rejected = _verify(root, "v1.2.3", commit)
    assert rejected.returncode != 0
    assert "versions/tag disagree" in rejected.stderr


def test_rejects_tag_that_does_not_point_to_workflow_commit(tmp_path: Path) -> None:
    root, tagged = _repository(tmp_path)
    _git(root, "tag", "v1.2.3")
    (root / "later").write_text("later")
    _git(root, "add", "later")
    _git(root, "commit", "-m", "later")
    rejected = _verify(root, "v1.2.3", _git(root, "rev-parse", "HEAD"))
    assert rejected.returncode != 0
    assert "not workflow commit" in rejected.stderr
    assert tagged != _git(root, "rev-parse", "HEAD")


def test_publish_artifacts_require_the_release_version(tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    _write_python_artifacts(dist, "0.7.0")
    assert _publish_artifacts(dist, "0.7.0").returncode == 0
    rejected = _publish_artifacts(dist, "0.8.0")
    assert rejected.returncode != 0
    assert "expected four platform wheels and one sdist" in rejected.stderr
    (dist / "unrelated.whl").touch()
    rejected = _publish_artifacts(dist, "0.7.0")
    assert rejected.returncode != 0
    assert "unrelated.whl" in rejected.stderr


def test_github_artifacts_require_complete_matching_checksums(tmp_path: Path) -> None:
    release = tmp_path / "release"
    _write_github_artifacts(release, "0.8.0")
    generated = subprocess.run(
        [sys.executable, str(RELEASE_CHECKS), "github-checksums", str(release)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert generated.returncode == 0
    assert all("/" not in line.split("  ", 1)[1] for line in (release / "SHA256SUMS").read_text().splitlines())
    assert _github_artifacts(release, "0.8.0").returncode == 0
    (release / "docs" / "monetdb.md").write_text("tampered")
    rejected = _github_artifacts(release, "0.8.0")
    assert rejected.returncode != 0
    assert "checksum mismatch" in rejected.stderr


def test_github_artifacts_reject_stray_release_assets(tmp_path: Path) -> None:
    release = tmp_path / "release"
    _write_github_artifacts(release, "0.8.0")
    (release / "dbc" / "old-release.tar.gz").touch()
    subprocess.run(
        [sys.executable, str(RELEASE_CHECKS), "github-checksums", str(release)],
        check=True,
    )
    rejected = _github_artifacts(release, "0.8.0")
    assert rejected.returncode != 0
    assert "dbc artifact set mismatch" in rejected.stderr
