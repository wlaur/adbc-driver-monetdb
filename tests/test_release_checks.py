import subprocess
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
            "python",
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
