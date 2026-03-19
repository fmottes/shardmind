"""Configuration and project path helpers."""

from __future__ import annotations

import os
import platform
from dataclasses import dataclass
from pathlib import Path


def find_project_root(start: Path | None = None) -> Path:
    current = (start or Path(__file__)).resolve()
    for candidate in [current, *current.parents]:
        if (candidate / "pyproject.toml").exists():
            return candidate
    raise RuntimeError("Could not locate project root from pyproject.toml.")


def bundled_shared_path() -> Path:
    return Path(__file__).resolve().parent / "_bundled" / "shared"


def resolve_shared_path(project_root: Path | None) -> Path:
    override = os.environ.get("SHARDMIND_SHARED_PATH")
    if override:
        return Path(override)
    if project_root is not None:
        repo_shared = project_root / "shared"
        if repo_shared.exists():
            return repo_shared
    bundled = bundled_shared_path()
    if bundled.exists():
        return bundled
    if project_root is not None:
        return project_root / "shared"
    raise RuntimeError("Could not locate shared runtime assets.")


def default_vault_path(home: Path | None = None) -> Path:
    return (home or Path.home()) / "Documents" / "ShardMind"


def default_state_dir(home: Path | None = None) -> Path:
    base_home = home or Path.home()
    system = platform.system()
    if system == "Darwin":
        return base_home / "Library" / "Application Support" / "shardmind"
    if system == "Windows":
        appdata = os.environ.get("APPDATA")
        if appdata:
            return Path(appdata) / "shardmind"
        return base_home / "AppData" / "Roaming" / "shardmind"
    xdg_state_home = os.environ.get("XDG_STATE_HOME")
    if xdg_state_home:
        return Path(xdg_state_home) / "shardmind"
    return base_home / ".local" / "state" / "shardmind"


@dataclass(slots=True)
class Settings:
    project_root: Path | None
    vault_path: Path
    sqlite_path: Path
    shared_path: Path
    default_note_destination: str = "inbox"
    embedding_backend: str = "stub"

    @classmethod
    def load(cls) -> Settings:
        try:
            project_root = find_project_root()
        except RuntimeError:
            project_root = None
        vault_path = Path(os.environ.get("SHARDMIND_VAULT_PATH", default_vault_path()))
        state_dir = default_state_dir()
        sqlite_path = Path(
            os.environ.get(
                "SHARDMIND_SQLITE_PATH",
                state_dir / "shardmind.sqlite3",
            )
        )
        shared_path = resolve_shared_path(project_root)
        default_note_destination = os.environ.get("SHARDMIND_DEFAULT_NOTE_DESTINATION", "inbox")
        embedding_backend = os.environ.get("SHARDMIND_EMBEDDING_BACKEND", "stub")
        return cls(
            project_root=project_root,
            vault_path=vault_path,
            sqlite_path=sqlite_path,
            shared_path=shared_path,
            default_note_destination=default_note_destination,
            embedding_backend=embedding_backend,
        )
