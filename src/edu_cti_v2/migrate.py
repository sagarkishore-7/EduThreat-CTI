"""Programmatic Alembic runner for the Postgres-backed v2 schema."""

from __future__ import annotations

import argparse
from pathlib import Path

from src.edu_cti_v2.db import V2DatabaseSettings


def detect_repo_root() -> Path:
    """Locate the deployed/app repo root that contains Alembic assets."""
    candidates = [Path.cwd(), *Path(__file__).resolve().parents]
    seen: set[Path] = set()
    for candidate in candidates:
        candidate = candidate.resolve()
        if candidate in seen:
            continue
        seen.add(candidate)
        if (candidate / "alembic.ini").exists() and (candidate / "alembic").is_dir():
            return candidate
    raise FileNotFoundError("Could not locate repo root containing alembic.ini and alembic/")


def build_alembic_config():
    from alembic.config import Config

    repo_root = detect_repo_root()
    config = Config(str(repo_root / "alembic.ini"))
    config.set_main_option("script_location", str(repo_root / "alembic"))
    config.set_main_option(
        "prepend_sys_path",
        str(repo_root),
    )
    settings = V2DatabaseSettings.from_env()
    config.set_main_option("sqlalchemy.url", settings.alembic_database_url)
    return config


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run Alembic commands for EduThreat-CTI v2")
    parser.add_argument(
        "command",
        nargs="?",
        default="upgrade",
        choices=("upgrade", "downgrade", "current", "history", "stamp"),
        help="Alembic command to run",
    )
    parser.add_argument(
        "revision",
        nargs="?",
        default="head",
        help="Target revision for upgrade/downgrade/stamp",
    )
    return parser


def main() -> None:
    from alembic import command

    args = build_parser().parse_args()
    config = build_alembic_config()

    if args.command == "upgrade":
        command.upgrade(config, args.revision)
    elif args.command == "downgrade":
        command.downgrade(config, args.revision)
    elif args.command == "current":
        command.current(config)
    elif args.command == "history":
        command.history(config)
    elif args.command == "stamp":
        command.stamp(config, args.revision)
    else:
        raise ValueError(f"Unsupported Alembic command: {args.command}")


if __name__ == "__main__":
    main()
