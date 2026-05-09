from pathlib import Path
import sys
import types

from src.edu_cti_v2 import migrate


def test_detect_repo_root_prefers_current_working_directory(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo"
    (repo_root / "alembic").mkdir(parents=True)
    (repo_root / "alembic.ini").write_text("[alembic]\n")

    monkeypatch.chdir(repo_root)

    detected = migrate.detect_repo_root()

    assert detected == repo_root.resolve()


def test_build_alembic_config_uses_detected_repo_root(monkeypatch, tmp_path):
    repo_root = tmp_path / "repo"
    alembic_dir = repo_root / "alembic"
    alembic_dir.mkdir(parents=True)
    (repo_root / "alembic.ini").write_text("[alembic]\nscript_location = alembic\n")

    monkeypatch.setattr(migrate, "detect_repo_root", lambda: repo_root)
    fake_alembic = types.ModuleType("alembic")
    fake_config_module = types.ModuleType("alembic.config")

    class _FakeConfig:
        def __init__(self, config_file_name):
            self.config_file_name = config_file_name
            self.options = {}

        def set_main_option(self, key, value):
            self.options[key] = value

        def get_main_option(self, key):
            return self.options.get(key)

    fake_config_module.Config = _FakeConfig
    monkeypatch.setitem(sys.modules, "alembic", fake_alembic)
    monkeypatch.setitem(sys.modules, "alembic.config", fake_config_module)

    config = migrate.build_alembic_config()

    assert Path(config.config_file_name).resolve() == (repo_root / "alembic.ini").resolve()
    assert config.get_main_option("script_location") == str(alembic_dir)
