from pathlib import Path

import pytest

from dbx_client import load_config


def test_load_config_reads_scan_and_paths(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\n"
        "min_file_size_bytes = 102400\n"
        "skip_shared_not_owned = true\n"
        "skip_hidden = true\n"
        "early_exit_row_threshold = 1000\n"
        "max_csv_rows = 100\n"
        "\n"
        "[paths]\n"
        "csv_output_dir = ./output\n"
        "log_dir = ./logs\n"
    )

    cfg = load_config(cfg_path)

    assert cfg.min_file_size_bytes == 102400
    assert cfg.skip_shared_not_owned is True
    assert cfg.skip_hidden is True
    assert cfg.early_exit_row_threshold == 1000
    assert cfg.max_csv_rows == 100
    assert cfg.csv_output_dir == Path("./output")
    assert cfg.log_dir == Path("./logs")


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope.ini"
    with pytest.raises(FileNotFoundError):
        load_config(missing)


from dbx_client import MissingTokenError, load_token


def test_load_token_returns_value(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DROPBOX_ACCESS_TOKEN=sl.test123\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)
    assert load_token(env_file) == "sl.test123"


def test_load_token_missing_raises_with_helpful_message(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("DROPBOX_ACCESS_TOKEN=\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("DROPBOX_ACCESS_TOKEN", raising=False)
    with pytest.raises(MissingTokenError) as excinfo:
        load_token(env_file)
    assert "DROPBOX_ACCESS_TOKEN" in str(excinfo.value)
    assert "README" in str(excinfo.value)
