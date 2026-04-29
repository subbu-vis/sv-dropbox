import configparser
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
