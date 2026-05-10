from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dropbox.exceptions import AuthError, RateLimitError

from dbx_client import load_config, MissingTokenError, load_token, with_retry, MediaConfig, load_media_config


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
    assert cfg.ignored_folders == ()


def test_load_config_parses_multiline_ignored_folders(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\n"
        "min_file_size_bytes = 102400\n"
        "skip_shared_not_owned = true\n"
        "skip_hidden = true\n"
        "early_exit_row_threshold = 1000\n"
        "max_csv_rows = 100\n"
        "ignored_folders =\n"
        "    /Old Backups\n"
        "    /Photos/raw/\n"
        "    NoLeadingSlash\n"
        "\n"
        "[paths]\n"
        "csv_output_dir = ./output\n"
        "log_dir = ./logs\n"
    )

    cfg = load_config(cfg_path)

    # Normalized: leading slash, no trailing slash, lowercased.
    assert cfg.ignored_folders == ("/old backups", "/photos/raw", "/noleadingslash")


def test_load_config_missing_file_raises(tmp_path: Path) -> None:
    missing = tmp_path / "nope.ini"
    with pytest.raises(FileNotFoundError):
        load_config(missing)


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


def _rate_limit_error(backoff: float) -> RateLimitError:
    """RateLimitError(request_id, error, backoff) — backoff is seconds to wait."""
    return RateLimitError("req-id", MagicMock(), backoff)


def test_with_retry_retries_on_rate_limit(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr("dbx_client.time.sleep", lambda s: sleep_calls.append(s))

    call = MagicMock()
    call.side_effect = [_rate_limit_error(2.0), "ok"]
    result = with_retry(call)
    assert result == "ok"
    assert sleep_calls == [2.0]


def test_with_retry_gives_up_after_max_attempts(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr("dbx_client.time.sleep", lambda s: sleep_calls.append(s))
    call = MagicMock()
    call.side_effect = _rate_limit_error(1.0)
    with pytest.raises(RateLimitError):
        with_retry(call, max_attempts=3)
    assert call.call_count == 3
    # 3 attempts -> 2 sleeps between them; no sleep after the final failing attempt.
    assert sleep_calls == [1, 1]


def test_with_retry_does_not_retry_auth_error() -> None:
    call = MagicMock(side_effect=AuthError("req-id", "user-message"))
    with pytest.raises(AuthError):
        with_retry(call)
    assert call.call_count == 1


def test_with_retry_max_attempts_one_attempts_once_no_sleep(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr("dbx_client.time.sleep", lambda s: sleep_calls.append(s))
    call = MagicMock(side_effect=_rate_limit_error(5.0))
    with pytest.raises(RateLimitError):
        with_retry(call, max_attempts=1)
    assert call.call_count == 1
    assert sleep_calls == []


def test_with_retry_max_attempts_zero_raises_value_error() -> None:
    with pytest.raises(ValueError, match="max_attempts must be >= 1"):
        with_retry(MagicMock(), max_attempts=0)


def test_with_retry_handles_none_backoff(monkeypatch: pytest.MonkeyPatch) -> None:
    sleep_calls: list[float] = []
    monkeypatch.setattr("dbx_client.time.sleep", lambda s: sleep_calls.append(s))
    call = MagicMock()
    call.side_effect = [RateLimitError("req-id", MagicMock(), None), "ok"]
    assert with_retry(call) == "ok"
    assert sleep_calls == [1]


def test_load_media_config_happy_path(tmp_path: Path) -> None:
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
        "\n"
        "[media]\n"
        "photo_extensions = jpg,jpeg,png\n"
        "video_extensions = mp4,mov\n"
        "batch_size = 50\n"
        "thumbnail_width = 480\n"
        "tag_archive_path = ./output/tag-archive.json\n"
        "ignored_folders =\n"
        "    /Old Backups\n"
        "    /Screenshots/\n"
    )

    mc = load_media_config(cfg_path)

    assert mc.photo_extensions == frozenset({"jpg", "jpeg", "png"})
    assert mc.video_extensions == frozenset({"mp4", "mov"})
    assert mc.batch_size == 50
    assert mc.thumbnail_width == 480
    assert mc.tag_archive_path == Path("./output/tag-archive.json")
    assert mc.csv_output_dir == Path("./output")
    assert mc.log_dir == Path("./logs")
    assert mc.ignored_folders == ("/old backups", "/screenshots")


def test_load_media_config_rejects_bad_thumbnail_width(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\n\n"
        "[paths]\ncsv_output_dir=./o\nlog_dir=./l\n\n"
        "[media]\nphoto_extensions=jpg\nvideo_extensions=mp4\nbatch_size=10\n"
        "thumbnail_width=333\ntag_archive_path=./a.json\nignored_folders=\n"
    )
    with pytest.raises(ValueError, match="thumbnail_width must be one of"):
        load_media_config(cfg_path)


def test_load_media_config_rejects_zero_batch_size(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\n\n"
        "[paths]\ncsv_output_dir=./o\nlog_dir=./l\n\n"
        "[media]\nphoto_extensions=jpg\nvideo_extensions=mp4\nbatch_size=0\n"
        "thumbnail_width=480\ntag_archive_path=./a.json\nignored_folders=\n"
    )
    with pytest.raises(ValueError, match="batch_size must be positive"):
        load_media_config(cfg_path)


def test_load_media_config_rejects_empty_extension_list(tmp_path: Path) -> None:
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\n\n"
        "[paths]\ncsv_output_dir=./o\nlog_dir=./l\n\n"
        "[media]\nphoto_extensions=\nvideo_extensions=mp4\nbatch_size=10\n"
        "thumbnail_width=480\ntag_archive_path=./a.json\nignored_folders=\n"
    )
    with pytest.raises(ValueError, match="photo_extensions must not be empty"):
        load_media_config(cfg_path)


def test_load_media_config_independent_ignored_folders(tmp_path: Path) -> None:
    """[media].ignored_folders and [scan].ignored_folders parsed independently."""
    cfg_path = tmp_path / "config.ini"
    cfg_path.write_text(
        "[scan]\nmin_file_size_bytes=1\nskip_shared_not_owned=true\nskip_hidden=true\n"
        "early_exit_row_threshold=1\nmax_csv_rows=1\n"
        "ignored_folders =\n    /scan-only\n\n"
        "[paths]\ncsv_output_dir=./o\nlog_dir=./l\n\n"
        "[media]\nphoto_extensions=jpg\nvideo_extensions=mp4\nbatch_size=10\n"
        "thumbnail_width=480\ntag_archive_path=./a.json\n"
        "ignored_folders =\n    /media-only\n"
    )
    scan_cfg = load_config(cfg_path)
    media_cfg = load_media_config(cfg_path)
    assert scan_cfg.ignored_folders == ("/scan-only",)
    assert media_cfg.ignored_folders == ("/media-only",)
