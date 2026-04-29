from pathlib import Path
from unittest.mock import MagicMock

import pytest

from dropbox.exceptions import AuthError, RateLimitError

from dbx_client import load_config, MissingTokenError, load_token, with_retry


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
