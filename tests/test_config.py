"""Tests for core/config.py — YAML loading, env var resolution, lazy email validation."""

from __future__ import annotations

import textwrap
from decimal import Decimal

import pytest

from portfolio_manager.core.config import load_config


def _write_config(tmp_path, content: str) -> str:
    """Write a YAML config to a temp file and return its path."""
    config_file = tmp_path / "portfolio.yml"
    config_file.write_text(textwrap.dedent(content))
    return str(config_file)


def test_load_basic_config(tmp_path):
    path = _write_config(
        tmp_path,
        """
        base_currency: USD
        target_allocations:
          AAPL.US: 0.15
          MSFT.US: 0.20
        rebalance_threshold: 0.05
        atr_period: 14
        atr_multiplier: 2.0
        """,
    )
    config = load_config(path)
    assert config.base_currency == "USD"
    assert config.target_allocations == {
        "AAPL.US": Decimal("0.15"),
        "MSFT.US": Decimal("0.20"),
    }
    assert config.rebalance_threshold == Decimal("0.05")
    assert config.atr_period == 14
    assert config.atr_multiplier == Decimal("2.0")


def test_env_var_resolution(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_SMTP_USER", "user@example.com")
    monkeypatch.setenv("TEST_SMTP_PASS", "s3cret")

    path = _write_config(
        tmp_path,
        """
        base_currency: USD
        target_allocations: {}
        rebalance_threshold: 0.05
        atr_period: 14
        atr_multiplier: 2.0
        email:
          smtp_host: smtp.gmail.com
          smtp_port: 465
          smtp_username: $TEST_SMTP_USER
          smtp_password: $TEST_SMTP_PASS
          from: $TEST_SMTP_USER
          to:
            - $TEST_SMTP_USER
        """,
    )
    config = load_config(path)
    assert config.smtp_username == "user@example.com"
    assert config.smtp_password == "s3cret"
    assert config.email_from == "user@example.com"
    assert config.email_to == ["user@example.com"]


def test_missing_env_var_returns_none(tmp_path, monkeypatch):
    """Unset env vars should result in None (not crash)."""
    # Ensure var is NOT set
    monkeypatch.delenv("NONEXISTENT_VAR_12345", raising=False)

    path = _write_config(
        tmp_path,
        """
        base_currency: USD
        target_allocations: {}
        rebalance_threshold: 0.05
        atr_period: 14
        atr_multiplier: 2.0
        email:
          smtp_host: smtp.gmail.com
          smtp_port: 465
          smtp_username: $NONEXISTENT_VAR_12345
          smtp_password: $NONEXISTENT_VAR_12345
        """,
    )
    config = load_config(path)
    assert config.smtp_username is None
    assert config.smtp_password is None


def test_lazy_email_validation_passes(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_EMAIL", "test@test.com")
    monkeypatch.setenv("TEST_PASS", "pass")

    path = _write_config(
        tmp_path,
        """
        base_currency: USD
        target_allocations: {}
        rebalance_threshold: 0.05
        atr_period: 14
        atr_multiplier: 2.0
        email:
          smtp_host: smtp.gmail.com
          smtp_port: 465
          smtp_username: $TEST_EMAIL
          smtp_password: $TEST_PASS
          from: $TEST_EMAIL
          to:
            - $TEST_EMAIL
        """,
    )
    config = load_config(path)
    # Should not raise
    config.validate_email_config()


def test_lazy_email_validation_fails(tmp_path):
    path = _write_config(
        tmp_path,
        """
        base_currency: USD
        target_allocations: {}
        rebalance_threshold: 0.05
        atr_period: 14
        atr_multiplier: 2.0
        """,
    )
    config = load_config(path)
    with pytest.raises(ValueError, match="Email configuration incomplete"):
        config.validate_email_config()


def test_config_file_not_found():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/path/config.yml")


def test_defaults(tmp_path):
    """Verify defaults for optional fields."""
    path = _write_config(
        tmp_path,
        """
        target_allocations: {}
        """,
    )
    config = load_config(path)
    assert config.base_currency == "USD"
    assert config.rebalance_threshold == Decimal("0.05")
    assert config.atr_period == 14
    assert config.atr_multiplier == Decimal("2.0")
    assert config.email_subject_template == "Portfolio Report — {date}"
