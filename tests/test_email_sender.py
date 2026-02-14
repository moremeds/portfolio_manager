"""Tests for reporting/email_sender.py — SMTP delivery with mocked server."""

from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

from portfolio_manager.core.types import PortfolioConfig
from portfolio_manager.reporting.email_sender import send_report


def _make_config(**overrides: object) -> PortfolioConfig:
    defaults = {
        "base_currency": "USD",
        "target_allocations": {},
        "rebalance_threshold": Decimal("0.05"),
        "atr_period": 14,
        "atr_multiplier": Decimal("2.0"),
        "smtp_host": "smtp.gmail.com",
        "smtp_port": 465,
        "smtp_username": "user@gmail.com",
        "smtp_password": "app-password",
        "email_from": "user@gmail.com",
        "email_to": ["recipient@example.com"],
        "email_subject_template": "Portfolio Report — {date}",
    }
    defaults.update(overrides)
    return PortfolioConfig(**defaults)  # type: ignore[arg-type]


def test_send_report_ssl():
    """Should use SMTP_SSL for port 465."""
    config = _make_config(smtp_port=465)

    with patch("portfolio_manager.reporting.email_sender.smtplib.SMTP_SSL") as mock_ssl:
        mock_server = MagicMock()
        mock_ssl.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_ssl.return_value.__exit__ = MagicMock(return_value=False)

        send_report("<h1>Test</h1>", config, report_date=datetime.date(2026, 2, 14))

        mock_ssl.assert_called_once_with("smtp.gmail.com", 465)
        mock_server.login.assert_called_once_with("user@gmail.com", "app-password")
        mock_server.sendmail.assert_called_once()

        # Verify MIME structure
        args = mock_server.sendmail.call_args
        assert args[0][0] == "user@gmail.com"
        assert args[0][1] == ["recipient@example.com"]
        # Subject contains "Portfolio Report" (may be MIME-encoded due to em-dash)
        msg_str = args[0][2]
        assert "Portfolio" in msg_str or "Portfolio_Report" in msg_str


def test_send_report_starttls():
    """Should use STARTTLS for port 587."""
    config = _make_config(smtp_port=587)

    with patch("portfolio_manager.reporting.email_sender.smtplib.SMTP") as mock_smtp:
        mock_server = MagicMock()
        mock_smtp.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

        send_report("<h1>Test</h1>", config, report_date=datetime.date(2026, 2, 14))

        mock_smtp.assert_called_once_with("smtp.gmail.com", 587)
        mock_server.starttls.assert_called_once()
        mock_server.login.assert_called_once()


def test_send_report_validates_config():
    """Should raise ValueError if email config is incomplete."""
    config = _make_config(smtp_username=None)

    with pytest.raises(ValueError, match="Email configuration incomplete"):
        send_report("<h1>Test</h1>", config)


def test_send_report_subject_template():
    """Subject should use the template from config."""
    config = _make_config(email_subject_template="Daily Report - {date}")

    with patch("portfolio_manager.reporting.email_sender.smtplib.SMTP_SSL") as mock_ssl:
        mock_server = MagicMock()
        mock_ssl.return_value.__enter__ = MagicMock(return_value=mock_server)
        mock_ssl.return_value.__exit__ = MagicMock(return_value=False)

        send_report("<h1>Test</h1>", config, report_date=datetime.date(2026, 2, 14))

        msg_str = mock_server.sendmail.call_args[0][2]
        assert "Daily Report - Feb 14, 2026" in msg_str
