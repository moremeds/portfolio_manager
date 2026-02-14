"""SMTP email delivery for portfolio reports."""

from __future__ import annotations

import datetime
import logging
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from portfolio_manager.core.types import PortfolioConfig

logger = logging.getLogger(__name__)


def send_report(
    html_content: str,
    config: PortfolioConfig,
    report_date: datetime.date | None = None,
) -> None:
    """Send an HTML report via SMTP.

    Validates email config before sending. Supports both SMTP_SSL (port 465)
    and STARTTLS (port 587).

    Args:
        html_content: Complete HTML string to send as email body.
        config: Portfolio config with email settings.
        report_date: Date for the email subject. Defaults to today.

    Raises:
        ValueError: If email config is incomplete.
        smtplib.SMTPException: If email delivery fails.
    """
    # Lazy validation — only check when actually sending
    config.validate_email_config()

    if report_date is None:
        report_date = datetime.date.today()

    assert config.email_from is not None
    assert config.email_to is not None
    assert config.smtp_host is not None
    assert config.smtp_port is not None
    assert config.smtp_username is not None
    assert config.smtp_password is not None

    subject = (config.email_subject_template or "Portfolio Report — {date}").format(
        date=report_date.strftime("%b %d, %Y"),
    )

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = config.email_from
    msg["To"] = ", ".join(config.email_to)

    # Attach HTML body
    msg.attach(MIMEText(html_content, "html"))

    # Send via SMTP
    logger.info("Sending report to %s via %s:%d", config.email_to, config.smtp_host, config.smtp_port)

    if config.smtp_port == 465:
        # Implicit SSL
        with smtplib.SMTP_SSL(config.smtp_host, config.smtp_port) as server:
            server.login(config.smtp_username, config.smtp_password)
            server.sendmail(config.email_from, config.email_to, msg.as_string())
    else:
        # STARTTLS (port 587 or other)
        with smtplib.SMTP(config.smtp_host, config.smtp_port) as server:
            server.starttls()
            server.login(config.smtp_username, config.smtp_password)
            server.sendmail(config.email_from, config.email_to, msg.as_string())

    logger.info("Report sent successfully")
