"""YAML configuration loader with environment variable resolution."""

from __future__ import annotations

import os
from decimal import Decimal
from pathlib import Path
from typing import Any

import yaml

from portfolio_manager.core.types import PortfolioConfig

_DEFAULT_CONFIG_PATH = Path(__file__).resolve().parents[3] / "config" / "portfolio.yml"


def _resolve_env_vars(value: Any) -> Any:
    """Resolve $-prefixed string values from environment variables.

    Returns None if the env var is not set (for optional fields like email).
    """
    if isinstance(value, str) and value.startswith("$"):
        env_name = value[1:]
        return os.environ.get(env_name)
    return value


def _resolve_dict(d: dict[str, Any]) -> dict[str, Any]:
    """Recursively resolve env vars in a dict."""
    resolved: dict[str, Any] = {}
    for k, v in d.items():
        if isinstance(v, dict):
            resolved[k] = _resolve_dict(v)
        elif isinstance(v, list):
            resolved[k] = [_resolve_env_vars(item) for item in v]
        else:
            resolved[k] = _resolve_env_vars(v)
    return resolved


def load_config(config_path: str | Path | None = None) -> PortfolioConfig:
    """Load portfolio configuration from YAML file.

    Args:
        config_path: Path to YAML config file. Defaults to config/portfolio.yml.

    Returns:
        PortfolioConfig with env-var-resolved values.
    """
    path = Path(config_path) if config_path else _DEFAULT_CONFIG_PATH
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with open(path) as f:
        raw = yaml.safe_load(f)

    resolved = _resolve_dict(raw)

    # Parse target allocations as Decimal
    target_allocs: dict[str, Decimal] = {}
    raw_allocs = resolved.get("target_allocations") or {}
    for symbol, weight in raw_allocs.items():
        target_allocs[symbol] = Decimal(str(weight))

    # Parse email_to as list
    email_to_raw = resolved.get("email", {}).get("to")
    email_to: list[str] | None = None
    if email_to_raw is not None:
        if isinstance(email_to_raw, list):
            # Filter out None values (unresolved env vars)
            email_to = [addr for addr in email_to_raw if addr is not None]
            if not email_to:
                email_to = None
        elif isinstance(email_to_raw, str):
            email_to = [email_to_raw]

    email_cfg = resolved.get("email", {})
    smtp_port_raw = email_cfg.get("smtp_port")

    return PortfolioConfig(
        base_currency=resolved.get("base_currency", "USD"),
        target_allocations=target_allocs,
        rebalance_threshold=Decimal(str(resolved.get("rebalance_threshold", "0.05"))),
        atr_period=int(resolved.get("atr_period", 14)),
        atr_multiplier=Decimal(str(resolved.get("atr_multiplier", "2.0"))),
        smtp_host=email_cfg.get("smtp_host"),
        smtp_port=int(smtp_port_raw) if smtp_port_raw is not None else None,
        smtp_username=email_cfg.get("smtp_username"),
        smtp_password=email_cfg.get("smtp_password"),
        email_from=email_cfg.get("from"),
        email_to=email_to,
        email_subject_template=email_cfg.get("subject_template", "Portfolio Report — {date}"),
    )
