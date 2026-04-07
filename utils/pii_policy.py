"""
Central PII policy for quarantine and audit paths.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

import pandas as pd

from config.settings import get_settings
from utils import PII_handler

Action = Literal["drop", "hash", "mask", "keep"]


@dataclass(frozen=True)
class FieldRule:
    action: Action
    output_field: str | None = None


PII_RULES: dict[str, dict[str, FieldRule]] = {
    "source_customers": {
        "email": FieldRule("hash", output_field="email_hash"),
        "phone": FieldRule("hash", output_field="phone_hash"),
        "full_name": FieldRule("drop"),
    },
    "source_drivers": {
        "driver_phone": FieldRule("hash", output_field="driver_phone_hash"),
        "national_id": FieldRule("hash", output_field="national_id_hash"),
        "driver_name": FieldRule("mask"),
    },
    "source_agents": {
        "agent_email": FieldRule("hash", output_field="agent_email_hash"),
        "agent_phone": FieldRule("hash", output_field="agent_phone_hash"),
        "agent_name": FieldRule("mask"),
    },
    "source_orders": {},
    "source_tickets": {},
    "source_ticket_events": {},
    "orphan_reconciliation": {},
    "unknown": {},
}


def sanitize_record(entity_type: str, record: dict[str, Any]) -> dict[str, Any]:
    """Apply PII rules; unknown entity types pass through unchanged."""
    if not record:
        return {}

    rules = PII_RULES.get(entity_type, {})
    if not rules:
        return dict(record)

    settings = get_settings()
    pepper = settings.pii_hash_pepper
    out: dict[str, Any] = {}

    for k, v in record.items():
        rule = rules.get(k)
        if rule is None:
            out[k] = v
            continue
        if rule.action == "drop":
            continue
        if rule.action == "keep":
            out[k] = v
            continue
        if rule.action == "hash":
            hashed = _hash_scalar(v, pepper)
            out_key = rule.output_field or k
            out[out_key] = hashed
            continue
        if rule.action == "mask":
            masked = _mask_scalar(v)
            out_key = rule.output_field or k
            out[out_key] = masked
            continue
        out[k] = v

    return out


def _hash_scalar(value: Any, pepper: str) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    s = pd.Series([value])
    return PII_handler.hash_value(s, pepper).iloc[0]


def _mask_scalar(value: Any) -> Any:
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return value
    s = pd.Series([value])
    return PII_handler.partial_masking(s, keep_first=1).iloc[0]
