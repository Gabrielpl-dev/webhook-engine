"""Opaque identifier and secret generation."""

from __future__ import annotations

import os


def new_id(prefix: str) -> str:
    """Return an opaque id: ``<prefix>_<24 lowercase hex chars>``."""
    return prefix + os.urandom(12).hex()


def new_secret() -> str:
    """Return a signing secret: ``whsec_`` + 32 random bytes as lowercase hex."""
    return "whsec_" + os.urandom(32).hex()
