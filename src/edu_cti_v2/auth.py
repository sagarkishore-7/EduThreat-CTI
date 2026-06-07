"""Lightweight admin authentication for the dedicated v2 API surface."""

from __future__ import annotations

import base64
import hashlib
import hmac
import os
import secrets
from datetime import datetime, timedelta
from typing import Optional

from fastapi import Header, HTTPException
from pydantic import BaseModel

ADMIN_USERNAME = os.getenv("EDUTHREAT_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_HASH = os.getenv("EDUTHREAT_ADMIN_PASSWORD_HASH")
ADMIN_API_KEY = os.getenv("EDUTHREAT_ADMIN_API_KEY")

SESSION_DURATION_HOURS = 24

# Best-effort revocation set for logout. It is per-process, so it doesn't cross
# API workers — that's acceptable: tokens are stateless + short-lived (24h), and
# logout also clears the client-side token. The important property (a token
# minted on worker A validates on worker B) is provided by the HMAC signature
# below, NOT by shared in-memory state — that in-memory store is exactly what
# broke admin login under multiple API workers.
_revoked_tokens: set[str] = set()


def _session_secret() -> bytes:
    """Stable signing secret shared by all API workers (same env → same secret).

    Prefers an explicit secret; otherwise derives one deterministically from the
    admin credential so every worker computes the same value without requiring a
    new env var. Never a per-process random value (that would make tokens minted
    on one worker fail on another).
    """
    explicit = os.getenv("EDUTHREAT_ADMIN_SECRET")
    if explicit:
        return explicit.encode()
    seed = ADMIN_PASSWORD_HASH or ADMIN_API_KEY or os.getenv("EDUTHREAT_ADMIN_PASSWORD", "admin123")
    return hashlib.sha256(f"eduthreat-admin-session::{seed}".encode()).digest()


class V2LoginRequest(BaseModel):
    username: str
    password: str


class V2LoginResponse(BaseModel):
    success: bool
    session_token: Optional[str] = None
    expires_at: Optional[str] = None
    message: str


def hash_password(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()


def verify_password(password: str) -> bool:
    if not ADMIN_PASSWORD_HASH:
        default_hash = hash_password(os.getenv("EDUTHREAT_ADMIN_PASSWORD", "admin123"))
        return hash_password(password) == default_hash
    return hash_password(password) == ADMIN_PASSWORD_HASH


def verify_api_key(api_key: str) -> bool:
    if not ADMIN_API_KEY:
        return False
    return secrets.compare_digest(api_key, ADMIN_API_KEY)


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _sign(payload: str) -> str:
    return _b64(hmac.new(_session_secret(), payload.encode(), hashlib.sha256).digest())


def create_session_token() -> tuple[str, datetime]:
    """Mint a stateless, signed session token: '<exp_ts>.<nonce>.<hmac>'.

    Stateless by design so it validates on any API worker without shared memory.
    """
    expires_at = datetime.now() + timedelta(hours=SESSION_DURATION_HOURS)
    payload = f"{int(expires_at.timestamp())}.{secrets.token_urlsafe(8)}"
    token = f"{payload}.{_sign(payload)}"
    return token, expires_at


def verify_session(session_token: str) -> bool:
    if not session_token or session_token in _revoked_tokens:
        return False
    try:
        exp_str, nonce, signature = session_token.rsplit(".", 2)
    except ValueError:
        return False
    payload = f"{exp_str}.{nonce}"
    if not hmac.compare_digest(signature, _sign(payload)):
        return False
    try:
        if datetime.now().timestamp() > float(exp_str):
            return False
    except ValueError:
        return False
    return True


def revoke_session(session_token: Optional[str]) -> None:
    # Best-effort: prevents reuse on this worker; the token expires regardless.
    if session_token:
        _revoked_tokens.add(session_token)


def authenticate(
    authorization: Optional[str] = Header(None),
    x_api_key: Optional[str] = Header(None),
    x_session_token: Optional[str] = Header(None),
) -> bool:
    if x_session_token and verify_session(x_session_token):
        return True

    if x_api_key and verify_api_key(x_api_key):
        return True

    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        if verify_session(token):
            return True

    raise HTTPException(
        status_code=401,
        detail="Authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )

