"""Lightweight admin authentication for the dedicated v2 API surface."""

from __future__ import annotations

import hashlib
import os
import secrets
from datetime import datetime, timedelta
from typing import Dict, Optional

from fastapi import Header, HTTPException
from pydantic import BaseModel

ADMIN_USERNAME = os.getenv("EDUTHREAT_ADMIN_USERNAME", "admin")
ADMIN_PASSWORD_HASH = os.getenv("EDUTHREAT_ADMIN_PASSWORD_HASH")
ADMIN_API_KEY = os.getenv("EDUTHREAT_ADMIN_API_KEY")

SESSION_DURATION_HOURS = 24
_active_sessions: Dict[str, datetime] = {}


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


def verify_session(session_token: str) -> bool:
    if session_token not in _active_sessions:
        return False

    expires = _active_sessions[session_token]
    if datetime.now() > expires:
        del _active_sessions[session_token]
        return False
    return True


def create_session_token() -> tuple[str, datetime]:
    session_token = secrets.token_urlsafe(32)
    expires_at = datetime.now() + timedelta(hours=SESSION_DURATION_HOURS)
    _active_sessions[session_token] = expires_at
    return session_token, expires_at


def revoke_session(session_token: Optional[str]) -> None:
    if session_token and session_token in _active_sessions:
        del _active_sessions[session_token]


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

