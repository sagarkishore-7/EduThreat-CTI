"""
EduThreat-CTI REST API

FastAPI-based REST API for serving cyber threat intelligence data
to the CTI dashboard and external consumers.
"""

from .main import app, create_app

__all__ = ["app", "create_app"]

