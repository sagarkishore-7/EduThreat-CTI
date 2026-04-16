"""Compatibility shim for setuptools-based tooling.

Project metadata lives in ``pyproject.toml`` so editable installs and CI share one
consistent source of truth.
"""

from setuptools import setup


setup()
