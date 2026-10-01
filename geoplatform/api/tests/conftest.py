"""Pytest fixtures / environment setup for the API test suite.

Auth (app.auth) fails closed and raises at import time when JWT_SECRET or
API_KEY are missing. conftest runs before test modules are imported, so set
deterministic test credentials here to guarantee they exist during collection.
"""
import os

os.environ.setdefault("JWT_SECRET", "test-jwt-secret")
os.environ.setdefault("API_KEY", "test-api-key")
