"""Test suite for tap-databricks.
Shared pytest setup.

Loads a local, gitignored `.env` (if present) into the environment so the
live-gated tests in test_core.py can read TAP_DATABRICKS_* credentials without
manual `export`. No-op if `.env` is absent (e.g. public CI), so the live tests
simply skip there.
"""
from dotenv import load_dotenv

# Looks for a `.env` file walking up from the tests dir / cwd; harmless if none.
load_dotenv()