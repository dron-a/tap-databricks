"""Live integration tests — run only when real credentials are present.

Uses the SDK's standard tap test suite (get_tap_test_class), which runs
discovery and a bounded sync against a real Databricks workspace, checking
schema/record/state conformance.

SKIPPED automatically when credentials are absent, so public CI (no secrets)
skips cleanly; locally / credential-gated CI, provide the env vars below
(conftest.py loads a gitignored .env if present) and they run.
The ENV vars are handled by dotenv so one can maintin a simple .env file for this test
The sample .env.example file from the repo can be referred for this 

Catalog resolution (first match wins) — nothing schema-specific is committed:
  1. tests/fixtures/test_catalog.json  — if you drop a catalog here (gitignored),
     it is used verbatim. For fine control over which streams/settings are tested.
  2. otherwise, a catalog is built dynamically from live discovery: one stream is
     selected (named by TAP_DATABRICKS_TEST_STREAM, else the first discovered).

Required env vars (SDK maps TAP_DATABRICKS_* -> config automatically):
    TAP_DATABRICKS_SERVER_HOSTNAME
    TAP_DATABRICKS_HTTP_PATH
    TAP_DATABRICKS_CATALOG
    TAP_DATABRICKS_SCHEMA            (recommended: scope to a small schema)
    one auth set:
      TAP_DATABRICKS_ACCESS_TOKEN                              (PAT), or
      TAP_DATABRICKS_CLIENT_ID + TAP_DATABRICKS_CLIENT_SECRET  (SP/M2M)
    TAP_DATABRICKS_TEST_STREAM      (optional: which stream to test; else first)
"""
import json
import os
import pathlib

import pytest
from singer_sdk.testing import get_tap_test_class

from tap_databricks.tap import TapDatabricks

REQUIRED = ("TAP_DATABRICKS_SERVER_HOSTNAME", "TAP_DATABRICKS_HTTP_PATH", "TAP_DATABRICKS_CATALOG")
HAS_AUTH = os.environ.get("TAP_DATABRICKS_ACCESS_TOKEN") or (
    os.environ.get("TAP_DATABRICKS_CLIENT_ID") and os.environ.get("TAP_DATABRICKS_CLIENT_SECRET")
)
CREDS_PRESENT = all(os.environ.get(k) for k in REQUIRED) and bool(HAS_AUTH)

CATALOG_PATH = pathlib.Path(__file__).parent / "fixtures" / "test_catalog.json"

pytestmark = pytest.mark.skipif(
    not CREDS_PRESENT,
    reason="Live Databricks credentials not set (TAP_DATABRICKS_* env vars); skipping integration tests.",
)


def _config_from_env() -> dict:
    keys = (
        "server_hostname", "http_path", "catalog", "schema",
        "access_token", "client_id", "client_secret",
    )
    cfg = {}
    for k in keys:
        v = os.environ.get(f"TAP_DATABRICKS_{k.upper()}")
        if v:
            cfg[k] = v
    return cfg


def _dynamic_catalog(config: dict) -> dict:
    """Build a catalog from live discovery and select exactly one stream.

    Nothing schema-specific is committed: the catalog is generated at runtime
    from whatever workspace the credentials point at. The stream is chosen by
    TAP_DATABRICKS_TEST_STREAM if set, otherwise the first discovered stream.
    """
    tap = TapDatabricks(config=config)
    catalog = tap.catalog_dict  # runs our discover_catalog_entries
    streams = catalog.get("streams", [])
    if not streams:
        return catalog

    wanted = os.environ.get("TAP_DATABRICKS_TEST_STREAM")
    chosen = next((s for s in streams if s["tap_stream_id"] == wanted), streams[0]) if wanted else streams[0]
    catalog["streams"] = [chosen]
    for m in chosen.get("metadata", []):
        if m.get("breadcrumb") == []:
            m["metadata"]["selected"] = True
    return catalog


def _resolve_catalog(config: dict) -> dict:
    # Tier 1: explicit on-disk catalog (gitignored) wins.
    if CATALOG_PATH.exists():
        return json.loads(CATALOG_PATH.read_text())
    # Tier 2: build dynamically from live discovery.
    return _dynamic_catalog(config)


# Generated standard pytest test class: discovery + bounded sync conformance.
# Only constructed when creds are present, so import-time is safe in public CI.
if CREDS_PRESENT:
    _config = _config_from_env()
    TestTapDatabricks = get_tap_test_class(
        tap_class=TapDatabricks,
        config=_config,
        catalog=_resolve_catalog(_config),
    )