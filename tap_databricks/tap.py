"""Databricks tap class."""

from __future__ import annotations

from singer_sdk import typing as th  # JSON schema typing helpers
from singer_sdk.sql import SQLTap
from singer_sdk.helpers.capabilities import PluginCapabilities

from tap_databricks.client import DatabricksStream


class TapDatabricks(SQLTap):
    """Singer tap for Databricks."""

    name = "tap-databricks"

    default_stream_class = DatabricksStream

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
        "server_hostname", th.StringType, required=True,
        title="Server Hostname",
        description="Workspace hostname, e.g. dbc-xxxx.cloud.databricks.com (no https://)",
        ),
        th.Property(
            "http_path", th.StringType, required=True,
            title="HTTP Path",
            description="SQL Warehouse / cluster HTTP path, e.g. /sql/1.0/warehouses/abc123",
        ),
        th.Property(
            "access_token", th.StringType, required=False, secret=True,
            title="Access Token",
            description="Databricks personal access token",
        ),
        th.Property(
            "client_id", th.StringType, required=False,
            title="Client ID", description="Service principal Client ID (M2M auth)"
        ),
        th.Property(
            "client_secret", th.StringType, required=False, secret=True,
            title="Client Secret", description="Service principal OAuth secret (M2M auth)"
        ),
        th.Property(
            "catalog", th.StringType, required=True,
            title="Catalog",
            description="Unity Catalog catalog to extract from",
        ),
        th.Property(
            "schema", th.StringType, required=False,
            title="Schema",
            description="Optional schema to scope extraction; if omitted, all schemas in the catalog are discovered",
        ),
    ).to_dict()

    # BATCH is inherited from the SDK but not yet implemented/validated here
    # (planned as future work)
    capabilities =  capabilities = [c for c in SQLTap.capabilities if c != PluginCapabilities.BATCH]

if __name__ == "__main__":
    TapDatabricks.cli()
