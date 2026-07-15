# tap-databricks

`tap-databricks` is a Singer tap for [Databricks](https://www.databricks.com/), built with the [Meltano Tap SDK](https://sdk.meltano.com).

It extracts data from Databricks SQL Warehouses over the [`databricks-sqlalchemy`](https://pypi.org/project/databricks-sqlalchemy/) dialect, with automatic stream discovery, incremental replication, and support for both personal-access-token and service-principal (OAuth M2M) authentication.

## Capabilities

- `catalog`
- `state`
- `discover`
- `about`
- `activate-version`
- `stream-maps`
- `schema-flattening`
- `structured-logging`

Supports **full-table** and **incremental** replication, automatic schema discovery via SQLAlchemy reflection, and resilient discovery that skips unreadable tables rather than failing the whole run.

## Installation

Install from PyPI:

```bash
uv tool install tap-databricks
```

Install from GitHub:

```bash
uv tool install git+https://github.com/MeltanoLabs/tap-databricks.git@main
```

Or add it to a Meltano project:

```bash
meltano add extractor tap-databricks
```

## Configuration

### Accepted Config Options

The following settings are supported by `tap-databricks`:

| Setting | Type | Required? | Description |
| :--- | :--- | :---: | :--- |
| `server_hostname` | string | **Yes** | Workspace hostname, e.g. `dbc-xxxx.cloud.databricks.com` (no `https://`). |
| `http_path` | string | **Yes** | SQL Warehouse / cluster HTTP path, e.g. `/sql/1.0/warehouses/abc123`. |
| `catalog` | string | **Yes** | Unity Catalog catalog to extract from. |
| `schema` | string | No | Optional schema to scope extraction; if omitted, all schemas in the catalog are discovered. |
| `access_token` | string (secret) | No | Databricks personal access token (PAT authentication). |
| `client_id` | string | No | Service principal Client ID (M2M OAuth authentication). |
| `client_secret` | string (secret) | No | Service principal OAuth secret (M2M OAuth authentication). |

> [!IMPORTANT]
> You must provide either `access_token` (PAT authentication) **or** both `client_id` and `client_secret` (M2M OAuth authentication). If both are present, the PAT takes precedence.

A full list of supported settings and capabilities is available by running:

```bash
tap-databricks --about
```

### Configure using environment variables

This tap imports environment variables (including those in a working-directory `.env`) when `--config=ENV` is provided. Each setting maps to `TAP_DATABRICKS_<SETTING>` — e.g. `server_hostname` → `TAP_DATABRICKS_SERVER_HOSTNAME`.

### Source Authentication and Authorization

`tap-databricks` supports two authentication methods, evaluated **PAT-first**:

1. **Personal Access Token (PAT).** Create a token under your Databricks user settings and supply it via `access_token`.
2. **OAuth Service Principal (M2M).** Register a service principal, generate an OAuth secret, and configure `client_id` and `client_secret`. The service principal needs `CAN USE` on the SQL Warehouse and appropriate `USE CATALOG` / `USE SCHEMA` / `SELECT` grants.

Example (PAT):

```json
{
  "server_hostname": "dbc-xxxx.cloud.databricks.com",
  "http_path": "/sql/1.0/warehouses/abc123",
  "catalog": "my_catalog",
  "access_token": "dapi..."
}
```

Example (service principal):

```json
{
  "server_hostname": "dbc-xxxx.cloud.databricks.com",
  "http_path": "/sql/1.0/warehouses/abc123",
  "catalog": "my_catalog",
  "client_id": "your-sp-client-id",
  "client_secret": "your-sp-oauth-secret"
}
```

## Behavior & Limitations

Please read this section before running against a production catalog — a few Databricks-specific behaviors are worth knowing up front.

### Primary keys are informational

Databricks primary keys are *informational* constraints and are optional. Discovery reads declared primary keys from `information_schema` and populates `key_properties` when they exist. When a table has **no declared primary key**, `key_properties` is empty — and targets cannot deduplicate or upsert without a key. Discovery logs a summary of how many streams have no key.

To supply a key yourself, set `table-key-properties` in `meltano.yml` (or edit the catalog directly):

```yaml
plugins:
  extractors:
    - name: tap-databricks
      metadata:
        my_table:
          table-key-properties: [id]
```

### Replication keys are suggested, not activated

During discovery, the tap suggests a replication key when a table has a column matching common names (`lastmodifieddate`, `systemmodstamp`, `updated_at`, and similar). This is **suggest-only**: it sets `replication-key` but leaves the stream in full-table mode. To turn on incremental replication, set the method yourself:

```yaml
plugins:
  extractors:
    - name: tap-databricks
      metadata:
        my_table:
          replication-method: INCREMENTAL
          replication-key: LASTMODIFIEDDATE
```

Incremental uses a `>=` bookmark, so the boundary row(s) at the last replication-key value are re-emitted on the next run (at-least-once delivery). Targets that upsert on the primary key will collapse the duplicate; append-only targets will not.

### Scoping discovery

Set `schema` to restrict discovery to a single schema. Without it, all schemas in the catalog are discovered, which can be slow on large catalogs.

### Unity Catalog only

The `databricks-sqlalchemy` dialect is built and tested against Unity-Catalog-enabled workspaces. `hive_metastore` is untested.

### `--test` mode and full-table streams

`tap-databricks --config CONFIG --test` succeeds (exit 0), but full-table streams log an `AbortedSyncFailedException` line as the SDK aborts each stream after the single-record limit. This is expected SDK behavior for full-table streams and does not indicate a failure.

## Usage

Run `tap-databricks` standalone or in a [Meltano](https://meltano.com/) pipeline.

### Executing the tap directly

```bash
tap-databricks --version
tap-databricks --help
tap-databricks --config CONFIG --discover > ./catalog.json
tap-databricks --config CONFIG --catalog ./catalog.json
```

## Developer Resources

### Initialize your development environment

Prerequisites: Python 3.10+ and [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

### Running tests

```bash
uv run pytest
```

The suite has two parts:

- **Mocked unit tests** (`tests/test_discovery_mock.py`) run offline with no credentials — these are the CI backbone.
- **Live integration tests** (`tests/test_core.py`) run only when Databricks credentials are present, and skip otherwise. Provide credentials via environment variables or a gitignored `.env` file (copy `.env.example` to `.env` and fill it in). Optionally set `TAP_DATABRICKS_TEST_STREAM` to pin which stream the live tests exercise; otherwise the first discovered stream is used.

You can also invoke the CLI directly:

```bash
uv run tap-databricks --help
```

### Testing with Meltano

_This tap works in any Singer environment and does not require Meltano; the examples below are for convenience._

```bash
uv tool install meltano
meltano invoke tap-databricks --version
meltano run tap-databricks target-jsonl
```

### SDK Dev Guide

See the [SDK dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more on building taps and targets.
